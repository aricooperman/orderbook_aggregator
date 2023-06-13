use crate::errors::{KeyrockError, Result};
use crate::try_values_to_orderbook;
use crate::types::OrderBook;
use async_trait::async_trait;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite, MaybeTlsStream, WebSocketStream};
use url::Url;

pub enum Command {
    Send(Message),
    Subscribe(String),
    Unsubscribe(String),
    Disconnect,
    Ping,
}

pub enum DataMessageType {
    SubscribeSuccessful(String),
    UnsubscribeSuccessful(Option<String>),
    OrderBookData(String, Value, Value),
    // Future other types
}

pub type OrderbookSubscriptions = Arc<RwLock<HashMap<String, Sender<OrderBook>>>>;

#[async_trait]
pub trait ExchangeWebSocketApi: Sized + Display + 'static {
    fn new(
        url: Url,
        command_in_tx: UnboundedSender<Command>,
        orderbook_subscriptions: OrderbookSubscriptions,
        is_disconnected: Arc<RwLock<bool>>,
    ) -> Self;

    fn default_url() -> &'static str;

    async fn connect_default() -> Result<Self> {
        Self::connect(Self::default_url()).await
    }

    async fn connect(conn_url: &str) -> Result<Self> {
        let url = Url::parse(conn_url)?; //Check if valid url

        let websocket = match connect_async(&url).await {
            Ok((websocket, _)) => {
                log::info!("Connected to {} WebSocket API", Self::exchange_name());
                websocket
            }
            Err(e) => return Err(KeyrockError::ConnectionError(Box::new(e))),
        };

        let (command_in_tx, command_in_rx) = futures_channel::mpsc::unbounded::<Command>();
        let (websocket_sink, websocket_stream) = websocket.split();

        let orderbook_subscriptions = Arc::new(RwLock::new(HashMap::new()));

        let disconnect_flag = Arc::new(RwLock::new(false));

        tokio::spawn(Self::websocket_incoming_loop(
            websocket_stream,
            command_in_tx.clone(),
            Arc::clone(&orderbook_subscriptions),
        ));

        tokio::spawn(Self::command_incoming_loop(
            command_in_rx,
            websocket_sink,
            Arc::clone(&orderbook_subscriptions),
            Arc::clone(&disconnect_flag),
        ));

        Ok(Self::new(
            url,
            command_in_tx,
            orderbook_subscriptions,
            disconnect_flag,
        ))
    }

    async fn subscribe_orderbook(
        &mut self,
        symbol: &str,
        depth: usize,
    ) -> Result<Receiver<OrderBook>> {
        self.check_orderbook_subscription_params(symbol, depth)?;

        let channel = Self::order_book_channel(symbol, depth);
        let mut subs = self.orderbook_subscriptions().write()?;
        return match (*subs).get_mut(&channel) {
            None => {
                if let Err(e) = self
                    .command_in_tx()
                    .unbounded_send(Command::Subscribe(channel.clone()))
                {
                    return Err(KeyrockError::WebSocketSendError(
                        format!(
                            "Unable to subscribe to {} order book stream for {}",
                            Self::exchange_name(),
                            symbol
                        ),
                        Box::from(e),
                    ));
                }

                //TODO drive bounds via config - should have proper amount of backpressure
                let (tx, rx) = tokio::sync::broadcast::channel::<OrderBook>(128);
                (*subs).insert(channel, tx);
                Ok(rx)
            }
            Some(sender) => Ok(sender.subscribe()),
        };
    }

    async fn disconnect(&mut self) -> Result<()> {
        match self.command_in_tx().unbounded_send(Command::Disconnect) {
            Ok(_) => Ok(()),
            Err(e) => Err(KeyrockError::DisconnectionError(Box::new(e))),
        }
    }

    // fn is_disconnected(&self) -> bool;

    // fn set_disconnected(&mut self);

    fn command_in_tx(&self) -> &UnboundedSender<Command>;

    //TODO Other subscriber types - use map from subscription type -> subscription -> subscribers
    fn orderbook_subscriptions(&self) -> &OrderbookSubscriptions;

    fn create_subscribe_message(channel: &str) -> Result<String>;

    fn create_unsubscribe_message(channel: &str) -> Result<String>;

    fn unsubscribe_channel(
        command_in_tx: &UnboundedSender<Command>,
        channel: &str,
        orderbook_subscriptions: &OrderbookSubscriptions,
    ) {
        match orderbook_subscriptions.read() {
            Ok(subs) => {
                if !subs.contains_key(channel) {
                    return; // Already unsubscribed
                }
            }
            Err(e) => log::warn!(
                "Unable to lock orderbook subscriptions for reading: {:?}",
                e
            ),
        }

        if !command_in_tx.is_closed() {
            if let Err(e) = command_in_tx.unbounded_send(Command::Unsubscribe(channel.to_string()))
            {
                log::error!("Unable to send unsubscribe command to channel: {:?}", e)
            }
        }
    }

    async fn process_message(
        msg: &str,
        orderbook_subscriptions: &OrderbookSubscriptions,
    ) -> Result<DataMessageType>;

    fn check_orderbook_subscription_params(&self, symbol: &str, depth: usize) -> Result<()>;

    fn order_book_channel(symbol: &str, depth: usize) -> String;

    fn exchange_name() -> &'static str;

    fn handle_orderbook_data(
        command_in_tx: &UnboundedSender<Command>,
        orderbook_subscriptions: &OrderbookSubscriptions,
        bids: &Value,
        asks: &Value,
        channel: &String,
    ) {
        let subs = match orderbook_subscriptions.read() {
            Ok(subs) => subs,
            Err(e) => {
                log::error!(
                    "Unable to acquire read lock on orderbook subscribers: {:?}",
                    e
                );
                return;
            }
        };

        match (*subs).get(channel) {
            None => {
                log::debug!("No more orderbook subscribers for {}", channel);
                Self::unsubscribe_channel(command_in_tx, channel, orderbook_subscriptions);
            }
            Some(sender) => {
                if sender.receiver_count() == 0 {
                    log::debug!("No more orderbook listeners for {}", channel);
                    Self::unsubscribe_channel(command_in_tx, channel, orderbook_subscriptions);
                    return;
                }

                match Self::parse_orderbook(bids, asks) {
                    Ok(obm) => {
                        if let Err(e) = sender.send(obm) {
                            log::error!(
                                "Unable to broadcast order book {}, unsubscribing: {:?}",
                                channel,
                                e
                            );
                            Self::unsubscribe_channel(
                                command_in_tx,
                                channel,
                                orderbook_subscriptions,
                            );
                        }
                    }
                    Err(e) => {
                        log::error!("Problem parsing orderbook message: {:?}", e);
                    }
                }
            }
        }
    }

    fn parse_orderbook(bids: &Value, asks: &Value) -> Result<OrderBook> {
        try_values_to_orderbook(
            Self::exchange_name(),
            bids,
            asks,
            10, //TODO
        )
    }

    async fn websocket_incoming_loop(
        mut websocket_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        command_in_tx: UnboundedSender<Command>,
        orderbook_subscriptions: OrderbookSubscriptions,
    ) {
        loop {
            match websocket_stream.next().await {
                None => {
                    log::debug!("No more incoming messages from websocket");
                    break;
                }
                Some(message_result) => match message_result {
                    Ok(message) => match message {
                        Message::Text(msg) => {
                            match Self::process_message(&msg, &orderbook_subscriptions).await {
                                Ok(msg_type) => match msg_type {
                                    DataMessageType::SubscribeSuccessful(channel) => {
                                        log::info!("Subscribed to {}", channel)
                                    }
                                    DataMessageType::UnsubscribeSuccessful(channel) => {
                                        match channel {
                                            None => log::info!("Unsubscribed"),
                                            Some(channel) => {
                                                log::info!("Unsubscribed to {}", channel)
                                            }
                                        }
                                    }
                                    DataMessageType::OrderBookData(channel, bids, asks) => {
                                        Self::handle_orderbook_data(
                                            &command_in_tx,
                                            &orderbook_subscriptions,
                                            &bids,
                                            &asks,
                                            &channel,
                                        )
                                    }
                                },
                                Err(e) => {
                                    log::error!("Problem occurred processing message: {:?}", e)
                                }
                            }
                        }
                        Message::Ping(data) => {
                            log::debug!("Received Ping message, sending Pong");
                            if let Err(e) =
                                command_in_tx.unbounded_send(Command::Send(Message::Pong(data)))
                            {
                                log::error!("Failed to send pong message: {:?}", e);
                            }
                        }
                        Message::Pong(_) => {
                            log::debug!("Received a pong message");
                        }
                        //TODO handle 24 hour or other closure unrequested
                        Message::Close(_) => {
                            log::debug!("Received close message");
                            //TODO reconnect logic
                        }
                        _ => {
                            log::error!("Unhandled message type: {:?}", message);
                        }
                    },
                    Err(e) => {
                        log::error!("Error retrieving next message: {:?}", e)
                    }
                },
            }
        }
    }

    async fn command_incoming_loop(
        mut command_in_rx: UnboundedReceiver<Command>,
        mut websocket_sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        orderbook_subscriptions: OrderbookSubscriptions,
        disconnect_flag: Arc<RwLock<bool>>,
    ) {
        loop {
            let command = match time::timeout(Duration::from_secs(15), command_in_rx.next()).await {
                Ok(opt_command) => match opt_command {
                    None => break,
                    Some(command) => command,
                },
                Err(_) => {
                    websocket_sink.send(Message::Close(None)).await.unwrap();
                    Command::Ping
                }
            };

            match command {
                Command::Subscribe(channel) => {
                    let msg = match Self::create_subscribe_message(&channel) {
                        Ok(msg) => msg,
                        Err(e) => {
                            log::error!("{:?}", e);
                            continue;
                        }
                    };

                    if let Err(e) = websocket_sink.send(Message::Text(msg)).await {
                        log::warn!("Unable to subscribe to channel {}: {:?}", &channel, e);
                    }
                }
                Command::Unsubscribe(channel) => {
                    match orderbook_subscriptions.write() {
                        Ok(mut subs) => match (*subs).remove(&channel) {
                            None => continue, //Already unsubscribed
                            Some(sender) => {
                                drop(sender);
                            }
                        },
                        Err(e) => {
                            log::error!("{:?}", KeyrockError::from(e));
                            continue;
                        }
                    };

                    let msg = match Self::create_unsubscribe_message(&channel) {
                        Ok(msg) => msg,
                        Err(e) => {
                            log::error!("{:?}", e);
                            continue;
                        }
                    };

                    log::info!("Unsubscribing from channel {}", channel);
                    if let Err(e) = websocket_sink.send(Message::Text(msg)).await {
                        log::warn!("Unable to unsubscribe to channel {}: {:?}", channel, e);
                    }
                }
                Command::Disconnect => {
                    match disconnect_flag.write() {
                        Ok(mut flag) => *flag = true,
                        Err(e) => log::warn!("Unable to lock for writing disconnect flag: {:?}", e),
                    }

                    match orderbook_subscriptions.write() {
                        Ok(mut subs) => (*subs).clear(),
                        Err(e) => {
                            log::error!(
                                "Unable to acquire write lock on orderbook subscribers: {:?}",
                                e
                            )
                        }
                    }

                    match websocket_sink.send(Message::Close(None)).await {
                        Ok(_) => {
                            log::info!("Disconnected from WebSocket API");
                            command_in_rx.close();
                            break;
                        }
                        Err(e) => {
                            match e {
                                tungstenite::error::Error::AlreadyClosed => {} //TODO for now ignore, need a check if we need to reconnect
                                _ => log::error!("Unable to send close message: {:?}", e),
                            }
                        }
                    }

                    let _ = websocket_sink.close().await;
                }
                Command::Send(msg) => {
                    if let Err(e) = websocket_sink.send(msg).await {
                        match e {
                            tungstenite::error::Error::AlreadyClosed => {} //TODO for now ignore, need a check if we need to reconnect
                            _ => log::warn!("Unable to send message: {:?}", e),
                        }
                    }
                }
                Command::Ping => {
                    if let Err(e) = websocket_sink.send(Message::Ping(vec![])).await {
                        match e {
                            tungstenite::error::Error::AlreadyClosed => {} //TODO for now ignore, need a check if we need to reconnect
                            _ => log::warn!("Unable to send ping message: {:?}", e),
                        }
                    }
                }
            }
        }
    }
}
