use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::sleep;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::errors::{Error, Result};
use crate::try_values_to_orderbook;
use crate::types::Orderbook;

/// An enum of the various commands that can be sent to the websocket sink handler
#[derive(Clone, Debug)]
pub enum Command {
    /// Command to send the enclosed WS Message
    Send(Message),
    /// Request to subscribe the enclosed channel
    Subscribe(String),
    /// Request to unsubscribe the enclosed channel
    Unsubscribe(String),
    /// Request a clean disconnection from server
    Disconnect,
    /// Send a ping request
    Ping,
}

/// Enum representing message types coming back from websocket stream
#[derive(Clone, Debug)]
pub enum DataMessageType {
    /// The enclosed channel subscribed successfully
    SubscribeSuccessful(String),
    /// The enclosed channel, if it is provided, unsubscribed successfully
    UnsubscribeSuccessful(Option<String>),
    /// A message that represents an orderbook
    OrderBookData(String, Value, Value),
    /// The exchange requested we reconnect
    Reconnect,
}

/// A simplifying type for the shared map of channel to orderbook subscribers
pub type OrderbookSubscriptions = Arc<RwLock<HashMap<String, Sender<Orderbook>>>>;

/// Trait that defines the behavior of an exchange websocket api
#[async_trait]
pub trait ExchangeWebSocketApi: Sized + Display + 'static {
    /// Create an instance of the implementing ExchangeWebSocketApi
    ///
    /// # Arguments
    ///
    /// * `url`: The url of the websocket endpoint
    /// * `command_in_tx`: A channel sender to send Commands to the ws sink
    /// * `orderbook_subscriptions`: The shared map of orderbook subscriptions
    ///
    /// returns: A new instance of the exchange websocket api
    fn new(
        url: Url,
        command_in_tx: Sender<Command>,
        orderbook_subscriptions: OrderbookSubscriptions,
    ) -> Self;

    /// Return the default endpoint url for this implementation
    ///
    /// returns: The default url str
    fn default_url() -> &'static str;

    /// Connect to the websocket endpoint with the default endpoint url
    ///
    /// returns: The Result of trying to connect with the default url
    async fn connect_default() -> Result<Self> {
        Self::connect(Self::default_url()).await
    }

    /// Connect to the websocket endpoint and start async loops for incoming and outgoing message handling
    ///
    /// # Arguments
    ///
    /// * `conn_url`: The string representation of the websocket url
    ///
    /// returns: The API result of trying to connect with the given Url
    async fn connect(conn_url: &str) -> Result<Self> {
        //Check if valid url
        let url = Url::parse(conn_url)?;

        // Channel for sending commands to websocket sink handling loop
        let (command_in_tx, _) = tokio::sync::broadcast::channel::<Command>(128); //TODO by config value

        // The shared orderbook subscription map
        let orderbook_subscriptions = Arc::new(RwLock::new(HashMap::new()));

        // A shared flag to indicate that we want to actively disconnect and not auto reconnect
        let disconnect_flag = Arc::new(RwLock::new(false));

        let api = Self::new(
            url.clone(),
            command_in_tx.clone(),
            Arc::clone(&orderbook_subscriptions),
        );

        // We need to wait till the processing loops fully start on initial connect or else the
        // client may subscribe on return and fail as there is nothing processing those commands
        let start_cond = Arc::new((Mutex::new(false), Condvar::new()));
        let start_cond_loop = Arc::clone(&start_cond);

        // Spawn a the main loop that handles auto reconnecting if there was an issue and we didn't
        // actively disconnect the connection
        tokio::spawn(async move {
            loop {
                // Create the websocket connection
                let websocket = match connect_async(&url).await {
                    Ok((websocket, _)) => {
                        log::info!("Connected to {} WebSocket API", Self::exchange_name());
                        websocket
                    }
                    Err(e) => {
                        log::error!("{}", e);
                        sleep(Duration::from_secs(5));
                        log::info!("Retrying connection");
                        continue;
                    }
                };

                // Split the websocket connection into the sink and stream so we can handle
                // bidirectional messages asyncly
                let (websocket_sink, websocket_stream) = websocket.split();

                // Start the processing loop for incoming messages from the ws stream
                let ws_loop_jh = tokio::spawn(Self::websocket_incoming_loop(
                    websocket_stream,
                    command_in_tx.clone(),
                    Arc::clone(&orderbook_subscriptions),
                ));

                // Start the processing loop for the outgoing command message lopp to the ws sink
                let cmd_loop_jh = tokio::spawn(Self::command_outgoing_loop(
                    command_in_tx.subscribe(),
                    websocket_sink,
                    Arc::clone(&orderbook_subscriptions),
                    Arc::clone(&disconnect_flag),
                ));

                // We may be in a subsequent iteration of the main loop because there was a
                // connection issue. Check if we had subscriptions already and if so resubscribe
                match orderbook_subscriptions.read() {
                    Ok(subs) => {
                        for channel in subs.keys() {
                            if let Err(e) = command_in_tx.send(Command::Subscribe(channel.clone()))
                            {
                                log::error!("Unable to send subscribe message for api {} on channel {}: {:?}", Self::exchange_name(),
                                            channel, e)
                            }
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to acquire read lock on orderbook subscriptions: {:?}",
                            e
                        );
                    }
                }

                // We made it this far, then signal we have started so we can return the api
                let (lock, cvar) = &*start_cond_loop;
                match lock.lock() {
                    Ok(mut started) => {
                        *started = true;
                        cvar.notify_one()
                    }
                    Err(e) => {
                        log::error!("{}", e);
                        // If we fail to signal then this won't return so abort the spawned
                        // processing loops and retry
                        cmd_loop_jh.abort();
                        ws_loop_jh.abort();
                        continue;
                    }
                }

                // This will block on either of these completing which will happen if the client
                // asks to disconnect or we had a connection issue
                tokio::select! {
                    _ = ws_loop_jh => {}
                    _ = cmd_loop_jh => {}
                }

                // Processing loops ended, check if client wanted to disconnect, if so break out of
                // loop. If not, continue on and retry connection
                match disconnect_flag.read() {
                    Ok(flag) => {
                        if *flag {
                            log::debug!("Connection disconnected, stopping main connection loop");
                            break;
                        } else {
                            log::info!("Connection disconnected, reconnecting main connection loop in 5 seconds");
                            sleep(Duration::from_secs(5));
                            continue;
                        }
                    }
                    Err(e) => {
                        log::error!("Unable to acquire read lock on disconnect flag in main connection loop: {:?}", e);
                        continue;
                    }
                }
            }
        });

        // Wait till we get the signal that all the async processing loops are started and good
        let (lock, cvar) = &*start_cond;
        let mut started = lock.lock()?;
        while !*started {
            started = cvar.wait(started)?;
        }

        Ok(api)
    }

    /// Subscribe to the exchange's orderbook channel for the given symbol and get a channel
    /// receiver to listen for incoming orderbook updates
    ///
    /// # Arguments
    ///
    /// * `symbol`: The symbol pair to subscribe to. Ex. 'BTCUSD'
    /// * `depth`: The number of levels the subscriber is interested in
    ///
    /// returns: Result for a Receiver the receives Orderbook's
    async fn subscribe_orderbook(
        &mut self,
        symbol: &str,
        depth: usize,
    ) -> Result<Receiver<Orderbook>> {
        self.check_orderbook_subscription_params(symbol, depth)?;

        // The is a websocket channel based on the exchange's format, not a rust channel
        let channel = Self::order_book_channel(symbol, depth);
        // Check if we already have subscribe by another client, if not, then send subscription
        // request and update map
        let mut subs = self.orderbook_subscriptions().write()?;
        return match (*subs).get_mut(&channel) {
            None => {
                if let Err(e) = self
                    .command_in_tx()
                    .send(Command::Subscribe(channel.clone()))
                {
                    return Err(Error::WebSocketSendError(
                        format!(
                            "Unable to subscribe to {} order book stream for {}",
                            Self::exchange_name(),
                            symbol
                        ),
                        Box::from(e),
                    ));
                }

                //TODO drive bounds via config - should have proper amount of backpressure
                let (tx, rx) = tokio::sync::broadcast::channel::<Orderbook>(128);
                (*subs).insert(channel, tx);
                Ok(rx)
            }

            Some(sender) => Ok(sender.subscribe()),
        };
    }

    /// Disconnect from the exchange websocket api
    ///
    /// returns: The result of the disconnect command
    async fn disconnect(&mut self) -> Result<()> {
        match self.command_in_tx().send(Command::Disconnect) {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::DisconnectionError(format!("{}", e))),
        }
    }

    /// Get this instances' Command Sender
    ///
    /// returns: A reference to the instance Command Sender
    fn command_in_tx(&self) -> &Sender<Command>;

    //TODO Other subscriber types - use map from subscription type -> subscription -> subscribers
    /// Get this instances' shared orderbook subscription map
    ///
    /// returns: A reference to this instances copy of the shared orderbook subscriptions map
    fn orderbook_subscriptions(&self) -> &OrderbookSubscriptions;

    /// Create the appropriate subscribe message for the particular exchange api
    ///
    /// # Arguments
    ///
    /// * `channel`: The channel name for the exchange
    ///
    /// returns: The subscribe channel exchange message
    fn create_subscribe_message(channel: &str) -> Result<String>;

    /// Create the appropriate unsubscribe message for the particular exchange api
    ///
    /// # Arguments
    ///
    /// * `channel`: The channel name of the subscription
    ///
    /// returns: The exchange formatted unsubscribe message
    fn create_unsubscribe_message(channel: &str) -> Result<String>;

    /// Send request to unsubscribe from a channel. This is used internally
    ///
    /// # Arguments
    ///
    /// * `command_in_tx`: The Command Sender for this instance
    /// * `channel`: The subscription channel name
    /// * `orderbook_subscriptions`: The map of orderbook subscribers
    ///
    fn unsubscribe_channel(
        command_in_tx: &Sender<Command>,
        channel: &str,
        orderbook_subscriptions: &OrderbookSubscriptions,
    ) {
        // Check if there are no subscriptions
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

        // The command processing loop may have already ended depending on why we are unsubscribing.
        // So check first before sending an unsubscribe request
        if command_in_tx.receiver_count() > 0 {
            if let Err(e) = command_in_tx.send(Command::Unsubscribe(channel.to_string())) {
                log::error!("Unable to send unsubscribe command to channel: {:?}", e)
            }
        }
    }

    /// Processes an incoming exchange message into a common type that can be handled
    ///
    /// # Arguments
    ///
    /// * `msg`: The text websocket message
    /// * `orderbook_subscriptions`: The orderbook subscriptions if it is an orderbook data message
    ///
    /// returns: The DataMessageType of the incoming message
    fn process_message(
        msg: &str,
        orderbook_subscriptions: &OrderbookSubscriptions,
    ) -> Result<DataMessageType>;

    /// Exchange specific handling of orderbook subscribe parameter checking
    ///
    /// # Arguments
    ///
    /// * `symbol`: The symbol pair. Ex 'BTCUSD'
    /// * `depth`: The desired depth
    ///
    /// returns: Whether the parameters were ok or had an issue
    fn check_orderbook_subscription_params(&self, symbol: &str, depth: usize) -> Result<()>;

    /// Creates the exchange specific orderbook channel string to properly (un)subscribe
    ///
    /// # Arguments
    ///
    /// * `symbol`: The symbol pair. Ex 'BTCUSD'
    /// * `depth`: The desired depth
    ///
    /// returns: The exchange formatted string for an orderbook channel
    fn order_book_channel(symbol: &str, depth: usize) -> String;

    /// The exchange identifier
    ///
    /// returns: The exchange name identifier
    fn exchange_name() -> &'static str;

    /// Default implementation of processing an orderbook data message that is in JSON format and
    /// sends it to subscribers
    ///
    /// # Arguments
    ///
    /// * `command_in_tx`: The Command Sender for this api instance
    /// * `orderbook_subscriptions`: The current orderbook subscriptions
    /// * `bids`: JSON array of arrays of two floats as strings
    /// * `asks`: JSON array of arrays of two floats as strings
    /// * `channel`: The channel this update came on
    ///
    fn handle_orderbook_data(
        command_in_tx: &Sender<Command>,
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

                match try_values_to_orderbook(
                    Self::exchange_name(),
                    bids,
                    asks,
                    10, //TODO this can be handled per subscriber and parse to max requested
                ) {
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

    /// The main async loop for handling incoming websocket messages
    ///
    /// # Arguments
    ///
    /// * `websocket_stream`: The incoming websocket stream
    /// * `command_in_tx`: The outgoing Command Sender
    /// * `orderbook_subscriptions`: The shared orderbook subscriptions
    ///
    async fn websocket_incoming_loop(
        mut websocket_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        command_in_tx: Sender<Command>,
        orderbook_subscriptions: OrderbookSubscriptions,
    ) {
        loop {
            // We should get at a minimum of a pong message within 2 minutes, if not something is
            // wrong with the connection
            let message =
                match time::timeout(Duration::from_secs(2 * 60), websocket_stream.next()).await {
                    Ok(opt_msg) => {
                        match opt_msg {
                            None => {
                                // WS connection is closed. Break loop and let reconnect logic determine
                                // if a reconnect attempt should be made
                                log::debug!("No more incoming messages from websocket");
                                drop(websocket_stream);
                                drop(command_in_tx);
                                break;
                            }
                            Some(msg_res) => {
                                match msg_res {
                                    Ok(message) => message,
                                    Err(e) => {
                                        log::error!("Error retrieving next message: {:?}", e);
                                        // Attempt reconnection in case there is an issue as this should
                                        // not happen
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(_elapsed) => {
                        // Haven't received a message in too long, connection maybe dropped on exchange
                        // end, try to reconnect by breaking loop
                        log::debug!("WS incoming stream timed out");
                        break;
                    }
                };

            match message {
                Message::Text(msg) => {
                    // Process the message and handle the type
                    match Self::process_message(&msg, &orderbook_subscriptions) {
                        Ok(msg_type) => match msg_type {
                            DataMessageType::SubscribeSuccessful(channel) => {
                                log::info!("Subscribed to {}", channel)
                            }
                            DataMessageType::UnsubscribeSuccessful(channel) => match channel {
                                None => log::info!("Unsubscribed"),
                                Some(channel) => {
                                    log::info!("Unsubscribed to {}", channel)
                                }
                            },
                            DataMessageType::OrderBookData(channel, bids, asks) => {
                                Self::handle_orderbook_data(
                                    &command_in_tx,
                                    &orderbook_subscriptions,
                                    &bids,
                                    &asks,
                                    &channel,
                                )
                            }
                            DataMessageType::Reconnect => {
                                //Simply break loop without flagging disconnect and it will reconnect
                                break;
                            }
                        },
                        Err(e) => {
                            log::error!("Problem occurred processing message: {:?}", e)
                        }
                    }
                }
                Message::Ping(data) => {
                    log::debug!("Received Ping message, sending Pong");
                    if let Err(e) = command_in_tx.send(Command::Send(Message::Pong(data))) {
                        log::error!("Failed to send pong message: {:?}", e);
                    }
                }
                Message::Pong(_) => {
                    log::debug!("Received a pong message");
                }
                Message::Close(_) => {
                    log::debug!("Received close message");
                    // Simply break loop without flagging disconnect and it will reconnect
                    // if we did not ask for the disconnection
                    break;
                }
                _ => {
                    log::error!("Unhandled message type: {:?}", message);
                }
            }
        }

        log::debug!("websocket_incoming_loop ending");
    }

    /// The main async processing loop for outgoing messages to the websocket sink
    ///
    /// # Arguments
    ///
    /// * `command_in_rx`: The Command Sender for this API
    /// * `websocket_sink`: The websocket sink to send messages on
    /// * `orderbook_subscriptions`: The shared orderbook subscriptions
    /// * `disconnect_flag`: The shared flag to toggle if we get a disconnect command
    ///
    async fn command_outgoing_loop(
        mut command_in_rx: Receiver<Command>,
        mut websocket_sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        orderbook_subscriptions: OrderbookSubscriptions,
        disconnect_flag: Arc<RwLock<bool>>,
    ) {
        loop {
            // Minimally send a ping every 60 seconds if we have not received any other commands to
            // keep connection open and check if there are any issues with it
            let command = match time::timeout(Duration::from_secs(60), command_in_rx.recv()).await {
                Ok(opt_command) => match opt_command {
                    Ok(command) => command,
                    Err(_) => {
                        log::debug!("No more incoming command messages for websocket");
                        drop(command_in_rx);
                        drop(websocket_sink);
                        break;
                    }
                },
                Err(_elapsed) => {
                    // Timeout, send Ping
                    Command::Ping
                }
            };

            // Process the next Command
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
                    // Check if we unsubscribed already, if not, remove subscription and send
                    // unsubscribe message
                    match orderbook_subscriptions.write() {
                        Ok(mut subs) => match (*subs).remove(&channel) {
                            None => continue, //Already unsubscribed
                            Some(sender) => {
                                // Drop sender so receiver knows nothing else is coming
                                drop(sender);
                            }
                        },
                        Err(e) => {
                            log::error!("{:?}", Error::from(e));
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
                    // Update disconnect flag so downstream processes know we intend to close
                    // connection
                    match disconnect_flag.write() {
                        Ok(mut flag) => *flag = true,
                        Err(e) => {
                            log::error!("Unable to acquire write lock on disconnect flag: {:?}", e)
                        }
                    }

                    // Remove all subscriptions
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
                            drop(command_in_rx);
                            break;
                        }
                        Err(e) => {
                            log::error!("Unable to send close message: {:?}", e)
                        }
                    }

                    // Close outgoing sink which should end connection
                    let _ = websocket_sink.close().await;
                    break;
                }
                Command::Send(msg) => {
                    if let Err(e) = websocket_sink.send(msg).await {
                        log::warn!("Unable to send message: {:?}", e)
                    }
                }
                Command::Ping => {
                    if let Err(e) = websocket_sink.send(Message::Ping(vec![])).await {
                        log::warn!("Unable to send ping message: {:?}", e)
                    }
                }
            }
        }

        log::debug!("command_incoming_loop ending");
    }
}
