use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use tokio::sync::broadcast::Sender;
use url::Url;

use orderbook_aggregator_common::errors::{Error, Result};
use orderbook_aggregator_common::ws::{
    Command, DataMessageType, ExchangeWebSocketApi, OrderbookSubscriptions,
};

use crate::types::BitstampMessage;

mod types;

/// The default websocket api url
pub const DEFAULT_BITSTAMP_WS_URL: &str = "wss://ws.bitstamp.net/";
/// The exchange indetifier
pub const BITSTAMP_EXCHANGE_NAME: &str = "bitstamp";

/// A Bitstamp instance of ExchangeWebSocketAPI
///
/// Documentation at <https://www.bitstamp.net/websocket/v2/>
///
/// Note: Maximum connection age is 90 days from the time the connection is established.
pub struct BitstampWebSocketApi {
    connection_url: Url,
    command_in_tx: Sender<Command>,
    orderbook_subscriptions: OrderbookSubscriptions,
}

impl Display for BitstampWebSocketApi {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bitstamp WS API @ {}", self.connection_url)
    }
}

#[async_trait]
impl ExchangeWebSocketApi for BitstampWebSocketApi {
    fn new(
        url: Url,
        command_in_tx: Sender<Command>,
        orderbook_subscriptions: OrderbookSubscriptions,
    ) -> Self {
        Self {
            connection_url: url,
            command_in_tx,
            orderbook_subscriptions,
        }
    }

    #[inline]
    fn default_url() -> &'static str {
        DEFAULT_BITSTAMP_WS_URL
    }

    #[inline]
    fn command_in_tx(&self) -> &Sender<Command> {
        &self.command_in_tx
    }

    #[inline]
    fn orderbook_subscriptions(&self) -> &OrderbookSubscriptions {
        &self.orderbook_subscriptions
    }

    fn create_subscribe_message(channel: &str) -> Result<String> {
        let msg = BitstampMessage::subscribe(channel);
        match serde_json::to_string(&msg) {
            Ok(s) => Ok(s),
            Err(e) => Err(Error::SerDeError(e)),
        }
    }

    fn create_unsubscribe_message(channel: &str) -> Result<String> {
        let msg = BitstampMessage::unsubscribe(channel);
        match serde_json::to_string(&msg) {
            Ok(s) => Ok(s),
            Err(e) => Err(Error::SerDeError(e)),
        }
    }

    fn process_message(
        msg: &str,
        _orderbook_subscriptions: &OrderbookSubscriptions,
    ) -> Result<DataMessageType> {
        match serde_json::from_str::<BitstampMessage>(msg) {
            Ok(mut bs_msg) => {
                let channel = bs_msg
                    .channel
                    .unwrap_or_else(|| "<unknown channel>".to_string());
                match bs_msg.event.as_str() {
                    "bts:subscription_succeeded" => {
                        Ok(DataMessageType::SubscribeSuccessful(channel))
                    }
                    "bts:unsubscription_succeeded" => {
                        Ok(DataMessageType::UnsubscribeSuccessful(Some(channel)))
                    }
                    "bts:request_reconnect" => Ok(DataMessageType::Reconnect),
                    "data" => {
                        // Check if the incoming data is orderbook type
                        if channel.starts_with("order_book_") {
                            Ok(DataMessageType::OrderBookData(
                                channel,
                                bs_msg.data["bids"].take(),
                                bs_msg.data["asks"].take(),
                            ))
                        } else {
                            Err(Error::WebSocketReceiveError(format!(
                                "Unhandled channel type {}",
                                channel
                            )))
                        }
                    }
                    event => Err(Error::WebSocketReceiveError(format!(
                        "Unhandled message type: {}",
                        event
                    ))),
                }
            }
            Err(e) => Err(Error::WebSocketReceiveError(format!(
                "Unable to parse message {} as a bitstamp message: {:?}",
                msg, e
            ))),
        }
    }

    fn check_orderbook_subscription_params(&self, _symbol: &str, depth: usize) -> Result<()> {
        if depth > 0 && depth <= 100 {
            Ok(())
        } else {
            Err(Error::BadArgument(format!(
                "Bad depth parameter for Bitstamp {}, supports depth levels between 1 & 100",
                depth
            )))
        }
    }

    #[inline]
    fn order_book_channel(symbol: &str, _depth: usize) -> String {
        format!("order_book_{}", symbol.to_lowercase())
    }

    #[inline]
    fn exchange_name() -> &'static str {
        BITSTAMP_EXCHANGE_NAME
    }
}

#[cfg(test)]
mod tests {
    use tokio_tungstenite::tungstenite::Message;

    use orderbook_aggregator_common::types::Orderbook;

    use crate::ExchangeWebSocketApi;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn get_order_book_updates() {
        let mut bitstamp_api = BitstampWebSocketApi::connect_default().await.unwrap();
        let count = 10;
        let mut rx = bitstamp_api
            .subscribe_orderbook("BTCUSDT", 10)
            .await
            .expect("Successful subscription");
        let mut msgs: Vec<Orderbook> = vec![];

        for i in 0..count {
            let obm = rx.recv().await.unwrap();
            log::info!("{}: {:?}", i, obm);
            msgs.push(obm);
        }

        bitstamp_api.disconnect().await.unwrap();

        assert_eq!(msgs.len(), count);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_reconnect() {
        let mut bitstamp_api = BitstampWebSocketApi::connect_default().await.unwrap();
        let count = 10;
        let mut rx = bitstamp_api
            .subscribe_orderbook("BTCUSDT", 10)
            .await
            .expect("Successful subscription");

        let mut msgs: Vec<Orderbook> = vec![];
        for i in 0..count {
            match rx.recv().await {
                Ok(obm) => {
                    log::info!("{}: {:?}", i, obm);
                    msgs.push(obm);

                    if i == 2 {
                        // Force close to check reconnection logic
                        bitstamp_api
                            .command_in_tx
                            .send(Command::Send(Message::Close(None)))
                            .expect("DO: panic message");
                    }
                }
                Err(e) => {
                    log::error!("Got channel receive error: {:?}", e);
                    break;
                }
            }
        }

        bitstamp_api.disconnect().await.unwrap();

        assert_eq!(msgs.len(), count);
    }
}
