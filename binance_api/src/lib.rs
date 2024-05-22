use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast::Sender;
use url::Url;

use orderbook_aggregator_common::errors::{Error, Result};
use orderbook_aggregator_common::ws::{
    Command, DataMessageType, ExchangeWebSocketApi, OrderbookSubscriptions,
};

use crate::types::BinanceMessage;

mod types;

/// The default Binance websocket api url
pub const DEFAULT_BINANCE_WS_URL: &str = "wss://stream.binance.us:9443/ws";
/// The exchange identifier for Binance
pub const BINANCE_EXCHANGE_NAME: &str = "binance";

/// Binance implementation of ExchangeWebSocketApi
///
/// Documentation at <https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md>
///
/// Note:
///     A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark
///     This currently only handles a single subscription per instance because the incoming orderbook messages
///         do not properly indicate the channel it corresponds to
pub struct BinanceWebSocketApi {
    connection_url: Url,
    command_in_tx: Sender<Command>,
    orderbook_subscriptions: OrderbookSubscriptions,
}

impl BinanceWebSocketApi {
    /// Helper method to get the singleton subscription for Binance from the subscription map
    pub(crate) fn get_primary_orderbook_subscription(
        orderbook_subscriptions: &OrderbookSubscriptions,
    ) -> Option<String> {
        match orderbook_subscriptions.read() {
            Ok(subs) => subs.keys().next().map(|s| s.to_owned()),
            Err(e) => {
                log::error!(
                    "Unable to lock orderbook subscriptions for reading: {:?}",
                    e
                );
                None
            }
        }
    }
}

impl Display for BinanceWebSocketApi {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Binance WS API @ {}", self.connection_url)
    }
}

#[async_trait]
impl ExchangeWebSocketApi for BinanceWebSocketApi {
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
        DEFAULT_BINANCE_WS_URL
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
        let msg = BinanceMessage::subscribe(channel);
        match serde_json::to_string(&msg) {
            Ok(s) => Ok(s),
            Err(e) => Err(Error::SerDeError(e)),
        }
    }

    fn create_unsubscribe_message(channel: &str) -> Result<String> {
        let msg = BinanceMessage::unsubscribe(channel);
        match serde_json::to_string(&msg) {
            Ok(s) => Ok(s),
            Err(e) => Err(Error::SerDeError(e)),
        }
    }

    fn process_message(
        msg: &str,
        orderbook_subscriptions: &OrderbookSubscriptions,
    ) -> Result<DataMessageType> {
        match serde_json::from_str::<Value>(msg) {
            Ok(value) => {
                match value.as_object() {
                    None => Err(Error::WebSocketReceiveError(format!(
                        "Message received is not a JSON object: {}",
                        value
                    ))),
                    Some(map) => {
                        // This key is what is returned for subscribe and unsubscribe messages and
                        // only an id  with it
                        if map.contains_key("result") {
                            let id = &value["id"];
                            if id.is_null() || !id.is_u64() {
                                return Err(Error::WebSocketReceiveError(format!(
                                    "Message received is result, but no id: {}",
                                    value
                                )));
                            }

                            let id = id.as_u64().unwrap();
                            if id != 1 {
                                // Currently supports one subscription per instance for Binance API
                                log::warn!(
                                    "Unexpected id ({}) in subscription message {}",
                                    id,
                                    value
                                );
                            }

                            // If we have a channel in the subscriber map then this message indicates
                            // successful subscribe, if not, it indicates successful unsubscribe
                            match Self::get_primary_orderbook_subscription(orderbook_subscriptions)
                            {
                                None => Ok(DataMessageType::UnsubscribeSuccessful(None)),
                                Some(channel) => Ok(DataMessageType::SubscribeSuccessful(channel)),
                            }
                        } else if map.contains_key("lastUpdateId")
                            && map.contains_key("bids")
                            && map.contains_key("asks")
                        {
                            // These keys indicate it is an orderbook message

                            //TODO Need a clean way to differentiate incoming messages from Binance that don't specify the channel or subscriber
                            // For now assuming only handling a single subscription
                            let channel = match Self::get_primary_orderbook_subscription(
                                orderbook_subscriptions,
                            ) {
                                None => "Unknown orderbook".to_string(),
                                Some(channel) => channel,
                            };

                            Ok(DataMessageType::OrderBookData(
                                channel,
                                map.get("bids").unwrap().clone(),
                                map.get("asks").unwrap().clone(),
                            ))
                        } else {
                            return Err(Error::WebSocketReceiveError(format!(
                                "Unhandled Binance message {}",
                                value
                            )));
                        }
                    }
                }
            }
            Err(e) => Err(Error::WebSocketReceiveError(format!(
                "Unable to parse message {} as a bitstamp message: {:?}",
                msg, e
            ))),
        }
    }

    fn check_orderbook_subscription_params(&self, _symbol: &str, depth: usize) -> Result<()> {
        if depth != 5 && depth != 10 && depth != 20 {
            return Err(Error::BadArgument(format!(
                "Depth for Binance orderbook snapshot must be 5, 10 or 20. {} given",
                depth
            )));
        }

        if let Some(sub) = Self::get_primary_orderbook_subscription(&self.orderbook_subscriptions) {
            Err(Error::SubscriptionError(format!("Binance API v2 only supports one orderbook subscription simultaneously and is already subscribed to {}", sub)))
        } else {
            Ok(())
        }
    }

    #[inline]
    fn order_book_channel(symbol: &str, depth: usize) -> String {
        format!("{}@depth{}@100ms", symbol.to_lowercase(), depth)
    }

    #[inline]
    fn exchange_name() -> &'static str {
        BINANCE_EXCHANGE_NAME
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
        let mut api = BinanceWebSocketApi::connect_default().await.unwrap();
        let mut rx = api.subscribe_orderbook("BTCUSDT", 10).await.unwrap();
        let count = 10;
        let mut msgs: Vec<Orderbook> = vec![];

        for i in 0..count {
            let obm = rx.recv().await.unwrap();
            log::info!("{}: {:?}", i, obm);
            msgs.push(obm);
        }

        api.disconnect().await.unwrap();

        assert_eq!(msgs.len(), 10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_reconnect() {
        let mut binance_api = BinanceWebSocketApi::connect_default().await.unwrap();
        let count = 10;
        let mut rx = binance_api
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
                        binance_api
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

        binance_api.disconnect().await.unwrap();

        assert_eq!(msgs.len(), count);
    }
}
