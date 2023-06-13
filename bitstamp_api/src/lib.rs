mod types;

use crate::types::BitstampMessage;
use async_trait::async_trait;
use futures_channel::mpsc::UnboundedSender;
use orderbook_aggregator_common::errors::{KeyrockError, Result};
use orderbook_aggregator_common::ws::{
    Command, DataMessageType, ExchangeWebSocketApi, OrderbookSubscriptions,
};
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};
use url::Url;

/// <https://www.bitstamp.net/websocket/v2/>
/// Maximum connection age is 90 days from the time the connection is established.

pub const DEFAULT_BITSTAMP_WS_URL: &str = "wss://ws.bitstamp.net/";
pub const BITSTAMP_EXCHANGE_NAME: &str = "bitstamp";

pub struct BitstampWebSocketApi {
    connection_url: Url,
    command_in_tx: UnboundedSender<Command>,
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
        command_in_tx: UnboundedSender<Command>,
        orderbook_subscriptions: OrderbookSubscriptions,
        _is_disconnected: Arc<RwLock<bool>>,
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
    fn command_in_tx(&self) -> &UnboundedSender<Command> {
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
            Err(e) => Err(KeyrockError::SerDeError(e)),
        }
    }

    fn create_unsubscribe_message(channel: &str) -> Result<String> {
        let msg = BitstampMessage::unsubscribe(channel);
        match serde_json::to_string(&msg) {
            Ok(s) => Ok(s),
            Err(e) => Err(KeyrockError::SerDeError(e)),
        }
    }

    async fn process_message(
        msg: &str,
        _orderbook_subscriptions: &OrderbookSubscriptions,
    ) -> Result<DataMessageType> {
        match serde_json::from_str::<BitstampMessage>(msg) {
            Ok(mut bs_msg) => {
                let channel = bs_msg
                    .channel
                    .unwrap_or_else(|| "<unknown channel>".to_string());
                match bs_msg.event.as_str() {
                    // {"event":"bts:subscription_succeeded","channel":"order_book_ethbtc","data":{}}
                    "bts:subscription_succeeded" => {
                        Ok(DataMessageType::SubscribeSuccessful(channel))
                    }
                    "bts:unsubscription_succeeded" => {
                        Ok(DataMessageType::UnsubscribeSuccessful(Some(channel)))
                    }
                    "bts:request_reconnect" => {
                        todo!()
                    }
                    "data" => {
                        if channel.starts_with("order_book_") {
                            Ok(DataMessageType::OrderBookData(
                                channel,
                                bs_msg.data["bids"].take(),
                                bs_msg.data["asks"].take(),
                            ))
                        } else {
                            Err(KeyrockError::WebSocketReceiveError(format!(
                                "Unhandled channel type {}",
                                channel
                            )))
                        }
                    }
                    event => Err(KeyrockError::WebSocketReceiveError(format!(
                        "Unhandled message type: {}",
                        event
                    ))),
                }
            }
            Err(e) => Err(KeyrockError::WebSocketReceiveError(format!(
                "Unable to parse message {} as a bitstamp message: {:?}",
                msg, e
            ))),
        }
    }

    fn check_orderbook_subscription_params(&self, _symbol: &str, depth: usize) -> Result<()> {
        if depth > 0 && depth <= 100 {
            Ok(())
        } else {
            Err(KeyrockError::BadArgument(format!(
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
    use crate::ExchangeWebSocketApi;
    use env_logger::Builder;
    use orderbook_aggregator_common::types::OrderBook;
    use std::time::Duration;
    use tokio::time::sleep;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn get_order_book_updates() {
        Builder::from_default_env()
            .filter(None, log::LevelFilter::Debug)
            .init();

        let mut bitstamp_api = BitstampWebSocketApi::connect_default().await.unwrap();
        let mut rx = bitstamp_api
            .subscribe_orderbook("BTCUSDT", 10)
            .await
            .unwrap();

        let mut msgs: Vec<OrderBook> = vec![];
        for i in 0..10 {
            match rx.recv().await {
                Ok(obm) => {
                    log::info!("{}: {:?}", i, obm);
                    msgs.push(obm);
                }
                Err(e) => {
                    log::error!("Got channel receive error: {:?}", e);
                    break;
                }
            }
        }

        assert_eq!(msgs.len(), 10);

        drop(rx);

        //Check for unsubscribe
        sleep(Duration::from_secs(50)).await;

        bitstamp_api.disconnect().await.unwrap();

        //Check for close handling
        sleep(Duration::from_secs(5)).await;

        log::info!("Done");
    }
}
