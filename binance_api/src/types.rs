use serde::{Deserialize, Serialize};

/// Struct representing an outgoing message to Binance
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BinanceMessage {
    pub method: &'static str,
    pub params: Vec<String>,
    pub id: u64,
}

impl BinanceMessage {
    /// Create a Binance subscribe message
    ///
    /// # Arguments
    ///
    /// * `channel`: The channel to subscribe to
    ///
    /// returns: Binancee sSubscribe message like
    ///     '{"method": "SUBSCRIBE","params":["btcusdt@depth10"],"id": 1}'
    pub(crate) fn subscribe(channel: &str) -> Self {
        Self {
            method: "SUBSCRIBE",
            params: vec![channel.to_string()],
            id: 1,
        }
    }

    /// Create a Binance unsubscribe message
    ///
    /// # Arguments
    ///
    /// * `channel`: The channel to unsubscribe from
    ///
    /// returns: Binance unsubscribe message like:
    ///     '{"method": "UNSUBSCRIBE", "params": ["btcusdt@depth10" ], "id": 1 }'
    pub(crate) fn unsubscribe(channel: &str) -> Self {
        Self {
            method: "UNSUBSCRIBE",
            params: vec![channel.to_string()],
            id: 1,
        }
    }
}
