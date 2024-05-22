use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The struct version of a Bitstamp incoming JSON message
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BitstampMessage {
    pub(crate) event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) channel: Option<String>,
    pub(crate) data: Value,
}

impl BitstampMessage {
    /// Create the JSON representation of a Bitstamp subscribe message
    ///
    /// # Arguments
    ///
    /// * `channel`: The channel name to subscribe to
    ///
    /// returns: A suscribe BitstampMessage
    pub(crate) fn subscribe(channel: &str) -> Self {
        Self {
            event: String::from("bts:subscribe"),
            channel: None,
            data: json!({ "channel": channel }),
        }
    }

    /// Create the JSON representation of a Bitstamp unsubscribe message
    ///
    /// # Arguments
    ///
    /// * `channel`: The channel to unsubscribe from
    ///
    /// returns: An unsubscribe BitstampMessage
    pub(crate) fn unsubscribe(channel: &str) -> Self {
        Self {
            event: String::from("bts:unsubscribe"),
            channel: None,
            data: json!({ "channel": channel }),
        }
    }
}
