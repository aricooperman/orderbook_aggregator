use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BitstampMessage {
    pub(crate) event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) channel: Option<String>,
    pub(crate) data: Value,
}

impl BitstampMessage {
    //{"event": "bts:subscribe", "data": {"channel": "order_book_ethbtc"}}
    pub(crate) fn subscribe(channel: &str) -> Self {
        Self {
            event: String::from("bts:subscribe"),
            channel: None,
            data: json!({ "channel": channel }),
        }
    }

    pub(crate) fn unsubscribe(channel: &str) -> Self {
        Self {
            event: String::from("bts:unsubscribe"),
            channel: None,
            data: json!({ "channel": channel }),
        }
    }
}
