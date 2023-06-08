use std::error::Error;
use std::sync::PoisonError;
use url::ParseError;

pub type Result<T> = std::result::Result<T, KeyrockError>;

#[derive(thiserror::Error, Debug)]
pub enum KeyrockError {
    #[error("Unable to make a connection: {0}")]
    ConnectionError(#[source] Box<dyn Error>),

    #[error("Unable to make a connection: {0}")]
    DisconnectionError(#[source] Box<dyn Error>),

    #[error("I/O error occurred: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Malformed Url: {0}")]
    BadUrl(#[from] ParseError),

    #[error("Problem sending websocket message {0}: {1}")]
    WebSocketSendError(String, Box<dyn Error + Send + Sync>),

    #[error("Problem receiving websocket message {0}")]
    WebSocketReceiveError(String),

    #[error("Issue serializing/deserializing: {0}")]
    SerDeError(#[from] serde_json::Error),

    #[error("Issue handling subscriptions: {0}")]
    SubscriptionError(String),

    #[error("Bad data received: {0}")]
    BadData(String),

    #[error("Unable to lock shared struct: {0}")]
    RwLockingError(String),

    #[error("Provided an illegal argument: {0}")]
    BadArgument(String),
}

impl<T> From<PoisonError<T>> for KeyrockError {
    fn from(value: PoisonError<T>) -> Self {
        Self::RwLockingError(format!("Unable to acquire RW lock: {:?}", value))
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//
// }
