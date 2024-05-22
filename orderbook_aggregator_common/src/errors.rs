use std::sync::PoisonError;

use url::ParseError;

/// Type alias for typical results within the orderbook aggregator crates
pub type Result<T> = std::result::Result<T, Error>;

/// The various types of errors that may occur
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Unable to make a connection: {0}")]
    ConnectionError(String),

    #[error("Unable to make a connection: {0}")]
    DisconnectionError(String),

    #[error("I/O error occurred: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Malformed Url: {0}")]
    BadUrl(#[from] ParseError),

    #[error("Problem sending websocket message {0}: {1}")]
    WebSocketSendError(String, Box<dyn std::error::Error + Send + Sync>),

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

impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Self::RwLockingError(format!("Unable to acquire RW lock: {:?}", value))
    }
}

unsafe impl Send for Error {}
