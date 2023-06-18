use std::sync::Arc;

use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;
use tokio::sync::watch::Sender;
use tokio_stream::wrappers::WatchStream;
use tonic::{Request, Response, Status};

use crate::orderbook::orderbook_aggregator_server::OrderbookAggregator;
use crate::orderbook::{Empty, Summary};

/// The gRPC orderbook aggregator service
#[derive(Debug)]
pub struct OrderbookAggregatorService {
    /// The sender used to update clients that have subscribed to Summary updates
    summary_tx: Arc<Sender<Result<Summary, Status>>>,
}

impl OrderbookAggregatorService {
    /// Creates a new OrderbookAggregatorService instance with the receiving end of a channel
    /// that receives new Summary updates
    ///
    /// # Arguments
    ///
    /// * `rx`: A channel receiver for Summary updates
    ///
    /// returns: A new OrderbookAggregatorService
    pub(crate) fn new(rx: Receiver<Summary>) -> Self {
        let (tx, _) = watch::channel::<Result<Summary, Status>>(Ok(Summary {
            // Watch channel does not hold history and needs an initial value. A NaN spread tells
            // the service processing thread that this is not a valid value if for some reason a
            // client subscribes before we ever get an orderbook update from an exchange.
            // Unlikely but possible
            spread: f64::NAN,
            bids: Default::default(),
            asks: Default::default(),
        }));

        // Both the async update processing thread (to send updates) and the service itself
        // (to create new receivers) need access to the Summary sender so make it shareable.
        let tx = Arc::new(tx);

        // Start the main update processing loop
        tokio::spawn(Self::exchange_orderbook_update_loop(rx, Arc::clone(&tx)));

        Self { summary_tx: tx }
    }

    /// Async processing function that gets Summary updates and forwards them on to every gRPC
    /// client subscriber
    ///
    /// # Arguments
    ///
    /// * `summary_rx`: Incoming Summary channel receiver for updates
    /// * `summary_tx`: Outgoing Summary sender for gRPC clients
    ///
    async fn exchange_orderbook_update_loop(
        mut summary_rx: Receiver<Summary>,
        summary_tx: Arc<Sender<Result<Summary, Status>>>,
    ) {
        loop {
            // Get the next Summary update
            let res = match summary_rx.recv().await {
                None => Err(Status::cancelled("Service was cancelled")),
                Some(summary) => {
                    if summary.spread.is_nan() {
                        continue; //Either bad data or initial value in watch channel
                    }

                    Ok(summary)
                }
            };

            // Only send if there are active clients listening
            if summary_tx.receiver_count() > 0 {
                if let Err(e) = summary_tx.send(res) {
                    log::error!("Unexpected error trying to send out orderbook summary to rpc stream listeners: {:?}", e);
                }
            } else {
                //TODO handle dynamic subscriptions to exchanges based on service client subscriptions
                log::debug!("No listeners for orderbook summary update")
            }
        }
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for OrderbookAggregatorService {
    /// The stream type for outgoing Summary updates
    type BookSummaryStream = WatchStream<Result<Summary, Status>>;

    /// Implementation of the orderbook aggregator proto server streaming gRPC service
    ///
    /// returns: The stream of Summary updates for the requesting client
    async fn book_summary(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        let rx = self.summary_tx.subscribe();
        Ok(Response::new(WatchStream::from_changes(rx)))
    }
}
