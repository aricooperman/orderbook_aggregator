use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;
use tokio::sync::watch::Sender;
use tokio_stream::wrappers::WatchStream;
use tonic::{Request, Response, Status};

use crate::orderbook::orderbook_aggregator_server::OrderbookAggregator;
use crate::orderbook::{Empty, Summary};

#[derive(Debug)]
pub struct OrderbookAggregatorService {
    tx: Arc<Sender<Result<Summary, Status>>>,
}

impl OrderbookAggregatorService {
    pub(crate) fn new(rx: Receiver<Summary>) -> Self {
        let (tx, _) = watch::channel::<Result<Summary, Status>>(Ok(Summary {
            spread: f64::NAN,
            bids: Default::default(),
            asks: Default::default(),
        }));

        let tx = Arc::new(tx);
        let tx_loop = Arc::clone(&tx);

        tokio::spawn(Self::exchange_orderbook_update_loop(rx, tx_loop));

        Self { tx }
    }

    async fn exchange_orderbook_update_loop(
        mut rx: Receiver<Summary>,
        tx_loop: Arc<Sender<Result<Summary, Status>>>,
    ) {
        loop {
            let res = match rx.recv().await {
                None => Err(Status::cancelled("Service was cancelled")),
                Some(summary) => {
                    if summary.spread.is_nan() {
                        continue; //Either bad data or initial value in watch channel
                    }

                    Ok(summary)
                }
            };

            if res.is_err() || tx_loop.receiver_count() > 0 {
                if let Err(e) = tx_loop.send(res) {
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
    type BookSummaryStream = WatchStream<Result<Summary, Status>>;
    async fn book_summary(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        let rx = self.tx.subscribe();
        Ok(Response::new(WatchStream::from_changes(rx)))
    }
}
