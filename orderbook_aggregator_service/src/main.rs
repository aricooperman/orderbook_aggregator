use std::cell::Cell;
use std::collections::BTreeSet;

use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::transport::Server;

use binance_api::{BinanceWebSocketApi, BINANCE_EXCHANGE_NAME};
use bitstamp_api::{BitstampWebSocketApi, BITSTAMP_EXCHANGE_NAME};
use orderbook_aggregator_common::types::Orderbook;
use orderbook_aggregator_common::ws::ExchangeWebSocketApi;
use orderbook_aggregator_common::{errors::Result, types::OrderbookLevel};
use utils::check_half_book_changed_and_update;

use crate::orderbook::orderbook_aggregator_server::OrderbookAggregatorServer;
use crate::orderbook::{Level, Summary};
use crate::service::OrderbookAggregatorService;

mod service;
mod utils;

pub mod orderbook {
    tonic::include_proto!("orderbook");
}

const NUMBER_OF_EXCHANGES: usize = 2;

// The command line options the service takes
#[derive(Parser, Debug)]
#[clap(version = "0.0.1", about = "Orderbook Aggregator Service")]
struct Opts {
    #[arg(short, long, default_value = "BTCUSDT")]
    symbol: String,
    #[arg(short, long, default_value = "127.0.0.1:9876")]
    listen_at: String,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbosity: u8,
    #[arg(short, long, default_value = "10")]
    depth: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();

    // Change logging verbosity
    let log_level = match opts.verbosity {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    Builder::from_default_env().filter(None, log_level).init();

    log::info!("Starting order book aggregator service for {}", opts.symbol);

    // Connect to Bitstamp API & subscribe for the orderbook
    let mut bitstamp_api = BitstampWebSocketApi::connect_default().await.unwrap();
    let bitstamp_feed: tokio::sync::broadcast::Receiver<Orderbook> = bitstamp_api
        .subscribe_orderbook(&opts.symbol, opts.depth)
        .await
        .unwrap();

    // Connect to Binance API & subscribe for the orderbook
    let mut binance_api = BinanceWebSocketApi::connect_default().await.unwrap();
    let binance_feed: tokio::sync::broadcast::Receiver<Orderbook> = binance_api
        .subscribe_orderbook(&opts.symbol, opts.depth)
        .await
        .unwrap();

    // This is the channel for sending new orderbook 'Summary' updates to the gRPC server
    let (summary_tx, summary_rx) = mpsc::channel::<Summary>(128);
    // Create the gRPC service and register the service
    let orderbook_service = OrderbookAggregatorService::new(summary_rx);
    let svc = OrderbookAggregatorServer::new(orderbook_service);

    // This is the channel that gets updates from the exchanges when a new changed orderbook comes in
    let (exch_ob_tx, exch_ob_rx) = mpsc::channel::<Orderbook>(128);

    // Start processing loop for converting new orderbook updates from the exchanges into an updated Summary to be sent to clients
    tokio::spawn(outgoing_summary_loop(opts.depth, summary_tx, exch_ob_rx));

    // Start async loop to process bitstamp updates
    tokio::spawn(incoming_exchange_feed_loop(
        opts.depth,
        bitstamp_feed,
        exch_ob_tx.clone(),
        BITSTAMP_EXCHANGE_NAME,
    ));

    // Start async loop to process binance updates
    tokio::spawn(incoming_exchange_feed_loop(
        opts.depth,
        binance_feed,
        exch_ob_tx,
        BINANCE_EXCHANGE_NAME,
    ));

    // Start the gRPC server
    let addr = opts.listen_at.parse().unwrap();
    Server::builder()
        .add_service(svc)
        .serve(addr)
        .await
        .unwrap();

    // Server stopped, disconnect from the respective APIs
    let _ = bitstamp_api.disconnect().await;
    let _ = binance_api.disconnect().await;

    Ok(())
}

/// Processor for orderbook updates from each exchange and updates a new Summary
/// to send to clients which merges orderbooks up to desired depth
///
/// # Arguments
///
/// * `depth`: Desired aggregate depth of the Summary orderbook
/// * `summary_out_tx`: The channel sender for the Summary updates
/// * `exch_ob_in_rx`: The channel receiver for exchange api orderbook updates
///
async fn outgoing_summary_loop(
    depth: usize,
    summary_out_tx: Sender<Summary>,
    mut exch_ob_in_rx: Receiver<Orderbook>,
) {
    // Keep a reference of each exchanges half book
    let mut exchange_bids: [Vec<OrderbookLevel>; NUMBER_OF_EXCHANGES] = Default::default();
    let mut exchange_asks: [Vec<OrderbookLevel>; NUMBER_OF_EXCHANGES] = Default::default();

    //The exchange indexes in the above arrays
    const BITSTAMP_IDX: usize = 0;
    const BINANCE_IDX: usize = 1;

    /// function to get the minimum bid price needed to add to aggregate book
    fn bid_cutoff_fn(depth: usize, orderbook: &BTreeSet<OrderbookLevel>) -> Decimal {
        if orderbook.len() >= depth {
            orderbook
                .last()
                .map(|obl| obl.price)
                .unwrap_or(Decimal::MIN)
        } else {
            Decimal::MIN
        }
    }

    /// function for determining if the bid price level is not enough to add to aggregate book
    fn bid_should_skip_level_fn(price: &Decimal, cutoff: &Decimal) -> bool {
        price < cutoff
    }

    /// function to get the maximum ask price needed to add to aggregate book
    fn ask_cutoff_fn(depth: usize, orderbook: &BTreeSet<OrderbookLevel>) -> Decimal {
        if orderbook.len() >= depth {
            orderbook
                .last()
                .map(|obl| obl.price)
                .unwrap_or(Decimal::MAX)
        } else {
            Decimal::MAX
        }
    }

    /// function for determining if the ask price level is not enough to add to aggregate book
    fn ask_should_skip_level_fn(price: &Decimal, cutoff: &Decimal) -> bool {
        price > cutoff
    }

    loop {
        match exch_ob_in_rx.recv().await {
            None => {
                log::debug!("No more exchange orderbook senders, stopping service");
                break;
            }
            Some(orderbook) => {
                match orderbook.exchange {
                    BITSTAMP_EXCHANGE_NAME => {
                        exchange_bids[BITSTAMP_IDX] = orderbook.bids;
                        exchange_asks[BITSTAMP_IDX] = orderbook.asks;
                    }
                    BINANCE_EXCHANGE_NAME => {
                        exchange_bids[BINANCE_IDX] = orderbook.bids;
                        exchange_asks[BINANCE_IDX] = orderbook.asks;
                    }
                    &_ => {
                        log::error!("Unknown exchange {}", orderbook.exchange);
                        continue;
                    }
                }

                // Load sorted half books
                let bids: Vec<Level> = aggregate_half_orderbook(
                    depth,
                    &exchange_bids,
                    bid_cutoff_fn,
                    bid_should_skip_level_fn,
                );

                let asks: Vec<Level> = aggregate_half_orderbook(
                    depth,
                    &exchange_asks,
                    ask_cutoff_fn,
                    ask_should_skip_level_fn,
                );

                // Calculate top of book spread
                let spread = if !bids.is_empty() && !asks.is_empty() {
                    asks.first().unwrap().price - bids.first().unwrap().price
                } else {
                    f64::NAN
                };

                // Send the new Summary to the gRPC service to broadcast to clients
                if (summary_out_tx.send(Summary { spread, bids, asks }).await).is_err() {
                    log::debug!(
                        "Service is no longer listening to consolidated order book updates"
                    );
                    break;
                }
            }
        }
    }
}

/// Type alias for a function that determines the cutoff price for adding new levels to the
/// aggregate half orderbook
type PriceCutoffFunc =
    fn(depth: usize, aggregate_half_orderbook: &BTreeSet<OrderbookLevel>) -> Decimal;
/// Type alias for a function that
type ShouldSkipLevelFunc = fn(price: &Decimal, cutoff: &Decimal) -> bool;

/// Main function that merges multiple orderbooks together in order and converts to the proto
/// type Level
///
/// # Arguments
///
/// * `depth`: How deep we want the merged book
/// * `exch_half_orderbooks`: The various exchange half orderbooks to merge
/// * `cutoff_price_fn`: Function to determine what is the current minimum price in order to be added
/// * `should_skip_level_fn`: Function to determine if this level is over the cutoff threshold
///
/// returns: A sorted vector of aggregated Level's
fn aggregate_half_orderbook(
    depth: usize,
    exch_half_orderbooks: &[Vec<OrderbookLevel>; 2],
    cutoff_price_fn: PriceCutoffFunc,
    should_skip_level_fn: ShouldSkipLevelFunc,
) -> Vec<Level> {
    let mut aggregate_book: BTreeSet<OrderbookLevel> = BTreeSet::new();

    let max_level = get_max_level_across_exchanges(exch_half_orderbooks);
    for level_idx in 0..max_level {
        let cutoff_price = cutoff_price_fn(depth, &aggregate_book);
        for exch_half_orderbook in exch_half_orderbooks.iter() {
            if exch_half_orderbook.len() <= level_idx {
                // One exchange has less levels, skip
                continue;
            }

            let level = &(exch_half_orderbook)[level_idx];
            if should_skip_level_fn(&level.price, &cutoff_price) {
                // This level is not enough to fit in top depth
                continue;
            }

            aggregate_book.insert(level.clone());
        }
    }

    aggregate_book.iter().take(depth).map(Level::from).collect()
}

///
///
/// # Arguments
///
/// * `exch_half_order_books`: The various exchange half orderbooks to compare
///
/// returns: The largest depth across all half orderbooks
fn get_max_level_across_exchanges(exch_half_order_books: &[Vec<OrderbookLevel>]) -> usize {
    exch_half_order_books
        .iter()
        .fold(0, |max, b| max.max(b.len()))
}

/// The main processing function for each exchange orderbook stream and determining if anything
/// material changed that is worth notifying the downstream SUmmary aggregator processor
///
/// # Arguments
///
/// * `depth`: The minimum depth we want
/// * `orderbook_rx`: The orderbook receiver for the particular exchange update stream
/// * `orderbook_tx`: The channel to send the orderbook update to get aggregated
/// * `exchange`: The exchange name for this handler
///
async fn incoming_exchange_feed_loop(
    depth: usize,
    mut orderbook_rx: tokio::sync::broadcast::Receiver<Orderbook>,
    orderbook_tx: Sender<Orderbook>,
    exchange: &'static str,
) {
    // Keep the last update to see if it has changed and if now don't send update
    let mut last_bids: Cell<Vec<OrderbookLevel>> = Cell::new(Vec::default());
    let mut last_asks: Cell<Vec<OrderbookLevel>> = Cell::new(Vec::default());

    loop {
        match orderbook_rx.recv().await {
            Ok(orderbook) => {
                // Check if anything changed
                if check_half_book_changed_and_update(depth, &mut last_bids, &orderbook.bids)
                    || check_half_book_changed_and_update(depth, &mut last_asks, &orderbook.asks)
                {
                    //Something changed, send it to Summary processor
                    if (orderbook_tx.send(orderbook).await).is_err() {
                        log::debug!("No more listeners to {} orderbook updates", exchange);
                        break;
                    }
                }
            }
            Err(_) => {
                log::debug!("{} API stopped sending orderbook updates", exchange);
                break;
            }
        }
    }
}
