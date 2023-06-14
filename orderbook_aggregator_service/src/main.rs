mod service;
mod types;

use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use std::cell::Cell;
use std::collections::BTreeSet;

use bitstamp_api::{BitstampWebSocketApi, BITSTAMP_EXCHANGE_NAME};
use orderbook_aggregator_common::{errors::Result, types::OrderBookLevel};

use crate::orderbook::orderbook_aggregator_server::OrderbookAggregatorServer;
use crate::orderbook::{Level, Summary};
use crate::service::OrderbookAggregatorService;
use crate::types::ExchangeOrderBook;

use binance_api::{BinanceWebSocketApi, BINANCE_EXCHANGE_NAME};
use orderbook_aggregator_common::types::OrderBook;
use orderbook_aggregator_common::ws::ExchangeWebSocketApi;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::transport::Server;

pub mod orderbook {
    tonic::include_proto!("orderbook");
}

const NUMBER_OF_EXCHANGES: usize = 2;

#[derive(Parser, Debug)]
#[clap(version = "0.0.1", about = "Keyrock Orderbook Aggregator Service")]
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

    let log_level = match opts.verbosity {
        0 => LevelFilter::Info,
        1 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    Builder::from_default_env().filter(None, log_level).init();

    log::info!("Starting order book aggregator service for {}", opts.symbol);

    let mut bitstamp_api = BitstampWebSocketApi::connect_default().await.unwrap();
    let bitstamp_feed: tokio::sync::broadcast::Receiver<OrderBook> = bitstamp_api
        .subscribe_orderbook(&opts.symbol, opts.depth)
        .await
        .unwrap();

    let mut binance_api = BinanceWebSocketApi::connect_default().await.unwrap();
    let binance_feed: tokio::sync::broadcast::Receiver<OrderBook> = binance_api
        .subscribe_orderbook(&opts.symbol, opts.depth)
        .await
        .unwrap();

    let (tx, rx) = mpsc::channel::<Summary>(128);
    let (exch_ob_tx, exch_ob_rx) = mpsc::channel::<ExchangeOrderBook>(128);

    let addr = opts.listen_at.parse().unwrap();
    let orderbook_service = OrderbookAggregatorService::new(rx);
    let svc = OrderbookAggregatorServer::new(orderbook_service);

    tokio::spawn(outgoing_summary_loop(opts.depth, tx, exch_ob_rx));

    tokio::spawn(incoming_bitstamp_feed_loop(
        opts.depth,
        bitstamp_feed,
        exch_ob_tx.clone(),
    ));

    tokio::spawn(incoming_binance_feed_loop(
        opts.depth,
        binance_feed,
        exch_ob_tx,
    ));

    Server::builder()
        .add_service(svc)
        .serve(addr)
        .await
        .unwrap();

    let _ = binance_api.disconnect().await;

    Ok(())
}

async fn outgoing_summary_loop(
    depth: usize,
    summary_out_tx: Sender<Summary>,
    mut exch_ob_in_rx: Receiver<ExchangeOrderBook>,
) {
    let mut exchange_bids: [Vec<OrderBookLevel>; NUMBER_OF_EXCHANGES] = Default::default();
    let mut exchange_asks: [Vec<OrderBookLevel>; NUMBER_OF_EXCHANGES] = Default::default();

    const BITSTAMP_IDX: usize = 0;
    const BINANCE_IDX: usize = 1;

    let bid_cutoff_fn: PriceCutoffFunc = |depth, orderbook| {
        if orderbook.len() >= depth {
            orderbook
                .first()
                .map(|obl| obl.price)
                .unwrap_or(Decimal::MIN)
        } else {
            Decimal::MIN
        }
    };
    let bid_should_skip_level_fn: ShouldSkipLevelFunc = |price, cutoff| price < cutoff;

    let ask_cutoff_fn: PriceCutoffFunc = |depth, orderbook| {
        if orderbook.len() >= depth {
            orderbook
                .last()
                .map(|obl| obl.price)
                .unwrap_or(Decimal::MAX)
        } else {
            Decimal::MAX
        }
    };
    let ask_should_skip_level_fn: ShouldSkipLevelFunc = |price, cutoff| price > cutoff;

    loop {
        match exch_ob_in_rx.recv().await {
            None => {
                log::debug!("No more exchange orderbook senders, stopping service");
                break;
            }
            Some(eob) => {
                match eob.exchange {
                    BITSTAMP_EXCHANGE_NAME => {
                        exchange_bids[BITSTAMP_IDX] = eob.bids;
                        exchange_asks[BITSTAMP_IDX] = eob.asks;
                    }
                    BINANCE_EXCHANGE_NAME => {
                        exchange_bids[BINANCE_IDX] = eob.bids;
                        exchange_asks[BINANCE_IDX] = eob.asks;
                    }
                    &_ => {
                        log::error!("Unknown exchange {}", eob.exchange);
                        continue;
                    }
                }

                let mut aggregate_bids: BTreeSet<OrderBookLevel> = BTreeSet::new();
                let mut aggregate_asks: BTreeSet<OrderBookLevel> = BTreeSet::new();

                add_to_aggregate_half_orderbook(
                    depth,
                    &mut exchange_bids,
                    &mut aggregate_bids,
                    bid_cutoff_fn,
                    bid_should_skip_level_fn,
                );

                add_to_aggregate_half_orderbook(
                    depth,
                    &mut exchange_asks,
                    &mut aggregate_asks,
                    ask_cutoff_fn,
                    ask_should_skip_level_fn,
                );

                //Reverse - higher bids at top
                let bids: Vec<Level> = aggregate_bids
                    .into_iter()
                    .rev()
                    .take(depth)
                    .map(Level::from)
                    .collect();
                let asks: Vec<Level> = aggregate_asks
                    .into_iter()
                    .take(depth)
                    .map(Level::from)
                    .collect();
                let spread = if !bids.is_empty() && !asks.is_empty() {
                    asks.first().unwrap().price - bids.first().unwrap().price
                } else {
                    f64::NAN
                };

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

type PriceCutoffFunc =
    fn(depth: usize, aggregate_half_orderbook: &mut BTreeSet<OrderBookLevel>) -> Decimal;
type ShouldSkipLevelFunc = fn(price: &Decimal, cutoff: &Decimal) -> bool;

fn add_to_aggregate_half_orderbook(
    depth: usize,
    exch_half_orderbooks: &mut [Vec<OrderBookLevel>; 2],
    aggregate_half_orderbook: &mut BTreeSet<OrderBookLevel>,
    cutoff_price_fn: PriceCutoffFunc,
    should_skip_level_fn: ShouldSkipLevelFunc,
) {
    let max_level = get_max_level_across_exchanges(exch_half_orderbooks);
    for level_idx in 0..max_level {
        let cutoff_price = cutoff_price_fn(depth, aggregate_half_orderbook);
        for exch_half_orderbook in exch_half_orderbooks.iter() {
            if exch_half_orderbook.len() <= level_idx {
                continue;
            }

            let level = &(exch_half_orderbook)[level_idx];
            if should_skip_level_fn(&level.price, &cutoff_price) {
                continue;
            }
            aggregate_half_orderbook.insert(level.clone());
        }
    }
}

fn get_max_level_across_exchanges(exch_half_order_books: &[Vec<OrderBookLevel>; 2]) -> usize {
    exch_half_order_books
        .iter()
        .fold(0, |max, b| max.max(b.len()))
}

async fn incoming_bitstamp_feed_loop(
    depth: usize,
    mut orderbook_rx: tokio::sync::broadcast::Receiver<OrderBook>,
    exch_ob_tx: Sender<ExchangeOrderBook>,
) {
    let mut last_bids: Cell<Vec<OrderBookLevel>> = Cell::new(Vec::default());
    let mut last_asks: Cell<Vec<OrderBookLevel>> = Cell::new(Vec::default());

    loop {
        match orderbook_rx.recv().await {
            Ok(orderbook) => {
                // Check if anything changed
                if check_half_book_changed_and_update(depth, &mut last_bids, &orderbook.bids)
                    || check_half_book_changed_and_update(depth, &mut last_asks, &orderbook.asks)
                {
                    //Something changed
                    if (exch_ob_tx
                        .send(ExchangeOrderBook {
                            exchange: BITSTAMP_EXCHANGE_NAME,
                            bids: orderbook.bids,
                            asks: orderbook.asks,
                        })
                        .await)
                        .is_err()
                    {
                        log::debug!("No more listeners to Bitstamp orderbook updates");
                        break;
                    }
                }
            }
            Err(_) => {
                log::debug!("Bitstamp API stopped sending orderbook updates");
                break;
            }
        }
    }
}

//TODO refactor this with bitstamp version and generalize
async fn incoming_binance_feed_loop(
    depth: usize,
    mut orderbook_rx: tokio::sync::broadcast::Receiver<OrderBook>,
    exch_ob_tx: Sender<ExchangeOrderBook>,
) {
    let mut last_bids: Cell<Vec<OrderBookLevel>> = Cell::new(Vec::default());
    let mut last_asks: Cell<Vec<OrderBookLevel>> = Cell::new(Vec::default());

    loop {
        match orderbook_rx.recv().await {
            Ok(orderbook) => {
                // Check if anything changed
                if check_half_book_changed_and_update(depth, &mut last_bids, &orderbook.bids)
                    || check_half_book_changed_and_update(depth, &mut last_asks, &orderbook.asks)
                {
                    //Something changed
                    if (exch_ob_tx
                        .send(ExchangeOrderBook {
                            exchange: BINANCE_EXCHANGE_NAME,
                            bids: orderbook.bids,
                            asks: orderbook.asks,
                        })
                        .await)
                        .is_err()
                    {
                        log::debug!("No more listeners to Binance orderbook updates");
                        break;
                    }
                }
            }
            Err(_) => {
                log::debug!("Binance API stopped sending orderbook updates");
                break;
            }
        }
    }
}

fn check_half_book_changed_and_update(
    depth: usize,
    half_book_orig: &mut Cell<Vec<OrderBookLevel>>,
    half_book_new: &[OrderBookLevel],
) -> bool {
    let orig = half_book_orig.get_mut();
    if orig.len() < depth || half_book_changed(orig, half_book_new) {
        half_book_orig.replace(half_book_new.to_vec());
        true
    } else {
        false
    }
}

//TODO test
fn half_book_changed(book_one: &[OrderBookLevel], book_two: &[OrderBookLevel]) -> bool {
    !book_one
        .iter()
        .zip(book_two.iter())
        .all(|(obl_one, obl_two)| obl_one.eq(obl_two))
}
