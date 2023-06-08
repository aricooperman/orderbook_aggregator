use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;

use crate::errors::{KeyrockError, Result};
use crate::types::{OrderBook, OrderBookLevel};

pub mod errors;
pub mod types;
pub mod ws;

pub fn value_to_decimal(value: &Value) -> Result<Decimal> {
    match value.as_str() {
        None => Err(KeyrockError::BadData(format!(
            "Decimal is not a str for level {}",
            value
        ))),
        Some(dec_str) => match Decimal::from_str(dec_str) {
            Ok(dec) => Ok(dec),
            Err(e) => Err(KeyrockError::BadData(format!(
                "Could not parse str as a decimal for level {}: {:?}",
                value, e
            ))),
        },
    }
}

pub fn try_values_to_orderbook(
    exchange_name: &'static str,
    bid_levels: &Value,
    ask_levels: &Value,
    depth: usize,
) -> Result<OrderBook> {
    if !bid_levels.is_array() || !ask_levels.is_array() {
        return Err(KeyrockError::BadData(format!(
            "Bids and/or asks are not arrays: {} - {}",
            bid_levels, ask_levels
        )));
    }

    let mut bids = vec![];
    let mut asks = vec![];
    for i in 0..depth {
        if !bid_levels[i].is_null() {
            let mut obl: OrderBookLevel = (&bid_levels[i]).try_into()?;
            obl.exchange = exchange_name;
            bids.push(obl);
        }

        if !ask_levels[i].is_null() {
            let mut obl: OrderBookLevel = (&ask_levels[i]).try_into()?;
            obl.exchange = exchange_name;
            asks.push(obl);
        }
    }

    Ok(OrderBook { bids, asks })
}
