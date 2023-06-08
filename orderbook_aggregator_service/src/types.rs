use crate::orderbook::Level;
use orderbook_aggregator_common::types::OrderBookLevel;
use rust_decimal::prelude::ToPrimitive;

#[derive(Clone, Debug)]
pub(crate) struct ExchangeOrderBook {
    pub(crate) exchange: &'static str,
    pub(crate) bids: Vec<OrderBookLevel>,
    pub(crate) asks: Vec<OrderBookLevel>,
}

impl From<OrderBookLevel> for Level {
    fn from(obl: OrderBookLevel) -> Self {
        Self {
            exchange: obl.exchange.to_string(),
            price: obl.price.to_f64().unwrap(),
            amount: obl.quantity.to_f64().unwrap(),
        }
    }
}
