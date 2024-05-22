use std::str::FromStr;

use rust_decimal::Decimal;
use serde_json::Value;

use crate::errors::{Error, Result};
use crate::types::{Orderbook, OrderbookLevel, Side};

pub mod errors;
pub mod types;
pub mod ws;

/// Parses a JSON Value to a Decimal
///
/// # Arguments
///
/// * `value`: A reference to a JSON value that is expected to be a string
///
/// returns: Result<Decimal, Error>
///
/// # Examples
///
/// ```
/// use std::str::FromStr;
/// use rust_decimal::Decimal;
/// use serde_json::json;
/// use orderbook_aggregator_common::value_to_decimal;
///
/// let dec_val = json!("1.2345");
/// let decimal = value_to_decimal(&dec_val).unwrap();
/// ```
pub fn value_to_decimal(value: &Value) -> Result<Decimal> {
    if value.is_null() {
        return Err(Error::BadData(format!(
            "Could not parse price from {}",
            value
        )));
    }

    match value.as_str() {
        None => Err(Error::BadData(format!(
            "Decimal is not a str for level {}",
            value
        ))),
        Some(dec_str) => match Decimal::from_str(dec_str) {
            Ok(dec) => Ok(dec),
            Err(e) => Err(Error::BadData(format!(
                "Could not parse str as a decimal for level {}: {:?}",
                value, e
            ))),
        },
    }
}

/// Attempts to parse the JSON representation of the bid and ask levels of an orderbook
///
/// # Arguments
///
/// * `exchange_name`: The name of the exchange orderbook we are parsing
/// * `bid_levels`: A JSON Value which is an JSON array of 2 element arrays of strings that
/// represent the price and quantity at each level
/// * `ask_levels`: A JSON Value which is an JSON array of 2 element arrays of strings that
/// represent the price and quantity at each level
/// * `depth`: The number of levels we are interested in parsing
///
/// returns: Result<OrderBook, Error>
///
/// # Examples
///
/// ```
/// use serde_json::json;
/// use orderbook_aggregator_common::try_values_to_orderbook;
///
/// let bids = json!([["25526","0.09509835"],["25525","0.00960334"],["25524","0.42000000"],["25523","1.13385017"]]);
/// let asks = json!([["25528","0.29949565"],["25529","0.45172593"],["25531","0.58753794"],["25533","0.42268177"]]);
/// let orderbook = try_values_to_orderbook("EXCHANGE", &bids, &asks, 4).unwrap();
///
/// println!("{:?}", orderbook);
/// ```
pub fn try_values_to_orderbook(
    exchange: &'static str,
    bid_levels: &Value,
    ask_levels: &Value,
    depth: usize,
) -> Result<Orderbook> {
    if !bid_levels.is_array() || !ask_levels.is_array() {
        return Err(Error::BadData(format!(
            "Bids and/or asks are not arrays: {} - {}",
            bid_levels, ask_levels
        )));
    }

    let mut bids = vec![];
    let mut asks = vec![];
    for i in 0..depth {
        if !bid_levels[i].is_null() {
            bids.push(try_parse_orderbook_level(
                exchange,
                Side::Buy,
                &bid_levels[i],
            )?);
        }

        if !ask_levels[i].is_null() {
            asks.push(try_parse_orderbook_level(
                exchange,
                Side::Sell,
                &ask_levels[i],
            )?);
        }
    }

    Ok(Orderbook {
        exchange,
        bids,
        asks,
    })
}

/// Attempts to parse a JSON level into an orderbook level
///
/// # Arguments
///
/// * `exchange`: The exchange this levelis for
/// * `side`: The side of this level
/// * `level_value`: The JSON Value reference that should represent an array of two strings
///
/// returns: The result of the conversion
fn try_parse_orderbook_level(
    exchange: &'static str,
    side: Side,
    level_value: &Value,
) -> Result<OrderbookLevel> {
    let mut obl: OrderbookLevel = (level_value).try_into()?;
    obl.side = side;
    obl.exchange = exchange;
    Ok(obl)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parse_good_decimal() {
        let str_decimal = "1.2345";
        let dec_val = json!(str_decimal);
        let exp_decimal = Decimal::from_str(str_decimal).unwrap();
        let decimal = value_to_decimal(&dec_val).unwrap();
        assert_eq!(decimal, exp_decimal)
    }

    #[test]
    fn parse_bad_decimal() {
        let dec_val = json!("ABC");
        assert!(value_to_decimal(&dec_val).is_err())
    }

    #[test]
    fn parse_bad_value_type() {
        let dec_val = json!("[1.2345]");
        assert!(value_to_decimal(&dec_val).is_err())
    }

    #[test]
    fn parse_good_orderbook() {
        let bids = json!([
            ["25526", "0.09509835"],
            ["25525", "0.00960334"],
            ["25524", "0.42000000"],
            ["25523", "1.13385017"]
        ]);
        let asks = json!([
            ["25528", "0.29949565"],
            ["25529", "0.45172593"],
            ["25531", "0.58753794"],
            ["25533", "0.42268177"]
        ]);

        let orderbook = try_values_to_orderbook("EXCHANGE", &bids, &asks, 4).unwrap();
        assert_eq!(orderbook.exchange, "EXCHANGE");
        assert_eq!(orderbook.bids.len(), 4);
        assert_eq!(orderbook.asks.len(), 4);
        assert_eq!(orderbook.bids[0].price, Decimal::from_str("25526").unwrap());
        assert_eq!(
            orderbook.bids[0].quantity,
            Decimal::from_str("0.09509835").unwrap()
        );
    }

    #[test]
    fn parse_bad_orderbook() {
        let bids = json!(["25526", "0.09509835"]);
        let asks = json!("25528");
        assert!(try_values_to_orderbook("EXCHANGE", &bids, &asks, 1).is_err());
    }

    #[test]
    fn parse_less_depth_orderbook() {
        let bids = json!([
            ["25526", "0.09509835"],
            ["25525", "0.00960334"],
            ["25524", "0.42000000"]
        ]);
        let asks = json!([
            ["25528", "0.29949565"],
            ["25529", "0.45172593"],
            ["25531", "0.58753794"],
            ["25533", "0.42268177"]
        ]);

        let orderbook = try_values_to_orderbook("EXCHANGE", &bids, &asks, 2).unwrap();
        assert_eq!(orderbook.exchange, "EXCHANGE");
        assert_eq!(orderbook.bids.len(), 2);
        assert_eq!(orderbook.asks.len(), 2);
        assert_eq!(orderbook.bids[0].price, Decimal::from_str("25526").unwrap());
        assert_eq!(
            orderbook.bids[0].quantity,
            Decimal::from_str("0.09509835").unwrap()
        );
    }

    #[test]
    fn parse_more_depth_orderbook() {
        let bids = json!([
            ["25526", "0.09509835"],
            ["25525", "0.00960334"],
            ["25524", "0.42000000"]
        ]);
        let asks = json!([
            ["25528", "0.29949565"],
            ["25529", "0.45172593"],
            ["25531", "0.58753794"],
            ["25533", "0.42268177"]
        ]);

        let orderbook = try_values_to_orderbook("EXCHANGE", &bids, &asks, 10).unwrap();
        assert_eq!(orderbook.exchange, "EXCHANGE");
        assert_eq!(orderbook.bids.len(), 3);
        assert_eq!(orderbook.asks.len(), 4);
        assert_eq!(orderbook.bids[0].price, Decimal::from_str("25526").unwrap());
        assert_eq!(
            orderbook.bids[0].quantity,
            Decimal::from_str("0.09509835").unwrap()
        );
    }

    #[test]
    fn parse_good_level() {
        let level = json!(["25526", "0.09509835"]);
        let obl = try_parse_orderbook_level("EXCH", Side::Buy, &level).unwrap();
        assert_eq!(obl.side, Side::Buy);
        assert_eq!(obl.price, Decimal::from_str("25526").unwrap());
        assert_eq!(obl.quantity, Decimal::from_str("0.09509835").unwrap());
    }

    #[test]
    fn parse_bad_level() {
        let level = json!(["ABC", "XYZ"]);
        assert!(try_parse_orderbook_level("EXCH", Side::Sell, &level).is_err());
    }
}
