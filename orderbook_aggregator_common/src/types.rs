use std::cmp::Ordering;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::Error;
use crate::value_to_decimal;

/// Side of the orderbook enum
#[derive(Default, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Side {
    #[default]
    Buy,
    Sell,
}

/// Represents the two half's of an orderbook
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Orderbook {
    /// The exchange this orderbook represents
    #[serde(skip)]
    pub exchange: &'static str,
    /// The bid half book
    pub bids: Vec<OrderbookLevel>,
    /// The ask half book
    pub asks: Vec<OrderbookLevel>,
}

/// Represents a level in a half of an orderbook
#[derive(Clone, Debug, Eq, Serialize, Deserialize)]
pub struct OrderbookLevel {
    #[serde(skip)]
    pub exchange: &'static str,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(skip)]
    pub side: Side,
}

impl PartialOrd for OrderbookLevel {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderbookLevel {
    /// Compares the price level depending on side and then quantity descending
    ///
    /// # Arguments
    ///
    /// * `other`: The other OrderBookLevel to compare against
    ///
    /// returns: Whether self is less than, equal, or grater than other
    ///
    /// # Examples
    ///
    /// ```
    /// use std::cmp::Ordering;
    /// use rust_decimal::Decimal;
    /// use orderbook_aggregator_common::types::{OrderbookLevel, Side};
    ///
    ///
    /// let orderbook_level = OrderbookLevel {
    ///     exchange: "",
    ///     price: Decimal::ONE,
    ///     quantity: Decimal::ONE,
    ///     side: Side::Buy
    /// };
    ///
    /// let orderbook_level_2 = OrderbookLevel {
    ///     exchange: "",
    ///     price: Decimal::ONE,
    ///     quantity: Decimal::ONE_THOUSAND,
    ///     side: Side::Buy };
    ///
    /// assert!(orderbook_level > orderbook_level_2);
    ///
    /// assert_eq!(orderbook_level.cmp(&OrderbookLevel {
    ///     exchange: "",
    ///     price: Decimal::ONE,
    ///     quantity: Decimal::ZERO,
    ///     side: Side::Buy }
    /// ), Ordering::Less);
    /// assert_eq!(orderbook_level.cmp(&OrderbookLevel {
    ///     exchange: "",
    ///     price: Decimal::ONE,
    ///     quantity: Decimal::ONE,
    ///     side: Side::Buy }
    /// ), Ordering::Equal);
    /// assert_eq!(orderbook_level.cmp(&OrderbookLevel {
    ///     exchange: "",
    ///     price: Decimal::ONE_THOUSAND,
    ///     quantity: Decimal::ONE,
    ///     side: Side::Buy }
    /// ), Ordering::Greater);
    /// assert_eq!(orderbook_level.cmp(&OrderbookLevel {
    ///     exchange: "",
    ///     price: Decimal::ZERO,
    ///     quantity: Decimal::ONE,
    ///     side: Side::Buy }
    /// ), Ordering::Less);
    /// ```
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        if self.side != other.side {
            self.side.cmp(&other.side) // This should not happen
        } else {
            match self.side {
                Side::Buy => other
                    .price
                    .cmp(&self.price)
                    .then(other.quantity.cmp(&self.quantity)),
                Side::Sell => self
                    .price
                    .cmp(&other.price)
                    .then(other.quantity.cmp(&self.quantity)),
            }
        }
    }
}

impl PartialEq for OrderbookLevel {
    /// Two OrderbookLevels are equal if they have the same price, quantity & side
    ///
    /// # Arguments
    ///
    /// * `other`: The other OrderbookLevel to compare partial equality to
    ///
    /// returns: Whether self is equal to other
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.side.eq(&other.side)
            && self.price.eq(&other.price)
            && self.quantity.eq(&other.quantity)
    }
}

impl TryFrom<&Value> for OrderbookLevel {
    type Error = Error;

    /// Tries to convert a JSON Value that refers to a JSON level to an OrderbookLevel
    ///
    /// # Arguments
    ///
    /// * `value`: A JSON Value that represents an array of two floats as strings
    ///
    /// returns: The Result of trying to convert the JSON Value to an OrderBookLevel
    ///
    /// # Examples
    ///
    /// ```
    /// use serde_json::json;
    /// use orderbook_aggregator_common::types::OrderbookLevel;
    ///
    /// let level = json!(["1.2345", "5.4321"]);
    /// let orderbook_level = OrderbookLevel::try_from(&level).unwrap();
    /// ```
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        if value.is_null() || !value.is_array() {
            return Err(Error::BadData(format!(
                "Could not parse value {} as level as it was not an array",
                value
            )));
        }

        let price = value_to_decimal(&value[0])?;
        let quantity = value_to_decimal(&value[1])?;

        Ok(OrderbookLevel {
            exchange: "",
            price,
            quantity,
            side: Side::Buy,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_ask_orderbook_level_cmp() {
        let orderbook_level = OrderbookLevel {
            exchange: "",
            price: Decimal::ONE,
            quantity: Decimal::ONE,
            side: Side::Sell,
        };

        assert_eq!(
            orderbook_level.cmp(&OrderbookLevel {
                exchange: "",
                price: Decimal::ONE,
                quantity: Decimal::ONE_THOUSAND,
                side: Side::Sell
            }),
            Ordering::Greater
        );
        assert_eq!(
            orderbook_level.cmp(&OrderbookLevel {
                exchange: "",
                price: Decimal::ONE,
                quantity: Decimal::ZERO,
                side: Side::Sell
            }),
            Ordering::Less
        );
        assert_eq!(
            orderbook_level.cmp(&OrderbookLevel {
                exchange: "",
                price: Decimal::ONE,
                quantity: Decimal::ONE,
                side: Side::Sell
            }),
            Ordering::Equal
        );
        assert_eq!(
            orderbook_level.cmp(&OrderbookLevel {
                exchange: "",
                price: Decimal::ONE_THOUSAND,
                quantity: Decimal::ONE,
                side: Side::Sell
            }),
            Ordering::Less
        );
        assert_eq!(
            orderbook_level.cmp(&OrderbookLevel {
                exchange: "",
                price: Decimal::ZERO,
                quantity: Decimal::ONE,
                side: Side::Sell
            }),
            Ordering::Greater
        );
    }

    #[test]
    fn test_bid_orderbook_level_cmp() {
        let orderbook_level = OrderbookLevel {
            exchange: "",
            price: Decimal::ONE,
            quantity: Decimal::ONE,
            side: Side::Buy,
        };

        assert_eq!(
            orderbook_level.cmp(&OrderbookLevel {
                exchange: "",
                price: Decimal::ONE,
                quantity: Decimal::ONE_THOUSAND,
                side: Side::Buy
            }),
            Ordering::Greater
        );
        assert_eq!(
            orderbook_level.cmp(&OrderbookLevel {
                exchange: "",
                price: Decimal::ONE,
                quantity: Decimal::ZERO,
                side: Side::Buy
            }),
            Ordering::Less
        );
        assert_eq!(
            orderbook_level.cmp(&OrderbookLevel {
                exchange: "",
                price: Decimal::ONE,
                quantity: Decimal::ONE,
                side: Side::Buy
            }),
            Ordering::Equal
        );
        assert_eq!(
            orderbook_level.cmp(&OrderbookLevel {
                exchange: "",
                price: Decimal::ONE_THOUSAND,
                quantity: Decimal::ONE,
                side: Side::Buy
            }),
            Ordering::Greater
        );
        assert_eq!(
            orderbook_level.cmp(&OrderbookLevel {
                exchange: "",
                price: Decimal::ZERO,
                quantity: Decimal::ONE,
                side: Side::Buy
            }),
            Ordering::Less
        );
    }

    #[test]
    fn orderbook_level_try_from_good() {
        let level = json!(["1.2345", "5.4321"]);
        assert!(OrderbookLevel::try_from(&level).is_ok())
    }

    #[test]
    fn orderbook_level_try_from_bad() {
        let level = json!("ABC");
        assert!(OrderbookLevel::try_from(&level).is_err())
    }
}
