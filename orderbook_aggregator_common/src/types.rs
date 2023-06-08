use crate::errors::KeyrockError;
use crate::value_to_decimal;
use rust_decimal::Decimal;
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::str::FromStr;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBook {
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
}

/// A position represents an unfilled order that is kept in the system for later filling.
#[derive(Clone, Debug, Eq, Serialize, Deserialize)]
pub struct OrderBookLevel {
    #[serde(skip)]
    pub exchange: &'static str,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
}

impl PartialOrd for OrderBookLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderBookLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.price
            .cmp(&other.price)
            .then(self.quantity.cmp(&other.quantity))
    }
}

impl PartialEq for OrderBookLevel {
    fn eq(&self, other: &Self) -> bool {
        self.price.eq(&other.price) && self.quantity.eq(&other.quantity)
    }
}

impl TryFrom<&Value> for OrderBookLevel {
    type Error = KeyrockError;
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        if !value.is_array() {
            return Err(KeyrockError::BadData(format!(
                "Could not parse value {} as level as it was not an array",
                value
            )));
        }

        let price = &value[0];
        if price.is_null() {
            return Err(KeyrockError::BadData(format!(
                "Could not parse price from {}",
                price
            )));
        }
        let price = value_to_decimal(price)?;

        let quantity = &value[1];
        if quantity.is_null() {
            return Err(KeyrockError::BadData(format!(
                "Could not parse price from {}",
                quantity
            )));
        }
        let quantity = value_to_decimal(quantity)?;

        Ok(OrderBookLevel {
            exchange: "",
            price,
            quantity,
        })
    }
}

///
///
/// # Arguments
///
/// * `deserializer`:
///
/// returns: Result<Decimal, <D as Deserializer>::Error>
///
/// # Examples
///
/// ```
///
/// ```
pub fn decimal_from_str<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Decimal, D::Error> {
    Ok(match Value::deserialize(deserializer)? {
        Value::String(s) => Decimal::from_str(s.as_str()).map_err(de::Error::custom)?,
        _ => return Err(de::Error::custom("wrong type")),
    })
}

pub fn u32_from_str<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    Ok(match Value::deserialize(deserializer)? {
        Value::String(s) => u32::from_str(s.as_str()).map_err(de::Error::custom)?,
        _ => return Err(de::Error::custom("wrong type")),
    })
}
