use std::cell::Cell;
use std::iter::zip;

use rust_decimal::prelude::ToPrimitive;

use orderbook_aggregator_common::types::OrderbookLevel;

use crate::orderbook::Level;

/// Compares the previous half book to the new one just received and if different, updates the last
/// value reference
///
/// # Arguments
///
/// * `depth`: What depth do we minimally need
/// * `half_book_orig`: The previous update
/// * `half_book_new`: The new update
///
/// returns: bool
pub(crate) fn check_half_book_changed_and_update(
    depth: usize,
    half_book_orig: &mut Cell<Vec<OrderbookLevel>>,
    half_book_new: &[OrderbookLevel],
) -> bool {
    let orig = half_book_orig.get_mut();
    // If our last update was less depth than we want and new has more, then no need to check differences
    if (orig.len() < depth && orig.len() < half_book_new.len())
        || half_book_changed(orig, half_book_new)
    {
        // It has changed so update our previous value reference for next update
        half_book_orig.replace(half_book_new.to_vec());
        true
    } else {
        false
    }
}

/// Takes two half books (as OrderbookLevel slices) and checks if they are different, returning
/// immediately when a difference is detected
///
/// # Arguments
///
/// * `book_one`: The first half book to compare
/// * `book_two`: The second half book to compare
///
/// returns: bool
pub(crate) fn half_book_changed(book_one: &[OrderbookLevel], book_two: &[OrderbookLevel]) -> bool {
    book_one.len() != book_two.len()
        || !zip(book_one, book_two).all(|(obl_one, obl_two)| obl_one.eq(obl_two))
}

impl From<&OrderbookLevel> for Level {
    /// Takes an internal representation of an orderbook level with fixed precision and converts to
    /// the proto version Level with floating point numeric representation
    ///
    /// # Arguments
    ///
    /// * `obl`: The OrderbookLevel to convert
    ///
    /// returns: The converted Level instance
    ///
    /// # Examples
    ///
    /// ```
    /// let orderbook_level = OrderbookLevel{ price: Decimal::ONE, quantity: Decimal::ONE, side: Side::Buy };
    /// println!("{:?}", orderbook_level);
    /// let level = convert_orderbook_to_level(orderbook_level, "Some Exchange");
    /// println!("{:?}", level);
    /// ```
    fn from(obl: &OrderbookLevel) -> Self {
        Self {
            exchange: obl.exchange.to_string(),
            price: obl.price.to_f64().unwrap(), // This should not error, if it does fail fast
            amount: obl.quantity.to_f64().unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use orderbook_aggregator_common::types::Side;

    use super::*;

    #[test]
    fn test_half_book_changed() {
        let half_book_one = vec![
            OrderbookLevel {
                exchange: "",
                price: Decimal::ONE,
                quantity: Decimal::ONE,
                side: Side::Buy,
            },
            OrderbookLevel {
                exchange: "",
                price: Decimal::ONE_HUNDRED,
                quantity: Decimal::ONE,
                side: Side::Buy,
            },
        ];
        let half_book_one_clone = half_book_one.clone();

        assert!(!half_book_changed(&half_book_one, &half_book_one_clone));

        let half_book_two = vec![
            OrderbookLevel {
                exchange: "",
                price: Decimal::ONE_HUNDRED,
                quantity: Decimal::ONE,
                side: Side::Buy,
            },
            OrderbookLevel {
                exchange: "",
                price: Decimal::ONE_THOUSAND,
                quantity: Decimal::ONE,
                side: Side::Buy,
            },
        ];

        assert!(half_book_changed(&half_book_one, &half_book_two));

        let half_book_three = vec![OrderbookLevel {
            exchange: "",
            price: Decimal::ONE_HUNDRED,
            quantity: Decimal::ONE,
            side: Side::Buy,
        }];

        assert!(half_book_changed(&half_book_one, &half_book_three));
    }

    #[test]
    fn test_convert_orderbook_to_level() {
        let orderbook_level = OrderbookLevel {
            exchange: "Some Exchange",
            price: Decimal::ONE,
            quantity: Decimal::ONE,
            side: Side::Buy,
        };
        let level: Level = Level::from(&orderbook_level);
        assert_eq!(orderbook_level.price.to_f64().unwrap(), level.price);
        assert_eq!(orderbook_level.quantity.to_f64().unwrap(), level.amount);
        assert_eq!(orderbook_level.exchange.to_string(), level.exchange);
    }
}
