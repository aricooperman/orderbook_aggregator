use std::fmt::{Display, Formatter};

use crate::orderbook::{Level, Summary};

impl Display for Summary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Spread: {:.6}", self.spread)?;
        writeln!(f, "{:<2}: {:>35} | {:<35}", "L", "Bids", "Asks")?;

        let mut i = 1;
        for (bid_level, ask_level) in self.bids.iter().zip(self.asks.iter()) {
            writeln!(f, "{:<2}: {:>35} | {:<35}", i, bid_level, ask_level)?;
            i += 1;
        }

        Ok(())
    }
}

impl Display for Level {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:.6} @ {:.6} on {}",
            self.price, self.amount, self.exchange
        )
    }
}
