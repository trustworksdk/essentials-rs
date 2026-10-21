//! Matching engine. [`OrderBook`] is a pure, single-instrument, price-time-priority book: it performs no I/O, and it
//! splits every command into a side-effect-free *plan* (a [`BookUpdate`]) and an *apply* step, so the caller can
//! persist a proposed transition and only apply it to memory once the commit succeeded. [`Exchange`] is that caller:
//! it owns the books and the SQLite store and runs the plan, commit, apply cycle.

mod book;
mod dispatcher;
mod exchange;

pub use crate::clock::{Clock, FixedClock, SystemClock};
pub use book::{BookError, BookUpdate, InvalidOrder, Level, OrderBook, RejectReason};
pub use dispatcher::{BookSnapshot, DispatchError, Dispatcher, GroupCommit};
pub use exchange::{
    CancelTarget, ClockChange, ClockState, Exchange, ExchangeError, ExchangeOptions,
    LiquidityPatch, LiquidityPolicy, LiquidityStatus, SubmitOrder, SubmitOutcome,
};

pub use crate::ids::IdScheme;

use chrono::{DateTime, Utc};

/// Inputs a matching step needs from outside the book: the clock reading, the ID scheme, and the dates its
/// executions carry.
#[derive(Clone, Copy, Debug)]
pub struct MatchContext {
    pub now: DateTime<Utc>,
    pub ids: IdScheme,
    pub dates: crate::domain::TradeDates,
    /// The instrument's market is outside its session: client orders are rejected (SR-16).
    pub market_closed: bool,
}

impl MatchContext {
    /// A context dated by `now`'s UTC date and settling T+1, for callers without a business calendar.
    pub fn utc(now: DateTime<Utc>, ids: IdScheme) -> Self {
        Self {
            now,
            ids,
            dates: crate::settlement::TradeDating::default().dates(now, None),
            market_closed: false,
        }
    }
}
