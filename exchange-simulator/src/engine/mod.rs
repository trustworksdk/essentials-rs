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
    ClockChange, ClockState, Exchange, ExchangeError, ExchangeOptions, SubmitOrder, SubmitOutcome,
};

pub use crate::ids::IdScheme;

use chrono::{DateTime, Utc};

/// Inputs a matching step needs from outside the book: the clock reading and the ID scheme.
#[derive(Clone, Copy, Debug)]
pub struct MatchContext {
    pub now: DateTime<Utc>,
    pub ids: IdScheme,
}
