//! Matching engine. [`OrderBook`] is a pure, single-instrument, price-time-priority book: it performs no I/O, and it
//! splits every command into a side-effect-free *plan* (a [`BookUpdate`]) and an *apply* step, so the caller can
//! persist a proposed transition and only apply it to memory once the commit succeeded. [`Exchange`] is that caller:
//! it owns the books and the SQLite store and runs the plan, commit, apply cycle.

mod book;
mod exchange;

pub use book::{BookError, BookUpdate, InvalidOrder, Level, OrderBook, RejectReason};
pub use exchange::{Exchange, ExchangeError, ExchangeOptions, SubmitOrder, SubmitOutcome};

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Source of execution IDs, injectable so tests and seeded scenarios are reproducible.
pub trait IdGenerator {
    fn next_id(&mut self) -> Uuid;
}

/// Time-ordered random UUIDs (v7) for production use.
#[derive(Clone, Copy, Debug, Default)]
pub struct UuidV7Ids;

impl IdGenerator for UuidV7Ids {
    fn next_id(&mut self) -> Uuid {
        Uuid::now_v7()
    }
}

/// Deterministic IDs `00000000-0000-0000-0000-000000000001`, `...02`, ... for tests and replayable scenarios.
#[derive(Clone, Copy, Debug, Default)]
pub struct SequentialIds(pub u128);

impl IdGenerator for SequentialIds {
    fn next_id(&mut self) -> Uuid {
        self.0 += 1;
        Uuid::from_u128(self.0)
    }
}

/// Wall-clock source, injectable for reproducible tests and, later, a virtual clock.
pub trait Clock {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock stuck at one instant.
#[derive(Clone, Copy, Debug)]
pub struct FixedClock(pub DateTime<Utc>);

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

/// Inputs a matching step needs from outside the book: the clock reading and an ID source.
pub struct MatchContext<'a> {
    pub now: DateTime<Utc>,
    pub ids: &'a mut dyn IdGenerator,
}
