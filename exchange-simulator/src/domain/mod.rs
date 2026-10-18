//! Domain types: instruments, participants, orders and executions. Prices are integer instrument ticks and
//! quantities positive integers; nothing here uses floating point.

mod identifiers;
mod instrument;
mod order;

pub use identifiers::{IdentifierError, validate_cusip, validate_isin, validate_mic};
pub use instrument::{IdentifierPolicy, Instrument, InstrumentChange, InstrumentError, PriceError};
pub(crate) use order::wire_enum;
pub use order::{Execution, Order, OrderStatus, OrderType, ParseEnumError, Side};

use serde::{Deserialize, Serialize};

/// Immutable internal instrument (listing) identity; the foreign key for orders, executions and books.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InstrumentId(pub String);

/// Trading participant identity, carried on orders and on both sides of an execution.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ParticipantId(pub String);

impl std::fmt::Display for InstrumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::fmt::Display for ParticipantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A registered trading participant. No cash or position ledger in the MVP.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Participant {
    pub id: ParticipantId,
    pub name: String,
    /// Free-form settlement details (a JSON object, e.g. account, BIC, custodian), copied into every
    /// `TradeExecuted` the participant is party to.
    pub settlement: Option<serde_json::Value>,
}
