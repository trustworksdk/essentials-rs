//! Rust port of the Trustworks Essentials PostgreSQL Durable Queues, schema- and behaviour-compatible with
//! Essentials 0.60.0. `docs/CONTRACT.md` is the behaviour this crate reproduces.

mod error;
mod storage;
mod table_name;
mod types;

pub use error::{DurableQueuesError, Result};
pub use storage::postgres::{
    PostgresQueueStorage, PostgresQueueStorageConfig, QueueEntry, SchemaOwnership,
};
pub use table_name::{MAX_IDENTIFIER_LENGTH, TableName};
pub use types::{DeliveryMode, QueueEntryId, QueueName, UnknownDeliveryMode};
