//! Rust port of the Trustworks Essentials PostgreSQL Durable Queues, schema- and behaviour-compatible with
//! Essentials 0.60.0. `docs/CONTRACT.md` is the behaviour this crate reproduces.

#![warn(missing_docs)]

mod consumer;
mod durable_queues;
mod error;
mod message;
mod polling;
mod redelivery;
mod storage;
mod table_name;
mod types;

pub use consumer::{ConsumerConfig, FetchPacing, MessageHandler, QueueConsumer};
pub use durable_queues::DurableQueues;
pub use error::{DurableQueuesError, Result};
pub use message::{
    BoxError, DecodeError, HandlerError, MANUALLY_REQUESTED_REDELIVERY, Message, MessageMetaData,
    MessageOrder, MessageType, QueueMessage, QueuedMessage,
};
pub use polling::PollingBackoff;
pub use redelivery::RedeliveryPolicy;
pub use storage::postgres::{PostgresQueueStorage, PostgresQueueStorageConfig, SchemaOwnership};
pub use storage::{
    ClaimResult, NewEntries, QueueEntry, QueueStorage, QueueingSortOrder, STUCK_MESSAGE_ERROR,
    StuckMessageReset,
};
pub use table_name::{MAX_IDENTIFIER_LENGTH, TableName};
pub use types::{DeliveryMode, QueueEntryId, QueueName, UnknownDeliveryMode};
