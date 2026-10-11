//! Rust port of the Trustworks Essentials PostgreSQL Durable Queues, schema- and behaviour-compatible with
//! Essentials 0.60.0. `docs/CONTRACT.md` is the behaviour this crate reproduces.

mod consumer;
mod durable_queues;
mod error;
mod message;
mod storage;
mod table_name;
mod types;

pub use consumer::{ConsumerConfig, MessageHandler, QueueConsumer};
pub use durable_queues::DurableQueues;
pub use error::{DurableQueuesError, Result};
pub use message::{
    BoxError, DecodeError, HandlerError, Message, MessageMetaData, MessageType, QueueMessage,
    QueuedMessage,
};
pub use storage::postgres::{
    PostgresQueueStorage, PostgresQueueStorageConfig, QueueEntry, SchemaOwnership,
};
pub use table_name::{MAX_IDENTIFIER_LENGTH, TableName};
pub use types::{DeliveryMode, QueueEntryId, QueueName, UnknownDeliveryMode};
