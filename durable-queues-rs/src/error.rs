use thiserror::Error;

/// This crate's result type.
pub type Result<T, E = DurableQueuesError> = std::result::Result<T, E>;

/// Errors from configuring, queueing to and consuming from durable queues.
#[derive(Debug, Error)]
pub enum DurableQueuesError {
    /// A queue table name failed validation; see [`TableName`](crate::TableName).
    #[error("invalid queue table name '{name}': {reason}")]
    InvalidTableName {
        /// The rejected name, lower-cased.
        name: String,
        /// Which rule it broke.
        reason: &'static str,
    },

    /// An externally managed table lacks columns the queue SQL needs, or has them with another type.
    #[error("table '{table}' does not match the Essentials durable queues schema: {}", problems.join("; "))]
    SchemaMismatch {
        /// The table checked.
        table: String,
        /// One line per missing or mistyped column.
        problems: Vec<String>,
    },

    /// A message cannot be queued as it is, e.g. an ordered message with a negative order.
    #[error("invalid message: {0}")]
    InvalidMessage(&'static str),

    /// A payload could not be serialized to JSON while queueing.
    #[error("failed to serialize a message: {0}")]
    Serialization(#[from] serde_json::Error),

    /// [`DurableQueues::consume_from_queue`](crate::DurableQueues::consume_from_queue) was called for a queue that
    /// already has a consumer on this instance.
    #[error("there is already a consumer for queue '{0}'")]
    ConsumerAlreadyExists(crate::types::QueueName),

    /// A [`ConsumerConfig`](crate::ConsumerConfig) or [`PollingBackoff`](crate::PollingBackoff) setting is out of
    /// range.
    #[error("invalid consumer configuration: {0}")]
    InvalidConsumerConfig(&'static str),

    /// The database operation failed.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}
