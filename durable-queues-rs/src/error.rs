use thiserror::Error;

pub type Result<T, E = DurableQueuesError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum DurableQueuesError {
    #[error("invalid queue table name '{name}': {reason}")]
    InvalidTableName { name: String, reason: &'static str },

    #[error("table '{table}' does not match the Essentials durable queues schema: {}", problems.join("; "))]
    SchemaMismatch {
        table: String,
        problems: Vec<String>,
    },

    #[error("failed to serialize a message: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("there is already a consumer for queue '{0}'")]
    ConsumerAlreadyExists(crate::types::QueueName),

    #[error("invalid consumer configuration: {0}")]
    InvalidConsumerConfig(&'static str),

    #[error(transparent)]
    Database(#[from] sqlx::Error),
}
