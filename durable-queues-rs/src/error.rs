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

    #[error(transparent)]
    Database(#[from] sqlx::Error),
}
