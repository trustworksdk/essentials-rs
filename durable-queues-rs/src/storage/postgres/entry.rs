use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::{FromRow, Row};

use crate::types::{DeliveryMode, QueueEntryId, QueueName};

/// One row of the queue table, as stored.
///
/// NULLs in the nullable counter/flag columns decode like Java's `QueuedMessageRowMapper` (JDBC `getInt`, `getLong`
/// and `getBoolean` read NULL as 0 / false). The payload and metadata stay raw JSON; turning them into typed values
/// is the consumer's job (Phase 2).
#[derive(Debug, Clone, PartialEq)]
pub struct QueueEntry {
    pub id: QueueEntryId,
    pub queue_name: QueueName,
    pub message_payload: Value,
    /// The payload's type name. For Java-produced rows: the Java FQCN.
    pub message_payload_type: String,
    pub added_ts: DateTime<Utc>,
    /// `None` while the message is being delivered and for dead letters.
    pub next_delivery_ts: Option<DateTime<Utc>>,
    pub delivery_ts: Option<DateTime<Utc>>,
    pub total_attempts: i32,
    pub redelivery_attempts: i32,
    pub last_delivery_error: Option<String>,
    pub is_being_delivered: bool,
    pub is_dead_letter_message: bool,
    pub meta_data: Option<Value>,
    pub delivery_mode: DeliveryMode,
    pub key: Option<String>,
    /// -1 for [`DeliveryMode::Normal`].
    pub key_order: i64,
}

impl<'r> FromRow<'r, PgRow> for QueueEntry {
    fn from_row(row: &'r PgRow) -> sqlx::Result<Self> {
        let delivery_mode: String = row.try_get("delivery_mode")?;
        Ok(Self {
            id: QueueEntryId::new(row.try_get::<String, _>("id")?),
            queue_name: QueueName::new(row.try_get::<String, _>("queue_name")?),
            message_payload: row.try_get("message_payload")?,
            message_payload_type: row.try_get("message_payload_type")?,
            added_ts: row.try_get("added_ts")?,
            next_delivery_ts: row.try_get("next_delivery_ts")?,
            delivery_ts: row.try_get("delivery_ts")?,
            total_attempts: row
                .try_get::<Option<i32>, _>("total_attempts")?
                .unwrap_or(0),
            redelivery_attempts: row
                .try_get::<Option<i32>, _>("redelivery_attempts")?
                .unwrap_or(0),
            last_delivery_error: row.try_get("last_delivery_error")?,
            is_being_delivered: row
                .try_get::<Option<bool>, _>("is_being_delivered")?
                .unwrap_or(false),
            is_dead_letter_message: row.try_get("is_dead_letter_message")?,
            meta_data: row.try_get("meta_data")?,
            delivery_mode: delivery_mode
                .parse()
                .map_err(|e| sqlx::Error::ColumnDecode {
                    index: "delivery_mode".to_owned(),
                    source: Box::new(e),
                })?,
            key: row.try_get("key")?,
            key_order: row.try_get::<Option<i64>, _>("key_order")?.unwrap_or(0),
        })
    }
}
