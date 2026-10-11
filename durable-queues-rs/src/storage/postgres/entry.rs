use sqlx::postgres::PgRow;
use sqlx::{FromRow, Row};

use crate::storage::QueueEntry;
use crate::types::{QueueEntryId, QueueName};

/// Decodes a row like Java's `QueuedMessageRowMapper`: NULLs in the nullable counter and flag columns read as
/// 0 / false (JDBC `getInt`, `getLong`, `getBoolean`).
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
