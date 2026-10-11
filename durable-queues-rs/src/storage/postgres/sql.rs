use sqlx::{AssertSqlSafe, SqlSafeStr, SqlStr};

use crate::table_name::TableName;

/// The statements of Java's `DurableQueuesSql` (Essentials 0.60.0) bound to one table. Text is kept as close to
/// the Java source as possible so the two can be diffed; see CONTRACT.md for each statement's role.
#[derive(Debug, Clone)]
pub(crate) struct QueueSql {
    pub create_table: SqlStr,
    pub create_ordered_msg_index: SqlStr,
    pub create_unordered_ready_index: SqlStr,
    pub create_ordered_head_index: SqlStr,
    pub select_entry_by_id: SqlStr,
}

impl QueueSql {
    pub fn new(table: &TableName) -> Self {
        // `table` is the only value spliced into these statements and `TableName` validated it - that is the audit
        // `AssertSqlSafe` asks for. Everything else is a bind parameter.
        let bind = |template: &str| {
            AssertSqlSafe(template.replace("{table}", table.as_str())).into_sql_str()
        };
        Self {
            create_table: bind(
                "CREATE TABLE IF NOT EXISTS {table} (
                id                     TEXT PRIMARY KEY,
                queue_name             TEXT NOT NULL,
                message_payload        JSONB NOT NULL,
                message_payload_type   TEXT NOT NULL,
                added_ts               TIMESTAMPTZ NOT NULL,
                next_delivery_ts       TIMESTAMPTZ,
                delivery_ts            TIMESTAMPTZ DEFAULT NULL,
                total_attempts         INTEGER DEFAULT 0,
                redelivery_attempts    INTEGER DEFAULT 0,
                last_delivery_error    TEXT DEFAULT NULL,
                is_being_delivered     BOOLEAN DEFAULT FALSE,
                is_dead_letter_message BOOLEAN NOT NULL DEFAULT FALSE,
                meta_data              JSONB DEFAULT NULL,
                delivery_mode          TEXT NOT NULL,
                key                    TEXT DEFAULT NULL,
                key_order              BIGINT DEFAULT -1
                )",
            ),
            create_ordered_msg_index: bind(
                "CREATE INDEX IF NOT EXISTS idx_{table}_ordered_msg ON {table} (queue_name, key, key_order)",
            ),
            create_unordered_ready_index: bind(
                "CREATE INDEX IF NOT EXISTS idx_{table}_unordered_ready
                  ON {table} (queue_name, next_delivery_ts)
                  INCLUDE (id)
                  WHERE key IS NULL
                    AND NOT is_dead_letter_message
                    AND NOT is_being_delivered",
            ),
            create_ordered_head_index: bind(
                "CREATE INDEX IF NOT EXISTS idx_{table}_ordered_head
                  ON {table} (queue_name, key_order, next_delivery_ts)
                  INCLUDE (id)
                  WHERE key IS NOT NULL
                    AND is_dead_letter_message = FALSE
                    AND is_being_delivered     = FALSE",
            ),
            select_entry_by_id: bind("SELECT * FROM {table} WHERE id = $1"),
        }
    }
}
