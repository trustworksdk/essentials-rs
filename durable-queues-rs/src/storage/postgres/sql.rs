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
    pub insert_messages: SqlStr,
    pub claim_unordered: SqlStr,
    pub acknowledge: SqlStr,
    pub is_dead_letter: SqlStr,
    pub mark_as_dead_letter_direct: SqlStr,
    pub mark_as_dead_letter: SqlStr,
    pub retry: SqlStr,
    pub resurrect_dead_letter: SqlStr,
    pub reset_stuck_messages: SqlStr,
    pub dead_letters_ascending: SqlStr,
    pub dead_letters_descending: SqlStr,
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
            // Java's getQueueMessageSql, one row per UNNEST element - a JDBC batch in one round trip. Normal
            // messages only: delivery_mode NORMAL, key NULL, key_order -1 (CONTRACT.md §2).
            // $1 queue_name, $2 added_ts, $3 next_delivery_ts, $4 ids, $5 payloads, $6 payload types, $7 meta_data
            insert_messages: bind(
                "INSERT INTO {table} (
                    id, queue_name, message_payload, message_payload_type, added_ts, next_delivery_ts,
                    last_delivery_error, is_dead_letter_message, meta_data, delivery_mode, key, key_order
                )
                SELECT m.id, $1, m.payload, m.payload_type, $2, $3, NULL, FALSE, m.meta_data, 'NORMAL', NULL, -1
                FROM UNNEST($4::text[], $5::jsonb[], $6::text[], $7::jsonb[]) AS m(id, payload, payload_type, meta_data)",
            ),
            // buildUnorderedSqlStatement, verbatim. $1 queue_name, $2 now, $3 limit
            claim_unordered: bind(
                "WITH cte_unordered AS (
                  SELECT id
                  FROM {table} q
                  WHERE
                       queue_name = $1
                    AND is_dead_letter_message = FALSE
                    AND is_being_delivered     = FALSE
                    AND next_delivery_ts <= $2
                    AND key IS NULL
                  ORDER BY next_delivery_ts
                  LIMIT $3
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE {table} q
                SET
                  total_attempts       = q.total_attempts + 1,
                  next_delivery_ts     = NULL,
                  is_being_delivered   = TRUE,
                  delivery_ts          = $2
                FROM cte_unordered u
                WHERE q.id = u.id
                  AND q.queue_name = $1
                RETURNING q.*",
            ),
            acknowledge: bind("DELETE FROM {table} WHERE id = $1 AND is_dead_letter_message = FALSE"),
            is_dead_letter: bind("SELECT EXISTS (SELECT 1 FROM {table} WHERE id = $1 AND is_dead_letter_message = TRUE)"),
            // getMarkAsDeadLetterMessageDirectSql, verbatim. $1 id, $2 last_delivery_error
            mark_as_dead_letter_direct: bind(
                "UPDATE {table} SET
                next_delivery_ts = NULL,
                last_delivery_error = $2,
                is_dead_letter_message = TRUE,
                is_being_delivered = FALSE,
                delivery_ts = NULL
                WHERE id = $1 AND is_dead_letter_message = FALSE",
            ),
            // getMarkAsDeadLetterMessageSql, verbatim. $1 id, $2 last_delivery_error
            mark_as_dead_letter: bind(
                "UPDATE {table} SET
                next_delivery_ts = NULL,
                last_delivery_error = $2,
                is_dead_letter_message = TRUE,
                is_being_delivered = FALSE,
                delivery_ts = NULL
                WHERE id = $1 AND is_dead_letter_message = FALSE
                RETURNING *",
            ),
            // getRetryMessageSql, verbatim. $1 id, $2 next_delivery_ts, $3 last_delivery_error
            retry: bind(
                "UPDATE {table} SET
                next_delivery_ts = $2,
                last_delivery_error = $3,
                redelivery_attempts = redelivery_attempts + 1,
                is_being_delivered = FALSE,
                delivery_ts = NULL
                WHERE id = $1
                RETURNING *",
            ),
            // getResurrectDeadLetterMessageSql, verbatim. Counters are deliberately not reset. $1 id, $2 next_delivery_ts
            resurrect_dead_letter: bind(
                "UPDATE {table} SET
                next_delivery_ts = $2,
                is_dead_letter_message = FALSE
                WHERE id = $1 AND
                is_dead_letter_message = TRUE
                RETURNING *",
            ),
            // getResetMessagesStuckBeingDeliveredAcrossMultipleQueuesSql, verbatim but for `= ANY` in place of JDBI's
            // `IN (<list>)`. The single-queue Java variant lacks the queue filter (CONTRACT.md §6) and is not used.
            // $1 now, $2 threshold, $3 error, $4 queue names
            reset_stuck_messages: bind(
                "UPDATE {table} SET
                is_being_delivered = FALSE,
                delivery_ts = NULL,
                redelivery_attempts = redelivery_attempts + 1,
                next_delivery_ts = $1,
                last_delivery_error = $3
                WHERE is_being_delivered = TRUE
                AND delivery_ts <= $2
                AND queue_name = ANY($4)",
            ),
            // Java's queryQueuedMessages(DEAD_LETTER_MESSAGES) has no ORDER BY - its sort order parameter is ignored,
            // so paging is nondeterministic. Ordering by (added_ts, id) is a read-only fix (CONTRACT.md §5).
            // $1 queue_name, $2 limit, $3 offset
            dead_letters_ascending: bind(
                "SELECT * FROM {table}
                WHERE queue_name = $1 AND is_dead_letter_message = TRUE
                ORDER BY added_ts ASC, id ASC
                LIMIT $2 OFFSET $3",
            ),
            dead_letters_descending: bind(
                "SELECT * FROM {table}
                WHERE queue_name = $1 AND is_dead_letter_message = TRUE
                ORDER BY added_ts DESC, id DESC
                LIMIT $2 OFFSET $3",
            ),
        }
    }
}
