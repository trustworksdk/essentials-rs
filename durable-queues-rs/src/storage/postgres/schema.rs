use std::collections::HashMap;

use sqlx::PgPool;

use super::sql::QueueSql;
use crate::error::{DurableQueuesError, Result};
use crate::table_name::TableName;

/// Java's `PostgresqlUtil.ESSENTIALS_BOOTSTRAP_LOCK_KEY` (a signed `long`). Taking the same transaction-scoped
/// advisory lock serialises schema creation between Java and Rust nodes starting at the same time.
const ESSENTIALS_BOOTSTRAP_LOCK_KEY: i64 = 0xE55E_4711_B007_DD15_u64 as i64;

/// Columns the queue SQL relies on, with their `information_schema.columns.data_type`.
const EXPECTED_COLUMNS: [(&str, &str); 16] = [
    ("id", "text"),
    ("queue_name", "text"),
    ("message_payload", "jsonb"),
    ("message_payload_type", "text"),
    ("added_ts", "timestamp with time zone"),
    ("next_delivery_ts", "timestamp with time zone"),
    ("delivery_ts", "timestamp with time zone"),
    ("total_attempts", "integer"),
    ("redelivery_attempts", "integer"),
    ("last_delivery_error", "text"),
    ("is_being_delivered", "boolean"),
    ("is_dead_letter_message", "boolean"),
    ("meta_data", "jsonb"),
    ("delivery_mode", "text"),
    ("key", "text"),
    ("key_order", "bigint"),
];

/// Creates the table and its indexes if they do not exist - Java's `queue-table` and `queue-indexes` schema changes.
/// Java's legacy index drops, statistics removal and schema-history ledger are deliberately not reproduced:
/// they are Java's migration path, and this crate never drops or records anything.
pub(crate) async fn create_schema(pool: &PgPool, sql: &QueueSql) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(ESSENTIALS_BOOTSTRAP_LOCK_KEY)
        .execute(&mut *tx)
        .await?;
    for statement in [
        &sql.create_table,
        &sql.create_ordered_msg_index,
        &sql.create_unordered_ready_index,
        &sql.create_ordered_head_index,
    ] {
        sqlx::query(statement.clone()).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Checks that an externally managed table has every column the queue SQL uses, with the expected type.
/// Indexes are not checked: they affect performance, not correctness.
pub(crate) async fn verify_schema(pool: &PgPool, table: &TableName) -> Result<()> {
    let actual: HashMap<String, String> = sqlx::query_as(
        "SELECT column_name::text, data_type::text FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = $1",
    )
    .bind(table.as_str())
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();

    let problems: Vec<String> = if actual.is_empty() {
        vec!["table does not exist".to_owned()]
    } else {
        EXPECTED_COLUMNS
            .iter()
            .filter_map(|&(column, expected)| match actual.get(column) {
                None => Some(format!("missing column '{column}'")),
                Some(found) if found != expected => Some(format!(
                    "column '{column}' is '{found}', expected '{expected}'"
                )),
                Some(_) => None,
            })
            .collect()
    };

    if problems.is_empty() {
        Ok(())
    } else {
        Err(DurableQueuesError::SchemaMismatch {
            table: table.to_string(),
            problems,
        })
    }
}
