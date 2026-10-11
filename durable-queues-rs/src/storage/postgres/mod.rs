mod entry;
mod schema;
mod sql;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgExecutor, PgPool};
use tracing::info;

pub use entry::QueueEntry;
use sql::QueueSql;

use crate::error::Result;
use crate::table_name::TableName;
use crate::types::{QueueEntryId, QueueName};

/// Who creates the queue table - Java's `SchemaOwnership`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SchemaOwnership {
    /// This crate creates the table and indexes if missing (Java's `COMPONENT`, the default).
    #[default]
    Component,
    /// Someone else owns the schema - a Java service, a migration tool. The table is verified, never changed.
    External,
}

#[derive(Debug, Clone, Default)]
pub struct PostgresQueueStorageConfig {
    pub table_name: TableName,
    pub schema_ownership: SchemaOwnership,
}

/// The PostgreSQL queue table, schema-compatible with Essentials 0.60.0 `PostgresqlDurableQueues`.
#[derive(Debug, Clone)]
pub struct PostgresQueueStorage {
    pool: PgPool,
    table_name: TableName,
    sql: QueueSql,
}

impl PostgresQueueStorage {
    /// Connects the storage to its table, creating or verifying the schema according to `config.schema_ownership`.
    pub async fn new(pool: PgPool, config: PostgresQueueStorageConfig) -> Result<Self> {
        let sql = QueueSql::new(&config.table_name);
        match config.schema_ownership {
            SchemaOwnership::Component => {
                schema::create_schema(&pool, &sql).await?;
                info!(table = %config.table_name, "ensured durable queues table exists");
            }
            SchemaOwnership::External => {
                schema::verify_schema(&pool, &config.table_name).await?;
                info!(table = %config.table_name, "verified externally managed durable queues table");
            }
        }
        Ok(Self {
            pool,
            table_name: config.table_name,
            sql,
        })
    }

    pub fn table_name(&self) -> &TableName {
        &self.table_name
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Inserts ready-to-deliver normal messages in one statement. Returns the number of rows inserted.
    pub(crate) async fn insert_messages(
        &self,
        executor: impl PgExecutor<'_>,
        queue_name: &QueueName,
        entries: NewEntries,
        now: DateTime<Utc>,
    ) -> Result<u64> {
        Ok(sqlx::query(self.sql.insert_messages.clone())
            .bind(queue_name.as_str())
            .bind(now)
            .bind(now)
            .bind(entries.ids)
            .bind(entries.payloads)
            .bind(entries.payload_types)
            .bind(entries.meta_data)
            .execute(executor)
            .await?
            .rows_affected())
    }

    /// Claims up to `limit` ready normal messages (CONTRACT.md §4.1), in its own transaction.
    pub(crate) async fn claim_unordered(
        &self,
        queue_name: &QueueName,
        limit: i64,
        now: DateTime<Utc>,
    ) -> Result<Vec<QueueEntry>> {
        Ok(sqlx::query_as(self.sql.claim_unordered.clone())
            .bind(queue_name.as_str())
            .bind(now)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?)
    }

    /// Deletes a handled message. Like Java, a message that was dead-lettered while being handled also counts as
    /// acknowledged; `false` means the row was already gone.
    pub(crate) async fn acknowledge(&self, id: &QueueEntryId) -> Result<bool> {
        let deleted = sqlx::query(self.sql.acknowledge.clone())
            .bind(id.as_str())
            .execute(&self.pool)
            .await?
            .rows_affected();
        if deleted == 1 {
            return Ok(true);
        }
        Ok(sqlx::query_scalar(self.sql.is_dead_letter.clone())
            .bind(id.as_str())
            .fetch_one(&self.pool)
            .await?)
    }

    /// Dead-letters a message without reading it back - for payloads that cannot be decoded.
    pub(crate) async fn mark_as_dead_letter_direct(
        &self,
        id: &QueueEntryId,
        error: &str,
    ) -> Result<bool> {
        Ok(sqlx::query(self.sql.mark_as_dead_letter_direct.clone())
            .bind(id.as_str())
            .bind(error)
            .execute(&self.pool)
            .await?
            .rows_affected()
            > 0)
    }

    /// Loads one row by id, whatever its state.
    pub async fn find_entry(&self, id: &QueueEntryId) -> Result<Option<QueueEntry>> {
        Ok(sqlx::query_as(self.sql.select_entry_by_id.clone())
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await?)
    }
}

/// Column arrays for [`PostgresQueueStorage::insert_messages`], one element per message.
#[derive(Debug, Default)]
pub(crate) struct NewEntries {
    pub ids: Vec<String>,
    pub payloads: Vec<Value>,
    pub payload_types: Vec<String>,
    pub meta_data: Vec<Value>,
}
