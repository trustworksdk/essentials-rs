mod entry;
mod schema;
mod sql;

use chrono::{DateTime, Utc};
use sqlx::{PgExecutor, PgPool};
use tracing::info;

use sql::QueueSql;

use crate::error::Result;
use crate::storage::{
    ClaimResult, NewEntries, QueueEntry, QueueStorage, QueueingSortOrder, STUCK_MESSAGE_ERROR,
    StuckMessageReset,
};
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
/// Settings for [`PostgresQueueStorage::new`].
pub struct PostgresQueueStorageConfig {
    /// The shared queue table; default `durable_queues`.
    pub table_name: TableName,
    /// Whether this crate creates the table or only verifies it.
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

    /// The queue table.
    pub fn table_name(&self) -> &TableName {
        &self.table_name
    }

    /// [`QueueStorage::insert_messages`] on any executor - the pool, or a caller's transaction.
    pub(crate) async fn insert_messages_with(
        &self,
        executor: impl PgExecutor<'_>,
        queue_name: &QueueName,
        entries: NewEntries,
        added_ts: DateTime<Utc>,
        next_delivery_ts: DateTime<Utc>,
    ) -> Result<u64> {
        Ok(sqlx::query(self.sql.insert_messages.clone())
            .bind(queue_name.as_str())
            .bind(added_ts)
            .bind(next_delivery_ts)
            .bind(entries.ids)
            .bind(entries.payloads)
            .bind(entries.payload_types)
            .bind(entries.meta_data)
            .bind(entries.delivery_modes)
            .bind(entries.keys)
            .bind(entries.key_orders)
            .execute(executor)
            .await?
            .rows_affected())
    }
}

impl QueueStorage for PostgresQueueStorage {
    #[tracing::instrument(name = "durable_queues.insert", level = "debug", skip_all, fields(table = %self.table_name, queue_name = %queue_name, count = entries.ids.len()))]
    async fn insert_messages(
        &self,
        queue_name: &QueueName,
        entries: NewEntries,
        added_ts: DateTime<Utc>,
        next_delivery_ts: DateTime<Utc>,
    ) -> Result<u64> {
        self.insert_messages_with(&self.pool, queue_name, entries, added_ts, next_delivery_ts)
            .await
    }

    #[tracing::instrument(name = "durable_queues.claim", level = "debug", skip_all, fields(table = %self.table_name, queue_name = %queue_name, limit, reset_stuck = reset_stuck.is_some(), excluded_keys = exclude_keys.len(), ordered))]
    async fn claim(
        &self,
        queue_name: &QueueName,
        limit: i64,
        now: DateTime<Utc>,
        reset_stuck: Option<StuckMessageReset>,
        exclude_keys: &[String],
    ) -> Result<ClaimResult> {
        let mut tx = self.pool.begin().await?;
        let reset = match reset_stuck {
            Some(reset) => sqlx::query(self.sql.reset_stuck_messages.clone())
                .bind(now)
                .bind(reset.claimed_before)
                .bind(STUCK_MESSAGE_ERROR)
                .bind(vec![queue_name.as_str()])
                .execute(&mut *tx)
                .await?
                .rows_affected(),
            None => 0,
        };
        let mut claimed: Vec<QueueEntry> = sqlx::query_as(self.sql.claim_ordered.clone())
            .bind(queue_name.as_str())
            .bind(now)
            .bind(limit)
            .bind(exclude_keys)
            .fetch_all(&mut *tx)
            .await?;
        tracing::Span::current().record("ordered", claimed.len());
        // Java's fetch: normal messages only when no ordered message could be claimed.
        if claimed.is_empty() {
            claimed = sqlx::query_as(self.sql.claim_unordered.clone())
                .bind(queue_name.as_str())
                .bind(now)
                .bind(limit)
                .fetch_all(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(ClaimResult { claimed, reset })
    }

    #[tracing::instrument(name = "durable_queues.has_ordered_message_queued_for_key", level = "debug", skip_all, fields(table = %self.table_name, queue_name = %queue_name))]
    async fn has_ordered_message_queued_for_key(
        &self,
        queue_name: &QueueName,
        key: &str,
    ) -> Result<bool> {
        let count: i64 = sqlx::query_scalar(self.sql.has_ordered_message_for_key.clone())
            .bind(queue_name.as_str())
            .bind(key)
            .fetch_one(&self.pool)
            .await?;
        Ok(count > 0)
    }

    /// Returns a failed message to the queue for redelivery at `next_delivery_ts`. `false` if the row is gone.
    #[tracing::instrument(name = "durable_queues.retry", level = "debug", skip_all, fields(table = %self.table_name, queue_entry_id = %id))]
    async fn retry(
        &self,
        id: &QueueEntryId,
        next_delivery_ts: DateTime<Utc>,
        error: &str,
    ) -> Result<bool> {
        Ok(sqlx::query(self.sql.retry.clone())
            .bind(id.as_str())
            .bind(next_delivery_ts)
            .bind(error)
            .execute(&self.pool)
            .await?
            .rows_affected()
            > 0)
    }

    /// Dead-letters a message. `None` if it does not exist or already is a dead letter.
    #[tracing::instrument(name = "durable_queues.mark_as_dead_letter", level = "debug", skip_all, fields(table = %self.table_name, queue_entry_id = %id))]
    async fn mark_as_dead_letter(
        &self,
        id: &QueueEntryId,
        reason: &str,
    ) -> Result<Option<QueueEntry>> {
        Ok(sqlx::query_as(self.sql.mark_as_dead_letter.clone())
            .bind(id.as_str())
            .bind(reason)
            .fetch_optional(&self.pool)
            .await?)
    }

    /// Makes a dead letter deliverable again at `next_delivery_ts`. `None` if it is not a dead letter.
    #[tracing::instrument(name = "durable_queues.resurrect_dead_letter", level = "debug", skip_all, fields(table = %self.table_name, queue_entry_id = %id))]
    async fn resurrect_dead_letter(
        &self,
        id: &QueueEntryId,
        next_delivery_ts: DateTime<Utc>,
    ) -> Result<Option<QueueEntry>> {
        Ok(sqlx::query_as(self.sql.resurrect_dead_letter.clone())
            .bind(id.as_str())
            .bind(next_delivery_ts)
            .fetch_optional(&self.pool)
            .await?)
    }

    #[tracing::instrument(name = "durable_queues.dead_letters", level = "debug", skip_all, fields(table = %self.table_name, queue_name = %queue_name, offset, limit))]
    async fn dead_letters(
        &self,
        queue_name: &QueueName,
        order: QueueingSortOrder,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<QueueEntry>> {
        let sql = match order {
            QueueingSortOrder::Ascending => &self.sql.dead_letters_ascending,
            QueueingSortOrder::Descending => &self.sql.dead_letters_descending,
        };
        Ok(sqlx::query_as(sql.clone())
            .bind(queue_name.as_str())
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?)
    }

    /// Deletes a handled message. Like Java, a message that was dead-lettered while being handled also counts as
    /// acknowledged; `false` means the row was already gone.
    #[tracing::instrument(name = "durable_queues.acknowledge", level = "debug", skip_all, fields(table = %self.table_name, queue_entry_id = %id))]
    async fn acknowledge(&self, id: &QueueEntryId) -> Result<bool> {
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
    #[tracing::instrument(name = "durable_queues.mark_as_dead_letter_direct", level = "debug", skip_all, fields(table = %self.table_name, queue_entry_id = %id))]
    async fn mark_as_dead_letter_direct(&self, id: &QueueEntryId, error: &str) -> Result<bool> {
        Ok(sqlx::query(self.sql.mark_as_dead_letter_direct.clone())
            .bind(id.as_str())
            .bind(error)
            .execute(&self.pool)
            .await?
            .rows_affected()
            > 0)
    }

    /// Loads one row by id, whatever its state.
    #[tracing::instrument(name = "durable_queues.find_entry", level = "debug", skip_all, fields(table = %self.table_name, queue_entry_id = %id))]
    async fn find_entry(&self, id: &QueueEntryId) -> Result<Option<QueueEntry>> {
        Ok(sqlx::query_as(self.sql.select_entry_by_id.clone())
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await?)
    }
}
