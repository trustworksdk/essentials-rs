mod entry;
mod schema;
mod sql;

use sqlx::PgPool;
use tracing::info;

pub use entry::QueueEntry;
use sql::QueueSql;

use crate::error::Result;
use crate::table_name::TableName;
use crate::types::QueueEntryId;

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

    /// Loads one row by id, whatever its state.
    pub async fn find_entry(&self, id: &QueueEntryId) -> Result<Option<QueueEntry>> {
        Ok(sqlx::query_as(self.sql.select_entry_by_id.clone())
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await?)
    }
}
