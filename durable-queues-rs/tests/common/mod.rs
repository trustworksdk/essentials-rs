//! Shared Testcontainers fixture: one throwaway PostgreSQL per test, removed when the test's `TestDatabase` drops.
#![allow(dead_code)] // each test binary uses a different subset

use std::time::{Duration, Instant};

use durable_queues_rs::{DurableQueues, PostgresQueueStorage};

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

/// Java's DDL, verbatim - "a schema Java created".
pub const JAVA_SCHEMA: &str = include_str!("../../docs/fixtures/java_schema.sql");
/// Rows shaped as Java's `queueMessage` writes them (CONTRACT.md §13).
pub const JAVA_ROWS: &str = include_str!("../../docs/fixtures/java_rows.sql");

pub struct TestDatabase {
    pub pool: PgPool,
    _container: ContainerAsync<Postgres>,
}

impl TestDatabase {
    pub async fn start() -> Self {
        let container = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .expect("start PostgreSQL container");
        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("container port");
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(&format!(
                "postgres://postgres:postgres@{host}:{port}/postgres"
            ))
            .await
            .expect("connect to PostgreSQL container");
        Self {
            pool,
            _container: container,
        }
    }

    /// A pool with one already-open connection, standing in for a separate application node.
    pub async fn dedicated_pool(&self) -> PgPool {
        self.node_pool(1).await
    }

    /// A pool of its own with `connections` already-open connections, standing in for a separate application node.
    pub async fn node_pool(&self, connections: u32) -> PgPool {
        PgPoolOptions::new()
            .max_connections(connections)
            .min_connections(connections)
            .connect_with((*self.pool.connect_options()).clone())
            .await
            .expect("open node pool")
    }

    /// `DurableQueues` on the default table, creating it if needed.
    pub async fn durable_queues(&self, pool: PgPool) -> DurableQueues {
        DurableQueues::new(
            PostgresQueueStorage::new(pool, Default::default())
                .await
                .expect("init storage"),
        )
    }

    pub async fn count(&self, sql: &'static str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&self.pool)
            .await
            .expect("count")
    }

    /// Runs a multi-statement script.
    pub async fn run_script(&self, script: &'static str) {
        sqlx::raw_sql(script)
            .execute(&self.pool)
            .await
            .expect("run SQL script");
    }

    /// Columns (name, type, nullability, default) and index definitions of `table`, one per line.
    pub async fn schema_snapshot(&self, table: &str) -> String {
        let columns: String = sqlx::query_scalar(
            "SELECT coalesce(string_agg(format('%s %s %s %s', column_name, data_type, is_nullable,
                                               coalesce(column_default, '-')), E'\n' ORDER BY ordinal_position), '')
             FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1",
        )
        .bind(table)
        .fetch_one(&self.pool)
        .await
        .expect("read columns");
        let indexes: String = sqlx::query_scalar(
            "SELECT coalesce(string_agg(indexdef, E'\n' ORDER BY indexname), '')
             FROM pg_indexes WHERE schemaname = current_schema() AND tablename = $1",
        )
        .bind(table)
        .fetch_one(&self.pool)
        .await
        .expect("read indexes");
        format!("{columns}\n{indexes}")
    }

    /// A digest of every row in `durable_queues`.
    pub async fn rows_digest(&self) -> String {
        sqlx::query_scalar(
            "SELECT coalesce(md5(string_agg(t::text, ',' ORDER BY id)), '') FROM durable_queues t",
        )
        .fetch_one(&self.pool)
        .await
        .expect("digest rows")
    }
}

/// Polls `check` until it holds, failing the test after 30 s.
pub async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
