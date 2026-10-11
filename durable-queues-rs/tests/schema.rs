//! Phase 1 exit gate: the Rust-created schema equals Java's, and a Java-created schema (with rows) is used as-is.

mod common;

use std::sync::Arc;

use tokio::sync::Barrier;

use common::{JAVA_ROWS, JAVA_SCHEMA, TestDatabase};
use durable_queues_rs::{
    DeliveryMode, DurableQueuesError, PostgresQueueStorage, PostgresQueueStorageConfig,
    QueueEntryId, QueueStorage, SchemaOwnership, TableName,
};

fn config(ownership: SchemaOwnership) -> PostgresQueueStorageConfig {
    PostgresQueueStorageConfig {
        table_name: TableName::default(),
        schema_ownership: ownership,
    }
}

#[tokio::test]
async fn rust_created_schema_is_identical_to_the_java_created_schema() {
    let (java_db, rust_db) = tokio::join!(TestDatabase::start(), TestDatabase::start());
    java_db.run_script(JAVA_SCHEMA).await;
    PostgresQueueStorage::new(rust_db.pool.clone(), config(SchemaOwnership::Component))
        .await
        .unwrap();

    let java_schema = java_db.schema_snapshot("durable_queues").await;
    assert!(
        java_schema.contains("idx_durable_queues_ordered_head"),
        "{java_schema}"
    );
    assert_eq!(rust_db.schema_snapshot("durable_queues").await, java_schema);
}

#[tokio::test]
async fn a_java_created_schema_and_its_rows_are_left_untouched() {
    let db = TestDatabase::start().await;
    db.run_script(JAVA_SCHEMA).await;
    db.run_script(JAVA_ROWS).await;
    let schema_before = db.schema_snapshot("durable_queues").await;
    let rows_before = db.rows_digest().await;

    for ownership in [
        SchemaOwnership::Component,
        SchemaOwnership::Component,
        SchemaOwnership::External,
    ] {
        PostgresQueueStorage::new(db.pool.clone(), config(ownership))
            .await
            .unwrap();
    }

    assert_eq!(db.schema_snapshot("durable_queues").await, schema_before);
    assert_eq!(db.rows_digest().await, rows_before);
}

#[tokio::test]
async fn external_ownership_rejects_a_missing_or_incompatible_table_without_changing_anything() {
    let db = TestDatabase::start().await;

    let missing =
        PostgresQueueStorage::new(db.pool.clone(), config(SchemaOwnership::External)).await;
    assert!(
        matches!(&missing, Err(DurableQueuesError::SchemaMismatch { problems, .. }) if problems == &["table does not exist"]),
        "{missing:?}"
    );
    assert_eq!(
        db.schema_snapshot("durable_queues").await,
        "\n",
        "External must not create the table"
    );

    sqlx::raw_sql("CREATE TABLE durable_queues (id TEXT PRIMARY KEY, queue_name TEXT NOT NULL, key_order INTEGER)")
        .execute(&db.pool)
        .await
        .unwrap();
    let Err(DurableQueuesError::SchemaMismatch { problems, .. }) =
        PostgresQueueStorage::new(db.pool.clone(), config(SchemaOwnership::External)).await
    else {
        panic!("an incompatible table must be rejected");
    };
    assert!(
        problems.contains(&"missing column 'message_payload'".to_owned()),
        "{problems:?}"
    );
    assert!(
        problems.contains(&"column 'key_order' is 'integer', expected 'bigint'".to_owned()),
        "{problems:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_schema_creation_is_serialised_by_the_bootstrap_lock() {
    // Without the lock, concurrent `CREATE TABLE IF NOT EXISTS` fails with a duplicate key on `pg_type` - measured
    // at 290 of 300 initialisations. Pre-opened connections plus a barrier make every attempt race.
    const NODES: usize = 30;
    let db = TestDatabase::start().await;
    let barrier = Arc::new(Barrier::new(NODES));
    let mut initialisations = Vec::with_capacity(NODES);
    for _ in 0..NODES {
        let pool = db.dedicated_pool().await;
        let barrier = barrier.clone();
        initialisations.push(tokio::spawn(async move {
            barrier.wait().await;
            PostgresQueueStorage::new(pool, config(SchemaOwnership::Component)).await
        }));
    }

    for initialisation in initialisations {
        initialisation
            .await
            .unwrap()
            .expect("every concurrent initialisation succeeds");
    }
}

#[tokio::test]
async fn custom_table_names_are_lower_cased_and_name_the_indexes() {
    let db = TestDatabase::start().await;
    let config = PostgresQueueStorageConfig {
        table_name: TableName::new("Orders_Queue").unwrap(),
        schema_ownership: SchemaOwnership::Component,
    };

    let storage = PostgresQueueStorage::new(db.pool.clone(), config)
        .await
        .unwrap();

    assert_eq!(storage.table_name().as_str(), "orders_queue");
    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT indexname::text FROM pg_indexes WHERE tablename = 'orders_queue' AND indexname LIKE 'idx_%' ORDER BY 1",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        indexes,
        [
            "idx_orders_queue_ordered_head",
            "idx_orders_queue_ordered_msg",
            "idx_orders_queue_unordered_ready"
        ]
    );
}

#[tokio::test]
async fn java_rows_decode_like_the_java_row_mapper() {
    let db = TestDatabase::start().await;
    db.run_script(JAVA_SCHEMA).await;
    db.run_script(JAVA_ROWS).await;
    let storage = PostgresQueueStorage::new(db.pool.clone(), config(SchemaOwnership::External))
        .await
        .unwrap();
    let find = |suffix: &str| {
        let id = QueueEntryId::new(format!("2f1c7c6e-6a4b-11f1-9a2e-0242ac12{suffix}"));
        let storage = storage.clone();
        async move {
            storage
                .find_entry(&id)
                .await
                .unwrap()
                .expect("fixture row exists")
        }
    };

    let plain = find("0002").await;
    assert_eq!(plain.queue_name.as_str(), "Orders");
    assert_eq!(plain.message_payload_type, "com.acme.orders.OrderPlaced");
    assert_eq!(plain.message_payload["orderId"], "order-1");
    assert_eq!(plain.meta_data.unwrap()["correlationId"], "corr-1");
    assert_eq!(plain.delivery_mode, DeliveryMode::Normal);
    assert_eq!((plain.key, plain.key_order), (None, -1));
    assert_eq!((plain.total_attempts, plain.redelivery_attempts), (0, 0));
    assert!(!plain.is_being_delivered && !plain.is_dead_letter_message);
    assert!(plain.next_delivery_ts.is_some() && plain.delivery_ts.is_none());

    let ordered = find("0006").await;
    assert_eq!(ordered.delivery_mode, DeliveryMode::InOrder);
    assert_eq!(
        (ordered.key.as_deref(), ordered.key_order),
        (Some("order-4"), 1)
    );
    assert_eq!(
        ordered.message_payload_type,
        "com.acme.orders.OrderEvent$OrderAdded"
    );

    let dead_letter = find("0007").await;
    assert!(dead_letter.is_dead_letter_message);
    assert_eq!(dead_letter.next_delivery_ts, None);
    assert!(
        dead_letter
            .last_delivery_error
            .unwrap()
            .starts_with("java.lang.IllegalStateException: boom")
    );

    assert_eq!(
        storage.find_entry(&QueueEntryId::random()).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn null_counters_decode_as_java_defaults_and_unknown_delivery_modes_fail() {
    let db = TestDatabase::start().await;
    let storage = PostgresQueueStorage::new(db.pool.clone(), config(SchemaOwnership::Component))
        .await
        .unwrap();
    sqlx::raw_sql(
        "INSERT INTO durable_queues (id, queue_name, message_payload, message_payload_type, added_ts, delivery_mode,
                                     total_attempts, redelivery_attempts, is_being_delivered, key_order)
         VALUES ('nulls', 'q', '{}', 't', now(), 'NORMAL', NULL, NULL, NULL, NULL),
                ('bogus', 'q', '{}', 't', now(), 'BOGUS', 0, 0, FALSE, -1)",
    )
    .execute(&db.pool)
    .await
    .unwrap();

    let nulls = storage.find_entry(&"nulls".into()).await.unwrap().unwrap();
    assert_eq!(
        (
            nulls.total_attempts,
            nulls.redelivery_attempts,
            nulls.key_order
        ),
        (0, 0, 0)
    );
    assert!(!nulls.is_being_delivered);

    let bogus = storage.find_entry(&"bogus".into()).await;
    assert!(
        matches!(
            bogus,
            Err(DurableQueuesError::Database(
                sqlx::Error::ColumnDecode { .. }
            ))
        ),
        "{bogus:?}"
    );
}
