//! Persistence modes (step 4.4): `durable`, `fast` and `memory`, what each survives, and how the service reports
//! which one it runs in.

mod common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::TestApp;
use exchange_simulator::config::{ClockMode, PersistenceMode, SettlementMode};
use exchange_simulator::persistence::{self, open_mode, open_reader};
use serde_json::json;
use tower::ServiceExt;

const GIB: u64 = 1 << 30;

fn pragma(conn: &rusqlite::Connection, name: &str) -> String {
    conn.query_row(&format!("PRAGMA {name}"), [], |row| {
        row.get::<_, rusqlite::types::Value>(0)
    })
    .map(|value| match value {
        rusqlite::types::Value::Integer(i) => i.to_string(),
        rusqlite::types::Value::Text(t) => t,
        other => format!("{other:?}"),
    })
    .unwrap()
}

#[test]
fn each_mode_opens_the_database_it_promises() {
    let dir = std::env::temp_dir().join(format!("exchange-modes-{}", uuid::Uuid::now_v7()));
    let file = |name: &str| dir.join(name).to_string_lossy().into_owned();
    let durable = open_mode(
        &file("durable.db"),
        PersistenceMode::Durable,
        Duration::from_secs(1),
        GIB,
    )
    .unwrap();
    assert_eq!(
        (
            pragma(&durable, "journal_mode"),
            pragma(&durable, "synchronous")
        ),
        ("wal".into(), "2".into()),
        "durable: WAL, synchronous=FULL"
    );
    let fast = open_mode(
        &file("fast.db"),
        PersistenceMode::Fast,
        Duration::from_secs(1),
        GIB,
    )
    .unwrap();
    assert_eq!(
        (pragma(&fast, "journal_mode"), pragma(&fast, "synchronous")),
        ("wal".into(), "1".into()),
        "fast: WAL, synchronous=NORMAL"
    );

    let location = persistence::memory_location();
    let memory = open_mode(
        &location,
        PersistenceMode::Memory,
        Duration::from_secs(1),
        GIB,
    )
    .unwrap();
    assert_eq!(
        (
            pragma(&memory, "journal_mode"),
            pragma(&memory, "synchronous")
        ),
        ("memory".into(), "0".into()),
        "memory: no file, no syncing"
    );
    memory
        .execute("INSERT INTO participants (id, name) VALUES ('A', 'A')", [])
        .unwrap();
    let reader = open_reader(&location, Duration::from_secs(1)).unwrap();
    let count = |conn: &rusqlite::Connection| -> i64 {
        conn.query_row("SELECT COUNT(*) FROM participants", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(count(&reader), 1, "a reader shares the in-memory database");
    assert!(
        reader
            .execute("INSERT INTO participants (id, name) VALUES ('B', 'B')", [])
            .is_err(),
        "the reader is read-only"
    );
    drop(reader);
    drop(memory);
    let reopened = open_mode(
        &location,
        PersistenceMode::Memory,
        Duration::from_secs(1),
        GIB,
    )
    .unwrap();
    assert_eq!(count(&reopened), 0, "nothing survives the last connection");
    reopened
        .execute("INSERT INTO participants (id, name) VALUES ('C', 'C')", [])
        .unwrap();
    let other = open_mode(
        &persistence::memory_location(),
        PersistenceMode::Memory,
        Duration::from_secs(1),
        GIB,
    )
    .unwrap();
    assert_eq!(
        count(&other),
        0,
        "each memory location is a database of its own"
    );
    assert!(
        std::fs::read_dir(&dir).unwrap().all(|entry| {
            let name = entry.unwrap().file_name();
            let name = name.to_string_lossy();
            name.starts_with("durable") || name.starts_with("fast")
        }),
        "memory mode writes no files"
    );
    drop((durable, fast));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_in_memory_database_is_capped_at_the_configured_size() {
    let small = open_mode(
        &persistence::memory_location(),
        PersistenceMode::Memory,
        Duration::from_secs(1),
        4 << 20,
    )
    .unwrap();
    small
        .execute_batch("CREATE TABLE filler (data BLOB)")
        .unwrap();
    let error = (0..10)
        .find_map(|_| {
            small
                .execute("INSERT INTO filler VALUES (zeroblob(1000000))", [])
                .err()
        })
        .expect("4 MiB fill up");
    assert_eq!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DiskFull),
        "{error}"
    );

    // SQLite's own cap is 1 GiB; the configured one replaces it.
    let large = open_mode(
        &persistence::memory_location(),
        PersistenceMode::Memory,
        Duration::from_secs(1),
        8 << 20,
    )
    .unwrap();
    large
        .execute_batch("CREATE TABLE filler (data BLOB)")
        .unwrap();
    for _ in 0..6 {
        large
            .execute("INSERT INTO filler VALUES (zeroblob(1000000))", [])
            .unwrap();
    }
}

/// The value of the persistence header on a response to `uri`, and its status.
async fn header(app: &TestApp, method: &str, uri: &str) -> (StatusCode, Option<String>) {
    let response = app
        .router()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let value = response
        .headers()
        .get("exchange-persistence")
        .map(|v| v.to_str().unwrap().to_string());
    (response.status(), value)
}

async fn trade(app: &TestApp) {
    assert_eq!(
        app.order("A", "SELL", Some(10025), 200).await.0,
        StatusCode::CREATED
    );
    let (status, body) = app.order("B", "BUY", Some(10025), 150).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

#[tokio::test]
async fn durable_responses_carry_no_persistence_header() {
    let mut app = TestApp::start().await;
    assert_eq!(header(&app, "GET", "/health").await, (StatusCode::OK, None));
    assert_eq!(app.get("/health").await.1["persistence"], "durable");
    app.stop().await;
}

#[tokio::test]
async fn fast_mode_is_reported_and_survives_a_restart() {
    let mut app = TestApp::start_with(|c| c.database.mode = PersistenceMode::Fast).await;
    app.seed().await;
    trade(&app).await;
    assert_eq!(app.get("/health").await.1["persistence"], "fast");
    for (method, uri, status) in [
        ("GET", "/health", StatusCode::OK),
        ("GET", "/api/trades", StatusCode::OK),
        (
            "GET",
            "/api/orders/00000000-0000-0000-0000-000000000000",
            StatusCode::NOT_FOUND,
        ),
        ("GET", "/test/clock", StatusCode::OK),
        ("GET", "/no-such-route", StatusCode::NOT_FOUND),
    ] {
        assert_eq!(
            header(&app, method, uri).await,
            (status, Some("fast".into())),
            "{method} {uri}"
        );
    }
    let before = (
        app.get("/api/trades").await.1,
        app.get("/api/orderbooks/ACME").await.1,
        app.get("/api/events?limit=1000").await.1,
    );
    app.restart().await;
    let after = (
        app.get("/api/trades").await.1,
        app.get("/api/orderbooks/ACME").await.1,
        app.get("/api/events?limit=1000").await.1,
    );
    assert_eq!(
        after, before,
        "trades, the book and the event log survive a restart"
    );
    assert_eq!(before.0.as_array().unwrap().len(), 1);
    app.stop().await;
}

#[tokio::test]
async fn memory_mode_trades_and_settles_while_running_and_forgets_everything_on_restart() {
    let mut app = TestApp::start_with(|c| {
        c.database.mode = PersistenceMode::Memory;
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
        c.settlement.mode = SettlementMode::Simulated;
        c.settlement.fail_probability = 0.0;
        c.settlement.delay_probability = 0.0;
    })
    .await;
    assert!(
        !std::path::Path::new(&app.config.database.path).exists(),
        "no database file"
    );
    app.seed().await;
    trade(&app).await;
    assert_eq!(app.get("/health").await.1["persistence"], "memory");
    assert_eq!(
        header(&app, "GET", "/api/trades").await,
        (StatusCode::OK, Some("memory".into()))
    );
    let (status, body) = app.post("/test/clock/advance", json!({ "days": 2 })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, settled) = app.get("/api/events?eventType=SettlementCompleted").await;
    assert_eq!(
        settled.as_array().unwrap().len(),
        1,
        "settlement runs while the process does"
    );
    assert_eq!(
        app.count("SELECT COUNT(*) FROM orders"),
        2,
        "the database is reachable through the app's location"
    );

    app.restart().await;
    assert_eq!(app.get("/api/instruments").await.1, json!([]));
    assert_eq!(app.get("/api/trades").await.1, json!([]));
    assert_eq!(app.get("/api/events").await.1, json!([]));
    assert!(!std::path::Path::new(&app.config.database.path).exists());
    app.stop().await;
}
