//! SQLite access: connection setup, schema migrations and the [`Store`] repository.

mod store;

pub use store::{ExecutionFilter, InstrumentFilter, OrderRecord, Store};

use std::path::Path;
use std::time::Duration;

use rusqlite::Connection;

/// Ordered schema migrations; the index + 1 is the version recorded in `PRAGMA user_version`.
const MIGRATIONS: &[&str] = &[
    include_str!("../../migrations/001_initial.sql"),
    include_str!("../../migrations/002_webhook_signing.sql"),
    include_str!("../../migrations/003_ordered_delivery.sql"),
    include_str!("../../migrations/004_instrument_enabled.sql"),
];

/// Opens (creating if needed) the database at `path`, applies connection pragmas and runs pending migrations.
pub fn open(path: impl AsRef<Path>, busy_timeout: Duration) -> rusqlite::Result<Connection> {
    let path = path.as_ref();
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|e| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
                Some(e.to_string()),
            )
        })?;
    }
    let mut conn = Connection::open(path)?;
    configure(&conn, busy_timeout)?;
    let mode: String =
        conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        tracing::warn!(journal_mode = %mode, "SQLite did not switch to WAL");
    }
    migrate(&mut conn)?;
    Ok(conn)
}

/// Opens an extra read-only connection to a database [`open`] already created. In WAL mode readers see every committed
/// transaction without blocking the writer.
pub fn open_reader(path: impl AsRef<Path>, busy_timeout: Duration) -> rusqlite::Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    configure(&conn, busy_timeout)?;
    Ok(conn)
}

/// Per-connection settings. SQLite does not persist these, so every connection must apply them.
pub fn configure(conn: &Connection, busy_timeout: Duration) -> rusqlite::Result<()> {
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.busy_timeout(busy_timeout)
}

/// Applies every migration newer than the database's `user_version`, each in its own transaction.
pub fn migrate(conn: &mut Connection) -> rusqlite::Result<()> {
    let current = schema_version(conn)?;
    for (version, sql) in (1_u32..).zip(MIGRATIONS).skip(current as usize) {
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", version)?;
        tx.commit()?;
        tracing::info!(version, "applied schema migration");
    }
    Ok(())
}

/// The schema version recorded in the database.
pub fn schema_version(conn: &Connection) -> rusqlite::Result<u32> {
    conn.pragma_query_value(None, "user_version", |row| row.get(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        configure(&conn, Duration::from_secs(1)).unwrap();
        migrate(&mut conn).unwrap();
        conn
    }

    #[test]
    fn migrations_create_schema_and_are_idempotent() {
        let mut conn = memory();
        assert_eq!(schema_version(&conn).unwrap() as usize, MIGRATIONS.len());
        migrate(&mut conn).unwrap();
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            tables,
            [
                "events",
                "executions",
                "instruments",
                "orders",
                "participants",
                "webhook_deliveries",
                "webhook_subscriptions"
            ]
        );
    }

    #[test]
    fn schema_enforces_foreign_keys_and_checks() {
        let conn = memory();
        conn.execute_batch(
            "INSERT INTO instruments (id, symbol, mic, isin, cusip, currency, currency_scale, tick_size_minor, lot_size) VALUES ('ACME', 'ACME', 'XCSE', NULL, NULL, 'DKK', 2, 1, 1);
             INSERT INTO participants VALUES ('A', 'Participant A');",
        )
        .unwrap();
        let insert_order = |id: &str,
                            participant: &str,
                            order_type: &str,
                            price: Option<i64>,
                            sequence: i64| {
            conn.execute(
                "INSERT INTO orders (id, instrument_id, participant_id, side, order_type, limit_price_ticks, quantity,
                     remaining_quantity, sequence, status, created_at, updated_at)
                 VALUES (?1, 'ACME', ?2, 'BUY', ?3, ?4, 10, 10, ?5, 'OPEN', 'now', 'now')",
                rusqlite::params![id, participant, order_type, price, sequence],
            )
        };
        insert_order("o1", "A", "LIMIT", Some(10025), 1).unwrap();
        assert!(
            insert_order("o2", "nobody", "LIMIT", Some(10025), 2).is_err(),
            "unknown participant"
        );
        assert!(
            insert_order("o3", "A", "MARKET", Some(10025), 3).is_err(),
            "market order with a price"
        );
        assert!(
            insert_order("o4", "A", "LIMIT", None, 4).is_err(),
            "limit order without a price"
        );
        assert!(
            insert_order("o5", "A", "LIMIT", Some(10025), 1).is_err(),
            "duplicate sequence"
        );
        assert!(
            conn.execute("INSERT INTO instruments (id, symbol, mic, isin, cusip, currency, currency_scale, tick_size_minor, lot_size) VALUES ('ACME2', 'ACME', 'XCSE', NULL, NULL, 'DKK', 2, 1, 1)", [])
                .is_err(),
            "duplicate (mic, symbol)"
        );
        conn.execute(
            "INSERT INTO instruments (id, symbol, mic, isin, cusip, currency, currency_scale, tick_size_minor, lot_size) VALUES ('ACME3', 'ACME', 'XSTO', NULL, NULL, 'DKK', 2, 1, 1)",
            [],
        )
        .expect("same symbol on another market");
    }

    #[test]
    fn upgrading_from_version_1_keeps_existing_subscriptions_unsigned() {
        let mut conn = Connection::open_in_memory().unwrap();
        configure(&conn, Duration::from_secs(1)).unwrap();
        conn.execute_batch(MIGRATIONS[0]).unwrap();
        conn.pragma_update(None, "user_version", 1).unwrap();
        conn.execute(
            "INSERT INTO webhook_subscriptions (id, url, event_types, created_at)
             VALUES ('s1', 'http://consumer', '[\"TradeExecuted\"]', 'now')",
            [],
        )
        .unwrap();

        migrate(&mut conn).unwrap();
        assert_eq!(schema_version(&conn).unwrap() as usize, MIGRATIONS.len());
        let secret: Option<String> = conn
            .query_row(
                "SELECT secret FROM webhook_subscriptions WHERE id = 's1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(secret, None);
        assert!(
            conn.execute("UPDATE webhook_subscriptions SET secret = 'short'", [])
                .is_err(),
            "secrets are 16 to 256 characters"
        );
    }

    #[test]
    fn open_creates_file_database_in_wal_mode() {
        let dir = std::env::temp_dir().join(format!("exchange-sim-{}", uuid::Uuid::now_v7()));
        let path = dir.join("nested").join("exchange.db");
        let conn = open(&path, Duration::from_secs(1)).unwrap();
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        drop(conn);
        let conn = open(&path, Duration::from_secs(1)).unwrap();
        assert_eq!(schema_version(&conn).unwrap() as usize, MIGRATIONS.len());
        drop(conn);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
