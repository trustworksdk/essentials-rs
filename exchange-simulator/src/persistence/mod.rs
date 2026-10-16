//! SQLite access: connection setup, schema migrations and the [`Store`] repository.

mod checkpoint;
mod store;

pub use store::{
    AttemptRow, DeliveryRecord, DeliveryStats, ExecutionFilter, InstrumentFilter, OrderRecord,
    Outbox, SettlementWrite, Store,
};

use std::path::Path;
use std::time::Duration;

use rusqlite::Connection;

/// Ordered schema migrations; the index + 1 is the version recorded in `PRAGMA user_version`.
const MIGRATIONS: &[&str] = &[
    include_str!("../../migrations/001_initial.sql"),
    include_str!("../../migrations/002_webhook_signing.sql"),
    include_str!("../../migrations/003_ordered_delivery.sql"),
    include_str!("../../migrations/004_instrument_enabled.sql"),
    include_str!("../../migrations/005_participant_settlement.sql"),
    include_str!("../../migrations/006_virtual_clock.sql"),
    include_str!("../../migrations/007_settlement.sql"),
    include_str!("../../migrations/008_settlement_reports.sql"),
    include_str!("../../migrations/009_delivery_faults.sql"),
    include_str!("../../migrations/010_named_subscriptions.sql"),
    include_str!("../../migrations/011_webhook_maintenance.sql"),
];

/// How much of the exchange's state survives what (`database.mode`).
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceMode {
    /// WAL with `synchronous=FULL`: every acknowledged command is on disk, and survives a host crash or power loss.
    #[default]
    Durable,
    /// WAL with `synchronous=NORMAL`: commits reach the database file but are synced only at checkpoints. Everything
    /// survives a process or container restart; a host crash or power loss can lose the last commits, but never
    /// corrupts the database.
    Fast,
    /// An in-memory database (SQLite's `memdb`): nothing survives the process.
    Memory,
}

impl PersistenceMode {
    pub const ALL: [Self; 3] = [Self::Durable, Self::Fast, Self::Memory];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Fast => "fast",
            Self::Memory => "memory",
        }
    }
}

/// A fresh, process-wide name for an in-memory database. Every connection opened with it (by [`open_mode`] and
/// [`open_reader`]) shares the database, which disappears when the last of them closes.
pub fn memory_location() -> String {
    format!(
        "file:/exchange-simulator-{}?vfs=memdb",
        uuid::Uuid::now_v7()
    )
}

/// Opens (creating if needed) the database at `location` for `mode`, applies connection pragmas and runs pending
/// migrations. `location` is a file path, or with [`PersistenceMode::Memory`] a name from [`memory_location`].
/// `memory_max_bytes` caps an in-memory database (SQLite's own default is 1 GiB).
pub fn open_mode(
    location: &str,
    mode: PersistenceMode,
    busy_timeout: Duration,
    memory_max_bytes: u64,
) -> rusqlite::Result<Connection> {
    match mode {
        PersistenceMode::Durable => open(location, busy_timeout),
        PersistenceMode::Fast => {
            let conn = open(location, busy_timeout)?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            Ok(conn)
        }
        PersistenceMode::Memory => {
            let mut conn = Connection::open_with_flags(
                location,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                    | rusqlite::OpenFlags::SQLITE_OPEN_URI
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            configure(&conn, busy_timeout)?;
            // There is no disk to sync. memdb keeps its rollback journal in memory and has no WAL.
            conn.pragma_update(None, "synchronous", "OFF")?;
            set_memory_limit(&conn, memory_max_bytes)?;
            migrate(&mut conn)?;
            Ok(conn)
        }
    }
}

/// Raises (or lowers) the size limit of the in-memory database `conn` uses.
fn set_memory_limit(conn: &Connection, max_bytes: u64) -> rusqlite::Result<()> {
    let mut limit = i64::try_from(max_bytes).unwrap_or(i64::MAX);
    // SAFETY: `conn.handle()` is the live connection, `c"main"` a NUL-terminated schema name, and for
    // SQLITE_FCNTL_SIZE_LIMIT the argument must point to an `sqlite3_int64`, which `limit` is; SQLite only reads and
    // writes it during the call. rusqlite has no safe wrapper for this file control.
    let rc = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            conn.handle(),
            c"main".as_ptr(),
            rusqlite::ffi::SQLITE_FCNTL_SIZE_LIMIT,
            (&raw mut limit).cast(),
        )
    };
    if rc != rusqlite::ffi::SQLITE_OK {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rc),
            Some("setting the in-memory database size limit".into()),
        ));
    }
    Ok(())
}

/// Opens (creating if needed) the database file at `path` for durable use (WAL, `synchronous=FULL`), applies
/// connection pragmas and runs pending migrations.
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
    conn.pragma_update(None, "synchronous", "FULL")?;
    migrate(&mut conn)?;
    Ok(conn)
}

/// Opens an extra read-only connection to a database [`open`] or [`open_mode`] already created. In WAL mode readers see
/// every committed transaction without blocking the writer. An in-memory database has no WAL: a reader there waits
/// while a write transaction is open, and the writer waits for a read to finish (up to the busy timeout).
pub fn open_reader(path: impl AsRef<Path>, busy_timeout: Duration) -> rusqlite::Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_URI
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
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
                "consumer_offsets",
                "events",
                "executions",
                "instruments",
                "orders",
                "participants",
                "settlement_instructions",
                "settlement_reports",
                "virtual_clock",
                "webhook_attempts",
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
             INSERT INTO participants (id, name) VALUES ('A', 'Participant A');",
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
        assert!(
            conn.execute(
                "INSERT INTO participants (id, name, settlement) VALUES ('B', 'B', '[1]')",
                []
            )
            .is_err(),
            "settlement must be a JSON object"
        );
        conn.execute(
            "INSERT INTO participants (id, name, settlement) VALUES ('C', 'C', '{\"bic\": \"X\"}')",
            [],
        )
        .expect("a JSON object is accepted");
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
