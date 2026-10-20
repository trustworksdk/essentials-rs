//! SQLite access: the exchange's schema migrations and the [`Store`] repository. Connection setup, persistence modes
//! and background checkpoints are shared (`simulator_kit::persistence`).

mod store;

pub use simulator_kit::outbox::{AttemptRow, DeliveryRecord, DeliveryStats, Outbox};
pub use simulator_kit::persistence::{PersistenceMode, configure, open_reader, schema_version};
pub use store::{ExecutionFilter, InstrumentFilter, OrderRecord, SettlementWrite, Store};

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
    include_str!("../../migrations/012_clearing.sql"),
    include_str!("../../migrations/013_instrument_reference_symbol.sql"),
    include_str!("../../migrations/014_price_sources.sql"),
    include_str!("../../migrations/015_webhook_participant_filter.sql"),
    include_str!("../../migrations/016_business_days.sql"),
    include_str!("../../migrations/017_liquidity.sql"),
    include_str!("../../migrations/018_trade_dates.sql"),
];

/// A fresh name for an in-memory exchange database (see [`simulator_kit::persistence::memory_location`]).
pub fn memory_location() -> String {
    simulator_kit::persistence::memory_location("exchange-simulator")
}

/// Opens the exchange database at `location` for `mode` and migrates it (see [`simulator_kit::persistence::open_mode`]).
pub fn open_mode(
    location: &str,
    mode: PersistenceMode,
    busy_timeout: Duration,
    memory_max_bytes: u64,
) -> rusqlite::Result<Connection> {
    simulator_kit::persistence::open_mode(
        location,
        mode,
        busy_timeout,
        memory_max_bytes,
        MIGRATIONS,
    )
}

/// Opens the exchange database file at `path` for durable use and migrates it.
pub fn open(path: impl AsRef<Path>, busy_timeout: Duration) -> rusqlite::Result<Connection> {
    simulator_kit::persistence::open(path, busy_timeout, MIGRATIONS)
}

/// Applies the exchange's pending migrations.
pub fn migrate(conn: &mut Connection) -> rusqlite::Result<()> {
    simulator_kit::persistence::migrate(conn, MIGRATIONS)
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
    fn the_migrations_build_the_shared_outbox_schema() {
        let exchange = memory();
        let shared = Connection::open_in_memory().unwrap();
        shared.execute_batch(simulator_kit::outbox::SCHEMA).unwrap();
        shared
            .execute_batch(simulator_kit::outbox::PARTICIPANT_FILTER)
            .unwrap();
        assert_eq!(
            simulator_kit::outbox::shared_schema(&exchange).unwrap(),
            simulator_kit::outbox::shared_schema(&shared).unwrap(),
            "the exchange's events, webhook and virtual clock tables differ from simulator_kit::outbox::SCHEMA and \
             PARTICIPANT_FILTER"
        );
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
                "business_day_cursor",
                "clearing_cursor",
                "clearing_statements",
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
