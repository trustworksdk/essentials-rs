//! Background WAL checkpointing (step 4.3): the writer never checkpoints, a background connection keeps the WAL
//! bounded, and nothing is lost.

mod common;

use std::path::PathBuf;
use std::time::Duration;

use exchange_simulator::domain::{
    Instrument, InstrumentId, OrderType, Participant, ParticipantId, Side,
};
use exchange_simulator::engine::{Exchange, ExchangeOptions, SubmitOrder};
use exchange_simulator::persistence::{self, Store};
use uuid::Uuid;

/// A database file in its own directory, removed on drop.
struct TempDb(PathBuf);

impl TempDb {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("exchange-checkpoint-{}", Uuid::now_v7())))
    }

    fn path(&self) -> PathBuf {
        self.0.join("exchange.db")
    }

    fn wal_bytes(&self) -> u64 {
        std::fs::metadata(self.0.join("exchange.db-wal")).map_or(0, |m| m.len())
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const ORDERS: u64 = 400;
const MIB: u64 = 1 << 20;
// Measured in the devcontainer (tmpfs): 400 orders leave about 0.5-1.2 MB of WAL with background checkpoints from 100
// pages, 4.2 MB with SQLite's automatic checkpoint (1000 pages), and 19 MB with no checkpoint at all. The bounds sit
// between those, so each test tells the modes apart.

enum Checkpoints {
    /// SQLite's automatic checkpoint on the writer.
    Inline,
    /// In the background, from this many pages.
    Background(u32),
}

fn open(db: &TempDb, checkpoints: &Checkpoints) -> Exchange {
    let conn = persistence::open(db.path(), Duration::from_secs(5)).unwrap();
    let mut store = Store::new(conn);
    if let Checkpoints::Background(pages) = checkpoints {
        store
            .checkpoint_in_background(Duration::from_secs(5), *pages)
            .unwrap();
    }
    Exchange::recover(store, ExchangeOptions::default()).unwrap()
}

/// Registers ACME and two participants, then submits [`ORDERS`] orders that alternately rest and trade, asking for
/// a checkpoint after each as the exchange thread does.
fn trade(exchange: &mut Exchange) {
    exchange
        .register_instrument(Instrument {
            id: InstrumentId("ACME".into()),
            symbol: "ACME".into(),
            mic: Some("XCSE".into()),
            isin: None,
            cusip: None,
            currency: "DKK".into(),
            currency_scale: 2,
            tick_size_minor: 1,
            lot_size: 1,
            enabled: true,
        })
        .unwrap();
    for id in ["A", "B"] {
        exchange
            .register_participant(Participant {
                id: ParticipantId(id.into()),
                name: id.into(),
                settlement: None,
            })
            .unwrap();
    }
    for i in 0..ORDERS {
        let (participant, side) = if i % 2 == 0 {
            ("A", Side::Sell)
        } else {
            ("B", Side::Buy)
        };
        exchange
            .submit(SubmitOrder {
                client_order_id: None,
                instrument_id: InstrumentId("ACME".into()),
                participant_id: ParticipantId(participant.into()),
                side,
                order_type: OrderType::Limit,
                limit_price_ticks: Some(10_000),
                quantity: 10,
            })
            .unwrap();
        exchange.store().request_checkpoint_if_due();
    }
}

fn count(exchange: &Exchange, sql: &str) -> i64 {
    exchange
        .store()
        .connection()
        .query_row(sql, [], |row| row.get(0))
        .unwrap()
}

#[test]
fn background_checkpoints_keep_the_wal_bounded_and_lose_nothing() {
    let handle = exchange_simulator::metrics::install();
    let db = TempDb::new();
    let mut exchange = open(&db, &Checkpoints::Background(100));
    trade(&mut exchange);
    let wal = db.wal_bytes();
    assert!(
        wal < 5 * MIB / 2,
        "the WAL stays near its 100-page threshold: {wal} bytes"
    );
    assert_eq!(
        count(&exchange, "SELECT COUNT(*) FROM orders"),
        ORDERS as i64
    );
    let metrics = handle.render();
    let complete = metrics
        .lines()
        .find_map(|l| {
            l.strip_prefix(
                r#"exchange_wal_checkpoints_total{by="background",outcome="complete"} "#,
            )?
            .parse::<f64>()
            .ok()
        })
        .unwrap_or(0.0);
    assert!(complete >= 1.0, "checkpoints ran: {metrics}");
    assert!(metrics.contains("exchange_wal_frames "), "{metrics}");

    // Dropping the exchange stops the checkpointer; everything committed is there after a restart.
    drop(exchange);
    let restarted = open(&db, &Checkpoints::Background(100));
    assert_eq!(
        count(&restarted, "SELECT COUNT(*) FROM orders"),
        ORDERS as i64
    );
    assert_eq!(
        count(&restarted, "SELECT COUNT(*) FROM executions"),
        ORDERS as i64 / 2
    );
    assert!(
        restarted
            .book(&InstrumentId("ACME".into()))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn the_writer_never_checkpoints_by_itself_in_background_mode() {
    // With a threshold that is never reached, nothing checkpoints: the WAL grows past SQLite's own 1000-page
    // auto-checkpoint, so the writer's automatic checkpoint is off.
    let db = TempDb::new();
    let mut exchange = open(&db, &Checkpoints::Background(u32::MAX));
    trade(&mut exchange);
    let wal = db.wal_bytes();
    assert!(
        wal > 8 * MIB,
        "no checkpoint ran, so the WAL kept growing: {wal} bytes"
    );
}

#[test]
fn inline_mode_keeps_sqlites_automatic_checkpoint() {
    let db = TempDb::new();
    let mut exchange = open(&db, &Checkpoints::Inline);
    trade(&mut exchange);
    let wal = db.wal_bytes();
    assert!(
        wal < 6 * MIB,
        "the writer checkpoints at 1000 pages (4 MiB): {wal} bytes"
    );
}

#[tokio::test]
async fn the_service_checkpoints_in_the_background_by_default() {
    let mut app =
        common::TestApp::start_with(|config| config.database.checkpoint_pages = 100).await;
    app.seed().await;
    for i in 0..ORDERS {
        let (participant, side) = if i % 2 == 0 {
            ("A", "SELL")
        } else {
            ("B", "BUY")
        };
        let (status, body) = app.order(participant, side, Some(10_000), 10).await;
        assert_eq!(status, axum::http::StatusCode::CREATED, "{body}");
    }
    let wal = std::fs::metadata(format!("{}-wal", app.config.database.path)).map_or(0, |m| m.len());
    assert!(
        wal < 5 * MIB / 2,
        "the exchange thread asked for checkpoints, so the WAL stays bounded: {wal} bytes"
    );
    assert_eq!(app.count("SELECT COUNT(*) FROM orders"), ORDERS as i64);
    app.stop().await;
}
