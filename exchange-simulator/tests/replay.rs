//! The replay source (step 6.13, SR-4): a file of rows played on the exchange's clock from the moment the source
//! starts, looping if asked, at the clock's speed, reproducibly. Files are written by the tests, never committed.

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::http::StatusCode;
use common::TestApp;
use exchange_simulator::app::App;
use exchange_simulator::config::{ClockMode, Config, IdMode, InstrumentConfig};
use exchange_simulator::liquidity::{LiquidityMode, LiquiditySpec};
use exchange_simulator::pricing::{PriceSourceKind, PriceSourceSpec};
use serde_json::{Value, json};

const START: &str = "2030-01-07T09:00:00Z";

fn instrument(id: &str) -> InstrumentConfig {
    InstrumentConfig {
        id: id.into(),
        symbol: id.into(),
        mic: None,
        isin: None,
        cusip: None,
        currency: "USD".into(),
        currency_scale: 2,
        tick_size_minor: 1,
        lot_size: 1,
        enabled: true,
        reference_symbol: None,
        price_source: Some(PriceSourceSpec {
            kind: Some(PriceSourceKind::Replay),
            ..Default::default()
        }),
        liquidity: None,
    }
}

/// A CSV replay file in a fresh directory: `(seconds after the first row, symbol, price)`, recorded on another day.
fn write_file(rows: &[(i64, &str, f64)]) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("exchange-replay-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("session.csv");
    let first: chrono::DateTime<chrono::Utc> = "2029-11-05T14:30:00Z".parse().unwrap();
    let mut text = String::from("timestamp,referenceSymbol,price,volume\n");
    for (seconds, symbol, price) in rows {
        let at = first + chrono::Duration::seconds(*seconds);
        text.push_str(&format!("{},{symbol},{price},10\n", at.to_rfc3339()));
    }
    std::fs::write(&path, text).unwrap();
    path
}

fn replay(file: &Path, instruments: &[&str]) -> impl FnOnce(&mut Config) {
    let file = file.display().to_string();
    let instruments: Vec<InstrumentConfig> = instruments.iter().map(|id| instrument(id)).collect();
    move |c: &mut Config| {
        c.ids.mode = IdMode::Deterministic;
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some(START.parse().unwrap());
        c.test_control.enabled = true;
        c.price_sources.replay.file = Some(file);
        c.instruments = instruments;
    }
}

async fn reference(app: &TestApp, id: &str) -> Value {
    let (status, md) = app.get(&format!("/api/marketdata/{id}")).await;
    assert_eq!(status, StatusCode::OK, "{md}");
    md["reference"].clone()
}

async fn advance(app: &TestApp, seconds: u64) {
    let (status, body) = app
        .post("/test/clock/advance", json!({ "seconds": seconds }))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn rows_play_on_the_clock_from_when_the_source_starts() {
    let file = write_file(&[
        (0, "AAPL", 187.5),
        (2, "MSFT", 410.0),
        (5, "AAPL", 188.0),
        (9, "AAPL", 188.25),
    ]);
    let mut app = TestApp::start_with(replay(&file, &["AAPL", "MSFT"])).await;

    let aapl = reference(&app, "AAPL").await;
    assert_eq!(
        aapl,
        json!({ "price": "187.50", "priceTicks": 18750, "source": "replay", "sourceTimestamp": START, "stale": false }),
        "the first row lands at the clock's start"
    );
    assert_eq!(
        reference(&app, "MSFT").await,
        Value::Null,
        "nothing before the symbol's first row"
    );
    advance(&app, 2).await;
    assert_eq!(reference(&app, "MSFT").await["price"], "410.00");
    advance(&app, 3).await;
    let aapl = reference(&app, "AAPL").await;
    assert_eq!(
        (&aapl["price"], &aapl["sourceTimestamp"]),
        (&json!("188.00"), &json!("2030-01-07T09:00:05Z"))
    );
    advance(&app, 3600).await;
    assert_eq!(
        reference(&app, "AAPL").await["price"],
        "188.25",
        "the last row holds without loop"
    );
    app.stop().await;
    let _ = std::fs::remove_dir_all(file.parent().unwrap());
}

#[tokio::test]
async fn a_looping_replay_starts_over() {
    let file = write_file(&[(0, "AAPL", 100.0), (10, "AAPL", 101.0)]);
    let mut app = TestApp::start_with(|c| {
        replay(&file, &["AAPL"])(c);
        c.price_sources.replay.looping = true;
        c.price_sources.replay.loop_gap_ms = 5_000;
    })
    .await;
    advance(&app, 10).await;
    assert_eq!(reference(&app, "AAPL").await["price"], "101.00");
    advance(&app, 5).await; // 15 s: one pass is 10 s of rows and a 5 s gap.
    let looped = reference(&app, "AAPL").await;
    assert_eq!(
        (&looped["price"], &looped["sourceTimestamp"]),
        (&json!("100.00"), &json!("2030-01-07T09:00:15Z"))
    );
    advance(&app, 10).await;
    assert_eq!(reference(&app, "AAPL").await["price"], "101.00");
    app.stop().await;
    let _ = std::fs::remove_dir_all(file.parent().unwrap());
}

#[tokio::test]
async fn at_speed_60_the_replay_follows_the_recorded_path() {
    // Rows 6 simulated seconds apart: at speed 60 they play 0.1 s of real time apart.
    let path: Vec<f64> = (0..15).map(|i| 100.0 + f64::from(i) * 0.25).collect();
    let rows: Vec<(i64, &str, f64)> = path
        .iter()
        .enumerate()
        .map(|(i, p)| (i as i64 * 6, "AAPL", *p))
        .collect();
    let file = write_file(&rows);
    let mut app = TestApp::start_with(|c| {
        replay(&file, &["AAPL"])(c);
        c.clock.running = true;
        c.clock.speed = 60.0;
    })
    .await;
    let mut seen: Vec<String> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while seen.last().map(String::as_str) != Some("103.50") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "never reached the last row: {seen:?}"
        );
        let price = reference(&app, "AAPL").await["price"]
            .as_str()
            .unwrap()
            .to_string();
        if seen.last() != Some(&price) {
            seen.push(price);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let recorded: Vec<String> = path.iter().map(|p| format!("{p:.2}")).collect();
    assert_eq!(seen.first(), recorded.first(), "it starts at the first row");
    let mut rest = recorded.iter();
    assert!(
        seen.iter().all(|p| rest.any(|r| r == p)),
        "every price seen is a recorded one, in recorded order: {seen:?}"
    );
    assert!(
        seen.len() >= 10,
        "most rows were seen at 0.1 s each: {seen:?}"
    );
    app.stop().await;
    let _ = std::fs::remove_dir_all(file.parent().unwrap());
}

/// A replay with the liquidity provider quoting it and a client trading against it, across a day. Returns the log.
async fn replayed_session(file: &Path) -> Vec<Value> {
    let mut app = TestApp::start_with(|c| {
        replay(file, &["AAPL", "MSFT"])(c);
        c.liquidity.default = LiquiditySpec {
            mode: Some(LiquidityMode::On),
            levels: Some(2),
            level_quantity: Some(10),
            ..Default::default()
        };
    })
    .await;
    let (status, _) = app
        .post("/api/participants", json!({ "id": "A", "name": "A" }))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    for step in 0..6 {
        advance(&app, 1_800).await;
        for (id, side) in [("AAPL", "BUY"), ("MSFT", "SELL")] {
            let (status, body) = app
                .post(
                    "/api/orders",
                    json!({ "instrumentId": id, "participantId": "A", "side": side, "type": "MARKET",
                            "quantity": 2 + step }),
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }
    }
    advance(&app, 8 * 3_600).await; // Past the cutoff: closing prices.
    let log = app
        .get("/api/events?limit=1000")
        .await
        .1
        .as_array()
        .unwrap()
        .clone();
    app.stop().await;
    log
}

#[tokio::test]
async fn replaying_a_session_twice_gives_the_same_event_log() {
    let rows: Vec<(i64, &str, f64)> = (0..12)
        .flat_map(|i| {
            [
                (i * 1_500, "AAPL", 187.0 + i as f64 * 0.37),
                (i * 1_500 + 700, "MSFT", 410.0 - i as f64 * 0.21),
            ]
        })
        .collect();
    let file = write_file(&rows);
    let first = replayed_session(&file).await;
    let second = replayed_session(&file).await;
    assert!(first.iter().any(|e| e["eventType"] == "TradeExecuted"));
    assert!(first.iter().any(|e| e["eventType"] == "ClosingPriceIssued"));
    assert_eq!(
        serde_json::to_string(&first).unwrap(),
        serde_json::to_string(&second).unwrap()
    );
    let _ = std::fs::remove_dir_all(file.parent().unwrap());
}

#[tokio::test]
async fn a_replay_source_needs_a_file_with_its_symbol() {
    let file = write_file(&[(0, "AAPL", 100.0)]);
    let dir = std::env::temp_dir().join(format!("exchange-replay-cfg-{}", uuid::Uuid::now_v7()));
    let base = TestApp::config(&dir);
    for (adjust, expected) in [
        (
            Box::new(|c: &mut Config| c.instruments = vec![instrument("AAPL")])
                as Box<dyn Fn(&mut Config)>,
            "needs a replay file",
        ),
        (
            Box::new(|c: &mut Config| {
                replay(&file, &["MSFT"])(c);
            }),
            "no rows for MSFT",
        ),
        (
            Box::new(|c: &mut Config| {
                let broken = file.with_file_name("broken.csv");
                std::fs::write(
                    &broken,
                    "timestamp,referenceSymbol,price,volume\nnot a time,AAPL,1,1\n",
                )
                .unwrap();
                replay(&broken, &["AAPL"])(c);
            }),
            "line 2: timestamp",
        ),
    ] {
        let mut config = base.clone();
        adjust(&mut config);
        let error = format!("{:#}", App::start(&config).err().expect("refused"));
        assert!(error.contains(expected), "{error}");
    }
    let _ = std::fs::remove_dir_all(file.parent().unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}
