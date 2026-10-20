//! Closing prices (step 6.11, SR-14): at each cutoff, after `BusinessDayClosed`, one `ClosingPriceIssued` per instrument
//! that traded (its last trade of the day) or has a reference price (that price at the cutoff), with the day's volume;
//! exactly once across restarts, and byte-identical in two seeded runs.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, IdMode, InstrumentConfig};
use exchange_simulator::liquidity::{LiquidityMode, LiquiditySpec};
use exchange_simulator::pricing::{PriceSourceKind, PriceSourceSpec};
use serde_json::{Value, json};

fn instrument(id: &str, price_source: Option<PriceSourceSpec>) -> InstrumentConfig {
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
        price_source,
        liquidity: None,
    }
}

fn fixed(price: &str) -> Option<PriceSourceSpec> {
    Some(PriceSourceSpec {
        kind: Some(PriceSourceKind::Fixed),
        price: Some(price.into()),
        ..Default::default()
    })
}

fn synthetic() -> Option<PriceSourceSpec> {
    Some(PriceSourceSpec {
        kind: Some(PriceSourceKind::Synthetic),
        initial_price: Some("80.00".into()),
        volatility: Some(0.5),
        seed: Some(11),
        ..Default::default()
    })
}

/// Monday 2030-01-07 09:00 UTC on a frozen virtual clock, the business day ending at 17:30 UTC.
fn market(c: &mut Config) {
    c.ids.mode = IdMode::Deterministic;
    c.clock.mode = ClockMode::Virtual;
    c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
    c.test_control.enabled = true;
    c.clearing.cutoff = "17:30".into();
    c.clearing.time_zone = "UTC".into();
    c.instruments = vec![
        instrument("TRADED", fixed("100.00")),
        instrument("QUIET", fixed("50.00")),
        instrument("NONE", None),
        instrument("SYN", synthetic()),
    ];
}

async fn participants(app: &TestApp) {
    for id in ["A", "B"] {
        let (status, _) = app
            .post("/api/participants", json!({ "id": id, "name": id }))
            .await;
        assert_eq!(status, StatusCode::CREATED);
    }
}

async fn trade(app: &TestApp, instrument: &str, price: i64, quantity: u64) {
    for (participant, side) in [("A", "SELL"), ("B", "BUY")] {
        let (status, body) = app
            .post(
                "/api/orders",
                json!({ "instrumentId": instrument, "participantId": participant, "side": side, "type": "LIMIT",
                        "priceTicks": price, "quantity": quantity }),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }
}

async fn set_clock(app: &TestApp, to: &str) {
    let (status, body) = app.post("/test/clock/set", json!({ "to": to })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

async fn events(app: &TestApp) -> Vec<Value> {
    app.get("/api/events?limit=1000")
        .await
        .1
        .as_array()
        .unwrap()
        .clone()
}

/// `(eventType, businessDate, instrumentId)` of the end-of-day events, in log order.
fn end_of_day(events: &[Value]) -> Vec<(String, String, String)> {
    events
        .iter()
        .filter(|e| e["eventType"] == "BusinessDayClosed" || e["eventType"] == "ClosingPriceIssued")
        .map(|e| {
            (
                e["eventType"].as_str().unwrap().to_string(),
                e["payload"]["businessDate"].as_str().unwrap().to_string(),
                e["payload"]["instrumentId"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            )
        })
        .collect()
}

fn close<'a>(events: &'a [Value], date: &str, instrument: &str) -> &'a Value {
    &events
        .iter()
        .find(|e| {
            e["eventType"] == "ClosingPriceIssued"
                && e["payload"]["businessDate"] == date
                && e["payload"]["instrumentId"] == instrument
        })
        .unwrap_or_else(|| panic!("no close for {instrument} on {date}"))["payload"]
}

#[tokio::test]
async fn each_instrument_closes_at_its_last_trade_or_its_reference_price() {
    let mut app = TestApp::start_with(market).await;
    participants(&app).await;
    trade(&app, "TRADED", 10_100, 5).await;
    trade(&app, "TRADED", 10_200, 3).await;
    set_clock(&app, "2030-01-07T18:00:00Z").await;
    trade(&app, "TRADED", 10_300, 2).await; // After Monday's cutoff: Tuesday's.
    set_clock(&app, "2030-01-08T18:00:00Z").await;

    let all = events(&app).await;
    let day = |date: &str| {
        vec![
            (
                "BusinessDayClosed".to_string(),
                date.to_string(),
                String::new(),
            ),
            (
                "ClosingPriceIssued".to_string(),
                date.to_string(),
                "QUIET".to_string(),
            ),
            (
                "ClosingPriceIssued".to_string(),
                date.to_string(),
                "SYN".to_string(),
            ),
            (
                "ClosingPriceIssued".to_string(),
                date.to_string(),
                "TRADED".to_string(),
            ),
        ]
    };
    assert_eq!(
        end_of_day(&all),
        [day("2030-01-07"), day("2030-01-08")].concat(),
        "after the day closes, in instrument order; NONE has no price to close at"
    );

    assert_eq!(
        close(&all, "2030-01-07", "TRADED"),
        &json!({ "instrumentId": "TRADED", "businessDate": "2030-01-07", "closingPrice": "102.00",
                 "closingPriceTicks": 10200, "currency": "USD", "basis": "LAST_TRADE", "volume": 8 })
    );
    let tuesday = close(&all, "2030-01-08", "TRADED");
    assert_eq!(
        (&tuesday["closingPrice"], &tuesday["volume"]),
        (&json!("103.00"), &json!(2))
    );
    let quiet = close(&all, "2030-01-07", "QUIET");
    assert_eq!(
        (&quiet["closingPrice"], &quiet["basis"], &quiet["volume"]),
        (&json!("50.00"), &json!("REFERENCE"), &json!(0))
    );
    let issued = all
        .iter()
        .find(|e| e["eventType"] == "ClosingPriceIssued")
        .unwrap();
    assert_eq!(
        issued["occurredAt"], "2030-01-07T17:30:00Z",
        "stamped at the cutoff"
    );
    assert_eq!(
        (&issued["aggregateId"], &issued["correlationId"]),
        (&json!("QUIET"), &json!("2030-01-07"))
    );

    // Exactly once: a restart issues nothing again.
    app.restart().await;
    set_clock(&app, "2030-01-08T19:00:00Z").await;
    assert_eq!(end_of_day(&events(&app).await).len(), 8);
    app.stop().await;
}

/// A seeded run over three business days: synthetic prices, the liquidity provider quoting them, client market
/// orders against it, and the clock moved across each cutoff. Returns the whole event log.
async fn seeded_run() -> Vec<Value> {
    let mut app = TestApp::start_with(|c| {
        market(c);
        c.price_sources.seed = 7;
        c.instruments = vec![
            instrument("SYN", synthetic()),
            instrument("SYN2", synthetic()),
        ];
        c.instruments[1].price_source.as_mut().unwrap().seed = Some(12);
        c.liquidity.default = LiquiditySpec {
            mode: Some(LiquidityMode::On),
            levels: Some(2),
            level_quantity: Some(10),
            ..Default::default()
        };
    })
    .await;
    participants(&app).await;
    for (day, at) in ["2030-01-07", "2030-01-08", "2030-01-09"]
        .iter()
        .enumerate()
    {
        set_clock(&app, &format!("{at}T12:00:00Z")).await;
        for (instrument, side) in [("SYN", "BUY"), ("SYN2", "SELL")] {
            let (status, body) = app
                .post(
                    "/api/orders",
                    json!({ "instrumentId": instrument, "participantId": "A", "side": side, "type": "MARKET",
                            "quantity": 3 + day as u64 }),
                )
                .await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }
    }
    set_clock(&app, "2030-01-09T18:00:00Z").await;
    let log = events(&app).await;
    app.stop().await;
    log
}

#[tokio::test]
async fn two_seeded_runs_give_a_byte_identical_event_log_closing_prices_included() {
    let first = seeded_run().await;
    let second = seeded_run().await;
    assert_eq!(
        first
            .iter()
            .filter(|e| e["eventType"] == "ClosingPriceIssued")
            .count(),
        6,
        "two instruments, three days"
    );
    assert_eq!(
        serde_json::to_string(&first).unwrap(),
        serde_json::to_string(&second).unwrap()
    );
}
