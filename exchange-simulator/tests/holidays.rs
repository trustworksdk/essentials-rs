//! Holidays (step 6.20, SR-35): with Thursday 2030-01-10 a holiday and T+1, no business day closes on Thursday, trade
//! and settlement dates skip it, markets are closed unless their session names its own holidays, and
//! `GET /api/calendar` describes the calendar.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, SessionConfig, SettlementMode};
use serde_json::{Value, json};

fn d(text: &str) -> chrono::NaiveDate {
    text.parse().unwrap()
}

fn session(holidays: Option<Vec<chrono::NaiveDate>>) -> SessionConfig {
    SessionConfig {
        open: "08:00".into(),
        close: "17:00".into(),
        time_zone: "UTC".into(),
        holidays,
    }
}

/// From Monday 2030-01-07 09:00 UTC; the cutoff 17:30 UTC; Thursday a holiday. XCSE (ACME) closes with the business
/// calendar; XNAS (NASQ) names no holidays of its own and trades on Thursday.
fn thursday_off(c: &mut Config) {
    c.clock.mode = ClockMode::Virtual;
    c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
    c.test_control.enabled = true;
    c.clearing.enabled = true;
    c.clearing.cutoff = "17:30".into();
    c.clearing.time_zone = "UTC".into();
    c.clearing.holidays = vec![d("2030-01-10"), d("2030-12-25")]; // Christmas: outside every range read.
    c.settlement.mode = SettlementMode::Simulated;
    c.settlement.settlement_days = 1;
    c.settlement.tick_interval_ms = 20;
    c.sessions = [
        ("XCSE".to_string(), session(None)),
        ("XNAS".to_string(), session(Some(vec![]))),
    ]
    .into();
}

async fn set_clock(app: &TestApp, to: &str) {
    let (status, body) = app.post("/test/clock/set", json!({ "to": to })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// A sells and B buys 10 of `instrument` at 100.00; B's order result.
async fn trade(app: &TestApp, instrument: &str) -> Value {
    let order = |participant: &str, side: &str| {
        json!({ "instrumentId": instrument, "participantId": participant, "side": side, "type": "LIMIT",
                "priceTicks": 10_000, "quantity": 10 })
    };
    let (status, sell) = app.post("/api/orders", order("A", "SELL")).await;
    assert_eq!(status, StatusCode::CREATED, "{sell}");
    let (status, buy) = app.post("/api/orders", order("B", "BUY")).await;
    assert_eq!(status, StatusCode::CREATED, "{buy}");
    buy
}

fn dates(execution: &Value) -> (&str, &str) {
    (
        execution["tradeDate"].as_str().unwrap(),
        execution["intendedSettlementDate"].as_str().unwrap(),
    )
}

async fn business_dates(app: &TestApp, event_type: &str) -> Vec<String> {
    let (_, events) = app
        .get(&format!("/api/events?eventType={event_type}&limit=1000"))
        .await;
    let mut dates: Vec<String> = events
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["payload"]["businessDate"].as_str().unwrap().to_string())
        .collect();
    dates.dedup();
    dates
}

#[tokio::test]
async fn a_holiday_is_skipped_by_every_date_and_closes_nothing() {
    let mut app = TestApp::start_with(thursday_off).await;
    app.seed().await;
    let (status, body) = app
        .post(
            "/api/instruments",
            json!({ "id": "NASQ", "symbol": "NASQ", "mic": "XNAS", "currency": "USD", "currencyScale": 2,
                    "tickSizeMinor": 1 }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    set_clock(&app, "2030-01-09T12:00:00Z").await; // Wednesday.
    let wednesday = trade(&app, "ACME").await;
    assert_eq!(
        dates(&wednesday["executions"][0]),
        ("2030-01-09", "2030-01-11"),
        "T+1 from Wednesday is Friday"
    );

    set_clock(&app, "2030-01-10T12:00:00Z").await; // Thursday, the holiday.
    let (_, closed) = app
        .post(
            "/api/orders",
            json!({ "instrumentId": "ACME", "participantId": "A", "side": "SELL", "type": "LIMIT",
                    "priceTicks": 10_000, "quantity": 10 }),
        )
        .await;
    assert_eq!(
        closed["order"]["rejectReason"], "MARKET_CLOSED",
        "XCSE closes on the holiday"
    );
    let thursday = trade(&app, "NASQ").await;
    assert_eq!(
        dates(&thursday["executions"][0]),
        ("2030-01-11", "2030-01-14"),
        "a trade on the holiday belongs to Friday, and settles on Monday"
    );

    set_clock(&app, "2030-01-12T12:00:00Z").await; // Saturday.
    let closes = ["2030-01-07", "2030-01-08", "2030-01-09", "2030-01-11"];
    assert_eq!(business_dates(&app, "BusinessDayClosed").await, closes);
    for event_type in ["ClosingPriceIssued", "ClearingStatementIssued"] {
        assert!(
            !business_dates(&app, event_type)
                .await
                .contains(&"2030-01-10".to_string()),
            "no {event_type} for the holiday"
        );
    }
    let (_, statements) = app.get("/api/clearing/statements?date=2030-01-10").await;
    assert_eq!(
        statements,
        json!([]),
        "no clearing statement for the holiday"
    );
    let (_, pending) = app
        .get("/api/events?eventType=SettlementPending&limit=1000")
        .await;
    let wednesday_id = &wednesday["executions"][0]["executionId"];
    let instruction = pending
        .as_array()
        .unwrap()
        .iter()
        .find(|e| &e["payload"]["executionId"] == wednesday_id)
        .expect("the Wednesday trade's instruction");
    assert_eq!(
        instruction["payload"]["intendedSettlementDate"],
        "2030-01-11"
    );
    app.stop().await;
}

#[tokio::test]
async fn the_calendar_lists_business_days_with_their_cutoffs_and_the_holidays() {
    let mut app = TestApp::start_with(thursday_off).await;
    let (status, calendar) = app.get("/api/calendar?from=2030-01-07&to=2030-01-13").await;
    assert_eq!(status, StatusCode::OK, "{calendar}");
    let days: Vec<&str> = calendar["businessDays"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["date"].as_str().unwrap())
        .collect();
    assert_eq!(
        days,
        ["2030-01-07", "2030-01-08", "2030-01-09", "2030-01-11"]
    );
    assert_eq!(
        calendar["businessDays"][2]["cutoffAt"],
        "2030-01-09T17:30:00Z"
    );
    assert_eq!(calendar["holidays"], json!(["2030-01-10"]));
    assert_eq!(
        (&calendar["cutoff"], &calendar["timeZone"]),
        (&json!("17:30"), &json!("UTC"))
    );
    for query in [
        "from=2030-01-07",
        "from=2030-01-08&to=2030-01-07",
        "from=2030-01-01&to=2040-12-31",
    ] {
        let (status, body) = app.get(&format!("/api/calendar?{query}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
    }
    app.stop().await;
}
