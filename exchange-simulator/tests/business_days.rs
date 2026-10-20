//! `BusinessDayClosed` (step 6.5, SR-26): one at the cutoff of every business day, also without trades and without
//! clearing, before that day's clearing statements; weekends skipped, each day of a large clock move closed in turn,
//! never twice across a restart, and not for days before a reset.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config};
use serde_json::{Value, json};

/// A frozen virtual clock at `start`, clearing off.
fn at(start: &'static str) -> impl FnOnce(&mut Config) {
    move |c: &mut Config| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some(start.parse().unwrap());
    }
}

async fn advance(app: &TestApp, body: Value) {
    let (status, moved) = app.post("/test/clock/advance", body).await;
    assert_eq!(status, StatusCode::OK, "{moved}");
}

async fn closes(app: &TestApp) -> Vec<Value> {
    let (_, events) = app.get("/api/events?eventType=BusinessDayClosed").await;
    events.as_array().unwrap().clone()
}

fn dates(closes: &[Value]) -> Vec<&str> {
    closes
        .iter()
        .map(|e| e["payload"]["businessDate"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn every_business_day_closes_without_trades_or_clearing_and_weekends_are_skipped() {
    // Friday morning; four days later is Tuesday morning.
    let mut app = TestApp::start_with(at("2030-01-11T09:00:00Z")).await;
    assert!(closes(&app).await.is_empty());
    advance(&app, json!({ "days": 4 })).await;
    let closed = closes(&app).await;
    assert_eq!(dates(&closed), ["2030-01-11", "2030-01-14"]);
    assert_eq!(
        closed[0]["payload"],
        json!({ "businessDate": "2030-01-11", "cutoffAt": "2030-01-11T17:30:00Z",
                "nextBusinessDate": "2030-01-14", "timeZone": "UTC" })
    );
    assert_eq!(
        (
            &closed[0]["occurredAt"],
            &closed[0]["source"],
            &closed[0]["aggregateId"]
        ),
        (
            &json!("2030-01-11T17:30:00Z"),
            &json!("exchange"),
            &json!("2030-01-11")
        )
    );
    app.stop().await;
}

#[tokio::test]
async fn the_cutoff_follows_the_clearing_time_zone() {
    let mut app = TestApp::start_with(|c| {
        at("2030-01-07T09:00:00Z")(c);
        c.clearing.cutoff = "17:30".into();
        c.clearing.time_zone = "Europe/Copenhagen".into();
    })
    .await;
    advance(&app, json!({ "hours": 8 })).await;
    let closed = closes(&app).await;
    assert_eq!(
        (
            &closed[0]["payload"]["cutoffAt"],
            &closed[0]["payload"]["timeZone"]
        ),
        (&json!("2030-01-07T16:30:00Z"), &json!("Europe/Copenhagen")),
        "17:30 in Copenhagen in winter"
    );
    app.stop().await;
}

#[tokio::test]
async fn the_day_closes_before_its_clearing_statements() {
    let mut app = TestApp::start_with(|c| {
        at("2030-01-07T09:00:00Z")(c);
        c.clearing.enabled = true;
    })
    .await;
    app.seed().await;
    app.order("A", "SELL", Some(10_000), 5).await;
    app.order("B", "BUY", Some(10_000), 5).await;
    advance(&app, json!({ "days": 1 })).await;
    let (_, events) = app.get("/api/events?limit=1000").await;
    let end_of_day: Vec<(&str, &str)> = events
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["occurredAt"] == "2030-01-07T17:30:00Z")
        .map(|e| {
            (
                e["eventType"].as_str().unwrap(),
                e["aggregateId"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(end_of_day[0], ("BusinessDayClosed", "2030-01-07"));
    assert_eq!(
        end_of_day[1..].iter().map(|(t, _)| *t).collect::<Vec<_>>(),
        [
            "ClosingPriceIssued",
            "ClearingStatementIssued",
            "ClearingStatementIssued"
        ],
        "the day closes, its closing prices follow, then its statements"
    );
    app.stop().await;
}

#[tokio::test]
async fn days_close_once_across_restarts_and_not_before_a_reset() {
    let mut app = TestApp::start_with(at("2030-01-07T09:00:00Z")).await;
    advance(&app, json!({ "days": 1 })).await;
    app.restart().await;
    assert_eq!(dates(&closes(&app).await), ["2030-01-07"], "not again");

    // A cutoff passed while the service was down closes at startup, once.
    app.stop().await;
    app.config.clock.start = Some("2030-01-10T09:00:00Z".parse().unwrap());
    let db = app.db();
    db.execute(
        "UPDATE virtual_clock SET now = '2030-01-10T09:00:00.000000Z'",
        [],
    )
    .unwrap();
    drop(db);
    app.restart().await;
    assert_eq!(
        dates(&closes(&app).await),
        ["2030-01-07", "2030-01-08", "2030-01-09"]
    );

    let (status, _) = app.post("/test/reset", json!({})).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    advance(&app, json!({ "hours": 1 })).await;
    assert!(
        closes(&app).await.is_empty(),
        "after a reset, no day before the clock's reading closes"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_running_clock_closes_days_on_its_own() {
    let mut app = TestApp::start_with(|c| {
        at("2030-01-07T17:00:00Z")(c);
        c.clock.running = true;
        c.clock.speed = 36_000.0; // ten simulated hours a real second
    })
    .await;
    let mut closed = Vec::new();
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        closed = closes(&app).await;
        if !closed.is_empty() {
            break;
        }
    }
    assert_eq!(
        closed.first().map(|e| e["payload"]["businessDate"].clone()),
        Some(json!("2030-01-07")),
        "closed by the background ticker, with no request"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_reset_closes_the_same_days_again_from_the_start() {
    let mut app = TestApp::start_with(at("2030-01-07T09:00:00Z")).await;
    advance(&app, json!({ "days": 3 })).await;
    assert_eq!(
        dates(&closes(&app).await),
        ["2030-01-07", "2030-01-08", "2030-01-09"]
    );
    // The clock goes back to its start; a run after the reset closes the same days as the first run.
    let (status, _) = app.post("/test/reset", json!({})).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    advance(&app, json!({ "days": 1 })).await;
    assert_eq!(dates(&closes(&app).await), ["2030-01-07"]);
    app.stop().await;
}
