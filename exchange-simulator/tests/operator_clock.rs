//! The operator clock and `ClockChanged` (step 6.4, SR-24, SR-25, D17): `GET /api/clock` is always mounted; speed,
//! freeze and resume are mounted under `/api/clock` with `clock.operator_control`, independent of test control; every
//! change that alters the clock publishes `ClockChanged`, from which a follower can compute simulated time.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config};
use serde_json::{Value, json};

const START: &str = "2030-01-07T09:00:00Z";

fn virtual_clock(c: &mut Config) {
    c.clock.mode = ClockMode::Virtual;
    c.clock.start = Some(START.parse().unwrap());
}

async fn clock_events(app: &TestApp) -> Vec<Value> {
    let (_, events) = app.get("/api/events?eventType=ClockChanged").await;
    events.as_array().unwrap().clone()
}

#[tokio::test]
async fn the_clock_is_always_readable_and_operator_control_is_separate_from_test_control() {
    let mut app = TestApp::start_with(|c| {
        virtual_clock(c);
        c.test_control.enabled = false;
    })
    .await;
    let (status, clock) = app.get("/api/clock").await;
    assert_eq!(
        (status, clock),
        (
            StatusCode::OK,
            json!({ "mode": "virtual", "running": false, "speed": 1.0, "now": START })
        )
    );
    for route in ["/api/clock/speed", "/api/clock/freeze", "/api/clock/resume"] {
        let (status, _) = app.post(route, json!({ "speed": 2 })).await;
        assert!(
            matches!(
                status,
                StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
            ),
            "{route} without operator control: {status}"
        );
    }
    app.stop().await;

    let mut app = TestApp::start_with(|c| {
        virtual_clock(c);
        c.clock.operator_control = true;
        c.test_control.enabled = false;
    })
    .await;
    let (status, clock) = app.post("/api/clock/speed", json!({ "speed": 60 })).await;
    assert_eq!((status, &clock["speed"]), (StatusCode::OK, &json!(60.0)));
    let (status, clock) = app.post("/api/clock/resume", json!({})).await;
    assert_eq!((status, &clock["running"]), (StatusCode::OK, &json!(true)));
    let (status, clock) = app.post("/api/clock/freeze", json!({})).await;
    assert_eq!((status, &clock["running"]), (StatusCode::OK, &json!(false)));
    let (status, _) = app
        .post("/test/clock/advance", json!({ "seconds": 1 }))
        .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "set and advance stay test-only"
    );
    let (status, _) = app.post("/test/reset", json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "reset is not exposed");
    app.stop().await;
}

#[tokio::test]
async fn the_system_clock_is_readable_and_refuses_changes() {
    let mut app = TestApp::start_with(|c| c.clock.operator_control = true).await;
    let (status, clock) = app.get("/api/clock").await;
    assert_eq!(
        (status, &clock["mode"], &clock["running"]),
        (StatusCode::OK, &json!("system"), &json!(true))
    );
    let (status, error) = app.post("/api/clock/speed", json!({ "speed": 2 })).await;
    assert_eq!(
        (status, &error["error"]),
        (StatusCode::CONFLICT, &json!("clock_not_virtual"))
    );
    assert!(clock_events(&app).await.is_empty());
    app.stop().await;
}

#[tokio::test]
async fn every_change_publishes_clock_changed_and_no_ops_publish_nothing() {
    let mut app = TestApp::start_with(|c| {
        virtual_clock(c);
        c.clock.operator_control = true;
        c.test_control.enabled = true;
    })
    .await;
    app.post("/test/clock/advance", json!({ "hours": 1 })).await;
    app.post("/test/clock/set", json!({ "to": "2030-01-07T12:00:00Z" }))
        .await;
    app.post("/api/clock/freeze", json!({})).await; // already frozen: nothing
    app.post("/api/clock/speed", json!({ "speed": 1 })).await; // unchanged: nothing
    app.post("/api/clock/speed", json!({ "speed": 120 })).await;
    app.post("/api/clock/resume", json!({})).await;
    app.post("/api/clock/resume", json!({})).await; // already running: nothing
    app.post("/api/clock/freeze", json!({})).await;

    let events = clock_events(&app).await;
    let changes: Vec<&str> = events
        .iter()
        .map(|e| e["payload"]["change"].as_str().unwrap())
        .collect();
    assert_eq!(changes, ["advance", "set", "speed", "resume", "freeze"]);
    let first = &events[0];
    assert_eq!(
        (
            &first["source"],
            &first["aggregateId"],
            &first["occurredAt"]
        ),
        (
            &json!("exchange"),
            &json!("clock"),
            &json!("2030-01-07T10:00:00Z")
        )
    );
    assert_eq!(
        first["payload"],
        json!({ "change": "advance", "mode": "virtual", "running": false, "speed": 1.0,
                "now": "2030-01-07T10:00:00Z", "effectiveAt": null }),
        "a frozen clock carries no real time"
    );
    let resumed = &events[3]["payload"];
    assert_eq!(
        (&resumed["running"], &resumed["speed"]),
        (&json!(true), &json!(120.0))
    );
    assert!(resumed["effectiveAt"].is_string());
    assert_eq!(events[4]["payload"]["effectiveAt"], Value::Null);
    app.stop().await;
}

#[tokio::test]
async fn a_follower_computes_simulated_time_from_the_event() {
    let mut app = TestApp::start_with(|c| {
        virtual_clock(c);
        c.clock.operator_control = true;
        c.clock.speed = 3_600.0;
    })
    .await;
    app.post("/api/clock/resume", json!({})).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let event = clock_events(&app).await.pop().unwrap();
    let payload = &event["payload"];
    let at = |v: &Value| v.as_str().unwrap().parse::<DateTime<Utc>>().unwrap();
    let followed = at(&payload["now"])
        + (Utc::now() - at(&payload["effectiveAt"])) * payload["speed"].as_f64().unwrap() as i32;
    let (_, clock) = app.get("/api/clock").await;
    let drift = (at(&clock["now"]) - followed).num_milliseconds().abs();
    // Reading the clock after computing can only move it on: at 3600x, 50 ms of real time is 180 s.
    assert!(drift < 180_000, "follower off by {drift} ms");
    app.stop().await;
}
