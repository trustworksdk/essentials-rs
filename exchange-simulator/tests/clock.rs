//! The virtual clock and the test-control API: simulated time stamps the domain, moves only forward, survives
//! restarts, and is only reachable when test control is enabled.

mod common;

use axum::http::{Method, StatusCode};
use chrono::{DateTime, Utc};
use common::TestApp;
use exchange_simulator::config::ClockMode;
use serde_json::{Value, json};

const START: &str = "2030-01-07T09:00:00Z"; // a Monday

fn start() -> DateTime<Utc> {
    START.parse().unwrap()
}

async fn virtual_app() -> TestApp {
    TestApp::start_with(|c| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some(start());
    })
    .await
}

fn time(value: &Value) -> DateTime<Utc> {
    value
        .as_str()
        .unwrap_or_else(|| panic!("not a timestamp: {value}"))
        .parse()
        .unwrap()
}

#[tokio::test]
async fn simulated_time_stamps_orders_trades_and_events() {
    let mut app = virtual_app().await;
    app.seed().await;
    let (status, clock) = app.get("/test/clock").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        (&clock["mode"], &clock["running"]),
        (&json!("virtual"), &json!(false))
    );
    assert_eq!(time(&clock["now"]), start());

    let (_, sell) = app.order("A", "SELL", Some(10025), 20).await;
    let (_, buy) = app.order("B", "BUY", Some(10025), 10).await;
    assert_eq!(
        time(&buy["executions"][0]["executedAt"]),
        start(),
        "the clock is frozen"
    );
    let sell_id = sell["order"]["orderId"].as_str().unwrap();
    assert_eq!(
        time(&app.get(&format!("/api/orders/{sell_id}")).await.1["createdAt"]),
        start()
    );

    let (status, moved) = app
        .post("/test/clock/advance", json!({ "days": 2, "hours": 3 }))
        .await;
    assert_eq!(status, StatusCode::OK, "{moved}");
    let later = start() + chrono::Duration::days(2) + chrono::Duration::hours(3);
    assert_eq!(time(&moved["now"]), later);
    let (_, second) = app.order("C", "BUY", Some(10025), 10).await;
    assert_eq!(time(&second["executions"][0]["executedAt"]), later);
    assert_eq!(
        time(&app.get(&format!("/api/orders/{sell_id}")).await.1["updatedAt"]),
        later
    );

    let events = app.get("/api/events?limit=1000").await.1;
    let stamps: Vec<_> = events
        .as_array()
        .unwrap()
        .iter()
        .map(|e| time(&e["occurredAt"]))
        .collect();
    assert!(
        stamps.iter().all(|t| *t == start() || *t == later),
        "{stamps:?}"
    );
    assert_eq!(*stamps.last().unwrap(), later);
    app.stop().await;
}

#[tokio::test]
async fn the_clock_moves_forward_only_and_can_run_or_freeze() {
    let mut app = virtual_app().await;
    let (status, body) = app
        .post("/test/clock/set", json!({ "to": "2029-12-31T00:00:00Z" }))
        .await;
    assert_eq!(
        (status, &body["error"]),
        (StatusCode::CONFLICT, &json!("clock_backwards"))
    );
    let (status, body) = app
        .post("/test/clock/set", json!({ "to": "2030-02-01T00:00:00Z" }))
        .await;
    assert_eq!(
        (status, time(&body["now"])),
        (StatusCode::OK, "2030-02-01T00:00:00Z".parse().unwrap())
    );
    for bad in [
        json!({}),
        json!({ "days": 0 }),
        json!({ "weeks": 1 }),
        json!({ "seconds": -5 }),
    ] {
        assert_eq!(
            app.post("/test/clock/advance", bad.clone()).await.0,
            StatusCode::BAD_REQUEST,
            "{bad}"
        );
    }

    let (_, resumed) = app.call(Method::POST, "/test/clock/resume", None).await;
    assert_eq!(resumed["running"], true);
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let (_, running) = app.get("/test/clock").await;
    assert!(
        time(&running["now"]) > "2030-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(),
        "a running clock moves"
    );
    let (_, frozen) = app.call(Method::POST, "/test/clock/freeze", None).await;
    assert_eq!(frozen["running"], false);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(app.get("/test/clock").await.1["now"], frozen["now"]);
    app.stop().await;
}

#[tokio::test]
async fn a_restart_resumes_simulated_time_instead_of_jumping_back() {
    let mut app = virtual_app().await;
    let (_, moved) = app.post("/test/clock/advance", json!({ "days": 30 })).await;
    app.restart().await;
    let (_, clock) = app.get("/test/clock").await;
    assert_eq!(
        clock["now"], moved["now"],
        "not back to the configured start"
    );
    assert_eq!(clock["running"], false);

    app.call(Method::POST, "/test/clock/resume", None).await;
    app.restart().await;
    assert_eq!(
        app.get("/test/clock").await.1["running"],
        true,
        "running state survives too"
    );
    app.stop().await;
}

#[tokio::test]
async fn the_system_clock_cannot_be_controlled() {
    let mut app = TestApp::start().await;
    let (_, clock) = app.get("/test/clock").await;
    assert_eq!(
        (&clock["mode"], &clock["running"]),
        (&json!("system"), &json!(true))
    );
    let now = time(&clock["now"]);
    assert!((Utc::now() - now).num_seconds().abs() < 5);
    let (status, body) = app.post("/test/clock/advance", json!({ "days": 1 })).await;
    assert_eq!(
        (status, &body["error"]),
        (StatusCode::CONFLICT, &json!("clock_not_virtual"))
    );
    app.stop().await;
}

#[tokio::test]
async fn test_control_routes_exist_only_when_enabled() {
    let mut app = TestApp::start_with(|c| {
        c.test_control.enabled = false;
        c.clock.mode = ClockMode::Virtual;
    })
    .await;
    assert_eq!(app.get("/test/clock").await.0, StatusCode::NOT_FOUND);
    assert_eq!(
        app.post("/test/clock/advance", json!({ "days": 1 }))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(app.get("/health").await.0, StatusCode::OK);
    app.stop().await;
}
