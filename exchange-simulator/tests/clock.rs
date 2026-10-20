//! The virtual clock and the test-control API: simulated time stamps the domain, moves only forward, runs at a
//! configurable speed, survives restarts, and is only reachable when test control is enabled.

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
    let (closes, others): (Vec<&Value>, Vec<&Value>) = events
        .as_array()
        .unwrap()
        .iter()
        .partition(|e| e["eventType"] == "BusinessDayClosed");
    // The day's closing price is stamped at its cutoff too.
    let others: Vec<&Value> = others
        .into_iter()
        .filter(|e| e["eventType"] != "ClosingPriceIssued")
        .collect();
    assert_eq!(
        closes
            .iter()
            .map(|e| e["occurredAt"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["2030-01-07T17:30:00Z", "2030-01-08T17:30:00Z"],
        "the advance passed two cutoffs"
    );
    let stamps: Vec<_> = others.iter().map(|e| time(&e["occurredAt"])).collect();
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

async fn fast_app(speed: f64) -> TestApp {
    TestApp::start_with(|c| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some(start());
        c.clock.running = true;
        c.clock.speed = speed;
    })
    .await
}

#[tokio::test]
async fn a_fast_clock_speeds_up_domain_time_and_its_speed_can_change() {
    let mut app = fast_app(3600.0).await;
    let (_, clock) = app.get("/test/clock").await;
    assert_eq!(
        (&clock["running"], &clock["speed"]),
        (&json!(true), &json!(3600.0))
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (_, clock) = app.get("/test/clock").await;
    assert!(
        time(&clock["now"]) - start() >= chrono::Duration::minutes(6),
        "an hour per real second: {}",
        clock["now"]
    );

    let (status, slowed) = app.post("/test/clock/speed", json!({ "speed": 1 })).await;
    assert_eq!((status, &slowed["speed"]), (StatusCode::OK, &json!(1.0)));
    assert!(
        time(&slowed["now"]) >= time(&clock["now"]),
        "the reading is kept"
    );
    for bad in [
        json!({ "speed": 0 }),
        json!({ "speed": -1 }),
        json!({ "speed": 2e6 }),
    ] {
        let (status, body) = app.post("/test/clock/speed", bad.clone()).await;
        assert_eq!(
            (status, &body["error"]),
            (StatusCode::BAD_REQUEST, &json!("invalid_clock_speed")),
            "{bad}"
        );
    }
    assert_eq!(
        app.post("/test/clock/speed", json!({ "speed": "fast" }))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(app.get("/test/clock").await.1["speed"], 1.0);

    assert_eq!(
        app.call(Method::POST, "/test/reset", None).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        app.get("/test/clock").await.1["speed"],
        3600.0,
        "a reset goes back to the configured speed"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_restart_on_a_fast_clock_continues_from_the_latest_stamp() {
    // Ten simulated hours per real second: the reading stored at startup is soon far behind.
    let mut app = fast_app(36_000.0).await;
    app.seed().await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    app.order("A", "SELL", Some(10025), 20).await;
    let (_, buy) = app.order("B", "BUY", Some(10025), 10).await;
    let traded = time(&buy["executions"][0]["executedAt"]);
    assert!(traded - start() >= chrono::Duration::hours(2), "{traded}");

    app.restart().await;
    let (_, clock) = app.get("/test/clock").await;
    assert!(
        time(&clock["now"]) >= traded,
        "{} before {traded}",
        clock["now"]
    );
    assert_eq!(clock["speed"], 36_000.0, "the configured speed");
    let (_, again) = app.order("C", "BUY", Some(10025), 10).await;
    assert!(time(&again["executions"][0]["executedAt"]) >= traded);
    app.stop().await;
}

#[tokio::test]
async fn startup_refuses_a_speed_it_cannot_honour() {
    let config = TestApp::start().await.config.clone();
    for (mode, speed, message) in [
        (
            ClockMode::System,
            1440.0,
            "clock.speed needs clock.mode: virtual",
        ),
        (ClockMode::Virtual, 0.0, "clock speed must be more than 0"),
    ] {
        let mut config = config.clone();
        config.clock.mode = mode;
        config.clock.speed = speed;
        let error = exchange_simulator::app::App::start(&config)
            .err()
            .expect("refused");
        assert!(format!("{error:#}").contains(message), "{error:#}");
    }
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
    assert_eq!(clock["speed"], 1.0);
    assert_eq!(
        app.post("/test/clock/speed", json!({ "speed": 2 })).await.0,
        StatusCode::CONFLICT
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
