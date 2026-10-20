//! Following the exchange's clock (step 6.6, SR-27): with `clock.mode: follow` the gateway reads another simulator's
//! `GET /api/clock` and takes its reading, speed and running state; it never moves backwards, refuses clock changes
//! of its own, runs on when the followed clock does not answer, and publishes `ClockChanged` with `change: follow`.

mod common;

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use common::TestApp;
use payment_gateway::config::ClockMode;
use serde_json::{Value, json};

/// A stand-in for the exchange's `GET /api/clock`: answers with whatever the test sets, or 503 while `None`.
#[derive(Clone)]
struct Followed {
    url: String,
    clock: Arc<Mutex<Option<Value>>>,
}

impl Followed {
    async fn start(clock: Value) -> Self {
        let state = Arc::new(Mutex::new(Some(clock)));
        let served = state.clone();
        let app = Router::new().route(
            "/api/clock",
            get(move || {
                let clock = served.lock().unwrap().clone();
                async move {
                    match clock {
                        Some(clock) => (StatusCode::OK, axum::Json(clock)),
                        None => (StatusCode::SERVICE_UNAVAILABLE, axum::Json(json!({}))),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, clock: state }
    }

    fn set(&self, clock: Option<Value>) {
        *self.clock.lock().unwrap() = clock;
    }
}

fn frozen(now: &str) -> Value {
    json!({ "mode": "virtual", "running": false, "speed": 1.0, "now": now })
}

async fn following(followed: &Followed) -> TestApp {
    let url = followed.url.clone();
    TestApp::start_with(move |c| {
        c.clock.mode = ClockMode::Follow;
        c.clock.follow = Some(payment_gateway::config::FollowConfig {
            url,
            poll_interval_ms: 20,
        });
        c.statement.enabled = false;
    })
    .await
}

/// Waits until the gateway's clock satisfies `condition`.
async fn until(app: &TestApp, what: &str, condition: impl Fn(&Value) -> bool) -> Value {
    for _ in 0..200 {
        let (_, clock) = app.get("/api/clock").await;
        if condition(&clock) {
            return clock;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("{what}: {}", app.get("/api/clock").await.1);
}

#[tokio::test]
async fn the_gateway_takes_the_followed_reading_speed_and_running_state() {
    let followed = Followed::start(frozen("2030-01-07T12:00:00Z")).await;
    let mut app = following(&followed).await;
    let clock = until(&app, "the first read", |c| {
        c["now"] == "2030-01-07T12:00:00Z"
    })
    .await;
    assert_eq!(
        (&clock["mode"], &clock["running"]),
        (&json!("virtual"), &json!(false))
    );

    followed.set(Some(json!({ "mode": "virtual", "running": true, "speed": 3600.0, "now": "2030-01-07T13:00:00Z" })));
    let clock = until(&app, "running at 3600x", |c| c["running"] == true).await;
    assert_eq!(clock["speed"], 3600.0);
    assert!(clock["now"].as_str().unwrap() >= "2030-01-07T13:00:00");

    // A payment follows the followed time.
    let payment = app.pay("follows", "DEPOSIT", "10.00", "USD").await;
    assert!(payment["createdAt"].as_str().unwrap() >= "2030-01-07T13:00:00");

    let changes = app.events("ClockChanged").await;
    assert!(changes.len() >= 2, "{changes:#?}");
    assert!(
        changes
            .iter()
            .all(|e| e["payload"]["change"] == "follow" && e["source"] == "payment-gateway")
    );
    app.stop().await;
}

#[tokio::test]
async fn a_following_clock_never_moves_back_and_refuses_changes() {
    let followed = Followed::start(frozen("2030-01-08T00:00:00Z")).await;
    let mut app = following(&followed).await;
    until(&app, "the first read", |c| {
        c["now"] == "2030-01-08T00:00:00Z"
    })
    .await;
    followed.set(Some(frozen("2030-01-07T00:00:00Z")));
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        app.get("/api/clock").await.1["now"],
        "2030-01-08T00:00:00Z",
        "held, not moved back"
    );
    for (route, body) in [
        ("/test/clock/advance", json!({ "hours": 1 })),
        ("/test/clock/resume", json!({})),
        ("/test/clock/speed", json!({ "speed": 2 })),
    ] {
        let (status, error) = app.post(route, body).await;
        assert_eq!(
            (status, &error["error"]),
            (StatusCode::CONFLICT, &json!("clock_follows")),
            "{route}"
        );
    }
    // Ahead again: followed.
    followed.set(Some(frozen("2030-01-09T00:00:00Z")));
    until(&app, "caught up", |c| c["now"] == "2030-01-09T00:00:00Z").await;
    app.stop().await;
}

#[tokio::test]
async fn the_clock_runs_on_while_the_followed_one_does_not_answer() {
    let followed = Followed::start(json!({ "mode": "virtual", "running": true, "speed": 3600.0, "now": "2030-01-07T12:00:00Z" })).await;
    let mut app = following(&followed).await;
    until(&app, "running", |c| c["running"] == true).await;
    followed.set(None);
    let before = app.get("/api/clock").await.1["now"].clone();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let (status, clock) = app.get("/api/clock").await;
    assert_eq!((status, &clock["running"]), (StatusCode::OK, &json!(true)));
    assert!(
        clock["now"].as_str().unwrap() > before.as_str().unwrap(),
        "still moving at the last speed"
    );
    app.stop().await;
}

#[tokio::test]
async fn follow_mode_needs_a_url() {
    let mut config = TestApp::config_for_tests();
    config.clock.mode = ClockMode::Follow;
    let error = payment_gateway::app::App::start(&config)
        .err()
        .expect("startup fails");
    assert!(
        format!("{error:#}").contains("clock.mode: follow needs clock.follow.url"),
        "{error:#}"
    );
}
