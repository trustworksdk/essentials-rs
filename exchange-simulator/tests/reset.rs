//! Reset and replay controls (plan item 3.4): `POST /test/reset` gives the state of a new database, redelivery
//! re-enqueues events from a sequence, and dead letters can be listed and retried.

mod common;

use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::post;
use common::TestApp;
use exchange_simulator::config::{ClockMode, IdMode, SettlementMode};
use exchange_simulator::scenario::{self, Scenario};
use serde_json::{Value, json};

#[derive(Clone, Default)]
struct ReceiverState {
    sequences: Arc<Mutex<Vec<u64>>>,
    /// The status to answer with.
    status: Arc<AtomicU16>,
    /// How long the next request takes; reset to 0 after it.
    slow_ms: Arc<AtomicU64>,
}

/// Records the `X-Event-Sequence` of every request on `/hook`.
struct Receiver {
    url: String,
    state: ReceiverState,
}

impl Receiver {
    async fn start() -> Self {
        let state = ReceiverState::default();
        state.status.store(200, Ordering::SeqCst);
        let app = Router::new()
            .route(
                "/hook",
                post(
                    |State(state): State<ReceiverState>, headers: HeaderMap| async move {
                        let slow = state.slow_ms.swap(0, Ordering::SeqCst);
                        if slow > 0 {
                            tokio::time::sleep(Duration::from_millis(slow)).await;
                        }
                        let sequence = headers["X-Event-Sequence"]
                            .to_str()
                            .unwrap()
                            .parse()
                            .unwrap();
                        state.sequences.lock().unwrap().push(sequence);
                        StatusCode::from_u16(state.status.load(Ordering::SeqCst)).unwrap()
                    },
                ),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, state }
    }

    fn sequences(&self) -> Vec<u64> {
        self.state.sequences.lock().unwrap().clone()
    }
}

/// Polls `uri` until `done` holds (or 10 s pass) and returns the last answer.
async fn until(app: &TestApp, uri: &str, done: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let body = app.get(uri).await.1;
        if done(&body) || Instant::now() > deadline {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn len(value: &Value) -> usize {
    value.as_array().map_or(0, Vec::len)
}

/// A deterministic service on a frozen virtual clock with the settlement simulator on.
async fn deterministic() -> TestApp {
    TestApp::start_with(|c| {
        c.ids.mode = IdMode::Deterministic;
        c.ids.seed = 9;
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
        c.settlement.mode = SettlementMode::Both;
        c.settlement.fail_probability = 0.0;
        c.settlement.delay_probability = 0.0;
    })
    .await
}

/// Registers everything, trades, settles at T+2 and reports an outcome: every kind of state there is.
async fn script(app: &TestApp, webhook: &str) -> Value {
    app.seed().await;
    let (status, _) = app
        .post(
            "/api/webhooks",
            json!({ "url": webhook, "eventTypes": ["TradeExecuted"] }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        app.order("A", "SELL", Some(10025), 20).await.0,
        StatusCode::CREATED
    );
    let (_, buy) = app.order("B", "BUY", Some(10025), 10).await;
    let execution = buy["executions"][0]["executionId"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, _) = app.post("/test/clock/advance", json!({ "days": 2 })).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = app
        .post(
            &format!("/api/settlements/{execution}/outcomes"),
            json!({ "status": "SETTLED", "reference": "r-1" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    app.get("/api/events?limit=1000").await.1
}

#[tokio::test]
async fn a_reset_gives_a_new_database_and_the_same_commands_the_same_log() {
    let mut app = deterministic().await;
    let receiver = Receiver::start().await;
    let first = script(&app, &receiver.url).await;
    assert!(len(&first) >= 6, "{first}");
    until(&app, "/test/deliveries?status=DELIVERED", |d| len(d) == 1).await;

    let (status, body) = app.call(Method::POST, "/test/reset", None).await;
    assert_eq!((status, body), (StatusCode::NO_CONTENT, Value::Null));
    for table in [
        "instruments",
        "participants",
        "orders",
        "executions",
        "events",
        "webhook_subscriptions",
        "webhook_deliveries",
        "webhook_attempts",
        "settlement_instructions",
        "settlement_reports",
        "consumer_offsets",
        "sqlite_sequence",
    ] {
        assert_eq!(
            app.count(&format!("SELECT COUNT(*) FROM {table}")),
            0,
            "{table}"
        );
    }
    assert_eq!(app.get("/api/instruments").await.1, json!([]));
    assert_eq!(app.get("/api/webhooks").await.1, json!([]));
    assert_eq!(
        app.get("/api/orderbooks/ACME").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.get("/test/clock").await.1["now"],
        "2030-01-07T09:00:00Z",
        "the clock is back at its start, even though that is earlier"
    );

    let second = script(&app, &receiver.url).await;
    assert_eq!(first, second, "byte-identical log, IDs included");
    until(&app, "/test/deliveries?status=DELIVERED", |d| len(d) == 1).await;
    // A restart after a reset continues from the reset state.
    app.restart().await;
    assert_eq!(app.get("/api/events?limit=1000").await.1, second);
    assert_eq!(
        app.get("/test/clock").await.1["now"],
        "2030-01-09T09:00:00Z"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_result_for_a_delivery_from_before_the_reset_is_dropped() {
    let mut app = deterministic().await;
    let receiver = Receiver::start().await;
    receiver.state.slow_ms.store(600, Ordering::SeqCst);
    script(&app, &receiver.url).await;
    // The first delivery is in flight (the receiver takes 600 ms); reset under it and run the same commands, which
    // give a delivery with the same deterministic ID.
    until(&app, "/test/deliveries?status=IN_FLIGHT", |d| len(d) == 1).await;
    assert_eq!(
        app.call(Method::POST, "/test/reset", None).await.0,
        StatusCode::NO_CONTENT
    );
    script(&app, &receiver.url).await;
    let delivered = until(&app, "/test/deliveries?status=DELIVERED", |d| len(d) == 1).await;
    assert_eq!(delivered[0]["attempts"], 1);
    // Let the old request finish and its result come back.
    let deadline = Instant::now() + Duration::from_secs(5);
    while receiver.sequences().len() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(receiver.sequences().len(), 2, "the old request did arrive");
    let attempts = app.get("/test/attempts").await.1;
    assert_eq!(
        len(&attempts),
        1,
        "only the new attempt is recorded: {attempts}"
    );
    assert_eq!(app.get("/test/deliveries").await.1[0]["attempts"], 1);
    app.stop().await;
}

#[tokio::test]
async fn redelivery_requeues_from_a_sequence_including_earlier_events() {
    let mut app = TestApp::start().await;
    app.seed().await;
    for price in [100, 101, 102] {
        assert_eq!(
            app.order("A", "SELL", Some(price), 1).await.0,
            StatusCode::CREATED
        );
    }
    let receiver = Receiver::start().await;
    let (_, subscription) = app
        .post(
            "/api/webhooks",
            json!({ "url": receiver.url, "eventTypes": ["OrderAccepted"] }),
        )
        .await;
    let id = subscription["id"].as_str().unwrap();
    let events: Vec<u64> = app
        .get("/api/events?eventType=OrderAccepted")
        .await
        .1
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["sequence"].as_u64().unwrap())
        .collect();
    assert_eq!(events.len(), 3);
    assert_eq!(
        len(&app.get("/test/deliveries").await.1),
        0,
        "registered after the events"
    );

    let redeliver = |after: u64| {
        let uri = format!("/test/webhooks/{id}/redeliver?after={after}");
        let app = &app;
        async move { app.call(Method::POST, &uri, None).await }
    };
    assert_eq!(redeliver(0).await, (StatusCode::OK, json!({ "queued": 3 })));
    until(&app, "/test/deliveries?status=DELIVERED", |d| len(d) == 3).await;
    assert_eq!(receiver.sequences(), events);

    // Delivered ones go out again, from the given sequence on, with a fresh attempt count.
    assert_eq!(
        redeliver(events[0]).await,
        (StatusCode::OK, json!({ "queued": 2 }))
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while receiver.sequences().len() < 5 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let delivered = until(&app, "/test/deliveries?status=DELIVERED", |d| len(d) == 3).await;
    assert_eq!(receiver.sequences()[3..], events[1..]);
    assert!(
        delivered
            .as_array()
            .unwrap()
            .iter()
            .all(|d| d["attempts"] == 1),
        "{delivered}"
    );
    assert_eq!(len(&app.get("/test/attempts").await.1), 5);
    assert_eq!(
        redeliver(events[2]).await,
        (StatusCode::OK, json!({ "queued": 0 }))
    );

    let unknown = uuid::Uuid::nil();
    let (status, _) = app
        .call(
            Method::POST,
            &format!("/test/webhooks/{unknown}/redeliver"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = app
        .call(
            Method::POST,
            &format!("/test/webhooks/{id}/redeliver?after=x"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    app.stop().await;
}

#[tokio::test]
async fn dead_letters_are_listed_and_retried() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let failing = Receiver::start().await;
    failing.state.status.store(500, Ordering::SeqCst);
    let other = Receiver::start().await;
    other.state.status.store(500, Ordering::SeqCst);
    let mut ids = Vec::new();
    for receiver in [&failing, &other] {
        let (_, subscription) = app
            .post(
                "/api/webhooks",
                json!({ "url": receiver.url, "eventTypes": ["OrderAccepted"] }),
            )
            .await;
        ids.push(subscription["id"].as_str().unwrap().to_string());
    }
    for price in [100, 101] {
        app.order("A", "SELL", Some(price), 1).await;
    }
    let dead = until(&app, "/test/deliveries?status=DEAD", |d| len(d) == 4).await;
    assert_eq!(len(&dead), 4, "{dead}");
    assert!(
        dead.as_array()
            .unwrap()
            .iter()
            .all(|d| d["attempts"] == 5 && d["lastError"] == "HTTP 500 Internal Server Error")
    );
    let mine = app
        .get(&format!(
            "/test/deliveries?status=DEAD&subscriptionId={}",
            ids[0]
        ))
        .await
        .1;
    assert_eq!(len(&mine), 2);

    // One subscription's dead letters, after its consumer recovered.
    failing.state.status.store(200, Ordering::SeqCst);
    let retry = |body: Value| app.call(Method::POST, "/test/dead-letters/retry", Some(body));
    assert_eq!(
        retry(json!({ "subscriptionId": ids[0] })).await,
        (StatusCode::OK, json!({ "retried": 2 }))
    );
    let delivered = until(&app, "/test/deliveries?status=DELIVERED", |d| len(d) == 2).await;
    assert!(
        delivered
            .as_array()
            .unwrap()
            .iter()
            .all(|d| d["subscriptionId"] == ids[0].as_str() && d["attempts"] == 1)
    );
    assert_eq!(
        len(&app.get("/test/deliveries?status=DEAD").await.1),
        2,
        "the other's stay dead"
    );

    // One listed dead letter; then all that are left, with an empty body.
    other.state.status.store(200, Ordering::SeqCst);
    let one = mine_of(&app, &ids[1]).await;
    assert_eq!(
        retry(json!({ "deliveryIds": [one] })).await.1,
        json!({ "retried": 1 })
    );
    until(&app, "/test/deliveries?status=DELIVERED", |d| len(d) == 3).await;
    let (status, body) = app
        .call(Method::POST, "/test/dead-letters/retry", None)
        .await;
    assert_eq!((status, body), (StatusCode::OK, json!({ "retried": 1 })));
    until(&app, "/test/deliveries?status=DELIVERED", |d| len(d) == 4).await;
    assert_eq!(
        retry(json!({})).await.1,
        json!({ "retried": 0 }),
        "nothing dead any more"
    );

    assert_eq!(
        app.get("/test/deliveries?status=GONE").await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        retry(json!({ "subscriptionId": uuid::Uuid::nil() }))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        retry(json!({ "everything": true })).await.0,
        StatusCode::BAD_REQUEST
    );
    app.stop().await;

    let mut closed = TestApp::start_with(|c| c.test_control.enabled = false).await;
    assert_eq!(
        closed.call(Method::POST, "/test/reset", None).await.0,
        StatusCode::NOT_FOUND
    );
    closed.stop().await;
}

async fn mine_of(app: &TestApp, subscription: &str) -> String {
    let dead = app
        .get(&format!(
            "/test/deliveries?status=DEAD&subscriptionId={subscription}"
        ))
        .await
        .1;
    dead[0]["deliveryId"].as_str().unwrap().to_string()
}

/// A scenario that resets halfway and repeats its first half ends with the same log as one that ran the first
/// half only.
#[tokio::test]
async fn a_scenario_can_reset_and_start_over() {
    let config = TestApp::start().await.config.clone();
    let half = r#"
  - order: { as: sell, participantId: A, side: SELL, type: LIMIT, priceTicks: 10025, quantity: 20 }
  - order: { as: buy, participantId: B, side: BUY, type: LIMIT, priceTicks: 10025, quantity: 10 }
  - advance_clock: { days: 2 }
  - expect: { settlement: "buy#0", matches: { simulated: { status: SETTLED } } }"#;
    let header = "name: reset\nseed: 4\nsettlement: { mode: simulated, fail_probability: 0, delay_probability: 0 }\n";
    let setup = "instruments: [ { id: ACME, symbol: ACME, currency: DKK, currencyScale: 2 } ]\n\
                 participants: [ { id: A, name: A }, { id: B, name: B } ]\n";
    let register = "  - register: { instruments: [ { id: ACME, symbol: ACME, currency: DKK, currencyScale: 2 } ],\n\
                    \x20              participants: [ { id: A, name: A }, { id: B, name: B } ] }";
    let once = format!("{header}{setup}steps:{half}\n");
    let twice = format!(
        "{header}{setup}steps:{half}\n  - reset: {{}}\n  - expect: {{ events: {{}}, count: 0 }}\n  - expect: {{ order: sell, matches: {{}} }}\n{register}{half}\n"
    );
    let once = scenario::run(&Scenario::parse(&once).unwrap(), &config)
        .await
        .unwrap();
    let twice = scenario::run(&Scenario::parse(&twice).unwrap(), &config)
        .await
        .unwrap();
    assert!(once.passed, "{}", once.render());
    let failed: Vec<(usize, &Vec<String>)> = twice
        .steps
        .iter()
        .filter(|s| !s.passed)
        .map(|s| (s.index, &s.failures))
        .collect();
    assert_eq!(
        failed,
        [(
            7,
            &vec!["unknown order reference sell (name orders with `as:`)".to_string()]
        )],
        "references are forgotten on reset: {}",
        twice.render()
    );
    assert_eq!(
        once.event_log, twice.event_log,
        "the same log, IDs and clock included"
    );
}
