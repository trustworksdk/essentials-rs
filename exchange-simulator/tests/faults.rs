//! Delivery fault injection (plan item 3.3) end to end: a real HTTP receiver sees damaged, duplicated, delayed and
//! reordered copies, `/test/attempts` records exactly what was sent, and the canonical event log never changes.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::post;
use common::TestApp;
use exchange_simulator::scenario::{self, Scenario};
use exchange_simulator::webhooks::signing;
use serde_json::{Value, json};

#[derive(Clone, Debug)]
struct Received {
    sequence: u64,
    raw: Vec<u8>,
    headers: HeaderMap,
    at: Instant,
}

/// Records every request on `/hook` and answers 200.
struct Receiver {
    url: String,
    received: Arc<Mutex<Vec<Received>>>,
}

impl Receiver {
    async fn start() -> Self {
        let received: Arc<Mutex<Vec<Received>>> = Arc::default();
        let app = Router::new()
            .route(
                "/hook",
                post(
                    |State(seen): State<Arc<Mutex<Vec<Received>>>>,
                     headers: HeaderMap,
                     body: axum::body::Bytes| async move {
                        let sequence = headers["X-Event-Sequence"]
                            .to_str()
                            .unwrap()
                            .parse()
                            .unwrap();
                        seen.lock().unwrap().push(Received {
                            sequence,
                            raw: body.to_vec(),
                            headers,
                            at: Instant::now(),
                        });
                        StatusCode::OK
                    },
                ),
            )
            .with_state(received.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, received }
    }

    fn received(&self) -> Vec<Received> {
        self.received.lock().unwrap().clone()
    }

    async fn wait_for(&self, count: usize) -> Vec<Received> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let received = self.received();
            if received.len() >= count || Instant::now() > deadline {
                return received;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Registers a webhook for the receiver and sets its fault policy; returns the subscription ID.
async fn subscribe(app: &TestApp, receiver: &Receiver, extra: Value, faults: Value) -> String {
    let mut body = json!({ "url": receiver.url, "eventTypes": ["TradeExecuted"] });
    body.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let (status, subscription) = app.post("/api/webhooks", body).await;
    assert_eq!(status, StatusCode::CREATED, "{subscription}");
    let id = subscription["id"].as_str().unwrap().to_string();
    let (status, view) = put_faults(app, &id, faults.clone()).await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(view["faults"]["seed"], faults["seed"].as_u64().unwrap_or(0));
    id
}

async fn put_faults(app: &TestApp, id: &str, faults: Value) -> (StatusCode, Value) {
    app.call(
        Method::PUT,
        &format!("/test/webhooks/{id}/faults"),
        Some(faults),
    )
    .await
}

async fn trade(app: &TestApp) {
    assert_eq!(
        app.order("A", "SELL", Some(10025), 10).await.0,
        StatusCode::CREATED
    );
    assert_eq!(
        app.order("B", "BUY", Some(10025), 10).await.0,
        StatusCode::CREATED
    );
}

async fn canonical_trades(app: &TestApp) -> Vec<Value> {
    app.get("/api/events?eventType=TradeExecuted")
        .await
        .1
        .as_array()
        .unwrap()
        .clone()
}

async fn attempts(app: &TestApp, count: usize) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let attempts = app
            .get("/test/attempts")
            .await
            .1
            .as_array()
            .unwrap()
            .clone();
        if attempts.len() >= count || Instant::now() > deadline {
            return attempts;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn field_faults_damage_the_sent_copy_only_and_are_recorded() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let receiver = Receiver::start().await;
    let secret = "a-secret-of-sixteen-plus";
    subscribe(
        &app,
        &receiver,
        json!({ "secret": secret }),
        json!({
            "missing": { "paths": ["payload.buyerId", "payload.instrument.isin"] },
            "nulls": { "paths": ["payload.price"] },
            "invalid": { "paths": ["payload.quantity", "payload.executedAt"] }
        }),
    )
    .await;
    trade(&app).await;

    let received = receiver.wait_for(1).await;
    assert_eq!(received.len(), 1);
    let sent: Value = serde_json::from_slice(&received[0].raw).unwrap();
    let canonical = canonical_trades(&app).await.remove(0);
    let mut expected = canonical.clone();
    let payload = expected["payload"].as_object_mut().unwrap();
    payload.remove("buyerId");
    payload["instrument"]
        .as_object_mut()
        .unwrap()
        .remove("isin");
    payload["price"] = Value::Null;
    payload["quantity"] = json!("not-a-number");
    payload["executedAt"] = json!(-12345);
    assert_eq!(sent, expected, "exactly the configured damage");
    assert_eq!(
        canonical["payload"]["price"], "100.25",
        "the log keeps the canonical event"
    );
    assert_eq!(canonical["payload"]["quantity"], 10);
    assert_eq!(canonical["payload"]["buyerId"], "B");

    // The signature covers the bytes actually sent.
    let headers = &received[0].headers;
    let timestamp: i64 = headers[signing::TIMESTAMP_HEADER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        headers[signing::SIGNATURE_HEADER].to_str().unwrap(),
        signing::sign(secret, timestamp, &received[0].raw)
    );

    let attempts = attempts(&app, 1).await;
    assert_eq!(attempts.len(), 1);
    let attempt = &attempts[0];
    assert_eq!(
        (
            &attempt["outcome"],
            &attempt["attempt"],
            &attempt["httpStatus"],
            &attempt["eventType"]
        ),
        (
            &json!("SENT"),
            &json!(1),
            &json!(200),
            &json!("TradeExecuted")
        )
    );
    assert_eq!(
        attempt["faults"],
        json!([
            "missing:payload.buyerId",
            "missing:payload.instrument.isin",
            "null:payload.price",
            "invalid:payload.quantity",
            "invalid:payload.executedAt"
        ])
    );
    assert_eq!(
        attempt["body"], sent,
        "the recorded body is what the receiver got"
    );
    app.stop().await;
}

#[tokio::test]
async fn duplicates_and_delays_reach_the_receiver() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let receiver = Receiver::start().await;
    subscribe(
        &app,
        &receiver,
        json!({}),
        json!({ "duplicateProbability": 1, "delay": { "ms": 300 } }),
    )
    .await;
    let started = Instant::now();
    trade(&app).await;

    let received = receiver.wait_for(2).await;
    assert_eq!(received.len(), 2, "sent twice");
    assert_eq!(
        received[0].raw, received[1].raw,
        "the same bytes both times"
    );
    assert_eq!(
        received[0].headers["X-Event-Id"],
        received[1].headers["X-Event-Id"]
    );
    assert!(
        received[0].at - started >= Duration::from_millis(300),
        "delayed"
    );
    let canonical = canonical_trades(&app).await.remove(0);
    assert_eq!(
        serde_json::from_slice::<Value>(&received[0].raw).unwrap(),
        canonical,
        "undamaged"
    );

    let attempts = attempts(&app, 2).await;
    assert_eq!(attempts.len(), 2);
    for attempt in &attempts {
        assert_eq!(attempt["attempt"], 1, "a duplicate is the same attempt");
        assert_eq!(attempt["faults"], json!(["duplicate", "delay:300ms"]));
        assert!(attempt.get("body").is_none(), "canonical body: not stored");
    }
    let deliveries = app.get("/test/deliveries").await.1;
    assert_eq!(
        (&deliveries[0]["status"], &deliveries[0]["attempts"]),
        (&json!("DELIVERED"), &json!(1))
    );
    app.stop().await;
}

#[tokio::test]
async fn a_held_delivery_is_overtaken_by_later_events() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let receiver = Receiver::start().await;
    let id = subscribe(
        &app,
        &receiver,
        json!({}),
        json!({ "outOfOrder": { "holdMs": 600 } }),
    )
    .await;
    trade(&app).await;
    let held = attempts(&app, 1).await;
    assert_eq!(
        (&held[0]["outcome"], &held[0]["faults"]),
        (&json!("HELD"), &json!(["held:600ms"]))
    );
    assert!(held[0]["httpStatus"].is_null());
    // Clear the policy, so the next trade goes out at once; the held one is still waiting.
    assert_eq!(
        app.call(Method::DELETE, &format!("/test/webhooks/{id}/faults"), None)
            .await
            .0,
        StatusCode::OK
    );
    trade(&app).await;

    let received = receiver.wait_for(2).await;
    let sequences: Vec<u64> = received.iter().map(|r| r.sequence).collect();
    let canonical: Vec<u64> = canonical_trades(&app)
        .await
        .iter()
        .map(|e| e["sequence"].as_u64().unwrap())
        .collect();
    assert_eq!(
        sequences,
        [canonical[1], canonical[0]],
        "the later trade arrives first"
    );
    let attempts = attempts(&app, 3).await;
    let outcomes: Vec<(&Value, &Value)> = attempts
        .iter()
        .map(|a| (&a["outcome"], &a["attempt"]))
        .collect();
    assert_eq!(
        outcomes,
        [
            (&json!("HELD"), &json!(1)),
            (&json!("SENT"), &json!(1)),
            (&json!("SENT"), &json!(1))
        ],
        "a hold does not use up an attempt"
    );
    let deliveries = app.get("/test/deliveries").await.1;
    assert!(
        deliveries
            .as_array()
            .unwrap()
            .iter()
            .all(|d| d["status"] == "DELIVERED" && d["attempts"] == 1),
        "{deliveries}"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_policy_survives_a_restart_and_holds_happen_once() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let receiver = Receiver::start().await;
    let policy = json!({ "seed": 5, "outOfOrder": { "holdMs": 300 } });
    subscribe(&app, &receiver, json!({}), policy).await;
    app.restart().await;
    let webhooks = app.get("/api/webhooks").await.1;
    assert_eq!(
        webhooks[0]["faults"],
        json!({ "seed": 5, "duplicateProbability": 0.0, "outOfOrder": { "probability": 1.0, "holdMs": 300 } }),
        "stored, with its defaults spelled out"
    );
    trade(&app).await;
    assert_eq!(attempts(&app, 1).await[0]["outcome"], "HELD");
    app.restart().await; // the hold is stored: after a restart the delivery is not held again
    let received = receiver.wait_for(1).await;
    assert_eq!(received.len(), 1);
    let attempts = attempts(&app, 2).await;
    assert_eq!(attempts.len(), 2, "{attempts:?}");
    assert_eq!(attempts[1]["outcome"], "SENT");
    app.stop().await;
}

#[tokio::test]
async fn invalid_policies_are_refused() {
    let mut app = TestApp::start().await;
    let receiver = Receiver::start().await;
    let (_, plain) = app
        .post(
            "/api/webhooks",
            json!({ "url": receiver.url, "eventTypes": ["TradeExecuted"] }),
        )
        .await;
    let (_, ordered) = app
        .post(
            "/api/webhooks",
            json!({ "url": receiver.url, "eventTypes": ["TradeExecuted"], "ordered": true }),
        )
        .await;
    let plain = plain["id"].as_str().unwrap();
    let ordered = ordered["id"].as_str().unwrap();

    let (status, error) = put_faults(&app, plain, json!({ "duplicateProbability": 3 })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("duplicateProbability"),
        "{error}"
    );
    let (status, _) = put_faults(&app, plain, json!({ "dupes": 1 })).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unknown fields are refused"
    );
    let (status, error) =
        put_faults(&app, ordered, json!({ "outOfOrder": { "holdMs": 10 } })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        error["message"].as_str().unwrap().contains("ordered"),
        "{error}"
    );
    assert_eq!(
        put_faults(&app, ordered, json!({ "duplicateProbability": 1 }))
            .await
            .0,
        StatusCode::OK,
        "other faults are fine on an ordered subscription"
    );
    let unknown = uuid::Uuid::nil().to_string();
    assert_eq!(
        put_faults(&app, &unknown, json!({})).await.0,
        StatusCode::NOT_FOUND
    );
    app.stop().await;

    let mut closed = TestApp::start_with(|c| c.test_control.enabled = false).await;
    assert_eq!(
        put_faults(&closed, plain, json!({})).await.0,
        StatusCode::NOT_FOUND
    );
    closed.stop().await;
}

/// The same scenario sends the same damaged copies: faults are drawn from the policy seed and the deterministic
/// delivery IDs.
#[tokio::test]
async fn random_faults_are_reproducible_in_a_scenario() {
    let config = TestApp::start().await.config.clone();
    let mut runs = Vec::new();
    for _ in 0..2 {
        let receiver = Receiver::start().await;
        let text = format!(
            r#"
name: random faults
seed: 11
instruments: [ {{ id: ACME, symbol: ACME, currency: DKK, currencyScale: 2 }} ]
webhooks:
  - {{ url: "{url}", eventTypes: [OrderAccepted, TradeExecuted, OrderCancelled],
      faults: {{ seed: 3, missing: {{ probability: 0.3 }}, nulls: {{ probability: 0.3 }},
                invalid: {{ probability: 0.3 }}, duplicateProbability: 0.2 }} }}
steps:
  - market: {{ orders: 60, participants: 4, cancellation_probability: 0.1,
              instruments: [ {{ id: ACME, price: {{ mid_ticks: 1000, width_ticks: 5 }} }} ] }}
  - expect: {{ attempts: {{ faults: ["duplicate"] }} }}
  - expect: {{ deliveries: {{ status: DELIVERED }}, all: true, wait_ms: 10000 }}
  - set_faults: {{ url: "{url}", faults: null }}
"#,
            url = receiver.url
        );
        let report = scenario::run(&Scenario::parse(&text).unwrap(), &config)
            .await
            .unwrap();
        assert!(report.passed, "{}", report.render());
        // Every delivery was recorded as delivered, so the receiver has had everything.
        let received = receiver.received();
        let mut copies: Vec<(u64, Vec<u8>)> =
            received.into_iter().map(|r| (r.sequence, r.raw)).collect();
        copies.sort();
        runs.push((report.event_log, copies));
    }
    assert_eq!(runs[0].0, runs[1].0, "same canonical log");
    let copies = &runs[0].1;
    let events: std::collections::BTreeSet<u64> =
        copies.iter().map(|(sequence, _)| *sequence).collect();
    assert!(copies.len() > events.len(), "some copies were duplicated");
    assert!(
        events.len() > 60,
        "orders, trades and cancels were all delivered"
    );
    assert_eq!(
        runs[0].1, runs[1].1,
        "the same copies, damage and duplicates included"
    );
}
