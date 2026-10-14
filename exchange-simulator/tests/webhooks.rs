//! Webhook delivery end to end: a real HTTP receiver, the delivery worker, retries, dead letters, restart recovery,
//! and event replay.

mod common;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use common::TestApp;
use serde_json::{Value, json};

#[derive(Clone, Debug)]
struct Received {
    body: Value,
    /// The body bytes exactly as received, for signature checks.
    raw: Vec<u8>,
    headers: HeaderMap,
}

#[derive(Clone, Default)]
struct ReceiverState {
    received: Arc<Mutex<Vec<Received>>>,
    /// Status codes to answer with, in order; when empty, `fallback`.
    script: Arc<Mutex<VecDeque<u16>>>,
    fallback: Arc<Mutex<u16>>,
}

/// An HTTP endpoint that records every request.
struct Receiver {
    url: String,
    state: ReceiverState,
}

impl Receiver {
    async fn start() -> Self {
        let state = ReceiverState {
            fallback: Arc::new(Mutex::new(200)),
            ..Default::default()
        };
        let app = Router::new()
            .route("/hook", post(receive))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, state }
    }

    fn respond(&self, script: &[u16], fallback: u16) {
        *self.state.script.lock().unwrap() = script.iter().copied().collect();
        *self.state.fallback.lock().unwrap() = fallback;
    }

    fn received(&self) -> Vec<Received> {
        self.state.received.lock().unwrap().clone()
    }
}

async fn receive(
    State(state): State<ReceiverState>,
    headers: HeaderMap,
    raw: axum::body::Bytes,
) -> StatusCode {
    let body = serde_json::from_slice(&raw).expect("JSON body");
    state.received.lock().unwrap().push(Received {
        body,
        raw: raw.to_vec(),
        headers,
    });
    let status = state
        .script
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or(*state.fallback.lock().unwrap());
    StatusCode::from_u16(status).unwrap()
}

async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..500 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn subscribe(app: &TestApp, url: &str, event_types: &[&str]) -> Value {
    let (status, body) = app
        .post(
            "/api/webhooks",
            json!({ "url": url, "eventTypes": event_types }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body
}

/// Acceptance steps 2 to 6: a resting sell of 200 and a crossing buy of 150.
async fn trade(app: &TestApp) {
    assert_eq!(
        app.order("A", "SELL", Some(10025), 200).await.0,
        StatusCode::CREATED
    );
    assert_eq!(
        app.order("B", "BUY", Some(10025), 150).await.0,
        StatusCode::CREATED
    );
}

fn header(received: &Received, name: &str) -> String {
    received.headers[name].to_str().unwrap().to_string()
}

#[tokio::test]
async fn trade_event_is_delivered_with_a_stable_event_id() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let receiver = Receiver::start().await;
    let other = Receiver::start().await;
    let subscription = subscribe(&app, &receiver.url, &["TradeExecuted"]).await;
    subscribe(&app, &other.url, &["TradeExecuted", "OrderFilled"]).await;
    trade(&app).await;

    eventually("trade delivery", || receiver.received().len() == 1).await;
    eventually("both events to the second subscriber", || {
        other.received().len() == 2
    })
    .await;
    let delivered = &receiver.received()[0];
    let (_, replay) = app.get("/api/events?eventType=TradeExecuted").await;
    assert_eq!(
        delivered.body, replay[0],
        "the delivered envelope is the canonical event"
    );
    let event_id = delivered.body["eventId"].as_str().unwrap();
    assert_eq!(header(delivered, "x-event-id"), event_id);
    assert_eq!(header(delivered, "x-event-type"), "TradeExecuted");
    assert_eq!(header(delivered, "x-delivery-attempt"), "1");
    assert_eq!(
        header(delivered, "x-subscription-id"),
        subscription["id"].as_str().unwrap()
    );
    assert_eq!(
        header(delivered, "x-event-sequence"),
        delivered.body["sequence"].to_string()
    );
    let payload = &delivered.body["payload"];
    assert_eq!(
        (&payload["quantity"], &payload["price"], &payload["buyerId"]),
        (&json!(150), &json!("100.25"), &json!("B"))
    );
    assert_eq!(payload["instrument"]["mic"], "XCSE");

    eventually("delivery recorded", || {
        app.count(
            "SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'DELIVERED' AND attempts = 1",
        ) == 3
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        receiver.received().len(),
        1,
        "order events are not sent to a TradeExecuted-only subscriber"
    );
    app.stop().await;
}

#[tokio::test]
async fn failed_deliveries_are_retried_with_the_same_event_and_no_new_trades() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let receiver = Receiver::start().await;
    receiver.respond(&[500, 503], 200);
    subscribe(&app, &receiver.url, &["TradeExecuted"]).await;
    trade(&app).await;

    eventually("third attempt", || receiver.received().len() == 3).await;
    let received = receiver.received();
    let ids: Vec<_> = received.iter().map(|r| r.body["eventId"].clone()).collect();
    assert!(
        ids.iter().all(|id| *id == ids[0]),
        "every attempt carries the same eventId"
    );
    let attempts: Vec<_> = received
        .iter()
        .map(|r| header(r, "x-delivery-attempt"))
        .collect();
    assert_eq!(attempts, ["1", "2", "3"]);
    eventually("delivered after retries", || {
        app.count(
            "SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'DELIVERED' AND attempts = 3",
        ) == 1
    })
    .await;
    assert_eq!(
        app.count("SELECT COUNT(*) FROM executions"),
        1,
        "retries never create trades"
    );
    assert_eq!(
        app.count("SELECT COUNT(*) FROM events WHERE event_type = 'TradeExecuted'"),
        1
    );
    app.stop().await;
}

#[tokio::test]
async fn deliveries_are_dead_lettered_after_max_attempts() {
    let mut app = TestApp::start_with(|c| c.webhooks.max_attempts = 3).await;
    app.seed().await;
    let receiver = Receiver::start().await;
    receiver.respond(&[], 500);
    subscribe(&app, &receiver.url, &["TradeExecuted"]).await;
    trade(&app).await;

    eventually("dead letter", || {
        app.count("SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'DEAD'") == 1
    })
    .await;
    let (attempts, error): (i64, String) = app
        .db()
        .query_row(
            "SELECT attempts, last_error FROM webhook_deliveries",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        (attempts, error.as_str()),
        (3, "HTTP 500 Internal Server Error")
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        receiver.received().len(),
        3,
        "no attempts after dead-lettering"
    );
    app.stop().await;
}

#[tokio::test]
async fn undelivered_and_claimed_deliveries_resume_after_a_restart() {
    let mut app = TestApp::start_with(|c| c.webhooks.max_attempts = 1000).await;
    app.seed().await;
    let receiver = Receiver::start().await;
    receiver.respond(&[], 500);
    subscribe(&app, &receiver.url, &["TradeExecuted"]).await;
    trade(&app).await;
    eventually("a failed attempt", || !receiver.received().is_empty()).await;
    app.stop().await;

    // As if the process died holding a claim: in flight, lease far in the future.
    app.db()
        .execute("UPDATE webhook_deliveries SET status = 'IN_FLIGHT', next_attempt_at = '9999-12-31T00:00:00.000000Z'", [])
        .unwrap();
    receiver.respond(&[], 200);
    app.restart().await;

    eventually("delivery after restart", || {
        app.count("SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'DELIVERED'") == 1
    })
    .await;
    let received = receiver.received();
    assert!(received.len() >= 2);
    assert!(
        received
            .iter()
            .all(|r| r.body["eventId"] == received[0].body["eventId"])
    );
    assert_eq!(app.count("SELECT COUNT(*) FROM executions"), 1);
    app.stop().await;
}

#[tokio::test]
async fn events_can_be_replayed_by_sequence() {
    let mut app = TestApp::start_with(|c| c.events.simulation_id = Some("sim-001".into())).await;
    app.seed().await;
    trade(&app).await;

    let (status, all) = app.get("/api/events").await;
    assert_eq!(status, StatusCode::OK);
    let all = all.as_array().unwrap().clone();
    let types: Vec<_> = all
        .iter()
        .map(|e| e["eventType"].as_str().unwrap())
        .collect();
    assert_eq!(
        types,
        [
            "OrderAccepted",
            "OrderAccepted",
            "TradeExecuted",
            "OrderPartiallyFilled",
            "OrderFilled"
        ]
    );
    let first = &all[0];
    assert_eq!(
        (
            &first["schemaVersion"],
            &first["source"],
            &first["simulationId"]
        ),
        (&json!(1), &json!("exchange"), &json!("sim-001"))
    );
    assert!(
        first["occurredAt"].is_string()
            && first["correlationId"].is_string()
            && first["aggregateId"].is_string()
    );

    let cursor = all[1]["sequence"].as_u64().unwrap();
    let (_, rest) = app
        .get(&format!("/api/events?after={cursor}&limit=2"))
        .await;
    assert_eq!(rest.as_array().unwrap(), &all[2..4]);
    let (_, none) = app
        .get(&format!("/api/events?after={}", all[4]["sequence"]))
        .await;
    assert_eq!(none, json!([]));
    assert_eq!(
        app.get("/api/events?limit=0").await.0,
        StatusCode::BAD_REQUEST
    );

    app.restart().await;
    assert_eq!(
        app.get("/api/events").await.1.as_array().unwrap(),
        &all,
        "replay is stable across restarts"
    );
    app.stop().await;
}

#[tokio::test]
async fn webhook_registration_is_validated_and_listed() {
    let mut app = TestApp::start().await;
    let (status, created) =
        app.post("/api/webhooks", json!({ "url": "http://consumer/hook", "eventTypes": ["TradeExecuted", "TradeExecuted"] })).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        (&created["eventTypes"], &created["enabled"]),
        (&json!(["TradeExecuted"]), &json!(true))
    );
    for bad in [
        json!({ "url": "not a url", "eventTypes": ["TradeExecuted"] }),
        json!({ "url": "ftp://consumer", "eventTypes": ["TradeExecuted"] }),
        json!({ "url": "http://consumer", "eventTypes": [] }),
        json!({ "url": "http://consumer", "eventTypes": ["SettlementCompleted"] }),
    ] {
        assert_eq!(
            app.post("/api/webhooks", bad.clone()).await.0,
            StatusCode::BAD_REQUEST,
            "{bad}"
        );
    }
    let (_, listed) = app.get("/api/webhooks").await;
    assert_eq!(listed, json!([created]));
    app.stop().await;
}

/// The value of a sample line `name{labels} value` (labels given exactly as rendered, or empty).
fn sample(metrics: &str, series: &str) -> Option<f64> {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
}

#[tokio::test]
async fn metrics_expose_commands_trades_latencies_and_the_outbox() {
    // Counters are process-wide and other tests in this binary also trade, so assert lower bounds; this is the only
    // test here that scrapes, so the outbox gauge reflects this test's database.
    let mut app = TestApp::start().await;
    app.seed().await;
    let receiver = Receiver::start().await;
    receiver.respond(&[500], 200);
    subscribe(&app, &receiver.url, &["TradeExecuted"]).await;
    trade(&app).await;
    eventually("delivery", || {
        app.count("SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'DELIVERED'") == 1
    })
    .await;

    let text = metrics_text(&app).await;
    assert!(
        sample(&text, "exchange_executions_total").unwrap() >= 1.0,
        "{text}"
    );
    assert!(sample(&text, "exchange_traded_quantity_total").unwrap() >= 150.0);
    assert!(sample(&text, r#"exchange_orders_total{status="FILLED"}"#).unwrap() >= 1.0);
    assert!(
        sample(
            &text,
            r#"exchange_commands_total{command="submit",outcome="ok"}"#
        )
        .unwrap()
            >= 2.0
    );
    assert!(
        sample(
            &text,
            r#"exchange_events_total{event_type="TradeExecuted"}"#
        )
        .unwrap()
            >= 1.0
    );
    assert!(sample(&text, r#"webhook_delivery_attempts_total{outcome="retry"}"#).unwrap() >= 1.0);
    assert!(
        sample(
            &text,
            r#"webhook_delivery_attempts_total{outcome="delivered"}"#
        )
        .unwrap()
            >= 1.0
    );
    assert_eq!(
        sample(&text, r#"webhook_outbox_deliveries{status="DELIVERED"}"#),
        Some(1.0)
    );
    assert_eq!(
        sample(&text, r#"webhook_outbox_deliveries{status="PENDING"}"#),
        Some(0.0)
    );
    for histogram in [
        "exchange_match_duration_seconds_count",
        "exchange_commit_duration_seconds_count",
        r#"exchange_command_duration_seconds_count{command="submit"}"#,
        "webhook_delivery_duration_seconds_count",
    ] {
        assert!(
            sample(&text, histogram).unwrap_or(0.0) >= 1.0,
            "{histogram} in {text}"
        );
    }
    assert!(text.contains("# HELP exchange_commit_duration_seconds"));
    app.stop().await;
}

async fn metrics_text(app: &TestApp) -> String {
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let response = app
        .router()
        .oneshot(
            axum::http::Request::get("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.headers()["content-type"],
        "text/plain; version=0.0.4"
    );
    String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap()
}

/// Recomputes an `X-Signature` with the `hmac` crate directly, independently of the service's signing code.
fn expected_signature(secret: &str, timestamp: &str, raw: &[u8]) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{timestamp}.").as_bytes());
    mac.update(raw);
    let hex: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("sha256={hex}")
}

#[tokio::test]
async fn deliveries_to_a_subscription_with_a_secret_are_signed() {
    const SECRET: &str = "whsec_e2e_0123456789abcdef";
    let mut app = TestApp::start().await;
    app.seed().await;
    let signed = Receiver::start().await;
    signed.respond(&[500], 200);
    let unsigned = Receiver::start().await;
    let (status, created) = app
        .post(
            "/api/webhooks",
            json!({ "url": signed.url, "eventTypes": ["TradeExecuted"], "secret": SECRET }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["signed"], true);
    assert!(
        created.get("secret").is_none(),
        "the secret is never returned"
    );
    subscribe(&app, &unsigned.url, &["TradeExecuted"]).await;
    trade(&app).await;

    eventually("a failed and a successful signed attempt", || {
        signed.received().len() == 2
    })
    .await;
    eventually("the unsigned delivery", || unsigned.received().len() == 1).await;
    let now = chrono::Utc::now().timestamp();
    for attempt in signed.received() {
        let timestamp = header(&attempt, "x-signature-timestamp");
        assert!(
            (now - timestamp.parse::<i64>().unwrap()).abs() < 60,
            "timestamp is the attempt time"
        );
        assert_eq!(
            header(&attempt, "x-signature"),
            expected_signature(SECRET, &timestamp, &attempt.raw)
        );
        assert_eq!(attempt.body["eventType"], "TradeExecuted");
    }
    let attempts = signed.received();
    assert_eq!(
        attempts[0].raw, attempts[1].raw,
        "a retry sends the identical body"
    );
    let delivered = &unsigned.received()[0];
    assert!(delivered.headers.get("x-signature").is_none());
    assert!(delivered.headers.get("x-signature-timestamp").is_none());

    let (_, listed) = app.get("/api/webhooks").await;
    let signed_flags: Vec<_> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["signed"].clone())
        .collect();
    assert_eq!(signed_flags, [json!(true), json!(false)]);
    assert!(
        !listed.to_string().contains(SECRET),
        "the secret is not listed"
    );

    let (status, _) = app
        .post(
            "/api/webhooks",
            json!({ "url": signed.url, "eventTypes": ["TradeExecuted"], "secret": "short" }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    app.restart().await;
    let (_, listed) = app.get("/api/webhooks").await;
    assert_eq!(listed[0]["signed"], true, "the secret survives a restart");
    app.stop().await;
}
