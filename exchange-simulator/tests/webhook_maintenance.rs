//! Webhook maintenance through `/api/webhooks/{id}` (step 4.6b): inspect, change, pause, delete, rotate the secret,
//! list deliveries and dead letters, retry and redeliver; and the rules for subscriptions declared in configuration.

mod common;

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::post;
use common::TestApp;
use exchange_simulator::config::SubscriptionConfig;
use exchange_simulator::webhooks::signing::sign;
use serde_json::{Value, json};

/// Every request's headers and body.
type Requests = Arc<Mutex<Vec<(HeaderMap, Vec<u8>)>>>;

/// An endpoint that records every request and answers with a settable status (default 204).
#[derive(Clone)]
struct Receiver {
    url: String,
    received: Requests,
    status: Arc<AtomicU16>,
}

impl Receiver {
    async fn start() -> Self {
        let received: Requests = Arc::default();
        let status = Arc::new(AtomicU16::new(204));
        let app = Router::new().route(
            "/hook",
            post({
                let (received, status) = (received.clone(), status.clone());
                move |headers: HeaderMap, body: axum::body::Bytes| {
                    received.lock().unwrap().push((headers, body.to_vec()));
                    let code = status.load(Ordering::Relaxed);
                    async move { StatusCode::from_u16(code).unwrap() }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            url,
            received,
            status,
        }
    }

    fn answer(&self, status: u16) {
        self.status.store(status, Ordering::Relaxed);
    }

    fn count(&self) -> usize {
        self.received.lock().unwrap().len()
    }

    fn sequences(&self) -> Vec<u64> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .map(|(h, _)| h["x-event-sequence"].to_str().unwrap().parse().unwrap())
            .collect()
    }

    fn last(&self) -> (HeaderMap, Vec<u8>) {
        self.received.lock().unwrap().last().unwrap().clone()
    }
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

async fn eventually_async(what: &str, mut probe: impl AsyncFnMut() -> bool) {
    for _ in 0..500 {
        if probe().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Registers a subscription through the API; returns its ID.
async fn subscribe(app: &TestApp, body: Value) -> String {
    let (status, created) = app.post("/api/webhooks", body).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    created["id"].as_str().unwrap().to_string()
}

/// Rests an order: one `OrderAccepted`.
async fn rest(app: &TestApp, quantity: u64) {
    assert_eq!(
        app.order("A", "SELL", Some(10_100), quantity).await.0,
        StatusCode::CREATED
    );
}

async fn patch(app: &TestApp, id: &str, body: Value) -> (StatusCode, Value) {
    app.call(Method::PATCH, &format!("/api/webhooks/{id}"), Some(body))
        .await
}

#[tokio::test]
async fn detail_shows_delivery_stats_and_dead_letters_can_be_listed_and_retried() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let receiver = Receiver::start().await;
    receiver.answer(500);
    let id = subscribe(
        &app,
        json!({ "url": receiver.url, "eventTypes": ["OrderAccepted"] }),
    )
    .await;
    rest(&app, 1).await;
    rest(&app, 2).await;
    // Five attempts each (max_attempts in the test config), then dead.
    eventually_async("dead letters", async || {
        app.get(&format!("/api/webhooks/{id}")).await.1["deliveries"]["dead"] == 2
    })
    .await;
    let (status, detail) = app.get(&format!("/api/webhooks/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["id"], json!(id));
    assert_eq!(
        detail["deliveries"],
        json!({ "pending": 0, "inFlight": 0, "delivered": 0, "dead": 2, "oldestUnfinished": null })
    );

    let (_, dead) = app
        .get(&format!("/api/webhooks/{id}/deliveries?status=DEAD"))
        .await;
    let dead = dead.as_array().unwrap().clone();
    assert_eq!(dead.len(), 2);
    assert_eq!(dead[0]["attempts"], 5);
    assert!(
        dead[0]["lastError"].as_str().unwrap().contains("500"),
        "{dead:?}"
    );
    let (_, paged) = app
        .get(&format!(
            "/api/webhooks/{id}/deliveries?after={}&limit=1",
            dead[0]["eventSequence"]
        ))
        .await;
    assert_eq!(paged.as_array().unwrap().len(), 1);
    assert_eq!(paged[0]["deliveryId"], dead[1]["deliveryId"]);

    // Retry one, then the rest, once the consumer is back.
    receiver.answer(204);
    let (status, retried) = app
        .post(
            &format!("/api/webhooks/{id}/dead-letters/retry"),
            json!({ "deliveryIds": [dead[0]["deliveryId"]] }),
        )
        .await;
    assert_eq!((status, retried), (StatusCode::OK, json!({ "retried": 1 })));
    eventually_async("one delivered", async || {
        app.get(&format!("/api/webhooks/{id}")).await.1["deliveries"]["delivered"] == 1
    })
    .await;
    let (_, retried) = app
        .call(
            Method::POST,
            &format!("/api/webhooks/{id}/dead-letters/retry"),
            None,
        )
        .await;
    assert_eq!(retried, json!({ "retried": 1 }));
    eventually_async("both delivered", async || {
        app.get(&format!("/api/webhooks/{id}")).await.1["deliveries"]["delivered"] == 2
    })
    .await;

    // Redelivery from a sequence, through the regular API.
    let before = receiver.count();
    let (status, queued) = app
        .post(
            &format!(
                "/api/webhooks/{id}/redeliver?after={}",
                dead[0]["eventSequence"]
            ),
            json!({}),
        )
        .await;
    assert_eq!((status, queued), (StatusCode::OK, json!({ "queued": 1 })));
    eventually("the redelivery", || receiver.count() == before + 1).await;

    for (uri, expected) in [
        (
            format!("/api/webhooks/{id}/deliveries?status=LOST"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/api/webhooks/{id}/deliveries?limit=0"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/api/webhooks/{id}/deliveries?limit=1001"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/api/webhooks/{id}/deliveries?page=2"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/api/webhooks/{}", uuid::Uuid::nil()),
            StatusCode::NOT_FOUND,
        ),
        (
            format!("/api/webhooks/{}/deliveries", uuid::Uuid::nil()),
            StatusCode::NOT_FOUND,
        ),
    ] {
        assert_eq!(app.get(&uri).await.0, expected, "{uri}");
    }
    app.stop().await;
}

#[tokio::test]
async fn a_subscription_can_be_moved_narrowed_paused_disabled_and_deleted() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let (old, new) = (Receiver::start().await, Receiver::start().await);
    old.answer(503);
    let id = subscribe(
        &app,
        json!({ "url": old.url, "eventTypes": ["OrderAccepted", "TradeExecuted"], "ordered": true }),
    )
    .await;
    rest(&app, 1).await;
    eventually("the first attempt", || old.count() >= 1).await;

    // Move it: the failing delivery goes to the new URL on its next attempt.
    let (status, moved) = patch(&app, &id, json!({ "url": new.url })).await;
    assert_eq!(status, StatusCode::OK, "{moved}");
    assert_eq!(moved["url"], json!(new.url));
    eventually("delivery at the new URL", || new.count() == 1).await;

    // Pause: deliveries are recorded and held, then go out in order.
    let (_, paused) = patch(&app, &id, json!({ "paused": true })).await;
    assert_eq!(paused["paused"], true);
    rest(&app, 2).await;
    rest(&app, 3).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(new.count(), 1, "nothing sent while paused");
    let (_, detail) = app.get(&format!("/api/webhooks/{id}")).await;
    assert_eq!(detail["deliveries"]["pending"], 2);
    assert!(detail["deliveries"]["oldestUnfinished"]["eventSequence"].is_u64());
    patch(&app, &id, json!({ "paused": false })).await;
    eventually("held deliveries", || new.count() == 3).await;
    let sequences = new.sequences();
    assert!(
        sequences.windows(2).all(|w| w[0] < w[1]),
        "in order: {sequences:?}"
    );

    // Narrow the event types; disable; enable.
    let (_, narrowed) = patch(&app, &id, json!({ "eventTypes": ["TradeExecuted"] })).await;
    assert_eq!(narrowed["eventTypes"], json!(["TradeExecuted"]));
    rest(&app, 4).await;
    let (_, disabled) = patch(&app, &id, json!({ "enabled": false })).await;
    assert_eq!(disabled["enabled"], false);
    assert_eq!(
        app.order("B", "BUY", Some(10_100), 1).await.0,
        StatusCode::CREATED
    );
    patch(&app, &id, json!({ "enabled": true })).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        new.count(),
        3,
        "narrowed, then off while the trade happened"
    );

    // Invalid changes change nothing.
    for (body, expected) in [
        (json!({ "url": "ftp://x/" }), StatusCode::BAD_REQUEST),
        (json!({ "eventTypes": ["Nope"] }), StatusCode::BAD_REQUEST),
        (json!({ "eventTypes": [] }), StatusCode::BAD_REQUEST),
        (
            json!({ "secret": "x".repeat(20) }),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let (status, error) = patch(&app, &id, body.clone()).await;
        assert!(
            status == expected || status == StatusCode::BAD_REQUEST,
            "{body}: {status} {error}"
        );
    }
    assert_eq!(
        app.get(&format!("/api/webhooks/{id}")).await.1["eventTypes"],
        json!(["TradeExecuted"])
    );

    // A delivery still being retried when the subscription is deleted is never sent afterwards.
    patch(&app, &id, json!({ "eventTypes": ["OrderAccepted"] })).await;
    new.answer(503);
    rest(&app, 5).await;
    eventually("a failing attempt", || new.count() == 4).await;
    new.answer(204);

    // Delete: gone from the API, sends nothing more, history kept.
    let (status, _) = app
        .call(Method::DELETE, &format!("/api/webhooks/{id}"), None)
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(app.get("/api/webhooks").await.1, json!([]));
    let (_, all) = app.get("/api/webhooks?includeDeleted=true").await;
    assert_eq!(
        (all[0]["id"].clone(), all[0]["deleted"].clone()),
        (json!(id), json!(true))
    );
    for (method, uri) in [
        (Method::GET, format!("/api/webhooks/{id}")),
        (Method::DELETE, format!("/api/webhooks/{id}")),
        (Method::PATCH, format!("/api/webhooks/{id}")),
        (Method::GET, format!("/api/webhooks/{id}/deliveries")),
        (Method::POST, format!("/api/webhooks/{id}/redeliver")),
    ] {
        let body = (method == Method::PATCH).then(|| json!({ "paused": true }));
        assert_eq!(
            app.call(method.clone(), &uri, body).await.0,
            StatusCode::NOT_FOUND,
            "{method} {uri}"
        );
    }
    assert_eq!(
        app.order("B", "BUY", Some(10_100), 1).await.0,
        StatusCode::CREATED
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        new.count(),
        4,
        "a deleted subscription sends nothing, not even its retries"
    );
    assert_eq!(
        app.count("SELECT COUNT(*) FROM webhook_deliveries"),
        4,
        "history kept: the four OrderAccepted deliveries, one still pending"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_rotated_secret_is_sent_alongside_the_old_one_during_its_grace_period() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let receiver = Receiver::start().await;
    let (old, new, newer) = (
        "old-secret-0123456789",
        "new-secret-0123456789",
        "newest-secret-0123456789",
    );
    let id = subscribe(
        &app,
        json!({ "url": receiver.url, "eventTypes": ["OrderAccepted"], "secret": old }),
    )
    .await;
    let signatures = |headers: &HeaderMap| -> Vec<String> {
        headers["x-signature"]
            .to_str()
            .unwrap()
            .split(", ")
            .map(str::to_string)
            .collect()
    };

    let (status, rotated) = app
        .post(
            &format!("/api/webhooks/{id}/secret"),
            json!({ "secret": new, "graceSeconds": 600 }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{rotated}");
    assert!(rotated["previousSecretValidUntil"].is_string(), "{rotated}");
    assert!(rotated.get("secret").is_none());
    rest(&app, 1).await;
    eventually("a delivery", || receiver.count() == 1).await;
    let (headers, body) = receiver.last();
    let timestamp: i64 = headers["x-signature-timestamp"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        signatures(&headers),
        [sign(new, timestamp, &body), sign(old, timestamp, &body)],
        "the new signature first, then the previous one"
    );

    // Without a grace period the previous secret is dropped at once.
    let (_, rotated) = app
        .post(
            &format!("/api/webhooks/{id}/secret"),
            json!({ "secret": newer, "graceSeconds": 0 }),
        )
        .await;
    assert!(
        rotated.get("previousSecretValidUntil").is_none(),
        "{rotated}"
    );
    rest(&app, 2).await;
    eventually("a second delivery", || receiver.count() == 2).await;
    let (headers, body) = receiver.last();
    let timestamp: i64 = headers["x-signature-timestamp"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(signatures(&headers), [sign(newer, timestamp, &body)]);

    // A grace period ends: afterwards only the current secret signs.
    let (_, rotated) = app
        .post(
            &format!("/api/webhooks/{id}/secret"),
            json!({ "secret": new, "graceSeconds": 1 }),
        )
        .await;
    assert!(rotated["previousSecretValidUntil"].is_string());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    rest(&app, 3).await;
    eventually("a third delivery", || receiver.count() == 3).await;
    let (headers, body) = receiver.last();
    let timestamp: i64 = headers["x-signature-timestamp"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        signatures(&headers),
        [sign(new, timestamp, &body)],
        "the grace period is over"
    );

    // An unsigned subscription becomes signed.
    let unsigned = subscribe(
        &app,
        json!({ "url": receiver.url, "eventTypes": ["OrderCancelled"] }),
    )
    .await;
    let (_, signed) = app
        .post(
            &format!("/api/webhooks/{unsigned}/secret"),
            json!({ "secret": new }),
        )
        .await;
    assert_eq!(
        (
            signed["signed"].clone(),
            signed.get("previousSecretValidUntil")
        ),
        (json!(true), None)
    );

    for (body, expected) in [
        (json!({ "secret": "short" }), StatusCode::BAD_REQUEST),
        (
            json!({ "secret": new, "graceSeconds": 604_801 }),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({ "secret": new, "extra": 1 }),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (json!({}), StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        let (status, error) = app
            .post(&format!("/api/webhooks/{id}/secret"), body.clone())
            .await;
        assert!(
            status == expected || status == StatusCode::BAD_REQUEST,
            "{body}: {status} {error}"
        );
    }
    app.stop().await;
}

#[tokio::test]
async fn declared_subscriptions_take_only_a_pause_through_the_api() {
    let receiver = Receiver::start().await;
    let (old, new) = ("declared-old-0123456789", "declared-new-0123456789");
    let url = receiver.url.clone();
    let mut app = TestApp::start_with(move |c| {
        c.webhooks.subscriptions = vec![SubscriptionConfig {
            name: "settlement".into(),
            url,
            event_types: vec!["OrderAccepted".into()],
            secret: Some(old.into()),
            secret_env: None,
            secret_file: None,
            ordered: false,
            enabled: true,
            paused: false,
        }];
    })
    .await;
    app.seed().await;
    let id = app.get("/api/webhooks").await.1[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    for (method, uri, body) in [
        (
            Method::PATCH,
            format!("/api/webhooks/{id}"),
            Some(json!({ "url": "http://x/" })),
        ),
        (
            Method::PATCH,
            format!("/api/webhooks/{id}"),
            Some(json!({ "enabled": false })),
        ),
        (Method::DELETE, format!("/api/webhooks/{id}"), None),
        (
            Method::POST,
            format!("/api/webhooks/{id}/secret"),
            Some(json!({ "secret": new })),
        ),
    ] {
        let (status, error) = app.call(method.clone(), &uri, body).await;
        assert_eq!(
            (status, error["error"].clone()),
            (StatusCode::CONFLICT, json!("managed_by_configuration")),
            "{method} {uri}: {error}"
        );
        assert!(error["message"].as_str().unwrap().contains("settlement"));
    }
    let (status, paused) = patch(&app, &id, json!({ "paused": true })).await;
    assert_eq!(
        (status, paused["paused"].clone()),
        (StatusCode::OK, json!(true))
    );
    rest(&app, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(receiver.count(), 0, "paused through the API");

    // A restart reapplies the configuration: the pause ends, the held delivery goes out. The configuration also
    // changes the secret, so for the grace period both signatures are sent.
    app.config.webhooks.subscriptions[0].secret = Some(new.into());
    app.restart().await;
    let (_, after) = app.get(&format!("/api/webhooks/{id}")).await;
    assert_eq!(after["paused"], false);
    assert!(after["previousSecretValidUntil"].is_string(), "{after}");
    eventually("the held delivery", || receiver.count() == 1).await;
    let (headers, body) = receiver.last();
    let timestamp: i64 = headers["x-signature-timestamp"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        headers["x-signature"].to_str().unwrap(),
        format!(
            "{}, {}",
            sign(new, timestamp, &body),
            sign(old, timestamp, &body)
        )
    );

    // A second restart with the same secret keeps the grace period running.
    app.restart().await;
    assert_eq!(
        app.get(&format!("/api/webhooks/{id}")).await.1["previousSecretValidUntil"],
        after["previousSecretValidUntil"]
    );
    app.stop().await;
}
