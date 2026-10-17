//! Webhook subscriptions declared in configuration (step 4.6a): created and updated by name at startup, disabled when
//! removed, back after a reset, and never touching subscriptions created through the API.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use common::TestApp;
use exchange_simulator::config::{Config, SubscriptionConfig};
use serde_json::{Value, json};

/// Every request's headers and body.
type Requests = Arc<Mutex<Vec<(HeaderMap, Vec<u8>)>>>;

/// An endpoint that records the headers and body of every request and answers 204.
#[derive(Clone, Default)]
struct Receiver {
    url: String,
    received: Requests,
}

impl Receiver {
    async fn start() -> Self {
        let mut receiver = Self::default();
        let received = receiver.received.clone();
        let app = Router::new().route(
            "/hook",
            post(move |headers: HeaderMap, body: axum::body::Bytes| {
                received.lock().unwrap().push((headers, body.to_vec()));
                async { StatusCode::NO_CONTENT }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        receiver.url = format!("http://{}/hook", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        receiver
    }

    fn count(&self) -> usize {
        self.received.lock().unwrap().len()
    }

    fn event_types(&self) -> Vec<String> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .map(|(h, _)| h["x-event-type"].to_str().unwrap().to_string())
            .collect()
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

fn declared(name: &str, url: &str, event_types: &[&str]) -> SubscriptionConfig {
    SubscriptionConfig {
        name: name.into(),
        url: url.into(),
        event_types: event_types.iter().map(|t| t.to_string()).collect(),
        secret: None,
        secret_env: None,
        secret_file: None,
        ordered: false,
        enabled: true,
        paused: false,
    }
}

async fn trade(app: &TestApp) {
    assert_eq!(
        app.order("A", "SELL", Some(10025), 20).await.0,
        StatusCode::CREATED
    );
    assert_eq!(
        app.order("B", "BUY", Some(10025), 10).await.0,
        StatusCode::CREATED
    );
}

async fn webhooks(app: &TestApp) -> Vec<Value> {
    app.get("/api/webhooks").await.1.as_array().unwrap().clone()
}

fn by_name<'a>(list: &'a [Value], name: &str) -> &'a Value {
    list.iter()
        .find(|s| s["name"] == name)
        .unwrap_or_else(|| panic!("{name} in {list:?}"))
}

#[tokio::test]
async fn declared_subscriptions_are_created_signed_and_delivered() {
    let receiver = Receiver::start().await;
    let dir = std::env::temp_dir().join(format!("exchange-secret-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let secret_file = dir.join("settlement");
    std::fs::write(&secret_file, "a-secret-from-a-file-0123\n").unwrap();
    let url = receiver.url.clone();
    let mut app = TestApp::start_with(move |c| {
        c.webhooks.subscriptions = vec![
            SubscriptionConfig {
                secret_file: Some(secret_file.to_string_lossy().into_owned()),
                ordered: true,
                ..declared("settlement", &url, &["TradeExecuted"])
            },
            SubscriptionConfig {
                paused: true,
                ..declared("paused", &url, &["OrderAccepted"])
            },
            SubscriptionConfig {
                enabled: false,
                ..declared("off", &url, &["OrderAccepted"])
            },
        ];
    })
    .await;
    let list = webhooks(&app).await;
    assert_eq!(list.len(), 3);
    let settlement = by_name(&list, "settlement");
    assert_eq!(
        (
            &settlement["source"],
            &settlement["signed"],
            &settlement["ordered"],
            &settlement["enabled"]
        ),
        (&json!("config"), &json!(true), &json!(true), &json!(true))
    );
    assert!(
        settlement.get("secret").is_none(),
        "the secret is never shown"
    );
    let paused = by_name(&list, "paused");
    assert_eq!(
        (&paused["paused"], &paused["enabled"]),
        (&json!(true), &json!(true))
    );
    assert_eq!(by_name(&list, "off")["enabled"], false);

    app.seed().await;
    trade(&app).await;
    eventually("the trade webhook", || receiver.count() == 1).await;
    let (headers, body) = receiver.received.lock().unwrap()[0].clone();
    assert_eq!(headers["x-event-type"], "TradeExecuted");
    let timestamp: i64 = headers["x-signature-timestamp"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        headers["x-signature"].to_str().unwrap(),
        exchange_simulator::webhooks::signing::sign("a-secret-from-a-file-0123", timestamp, &body),
        "signed with the file's secret, trailing newline ignored"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        receiver.count(),
        1,
        "the paused and the disabled subscription received nothing"
    );
    let pending = |name: &str| {
        app.count(&format!(
            "SELECT COUNT(*) FROM webhook_deliveries d JOIN webhook_subscriptions s ON s.id = d.subscription_id
             WHERE s.name = '{name}' AND d.status = 'PENDING'"
        ))
    };
    assert_eq!(
        pending("paused"),
        2,
        "the paused subscription's deliveries wait (two orders accepted)"
    );
    assert_eq!(pending("off"), 0, "a disabled subscription gets none");
    app.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_restart_reconciles_by_name_and_leaves_api_subscriptions_alone() {
    let first = Receiver::start().await;
    let second = Receiver::start().await;
    let (url1, url2) = (first.url.clone(), second.url.clone());
    let mut app = TestApp::start_with(move |c| {
        c.webhooks.subscriptions = vec![
            declared("trades", &url1, &["TradeExecuted"]),
            declared("orders", &url1, &["OrderAccepted"]),
            SubscriptionConfig {
                paused: true,
                ..declared("later", &url1, &["OrderAccepted"])
            },
        ];
    })
    .await;
    let (status, api) = app
        .post(
            "/api/webhooks",
            json!({ "url": first.url, "eventTypes": ["OrderCancelled"] }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!((&api["source"], api.get("name")), (&json!("api"), None));
    let before = webhooks(&app).await;
    app.seed().await;
    trade(&app).await;
    // 2 OrderAccepted for "orders", 1 TradeExecuted for "trades".
    eventually("first deliveries", || first.count() == 3).await;

    // The configuration changes: "trades" moves to the second receiver and also wants OrderAccepted, "orders" is
    // removed, "later" is unpaused (and moves too), and "fresh" is new.
    app.config.webhooks.subscriptions = vec![
        declared("trades", &url2, &["OrderAccepted", "TradeExecuted"]),
        declared("later", &url2, &["OrderAccepted"]),
        declared("fresh", &url2, &["TradeExecuted"]),
    ];
    app.restart().await;
    let after = webhooks(&app).await;
    assert_eq!(after.len(), 5, "nothing is deleted");
    let trades = by_name(&after, "trades");
    assert_eq!(
        trades["id"],
        by_name(&before, "trades")["id"],
        "same subscription, updated"
    );
    assert_eq!(trades["url"], json!(second.url));
    assert_eq!(
        trades["eventTypes"],
        json!(["OrderAccepted", "TradeExecuted"])
    );
    assert_eq!(
        by_name(&after, "orders")["enabled"],
        false,
        "removed: disabled"
    );
    assert_eq!(by_name(&after, "fresh")["source"], "config");
    let api_after = after.iter().find(|s| s["id"] == api["id"]).unwrap();
    assert_eq!(api_after, &api, "the API subscription is untouched");

    // "later" was paused with two OrderAccepted waiting; unpaused, they go out to its new URL, along with what the new
    // trade produces.
    trade(&app).await;
    eventually("deliveries after the restart", || {
        second.count() == 2 + 2 + 2 + 1 + 1
    })
    .await;
    let mut types = second.event_types();
    types.sort();
    assert_eq!(
        types,
        [
            "OrderAccepted",
            "OrderAccepted",
            "OrderAccepted",
            "OrderAccepted",
            "OrderAccepted",
            "OrderAccepted",
            "TradeExecuted",
            "TradeExecuted"
        ]
    );
    assert_eq!(
        first.count(),
        3,
        "the removed subscription received nothing more"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_reset_brings_declared_subscriptions_back() {
    let receiver = Receiver::start().await;
    let url = receiver.url.clone();
    let mut app = TestApp::start_with(move |c| {
        c.webhooks.subscriptions = vec![declared("trades", &url, &["TradeExecuted"])];
    })
    .await;
    let (status, _) = app
        .post(
            "/api/webhooks",
            json!({ "url": receiver.url, "eventTypes": ["OrderAccepted"] }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = app.post("/test/reset", json!({})).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let list = webhooks(&app).await;
    assert_eq!(
        list.len(),
        1,
        "the API subscription is gone, the declared one is back"
    );
    assert_eq!(list[0]["name"], "trades");
    app.seed().await;
    trade(&app).await;
    eventually("delivery after the reset", || receiver.count() == 1).await;
    app.stop().await;
}

#[tokio::test]
async fn invalid_declarations_stop_the_service_from_starting() {
    let start = |adjust: Box<dyn FnOnce(&mut Config)>| {
        let dir = std::env::temp_dir().join(format!("exchange-bad-{}", uuid::Uuid::now_v7()));
        let mut config = TestApp::config(&dir);
        adjust(&mut config);
        let result = exchange_simulator::app::App::start(&config).map(drop);
        let _ = std::fs::remove_dir_all(&dir);
        format!("{:#}", result.expect_err("start fails"))
    };
    let url = "http://127.0.0.1:9/hook";
    for (adjust, expected) in [
        (
            Box::new(move |c: &mut Config| {
                c.webhooks.subscriptions = vec![
                    declared("twice", url, &["TradeExecuted"]),
                    declared("twice", url, &["OrderAccepted"]),
                ]
            }) as Box<dyn FnOnce(&mut Config)>,
            "name twice is declared twice",
        ),
        (
            Box::new(move |c: &mut Config| {
                c.webhooks.subscriptions = vec![declared("no spaces", url, &["TradeExecuted"])]
            }),
            "must be 1 to 64 letters",
        ),
        (
            Box::new(move |c: &mut Config| {
                c.webhooks.subscriptions = vec![declared("bad-type", url, &["Nope"])]
            }),
            "webhooks.subscriptions bad-type: unknown event type Nope",
        ),
        (
            Box::new(move |c: &mut Config| {
                c.webhooks.subscriptions = vec![declared("ftp", "ftp://host/x", &["TradeExecuted"])]
            }),
            "webhooks.subscriptions ftp: url must be http or https",
        ),
        (
            Box::new(move |c: &mut Config| {
                c.webhooks.subscriptions = vec![SubscriptionConfig {
                    secret_env: Some("EXCHANGE_TEST_NO_SUCH_VARIABLE".into()),
                    ..declared("env", url, &["TradeExecuted"])
                }]
            }),
            "secret_env EXCHANGE_TEST_NO_SUCH_VARIABLE is not set",
        ),
        (
            Box::new(move |c: &mut Config| {
                c.webhooks.subscriptions = vec![SubscriptionConfig {
                    secret: Some("x".repeat(20)),
                    secret_env: Some("HOME".into()),
                    ..declared("both", url, &["TradeExecuted"])
                }]
            }),
            "set at most one of secret, secret_env and secret_file",
        ),
        (
            Box::new(move |c: &mut Config| {
                c.webhooks.subscriptions = vec![SubscriptionConfig {
                    secret: Some("short".into()),
                    ..declared("short", url, &["TradeExecuted"])
                }]
            }),
            "secret must be 16 to 256 characters",
        ),
    ] {
        let message = start(adjust);
        assert!(
            message.contains(expected),
            "{message} should mention {expected}"
        );
    }
}
