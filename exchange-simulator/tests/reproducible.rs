//! Deterministic IDs (plan item 3.0, decision D6): with `ids.mode: deterministic`, a frozen virtual clock and the same
//! commands, two runs produce byte-identical event logs and API responses, with or without restarts in between.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, IdMode, SettlementMode};
use exchange_simulator::ids::IdScheme;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

fn deterministic(mode: IdMode) -> impl FnOnce(&mut Config) {
    move |c: &mut Config| {
        c.ids.mode = mode;
        c.ids.seed = 2030;
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
        c.settlement.mode = SettlementMode::Both;
        c.settlement.tick_interval_ms = 10;
        c.settlement.seed = 11;
        c.settlement.fail_probability = 0.2;
        c.settlement.delay_probability = 0.3;
        c.settlement.correct_probability = 0.3;
    }
}

/// The exact response bytes of a GET.
async fn raw(app: &TestApp, uri: &str) -> String {
    let response = app
        .router()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
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

async fn ok(app: &TestApp, uri: &str, body: Value) -> Value {
    let (status, response) = app.post(uri, body.clone()).await;
    assert!(status.is_success(), "{uri} {body} -> {status} {response}");
    response
}

/// Everything observable that should be reproducible.
#[derive(Debug, PartialEq)]
struct Snapshot {
    events: String,
    trades: String,
    webhooks: String,
    settlements: String,
    book: String,
    orders: Vec<String>,
    settlement_details: Vec<String>,
    deliveries: Vec<(String, String, i64)>,
    responses: Vec<Value>,
}

/// A script touching every kind of ID: orders (incl. client IDs, a self-trade rejection, a market order, a cancel),
/// executions, events of all three sources, webhook subscriptions (signed and ordered) and their deliveries, external
/// reports, and settlement transitions over clock moves. `restarts` restarts the service at two points.
async fn run(mode: IdMode, restarts: bool) -> Snapshot {
    let mut app = TestApp::start_with(deterministic(mode)).await;
    let mut responses = Vec::new();
    ok(&app, "/api/instruments", common::acme()).await;
    for (id, settlement) in [
        ("A", json!({ "account": "A-1" })),
        ("B", Value::Null),
        ("C", json!({ "bic": "X" })),
    ] {
        ok(
            &app,
            "/api/participants",
            json!({ "id": id, "name": id, "settlement": settlement }),
        )
        .await;
    }
    // Unreachable consumers: deliveries are queued (with IDs) but never succeed.
    responses.push(
        ok(&app, "/api/webhooks", json!({ "url": "http://127.0.0.1:9/a", "eventTypes": ["TradeExecuted", "SettlementCompleted"],
                                           "secret": "0123456789abcdef", "ordered": true }))
        .await,
    );
    responses.push(
        ok(
            &app,
            "/api/webhooks",
            json!({ "url": "http://127.0.0.1:9/b", "eventTypes": ["OrderAccepted"] }),
        )
        .await,
    );

    let mut order_ids = Vec::new();
    for (i, (participant, side, price, quantity)) in [
        ("A", "SELL", Some(10_025), 100),
        ("B", "BUY", Some(10_025), 40),
        ("A", "BUY", Some(10_025), 10), // self-trade: rejected
        ("C", "BUY", None, 30),         // market
        ("C", "SELL", Some(10_100), 50),
    ]
    .into_iter()
    .enumerate()
    {
        let body = json!({ "clientOrderId": format!("c-{i}"), "instrumentId": "ACME", "participantId": participant,
                           "side": side, "type": if price.is_some() { "LIMIT" } else { "MARKET" },
                           "priceTicks": price, "quantity": quantity });
        let response = ok(&app, "/api/orders", body).await;
        order_ids.push(response["order"]["orderId"].as_str().unwrap().to_string());
        responses.push(response);
    }
    if restarts {
        app.restart().await;
    }
    let (status, cancelled) = app.delete(&format!("/api/orders/{}", order_ids[4])).await;
    assert_eq!(status, StatusCode::OK);
    responses.push(cancelled);
    responses.push(ok(&app, "/test/clock/advance", json!({ "days": 2 })).await);
    let trades = app.get("/api/trades").await.1;
    let executions: Vec<String> = trades
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["executionId"].as_str().unwrap().to_string())
        .collect();
    responses.push(
        ok(
            &app,
            &format!("/api/settlements/{}/outcomes", executions[0]),
            json!({ "status": "SETTLED", "reference": "r-1" }),
        )
        .await,
    );
    if restarts {
        app.restart().await;
    }
    responses.push(
        ok(
            &app,
            &format!("/api/settlements/{}/outcomes", executions[0]),
            json!({ "status": "FAILED" }),
        )
        .await,
    );
    responses.push(ok(&app, "/test/clock/advance", json!({ "days": 10 })).await);
    responses.push(
        app.call(
            axum::http::Method::PATCH,
            "/api/instruments/ACME",
            Some(json!({ "enabled": false })),
        )
        .await
        .1,
    );
    responses.push(ok(&app, "/api/orders", json!({ "instrumentId": "ACME", "participantId": "B", "side": "BUY",
                                                   "type": "LIMIT", "priceTicks": 10_000, "quantity": 5 })).await);

    let mut orders = Vec::new();
    for id in &order_ids {
        orders.push(raw(&app, &format!("/api/orders/{id}")).await);
    }
    let mut settlement_details = Vec::new();
    for id in &executions {
        settlement_details.push(raw(&app, &format!("/api/settlements/{id}")).await);
    }
    let deliveries = {
        let db = app.db();
        let mut statement = db
            .prepare("SELECT id, subscription_id, event_sequence FROM webhook_deliveries ORDER BY event_sequence, subscription_id")
            .unwrap();
        statement
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    let snapshot = Snapshot {
        events: raw(&app, "/api/events?limit=1000").await,
        trades: raw(&app, "/api/trades?limit=1000").await,
        webhooks: raw(&app, "/api/webhooks").await,
        settlements: raw(&app, "/api/settlements?limit=1000").await,
        book: raw(&app, "/api/orderbooks/ACME").await,
        orders,
        settlement_details,
        deliveries,
        responses,
    };
    app.stop().await;
    snapshot
}

#[tokio::test]
async fn the_same_commands_give_byte_identical_logs_and_responses() {
    let first = run(IdMode::Deterministic, false).await;
    let second = run(IdMode::Deterministic, false).await;
    assert_eq!(first, second);

    let events: Value = serde_json::from_str(&first.events).unwrap();
    let events = events.as_array().unwrap();
    let sources: std::collections::BTreeSet<_> = events
        .iter()
        .map(|e| e["source"].as_str().unwrap())
        .collect();
    assert_eq!(
        sources.len(),
        3,
        "exchange, simulator and external events are all covered: {sources:?}"
    );
    let scheme = IdScheme::Deterministic { seed: 2030 };
    for event in events {
        let sequence = event["sequence"].as_u64().unwrap();
        assert_eq!(
            event["eventId"],
            scheme.event(sequence).to_string(),
            "event IDs follow from the sequence"
        );
    }
    let sequences: Vec<u64> = events
        .iter()
        .map(|e| e["sequence"].as_u64().unwrap())
        .collect();
    assert_eq!(sequences, (1..=events.len() as u64).collect::<Vec<_>>());
    assert!(!first.deliveries.is_empty());
}

#[tokio::test]
async fn restarts_in_between_change_nothing() {
    let straight = run(IdMode::Deterministic, false).await;
    let restarted = run(IdMode::Deterministic, true).await;
    assert_eq!(straight, restarted);
}

#[tokio::test]
async fn random_ids_differ_between_runs() {
    let first = run(IdMode::Random, false).await;
    let second = run(IdMode::Random, false).await;
    assert_ne!(first.events, second.events);
    let strip_ids = |events: &str| -> Vec<(String, u64)> {
        let events: Value = serde_json::from_str(events).unwrap();
        events
            .as_array()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    e["eventType"].as_str().unwrap().to_string(),
                    e["sequence"].as_u64().unwrap(),
                )
            })
            .collect()
    };
    assert_eq!(
        strip_ids(&first.events),
        strip_ids(&second.events),
        "the log's shape is the same; only IDs differ"
    );
}
