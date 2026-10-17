//! The OpenAPI document (`openapi.yaml`) against the service: the routes it describes are exactly the routes
//! mounted; every operation is exercised, with each request and response validated against the document's schemas;
//! and every event type is emitted and validated against its `webhooks` schema. (That the parts shared with the
//! exchange simulator's document are identical is checked in the exchange simulator's suite.)

mod common;

use axum::body::Body;
use axum::http::{Method, Request};
use common::{TestApp, payment};
use http_body_util::BodyExt;
use payment_gateway::statement::BreakPolicy;
use serde_json::{Value, json};
use simulator_kit::contract::{Spec, routes_in_source, without_parameter_names};
use simulator_kit::webhooks::SubscriptionConfig;
use tower::ServiceExt;

fn spec() -> Spec {
    Spec::from_yaml(payment_gateway::api::OPENAPI)
}

/// One JSON request (or none) through the router, expecting `status`. The response is checked against the
/// document, and the request too when it is expected to succeed.
async fn expect(
    app: &TestApp,
    spec: &Spec,
    status: u16,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> Value {
    let request = Request::builder().method(method.clone()).uri(uri);
    let request = match &body {
        Some(json) => {
            if status < 300 {
                spec.check_request(method.as_str(), uri, json);
            }
            request
                .header("content-type", "application/json")
                .body(Body::from(json.to_string()))
        }
        None => request.body(Body::empty()),
    }
    .unwrap();
    let response = app.router().oneshot(request).await.unwrap();
    let got = response.status().as_u16();
    let json = response
        .headers()
        .get("content-type")
        .is_some_and(|v| v.to_str().unwrap().starts_with("application/json"));
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else if json {
        serde_json::from_slice(&bytes).unwrap()
    } else {
        Value::String(String::from_utf8_lossy(&bytes).into_owned())
    };
    spec.check_response(method.as_str(), uri, got, &value);
    assert_eq!(got, status, "{method} {uri}: {value}");
    value
}

#[test]
fn the_document_describes_exactly_the_mounted_routes() {
    let sources = [
        include_str!("../src/api.rs"),
        include_str!("../../simulator-kit/src/api/webhooks.rs"),
    ];
    assert_eq!(
        without_parameter_names(&routes_in_source(&sources)),
        without_parameter_names(&spec().operations())
    );
}

#[tokio::test]
async fn every_operation_and_event_matches_the_document() {
    let spec = spec();
    let mut app = TestApp::start_with(|c| {
        c.payments.seed = 5;
        c.payments.deposit.fail_probability = 0.3;
        c.payments.return_probability = 0.5;
        c.payments.max_return_days = 1;
        c.statement.breaks = BreakPolicy {
            alter_probability: 0.5,
            late_probability: 0.5,
            ..BreakPolicy::default()
        };
        c.webhooks.subscriptions.push(SubscriptionConfig {
            name: "declared".into(),
            url: "http://127.0.0.1:9/declared".into(),
            event_types: vec!["PaymentCompleted".into()],
            secret: None,
            secret_env: None,
            secret_file: None,
            ordered: false,
            enabled: true,
            paused: false,
        });
    })
    .await;
    let (a, s) = (&app, &spec);
    use Method as M;

    expect(a, s, 200, M::GET, "/health", None).await;
    expect(a, s, 200, M::GET, "/metrics", None).await;
    let served = expect(a, s, 200, M::GET, "/openapi.yaml", None).await;
    assert_eq!(served, json!(payment_gateway::api::OPENAPI));

    // Webhooks first, so there are deliveries and attempts.
    let hook = expect(
        a,
        s,
        201,
        M::POST,
        "/api/webhooks",
        Some(json!({ "url": "http://127.0.0.1:9/hook", "eventTypes": ["PaymentPending", "BankStatementIssued"],
                     "secret": "a-secret-of-at-least-16" })),
    )
    .await;
    let hook_id = hook["id"].as_str().unwrap().to_string();
    expect(
        a,
        s,
        400,
        M::POST,
        "/api/webhooks",
        Some(json!({ "url": "http://x", "eventTypes": ["TradeExecuted"] })),
    )
    .await;
    let listed = expect(a, s, 200, M::GET, "/api/webhooks", None).await;
    let declared = listed
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["name"] == "declared")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Payments.
    let mut ids = Vec::new();
    for i in 0..16 {
        let kind = if i % 4 == 3 { "WITHDRAWAL" } else { "DEPOSIT" };
        let mut body = payment(&format!("k-{i}"), kind, &format!("{}.25", 100 + i), "DKK");
        if i == 0 {
            body["reference"] = json!("first");
        }
        let created = expect(a, s, 201, M::POST, "/api/payments", Some(body)).await;
        ids.push(created["paymentId"].as_str().unwrap().to_string());
    }
    let again = expect(
        a,
        s,
        200,
        M::POST,
        "/api/payments",
        Some(payment("k-1", "DEPOSIT", "101.25", "DKK")),
    )
    .await;
    assert_eq!(again["duplicate"], true);
    expect(
        a,
        s,
        409,
        M::POST,
        "/api/payments",
        Some(payment("k-1", "DEPOSIT", "999.00", "DKK")),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        "/api/payments",
        Some(payment("bad", "DEPOSIT", "1.001", "DKK")),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        "/api/payments",
        Some(json!({ "idempotencyKey": "x" })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::GET,
        &format!("/api/payments/{}", ids[0]),
        None,
    )
    .await;
    expect(
        a,
        s,
        404,
        M::GET,
        "/api/payments/00000000-0000-0000-0000-000000000000",
        None,
    )
    .await;
    expect(a, s, 400, M::GET, "/api/payments/nope", None).await;

    // Two business days: outcomes, returns and statements.
    expect(a, s, 200, M::GET, "/test/clock", None).await;
    expect(
        a,
        s,
        400,
        M::POST,
        "/test/clock/advance",
        Some(json!({ "days": 0 })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        "/test/clock/advance",
        Some(json!({ "days": 3, "hours": 1 })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        "/test/clock/set",
        Some(json!({ "to": "2030-01-11T09:00:00Z" })),
    )
    .await;
    expect(
        a,
        s,
        409,
        M::POST,
        "/test/clock/set",
        Some(json!({ "to": "2030-01-01T00:00:00Z" })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        "/test/clock/speed",
        Some(json!({ "speed": -1 })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        "/test/clock/speed",
        Some(json!({ "speed": 10 })),
    )
    .await;
    expect(a, s, 200, M::POST, "/test/clock/resume", None).await;
    expect(a, s, 200, M::POST, "/test/clock/freeze", None).await;
    expect(
        a,
        s,
        200,
        M::GET,
        "/api/payments?status=COMPLETED&limit=50",
        None,
    )
    .await;
    expect(
        a,
        s,
        200,
        M::GET,
        "/api/payments?accountId=acc-1&after=2",
        None,
    )
    .await;
    expect(a, s, 400, M::GET, "/api/payments?status=LOST", None).await;
    expect(a, s, 400, M::GET, "/api/payments?limit=0", None).await;
    let statements = expect(a, s, 200, M::GET, "/api/statements?currency=dkk", None).await;
    let first = statements[0]["statementId"].as_str().unwrap().to_string();
    expect(a, s, 200, M::GET, &format!("/api/statements/{first}"), None).await;
    expect(a, s, 200, M::GET, "/api/statements?date=2030-01-07", None).await;
    expect(a, s, 400, M::GET, "/api/statements?date=monday", None).await;
    expect(a, s, 404, M::GET, "/api/statements/BS:2030-01-07:EUR", None).await;
    let breaks = expect(a, s, 200, M::GET, "/test/statements/breaks", None).await;
    assert!(!breaks.as_array().unwrap().is_empty());
    expect(
        a,
        s,
        400,
        M::GET,
        "/test/statements/breaks?date=monday",
        None,
    )
    .await;

    // Webhook maintenance and delivery inspection.
    let hook_url = format!("/api/webhooks/{hook_id}");
    let zero = "00000000-0000-0000-0000-000000000000";
    expect(a, s, 200, M::GET, &hook_url, None).await;
    expect(a, s, 404, M::GET, &format!("/api/webhooks/{zero}"), None).await;
    expect(a, s, 400, M::GET, "/api/webhooks/nope", None).await;
    expect(
        a,
        s,
        200,
        M::PATCH,
        &hook_url,
        Some(json!({ "ordered": true })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::PATCH,
        &hook_url,
        Some(json!({ "url": "mailto:x" })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::PATCH,
        &format!("/api/webhooks/{zero}"),
        Some(json!({ "paused": true })),
    )
    .await;
    expect(
        a,
        s,
        409,
        M::PATCH,
        &format!("/api/webhooks/{declared}"),
        Some(json!({ "enabled": false })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        &format!("{hook_url}/secret"),
        Some(json!({ "secret": "another-secret-of-16+" })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        &format!("{hook_url}/secret"),
        Some(json!({ "secret": "another-secret-of-16+", "graceSeconds": 10000000 })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::POST,
        &format!("/api/webhooks/{zero}/secret"),
        Some(json!({ "secret": "another-secret-of-16+" })),
    )
    .await;
    expect(
        a,
        s,
        409,
        M::POST,
        &format!("/api/webhooks/{declared}/secret"),
        Some(json!({ "secret": "another-secret-of-16+" })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::GET,
        &format!("{hook_url}/deliveries?after=1&limit=10"),
        None,
    )
    .await;
    expect(
        a,
        s,
        400,
        M::GET,
        &format!("{hook_url}/deliveries?limit=0"),
        None,
    )
    .await;
    expect(
        a,
        s,
        404,
        M::GET,
        &format!("/api/webhooks/{zero}/deliveries"),
        None,
    )
    .await;
    expect(a, s, 200, M::POST, &format!("{hook_url}/redeliver"), None).await;
    expect(
        a,
        s,
        404,
        M::POST,
        &format!("/api/webhooks/{zero}/redeliver"),
        None,
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        &format!("{hook_url}/dead-letters/retry"),
        None,
    )
    .await;
    expect(
        a,
        s,
        404,
        M::POST,
        &format!("/api/webhooks/{zero}/dead-letters/retry"),
        None,
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        &format!("{hook_url}/dead-letters/retry"),
        Some(json!({ "deliveryIds": [1] })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::GET,
        &format!("/test/deliveries?subscriptionId={hook_id}"),
        None,
    )
    .await;
    expect(a, s, 400, M::GET, "/test/deliveries?status=LOST", None).await;
    expect(
        a,
        s,
        200,
        M::POST,
        "/test/dead-letters/retry",
        Some(json!({ "subscriptionId": hook_id })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::POST,
        "/test/dead-letters/retry",
        Some(json!({ "subscriptionId": zero })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        &format!("/test/webhooks/{hook_id}/redeliver?after=2"),
        None,
    )
    .await;
    expect(
        a,
        s,
        404,
        M::POST,
        &format!("/test/webhooks/{zero}/redeliver"),
        None,
    )
    .await;
    let faults = format!("/test/webhooks/{declared}/faults");
    expect(
        a,
        s,
        200,
        M::PUT,
        &faults,
        Some(json!({ "nulls": {}, "invalid": { "probability": 0.2 } })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::PUT,
        &faults,
        Some(json!({ "delay": { "ms": 100000 } })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::PUT,
        &format!("/test/webhooks/{zero}/faults"),
        Some(json!({})),
    )
    .await;
    expect(a, s, 200, M::DELETE, &faults, None).await;
    expect(
        a,
        s,
        404,
        M::DELETE,
        &format!("/test/webhooks/{zero}/faults"),
        None,
    )
    .await;
    let mut attempts = Value::Null;
    for _ in 0..200 {
        attempts = expect(a, s, 200, M::GET, "/test/attempts", None).await;
        if !attempts.as_array().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(!attempts.as_array().unwrap().is_empty());
    expect(
        a,
        s,
        200,
        M::GET,
        &format!("/test/attempts?subscriptionId={declared}"),
        None,
    )
    .await;
    expect(
        a,
        s,
        409,
        M::DELETE,
        &format!("/api/webhooks/{declared}"),
        None,
    )
    .await;
    expect(a, s, 204, M::DELETE, &hook_url, None).await;
    expect(a, s, 404, M::DELETE, &hook_url, None).await;
    expect(a, s, 200, M::GET, "/api/webhooks?includeDeleted=true", None).await;

    let events = expect(a, s, 200, M::GET, "/api/events?limit=1000", None).await;
    expect(
        a,
        s,
        200,
        M::GET,
        "/api/events?eventType=PaymentReturned&after=1",
        None,
    )
    .await;
    expect(a, s, 400, M::GET, "/api/events?limit=0", None).await;
    let mut seen = std::collections::BTreeSet::new();
    for event in events.as_array().unwrap() {
        spec.check_event(event);
        seen.insert(event["eventType"].as_str().unwrap().to_string());
    }
    assert_eq!(seen, spec.event_types(), "every event type occurs");

    expect(a, s, 204, M::POST, "/test/reset", None).await;
    assert_eq!(
        spec.uncovered(),
        Default::default(),
        "operations the test did not exercise"
    );
    app.stop().await;
}
