//! Forced payment outcomes (step 6.6, SR-22, D20): with test control, `POST /test/payments/{id}/outcome` replaces a
//! payment's seeded draws, so a test takes the same path every time, also across a restart.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use serde_json::{Value, json};

/// Every seeded draw would fail a deposit, or return a completed one; forced outcomes must win anyway.
async fn hostile() -> TestApp {
    TestApp::start_with(|c| {
        c.payments.deposit.fail_probability = 1.0;
        c.payments.return_probability = 1.0;
        c.statement.enabled = false;
    })
    .await
}

async fn force(app: &TestApp, payment: &Value, body: Value) -> (StatusCode, Value) {
    let id = payment["paymentId"].as_str().unwrap();
    app.post(&format!("/test/payments/{id}/outcome"), body)
        .await
}

async fn status(app: &TestApp, payment: &Value) -> Value {
    let id = payment["paymentId"].as_str().unwrap();
    app.get(&format!("/api/payments/{id}")).await.1
}

#[tokio::test]
async fn a_forced_failure_happens_at_once_with_its_reason() {
    let mut app = hostile().await;
    let payment = app.pay("fail-now", "DEPOSIT", "10.00", "USD").await;
    let (code, failed) = force(
        &app,
        &payment,
        json!({ "outcome": "FAILED", "reason": "INSUFFICIENT_FUNDS", "delaySeconds": 0 }),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{failed}");
    assert_eq!(
        (&failed["status"], &failed["reason"], &failed["failedAt"]),
        (
            &json!("FAILED"),
            &json!("INSUFFICIENT_FUNDS"),
            &json!(common::START)
        ),
        "before the answer, at the current time"
    );
    let events = app.events("PaymentFailed").await;
    assert_eq!(events[0]["payload"]["reason"], "INSUFFICIENT_FUNDS");

    let (code, error) = force(&app, &payment, json!({ "outcome": "COMPLETED" })).await;
    assert_eq!(
        (code, &error["error"]),
        (StatusCode::CONFLICT, &json!("payment_settled"))
    );
    app.stop().await;
}

#[tokio::test]
async fn a_forced_completion_happens_when_scheduled_and_is_never_returned() {
    let mut app = hostile().await;
    let payment = app.pay("complete", "DEPOSIT", "10.00", "USD").await;
    let scheduled = || -> String {
        app.db()
            .query_row("SELECT due_at FROM payments", [], |row| row.get(0))
            .unwrap()
    };
    let before = scheduled();
    let (code, pending) = force(&app, &payment, json!({ "outcome": "COMPLETED" })).await;
    assert_eq!(
        (code, &pending["status"]),
        (StatusCode::OK, &json!("PENDING"))
    );
    assert_eq!(scheduled(), before, "due as scheduled");
    let completed_at = before.replace(".000000Z", "Z");
    // The forced outcome is stored: it survives a restart.
    app.restart().await;
    app.advance(json!({ "days": 10 })).await;
    let done = status(&app, &payment).await;
    assert_eq!(
        (&done["status"], &done["completedAt"], &done["returnedAt"]),
        (&json!("COMPLETED"), &json!(completed_at), &Value::Null)
    );
    app.stop().await;
}

#[tokio::test]
async fn a_forced_return_follows_completion_after_its_delay() {
    let mut app = hostile().await;
    let pending = app.pay("return-later", "DEPOSIT", "10.00", "USD").await;
    force(
        &app,
        &pending,
        json!({ "outcome": "RETURNED", "returnAfterSeconds": 3600, "delaySeconds": 60 }),
    )
    .await;
    app.advance(json!({ "minutes": 1 })).await;
    let completed = status(&app, &pending).await;
    assert_eq!(
        (&completed["status"], &completed["completedAt"]),
        (&json!("COMPLETED"), &json!("2030-01-07T09:01:00Z"))
    );
    app.advance(json!({ "hours": 1 })).await;
    let returned = status(&app, &pending).await;
    assert_eq!(
        (&returned["status"], &returned["returnedAt"]),
        (&json!("RETURNED"), &json!("2030-01-07T10:01:00Z"))
    );

    // A completed payment that is not returned yet can still be forced to return.
    let mut app2 = TestApp::start_with(|c| c.statement.enabled = false).await;
    let paid = app2.pay("then-return", "DEPOSIT", "10.00", "USD").await;
    app2.advance(json!({ "hours": 1 })).await;
    assert_eq!(status(&app2, &paid).await["status"], "COMPLETED");
    let (code, body) = force(
        &app2,
        &paid,
        json!({ "outcome": "RETURNED", "returnAfterSeconds": 0 }),
    )
    .await;
    assert_eq!(
        (code, &body["status"]),
        (StatusCode::OK, &json!("RETURNED")),
        "{body}"
    );
    let (code, _) = force(&app2, &paid, json!({ "outcome": "RETURNED" })).await;
    assert_eq!(code, StatusCode::CONFLICT, "final");
    app.stop().await;
    app2.stop().await;
}

#[tokio::test]
async fn invalid_requests_are_refused() {
    let mut app = hostile().await;
    let payment = app.pay("invalid", "DEPOSIT", "10.00", "USD").await;
    for (body, why) in [
        (json!({ "outcome": "LOST" }), "an unknown outcome"),
        (
            json!({ "outcome": "COMPLETED", "reason": "X" }),
            "a reason without FAILED",
        ),
        (
            json!({ "outcome": "FAILED", "returnAfterSeconds": 1 }),
            "a return delay without RETURNED",
        ),
        (
            json!({ "outcome": "FAILED", "reason": " " }),
            "a blank reason",
        ),
        (
            json!({ "outcome": "COMPLETED", "delaySeconds": 604801 }),
            "a delay over a week",
        ),
    ] {
        let (code, error) = force(&app, &payment, body).await;
        assert_eq!(code, StatusCode::BAD_REQUEST, "{why}: {error}");
    }
    let (code, _) = app
        .post(
            "/test/payments/00000000-0000-0000-0000-000000000000/outcome",
            json!({ "outcome": "COMPLETED" }),
        )
        .await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    assert_eq!(
        status(&app, &payment).await["status"],
        "PENDING",
        "nothing changed"
    );
    app.stop().await;
}
