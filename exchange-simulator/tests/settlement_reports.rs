//! Externally reported settlement outcomes (combined mode): stored and published apart from the simulator's
//! lifecycle, idempotent by reference, refused in simulated-only mode.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, SettlementMode};
use serde_json::{Value, json};

const START: &str = "2030-01-07T09:00:00Z";

fn mode(mode: SettlementMode, adjust: impl FnOnce(&mut Config)) -> impl FnOnce(&mut Config) {
    move |c: &mut Config| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some(START.parse().unwrap());
        c.settlement.mode = mode;
        c.settlement.tick_interval_ms = 20;
        c.settlement.fail_probability = 0.0;
        c.settlement.delay_probability = 0.0;
        adjust(c);
    }
}

/// One execution of 150; returns its ID.
async fn execution(app: &TestApp) -> String {
    app.order("A", "SELL", Some(10025), 150).await;
    let (_, buy) = app.order("B", "BUY", Some(10025), 150).await;
    buy["executions"][0]["executionId"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn report(app: &TestApp, execution: &str, body: Value) -> (StatusCode, Value) {
    app.post(&format!("/api/settlements/{execution}/outcomes"), body)
        .await
}

async fn settle_simulated(app: &TestApp) {
    tokio::time::sleep(std::time::Duration::from_millis(100)).await; // let the ticker take the trade in
    let (status, _) = app.post("/test/clock/advance", json!({ "days": 3 })).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn in_combined_mode_external_and_simulated_outcomes_stay_apart() {
    let mut app = TestApp::start_with(mode(SettlementMode::Both, |c| {
        c.settlement.fail_probability = 1.0
    }))
    .await;
    app.seed().await;
    let id = execution(&app).await;

    let (status, stored) = report(
        &app,
        &id,
        json!({ "status": "SETTLED", "reference": "csd-1", "occurredAt": "2030-01-09T10:00:00Z",
                "details": { "csd": "VP Securities" } }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{stored}");
    assert_eq!(
        (&stored["status"], &stored["duplicate"]),
        (&json!("SETTLED"), &json!(false))
    );
    assert_eq!(
        stored["receivedAt"], START,
        "received on the exchange clock"
    );
    settle_simulated(&app).await;

    let (status, detail) = app.get(&format!("/api/settlements/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        detail["simulated"]["status"], "FAILED",
        "the simulator's verdict is its own"
    );
    assert_eq!(detail["external"].as_array().unwrap().len(), 1);
    assert_eq!(
        detail["external"][0]["status"], "SETTLED",
        "the external report is kept as reported"
    );
    let simulated: Vec<_> = detail["simulatedEvents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["eventType"].as_str().unwrap())
        .collect();
    assert_eq!(
        simulated,
        ["SettlementPending", "SettlementFailed"],
        "no external events mixed in"
    );

    let reported = app
        .get("/api/events?eventType=SettlementOutcomeReported")
        .await
        .1;
    let event = &reported[0];
    assert_eq!(
        (&event["source"], &event["aggregateId"]),
        (&json!("external"), &json!(id))
    );
    assert_eq!(event["occurredAt"], "2030-01-09T10:00:00Z");
    assert_eq!(event["payload"]["details"]["csd"], "VP Securities");
    let completed = app.get("/api/events?eventType=SettlementCompleted").await.1;
    assert_eq!(
        completed,
        json!([]),
        "an external SETTLED is not a SettlementCompleted"
    );
    app.stop().await;
}

#[tokio::test]
async fn reports_are_idempotent_by_reference() {
    let mut app = TestApp::start_with(mode(SettlementMode::External, |_| {})).await;
    app.seed().await;
    let id = execution(&app).await;
    let body =
        json!({ "status": "FAILED", "reference": "r-1", "reason": "insufficient securities" });
    let (status, first) = report(&app, &id, body.clone()).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, again) = report(&app, &id, body.clone()).await;
    assert_eq!(
        (status, &again["duplicate"]),
        (StatusCode::OK, &json!(true))
    );
    assert_eq!(again["reportId"], first["reportId"]);
    let with_time = json!({ "status": "FAILED", "reference": "r-1", "reason": "insufficient securities",
                            "occurredAt": first["occurredAt"] });
    assert_eq!(
        report(&app, &id, with_time).await.0,
        StatusCode::OK,
        "same occurredAt is the same report"
    );

    let (status, conflict) = report(
        &app,
        &id,
        json!({ "status": "SETTLED", "reference": "r-1" }),
    )
    .await;
    assert_eq!(
        (status, &conflict["error"]),
        (StatusCode::CONFLICT, &json!("report_conflict"))
    );
    let other_time = json!({ "status": "FAILED", "reference": "r-1", "reason": "insufficient securities",
                             "occurredAt": "2031-01-01T00:00:00Z" });
    assert_eq!(report(&app, &id, other_time).await.0, StatusCode::CONFLICT);

    for _ in 0..2 {
        assert_eq!(
            report(&app, &id, json!({ "status": "PENDING" })).await.0,
            StatusCode::CREATED,
            "no reference"
        );
    }
    let detail = app.get(&format!("/api/settlements/{id}")).await.1;
    assert_eq!(detail["external"].as_array().unwrap().len(), 3);
    assert_eq!(
        detail["simulated"],
        Value::Null,
        "external mode simulates nothing"
    );
    assert_eq!(
        app.get("/api/events?eventType=SettlementOutcomeReported")
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        3
    );

    app.restart().await;
    let detail = app.get(&format!("/api/settlements/{id}")).await.1;
    assert_eq!(
        detail["external"].as_array().unwrap().len(),
        3,
        "reports survive a restart"
    );
    assert_eq!(
        report(&app, &id, body).await.0,
        StatusCode::OK,
        "and so does idempotency"
    );
    app.stop().await;
}

#[tokio::test]
async fn simulated_only_mode_refuses_external_outcomes() {
    let mut app = TestApp::start_with(mode(SettlementMode::Simulated, |_| {})).await;
    app.seed().await;
    let id = execution(&app).await;
    let (status, body) = report(&app, &id, json!({ "status": "SETTLED" })).await;
    assert_eq!(
        (status, &body["error"]),
        (StatusCode::CONFLICT, &json!("external_outcomes_disabled"))
    );
    settle_simulated(&app).await;
    let detail = app.get(&format!("/api/settlements/{id}")).await.1;
    assert_eq!(
        (&detail["simulated"]["status"], &detail["external"]),
        (&json!("SETTLED"), &json!([]))
    );
    app.stop().await;
}

#[tokio::test]
async fn report_requests_are_validated() {
    let mut app = TestApp::start_with(mode(SettlementMode::Both, |_| {})).await;
    app.seed().await;
    let id = execution(&app).await;
    let unknown = uuid::Uuid::now_v7();
    assert_eq!(
        report(&app, &unknown.to_string(), json!({ "status": "SETTLED" }))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.get(&format!("/api/settlements/{unknown}")).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.get("/api/settlements/not-a-uuid").await.0,
        StatusCode::BAD_REQUEST
    );
    for bad in [
        json!({ "status": "DONE" }),
        json!({}),
        json!({ "status": "SETTLED", "details": [1] }),
        json!({ "status": "SETTLED", "reference": "x".repeat(129) }),
        json!({ "status": "SETTLED", "reference": "" }),
        json!({ "status": "SETTLED", "extra": true }),
    ] {
        let (status, body) = report(&app, &id, bad.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad} -> {body}");
    }
    let detail = app.get(&format!("/api/settlements/{id}")).await.1;
    assert_eq!(detail["external"], json!([]), "nothing invalid was stored");
    app.stop().await;
}

#[tokio::test]
async fn simulated_instructions_can_be_listed_by_status() {
    let mut app = TestApp::start_with(mode(SettlementMode::Simulated, |c| {
        c.settlement.fail_probability = 0.5;
        c.settlement.seed = 3;
    }))
    .await;
    app.seed().await;
    for _ in 0..6 {
        execution(&app).await;
    }
    settle_simulated(&app).await;
    let all = app.get("/api/settlements").await.1;
    assert_eq!(all.as_array().unwrap().len(), 6);
    let failed = app.get("/api/settlements?status=FAILED").await.1;
    let settled = app.get("/api/settlements?status=SETTLED").await.1;
    let (failed, settled) = (
        failed.as_array().unwrap().len(),
        settled.as_array().unwrap().len(),
    );
    assert_eq!(failed + settled, 6);
    assert!(
        all.as_array()
            .unwrap()
            .iter()
            .all(|s| s["nextTransitionAt"].is_null())
    );
    assert_eq!(
        app.get("/api/settlements?status=DONE").await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        app.get("/api/settlements?limit=0").await.0,
        StatusCode::BAD_REQUEST
    );
    app.stop().await;
}
