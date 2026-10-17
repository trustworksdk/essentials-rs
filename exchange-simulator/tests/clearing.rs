//! End-of-day clearing statements in the service (plan item 5.3): issued at each cutoff on the domain clock, served by
//! the API and delivered as events, in time order with settlement, exactly once across restarts, with seeded breaks.

mod common;

use axum::http::{Method, StatusCode};
use common::TestApp;
use exchange_simulator::clearing::BreakPolicy;
use exchange_simulator::config::{ClockMode, Config, SettlementMode};
use serde_json::{Value, json};

/// Monday 2030-01-07 09:00 UTC. The Copenhagen cutoff, 17:30, is 16:30 UTC in winter.
const START: &str = "2030-01-07T09:00:00Z";

fn clearing(adjust: impl FnOnce(&mut Config)) -> impl FnOnce(&mut Config) {
    move |c: &mut Config| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some(START.parse().unwrap());
        c.clearing.enabled = true;
        c.clearing.cutoff = "17:30".into();
        c.clearing.time_zone = "Europe/Copenhagen".into();
        c.settlement.tick_interval_ms = 20;
        adjust(c);
    }
}

async fn statements(app: &TestApp, query: &str) -> Vec<Value> {
    let (status, body) = app.get(&format!("/api/clearing/statements{query}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body.as_array().unwrap().clone()
}

async fn set_clock(app: &TestApp, to: &str) {
    let (status, body) = app.post("/test/clock/set", json!({ "to": to })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

async fn advance(app: &TestApp, body: Value) {
    let (status, body) = app.post("/test/clock/advance", body).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

async fn events(app: &TestApp, event_type: &str) -> Vec<Value> {
    app.get(&format!("/api/events?eventType={event_type}&limit=1000"))
        .await
        .1
        .as_array()
        .unwrap()
        .clone()
}

async fn trade(app: &TestApp, quantity: u64) {
    assert_eq!(
        app.order("A", "SELL", Some(10_000), quantity).await.0,
        StatusCode::CREATED
    );
    assert_eq!(
        app.order("B", "BUY", Some(10_000), quantity).await.0,
        StatusCode::CREATED
    );
}

#[tokio::test]
async fn statements_are_issued_at_the_cutoff_served_by_the_api_and_delivered() {
    let mut app = TestApp::start_with(clearing(|_| {})).await;
    app.seed().await;
    let (status, _) = app
        .post(
            "/api/webhooks",
            json!({ "url": "http://127.0.0.1:9/statements", "eventTypes": ["ClearingStatementIssued"] }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    trade(&app, 100).await;
    set_clock(&app, "2030-01-07T16:29:59Z").await;
    assert!(
        statements(&app, "").await.is_empty(),
        "not before the cutoff"
    );

    set_clock(&app, "2030-01-07T16:30:00Z").await;
    let all = statements(&app, "?date=2030-01-07").await;
    let ids: Vec<&str> = all
        .iter()
        .map(|s| s["statementId"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["CS:2030-01-07:A", "CS:2030-01-07:B"]);
    assert_eq!(all[0]["issuedAt"], "2030-01-07T16:30:00Z");
    assert_eq!(all[0]["timeZone"], "Europe/Copenhagen");
    assert_eq!(all[0]["obligations"][0]["securitiesNet"], -100);
    assert_eq!(all[1]["obligations"][0]["cashNet"], "-10000.00");
    assert_eq!(statements(&app, "?participant=B").await, [all[1].clone()]);
    assert!(statements(&app, "?date=2030-01-08").await.is_empty());
    let (status, one) = app.get("/api/clearing/statements/CS:2030-01-07:A").await;
    assert_eq!((status, &one), (StatusCode::OK, &all[0]));
    assert_eq!(
        app.get("/api/clearing/statements/CS:2030-01-07:C").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.get("/api/clearing/statements?date=monday").await.0,
        StatusCode::BAD_REQUEST
    );

    let issued = events(&app, "ClearingStatementIssued").await;
    assert_eq!(issued.len(), 2);
    assert_eq!(
        issued[0]["payload"], all[0],
        "the event carries the statement"
    );
    assert_eq!(
        (
            &issued[0]["source"],
            &issued[0]["aggregateId"],
            &issued[0]["correlationId"]
        ),
        (
            &json!("clearinghouse"),
            &json!("CS:2030-01-07:A"),
            &json!("A")
        )
    );
    let deliveries = app.get("/test/deliveries").await.1;
    assert_eq!(deliveries.as_array().unwrap().len(), 2, "{deliveries}");

    // Nothing traded since: no statements at Tuesday's cutoff.
    advance(&app, json!({ "days": 1 })).await;
    assert_eq!(statements(&app, "").await.len(), 2);
    app.stop().await;
}

#[tokio::test]
async fn a_large_clock_move_issues_every_business_day_in_time_order_with_settlement() {
    let mut app = TestApp::start_with(clearing(|c| {
        c.settlement.mode = SettlementMode::Simulated;
        c.settlement.fail_probability = 0.0;
        c.settlement.delay_probability = 0.0;
    }))
    .await;
    app.seed().await;
    trade(&app, 10).await; // Monday
    set_clock(&app, "2030-01-08T12:00:00Z").await; // Tuesday: Monday's statements, then a trade
    trade(&app, 20).await;
    set_clock(&app, "2030-01-11T20:00:00Z").await; // Friday after the cutoff
    trade(&app, 30).await; // dated Monday 2030-01-14
    advance(&app, json!({ "days": 7 })).await;

    let dates: Vec<String> = statements(&app, "?participant=A")
        .await
        .iter()
        .map(|s| s["businessDate"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(dates, ["2030-01-07", "2030-01-08", "2030-01-14"]);
    let monday = &statements(&app, "?date=2030-01-14&participant=B").await[0];
    assert_eq!(monday["trades"][0]["tradeDate"], "2030-01-14");
    assert_eq!(
        monday["obligations"][0]["intendedSettlementDate"],
        "2030-01-15"
    );
    assert_eq!(monday["tradesFrom"], "2030-01-11T16:30:00Z");

    let log = app.get("/api/events?limit=1000").await.1;
    let stamps: Vec<&str> = log
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["occurredAt"].as_str().unwrap())
        .collect();
    assert!(stamps.is_sorted(), "{stamps:#?}");
    let completed = events(&app, "SettlementCompleted").await;
    assert_eq!(completed.len(), 3);
    assert_eq!(
        completed[2]["payload"]["clearing"]["seller"]["obligationId"],
        "CS:2030-01-14:A:ACME:2030-01-15"
    );
    let (_, settlement) = app
        .get(&format!(
            "/api/settlements/{}",
            completed[2]["aggregateId"].as_str().unwrap()
        ))
        .await;
    assert_eq!(
        settlement["simulated"]["clearing"]["businessDate"],
        "2030-01-14"
    );
    app.stop().await;
}

#[tokio::test]
async fn late_statements_wait_and_every_break_is_listed_for_tests() {
    let mut app = TestApp::start_with(clearing(|c| {
        c.clearing.breaks = BreakPolicy {
            seed: 3,
            omit_probability: 1.0,
            late_probability: 1.0,
            max_late_minutes: 30,
            ..BreakPolicy::default()
        };
    }))
    .await;
    app.seed().await;
    trade(&app, 10).await;
    set_clock(&app, "2030-01-07T16:30:00Z").await;
    assert!(
        statements(&app, "").await.is_empty(),
        "late: not issued yet"
    );
    let (status, breaks) = app.get("/test/clearing/breaks?date=2030-01-07").await;
    assert_eq!(status, StatusCode::OK);
    let breaks = breaks.as_array().unwrap();
    assert_eq!(breaks.len(), 2);
    assert_eq!(breaks[0]["issued"], false);
    let kinds: Vec<&str> = breaks[0]["breaks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["OMITTED_TRADE", "LATE_STATEMENT"]);
    let due: Vec<&str> = breaks
        .iter()
        .map(|b| b["issuedAt"].as_str().unwrap())
        .collect();

    // A restart keeps the waiting statements.
    app.restart().await;
    advance(&app, json!({ "minutes": 30 })).await;
    let issued = statements(&app, "").await;
    assert_eq!(issued.len(), 2);
    let mut at: Vec<&str> = issued
        .iter()
        .map(|s| s["issuedAt"].as_str().unwrap())
        .collect();
    at.sort_unstable();
    let mut expected = due.clone();
    expected.sort_unstable();
    assert_eq!(at, expected);
    assert!(
        issued
            .iter()
            .all(|s| s["trades"] == json!([]) && s["obligations"] == json!([]))
    );
    let log = events(&app, "ClearingStatementIssued").await;
    assert_eq!(log.len(), 2, "issued once");
    assert!(
        log.iter()
            .all(|e| e["occurredAt"].as_str().unwrap() > "2030-01-07T16:30:00Z")
    );
    assert_eq!(
        events(&app, "TradeExecuted").await.len(),
        1,
        "the trade itself is untouched"
    );
    assert_eq!(app.get("/test/clearing/breaks").await.1[0]["issued"], true);
    app.stop().await;
}

#[tokio::test]
async fn a_cutoff_passed_while_the_service_was_down_is_issued_once_at_startup() {
    let mut app = TestApp::start_with(clearing(|_| {})).await;
    app.seed().await;
    trade(&app, 10).await;
    app.stop().await;
    // Simulated time moves on while the service is down: a later start, past two cutoffs.
    app.config.clock.start = Some("2030-01-09T09:00:00Z".parse().unwrap());
    app.restart().await;
    assert_eq!(
        app.get("/test/clock").await.1["now"],
        "2030-01-09T09:00:00Z"
    );
    assert_eq!(statements(&app, "").await.len(), 2);
    app.restart().await;
    advance(&app, json!({ "hours": 1 })).await;
    assert_eq!(events(&app, "ClearingStatementIssued").await.len(), 2);
    app.stop().await;
}

#[tokio::test]
async fn a_reset_clears_statements_and_starts_clearing_again() {
    let mut app = TestApp::start_with(clearing(|_| {})).await;
    app.seed().await;
    trade(&app, 10).await;
    set_clock(&app, "2030-01-07T17:00:00Z").await;
    assert_eq!(statements(&app, "").await.len(), 2);
    assert_eq!(
        app.call(Method::POST, "/test/reset", None).await.0,
        StatusCode::NO_CONTENT
    );
    assert!(statements(&app, "").await.is_empty());
    app.seed().await;
    trade(&app, 10).await;
    set_clock(&app, "2030-01-07T17:00:00Z").await;
    assert_eq!(
        statements(&app, "").await.len(),
        2,
        "the same day is issued again after a reset"
    );
    app.stop().await;
}

#[tokio::test]
async fn clearing_is_off_by_default_and_an_invalid_policy_stops_startup() {
    let mut app = TestApp::start_with(|c| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some(START.parse().unwrap());
    })
    .await;
    app.seed().await;
    trade(&app, 10).await;
    advance(&app, json!({ "days": 3 })).await;
    assert!(statements(&app, "").await.is_empty());
    assert!(events(&app, "ClearingStatementIssued").await.is_empty());
    let config = app.config.clone();
    app.stop().await;

    for (adjust, message) in [
        (
            Box::new(|c: &mut Config| c.clearing.time_zone = "Europe/Atlantis".into())
                as Box<dyn Fn(&mut Config)>,
            "not a known time zone",
        ),
        (
            Box::new(|c: &mut Config| c.clearing.cutoff = "5pm".into()),
            "HH:MM",
        ),
        (
            Box::new(|c: &mut Config| c.clearing.breaks.late_probability = 2.0),
            "late_probability",
        ),
    ] {
        let mut config = config.clone();
        config.clearing.enabled = true;
        adjust(&mut config);
        let error = exchange_simulator::app::App::start(&config)
            .err()
            .expect("refused");
        assert!(format!("{error:#}").contains(message), "{error:#}");
    }
}
