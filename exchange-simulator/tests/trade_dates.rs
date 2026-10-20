//! Trade date and intended settlement date on every execution (step 6.9a, SR-29): dated on the business-day calendar
//! whatever the clearing and settlement modes, identical on every event and response that carries them, and given to
//! executions stored before they carried dates.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, SettlementMode};
use serde_json::{Value, json};

/// Cutoff 17:30 Europe/Copenhagen (16:30 UTC in winter), a virtual clock from Monday 2030-01-07 09:00 UTC.
fn copenhagen(adjust: impl FnOnce(&mut Config)) -> impl FnOnce(&mut Config) {
    move |c: &mut Config| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
        c.test_control.enabled = true;
        c.clearing.cutoff = "17:30".into();
        c.clearing.time_zone = "Europe/Copenhagen".into();
        c.settlement.tick_interval_ms = 20;
        adjust(c);
    }
}

async fn set_clock(app: &TestApp, to: &str) {
    let (status, body) = app.post("/test/clock/set", json!({ "to": to })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// A sells and B buys 10 at 100.00; returns the execution from B's order result.
async fn trade(app: &TestApp) -> Value {
    assert_eq!(
        app.order("A", "SELL", Some(10_000), 10).await.0,
        StatusCode::CREATED
    );
    let (status, result) = app.order("B", "BUY", Some(10_000), 10).await;
    assert_eq!(status, StatusCode::CREATED, "{result}");
    result["executions"][0].clone()
}

fn dates(value: &Value) -> (String, String) {
    (
        value["tradeDate"].as_str().unwrap_or("").to_string(),
        value["intendedSettlementDate"]
            .as_str()
            .unwrap_or("")
            .to_string(),
    )
}

async fn events(app: &TestApp, event_type: &str) -> Vec<Value> {
    app.get(&format!("/api/events?eventType={event_type}&limit=1000"))
        .await
        .1
        .as_array()
        .unwrap()
        .clone()
}

/// Polls until `probe` holds, for up to five seconds.
async fn until<T>(why: &str, mut probe: impl AsyncFnMut() -> Option<T>) -> T {
    for _ in 0..250 {
        if let Some(value) = probe().await {
            return value;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("timed out: {why}");
}

#[tokio::test]
async fn executions_are_dated_on_the_business_day_calendar_and_every_event_agrees() {
    let mut app = TestApp::start_with(copenhagen(|c| {
        c.clearing.enabled = true;
        c.settlement.mode = SettlementMode::Simulated;
    }))
    .await;
    app.seed().await;

    // SR-29's acceptance cases, T+1.
    let cases = [
        (
            "2030-01-08T16:29:00Z",
            "Tuesday 17:29",
            ("2030-01-08", "2030-01-09"),
        ),
        (
            "2030-01-08T16:30:00Z",
            "Tuesday 17:30",
            ("2030-01-09", "2030-01-10"),
        ),
        (
            "2030-01-11T17:00:00Z",
            "Friday 18:00",
            ("2030-01-14", "2030-01-15"),
        ),
    ];
    let mut executions = Vec::new();
    for (at, when, (trade_date, settles)) in cases {
        set_clock(&app, at).await;
        let execution = trade(&app).await;
        assert_eq!(
            dates(&execution),
            (trade_date.to_string(), settles.to_string()),
            "a trade at {when} local"
        );
        executions.push(execution);
    }
    // Past Monday's cutoff: every statement is issued.
    set_clock(&app, "2030-01-14T17:00:00Z").await;

    let trades = events(&app, "TradeExecuted").await;
    let fills: Vec<Value> = [
        events(&app, "OrderFilled").await,
        events(&app, "OrderPartiallyFilled").await,
    ]
    .concat();
    let (_, listed) = app.get("/api/trades?limit=1000").await;
    for execution in &executions {
        let id = &execution["executionId"];
        let expected = dates(execution);
        let traded = trades
            .iter()
            .find(|e| &e["payload"]["executionId"] == id)
            .unwrap();
        assert_eq!(dates(&traded["payload"]), expected, "TradeExecuted");
        let filled: Vec<&Value> = fills
            .iter()
            .filter(|e| {
                &e["payload"]["executionId"] == id || e["aggregateId"] == execution["buyOrderId"]
            })
            .collect();
        assert_eq!(filled.len(), 2, "the maker's fill and the taker's");
        for fill in filled {
            assert_eq!(dates(&fill["payload"]), expected, "{}", fill["eventType"]);
        }
        let listed = listed
            .as_array()
            .unwrap()
            .iter()
            .find(|t| &t["executionId"] == id)
            .unwrap();
        assert_eq!(dates(listed), expected, "GET /api/trades");
        let pending = until("SettlementPending", async || {
            events(&app, "SettlementPending")
                .await
                .into_iter()
                .find(|e| &e["payload"]["executionId"] == id)
        })
        .await;
        assert_eq!(dates(&pending["payload"]), expected, "SettlementPending");
        let (status, statements) = app
            .get(&format!("/api/clearing/statements?date={}", expected.0))
            .await;
        assert_eq!(status, StatusCode::OK, "{statements}");
        let lines: Vec<&Value> = statements
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|s| s["trades"].as_array().unwrap())
            .filter(|line| &line["executionId"] == id)
            .collect();
        assert_eq!(
            lines.len(),
            2,
            "the buyer's statement line and the seller's"
        );
        for line in lines {
            assert_eq!(dates(line), expected, "the clearing statement line");
        }
    }
    app.stop().await;
}

#[tokio::test]
async fn the_dates_do_not_depend_on_clearing_or_the_settlement_mode() {
    for mode in [SettlementMode::External, SettlementMode::Simulated] {
        let mut app = TestApp::start_with(copenhagen(|c| {
            c.clearing.enabled = false;
            c.settlement.mode = mode;
            c.settlement.market_settlement_days = [("XCSE".to_string(), 2)].into();
        }))
        .await;
        app.seed().await; // ACME is on XCSE.
        set_clock(&app, "2030-01-08T16:30:00Z").await;
        let execution = trade(&app).await;
        // Tuesday 17:30 local belongs to Wednesday without clearing too; XCSE settles T+2, on Friday. In external mode
        // that is the date the exchange expects.
        let expected = ("2030-01-09".to_string(), "2030-01-11".to_string());
        assert_eq!(dates(&execution), expected, "{mode:?}");
        let traded = &events(&app, "TradeExecuted").await[0];
        assert_eq!(dates(&traded["payload"]), expected, "{mode:?}");
        if mode == SettlementMode::Simulated {
            let pending = until("SettlementPending", async || {
                events(&app, "SettlementPending").await.into_iter().next()
            })
            .await;
            assert_eq!(
                dates(&pending["payload"]),
                expected,
                "the simulator dates it the same way"
            );
        }
        app.stop().await;
    }
}

#[tokio::test]
async fn executions_stored_before_they_carried_dates_are_dated_at_startup() {
    let mut app = TestApp::start_with(copenhagen(|c| c.clearing.enabled = false)).await;
    app.seed().await;
    set_clock(&app, "2030-01-08T16:30:00Z").await;
    let execution = trade(&app).await;
    app.stop().await;
    // As a database from before migration 018 holds them.
    let erased = app
        .db()
        .execute(
            "UPDATE executions SET trade_date = NULL, intended_settlement_date = NULL",
            [],
        )
        .unwrap();
    assert_eq!(erased, 1);

    app.restart().await;
    let (_, listed) = app.get("/api/trades").await;
    assert_eq!(dates(&listed[0]), dates(&execution));
    assert_eq!(
        app.count("SELECT COUNT(*) FROM executions WHERE trade_date IS NULL OR intended_settlement_date IS NULL"),
        0
    );
    app.stop().await;
}

#[tokio::test]
async fn a_changed_cycle_dates_new_trades_only() {
    let mut app = TestApp::start_with(copenhagen(|c| c.clearing.enabled = true)).await;
    app.seed().await; // ACME is on XCSE, T+1 by default.
    set_clock(&app, "2030-01-08T10:00:00Z").await;
    let before = trade(&app).await;
    assert_eq!(
        dates(&before),
        ("2030-01-08".to_string(), "2030-01-09".to_string())
    );

    // XCSE moves to T+2 before Tuesday's statement is issued.
    app.config.settlement.market_settlement_days = [("XCSE".to_string(), 2)].into();
    app.restart().await;
    let after = trade(&app).await;
    assert_eq!(
        dates(&after),
        ("2030-01-08".to_string(), "2030-01-10".to_string())
    );
    set_clock(&app, "2030-01-08T17:00:00Z").await;

    let (_, statements) = app.get("/api/clearing/statements?date=2030-01-08").await;
    let line = |execution: &Value| {
        statements
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|s| s["trades"].as_array().unwrap())
            .find(|l| l["executionId"] == execution["executionId"])
            .cloned()
            .unwrap()
    };
    assert_eq!(
        dates(&line(&before)),
        dates(&before),
        "the statement keeps the date the trade was given"
    );
    assert_eq!(dates(&line(&after)), dates(&after));
    let (_, listed) = app.get("/api/trades").await;
    assert_eq!(dates(&listed[0]), dates(&before), "stored, not recomputed");
    app.stop().await;
}
