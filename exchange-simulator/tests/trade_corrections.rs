//! Trade corrections and busts (step 6.21, SR-36): an operator or a seeded draw corrects an execution's price or
//! lowers its quantity, or busts it. Both orders' filled quantities drop without anything going back on the book, both
//! members hear of it, the settlement instruction follows, and clearing statements drafted afterwards show the
//! execution as it now stands.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, SettlementMode};
use serde_json::{Value, json};

/// Monday 2030-01-07 09:00 UTC on a frozen virtual clock, clearing at 17:30 UTC, simulated settlement T+1 that settles
/// everything.
fn market(c: &mut Config) {
    c.clock.mode = ClockMode::Virtual;
    c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
    c.test_control.enabled = true;
    c.clearing.enabled = true;
    c.clearing.cutoff = "17:30".into();
    c.clearing.time_zone = "UTC".into();
    c.settlement.mode = SettlementMode::Simulated;
    c.settlement.settlement_days = 1;
    c.settlement.tick_interval_ms = 20;
}

async fn set_clock(app: &TestApp, to: &str) {
    let (status, body) = app.post("/test/clock/set", json!({ "to": to })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// A sells `resting` and B buys 10 at 100.00; B's order result.
async fn trade(app: &TestApp, resting: u64) -> Value {
    let (status, sell) = app.order("A", "SELL", Some(10_000), resting).await;
    assert_eq!(status, StatusCode::CREATED, "{sell}");
    let (status, buy) = app.order("B", "BUY", Some(10_000), 10).await;
    assert_eq!(status, StatusCode::CREATED, "{buy}");
    buy
}

async fn events(app: &TestApp, event_type: &str) -> Vec<Value> {
    app.get(&format!("/api/events?eventType={event_type}&limit=1000"))
        .await
        .1
        .as_array()
        .unwrap()
        .clone()
}

fn id(result: &Value) -> String {
    result["executions"][0]["executionId"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn a_bust_reduces_both_orders_fills_without_reinstating_them_and_cancels_the_settlement() {
    let mut app = TestApp::start_with(market).await;
    app.seed().await;
    let buy = trade(&app, 20).await; // The sell rests with 10 left.
    let execution = id(&buy);

    let (status, busted) = app
        .post(
            &format!("/api/trades/{execution}/bust"),
            json!({ "reason": "ERRONEOUS_TRADE" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{busted}");
    assert_eq!(busted["trade"]["status"], "BUSTED");
    let summary = |o: &Value| {
        (
            o["status"].as_str().unwrap().to_string(),
            o["filledQuantity"].as_u64().unwrap(),
            o["remainingQuantity"].as_u64().unwrap(),
            o["bustedQuantity"].as_u64().unwrap(),
        )
    };
    let orders = busted["orders"].as_array().unwrap();
    assert_eq!(
        summary(&orders[0]),
        ("CANCELLED".to_string(), 0, 0, 10),
        "the buy was filled by the busted trade alone"
    );
    assert_eq!(
        summary(&orders[1]),
        ("OPEN".to_string(), 0, 10, 10),
        "the sell keeps resting with what it had left, and nothing more"
    );
    let (_, book) = app.get("/api/orderbooks/ACME").await;
    assert_eq!(
        book["asks"][0]["quantity"], 10,
        "nothing went back on the book: {book}"
    );
    // The resting sell fills later on its remainder, keeping what was busted.
    let (_, later) = app.order("C", "BUY", Some(10_000), 10).await;
    assert_eq!(later["executions"][0]["quantity"], 10);
    let sell = orders[1]["orderId"].as_str().unwrap();
    let (_, sell) = app.get(&format!("/api/orders/{sell}")).await;
    assert_eq!(summary(&sell), ("FILLED".to_string(), 10, 0, 10));
    let filled = events(&app, "OrderFilled").await;
    let sell_filled = filled
        .iter()
        .find(|e| e["aggregateId"] == sell["orderId"])
        .expect("the sell's OrderFilled");
    assert_eq!(
        summary(&sell_filled["payload"]),
        ("FILLED".to_string(), 10, 0, 10),
        "the event knows what was busted"
    );

    let trade_busted = events(&app, "TradeBusted").await;
    assert_eq!(trade_busted.len(), 1);
    assert_eq!(
        (
            &trade_busted[0]["payload"]["reason"],
            &trade_busted[0]["payload"]["quantity"]
        ),
        (&json!("ERRONEOUS_TRADE"), &json!(10))
    );
    let fills = events(&app, "OrderFillBusted").await;
    let members: Vec<&str> = fills
        .iter()
        .map(|e| e["payload"]["participantId"].as_str().unwrap())
        .collect();
    assert_eq!(
        members,
        ["B", "A"],
        "both members hear of it, the buyer first"
    );
    for fill in &fills {
        assert_eq!(fill["payload"]["executionId"], json!(execution));
        assert_eq!(
            fill["payload"]["lastQuantity"], 10,
            "the busted execution, as SR-31 names it"
        );
        assert_eq!(fill["payload"]["filledQuantity"], 0);
    }
    let cancelled = events(&app, "SettlementCancelled").await;
    assert_eq!(cancelled.len(), 1);
    assert_eq!(
        (
            &cancelled[0]["payload"]["reason"],
            &cancelled[0]["payload"]["status"]
        ),
        (&json!("TRADE_BUSTED"), &json!("CANCELLED"))
    );
    let (_, trades) = app.get("/api/trades").await;
    assert_eq!(trades[0]["status"], "BUSTED");

    let (status, _) = app
        .post(&format!("/api/trades/{execution}/bust"), json!({}))
        .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "a busted trade cannot be busted again"
    );
    let (status, _) = app
        .post(
            &format!("/api/trades/{execution}/correction"),
            json!({ "priceTicks": 10_100 }),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "nor corrected");
    app.stop().await;
}

#[tokio::test]
async fn a_correction_shows_on_the_trade_the_fills_and_the_settlement_amount() {
    let mut app = TestApp::start_with(market).await;
    app.seed().await;
    let buy = trade(&app, 10).await;
    let execution = id(&buy);

    let (status, corrected) = app
        .post(
            &format!("/api/trades/{execution}/correction"),
            json!({ "priceTicks": 10_050 }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{corrected}");
    assert_eq!(
        (&corrected["trade"]["status"], &corrected["trade"]["price"]),
        (&json!("CORRECTED"), &json!("100.50"))
    );
    let traded = &events(&app, "TradeCorrected").await[0]["payload"];
    assert_eq!(
        (
            &traded["previousPrice"],
            &traded["price"],
            &traded["previousQuantity"],
            &traded["quantity"],
            &traded["reason"]
        ),
        (
            &json!("100.00"),
            &json!("100.50"),
            &json!(10),
            &json!(10),
            &json!("OPERATOR_CORRECTION")
        )
    );
    let fills = events(&app, "OrderFillCorrected").await;
    assert_eq!(fills.len(), 2);
    for fill in &fills {
        let p = &fill["payload"];
        assert_eq!(
            (
                &p["lastPrice"],
                &p["previousLastPrice"],
                &p["lastQuantity"],
                &p["filledQuantity"]
            ),
            (&json!("100.50"), &json!("100.00"), &json!(10), &json!(10))
        );
    }
    let settlement = &events(&app, "SettlementCorrected").await[0]["payload"];
    assert_eq!(
        (
            &settlement["settlementAmount"],
            &settlement["previousSettlementAmount"],
            &settlement["reason"],
            &settlement["status"]
        ),
        (
            &json!("1005.00"),
            &json!("1000.00"),
            &json!("TRADE_CORRECTED"),
            &json!("PENDING")
        ),
        "the amount follows; a pending instruction stays pending"
    );

    // Lower the quantity: the orders' fills drop, a filled order stays filled.
    let (status, lowered) = app
        .post(
            &format!("/api/trades/{execution}/correction"),
            json!({ "quantity": 4 }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{lowered}");
    let buy_order = &lowered["orders"][0];
    assert_eq!(
        (
            &buy_order["status"],
            &buy_order["filledQuantity"],
            &buy_order["bustedQuantity"]
        ),
        (&json!("FILLED"), &json!(4), &json!(6))
    );
    let settlement = events(&app, "SettlementCorrected").await;
    assert_eq!(settlement[1]["payload"]["settlementAmount"], "402.00");

    for (body, expected) in [
        (json!({ "quantity": 5 }), StatusCode::BAD_REQUEST),
        (json!({ "quantity": 0 }), StatusCode::BAD_REQUEST),
        (json!({ "priceTicks": 10_050 }), StatusCode::BAD_REQUEST),
        (json!({ "priceTicks": 0 }), StatusCode::BAD_REQUEST),
        (json!({ "price": 1 }), StatusCode::BAD_REQUEST),
    ] {
        let (status, answer) = app
            .post(&format!("/api/trades/{execution}/correction"), body.clone())
            .await;
        assert_eq!(status, expected, "{body}: {answer}");
    }
    let (status, _) = app
        .post(
            "/api/trades/00000000-0000-0000-0000-000000000000/bust",
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    app.stop().await;
}

#[tokio::test]
async fn statements_drafted_after_a_correction_show_it_and_issued_ones_stay() {
    let mut app = TestApp::start_with(market).await;
    app.seed().await;
    let monday = id(&trade(&app, 10).await);
    app.post(
        &format!("/api/trades/{monday}/correction"),
        json!({ "priceTicks": 10_100 }),
    )
    .await;
    set_clock(&app, "2030-01-07T18:00:00Z").await;
    let (_, statements) = app.get("/api/clearing/statements?date=2030-01-07").await;
    let lines: Vec<&Value> = statements
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["trades"].as_array().unwrap())
        .collect();
    assert_eq!(lines.len(), 2, "{statements}");
    for line in &lines {
        assert_eq!(line["priceTicks"], 10_100, "the corrected price");
    }

    // A bust after Monday's statements leaves them as issued.
    let issued = events(&app, "ClearingStatementIssued").await.len();
    let (status, _) = app
        .post(&format!("/api/trades/{monday}/bust"), json!({}))
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, again) = app.get("/api/clearing/statements?date=2030-01-07").await;
    assert_eq!(again, statements, "not reissued");
    assert_eq!(events(&app, "ClearingStatementIssued").await.len(), issued);

    // A trade busted before its cutoff is left off its statement.
    let tuesday = id(&trade(&app, 10).await);
    app.post(&format!("/api/trades/{tuesday}/bust"), json!({}))
        .await;
    set_clock(&app, "2030-01-08T18:00:00Z").await;
    let (_, statements) = app.get("/api/clearing/statements?date=2030-01-08").await;
    assert_eq!(statements, json!([]), "nothing cleared on Tuesday");
    app.stop().await;
}

#[tokio::test]
async fn seeded_busts_and_corrections_come_due_within_their_window_across_a_restart() {
    let mut app = TestApp::start_with(|c| {
        market(c);
        c.trades.seed = 7;
        c.trades.bust_probability = 0.5;
        c.trades.correct_probability = 0.5;
        c.trades.max_correction_days = 1;
    })
    .await;
    app.seed().await;
    let mut executions = Vec::new();
    for _ in 0..6 {
        executions.push(id(&trade(&app, 10).await));
    }
    assert_eq!(app.count("SELECT COUNT(*) FROM trade_adjustments"), 6);
    app.restart().await;
    set_clock(&app, "2030-01-08T17:30:00Z").await; // Tuesday's cutoff: the end of every window.
    let busted = events(&app, "TradeBusted").await;
    let corrected = events(&app, "TradeCorrected").await;
    assert_eq!(busted.len() + corrected.len(), 6, "every execution, once");
    assert!(
        !busted.is_empty() && !corrected.is_empty(),
        "both kinds at this seed"
    );
    for event in busted.iter().chain(&corrected) {
        let at = event["occurredAt"].as_str().unwrap();
        assert!(
            ("2030-01-07T09:00:00Z".."2030-01-08T17:30:00Z").contains(&at),
            "within the window: {at}"
        );
    }
    for event in &corrected {
        let p = &event["payload"];
        assert_eq!(p["reason"], "SIMULATED_CORRECTION");
        let moved = p["priceTicks"].as_i64().unwrap() - p["previousPriceTicks"].as_i64().unwrap();
        assert_eq!(moved.abs(), 1, "one tick");
    }
    assert!(
        busted
            .iter()
            .all(|e| e["payload"]["reason"] == "SIMULATED_BUST")
    );
    assert_eq!(app.count("SELECT COUNT(*) FROM trade_adjustments"), 0);
    app.stop().await;
}
