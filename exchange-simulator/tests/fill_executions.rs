//! The execution on every fill event (step 6.16, SR-31): each fill event, the incoming order's as well as the resting
//! order's, names its execution with what it filled and at which price, and agrees with `TradeExecuted` and the order
//! result.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use serde_json::{Value, json};

#[tokio::test]
async fn fill_events_agree_with_the_trades_and_the_order_result() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let mut asks = Vec::new();
    for (participant, price) in [("A", 1_000), ("C", 1_001), ("D", 1_002)] {
        let (status, ask) = app.order(participant, "SELL", Some(price), 100).await;
        assert_eq!(status, StatusCode::CREATED, "{ask}");
        asks.push(ask["order"]["orderId"].clone());
    }
    let (status, result) = app.order("B", "BUY", None, 300).await;
    assert_eq!(status, StatusCode::CREATED, "{result}");
    let executions = result["executions"].as_array().unwrap();
    assert_eq!(executions.len(), 3, "{result}");

    let (_, events) = app.get("/api/events?limit=1000").await;
    let events = events.as_array().unwrap();
    let fills_of = |order: &Value| -> Vec<&Value> {
        events
            .iter()
            .filter(|e| {
                &e["aggregateId"] == order
                    && matches!(
                        e["eventType"].as_str(),
                        Some("OrderPartiallyFilled" | "OrderFilled")
                    )
            })
            .collect()
    };
    let buy = fills_of(&result["order"]["orderId"]);
    assert_eq!(
        buy.iter()
            .map(|e| e["eventType"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "OrderPartiallyFilled",
            "OrderPartiallyFilled",
            "OrderFilled"
        ],
        "one fill event per execution, in execution order"
    );
    for (i, ((fill, execution), ask)) in buy.iter().zip(executions).zip(&asks).enumerate() {
        let id = &execution["executionId"];
        let trade = events
            .iter()
            .find(|e| e["eventType"] == "TradeExecuted" && &e["payload"]["executionId"] == id)
            .expect("the execution's TradeExecuted");
        let reported = |payload: &Value| {
            (
                payload["executionId"].clone(),
                payload
                    .get("lastQuantity")
                    .unwrap_or(&payload["quantity"])
                    .clone(),
                payload
                    .get("lastPriceTicks")
                    .unwrap_or(&payload["priceTicks"])
                    .clone(),
                payload
                    .get("lastPrice")
                    .unwrap_or(&payload["price"])
                    .clone(),
            )
        };
        let expected = (
            id.clone(),
            json!(100),
            json!(1_000 + i),
            json!(format!("10.0{i}")),
        );
        assert_eq!(
            reported(execution),
            expected,
            "the order result's execution {i}"
        );
        assert_eq!(reported(&trade["payload"]), expected, "TradeExecuted {i}");
        assert_eq!(
            reported(&fill["payload"]),
            expected,
            "the buy's fill event {i}"
        );
        assert_eq!(
            fill["payload"]["filledQuantity"],
            json!(100 * (i + 1)),
            "the running total after execution {i}"
        );
        let maker = fills_of(ask);
        assert_eq!(maker.len(), 1);
        assert_eq!(maker[0]["eventType"], "OrderFilled");
        assert_eq!(
            reported(&maker[0]["payload"]),
            expected,
            "ask {i}'s fill event"
        );
    }
    app.stop().await;
}
