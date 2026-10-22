//! Event-log reads by several event types (step 6.19, SR-34): `eventType` repeated or comma-separated, and the
//! `Event-Log-Scanned-Through` header on every answer.

mod common;

use axum::http::{Method, StatusCode};
use common::TestApp;
use serde_json::Value;

/// The events and the scanned-through header of a read.
async fn read(app: &TestApp, query: &str) -> (Vec<Value>, u64) {
    let (status, headers, body) = app
        .call_full(Method::GET, &format!("/api/events?{query}"), None)
        .await;
    assert_eq!(status, StatusCode::OK, "{query}: {body}");
    let scanned = headers["event-log-scanned-through"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    (body.as_array().unwrap().clone(), scanned)
}

fn sequences(events: &[Value]) -> Vec<u64> {
    events
        .iter()
        .map(|e| e["sequence"].as_u64().unwrap())
        .collect()
}

#[tokio::test]
async fn several_types_are_read_in_sequence_order_with_how_far_the_read_looked() {
    let mut app = TestApp::start().await;
    app.seed().await;
    for _ in 0..3 {
        app.order("A", "SELL", Some(10_000), 10).await;
        app.order("B", "BUY", Some(10_000), 10).await;
    }
    app.order("C", "BUY", Some(9_000), 10).await; // Rests: an OrderAccepted after the last trade.
    let (all, head) = read(&app, "limit=1000").await;
    assert_eq!(
        head,
        *sequences(&all).last().unwrap(),
        "unfiltered: the last event's"
    );

    let wanted = ["TradeExecuted", "OrderFilled"];
    let expected: Vec<u64> = sequences(
        &all.iter()
            .filter(|e| wanted.contains(&e["eventType"].as_str().unwrap()))
            .cloned()
            .collect::<Vec<_>>(),
    );
    assert_eq!(expected.len(), 9, "three trades, each with two fills");
    for query in [
        "eventType=TradeExecuted&eventType=OrderFilled",
        "eventType=TradeExecuted,OrderFilled",
        "eventType=OrderFilled%2CTradeExecuted",
    ] {
        let (events, scanned) = read(&app, query).await;
        assert_eq!(sequences(&events), expected, "{query}");
        assert_eq!(
            scanned, head,
            "{query}: a short page looked at the whole log"
        );
    }

    // Past the last matching event: none, scanned through the head, beyond the last match.
    let last = *expected.last().unwrap();
    assert!(last < head);
    let (events, scanned) = read(
        &app,
        &format!("eventType=TradeExecuted,OrderFilled&after={last}"),
    )
    .await;
    assert!(events.is_empty());
    assert_eq!(scanned, head);

    // A full page looked only as far as its last event.
    let (events, scanned) = read(&app, "eventType=TradeExecuted&limit=2").await;
    assert_eq!(events.len(), 2);
    assert_eq!(scanned, events[1]["sequence"].as_u64().unwrap());
    let (_, scanned) = read(&app, &format!("after={head}")).await;
    assert_eq!(scanned, head, "unfiltered, none returned: the head");

    for query in [
        "eventType=Nope",
        "eventType=TradeExecuted,Nope",
        "eventType=",
    ] {
        let (status, body) = app.get(&format!("/api/events?{query}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
    }
    app.stop().await;
}
