//! Event-log reads by several event types (step 6.19, SR-34): `eventType` repeated or comma-separated, and the
//! `Event-Log-Scanned-Through` header on every answer. By participant (step 6.24, SR-39): `participant` keeps the
//! events naming one of the listed participants and those naming none, as a subscription's participant filter.

mod common;

use axum::http::{Method, StatusCode};
use common::TestApp;
use exchange_simulator::config::ClockMode;
use serde_json::{Value, json};

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

/// The participants an event names, as a subscription's participant filter finds them.
fn named(event: &Value) -> Vec<&str> {
    ["participantId", "buyerId", "sellerId"]
        .iter()
        .filter_map(|field| event["payload"][field].as_str())
        .collect()
}

async fn trade(app: &TestApp, seller: &str, buyer: &str) {
    for (participant, side) in [(seller, "SELL"), (buyer, "BUY")] {
        let (status, body) = app.order(participant, side, Some(10_000), 10).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }
}

#[tokio::test]
async fn a_member_reads_only_its_own_statements_and_continues_past_the_others() {
    let mut app = TestApp::start_with(|c| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
        c.clearing.enabled = true;
        c.clearing.cutoff = "17:30".into();
        c.clearing.time_zone = "Europe/Copenhagen".into();
        c.settlement.tick_interval_ms = 20;
    })
    .await;
    app.seed().await;
    let (status, body) = app
        .post(
            "/api/participants",
            json!({ "id": "DEMO-BROKER", "name": "Demo broker" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    // C stands in for the liquidity provider, the firm's counterparty: `LP` itself is the built-in provider's ID, and
    // its API orders follow the provider's quoting.
    trade(&app, "C", "DEMO-BROKER").await;
    trade(&app, "C", "A").await;
    let (status, body) = app
        .post("/test/clock/set", json!({ "to": "2030-01-07T16:30:00Z" }))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (statements, _) = read(&app, "eventType=ClearingStatementIssued").await;
    assert_eq!(statements.len(), 3, "A, C and DEMO-BROKER");
    let (_, head) = read(&app, "limit=1000").await;

    let query = "participant=DEMO-BROKER&eventType=ClearingStatementIssued";
    let (events, scanned) = read(&app, query).await;
    let ids: Vec<&str> = events
        .iter()
        .map(|e| e["aggregateId"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        ["CS:2030-01-07:DEMO-BROKER"],
        "none of the counterparties'"
    );
    assert_eq!(scanned, head, "the read looked at the whole log");
    let (events, scanned) = read(&app, &format!("{query}&after={scanned}")).await;
    assert!(events.is_empty());
    assert_eq!(
        scanned, head,
        "the next read starts past what was filtered out"
    );
    app.stop().await;
}

#[tokio::test]
async fn the_participant_filter_follows_the_subscription_rule() {
    let mut app = TestApp::start().await;
    app.seed().await;
    trade(&app, "A", "B").await;
    trade(&app, "C", "D").await;
    trade(&app, "B", "D").await;
    let (all, head) = read(&app, "limit=1000").await;
    let expected = |listed: &[&str]| {
        let kept: Vec<Value> = all
            .iter()
            .filter(|e| {
                let named = named(e);
                named.is_empty() || named.iter().any(|p| listed.contains(p))
            })
            .cloned()
            .collect();
        sequences(&kept)
    };
    let only_a = expected(&["A"]);
    assert!(only_a.len() > 1 && only_a.len() < all.len());
    assert!(
        all.iter()
            .any(|e| named(e).is_empty() && only_a.contains(&e["sequence"].as_u64().unwrap())),
        "an event naming no participant is kept"
    );
    let (events, scanned) = read(&app, "participant=A").await;
    assert_eq!((sequences(&events), scanned), (only_a.clone(), head));
    let a_or_c = expected(&["A", "C"]);
    for query in [
        "participant=A&participant=C",
        "participant=A,C",
        "participant=C%2CA&participant=A",
    ] {
        let (events, scanned) = read(&app, query).await;
        assert_eq!(
            (sequences(&events), scanned),
            (a_or_c.clone(), head),
            "{query}"
        );
    }
    let (events, _) = read(&app, "participant=NOBODY").await;
    assert_eq!(
        sequences(&events),
        expected(&[]),
        "only events naming no participant"
    );

    // A full page looked only as far as its last event, and the next page continues after it.
    let (page, scanned) = read(&app, "participant=A&limit=2").await;
    assert_eq!(sequences(&page), only_a[..2]);
    assert_eq!(scanned, only_a[1]);
    let (rest, _) = read(&app, &format!("participant=A&after={scanned}")).await;
    assert_eq!(sequences(&rest), only_a[2..]);

    for query in ["participant=", "participant=A,", "participant=%20"] {
        let (status, body) = app.get(&format!("/api/events?{query}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
    }
    app.stop().await;
}
