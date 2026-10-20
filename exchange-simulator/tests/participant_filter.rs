//! Webhook participant filters (step 6.3, SR-19): a subscription with `participants` receives order events of those
//! participants' orders, trade, settlement and clearing events where either side is listed, and every event that
//! involves no participant. Redelivery applies the same filter; the filter can be changed, removed and declared.

mod common;

use axum::http::{Method, StatusCode};
use common::TestApp;
use exchange_simulator::config::SubscriptionConfig;
use serde_json::{Value, json};

/// Registers a subscription to every event type the test produces, optionally filtered; returns its ID.
async fn subscribe(app: &TestApp, name: &str, participants: Option<Value>) -> String {
    let mut body = json!({
        "url": format!("http://127.0.0.1:9/{name}"),
        "eventTypes": ["InstrumentListed", "OrderAccepted", "OrderFilled", "OrderPartiallyFilled", "TradeExecuted",
                       "SettlementOutcomeReported"],
    });
    if let Some(participants) = participants {
        body["participants"] = participants;
    }
    let (status, created) = app.post("/api/webhooks", body).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    created["id"].as_str().unwrap().to_string()
}

/// `(eventType, the participants it names)` of every delivery queued for the subscription, in event order.
fn received(app: &TestApp, subscription: &str) -> Vec<(String, Vec<String>)> {
    let db = app.db();
    let mut statement = db
        .prepare(
            "SELECT e.event_type, e.payload FROM webhook_deliveries d JOIN events e ON e.sequence = d.event_sequence
             WHERE d.subscription_id = ?1 ORDER BY e.sequence",
        )
        .unwrap();
    statement
        .query_map([subscription], |row| {
            let payload: Value = serde_json::from_str(&row.get::<_, String>(1)?).unwrap();
            let parties = ["participantId", "buyerId", "sellerId"]
                .iter()
                .filter_map(|f| payload[f].as_str().map(str::to_string))
                .collect();
            Ok((row.get(0)?, parties))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn types(received: &[(String, Vec<String>)]) -> Vec<&str> {
    received.iter().map(|(t, _)| t.as_str()).collect()
}

/// A sells to B, C sells to D; both trades get an outcome report. 13 events: the listing, then per trade two
/// `OrderAccepted`, `TradeExecuted`, two `OrderFilled` and `SettlementOutcomeReported`.
async fn trade_twice(app: &TestApp) {
    for (seller, buyer) in [("A", "B"), ("C", "D")] {
        let (status, _) = app.order(seller, "SELL", Some(10_000), 10).await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = app.order(buyer, "BUY", Some(10_000), 10).await;
        assert_eq!(status, StatusCode::CREATED);
    }
    let (_, trades) = app.get("/api/trades").await;
    for trade in trades.as_array().unwrap() {
        let (status, body) = app
            .post(
                &format!(
                    "/api/settlements/{}/outcomes",
                    trade["executionId"].as_str().unwrap()
                ),
                json!({ "status": "SETTLED" }),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }
}

#[tokio::test]
async fn a_filtered_subscription_receives_only_its_participants_events() {
    let mut app = TestApp::start().await;
    let all = subscribe(&app, "all", None).await;
    let first = subscribe(&app, "first", Some(json!(["A"]))).await;
    let second = subscribe(&app, "second", Some(json!(["D", "C", "C"]))).await;
    app.seed().await;
    trade_twice(&app).await;

    let everything = received(&app, &all);
    assert_eq!(everything.len(), 13);
    let for_a = received(&app, &first);
    assert_eq!(
        types(&for_a),
        [
            "InstrumentListed",
            "OrderAccepted",
            "TradeExecuted",
            "OrderFilled",
            "SettlementOutcomeReported"
        ],
        "A's own order (not B's), the trade with B, its outcome, and the listing"
    );
    assert!(
        for_a
            .iter()
            .all(|(t, parties)| t == "InstrumentListed" || parties.contains(&"A".to_string())),
        "{for_a:?}"
    );
    let for_cd = received(&app, &second);
    assert!(
        for_cd
            .iter()
            .all(|(t, parties)| t == "InstrumentListed"
                || parties.iter().any(|p| p == "C" || p == "D")),
        "{for_cd:?}"
    );
    assert_eq!(for_cd.len(), 7);

    let (_, listed) = app.get(&format!("/api/webhooks/{second}")).await;
    assert_eq!(
        listed["participants"],
        json!(["C", "D"]),
        "sorted, without duplicates"
    );
    let (_, unfiltered) = app.get(&format!("/api/webhooks/{all}")).await;
    assert!(unfiltered.get("participants").is_none());
    app.stop().await;
}

#[tokio::test]
async fn redelivery_changes_and_removal_apply_the_filter() {
    let mut app = TestApp::start().await;
    app.seed().await;
    trade_twice(&app).await;
    // Created after the events: redelivery queues the past ones that pass the filter.
    let late = subscribe(&app, "late", Some(json!(["B"]))).await;
    let (status, queued) = app
        .post(
            &format!("/api/webhooks/{late}/redeliver?after=0"),
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let for_b = received(&app, &late);
    assert_eq!(queued["queued"], json!(for_b.len()));
    assert_eq!(for_b.len(), 5);
    assert!(
        for_b
            .iter()
            .all(|(t, parties)| t == "InstrumentListed" || parties.contains(&"B".to_string()))
    );

    let url = format!("/api/webhooks/{late}");
    let (status, changed) = app
        .call(Method::PATCH, &url, Some(json!({ "participants": ["C"] })))
        .await;
    assert_eq!(
        (status, &changed["participants"]),
        (StatusCode::OK, &json!(["C"]))
    );
    let (status, removed) = app
        .call(Method::PATCH, &url, Some(json!({ "participants": null })))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(removed.get("participants").is_none());
    let (_, queued) = app
        .post(&format!("{url}/redeliver?after=0"), json!({}))
        .await;
    assert_eq!(queued["queued"], json!(13), "unfiltered again: every event");

    for (participants, why) in [
        (json!([]), "an empty list"),
        (json!([" "]), "a blank ID"),
        (json!(["x".repeat(65)]), "an ID too long"),
        (json!("A"), "not a list"),
    ] {
        let (status, body) = app
            .post(
                "/api/webhooks",
                json!({ "url": "http://127.0.0.1:9/x", "eventTypes": ["TradeExecuted"], "participants": participants }),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{why}: {body}");
    }
    app.stop().await;
}

#[tokio::test]
async fn a_declared_subscription_takes_its_filter_from_the_configuration() {
    let declared = |participants: Option<Vec<String>>| SubscriptionConfig {
        name: "stack-a".into(),
        url: "http://127.0.0.1:9/stack-a".into(),
        event_types: vec!["OrderAccepted".into(), "InstrumentListed".into()],
        secret: None,
        secret_env: None,
        secret_file: None,
        ordered: false,
        enabled: true,
        paused: true,
        participants,
    };
    let mut app = TestApp::start_with(|c| {
        c.webhooks.subscriptions = vec![declared(Some(vec!["A".into()]))];
    })
    .await;
    app.seed().await;
    for participant in ["A", "B"] {
        app.order(participant, "BUY", Some(9_000), 1).await;
    }
    let (_, hooks) = app.get("/api/webhooks").await;
    let id = hooks[0]["id"].as_str().unwrap().to_string();
    assert_eq!(hooks[0]["participants"], json!(["A"]));
    assert_eq!(
        types(&received(&app, &id)),
        ["InstrumentListed", "OrderAccepted"]
    );
    let (status, _) = app
        .call(
            Method::PATCH,
            &format!("/api/webhooks/{id}"),
            Some(json!({ "participants": ["B"] })),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "owned by the configuration");

    app.config.webhooks.subscriptions = vec![declared(None)];
    app.restart().await;
    let (_, hooks) = app.get("/api/webhooks").await;
    assert!(
        hooks[0].get("participants").is_none(),
        "the configuration removed it"
    );
    app.order("B", "BUY", Some(9_000), 1).await;
    assert_eq!(
        types(&received(&app, &id)),
        ["InstrumentListed", "OrderAccepted", "OrderAccepted"]
    );
    app.stop().await;
}
