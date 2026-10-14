//! The REST API end to end over the real router, exchange thread and a SQLite file.

mod common;

use axum::http::StatusCode;
use common::{TestApp, acme};
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
async fn health_reports_ok_with_schema_version() {
    let app = TestApp::start().await;
    assert_eq!(
        app.get("/health").await,
        (
            StatusCode::OK,
            json!({ "status": "ok", "schemaVersion": 4 })
        )
    );
}

#[tokio::test]
async fn milestone_acceptance_over_http_including_restart() {
    let mut app = TestApp::start().await;
    app.seed().await;

    let (status, sell) = app.order("A", "SELL", Some(10025), 200).await;
    assert_eq!(status, StatusCode::CREATED, "{sell}");
    assert_eq!(sell["order"]["status"], "OPEN");
    assert_eq!(sell["order"]["price"], "100.25");
    assert_eq!(sell["executions"], json!([]));
    let sell_id = sell["order"]["orderId"].as_str().unwrap().to_string();

    let (status, buy) = app.order("B", "BUY", Some(10025), 150).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(buy["order"]["status"], "FILLED");
    let executions = buy["executions"].as_array().unwrap();
    assert_eq!(executions.len(), 1);
    let execution = &executions[0];
    assert_eq!(
        (
            &execution["quantity"],
            &execution["priceTicks"],
            &execution["price"],
            &execution["currency"]
        ),
        (&json!(150), &json!(10025), &json!("100.25"), &json!("DKK"))
    );
    assert_eq!(
        (&execution["buyerId"], &execution["sellerId"]),
        (&json!("B"), &json!("A"))
    );
    assert_eq!(execution["sellOrderId"], sell_id.as_str());

    let (status, resting) = app.get(&format!("/api/orders/{sell_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        (
            &resting["status"],
            &resting["remainingQuantity"],
            &resting["filledQuantity"]
        ),
        (&json!("PARTIALLY_FILLED"), &json!(50), &json!(150))
    );
    assert!(resting["createdAt"].is_string() && resting["updatedAt"].is_string());

    let expected_book = json!({ "instrumentId": "ACME", "currency": "DKK", "enabled": true, "bids": [],
        "asks": [{ "priceTicks": 10025, "price": "100.25", "quantity": 50, "orders": 1 }], "lastSequence": 2 });
    assert_eq!(
        app.get("/api/orderbooks/ACME").await,
        (StatusCode::OK, expected_book.clone())
    );
    let (_, trades) = app.get("/api/trades").await;
    assert_eq!(trades.as_array().unwrap().len(), 1);
    assert_eq!(trades[0]["executionId"], execution["executionId"]);

    app.restart().await;
    assert_eq!(
        app.get("/api/orderbooks/ACME").await,
        (StatusCode::OK, expected_book),
        "book restored"
    );
    let (_, trades) = app.get("/api/trades").await;
    assert_eq!(trades.as_array().unwrap().len(), 1, "no execution repeated");
    let (_, next) = app.order("C", "BUY", Some(10025), 50).await;
    assert_eq!(next["order"]["sequence"], 3);
    assert_eq!(next["executions"][0]["sellOrderId"], sell_id.as_str());
}

#[tokio::test]
async fn order_requests_are_validated() {
    let app = TestApp::start().await;
    app.seed().await;
    let error = |(status, body): (StatusCode, Value)| {
        (
            status,
            body["error"].as_str().unwrap_or_default().to_string(),
        )
    };
    let code = |status: StatusCode, code: &str| (status, code.to_string());

    assert_eq!(
        error(app.order("A", "BUY", None, 0).await),
        code(StatusCode::BAD_REQUEST, "invalid_order")
    );
    assert_eq!(
        error(app.order("A", "BUY", Some(0), 10).await),
        code(StatusCode::BAD_REQUEST, "invalid_order")
    );
    assert_eq!(
        error(app.order("A", "BUY", Some(100), 0).await),
        code(StatusCode::BAD_REQUEST, "invalid_order")
    );
    assert_eq!(
        error(app.order("Z", "BUY", Some(100), 1).await),
        code(StatusCode::UNPROCESSABLE_ENTITY, "unknown_participant")
    );
    let bad_side = json!({ "instrumentId": "ACME", "participantId": "A", "side": "HOLD", "type": "LIMIT", "priceTicks": 1, "quantity": 1 });
    assert_eq!(
        error(app.post("/api/orders", bad_side).await),
        code(StatusCode::BAD_REQUEST, "invalid_json")
    );
    let negative = json!({ "instrumentId": "ACME", "participantId": "A", "side": "BUY", "type": "LIMIT", "priceTicks": 1, "quantity": -5 });
    assert_eq!(
        error(app.post("/api/orders", negative).await),
        code(StatusCode::BAD_REQUEST, "invalid_json")
    );
    let unknown_field = json!({ "instrumentId": "ACME", "participantId": "A", "side": "BUY", "type": "LIMIT", "price": "1.00", "quantity": 1 });
    assert_eq!(
        error(app.post("/api/orders", unknown_field).await),
        code(StatusCode::BAD_REQUEST, "invalid_json")
    );
    let other = json!({ "instrumentId": "NOPE", "participantId": "A", "side": "BUY", "type": "LIMIT", "priceTicks": 1, "quantity": 1 });
    assert_eq!(
        error(app.post("/api/orders", other).await),
        code(StatusCode::UNPROCESSABLE_ENTITY, "unknown_instrument")
    );
    assert_eq!(
        app.get("/api/orderbooks/ACME").await.1["lastSequence"],
        0,
        "no invalid order was sequenced"
    );
}

#[tokio::test]
async fn exchange_outcomes_market_self_trade_and_client_order_ids() {
    let app = TestApp::start().await;
    app.seed().await;
    app.order("A", "SELL", Some(10000), 10).await;

    let (status, rejected) = app.order("A", "BUY", Some(10000), 5).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "a rejection is a recorded outcome"
    );
    assert_eq!(
        (
            &rejected["order"]["status"],
            &rejected["order"]["rejectReason"]
        ),
        (&json!("REJECTED"), &json!("SELF_TRADE"))
    );

    let (_, market) = app.order("B", "BUY", None, 25).await;
    assert_eq!(
        (
            &market["order"]["status"],
            &market["order"]["remainingQuantity"]
        ),
        (&json!("CANCELLED"), &json!(15))
    );
    assert_eq!(market["order"]["price"], Value::Null);
    assert_eq!(market["executions"][0]["quantity"], 10);

    let with_client_id = |quantity: u64| {
        json!({ "clientOrderId": "c-1", "instrumentId": "ACME", "participantId": "C", "side": "BUY", "type": "LIMIT",
                "priceTicks": 9000, "quantity": quantity })
    };
    let (status, first) = app.post("/api/orders", with_client_id(10)).await;
    assert_eq!(
        (status, &first["duplicate"]),
        (StatusCode::CREATED, &json!(false))
    );
    let (status, again) = app.post("/api/orders", with_client_id(10)).await;
    assert_eq!(
        (status, &again["duplicate"]),
        (StatusCode::OK, &json!(true))
    );
    assert_eq!(again["order"]["orderId"], first["order"]["orderId"]);
    let (status, conflict) = app.post("/api/orders", with_client_id(11)).await;
    assert_eq!(
        (status, &conflict["error"]),
        (StatusCode::CONFLICT, &json!("client_order_id_conflict"))
    );
}

#[tokio::test]
async fn cancel_and_order_lookup_errors() {
    let app = TestApp::start().await;
    app.seed().await;
    let (_, placed) = app.order("A", "BUY", Some(9900), 10).await;
    let id = placed["order"]["orderId"].as_str().unwrap().to_string();

    let (status, cancelled) = app.delete(&format!("/api/orders/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        (
            &cancelled["order"]["status"],
            &cancelled["order"]["remainingQuantity"]
        ),
        (&json!("CANCELLED"), &json!(10))
    );
    assert_eq!(app.get("/api/orderbooks/ACME").await.1["bids"], json!([]));
    assert_eq!(
        app.get(&format!("/api/orders/{id}")).await.1["status"],
        "CANCELLED"
    );

    assert_eq!(
        app.delete(&format!("/api/orders/{id}")).await.0,
        StatusCode::CONFLICT
    );
    let unknown = Uuid::now_v7();
    assert_eq!(
        app.delete(&format!("/api/orders/{unknown}")).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.get(&format!("/api/orders/{unknown}")).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.get("/api/orders/not-a-uuid").await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        app.get("/api/orderbooks/NOPE").await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn instruments_and_participants_registration_and_lookup() {
    let app = TestApp::start().await;
    app.seed().await;
    let listing = |id: &str, mic: &str, isin: Option<&str>, cusip: Option<&str>| {
        json!({ "id": id, "symbol": "ACME", "mic": mic, "isin": isin, "cusip": cusip, "currency": "USD",
                "currencyScale": 2, "tickSizeMinor": 5, "lotSize": 100 })
    };
    let (status, created) = app
        .post(
            "/api/instruments",
            listing("ACME-XNAS", "XNAS", Some("US0378331005"), Some("037833100")),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        (&created["tickSize"], &created["lotSize"]),
        (&json!("0.05"), &json!(100))
    );
    assert_eq!(
        app.post(
            "/api/instruments",
            listing("ACME-XSTO", "XSTO", Some("US0378331005"), None)
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(
        app.post("/api/instruments", listing("DUP", "XNAS", None, None))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        app.post("/api/instruments", acme()).await.0,
        StatusCode::CONFLICT
    );
    let (status, bad) = app
        .post(
            "/api/instruments",
            listing("BAD", "XOSL", Some("US0378331006"), None),
        )
        .await;
    assert_eq!(
        (status, &bad["error"]),
        (StatusCode::BAD_REQUEST, &json!("invalid_request"))
    );

    let ids = |body: Value| {
        body.as_array()
            .unwrap()
            .iter()
            .map(|i| i["id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        ids(app.get("/api/instruments?isin=US0378331005").await.1),
        ["ACME-XNAS", "ACME-XSTO"]
    );
    assert_eq!(
        ids(app.get("/api/instruments?cusip=037833100").await.1),
        ["ACME-XNAS"]
    );
    assert_eq!(
        ids(app.get("/api/instruments?symbol=ACME&mic=XSTO").await.1),
        ["ACME-XSTO"]
    );
    assert_eq!(ids(app.get("/api/instruments").await.1).len(), 3);
    assert_eq!(
        app.get("/api/instruments?ticker=ACME").await.0,
        StatusCode::BAD_REQUEST
    );

    assert_eq!(
        app.post("/api/participants", json!({ "id": "A", "name": "again" }))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        app.post("/api/participants", json!({ "id": "", "name": "blank" }))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn trades_can_be_filtered() {
    let app = TestApp::start().await;
    app.seed().await;
    let (_, sell) = app.order("A", "SELL", Some(10000), 30).await;
    app.order("B", "BUY", Some(10000), 10).await;
    app.order("C", "BUY", Some(10000), 10).await;
    app.order("D", "BUY", Some(10000), 10).await;

    let count = |body: Value| body.as_array().unwrap().len();
    assert_eq!(count(app.get("/api/trades").await.1), 3);
    assert_eq!(count(app.get("/api/trades?participantId=A").await.1), 3);
    assert_eq!(count(app.get("/api/trades?participantId=C").await.1), 1);
    assert_eq!(
        count(
            app.get(&format!(
                "/api/trades?orderId={}",
                sell["order"]["orderId"].as_str().unwrap()
            ))
            .await
            .1
        ),
        3
    );
    assert_eq!(count(app.get("/api/trades?instrumentId=OTHER").await.1), 0);
    assert_eq!(count(app.get("/api/trades?limit=2").await.1), 2);
    let (_, ordered) = app.get("/api/trades").await;
    assert_eq!(
        ordered
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["buyerId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["B", "C", "D"]
    );
    assert_eq!(
        app.get("/api/trades?limit=0").await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        app.get("/api/trades?limit=1001").await.0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submissions_are_serialized_without_loss() {
    let app = std::sync::Arc::new(TestApp::start().await);
    app.seed().await;
    let tasks: Vec<_> = (0..200)
        .map(|i| {
            let app = app.clone();
            tokio::spawn(async move {
                let participant = ["A", "B", "C", "D"][i % 4];
                let side = if i % 2 == 0 { "BUY" } else { "SELL" };
                let (status, body) = app
                    .order(participant, side, Some(10_000 + (i % 7) as i64 - 3), 10)
                    .await;
                assert_eq!(status, StatusCode::CREATED, "{body}");
                body
            })
        })
        .collect();
    let mut sequences = Vec::new();
    let mut traded = 0;
    let mut rejected = 0;
    for task in tasks {
        let body = task.await.unwrap();
        sequences.push(body["order"]["sequence"].as_u64().unwrap());
        traded += body["executions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["quantity"].as_u64().unwrap())
            .sum::<u64>();
        if body["order"]["status"] == "REJECTED" {
            rejected += body["order"]["quantity"].as_u64().unwrap();
        }
    }
    sequences.sort_unstable();
    assert_eq!(
        sequences,
        (1..=200).collect::<Vec<_>>(),
        "every order got its own sequence"
    );

    let (_, trades) = app.get("/api/trades?limit=1000").await;
    let trades = trades.as_array().unwrap();
    let stored: u64 = trades.iter().map(|t| t["quantity"].as_u64().unwrap()).sum();
    assert_eq!(stored, traded, "responses and storage agree");
    assert!(
        trades.iter().all(|t| t["buyerId"] != t["sellerId"]),
        "no self-trades"
    );
    let (_, book) = app.get("/api/orderbooks/ACME").await;
    let resting: u64 = ["bids", "asks"]
        .iter()
        .flat_map(|s| book[s].as_array().unwrap())
        .map(|l| l["quantity"].as_u64().unwrap())
        .sum();
    assert!(traded > 0, "the flow trades");
    assert_eq!(
        2 * traded + resting + rejected,
        200 * 10,
        "every submitted unit traded (counted on both sides), rests, or was rejected"
    );
}

#[tokio::test]
async fn instruments_can_be_halted_and_resumed() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let (_, resting) = app.order("A", "SELL", Some(10025), 100).await;
    let resting_id = resting["order"]["orderId"].as_str().unwrap().to_string();
    app.order("A", "SELL", Some(10030), 100).await;

    let (status, halted) = app
        .call(
            axum::http::Method::PATCH,
            "/api/instruments/ACME",
            Some(json!({ "enabled": false })),
        )
        .await;
    assert_eq!(
        (status, &halted["enabled"]),
        (StatusCode::OK, &json!(false))
    );
    assert_eq!(
        app.get("/api/instruments?symbol=ACME").await.1[0]["enabled"],
        false
    );
    assert_eq!(app.get("/api/orderbooks/ACME").await.1["enabled"], false);

    let (status, rejected) = app.order("B", "BUY", Some(10025), 10).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "a halt rejection is a recorded outcome"
    );
    assert_eq!(
        (
            &rejected["order"]["status"],
            &rejected["order"]["rejectReason"]
        ),
        (&json!("REJECTED"), &json!("INSTRUMENT_DISABLED"))
    );
    let (status, cancelled) = app.delete(&format!("/api/orders/{resting_id}")).await;
    assert_eq!(
        (status, &cancelled["order"]["status"]),
        (StatusCode::OK, &json!("CANCELLED"))
    );

    app.restart().await;
    assert_eq!(
        app.get("/api/orderbooks/ACME").await.1["enabled"],
        false,
        "the halt survives a restart"
    );
    let (status, resumed) = app
        .call(
            axum::http::Method::PATCH,
            "/api/instruments/ACME",
            Some(json!({ "enabled": true })),
        )
        .await;
    assert_eq!(
        (status, &resumed["enabled"]),
        (StatusCode::OK, &json!(true))
    );
    let (_, trade) = app.order("B", "BUY", Some(10030), 10).await;
    assert_eq!(
        trade["executions"][0]["priceTicks"], 10030,
        "trading resumes against the remaining book"
    );

    let patch = |body: Value| {
        app.call(
            axum::http::Method::PATCH,
            "/api/instruments/ACME",
            Some(body),
        )
    };
    assert_eq!(
        patch(json!({ "enabled": "no" })).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        patch(json!({ "symbol": "X" })).await.0,
        StatusCode::BAD_REQUEST,
        "only enabled may change"
    );
    let (status, _) = app
        .call(
            axum::http::Method::PATCH,
            "/api/instruments/NOPE",
            Some(json!({ "enabled": false })),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, created) = app
        .post(
            "/api/instruments",
            json!({ "id": "HALTED", "symbol": "HLT", "currency": "DKK", "currencyScale": 2, "enabled": false }),
        )
        .await;
    assert_eq!(
        (status, &created["enabled"]),
        (StatusCode::CREATED, &json!(false))
    );
    app.stop().await;
}
