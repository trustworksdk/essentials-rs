//! The REST API end to end over the real router, exchange thread and a SQLite file.

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use exchange_simulator::app::App;
use exchange_simulator::config::{
    Config, DatabaseConfig, ExchangeConfig, LogFormat, LoggingConfig, ServerConfig,
};
use exchange_simulator::domain::IdentifierPolicy;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

struct TestApp {
    app: Option<App>,
    dir: PathBuf,
}

impl TestApp {
    fn start() -> Self {
        let dir = std::env::temp_dir().join(format!("exchange-sim-api-{}", Uuid::now_v7()));
        let mut test = Self { app: None, dir };
        test.restart();
        test
    }

    fn config(&self) -> Config {
        Config {
            server: ServerConfig {
                bind: "127.0.0.1:0".into(),
            },
            exchange: ExchangeConfig {
                identifier_policy: IdentifierPolicy::Strict,
                command_queue_capacity: 1024,
            },
            database: DatabaseConfig {
                path: self.dir.join("exchange.db").to_string_lossy().into_owned(),
                busy_timeout_ms: 5000,
            },
            logging: LoggingConfig {
                filter: "warn".into(),
                format: LogFormat::Pretty,
            },
        }
    }

    /// Stops the service (draining the exchange thread) and starts a new one on the same database.
    fn restart(&mut self) {
        if let Some(app) = self.app.take() {
            app.shutdown();
        }
        self.app = Some(App::start(&self.config()).unwrap());
    }

    async fn call(&self, method: Method, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
        let request = Request::builder().method(method).uri(uri);
        let request = match body {
            Some(body) => request
                .header("content-type", "application/json")
                .body(Body::from(body.to_string())),
            None => request.body(Body::empty()),
        }
        .unwrap();
        let response = self
            .app
            .as_ref()
            .unwrap()
            .router
            .clone()
            .oneshot(request)
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, body)
    }

    async fn get(&self, uri: &str) -> (StatusCode, Value) {
        self.call(Method::GET, uri, None).await
    }

    async fn post(&self, uri: &str, body: Value) -> (StatusCode, Value) {
        self.call(Method::POST, uri, Some(body)).await
    }

    async fn delete(&self, uri: &str) -> (StatusCode, Value) {
        self.call(Method::DELETE, uri, None).await
    }

    /// ACME (DKK, scale 2, tick 1) and participants A to D.
    async fn seed(&self) {
        let (status, body) = self.post("/api/instruments", acme()).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        for id in ["A", "B", "C", "D"] {
            let (status, _) = self
                .post(
                    "/api/participants",
                    json!({ "id": id, "name": format!("Participant {id}") }),
                )
                .await;
            assert_eq!(status, StatusCode::CREATED);
        }
    }

    async fn order(
        &self,
        participant: &str,
        side: &str,
        price_ticks: Option<i64>,
        quantity: u64,
    ) -> (StatusCode, Value) {
        let order_type = if price_ticks.is_some() {
            "LIMIT"
        } else {
            "MARKET"
        };
        self.post(
            "/api/orders",
            json!({ "instrumentId": "ACME", "participantId": participant, "side": side, "type": order_type,
                    "priceTicks": price_ticks, "quantity": quantity }),
        )
        .await
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        if let Some(app) = self.app.take() {
            app.shutdown();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn acme() -> Value {
    json!({ "id": "ACME", "symbol": "ACME", "mic": "XCSE", "currency": "DKK", "currencyScale": 2, "tickSizeMinor": 1 })
}

#[tokio::test]
async fn health_reports_ok_with_schema_version() {
    let app = TestApp::start();
    assert_eq!(
        app.get("/health").await,
        (
            StatusCode::OK,
            json!({ "status": "ok", "schemaVersion": 1 })
        )
    );
}

#[tokio::test]
async fn milestone_acceptance_over_http_including_restart() {
    let mut app = TestApp::start();
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

    let expected_book = json!({ "instrumentId": "ACME", "currency": "DKK", "bids": [],
        "asks": [{ "priceTicks": 10025, "price": "100.25", "quantity": 50, "orders": 1 }], "lastSequence": 2 });
    assert_eq!(
        app.get("/api/orderbooks/ACME").await,
        (StatusCode::OK, expected_book.clone())
    );
    let (_, trades) = app.get("/api/trades").await;
    assert_eq!(trades.as_array().unwrap().len(), 1);
    assert_eq!(trades[0]["executionId"], execution["executionId"]);

    app.restart();
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
    let app = TestApp::start();
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
    let app = TestApp::start();
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
    let app = TestApp::start();
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
    let app = TestApp::start();
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
    let app = TestApp::start();
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
    let app = std::sync::Arc::new(TestApp::start());
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
