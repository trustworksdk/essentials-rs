//! Acceptance steps 3 and 8–10 against MockServer as the settlement consumer: a `TradeExecuted` webhook, a consumer
//! that fails twice then recovers, and a restart. Needs Docker (Testcontainers starts MockServer); the simulator
//! itself runs in-process.

mod common;

use std::time::Duration;

use axum::http::StatusCode;
use common::TestApp;
use serde_json::{Value, json};
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

const PATH: &str = "/settlement/trades";

struct MockServer {
    base: String,
    http: reqwest::Client,
    _container: ContainerAsync<GenericImage>,
}

impl MockServer {
    async fn start() -> Self {
        let container = GenericImage::new("mockserver/mockserver", "5.15.0")
            .with_exposed_port(1080.tcp())
            .with_wait_for(WaitFor::message_on_stdout("started on port: 1080"))
            .start()
            .await
            .expect("start MockServer container");
        let base = format!(
            "http://{}:{}",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(1080).await.unwrap()
        );
        Self {
            base,
            http: reqwest::Client::new(),
            _container: container,
        }
    }

    async fn put(&self, path: &str, body: Value) -> reqwest::Response {
        let response = self
            .http
            .put(format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{path}: {}",
            response.status()
        );
        response
    }

    /// Requests MockServer received on `PATH`, oldest first, with their JSON bodies.
    async fn received(&self) -> Vec<Value> {
        let response = self
            .put(
                "/mockserver/retrieve?type=REQUESTS&format=JSON",
                json!({ "path": PATH }),
            )
            .await;
        let requests: Vec<Value> = response.json().await.unwrap();
        requests
            .into_iter()
            .map(|request| match &request["body"] {
                Value::Object(body) if body.contains_key("json") => body["json"].clone(),
                Value::Object(body) => {
                    serde_json::from_str(body["string"].as_str().unwrap()).unwrap()
                }
                other => other.clone(),
            })
            .collect()
    }
}

#[tokio::test]
async fn settlement_consumer_failure_and_recovery_via_mockserver() {
    let mock = MockServer::start().await;
    // Higher priority first: two 500s, then 202 forever.
    mock.put(
        "/mockserver/expectation",
        json!([
            { "httpRequest": { "method": "POST", "path": PATH }, "httpResponse": { "statusCode": 500 },
              "times": { "remainingTimes": 2, "unlimited": false }, "priority": 10 },
            { "httpRequest": { "method": "POST", "path": PATH }, "httpResponse": { "statusCode": 202 },
              "times": { "unlimited": true }, "priority": 0 }
        ]),
    )
    .await;

    let mut app = TestApp::start().await;
    app.seed().await;
    let (status, _) = app
        .post(
            "/api/webhooks",
            json!({ "url": format!("{}{PATH}", mock.base), "eventTypes": ["TradeExecuted"] }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        app.order("A", "SELL", Some(10025), 200).await.0,
        StatusCode::CREATED
    );
    let (_, buy) = app.order("B", "BUY", Some(10025), 150).await;
    let execution_id = buy["executions"][0]["executionId"].clone();

    let mut received = Vec::new();
    for _ in 0..200 {
        received = mock.received().await;
        if received.len() >= 3
            && app.count("SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'DELIVERED'") == 1
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(received.len(), 3, "two failed attempts and one success");
    assert!(
        received.iter().all(|event| event == &received[0]),
        "every attempt sends the identical event"
    );
    let event = &received[0];
    assert_eq!(event["eventType"], "TradeExecuted");
    assert_eq!(event["payload"]["executionId"], execution_id);
    assert_eq!(
        (&event["payload"]["quantity"], &event["payload"]["price"]),
        (&json!(150), &json!("100.25"))
    );
    let (_, replay) = app.get("/api/events?eventType=TradeExecuted").await;
    assert_eq!(replay[0]["eventId"], event["eventId"], "stable event ID");
    assert_eq!(app.count("SELECT attempts FROM webhook_deliveries"), 3);
    assert_eq!(
        app.count("SELECT COUNT(*) FROM executions"),
        1,
        "retries create no trades"
    );

    app.restart().await;
    let (_, book) = app.get("/api/orderbooks/ACME").await;
    assert_eq!(
        book["asks"],
        json!([{ "priceTicks": 10025, "price": "100.25", "quantity": 50, "orders": 1 }])
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        mock.received().await.len(),
        3,
        "a delivered event is not sent again after restart"
    );
    assert_eq!(app.count("SELECT COUNT(*) FROM executions"), 1);
    app.stop().await;
}
