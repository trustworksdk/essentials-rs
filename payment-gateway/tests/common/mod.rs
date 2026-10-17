//! Shared harness: the whole service (router, gateway, delivery worker, ticker) on a SQLite file, a virtual clock
//! frozen at Monday 2030-01-07 09:00 UTC, and test control enabled.
#![allow(dead_code)] // Each test binary uses a different subset.

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use payment_gateway::app::App;
use payment_gateway::config::{ClockMode, Config};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

pub const START: &str = "2030-01-07T09:00:00Z";

pub struct TestApp {
    app: Option<App>,
    dir: PathBuf,
    pub config: Config,
}

impl TestApp {
    pub async fn start() -> Self {
        Self::start_with(|_| {}).await
    }

    pub async fn start_with(adjust: impl FnOnce(&mut Config)) -> Self {
        let dir = std::env::temp_dir().join(format!("payment-gateway-it-{}", Uuid::now_v7()));
        let mut config = Config::defaults();
        config.database.path = dir.join("gateway.db").to_string_lossy().into_owned();
        config.clock.mode = ClockMode::Virtual;
        config.clock.start = Some(START.parse().unwrap());
        config.test_control.enabled = true;
        config.statement.tick_interval_ms = 20;
        config.webhooks.poll_interval_ms = 20;
        config.webhooks.initial_backoff_ms = 20;
        config.webhooks.max_backoff_ms = 100;
        config.webhooks.max_attempts = 5;
        config.webhooks.request_timeout_ms = 2000;
        config.logging.filter = "warn".into();
        // Every payment completes unless a test says otherwise.
        config.payments.deposit.fail_probability = 0.0;
        config.payments.withdrawal.fail_probability = 0.0;
        config.payments.return_probability = 0.0;
        adjust(&mut config);
        let mut test = Self {
            app: None,
            dir,
            config,
        };
        test.restart().await;
        test
    }

    pub async fn restart(&mut self) {
        self.stop().await;
        self.app = Some(App::start(&self.config).unwrap());
    }

    pub async fn stop(&mut self) {
        if let Some(app) = self.app.take() {
            app.shutdown().await;
        }
    }

    pub fn router(&self) -> axum::Router {
        self.app.as_ref().expect("running").router.clone()
    }

    pub fn db(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(&self.config.database.path).unwrap()
    }

    pub async fn call(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let router = self.app.as_ref().expect("running").router.clone();
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(match body {
                Some(body) => Body::from(body.to_string()),
                None => Body::empty(),
            })
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
        };
        (status, value)
    }

    pub async fn get(&self, uri: &str) -> (StatusCode, Value) {
        self.call(Method::GET, uri, None).await
    }

    pub async fn post(&self, uri: &str, body: Value) -> (StatusCode, Value) {
        self.call(Method::POST, uri, Some(body)).await
    }

    /// Initiates a payment and expects `201`.
    pub async fn pay(&self, key: &str, kind: &str, amount: &str, currency: &str) -> Value {
        let (status, body) = self
            .post("/api/payments", payment(key, kind, amount, currency))
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        body
    }

    pub async fn set_clock(&self, to: &str) -> Value {
        let (status, body) = self.post("/test/clock/set", json!({ "to": to })).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    pub async fn advance(&self, by: Value) -> Value {
        let (status, body) = self.post("/test/clock/advance", by).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    pub async fn events(&self, event_type: &str) -> Vec<Value> {
        self.get(&format!("/api/events?eventType={event_type}&limit=1000"))
            .await
            .1
            .as_array()
            .unwrap()
            .clone()
    }

    pub async fn all_events(&self) -> Vec<Value> {
        self.get("/api/events?limit=1000")
            .await
            .1
            .as_array()
            .unwrap()
            .clone()
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        drop(self.app.take());
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub fn payment(key: &str, kind: &str, amount: &str, currency: &str) -> Value {
    json!({
        "idempotencyKey": key,
        "type": kind,
        "accountId": "acc-1",
        "bankAccount": { "iban": "DE89 3704 0044 0532 0130 00", "holderName": "A. Customer" },
        "amount": amount,
        "currency": currency,
    })
}
