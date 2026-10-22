//! Shared harness for tests that run the whole service: router, exchange thread and delivery worker on a SQLite file.
#![allow(dead_code)] // Each test binary uses a different subset.

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use exchange_simulator::app::App;
use exchange_simulator::config::{
    ClockConfig, ClockMode, Config, DatabaseConfig, EventsConfig, ExchangeConfig, IdsConfig,
    LogFormat, LoggingConfig, ServerConfig, SettlementConfig, TestControlConfig, WebhooksConfig,
};
use exchange_simulator::domain::IdentifierPolicy;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

pub struct TestApp {
    app: Option<App>,
    dir: PathBuf,
    pub config: Config,
}

/// Fast webhook timings for tests.
pub fn webhooks_config() -> WebhooksConfig {
    WebhooksConfig {
        subscriptions: Vec::new(),
        secret_grace_seconds: 3600,
        poll_interval_ms: 20,
        batch_size: 50,
        concurrency: 8,
        request_timeout_ms: 2000,
        max_attempts: 5,
        initial_backoff_ms: 20,
        max_backoff_ms: 100,
    }
}

impl TestApp {
    pub async fn start() -> Self {
        Self::start_with(|_| {}).await
    }

    pub async fn start_with(adjust: impl FnOnce(&mut Config)) -> Self {
        let dir = std::env::temp_dir().join(format!("exchange-sim-it-{}", Uuid::now_v7()));
        let mut config = Self::config(&dir);
        adjust(&mut config);
        let mut test = Self {
            app: None,
            dir,
            config,
        };
        test.restart().await;
        test
    }

    /// The test configuration with its database in `dir`.
    pub fn config(dir: &std::path::Path) -> Config {
        Config {
            server: ServerConfig {
                bind: "127.0.0.1:0".into(),
            },
            exchange: ExchangeConfig {
                identifier_policy: IdentifierPolicy::Strict,
                command_queue_capacity: 1024,
                group_commit: Default::default(),
            },
            events: EventsConfig::default(),
            webhooks: webhooks_config(),
            instruments: Vec::new(),
            price_sources: Default::default(),
            liquidity: Default::default(),
            market_data: Default::default(),
            sessions: Default::default(),
            database: DatabaseConfig {
                mode: Default::default(),
                memory_max_mib: 1024,
                checkpoint: Default::default(),
                checkpoint_pages: 1000,
                path: dir.join("exchange.db").to_string_lossy().into_owned(),
                busy_timeout_ms: 5000,
            },
            clock: ClockConfig {
                mode: ClockMode::System,
                start: None,
                running: false,
                speed: 1.0,
                operator_control: false,
                follow: None,
            },
            ids: IdsConfig::default(),
            settlement: SettlementConfig::default(),
            clearing: Default::default(),
            trades: Default::default(),
            test_control: TestControlConfig { enabled: true },
            logging: LoggingConfig {
                filter: "warn".into(),
                format: LogFormat::Pretty,
            },
        }
    }

    /// Stops the service (draining the exchange thread) and starts a new one on the same database.
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

    /// A separate connection to the database, for asserting on stored state. In `memory` mode only while the service
    /// runs.
    pub fn db(&self) -> rusqlite::Connection {
        let location = match &self.app {
            Some(app) => app.database_location().to_string(),
            None => self.config.database.path.clone(),
        };
        rusqlite::Connection::open(location).unwrap()
    }

    pub fn count(&self, sql: &str) -> i64 {
        self.db().query_row(sql, [], |row| row.get(0)).unwrap()
    }

    pub async fn call(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let (status, _, body) = self.call_full(method, uri, body).await;
        (status, body)
    }

    /// [`Self::call`] with the response headers.
    pub async fn call_full(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, axum::http::HeaderMap, Value) {
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
            .expect("running")
            .router
            .clone()
            .oneshot(request)
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, headers, body)
    }

    pub async fn get(&self, uri: &str) -> (StatusCode, Value) {
        self.call(Method::GET, uri, None).await
    }

    pub async fn post(&self, uri: &str, body: Value) -> (StatusCode, Value) {
        self.call(Method::POST, uri, Some(body)).await
    }

    pub async fn delete(&self, uri: &str) -> (StatusCode, Value) {
        self.call(Method::DELETE, uri, None).await
    }

    /// ACME (DKK, scale 2, tick 1) and participants A to D.
    pub async fn seed(&self) {
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

    pub async fn order(
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
        // Best effort: tests should `stop().await`; otherwise the runtime's end stops the worker and thread.
        drop(self.app.take());
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub fn acme() -> Value {
    json!({ "id": "ACME", "symbol": "ACME", "mic": "XCSE", "currency": "DKK", "currencyScale": 2, "tickSizeMinor": 1 })
}
