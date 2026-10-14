//! HTTP API.

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

use crate::engine::Exchange;

/// Shared handler state.
#[derive(Clone)]
pub struct AppState {
    exchange: Arc<Mutex<Exchange>>,
}

impl AppState {
    pub fn new(exchange: Exchange) -> Self {
        Self {
            exchange: Arc::new(Mutex::new(exchange)),
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .with_state(state)
}

/// Liveness and readiness: the process is up and the database answers.
async fn health(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let exchange = state.exchange.clone();
    let probe = tokio::task::spawn_blocking(move || {
        let exchange = exchange
            .lock()
            .map_err(|_| "exchange lock poisoned".to_string())?;
        crate::persistence::schema_version(exchange.store().connection()).map_err(|e| e.to_string())
    })
    .await
    .unwrap_or_else(|e| Err(e.to_string()));
    match probe {
        Ok(version) => (
            StatusCode::OK,
            Json(json!({ "status": "ok", "schemaVersion": version })),
        ),
        Err(error) => {
            tracing::warn!(%error, "health check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "unavailable", "error": error })),
            )
        }
    }
}
