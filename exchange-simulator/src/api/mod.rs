//! HTTP API.

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use rusqlite::Connection;
use serde_json::{Value, json};

/// Shared handler state.
#[derive(Clone)]
pub struct AppState {
    db: Arc<Mutex<Connection>>,
}

impl AppState {
    pub fn new(db: Connection) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
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
    let db = state.db.clone();
    let probe = tokio::task::spawn_blocking(move || {
        let conn = db
            .lock()
            .map_err(|_| "database lock poisoned".to_string())?;
        crate::persistence::schema_version(&conn).map_err(|e| e.to_string())
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
