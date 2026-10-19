//! Test-control API, mounted under `/test` only when `test_control.enabled` is set. It controls simulated time and
//! must never be exposed outside test deployments.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};

use std::sync::Arc;

use super::{ApiError, AppState};
use crate::config::Config;
use crate::scenario::{Report, Scenario};

/// The test-control routes. `config` is the service's configuration, which scenario sandboxes start from.
pub fn router(state: AppState, config: Arc<Config>) -> Router {
    Router::new()
        .route("/test/scenarios", post(run_scenario))
        .route("/test/reset", post(reset))
        .route("/test/clearing/breaks", get(super::clearing::breaks))
        .layer(axum::Extension(config))
        .with_state(state.clone())
        .merge(simulator_kit::api::clock::test_router(state.clone()))
        .merge(simulator_kit::api::webhooks::test_router(state))
}

/// Runs a scenario (YAML or JSON in the request body) in a fresh sandbox, independent of this service's state, and
/// returns its report: `200` whether it passed or not (see `passed`), `400` if it cannot be parsed.
async fn run_scenario(
    axum::Extension(config): axum::Extension<Arc<Config>>,
    body: String,
) -> Result<Json<Report>, ApiError> {
    let scenario = Scenario::parse(&body).map_err(|error| {
        ApiError::new(
            axum::http::StatusCode::BAD_REQUEST,
            "invalid_scenario",
            error,
        )
    })?;
    let report = crate::scenario::run(&scenario, &config)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(report))
}

/// Clears all state, as on a new database: instruments, participants, orders, trades, events, subscriptions,
/// deliveries, settlements; sequences restart at 1 and the virtual clock goes back to its first reading.
async fn reset(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    state.dispatcher.reset().await?;
    Ok(StatusCode::NO_CONTENT)
}
