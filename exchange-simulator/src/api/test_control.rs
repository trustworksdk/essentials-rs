//! Test-control API, mounted under `/test` only when `test_control.enabled` is set. It controls simulated time and
//! must never be exposed outside test deployments.

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::sync::Arc;

use super::{ApiError, AppState};
use crate::config::Config;
use crate::engine::{ClockChange, ClockState};
use crate::scenario::{Report, Scenario};

/// The test-control routes. `config` is the service's configuration, which scenario sandboxes start from.
pub fn router(state: AppState, config: Arc<Config>) -> Router {
    Router::new()
        .route("/test/scenarios", post(run_scenario))
        .route("/test/reset", post(reset))
        .route("/test/clearing/breaks", get(super::clearing::breaks))
        .route("/test/clock", get(clock))
        .route("/test/clock/advance", post(advance))
        .route("/test/clock/set", post(set))
        .route("/test/clock/freeze", post(freeze))
        .route("/test/clock/resume", post(resume))
        .route("/test/clock/speed", post(speed))
        .layer(axum::Extension(config))
        .with_state(state.clone())
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClockView {
    /// `virtual` or `system`.
    pub mode: &'static str,
    pub running: bool,
    /// Simulated seconds per real second while running.
    pub speed: f64,
    pub now: DateTime<Utc>,
}

impl From<ClockState> for ClockView {
    fn from(state: ClockState) -> Self {
        Self {
            mode: if state.virtual_clock {
                "virtual"
            } else {
                "system"
            },
            running: state.running,
            speed: state.speed,
            now: state.now,
        }
    }
}

/// How far to move the clock: the given units are added up and must total more than zero.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdvanceRequest {
    #[serde(default)]
    pub days: u32,
    #[serde(default)]
    pub hours: u32,
    #[serde(default)]
    pub minutes: u32,
    #[serde(default)]
    pub seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SetRequest {
    pub to: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SpeedRequest {
    /// Simulated seconds per real second, more than 0 and at most 1000000.
    pub speed: f64,
}

async fn clock(State(state): State<AppState>) -> Result<Json<ClockView>, ApiError> {
    Ok(Json(state.dispatcher.clock_state().await?.into()))
}

async fn advance(
    State(state): State<AppState>,
    body: Result<Json<AdvanceRequest>, JsonRejection>,
) -> Result<Json<ClockView>, ApiError> {
    let Json(body) = body?;
    let seconds =
        i64::try_from(body.seconds).map_err(|_| ApiError::bad_request("seconds is too large"))?;
    let by = chrono::Duration::days(i64::from(body.days))
        + chrono::Duration::hours(i64::from(body.hours))
        + chrono::Duration::minutes(i64::from(body.minutes))
        + chrono::Duration::try_seconds(seconds)
            .ok_or_else(|| ApiError::bad_request("seconds is too large"))?;
    if by <= chrono::Duration::zero() {
        return Err(ApiError::bad_request(
            "advance by a positive amount of days, hours, minutes or seconds",
        ));
    }
    change(&state, ClockChange::Advance(by)).await
}

async fn set(
    State(state): State<AppState>,
    body: Result<Json<SetRequest>, JsonRejection>,
) -> Result<Json<ClockView>, ApiError> {
    let Json(body) = body?;
    change(&state, ClockChange::Set(body.to)).await
}

async fn freeze(State(state): State<AppState>) -> Result<Json<ClockView>, ApiError> {
    change(&state, ClockChange::Freeze).await
}

async fn resume(State(state): State<AppState>) -> Result<Json<ClockView>, ApiError> {
    change(&state, ClockChange::Resume).await
}

/// Changes how fast the clock runs, from its current reading on. Frozen, it stays frozen at the new speed.
async fn speed(
    State(state): State<AppState>,
    body: Result<Json<SpeedRequest>, JsonRejection>,
) -> Result<Json<ClockView>, ApiError> {
    let Json(body) = body?;
    change(&state, ClockChange::Speed(body.speed)).await
}

async fn change(state: &AppState, change: ClockChange) -> Result<Json<ClockView>, ApiError> {
    Ok(Json(state.dispatcher.change_clock(change).await?.into()))
}
