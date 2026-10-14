//! Test-control API, mounted under `/test` only when `test_control.enabled` is set. It controls simulated time and
//! must never be exposed outside test deployments.

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ApiError, AppState};
use crate::engine::{ClockChange, ClockState};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/test/clock", get(clock))
        .route("/test/clock/advance", post(advance))
        .route("/test/clock/set", post(set))
        .route("/test/clock/freeze", post(freeze))
        .route("/test/clock/resume", post(resume))
        .with_state(state)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClockView {
    /// `virtual` or `system`.
    pub mode: &'static str,
    pub running: bool,
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

async fn change(state: &AppState, change: ClockChange) -> Result<Json<ClockView>, ApiError> {
    Ok(Json(state.dispatcher.change_clock(change).await?.into()))
}
