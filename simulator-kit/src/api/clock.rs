//! The clock routes the simulators share: `GET /api/clock`, always mounted, so a consumer can follow simulated time;
//! speed, freeze and resume under `/api/clock` for operators (`clock.operator_control`, decision D17); and the full
//! set under `/test/clock`, with set and advance, for test deployments.

use std::future::Future;

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::ApiError;
use crate::clock::{ClockChange, ClockState};

/// What the clock routes need from a simulator.
pub trait ClockBackend: Clone + Send + Sync + 'static {
    fn clock_state(&self) -> impl Future<Output = Result<ClockState, ApiError>> + Send;
    /// Applies the change, publishes `ClockChanged` when it changed something, runs whatever became due, and answers
    /// with the state after it. A system clock refuses every change.
    fn change_clock(
        &self,
        change: ClockChange,
    ) -> impl Future<Output = Result<ClockState, ApiError>> + Send;
}

/// `GET /api/clock`, and with `operator_control` speed, freeze and resume.
pub fn router<B: ClockBackend>(backend: B, operator_control: bool) -> Router {
    let router = Router::new().route("/api/clock", get(clock::<B>));
    let router = if operator_control {
        router
            .route("/api/clock/freeze", post(freeze::<B>))
            .route("/api/clock/resume", post(resume::<B>))
            .route("/api/clock/speed", post(speed::<B>))
    } else {
        router
    };
    router.with_state(backend)
}

/// The test-control clock routes, under `/test`: mount them only in test deployments.
pub fn test_router<B: ClockBackend>(backend: B) -> Router {
    Router::new()
        .route("/test/clock", get(clock::<B>))
        .route("/test/clock/advance", post(advance::<B>))
        .route("/test/clock/set", post(set::<B>))
        .route("/test/clock/freeze", post(freeze::<B>))
        .route("/test/clock/resume", post(resume::<B>))
        .route("/test/clock/speed", post(speed::<B>))
        .with_state(backend)
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

async fn clock<B: ClockBackend>(State(backend): State<B>) -> Result<Json<ClockView>, ApiError> {
    Ok(Json(backend.clock_state().await?.into()))
}

async fn advance<B: ClockBackend>(
    State(backend): State<B>,
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
    change(&backend, ClockChange::Advance(by)).await
}

async fn set<B: ClockBackend>(
    State(backend): State<B>,
    body: Result<Json<SetRequest>, JsonRejection>,
) -> Result<Json<ClockView>, ApiError> {
    let Json(body) = body?;
    change(&backend, ClockChange::Set(body.to)).await
}

async fn freeze<B: ClockBackend>(State(backend): State<B>) -> Result<Json<ClockView>, ApiError> {
    change(&backend, ClockChange::Freeze).await
}

async fn resume<B: ClockBackend>(State(backend): State<B>) -> Result<Json<ClockView>, ApiError> {
    change(&backend, ClockChange::Resume).await
}

/// Changes how fast the clock runs, from its current reading on. Frozen, it stays frozen at the new speed.
async fn speed<B: ClockBackend>(
    State(backend): State<B>,
    body: Result<Json<SpeedRequest>, JsonRejection>,
) -> Result<Json<ClockView>, ApiError> {
    let Json(body) = body?;
    change(&backend, ClockChange::Speed(body.speed)).await
}

async fn change<B: ClockBackend>(
    backend: &B,
    change: ClockChange,
) -> Result<Json<ClockView>, ApiError> {
    Ok(Json(backend.change_clock(change).await?.into()))
}
