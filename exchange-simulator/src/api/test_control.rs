//! Test-control API, mounted under `/test` only when `test_control.enabled` is set. It controls simulated time and
//! must never be exposed outside test deployments.

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use std::sync::Arc;

use super::model::SubscriptionView;
use super::{ApiError, AppState};
use crate::config::Config;
use crate::engine::{ClockChange, ClockState};
use crate::persistence::{AttemptRow, DeliveryRecord};
use crate::scenario::{Report, Scenario};
use crate::webhooks::FaultPolicy;

/// The test-control routes. `config` is the service's configuration, which scenario sandboxes start from.
pub fn router(state: AppState, config: Arc<Config>) -> Router {
    Router::new()
        .route("/test/scenarios", post(run_scenario))
        .route("/test/reset", post(reset))
        .route("/test/deliveries", get(deliveries))
        .route("/test/dead-letters/retry", post(retry_dead_letters))
        .route("/test/webhooks/{id}/redeliver", post(redeliver))
        .route("/test/attempts", get(attempts))
        .route(
            "/test/webhooks/{id}/faults",
            axum::routing::put(set_faults).delete(clear_faults),
        )
        .route("/test/clock", get(clock))
        .route("/test/clock/advance", post(advance))
        .route("/test/clock/set", post(set))
        .route("/test/clock/freeze", post(freeze))
        .route("/test/clock/resume", post(resume))
        .layer(axum::Extension(config))
        .with_state(state)
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

/// A webhook delivery: which event goes to which subscription, and how far it got.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryView {
    pub delivery_id: Uuid,
    pub subscription_id: Uuid,
    pub url: String,
    pub event_sequence: u64,
    pub event_id: Uuid,
    pub event_type: String,
    pub status: String,
    pub attempts: u64,
    pub last_error: Option<String>,
}

impl From<DeliveryRecord> for DeliveryView {
    fn from(d: DeliveryRecord) -> Self {
        Self {
            delivery_id: d.delivery_id,
            subscription_id: d.subscription_id,
            url: d.url,
            event_sequence: d.event_sequence,
            event_id: d.event_id,
            event_type: d.event_type,
            status: d.status,
            attempts: d.attempts,
            last_error: d.last_error,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeliveriesQuery {
    pub subscription_id: Option<Uuid>,
    /// `PENDING`, `IN_FLIGHT`, `DELIVERED` or `DEAD` (the dead letters).
    pub status: Option<String>,
}

/// Webhook deliveries in event order, optionally one subscription's or one status. Delivery runs on real time, so
/// statuses move without clock changes.
async fn deliveries(
    State(state): State<AppState>,
    query: Result<Query<DeliveriesQuery>, QueryRejection>,
) -> Result<Json<Vec<DeliveryView>>, ApiError> {
    let Query(query) = query?;
    if let Some(status) = &query.status
        && !["PENDING", "IN_FLIGHT", "DELIVERED", "DEAD"].contains(&status.as_str())
    {
        return Err(ApiError::bad_request(format!(
            "unknown delivery status {status}"
        )));
    }
    let deliveries = state
        .read(move |store| store.deliveries(query.subscription_id, query.status.as_deref()))
        .await?;
    Ok(Json(
        deliveries.into_iter().map(DeliveryView::from).collect(),
    ))
}

/// Clears all state, as on a new database: instruments, participants, orders, trades, events, subscriptions,
/// deliveries, settlements; sequences restart at 1 and the virtual clock goes back to its first reading.
async fn reset(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    state.dispatcher.reset().await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RedeliverQuery {
    /// Redeliver events with a sequence after this one (default 0: all).
    #[serde(default)]
    pub after: u64,
}

/// Queues the subscription's events after `?after=` for delivery again, also those from before it was registered.
async fn redeliver(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
    query: Result<Query<RedeliverQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Path(id) = id?;
    let Query(query) = query?;
    let queued = state.dispatcher.redeliver(id, query.after).await?;
    Ok(Json(serde_json::json!({ "queued": queued })))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetryRequest {
    pub subscription_id: Option<Uuid>,
    #[serde(default)]
    pub delivery_ids: Vec<Uuid>,
}

/// Retries dead letters with a fresh attempt budget. An empty body retries all of them; `subscriptionId` and
/// `deliveryIds` narrow it down.
async fn retry_dead_letters(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, ApiError> {
    let request: RetryRequest = if body.is_empty() {
        RetryRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::bad_request(e.to_string()))?
    };
    let retried = state
        .dispatcher
        .retry_dead_letters(request.subscription_id, request.delivery_ids)
        .await?;
    Ok(Json(serde_json::json!({ "retried": retried })))
}

/// Sets a subscription's delivery fault policy (see [`crate::webhooks::faults`]); returns the subscription.
async fn set_faults(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<FaultPolicy>, JsonRejection>,
) -> Result<Json<SubscriptionView>, ApiError> {
    let Path(id) = id?;
    let Json(policy) = body?;
    let subscription = state
        .dispatcher
        .set_webhook_faults(id, Some(policy))
        .await?;
    Ok(Json(subscription.into()))
}

/// Removes a subscription's fault policy: its deliveries go out unchanged again.
async fn clear_faults(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<SubscriptionView>, ApiError> {
    let Path(id) = id?;
    let subscription = state.dispatcher.set_webhook_faults(id, None).await?;
    Ok(Json(subscription.into()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AttemptsQuery {
    pub subscription_id: Option<Uuid>,
}

/// What was actually sent for a delivery: one request, a duplicate of it, or a hold.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttemptView {
    pub id: u64,
    pub delivery_id: Uuid,
    pub subscription_id: Uuid,
    pub url: String,
    pub event_sequence: u64,
    pub event_id: Uuid,
    pub event_type: String,
    pub attempt: u32,
    /// `SENT` or `HELD`.
    pub outcome: &'static str,
    pub faults: Vec<String>,
    /// The body sent when a fault changed it; absent when the canonical envelope went out.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    pub http_status: Option<u16>,
    pub error: Option<String>,
    pub at: DateTime<Utc>,
}

impl From<AttemptRow> for AttemptView {
    fn from(a: AttemptRow) -> Self {
        Self {
            id: a.id,
            delivery_id: a.delivery_id,
            subscription_id: a.subscription_id,
            url: a.url,
            event_sequence: a.event_sequence,
            event_id: a.event_id,
            event_type: a.event_type,
            attempt: a.attempt,
            outcome: if a.held { "HELD" } else { "SENT" },
            faults: a.faults,
            body: a
                .body
                .map(|b| serde_json::from_str(&b).unwrap_or(Value::String(b))),
            http_status: a.http_status,
            error: a.error,
            at: a.at,
        }
    }
}

/// Every recorded webhook attempt, oldest first; `?subscriptionId=` narrows to one subscription.
async fn attempts(
    State(state): State<AppState>,
    query: Result<Query<AttemptsQuery>, QueryRejection>,
) -> Result<Json<Vec<AttemptView>>, ApiError> {
    let Query(query) = query?;
    let rows = state
        .read(move |store| store.attempts(query.subscription_id))
        .await?;
    Ok(Json(rows.into_iter().map(AttemptView::from).collect()))
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
