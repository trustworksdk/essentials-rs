//! Webhook subscriptions under `/api/webhooks`: registration, listing, and the maintenance operations (step 4.6b):
//! inspect with delivery stats, change, pause, delete, rotate the secret, list deliveries and dead letters, redeliver
//! and retry. Subscriptions declared in configuration are read-only here, except `paused`.

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use super::model::{SubscriptionView, WebhookRequest};
use super::{ApiError, AppState};
use crate::persistence::{DeliveryRecord, DeliveryStats};
use crate::webhooks::{NewSubscription, WebhookChange};

/// The longest grace period a secret rotation accepts: a week.
const MAX_GRACE_SECONDS: u64 = 7 * 24 * 3600;
/// Deliveries listed per request by default, and at most.
const DEFAULT_LIMIT: u32 = 100;
const MAX_LIMIT: u32 = 1000;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/webhooks", post(register).get(list))
        .route(
            "/api/webhooks/{id}",
            get(detail).patch(update).delete(delete),
        )
        .route("/api/webhooks/{id}/secret", post(rotate_secret))
        .route("/api/webhooks/{id}/deliveries", get(deliveries))
        .route("/api/webhooks/{id}/redeliver", post(redeliver))
        .route(
            "/api/webhooks/{id}/dead-letters/retry",
            post(retry_dead_letters),
        )
        .with_state(state)
}

async fn register(
    State(state): State<AppState>,
    body: Result<Json<WebhookRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<SubscriptionView>), ApiError> {
    let Json(body) = body?;
    let subscription = state
        .dispatcher
        .register_webhook(NewSubscription {
            url: body.url,
            event_types: body.event_types,
            secret: body.secret,
            ordered: body.ordered,
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(SubscriptionView::from(subscription)),
    ))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListQuery {
    /// Also list deleted subscriptions (shown with `deleted: true`).
    #[serde(default)]
    include_deleted: bool,
}

async fn list(
    State(state): State<AppState>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<Vec<SubscriptionView>>, ApiError> {
    let Query(query) = query?;
    let subscriptions = state.read(|store| store.subscriptions()).await?;
    Ok(Json(
        subscriptions
            .into_iter()
            .filter(|s| query.include_deleted || !s.deleted)
            .map(SubscriptionView::from)
            .collect(),
    ))
}

/// A subscription with how its deliveries stand.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Detail {
    #[serde(flatten)]
    subscription: SubscriptionView,
    deliveries: StatsView,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsView {
    pending: u64,
    in_flight: u64,
    delivered: u64,
    dead: u64,
    /// The oldest delivery not yet finished: how far behind the consumer is.
    oldest_unfinished: Option<Oldest>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Oldest {
    event_sequence: u64,
    occurred_at: DateTime<Utc>,
}

impl From<DeliveryStats> for StatsView {
    fn from(s: DeliveryStats) -> Self {
        Self {
            pending: s.pending,
            in_flight: s.in_flight,
            delivered: s.delivered,
            dead: s.dead,
            oldest_unfinished: s
                .oldest_unfinished
                .map(|(event_sequence, occurred_at)| Oldest {
                    event_sequence,
                    occurred_at,
                }),
        }
    }
}

async fn detail(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<Detail>, ApiError> {
    let Path(id) = id?;
    let found = state
        .read(move |store| {
            let Some(subscription) = store
                .subscriptions()?
                .into_iter()
                .find(|s| s.id == id && !s.deleted)
            else {
                return Ok(None);
            };
            Ok(Some((subscription, store.delivery_stats(id)?)))
        })
        .await?;
    let (subscription, stats) =
        found.ok_or_else(|| ApiError::not_found(format!("unknown webhook subscription {id}")))?;
    Ok(Json(Detail {
        subscription: subscription.into(),
        deliveries: stats.into(),
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateRequest {
    url: Option<String>,
    event_types: Option<Vec<String>>,
    ordered: Option<bool>,
    enabled: Option<bool>,
    paused: Option<bool>,
}

async fn update(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<UpdateRequest>, JsonRejection>,
) -> Result<Json<SubscriptionView>, ApiError> {
    let Path(id) = id?;
    let Json(body) = body?;
    let subscription = state
        .dispatcher
        .update_webhook(
            id,
            WebhookChange {
                url: body.url,
                event_types: body.event_types,
                ordered: body.ordered,
                enabled: body.enabled,
                paused: body.paused,
            },
        )
        .await?;
    Ok(Json(subscription.into()))
}

async fn delete(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
) -> Result<StatusCode, ApiError> {
    let Path(id) = id?;
    state.dispatcher.delete_webhook(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RotateRequest {
    /// The new secret, 16–256 characters.
    secret: String,
    /// How long the previous secret stays valid (default an hour, at most a week; 0 drops it at once).
    #[serde(default = "default_grace")]
    grace_seconds: u64,
}

fn default_grace() -> u64 {
    3600
}

async fn rotate_secret(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<RotateRequest>, JsonRejection>,
) -> Result<Json<SubscriptionView>, ApiError> {
    let Path(id) = id?;
    let Json(body) = body?;
    if body.grace_seconds > MAX_GRACE_SECONDS {
        return Err(ApiError::bad_request(format!(
            "graceSeconds must be at most {MAX_GRACE_SECONDS}"
        )));
    }
    let grace = chrono::Duration::seconds(body.grace_seconds as i64);
    let subscription = state
        .dispatcher
        .rotate_webhook_secret(id, body.secret, grace)
        .await?;
    Ok(Json(subscription.into()))
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
    /// When the next attempt is due (pending) or the current one's lease ends (in flight).
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub delivered_at: Option<DateTime<Utc>>,
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
            next_attempt_at: d.next_attempt_at,
            delivered_at: d.delivered_at,
        }
    }
}

/// Checks a delivery status filter.
pub fn delivery_status(status: Option<&str>) -> Result<(), ApiError> {
    match status {
        Some(status) if !["PENDING", "IN_FLIGHT", "DELIVERED", "DEAD"].contains(&status) => Err(
            ApiError::bad_request(format!("unknown delivery status {status}")),
        ),
        _ => Ok(()),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeliveriesQuery {
    /// `PENDING`, `IN_FLIGHT`, `DELIVERED` or `DEAD` (the dead letters).
    status: Option<String>,
    /// Only deliveries of events with a sequence after this one (default 0), for paging.
    #[serde(default)]
    after: u64,
    limit: Option<u32>,
}

async fn deliveries(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
    query: Result<Query<DeliveriesQuery>, QueryRejection>,
) -> Result<Json<Vec<DeliveryView>>, ApiError> {
    let Path(id) = id?;
    let Query(query) = query?;
    delivery_status(query.status.as_deref())?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(ApiError::bad_request(format!(
            "limit must be 1 to {MAX_LIMIT}"
        )));
    }
    let found = state
        .read(move |store| {
            if !store
                .subscriptions()?
                .iter()
                .any(|s| s.id == id && !s.deleted)
            {
                return Ok(None);
            }
            store
                .deliveries(Some(id), query.status.as_deref(), query.after, limit)
                .map(Some)
        })
        .await?;
    let deliveries =
        found.ok_or_else(|| ApiError::not_found(format!("unknown webhook subscription {id}")))?;
    Ok(Json(
        deliveries.into_iter().map(DeliveryView::from).collect(),
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RedeliverQuery {
    /// Redeliver events with a sequence after this one (default 0: all).
    #[serde(default)]
    pub after: u64,
}

/// Queues the subscription's events after `?after=` for delivery again, also those from before it was registered.
pub async fn redeliver(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
    query: Result<Query<RedeliverQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Path(id) = id?;
    let Query(query) = query?;
    let queued = state.dispatcher.redeliver(id, query.after).await?;
    Ok(Json(json!({ "queued": queued })))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RetryRequest {
    #[serde(default)]
    delivery_ids: Vec<Uuid>,
}

/// Retries the subscription's dead letters (all, or the listed ones) with a fresh attempt budget.
async fn retry_dead_letters(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, ApiError> {
    let Path(id) = id?;
    let request: RetryRequest = if body.is_empty() {
        RetryRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::bad_request(e.to_string()))?
    };
    let retried = state
        .dispatcher
        .retry_dead_letters(Some(id), request.delivery_ids)
        .await?;
    Ok(Json(json!({ "retried": retried })))
}
