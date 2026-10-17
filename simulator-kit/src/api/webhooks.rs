//! Webhook subscriptions under `/api/webhooks`: registration, listing, and the maintenance operations: inspect with
//! delivery stats, change, pause, delete, rotate the secret, list deliveries and dead letters, redeliver and retry.
//! Subscriptions declared in configuration are read-only here, except `paused`. [`test_router`] adds the
//! test-control routes under `/test`: every delivery and attempt, dead-letter retries across subscriptions, and
//! fault policies. A simulator mounts both over its own [`WebhookBackend`].

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use rusqlite::Connection;

use super::ApiError;
use crate::outbox::{self, AttemptRow, DeliveryRecord, DeliveryStats};
use crate::webhooks::{FaultPolicy, NewSubscription, Subscription, WebhookChange};

/// What the webhook routes need from a simulator: changes go to its single writer, queries to a read connection.
pub trait WebhookBackend: Clone + Send + Sync + 'static {
    fn register(
        &self,
        request: NewSubscription,
    ) -> impl Future<Output = Result<Subscription, ApiError>> + Send;
    fn update(
        &self,
        id: Uuid,
        change: WebhookChange,
    ) -> impl Future<Output = Result<Subscription, ApiError>> + Send;
    fn delete(&self, id: Uuid) -> impl Future<Output = Result<(), ApiError>> + Send;
    fn rotate_secret(
        &self,
        id: Uuid,
        secret: String,
        grace: chrono::Duration,
    ) -> impl Future<Output = Result<Subscription, ApiError>> + Send;
    fn set_faults(
        &self,
        id: Uuid,
        policy: Option<FaultPolicy>,
    ) -> impl Future<Output = Result<Subscription, ApiError>> + Send;
    fn redeliver(
        &self,
        id: Uuid,
        after: u64,
    ) -> impl Future<Output = Result<usize, ApiError>> + Send;
    fn retry_dead_letters(
        &self,
        subscription_id: Option<Uuid>,
        delivery_ids: Vec<Uuid>,
    ) -> impl Future<Output = Result<usize, ApiError>> + Send;
    /// Runs a query on a read connection, off the async runtime.
    fn read<T: Send + 'static>(
        &self,
        query: impl FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
    ) -> impl Future<Output = Result<T, ApiError>> + Send;
}

/// The longest grace period a secret rotation accepts: a week.
const MAX_GRACE_SECONDS: u64 = 7 * 24 * 3600;
/// Deliveries listed per request by default, and at most.
const DEFAULT_LIMIT: u32 = 100;
const MAX_LIMIT: u32 = 1000;

/// The `/api/webhooks` routes.
pub fn router<B: WebhookBackend>(backend: B) -> Router {
    Router::new()
        .route("/api/webhooks", post(register::<B>).get(list::<B>))
        .route(
            "/api/webhooks/{id}",
            get(detail::<B>).patch(update::<B>).delete(delete::<B>),
        )
        .route("/api/webhooks/{id}/secret", post(rotate_secret::<B>))
        .route("/api/webhooks/{id}/deliveries", get(deliveries::<B>))
        .route("/api/webhooks/{id}/redeliver", post(redeliver::<B>))
        .route(
            "/api/webhooks/{id}/dead-letters/retry",
            post(retry_dead_letters::<B>),
        )
        .with_state(backend)
}

async fn register<B: WebhookBackend>(
    State(backend): State<B>,
    body: Result<Json<WebhookRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<SubscriptionView>), ApiError> {
    let Json(body) = body?;
    let subscription = backend
        .register(NewSubscription {
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

async fn list<B: WebhookBackend>(
    State(backend): State<B>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<Vec<SubscriptionView>>, ApiError> {
    let Query(query) = query?;
    let subscriptions = backend.read(outbox::subscriptions).await?;
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

async fn detail<B: WebhookBackend>(
    State(backend): State<B>,
    id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<Detail>, ApiError> {
    let Path(id) = id?;
    let found = backend
        .read(move |conn| {
            let Some(subscription) = outbox::subscriptions(conn)?
                .into_iter()
                .find(|s| s.id == id && !s.deleted)
            else {
                return Ok(None);
            };
            Ok(Some((subscription, outbox::delivery_stats(conn, id)?)))
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

async fn update<B: WebhookBackend>(
    State(backend): State<B>,
    id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<UpdateRequest>, JsonRejection>,
) -> Result<Json<SubscriptionView>, ApiError> {
    let Path(id) = id?;
    let Json(body) = body?;
    let subscription = backend
        .update(
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

async fn delete<B: WebhookBackend>(
    State(backend): State<B>,
    id: Result<Path<Uuid>, PathRejection>,
) -> Result<StatusCode, ApiError> {
    let Path(id) = id?;
    backend.delete(id).await?;
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

async fn rotate_secret<B: WebhookBackend>(
    State(backend): State<B>,
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
    let subscription = backend.rotate_secret(id, body.secret, grace).await?;
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

async fn deliveries<B: WebhookBackend>(
    State(backend): State<B>,
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
    let found = backend
        .read(move |conn| {
            if !outbox::subscriptions(conn)?
                .iter()
                .any(|s| s.id == id && !s.deleted)
            {
                return Ok(None);
            }
            outbox::deliveries(conn, Some(id), query.status.as_deref(), query.after, limit)
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
pub async fn redeliver<B: WebhookBackend>(
    State(backend): State<B>,
    id: Result<Path<Uuid>, PathRejection>,
    query: Result<Query<RedeliverQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Path(id) = id?;
    let Query(query) = query?;
    let queued = backend.redeliver(id, query.after).await?;
    Ok(Json(json!({ "queued": queued })))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RetryRequest {
    #[serde(default)]
    delivery_ids: Vec<Uuid>,
}

/// Retries the subscription's dead letters (all, or the listed ones) with a fresh attempt budget.
async fn retry_dead_letters<B: WebhookBackend>(
    State(backend): State<B>,
    id: Result<Path<Uuid>, PathRejection>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, ApiError> {
    let Path(id) = id?;
    let request: RetryRequest = if body.is_empty() {
        RetryRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::bad_request(e.to_string()))?
    };
    let retried = backend
        .retry_dead_letters(Some(id), request.delivery_ids)
        .await?;
    Ok(Json(json!({ "retried": retried })))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WebhookRequest {
    pub url: String,
    pub event_types: Vec<String>,
    /// Optional HMAC secret (16–256 characters); when set, every delivery is signed. Never returned.
    pub secret: Option<String>,
    /// Deliver one at a time in event sequence order (default false).
    #[serde(default)]
    pub ordered: bool,
}

/// A subscription as the API shows it: the secret is never returned, only whether there is one.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionView {
    pub id: Uuid,
    pub url: String,
    pub event_types: Vec<String>,
    pub enabled: bool,
    /// Deliveries are recorded but held until it is unpaused.
    pub paused: bool,
    pub signed: bool,
    pub ordered: bool,
    pub created_at: DateTime<Utc>,
    /// The delivery fault policy, when test control set one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub faults: Option<FaultPolicy>,
    /// The name it is declared under in `webhooks.subscriptions`; API-created subscriptions have none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `api` or `config`: who owns it.
    pub source: crate::webhooks::SubscriptionSource,
    /// After a secret rotation, until when deliveries are also signed with the previous secret.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_secret_valid_until: Option<DateTime<Utc>>,
    /// Shown only when listing with `includeDeleted=true`.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub deleted: bool,
}

impl From<Subscription> for SubscriptionView {
    fn from(s: Subscription) -> Self {
        Self {
            id: s.id,
            url: s.url,
            event_types: s.event_types,
            enabled: s.enabled,
            paused: s.paused,
            signed: s.secret.is_some(),
            ordered: s.ordered,
            created_at: s.created_at,
            faults: s.faults,
            name: s.name,
            source: s.source,
            previous_secret_valid_until: s.previous_secret.map(|(_, until)| until),
            deleted: s.deleted,
        }
    }
}

/// The test-control routes for webhooks, under `/test`: mount them only in test deployments.
pub fn test_router<B: WebhookBackend>(backend: B) -> Router {
    Router::new()
        .route("/test/deliveries", get(all_deliveries::<B>))
        .route(
            "/test/dead-letters/retry",
            post(retry_all_dead_letters::<B>),
        )
        .route("/test/webhooks/{id}/redeliver", post(redeliver::<B>))
        .route("/test/attempts", get(attempts::<B>))
        .route(
            "/test/webhooks/{id}/faults",
            axum::routing::put(set_faults::<B>).delete(clear_faults::<B>),
        )
        .with_state(backend)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestDeliveriesQuery {
    pub subscription_id: Option<Uuid>,
    /// `PENDING`, `IN_FLIGHT`, `DELIVERED` or `DEAD` (the dead letters).
    pub status: Option<String>,
}

/// Webhook deliveries in event order, optionally one subscription's or one status. Delivery runs on real time, so
/// statuses move without clock changes.
async fn all_deliveries<B: WebhookBackend>(
    State(backend): State<B>,
    query: Result<Query<TestDeliveriesQuery>, QueryRejection>,
) -> Result<Json<Vec<DeliveryView>>, ApiError> {
    let Query(query) = query?;
    delivery_status(query.status.as_deref())?;
    let deliveries = backend
        .read(move |conn| {
            outbox::deliveries(
                conn,
                query.subscription_id,
                query.status.as_deref(),
                0,
                u32::MAX,
            )
        })
        .await?;
    Ok(Json(
        deliveries.into_iter().map(DeliveryView::from).collect(),
    ))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestRetryRequest {
    pub subscription_id: Option<Uuid>,
    #[serde(default)]
    pub delivery_ids: Vec<Uuid>,
}

/// Retries dead letters with a fresh attempt budget. An empty body retries all of them; `subscriptionId` and
/// `deliveryIds` narrow it down.
async fn retry_all_dead_letters<B: WebhookBackend>(
    State(backend): State<B>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, ApiError> {
    let request: TestRetryRequest = if body.is_empty() {
        TestRetryRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::bad_request(e.to_string()))?
    };
    let retried = backend
        .retry_dead_letters(request.subscription_id, request.delivery_ids)
        .await?;
    Ok(Json(serde_json::json!({ "retried": retried })))
}

/// Sets a subscription's delivery fault policy (see [`crate::webhooks::faults`]); returns the subscription.
async fn set_faults<B: WebhookBackend>(
    State(backend): State<B>,
    id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<FaultPolicy>, JsonRejection>,
) -> Result<Json<SubscriptionView>, ApiError> {
    let Path(id) = id?;
    let Json(policy) = body?;
    let subscription = backend.set_faults(id, Some(policy)).await?;
    Ok(Json(subscription.into()))
}

/// Removes a subscription's fault policy: its deliveries go out unchanged again.
async fn clear_faults<B: WebhookBackend>(
    State(backend): State<B>,
    id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<SubscriptionView>, ApiError> {
    let Path(id) = id?;
    let subscription = backend.set_faults(id, None).await?;
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
async fn attempts<B: WebhookBackend>(
    State(backend): State<B>,
    query: Result<Query<AttemptsQuery>, QueryRejection>,
) -> Result<Json<Vec<AttemptView>>, ApiError> {
    let Query(query) = query?;
    let rows = backend
        .read(move |conn| outbox::attempts(conn, query.subscription_id))
        .await?;
    Ok(Json(rows.into_iter().map(AttemptView::from).collect()))
}
