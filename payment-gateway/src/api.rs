//! HTTP API. Commands run on the [`Gateway`] (the single writer) off the async runtime; queries use a separate
//! read-only connection. Webhook maintenance and its test routes are the shared ones (`simulator_kit::api`).

use std::sync::{Arc, Mutex};

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use metrics_exporter_prometheus::PrometheusHandle;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use simulator_kit::api::ApiError;
use simulator_kit::api::webhooks::WebhookBackend;
use simulator_kit::clock::ClockError;
use simulator_kit::outbox;
use simulator_kit::persistence::PersistenceMode;
use simulator_kit::webhooks::{
    BackendError, DeliveryBackend, DeliveryJob, DeliveryResult, FaultPolicy, NewSubscription,
    Subscription, WebhookChange,
};
use uuid::Uuid;

use crate::gateway::{ClockChange, ClockState, Gateway, GatewayError, PaymentRequest};
use crate::payments::{BankAccount, PaymentType, payment_view};
use crate::statement::{Break, Statement};
use crate::store;

impl From<GatewayError> for ApiError {
    fn from(error: GatewayError) -> Self {
        use GatewayError as E;
        let (status, code) = match &error {
            E::Invalid(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            E::IdempotencyConflict { .. } => (StatusCode::CONFLICT, "idempotency_conflict"),
            E::ClockNotVirtual => (StatusCode::CONFLICT, "clock_not_virtual"),
            E::Clock(ClockError::Speed(_)) => (StatusCode::BAD_REQUEST, "invalid_clock_speed"),
            E::Clock(_) => (StatusCode::CONFLICT, "clock_backwards"),
            E::Webhook(_) => {
                let E::Webhook(error) = error else {
                    unreachable!()
                };
                return error.into();
            }
            E::Storage(_) => return Self::internal(error),
        };
        Self::new(status, code, error.to_string())
    }
}

/// The writer, shared by the HTTP handlers, the delivery worker and the ticker.
#[derive(Clone)]
pub struct Writer(Arc<Mutex<Gateway>>);

impl Writer {
    pub fn new(gateway: Gateway) -> Self {
        Self(Arc::new(Mutex::new(gateway)))
    }

    /// Runs `command` on the gateway, off the async runtime.
    pub async fn run<T: Send + 'static>(
        &self,
        command: impl FnOnce(&mut Gateway) -> Result<T, GatewayError> + Send + 'static,
    ) -> Result<T, GatewayError> {
        let gateway = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut gateway = gateway
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            command(&mut gateway)
        })
        .await
        .expect("gateway commands do not panic")
    }
}

impl DeliveryBackend for Writer {
    async fn claim(
        &self,
        limit: u32,
        lease: std::time::Duration,
    ) -> Result<Vec<DeliveryJob>, BackendError> {
        self.run(move |g| g.claim_deliveries(limit, lease))
            .await
            .map_err(|e| BackendError::Failed(e.to_string()))
    }

    async fn record(&self, results: Vec<DeliveryResult>) -> Result<(), BackendError> {
        self.run(move |g| g.record_deliveries(&results))
            .await
            .map_err(|e| BackendError::Failed(e.to_string()))
    }
}

/// Shared handler state.
#[derive(Clone)]
pub struct AppState {
    writer: Writer,
    reader: Arc<Mutex<Connection>>,
    metrics: PrometheusHandle,
    persistence: PersistenceMode,
}

impl AppState {
    pub fn new(
        writer: Writer,
        reader: Connection,
        metrics: PrometheusHandle,
        persistence: PersistenceMode,
    ) -> Self {
        Self {
            writer,
            reader: Arc::new(Mutex::new(reader)),
            metrics,
            persistence,
        }
    }

    async fn run<T: Send + 'static>(
        &self,
        command: impl FnOnce(&mut Gateway) -> Result<T, GatewayError> + Send + 'static,
    ) -> Result<T, ApiError> {
        Ok(self.writer.run(command).await?)
    }
}

impl WebhookBackend for AppState {
    async fn register(&self, request: NewSubscription) -> Result<Subscription, ApiError> {
        self.run(move |g| g.register_webhook(request)).await
    }

    async fn update(&self, id: Uuid, change: WebhookChange) -> Result<Subscription, ApiError> {
        self.run(move |g| g.update_webhook(id, change)).await
    }

    async fn delete(&self, id: Uuid) -> Result<(), ApiError> {
        self.run(move |g| g.delete_webhook(id)).await
    }

    async fn rotate_secret(
        &self,
        id: Uuid,
        secret: String,
        grace: chrono::Duration,
    ) -> Result<Subscription, ApiError> {
        self.run(move |g| g.rotate_webhook_secret(id, secret, grace))
            .await
    }

    async fn set_faults(
        &self,
        id: Uuid,
        policy: Option<FaultPolicy>,
    ) -> Result<Subscription, ApiError> {
        self.run(move |g| g.set_webhook_faults(id, policy)).await
    }

    async fn redeliver(&self, id: Uuid, after: u64) -> Result<usize, ApiError> {
        self.run(move |g| g.redeliver(id, after)).await
    }

    async fn retry_dead_letters(
        &self,
        subscription_id: Option<Uuid>,
        delivery_ids: Vec<Uuid>,
    ) -> Result<usize, ApiError> {
        self.run(move |g| g.retry_dead_letters(subscription_id, &delivery_ids))
            .await
    }

    async fn read<T: Send + 'static>(
        &self,
        query: impl FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
    ) -> Result<T, ApiError> {
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || {
            let conn = reader
                .lock()
                .map_err(|_| ApiError::internal("read connection lock poisoned"))?;
            query(&conn).map_err(ApiError::internal)
        })
        .await
        .map_err(ApiError::internal)?
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/api/payments", post(initiate).get(list_payments))
        .route("/api/payments/{id}", get(payment))
        .route("/api/events", get(events))
        .route("/api/statements", get(statements))
        .route("/api/statements/{id}", get(statement))
        .with_state(state.clone())
        .merge(simulator_kit::api::webhooks::router(state))
}

/// The routes under `/test`: clock control, reset, statement breaks, and the shared webhook test routes.
pub fn test_router(state: AppState) -> Router {
    Router::new()
        .route("/test/clock", get(clock))
        .route("/test/clock/advance", post(advance))
        .route("/test/clock/set", post(set_clock))
        .route("/test/clock/freeze", post(freeze))
        .route("/test/clock/resume", post(resume))
        .route("/test/clock/speed", post(speed))
        .route("/test/reset", post(reset))
        .route("/test/statements/breaks", get(breaks))
        .with_state(state.clone())
        .merge(simulator_kit::api::webhooks::test_router(state))
}

async fn health(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let version = state
        .read(simulator_kit::persistence::schema_version)
        .await?;
    Ok(Json(json!({
        "status": "ok",
        "schemaVersion": version,
        "persistence": state.persistence.as_str(),
    })))
}

async fn metrics(State(state): State<AppState>) -> Result<String, ApiError> {
    for (status, count) in state.read(outbox::delivery_counts).await? {
        ::metrics::gauge!(crate::metrics::OUTBOX, "status" => status).set(count as f64);
    }
    Ok(state.metrics.render())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PaymentBody {
    idempotency_key: String,
    #[serde(rename = "type")]
    payment_type: PaymentType,
    account_id: String,
    bank_account: BankAccount,
    amount: String,
    currency: String,
    #[serde(default)]
    reference: Option<String>,
}

/// `201` with the new payment, or `200` with `duplicate: true` for a repeated idempotency key with the same details.
async fn initiate(
    State(state): State<AppState>,
    body: Result<Json<PaymentBody>, JsonRejection>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let Json(body) = body?;
    let request = PaymentRequest {
        idempotency_key: body.idempotency_key,
        payment_type: body.payment_type,
        account_id: body.account_id,
        bank_account: body.bank_account,
        amount: body.amount,
        currency: body.currency,
        reference: body.reference,
    };
    let (payment, duplicate) = state.run(move |g| g.initiate(request)).await?;
    let mut view = payment_view(&payment);
    if duplicate {
        view["duplicate"] = json!(true);
        return Ok((StatusCode::OK, Json(view)));
    }
    Ok((StatusCode::CREATED, Json(view)))
}

async fn payment(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<Value>, ApiError> {
    let Path(id) = id?;
    let found = state.read(move |conn| store::payment(conn, id)).await?;
    let payment = found.ok_or_else(|| ApiError::not_found(format!("unknown payment {id}")))?;
    Ok(Json(payment_view(&payment)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PaymentQuery {
    account_id: Option<String>,
    /// `PENDING`, `COMPLETED`, `FAILED` or `RETURNED`.
    status: Option<String>,
    /// Payments with a sequence after this one (default 0), for paging.
    #[serde(default)]
    after: u64,
    /// 1 to 1000; default 100.
    limit: Option<u32>,
}

fn limit(limit: Option<u32>) -> Result<u32, ApiError> {
    let limit = limit.unwrap_or(100);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError::bad_request("limit must be 1 to 1000"));
    }
    Ok(limit)
}

async fn list_payments(
    State(state): State<AppState>,
    query: Result<Query<PaymentQuery>, QueryRejection>,
) -> Result<Json<Vec<Value>>, ApiError> {
    let Query(query) = query?;
    if let Some(status) = &query.status
        && status.parse::<crate::payments::PaymentStatus>().is_err()
    {
        return Err(ApiError::bad_request(format!(
            "unknown payment status {status}"
        )));
    }
    let limit = limit(query.limit)?;
    let payments = state
        .read(move |conn| {
            store::payments(
                conn,
                query.account_id.as_deref(),
                query.status.as_deref(),
                query.after,
                limit,
            )
        })
        .await?;
    Ok(Json(payments.iter().map(payment_view).collect()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EventQuery {
    #[serde(default)]
    after: u64,
    limit: Option<u32>,
    event_type: Option<String>,
}

async fn events(
    State(state): State<AppState>,
    query: Result<Query<EventQuery>, QueryRejection>,
) -> Result<Json<Vec<simulator_kit::events::EventEnvelope>>, ApiError> {
    let Query(query) = query?;
    let limit = limit(query.limit)?;
    let events = state
        .read(move |conn| outbox::events(conn, query.after, limit, query.event_type.as_deref()))
        .await?;
    Ok(Json(events))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StatementQuery {
    /// Business date, `YYYY-MM-DD`.
    date: Option<NaiveDate>,
    currency: Option<String>,
}

/// Issued statements, optionally of one business date and one currency, in date and currency order.
async fn statements(
    State(state): State<AppState>,
    query: Result<Query<StatementQuery>, QueryRejection>,
) -> Result<Json<Vec<Statement>>, ApiError> {
    let Query(query) = query?;
    let currency = query.currency.map(|c| c.to_ascii_uppercase());
    let found = state
        .read(move |conn| store::statements(conn, query.date, currency.as_deref(), false))
        .await?;
    Ok(Json(found.into_iter().map(|s| s.draft.statement).collect()))
}

async fn statement(
    State(state): State<AppState>,
    id: Result<Path<String>, PathRejection>,
) -> Result<Json<Statement>, ApiError> {
    let Path(id) = id?;
    let lookup = id.clone();
    let found = state
        .read(move |conn| store::statement(conn, &lookup))
        .await?
        .filter(|s| s.issued)
        .ok_or_else(|| ApiError::not_found(format!("no issued bank statement {id}")))?;
    Ok(Json(found.draft.statement))
}

/// A statement's breaks, for tests.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BreaksView {
    statement_id: String,
    business_date: NaiveDate,
    currency: String,
    /// `false` for a late statement still waiting; `issuedAt` is then when it will be issued.
    issued: bool,
    issued_at: DateTime<Utc>,
    breaks: Vec<Break>,
}

/// `GET /test/statements/breaks?date=&currency=`: statements with breaks, issued or waiting.
async fn breaks(
    State(state): State<AppState>,
    query: Result<Query<StatementQuery>, QueryRejection>,
) -> Result<Json<Vec<BreaksView>>, ApiError> {
    let Query(query) = query?;
    let currency = query.currency.map(|c| c.to_ascii_uppercase());
    let stored = state
        .read(move |conn| {
            let mut all = store::statements(conn, query.date, currency.as_deref(), false)?;
            all.extend(store::statements(
                conn,
                query.date,
                currency.as_deref(),
                true,
            )?);
            Ok(all)
        })
        .await?;
    let mut views: Vec<BreaksView> = stored
        .into_iter()
        .filter(|s| !s.draft.breaks.is_empty())
        .map(|s| BreaksView {
            statement_id: s.draft.statement.statement_id,
            business_date: s.draft.statement.business_date,
            currency: s.draft.statement.currency,
            issued: s.issued,
            issued_at: s.draft.statement.issued_at,
            breaks: s.draft.breaks,
        })
        .collect();
    views.sort_by(|a, b| (a.business_date, &a.currency).cmp(&(b.business_date, &b.currency)));
    Ok(Json(views))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClockView {
    mode: &'static str,
    running: bool,
    speed: f64,
    now: DateTime<Utc>,
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

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AdvanceRequest {
    #[serde(default)]
    days: u32,
    #[serde(default)]
    hours: u32,
    #[serde(default)]
    minutes: u32,
    #[serde(default)]
    seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SetRequest {
    to: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SpeedRequest {
    speed: f64,
}

async fn clock(State(state): State<AppState>) -> Result<Json<ClockView>, ApiError> {
    Ok(Json(state.run(|g| Ok(g.clock_state())).await?.into()))
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

async fn set_clock(
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

async fn speed(
    State(state): State<AppState>,
    body: Result<Json<SpeedRequest>, JsonRejection>,
) -> Result<Json<ClockView>, ApiError> {
    let Json(body) = body?;
    change(&state, ClockChange::Speed(body.speed)).await
}

async fn change(state: &AppState, change: ClockChange) -> Result<Json<ClockView>, ApiError> {
    Ok(Json(
        state.run(move |g| g.change_clock(change)).await?.into(),
    ))
}

async fn reset(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    state.run(Gateway::reset).await?;
    Ok(StatusCode::NO_CONTENT)
}
