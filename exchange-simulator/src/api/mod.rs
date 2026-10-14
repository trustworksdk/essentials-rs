//! HTTP API. Commands go to the exchange thread through the [`Dispatcher`] and are answered after their commit;
//! queries of stored state use a separate read-only SQLite connection.

mod error;
mod model;
pub mod settlements;
pub mod test_control;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::{Value, json};
use uuid::Uuid;

pub use error::ApiError;
pub use model::*;

use crate::domain::{Instrument, InstrumentId, Participant};
use crate::engine::{DispatchError, Dispatcher, ExchangeError, SubmitOutcome};
use crate::events::EventEnvelope;
use crate::persistence::{ExecutionFilter, InstrumentFilter, Store};
use crate::webhooks::NewSubscription;

/// Shared handler state.
#[derive(Clone)]
pub struct AppState {
    dispatcher: Dispatcher,
    reader: Arc<Mutex<Store>>,
    metrics: PrometheusHandle,
}

impl AppState {
    /// `reader` should be a read-only connection to the database the dispatcher's exchange writes.
    pub fn new(dispatcher: Dispatcher, reader: Store, metrics: PrometheusHandle) -> Self {
        Self {
            dispatcher,
            reader: Arc::new(Mutex::new(reader)),
            metrics,
        }
    }

    /// Runs a query on the read connection off the async runtime.
    async fn read<T: Send + 'static>(
        &self,
        query: impl FnOnce(&Store) -> rusqlite::Result<T> + Send + 'static,
    ) -> Result<T, ApiError> {
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || {
            let store = reader
                .lock()
                .map_err(|_| ApiError::internal("read connection lock poisoned"))?;
            query(&store).map_err(ApiError::internal)
        })
        .await
        .map_err(ApiError::internal)?
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route(
            "/api/instruments",
            post(register_instrument).get(list_instruments),
        )
        .route(
            "/api/instruments/{id}",
            axum::routing::patch(update_instrument),
        )
        .route("/api/participants", post(register_participant))
        .route("/api/orders", post(submit_order))
        .route("/api/orders/{id}", get(get_order).delete(cancel_order))
        .route("/api/orderbooks/{instrument_id}", get(order_book))
        .route("/api/trades", get(list_trades))
        .route("/api/events", get(list_events))
        .route("/api/webhooks", post(register_webhook).get(list_webhooks))
        .with_state(state.clone())
        .merge(settlements::router(state))
}

/// Liveness and readiness: the process is up and the database answers.
async fn health(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    match state
        .read(|store| crate::persistence::schema_version(store.connection()))
        .await
    {
        Ok(version) => (
            StatusCode::OK,
            Json(json!({ "status": "ok", "schemaVersion": version })),
        ),
        Err(error) => {
            tracing::warn!(?error, "health check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "unavailable" })),
            )
        }
    }
}

async fn register_instrument(
    State(state): State<AppState>,
    body: Result<Json<InstrumentRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<InstrumentView>), ApiError> {
    let Json(body) = body?;
    let instrument = state.dispatcher.register_instrument(body.into()).await?;
    Ok((StatusCode::CREATED, Json(InstrumentView::from(&instrument))))
}

async fn list_instruments(
    State(state): State<AppState>,
    query: Result<Query<InstrumentQuery>, QueryRejection>,
) -> Result<Json<Vec<InstrumentView>>, ApiError> {
    let Query(q) = query?;
    let filter = InstrumentFilter {
        symbol: q.symbol,
        mic: q.mic,
        isin: q.isin,
        cusip: q.cusip,
    };
    let instruments = state.read(move |store| store.instruments(&filter)).await?;
    Ok(Json(instruments.iter().map(InstrumentView::from).collect()))
}

/// Changes an instrument's trading status. Resting orders stay; cancels keep working while it is disabled.
async fn update_instrument(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Result<Json<InstrumentPatch>, JsonRejection>,
) -> Result<Json<InstrumentView>, ApiError> {
    let Json(patch) = body?;
    let instrument = state
        .dispatcher
        .set_instrument_enabled(InstrumentId(id), patch.enabled)
        .await
        .map_err(|error| match error {
            DispatchError::Exchange(ExchangeError::UnknownInstrument(id)) => {
                ApiError::not_found(format!("unknown instrument {id}"))
            }
            other => other.into(),
        })?;
    Ok(Json(InstrumentView::from(&instrument)))
}

async fn register_participant(
    State(state): State<AppState>,
    body: Result<Json<ParticipantBody>, JsonRejection>,
) -> Result<(StatusCode, Json<ParticipantBody>), ApiError> {
    let Json(body) = body?;
    let participant = state
        .dispatcher
        .register_participant(Participant::from(body))
        .await?;
    Ok((StatusCode::CREATED, Json(participant.into())))
}

/// `201` with the committed outcome (including exchange rejections, which are recorded with status `REJECTED`), or
/// `200` with the existing order when the client order ID was already used with the same terms.
async fn submit_order(
    State(state): State<AppState>,
    body: Result<Json<OrderRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<OrderResult>), ApiError> {
    let Json(body) = body?;
    let (outcome, instrument) = state.dispatcher.submit(body.into()).await?;
    Ok(match outcome {
        SubmitOutcome::Processed(update) => (
            StatusCode::CREATED,
            Json(OrderResult::new(
                &update.order,
                &update.executions,
                update.rejection,
                &instrument,
            )),
        ),
        SubmitOutcome::Duplicate(order) => {
            let mut result = OrderResult::new(&order, &[], None, &instrument);
            result.duplicate = true;
            (StatusCode::OK, Json(result))
        }
    })
}

async fn get_order(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<OrderView>, ApiError> {
    let Path(id) = id?;
    let found = state
        .read(move |store| {
            let Some(record) = store.order(id)? else {
                return Ok(None);
            };
            let instrument = store.instrument(&record.order.instrument_id)?;
            Ok(instrument.map(|instrument| (record, instrument)))
        })
        .await?;
    let (record, instrument) =
        found.ok_or_else(|| ApiError::not_found(format!("unknown order {id}")))?;
    let mut view = OrderView::new(&record.order, &instrument);
    view.created_at = Some(record.created_at);
    view.updated_at = Some(record.updated_at);
    Ok(Json(view))
}

async fn cancel_order(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<OrderResult>, ApiError> {
    let Path(id) = id?;
    let (update, instrument) = state.dispatcher.cancel(id).await?;
    Ok(Json(OrderResult::new(
        &update.order,
        &update.executions,
        update.rejection,
        &instrument,
    )))
}

async fn order_book(
    State(state): State<AppState>,
    Path(instrument_id): Path<String>,
) -> Result<Json<BookView>, ApiError> {
    let snapshot = state
        .dispatcher
        .snapshot(InstrumentId(instrument_id.clone()))
        .await?;
    let snapshot = snapshot
        .ok_or_else(|| ApiError::not_found(format!("unknown instrument {instrument_id}")))?;
    Ok(Json(snapshot.into()))
}

async fn list_trades(
    State(state): State<AppState>,
    query: Result<Query<TradeQuery>, QueryRejection>,
) -> Result<Json<Vec<TradeView>>, ApiError> {
    let Query(q) = query?;
    let limit = q.limit.unwrap_or(100);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError::bad_request("limit must be between 1 and 1000"));
    }
    let filter = ExecutionFilter {
        instrument_id: q.instrument_id,
        participant_id: q.participant_id,
        order_id: q.order_id,
        limit,
    };
    let (executions, instruments) = state
        .read(move |store| {
            let executions = store.executions(&filter)?;
            let mut instruments: HashMap<InstrumentId, Instrument> = HashMap::new();
            for execution in &executions {
                if !instruments.contains_key(&execution.instrument_id)
                    && let Some(instrument) = store.instrument(&execution.instrument_id)?
                {
                    instruments.insert(instrument.id.clone(), instrument);
                }
            }
            Ok((executions, instruments))
        })
        .await?;
    Ok(Json(
        executions
            .iter()
            .map(|e| TradeView::new(e, &instruments[&e.instrument_id]))
            .collect(),
    ))
}

/// Cursor-based replay of canonical events: those with `sequence > after`, in sequence order.
async fn list_events(
    State(state): State<AppState>,
    query: Result<Query<EventQuery>, QueryRejection>,
) -> Result<Json<Vec<EventEnvelope>>, ApiError> {
    let Query(q) = query?;
    let limit = q.limit.unwrap_or(100);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError::bad_request("limit must be between 1 and 1000"));
    }
    let after = q.after.unwrap_or(0);
    Ok(Json(
        state
            .read(move |store| store.events(after, limit, q.event_type.as_deref()))
            .await?,
    ))
}

async fn register_webhook(
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

async fn list_webhooks(
    State(state): State<AppState>,
) -> Result<Json<Vec<SubscriptionView>>, ApiError> {
    let subscriptions = state.read(|store| store.subscriptions()).await?;
    Ok(Json(
        subscriptions
            .into_iter()
            .map(SubscriptionView::from)
            .collect(),
    ))
}

/// Prometheus text exposition. The outbox gauges are refreshed from SQLite on each scrape.
async fn metrics(
    State(state): State<AppState>,
) -> Result<([(axum::http::HeaderName, &'static str); 1], String), ApiError> {
    let counts = state.read(|store| store.delivery_counts()).await?;
    for status in ["PENDING", "IN_FLIGHT", "DELIVERED", "DEAD"] {
        let count = counts
            .iter()
            .find(|(s, _)| s == status)
            .map_or(0, |(_, n)| *n);
        ::metrics::gauge!(crate::metrics::OUTBOX, "status" => status).set(count as f64);
    }
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        state.metrics.render(),
    ))
}
