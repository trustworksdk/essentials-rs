//! Settlement API: simulated instructions and externally reported outcomes, side by side but never merged.

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{ApiError, AppState};
use crate::domain::{InstrumentId, ParticipantId};
use crate::events::{self, EventEnvelope};
use crate::settlement::{ExternalReport, Instruction, NewReport, SettlementStatus, format_amount};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/settlements", get(list))
        .route("/api/settlements/{execution_id}", get(detail))
        .route("/api/settlements/{execution_id}/outcomes", post(report))
        .with_state(state)
}

/// A simulated settlement instruction.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettlementView {
    pub execution_id: Uuid,
    pub trade_sequence: u64,
    pub instrument_id: InstrumentId,
    pub buyer_id: ParticipantId,
    pub seller_id: ParticipantId,
    pub quantity: u64,
    pub price_ticks: i64,
    pub currency: String,
    pub settlement_amount: String,
    pub amount_minor: i64,
    pub trade_date: NaiveDate,
    pub intended_settlement_date: NaiveDate,
    pub status: SettlementStatus,
    pub attempts: u32,
    pub delays: u32,
    /// When the next simulated transition is due; absent once nothing more will happen.
    pub next_transition_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    /// The clearing statements and obligations it appears on, when clearing runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clearing: Option<crate::clearing::ClearingRefs>,
}

impl From<Instruction> for SettlementView {
    fn from(i: Instruction) -> Self {
        Self {
            execution_id: i.execution_id,
            trade_sequence: i.trade_sequence,
            instrument_id: i.instrument_id,
            buyer_id: i.buyer_id,
            seller_id: i.seller_id,
            quantity: i.quantity,
            price_ticks: i.price_ticks,
            settlement_amount: format_amount(i.amount_minor, i.currency_scale),
            currency: i.currency,
            amount_minor: i.amount_minor,
            trade_date: i.trade_date,
            intended_settlement_date: i.intended_settlement_date,
            status: i.status,
            attempts: i.attempts,
            delays: i.delays,
            next_transition_at: i.due_at,
            settled_at: i.settled_at,
            updated_at: i.updated_at,
            clearing: i.clearing,
        }
    }
}

/// An externally reported outcome.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportView {
    pub report_id: Uuid,
    pub execution_id: Uuid,
    pub status: SettlementStatus,
    pub reference: Option<String>,
    pub reason: Option<String>,
    pub details: Option<serde_json::Value>,
    pub occurred_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duplicate: Option<bool>,
}

impl From<ExternalReport> for ReportView {
    fn from(r: ExternalReport) -> Self {
        Self {
            report_id: r.id,
            execution_id: r.execution_id,
            status: r.status,
            reference: r.reference,
            reason: r.reason,
            details: r.details,
            occurred_at: r.occurred_at,
            received_at: r.received_at,
            duplicate: None,
        }
    }
}

/// Everything known about one execution's settlement. `simulated` and `simulatedEvents` come from the simulator,
/// `external` from reports; neither overrides the other.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettlementDetail {
    pub execution_id: Uuid,
    pub simulated: Option<SettlementView>,
    pub simulated_events: Vec<EventEnvelope>,
    pub external: Vec<ReportView>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListQuery {
    pub status: Option<SettlementStatus>,
    /// 1 to 1000; default 100.
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReportRequest {
    pub status: SettlementStatus,
    pub reference: Option<String>,
    pub reason: Option<String>,
    pub details: Option<serde_json::Value>,
    /// When the outcome happened; defaults to the time of receipt.
    pub occurred_at: Option<DateTime<Utc>>,
}

async fn list(
    State(state): State<AppState>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<Vec<SettlementView>>, ApiError> {
    let Query(q) = query?;
    let limit = q.limit.unwrap_or(100);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError::bad_request("limit must be between 1 and 1000"));
    }
    let instructions = state
        .read(move |store| store.settlements(q.status, limit))
        .await?;
    Ok(Json(
        instructions.into_iter().map(SettlementView::from).collect(),
    ))
}

async fn detail(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
) -> Result<Json<SettlementDetail>, ApiError> {
    let Path(execution_id) = id?;
    let found = state
        .read(move |store| {
            if !store.execution_exists(execution_id)? {
                return Ok(None);
            }
            let key = execution_id.to_string();
            Ok(Some(SettlementDetail {
                execution_id,
                simulated: store.settlement(execution_id)?.map(SettlementView::from),
                simulated_events: store.events_for(&key, Some(events::SETTLEMENT_SOURCE))?,
                external: store
                    .reports(execution_id)?
                    .into_iter()
                    .map(ReportView::from)
                    .collect(),
            }))
        })
        .await?;
    Ok(Json(found.ok_or_else(|| {
        ApiError::not_found(format!("unknown execution {execution_id}"))
    })?))
}

/// `201` with the stored report, or `200` with the original when `reference` repeats an identical report.
async fn report(
    State(state): State<AppState>,
    id: Result<Path<Uuid>, PathRejection>,
    body: Result<Json<ReportRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ReportView>), ApiError> {
    let Path(execution_id) = id?;
    let Json(body) = body?;
    let report = NewReport {
        execution_id,
        status: body.status,
        reference: body.reference,
        reason: body.reason,
        details: body.details,
        occurred_at: body.occurred_at,
    };
    let (stored, duplicate) = state.dispatcher.report_settlement(report).await?;
    let mut view = ReportView::from(stored);
    view.duplicate = Some(duplicate);
    Ok((
        if duplicate {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(view),
    ))
}
