//! Clearing API: end-of-day statements once issued. A late statement is not listed until its issue time; its
//! breaks are visible only through `/test/clearing/breaks`.

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use super::{ApiError, AppState};
use crate::clearing::{Break, Statement};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/clearing/statements", get(list))
        .route("/api/clearing/statements/{id}", get(detail))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatementQuery {
    /// Business date, `YYYY-MM-DD`.
    pub date: Option<NaiveDate>,
    pub participant: Option<String>,
}

/// Issued statements, optionally of one business date and one participant, in date and participant order.
async fn list(
    State(state): State<AppState>,
    query: Result<Query<StatementQuery>, QueryRejection>,
) -> Result<Json<Vec<Statement>>, ApiError> {
    let Query(query) = query?;
    let statements = state
        .read(move |store| {
            store.clearing_statements(query.date, query.participant.as_deref(), false)
        })
        .await?;
    Ok(Json(
        statements.into_iter().map(|s| s.draft.statement).collect(),
    ))
}

async fn detail(
    State(state): State<AppState>,
    id: Result<Path<String>, PathRejection>,
) -> Result<Json<Statement>, ApiError> {
    let Path(id) = id?;
    let lookup = id.clone();
    let stored = state
        .read(move |store| store.clearing_statement(&lookup))
        .await?
        .filter(|s| s.issued)
        .ok_or_else(|| ApiError::not_found(format!("no issued clearing statement {id}")))?;
    Ok(Json(stored.draft.statement))
}

/// A statement's breaks, for tests: what it says that the canonical trades do not.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BreaksView {
    pub statement_id: String,
    pub business_date: NaiveDate,
    pub participant_id: String,
    /// `false` for a late statement still waiting; `issuedAt` is then when it will be issued.
    pub issued: bool,
    pub issued_at: chrono::DateTime<chrono::Utc>,
    pub breaks: Vec<Break>,
}

/// `GET /test/clearing/breaks?date=&participant=`: statements with breaks, issued or waiting, in date and
/// participant order.
pub async fn breaks(
    State(state): State<AppState>,
    query: Result<Query<StatementQuery>, QueryRejection>,
) -> Result<Json<Vec<BreaksView>>, ApiError> {
    let Query(query) = query?;
    let stored = state
        .read(move |store| {
            let participant = query.participant.as_deref();
            let mut all = store.clearing_statements(query.date, participant, false)?;
            all.extend(store.clearing_statements(query.date, participant, true)?);
            Ok::<_, rusqlite::Error>(all)
        })
        .await?;
    let mut views: Vec<BreaksView> = stored
        .into_iter()
        .filter(|s| !s.draft.breaks.is_empty())
        .map(|s| BreaksView {
            statement_id: s.draft.statement.statement_id,
            business_date: s.draft.statement.business_date,
            participant_id: s.draft.statement.participant_id.0,
            issued: s.issued,
            issued_at: s.draft.statement.issued_at,
            breaks: s.draft.breaks,
        })
        .collect();
    views.sort_by(|a, b| {
        (a.business_date, &a.participant_id).cmp(&(b.business_date, &b.participant_id))
    });
    Ok(Json(views))
}
