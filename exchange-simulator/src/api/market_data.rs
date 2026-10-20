//! Market-data API (plan items 6.2 and 6.8, SR-12 and SR-13): each instrument's snapshot, and the live stream of
//! server-sent events (see [`crate::market_data`]).

use std::convert::Infallible;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::Value;

use super::{ApiError, AppState};
use crate::domain::InstrumentId;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/marketdata", get(list))
        .route("/api/marketdata/stream", get(stream))
        .route("/api/marketdata/{instrument_id}", get(detail))
        .with_state(state)
}

/// Every instrument's market data, in instrument ID order.
async fn list(State(state): State<AppState>) -> Result<Json<Vec<Value>>, ApiError> {
    let snapshots = state.dispatcher.market_data(None).await?;
    Ok(Json(snapshots.iter().map(|s| s.to_json()).collect()))
}

async fn detail(
    State(state): State<AppState>,
    Path(instrument_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let snapshots = state
        .dispatcher
        .market_data(Some(InstrumentId(instrument_id.clone())))
        .await?;
    let snapshot = snapshots
        .first()
        .ok_or_else(|| ApiError::not_found(format!("unknown instrument {instrument_id}")))?;
    Ok(Json(snapshot.to_json()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StreamQuery {
    /// Comma-separated instrument IDs; all when absent.
    instruments: Option<String>,
}

/// Server-sent events: `event:` the message kind, `id:` its `mdSequence`, `data:` its JSON. The stream ends when the
/// subscriber fell a whole buffer behind; reconnect, read the snapshot, and continue.
async fn stream(
    State(state): State<AppState>,
    query: Result<Query<StreamQuery>, QueryRejection>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let Query(query) = query?;
    let (sender, options) = state
        .market_data
        .clone()
        .ok_or_else(|| ApiError::not_found("the market-data stream is not running"))?;
    let instruments = match query.instruments {
        None => None,
        Some(list) => {
            let ids: Vec<InstrumentId> = list
                .split(',')
                .map(|id| InstrumentId(id.trim().to_string()))
                .filter(|id| !id.0.is_empty())
                .collect();
            let known = state.dispatcher.market_data(None).await?;
            if let Some(unknown) = ids
                .iter()
                .find(|id| !known.iter().any(|s| s.instrument.id == **id))
            {
                return Err(ApiError::bad_request(format!(
                    "unknown instrument {}",
                    unknown.0
                )));
            }
            Some(ids)
        }
    };
    // Subscribe before answering, so nothing published after the response started is missed.
    let messages = crate::market_data::subscribe(sender.subscribe(), instruments, options);
    let events = messages.map(|message| {
        Ok(Event::default()
            .event(message.kind)
            .id(message.md_sequence.to_string())
            .data(message.data.clone()))
    });
    Ok(Sse::new(events).keep_alive(KeepAlive::default()))
}
