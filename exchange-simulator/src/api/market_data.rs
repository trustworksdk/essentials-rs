//! Market-data API (plan item 6.2, the first cut of SR-12): each instrument's reference price. The order book's best
//! prices, the last trade and the stream follow with the market-data channel (6.8).

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};

use super::model::MarketDataView;
use super::{ApiError, AppState};
use crate::domain::InstrumentId;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/marketdata", get(list))
        .route("/api/marketdata/{instrument_id}", get(detail))
        .with_state(state)
}

/// Every instrument's market data, in instrument ID order.
async fn list(State(state): State<AppState>) -> Result<Json<Vec<MarketDataView>>, ApiError> {
    let references = state.dispatcher.references(None).await?;
    Ok(Json(
        references
            .iter()
            .map(|(instrument, reference)| MarketDataView::new(instrument, *reference))
            .collect(),
    ))
}

async fn detail(
    State(state): State<AppState>,
    Path(instrument_id): Path<String>,
) -> Result<Json<MarketDataView>, ApiError> {
    let references = state
        .dispatcher
        .references(Some(InstrumentId(instrument_id.clone())))
        .await?;
    let (instrument, reference) = references
        .first()
        .ok_or_else(|| ApiError::not_found(format!("unknown instrument {instrument_id}")))?;
    Ok(Json(MarketDataView::new(instrument, *reference)))
}
