//! Liquidity provider runtime control (plan item 6.12, SR-11): what the provider is doing in each instrument, and
//! changing its settings or the instrument's price source while the service runs.

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Map, Value, json};

use super::{ApiError, AppState};
use crate::domain::{InstrumentId, Side};
use crate::engine::{DispatchError, ExchangeError, LiquidityPatch, LiquidityStatus};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/liquidity", get(list))
        .route("/api/liquidity/{instrument_id}", get(detail).patch(patch))
        .with_state(state)
}

/// The provider in every instrument, in instrument ID order.
async fn list(State(state): State<AppState>) -> Result<Json<Vec<Value>>, ApiError> {
    let statuses = state.dispatcher.liquidity(None).await?;
    Ok(Json(statuses.iter().map(view).collect()))
}

async fn detail(
    State(state): State<AppState>,
    Path(instrument_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let statuses = state
        .dispatcher
        .liquidity(Some(InstrumentId(instrument_id.clone())))
        .await?;
    let status = statuses
        .first()
        .ok_or_else(|| ApiError::not_found(format!("unknown instrument {instrument_id}")))?;
    Ok(Json(view(status)))
}

/// Changes settings field by field (`null` clears one, so the default applies again) and, with `priceSource`, the
/// instrument's price source (replaced as a whole, as `PATCH /api/instruments` does). Answers with the new status.
async fn patch(
    State(state): State<AppState>,
    Path(instrument_id): Path<String>,
    body: Result<Json<Map<String, Value>>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    let Json(mut fields) = body?;
    if fields.is_empty() {
        return Err(ApiError::bad_request(
            "give at least one liquidity setting, or priceSource",
        ));
    }
    let price_source = match fields.remove("priceSource") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(spec) => {
            Some(Some(serde_json::from_value(spec).map_err(|e| {
                ApiError::bad_request(format!("priceSource: {e}"))
            })?))
        }
    };
    let status = state
        .dispatcher
        .patch_liquidity(
            InstrumentId(instrument_id),
            LiquidityPatch {
                fields,
                price_source,
            },
        )
        .await
        .map_err(|error| match error {
            DispatchError::Exchange(ExchangeError::UnknownInstrument(id)) => {
                ApiError::not_found(format!("unknown instrument {id}"))
            }
            other => other.into(),
        })?;
    Ok(Json(view(&status)))
}

fn view(status: &LiquidityStatus) -> Value {
    let instrument = &status.instrument;
    let level = |side: Side| -> Vec<Value> {
        status
            .ladder
            .iter()
            .filter(|o| o.side == side)
            .map(|o| {
                let ticks = o.limit_price_ticks.unwrap_or_default();
                json!({ "orderId": o.id, "priceTicks": ticks, "price": instrument.ticks_to_price(ticks),
                        "quantity": o.remaining_quantity })
            })
            .collect()
    };
    json!({
        "instrumentId": instrument.id,
        "providerId": status.provider,
        "state": status.state,
        "settings": status.settings,
        "own": instrument.liquidity,
        "inventory": status.inventory,
        "quotedAround": status.quoted_at.map(|ticks| {
            json!({ "priceTicks": ticks, "price": instrument.ticks_to_price(ticks) })
        }),
        "ladder": { "bids": level(Side::Buy), "asks": level(Side::Sell) },
        "priceSource": instrument.price_source,
        "reference": status.reference.as_ref().map(|r| crate::market_data::reference_json(instrument, r)),
        "session": if status.session_open { "open" } else { "closed" },
    })
}
