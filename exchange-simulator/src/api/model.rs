//! JSON request and response bodies. Field names are camelCase, enums use their upper-case wire names, prices are
//! given as integer `priceTicks` plus, in responses, a decimal `price` string in major currency units.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{
    Execution, Instrument, InstrumentChange, InstrumentId, Order, OrderStatus, OrderType,
    Participant, ParticipantId, Side,
};
use crate::engine::{BookSnapshot, Level, RejectReason, SubmitOrder};
use crate::liquidity::LiquiditySpec;
use crate::pricing::{PriceSourceSpec, Reference};

fn one() -> u64 {
    1
}

fn yes() -> bool {
    true
}

/// `PATCH /api/instruments/{id}`: the fields that may change after registration. Absent fields stay as they are;
/// `"referenceSymbol": null` clears the reference symbol.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstrumentPatch {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default, deserialize_with = "present")]
    pub reference_symbol: Option<Option<String>>,
    #[serde(default, deserialize_with = "present")]
    pub price_source: Option<Option<PriceSourceSpec>>,
    #[serde(default, deserialize_with = "present")]
    pub liquidity: Option<Option<LiquiditySpec>>,
}

impl From<InstrumentPatch> for InstrumentChange {
    fn from(p: InstrumentPatch) -> Self {
        Self {
            enabled: p.enabled,
            reference_symbol: p.reference_symbol,
            price_source: p.price_source,
            liquidity: p.liquidity,
        }
    }
}

/// Tells a field given as `null` (`Some(None)`) from an absent one (`None`, through `#[serde(default)]`).
fn present<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstrumentRequest {
    pub id: String,
    pub symbol: String,
    pub mic: Option<String>,
    pub isin: Option<String>,
    pub cusip: Option<String>,
    pub currency: String,
    pub currency_scale: u8,
    #[serde(default = "one")]
    pub tick_size_minor: u64,
    #[serde(default = "one")]
    pub lot_size: u64,
    /// Default true.
    #[serde(default = "yes")]
    pub enabled: bool,
    pub reference_symbol: Option<String>,
    pub price_source: Option<PriceSourceSpec>,
    pub liquidity: Option<LiquiditySpec>,
}

impl From<InstrumentRequest> for Instrument {
    fn from(r: InstrumentRequest) -> Self {
        Self {
            id: InstrumentId(r.id),
            symbol: r.symbol,
            mic: r.mic,
            isin: r.isin,
            cusip: r.cusip,
            currency: r.currency,
            currency_scale: r.currency_scale,
            tick_size_minor: r.tick_size_minor,
            lot_size: r.lot_size,
            enabled: r.enabled,
            reference_symbol: r.reference_symbol,
            price_source: r.price_source,
            liquidity: r.liquidity,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentView {
    pub id: InstrumentId,
    pub symbol: String,
    pub mic: Option<String>,
    pub isin: Option<String>,
    pub cusip: Option<String>,
    pub currency: String,
    pub currency_scale: u8,
    pub tick_size_minor: u64,
    /// One tick as a decimal price, e.g. `"0.01"`.
    pub tick_size: String,
    pub lot_size: u64,
    pub enabled: bool,
    pub reference_symbol: Option<String>,
    pub price_source: Option<PriceSourceSpec>,
    pub liquidity: Option<LiquiditySpec>,
}

impl From<&Instrument> for InstrumentView {
    fn from(i: &Instrument) -> Self {
        Self {
            id: i.id.clone(),
            symbol: i.symbol.clone(),
            mic: i.mic.clone(),
            isin: i.isin.clone(),
            cusip: i.cusip.clone(),
            currency: i.currency.clone(),
            currency_scale: i.currency_scale,
            tick_size_minor: i.tick_size_minor,
            tick_size: i.ticks_to_price(1),
            lot_size: i.lot_size,
            enabled: i.enabled,
            reference_symbol: i.reference_symbol.clone(),
            price_source: i.price_source.clone(),
            liquidity: i.liquidity.clone(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstrumentQuery {
    pub symbol: Option<String>,
    pub mic: Option<String>,
    pub isin: Option<String>,
    pub cusip: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ParticipantBody {
    pub id: ParticipantId,
    pub name: String,
    /// Optional settlement details: a JSON object of at most 4 KiB, copied into `TradeExecuted`.
    #[serde(default)]
    pub settlement: Option<serde_json::Value>,
}

impl From<ParticipantBody> for Participant {
    fn from(b: ParticipantBody) -> Self {
        Self {
            id: b.id,
            name: b.name,
            settlement: b.settlement,
        }
    }
}

impl From<Participant> for ParticipantBody {
    fn from(p: Participant) -> Self {
        Self {
            id: p.id,
            name: p.name,
            settlement: p.settlement,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OrderRequest {
    pub client_order_id: Option<String>,
    pub instrument_id: InstrumentId,
    pub participant_id: ParticipantId,
    pub side: Side,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    /// Limit price in ticks; required for `LIMIT`, absent for `MARKET`.
    pub price_ticks: Option<i64>,
    pub quantity: u64,
}

impl From<OrderRequest> for SubmitOrder {
    fn from(r: OrderRequest) -> Self {
        Self {
            client_order_id: r.client_order_id,
            instrument_id: r.instrument_id,
            participant_id: r.participant_id,
            side: r.side,
            order_type: r.order_type,
            limit_price_ticks: r.price_ticks,
            quantity: r.quantity,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderView {
    pub order_id: Uuid,
    pub client_order_id: Option<String>,
    pub instrument_id: InstrumentId,
    pub participant_id: ParticipantId,
    pub side: Side,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    pub price_ticks: Option<i64>,
    pub price: Option<String>,
    pub quantity: u64,
    pub remaining_quantity: u64,
    pub filled_quantity: u64,
    pub status: OrderStatus,
    pub sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

impl OrderView {
    pub fn new(order: &Order, instrument: &Instrument) -> Self {
        Self {
            order_id: order.id,
            client_order_id: order.client_order_id.clone(),
            instrument_id: order.instrument_id.clone(),
            participant_id: order.participant_id.clone(),
            side: order.side,
            order_type: order.order_type,
            price_ticks: order.limit_price_ticks,
            price: order
                .limit_price_ticks
                .map(|ticks| instrument.ticks_to_price(ticks)),
            quantity: order.quantity,
            remaining_quantity: order.remaining_quantity,
            filled_quantity: order.filled_quantity(),
            status: order.status,
            sequence: order.sequence,
            reject_reason: None,
            created_at: None,
            updated_at: None,
        }
    }
}

/// Response to `POST /api/orders` and `DELETE /api/orders/{id}`: the order's resulting state and the executions the
/// command produced. `duplicate` is true when a repeated client order ID returned an existing order.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResult {
    pub order: OrderView,
    pub executions: Vec<TradeView>,
    pub duplicate: bool,
}

impl OrderResult {
    pub fn new(
        order: &Order,
        executions: &[Execution],
        rejection: Option<RejectReason>,
        instrument: &Instrument,
    ) -> Self {
        let mut view = OrderView::new(order, instrument);
        view.reject_reason = rejection.map(RejectReason::code);
        Self {
            order: view,
            executions: executions
                .iter()
                .map(|e| TradeView::new(e, instrument))
                .collect(),
            duplicate: false,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeView {
    pub execution_id: Uuid,
    pub instrument_id: InstrumentId,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub buyer_id: ParticipantId,
    pub seller_id: ParticipantId,
    pub price_ticks: i64,
    pub price: String,
    pub quantity: u64,
    pub currency: String,
    pub executed_at: DateTime<Utc>,
}

impl TradeView {
    pub fn new(e: &Execution, instrument: &Instrument) -> Self {
        Self {
            execution_id: e.id,
            instrument_id: e.instrument_id.clone(),
            buy_order_id: e.buy_order_id,
            sell_order_id: e.sell_order_id,
            buyer_id: e.buyer_id.clone(),
            seller_id: e.seller_id.clone(),
            price_ticks: e.price_ticks,
            price: instrument.ticks_to_price(e.price_ticks),
            quantity: e.quantity,
            currency: instrument.currency.clone(),
            executed_at: e.executed_at,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TradeQuery {
    pub instrument_id: Option<InstrumentId>,
    pub participant_id: Option<ParticipantId>,
    pub order_id: Option<Uuid>,
    /// 1 to 1000; default 100.
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LevelView {
    pub price_ticks: i64,
    pub price: String,
    pub quantity: u64,
    pub orders: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BookView {
    pub instrument_id: InstrumentId,
    pub currency: String,
    /// Whether the instrument trades; a disabled book rejects new orders.
    pub enabled: bool,
    /// Best (highest) first.
    pub bids: Vec<LevelView>,
    /// Best (lowest) first.
    pub asks: Vec<LevelView>,
    pub last_sequence: u64,
}

impl From<BookSnapshot> for BookView {
    fn from(s: BookSnapshot) -> Self {
        let levels = |levels: Vec<Level>| {
            levels
                .into_iter()
                .map(|l| LevelView {
                    price_ticks: l.price_ticks,
                    price: s.instrument.ticks_to_price(l.price_ticks),
                    quantity: l.quantity,
                    orders: l.orders,
                })
                .collect()
        };
        Self {
            bids: levels(s.bids.clone()),
            asks: levels(s.asks.clone()),
            instrument_id: s.instrument.id.clone(),
            currency: s.instrument.currency.clone(),
            enabled: s.instrument.enabled,
            last_sequence: s.last_sequence,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventQuery {
    /// Return events with a sequence greater than this; default 0 (from the beginning).
    pub after: Option<u64>,
    /// 1 to 1000; default 100.
    pub limit: Option<u32>,
    pub event_type: Option<String>,
}

/// `GET /api/marketdata`: an instrument's market data. For now its reference price; the order book's best prices
/// and the last trade join it with the market-data channel (plan item 6.8).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketDataView {
    pub instrument_id: InstrumentId,
    /// `null` when the instrument's price source is `none`.
    pub reference: Option<ReferenceView>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceView {
    pub price: String,
    pub price_ticks: i64,
    /// The price source's kind.
    pub source: &'static str,
    pub source_timestamp: DateTime<Utc>,
}

impl MarketDataView {
    pub fn new(instrument: &Instrument, reference: Option<Reference>) -> Self {
        Self {
            instrument_id: instrument.id.clone(),
            reference: reference.map(|r| ReferenceView {
                price: instrument.ticks_to_price(r.ticks),
                price_ticks: r.ticks,
                source: r.kind.as_str(),
                source_timestamp: r.source_timestamp,
            }),
        }
    }
}
