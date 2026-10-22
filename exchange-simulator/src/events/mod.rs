//! Canonical event contracts. Events are derived from a committed book update, written in the same transaction as the
//! orders and executions they describe, and never change afterwards. The global `sequence` assigned by SQLite is the
//! replay cursor.

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::domain::{Execution, Instrument, Order, OrderStatus, OrderType, ParticipantId};
use crate::engine::BookUpdate;

/// Version of the payload schemas below. Bump on breaking payload changes.
pub const SCHEMA_VERSION: u32 = 1;

/// The component that produced the event: the exchange (trading events)...
pub const EXCHANGE_SOURCE: &str = "exchange";
/// ...or the optional settlement simulator (simulated lifecycle events)...
pub const SETTLEMENT_SOURCE: &str = "settlement-simulator";
/// ...or an external settlement service reporting outcomes back...
pub const EXTERNAL_SOURCE: &str = "external";
/// ...or the clearinghouse (end-of-day statements).
pub const CLEARING_SOURCE: &str = "clearinghouse";

pub const ORDER_ACCEPTED: &str = "OrderAccepted";
pub const ORDER_REJECTED: &str = "OrderRejected";
pub const ORDER_PARTIALLY_FILLED: &str = "OrderPartiallyFilled";
pub const ORDER_FILLED: &str = "OrderFilled";
pub const ORDER_CANCELLED: &str = "OrderCancelled";
/// A `DAY` order still open when its session closed (SR-17).
pub const ORDER_EXPIRED: &str = "OrderExpired";
pub const TRADE_EXECUTED: &str = "TradeExecuted";
/// An instrument was listed, through the API or from configuration.
pub const INSTRUMENT_LISTED: &str = "InstrumentListed";
/// A business day ended at its cutoff (`source: "exchange"`), also on days without trades.
pub const BUSINESS_DAY_CLOSED: &str = "BusinessDayClosed";
/// An instrument's closing price for a business day, right after its `BusinessDayClosed` (SR-14).
pub const CLOSING_PRICE_ISSUED: &str = "ClosingPriceIssued";
/// The virtual clock changed: speed, freeze, resume, set or advance (`source: "exchange"`).
pub use simulator_kit::clock::CLOCK_CHANGED;
/// A listed instrument changed (trading enabled or disabled, reference symbol).
pub const INSTRUMENT_UPDATED: &str = "InstrumentUpdated";

pub const SETTLEMENT_PENDING: &str = "SettlementPending";
pub const SETTLEMENT_COMPLETED: &str = "SettlementCompleted";
pub const SETTLEMENT_FAILED: &str = "SettlementFailed";
pub const SETTLEMENT_REJECTED: &str = "SettlementRejected";
pub const SETTLEMENT_DELAYED: &str = "SettlementDelayed";
pub const SETTLEMENT_CORRECTED: &str = "SettlementCorrected";
pub const SETTLEMENT_CANCELLED: &str = "SettlementCancelled";
/// An outcome reported by an external settlement service (`source: "external"`).
pub const SETTLEMENT_OUTCOME_REPORTED: &str = "SettlementOutcomeReported";
/// An end-of-day clearing statement for one participant (`source: "clearinghouse"`).
pub const CLEARING_STATEMENT_ISSUED: &str = "ClearingStatementIssued";

/// Every event type the service emits, for validating subscriptions.
pub const EVENT_TYPES: &[&str] = &[
    ORDER_ACCEPTED,
    ORDER_REJECTED,
    ORDER_PARTIALLY_FILLED,
    ORDER_FILLED,
    ORDER_CANCELLED,
    ORDER_EXPIRED,
    TRADE_EXECUTED,
    INSTRUMENT_LISTED,
    INSTRUMENT_UPDATED,
    CLOCK_CHANGED,
    BUSINESS_DAY_CLOSED,
    CLOSING_PRICE_ISSUED,
    SETTLEMENT_PENDING,
    SETTLEMENT_COMPLETED,
    SETTLEMENT_FAILED,
    SETTLEMENT_REJECTED,
    SETTLEMENT_DELAYED,
    SETTLEMENT_CORRECTED,
    SETTLEMENT_CANCELLED,
    SETTLEMENT_OUTCOME_REPORTED,
    CLEARING_STATEMENT_ISSUED,
];

/// `BusinessDayClosed` for `date` under `calendar`: the authoritative end of the business day. Its `occurredAt` is
/// the cutoff; the aggregate ID is the business date.
pub fn business_day_closed(
    date: chrono::NaiveDate,
    calendar: &simulator_kit::calendar::CutoffCalendar,
) -> NewEvent {
    let cutoff_at = calendar.cutoff_at(date);
    NewEvent {
        event_id: Uuid::nil(),
        sequence: 0,
        event_type: BUSINESS_DAY_CLOSED,
        source: EXCHANGE_SOURCE,
        aggregate_id: date.to_string(),
        correlation_id: None,
        occurred_at: cutoff_at,
        payload: json!({
            "businessDate": date,
            "cutoffAt": cutoff_at,
            "nextBusinessDate": calendar.days.next_business_day(date),
            "timeZone": calendar.time_zone.name(),
        }),
    }
}

/// Where a closing price comes from (SR-14).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClosingBasis {
    /// The instrument's last trade of the business day.
    LastTrade,
    /// No trade that day: its reference price at the cutoff.
    Reference,
}

/// `ClosingPriceIssued` for `instrument` on business date `date`, stamped at the cutoff. Aggregate: the instrument;
/// correlation: the business date.
pub fn closing_price(
    instrument: &Instrument,
    date: chrono::NaiveDate,
    cutoff_at: DateTime<Utc>,
    price_ticks: i64,
    basis: ClosingBasis,
    volume: u64,
) -> NewEvent {
    NewEvent {
        event_id: Uuid::nil(),
        sequence: 0,
        event_type: CLOSING_PRICE_ISSUED,
        source: EXCHANGE_SOURCE,
        aggregate_id: instrument.id.0.clone(),
        correlation_id: Some(date.to_string()),
        occurred_at: cutoff_at,
        payload: json!({
            "instrumentId": instrument.id,
            "businessDate": date,
            "closingPrice": instrument.ticks_to_price(price_ticks),
            "closingPriceTicks": price_ticks,
            "currency": instrument.currency,
            "basis": match basis {
                ClosingBasis::LastTrade => "LAST_TRADE",
                ClosingBasis::Reference => "REFERENCE",
            },
            "volume": volume,
        }),
    }
}

/// The participants an event involves, for webhook participant filters: the `participantId` of order events and
/// clearing statements, the `buyerId` and `sellerId` of trades and settlement events. Instrument events involve none.
pub fn parties(_event_type: &str, payload: &Value) -> Vec<String> {
    ["participantId", "buyerId", "sellerId"]
        .iter()
        .filter_map(|field| payload.get(field).and_then(Value::as_str))
        .map(str::to_string)
        .collect()
}

/// Why an order was cancelled, carried in `OrderCancelled` payloads.
pub const CANCEL_MARKET_REMAINDER: &str = "MARKET_REMAINDER";
pub const CANCEL_IOC_REMAINDER: &str = "IOC_REMAINDER";
pub const CANCEL_REQUESTED: &str = "REQUESTED";

pub use simulator_kit::events::{EventEnvelope, NewEvent};

/// Whether a command was a new order, a cancellation or an expiry, which decides the events an update gives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    Submit,
    Cancel,
    /// A `DAY` order expiring at its session's close, with the business day that ended (SR-32).
    Expire {
        business_date: NaiveDate,
    },
}

/// `InstrumentListed` (no `changes`) or `InstrumentUpdated` with the wire names of the fields that changed. The payload
/// is the whole instrument after the change, as `GET /api/instruments` shows it. Unstamped, like [`for_update`]'s.
pub fn instrument_event(
    instrument: &Instrument,
    changes: Option<&[&'static str]>,
    now: DateTime<Utc>,
) -> NewEvent {
    let mut payload = json!({
        "instrumentId": instrument.id,
        "symbol": instrument.symbol,
        "mic": instrument.mic,
        "isin": instrument.isin,
        "cusip": instrument.cusip,
        "currency": instrument.currency,
        "currencyScale": instrument.currency_scale,
        "tickSizeMinor": instrument.tick_size_minor,
        "tickSize": instrument.ticks_to_price(1),
        "lotSize": instrument.lot_size,
        "enabled": instrument.enabled,
        "referenceSymbol": instrument.reference_symbol,
        "priceSource": instrument.price_source,
        "liquidity": instrument.liquidity,
    });
    if let Some(changes) = changes {
        payload["changes"] = json!(changes);
    }
    NewEvent {
        event_id: Uuid::nil(),
        sequence: 0,
        event_type: if changes.is_some() {
            INSTRUMENT_UPDATED
        } else {
            INSTRUMENT_LISTED
        },
        source: EXCHANGE_SOURCE,
        aggregate_id: instrument.id.0.clone(),
        correlation_id: None,
        occurred_at: now,
        payload,
    }
}

/// The events a committed update produces, in this order:
///
/// - submit, rejected: `OrderRejected`
/// - submit: `OrderAccepted`, then for each execution `TradeExecuted` followed by the resting order's and then the
///   incoming order's `OrderPartiallyFilled`/`OrderFilled`, each naming the execution, then `OrderCancelled` if a market
///   or IOC remainder was cancelled
/// - cancel: `OrderCancelled`
///
/// Every event carries the subject order's ID as correlation ID. Events come back unstamped: the writer assigns their
/// sequences and IDs.
pub fn for_update(
    command: Command,
    update: &BookUpdate,
    instrument: &Instrument,
    settlement: &dyn Fn(&ParticipantId) -> Option<Value>,
    now: DateTime<Utc>,
) -> Vec<NewEvent> {
    let order = &update.order;
    let correlation_id = Some(order.id.to_string());
    let mut events = Vec::new();
    let mut push = |event_type: &'static str, aggregate_id: String, payload: Value| {
        events.push(NewEvent {
            event_id: Uuid::nil(),
            sequence: 0,
            event_type,
            source: EXCHANGE_SOURCE,
            aggregate_id,
            correlation_id: correlation_id.clone(),
            occurred_at: now,
            payload,
        });
    };

    if command == Command::Cancel {
        push(
            ORDER_CANCELLED,
            order.id.to_string(),
            order_payload(order, instrument, Some(CANCEL_REQUESTED)),
        );
        return events;
    }
    if let Command::Expire { business_date } = command {
        let mut payload = order_payload(order, instrument, None);
        payload["businessDate"] = json!(business_date);
        push(ORDER_EXPIRED, order.id.to_string(), payload);
        return events;
    }
    if let Some(reason) = update.rejection {
        let mut payload = order_payload(order, instrument, None);
        payload["rejectReason"] = json!(reason.code());
        push(ORDER_REJECTED, order.id.to_string(), payload);
        return events;
    }

    let mut accepted = order.clone();
    accepted.remaining_quantity = accepted.quantity;
    accepted.status = OrderStatus::Open;
    push(
        ORDER_ACCEPTED,
        order.id.to_string(),
        order_payload(&accepted, instrument, None),
    );
    // The incoming order as it stood after each execution (SR-31); after the last one, its resulting state.
    let mut taker = accepted;
    let last = update.executions.len().saturating_sub(1);
    for (i, (execution, maker)) in update.executions.iter().zip(&update.fills).enumerate() {
        push(
            TRADE_EXECUTED,
            execution.id.to_string(),
            trade_payload(execution, instrument, settlement),
        );
        push(
            fill_event(maker),
            maker.id.to_string(),
            fill_payload(maker, instrument, execution),
        );
        if i == last {
            taker = order.clone();
        } else {
            taker.remaining_quantity -= execution.quantity;
            taker.status = OrderStatus::PartiallyFilled;
        }
        push(
            fill_event(&taker),
            order.id.to_string(),
            fill_payload(&taker, instrument, execution),
        );
    }
    // A market or IOC order's remainder is cancelled as part of the submission.
    if order.status == OrderStatus::Cancelled {
        let reason = if order.order_type == OrderType::Market {
            CANCEL_MARKET_REMAINDER
        } else {
            CANCEL_IOC_REMAINDER
        };
        push(
            ORDER_CANCELLED,
            order.id.to_string(),
            order_payload(order, instrument, Some(reason)),
        );
    }
    events
}

/// A fill event's payload: the order after the execution, and the execution it reports, as a FIX execution report
/// names it (SR-31: `ExecID`, `LastQty`, `LastPx`), with its trade and intended settlement dates (SR-29).
fn fill_payload(order: &Order, instrument: &Instrument, execution: &Execution) -> Value {
    let mut payload = order_payload(order, instrument, None);
    payload["executionId"] = json!(execution.id);
    payload["lastQuantity"] = json!(execution.quantity);
    payload["lastPriceTicks"] = json!(execution.price_ticks);
    payload["lastPrice"] = json!(instrument.ticks_to_price(execution.price_ticks));
    payload["tradeDate"] = json!(execution.trade_date);
    payload["intendedSettlementDate"] = json!(execution.intended_settlement_date);
    payload
}

fn fill_event(order: &Order) -> &'static str {
    if order.remaining_quantity == 0 {
        ORDER_FILLED
    } else {
        ORDER_PARTIALLY_FILLED
    }
}

fn order_payload(order: &Order, instrument: &Instrument, cancel_reason: Option<&str>) -> Value {
    let mut payload = json!({
        "orderId": order.id,
        "clientOrderId": order.client_order_id,
        "instrumentId": order.instrument_id,
        "participantId": order.participant_id,
        "side": order.side,
        "type": order.order_type,
        "priceTicks": order.limit_price_ticks,
        "price": order.limit_price_ticks.map(|t| instrument.ticks_to_price(t)),
        "quantity": order.quantity,
        "remainingQuantity": order.remaining_quantity,
        "filledQuantity": order.filled_quantity(),
        "status": order.status,
        "sequence": order.sequence,
        "timeInForce": order.time_in_force,
    });
    if let Some(reason) = cancel_reason {
        payload["cancelReason"] = json!(reason);
    }
    payload
}

/// `TradeExecuted` payload: everything a settlement consumer needs, including snapshots, as of execution time, of
/// the instrument's identifiers and of both parties' settlement details (`null` when a party has none).
fn trade_payload(
    execution: &Execution,
    instrument: &Instrument,
    settlement: &dyn Fn(&ParticipantId) -> Option<Value>,
) -> Value {
    json!({
        "executionId": execution.id,
        "instrumentId": execution.instrument_id,
        "instrument": {
            "id": instrument.id,
            "symbol": instrument.symbol,
            "mic": instrument.mic,
            "isin": instrument.isin,
            "cusip": instrument.cusip,
            "currency": instrument.currency,
        },
        "buyOrderId": execution.buy_order_id,
        "sellOrderId": execution.sell_order_id,
        "buyerId": execution.buyer_id,
        "sellerId": execution.seller_id,
        "buyerSettlement": settlement(&execution.buyer_id),
        "sellerSettlement": settlement(&execution.seller_id),
        "priceTicks": execution.price_ticks,
        "price": instrument.ticks_to_price(execution.price_ticks),
        "quantity": execution.quantity,
        "currency": instrument.currency,
        "currencyScale": instrument.currency_scale,
        "tickSizeMinor": instrument.tick_size_minor,
        "executedAt": execution.executed_at,
        "tradeDate": execution.trade_date,
        "intendedSettlementDate": execution.intended_settlement_date,
    })
}
