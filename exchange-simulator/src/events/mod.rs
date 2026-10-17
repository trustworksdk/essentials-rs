//! Canonical event contracts. Events are derived from a committed book update, written in the same transaction as the
//! orders and executions they describe, and never change afterwards. The global `sequence` assigned by SQLite is the
//! replay cursor.

use chrono::{DateTime, Utc};
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
pub const TRADE_EXECUTED: &str = "TradeExecuted";

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
    TRADE_EXECUTED,
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

/// Why an order was cancelled, carried in `OrderCancelled` payloads.
pub const CANCEL_MARKET_REMAINDER: &str = "MARKET_REMAINDER";
pub const CANCEL_REQUESTED: &str = "REQUESTED";

pub use simulator_kit::events::{EventEnvelope, NewEvent};

/// Whether a command was a new order or a cancellation, which decides how a `CANCELLED` result is explained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    Submit,
    Cancel,
}

/// The events a committed update produces, in this order:
///
/// - submit, rejected: `OrderRejected`
/// - submit: `OrderAccepted`, then for each execution `TradeExecuted` followed by the resting order's
///   `OrderPartiallyFilled`/`OrderFilled`, then the incoming order's own fill event if it traded, then `OrderCancelled`
///   if a market remainder was cancelled
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
            order_payload(order, instrument, Some(CANCEL_REQUESTED), None),
        );
        return events;
    }
    if let Some(reason) = update.rejection {
        let mut payload = order_payload(order, instrument, None, None);
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
        order_payload(&accepted, instrument, None, None),
    );
    for (execution, maker) in update.executions.iter().zip(&update.fills) {
        push(
            TRADE_EXECUTED,
            execution.id.to_string(),
            trade_payload(execution, instrument, settlement),
        );
        push(
            fill_event(maker),
            maker.id.to_string(),
            order_payload(maker, instrument, None, Some(execution.id)),
        );
    }
    if order.filled_quantity() > 0 {
        push(
            fill_event(order),
            order.id.to_string(),
            order_payload(order, instrument, None, None),
        );
    }
    if order.status == OrderStatus::Cancelled && order.order_type == OrderType::Market {
        push(
            ORDER_CANCELLED,
            order.id.to_string(),
            order_payload(order, instrument, Some(CANCEL_MARKET_REMAINDER), None),
        );
    }
    events
}

fn fill_event(order: &Order) -> &'static str {
    if order.remaining_quantity == 0 {
        ORDER_FILLED
    } else {
        ORDER_PARTIALLY_FILLED
    }
}

fn order_payload(
    order: &Order,
    instrument: &Instrument,
    cancel_reason: Option<&str>,
    execution_id: Option<Uuid>,
) -> Value {
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
    });
    if let Some(reason) = cancel_reason {
        payload["cancelReason"] = json!(reason);
    }
    if let Some(execution_id) = execution_id {
        payload["executionId"] = json!(execution_id);
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
    })
}
