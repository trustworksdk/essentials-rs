//! Which canonical events a committed book update produces, in what order, with what payload.

use chrono::{TimeZone, Utc};
use exchange_simulator::domain::{
    Instrument, InstrumentId, Order, OrderStatus, OrderType, ParticipantId, Side,
};
use exchange_simulator::engine::{BookUpdate, MatchContext, OrderBook, SequentialIds};
use exchange_simulator::events::{self, Command, NewEvent};
use serde_json::json;
use uuid::Uuid;

fn acme() -> Instrument {
    Instrument {
        id: InstrumentId("ACME".into()),
        symbol: "ACME".into(),
        mic: Some("XCSE".into()),
        isin: Some("DK0010274414".into()),
        cusip: None,
        currency: "DKK".into(),
        currency_scale: 2,
        tick_size_minor: 1,
        lot_size: 1,
        enabled: true,
    }
}

struct Harness {
    book: OrderBook,
    ids: SequentialIds,
    sequence: u64,
}

impl Harness {
    fn new() -> Self {
        Self {
            book: OrderBook::new(acme()),
            ids: SequentialIds(1_000_000),
            sequence: 0,
        }
    }

    fn submit(
        &mut self,
        participant: &str,
        side: Side,
        price: Option<i64>,
        quantity: u64,
    ) -> (BookUpdate, Vec<NewEvent>) {
        self.sequence += 1;
        let order = Order {
            id: Uuid::from_u128(u128::from(self.sequence)),
            client_order_id: None,
            instrument_id: InstrumentId("ACME".into()),
            participant_id: ParticipantId(participant.into()),
            side,
            order_type: if price.is_some() {
                OrderType::Limit
            } else {
                OrderType::Market
            },
            limit_price_ticks: price,
            quantity,
            remaining_quantity: quantity,
            sequence: self.sequence,
            status: OrderStatus::Open,
        };
        let now = Utc.with_ymd_and_hms(2026, 10, 14, 12, 0, 0).unwrap();
        let update = self
            .book
            .submit(
                order,
                &mut MatchContext {
                    now,
                    ids: &mut self.ids,
                },
            )
            .unwrap();
        let events = events::for_update(Command::Submit, &update, &acme(), now, &mut self.ids);
        (update, events)
    }
}

fn types(events: &[NewEvent]) -> Vec<&str> {
    events.iter().map(|e| e.event_type).collect()
}

#[test]
fn resting_order_is_only_accepted() {
    let mut h = Harness::new();
    let (update, events) = h.submit("A", Side::Sell, Some(10025), 200);
    assert_eq!(types(&events), ["OrderAccepted"]);
    let event = &events[0];
    assert_eq!(event.aggregate_id, update.order.id.to_string());
    assert_eq!(
        event.correlation_id.as_deref(),
        Some(update.order.id.to_string().as_str())
    );
    assert_eq!(event.payload["status"], "OPEN");
    assert_eq!(event.payload["price"], "100.25");
}

#[test]
fn acceptance_trade_produces_trade_and_fill_events() {
    let mut h = Harness::new();
    let (sell, _) = h.submit("A", Side::Sell, Some(10025), 200);
    let (buy, events) = h.submit("B", Side::Buy, Some(10025), 150);
    assert_eq!(
        types(&events),
        [
            "OrderAccepted",
            "TradeExecuted",
            "OrderPartiallyFilled",
            "OrderFilled"
        ]
    );

    let accepted = &events[0].payload;
    assert_eq!(
        (&accepted["status"], &accepted["remainingQuantity"]),
        (&json!("OPEN"), &json!(150)),
        "as accepted"
    );

    let trade = &events[1];
    let execution = &buy.executions[0];
    assert_eq!(trade.aggregate_id, execution.id.to_string());
    assert_eq!(
        trade.correlation_id.as_deref(),
        Some(buy.order.id.to_string().as_str())
    );
    assert_eq!(
        trade.payload,
        json!({
            "executionId": execution.id,
            "instrumentId": "ACME",
            "instrument": { "id": "ACME", "symbol": "ACME", "mic": "XCSE", "isin": "DK0010274414", "cusip": null,
                            "currency": "DKK" },
            "buyOrderId": buy.order.id,
            "sellOrderId": sell.order.id,
            "buyerId": "B",
            "sellerId": "A",
            "priceTicks": 10025,
            "price": "100.25",
            "quantity": 150,
            "currency": "DKK",
            "currencyScale": 2,
            "tickSizeMinor": 1,
            "executedAt": "2026-10-14T12:00:00Z",
        })
    );

    let maker = &events[2];
    assert_eq!(maker.aggregate_id, sell.order.id.to_string());
    assert_eq!(
        (
            &maker.payload["remainingQuantity"],
            &maker.payload["executionId"]
        ),
        (&json!(50), &json!(execution.id))
    );
    assert_eq!(events[3].aggregate_id, buy.order.id.to_string());

    let ids: std::collections::HashSet<_> = events.iter().map(|e| e.event_id).collect();
    assert_eq!(ids.len(), events.len(), "event IDs are unique");
}

#[test]
fn sweep_interleaves_each_trade_with_its_maker_update() {
    let mut h = Harness::new();
    h.submit("A", Side::Sell, Some(10000), 10);
    h.submit("C", Side::Sell, Some(10100), 10);
    let (_, events) = h.submit("B", Side::Buy, Some(10100), 25);
    assert_eq!(
        types(&events),
        [
            "OrderAccepted",
            "TradeExecuted",
            "OrderFilled",
            "TradeExecuted",
            "OrderFilled",
            "OrderPartiallyFilled"
        ]
    );
    assert_eq!(events[5].payload["remainingQuantity"], 5);
}

#[test]
fn market_remainder_is_reported_as_cancelled() {
    let mut h = Harness::new();
    h.submit("A", Side::Sell, Some(10000), 10);
    let (_, events) = h.submit("B", Side::Buy, None, 25);
    assert_eq!(
        types(&events),
        [
            "OrderAccepted",
            "TradeExecuted",
            "OrderFilled",
            "OrderPartiallyFilled",
            "OrderCancelled"
        ]
    );
    assert_eq!(events[4].payload["cancelReason"], "MARKET_REMAINDER");
    assert_eq!(events[4].payload["remainingQuantity"], 15);

    let (_, events) = h.submit("B", Side::Buy, None, 5);
    assert_eq!(
        types(&events),
        ["OrderAccepted", "OrderCancelled"],
        "nothing to trade against"
    );
}

#[test]
fn self_trade_is_only_rejected() {
    let mut h = Harness::new();
    h.submit("A", Side::Sell, Some(10000), 10);
    let (_, events) = h.submit("A", Side::Buy, Some(10000), 10);
    assert_eq!(types(&events), ["OrderRejected"]);
    assert_eq!(events[0].payload["rejectReason"], "SELF_TRADE");
    assert_eq!(events[0].payload["status"], "REJECTED");
}

#[test]
fn cancel_is_reported_with_its_reason() {
    let mut h = Harness::new();
    let (placed, _) = h.submit("A", Side::Sell, Some(10000), 10);
    let update = h.book.cancel(placed.order.id).unwrap();
    let events = events::for_update(Command::Cancel, &update, &acme(), Utc::now(), &mut h.ids);
    assert_eq!(types(&events), ["OrderCancelled"]);
    assert_eq!(events[0].payload["cancelReason"], "REQUESTED");
}
