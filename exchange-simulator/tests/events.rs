//! Which canonical events a committed book update produces, in what order, with what payload.

use chrono::{TimeZone, Utc};
use exchange_simulator::domain::{
    Instrument, InstrumentId, Order, OrderStatus, OrderType, ParticipantId, Side,
};
use exchange_simulator::engine::{BookUpdate, IdScheme, MatchContext, OrderBook};
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
        reference_symbol: None,
        price_source: None,
        liquidity: None,
    }
}

struct Harness {
    book: OrderBook,
    ids: IdScheme,
    sequence: u64,
    /// Participants' settlement details, as the exchange would look them up.
    settlement: std::collections::HashMap<String, serde_json::Value>,
}

impl Harness {
    fn new() -> Self {
        Self {
            book: OrderBook::new(acme()),
            ids: IdScheme::Deterministic { seed: 1 },
            sequence: 0,
            settlement: Default::default(),
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
            time_in_force: exchange_simulator::domain::TimeInForce::Gtc,
        };
        let now = Utc.with_ymd_and_hms(2026, 10, 14, 12, 0, 0).unwrap();
        let update = self
            .book
            .submit(order, &mut MatchContext::utc(now, self.ids))
            .unwrap();
        let settlement = |id: &ParticipantId| self.settlement.get(&id.0).cloned();
        let events = events::for_update(Command::Submit, &update, &acme(), &settlement, now);
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
            "buyerSettlement": null,
            "sellerSettlement": null,
            "priceTicks": 10025,
            "price": "100.25",
            "quantity": 150,
            "currency": "DKK",
            "currencyScale": 2,
            "tickSizeMinor": 1,
            "executedAt": "2026-10-14T12:00:00Z",
            "tradeDate": "2026-10-14",
            "intendedSettlementDate": "2026-10-15",
        })
    );
    for fill in [&events[2], &events[3]] {
        assert_eq!(
            (
                &fill.payload["tradeDate"],
                &fill.payload["intendedSettlementDate"]
            ),
            (&json!("2026-10-14"), &json!("2026-10-15")),
            "{} carries its execution's dates",
            fill.event_type
        );
    }

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

    assert!(
        events
            .iter()
            .all(|e| e.sequence == 0 && e.event_id.is_nil()),
        "the writer stamps events, not the builder"
    );
    let mut stamped = events.clone();
    let next = NewEvent::stamp(&mut stamped, 41, h.ids);
    assert_eq!(next, 45);
    let sequences: Vec<_> = stamped.iter().map(|e| e.sequence).collect();
    assert_eq!(sequences, [41, 42, 43, 44]);
    assert!(
        stamped
            .iter()
            .all(|e| e.event_id == h.ids.event(e.sequence))
    );
}

#[test]
fn sweep_interleaves_each_trade_with_both_fill_events() {
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
            "OrderPartiallyFilled",
            "TradeExecuted",
            "OrderFilled",
            "OrderPartiallyFilled"
        ]
    );
    assert_eq!(
        (
            &events[3].payload["remainingQuantity"],
            &events[3].payload["status"]
        ),
        (&json!(15), &json!("PARTIALLY_FILLED")),
        "the incoming order after its first execution"
    );
    assert_eq!(events[6].payload["remainingQuantity"], 5);
}

/// SR-31's acceptance: a market buy of 300 against asks of 100 at 10.00, 10.01 and 10.02 gets one fill event per
/// execution, each naming it with what it filled and at which price, and each ask's fill names the same execution.
#[test]
fn every_fill_event_names_its_execution() {
    let mut h = Harness::new();
    let asks: Vec<BookUpdate> = [("A", 1000), ("C", 1001), ("D", 1002)]
        .into_iter()
        .map(|(participant, price)| h.submit(participant, Side::Sell, Some(price), 100).0)
        .collect();
    let (buy, events) = h.submit("B", Side::Buy, None, 300);
    assert_eq!(buy.executions.len(), 3);

    let order_id = buy.order.id.to_string();
    let taker: Vec<&NewEvent> = events
        .iter()
        .filter(|e| e.aggregate_id == order_id && e.event_type != "OrderAccepted")
        .collect();
    assert_eq!(
        taker.iter().map(|e| e.event_type).collect::<Vec<_>>(),
        [
            "OrderPartiallyFilled",
            "OrderPartiallyFilled",
            "OrderFilled"
        ]
    );
    let prices = ["10.00", "10.01", "10.02"];
    for (i, ((fill, execution), ask)) in taker.iter().zip(&buy.executions).zip(&asks).enumerate() {
        let trade = events
            .iter()
            .find(|e| e.event_type == "TradeExecuted" && e.aggregate_id == execution.id.to_string())
            .expect("the execution's TradeExecuted");
        let filled = 100 * (i as u64 + 1);
        assert_eq!(
            (
                &fill.payload["executionId"],
                &fill.payload["lastQuantity"],
                &fill.payload["lastPriceTicks"],
                &fill.payload["lastPrice"],
                &fill.payload["filledQuantity"],
                &fill.payload["remainingQuantity"],
            ),
            (
                &json!(execution.id),
                &trade.payload["quantity"],
                &trade.payload["priceTicks"],
                &json!(prices[i]),
                &json!(filled),
                &json!(300 - filled),
            ),
            "the buy's fill event for execution {i}"
        );
        assert_eq!(
            fill.payload["price"],
            json!(null),
            "a market order's own price"
        );
        assert_eq!(
            (
                fill.payload["tradeDate"].clone(),
                fill.payload["intendedSettlementDate"].clone()
            ),
            (
                json!(execution.trade_date),
                json!(execution.intended_settlement_date)
            )
        );

        let maker = events
            .iter()
            .find(|e| e.aggregate_id == ask.order.id.to_string())
            .expect("the ask's fill event");
        assert_eq!(maker.event_type, "OrderFilled");
        assert_eq!(
            (
                &maker.payload["executionId"],
                &maker.payload["lastQuantity"],
                &maker.payload["lastPrice"],
            ),
            (&fill.payload["executionId"], &json!(100), &json!(prices[i])),
            "ask {i} names the same execution as the buy's event for it"
        );
    }
    assert_eq!(taker[2].payload["status"], "FILLED");
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
    let events = events::for_update(Command::Cancel, &update, &acme(), &|_| None, Utc::now());
    assert_eq!(types(&events), ["OrderCancelled"]);
    assert_eq!(events[0].payload["cancelReason"], "REQUESTED");
}

#[test]
fn trade_carries_both_parties_settlement_details() {
    let mut h = Harness::new();
    let seller = json!({ "account": "DK-SELL-001", "bic": "DABADKKK" });
    h.settlement.insert("A".into(), seller.clone());
    h.submit("A", Side::Sell, Some(10025), 10);
    let (_, events) = h.submit("B", Side::Buy, Some(10025), 10);
    let trade = events
        .iter()
        .find(|e| e.event_type == "TradeExecuted")
        .unwrap();
    assert_eq!(trade.payload["sellerSettlement"], seller);
    assert_eq!(
        trade.payload["buyerSettlement"],
        serde_json::Value::Null,
        "B has none"
    );
}
