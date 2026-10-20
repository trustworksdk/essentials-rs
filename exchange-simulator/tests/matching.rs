//! Order-book matching: price-time priority, partial fills, cancellation, market remainders and self-trade policy.

use chrono::{DateTime, TimeZone, Utc};
use exchange_simulator::domain::{
    Instrument, InstrumentId, Order, OrderStatus, OrderType, ParticipantId, Side,
};
use exchange_simulator::engine::{
    BookError, IdScheme, InvalidOrder, Level, MatchContext, OrderBook, RejectReason,
};
use exchange_simulator::ids::ExchangeIds;
use uuid::Uuid;

fn acme() -> Instrument {
    Instrument {
        id: InstrumentId("ACME".into()),
        symbol: "ACME".into(),
        mic: Some("XCSE".into()),
        isin: None,
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

/// A book plus a command sequence and deterministic clock and IDs.
struct Harness {
    book: OrderBook,
    sequence: u64,
    ids: IdScheme,
    now: DateTime<Utc>,
}

impl Harness {
    fn new() -> Self {
        Self::with(acme())
    }

    fn with(instrument: Instrument) -> Self {
        Self {
            book: OrderBook::new(instrument),
            sequence: 0,
            // Order IDs come from a separate range so they never collide with execution IDs.
            ids: IdScheme::Deterministic { seed: 1 },
            now: Utc.with_ymd_and_hms(2026, 10, 14, 12, 0, 0).unwrap(),
        }
    }

    fn order(&mut self, participant: &str, side: Side, price: Option<i64>, quantity: u64) -> Order {
        self.sequence += 1;
        Order {
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
        }
    }

    fn submit(&mut self, order: Order) -> exchange_simulator::engine::BookUpdate {
        let mut ctx = MatchContext::utc(self.now, self.ids);
        self.book.submit(order, &mut ctx).expect("valid order")
    }

    fn limit(
        &mut self,
        participant: &str,
        side: Side,
        price: i64,
        quantity: u64,
    ) -> exchange_simulator::engine::BookUpdate {
        let order = self.order(participant, side, Some(price), quantity);
        self.submit(order)
    }

    fn market(
        &mut self,
        participant: &str,
        side: Side,
        quantity: u64,
    ) -> exchange_simulator::engine::BookUpdate {
        let order = self.order(participant, side, None, quantity);
        self.submit(order)
    }
}

fn level(price_ticks: i64, quantity: u64, orders: usize) -> Level {
    Level {
        price_ticks,
        quantity,
        orders,
    }
}

#[test]
fn milestone_acceptance_scenario() {
    let mut h = Harness::new();
    let sell = h.limit("A", Side::Sell, 10025, 200);
    assert_eq!(sell.order.status, OrderStatus::Open);
    assert!(sell.executions.is_empty());

    let buy = h.limit("B", Side::Buy, 10025, 150);
    assert_eq!(buy.executions.len(), 1);
    let execution = &buy.executions[0];
    assert_eq!(execution.quantity, 150);
    assert_eq!(execution.price_ticks, 10025);
    assert_eq!(
        (execution.buy_order_id, execution.sell_order_id),
        (buy.order.id, sell.order.id)
    );
    assert_eq!(
        (
            execution.buyer_id.0.as_str(),
            execution.seller_id.0.as_str()
        ),
        ("B", "A")
    );
    assert_eq!(execution.executed_at, h.now);

    assert_eq!(buy.order.status, OrderStatus::Filled);
    assert_eq!(buy.order.remaining_quantity, 0);
    assert_eq!(buy.fills.len(), 1);
    assert_eq!(buy.fills[0].status, OrderStatus::PartiallyFilled);
    assert_eq!(buy.fills[0].remaining_quantity, 50);

    let resting = h.book.order(sell.order.id).unwrap();
    assert_eq!(
        (resting.remaining_quantity, resting.status),
        (50, OrderStatus::PartiallyFilled)
    );
    assert_eq!(h.book.depth(Side::Sell), [level(10025, 50, 1)]);
    assert!(h.book.depth(Side::Buy).is_empty());
}

#[test]
fn non_crossing_limit_orders_rest_on_both_sides() {
    let mut h = Harness::new();
    h.limit("A", Side::Buy, 9900, 10);
    h.limit("A", Side::Buy, 10000, 20);
    h.limit("B", Side::Sell, 10100, 30);
    h.limit("B", Side::Sell, 10200, 40);
    assert_eq!(h.book.best_bid(), Some(10000));
    assert_eq!(h.book.best_ask(), Some(10100));
    assert_eq!(
        h.book.depth(Side::Buy),
        [level(10000, 20, 1), level(9900, 10, 1)]
    );
    assert_eq!(
        h.book.depth(Side::Sell),
        [level(10100, 30, 1), level(10200, 40, 1)]
    );
    assert_eq!(h.book.len(), 4);
}

#[test]
fn limit_one_tick_short_of_the_opposite_best_does_not_cross() {
    let mut h = Harness::new();
    h.limit("A", Side::Sell, 10000, 10);
    h.limit("A", Side::Buy, 9998, 10);
    assert!(
        h.limit("B", Side::Buy, 9999, 10).executions.is_empty(),
        "buy one tick below the best ask rests"
    );
    assert!(
        h.limit("C", Side::Sell, 10000, 10).executions.is_empty(),
        "sell one tick above the best bid rests"
    );
    assert_eq!(
        (h.book.best_bid(), h.book.best_ask()),
        (Some(9999), Some(10000))
    );
    assert_eq!(
        h.limit("D", Side::Sell, 9999, 10).executions.len(),
        1,
        "touching the best bid crosses"
    );
    assert_eq!(
        h.limit("D", Side::Buy, 10000, 10).executions.len(),
        1,
        "touching the best ask crosses"
    );
}

#[test]
fn crossing_order_trades_at_the_resting_price() {
    let mut h = Harness::new();
    h.limit("A", Side::Sell, 10000, 10);
    let buy = h.limit("B", Side::Buy, 10500, 10);
    assert_eq!(
        buy.executions[0].price_ticks, 10000,
        "price improvement goes to the aggressor"
    );

    h.limit("A", Side::Buy, 9000, 10);
    let sell = h.limit("B", Side::Sell, 8000, 10);
    assert_eq!(sell.executions[0].price_ticks, 9000);
    assert_eq!(sell.executions[0].buyer_id.0, "A");
    assert!(h.book.is_empty());
}

#[test]
fn same_price_fills_in_time_priority() {
    let mut h = Harness::new();
    let first = h.limit("A", Side::Sell, 10000, 100);
    let second = h.limit("C", Side::Sell, 10000, 100);
    let third = h.limit("D", Side::Sell, 10000, 100);

    let buy = h.limit("B", Side::Buy, 10000, 150);
    let sellers: Vec<_> = buy
        .executions
        .iter()
        .map(|e| (e.sell_order_id, e.quantity))
        .collect();
    assert_eq!(sellers, [(first.order.id, 100), (second.order.id, 50)]);
    assert_eq!(buy.fills[0].status, OrderStatus::Filled);
    assert_eq!(buy.fills[1].status, OrderStatus::PartiallyFilled);

    let queue: Vec<_> = h
        .book
        .resting_orders(Side::Sell)
        .iter()
        .map(|o| (o.id, o.remaining_quantity))
        .collect();
    assert_eq!(
        queue,
        [(second.order.id, 50), (third.order.id, 100)],
        "a partial fill keeps its place in the queue"
    );
}

#[test]
fn better_price_beats_earlier_time() {
    let mut h = Harness::new();
    let early_worse = h.limit("A", Side::Buy, 9900, 10);
    let late_better = h.limit("C", Side::Buy, 10000, 10);
    let sell = h.limit("B", Side::Sell, 9900, 15);
    let buyers: Vec<_> = sell
        .executions
        .iter()
        .map(|e| (e.buy_order_id, e.price_ticks, e.quantity))
        .collect();
    assert_eq!(
        buyers,
        [
            (late_better.order.id, 10000, 10),
            (early_worse.order.id, 9900, 5)
        ]
    );
}

#[test]
fn limit_order_sweeps_levels_up_to_its_limit_and_rests_the_remainder() {
    let mut h = Harness::new();
    h.limit("A", Side::Sell, 10000, 10);
    h.limit("A", Side::Sell, 10100, 10);
    h.limit("A", Side::Sell, 10300, 10);

    let buy = h.limit("B", Side::Buy, 10200, 50);
    let fills: Vec<_> = buy
        .executions
        .iter()
        .map(|e| (e.price_ticks, e.quantity))
        .collect();
    assert_eq!(fills, [(10000, 10), (10100, 10)]);
    assert_eq!(buy.order.status, OrderStatus::PartiallyFilled);
    assert_eq!(buy.order.remaining_quantity, 30);
    assert_eq!(h.book.depth(Side::Buy), [level(10200, 30, 1)]);
    assert_eq!(
        h.book.depth(Side::Sell),
        [level(10300, 10, 1)],
        "emptied levels are removed"
    );
}

#[test]
fn one_order_produces_one_execution_per_resting_order() {
    let mut h = Harness::new();
    for participant in ["A", "C", "D"] {
        h.limit(participant, Side::Buy, 10000, 5);
    }
    let sell = h.limit("B", Side::Sell, 10000, 15);
    assert_eq!(sell.executions.len(), 3);
    let ids: std::collections::HashSet<_> = sell.executions.iter().map(|e| e.id).collect();
    assert_eq!(ids.len(), 3, "execution IDs are distinct");
    assert!(
        sell.executions
            .iter()
            .all(|e| e.sell_order_id == sell.order.id)
    );
    assert_eq!(sell.order.status, OrderStatus::Filled);
}

#[test]
fn market_order_sweeps_and_cancels_its_remainder() {
    let mut h = Harness::new();
    h.limit("A", Side::Sell, 10000, 10);
    h.limit("A", Side::Sell, 99_999, 10);

    let buy = h.market("B", Side::Buy, 25);
    let fills: Vec<_> = buy
        .executions
        .iter()
        .map(|e| (e.price_ticks, e.quantity))
        .collect();
    assert_eq!(
        fills,
        [(10000, 10), (99_999, 10)],
        "a market order takes any price"
    );
    assert_eq!(buy.order.status, OrderStatus::Cancelled);
    assert_eq!(
        buy.order.remaining_quantity, 5,
        "the cancelled remainder is kept on the order"
    );
    assert!(h.book.is_empty(), "market orders never rest");
}

#[test]
fn market_order_against_an_empty_side_is_cancelled_without_trading() {
    let mut h = Harness::new();
    h.limit("A", Side::Buy, 10000, 10);
    let buy = h.market("B", Side::Buy, 10);
    assert_eq!(buy.order.status, OrderStatus::Cancelled);
    assert!(buy.executions.is_empty());
    assert_eq!(h.book.len(), 1);
}

#[test]
fn fully_filled_market_order_is_filled() {
    let mut h = Harness::new();
    h.limit("A", Side::Buy, 10000, 10);
    let sell = h.market("B", Side::Sell, 10);
    assert_eq!(sell.order.status, OrderStatus::Filled);
    assert!(h.book.is_empty());
}

#[test]
fn cancel_removes_the_order_and_its_empty_level() {
    let mut h = Harness::new();
    let a = h.limit("A", Side::Buy, 10000, 10);
    let b = h.limit("C", Side::Buy, 10000, 20);
    let c = h.limit("C", Side::Buy, 9900, 30);

    let cancelled = h.book.cancel(a.order.id).unwrap();
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);
    assert_eq!(cancelled.order.remaining_quantity, 10);
    assert!(cancelled.executions.is_empty());
    assert_eq!(
        h.book.depth(Side::Buy),
        [level(10000, 20, 1), level(9900, 30, 1)]
    );

    h.book.cancel(c.order.id).unwrap();
    assert_eq!(h.book.depth(Side::Buy), [level(10000, 20, 1)]);

    assert_eq!(
        h.book.cancel(a.order.id),
        Err(BookError::UnknownOrder(a.order.id)),
        "already cancelled"
    );
    let sell = h.limit("B", Side::Sell, 9000, 20);
    assert_eq!(sell.executions[0].buy_order_id, b.order.id);
    assert_eq!(
        h.book.cancel(b.order.id),
        Err(BookError::UnknownOrder(b.order.id)),
        "filled orders cannot be cancelled"
    );
}

#[test]
fn cancelling_a_partially_filled_order_keeps_its_fill() {
    let mut h = Harness::new();
    let sell = h.limit("A", Side::Sell, 10000, 100);
    h.limit("B", Side::Buy, 10000, 40);
    let cancelled = h.book.cancel(sell.order.id).unwrap();
    assert_eq!(cancelled.order.remaining_quantity, 60);
    assert_eq!(cancelled.order.filled_quantity(), 40);
    assert!(h.book.is_empty());
}

#[test]
fn self_trade_rejects_the_incoming_order_whole() {
    let mut h = Harness::new();
    h.limit("B", Side::Sell, 10000, 10);
    let own = h.limit("A", Side::Sell, 10000, 10);
    let before = h.book.depth(Side::Sell);

    let buy = h.limit("A", Side::Buy, 10000, 20);
    assert_eq!(buy.rejection, Some(RejectReason::SelfTrade));
    assert_eq!(buy.order.status, OrderStatus::Rejected);
    assert_eq!(buy.order.remaining_quantity, 20);
    assert!(
        buy.executions.is_empty() && buy.fills.is_empty(),
        "nothing trades, not even ahead of the own order"
    );
    assert_eq!(h.book.depth(Side::Sell), before);
    assert!(h.book.order(own.order.id).is_some());
    assert!(
        h.book.depth(Side::Buy).is_empty(),
        "a rejected order does not rest"
    );

    let market = h.market("A", Side::Buy, 15);
    assert_eq!(market.rejection, Some(RejectReason::SelfTrade));
}

#[test]
fn own_orders_the_incoming_order_would_not_reach_do_not_reject_it() {
    let mut h = Harness::new();
    h.limit("B", Side::Sell, 10000, 10);
    h.limit("A", Side::Sell, 10000, 10);
    h.limit("A", Side::Sell, 10100, 10);
    let buy = h.limit("A", Side::Buy, 10000, 10);
    assert_eq!(buy.rejection, None);
    assert_eq!(buy.order.status, OrderStatus::Filled);
    h.limit("A", Side::Buy, 9000, 10);
    assert_eq!(
        h.book.best_bid(),
        Some(9000),
        "a participant may quote both sides without crossing"
    );
}

#[test]
fn malformed_orders_are_refused_before_matching() {
    let mut h = Harness::with(Instrument {
        lot_size: 10,
        ..acme()
    });
    let ids = IdScheme::Deterministic { seed: 1 };
    let now = h.now;
    let refuse = |h: &mut Harness, mutate: &dyn Fn(&mut Order)| {
        let mut order = h.order("A", Side::Buy, Some(10000), 100);
        mutate(&mut order);
        h.book
            .plan_submit(order, &mut MatchContext::utc(now, ids))
            .unwrap_err()
    };
    assert_eq!(
        refuse(&mut h, &|o| (o.quantity, o.remaining_quantity) = (0, 0)),
        InvalidOrder::Quantity.into()
    );
    assert_eq!(
        refuse(&mut h, &|o| (o.quantity, o.remaining_quantity) = (15, 15)),
        InvalidOrder::Quantity.into()
    );
    assert_eq!(
        refuse(&mut h, &|o| o.limit_price_ticks = None),
        InvalidOrder::MissingLimitPrice.into()
    );
    assert_eq!(
        refuse(&mut h, &|o| o.limit_price_ticks = Some(0)),
        InvalidOrder::NonPositivePrice.into()
    );
    assert_eq!(
        refuse(&mut h, &|o| o.order_type = OrderType::Market),
        InvalidOrder::MarketOrderWithPrice.into()
    );
    assert_eq!(
        refuse(&mut h, &|o| o.remaining_quantity = 50),
        InvalidOrder::NotNew.into()
    );
    assert_eq!(
        refuse(&mut h, &|o| o.status = OrderStatus::Filled),
        InvalidOrder::NotNew.into()
    );
    assert!(matches!(
        refuse(&mut h, &|o| o.instrument_id = InstrumentId("OTHER".into())),
        BookError::WrongInstrument { .. }
    ));
}

#[test]
fn sequence_must_increase_and_ids_must_be_unique() {
    let mut h = Harness::new();
    let resting = h.limit("A", Side::Buy, 10000, 10);
    let mut ctx = MatchContext::utc(h.now, IdScheme::Deterministic { seed: 1 });

    let mut replay = resting.order.clone();
    replay.status = OrderStatus::Open;
    assert_eq!(
        h.book.plan_submit(replay.clone(), &mut ctx),
        Err(BookError::DuplicateOrder(resting.order.id))
    );

    replay.id = Uuid::from_u128(999);
    assert_eq!(
        h.book.plan_submit(replay, &mut ctx),
        Err(BookError::SequenceNotIncreasing { last: 1, got: 1 })
    );
}

#[test]
fn planning_leaves_the_book_untouched_until_applied() {
    let mut h = Harness::new();
    let sell = h.limit("A", Side::Sell, 10000, 100);
    let buy = h.order("B", Side::Buy, Some(10000), 60);
    let mut ctx = MatchContext::utc(h.now, h.ids);

    let update = h.book.plan_submit(buy, &mut ctx).unwrap();
    assert_eq!(update.executions.len(), 1);
    assert_eq!(
        h.book.order(sell.order.id).unwrap().remaining_quantity,
        100,
        "a dropped plan is a rollback"
    );
    assert_eq!(h.book.last_sequence(), 1);

    h.book.apply(&update).unwrap();
    assert_eq!(h.book.order(sell.order.id).unwrap().remaining_quantity, 40);
    assert_eq!(h.book.last_sequence(), 2);
}

#[test]
fn an_update_applies_only_to_the_version_it_was_planned_against() {
    let mut h = Harness::new();
    let sell = h.limit("A", Side::Sell, 10000, 100);
    let first = h.book.plan_cancel(sell.order.id).unwrap();
    let second = h.book.plan_cancel(sell.order.id).unwrap();
    h.book.apply(&first).unwrap();
    assert!(matches!(
        h.book.apply(&second),
        Err(BookError::StaleUpdate { .. })
    ));
    assert!(
        matches!(h.book.apply(&first), Err(BookError::StaleUpdate { .. })),
        "an update applies once"
    );
}

#[test]
fn restored_book_keeps_price_time_priority() {
    let mut h = Harness::new();
    h.limit("A", Side::Sell, 10000, 100);
    h.limit("C", Side::Sell, 10000, 50);
    h.limit("D", Side::Sell, 9900, 70);
    h.limit("B", Side::Buy, 9900, 20);
    h.limit("E", Side::Buy, 9800, 10);

    // What storage would hold: the active limit orders in sequence order.
    let mut stored: Vec<Order> = [Side::Buy, Side::Sell]
        .into_iter()
        .flat_map(|side| {
            h.book
                .resting_orders(side)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect();
    stored.sort_by_key(|o| o.sequence);
    let mut restored = OrderBook::new(acme());
    for order in stored {
        restored.restore(order).unwrap();
    }

    for side in [Side::Buy, Side::Sell] {
        assert_eq!(restored.resting_orders(side), h.book.resting_orders(side));
    }
    assert_eq!(restored.last_sequence(), h.book.last_sequence());

    let mut ctx = MatchContext::utc(h.now, IdScheme::Deterministic { seed: 1 });
    let mut next = h.order("F", Side::Buy, Some(10000), 150);
    next.sequence = restored.last_sequence() + 1;
    let update = restored.submit(next.clone(), &mut ctx).unwrap();
    let expected = h.submit(next);
    let pairs = |u: &exchange_simulator::engine::BookUpdate| {
        u.executions
            .iter()
            .map(|e| (e.sell_order_id, e.price_ticks, e.quantity))
            .collect::<Vec<_>>()
    };
    assert_eq!(pairs(&update), pairs(&expected));
}

#[test]
fn restore_refuses_orders_that_cannot_rest() {
    let mut h = Harness::new();
    let mut book = OrderBook::new(acme());
    let mut market = h.order("A", Side::Buy, None, 10);
    assert!(
        book.restore(market.clone()).is_err(),
        "market orders never rest"
    );
    market.order_type = OrderType::Limit;
    market.limit_price_ticks = Some(100);
    market.status = OrderStatus::Filled;
    assert!(book.restore(market).is_err(), "inactive orders do not rest");

    let later = h.order("A", Side::Buy, Some(100), 10);
    let earlier = h.order("A", Side::Buy, Some(100), 10);
    book.restore(earlier).unwrap();
    assert!(matches!(
        book.restore(later),
        Err(BookError::SequenceNotIncreasing { .. })
    ));
}

/// Tiny deterministic generator (xorshift64*) so the randomized test needs no extra dependency and replays exactly.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn seeded_random_flow_preserves_book_invariants() {
    for seed in 1..=20_u64 {
        let mut rng = Rng(seed);
        let mut h = Harness::new();
        let mut filled: std::collections::HashMap<Uuid, u64> = Default::default();
        let mut submitted: std::collections::HashMap<Uuid, Order> = Default::default();

        for _ in 0..2_000 {
            if rng.below(10) == 0 && !h.book.is_empty() {
                let side = if rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                if let Some(victim) = h.book.resting_orders(side).first().map(|o| o.id) {
                    h.book.cancel(victim).unwrap();
                }
                continue;
            }
            let participant = ["A", "B", "C", "D"][rng.below(4) as usize];
            let side = if rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = if rng.below(5) == 0 {
                None
            } else {
                Some(9_950 + rng.below(100) as i64)
            };
            let order = h.order(participant, side, price, 1 + rng.below(50));
            submitted.insert(order.id, order.clone());
            let update = h.submit(order);

            let traded: u64 = update.executions.iter().map(|e| e.quantity).sum();
            assert_eq!(
                traded,
                update.order.filled_quantity(),
                "seed {seed}: taker fills add up"
            );
            for execution in &update.executions {
                *filled.entry(execution.buy_order_id).or_default() += execution.quantity;
                *filled.entry(execution.sell_order_id).or_default() += execution.quantity;
                assert_ne!(
                    execution.buyer_id, execution.seller_id,
                    "seed {seed}: no self-trades"
                );
            }
            if let (Some(bid), Some(ask)) = (h.book.best_bid(), h.book.best_ask()) {
                assert!(bid < ask, "seed {seed}: book is crossed ({bid} >= {ask})");
            }
        }

        for side in [Side::Buy, Side::Sell] {
            let mut previous: Option<(i64, u64)> = None;
            for order in h.book.resting_orders(side) {
                assert!(order.status.is_active() && order.remaining_quantity > 0);
                let original = &submitted[&order.id];
                assert_eq!(
                    order.quantity - order.remaining_quantity,
                    filled.get(&order.id).copied().unwrap_or(0),
                    "seed {seed}: resting quantity matches its executions"
                );
                assert_eq!(order.quantity, original.quantity);
                let key = (order.limit_price_ticks.unwrap(), order.sequence);
                if let Some((price, sequence)) = previous {
                    let better_or_same = match side {
                        Side::Buy => key.0 <= price,
                        Side::Sell => key.0 >= price,
                    };
                    assert!(better_or_same, "seed {seed}: price priority");
                    if key.0 == price {
                        assert!(key.1 > sequence, "seed {seed}: time priority");
                    }
                }
                previous = Some(key);
            }
        }
    }
}

#[test]
fn execution_ids_follow_from_the_taker_sequence_and_fill() {
    let mut h = Harness::new();
    let first = h.limit("A", Side::Sell, 10000, 10);
    let second = h.limit("C", Side::Sell, 10000, 10);
    let sweep = h.limit("B", Side::Buy, 10000, 20);
    let ids: Vec<_> = sweep.executions.iter().map(|e| e.id).collect();
    let expected = [
        h.ids.execution(sweep.order.sequence, 0),
        h.ids.execution(sweep.order.sequence, 1),
    ];
    assert_eq!(ids, expected);
    assert!(first.executions.is_empty() && second.executions.is_empty());
}

#[test]
fn a_disabled_instrument_rejects_orders_but_still_cancels() {
    let mut h = Harness::new();
    let resting = h.limit("A", Side::Sell, 10000, 10);
    h.book.set_enabled(false);
    assert!(!h.book.instrument().enabled);

    let buy = h.limit("B", Side::Buy, 10000, 10);
    assert_eq!(buy.rejection, Some(RejectReason::InstrumentDisabled));
    assert_eq!(
        (buy.order.status, buy.order.remaining_quantity),
        (OrderStatus::Rejected, 10)
    );
    assert!(buy.executions.is_empty() && buy.fills.is_empty());
    let market = h.market("B", Side::Buy, 5);
    assert_eq!(market.rejection, Some(RejectReason::InstrumentDisabled));
    assert_eq!(
        h.book.depth(Side::Sell),
        [level(10000, 10, 1)],
        "resting orders stay"
    );
    assert!(
        h.book.depth(Side::Buy).is_empty(),
        "a rejected limit order does not rest"
    );

    let other = h.limit("C", Side::Sell, 10100, 5);
    assert_eq!(other.rejection, Some(RejectReason::InstrumentDisabled));
    let cancelled = h.book.cancel(resting.order.id).unwrap();
    assert_eq!(
        cancelled.order.status,
        OrderStatus::Cancelled,
        "cancels work while disabled"
    );
    let again = h.limit("A", Side::Sell, 10000, 10);
    assert_eq!(again.rejection, Some(RejectReason::InstrumentDisabled));

    h.book.set_enabled(true);
    h.limit("A", Side::Sell, 10000, 10);
    assert_eq!(
        h.limit("B", Side::Buy, 10000, 10).executions.len(),
        1,
        "trading resumes"
    );
}

#[test]
fn switching_trading_makes_earlier_plans_stale() {
    let mut h = Harness::new();
    let resting = h.limit("A", Side::Sell, 10000, 10);
    let planned = h.book.plan_cancel(resting.order.id).unwrap();
    h.book.set_enabled(false);
    assert!(matches!(
        h.book.apply(&planned),
        Err(BookError::StaleUpdate { .. })
    ));
}
