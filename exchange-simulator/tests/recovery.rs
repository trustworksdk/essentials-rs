//! The persistence coordinator: commit-then-apply, idempotent submission, registration rules and rebuilding the books
//! from SQLite after a restart.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use exchange_simulator::domain::{
    IdentifierPolicy, Instrument, InstrumentId, Order, OrderStatus, OrderType, Participant,
    ParticipantId, Side,
};
use exchange_simulator::engine::{
    BookUpdate, Exchange, ExchangeError, ExchangeOptions, SubmitOrder, SubmitOutcome, SystemClock,
    UuidV7Ids,
};
use exchange_simulator::persistence::{self, InstrumentFilter, Store};
use uuid::Uuid;

/// A database file in its own directory, removed on drop.
struct TempDb(PathBuf);

impl TempDb {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("exchange-sim-test-{}", Uuid::now_v7())))
    }

    fn path(&self) -> PathBuf {
        self.0.join("exchange.db")
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open(path: &Path, policy: IdentifierPolicy) -> Exchange {
    let conn = persistence::open(path, Duration::from_secs(5)).unwrap();
    Exchange::recover(
        Store::new(conn),
        ExchangeOptions {
            identifier_policy: policy,
            ids: Box::new(UuidV7Ids),
            clock: Box::new(SystemClock),
            simulation_id: None,
            delivery_signal: None,
        },
    )
    .unwrap()
}

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
    }
}

fn participant(id: &str) -> Participant {
    Participant {
        id: ParticipantId(id.into()),
        name: format!("Participant {id}"),
    }
}

/// A fresh exchange with ACME and participants A to D.
fn setup(db: &TempDb) -> Exchange {
    let mut exchange = open(&db.path(), IdentifierPolicy::Strict);
    exchange.register_instrument(acme()).unwrap();
    for id in ["A", "B", "C", "D"] {
        exchange.register_participant(participant(id)).unwrap();
    }
    exchange
}

fn order(participant: &str, side: Side, price: Option<i64>, quantity: u64) -> SubmitOrder {
    SubmitOrder {
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
    }
}

fn processed(outcome: Result<SubmitOutcome, ExchangeError>) -> BookUpdate {
    match outcome.unwrap() {
        SubmitOutcome::Processed(update) => update,
        SubmitOutcome::Duplicate(order) => panic!("unexpected duplicate {order:?}"),
    }
}

fn count(exchange: &Exchange, sql: &str) -> i64 {
    exchange
        .store()
        .connection()
        .query_row(sql, [], |row| row.get(0))
        .unwrap()
}

fn stored(exchange: &Exchange, id: Uuid) -> Order {
    exchange.store().order(id).unwrap().unwrap().order
}

#[test]
fn acceptance_trade_is_committed_and_the_book_survives_a_restart() {
    let db = TempDb::new();
    let mut exchange = setup(&db);
    let sell = processed(exchange.submit(order("A", Side::Sell, Some(10025), 200)));
    let buy = processed(exchange.submit(order("B", Side::Buy, Some(10025), 150)));
    assert_eq!(buy.executions.len(), 1);

    assert_eq!(
        stored(&exchange, sell.order.id),
        exchange
            .book(&acme().id)
            .unwrap()
            .order(sell.order.id)
            .unwrap()
            .clone()
    );
    let sell_row = stored(&exchange, sell.order.id);
    assert_eq!(
        (sell_row.remaining_quantity, sell_row.status),
        (50, OrderStatus::PartiallyFilled)
    );
    assert_eq!(stored(&exchange, buy.order.id).status, OrderStatus::Filled);
    let (quantity, price, buyer, seller): (i64, i64, String, String) = exchange
        .store()
        .connection()
        .query_row(
            "SELECT quantity, price_ticks, buyer_id, seller_id FROM executions",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        (quantity, price, buyer.as_str(), seller.as_str()),
        (150, 10025, "B", "A")
    );
    drop(exchange);

    let mut restarted = open(&db.path(), IdentifierPolicy::Strict);
    let book = restarted.book(&acme().id).unwrap();
    assert_eq!(book.len(), 1);
    assert_eq!(book.order(sell.order.id).unwrap().remaining_quantity, 50);
    assert_eq!(restarted.last_sequence(), 2);

    let next = processed(restarted.submit(order("C", Side::Buy, Some(10025), 50)));
    assert_eq!(
        next.order.sequence, 3,
        "sequencing continues after the restart"
    );
    assert_eq!(next.executions.len(), 1);
    assert_eq!(next.fills[0].status, OrderStatus::Filled);
    assert_eq!(
        count(&restarted, "SELECT COUNT(*) FROM executions"),
        2,
        "no execution is repeated"
    );
    assert!(restarted.book(&acme().id).unwrap().is_empty());
}

#[test]
fn a_failed_commit_changes_neither_memory_nor_storage() {
    let db = TempDb::new();
    let mut exchange = setup(&db);
    let sell = processed(exchange.submit(order("A", Side::Sell, Some(10000), 100)));
    exchange
        .store()
        .connection()
        .execute_batch("CREATE TEMP TRIGGER fail BEFORE INSERT ON executions BEGIN SELECT RAISE(ABORT, 'disk on fire'); END;")
        .unwrap();

    let error = exchange
        .submit(order("B", Side::Buy, Some(10000), 60))
        .unwrap_err();
    assert!(matches!(error, ExchangeError::Storage(_)), "{error}");
    let book = exchange.book(&acme().id).unwrap();
    assert_eq!(
        book.order(sell.order.id).unwrap().remaining_quantity,
        100,
        "the planned fill was not applied"
    );
    assert_eq!(book.len(), 1);
    assert_eq!(
        stored(&exchange, sell.order.id).remaining_quantity,
        100,
        "the maker update was rolled back"
    );
    assert_eq!(
        count(&exchange, "SELECT COUNT(*) FROM orders"),
        1,
        "the incoming order was rolled back"
    );
    assert_eq!(
        exchange.last_sequence(),
        1,
        "a failed command does not consume a sequence"
    );

    exchange
        .store()
        .connection()
        .execute_batch("DROP TRIGGER fail")
        .unwrap();
    let retry = processed(exchange.submit(order("B", Side::Buy, Some(10000), 60)));
    assert_eq!(retry.order.sequence, 2);
    assert_eq!(retry.executions[0].quantity, 60);
    assert_eq!(stored(&exchange, sell.order.id).remaining_quantity, 40);
}

#[test]
fn client_order_ids_make_submission_idempotent_across_restarts() {
    let db = TempDb::new();
    let mut exchange = setup(&db);
    let with_id = |participant: &str, quantity: u64| SubmitOrder {
        client_order_id: Some("cl-1".into()),
        ..order(participant, Side::Sell, Some(10000), quantity)
    };
    let first = processed(exchange.submit(with_id("A", 100)));
    assert_eq!(first.order.client_order_id.as_deref(), Some("cl-1"));
    processed(exchange.submit(order("B", Side::Buy, Some(10000), 30)));

    let SubmitOutcome::Duplicate(again) = exchange.submit(with_id("A", 100)).unwrap() else {
        panic!("not a duplicate")
    };
    assert_eq!(again.id, first.order.id);
    assert_eq!(
        again.remaining_quantity, 70,
        "the duplicate reports the order's current state"
    );
    assert!(matches!(
        exchange.submit(with_id("A", 999)),
        Err(ExchangeError::ClientOrderIdConflict { existing, .. }) if existing == first.order.id
    ));
    processed(exchange.submit(with_id("C", 100)));
    assert!(matches!(
        exchange.submit(SubmitOrder {
            client_order_id: Some(String::new()),
            ..order("A", Side::Buy, Some(1), 1)
        }),
        Err(ExchangeError::InvalidClientOrderId)
    ));
    assert_eq!(count(&exchange, "SELECT COUNT(*) FROM orders"), 3);
    drop(exchange);

    let mut restarted = open(&db.path(), IdentifierPolicy::Strict);
    assert!(
        matches!(restarted.submit(with_id("A", 100)), Ok(SubmitOutcome::Duplicate(o)) if o.id == first.order.id)
    );
    assert_eq!(count(&restarted, "SELECT COUNT(*) FROM orders"), 3);
}

#[test]
fn rejections_market_orders_and_cancels_are_stored_but_never_restored() {
    let db = TempDb::new();
    let mut exchange = setup(&db);
    let resting = processed(exchange.submit(order("A", Side::Sell, Some(10000), 10)));
    let cancelled = processed(exchange.submit(order("A", Side::Sell, Some(10100), 10)));
    let self_trade = processed(exchange.submit(order("A", Side::Buy, Some(10000), 5)));
    let market = processed(exchange.submit(order("B", Side::Sell, None, 5)));
    exchange.cancel(cancelled.order.id).unwrap();

    assert_eq!(
        stored(&exchange, self_trade.order.id).status,
        OrderStatus::Rejected
    );
    assert_eq!(stored(&exchange, self_trade.order.id).sequence, 3);
    assert_eq!(
        stored(&exchange, market.order.id).status,
        OrderStatus::Cancelled,
        "no bids: remainder cancelled"
    );
    let cancel_row = stored(&exchange, cancelled.order.id);
    assert_eq!(
        (cancel_row.status, cancel_row.remaining_quantity),
        (OrderStatus::Cancelled, 10)
    );
    assert!(matches!(
        exchange.cancel(cancelled.order.id),
        Err(ExchangeError::OrderNotActive {
            status: OrderStatus::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        exchange.cancel(Uuid::nil()),
        Err(ExchangeError::UnknownOrder(_))
    ));

    let error = exchange
        .submit(order("A", Side::Buy, Some(0), 5))
        .unwrap_err();
    assert!(matches!(error, ExchangeError::Book(_)), "{error}");
    assert_eq!(
        exchange.last_sequence(),
        4,
        "malformed orders get no sequence"
    );
    drop(exchange);

    let restarted = open(&db.path(), IdentifierPolicy::Strict);
    let book = restarted.book(&acme().id).unwrap();
    assert_eq!(book.len(), 1);
    assert!(book.order(resting.order.id).is_some());
    assert_eq!(
        restarted.last_sequence(),
        4,
        "the rejected order's sequence is not reused"
    );
}

#[test]
fn commands_for_unknown_instruments_or_participants_are_refused() {
    let db = TempDb::new();
    let mut exchange = setup(&db);
    assert!(matches!(
        exchange.submit(order("Z", Side::Buy, Some(1), 1)),
        Err(ExchangeError::UnknownParticipant(_))
    ));
    let other = SubmitOrder {
        instrument_id: InstrumentId("NOPE".into()),
        ..order("A", Side::Buy, Some(1), 1)
    };
    assert!(matches!(
        exchange.submit(other),
        Err(ExchangeError::UnknownInstrument(_))
    ));
    assert!(matches!(
        exchange.register_participant(participant("A")),
        Err(ExchangeError::DuplicateParticipant(_))
    ));
    assert!(matches!(
        exchange.register_participant(Participant {
            id: ParticipantId(" ".into()),
            name: "x".into()
        }),
        Err(ExchangeError::InvalidParticipant)
    ));
}

#[test]
fn instrument_registration_follows_the_identifier_rules() {
    let db = TempDb::new();
    let mut exchange = setup(&db);
    let listing =
        |id: &str, mic: Option<&str>, isin: Option<&str>, cusip: Option<&str>| Instrument {
            id: InstrumentId(id.into()),
            mic: mic.map(Into::into),
            isin: isin.map(Into::into),
            cusip: cusip.map(Into::into),
            ..acme()
        };

    assert!(matches!(
        exchange.register_instrument(acme()),
        Err(ExchangeError::DuplicateInstrument(_))
    ));
    assert!(matches!(
        exchange.register_instrument(listing("ACME-2", Some("XCSE"), None, None)),
        Err(ExchangeError::DuplicateListing { .. })
    ));
    exchange
        .register_instrument(listing(
            "ACME-XSTO",
            Some("XSTO"),
            Some("US0378331005"),
            None,
        ))
        .unwrap();
    exchange
        .register_instrument(listing(
            "ACME-XNAS",
            Some("XNAS"),
            Some("US0378331005"),
            Some("037833100"),
        ))
        .unwrap();
    exchange
        .register_instrument(listing("ACME-NOMIC-1", None, None, None))
        .unwrap();
    exchange
        .register_instrument(listing("ACME-NOMIC-2", None, None, None))
        .unwrap();
    assert!(matches!(
        exchange.register_instrument(listing("BAD", Some("XOSL"), Some("US0378331006"), None)),
        Err(ExchangeError::Instrument(_))
    ));
    assert!(matches!(
        exchange.register_instrument(listing("BAD", Some("XOSL"), None, Some("037833101"))),
        Err(ExchangeError::Instrument(_))
    ));

    let ids = |filter: InstrumentFilter| -> Vec<String> {
        exchange
            .store()
            .instruments(&filter)
            .unwrap()
            .into_iter()
            .map(|i| i.id.0)
            .collect()
    };
    assert_eq!(
        ids(InstrumentFilter {
            isin: Some("US0378331005".into()),
            ..Default::default()
        }),
        ["ACME-XNAS", "ACME-XSTO"]
    );
    assert_eq!(
        ids(InstrumentFilter {
            cusip: Some("037833100".into()),
            ..Default::default()
        }),
        ["ACME-XNAS"]
    );
    assert_eq!(
        ids(InstrumentFilter {
            symbol: Some("ACME".into()),
            mic: Some("XSTO".into()),
            ..Default::default()
        }),
        ["ACME-XSTO"]
    );
    assert_eq!(
        ids(InstrumentFilter {
            symbol: Some("ACME".into()),
            ..Default::default()
        })
        .len(),
        5,
        "symbol alone is ambiguous"
    );
    drop(exchange);

    let mut restarted = open(&db.path(), IdentifierPolicy::Permissive);
    assert!(
        matches!(
            restarted.register_instrument(listing("ACME-3", Some("XSTO"), None, None)),
            Err(ExchangeError::DuplicateListing { .. })
        ),
        "listings are known after a restart"
    );
    restarted
        .register_instrument(listing("FIXTURE", Some("XOSL"), Some("US0378331006"), None))
        .expect("permissive policy accepts a bad check digit");
    let stored = restarted
        .store()
        .instruments(&InstrumentFilter {
            mic: Some("XNAS".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        stored,
        [listing(
            "ACME-XNAS",
            Some("XNAS"),
            Some("US0378331005"),
            Some("037833100")
        )]
    );
}

/// Deterministic generator (xorshift64*), as in the matching tests.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

enum Command {
    Submit(SubmitOrder),
    /// Cancel the n-th resting order (in priority order) of a side, if there is one.
    Cancel(Side, u64),
}

fn commands(seed: u64, n: usize) -> Vec<Command> {
    let mut rng = Rng(seed);
    (0..n)
        .map(|_| {
            let side = if rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            if rng.below(8) == 0 {
                return Command::Cancel(side, rng.below(4));
            }
            let participant = ["A", "B", "C", "D"][rng.below(4) as usize];
            let price = if rng.below(6) == 0 {
                None
            } else {
                Some(9_950 + rng.below(100) as i64)
            };
            Command::Submit(order(participant, side, price, 1 + rng.below(40)))
        })
        .collect()
}

/// Runs the commands and describes every outcome by order sequence, which is stable across runs, unlike UUIDs.
fn run(
    exchange: &mut Exchange,
    command: &Command,
    sequences: &mut HashMap<Uuid, u64>,
) -> Option<String> {
    let book = exchange.book(&acme().id).unwrap();
    let update = match command {
        Command::Submit(submit) => processed(exchange.submit(submit.clone())),
        Command::Cancel(side, nth) => {
            let target = book
                .resting_orders(*side)
                .get(*nth as usize)
                .map(|o| o.id)?;
            exchange.cancel(target).unwrap()
        }
    };
    sequences.insert(update.order.id, update.order.sequence);
    let fills: Vec<_> = update
        .executions
        .iter()
        .map(|e| {
            (
                sequences[&e.buy_order_id],
                sequences[&e.sell_order_id],
                e.price_ticks,
                e.quantity,
            )
        })
        .collect();
    Some(format!(
        "{} {:?} {} {:?}",
        update.order.sequence, update.order.status, update.order.remaining_quantity, fills
    ))
}

fn book_state(exchange: &Exchange, sequences: &HashMap<Uuid, u64>) -> Vec<(u64, u64)> {
    let book = exchange.book(&acme().id).unwrap();
    [Side::Buy, Side::Sell]
        .into_iter()
        .flat_map(|side| {
            book.resting_orders(side)
                .into_iter()
                .map(|o| (sequences[&o.id], o.remaining_quantity))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn restarting_at_any_point_gives_the_same_trades_as_running_straight_through() {
    for seed in [7, 42, 1234] {
        let script = commands(seed, 600);

        let straight_db = TempDb::new();
        let mut straight = setup(&straight_db);
        let mut straight_sequences = HashMap::new();
        let expected: Vec<_> = script
            .iter()
            .filter_map(|c| run(&mut straight, c, &mut straight_sequences))
            .collect();

        let restarted_db = TempDb::new();
        let mut restarted = setup(&restarted_db);
        let mut sequences = HashMap::new();
        let mut actual = Vec::new();
        for (i, command) in script.iter().enumerate() {
            if i % 97 == 0 {
                drop(restarted);
                restarted = open(&restarted_db.path(), IdentifierPolicy::Strict);
            }
            actual.extend(run(&mut restarted, command, &mut sequences));
        }

        assert_eq!(actual, expected, "seed {seed}");
        assert_eq!(
            book_state(&restarted, &sequences),
            book_state(&straight, &straight_sequences),
            "seed {seed}"
        );
        let executions = |e: &Exchange| count(e, "SELECT COUNT(*) FROM executions");
        assert_eq!(executions(&restarted), executions(&straight), "seed {seed}");
        assert!(
            executions(&straight) > 100,
            "seed {seed}: the script trades"
        );
    }
}

#[test]
fn events_and_webhook_deliveries_commit_or_roll_back_with_the_trade() {
    let db = TempDb::new();
    let conn = persistence::open(db.path(), Duration::from_secs(5)).unwrap();
    let mut exchange = Exchange::recover(
        Store::new(conn),
        ExchangeOptions {
            identifier_policy: IdentifierPolicy::Strict,
            ids: Box::new(UuidV7Ids),
            clock: Box::new(SystemClock),
            simulation_id: Some("sim-7".into()),
            delivery_signal: None,
        },
    )
    .unwrap();
    exchange.register_instrument(acme()).unwrap();
    for id in ["A", "B"] {
        exchange.register_participant(participant(id)).unwrap();
    }
    let trades = exchange
        .register_webhook(
            "http://localhost:1/trades".into(),
            vec!["TradeExecuted".into()],
            None,
        )
        .unwrap();
    let all = exchange
        .register_webhook(
            "http://localhost:1/all".into(),
            [
                "OrderAccepted",
                "TradeExecuted",
                "OrderFilled",
                "OrderPartiallyFilled",
            ]
            .map(String::from)
            .to_vec(),
            None,
        )
        .unwrap();

    processed(exchange.submit(order("A", Side::Sell, Some(10025), 200)));
    exchange
        .store()
        .connection()
        .execute_batch(
            "CREATE TEMP TRIGGER fail BEFORE INSERT ON webhook_deliveries
             WHEN (SELECT event_type FROM events WHERE sequence = NEW.event_sequence) = 'TradeExecuted'
             BEGIN SELECT RAISE(ABORT, 'outbox full'); END;",
        )
        .unwrap();
    assert!(matches!(
        exchange.submit(order("B", Side::Buy, Some(10025), 150)),
        Err(ExchangeError::Storage(_))
    ));
    assert_eq!(count(&exchange, "SELECT COUNT(*) FROM executions"), 0);
    assert_eq!(count(&exchange, "SELECT COUNT(*) FROM orders"), 1);
    assert_eq!(
        count(&exchange, "SELECT COUNT(*) FROM events"),
        1,
        "only the first order's OrderAccepted"
    );
    assert_eq!(
        count(&exchange, "SELECT COUNT(*) FROM webhook_deliveries"),
        1
    );

    exchange
        .store()
        .connection()
        .execute_batch("DROP TRIGGER fail")
        .unwrap();
    processed(exchange.submit(order("B", Side::Buy, Some(10025), 150)));
    let events = exchange.store().events(0, 100, None).unwrap();
    let types: Vec<_> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(
        types,
        [
            "OrderAccepted",
            "OrderAccepted",
            "TradeExecuted",
            "OrderPartiallyFilled",
            "OrderFilled"
        ]
    );
    assert!(
        events
            .iter()
            .all(|e| e.simulation_id.as_deref() == Some("sim-7") && e.source == "exchange")
    );
    assert!(events.windows(2).all(|w| w[0].sequence < w[1].sequence));
    let deliveries = |subscription: Uuid| {
        count(
            &exchange,
            &format!(
                "SELECT COUNT(*) FROM webhook_deliveries WHERE subscription_id = '{subscription}' AND status = 'PENDING'"
            ),
        )
    };
    assert_eq!(deliveries(trades.id), 1);
    assert_eq!(deliveries(all.id), 5);

    assert!(matches!(
        exchange.register_webhook("ftp://x".into(), vec!["TradeExecuted".into()], None),
        Err(ExchangeError::InvalidWebhook(_))
    ));
    assert!(matches!(
        exchange.register_webhook("http://x".into(), vec![], None),
        Err(ExchangeError::InvalidWebhook(_))
    ));
    assert!(matches!(
        exchange.register_webhook("http://x".into(), vec!["Trade".into()], None),
        Err(ExchangeError::InvalidWebhook(_))
    ));
    assert!(matches!(
        exchange.register_webhook(
            "http://x".into(),
            vec!["TradeExecuted".into()],
            Some("too-short".into())
        ),
        Err(ExchangeError::InvalidWebhook(_))
    ));
}

#[test]
fn a_delivery_claim_is_a_lease_and_late_results_do_not_overwrite() {
    use chrono::{TimeZone, Utc};
    use exchange_simulator::webhooks::{DeliveryOutcome, DeliveryResult};

    let db = TempDb::new();
    let mut exchange = setup(&db);
    exchange
        .register_webhook(
            "http://localhost:1/hook".into(),
            vec!["OrderAccepted".into()],
            None,
        )
        .unwrap();
    processed(exchange.submit(order("A", Side::Sell, Some(10025), 200)));
    drop(exchange);

    let mut store = Store::new(persistence::open(db.path(), Duration::from_secs(5)).unwrap());
    let at = |second: u32| Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, second).unwrap();
    let first = store.claim_deliveries(at(0), at(10), 10).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(
        (first[0].attempts, first[0].event.event_type.as_str()),
        (0, "OrderAccepted")
    );
    assert!(
        store
            .claim_deliveries(at(5), at(15), 10)
            .unwrap()
            .is_empty(),
        "leased"
    );

    let second = store.claim_deliveries(at(11), at(20), 10).unwrap();
    assert_eq!(second.len(), 1, "an expired lease is claimable again");
    assert_eq!(second[0].event, first[0].event);

    let delivered = DeliveryResult {
        delivery_id: second[0].delivery_id,
        outcome: DeliveryOutcome::Delivered,
    };
    store.record_deliveries(&[delivered], at(12)).unwrap();
    let late = DeliveryResult {
        delivery_id: first[0].delivery_id,
        outcome: DeliveryOutcome::Retry {
            at: at(30),
            error: "timeout".into(),
        },
    };
    store.record_deliveries(&[late], at(13)).unwrap();
    let (status, attempts): (String, i64) = store
        .connection()
        .query_row("SELECT status, attempts FROM webhook_deliveries", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(
        (status.as_str(), attempts),
        ("DELIVERED", 1),
        "the late result for the old claim was ignored"
    );
    assert!(
        store
            .claim_deliveries(at(50), at(55), 10)
            .unwrap()
            .is_empty()
    );
}
