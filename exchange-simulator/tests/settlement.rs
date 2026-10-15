//! The settlement state machine on its own: intake, T+N, outcome rules, delays, corrections, determinism.

use chrono::{DateTime, Duration, NaiveDate, Utc};
use exchange_simulator::domain::{InstrumentId, ParticipantId};
use exchange_simulator::settlement::{
    SettlementPlan, SettlementPolicy, SettlementSimulator, SettlementStatus, TradeFacts,
};
use uuid::Uuid;

/// Monday 2030-01-07 09:30 UTC.
fn monday() -> DateTime<Utc> {
    "2030-01-07T09:30:00Z".parse().unwrap()
}

fn date(text: &str) -> NaiveDate {
    text.parse().unwrap()
}

fn midnight(text: &str) -> DateTime<Utc> {
    format!("{text}T00:00:00Z").parse().unwrap()
}

fn trade(sequence: u64, at: DateTime<Utc>) -> TradeFacts {
    TradeFacts {
        sequence,
        execution_id: Uuid::from_u128(u128::from(sequence)),
        instrument_id: InstrumentId("ACME".into()),
        buyer_id: ParticipantId("B".into()),
        seller_id: ParticipantId("A".into()),
        quantity: 150,
        price_ticks: 10025,
        currency: "DKK".into(),
        currency_scale: 2,
        tick_size_minor: 1,
        executed_at: at,
    }
}

/// Plans and applies in one go.
fn step(
    sim: &mut SettlementSimulator,
    trades: &[TradeFacts],
    now: DateTime<Utc>,
) -> SettlementPlan {
    let plan = sim.plan(trades, now);
    sim.apply(&plan);
    plan
}

fn types(plan: &SettlementPlan) -> Vec<&str> {
    plan.transitions
        .iter()
        .map(|t| t.event.event_type)
        .collect()
}

fn policy(adjust: impl FnOnce(&mut SettlementPolicy)) -> SettlementPolicy {
    let mut policy = SettlementPolicy::default();
    adjust(&mut policy);
    policy
}

#[test]
fn a_trade_is_pending_until_t_plus_2_and_then_settles() {
    let mut sim = SettlementSimulator::new(SettlementPolicy::default(), vec![], 0);
    let intake = step(&mut sim, &[trade(3, monday())], monday());
    assert_eq!(types(&intake), ["SettlementPending"]);
    let pending = &intake.transitions[0];
    assert_eq!(pending.instruction.status, SettlementStatus::Pending);
    assert_eq!(
        pending.instruction.intended_settlement_date,
        date("2030-01-09")
    );
    assert_eq!(pending.instruction.due_at, Some(midnight("2030-01-09")));
    assert_eq!(pending.instruction.amount_minor, 1_503_750);
    let event = &pending.event;
    assert_eq!(
        (event.source, event.occurred_at),
        ("settlement-simulator", monday())
    );
    assert_eq!(event.aggregate_id, Uuid::from_u128(3).to_string());
    assert_eq!(event.payload["settlementAmount"], "15037.50");
    assert_eq!(event.payload["intendedSettlementDate"], "2030-01-09");
    assert_eq!(event.payload["tradeDate"], "2030-01-07");
    assert_eq!(sim.cursor(), 3);

    assert!(
        step(&mut sim, &[], midnight("2030-01-09") - Duration::seconds(1))
            .transitions
            .is_empty()
    );
    let settled = step(&mut sim, &[], midnight("2030-01-09"));
    assert_eq!(types(&settled), ["SettlementCompleted"]);
    let done = &settled.transitions[0];
    assert_eq!(
        (done.instruction.status, done.instruction.due_at),
        (SettlementStatus::Settled, None)
    );
    assert_eq!(
        done.event.occurred_at,
        midnight("2030-01-09"),
        "the scheduled instant, not when it ran"
    );
    assert_eq!(done.event.payload["settledAt"], "2030-01-09T00:00:00Z");
    assert_eq!(sim.open(), 0);
}

#[test]
fn trades_already_taken_in_are_skipped() {
    let mut sim = SettlementSimulator::new(SettlementPolicy::default(), vec![], 5);
    let plan = step(
        &mut sim,
        &[trade(4, monday()), trade(5, monday()), trade(6, monday())],
        monday(),
    );
    assert_eq!(plan.transitions.len(), 1);
    assert_eq!(plan.transitions[0].instruction.trade_sequence, 6);
}

#[test]
fn each_outcome_follows_its_probability() {
    type Case = (fn(&mut SettlementPolicy), &'static str, SettlementStatus);
    let cases: [Case; 4] = [
        (
            |p| p.reject_probability = 1.0,
            "SettlementRejected",
            SettlementStatus::Rejected,
        ),
        (
            |p| p.cancel_probability = 1.0,
            "SettlementCancelled",
            SettlementStatus::Cancelled,
        ),
        (
            |p| p.fail_probability = 1.0,
            "SettlementFailed",
            SettlementStatus::Failed,
        ),
        (|_| {}, "SettlementCompleted", SettlementStatus::Settled),
    ];
    for (adjust, event_type, status) in cases {
        let mut sim = SettlementSimulator::new(policy(adjust), vec![], 0);
        let plan = step(
            &mut sim,
            &[trade(1, monday())],
            monday() + Duration::days(10),
        );
        let last = plan.transitions.last().unwrap();
        assert_eq!(
            (last.event.event_type, last.instruction.status),
            (event_type, status)
        );
        assert_eq!(last.instruction.due_at, None, "{event_type} is final");
        assert_eq!(sim.open(), 0);
    }
}

#[test]
fn delays_move_the_date_by_business_days_and_are_capped() {
    let mut sim = SettlementSimulator::new(
        policy(|p| {
            p.delay_probability = 1.0;
            p.max_delays = 2;
            p.max_delay_days = 1;
        }),
        vec![],
        0,
    );
    step(&mut sim, &[trade(1, monday())], monday());
    let plan = step(&mut sim, &[], monday() + Duration::days(30));
    assert_eq!(
        types(&plan),
        [
            "SettlementDelayed",
            "SettlementDelayed",
            "SettlementCompleted"
        ],
        "forced to resolve"
    );
    let dates: Vec<_> = plan
        .transitions
        .iter()
        .map(|t| t.instruction.intended_settlement_date)
        .collect();
    assert_eq!(
        dates,
        [date("2030-01-10"), date("2030-01-11"), date("2030-01-11")]
    );
    assert_eq!(
        plan.transitions[0].event.payload["previousIntendedSettlementDate"],
        "2030-01-09"
    );
    assert_eq!(plan.transitions[0].event.payload["delayBusinessDays"], 1);
    assert_eq!(plan.transitions[2].instruction.attempts, 3);
    let stamps: Vec<_> = plan
        .transitions
        .iter()
        .map(|t| t.event.occurred_at)
        .collect();
    assert_eq!(
        stamps,
        [
            midnight("2030-01-09"),
            midnight("2030-01-10"),
            midnight("2030-01-11")
        ]
    );
}

#[test]
fn a_delay_over_the_weekend_lands_on_a_business_day() {
    let mut sim = SettlementSimulator::new(
        policy(|p| {
            p.delay_probability = 1.0;
            p.max_delays = 1;
            p.max_delay_days = 1;
        }),
        vec![],
        0,
    );
    let thursday: DateTime<Utc> = "2030-01-10T12:00:00Z".parse().unwrap();
    let intake = step(&mut sim, &[trade(1, thursday)], thursday);
    assert_eq!(
        intake.transitions[0].instruction.intended_settlement_date,
        date("2030-01-14"),
        "Thu + 2 = Mon"
    );
    let plan = step(&mut sim, &[], midnight("2030-01-20"));
    assert_eq!(
        plan.transitions[0].instruction.intended_settlement_date,
        date("2030-01-15")
    );
}

#[test]
fn a_settled_instruction_can_be_corrected_later() {
    let mut sim = SettlementSimulator::new(policy(|p| p.correct_probability = 1.0), vec![], 0);
    step(&mut sim, &[trade(1, monday())], monday());
    let settled = step(&mut sim, &[], midnight("2030-01-09"));
    assert_eq!(types(&settled), ["SettlementCompleted"]);
    assert_eq!(
        settled.transitions[0].instruction.due_at,
        Some(midnight("2030-01-10"))
    );
    assert_eq!(sim.open(), 1, "still open: a correction is scheduled");

    let corrected = step(&mut sim, &[], midnight("2030-01-10"));
    assert_eq!(types(&corrected), ["SettlementCorrected"]);
    let instruction = &corrected.transitions[0].instruction;
    assert_eq!(instruction.status, SettlementStatus::Corrected);
    assert_eq!(
        (instruction.amount_minor - 1_503_750).abs(),
        150,
        "one tick per unit"
    );
    assert_eq!(
        corrected.transitions[0].event.payload["previousSettlementAmount"],
        "15037.50"
    );
    assert_eq!(sim.open(), 0);
}

#[test]
fn t_plus_0_settles_at_the_trade_not_before_it() {
    let mut sim = SettlementSimulator::new(policy(|p| p.settlement_days = 0), vec![], 0);
    let plan = step(&mut sim, &[trade(1, monday())], monday());
    assert_eq!(types(&plan), ["SettlementPending", "SettlementCompleted"]);
    assert_eq!(plan.transitions[1].event.occurred_at, monday());
}

#[test]
fn an_amount_that_does_not_fit_is_rejected() {
    let mut sim = SettlementSimulator::new(SettlementPolicy::default(), vec![], 0);
    let huge = TradeFacts {
        price_ticks: i64::MAX,
        quantity: 10,
        ..trade(1, monday())
    };
    let plan = step(&mut sim, &[huge], monday());
    assert_eq!(types(&plan), ["SettlementRejected"]);
    assert_eq!(
        plan.transitions[0].event.payload["reason"],
        "AMOUNT_OUT_OF_RANGE"
    );
}

fn mixed() -> SettlementPolicy {
    policy(|p| {
        p.seed = 99;
        p.reject_probability = 0.05;
        p.cancel_probability = 0.05;
        p.fail_probability = 0.1;
        p.delay_probability = 0.2;
        p.correct_probability = 0.1;
    })
}

fn outcomes(plan_steps: &[SettlementPlan]) -> Vec<(u64, &'static str, NaiveDate, DateTime<Utc>)> {
    let mut all: Vec<_> = plan_steps
        .iter()
        .flat_map(|p| &p.transitions)
        .map(|t| {
            (
                t.instruction.trade_sequence,
                t.event.event_type,
                t.instruction.intended_settlement_date,
                t.event.occurred_at,
            )
        })
        .collect();
    all.sort();
    all
}

#[test]
fn outcomes_do_not_depend_on_how_time_is_stepped() {
    let trades: Vec<_> = (1..=200)
        .map(|i| trade(i, monday() + Duration::minutes(i as i64)))
        .collect();
    let end = monday() + Duration::days(40);

    let mut at_once = SettlementSimulator::new(mixed(), vec![], 0);
    let one = vec![step(&mut at_once, &trades, end)];

    let mut hourly = SettlementSimulator::new(mixed(), vec![], 0);
    let mut many = Vec::new();
    for (i, chunk) in trades.chunks(7).enumerate() {
        many.push(step(
            &mut hourly,
            chunk,
            monday() + Duration::hours(i as i64),
        ));
    }
    let mut t = monday();
    while t < end {
        t += Duration::hours(5);
        many.push(step(&mut hourly, &[], t));
    }
    assert_eq!(outcomes(&one), outcomes(&many));

    let mut other_seed = SettlementSimulator::new(
        SettlementPolicy {
            seed: 100,
            ..mixed()
        },
        vec![],
        0,
    );
    let different = vec![step(&mut other_seed, &trades, end)];
    assert_ne!(outcomes(&one), outcomes(&different), "the seed matters");
}

#[test]
fn a_restored_simulator_continues_identically() {
    let trades: Vec<_> = (1..=100).map(|i| trade(i, monday())).collect();
    let end = monday() + Duration::days(30);
    let mut straight = SettlementSimulator::new(mixed(), vec![], 0);
    let first = step(&mut straight, &trades, midnight("2030-01-09"));
    let rest = step(&mut straight, &[], end);

    // What storage holds after the first step: the latest state of every instruction still open.
    let mut latest = std::collections::HashMap::new();
    for t in &first.transitions {
        latest.insert(t.instruction.execution_id, t.instruction.clone());
    }
    let open: Vec<_> = latest
        .into_values()
        .filter(|i| i.due_at.is_some())
        .collect();
    let mut restored = SettlementSimulator::new(mixed(), open, first.cursor);
    let resumed = step(&mut restored, &[], end);
    assert_eq!(outcomes(&[rest]), outcomes(&[resumed]));
}

#[test]
fn outcome_rates_match_the_probabilities() {
    let policy = policy(|p| {
        p.seed = 7;
        p.fail_probability = 0.2;
        p.cancel_probability = 0.1;
    });
    let mut sim = SettlementSimulator::new(policy, vec![], 0);
    let trades: Vec<_> = (1..=5000).map(|i| trade(i, monday())).collect();
    let plan = step(&mut sim, &trades, monday() + Duration::days(5));
    let rate = |t: &str| types(&plan).iter().filter(|x| **x == t).count() as f64 / 5000.0;
    assert!(
        (rate("SettlementFailed") - 0.2).abs() < 0.02,
        "{}",
        rate("SettlementFailed")
    );
    assert!(
        (rate("SettlementCancelled") - 0.1).abs() < 0.015,
        "{}",
        rate("SettlementCancelled")
    );
    assert!((rate("SettlementCompleted") - 0.7).abs() < 0.02);
}

#[test]
fn planning_changes_nothing_until_applied_and_events_come_in_time_order() {
    let mut sim = SettlementSimulator::new(mixed(), vec![], 0);
    let trades: Vec<_> = (1..=50)
        .map(|i| trade(i, monday() + Duration::hours(i as i64)))
        .collect();
    let plan = sim.plan(&trades, monday() + Duration::days(20));
    assert_eq!(
        (sim.cursor(), sim.open()),
        (0, 0),
        "a dropped plan is a rollback"
    );
    sim.apply(&plan);
    assert_eq!(sim.cursor(), 50);
    let due_events: Vec<_> = plan
        .transitions
        .iter()
        .filter(|t| {
            t.event.event_type != "SettlementPending" && t.event.event_type != "SettlementRejected"
        })
        .map(|t| t.event.occurred_at)
        .collect();
    assert!(
        due_events.windows(2).all(|w| w[0] <= w[1]),
        "transitions run in global time order"
    );
}
