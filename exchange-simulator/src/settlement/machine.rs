use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{Instruction, SettlementPolicy, SettlementStatus, TradeFacts, draw, format_amount};
use crate::events::{self, NewEvent};

// Draw purposes: each decision gets its own independent stream.
const REJECT: u64 = 1;
const OUTCOME: u64 = 2;
const DELAY_DAYS: u64 = 3;
const CORRECT: u64 = 4;
const CORRECT_SIGN: u64 = 5;

/// One instruction's move to a new state, and the event that announces it.
#[derive(Clone, Debug, PartialEq)]
pub struct Transition {
    pub instruction: Instruction,
    pub event: NewEvent,
}

/// A proposed simulator step: transitions in order, and the trade cursor after it. Planning leaves the simulator
/// unchanged; [`SettlementSimulator::apply`] makes the plan current once it is committed.
#[derive(Clone, Debug, PartialEq)]
pub struct SettlementPlan {
    base_version: u64,
    pub transitions: Vec<Transition>,
    /// The last `TradeExecuted` sequence taken in, after this step.
    pub cursor: u64,
}

/// The settlement state machine: open instructions in memory, ordered by when their next transition is due. It does
/// no I/O; the caller persists a plan before applying it, as with the order book.
#[derive(Clone, Debug)]
pub struct SettlementSimulator {
    policy: SettlementPolicy,
    open: HashMap<Uuid, Instruction>,
    /// (due at, trade sequence, execution ID) of every open instruction: the global processing order.
    queue: BTreeSet<(DateTime<Utc>, u64, Uuid)>,
    cursor: u64,
    version: u64,
}

impl SettlementSimulator {
    /// A simulator resuming from stored open instructions (those with a `due_at`) and its trade cursor.
    pub fn new(policy: SettlementPolicy, open: Vec<Instruction>, cursor: u64) -> Self {
        let mut simulator = Self {
            policy,
            open: HashMap::new(),
            queue: BTreeSet::new(),
            cursor,
            version: 0,
        };
        for instruction in open {
            simulator.reconcile(instruction);
        }
        simulator
    }

    pub fn policy(&self) -> &SettlementPolicy {
        &self.policy
    }

    /// The last `TradeExecuted` sequence taken in.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Instructions with a transition still to come.
    pub fn open(&self) -> usize {
        self.open.len()
    }

    /// When the next transition is due, if any.
    pub fn next_due(&self) -> Option<DateTime<Utc>> {
        self.queue.first().map(|(due, _, _)| *due)
    }

    /// Plans taking in `trades` (those after the cursor, in sequence order) and then every transition due at or
    /// before `now`, in due order (ties by trade sequence). An instruction can move several times in one step, e.g.
    /// delayed and then settled when the clock jumps ahead.
    pub fn plan(&self, trades: &[TradeFacts], now: DateTime<Utc>) -> SettlementPlan {
        let mut transitions = Vec::new();
        let mut working: HashMap<Uuid, Instruction> = HashMap::new();
        let mut queue = self.queue.clone();
        let mut cursor = self.cursor;

        for trade in trades.iter().filter(|t| t.sequence > self.cursor) {
            let transition = self.intake(trade);
            if let Some(due) = transition.instruction.due_at {
                queue.insert((due, trade.sequence, trade.execution_id));
            }
            working.insert(trade.execution_id, transition.instruction.clone());
            transitions.push(transition);
            cursor = cursor.max(trade.sequence);
        }

        while let Some(&(due, sequence, id)) = queue.first() {
            if due > now {
                break;
            }
            queue.pop_first();
            let current = working
                .get(&id)
                .or_else(|| self.open.get(&id))
                .expect("queued instructions are open")
                .clone();
            let transition = self.evaluate(&current);
            if let Some(next) = transition.instruction.due_at {
                queue.insert((next, sequence, id));
            }
            working.insert(id, transition.instruction.clone());
            transitions.push(transition);
        }
        SettlementPlan {
            base_version: self.version,
            transitions,
            cursor,
        }
    }

    /// Makes a committed plan current. Panics if the simulator changed since the plan was made: the exchange thread
    /// serializes planning and applying, so that would be a bug.
    pub fn apply(&mut self, plan: &SettlementPlan) {
        assert_eq!(
            plan.base_version, self.version,
            "settlement plan applied to a changed simulator"
        );
        for transition in &plan.transitions {
            self.reconcile(transition.instruction.clone());
        }
        self.cursor = plan.cursor;
        self.version += 1;
    }

    /// Plans an instruction's answer to its trade being corrected or busted (SR-36), from `stored`, its state in
    /// storage: a bust cancels it (`SettlementCancelled`, reason `TRADE_BUSTED`), a correction takes the corrected
    /// quantity and price and recomputes the amount (`SettlementCorrected`, reason `TRADE_CORRECTED`). A correction
    /// keeps the instruction's status and due transition, except that a settled one becomes `CORRECTED`. An
    /// instruction that was rejected, failed or cancelled has nothing to answer: `None`.
    pub fn adjust(
        &self,
        stored: &Instruction,
        corrected: Option<(u64, i64)>,
        at: DateTime<Utc>,
    ) -> Option<SettlementPlan> {
        let current = self.open.get(&stored.execution_id).unwrap_or(stored);
        if matches!(
            current.status,
            SettlementStatus::Rejected | SettlementStatus::Failed | SettlementStatus::Cancelled
        ) {
            return None;
        }
        let mut next = current.clone();
        next.updated_at = at;
        let (event_type, extra) = match corrected {
            None => {
                next.status = SettlementStatus::Cancelled;
                next.due_at = None;
                (
                    events::SETTLEMENT_CANCELLED,
                    json!({ "reason": "TRADE_BUSTED" }),
                )
            }
            Some((quantity, price_ticks)) => {
                let amount = i128::from(price_ticks)
                    * i128::from(current.tick_size_minor)
                    * i128::from(quantity);
                next.quantity = quantity;
                next.price_ticks = price_ticks;
                next.amount_minor = i64::try_from(amount).unwrap_or(i64::MAX);
                if current.status == SettlementStatus::Settled {
                    next.status = SettlementStatus::Corrected;
                    next.due_at = None;
                }
                (
                    events::SETTLEMENT_CORRECTED,
                    json!({
                        "reason": "TRADE_CORRECTED",
                        "previousSettlementAmount": format_amount(current.amount_minor, current.currency_scale),
                        "previousAmountMinor": current.amount_minor,
                        "previousQuantity": current.quantity,
                        "previousPriceTicks": current.price_ticks,
                    }),
                )
            }
        };
        let event = settlement_event(event_type, &next, at, extra);
        Some(SettlementPlan {
            base_version: self.version,
            transitions: vec![Transition {
                instruction: next,
                event,
            }],
            cursor: self.cursor,
        })
    }

    fn reconcile(&mut self, instruction: Instruction) {
        let id = instruction.execution_id;
        if let Some(old) = self.open.remove(&id)
            && let Some(due) = old.due_at
        {
            self.queue.remove(&(due, old.trade_sequence, id));
        }
        if let Some(due) = instruction.due_at {
            self.queue.insert((due, instruction.trade_sequence, id));
            self.open.insert(id, instruction);
        }
    }

    fn intake(&self, trade: &TradeFacts) -> Transition {
        let policy = &self.policy;
        // The trade's own dates (SR-29), as every other event about it carries them. A trade from before executions
        // carried dates is dated as it was then: on the clearing calendar when clearing runs, else by UTC date.
        let settlement_days = policy.settlement_days_for(trade.mic.as_deref());
        let (trade_date, isd) = match trade.dates {
            Some(dates) => (dates.trade_date, dates.intended_settlement_date),
            None => {
                let trade_date = policy
                    .clearing
                    .as_ref()
                    .map_or(trade.executed_at.date_naive(), |calendar| {
                        calendar.business_date(trade.executed_at)
                    });
                (
                    trade_date,
                    policy
                        .business_days
                        .add_business_days(trade_date, settlement_days),
                )
            }
        };
        let amount = i128::from(trade.price_ticks)
            * i128::from(trade.tick_size_minor)
            * i128::from(trade.quantity);
        let mut instruction = Instruction {
            execution_id: trade.execution_id,
            trade_sequence: trade.sequence,
            instrument_id: trade.instrument_id.clone(),
            buyer_id: trade.buyer_id.clone(),
            seller_id: trade.seller_id.clone(),
            quantity: trade.quantity,
            price_ticks: trade.price_ticks,
            currency: trade.currency.clone(),
            currency_scale: trade.currency_scale,
            tick_size_minor: trade.tick_size_minor,
            amount_minor: i64::try_from(amount).unwrap_or(0),
            trade_date,
            intended_settlement_date: isd,
            status: SettlementStatus::Pending,
            attempts: 0,
            delays: 0,
            // T+0 settles at the trade, not at the midnight before it.
            due_at: Some(start_of(isd).max(trade.executed_at)),
            settled_at: None,
            updated_at: trade.executed_at,
            clearing: policy.clearing.as_ref().map(|calendar| {
                crate::clearing::refs(
                    calendar,
                    trade.executed_at,
                    &trade.instrument_id,
                    &trade.buyer_id,
                    &trade.seller_id,
                    isd,
                )
            }),
        };
        let reason = if i64::try_from(amount).is_err() {
            Some("AMOUNT_OUT_OF_RANGE")
        } else if draw(policy.seed, trade.sequence, 0, REJECT) < policy.reject_probability {
            Some("SIMULATED_REJECTION")
        } else {
            None
        };
        let (event_type, extra) = match reason {
            Some(reason) => {
                instruction.status = SettlementStatus::Rejected;
                instruction.due_at = None;
                (events::SETTLEMENT_REJECTED, json!({ "reason": reason }))
            }
            None => (
                events::SETTLEMENT_PENDING,
                json!({ "settlementDays": settlement_days }),
            ),
        };
        let event = settlement_event(event_type, &instruction, trade.executed_at, extra);
        Transition { instruction, event }
    }

    fn evaluate(&self, current: &Instruction) -> Transition {
        let policy = &self.policy;
        let at = current
            .due_at
            .expect("only open instructions are evaluated");
        let mut next = current.clone();
        next.updated_at = at;
        let seq = current.trade_sequence;
        let (event_type, extra) = match current.status {
            SettlementStatus::Pending | SettlementStatus::Delayed => {
                next.attempts += 1;
                let u = draw(policy.seed, seq, next.attempts, OUTCOME);
                let cancel = policy.cancel_probability;
                let fail = cancel + policy.fail_probability;
                let delay = fail + policy.delay_probability;
                if u < cancel {
                    next.status = SettlementStatus::Cancelled;
                    next.due_at = None;
                    (
                        events::SETTLEMENT_CANCELLED,
                        json!({ "reason": "SIMULATED_CANCELLATION" }),
                    )
                } else if u < fail {
                    next.status = SettlementStatus::Failed;
                    next.due_at = None;
                    (
                        events::SETTLEMENT_FAILED,
                        json!({ "reason": "SIMULATED_FAILURE" }),
                    )
                } else if u < delay && current.delays < policy.max_delays {
                    let days = 1
                        + (draw(policy.seed, seq, next.attempts, DELAY_DAYS)
                            * f64::from(policy.max_delay_days)) as u32;
                    let days = days.min(policy.max_delay_days);
                    let previous = current.intended_settlement_date;
                    next.intended_settlement_date =
                        policy.business_days.add_business_days(previous, days);
                    next.status = SettlementStatus::Delayed;
                    next.delays += 1;
                    next.due_at = Some(start_of(next.intended_settlement_date));
                    (
                        events::SETTLEMENT_DELAYED,
                        json!({ "previousIntendedSettlementDate": previous, "delayBusinessDays": days }),
                    )
                } else {
                    next.status = SettlementStatus::Settled;
                    next.settled_at = Some(at);
                    next.due_at = (draw(policy.seed, seq, next.attempts, CORRECT)
                        < policy.correct_probability)
                        .then(|| {
                            start_of(policy.business_days.add_business_days(
                                current.intended_settlement_date,
                                policy.correction_days,
                            ))
                        })
                        .filter(|correction| *correction > at);
                    (events::SETTLEMENT_COMPLETED, json!({ "settledAt": at }))
                }
            }
            SettlementStatus::Settled => {
                // A simulated price correction of one tick, up or down.
                let delta = i64::try_from(
                    i128::from(current.tick_size_minor) * i128::from(current.quantity),
                )
                .unwrap_or(0);
                let up = draw(policy.seed, seq, current.attempts, CORRECT_SIGN) < 0.5;
                let corrected = if up {
                    current.amount_minor.saturating_add(delta)
                } else {
                    current.amount_minor.saturating_sub(delta)
                };
                next.amount_minor = corrected;
                next.status = SettlementStatus::Corrected;
                next.due_at = None;
                (
                    events::SETTLEMENT_CORRECTED,
                    json!({
                        "reason": "SIMULATED_CORRECTION",
                        "previousSettlementAmount": format_amount(current.amount_minor, current.currency_scale),
                        "previousAmountMinor": current.amount_minor,
                    }),
                )
            }
            final_status => unreachable!("{final_status} instructions have no due transition"),
        };
        let event = settlement_event(event_type, &next, at, extra);
        Transition {
            instruction: next,
            event,
        }
    }
}

/// 00:00 UTC on `date`.
fn start_of(date: NaiveDate) -> DateTime<Utc> {
    date.and_hms_opt(0, 0, 0)
        .expect("midnight exists")
        .and_utc()
}

/// A settlement lifecycle event (unstamped: the writer assigns sequence and ID): the instruction's state after the transition plus `extra` fields. Aggregate and
/// correlation ID are the execution ID.
fn settlement_event(
    event_type: &'static str,
    instruction: &Instruction,
    at: DateTime<Utc>,
    extra: Value,
) -> NewEvent {
    let mut payload = json!({
        "executionId": instruction.execution_id,
        "tradeSequence": instruction.trade_sequence,
        "instrumentId": instruction.instrument_id,
        "buyerId": instruction.buyer_id,
        "sellerId": instruction.seller_id,
        "quantity": instruction.quantity,
        "priceTicks": instruction.price_ticks,
        "currency": instruction.currency,
        "settlementAmount": format_amount(instruction.amount_minor, instruction.currency_scale),
        "amountMinor": instruction.amount_minor,
        "tradeDate": instruction.trade_date,
        "intendedSettlementDate": instruction.intended_settlement_date,
        "status": instruction.status,
        "attempt": instruction.attempts,
    });
    if let Some(refs) = &instruction.clearing {
        payload["clearing"] = serde_json::to_value(refs).expect("references serialize");
    }
    if let (Value::Object(payload), Value::Object(extra)) = (&mut payload, extra) {
        payload.extend(extra);
    }
    NewEvent {
        event_id: Uuid::nil(),
        sequence: 0,
        event_type,
        source: events::SETTLEMENT_SOURCE,
        aggregate_id: instruction.execution_id.to_string(),
        correlation_id: Some(instruction.execution_id.to_string()),
        occurred_at: at,
        payload,
    }
}
