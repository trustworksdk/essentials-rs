//! End-of-day clearing statements (plan item 5.3). The clearinghouse is an outside party (D8): it keeps no positions
//! or cash ledger. At each business day's cutoff it issues one statement per participant that traded since the
//! previous cutoff: the day's cleared trades, and the net obligations they create per instrument and intended
//! settlement date, computed from those trades alone.
//!
//! - **Business date:** a trade belongs to the first business day whose cutoff (`cutoff` in `time_zone`) is after it,
//!   so trades after the cutoff, or on a weekend, appear on the next business day's statement. That date is the
//!   trade date: T+N counts from it, here and in the settlement simulator, so nothing settles before the statement
//!   that clears it is issued (T+0 aside).
//! - **Identifiers** are natural keys, so a settlement instruction can name its statement and obligation at intake,
//!   before the statement exists: `CS:<businessDate>:<participant>` and `<statementId>:<instrument>:<settlementDate>`.
//! - **Breaks** are opt-in and seeded, like the webhook faults: a trade left out, a quantity or price changed, a trade
//!   that does not exist, or a statement issued late. They change only what the statement says; the canonical trades
//!   never change. Every break is recorded beside its statement and listed under `/test/clearing/breaks`.
//!
//! Draws are a pure function of (`seed`, the statement, the trade's event sequence, the decision), so the same trades
//! give the same statements and breaks, whatever the tick timing.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::domain::{InstrumentId, ParticipantId};
use crate::events::{self, NewEvent};
use crate::settlement::{SettlementPolicy, TradeFacts, draw, format_amount};

/// When statements are issued: the clearinghouse's cutoff calendar (see [`simulator_kit::calendar::CutoffCalendar`]).
pub type ClearingCalendar = simulator_kit::calendar::CutoffCalendar;

pub fn statement_id(date: NaiveDate, participant: &ParticipantId) -> String {
    format!("CS:{date}:{}", participant.0)
}

pub fn obligation_id(statement_id: &str, instrument: &InstrumentId, settles: NaiveDate) -> String {
    format!("{statement_id}:{}:{settles}", instrument.0)
}

/// Seeded reconciliation breaks (`clearing.breaks`). All probabilities default to 0: statements are exact.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BreakPolicy {
    pub seed: u64,
    /// Per trade line: the trade is left out of the statement.
    pub omit_probability: f64,
    /// Per trade line: its quantity or its price is off by one (unit or tick).
    pub alter_probability: f64,
    /// Per statement with trades: a copy of one of its trades under an execution ID that does not exist.
    pub phantom_probability: f64,
    /// Per statement: issued 1..=`max_late_minutes` minutes (domain time) after the cutoff.
    pub late_probability: f64,
    pub max_late_minutes: u32,
}

impl Default for BreakPolicy {
    fn default() -> Self {
        Self {
            seed: 1,
            omit_probability: 0.0,
            alter_probability: 0.0,
            phantom_probability: 0.0,
            late_probability: 0.0,
            max_late_minutes: 60,
        }
    }
}

impl BreakPolicy {
    pub fn validate(&self) -> Result<(), String> {
        let probabilities = [
            ("omit_probability", self.omit_probability),
            ("alter_probability", self.alter_probability),
            ("phantom_probability", self.phantom_probability),
            ("late_probability", self.late_probability),
        ];
        for (name, p) in probabilities {
            if !(0.0..=1.0).contains(&p) {
                return Err(format!("clearing.breaks.{name} must be between 0 and 1"));
            }
        }
        if self.omit_probability + self.alter_probability > 1.0 {
            return Err(
                "clearing.breaks.omit_probability + alter_probability must not exceed 1".into(),
            );
        }
        if self.max_late_minutes == 0 || self.max_late_minutes > 24 * 60 {
            return Err("clearing.breaks.max_late_minutes must be between 1 and 1440".into());
        }
        Ok(())
    }
}

/// Everything the clearinghouse needs: when it issues, which breaks it makes, and the settlement cycles that give
/// each trade its intended settlement date.
#[derive(Clone, Debug, PartialEq)]
pub struct ClearingPolicy {
    pub calendar: ClearingCalendar,
    pub breaks: BreakPolicy,
    pub cycles: SettlementPolicy,
}

/// One trade as a participant's statement reports it, from the participant's side.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatementTrade {
    pub execution_id: Uuid,
    pub trade_sequence: u64,
    pub instrument_id: InstrumentId,
    /// `BUY` or `SELL`.
    pub side: String,
    pub quantity: u64,
    pub price_ticks: i64,
    pub currency: String,
    pub currency_scale: u8,
    /// Price ticks × tick size × quantity, in minor units.
    pub amount_minor: i64,
    pub amount: String,
    pub executed_at: DateTime<Utc>,
    pub trade_date: NaiveDate,
    pub intended_settlement_date: NaiveDate,
    pub obligation_id: String,
    /// What the participant pays the exchange for this trade (SR-37), apart from the cash it settles.
    #[serde(default)]
    pub fee_minor: i64,
    #[serde(default)]
    pub fee: String,
    #[serde(skip)]
    tick_size_minor: u64,
}

/// A net obligation per instrument and settlement date. Positive means the participant receives, negative that it
/// delivers: securities in units, cash in minor units of `currency`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Obligation {
    pub obligation_id: String,
    pub instrument_id: InstrumentId,
    pub intended_settlement_date: NaiveDate,
    pub currency: String,
    pub currency_scale: u8,
    pub securities_net: i64,
    pub cash_net_minor: i64,
    pub cash_net: String,
    pub trade_count: u32,
    /// The obligation's trades' fees (SR-37): what the participant owes the exchange, separate from `cashNet`.
    #[serde(default)]
    pub fees_net_minor: i64,
    #[serde(default)]
    pub fees_net: String,
}

/// A clearing statement, as delivered in `ClearingStatementIssued` and served by the API.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Statement {
    pub statement_id: String,
    pub business_date: NaiveDate,
    pub participant_id: ParticipantId,
    pub time_zone: String,
    /// The window of execution times the statement covers: `[tradesFrom, cutoffAt)`.
    pub trades_from: DateTime<Utc>,
    pub cutoff_at: DateTime<Utc>,
    pub issued_at: DateTime<Utc>,
    pub trades: Vec<StatementTrade>,
    pub obligations: Vec<Obligation>,
}

/// A difference between a statement and the canonical trades, recorded for tests and never delivered.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Break {
    /// `OMITTED_TRADE`, `QUANTITY_CHANGED`, `PRICE_CHANGED`, `PHANTOM_TRADE` or `LATE_STATEMENT`.
    pub kind: String,
    pub execution_id: Option<Uuid>,
    /// What the canonical trade says, and what the statement says instead (absent where there is nothing to say).
    pub canonical: Option<Value>,
    pub reported: Option<Value>,
}

/// A stored statement: issued, or (late) waiting for its `issued_at`.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredStatement {
    pub draft: Draft,
    pub issued: bool,
    /// The `ClearingStatementIssued` event's sequence, once issued.
    pub event_sequence: Option<u64>,
}

/// A statement as drafted at the cutoff, with its breaks. Its `issued_at` is when it is due to be issued.
#[derive(Clone, Debug, PartialEq)]
pub struct Draft {
    pub statement: Statement,
    pub breaks: Vec<Break>,
}

// Draw purposes.
const OMIT_OR_ALTER: u64 = 11;
const ALTER_FIELD: u64 = 12;
const ALTER_SIGN: u64 = 13;
const PHANTOM: u64 = 14;
const PHANTOM_PICK: u64 = 15;
const LATE: u64 = 16;
const LATE_MINUTES: u64 = 17;

impl ClearingPolicy {
    /// The statements for business date `date`: one per participant on either side of `trades` (the trades executed
    /// in the date's window, in sequence order), in participant order.
    pub fn draft(&self, date: NaiveDate, trades: &[TradeFacts]) -> Vec<Draft> {
        let (from, cutoff) = self.calendar.window(date);
        let mut lines: BTreeMap<&ParticipantId, Vec<StatementTrade>> = BTreeMap::new();
        for trade in trades {
            for (participant, side) in [(&trade.buyer_id, "BUY"), (&trade.seller_id, "SELL")] {
                let line = self.line(date, participant, side, trade);
                lines.entry(participant).or_default().push(line);
            }
        }
        lines
            .into_iter()
            .map(|(participant, trades)| self.statement(date, participant, from, cutoff, trades))
            .collect()
    }

    fn line(
        &self,
        date: NaiveDate,
        participant: &ParticipantId,
        side: &str,
        trade: &TradeFacts,
    ) -> StatementTrade {
        // The trade's own dates (SR-29). They agree with the statement's business date, since both come from the
        // business-day calendar; a trade from before executions carried dates takes the statement's.
        let (trade_date, settles) = match trade.dates {
            Some(dates) => (dates.trade_date, dates.intended_settlement_date),
            None => (
                date,
                self.calendar
                    .days
                    .add_business_days(date, self.cycles.settlement_days_for(trade.mic.as_deref())),
            ),
        };
        let mut line = StatementTrade {
            execution_id: trade.execution_id,
            trade_sequence: trade.sequence,
            instrument_id: trade.instrument_id.clone(),
            side: side.to_string(),
            quantity: trade.quantity,
            price_ticks: trade.price_ticks,
            currency: trade.currency.clone(),
            currency_scale: trade.currency_scale,
            amount_minor: 0,
            amount: String::new(),
            executed_at: trade.executed_at,
            trade_date,
            intended_settlement_date: settles,
            obligation_id: obligation_id(
                &statement_id(date, participant),
                &trade.instrument_id,
                settles,
            ),
            fee_minor: if side == "BUY" {
                trade.buyer_fee_minor
            } else {
                trade.seller_fee_minor
            },
            fee: String::new(),
            tick_size_minor: trade.tick_size_minor,
        };
        line.fee = format_amount(line.fee_minor, line.currency_scale);
        line.reprice();
        line
    }

    fn statement(
        &self,
        date: NaiveDate,
        participant: &ParticipantId,
        from: DateTime<Utc>,
        cutoff: DateTime<Utc>,
        canonical: Vec<StatementTrade>,
    ) -> Draft {
        let id = statement_id(date, participant);
        let key = fnv1a(&id);
        let b = &self.breaks;
        let mut breaks = Vec::new();
        let mut trades = Vec::new();
        for line in &canonical {
            let line_key = key ^ line.trade_sequence;
            let u = draw(b.seed, line_key, 0, OMIT_OR_ALTER);
            if u < b.omit_probability {
                breaks.push(Break {
                    kind: "OMITTED_TRADE".into(),
                    execution_id: Some(line.execution_id),
                    canonical: Some(
                        json!({ "quantity": line.quantity, "priceTicks": line.price_ticks }),
                    ),
                    reported: None,
                });
                continue;
            }
            let mut line = line.clone();
            if u < b.omit_probability + b.alter_probability {
                let up = draw(b.seed, line_key, 0, ALTER_SIGN) < 0.5 || line.quantity == 1;
                let step = |value: i64| if up { value + 1 } else { value - 1 };
                if draw(b.seed, line_key, 0, ALTER_FIELD) < 0.5 {
                    let reported = step(line.quantity as i64) as u64;
                    breaks.push(Break {
                        kind: "QUANTITY_CHANGED".into(),
                        execution_id: Some(line.execution_id),
                        canonical: Some(json!(line.quantity)),
                        reported: Some(json!(reported)),
                    });
                    line.quantity = reported;
                } else {
                    let reported = step(line.price_ticks);
                    breaks.push(Break {
                        kind: "PRICE_CHANGED".into(),
                        execution_id: Some(line.execution_id),
                        canonical: Some(json!(line.price_ticks)),
                        reported: Some(json!(reported)),
                    });
                    line.price_ticks = reported;
                }
                line.reprice();
            }
            trades.push(line);
        }
        if !canonical.is_empty() && draw(b.seed, key, 0, PHANTOM) < b.phantom_probability {
            let pick = (draw(b.seed, key, 0, PHANTOM_PICK) * canonical.len() as f64) as usize;
            let mut phantom = canonical[pick.min(canonical.len() - 1)].clone();
            phantom.execution_id = phantom_execution_id(b.seed, key);
            phantom.trade_sequence = 0;
            breaks.push(Break {
                kind: "PHANTOM_TRADE".into(),
                execution_id: Some(phantom.execution_id),
                canonical: None,
                reported: Some(
                    json!({ "copiedFrom": canonical[pick.min(canonical.len() - 1)].execution_id }),
                ),
            });
            trades.push(phantom);
        }
        let mut issued_at = cutoff;
        if draw(b.seed, key, 0, LATE) < b.late_probability {
            let minutes =
                1 + (draw(b.seed, key, 0, LATE_MINUTES) * f64::from(b.max_late_minutes)) as i64;
            let minutes = minutes.min(i64::from(b.max_late_minutes));
            issued_at = cutoff + chrono::Duration::minutes(minutes);
            breaks.push(Break {
                kind: "LATE_STATEMENT".into(),
                execution_id: None,
                canonical: Some(json!(cutoff)),
                reported: Some(json!(issued_at)),
            });
        }
        let obligations = net(&trades);
        Draft {
            statement: Statement {
                statement_id: id,
                business_date: date,
                participant_id: participant.clone(),
                time_zone: self.calendar.time_zone.name().to_string(),
                trades_from: from,
                cutoff_at: cutoff,
                issued_at,
                trades,
                obligations,
            },
            breaks,
        }
    }
}

impl StatementTrade {
    fn reprice(&mut self) {
        let amount = i128::from(self.price_ticks)
            * i128::from(self.tick_size_minor)
            * i128::from(self.quantity);
        self.amount_minor = i64::try_from(amount).unwrap_or(i64::MAX);
        self.amount = format_amount(self.amount_minor, self.currency_scale);
    }
}

/// Nets the statement's trades per obligation: buys receive securities and pay cash, sells the reverse.
fn net(trades: &[StatementTrade]) -> Vec<Obligation> {
    let mut obligations: BTreeMap<&str, Obligation> = BTreeMap::new();
    for t in trades {
        let o = obligations
            .entry(&t.obligation_id)
            .or_insert_with(|| Obligation {
                obligation_id: t.obligation_id.clone(),
                instrument_id: t.instrument_id.clone(),
                intended_settlement_date: t.intended_settlement_date,
                currency: t.currency.clone(),
                currency_scale: t.currency_scale,
                securities_net: 0,
                cash_net_minor: 0,
                cash_net: String::new(),
                trade_count: 0,
                fees_net_minor: 0,
                fees_net: String::new(),
            });
        let sign = if t.side == "BUY" { 1 } else { -1 };
        o.securities_net = o
            .securities_net
            .saturating_add(sign * i64::try_from(t.quantity).unwrap_or(i64::MAX));
        o.cash_net_minor = o.cash_net_minor.saturating_sub(sign * t.amount_minor);
        o.trade_count += 1;
        o.fees_net_minor = o.fees_net_minor.saturating_add(t.fee_minor);
    }
    obligations
        .into_values()
        .map(|mut o| {
            o.cash_net = format_amount(o.cash_net_minor, o.currency_scale);
            o.fees_net = format_amount(o.fees_net_minor, o.currency_scale);
            o
        })
        .collect()
}

/// `ClearingStatementIssued`, stamped with the issue instant. Aggregate ID is the statement ID; correlation ID the
/// participant.
pub fn statement_event(statement: &Statement) -> NewEvent {
    NewEvent {
        event_id: Uuid::nil(),
        sequence: 0,
        event_type: events::CLEARING_STATEMENT_ISSUED,
        source: events::CLEARING_SOURCE,
        aggregate_id: statement.statement_id.clone(),
        correlation_id: Some(statement.participant_id.0.clone()),
        occurred_at: statement.issued_at,
        payload: serde_json::to_value(statement).expect("a statement serializes"),
    }
}

/// Which statement and obligation each side of a trade appears on, carried by its settlement events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClearingRefs {
    pub business_date: NaiveDate,
    pub buyer: PartyRef,
    pub seller: PartyRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartyRef {
    pub statement_id: String,
    pub obligation_id: String,
}

/// The references for a trade executed at `executed_at` that settles on `settles`.
pub fn refs(
    calendar: &ClearingCalendar,
    executed_at: DateTime<Utc>,
    instrument: &InstrumentId,
    buyer: &ParticipantId,
    seller: &ParticipantId,
    settles: NaiveDate,
) -> ClearingRefs {
    let date = calendar.business_date(executed_at);
    let party = |participant: &ParticipantId| {
        let statement_id = statement_id(date, participant);
        PartyRef {
            obligation_id: obligation_id(&statement_id, instrument, settles),
            statement_id,
        }
    };
    ClearingRefs {
        business_date: date,
        buyer: party(buyer),
        seller: party(seller),
    }
}

/// FNV-1a: a stable hash of a statement ID for the seeded draws.
fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// An execution ID no trade has: a UUIDv8 from the seed and the statement.
fn phantom_execution_id(seed: u64, key: u64) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&(draw(seed, key, 1, PHANTOM).to_bits()).to_be_bytes());
    bytes[8..].copy_from_slice(&(draw(seed, key, 2, PHANTOM).to_bits()).to_be_bytes());
    Uuid::new_v8(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(text: &str) -> NaiveDate {
        text.parse().unwrap()
    }

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn copenhagen() -> ClearingCalendar {
        ClearingCalendar::parse("17:30", "Europe/Copenhagen").unwrap()
    }

    fn trade(sequence: u64, buyer: &str, seller: &str, quantity: u64, price: i64) -> TradeFacts {
        TradeFacts {
            sequence,
            execution_id: Uuid::from_u128(u128::from(sequence)),
            instrument_id: InstrumentId("ACME".into()),
            mic: Some("XCSE".into()),
            buyer_id: ParticipantId(buyer.into()),
            seller_id: ParticipantId(seller.into()),
            quantity,
            price_ticks: price,
            currency: "DKK".into(),
            currency_scale: 2,
            tick_size_minor: 1,
            executed_at: at("2030-01-07T10:00:00Z"),
            dates: None,
            buyer_fee_minor: 0,
            seller_fee_minor: 0,
        }
    }

    fn policy(breaks: BreakPolicy) -> ClearingPolicy {
        ClearingPolicy {
            calendar: copenhagen(),
            breaks,
            cycles: SettlementPolicy {
                market_settlement_days: [("XCSE".to_string(), 2)].into(),
                ..SettlementPolicy::default()
            },
        }
    }

    #[test]
    fn statements_net_each_participants_trades_per_instrument_and_settlement_date() {
        let trades = [
            trade(5, "A", "B", 100, 10_000),
            trade(9, "B", "A", 30, 10_100),
            trade(12, "C", "A", 10, 9_900),
        ];
        let drafts = policy(BreakPolicy::default()).draft(d("2030-01-07"), &trades);
        let ids: Vec<&str> = drafts
            .iter()
            .map(|d| d.statement.statement_id.as_str())
            .collect();
        assert_eq!(
            ids,
            ["CS:2030-01-07:A", "CS:2030-01-07:B", "CS:2030-01-07:C"]
        );
        assert!(drafts.iter().all(|d| d.breaks.is_empty()));
        let a = &drafts[0].statement;
        assert_eq!(a.trades.len(), 3);
        assert_eq!(a.cutoff_at, at("2030-01-07T16:30:00Z"));
        assert_eq!(a.issued_at, a.cutoff_at);
        assert_eq!(a.trades_from, at("2030-01-04T16:30:00Z"));
        assert_eq!(a.obligations.len(), 1);
        let o = &a.obligations[0];
        assert_eq!(
            o.obligation_id, "CS:2030-01-07:A:ACME:2030-01-09",
            "XCSE is T+2"
        );
        // Bought 100 @ 100.00, sold 30 @ 101.00 and 10 @ 99.00.
        assert_eq!(o.securities_net, 60);
        assert_eq!(o.cash_net_minor, -1_000_000 + 303_000 + 99_000);
        assert_eq!(o.cash_net, "-5980.00");
        assert_eq!(o.trade_count, 3);
        let b = &drafts[1].statement.obligations[0];
        assert_eq!(
            (b.securities_net, b.cash_net_minor),
            (-70, 1_000_000 - 303_000)
        );
        // Across participants, obligations net to zero.
        let total: (i64, i64) = drafts
            .iter()
            .flat_map(|d| &d.statement.obligations)
            .fold((0, 0), |(s, c), o| {
                (s + o.securities_net, c + o.cash_net_minor)
            });
        assert_eq!(total, (0, 0));
        let event = statement_event(a);
        assert_eq!(
            (event.event_type, event.source, event.aggregate_id.as_str()),
            (
                "ClearingStatementIssued",
                "clearinghouse",
                "CS:2030-01-07:A"
            )
        );
        assert_eq!(event.payload["obligations"][0]["cashNet"], "-5980.00");
        assert_eq!(event.payload["trades"][0]["side"], "BUY");
    }

    #[test]
    fn settlement_references_name_the_statement_and_obligation() {
        let refs = refs(
            &copenhagen(),
            at("2030-01-07T17:00:00Z"),
            &InstrumentId("ACME".into()),
            &ParticipantId("B".into()),
            &ParticipantId("A".into()),
            d("2030-01-09"),
        );
        assert_eq!(refs.business_date, d("2030-01-08"), "after Monday's cutoff");
        assert_eq!(refs.buyer.statement_id, "CS:2030-01-08:B");
        assert_eq!(refs.seller.obligation_id, "CS:2030-01-08:A:ACME:2030-01-09");
    }

    #[test]
    fn each_break_shows_up_in_the_statement_and_in_its_record() {
        let trades: Vec<_> = (1..=200).map(|i| trade(i, "A", "B", 10, 10_000)).collect();
        let all = |breaks: BreakPolicy| policy(breaks).draft(d("2030-01-07"), &trades);
        let kinds = |drafts: &[Draft]| -> Vec<String> {
            drafts
                .iter()
                .flat_map(|d| d.breaks.iter().map(|b| b.kind.clone()))
                .collect()
        };

        let omitted = all(BreakPolicy {
            omit_probability: 0.1,
            ..Default::default()
        });
        let count = kinds(&omitted).len();
        assert!(
            (20..=60).contains(&count),
            "about 10% of 400 lines: {count}"
        );
        for draft in &omitted {
            assert_eq!(draft.statement.trades.len() + draft.breaks.len(), 200);
        }

        let altered = all(BreakPolicy {
            alter_probability: 1.0,
            ..Default::default()
        });
        let a = &altered[0];
        assert_eq!(a.breaks.len(), 200);
        for (line, record) in a.statement.trades.iter().zip(&a.breaks) {
            match record.kind.as_str() {
                "QUANTITY_CHANGED" => {
                    assert_eq!(json!(line.quantity), record.reported.clone().unwrap())
                }
                "PRICE_CHANGED" => {
                    assert_eq!(json!(line.price_ticks), record.reported.clone().unwrap())
                }
                other => panic!("{other}"),
            }
            assert_eq!(line.amount_minor, line.price_ticks * line.quantity as i64);
        }
        let net: i64 = a.statement.trades.iter().map(|t| t.quantity as i64).sum();
        assert_eq!(
            a.statement.obligations[0].securities_net, net,
            "nets what it reports"
        );

        let phantom = all(BreakPolicy {
            phantom_probability: 1.0,
            ..Default::default()
        });
        for draft in &phantom {
            assert_eq!(draft.statement.trades.len(), 201);
            let extra = draft.statement.trades.last().unwrap();
            assert!(trades.iter().all(|t| t.execution_id != extra.execution_id));
            assert_eq!(draft.breaks[0].execution_id, Some(extra.execution_id));
        }

        let late = all(BreakPolicy {
            late_probability: 1.0,
            max_late_minutes: 30,
            ..Default::default()
        });
        for draft in &late {
            let delay = draft.statement.issued_at - draft.statement.cutoff_at;
            assert!(
                delay >= chrono::Duration::minutes(1) && delay <= chrono::Duration::minutes(30)
            );
            assert_eq!(kinds(std::slice::from_ref(draft)), ["LATE_STATEMENT"]);
        }

        let again = all(BreakPolicy {
            omit_probability: 0.1,
            ..Default::default()
        });
        assert_eq!(
            again, omitted,
            "seeded: the same trades give the same breaks"
        );
    }

    #[test]
    fn break_policies_are_validated() {
        assert_eq!(BreakPolicy::default().validate(), Ok(()));
        let bad = |p: BreakPolicy| p.validate().is_err();
        assert!(bad(BreakPolicy {
            late_probability: 1.5,
            ..Default::default()
        }));
        assert!(bad(BreakPolicy {
            omit_probability: 0.6,
            alter_probability: 0.6,
            ..Default::default()
        }));
        assert!(bad(BreakPolicy {
            max_late_minutes: 0,
            ..Default::default()
        }));
    }
}
