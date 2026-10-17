//! End-of-day bank statements (D14): the bank's statement of the firm's client-money account, one per currency, at
//! each business day's cutoff. It lists every movement booked since the previous cutoff (deposit credits, withdrawal
//! debits, and the returns that reverse them) with the opening and closing balance. The balance is derived from all
//! bookings before the window and the configured opening balance; there is no separate ledger. A currency gets
//! statements from the day its first movement is booked.
//!
//! **Breaks** (D16) are opt-in and seeded, as in the exchange's clearing statements: an entry left out, an amount off
//! by one minor unit, an entry for a payment that does not exist, or a statement issued late. A broken statement is
//! internally consistent (its closing balance is its opening balance plus the entries it lists), so the break shows
//! when it is reconciled against the payments, or as the next day's opening balance not matching. Payments and
//! bookings never change. Every break is recorded beside its statement and listed under `/test/statements/breaks`.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use simulator_kit::calendar::CutoffCalendar;
use simulator_kit::events::NewEvent;
use simulator_kit::rng::draw;
use uuid::Uuid;

use crate::money::format_amount;
use crate::payments::{BankAccount, BookingKind};

pub const SOURCE: &str = "bank";
pub const BANK_STATEMENT_ISSUED: &str = "BankStatementIssued";

/// Seeded reconciliation breaks (`statement.breaks`). All probabilities default to 0: statements are exact.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BreakPolicy {
    pub seed: u64,
    /// Per entry: left out of the statement.
    pub omit_probability: f64,
    /// Per entry: its amount is off by one minor unit.
    pub alter_probability: f64,
    /// Per statement with entries: an entry for a payment that does not exist.
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
                return Err(format!("statement.breaks.{name} must be between 0 and 1"));
            }
        }
        if self.omit_probability + self.alter_probability > 1.0 {
            return Err(
                "statement.breaks.omit_probability + alter_probability must not exceed 1".into(),
            );
        }
        if self.max_late_minutes == 0 || self.max_late_minutes > 24 * 60 {
            return Err("statement.breaks.max_late_minutes must be between 1 and 1440".into());
        }
        Ok(())
    }
}

/// When and how statements are issued.
#[derive(Clone, Debug, PartialEq)]
pub struct StatementPolicy {
    pub calendar: CutoffCalendar,
    pub account_iban: String,
    /// Currency → balance before the first booking, in minor units.
    pub opening_balances: BTreeMap<String, i64>,
    pub breaks: BreakPolicy,
}

/// A booked movement with the payment it belongs to: what a statement entry is made from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookedMovement {
    /// The booking's own sequence: its identity in the seeded draws.
    pub booking_id: u64,
    pub payment_id: Uuid,
    pub idempotency_key: String,
    pub account_id: String,
    pub counterparty: BankAccount,
    pub kind: BookingKind,
    pub amount_minor: i64,
    pub booked_at: DateTime<Utc>,
}

/// One line of a statement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    /// `<statementId>:<n>`, numbered in booking order.
    pub entry_id: String,
    pub payment_id: Uuid,
    /// The payment's idempotency key, as the end-to-end reference banks carry.
    pub end_to_end_id: String,
    pub account_id: String,
    pub counterparty: BankAccount,
    pub kind: BookingKind,
    /// `CREDIT` or `DEBIT` on the firm's account.
    pub direction: String,
    /// Positive credits, negative debits, in minor units.
    pub amount_minor: i64,
    pub amount: String,
    pub booked_at: DateTime<Utc>,
    pub value_date: NaiveDate,
}

/// A bank statement, as delivered in `BankStatementIssued` and served by the API.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Statement {
    /// `BS:<businessDate>:<currency>`.
    pub statement_id: String,
    pub business_date: NaiveDate,
    pub account_iban: String,
    pub currency: String,
    pub currency_scale: u8,
    pub time_zone: String,
    /// The window of booking times the statement covers: `[entriesFrom, cutoffAt)`.
    pub entries_from: DateTime<Utc>,
    pub cutoff_at: DateTime<Utc>,
    pub issued_at: DateTime<Utc>,
    pub opening_balance_minor: i64,
    pub opening_balance: String,
    pub total_credits_minor: i64,
    pub total_debits_minor: i64,
    pub closing_balance_minor: i64,
    pub closing_balance: String,
    pub entries: Vec<Entry>,
}

/// A difference between a statement and the canonical bookings, recorded for tests and never delivered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Break {
    /// `OMITTED_ENTRY`, `AMOUNT_CHANGED`, `PHANTOM_ENTRY` or `LATE_STATEMENT`.
    pub kind: String,
    pub payment_id: Option<Uuid>,
    pub canonical: Option<serde_json::Value>,
    pub reported: Option<serde_json::Value>,
}

/// A statement as drafted at the cutoff, with its breaks; its `issued_at` is when it is due to be issued.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Draft {
    pub statement: Statement,
    pub breaks: Vec<Break>,
}

/// A stored statement: issued, or (late) waiting for its `issued_at`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredStatement {
    pub draft: Draft,
    pub issued: bool,
    pub event_sequence: Option<u64>,
}

pub fn statement_id(date: NaiveDate, currency: &str) -> String {
    format!("BS:{date}:{currency}")
}

// Draw purposes.
const OMIT_OR_ALTER: u64 = 31;
const ALTER_SIGN: u64 = 32;
const PHANTOM: u64 = 33;
const PHANTOM_PICK: u64 = 34;
const LATE: u64 = 35;
const LATE_MINUTES: u64 = 36;

impl StatementPolicy {
    /// The statement for `currency` on business date `date`: `opening_minor` is the canonical balance at the start of
    /// the window, `movements` the bookings in it, in booking order.
    pub fn draft(
        &self,
        date: NaiveDate,
        currency: &str,
        scale: u8,
        opening_minor: i64,
        movements: &[BookedMovement],
    ) -> Draft {
        let (from, cutoff) = self.calendar.window(date);
        let id = statement_id(date, currency);
        let key = fnv1a(&id);
        let b = &self.breaks;
        let entry = |n: usize, m: &BookedMovement, amount_minor: i64| Entry {
            entry_id: format!("{id}:{n}"),
            payment_id: m.payment_id,
            end_to_end_id: m.idempotency_key.clone(),
            account_id: m.account_id.clone(),
            counterparty: m.counterparty.clone(),
            kind: m.kind,
            direction: if amount_minor >= 0 { "CREDIT" } else { "DEBIT" }.into(),
            amount_minor,
            amount: format_amount(amount_minor, scale),
            booked_at: m.booked_at,
            value_date: date,
        };
        let mut breaks = Vec::new();
        let mut entries = Vec::new();
        for (index, m) in movements.iter().enumerate() {
            let line_key = key ^ m.booking_id;
            let u = draw(b.seed, line_key, 0, OMIT_OR_ALTER);
            if u < b.omit_probability {
                breaks.push(Break {
                    kind: "OMITTED_ENTRY".into(),
                    payment_id: Some(m.payment_id),
                    canonical: Some(json!(m.amount_minor)),
                    reported: None,
                });
                continue;
            }
            let mut amount = m.amount_minor;
            if u < b.omit_probability + b.alter_probability {
                let up = draw(b.seed, line_key, 0, ALTER_SIGN) < 0.5;
                // Off by one in magnitude, never across zero.
                let magnitude = if up || amount.abs() == 1 {
                    amount.abs() + 1
                } else {
                    amount.abs() - 1
                };
                let reported = magnitude * amount.signum();
                breaks.push(Break {
                    kind: "AMOUNT_CHANGED".into(),
                    payment_id: Some(m.payment_id),
                    canonical: Some(json!(amount)),
                    reported: Some(json!(reported)),
                });
                amount = reported;
            }
            entries.push(entry(index + 1, m, amount));
        }
        if !movements.is_empty() && draw(b.seed, key, 0, PHANTOM) < b.phantom_probability {
            let pick = ((draw(b.seed, key, 0, PHANTOM_PICK) * movements.len() as f64) as usize)
                .min(movements.len() - 1);
            let copied = &movements[pick];
            let phantom = BookedMovement {
                payment_id: phantom_payment_id(b.seed, key),
                ..copied.clone()
            };
            breaks.push(Break {
                kind: "PHANTOM_ENTRY".into(),
                payment_id: Some(phantom.payment_id),
                canonical: None,
                reported: Some(json!({ "copiedFrom": copied.payment_id })),
            });
            entries.push(entry(movements.len() + 1, &phantom, phantom.amount_minor));
        }
        let mut issued_at = cutoff;
        if draw(b.seed, key, 0, LATE) < b.late_probability {
            let minutes = (1
                + (draw(b.seed, key, 0, LATE_MINUTES) * f64::from(b.max_late_minutes)) as i64)
                .min(i64::from(b.max_late_minutes));
            issued_at = cutoff + chrono::Duration::minutes(minutes);
            breaks.push(Break {
                kind: "LATE_STATEMENT".into(),
                payment_id: None,
                canonical: Some(json!(cutoff)),
                reported: Some(json!(issued_at)),
            });
        }
        let credits: i64 = entries
            .iter()
            .map(|e| e.amount_minor)
            .filter(|a| *a > 0)
            .sum();
        let debits: i64 = entries
            .iter()
            .map(|e| e.amount_minor)
            .filter(|a| *a < 0)
            .sum();
        let closing = opening_minor + credits + debits;
        Draft {
            statement: Statement {
                statement_id: id,
                business_date: date,
                account_iban: self.account_iban.clone(),
                currency: currency.to_string(),
                currency_scale: scale,
                time_zone: self.calendar.time_zone.name().to_string(),
                entries_from: from,
                cutoff_at: cutoff,
                issued_at,
                opening_balance_minor: opening_minor,
                opening_balance: format_amount(opening_minor, scale),
                total_credits_minor: credits,
                total_debits_minor: debits,
                closing_balance_minor: closing,
                closing_balance: format_amount(closing, scale),
                entries,
            },
            breaks,
        }
    }
}

/// `BankStatementIssued`, stamped with the issue instant. Aggregate ID is the statement ID; correlation ID the
/// currency.
pub fn statement_event(statement: &Statement) -> NewEvent {
    NewEvent {
        event_id: Uuid::nil(),
        sequence: 0,
        event_type: BANK_STATEMENT_ISSUED,
        source: SOURCE,
        aggregate_id: statement.statement_id.clone(),
        correlation_id: Some(statement.currency.clone()),
        occurred_at: statement.issued_at,
        payload: serde_json::to_value(statement).expect("a statement serializes"),
    }
}

/// FNV-1a: a stable hash of a statement ID for the seeded draws.
fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A payment ID no payment has: a UUIDv8 from the seed and the statement.
fn phantom_payment_id(seed: u64, key: u64) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&draw(seed, key, 1, PHANTOM).to_bits().to_be_bytes());
    bytes[8..].copy_from_slice(&draw(seed, key, 2, PHANTOM).to_bits().to_be_bytes());
    Uuid::new_v8(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(breaks: BreakPolicy) -> StatementPolicy {
        StatementPolicy {
            calendar: CutoffCalendar::parse("17:30", "Europe/Copenhagen").unwrap(),
            account_iban: "DK5000400440116243".into(),
            opening_balances: BTreeMap::new(),
            breaks,
        }
    }

    fn movement(booking_id: u64, kind: BookingKind, amount_minor: i64) -> BookedMovement {
        BookedMovement {
            booking_id,
            payment_id: Uuid::from_u128(u128::from(booking_id)),
            idempotency_key: format!("k{booking_id}"),
            account_id: "acc-1".into(),
            counterparty: BankAccount {
                iban: "DE89370400440532013000".into(),
                holder_name: "A. Customer".into(),
            },
            kind,
            amount_minor,
            booked_at: "2030-01-07T10:00:00Z".parse().unwrap(),
        }
    }

    fn date() -> NaiveDate {
        "2030-01-07".parse().unwrap()
    }

    #[test]
    fn a_statement_lists_the_days_movements_between_its_balances() {
        let movements = [
            movement(1, BookingKind::Deposit, 150_000),
            movement(2, BookingKind::Withdrawal, -40_000),
            movement(3, BookingKind::DepositReturn, -10_000),
        ];
        let draft = policy(BreakPolicy::default()).draft(date(), "DKK", 2, 1_000_000, &movements);
        assert!(draft.breaks.is_empty());
        let s = &draft.statement;
        assert_eq!(s.statement_id, "BS:2030-01-07:DKK");
        assert_eq!(
            s.cutoff_at,
            "2030-01-07T16:30:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(s.issued_at, s.cutoff_at);
        assert_eq!(
            (s.total_credits_minor, s.total_debits_minor),
            (150_000, -50_000)
        );
        assert_eq!(s.closing_balance_minor, 1_100_000);
        assert_eq!(s.closing_balance, "11000.00");
        let directions: Vec<&str> = s.entries.iter().map(|e| e.direction.as_str()).collect();
        assert_eq!(directions, ["CREDIT", "DEBIT", "DEBIT"]);
        assert_eq!(s.entries[1].entry_id, "BS:2030-01-07:DKK:2");
        assert_eq!(s.entries[0].end_to_end_id, "k1");
        let event = statement_event(s);
        assert_eq!(
            (event.event_type, event.source),
            (BANK_STATEMENT_ISSUED, SOURCE)
        );
        assert_eq!(event.payload["closingBalance"], "11000.00");
    }

    #[test]
    fn each_break_shows_in_the_statement_and_in_its_record() {
        let movements: Vec<_> = (1..=300)
            .map(|i| movement(i, BookingKind::Deposit, 10_000))
            .collect();
        let draft = |breaks: BreakPolicy| policy(breaks).draft(date(), "DKK", 2, 0, &movements);

        let omitted = draft(BreakPolicy {
            omit_probability: 0.1,
            ..Default::default()
        });
        let n = omitted.breaks.len();
        assert!((15..=50).contains(&n), "about 10% of 300: {n}");
        assert_eq!(omitted.statement.entries.len() + n, 300);
        assert_eq!(
            omitted.statement.closing_balance_minor,
            10_000 * (300 - n as i64),
            "nets what it lists"
        );

        let altered = draft(BreakPolicy {
            alter_probability: 1.0,
            ..Default::default()
        });
        for (entry, record) in altered.statement.entries.iter().zip(&altered.breaks) {
            assert_eq!(record.kind, "AMOUNT_CHANGED");
            assert_eq!(json!(entry.amount_minor), record.reported.clone().unwrap());
            assert_eq!((entry.amount_minor - 10_000).abs(), 1);
        }

        let phantom = draft(BreakPolicy {
            phantom_probability: 1.0,
            ..Default::default()
        });
        let extra = phantom.statement.entries.last().unwrap();
        assert_eq!(phantom.statement.entries.len(), 301);
        assert!(movements.iter().all(|m| m.payment_id != extra.payment_id));
        assert_eq!(phantom.breaks[0].payment_id, Some(extra.payment_id));

        let late = draft(BreakPolicy {
            late_probability: 1.0,
            max_late_minutes: 15,
            ..Default::default()
        });
        let delay = late.statement.issued_at - late.statement.cutoff_at;
        assert!(delay >= chrono::Duration::minutes(1) && delay <= chrono::Duration::minutes(15));

        assert_eq!(
            draft(BreakPolicy {
                omit_probability: 0.1,
                ..Default::default()
            }),
            omitted,
            "seeded"
        );
    }

    #[test]
    fn an_altered_amount_never_crosses_zero() {
        let one = [movement(1, BookingKind::Withdrawal, -1)];
        let breaks = BreakPolicy {
            alter_probability: 1.0,
            ..Default::default()
        };
        let draft = policy(breaks).draft(date(), "DKK", 2, 0, &one);
        assert_eq!(draft.statement.entries[0].amount_minor, -2);
    }

    #[test]
    fn break_policies_are_validated() {
        assert!(BreakPolicy::default().validate().is_ok());
        assert!(
            BreakPolicy {
                late_probability: 1.5,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            BreakPolicy {
                omit_probability: 0.6,
                alter_probability: 0.6,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            BreakPolicy {
                max_late_minutes: 0,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
}
