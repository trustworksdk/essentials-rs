//! Payments and their lifecycle. A deposit (money in from the customer's bank account) or withdrawal (money out to
//! it) is `PENDING` from the moment it is initiated. After a seeded delay on the domain clock it is `COMPLETED`, and
//! booked on the firm's account, or `FAILED`. A completed payment may later be `RETURNED`: the bank reverses it 1 to
//! `max_return_days` business days on, and books the reversal.
//!
//! Every decision is a pure function of (`seed`, the payment's sequence, the decision), so outcomes do not depend on
//! tick timing or restarts, and events carry the scheduled instant as `occurredAt`.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use simulator_kit::calendar::CutoffCalendar;
use simulator_kit::events::NewEvent;
use simulator_kit::rng::draw;
use uuid::Uuid;

use crate::config::OutcomeConfig;
use crate::money::format_amount;

pub const SOURCE: &str = "payment-gateway";
pub const PAYMENT_PENDING: &str = "PaymentPending";
pub const PAYMENT_COMPLETED: &str = "PaymentCompleted";
pub const PAYMENT_FAILED: &str = "PaymentFailed";
pub const PAYMENT_RETURNED: &str = "PaymentReturned";

/// The longest a payment may take to complete or fail: a week.
pub const MAX_DELAY_SECONDS: u64 = 7 * 24 * 3600;

macro_rules! wire_enum {
    ($(#[$doc:meta])* $name:ident { $($variant:ident => $text:literal),* $(,)? }) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum $name {
            $(#[serde(rename = $text)] $variant),*
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $text),* }
            }
        }

        impl std::str::FromStr for $name {
            type Err = String;
            fn from_str(text: &str) -> Result<Self, String> {
                match text {
                    $($text => Ok(Self::$variant),)*
                    other => Err(format!("unknown {} {other}", stringify!($name))),
                }
            }
        }
    };
}

wire_enum!(
    /// Money in from the customer's bank account, or out to it.
    PaymentType { Deposit => "DEPOSIT", Withdrawal => "WITHDRAWAL" }
);

wire_enum!(
    /// `PENDING` until the outcome; `COMPLETED` may still be `RETURNED`.
    PaymentStatus { Pending => "PENDING", Completed => "COMPLETED", Failed => "FAILED", Returned => "RETURNED" }
);

wire_enum!(
    /// What a booking on the firm's account is for. Credits: deposits and returned withdrawals; debits: withdrawals and
    /// returned deposits.
    BookingKind {
        Deposit => "DEPOSIT",
        Withdrawal => "WITHDRAWAL",
        DepositReturn => "DEPOSIT_RETURN",
        WithdrawalReturn => "WITHDRAWAL_RETURN",
    }
);

/// The customer's account at their bank.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BankAccount {
    pub iban: String,
    pub holder_name: String,
}

/// A deposit or withdrawal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Payment {
    pub id: Uuid,
    /// Its place among all payments (from 1): its identity in the seeded draws.
    pub sequence: u64,
    pub idempotency_key: String,
    pub payment_type: PaymentType,
    /// The demo's account the money belongs to. Opaque: the gateway never interprets it (D13).
    pub account_id: String,
    pub bank_account: BankAccount,
    pub amount_minor: i64,
    pub currency: String,
    pub currency_scale: u8,
    pub reference: Option<String>,
    pub status: PaymentStatus,
    /// Why it failed or was returned.
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
    /// When the next transition is due; `None` once nothing more will happen.
    pub due_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub returned_at: Option<DateTime<Utc>>,
    /// The value date of the latest booking (SR-33): the completion's, then the return's. The business date the
    /// booking falls in, so it is the date of the statement that lists it.
    pub value_date: Option<NaiveDate>,
    pub updated_at: DateTime<Utc>,
    /// An outcome a test forced; it replaces the seeded draws.
    pub forced: Option<ForcedOutcome>,
}

/// An outcome a test forced on a payment (`POST /test/payments/{id}/outcome`, decision D20). It is stored with the
/// payment and replaces the seeded draws when the outcome is due, so a test takes the same path every time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ForcedOutcome {
    /// Completes, and is never returned.
    Completed,
    /// Fails with `reason`.
    Failed { reason: String },
    /// Completes, and is returned `return_after_seconds` later.
    #[serde(rename_all = "camelCase")]
    Returned { return_after_seconds: u64 },
}

/// A movement on the firm's account at the bank, booked when a payment completes or is returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Booking {
    pub payment_id: Uuid,
    pub kind: BookingKind,
    /// Positive credits the firm's account, negative debits it.
    pub amount_minor: i64,
    pub currency: String,
    pub booked_at: DateTime<Utc>,
}

/// A payment's move to a new state, the event that announces it, and the booking it makes, if any.
#[derive(Clone, Debug, PartialEq)]
pub struct Transition {
    pub payment: Payment,
    pub event: NewEvent,
    pub booking: Option<Booking>,
}

/// How payments turn out (`payments:` in the configuration).
#[derive(Clone, Debug, PartialEq)]
pub struct PaymentPolicy {
    pub seed: u64,
    pub deposit: OutcomeConfig,
    pub withdrawal: OutcomeConfig,
    pub return_probability: f64,
    pub max_return_days: u32,
    /// The bank's business-day calendar, which dates bookings and counts return delays: `statement.cutoff` and
    /// `time_zone`, or the exchange's calendar with `calendar.mode: follow` (SR-35).
    pub calendar: CutoffCalendar,
}

// Draw purposes.
const DELAY: u64 = 21;
const FAIL: u64 = 22;
const FAIL_REASON: u64 = 23;
const RETURN: u64 = 24;
const RETURN_DAYS: u64 = 25;

const DEPOSIT_FAILURES: [&str; 3] = ["ACCOUNT_CLOSED", "INSUFFICIENT_FUNDS", "REFUSED_BY_BANK"];
const WITHDRAWAL_FAILURES: [&str; 3] = ["ACCOUNT_CLOSED", "INVALID_ACCOUNT", "REFUSED_BY_BANK"];
pub const RETURN_REASON: &str = "RETURNED_BY_BANK";

impl PaymentPolicy {
    pub fn validate(&self) -> Result<(), String> {
        for (name, outcome) in [("deposit", &self.deposit), ("withdrawal", &self.withdrawal)] {
            if !(0.0..=1.0).contains(&outcome.fail_probability) {
                return Err(format!(
                    "payments.{name}.fail_probability must be between 0 and 1"
                ));
            }
            if outcome.min_delay_seconds > outcome.max_delay_seconds
                || outcome.max_delay_seconds > MAX_DELAY_SECONDS
            {
                return Err(format!(
                    "payments.{name}: min_delay_seconds must not exceed max_delay_seconds, which is at most \
                     {MAX_DELAY_SECONDS}"
                ));
            }
        }
        if !(0.0..=1.0).contains(&self.return_probability) {
            return Err("payments.return_probability must be between 0 and 1".into());
        }
        if !(1..=30).contains(&self.max_return_days) {
            return Err("payments.max_return_days must be between 1 and 30".into());
        }
        Ok(())
    }

    fn outcome(&self, payment_type: PaymentType) -> &OutcomeConfig {
        match payment_type {
            PaymentType::Deposit => &self.deposit,
            PaymentType::Withdrawal => &self.withdrawal,
        }
    }

    /// When a new payment's outcome is due: its creation plus a seeded delay.
    pub fn outcome_due(&self, payment: &Payment) -> DateTime<Utc> {
        let o = self.outcome(payment.payment_type);
        let span = o.max_delay_seconds - o.min_delay_seconds + 1;
        let u = draw(self.seed, payment.sequence, 0, DELAY);
        let seconds = o.min_delay_seconds + ((u * span as f64) as u64).min(span - 1);
        payment.created_at + chrono::Duration::seconds(seconds as i64)
    }

    /// The `PaymentPending` event of a new payment.
    pub fn initiated(&self, payment: &Payment) -> NewEvent {
        payment_event(PAYMENT_PENDING, payment, payment.created_at)
    }

    /// The next transition of a payment that has one due.
    pub fn transition(&self, current: &Payment) -> Transition {
        let at = current
            .due_at
            .expect("only payments with a due transition move");
        let mut next = current.clone();
        next.updated_at = at;
        let seq = current.sequence;
        match current.status {
            PaymentStatus::Pending => {
                let o = self.outcome(current.payment_type);
                let fails = match &current.forced {
                    Some(ForcedOutcome::Failed { reason }) => Some(reason.clone()),
                    Some(_) => None,
                    None => (draw(self.seed, seq, 0, FAIL) < o.fail_probability).then(|| {
                        let reasons = match current.payment_type {
                            PaymentType::Deposit => DEPOSIT_FAILURES,
                            PaymentType::Withdrawal => WITHDRAWAL_FAILURES,
                        };
                        let pick =
                            (draw(self.seed, seq, 0, FAIL_REASON) * reasons.len() as f64) as usize;
                        reasons[pick.min(reasons.len() - 1)].to_string()
                    }),
                };
                if let Some(reason) = fails {
                    next.status = PaymentStatus::Failed;
                    next.reason = Some(reason);
                    next.failed_at = Some(at);
                    next.due_at = None;
                    let event = payment_event(PAYMENT_FAILED, &next, at);
                    return Transition {
                        payment: next,
                        event,
                        booking: None,
                    };
                }
                next.status = PaymentStatus::Completed;
                next.completed_at = Some(at);
                next.value_date = Some(self.calendar.business_date(at));
                next.due_at = match &current.forced {
                    Some(ForcedOutcome::Returned {
                        return_after_seconds,
                    }) => Some(at + chrono::Duration::seconds(*return_after_seconds as i64)),
                    Some(_) => None,
                    None => {
                        (draw(self.seed, seq, 0, RETURN) < self.return_probability).then(|| {
                            let days = 1
                                + ((draw(self.seed, seq, 0, RETURN_DAYS)
                                    * f64::from(self.max_return_days))
                                    as u32)
                                    .min(self.max_return_days - 1);
                            let date = self.calendar.days.add_business_days(at.date_naive(), days);
                            date.and_time(at.time()).and_utc()
                        })
                    }
                };
                let (kind, sign) = match current.payment_type {
                    PaymentType::Deposit => (BookingKind::Deposit, 1),
                    PaymentType::Withdrawal => (BookingKind::Withdrawal, -1),
                };
                let event = payment_event(PAYMENT_COMPLETED, &next, at);
                Transition {
                    booking: Some(booking(&next, kind, sign, at)),
                    payment: next,
                    event,
                }
            }
            PaymentStatus::Completed => {
                next.status = PaymentStatus::Returned;
                next.reason = Some(RETURN_REASON.to_string());
                next.returned_at = Some(at);
                next.value_date = Some(self.calendar.business_date(at));
                next.due_at = None;
                let (kind, sign) = match current.payment_type {
                    PaymentType::Deposit => (BookingKind::DepositReturn, -1),
                    PaymentType::Withdrawal => (BookingKind::WithdrawalReturn, 1),
                };
                let event = payment_event(PAYMENT_RETURNED, &next, at);
                Transition {
                    booking: Some(booking(&next, kind, sign, at)),
                    payment: next,
                    event,
                }
            }
            PaymentStatus::Failed | PaymentStatus::Returned => {
                unreachable!("final payments have nothing due")
            }
        }
    }
}

fn booking(payment: &Payment, kind: BookingKind, sign: i64, at: DateTime<Utc>) -> Booking {
    Booking {
        payment_id: payment.id,
        kind,
        amount_minor: sign * payment.amount_minor,
        currency: payment.currency.clone(),
        booked_at: at,
    }
}

/// A payment as the API and its events show it.
pub fn payment_view(p: &Payment) -> Value {
    json!({
        "paymentId": p.id,
        "sequence": p.sequence,
        "idempotencyKey": p.idempotency_key,
        "type": p.payment_type,
        "accountId": p.account_id,
        "bankAccount": p.bank_account,
        "amount": format_amount(p.amount_minor, p.currency_scale),
        "amountMinor": p.amount_minor,
        "currency": p.currency,
        "currencyScale": p.currency_scale,
        "reference": p.reference,
        "status": p.status,
        "reason": p.reason,
        "createdAt": p.created_at,
        "completedAt": p.completed_at,
        "failedAt": p.failed_at,
        "returnedAt": p.returned_at,
        "valueDate": p.value_date,
        "updatedAt": p.updated_at,
    })
}

/// A payment lifecycle event, unstamped. Aggregate ID is the payment ID, correlation ID the demo's account.
fn payment_event(event_type: &'static str, payment: &Payment, at: DateTime<Utc>) -> NewEvent {
    NewEvent {
        event_id: Uuid::nil(),
        sequence: 0,
        event_type,
        source: SOURCE,
        aggregate_id: payment.id.to_string(),
        correlation_id: Some(payment.account_id.clone()),
        occurred_at: at,
        payload: payment_view(payment),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(adjust: impl FnOnce(&mut PaymentPolicy)) -> PaymentPolicy {
        let mut policy = PaymentPolicy {
            seed: 1,
            deposit: OutcomeConfig {
                fail_probability: 0.0,
                min_delay_seconds: 60,
                max_delay_seconds: 120,
            },
            withdrawal: OutcomeConfig {
                fail_probability: 0.0,
                min_delay_seconds: 600,
                max_delay_seconds: 600,
            },
            return_probability: 0.0,
            max_return_days: 3,
            calendar: CutoffCalendar::parse("17:30", "Europe/Copenhagen").unwrap(),
        };
        adjust(&mut policy);
        policy
    }

    fn payment(sequence: u64, payment_type: PaymentType) -> Payment {
        let at: DateTime<Utc> = "2030-01-07T09:00:00Z".parse().unwrap();
        Payment {
            id: Uuid::from_u128(u128::from(sequence)),
            sequence,
            idempotency_key: format!("k{sequence}"),
            payment_type,
            account_id: "acc-1".into(),
            bank_account: BankAccount {
                iban: "DK5000400440116243".into(),
                holder_name: "A. Customer".into(),
            },
            amount_minor: 150_000,
            currency: "DKK".into(),
            currency_scale: 2,
            reference: None,
            status: PaymentStatus::Pending,
            reason: None,
            created_at: at,
            due_at: None,
            completed_at: None,
            failed_at: None,
            returned_at: None,
            value_date: None,
            updated_at: at,
            forced: None,
        }
    }

    fn due(policy: &PaymentPolicy, mut p: Payment) -> Payment {
        p.due_at = Some(policy.outcome_due(&p));
        p
    }

    #[test]
    fn a_forced_outcome_replaces_the_seeded_draws() {
        // Every seeded draw would fail the payment, or return it; the forced outcome wins either way.
        let policy = policy(|p| {
            p.deposit.fail_probability = 1.0;
            p.return_probability = 1.0;
        });
        let forced = |outcome| {
            let mut p = due(&policy, payment(1, PaymentType::Deposit));
            p.forced = Some(outcome);
            policy.transition(&p)
        };
        let completed = forced(ForcedOutcome::Completed);
        assert_eq!(
            (completed.payment.status, completed.payment.due_at),
            (PaymentStatus::Completed, None),
            "completed, never returned"
        );
        let failed = forced(ForcedOutcome::Failed {
            reason: "TEST_REASON".into(),
        });
        assert_eq!(
            (failed.payment.status, failed.payment.reason.as_deref()),
            (PaymentStatus::Failed, Some("TEST_REASON"))
        );
        let returned = forced(ForcedOutcome::Returned {
            return_after_seconds: 90,
        });
        assert_eq!(returned.payment.status, PaymentStatus::Completed);
        assert_eq!(
            returned.payment.due_at,
            Some(returned.payment.completed_at.unwrap() + chrono::Duration::seconds(90))
        );
        assert_eq!(
            policy.transition(&returned.payment).payment.status,
            PaymentStatus::Returned
        );
        assert_eq!(
            serde_json::to_value(ForcedOutcome::Returned {
                return_after_seconds: 90
            })
            .unwrap(),
            serde_json::json!({ "outcome": "RETURNED", "returnAfterSeconds": 90 })
        );
    }

    #[test]
    fn a_deposit_completes_after_its_delay_and_is_credited() {
        let policy = policy(|_| {});
        let pending = due(&policy, payment(1, PaymentType::Deposit));
        let delay = pending.due_at.unwrap() - pending.created_at;
        assert!(
            delay >= chrono::Duration::seconds(60) && delay <= chrono::Duration::seconds(120),
            "{delay}"
        );
        assert_eq!(policy.initiated(&pending).event_type, PAYMENT_PENDING);
        let done = policy.transition(&pending);
        assert_eq!(done.payment.status, PaymentStatus::Completed);
        assert_eq!(done.payment.due_at, None, "no return drawn");
        assert_eq!(done.event.event_type, PAYMENT_COMPLETED);
        assert_eq!(done.event.occurred_at, pending.due_at.unwrap());
        assert_eq!(done.event.payload["amount"], "1500.00");
        let booking = done.booking.unwrap();
        assert_eq!(
            (booking.kind, booking.amount_minor),
            (BookingKind::Deposit, 150_000)
        );
    }

    #[test]
    fn a_withdrawal_is_debited_and_a_return_reverses_it_on_a_later_business_day() {
        let policy = policy(|p| p.return_probability = 1.0);
        let pending = due(&policy, payment(2, PaymentType::Withdrawal));
        assert_eq!(
            pending.due_at.unwrap() - pending.created_at,
            chrono::Duration::seconds(600)
        );
        let done = policy.transition(&pending);
        assert_eq!(done.booking.as_ref().unwrap().amount_minor, -150_000);
        let returned_at = done.payment.due_at.expect("a return is due");
        let days = returned_at.date_naive() - done.payment.completed_at.unwrap().date_naive();
        assert!(
            (1..=5).contains(&days.num_days()),
            "1..=3 business days from a Monday: {days}"
        );
        assert_eq!(
            returned_at.time(),
            done.payment.completed_at.unwrap().time()
        );
        let back = policy.transition(&done.payment);
        assert_eq!(back.payment.status, PaymentStatus::Returned);
        assert_eq!(back.payment.reason.as_deref(), Some(RETURN_REASON));
        assert_eq!(back.event.event_type, PAYMENT_RETURNED);
        let booking = back.booking.unwrap();
        assert_eq!(
            (booking.kind, booking.amount_minor),
            (BookingKind::WithdrawalReturn, 150_000)
        );
    }

    #[test]
    fn failures_follow_their_probability_with_a_reason_and_no_booking() {
        let policy = policy(|p| p.deposit.fail_probability = 0.3);
        let n = 4000;
        let failed: Vec<Transition> = (1..=n)
            .map(|s| policy.transition(&due(&policy, payment(s, PaymentType::Deposit))))
            .filter(|t| t.payment.status == PaymentStatus::Failed)
            .collect();
        let rate = failed.len() as f64 / n as f64;
        assert!((rate - 0.3).abs() < 0.03, "{rate}");
        assert!(
            failed
                .iter()
                .all(|t| t.booking.is_none() && t.event.event_type == PAYMENT_FAILED)
        );
        assert!(
            failed
                .iter()
                .all(|t| DEPOSIT_FAILURES.contains(&t.payment.reason.as_deref().unwrap()))
        );
        let again = policy.transition(&due(&policy, payment(7, PaymentType::Deposit)));
        assert_eq!(
            again,
            policy.transition(&due(&policy, payment(7, PaymentType::Deposit))),
            "seeded"
        );
    }

    #[test]
    fn policies_are_validated() {
        assert!(policy(|_| {}).validate().is_ok());
        assert!(policy(|p| p.return_probability = 2.0).validate().is_err());
        assert!(policy(|p| p.max_return_days = 0).validate().is_err());
        assert!(
            policy(|p| p.deposit.min_delay_seconds = 500)
                .validate()
                .is_err()
        );
    }
}
