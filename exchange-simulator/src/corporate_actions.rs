//! Corporate actions (plan item 6.23, SR-38): cash dividends and splits, announced from configuration or by an
//! operator. Holdings are contractual (user decision 2026-10-22): a participant's net quantity of the instrument over
//! its executions that are not busted and settle on or before the date in question, whatever the settlement mode,
//! scaled by every split applied after the execution.
//!
//! - **Cash dividend:** at 00:00 UTC on the pay date, one `DividendPaid` per participant holding a positive quantity at
//!   the record date: that quantity × the amount per share.
//! - **Split** `new:old`: at 00:00 UTC on the effective date, resting orders keep their place in time priority with
//!   their remaining quantity × new / old (rounded down; an order left with nothing is cancelled) and their limit price
//!   × old / new (rounded to the nearest tick, at least one); reference prices of sources that are not live follow
//!   the same factor. One `SplitApplied` per participant with holdings or resting orders in the instrument.

use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::domain::{Instrument, InstrumentId, ParticipantId, wire_enum};
use crate::events::NewEvent;

pub const CORPORATE_ACTION_ANNOUNCED: &str = "CorporateActionAnnounced";
pub const DIVIDEND_PAID: &str = "DividendPaid";
pub const SPLIT_APPLIED: &str = "SplitApplied";

wire_enum!(
    ActionKind, "corporate action kind" {
        CashDividend => "CASH_DIVIDEND",
        Split => "SPLIT",
    }
);

/// A corporate action as configured (`corporate_actions:`) or announced through the API.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActionSpec {
    pub kind: ActionKind,
    #[serde(alias = "instrument_id")]
    pub instrument_id: InstrumentId,
    #[serde(default, alias = "ex_date")]
    pub ex_date: Option<NaiveDate>,
    #[serde(default, alias = "record_date")]
    pub record_date: Option<NaiveDate>,
    #[serde(default, alias = "pay_date")]
    pub pay_date: Option<NaiveDate>,
    /// A dividend's cash per share, a decimal in the instrument's currency with at most its decimals.
    #[serde(default, alias = "amount_per_share")]
    pub amount_per_share: Option<String>,
    /// A split's ratio, `new:old`, e.g. `"2:1"`.
    #[serde(default)]
    pub ratio: Option<String>,
    #[serde(default, alias = "effective_date")]
    pub effective_date: Option<NaiveDate>,
}

/// An announced corporate action, as stored.
#[derive(Clone, Debug, PartialEq)]
pub struct CorporateAction {
    /// `CA:<instrument>:<kind>:<payDate or effectiveDate>`.
    pub id: String,
    pub kind: ActionKind,
    pub instrument_id: InstrumentId,
    pub ex_date: Option<NaiveDate>,
    pub record_date: Option<NaiveDate>,
    pub pay_date: Option<NaiveDate>,
    pub amount_per_share_minor: i64,
    /// A split's `new:old`; `(1, 1)` for a dividend.
    pub ratio: (u64, u64),
    pub effective_date: Option<NaiveDate>,
    pub announced_at: DateTime<Utc>,
    /// When it was paid or applied; `None` while it waits.
    pub done_at: Option<DateTime<Utc>>,
}

impl CorporateAction {
    /// Validates a spec for `instrument` and makes it an action announced at `now`.
    pub fn announce(
        spec: &ActionSpec,
        instrument: &Instrument,
        now: DateTime<Utc>,
    ) -> Result<Self, String> {
        let mut action = Self {
            id: String::new(),
            kind: spec.kind,
            instrument_id: spec.instrument_id.clone(),
            ex_date: None,
            record_date: None,
            pay_date: None,
            amount_per_share_minor: 0,
            ratio: (1, 1),
            effective_date: None,
            announced_at: now,
            done_at: None,
        };
        let date = match spec.kind {
            ActionKind::CashDividend => {
                if spec.ratio.is_some() || spec.effective_date.is_some() {
                    return Err("a cash dividend takes no ratio or effectiveDate".into());
                }
                let (Some(ex), Some(record), Some(pay)) =
                    (spec.ex_date, spec.record_date, spec.pay_date)
                else {
                    return Err("a cash dividend needs exDate, recordDate and payDate".into());
                };
                if !(ex <= record && record <= pay) {
                    return Err("exDate, recordDate and payDate must be in that order".into());
                }
                let amount = spec
                    .amount_per_share
                    .as_deref()
                    .ok_or("a cash dividend needs amountPerShare")?;
                action.amount_per_share_minor =
                    parse_amount(amount, instrument.currency_scale).ok_or_else(|| {
                        format!(
                            "amountPerShare must be a positive decimal with at most {} decimals, got {amount:?}",
                            instrument.currency_scale
                        )
                    })?;
                (action.ex_date, action.record_date, action.pay_date) =
                    (Some(ex), Some(record), Some(pay));
                pay
            }
            ActionKind::Split => {
                if spec.ex_date.is_some()
                    || spec.record_date.is_some()
                    || spec.pay_date.is_some()
                    || spec.amount_per_share.is_some()
                {
                    return Err("a split takes ratio and effectiveDate only".into());
                }
                let ratio = spec
                    .ratio
                    .as_deref()
                    .ok_or("a split needs ratio, e.g. \"2:1\"")?;
                action.ratio = parse_ratio(ratio).ok_or_else(|| {
                    format!("ratio must be new:old in positive integers, got {ratio:?}")
                })?;
                let effective = spec.effective_date.ok_or("a split needs effectiveDate")?;
                action.effective_date = Some(effective);
                effective
            }
        };
        action.id = format!(
            "CA:{}:{}:{date}",
            action.instrument_id.0,
            action.kind.as_str()
        );
        Ok(action)
    }

    /// When it is paid or applied: 00:00 UTC on the pay or effective date.
    pub fn due_at(&self) -> DateTime<Utc> {
        let date = self
            .pay_date
            .or(self.effective_date)
            .expect("an action has a pay or an effective date");
        date.and_hms_opt(0, 0, 0)
            .expect("midnight exists")
            .and_utc()
    }

    /// The `CorporateActionAnnounced` payload: the action as announced.
    pub fn view(&self, instrument: &Instrument) -> Value {
        let mut view = json!({
            "corporateActionId": self.id,
            "kind": self.kind,
            "instrumentId": self.instrument_id,
            "announcedAt": self.announced_at,
            "status": if self.done_at.is_some() { "DONE" } else { "ANNOUNCED" },
        });
        match self.kind {
            ActionKind::CashDividend => {
                view["exDate"] = json!(self.ex_date);
                view["recordDate"] = json!(self.record_date);
                view["payDate"] = json!(self.pay_date);
                view["amountPerShare"] = json!(crate::settlement::format_amount(
                    self.amount_per_share_minor,
                    instrument.currency_scale
                ));
                view["amountPerShareMinor"] = json!(self.amount_per_share_minor);
                view["currency"] = json!(instrument.currency);
            }
            ActionKind::Split => {
                view["ratio"] = json!(format!("{}:{}", self.ratio.0, self.ratio.1));
                view["effectiveDate"] = json!(self.effective_date);
            }
        }
        view
    }
}

/// A split applied to an instrument: when it took effect, and its `new:old`.
pub type AppliedSplit = (DateTime<Utc>, (u64, u64));

/// A participant's side of an execution, for holdings: its signed quantity, when it was executed and when it settles.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub participant_id: ParticipantId,
    pub quantity: i64,
    pub executed_at: DateTime<Utc>,
    pub intended_settlement_date: NaiveDate,
}

/// Contractual holdings per participant on `date` (sides settling on or before it), each side's quantity scaled by
/// the `splits` (effective instant, `new:old`) applied after it was executed, rounded down at the end.
pub fn holdings(
    positions: &[Position],
    date: NaiveDate,
    splits: &[AppliedSplit],
) -> std::collections::BTreeMap<ParticipantId, i64> {
    let mut held: std::collections::BTreeMap<ParticipantId, i128> = Default::default();
    // In units of 1 / (product of every split's `old`), so splits never lose a fraction on the way.
    let denominator: i128 = splits
        .iter()
        .map(|(_, (_, old))| i128::from(*old))
        .product();
    for p in positions
        .iter()
        .filter(|p| p.intended_settlement_date <= date)
    {
        let mut quantity = i128::from(p.quantity) * denominator;
        for (at, (new, old)) in splits {
            if *at > p.executed_at {
                quantity = quantity * i128::from(*new) / i128::from(*old);
            }
        }
        *held.entry(p.participant_id.clone()).or_default() += quantity;
    }
    held.into_iter()
        .map(|(id, q)| {
            (
                id,
                i64::try_from(q.div_euclid(denominator)).unwrap_or(i64::MAX),
            )
        })
        .collect()
}

/// A corporate-action event for one participant, or the announcement (no participant): aggregate the action,
/// correlation the participant.
pub fn event(
    event_type: &'static str,
    action: &CorporateAction,
    participant: Option<&ParticipantId>,
    at: DateTime<Utc>,
    payload: Value,
) -> NewEvent {
    NewEvent {
        event_id: Uuid::nil(),
        sequence: 0,
        event_type,
        source: crate::events::EXCHANGE_SOURCE,
        aggregate_id: action.id.clone(),
        correlation_id: participant.map(|p| p.0.clone()),
        occurred_at: at,
        payload,
    }
}

/// A positive decimal in major units as minor units with at most `scale` decimals.
fn parse_amount(text: &str, scale: u8) -> Option<i64> {
    let (whole, fraction) = text.trim().split_once('.').unwrap_or((text.trim(), ""));
    if fraction.len() > usize::from(scale)
        || whole.is_empty()
        || !whole
            .chars()
            .chain(fraction.chars())
            .all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{whole}{fraction:0<width$}", width = usize::from(scale));
    digits.parse().ok().filter(|minor: &i64| *minor > 0)
}

/// `new:old` in positive integers, not 1:1.
fn parse_ratio(text: &str) -> Option<(u64, u64)> {
    let (new, old) = text.split_once(':')?;
    let ratio = (new.trim().parse().ok()?, old.trim().parse().ok()?);
    (ratio.0 > 0 && ratio.1 > 0 && ratio.0 != ratio.1).then_some(ratio)
}

/// `price × old / new`, rounded to the nearest tick and at least one: a resting order's limit after a split.
pub fn split_price(ticks: i64, (new, old): (u64, u64)) -> i64 {
    let scaled = i128::from(ticks) * i128::from(old);
    let new = i128::from(new);
    i64::try_from((scaled * 2 + new) / (2 * new))
        .unwrap_or(i64::MAX)
        .max(1)
}

/// `quantity × new / old`, rounded down: a resting order's remainder after a split.
pub fn split_quantity(quantity: u64, (new, old): (u64, u64)) -> u64 {
    u64::try_from(u128::from(quantity) * u128::from(new) / u128::from(old)).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn d(text: &str) -> NaiveDate {
        text.parse().unwrap()
    }

    fn side(id: &str, quantity: i64, executed: &str, settles: &str) -> Position {
        Position {
            participant_id: ParticipantId(id.into()),
            quantity,
            executed_at: at(executed),
            intended_settlement_date: d(settles),
        }
    }

    #[test]
    fn holdings_are_contractual_and_follow_splits() {
        let positions = [
            side("A", 100, "2030-01-07T10:00:00Z", "2030-01-08"),
            side("B", -100, "2030-01-07T10:00:00Z", "2030-01-08"),
            side("A", 30, "2030-01-09T10:00:00Z", "2030-01-10"),
            side("C", 7, "2030-01-10T10:00:00Z", "2030-01-11"),
        ];
        let on = |date| holdings(&positions, d(date), &[]);
        assert_eq!(on("2030-01-08")[&ParticipantId("A".into())], 100);
        assert_eq!(on("2030-01-08")[&ParticipantId("B".into())], -100);
        assert_eq!(
            on("2030-01-10")[&ParticipantId("A".into())],
            130,
            "settled by then"
        );
        assert!(
            !on("2030-01-10").contains_key(&ParticipantId("C".into())),
            "not yet settled"
        );
        // A 3:2 split between the two buys: 100 × 3/2 + 30 = 180.
        let split = [(at("2030-01-09T00:00:00Z"), (3, 2))];
        assert_eq!(
            holdings(&positions, d("2030-01-10"), &split)[&ParticipantId("A".into())],
            180
        );
        // 1:3 then 3:1 leaves 7 at 7, with no fraction lost on the way.
        let both = [
            (at("2030-01-11T00:00:00Z"), (1, 3)),
            (at("2030-01-12T00:00:00Z"), (3, 1)),
        ];
        assert_eq!(
            holdings(&positions, d("2030-01-11"), &both)[&ParticipantId("C".into())],
            7
        );
    }

    #[test]
    fn split_prices_and_quantities_round_as_documented() {
        assert_eq!(
            split_price(10_001, (2, 1)),
            5_001,
            "5000.5 to the nearest tick, up"
        );
        assert_eq!(split_price(1, (2, 1)), 1, "at least one tick");
        assert_eq!(split_price(1_000, (1, 3)), 3_000);
        assert_eq!(split_quantity(25, (2, 1)), 50);
        assert_eq!(split_quantity(5, (1, 2)), 2, "rounded down");
        assert_eq!(parse_ratio("2:1"), Some((2, 1)));
        assert_eq!(parse_ratio("1:1"), None);
        assert_eq!(parse_ratio("0:1"), None);
        assert_eq!(parse_amount("0.50", 2), Some(50));
        assert_eq!(parse_amount("0.505", 2), None);
        assert_eq!(parse_amount("0", 2), None);
    }
}
