//! Scenarios (plan item 3.1): YAML files of setup, ordered commands, clock moves and expectations, run against a
//! fresh sandboxed service with a frozen virtual clock and deterministic IDs, so the same file always produces the
//! same events, IDs and verdict. The CLI (`exchange-simulator scenario run`) and the test-control API
//! (`POST /test/scenarios`) share one runner.
//!
//! ```yaml
//! name: partial fill
//! seed: 7                         # deterministic IDs (and, unless set below, the settlement seed)
//! start: 2030-01-07T09:00:00Z     # the virtual clock's start (default: this Monday)
//! settlement: { mode: both, fail_probability: 0 }
//! instruments:  [ { id: ACME, symbol: ACME, currency: DKK, currencyScale: 2 } ]   # API bodies, as for POST
//! participants: [ { id: A, name: Seller }, { id: B, name: Buyer } ]
//! steps:
//!   - order: { as: sell, participantId: A, side: SELL, type: LIMIT, priceTicks: 10025, quantity: 200 }
//!   - order: { as: buy, participantId: B, side: BUY, type: LIMIT, priceTicks: 10025, quantity: 150,
//!              expect: { status: 201, matches: { order: { status: FILLED } } } }
//!   - expect: { order: sell, matches: { status: PARTIALLY_FILLED, remainingQuantity: 50 } }
//!   - advance_clock: { days: 2 }
//!   - expect: { settlement: "buy#0", matches: { simulated: { status: SETTLED } } }
//!   - expect: { events: { eventType: SettlementCompleted }, count: 1 }
//!   - market: { orders: 500, orders_per_second: 10, participants: 5, cancellation_probability: 0.1,
//!               instruments: [ { id: ACME, price: { mid_ticks: 10025, width_ticks: 10 } } ] }
//!   - expect: { deliveries: { eventType: SettlementCompleted, status: DELIVERED }, count: 1, wait_ms: 2000 }
//! ```
//!
//! References: `as: name` names an order; `name` stands for its order ID and `name#i` for the ID of its `i`-th
//! execution (from its submit response). `instrumentId` may be left out when the scenario has one instrument.

mod matcher;
mod runner;

pub use matcher::{contains, mismatches};
pub use runner::{EventLog, Report, StepReport, run};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::config::SettlementMode;
use crate::market::MarketConfig;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Seeds the deterministic IDs, and the settlement simulator unless `settlement.seed` is given.
    #[serde(default)]
    pub seed: u64,
    /// The virtual clock's first reading; it stays frozen except for clock steps.
    #[serde(default = "default_start")]
    pub start: DateTime<Utc>,
    #[serde(default)]
    pub settlement: Option<SettlementSection>,
    /// `POST /api/instruments` bodies.
    #[serde(default)]
    pub instruments: Vec<Value>,
    /// `POST /api/participants` bodies.
    #[serde(default)]
    pub participants: Vec<Value>,
    /// `POST /api/webhooks` bodies.
    #[serde(default)]
    pub webhooks: Vec<Value>,
    pub steps: Vec<Step>,
}

/// Monday 2030-01-07 09:00 UTC.
fn default_start() -> DateTime<Utc> {
    "2030-01-07T09:00:00Z".parse().expect("valid instant")
}

/// Settlement for the run; unset fields keep the service's configuration.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementSection {
    pub mode: Option<SettlementMode>,
    pub seed: Option<u64>,
    pub settlement_days: Option<u32>,
    pub reject_probability: Option<f64>,
    pub cancel_probability: Option<f64>,
    pub fail_probability: Option<f64>,
    pub delay_probability: Option<f64>,
    pub correct_probability: Option<f64>,
    pub max_delay_days: Option<u32>,
    pub max_delays: Option<u32>,
    pub correction_days: Option<u32>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    /// `POST /api/orders`.
    Order(OrderStep),
    /// `DELETE /api/orders/{order}`.
    Cancel(CancelStep),
    /// `POST /test/clock/advance`.
    AdvanceClock(Advance),
    /// `POST /test/clock/set`.
    SetClock(SetClock),
    /// `POST /api/settlements/{execution}/outcomes`.
    ReportSettlement(ReportStep),
    /// `PATCH /api/instruments/{instrument}`.
    SetInstrument(SetInstrument),
    /// Sets the delivery fault policy of the webhook registered for `url` (`PUT /test/webhooks/{id}/faults`), or
    /// clears it with `faults: null`.
    SetFaults(SetFaults),
    /// A seeded random market ([`crate::market`]): its participants are registered if new, then its commands are
    /// sent in order, the virtual clock set to each arrival time.
    Market(MarketConfig),
    /// A check of the current state.
    Expect(Expectation),
}

/// A check of a command's own HTTP response.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseCheck {
    pub status: Option<u16>,
    pub matches: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OrderStep {
    #[serde(rename = "as")]
    pub name: Option<String>,
    pub expect: Option<ResponseCheck>,
    /// The request body.
    #[serde(flatten)]
    pub body: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelStep {
    pub order: String,
    pub expect: Option<ResponseCheck>,
}

#[derive(Clone, Debug, Default, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Advance {
    #[serde(default)]
    pub days: u32,
    #[serde(default)]
    pub hours: u32,
    #[serde(default)]
    pub minutes: u32,
    #[serde(default)]
    pub seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetClock {
    pub to: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReportStep {
    /// An execution reference, `order#i`, or a literal execution ID.
    pub execution: String,
    pub expect: Option<ResponseCheck>,
    #[serde(flatten)]
    pub body: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetFaults {
    pub url: String,
    pub faults: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetInstrument {
    pub instrument: String,
    pub enabled: bool,
    pub expect: Option<ResponseCheck>,
}

/// A check of the current state. Exactly one target: an `order`, a `book`, a `settlement`, or a filter over `trades`,
/// `events`, webhook `deliveries` or webhook `attempts` (what was actually sent). For single targets `matches` must be
/// contained in the target's JSON; for lists the filter selects the matching items and `count` checks how many
/// (default: at least one).
///
/// Webhook delivery runs on real time, not on the virtual clock, so `deliveries` and `attempts` checks are retried
/// until they hold or `wait_ms` (default 5000) has passed.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expectation {
    pub order: Option<String>,
    pub book: Option<String>,
    pub settlement: Option<String>,
    pub trades: Option<Value>,
    pub events: Option<Value>,
    pub deliveries: Option<Value>,
    pub attempts: Option<Value>,
    pub count: Option<usize>,
    /// Every item must match the filter, and there must be at least one. With `wait_ms`, e.g. "every delivery is
    /// `DELIVERED`" waits until the outbox has drained.
    pub all: Option<bool>,
    pub wait_ms: Option<u64>,
    pub matches: Option<Value>,
}

impl Scenario {
    /// Parses a scenario from YAML (JSON is valid YAML too). Errors carry the line and column.
    pub fn parse(text: &str) -> Result<Self, String> {
        let scenario: Self = serde_saphyr::from_str(text).map_err(|e| e.to_string())?;
        for (i, step) in scenario.steps.iter().enumerate() {
            if let Step::Market(market) = step {
                market
                    .validate()
                    .map_err(|error| format!("step {}: market: {error}", i + 1))?;
            }
            if let Step::Expect(expectation) = step {
                let targets = [
                    expectation.order.is_some(),
                    expectation.book.is_some(),
                    expectation.settlement.is_some(),
                    expectation.trades.is_some(),
                    expectation.events.is_some(),
                    expectation.deliveries.is_some(),
                    expectation.attempts.is_some(),
                ];
                if targets.iter().filter(|t| **t).count() != 1 {
                    return Err(format!(
                        "step {}: expect needs exactly one of order, book, settlement, trades, events, deliveries, \
                         attempts",
                        i + 1
                    ));
                }
                if expectation.all.is_some() && expectation.count.is_some() {
                    return Err(format!(
                        "step {}: give either count or all, not both",
                        i + 1
                    ));
                }
                if expectation.wait_ms.is_some()
                    && expectation.deliveries.is_none()
                    && expectation.attempts.is_none()
                {
                    return Err(format!(
                        "step {}: wait_ms applies only to deliveries and attempts; everything else follows the \
                         virtual clock",
                        i + 1
                    ));
                }
            }
        }
        Ok(scenario)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_documented_example() {
        let text = r#"
name: partial fill
seed: 7
settlement: { mode: both, fail_probability: 0 }
instruments: [ { id: ACME, symbol: ACME, currency: DKK, currencyScale: 2 } ]
participants: [ { id: A, name: Seller }, { id: B, name: Buyer } ]
steps:
  - order: { as: sell, participantId: A, side: SELL, type: LIMIT, priceTicks: 10025, quantity: 200 }
  - order: { as: buy, participantId: B, side: BUY, type: LIMIT, priceTicks: 10025, quantity: 150,
             expect: { status: 201, matches: { order: { status: FILLED } } } }
  - cancel: { order: sell }
  - advance_clock: { days: 2 }
  - set_clock: { to: 2030-01-20T00:00:00Z }
  - report_settlement: { execution: "buy#0", status: SETTLED, reference: r-1 }
  - set_instrument: { instrument: ACME, enabled: false }
  - market: { orders: 10, instruments: [ { id: ACME, price: { mid_ticks: 100 } } ] }
  - expect: { order: sell, matches: { status: CANCELLED } }
  - expect: { events: { eventType: SettlementCompleted }, count: 1 }
"#;
        let scenario = Scenario::parse(text).unwrap();
        assert_eq!(
            (scenario.name.as_str(), scenario.seed, scenario.steps.len()),
            ("partial fill", 7, 10)
        );
        assert_eq!(scenario.start, default_start());
        let Step::Order(order) = &scenario.steps[1] else {
            panic!("order step")
        };
        assert_eq!(order.name.as_deref(), Some("buy"));
        assert_eq!(order.body["priceTicks"], 10025);
        assert!(!order.body.contains_key("expect") && !order.body.contains_key("as"));
        assert_eq!(order.expect.as_ref().unwrap().status, Some(201));
    }

    #[test]
    fn mistakes_are_reported_with_their_location() {
        let unknown_step = Scenario::parse("name: x\nsteps:\n  - buy: {}\n").unwrap_err();
        assert!(
            unknown_step.contains("unknown variant `buy`"),
            "{unknown_step}"
        );
        assert!(unknown_step.contains('3'), "line number: {unknown_step}");
        let typo = Scenario::parse("name: x\nsede: 1\nsteps: []\n").unwrap_err();
        assert!(typo.contains("sede"), "{typo}");
        let two_targets =
            Scenario::parse("name: x\nsteps:\n  - expect: { order: a, book: ACME }\n").unwrap_err();
        assert!(two_targets.contains("exactly one"), "{two_targets}");
        let both =
            Scenario::parse("name: x\nsteps:\n  - expect: { trades: {}, count: 1, all: true }\n")
                .unwrap_err();
        assert!(both.contains("either count or all"), "{both}");
        assert!(
            Scenario::parse(
                "name: x\nsteps:\n  - expect: { attempts: {}, all: true, wait_ms: 5 }\n"
            )
            .is_ok()
        );
        let bad_market = Scenario::parse(
            "name: x\nsteps:\n  - market: { orders: 1, buy_probability: 2, instruments: [ { id: A, price: { mid_ticks: 1 } } ] }\n",
        )
        .unwrap_err();
        assert!(
            bad_market.contains("step 1: market: buy_probability"),
            "{bad_market}"
        );
        let no_target = Scenario::parse("name: x\nsteps:\n  - expect: { count: 1 }\n").unwrap_err();
        assert!(no_target.contains("step 1"), "{no_target}");
        let misplaced_wait =
            Scenario::parse("name: x\nsteps:\n  - expect: { trades: {}, wait_ms: 10 }\n")
                .unwrap_err();
        assert!(misplaced_wait.contains("wait_ms"), "{misplaced_wait}");
        assert!(
            Scenario::parse("name: x\nsteps:\n  - expect: { deliveries: {}, wait_ms: 10 }\n")
                .is_ok()
        );
    }
}
