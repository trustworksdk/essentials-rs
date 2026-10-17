//! Optional settlement simulation (Milestone 2). The simulator takes in every `TradeExecuted` event, creates a
//! settlement instruction for it, and moves it through a lifecycle under T+N business-day rules on the exchange
//! clock, with outcomes drawn from configured probabilities. Its lifecycle events carry
//! `source: "settlement-simulator"`.
//!
//! Lifecycle (one seeded draw per decision):
//! - intake: `REJECTED` (`reject_probability`) or `PENDING`, due at 00:00 UTC on the intended settlement date (ISD),
//!   T+N business days after the trade date, where N is the instrument's market (MIC) entry in
//!   `market_settlement_days`, or else `settlement_days` (default 1, the T+1 cycle);
//! - at the ISD: `CANCELLED`, `FAILED`, `DELAYED` by 1..=`max_delay_days` business days (at most `max_delays` times,
//!   then it must resolve), or `SETTLED`;
//! - a settled instruction is `CORRECTED` `correction_days` business days later with `correct_probability`.
//!
//! Draws are a pure function of (`seed`, the trade's event sequence, the attempt, the decision), so outcomes do not
//! depend on processing order, tick timing or restarts, and events carry the scheduled instant as `occurredAt`.

pub mod calendar;
mod machine;

pub use machine::{SettlementPlan, SettlementSimulator, Transition};

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use uuid::Uuid;

use crate::domain::{InstrumentId, ParticipantId, wire_enum};
use crate::events::EventEnvelope;

wire_enum!(
    /// `SETTLED` may still be corrected later (its instruction then has a `due_at`); the others are final once their
    /// `due_at` is cleared.
    SettlementStatus, "settlement status" {
        Pending => "PENDING",
        Delayed => "DELAYED",
        Settled => "SETTLED",
        Failed => "FAILED",
        Rejected => "REJECTED",
        Cancelled => "CANCELLED",
        Corrected => "CORRECTED",
    }
);

/// A simulated settlement instruction for one execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instruction {
    pub execution_id: Uuid,
    /// The `TradeExecuted` event's sequence: the instruction's identity in the seeded draws and its tie-breaker.
    pub trade_sequence: u64,
    pub instrument_id: InstrumentId,
    pub buyer_id: ParticipantId,
    pub seller_id: ParticipantId,
    pub quantity: u64,
    pub price_ticks: i64,
    pub currency: String,
    pub currency_scale: u8,
    pub tick_size_minor: u64,
    /// Cash amount in minor currency units: price ticks × tick size × quantity.
    pub amount_minor: i64,
    pub trade_date: NaiveDate,
    pub intended_settlement_date: NaiveDate,
    pub status: SettlementStatus,
    /// Settlement-date evaluations so far.
    pub attempts: u32,
    pub delays: u32,
    /// When the next transition is due; `None` once nothing more will happen.
    pub due_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

/// Simulation parameters (`settlement:` in the configuration).
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub struct SettlementPolicy {
    pub seed: u64,
    /// The cycle for instruments whose market has no entry in `market_settlement_days`.
    pub settlement_days: u32,
    /// Cycles per market, keyed by uppercase MIC, e.g. `XCSE: 2` for a market still on T+2.
    #[serde(default)]
    pub market_settlement_days: BTreeMap<String, u32>,
    pub reject_probability: f64,
    pub cancel_probability: f64,
    pub fail_probability: f64,
    pub delay_probability: f64,
    pub correct_probability: f64,
    pub max_delay_days: u32,
    pub max_delays: u32,
    pub correction_days: u32,
}

impl Default for SettlementPolicy {
    fn default() -> Self {
        Self {
            seed: 1,
            settlement_days: 1,
            market_settlement_days: BTreeMap::new(),
            reject_probability: 0.0,
            cancel_probability: 0.0,
            fail_probability: 0.0,
            delay_probability: 0.0,
            correct_probability: 0.0,
            max_delay_days: 3,
            max_delays: 2,
            correction_days: 1,
        }
    }
}

impl SettlementPolicy {
    pub fn validate(&self) -> Result<(), String> {
        let probabilities = [
            ("reject_probability", self.reject_probability),
            ("cancel_probability", self.cancel_probability),
            ("fail_probability", self.fail_probability),
            ("delay_probability", self.delay_probability),
            ("correct_probability", self.correct_probability),
        ];
        for (name, p) in probabilities {
            if !(0.0..=1.0).contains(&p) {
                return Err(format!("{name} must be between 0 and 1"));
            }
        }
        if self.cancel_probability + self.fail_probability + self.delay_probability > 1.0 {
            return Err(
                "cancel_probability + fail_probability + delay_probability must not exceed 1"
                    .into(),
            );
        }
        if self.settlement_days > 30 {
            return Err("settlement_days must be at most 30".into());
        }
        for (mic, days) in &self.market_settlement_days {
            crate::domain::validate_mic(mic).map_err(|e| format!("market_settlement_days: {e}"))?;
            if *days > 30 {
                return Err(format!("market_settlement_days: {mic} must be at most 30"));
            }
        }
        if self.max_delay_days == 0 || self.correction_days == 0 {
            return Err("max_delay_days and correction_days must be at least 1".into());
        }
        Ok(())
    }

    /// The settlement cycle, in business days, for an instrument on market `mic`.
    pub fn settlement_days_for(&self, mic: Option<&str>) -> u32 {
        mic.and_then(|mic| self.market_settlement_days.get(mic))
            .copied()
            .unwrap_or(self.settlement_days)
    }
}

/// A settlement outcome reported by an external service. It is stored and published as is, beside (never merged
/// into) any simulated instruction for the same execution.
#[derive(Clone, Debug, PartialEq)]
pub struct ExternalReport {
    pub id: Uuid,
    pub execution_id: Uuid,
    pub status: SettlementStatus,
    /// The reporter's own identifier; a repeated reference with the same content is the same report.
    pub reference: Option<String>,
    pub reason: Option<String>,
    /// Free-form JSON object.
    pub details: Option<serde_json::Value>,
    /// When the reporter says it happened; defaults to the time of receipt.
    pub occurred_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

/// An incoming external report, before it is stored.
#[derive(Clone, Debug, PartialEq)]
pub struct NewReport {
    pub execution_id: Uuid,
    pub status: SettlementStatus,
    pub reference: Option<String>,
    pub reason: Option<String>,
    pub details: Option<serde_json::Value>,
    pub occurred_at: Option<DateTime<Utc>>,
}

impl NewReport {
    /// Whether this is a retry of `existing`: the same outcome, and the same `occurred_at` if this one gives one
    /// (a retry that leaves it out would otherwise get a fresh receipt time).
    pub fn repeats(&self, existing: &ExternalReport) -> bool {
        self.execution_id == existing.execution_id
            && self.status == existing.status
            && self.reference == existing.reference
            && self.reason == existing.reason
            && self.details == existing.details
            && self.occurred_at.is_none_or(|at| at == existing.occurred_at)
    }
}

/// What the simulator needs from a `TradeExecuted` event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeFacts {
    pub sequence: u64,
    pub execution_id: Uuid,
    pub instrument_id: InstrumentId,
    /// The instrument's market at execution time, which picks its settlement cycle.
    pub mic: Option<String>,
    pub buyer_id: ParticipantId,
    pub seller_id: ParticipantId,
    pub quantity: u64,
    pub price_ticks: i64,
    pub currency: String,
    pub currency_scale: u8,
    pub tick_size_minor: u64,
    pub executed_at: DateTime<Utc>,
}

impl TradeFacts {
    /// Reads a canonical `TradeExecuted` envelope.
    pub fn from_event(event: &EventEnvelope) -> Result<Self, String> {
        Self::from_payload(event.sequence, &event.payload)
    }

    /// Reads a `TradeExecuted` payload whose event has `sequence`.
    pub fn from_payload(sequence: u64, p: &serde_json::Value) -> Result<Self, String> {
        let text = |field: &str| {
            p[field]
                .as_str()
                .map(str::to_string)
                .ok_or(format!("{field} missing"))
        };
        let number = |field: &str| p[field].as_u64().ok_or(format!("{field} missing"));
        Ok(Self {
            sequence,
            execution_id: text("executionId")?
                .parse()
                .map_err(|e| format!("executionId: {e}"))?,
            instrument_id: InstrumentId(text("instrumentId")?),
            mic: p["instrument"]["mic"].as_str().map(str::to_string),
            buyer_id: ParticipantId(text("buyerId")?),
            seller_id: ParticipantId(text("sellerId")?),
            quantity: number("quantity")?,
            price_ticks: p["priceTicks"].as_i64().ok_or("priceTicks missing")?,
            currency: text("currency")?,
            currency_scale: u8::try_from(number("currencyScale")?)
                .map_err(|e| format!("currencyScale: {e}"))?,
            tick_size_minor: number("tickSizeMinor")?,
            executed_at: text("executedAt")?
                .parse()
                .map_err(|e| format!("executedAt: {e}"))?,
        })
    }
}

/// Formats minor units as a decimal amount with `scale` decimals, e.g. 1503750 at scale 2 is `"15037.50"`.
pub fn format_amount(minor: i64, scale: u8) -> String {
    let scale = u32::from(scale);
    if scale == 0 {
        return minor.to_string();
    }
    let divisor = 10_i128.pow(scale);
    let value = i128::from(minor);
    let sign = if value < 0 { "-" } else { "" };
    let value = value.abs();
    format!(
        "{sign}{}.{:0width$}",
        value / divisor,
        value % divisor,
        width = scale as usize
    )
}

/// A uniform draw in [0, 1) that depends only on its inputs (SplitMix64 over a mix of them).
pub(crate) fn draw(seed: u64, sequence: u64, attempt: u32, purpose: u64) -> f64 {
    fn mix(mut z: u64) -> u64 {
        z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    let x = mix(mix(mix(seed) ^ sequence) ^ (u64::from(attempt) << 8 | purpose));
    (x >> 11) as f64 / (1_u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn draws_are_deterministic_uniform_and_input_sensitive() {
        assert_eq!(draw(7, 100, 1, 2), draw(7, 100, 1, 2));
        assert_ne!(draw(7, 100, 1, 2), draw(8, 100, 1, 2));
        assert_ne!(draw(7, 100, 1, 2), draw(7, 101, 1, 2));
        assert_ne!(draw(7, 100, 1, 2), draw(7, 100, 2, 2));
        assert_ne!(draw(7, 100, 1, 2), draw(7, 100, 1, 3));
        let n = 100_000;
        let values: Vec<f64> = (0..n).map(|i| draw(42, i, 1, 0)).collect();
        assert!(values.iter().all(|v| (0.0..1.0).contains(v)));
        let below = values.iter().filter(|v| **v < 0.2).count() as f64 / n as f64;
        assert!((below - 0.2).abs() < 0.01, "{below}");
        let mean = values.iter().sum::<f64>() / n as f64;
        assert!((mean - 0.5).abs() < 0.01, "{mean}");
    }

    #[test]
    fn amounts_are_formatted_with_the_currency_scale() {
        assert_eq!(format_amount(1_503_750, 2), "15037.50");
        assert_eq!(format_amount(5, 2), "0.05");
        assert_eq!(format_amount(-150, 2), "-1.50");
        assert_eq!(format_amount(42, 0), "42");
    }

    #[test]
    fn the_cycle_is_t_plus_1_unless_the_market_has_its_own() {
        let policy = SettlementPolicy {
            market_settlement_days: [("XCSE".to_string(), 2)].into(),
            ..Default::default()
        };
        assert_eq!(policy.settlement_days_for(None), 1);
        assert_eq!(policy.settlement_days_for(Some("XNYS")), 1);
        assert_eq!(policy.settlement_days_for(Some("XCSE")), 2);
    }

    #[test]
    fn policies_are_validated() {
        assert_eq!(SettlementPolicy::default().validate(), Ok(()));
        let bad = |p: SettlementPolicy| p.validate().is_err();
        assert!(bad(SettlementPolicy {
            fail_probability: 1.5,
            ..Default::default()
        }));
        assert!(bad(SettlementPolicy {
            fail_probability: 0.6,
            delay_probability: 0.6,
            ..Default::default()
        }));
        assert!(bad(SettlementPolicy {
            settlement_days: 31,
            ..Default::default()
        }));
        let market = |mic: &str, days: u32| SettlementPolicy {
            market_settlement_days: [(mic.to_string(), days)].into(),
            ..Default::default()
        };
        assert_eq!(market("XCSE", 2).validate(), Ok(()));
        assert!(bad(market("XCSE", 31)));
        assert!(bad(market("xcse", 2)));
        assert!(bad(market("XCS", 2)));
        assert!(bad(SettlementPolicy {
            correction_days: 0,
            ..Default::default()
        }));
    }
}
