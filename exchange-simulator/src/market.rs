//! Seeded random markets (plan item 3.2): a generator of order and cancel commands drawn from configured
//! distributions (plan §6). It is pure: the same configuration and seed give the same command stream, independent of
//! how the exchange answers. Reproducible runs then follow from feeding that stream in order, under the virtual clock,
//! with deterministic IDs. Scenarios do that with a `market` step; the benchmark uses the generator for its workload.
//!
//! ```yaml
//! seed: 12345                     # default 0; in a scenario, the scenario's seed
//! orders: 1000                    # how many commands (submits and cancels) to generate
//! orders_per_second: 50           # Poisson arrivals in simulated time
//! participants: 20                # P01..P20, or a list of participant IDs
//! buy_probability: 0.5
//! market_order_probability: 0.2
//! cancellation_probability: 0.1   # chance that a command cancels an earlier limit order instead of submitting
//! instruments:
//!   - id: ACME
//!     weight: 1                   # share of the arrivals
//!     price: { mid_ticks: 10000, width_ticks: 50, skew_ticks: 0, distribution: uniform }
//!     quantity: { min: 1, max: 100, lot: 1 }
//! ```

use serde::{Deserialize, Serialize};

use crate::domain::{InstrumentId, OrderType, ParticipantId, Side};
use crate::engine::SubmitOrder;
pub use simulator_kit::rng::Rng;

/// A random market's configuration. Every probability is in `[0, 1]`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketConfig {
    /// Default 0; in a scenario, the scenario's seed.
    #[serde(default)]
    pub seed: Option<u64>,
    /// The number of commands, submits and cancels together.
    pub orders: usize,
    /// The mean arrival rate in simulated time; arrivals are a Poisson process. `0` means all at the same instant.
    #[serde(default)]
    pub orders_per_second: f64,
    #[serde(default = "default_participants")]
    pub participants: Participants,
    #[serde(default = "half")]
    pub buy_probability: f64,
    #[serde(default)]
    pub market_order_probability: f64,
    #[serde(default)]
    pub cancellation_probability: f64,
    pub instruments: Vec<InstrumentMarket>,
}

fn default_participants() -> Participants {
    Participants::Count(10)
}

fn half() -> f64 {
    0.5
}

/// The participants who trade: a count (`P01`, `P02`, …) or explicit IDs. Each order picks one uniformly.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Participants {
    Count(usize),
    Ids(Vec<String>),
}

impl Participants {
    pub fn ids(&self) -> Vec<String> {
        match self {
            Self::Count(n) => {
                let width = n.to_string().len().max(2);
                (1..=*n).map(|i| format!("P{i:0width$}")).collect()
            }
            Self::Ids(ids) => ids.clone(),
        }
    }
}

/// One instrument's share of the arrivals and its price and size distributions.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentMarket {
    pub id: String,
    #[serde(default = "one")]
    pub weight: f64,
    pub price: PriceDistribution,
    #[serde(default)]
    pub quantity: QuantityDistribution,
}

fn one() -> f64 {
    1.0
}

/// Limit prices, in ticks. Buys centre on `mid_ticks - skew_ticks` and sells on `mid_ticks + skew_ticks`, so a
/// positive skew builds passive liquidity and a negative one makes orders cross. `width_ticks` is the half-width of a
/// uniform distribution, or the standard deviation of a normal one (rounded to whole ticks). Prices below one tick are
/// raised to one tick.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PriceDistribution {
    pub mid_ticks: i64,
    #[serde(default)]
    pub width_ticks: u32,
    #[serde(default)]
    pub skew_ticks: i64,
    #[serde(default)]
    pub distribution: Distribution,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Distribution {
    #[default]
    Uniform,
    Normal,
}

/// Order sizes: uniform whole lots between `min` and `max` (inclusive, rounded inwards to lots).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QuantityDistribution {
    pub min: u64,
    pub max: u64,
    #[serde(default = "one_lot")]
    pub lot: u64,
}

fn one_lot() -> u64 {
    1
}

impl Default for QuantityDistribution {
    fn default() -> Self {
        Self {
            min: 1,
            max: 100,
            lot: 1,
        }
    }
}

/// One generated command, `at_micros` after the start of the market in simulated time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketCommand {
    pub at_micros: u64,
    pub action: MarketAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarketAction {
    Submit(SubmitOrder),
    /// Cancels the generated order with this index (counting submits only, from 0). It was a limit order, but it may
    /// have been filled since, in which case the exchange refuses the cancel.
    Cancel {
        submit_index: usize,
    },
}

impl MarketConfig {
    /// Checks that the distributions make sense; the error names the field.
    pub fn validate(&self) -> Result<(), String> {
        let probabilities = [
            ("buy_probability", self.buy_probability),
            ("market_order_probability", self.market_order_probability),
            ("cancellation_probability", self.cancellation_probability),
        ];
        for (name, p) in probabilities {
            if !(0.0..=1.0).contains(&p) {
                return Err(format!("{name} must be between 0 and 1, got {p}"));
            }
        }
        if !(self.orders_per_second.is_finite() && self.orders_per_second >= 0.0) {
            return Err("orders_per_second must be zero or positive".into());
        }
        if self.participants.ids().is_empty() {
            return Err("participants must not be empty".into());
        }
        if self.instruments.is_empty() {
            return Err("instruments must not be empty".into());
        }
        if self.instruments.iter().map(|i| i.weight).sum::<f64>() <= 0.0 {
            return Err("instrument weights must add up to more than zero".into());
        }
        for instrument in &self.instruments {
            let id = &instrument.id;
            if !(instrument.weight.is_finite() && instrument.weight >= 0.0) {
                return Err(format!("{id}: weight must be zero or positive"));
            }
            if instrument.price.mid_ticks < 1 {
                return Err(format!("{id}: price.mid_ticks must be at least 1"));
            }
            let q = &instrument.quantity;
            if q.lot == 0
                || q.min > q.max
                || q.max / q.lot == 0
                || q.min.div_ceil(q.lot) > q.max / q.lot
            {
                return Err(format!(
                    "{id}: quantity needs 0 < lot and at least one whole lot between min and max"
                ));
            }
        }
        Ok(())
    }

    /// The command stream. Call [`MarketConfig::validate`] first; an invalid configuration may panic.
    pub fn generate(&self) -> Generator<'_> {
        Generator {
            config: self,
            rng: Rng::new(self.seed.unwrap_or(0)),
            participants: self.participants.ids(),
            total_weight: self.instruments.iter().map(|i| i.weight).sum(),
            produced: 0,
            micros: 0.0,
            submits: 0,
            cancellable: Vec::new(),
        }
    }
}

/// Iterates the generated commands. Each draws from one seeded stream in a fixed order, so the sequence depends only
/// on the configuration.
pub struct Generator<'a> {
    config: &'a MarketConfig,
    rng: Rng,
    participants: Vec<String>,
    total_weight: f64,
    produced: usize,
    micros: f64,
    submits: usize,
    /// Submit indexes of limit orders not yet chosen for a cancel.
    cancellable: Vec<usize>,
}

impl Iterator for Generator<'_> {
    type Item = MarketCommand;

    fn next(&mut self) -> Option<MarketCommand> {
        if self.produced == self.config.orders {
            return None;
        }
        self.produced += 1;
        let rate = self.config.orders_per_second;
        if rate > 0.0 {
            // Exponential inter-arrival times; 1 - u is in (0, 1], so the logarithm is finite.
            self.micros += -(1.0 - self.rng.unit()).ln() / rate * 1e6;
        }
        let at_micros = self.micros.round() as u64;
        if !self.cancellable.is_empty() && self.rng.chance(self.config.cancellation_probability) {
            let pick = self.rng.below(self.cancellable.len() as u64) as usize;
            let submit_index = self.cancellable.swap_remove(pick);
            return Some(MarketCommand {
                at_micros,
                action: MarketAction::Cancel { submit_index },
            });
        }
        let instrument = self.instrument();
        let side = if self.rng.chance(self.config.buy_probability) {
            Side::Buy
        } else {
            Side::Sell
        };
        let market = self.rng.chance(self.config.market_order_probability);
        let participant =
            self.participants[self.rng.below(self.participants.len() as u64) as usize].clone();
        let price = (!market).then(|| self.price(&instrument.price, side));
        let quantity = self.quantity(&instrument.quantity);
        if !market {
            self.cancellable.push(self.submits);
        }
        self.submits += 1;
        Some(MarketCommand {
            at_micros,
            action: MarketAction::Submit(SubmitOrder {
                client_order_id: None,
                instrument_id: InstrumentId(instrument.id.clone()),
                participant_id: ParticipantId(participant),
                side,
                order_type: if market {
                    OrderType::Market
                } else {
                    OrderType::Limit
                },
                limit_price_ticks: price,
                quantity,
            }),
        })
    }
}

impl<'a> Generator<'a> {
    fn instrument(&mut self) -> &'a InstrumentMarket {
        let config: &'a MarketConfig = self.config;
        let instruments = &config.instruments;
        if let [only] = instruments.as_slice() {
            return only;
        }
        let mut target = self.rng.unit() * self.total_weight;
        for instrument in instruments {
            if target < instrument.weight {
                return instrument;
            }
            target -= instrument.weight;
        }
        // Rounding can leave a sliver past the last weight.
        instruments
            .iter()
            .rfind(|i| i.weight > 0.0)
            .expect("validated: positive total weight")
    }

    fn price(&mut self, price: &PriceDistribution, side: Side) -> i64 {
        let centre = match side {
            Side::Buy => price.mid_ticks - price.skew_ticks,
            Side::Sell => price.mid_ticks + price.skew_ticks,
        };
        let width = i64::from(price.width_ticks);
        let offset = match price.distribution {
            Distribution::Uniform => self.rng.below(2 * width as u64 + 1) as i64 - width,
            Distribution::Normal => (self.rng.normal() * width as f64).round() as i64,
        };
        (centre + offset).max(1)
    }

    fn quantity(&mut self, q: &QuantityDistribution) -> u64 {
        let low = q.min.div_ceil(q.lot).max(1);
        let high = q.max / q.lot;
        (low + self.rng.below(high - low + 1)) * q.lot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(text: &str) -> MarketConfig {
        let config: MarketConfig = serde_saphyr::from_str(text).unwrap();
        config.validate().unwrap();
        config
    }

    const BASIC: &str = "
seed: 9
orders: 20000
orders_per_second: 100
participants: 5
buy_probability: 0.3
market_order_probability: 0.2
cancellation_probability: 0.1
instruments:
  - { id: ACME, weight: 3, price: { mid_ticks: 10000, width_ticks: 20, skew_ticks: 5 },
      quantity: { min: 10, max: 95, lot: 10 } }
  - { id: BETA, weight: 1, price: { mid_ticks: 500, width_ticks: 10, distribution: normal } }
";

    fn submits(commands: &[MarketCommand]) -> Vec<&SubmitOrder> {
        commands
            .iter()
            .filter_map(|c| match &c.action {
                MarketAction::Submit(order) => Some(order),
                MarketAction::Cancel { .. } => None,
            })
            .collect()
    }

    #[test]
    fn the_same_seed_gives_the_same_stream_and_another_seed_another() {
        let a = config(BASIC);
        let first: Vec<_> = a.generate().collect();
        assert_eq!(first, a.generate().collect::<Vec<_>>());
        assert_eq!(first.len(), 20_000);
        let mut b = a.clone();
        b.seed = Some(10);
        assert_ne!(first, b.generate().collect::<Vec<_>>());
    }

    #[test]
    fn the_stream_is_pinned() {
        // Guards against accidental changes to the generator: recorded runs must stay reproducible across versions.
        let config = config(BASIC);
        let head: Vec<String> = config
            .generate()
            .take(3)
            .map(|c| match c.action {
                MarketAction::Submit(o) => format!(
                    "{} {} {:?} {:?} {:?} {:?} {}",
                    c.at_micros,
                    o.instrument_id.0,
                    o.participant_id.0,
                    o.side,
                    o.order_type,
                    o.limit_price_ticks,
                    o.quantity
                ),
                MarketAction::Cancel { submit_index } => {
                    format!("{} cancel {submit_index}", c.at_micros)
                }
            })
            .collect();
        assert_eq!(head, PINNED);
    }

    /// The first command was recomputed independently, with SplitMix64 written in Python.
    const PINNED: [&str; 3] = [
        "11468 BETA \"P02\" Buy Limit Some(497) 99",
        "13941 ACME \"P02\" Buy Limit Some(10006) 80",
        "22271 ACME \"P01\" Sell Limit Some(10004) 40",
    ];

    #[test]
    fn draws_follow_the_configured_distributions() {
        let config = config(BASIC);
        let commands: Vec<_> = config.generate().collect();
        let orders = submits(&commands);
        let n = orders.len() as f64;
        let share =
            |f: &dyn Fn(&SubmitOrder) -> bool| orders.iter().filter(|o| f(o)).count() as f64 / n;
        let near = |actual: f64, expected: f64| (actual - expected).abs() < 0.015;

        let cancels = (commands.len() - orders.len()) as f64 / commands.len() as f64;
        assert!(near(cancels, 0.1), "cancellations {cancels}");
        assert!(near(share(&|o| o.side == Side::Buy), 0.3));
        assert!(near(share(&|o| o.order_type == OrderType::Market), 0.2));
        assert!(near(share(&|o| o.instrument_id.0 == "ACME"), 0.75));
        let participants: std::collections::BTreeSet<_> =
            orders.iter().map(|o| o.participant_id.0.as_str()).collect();
        assert_eq!(
            participants.into_iter().collect::<Vec<_>>(),
            ["P01", "P02", "P03", "P04", "P05"]
        );

        for order in &orders {
            assert_eq!(
                order.limit_price_ticks.is_some(),
                order.order_type == OrderType::Limit
            );
            if order.instrument_id.0 == "ACME" {
                assert!(
                    [10, 20, 30, 40, 50, 60, 70, 80, 90].contains(&order.quantity),
                    "{}",
                    order.quantity
                );
                if let Some(price) = order.limit_price_ticks {
                    let centre = if order.side == Side::Buy {
                        9_995
                    } else {
                        10_005
                    };
                    assert!((price - centre).abs() <= 20, "{price}");
                }
            } else {
                assert!((1..=100).contains(&order.quantity));
            }
        }
        // Uniform: every tick of the range is used. Normal: about 68 % within one standard deviation.
        let acme_buys: std::collections::BTreeSet<i64> = orders
            .iter()
            .filter(|o| o.instrument_id.0 == "ACME" && o.side == Side::Buy)
            .filter_map(|o| o.limit_price_ticks)
            .collect();
        assert_eq!(acme_buys.len(), 41);
        let beta: Vec<i64> = orders
            .iter()
            .filter(|o| o.instrument_id.0 == "BETA")
            .filter_map(|o| o.limit_price_ticks)
            .collect();
        let within =
            beta.iter().filter(|p| (**p - 500).abs() <= 10).count() as f64 / beta.len() as f64;
        assert!((within - 0.68).abs() < 0.04, "normal within 1σ: {within}");

        // Poisson arrivals: increasing, with a mean gap of 1/rate (10 ms).
        assert!(
            commands
                .windows(2)
                .all(|w| w[0].at_micros <= w[1].at_micros)
        );
        let mean_gap = commands.last().unwrap().at_micros as f64 / commands.len() as f64;
        assert!(
            (mean_gap - 10_000.0).abs() < 300.0,
            "mean gap {mean_gap} µs"
        );
        // Exponential gaps have a standard deviation equal to their mean; evenly spaced ones would have none.
        let gaps: Vec<f64> = commands
            .windows(2)
            .map(|w| (w[1].at_micros - w[0].at_micros) as f64)
            .collect();
        let variance = gaps.iter().map(|g| (g - mean_gap).powi(2)).sum::<f64>() / gaps.len() as f64;
        let cv = variance.sqrt() / mean_gap;
        assert!((cv - 1.0).abs() < 0.05, "coefficient of variation {cv}");
    }

    #[test]
    fn cancels_name_earlier_limit_orders_once() {
        let config = config(BASIC);
        let commands: Vec<_> = config.generate().collect();
        let mut submitted = Vec::new();
        let mut cancelled = std::collections::HashSet::new();
        for command in &commands {
            match &command.action {
                MarketAction::Submit(order) => submitted.push(order.order_type),
                MarketAction::Cancel { submit_index } => {
                    assert_eq!(submitted.get(*submit_index), Some(&OrderType::Limit));
                    assert!(cancelled.insert(*submit_index), "cancelled twice");
                }
            }
        }
        assert!(!cancelled.is_empty());
    }

    #[test]
    fn no_rate_means_one_instant_and_participants_can_be_named() {
        let config = config(
            "orders: 50\nparticipants: [A, B]\ninstruments: [ { id: X, price: { mid_ticks: 1 , width_ticks: 3 } } ]\n",
        );
        let commands: Vec<_> = config.generate().collect();
        assert!(commands.iter().all(|c| c.at_micros == 0));
        for order in submits(&commands) {
            assert!(["A", "B"].contains(&order.participant_id.0.as_str()));
            assert!(
                order.limit_price_ticks.is_none_or(|p| p >= 1),
                "prices stay positive"
            );
        }
        assert_eq!(Participants::Count(3).ids(), ["P01", "P02", "P03"]);
        assert_eq!(Participants::Count(120).ids()[0], "P001");
    }

    #[test]
    fn invalid_configurations_name_the_field() {
        let base = config(BASIC);
        let broken = |change: &dyn Fn(&mut MarketConfig)| {
            let mut c = base.clone();
            change(&mut c);
            c.validate().unwrap_err()
        };
        assert!(broken(&|c| c.buy_probability = 1.5).contains("buy_probability"));
        assert!(broken(&|c| c.orders_per_second = -1.0).contains("orders_per_second"));
        assert!(broken(&|c| c.instruments.clear()).contains("instruments"));
        assert!(broken(&|c| c.participants = Participants::Count(0)).contains("participants"));
        assert!(
            broken(&|c| c.instruments[0].quantity = QuantityDistribution {
                min: 11,
                max: 19,
                lot: 10
            })
            .contains("ACME: quantity")
        );
        assert!(broken(&|c| c.instruments[1].price.mid_ticks = 0).contains("BETA: price"));
        assert!(
            broken(&|c| {
                c.instruments[0].weight = 0.0;
                c.instruments[1].weight = 0.0;
            })
            .contains("weights")
        );
        let typo = serde_saphyr::from_str::<MarketConfig>("orders: 1\nsede: 2\ninstruments: []\n")
            .unwrap_err();
        assert!(typo.to_string().contains("sede"));
    }
}
