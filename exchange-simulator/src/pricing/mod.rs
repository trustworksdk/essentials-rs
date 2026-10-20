//! Reference prices (plan item 6.2, SR-1 and SR-3): each instrument has a price source that produces a reference
//! price, which the liquidity provider quotes around (6.7) and the market data publishes (6.8).
//!
//! - `none`: no reference price; the instrument trades only on client orders.
//! - `fixed`: a constant price.
//! - `synthetic`: a seeded geometric Brownian motion on simulated time, stepped on a fixed grid from the moment the
//!   source was configured (`since`). Each step multiplies the price by `exp((μ − σ²/2)·Δt + σ·√Δt·Z)`, with `drift`
//!   μ and `volatility` σ annualized (a year of 365.25 simulated days) and `Z` a standard normal from the source's own
//!   stream. The path depends only on the parameters, the seed and `since`, never on when it is read: reading
//!   catches up step by step. Published prices are rounded to the nearest tick and never below one tick; the walk
//!   itself is not rounded.
//!
//! - `finnhub`: live trade prices for the instrument's `referenceSymbol` (else its `symbol`), streamed by
//!   [`finnhub`]. A new price is adopted at most `max_updates_per_second` times a second, the latest winning; the
//!   reference price is stale once nothing has arrived for `stale_after_ms` of real time. Its `sourceTimestamp` is
//!   the provider's. Live prices are not reproducible.
//!
//! An instrument's own source (`priceSource`) may leave fields out; they come from `price_sources.default`, field by
//! field, and then from the built-in defaults (`none`, drift 0, volatility 0.2, a step a second).

pub mod finnhub;

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::{Instrument, InstrumentId};
use simulator_kit::rng::Rng;

/// Simulated seconds in the year `drift` and `volatility` refer to.
const YEAR_SECONDS: f64 = 365.25 * 86_400.0;
const DEFAULT_VOLATILITY: f64 = 0.2;
const DEFAULT_TICK_INTERVAL_MS: u64 = 1_000;
/// The shortest step. A clock jump costs one step per interval it skips, on the exchange thread.
pub const MIN_TICK_INTERVAL_MS: u64 = 100;
const MAX_TICK_INTERVAL_MS: u64 = 86_400_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PriceSourceKind {
    None,
    Fixed,
    Synthetic,
    Finnhub,
}

impl PriceSourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Fixed => "fixed",
            Self::Synthetic => "synthetic",
            Self::Finnhub => "finnhub",
        }
    }
}

/// A price source as configured: on an instrument (`priceSource`, API and configuration) or as the default
/// (`price_sources.default`). Every field is optional; fields the kind does not use are ignored, so switching kinds
/// needs no clean-up. Prices are decimal strings in major currency units, on the instrument's tick grid.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PriceSourceSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<PriceSourceKind>,
    /// `fixed`: the price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    /// `synthetic`: where the walk starts. Left out on a switch to `synthetic`, the current reference price.
    #[serde(
        default,
        alias = "initial_price",
        skip_serializing_if = "Option::is_none"
    )]
    pub initial_price: Option<String>,
    /// `synthetic`: annualized drift μ.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drift: Option<f64>,
    /// `synthetic`: annualized volatility σ.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volatility: Option<f64>,
    /// `synthetic`: simulated milliseconds per step.
    #[serde(
        default,
        alias = "tick_interval_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub tick_interval_ms: Option<u64>,
    /// `synthetic`: the stream's seed. Left out, it derives from `price_sources.seed` and the instrument ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum PriceSourceError {
    #[error("priceSource.{0} must be a finite number")]
    NotFinite(&'static str),
    #[error("priceSource.volatility must not be negative")]
    NegativeVolatility,
    #[error(
        "priceSource.tickIntervalMs must be between {MIN_TICK_INTERVAL_MS} and {MAX_TICK_INTERVAL_MS}"
    )]
    TickInterval,
    #[error("priceSource.{field} is required for kind {kind}")]
    Missing {
        field: &'static str,
        kind: &'static str,
    },
    #[error(
        "priceSource.kind finnhub needs an API key (price_sources.finnhub.api_key_file or FINNHUB_API_KEY)"
    )]
    NoLiveFeed,
    #[error(
        "priceSource.kind finnhub: at most {max} symbols can stream (price_sources.finnhub.max_symbols)"
    )]
    SymbolLimit { max: usize },
    #[error("priceSource.{field}: {error}")]
    Price {
        field: &'static str,
        error: crate::domain::PriceError,
    },
}

impl PriceSourceSpec {
    /// Checks the fields given, whatever the kind: numbers finite, volatility not negative, the step in range.
    /// Prices are checked against the instrument when the source is resolved.
    pub fn validate(&self) -> Result<(), PriceSourceError> {
        for (name, value) in [("drift", self.drift), ("volatility", self.volatility)] {
            if value.is_some_and(|v| !v.is_finite()) {
                return Err(PriceSourceError::NotFinite(name));
            }
        }
        if self.volatility.is_some_and(|v| v < 0.0) {
            return Err(PriceSourceError::NegativeVolatility);
        }
        if self
            .tick_interval_ms
            .is_some_and(|ms| !(MIN_TICK_INTERVAL_MS..=MAX_TICK_INTERVAL_MS).contains(&ms))
        {
            return Err(PriceSourceError::TickInterval);
        }
        Ok(())
    }

    /// This spec with its missing fields taken from `default`.
    pub fn or(&self, default: &Self) -> Self {
        Self {
            kind: self.kind.or(default.kind),
            price: self.price.clone().or_else(|| default.price.clone()),
            initial_price: self
                .initial_price
                .clone()
                .or_else(|| default.initial_price.clone()),
            drift: self.drift.or(default.drift),
            volatility: self.volatility.or(default.volatility),
            tick_interval_ms: self.tick_interval_ms.or(default.tick_interval_ms),
            seed: self.seed.or(default.seed),
        }
    }
}

/// A reference price at a point in time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Reference {
    pub kind: PriceSourceKind,
    pub ticks: i64,
    /// When the price was produced: when a fixed source was configured, or the synthetic step's grid time.
    pub source_timestamp: DateTime<Utc>,
    /// Nothing new from a live source for longer than `stale_after`; never for `fixed` and `synthetic`.
    pub stale: bool,
}

/// The live feed and how its prices are read (`price_sources.finnhub`).
#[derive(Clone, Debug)]
pub struct LiveSource {
    pub feed: finnhub::LiveFeed,
    /// Real time without an update after which the reference price is stale.
    pub stale_after: std::time::Duration,
    /// The least real time between two adopted prices of one instrument (`1 / max_updates_per_second`).
    pub min_interval: std::time::Duration,
    pub max_symbols: usize,
}

/// A live price taken in as the reference price.
#[derive(Clone, Copy, Debug)]
struct Adopted {
    ticks: i64,
    source_timestamp: DateTime<Utc>,
    /// When it was taken in, for conflation.
    at: std::time::Instant,
    /// When the feed received it, to tell a newer price.
    received: std::time::Instant,
}

/// The parameters of a synthetic walk, resolved.
#[derive(Clone, Debug, PartialEq)]
struct Synthetic {
    initial_ticks: f64,
    drift: f64,
    volatility: f64,
    interval: Duration,
    seed: u64,
}

#[derive(Clone, Debug)]
enum State {
    None,
    Fixed {
        ticks: i64,
    },
    Synthetic {
        params: Synthetic,
        rng: Rng,
        /// The walk in ticks, unrounded.
        ticks: f64,
        step: u64,
    },
    Live {
        symbol: String,
        /// Ticks per major currency unit.
        ticks_per_unit: f64,
        adopted: Option<Adopted>,
    },
}

#[derive(Clone, Debug)]
struct Active {
    since: DateTime<Utc>,
    state: State,
}

/// The running price sources of every listed instrument.
#[derive(Clone, Debug)]
pub struct PriceSources {
    default: PriceSourceSpec,
    seed: u64,
    active: HashMap<InstrumentId, Active>,
    /// `None` without an API key: `finnhub` sources are then refused.
    live: Option<LiveSource>,
}

impl PriceSources {
    /// Sources with `default` filling in what an instrument's own source leaves out, and `seed` the base of the
    /// derived synthetic seeds.
    pub fn new(default: PriceSourceSpec, seed: u64) -> Result<Self, PriceSourceError> {
        default.validate()?;
        Ok(Self {
            default,
            seed,
            active: HashMap::new(),
            live: None,
        })
    }

    /// These sources with `finnhub` prices from `live`.
    pub fn with_live(mut self, live: LiveSource) -> Self {
        self.live = Some(live);
        self
    }

    /// Whether `instrument`'s source, resolved, is a live one, so a new `referenceSymbol` must restart it.
    pub fn is_live(&self, instrument: &Instrument) -> bool {
        self.merged(instrument).kind == Some(PriceSourceKind::Finnhub)
    }

    /// Makes `instrument`'s own source ready to store, given its `current` reference price: validates it and, when it
    /// resolves to `synthetic` with no initial price anywhere, fills in `current` as its initial price, so the stored
    /// source reproduces the walk. Fails when the source cannot be resolved.
    pub fn prepare(
        &self,
        instrument: &mut Instrument,
        current: Option<Reference>,
    ) -> Result<(), PriceSourceError> {
        if let Some(own) = &instrument.price_source {
            own.validate()?;
        }
        let merged = self.merged(instrument);
        if merged.kind == Some(PriceSourceKind::Synthetic)
            && merged.initial_price.is_none()
            && let Some(current) = current
        {
            instrument
                .price_source
                .get_or_insert_with(Default::default)
                .initial_price = Some(instrument.ticks_to_price(current.ticks));
        }
        self.resolve(instrument).map(drop)
    }

    /// Starts (or restarts) `instrument`'s source with its grid at `since`.
    pub fn start(
        &mut self,
        instrument: &Instrument,
        since: DateTime<Utc>,
    ) -> Result<(), PriceSourceError> {
        let state = match self.resolve(instrument)? {
            Resolved::None => State::None,
            Resolved::Fixed(ticks) => State::Fixed { ticks },
            Resolved::Synthetic(params) => State::Synthetic {
                rng: Rng::new(params.seed),
                ticks: params.initial_ticks,
                step: 0,
                params,
            },
            Resolved::Live {
                symbol,
                ticks_per_unit,
            } => State::Live {
                symbol,
                ticks_per_unit,
                adopted: None,
            },
        };
        self.active
            .insert(instrument.id.clone(), Active { since, state });
        self.sync_wanted();
        Ok(())
    }

    /// Tells the feed which symbols the live sources need.
    fn sync_wanted(&self) {
        if let Some(live) = &self.live {
            live.feed.set_wanted(self.live_symbols(None));
        }
    }

    /// The symbols of the running live sources, `except` one instrument's.
    fn live_symbols(&self, except: Option<&InstrumentId>) -> BTreeSet<String> {
        self.active
            .iter()
            .filter(|(id, _)| Some(*id) != except)
            .filter_map(|(_, active)| match &active.state {
                State::Live { symbol, .. } => Some(symbol.clone()),
                _ => None,
            })
            .collect()
    }

    /// The instrument's reference price at `now`, catching a synthetic walk up to the last step at or before `now`;
    /// `None` for an unknown instrument or a `none` source.
    pub fn reference(&mut self, id: &InstrumentId, now: DateTime<Utc>) -> Option<Reference> {
        let live = self.live.clone();
        read(self.active.get_mut(id)?, live, now)
    }

    /// The instrument's reference price at `at`, which may be in the past, without moving the source: a synthetic
    /// walk already beyond `at` is replayed from its start, so the answer does not depend on when it was read before
    /// (closing prices, SR-14). A live source can only answer with its latest price.
    pub fn reference_at(&self, id: &InstrumentId, at: DateTime<Utc>) -> Option<Reference> {
        let mut active = self.active.get(id)?.clone();
        if let State::Synthetic {
            params,
            rng,
            ticks,
            step,
        } = &mut active.state
            && *step > steps_until(params, active.since, at)
        {
            *rng = Rng::new(params.seed);
            *ticks = params.initial_ticks;
            *step = 0;
        }
        read(&mut active, self.live.clone(), at)
    }

    /// Forgets every source (after a reset).
    pub fn clear(&mut self) {
        self.active.clear();
        self.sync_wanted();
    }

    fn merged(&self, instrument: &Instrument) -> PriceSourceSpec {
        instrument
            .price_source
            .clone()
            .unwrap_or_default()
            .or(&self.default)
    }

    fn resolve(&self, instrument: &Instrument) -> Result<Resolved, PriceSourceError> {
        let spec = self.merged(instrument);
        let price = |field: &'static str, value: &Option<String>, kind: &'static str| {
            let value = value
                .as_deref()
                .ok_or(PriceSourceError::Missing { field, kind })?;
            instrument
                .price_to_ticks(value)
                .map_err(|error| PriceSourceError::Price { field, error })
        };
        Ok(match spec.kind.unwrap_or(PriceSourceKind::None) {
            PriceSourceKind::None => Resolved::None,
            PriceSourceKind::Fixed => Resolved::Fixed(price("price", &spec.price, "fixed")?),
            PriceSourceKind::Synthetic => Resolved::Synthetic(Synthetic {
                initial_ticks: price("initialPrice", &spec.initial_price, "synthetic")? as f64,
                drift: spec.drift.unwrap_or(0.0),
                volatility: spec.volatility.unwrap_or(DEFAULT_VOLATILITY),
                interval: Duration::milliseconds(
                    i64::try_from(spec.tick_interval_ms.unwrap_or(DEFAULT_TICK_INTERVAL_MS))
                        .expect("validated interval fits i64"),
                ),
                seed: spec
                    .seed
                    .unwrap_or_else(|| derived_seed(self.seed, &instrument.id)),
            }),
            PriceSourceKind::Finnhub => {
                let live = self.live.as_ref().ok_or(PriceSourceError::NoLiveFeed)?;
                let symbol = instrument
                    .reference_symbol
                    .clone()
                    .unwrap_or_else(|| instrument.symbol.clone());
                let mut symbols = self.live_symbols(Some(&instrument.id));
                symbols.insert(symbol.clone());
                if symbols.len() > live.max_symbols {
                    return Err(PriceSourceError::SymbolLimit {
                        max: live.max_symbols,
                    });
                }
                Resolved::Live {
                    symbol,
                    ticks_per_unit: 10f64.powi(i32::from(instrument.currency_scale))
                        / instrument.tick_size_minor as f64,
                }
            }
        })
    }
}

/// Evaluates one source at `now`, catching a synthetic walk up to the last step at or before it.
fn read(active: &mut Active, live: Option<LiveSource>, now: DateTime<Utc>) -> Option<Reference> {
    match &mut active.state {
        State::None => None,
        State::Fixed { ticks } => Some(Reference {
            kind: PriceSourceKind::Fixed,
            ticks: *ticks,
            source_timestamp: active.since,
            stale: false,
        }),
        State::Synthetic {
            params,
            rng,
            ticks,
            step,
        } => {
            let interval_ms = params.interval.num_milliseconds();
            let target = steps_until(params, active.since, now);
            let dt = interval_ms as f64 / 1_000.0 / YEAR_SECONDS;
            let mean = (params.drift - params.volatility * params.volatility / 2.0) * dt;
            let scale = params.volatility * dt.sqrt();
            while *step < target {
                *ticks *= (mean + scale * rng.normal()).exp();
                *step += 1;
            }
            let offset = i64::try_from(*step)
                .ok()
                .and_then(|step| step.checked_mul(interval_ms))
                .map_or(Duration::MAX, Duration::milliseconds);
            let at = active.since + offset;
            Some(Reference {
                kind: PriceSourceKind::Synthetic,
                ticks: publish(*ticks),
                source_timestamp: at,
                stale: false,
            })
        }
        State::Live {
            symbol,
            ticks_per_unit,
            adopted,
        } => {
            let live = live?;
            let latest = live.feed.latest(symbol);
            if let Some(latest) = latest {
                let newer = adopted.is_none_or(|a| {
                    a.received != latest.received && a.at.elapsed() >= live.min_interval
                });
                if newer {
                    *adopted = Some(Adopted {
                        ticks: publish(latest.price * *ticks_per_unit),
                        source_timestamp: latest.source_timestamp,
                        at: std::time::Instant::now(),
                        received: latest.received,
                    });
                }
            }
            let adopted = (*adopted)?;
            let last_heard = latest.map_or(adopted.received, |l| l.received);
            Some(Reference {
                kind: PriceSourceKind::Finnhub,
                ticks: adopted.ticks,
                source_timestamp: adopted.source_timestamp,
                stale: last_heard.elapsed() > live.stale_after,
            })
        }
    }
}

/// The whole steps of a synthetic walk started at `since` by `at`.
fn steps_until(params: &Synthetic, since: DateTime<Utc>, at: DateTime<Utc>) -> u64 {
    let elapsed_ms = (at - since).num_milliseconds().max(0);
    u64::try_from(elapsed_ms / params.interval.num_milliseconds()).unwrap_or(0)
}

enum Resolved {
    None,
    Fixed(i64),
    Synthetic(Synthetic),
    Live { symbol: String, ticks_per_unit: f64 },
}

/// Rounds the walk to the nearest tick, never below one tick.
fn publish(ticks: f64) -> i64 {
    let rounded = ticks.round();
    if rounded >= i64::MAX as f64 {
        i64::MAX
    } else {
        (rounded as i64).max(1)
    }
}

/// A seed per instrument: FNV-1a over the instrument ID, mixed with the base seed. Stable across Rust versions, and
/// independent of which other instruments exist.
fn derived_seed(base: u64, id: &InstrumentId) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in base.to_le_bytes().iter().chain(id.0.as_bytes()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acme(price_source: PriceSourceSpec) -> Instrument {
        Instrument {
            id: InstrumentId("ACME".into()),
            symbol: "ACME".into(),
            mic: None,
            isin: None,
            cusip: None,
            currency: "USD".into(),
            currency_scale: 2,
            tick_size_minor: 1,
            lot_size: 1,
            enabled: true,
            reference_symbol: None,
            price_source: Some(price_source),
            liquidity: None,
        }
    }

    fn synthetic(volatility: f64) -> PriceSourceSpec {
        PriceSourceSpec {
            kind: Some(PriceSourceKind::Synthetic),
            initial_price: Some("100.00".into()),
            volatility: Some(volatility),
            ..Default::default()
        }
    }

    fn t0() -> DateTime<Utc> {
        "2030-01-07T09:00:00Z".parse().unwrap()
    }

    fn path(sources: &mut PriceSources, reads: &[i64]) -> Vec<i64> {
        let id = InstrumentId("ACME".into());
        reads
            .iter()
            .map(|s| {
                sources
                    .reference(&id, t0() + Duration::seconds(*s))
                    .unwrap()
                    .ticks
            })
            .collect()
    }

    #[test]
    fn a_past_reference_is_the_same_however_far_the_walk_has_gone() {
        let instrument = acme(synthetic(0.8));
        let mut ahead = PriceSources::new(Default::default(), 7).unwrap();
        ahead.start(&instrument, t0()).unwrap();
        let mut fresh = ahead.clone();
        let id = InstrumentId("ACME".into());
        let later = ahead.reference(&id, t0() + Duration::hours(5)).unwrap();
        let close = t0() + Duration::hours(2) + Duration::milliseconds(1_500);
        assert_eq!(
            ahead.reference_at(&id, close),
            fresh.reference(&id, close),
            "replayed from the start"
        );
        assert_eq!(
            ahead.reference(&id, t0() + Duration::hours(5)).unwrap(),
            later,
            "and the walk itself did not move"
        );
        let mut third = PriceSources::new(Default::default(), 7).unwrap();
        third.start(&instrument, t0()).unwrap();
        let onward = close + Duration::hours(1);
        assert_eq!(
            fresh.reference_at(&id, onward),
            third.reference(&id, onward),
            "forward of the walk, it steps on in a copy"
        );
    }

    #[test]
    fn the_walk_does_not_depend_on_when_it_is_read() {
        let mut often = PriceSources::new(Default::default(), 7).unwrap();
        let mut once = often.clone();
        let instrument = acme(synthetic(0.8));
        often.start(&instrument, t0()).unwrap();
        once.start(&instrument, t0()).unwrap();
        let reads: Vec<i64> = (0..=3_600).step_by(7).collect();
        let last = *path(&mut often, &reads).last().unwrap();
        assert_eq!(path(&mut once, &[3_598]), [last]);
        assert_ne!(last, 10_000, "an hour at σ = 0.8 moves the price");
    }

    #[test]
    fn steps_are_on_a_grid_from_since() {
        let mut sources = PriceSources::new(Default::default(), 7).unwrap();
        sources.start(&acme(synthetic(0.2)), t0()).unwrap();
        let id = InstrumentId("ACME".into());
        let at = |ms| {
            sources
                .clone()
                .reference(&id, t0() + Duration::milliseconds(ms))
                .unwrap()
                .source_timestamp
        };
        assert_eq!(at(0), t0());
        assert_eq!(at(999), t0());
        assert_eq!(at(2_500), t0() + Duration::seconds(2));
    }

    #[test]
    fn seeds_derive_per_instrument_unless_given() {
        let start = |seed: u64, own: Option<u64>, id: &str| {
            let mut sources = PriceSources::new(Default::default(), seed).unwrap();
            let mut instrument = acme(PriceSourceSpec {
                seed: own,
                ..synthetic(1.0)
            });
            instrument.id = InstrumentId(id.into());
            sources.start(&instrument, t0()).unwrap();
            sources
                .reference(&instrument.id, t0() + Duration::hours(1))
                .unwrap()
                .ticks
        };
        assert_eq!(start(1, None, "ACME"), start(1, None, "ACME"));
        assert_ne!(start(1, None, "ACME"), start(2, None, "ACME"));
        assert_ne!(start(1, None, "ACME"), start(1, None, "BETA"));
        assert_eq!(start(1, Some(9), "ACME"), start(2, Some(9), "BETA"));
    }

    #[test]
    fn prices_stay_on_the_grid_and_above_zero() {
        let mut sources = PriceSources::new(Default::default(), 3).unwrap();
        let mut instrument = acme(PriceSourceSpec {
            initial_price: Some("0.02".into()),
            drift: Some(-5_000.0),
            ..synthetic(5.0)
        });
        instrument.tick_size_minor = 1;
        sources.start(&instrument, t0()).unwrap();
        let prices = path(&mut sources, &(0..86_400).step_by(600).collect::<Vec<_>>());
        assert!(prices.iter().all(|p| *p >= 1));
        assert_eq!(
            *prices.last().unwrap(),
            1,
            "a strong negative drift ends at one tick"
        );
        assert_eq!(publish(2.5), 3);
        assert_eq!(publish(0.2), 1);
        assert_eq!(publish(f64::INFINITY), i64::MAX);
    }

    #[test]
    fn zero_volatility_and_drift_hold_the_price() {
        let mut sources = PriceSources::new(Default::default(), 3).unwrap();
        sources.start(&acme(synthetic(0.0)), t0()).unwrap();
        assert_eq!(path(&mut sources, &[0, 60, 86_400]), [10_000; 3]);
    }

    #[test]
    fn missing_fields_come_from_the_default() {
        let default = PriceSourceSpec {
            kind: Some(PriceSourceKind::Synthetic),
            volatility: Some(0.3),
            tick_interval_ms: Some(500),
            ..Default::default()
        };
        let mut sources = PriceSources::new(default, 1).unwrap();
        let mut instrument = acme(PriceSourceSpec {
            initial_price: Some("50.00".into()),
            ..Default::default()
        });
        sources.prepare(&mut instrument, None).unwrap();
        sources.start(&instrument, t0()).unwrap();
        let reference = sources
            .reference(&instrument.id, t0() + Duration::milliseconds(1_200))
            .unwrap();
        assert_eq!(reference.kind, PriceSourceKind::Synthetic);
        assert_eq!(
            reference.source_timestamp,
            t0() + Duration::milliseconds(1_000)
        );

        let mut fixed = acme(PriceSourceSpec {
            kind: Some(PriceSourceKind::Fixed),
            price: Some("12.34".into()),
            ..Default::default()
        });
        sources.start(&fixed, t0()).unwrap();
        assert_eq!(sources.reference(&fixed.id, t0()).unwrap().ticks, 1_234);
        fixed.price_source = None;
        assert_eq!(
            sources.prepare(&mut fixed, None),
            Err(PriceSourceError::Missing {
                field: "initialPrice",
                kind: "synthetic"
            }),
            "the default's kind applies, and it needs an initial price"
        );
    }

    #[test]
    fn a_switch_to_synthetic_starts_from_the_current_price() {
        let sources = PriceSources::new(Default::default(), 1).unwrap();
        let mut instrument = acme(PriceSourceSpec {
            kind: Some(PriceSourceKind::Synthetic),
            ..Default::default()
        });
        let current = Reference {
            kind: PriceSourceKind::Fixed,
            ticks: 12_345,
            source_timestamp: t0(),
            stale: false,
        };
        sources.prepare(&mut instrument, Some(current)).unwrap();
        assert_eq!(
            instrument.price_source.unwrap().initial_price.as_deref(),
            Some("123.45")
        );
    }

    #[test]
    fn invalid_sources_are_rejected() {
        let sources = PriceSources::new(Default::default(), 1).unwrap();
        let check = |spec: PriceSourceSpec| sources.prepare(&mut acme(spec), None).unwrap_err();
        assert_eq!(
            check(PriceSourceSpec {
                volatility: Some(-0.1),
                ..synthetic(0.0)
            }),
            PriceSourceError::NegativeVolatility
        );
        assert_eq!(
            check(PriceSourceSpec {
                drift: Some(f64::NAN),
                ..synthetic(0.0)
            }),
            PriceSourceError::NotFinite("drift")
        );
        assert_eq!(
            check(PriceSourceSpec {
                tick_interval_ms: Some(99),
                ..synthetic(0.0)
            }),
            PriceSourceError::TickInterval
        );
        assert_eq!(
            check(PriceSourceSpec {
                kind: Some(PriceSourceKind::Fixed),
                price: Some("1.005".into()),
                ..Default::default()
            }),
            PriceSourceError::Price {
                field: "price",
                error: crate::domain::PriceError::Scale(2)
            }
        );
        assert_eq!(
            check(PriceSourceSpec {
                kind: Some(PriceSourceKind::Fixed),
                ..Default::default()
            }),
            PriceSourceError::Missing {
                field: "price",
                kind: "fixed"
            }
        );
        assert!(PriceSources::new(synthetic(-1.0), 1).is_err());
    }
}
