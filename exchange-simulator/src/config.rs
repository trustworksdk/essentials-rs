//! Application configuration: compiled-in defaults, an optional YAML file, then environment overrides.

use serde::Deserialize;

use std::collections::BTreeMap;
use std::time::Duration;

use crate::domain::IdentifierPolicy;
use crate::ids::IdScheme;
pub use crate::persistence::PersistenceMode;
use crate::settlement::SettlementPolicy;
pub use simulator_kit::clock::{ClockConfig, ClockMode};
pub use simulator_kit::webhooks::{SubscriptionConfig, WebhooksConfig};

const DEFAULTS: &str = include_str!("../config/default.yaml");

/// Environment variable naming an optional YAML file layered over the defaults.
pub const CONFIG_FILE_ENV: &str = "EXCHANGE_CONFIG";

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub exchange: ExchangeConfig,
    #[serde(default)]
    pub events: EventsConfig,
    pub webhooks: WebhooksConfig,
    /// Instruments listed at startup and after a reset (plan item 6.1).
    #[serde(default)]
    pub instruments: Vec<InstrumentConfig>,
    /// Reference prices (plan item 6.2).
    #[serde(default)]
    pub price_sources: PriceSourcesConfig,
    /// The built-in liquidity provider (plan item 6.7).
    #[serde(default)]
    pub liquidity: LiquidityConfig,
    /// The market-data stream (plan item 6.8).
    #[serde(default)]
    pub market_data: MarketDataConfig,
    pub ids: IdsConfig,
    pub settlement: SettlementConfig,
    #[serde(default)]
    pub clearing: ClearingConfig,
    pub clock: ClockConfig,
    pub test_control: TestControlConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ExchangeConfig {
    /// `strict` validates MIC/ISIN/CUSIP formats and check digits at registration; `permissive` checks lengths only.
    pub identifier_policy: IdentifierPolicy,
    /// Bound of the exchange command queue; a full queue rejects requests with 503.
    pub command_queue_capacity: usize,
    #[serde(default)]
    pub group_commit: GroupCommitConfig,
}

/// How orders and cancellations are grouped into one commit (see [`crate::engine::GroupCommit`]).
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default)]
pub struct GroupCommitConfig {
    /// Most commands per commit; 1 commits every command on its own.
    pub max_commands: usize,
    /// How long to wait for more commands once the queue is empty; 0 groups only what is already queued.
    pub max_wait_us: u64,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_commands: 256,
            max_wait_us: 0,
        }
    }
}

impl GroupCommitConfig {
    pub fn policy(&self) -> crate::engine::GroupCommit {
        crate::engine::GroupCommit {
            max_commands: self.max_commands.max(1),
            max_wait: Duration::from_micros(self.max_wait_us),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct IdsConfig {
    pub mode: IdMode,
    #[serde(default)]
    pub seed: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum IdMode {
    #[default]
    Random,
    Deterministic,
}

impl IdsConfig {
    pub fn scheme(&self) -> IdScheme {
        match self.mode {
            IdMode::Random => IdScheme::Random,
            IdMode::Deterministic => IdScheme::Deterministic { seed: self.seed },
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct SettlementConfig {
    pub mode: SettlementMode,
    pub tick_interval_ms: u64,
    pub seed: u64,
    pub settlement_days: u32,
    /// Settlement cycles per market (MIC). Keys are matched in uppercase, since environment variables arrive
    /// lowercased.
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

impl Default for SettlementConfig {
    /// External mode with the shipped simulation defaults.
    fn default() -> Self {
        let policy = SettlementPolicy {
            fail_probability: 0.02,
            delay_probability: 0.05,
            ..SettlementPolicy::default()
        };
        Self {
            mode: SettlementMode::External,
            tick_interval_ms: 250,
            seed: policy.seed,
            settlement_days: policy.settlement_days,
            market_settlement_days: policy.market_settlement_days,
            reject_probability: policy.reject_probability,
            cancel_probability: policy.cancel_probability,
            fail_probability: policy.fail_probability,
            delay_probability: policy.delay_probability,
            correct_probability: policy.correct_probability,
            max_delay_days: policy.max_delay_days,
            max_delays: policy.max_delays,
            correction_days: policy.correction_days,
        }
    }
}

impl SettlementConfig {
    /// The simulator's policy, when this mode runs the simulator.
    pub fn simulator_policy(&self) -> Option<SettlementPolicy> {
        matches!(self.mode, SettlementMode::Simulated | SettlementMode::Both).then(|| self.policy())
    }

    /// The configured policy, whether or not the simulator runs: clearing takes the settlement cycles from it.
    pub fn policy(&self) -> SettlementPolicy {
        {
            SettlementPolicy {
                seed: self.seed,
                settlement_days: self.settlement_days,
                market_settlement_days: self
                    .market_settlement_days
                    .iter()
                    .map(|(mic, days)| (mic.to_ascii_uppercase(), *days))
                    .collect(),
                clearing: None,
                reject_probability: self.reject_probability,
                cancel_probability: self.cancel_probability,
                fail_probability: self.fail_probability,
                delay_probability: self.delay_probability,
                correct_probability: self.correct_probability,
                max_delay_days: self.max_delay_days,
                max_delays: self.max_delays,
                correction_days: self.correction_days,
            }
        }
    }
}

/// End-of-day clearing statements (`clearing:`), off by default.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClearingConfig {
    pub enabled: bool,
    /// Local time of the daily cutoff, `HH:MM`.
    pub cutoff: String,
    /// IANA time zone of the cutoff, e.g. `Europe/Copenhagen`.
    pub time_zone: String,
    pub breaks: crate::clearing::BreakPolicy,
}

impl Default for ClearingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cutoff: "17:30".into(),
            time_zone: "UTC".into(),
            breaks: crate::clearing::BreakPolicy::default(),
        }
    }
}

impl Config {
    /// The declared instruments, in order; an error when an ID is declared twice. Field values are validated when
    /// the exchange lists them, as for the API.
    pub fn declared_instruments(&self) -> Result<Vec<crate::domain::Instrument>, String> {
        let mut seen = std::collections::BTreeSet::new();
        self.instruments
            .iter()
            .map(|c| {
                if seen.insert(c.id.as_str()) {
                    Ok(c.into())
                } else {
                    Err(format!("instruments: {} is declared twice", c.id))
                }
            })
            .collect()
    }

    /// The business-day calendar: `clearing.cutoff` in `clearing.time_zone`, on weekdays. Business days close on it
    /// (`BusinessDayClosed`) whether or not clearing is enabled.
    pub fn business_calendar(&self) -> Result<crate::clearing::ClearingCalendar, String> {
        crate::clearing::ClearingCalendar::parse(&self.clearing.cutoff, &self.clearing.time_zone)
            .map_err(|e| format!("clearing.{e}"))
    }

    /// The clearinghouse's policy when clearing is enabled; an error for an invalid cutoff, time zone or break
    /// policy.
    pub fn clearing_policy(&self) -> Result<Option<crate::clearing::ClearingPolicy>, String> {
        if !self.clearing.enabled {
            return Ok(None);
        }
        let calendar = self.business_calendar()?;
        self.clearing.breaks.validate()?;
        Ok(Some(crate::clearing::ClearingPolicy {
            calendar,
            breaks: self.clearing.breaks.clone(),
            cycles: self.settlement.policy(),
        }))
    }
}

/// Who owns settlement outcomes.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SettlementMode {
    /// No simulation: an external service settles trades (and may report outcomes back).
    External,
    /// The built-in simulator only.
    Simulated,
    /// The simulator and externally reported outcomes, side by side.
    Both,
}

/// The market-data stream (see [`crate::market_data`]).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataConfig {
    /// Per subscriber, per instrument and message kind, at most one message this often; 0 sends every message.
    #[serde(default)]
    pub conflation_interval_ms: u64,
    /// Messages a subscriber may fall behind before its stream is closed.
    #[serde(default = "stream_buffer")]
    pub stream_buffer: usize,
}

fn stream_buffer() -> usize {
    1024
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        Self {
            conflation_interval_ms: 0,
            stream_buffer: stream_buffer(),
        }
    }
}

/// The built-in liquidity provider (see [`crate::liquidity`]).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiquidityConfig {
    /// The participant it trades as.
    #[serde(default = "provider_id")]
    pub participant_id: String,
    /// Publish its order lifecycle in the event log and over webhooks; its trades are published either way.
    #[serde(default)]
    pub emit_order_events: bool,
    /// What an instrument's own `liquidity` leaves out, field by field.
    #[serde(default)]
    pub default: crate::liquidity::LiquiditySpec,
}

fn provider_id() -> String {
    "LP".into()
}

impl Default for LiquidityConfig {
    fn default() -> Self {
        Self {
            participant_id: provider_id(),
            emit_order_events: false,
            default: Default::default(),
        }
    }
}

impl LiquidityConfig {
    pub fn policy(&self) -> Result<crate::engine::LiquidityPolicy, String> {
        if self.participant_id.trim().is_empty() {
            return Err("liquidity.participant_id must not be blank".into());
        }
        self.default
            .validate()
            .map_err(|e| format!("liquidity.default: {e}"))?;
        Ok(crate::engine::LiquidityPolicy {
            participant: crate::domain::ParticipantId(self.participant_id.clone()),
            emit_order_events: self.emit_order_events,
            default: self.default.clone(),
        })
    }
}

/// Defaults for instruments' price sources (see [`crate::pricing`]).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceSourcesConfig {
    /// The base of each instrument's derived synthetic seed.
    #[serde(default)]
    pub seed: u64,
    /// What an instrument's own price source leaves out, field by field.
    #[serde(default)]
    pub default: crate::pricing::PriceSourceSpec,
    /// The Finnhub live source (plan item 6.9).
    #[serde(default)]
    pub finnhub: FinnhubConfig,
}

impl PriceSourcesConfig {
    pub fn sources(&self) -> Result<crate::pricing::PriceSources, String> {
        crate::pricing::PriceSources::new(self.default.clone(), self.seed)
            .map_err(|e| format!("price_sources.default: {e}"))
    }
}

/// The Finnhub live source (see [`crate::pricing::finnhub`]). It runs when an API key is found: in the environment
/// variable `FINNHUB_API_KEY` (how the compose files deliver it), or in the file `api_key_file`, which wins when set. The limits default to Finnhub's free tier
/// (checked 2026-10-19: 60 REST calls a minute, 50 websocket symbols).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinnhubConfig {
    #[serde(default)]
    pub api_key_file: Option<String>,
    #[serde(default = "finnhub_ws_url")]
    pub ws_url: String,
    #[serde(default = "finnhub_rest_url")]
    pub rest_url: String,
    /// Real time without an update after which a reference price is stale.
    #[serde(default = "finnhub_stale_after_ms")]
    pub stale_after_ms: u64,
    /// Real time without an update after which a symbol is refreshed over REST.
    #[serde(default = "finnhub_refresh_after_ms")]
    pub refresh_after_ms: u64,
    /// Reference price updates adopted per instrument and second, at most; the latest price wins.
    #[serde(default = "finnhub_max_updates_per_second")]
    pub max_updates_per_second: u32,
    #[serde(default = "finnhub_max_symbols")]
    pub max_symbols: usize,
    #[serde(default = "finnhub_rest_calls_per_minute")]
    pub rest_calls_per_minute: u32,
    #[serde(default = "finnhub_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "finnhub_reconnect_initial_ms")]
    pub reconnect_initial_ms: u64,
    #[serde(default = "finnhub_reconnect_max_ms")]
    pub reconnect_max_ms: u64,
}

fn finnhub_ws_url() -> String {
    "wss://ws.finnhub.io".into()
}
fn finnhub_rest_url() -> String {
    "https://finnhub.io/api/v1".into()
}
fn finnhub_stale_after_ms() -> u64 {
    120_000
}
fn finnhub_refresh_after_ms() -> u64 {
    60_000
}
fn finnhub_max_updates_per_second() -> u32 {
    4
}
fn finnhub_max_symbols() -> usize {
    50
}
fn finnhub_rest_calls_per_minute() -> u32 {
    60
}
fn finnhub_request_timeout_ms() -> u64 {
    10_000
}
fn finnhub_reconnect_initial_ms() -> u64 {
    1_000
}
fn finnhub_reconnect_max_ms() -> u64 {
    60_000
}

impl Default for FinnhubConfig {
    fn default() -> Self {
        Self {
            api_key_file: None,
            ws_url: finnhub_ws_url(),
            rest_url: finnhub_rest_url(),
            stale_after_ms: finnhub_stale_after_ms(),
            refresh_after_ms: finnhub_refresh_after_ms(),
            max_updates_per_second: finnhub_max_updates_per_second(),
            max_symbols: finnhub_max_symbols(),
            rest_calls_per_minute: finnhub_rest_calls_per_minute(),
            request_timeout_ms: finnhub_request_timeout_ms(),
            reconnect_initial_ms: finnhub_reconnect_initial_ms(),
            reconnect_max_ms: finnhub_reconnect_max_ms(),
        }
    }
}

/// A live source ready to run: what the exchange reads, and what the feed task needs.
pub struct LiveConfig {
    pub source: crate::pricing::LiveSource,
    pub settings: crate::pricing::finnhub::FeedSettings,
    pub key: crate::pricing::finnhub::ApiKey,
}

impl FinnhubConfig {
    /// The live source, or `None` when no API key is found. A key file that cannot be read, or settings that
    /// contradict each other, are errors.
    pub fn live(&self) -> Result<Option<LiveConfig>, String> {
        self.live_with(|name| std::env::var(name).ok())
    }

    /// [`Self::live`] with `env` for the environment.
    fn live_with(
        &self,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<LiveConfig>, String> {
        let key = match &self.api_key_file {
            Some(path) => Some(
                std::fs::read_to_string(path)
                    .map_err(|e| format!("price_sources.finnhub.api_key_file {path}: {e}"))?,
            ),
            None => env("FINNHUB_API_KEY"),
        };
        let Some(key) = key.and_then(crate::pricing::finnhub::ApiKey::new) else {
            return Ok(None);
        };
        if self.max_updates_per_second == 0
            || self.rest_calls_per_minute == 0
            || self.max_symbols == 0
        {
            return Err(
                "price_sources.finnhub: max_updates_per_second, rest_calls_per_minute and max_symbols must be at least 1"
                    .into(),
            );
        }
        if self.stale_after_ms <= self.refresh_after_ms {
            return Err(
                "price_sources.finnhub.stale_after_ms must be longer than refresh_after_ms, or a closed market's prices \
                 go stale between refreshes"
                    .into(),
            );
        }
        let ms = Duration::from_millis;
        Ok(Some(LiveConfig {
            source: crate::pricing::LiveSource {
                feed: Default::default(),
                stale_after: ms(self.stale_after_ms),
                min_interval: Duration::from_secs(1) / self.max_updates_per_second,
                max_symbols: self.max_symbols,
            },
            settings: crate::pricing::finnhub::FeedSettings {
                ws_url: self.ws_url.clone(),
                rest_url: self.rest_url.clone(),
                refresh_after: ms(self.refresh_after_ms),
                rest_calls_per_minute: self.rest_calls_per_minute,
                request_timeout: ms(self.request_timeout_ms),
                reconnect_initial: ms(self.reconnect_initial_ms.max(1)),
                reconnect_max: ms(self.reconnect_max_ms.max(self.reconnect_initial_ms.max(1))),
            },
            key,
        }))
    }
}

/// An instrument declared in configuration: the fields of `POST /api/instruments`, in snake_case.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentConfig {
    pub id: String,
    pub symbol: String,
    #[serde(default)]
    pub mic: Option<String>,
    #[serde(default)]
    pub isin: Option<String>,
    #[serde(default)]
    pub cusip: Option<String>,
    pub currency: String,
    pub currency_scale: u8,
    #[serde(default = "one")]
    pub tick_size_minor: u64,
    #[serde(default = "one")]
    pub lot_size: u64,
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default)]
    pub reference_symbol: Option<String>,
    /// The instrument's own price source; fields left out come from `price_sources.default`.
    #[serde(default)]
    pub price_source: Option<crate::pricing::PriceSourceSpec>,
    /// The instrument's own liquidity settings; fields left out come from `liquidity.default`.
    #[serde(default)]
    pub liquidity: Option<crate::liquidity::LiquiditySpec>,
}

fn one() -> u64 {
    1
}

fn yes() -> bool {
    true
}

impl From<&InstrumentConfig> for crate::domain::Instrument {
    fn from(c: &InstrumentConfig) -> Self {
        Self {
            id: crate::domain::InstrumentId(c.id.clone()),
            symbol: c.symbol.clone(),
            mic: c.mic.clone(),
            isin: c.isin.clone(),
            cusip: c.cusip.clone(),
            currency: c.currency.clone(),
            currency_scale: c.currency_scale,
            tick_size_minor: c.tick_size_minor,
            lot_size: c.lot_size,
            enabled: c.enabled,
            reference_symbol: c.reference_symbol.clone(),
            price_source: c.price_source.clone(),
            liquidity: c.liquidity.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TestControlConfig {
    pub enabled: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct EventsConfig {
    /// Stamped on every event as `simulationId`.
    pub simulation_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DatabaseConfig {
    /// `durable`, `fast` or `memory` (see [`PersistenceMode`]).
    #[serde(default)]
    pub mode: PersistenceMode,
    /// The database file; unused in `memory` mode.
    pub path: String,
    pub busy_timeout_ms: u64,
    /// In `memory` mode, the most the database may grow to, in MiB; beyond it commands fail with a storage error.
    #[serde(default = "default_memory_max_mib")]
    pub memory_max_mib: u64,
    /// Where WAL checkpoints run.
    #[serde(default)]
    pub checkpoint: CheckpointMode,
    /// With background checkpoints: the WAL size, in pages, from which one is requested.
    #[serde(default = "default_checkpoint_pages")]
    pub checkpoint_pages: u32,
}

fn default_checkpoint_pages() -> u32 {
    1000
}

fn default_memory_max_mib() -> u64 {
    4096
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CheckpointMode {
    /// A background connection checkpoints once the WAL reaches `checkpoint_pages`; commits never wait for it.
    #[default]
    Background,
    /// SQLite's automatic checkpoint, inside the commit that crosses 1000 pages.
    Inline,
}

#[derive(Clone, Debug, Deserialize)]
pub struct LoggingConfig {
    pub filter: String,
    pub format: LogFormat,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Pretty,
    Json,
}

impl Config {
    /// Loads the defaults, the file named by [`CONFIG_FILE_ENV`] if set, and `EXCHANGE__*` environment variables.
    pub fn load() -> Result<Self, config::ConfigError> {
        let mut builder = config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Yaml));
        if let Ok(path) = std::env::var(CONFIG_FILE_ENV) {
            builder = builder.add_source(config::File::with_name(&path));
        }
        builder
            .add_source(
                config::Environment::with_prefix("EXCHANGE")
                    .prefix_separator("__")
                    .separator("__"),
            )
            .build()?
            .try_deserialize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_finnhub_key_comes_from_the_environment_unless_a_file_is_named() {
        let finnhub = FinnhubConfig::default();
        let env = |key: &'static str| {
            move |name: &str| (name == "FINNHUB_API_KEY").then(|| key.to_string())
        };
        assert!(
            finnhub.live_with(env("from-env")).unwrap().is_some(),
            "compose passes it as FINNHUB_API_KEY"
        );
        assert!(
            finnhub.live_with(env("")).unwrap().is_none(),
            "compose passes an unset variable as empty"
        );
        assert!(finnhub.live_with(|_| None).unwrap().is_none());

        let dir = std::env::temp_dir().join(format!("exchange-sim-key-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("key");
        std::fs::write(&file, "").unwrap();
        let from_file = FinnhubConfig {
            api_key_file: Some(file.display().to_string()),
            ..Default::default()
        };
        assert!(
            from_file.live_with(env("from-env")).unwrap().is_none(),
            "a named file wins, even when empty"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn defaults_parse() {
        let config: Config = config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        assert_eq!(config.server.bind, "127.0.0.1:8080");
        assert_eq!(config.logging.format, LogFormat::Pretty);
        assert_eq!(config.exchange.identifier_policy, IdentifierPolicy::Strict);
        assert_eq!(config.events.simulation_id, None);
        assert_eq!(config.webhooks.worker().retry.max_attempts, 10);
        assert_eq!(config.clock.mode, ClockMode::System);
        assert_eq!(config.ids.scheme(), IdScheme::Random);
        let seeded = IdsConfig {
            mode: IdMode::Deterministic,
            seed: 9,
        };
        assert_eq!(seeded.scheme(), IdScheme::Deterministic { seed: 9 });
        assert_eq!(config.settlement.mode, SettlementMode::External);
        assert_eq!(config.settlement.simulator_policy(), None);
        let simulated = SettlementConfig {
            mode: SettlementMode::Both,
            ..config.settlement.clone()
        };
        let policy = simulated.simulator_policy().unwrap();
        assert_eq!((policy.settlement_days, policy.fail_probability), (1, 0.02));
        assert_eq!(policy.validate(), Ok(()));
        let defaults = SettlementConfig::default();
        assert_eq!(
            (
                defaults.mode,
                defaults.fail_probability,
                defaults.delay_probability,
                defaults.tick_interval_ms
            ),
            (
                config.settlement.mode,
                config.settlement.fail_probability,
                config.settlement.delay_probability,
                250
            ),
            "Default matches config/default.yaml"
        );
        assert!(!config.test_control.enabled);
    }

    #[test]
    fn instruments_and_price_sources_read_in_snake_case() {
        let config = with_overlay(
            r#"
price_sources:
  seed: 7
  default: { kind: synthetic, volatility: 0.3, tick_interval_ms: 500 }
instruments:
  - id: AAPL
    symbol: AAPL
    mic: XNAS
    currency: USD
    currency_scale: 2
    reference_symbol: AAPL
    price_source: { initial_price: "187.50" }
  - id: MSFT
    symbol: MSFT
    currency: USD
    currency_scale: 2
    price_source: { kind: fixed, price: "410.00" }
"#,
        )
        .unwrap();
        let instruments = config.declared_instruments().unwrap();
        let aapl = instruments[0].price_source.as_ref().unwrap();
        assert_eq!(aapl.initial_price.as_deref(), Some("187.50"));
        assert_eq!(instruments[0].reference_symbol.as_deref(), Some("AAPL"));
        let msft = instruments[1].price_source.as_ref().unwrap();
        assert_eq!(msft.kind, Some(crate::pricing::PriceSourceKind::Fixed));
        assert_eq!(config.price_sources.seed, 7);
        assert_eq!(config.price_sources.default.tick_interval_ms, Some(500));
        config.price_sources.sources().unwrap();

        let invalid = with_overlay("price_sources: { default: { volatility: -1 } }").unwrap();
        assert_eq!(
            invalid.price_sources.sources().unwrap_err(),
            "price_sources.default: priceSource.volatility must not be negative"
        );
        assert!(
            with_overlay("instruments: [ { id: A, symbol: A, currency: USD, currency_scale: 2, colour: red } ]")
                .is_err(),
            "unknown fields are rejected"
        );
    }

    #[test]
    fn liquidity_reads_in_snake_case() {
        let config = with_overlay(
            r#"
liquidity:
  participant_id: MM
  emit_order_events: true
  default: { mode: on, spread_bps: 5, level_quantity: 20, refill_delay_ms: 500 }
instruments:
  - { id: A, symbol: A, currency: USD, currency_scale: 2, liquidity: { levels: 2, skew_bps_per_lot: 0.5 } }
"#,
        )
        .unwrap();
        let policy = config.liquidity.policy().unwrap();
        assert_eq!(policy.participant.0, "MM");
        assert!(policy.emit_order_events);
        let own = config.declared_instruments().unwrap()[0]
            .liquidity
            .clone()
            .unwrap();
        let resolved = own.or(&policy.default).resolve();
        assert_eq!(
            (
                resolved.mode,
                resolved.levels,
                resolved.spread_bps,
                resolved.level_quantity,
                resolved.refill_delay_ms,
                resolved.skew_bps_per_lot
            ),
            (crate::liquidity::LiquidityMode::On, 2, 5.0, 20, 500, 0.5)
        );
        let invalid = with_overlay("liquidity: { default: { levels: 0 } }").unwrap();
        assert!(
            invalid
                .liquidity
                .policy()
                .unwrap_err()
                .contains("liquidity.levels")
        );
    }

    fn with_overlay(yaml: &str) -> Result<Config, config::ConfigError> {
        config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Yaml))
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .build()?
            .try_deserialize()
    }

    #[test]
    fn market_settlement_cycles_come_from_the_file_and_the_environment() {
        let environment = [(
            "EXCHANGE__SETTLEMENT__MARKET_SETTLEMENT_DAYS__XSTO".to_string(),
            "3".to_string(),
        )];
        let config: Config = config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Yaml))
            .add_source(config::File::from_str(
                "settlement: { mode: simulated, market_settlement_days: { XCSE: 2 } }",
                config::FileFormat::Yaml,
            ))
            .add_source(
                config::Environment::with_prefix("EXCHANGE")
                    .prefix_separator("__")
                    .separator("__")
                    .source(Some(environment.into_iter().collect())),
            )
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        let policy = config.settlement.simulator_policy().unwrap();
        assert_eq!(policy.validate(), Ok(()));
        assert_eq!(policy.settlement_days_for(Some("XCSE")), 2);
        assert_eq!(policy.settlement_days_for(Some("XSTO")), 3);
        assert_eq!(policy.settlement_days_for(Some("XNYS")), 1);
    }

    #[test]
    fn declared_subscriptions_parse_from_a_yaml_file() {
        let config = with_overlay(
            "
webhooks:
  subscriptions:
    - name: settlement
      url: http://settlement:8080/trades
      event_types: [TradeExecuted]
      secret_env: SETTLEMENT_SECRET
      ordered: true
    - name: audit
      url: http://audit:9000/events
      event_types: [OrderAccepted, TradeExecuted]
      paused: true
",
        )
        .unwrap();
        let [settlement, audit] = config.webhooks.subscriptions.as_slice() else {
            panic!("two subscriptions")
        };
        assert_eq!(settlement.name, "settlement");
        assert_eq!(settlement.secret_env.as_deref(), Some("SETTLEMENT_SECRET"));
        assert!(settlement.ordered && settlement.enabled && !settlement.paused);
        assert_eq!(audit.event_types, ["OrderAccepted", "TradeExecuted"]);
        assert!(audit.paused && audit.enabled && !audit.ordered);
        assert!(
            with_overlay("{}")
                .unwrap()
                .webhooks
                .subscriptions
                .is_empty()
        );

        let typo = with_overlay(
            "
webhooks:
  subscriptions:
    - name: x
      url: http://x/
      event_types: [TradeExecuted]
      secrets: oops
",
        )
        .unwrap_err()
        .to_string();
        assert!(
            typo.contains("secrets"),
            "unknown fields are refused: {typo}"
        );
    }

    #[test]
    fn secrets_resolve_from_the_value_an_environment_variable_or_a_file() {
        let base = SubscriptionConfig {
            name: "s".into(),
            url: "http://x/".into(),
            event_types: vec!["TradeExecuted".into()],
            secret: None,
            secret_env: None,
            secret_file: None,
            ordered: false,
            enabled: true,
            paused: false,
            participants: None,
        };
        let secret = |entry: SubscriptionConfig| entry.resolve().map(|d| d.subscription.secret);
        assert_eq!(secret(base.clone()).unwrap(), None);
        assert_eq!(
            secret(SubscriptionConfig {
                secret: Some("literal".into()),
                ..base.clone()
            })
            .unwrap()
            .as_deref(),
            Some("literal")
        );
        // HOME is set in any environment these tests run in.
        assert_eq!(
            secret(SubscriptionConfig {
                secret_env: Some("HOME".into()),
                ..base.clone()
            })
            .unwrap(),
            std::env::var("HOME").ok()
        );
        let file = std::env::temp_dir().join(format!("exchange-secret-{}", uuid::Uuid::now_v7()));
        std::fs::write(&file, "  from-a-file\n").unwrap();
        assert_eq!(
            secret(SubscriptionConfig {
                secret_file: Some(file.to_string_lossy().into_owned()),
                ..base.clone()
            })
            .unwrap()
            .as_deref(),
            Some("from-a-file")
        );
        let _ = std::fs::remove_file(&file);
        let missing = secret(SubscriptionConfig {
            secret_file: Some("/nonexistent/secret".into()),
            ..base.clone()
        })
        .unwrap_err()
        .to_string();
        assert!(
            missing.contains("secret_file /nonexistent/secret"),
            "{missing}"
        );
    }

    /// Every `yaml` block in the usage guide and the README that configures the service (Compose files and scenario
    /// files are skipped) parses as an overlay on the defaults, so the documented examples stay valid.
    #[test]
    fn documented_configuration_examples_parse() {
        let documents = [
            ("README.md", include_str!("../README.md")),
            (
                "docs/getting-started.md",
                include_str!("../docs/getting-started.md"),
            ),
            (
                "docs/configuration.md",
                include_str!("../docs/configuration.md"),
            ),
            (
                "examples/back-office/exchange.yaml",
                concat!(
                    "```yaml\n",
                    include_str!("../examples/back-office/exchange.yaml"),
                    "```\n"
                ),
            ),
            ("docs/operations.md", include_str!("../docs/operations.md")),
            (
                "docs/webhook-receivers.md",
                include_str!("../docs/webhook-receivers.md"),
            ),
            (
                "examples/compose/exchange.yaml",
                concat!(
                    "```yaml\n",
                    include_str!("../examples/compose/exchange.yaml"),
                    "```\n"
                ),
            ),
        ];
        let sections = [
            "server",
            "exchange",
            "events",
            "webhooks",
            "ids",
            "settlement",
            "clearing",
            "clock",
            "test_control",
            "database",
            "logging",
        ];
        let mut checked = 0;
        for (document, text) in documents {
            for block in text.split("```yaml\n").skip(1) {
                let block = block.split("```").next().unwrap();
                let configures = block
                    .lines()
                    .any(|line| sections.iter().any(|s| line.starts_with(&format!("{s}:"))));
                if !configures || block.contains("services:") || block.contains("steps:") {
                    continue;
                }
                let config =
                    with_overlay(block).unwrap_or_else(|e| panic!("{document}: {e}\n{block}"));
                config
                    .clearing_policy()
                    .unwrap_or_else(|e| panic!("{document}: {e}\n{block}"));
                for subscription in &config.webhooks.subscriptions {
                    for event_type in &subscription.event_types {
                        assert!(
                            crate::events::EVENT_TYPES.contains(&event_type.as_str()),
                            "{document}: {} has an unknown event type {event_type}",
                            subscription.name
                        );
                    }
                    assert!(
                        subscription.url.starts_with("http"),
                        "{document}: {}",
                        subscription.name
                    );
                }
                checked += 1;
            }
        }
        assert!(checked >= 13, "found {checked} configuration examples");
    }
}
