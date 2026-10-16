//! Application configuration: compiled-in defaults, an optional YAML file, then environment overrides.

use serde::Deserialize;

use std::time::Duration;

use crate::domain::IdentifierPolicy;
use crate::ids::IdScheme;
use crate::settlement::SettlementPolicy;
use crate::webhooks::{RetryPolicy, WorkerConfig};

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
    pub ids: IdsConfig,
    pub settlement: SettlementConfig,
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
        matches!(self.mode, SettlementMode::Simulated | SettlementMode::Both).then(|| {
            SettlementPolicy {
                seed: self.seed,
                settlement_days: self.settlement_days,
                reject_probability: self.reject_probability,
                cancel_probability: self.cancel_probability,
                fail_probability: self.fail_probability,
                delay_probability: self.delay_probability,
                correct_probability: self.correct_probability,
                max_delay_days: self.max_delay_days,
                max_delays: self.max_delays,
                correction_days: self.correction_days,
            }
        })
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

#[derive(Clone, Debug, Deserialize)]
pub struct ClockConfig {
    pub mode: ClockMode,
    /// Virtual clock only: the first reading. Default: the real time at first startup.
    #[serde(default)]
    pub start: Option<chrono::DateTime<chrono::Utc>>,
    /// Virtual clock only: start running with real time instead of frozen.
    #[serde(default)]
    pub running: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClockMode {
    System,
    Virtual,
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
pub struct WebhooksConfig {
    pub poll_interval_ms: u64,
    pub batch_size: u32,
    pub concurrency: usize,
    pub request_timeout_ms: u64,
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl WebhooksConfig {
    pub fn worker(&self) -> WorkerConfig {
        WorkerConfig {
            poll_interval: Duration::from_millis(self.poll_interval_ms),
            batch_size: self.batch_size.max(1),
            concurrency: self.concurrency.max(1),
            request_timeout: Duration::from_millis(self.request_timeout_ms),
            retry: RetryPolicy {
                max_attempts: self.max_attempts.max(1),
                initial_backoff: Duration::from_millis(self.initial_backoff_ms),
                max_backoff: Duration::from_millis(self.max_backoff_ms),
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DatabaseConfig {
    pub path: String,
    pub busy_timeout_ms: u64,
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
        assert_eq!((policy.settlement_days, policy.fail_probability), (2, 0.02));
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
}
