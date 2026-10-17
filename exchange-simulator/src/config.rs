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
    /// The clearinghouse's policy when clearing is enabled; an error for an invalid cutoff, time zone or break
    /// policy.
    pub fn clearing_policy(&self) -> Result<Option<crate::clearing::ClearingPolicy>, String> {
        if !self.clearing.enabled {
            return Ok(None);
        }
        let calendar = crate::clearing::ClearingCalendar::parse(
            &self.clearing.cutoff,
            &self.clearing.time_zone,
        )?;
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
        assert!(checked >= 10, "found {checked} configuration examples");
    }
}
