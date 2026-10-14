//! Application configuration: compiled-in defaults, an optional YAML file, then environment overrides.

use serde::Deserialize;

use std::time::Duration;

use crate::domain::IdentifierPolicy;
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
    }
}
