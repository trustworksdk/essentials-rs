//! Configuration: built-in defaults (`config/default.yaml`), an optional YAML file named by [`CONFIG_FILE_ENV`], and
//! `GATEWAY__*` environment variables, in that order.

use std::collections::BTreeMap;

use serde::Deserialize;
use simulator_kit::calendar::CutoffCalendar;
use simulator_kit::ids::IdScheme;
use simulator_kit::persistence::PersistenceMode;

pub use simulator_kit::clock::{ClockConfig, ClockMode, FollowConfig};
pub use simulator_kit::webhooks::{SubscriptionConfig, WebhooksConfig};

use crate::money::{Currencies, parse_amount};
use crate::payments::PaymentPolicy;
use crate::statement::{BreakPolicy, StatementPolicy};

/// Environment variable naming an optional YAML file layered over the defaults.
pub const CONFIG_FILE_ENV: &str = "GATEWAY_CONFIG";

const DEFAULTS: &str = include_str!("../config/default.yaml");

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub payments: PaymentsConfig,
    pub statement: StatementConfig,
    #[serde(default)]
    pub events: EventsConfig,
    pub webhooks: WebhooksConfig,
    pub ids: IdsConfig,
    pub clock: ClockConfig,
    pub test_control: TestControlConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
}

/// Outcome timing for one kind of payment.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OutcomeConfig {
    pub fail_probability: f64,
    pub min_delay_seconds: u64,
    pub max_delay_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaymentsConfig {
    pub seed: u64,
    /// Currency code → decimals. Keys are matched in uppercase, since environment variables arrive lowercased.
    pub currencies: BTreeMap<String, u8>,
    pub deposit: OutcomeConfig,
    pub withdrawal: OutcomeConfig,
    pub return_probability: f64,
    pub max_return_days: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatementConfig {
    pub enabled: bool,
    /// Local time of the daily cutoff, `HH:MM`.
    pub cutoff: String,
    /// IANA time zone of the cutoff.
    pub time_zone: String,
    pub account_iban: String,
    /// Currency → opening balance as a decimal string, e.g. `{ DKK: "250000.00" }`.
    #[serde(default)]
    pub opening_balances: BTreeMap<String, String>,
    #[serde(default)]
    pub breaks: BreakPolicy,
    pub tick_interval_ms: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct EventsConfig {
    /// Stamped on every event as `simulationId`.
    pub simulation_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum IdMode {
    Random,
    Deterministic,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct IdsConfig {
    pub mode: IdMode,
    pub seed: u64,
}

impl IdsConfig {
    pub fn scheme(&self) -> IdScheme {
        match self.mode {
            IdMode::Random => IdScheme::Random,
            IdMode::Deterministic => IdScheme::Deterministic { seed: self.seed },
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TestControlConfig {
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub mode: PersistenceMode,
    /// The database file; unused in `memory` mode.
    pub path: String,
    pub busy_timeout_ms: u64,
    pub memory_max_mib: u64,
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
    /// Loads the defaults, the file named by [`CONFIG_FILE_ENV`] if set, and `GATEWAY__*` environment variables.
    pub fn load() -> Result<Self, config::ConfigError> {
        let mut builder = config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Yaml));
        if let Ok(path) = std::env::var(CONFIG_FILE_ENV) {
            builder = builder.add_source(config::File::with_name(&path));
        }
        builder
            .add_source(
                config::Environment::with_prefix("GATEWAY")
                    .prefix_separator("__")
                    .separator("__"),
            )
            .build()?
            .try_deserialize()
    }

    /// The built-in defaults alone.
    pub fn defaults() -> Self {
        config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Yaml))
            .build()
            .and_then(config::Config::try_deserialize)
            .expect("the built-in defaults are valid")
    }

    /// The accepted currencies.
    pub fn currencies(&self) -> Result<Currencies, String> {
        Currencies::new(&self.payments.currencies)
    }

    /// The payment lifecycle's policy, validated.
    pub fn payment_policy(&self) -> Result<PaymentPolicy, String> {
        let p = &self.payments;
        let policy = PaymentPolicy {
            seed: p.seed,
            deposit: p.deposit.clone(),
            withdrawal: p.withdrawal.clone(),
            return_probability: p.return_probability,
            max_return_days: p.max_return_days,
        };
        policy.validate()?;
        Ok(policy)
    }

    /// The statement's policy when statements are enabled, validated.
    pub fn statement_policy(&self) -> Result<Option<StatementPolicy>, String> {
        let s = &self.statement;
        if !s.enabled {
            return Ok(None);
        }
        let calendar =
            CutoffCalendar::parse(&s.cutoff, &s.time_zone).map_err(|e| format!("statement.{e}"))?;
        crate::iban::validate(&s.account_iban)
            .map_err(|e| format!("statement.account_iban: {e}"))?;
        let currencies = self.currencies()?;
        let mut opening_balances = BTreeMap::new();
        for (currency, amount) in &s.opening_balances {
            let currency = currency.to_ascii_uppercase();
            let scale = currencies.scale(&currency).ok_or_else(|| {
                format!("statement.opening_balances: {currency} is not an accepted currency")
            })?;
            let minor = parse_amount(amount, scale, true)
                .map_err(|e| format!("statement.opening_balances.{currency}: {e}"))?;
            opening_balances.insert(currency, minor);
        }
        s.breaks.validate()?;
        Ok(Some(StatementPolicy {
            calendar,
            account_iban: s.account_iban.clone(),
            opening_balances,
            breaks: s.breaks.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_parse_and_validate() {
        let config = Config::defaults();
        assert_eq!(config.server.bind, "127.0.0.1:8081");
        assert_eq!(config.clock.mode, ClockMode::System);
        assert_eq!(config.ids.scheme(), IdScheme::Random);
        assert_eq!(config.database.mode, PersistenceMode::Durable);
        assert!(config.payment_policy().is_ok());
        assert!(config.statement_policy().unwrap().is_some());
        assert_eq!(config.currencies().unwrap().scale("JPY"), Some(0));
        assert_eq!(config.webhooks.worker().retry.max_attempts, 10);
    }

    #[test]
    fn the_back_office_example_configuration_is_valid() {
        let config: Config = config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Yaml))
            .add_source(config::File::from_str(
                include_str!("../../exchange-simulator/examples/back-office/gateway.yaml"),
                config::FileFormat::Yaml,
            ))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        assert_eq!(config.clock.mode, ClockMode::Follow);
        assert_eq!(
            config.clock.follow.as_ref().map(|f| f.url.as_str()),
            Some("http://exchange:8080")
        );
        assert!(config.payment_policy().is_ok());
        let statements = config.statement_policy().unwrap().unwrap();
        assert_eq!(statements.opening_balances["DKK"], 100_000_000);
        assert_eq!(statements.opening_balances["USD"], 100_000_000);
        // Event types only: the secret file exists in the container.
        let declared = &config.webhooks.subscriptions[0];
        assert!(
            declared
                .event_types
                .iter()
                .all(|t| crate::gateway::EVENT_TYPES.contains(&t.as_str()))
        );
        assert_eq!(
            declared.event_types.len(),
            crate::gateway::EVENT_TYPES.len()
        );
    }

    #[test]
    fn invalid_settings_are_refused() {
        let with = |adjust: fn(&mut Config)| {
            let mut config = Config::defaults();
            adjust(&mut config);
            config
        };
        assert!(
            with(|c| c.statement.time_zone = "Mars/Olympus".into())
                .statement_policy()
                .unwrap_err()
                .contains("statement.time_zone")
        );
        assert!(
            with(|c| c.statement.account_iban = "DK0000400440116243".into())
                .statement_policy()
                .is_err()
        );
        assert!(
            with(|c| {
                c.statement
                    .opening_balances
                    .insert("XXX".into(), "1.00".into());
            })
            .statement_policy()
            .is_err()
        );
        assert!(
            with(|c| c.payments.deposit.fail_probability = 1.5)
                .payment_policy()
                .is_err()
        );
        assert!(
            with(|c| c.payments.withdrawal.min_delay_seconds = 10_000_000)
                .payment_policy()
                .is_err()
        );
        let lower = with(|c| {
            c.statement
                .opening_balances
                .insert("dkk".into(), "250000.00".into());
        });
        let policy = lower.statement_policy().unwrap().unwrap();
        assert_eq!(policy.opening_balances["DKK"], 25_000_000);
    }
}
