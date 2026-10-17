//! Webhook settings (`webhooks:` in a simulator's configuration): delivery timing, and subscriptions declared by
//! name, reconciled at startup by the [`super::WebhookHub`].

use std::time::Duration;

use serde::Deserialize;

use super::{RetryPolicy, WorkerConfig};

#[derive(Clone, Debug, Deserialize)]
pub struct WebhooksConfig {
    pub poll_interval_ms: u64,
    pub batch_size: u32,
    pub concurrency: usize,
    pub request_timeout_ms: u64,
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    /// Subscriptions declared here are created or updated at startup, by name (see
    /// [`super::DeclaredSubscription`]).
    #[serde(default)]
    pub subscriptions: Vec<SubscriptionConfig>,
    /// When the configuration changes a declared subscription's secret, how long the previous one stays valid (both
    /// signatures are sent meanwhile).
    #[serde(default = "default_secret_grace_seconds")]
    pub secret_grace_seconds: u64,
}

fn default_secret_grace_seconds() -> u64 {
    3600
}

/// One entry of `webhooks.subscriptions`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionConfig {
    /// Identifies the subscription across restarts and configuration changes.
    pub name: String,
    pub url: String,
    pub event_types: Vec<String>,
    /// The signing secret itself. At most one of `secret`, `secret_env` and `secret_file` may be set; with none,
    /// deliveries are unsigned.
    #[serde(default)]
    pub secret: Option<String>,
    /// The name of an environment variable holding the secret.
    #[serde(default)]
    pub secret_env: Option<String>,
    /// A file holding the secret, e.g. a Docker secret under `/run/secrets`; surrounding whitespace is ignored.
    #[serde(default)]
    pub secret_file: Option<String>,
    #[serde(default)]
    pub ordered: bool,
    /// `false` turns it off: no deliveries are recorded for it until it is enabled again.
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
    /// `true` holds its deliveries: they are recorded and sent, in order, once it is unpaused.
    #[serde(default)]
    pub paused: bool,
}

fn enabled_by_default() -> bool {
    true
}

impl SubscriptionConfig {
    /// The subscription with its secret read from wherever it is configured.
    pub fn resolve(&self) -> anyhow::Result<super::DeclaredSubscription> {
        let name = &self.name;
        let secret = match (&self.secret, &self.secret_env, &self.secret_file) {
            (None, None, None) => None,
            (Some(secret), None, None) => Some(secret.clone()),
            (None, Some(variable), None) => Some(std::env::var(variable).map_err(|_| {
                anyhow::anyhow!("webhooks.subscriptions {name}: secret_env {variable} is not set")
            })?),
            (None, None, Some(file)) => Some(
                std::fs::read_to_string(file)
                    .map_err(|e| {
                        anyhow::anyhow!("webhooks.subscriptions {name}: secret_file {file}: {e}")
                    })?
                    .trim()
                    .to_string(),
            ),
            _ => anyhow::bail!(
                "webhooks.subscriptions {name}: set at most one of secret, secret_env and secret_file"
            ),
        };
        Ok(super::DeclaredSubscription {
            name: name.clone(),
            subscription: super::NewSubscription {
                url: self.url.clone(),
                event_types: self.event_types.clone(),
                secret,
                ordered: self.ordered,
            },
            enabled: self.enabled,
            paused: self.paused,
        })
    }
}

impl WebhooksConfig {
    /// Every declared subscription, secrets resolved.
    pub fn declared(&self) -> anyhow::Result<Vec<super::DeclaredSubscription>> {
        self.subscriptions
            .iter()
            .map(SubscriptionConfig::resolve)
            .collect()
    }

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
