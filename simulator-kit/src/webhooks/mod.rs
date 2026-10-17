//! Webhook subscriptions and at-least-once delivery from the transactional outbox.
//!
//! Every committed event gets one `webhook_deliveries` row per matching subscription, in the same transaction. The
//! [`DeliveryWorker`] claims due rows through a [`DeliveryBackend`] (the single SQLite writer), POSTs the canonical
//! envelope, and records the outcome. A 2xx response is delivered; anything else is retried with exponential backoff
//! and jitter until `max_attempts`, after which the delivery is dead-lettered. A claim is a lease: if the outcome is
//! never recorded (crash, shutdown), the delivery becomes due again when the lease expires, and startup releases all
//! claims. Consumers must therefore deduplicate by `eventId`. Concurrent deliveries may arrive out of order; the
//! envelope's `sequence` gives the canonical order.

mod config;
pub mod faults;
mod hub;
pub mod signing;
mod worker;

pub use config::{SubscriptionConfig, WebhooksConfig};
pub use faults::FaultPolicy;
pub use hub::{WebhookError, WebhookHub};
pub use worker::{BackendError, DeliveryBackend, DeliveryWorker, RetryPolicy, WorkerConfig};

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::events::EventEnvelope;

/// A registered webhook endpoint and the event types it receives.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    pub id: Uuid,
    pub url: String,
    pub event_types: Vec<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    /// HMAC secret; deliveries are signed when set. Never serialized.
    #[serde(skip_serializing)]
    pub secret: Option<String>,
    /// Deliver strictly in event sequence order, one at a time.
    pub ordered: bool,
    /// Faults injected into outgoing copies (test control only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub faults: Option<FaultPolicy>,
    /// The name a configuration declares it under; API-created subscriptions have none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub source: SubscriptionSource,
    /// Deliveries are recorded but held until unpaused, then sent in order. (Disabled, by contrast, records none.)
    pub paused: bool,
    /// The secret replaced by the last rotation, and until when deliveries are also signed with it. Never serialized.
    #[serde(skip_serializing)]
    pub previous_secret: Option<(String, DateTime<Utc>)>,
    /// Deleted through the API: kept for its history, but hidden, receiving nothing and sending nothing.
    #[serde(skip_serializing)]
    pub deleted: bool,
}

/// Who owns a subscription.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionSource {
    /// Created through `POST /api/webhooks`.
    #[default]
    Api,
    /// Declared under `webhooks.subscriptions` and reconciled at startup; the configuration is its source of truth.
    Config,
}

impl SubscriptionSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Config => "config",
        }
    }
}

/// A subscription declared in configuration, with its secret already resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredSubscription {
    /// Its identity across restarts: 1 to 64 letters, digits, `.`, `_` or `-`.
    pub name: String,
    pub subscription: NewSubscription,
    /// Off: no deliveries are recorded for it.
    pub enabled: bool,
    /// Deliveries are recorded and held.
    pub paused: bool,
}

/// Checks the names of declared subscriptions: each 1 to 64 letters, digits, `.`, `_` or `-`, and unique.
pub fn validate_declared(declared: &[DeclaredSubscription]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for entry in declared {
        let valid = (1..=64).contains(&entry.name.len())
            && entry
                .name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
        if !valid {
            return Err(format!(
                "webhooks.subscriptions: name {:?} must be 1 to 64 letters, digits, '.', '_' or '-'",
                entry.name
            ));
        }
        if !seen.insert(entry.name.as_str()) {
            return Err(format!(
                "webhooks.subscriptions: name {} is declared twice",
                entry.name
            ));
        }
    }
    Ok(())
}

/// A change to a subscription (`PATCH /api/webhooks/{id}`); `None` leaves a field as it is.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WebhookChange {
    pub url: Option<String>,
    pub event_types: Option<Vec<String>>,
    pub ordered: Option<bool>,
    pub enabled: Option<bool>,
    pub paused: Option<bool>,
}

/// A request to register a subscription.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NewSubscription {
    pub url: String,
    pub event_types: Vec<String>,
    /// 16–256 characters; deliveries are signed when set.
    pub secret: Option<String>,
    /// At most one delivery in flight, in sequence order. A failing delivery blocks later ones until it is delivered
    /// or dead-lettered; after a dead letter delivery continues with the next event.
    pub ordered: bool,
}

impl Subscription {
    pub fn wants(&self, event_type: &str) -> bool {
        self.enabled && !self.deleted && self.event_types.iter().any(|t| t == event_type)
    }
}

/// A claimed delivery: one event for one subscription.
#[derive(Clone, Debug, PartialEq)]
pub struct DeliveryJob {
    pub delivery_id: Uuid,
    pub subscription_id: Uuid,
    pub url: String,
    /// Attempts made before this one.
    pub attempts: u32,
    /// The subscription's signing secret, if any.
    pub secret: Option<String>,
    /// The previous secret while its grace period after a rotation lasts: deliveries are signed with both.
    pub previous_secret: Option<String>,
    /// The subscription's fault policy, if any.
    pub faults: Option<FaultPolicy>,
    /// Whether an out-of-order fault held this delivery back before.
    pub held: bool,
    /// The service's reset epoch at claim time; results from before a reset are dropped.
    pub epoch: u64,
    pub event: EventEnvelope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Delivered,
    Retry {
        at: DateTime<Utc>,
        error: String,
    },
    Dead {
        error: String,
    },
    /// Held back by an out-of-order fault: due again at `at`, without counting an attempt.
    Held {
        at: DateTime<Utc>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryResult {
    pub delivery_id: Uuid,
    /// The epoch of the claim this result answers (see [`DeliveryJob::epoch`]).
    pub epoch: u64,
    pub outcome: DeliveryOutcome,
    /// What happened on the wire: one entry per request sent (a duplicate adds one), or one for a hold.
    pub sent: Vec<AttemptRecord>,
}

/// One request sent for a delivery, or one hold.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttemptRecord {
    /// The delivery attempt number; a duplicate shares it.
    pub attempt: u32,
    pub held: bool,
    /// The faults applied, e.g. `missing:payload.price`, `duplicate`, `delay:500ms`, `held:1000ms`.
    pub faults: Vec<String>,
    /// The body sent, when a fault changed it.
    pub body: Option<String>,
    pub http_status: Option<u16>,
    pub error: Option<String>,
    pub at: DateTime<Utc>,
}
