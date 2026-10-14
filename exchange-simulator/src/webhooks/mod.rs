//! Webhook subscriptions and at-least-once delivery from the transactional outbox.
//!
//! Every committed event gets one `webhook_deliveries` row per matching subscription, in the same transaction. The
//! [`DeliveryWorker`] claims due rows through the exchange thread (the single SQLite writer), POSTs the canonical
//! envelope, and records the outcome. A 2xx response is delivered; anything else is retried with exponential backoff
//! and jitter until `max_attempts`, after which the delivery is dead-lettered. A claim is a lease: if the outcome is
//! never recorded (crash, shutdown), the delivery becomes due again when the lease expires, and startup releases all
//! claims. Consumers must therefore deduplicate by `eventId`. Concurrent deliveries may arrive out of order; the
//! envelope's `sequence` gives the canonical order.

mod worker;

pub use worker::{DeliveryWorker, RetryPolicy, WorkerConfig};

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::events::EventEnvelope;

/// A registered webhook endpoint and the event types it receives.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    pub id: Uuid,
    pub url: String,
    pub event_types: Vec<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

impl Subscription {
    pub fn wants(&self, event_type: &str) -> bool {
        self.enabled && self.event_types.iter().any(|t| t == event_type)
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
    pub event: EventEnvelope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Delivered,
    Retry { at: DateTime<Utc>, error: String },
    Dead { error: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryResult {
    pub delivery_id: Uuid,
    pub outcome: DeliveryOutcome,
}
