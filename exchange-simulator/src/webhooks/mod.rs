//! Webhook subscriptions and at-least-once delivery from the transactional outbox.
//!
//! Every committed event gets one `webhook_deliveries` row per matching subscription, in the same transaction. The
//! [`DeliveryWorker`] claims due rows through the exchange thread (the single SQLite writer), POSTs the canonical
//! envelope, and records the outcome. A 2xx response is delivered; anything else is retried with exponential backoff
//! and jitter until `max_attempts`, after which the delivery is dead-lettered. A claim is a lease: if the outcome is
//! never recorded (crash, shutdown), the delivery becomes due again when the lease expires, and startup releases all
//! claims. Consumers must therefore deduplicate by `eventId`. Concurrent deliveries may arrive out of order; the
//! envelope's `sequence` gives the canonical order.

pub mod faults;
pub mod signing;
mod worker;

pub use faults::FaultPolicy;
pub use worker::{DeliveryWorker, RetryPolicy, WorkerConfig};

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
    /// The subscription's signing secret, if any.
    pub secret: Option<String>,
    /// The subscription's fault policy, if any.
    pub faults: Option<FaultPolicy>,
    /// Whether an out-of-order fault held this delivery back before.
    pub held: bool,
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
