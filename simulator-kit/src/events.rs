//! The canonical event log's records. Events are written in the same transaction as the state they describe and never
//! change afterwards; the global `sequence` is the replay cursor. A simulator defines its own event types, sources and
//! payloads.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// An event before storage has assigned its sequence.
#[derive(Clone, Debug, PartialEq)]
pub struct NewEvent {
    /// Assigned with `sequence` by the writer just before commit ([`NewEvent::stamp`]); nil until then.
    pub event_id: Uuid,
    /// The event's position in the canonical log; 0 until stamped.
    pub sequence: u64,
    pub event_type: &'static str,
    /// The component that produced it, e.g. `exchange` for the exchange's trading events.
    pub source: &'static str,
    pub aggregate_id: String,
    pub correlation_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value,
}

impl NewEvent {
    /// Gives each event its place in the log, starting at `first`, and the ID derived from it. Returns the sequence
    /// after the last one.
    pub fn stamp(events: &mut [NewEvent], first: u64, ids: crate::ids::IdScheme) -> u64 {
        let mut sequence = first;
        for event in events {
            event.sequence = sequence;
            event.event_id = ids.event(sequence);
            sequence += 1;
        }
        sequence
    }
}

/// The canonical envelope, as stored and as delivered to webhooks and the replay API.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub sequence: u64,
    pub event_type: String,
    pub schema_version: u32,
    pub source: String,
    pub simulation_id: Option<String>,
    pub aggregate_id: String,
    pub correlation_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value,
}
