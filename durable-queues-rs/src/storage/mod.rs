//! Queue storage: the [`QueueStorage`] trait and the backend-neutral types it trades in. The PostgreSQL backend,
//! schema-compatible with Java Essentials, is [`postgres::PostgresQueueStorage`].

pub(crate) mod postgres;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::error::Result;
use crate::types::{DeliveryMode, QueueEntryId, QueueName};

/// Where queued messages live. [`DurableQueues`](crate::DurableQueues) and its consumers do everything through
/// this trait; the semantics each method must provide are those of CONTRACT.md, cited per method.
///
/// Every method is one atomic operation of its own: the claim, the handler and the settling of a message are never
/// one transaction. Timestamps are passed in rather than read from a storage clock (CONTRACT.md §2).
pub trait QueueStorage: Clone + Send + Sync + 'static {
    /// Inserts ready-to-deliver normal messages, all or none. Returns the number inserted.
    fn insert_messages(
        &self,
        queue_name: &QueueName,
        entries: NewEntries,
        added_ts: DateTime<Utc>,
        next_delivery_ts: DateTime<Utc>,
    ) -> impl Future<Output = Result<u64>> + Send;

    /// Claims up to `limit` normal messages due at `now`, so that no two callers claim the same message (§4.1).
    /// With `reset_stuck`, first returns this queue's messages claimed at or before its threshold to the queue (§6),
    /// atomically with the claim.
    fn claim_unordered(
        &self,
        queue_name: &QueueName,
        limit: i64,
        now: DateTime<Utc>,
        reset_stuck: Option<StuckMessageReset>,
    ) -> impl Future<Output = Result<ClaimResult>> + Send;

    /// Deletes a handled message. A message dead-lettered while it was handled also counts as acknowledged;
    /// `false` means it was already gone (§5).
    fn acknowledge(&self, id: &QueueEntryId) -> impl Future<Output = Result<bool>> + Send;

    /// Returns a failed message to the queue for redelivery at `next_delivery_ts`, incrementing its redelivery
    /// count (§5). `false` if it is gone.
    fn retry(
        &self,
        id: &QueueEntryId,
        next_delivery_ts: DateTime<Utc>,
        error: &str,
    ) -> impl Future<Output = Result<bool>> + Send;

    /// Dead-letters a message. `None` if it does not exist or already is a dead letter (§5).
    fn mark_as_dead_letter(
        &self,
        id: &QueueEntryId,
        reason: &str,
    ) -> impl Future<Output = Result<Option<QueueEntry>>> + Send;

    /// [`Self::mark_as_dead_letter`] without reading the message back - for payloads that cannot be decoded.
    fn mark_as_dead_letter_direct(
        &self,
        id: &QueueEntryId,
        reason: &str,
    ) -> impl Future<Output = Result<bool>> + Send;

    /// Makes a dead letter deliverable again at `next_delivery_ts`, keeping its counters. `None` if it is not a
    /// dead letter (§5).
    fn resurrect_dead_letter(
        &self,
        id: &QueueEntryId,
        next_delivery_ts: DateTime<Utc>,
    ) -> impl Future<Output = Result<Option<QueueEntry>>> + Send;

    /// A page of a queue's dead letters, ordered by the time they were queued.
    fn dead_letters(
        &self,
        queue_name: &QueueName,
        order: QueueingSortOrder,
        offset: i64,
        limit: i64,
    ) -> impl Future<Output = Result<Vec<QueueEntry>>> + Send;

    /// Loads one message by id, whatever its state.
    fn find_entry(
        &self,
        id: &QueueEntryId,
    ) -> impl Future<Output = Result<Option<QueueEntry>>> + Send;
}

/// One stored message, whatever its state.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueEntry {
    pub id: QueueEntryId,
    pub queue_name: QueueName,
    pub message_payload: Value,
    /// The payload's type name. For Java-produced rows: the Java FQCN.
    pub message_payload_type: String,
    pub added_ts: DateTime<Utc>,
    /// `None` while the message is being delivered and for dead letters.
    pub next_delivery_ts: Option<DateTime<Utc>>,
    pub delivery_ts: Option<DateTime<Utc>>,
    pub total_attempts: i32,
    pub redelivery_attempts: i32,
    pub last_delivery_error: Option<String>,
    pub is_being_delivered: bool,
    pub is_dead_letter_message: bool,
    pub meta_data: Option<Value>,
    pub delivery_mode: DeliveryMode,
    pub key: Option<String>,
    /// -1 for [`DeliveryMode::Normal`].
    pub key_order: i64,
}

/// Messages to insert, as parallel arrays with one element per message.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct NewEntries {
    pub ids: Vec<String>,
    pub payloads: Vec<Value>,
    pub payload_types: Vec<String>,
    pub meta_data: Vec<Value>,
}

/// Asks [`QueueStorage::claim_unordered`] to recover stuck messages first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StuckMessageReset {
    /// Messages whose `delivery_ts` is at or before this are considered stuck.
    pub claimed_before: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimResult {
    pub claimed: Vec<QueueEntry>,
    /// Stuck messages returned to the queue before claiming.
    pub reset: u64,
}

/// Order of dead letters by the time they were queued.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueueingSortOrder {
    #[default]
    Ascending,
    Descending,
}

/// Java's `last_delivery_error` for a message reset by stuck-message recovery, verbatim.
pub const STUCK_MESSAGE_ERROR: &str =
    "Handler Processing of the Message was determined to have Timed Out";
