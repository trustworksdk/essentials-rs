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

    /// Claims up to `limit` messages due at `now`, so that no two callers claim the same message - Java's fetch
    /// (CONTRACT.md §4.3), atomically:
    /// 1. with `reset_stuck`, returns this queue's messages claimed at or before its threshold to the queue (§6);
    /// 2. claims ordered messages that head their key (§4.2), skipping `exclude_keys` - the keys this node is
    ///    handling;
    /// 3. only if step 2 claimed nothing, claims normal messages (§4.1).
    fn claim(
        &self,
        queue_name: &QueueName,
        limit: i64,
        now: DateTime<Utc>,
        reset_stuck: Option<StuckMessageReset>,
        exclude_keys: &[String],
    ) -> impl Future<Output = Result<ClaimResult>> + Send;

    /// Whether the queue holds any ordered message for `key`, dead letters included.
    fn has_ordered_message_queued_for_key(
        &self,
        queue_name: &QueueName,
        key: &str,
    ) -> impl Future<Output = Result<bool>> + Send;

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
    /// The `id` column - opaque text.
    pub id: QueueEntryId,
    /// The queue it belongs to.
    pub queue_name: QueueName,
    /// The payload JSON.
    pub message_payload: Value,
    /// The payload's type name. For Java-produced rows: the Java FQCN.
    pub message_payload_type: String,
    /// When it was queued.
    pub added_ts: DateTime<Utc>,
    /// `None` while the message is being delivered and for dead letters.
    pub next_delivery_ts: Option<DateTime<Utc>>,
    /// When the current delivery claimed it; `None` unless it is being delivered.
    pub delivery_ts: Option<DateTime<Utc>>,
    /// Deliveries so far, including a current one.
    pub total_attempts: i32,
    /// Redeliveries so far.
    pub redelivery_attempts: i32,
    /// Why the last delivery failed, or why it was dead-lettered.
    pub last_delivery_error: Option<String>,
    /// Whether a consumer has claimed it.
    pub is_being_delivered: bool,
    /// Whether it is a dead letter.
    pub is_dead_letter_message: bool,
    /// The metadata JSON; NULL counts as empty.
    pub meta_data: Option<Value>,
    /// Normal or ordered.
    pub delivery_mode: DeliveryMode,
    /// The ordering key of an ordered message.
    pub key: Option<String>,
    /// -1 for [`DeliveryMode::Normal`].
    pub key_order: i64,
}

/// Messages to insert, as parallel arrays with one element per message.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct NewEntries {
    /// Message ids.
    pub ids: Vec<String>,
    /// Payload JSON.
    pub payloads: Vec<Value>,
    /// Payload type names.
    pub payload_types: Vec<String>,
    /// Metadata JSON objects.
    pub meta_data: Vec<Value>,
    /// Java's `delivery_mode` names: `NORMAL` or `IN_ORDER`.
    pub delivery_modes: Vec<String>,
    /// Ordering keys; `None` for normal messages.
    pub keys: Vec<Option<String>>,
    /// Orders within the key; -1 for normal messages.
    pub key_orders: Vec<i64>,
}

/// Asks [`QueueStorage::claim`] to recover stuck messages first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StuckMessageReset {
    /// Messages whose `delivery_ts` is at or before this are considered stuck.
    pub claimed_before: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
/// What [`QueueStorage::claim`] did.
pub struct ClaimResult {
    /// The claimed messages, now being delivered.
    pub claimed: Vec<QueueEntry>,
    /// Stuck messages returned to the queue before claiming.
    pub reset: u64,
}

/// Order of dead letters by the time they were queued.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueueingSortOrder {
    #[default]
    /// Oldest first.
    Ascending,
    /// Newest first.
    Descending,
}

/// Java's `last_delivery_error` for a message reset by stuck-message recovery, verbatim.
pub const STUCK_MESSAGE_ERROR: &str =
    "Handler Processing of the Message was determined to have Timed Out";
