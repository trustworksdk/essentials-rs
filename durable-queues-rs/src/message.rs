use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;

use crate::storage::QueueEntry;
use crate::types::{DeliveryMode, QueueEntryId, QueueName};

/// A boxed error that can cross threads.
pub type BoxError = Box<dyn StdError + Send + Sync>;

/// A payload type with a stable name - the `message_payload_type` written for it.
///
/// For a queue shared with Java, `TYPE_NAME` must be the FQCN of the Java class the payload deserializes into
/// (nested classes with `$`, e.g. `com.acme.OrderEvent$OrderAdded`), and the JSON must be what Jackson expects.
/// Implementing this trait (plus serde) makes the type a [`QueueMessage`].
pub trait MessageType: Serialize + DeserializeOwned + Send + 'static {
    /// The `message_payload_type` stored for this type.
    const TYPE_NAME: &'static str;
}

/// How a queue's messages map to and from the `message_payload` / `message_payload_type` columns.
///
/// Implemented automatically for every [`MessageType`]. A queue that carries several payload types - common with
/// Java, where the type is chosen per row - implements it by hand on an enum, dispatching on `payload_type`.
pub trait QueueMessage: Sized + Send + 'static {
    /// The `message_payload_type` to store for this message.
    fn payload_type(&self) -> &str;
    /// The JSON stored in `message_payload`.
    fn to_payload(&self) -> serde_json::Result<Value>;
    /// Rebuilds a message from a stored payload and its type name; fails for a type this queue does not carry.
    fn from_payload(payload_type: &str, payload: Value) -> Result<Self, DecodeError>;
}

impl<T: MessageType> QueueMessage for T {
    fn payload_type(&self) -> &str {
        T::TYPE_NAME
    }

    fn to_payload(&self) -> serde_json::Result<Value> {
        serde_json::to_value(self)
    }

    fn from_payload(payload_type: &str, payload: Value) -> Result<Self, DecodeError> {
        if payload_type != T::TYPE_NAME {
            return Err(DecodeError::UnknownPayloadType(payload_type.to_owned()));
        }
        Ok(serde_json::from_value(payload)?)
    }
}

/// Why a stored message could not be turned back into a typed message. Always permanent: the stored bytes will not
/// parse on the next attempt either, so the message is dead-lettered (Java's `DurableQueueDeserializationException`).
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("no message type is registered for payload type '{0}'")]
    /// The stored `message_payload_type` is not one this queue's message type knows.
    UnknownPayloadType(String),
    #[error("payload does not match its type: {0}")]
    /// The payload JSON does not deserialize into the type its `message_payload_type` names.
    InvalidPayload(#[from] serde_json::Error),
    #[error("meta_data is not a JSON object of strings")]
    /// The stored `meta_data` is not a JSON object of strings.
    InvalidMetaData,
}

impl DecodeError {
    /// A description safe to log: serde's messages quote the offending input, which is payload data, so for an
    /// invalid payload only the error category and position are kept.
    pub fn redacted(&self) -> String {
        match self {
            Self::InvalidPayload(e) => format!(
                "payload does not match its type: {:?} error at line {} column {}",
                e.classify(),
                e.line(),
                e.column()
            ),
            other => other.to_string(),
        }
    }
}

/// Java's `MessageMetaData`: string keys to string values, stored as a flat JSON object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageMetaData(BTreeMap<String, String>);

impl MessageMetaData {
    /// Empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a value, returning the previous one.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) -> Option<String> {
        self.0.insert(key.into(), value.into())
    }

    /// The value for `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The entries, ordered by key.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub(crate) fn to_json(&self) -> Value {
        Value::Object(
            self.0
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect(),
        )
    }

    /// NULL reads as empty, like Java's row mapper.
    pub(crate) fn from_json(value: Option<Value>) -> Result<Self, DecodeError> {
        let Some(value) = value else {
            return Ok(Self::default());
        };
        let Value::Object(entries) = value else {
            return Err(DecodeError::InvalidMetaData);
        };
        entries
            .into_iter()
            .map(|(k, v)| match v {
                Value::String(s) => Ok((k, s)),
                _ => Err(DecodeError::InvalidMetaData),
            })
            .collect::<Result<_, _>>()
            .map(Self)
    }
}

impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for MessageMetaData {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

/// A message to queue: payload plus metadata (Java's `Message`).
#[derive(Debug, Clone, PartialEq)]
pub struct Message<M> {
    /// The payload, stored as `message_payload`.
    pub payload: M,
    /// Stored as `meta_data`.
    pub metadata: MessageMetaData,
}

impl<M> Message<M> {
    /// A message without metadata.
    pub fn new(payload: M) -> Self {
        Self {
            payload,
            metadata: MessageMetaData::new(),
        }
    }

    /// Adds a metadata entry.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// A message claimed for delivery (Java's `QueuedMessage`).
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedMessage<M> {
    /// The `id` column.
    pub id: QueueEntryId,
    /// The queue it was claimed from.
    pub queue_name: QueueName,
    /// The decoded payload and metadata.
    pub message: Message<M>,
    /// When it was queued.
    pub added_ts: DateTime<Utc>,
    /// When this delivery claimed it.
    pub delivery_ts: Option<DateTime<Utc>>,
    /// Includes this delivery.
    pub total_attempts: i32,
    /// Redeliveries before this delivery: 0 on the first delivery.
    pub redelivery_attempts: i32,
    /// Why the previous delivery failed, if it did.
    pub last_delivery_error: Option<String>,
    /// Normal or ordered.
    pub delivery_mode: DeliveryMode,
}

impl<M: QueueMessage> QueuedMessage<M> {
    pub(crate) fn decode(entry: QueueEntry) -> Result<Self, DecodeError> {
        let payload = M::from_payload(&entry.message_payload_type, entry.message_payload)?;
        let metadata = MessageMetaData::from_json(entry.meta_data)?;
        Ok(Self {
            id: entry.id,
            queue_name: entry.queue_name,
            message: Message { payload, metadata },
            added_ts: entry.added_ts,
            delivery_ts: entry.delivery_ts,
            total_attempts: entry.total_attempts,
            redelivery_attempts: entry.redelivery_attempts,
            last_delivery_error: entry.last_delivery_error,
            delivery_mode: entry.delivery_mode,
        })
    }
}

/// Why a delivery did not end in an acknowledgement. `?` on any error produces [`HandlerError::Transient`].
pub enum HandlerError {
    /// Retried according to the consumer's [`RedeliveryPolicy`](crate::RedeliveryPolicy) until its redeliveries are
    /// exhausted.
    Transient(BoxError),
    /// Dead-lettered on this attempt: retrying cannot succeed (Java's built-in permanent error list, or
    /// `stopRedeliveryOn`).
    Permanent(BoxError),
    /// Not a failure: the handler asks for the message again after the delay - Java's
    /// `QueuedMessage.markForRedeliveryIn`. The redelivery policy is not consulted, so this never dead-letters;
    /// the redelivery count still grows and `last_delivery_error` becomes Java's "Manually requested redelivery".
    RedeliveryRequested(Duration),
}

/// Java's `RetryMessage.MANUALLY_REQUESTED_REDELIVERY`, stored as `last_delivery_error` by
/// [`HandlerError::RedeliveryRequested`].
pub const MANUALLY_REQUESTED_REDELIVERY: &str = "Manually requested redelivery";

impl HandlerError {
    /// A failure the redelivery policy may retry.
    pub fn transient(error: impl Into<BoxError>) -> Self {
        Self::Transient(error.into())
    }

    /// A failure that dead-letters the message at once.
    pub fn permanent(error: impl Into<BoxError>) -> Self {
        Self::Permanent(error.into())
    }

    /// Asks for redelivery after `delay` - return it as `Err(HandlerError::redeliver_in(delay))`.
    pub fn redeliver_in(delay: Duration) -> Self {
        Self::RedeliveryRequested(delay)
    }

    /// Whether this is [`Self::Permanent`].
    pub fn is_permanent(&self) -> bool {
        matches!(self, Self::Permanent(_))
    }

    /// The underlying error; `None` for [`Self::RedeliveryRequested`].
    pub fn error(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        match self {
            Self::Transient(e) | Self::Permanent(e) => Some(e.as_ref()),
            Self::RedeliveryRequested(_) => None,
        }
    }

    /// The underlying error; `None` for [`Self::RedeliveryRequested`].
    pub fn into_inner(self) -> Option<BoxError> {
        match self {
            Self::Transient(e) | Self::Permanent(e) => Some(e),
            Self::RedeliveryRequested(_) => None,
        }
    }

    /// The text stored in `last_delivery_error`: the error, then each `source()` on its own `Caused by:` line.
    /// Java stores a stack trace here; Rust has none to offer, so the cause chain stands in for it.
    pub(crate) fn report(&self) -> String {
        let Some(error) = self.error() else {
            return MANUALLY_REQUESTED_REDELIVERY.to_owned();
        };
        let mut report = error.to_string();
        let mut source = error.source();
        for _ in 0..100 {
            let Some(cause) = source else { break };
            report.push_str("\nCaused by: ");
            report.push_str(&cause.to_string());
            source = cause.source();
        }
        report
    }
}

impl<E: Into<BoxError>> From<E> for HandlerError {
    fn from(error: E) -> Self {
        Self::Transient(error.into())
    }
}

impl fmt::Debug for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transient(e) => f.debug_tuple("Transient").field(e).finish(),
            Self::Permanent(e) => f.debug_tuple("Permanent").field(e).finish(),
            Self::RedeliveryRequested(delay) => {
                f.debug_tuple("RedeliveryRequested").field(delay).finish()
            }
        }
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.error() {
            Some(error) => fmt::Display::fmt(error, f),
            None => f.write_str(MANUALLY_REQUESTED_REDELIVERY),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct OrderPlaced {
        order_id: String,
    }

    impl MessageType for OrderPlaced {
        const TYPE_NAME: &'static str = "com.acme.orders.OrderPlaced";
    }

    #[test]
    fn message_types_round_trip_and_reject_other_type_names() {
        let message = OrderPlaced {
            order_id: "o-1".into(),
        };
        assert_eq!(message.payload_type(), "com.acme.orders.OrderPlaced");
        let payload = message.to_payload().unwrap();
        assert_eq!(payload, json!({"orderId": "o-1"}));

        assert_eq!(
            OrderPlaced::from_payload("com.acme.orders.OrderPlaced", payload.clone()).unwrap(),
            message
        );
        assert!(matches!(
            OrderPlaced::from_payload("com.acme.Other", payload),
            Err(DecodeError::UnknownPayloadType(t)) if t == "com.acme.Other"
        ));
        assert!(matches!(
            OrderPlaced::from_payload("com.acme.orders.OrderPlaced", json!({"unexpected": true})),
            Err(DecodeError::InvalidPayload(_))
        ));
    }

    #[test]
    fn handler_error_reports_include_the_source_chain() {
        #[derive(Debug, thiserror::Error)]
        #[error("connection refused")]
        struct Io;
        #[derive(Debug, thiserror::Error)]
        #[error("failed to call billing")]
        struct Billing(#[source] Io);

        fn fails() -> Result<(), HandlerError> {
            Err(Billing(Io))?
        }
        let error = fails().unwrap_err();
        assert!(!error.is_permanent(), "? produces a transient error");
        assert_eq!(
            error.report(),
            "failed to call billing\nCaused by: connection refused"
        );
        assert!(HandlerError::permanent("bad input").is_permanent());
    }

    #[test]
    fn metadata_is_a_flat_string_map_and_null_reads_as_empty() {
        let metadata: MessageMetaData = [("correlationId", "c-1")].into_iter().collect();
        let json = metadata.to_json();
        assert_eq!(json, json!({"correlationId": "c-1"}));
        assert_eq!(MessageMetaData::from_json(Some(json)).unwrap(), metadata);
        assert!(MessageMetaData::from_json(None).unwrap().is_empty());
        assert!(MessageMetaData::from_json(Some(json!({"n": 1}))).is_err());
        assert!(MessageMetaData::from_json(Some(json!(["a"]))).is_err());
    }
}
