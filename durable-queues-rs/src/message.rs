use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;

use crate::storage::postgres::QueueEntry;
use crate::types::{DeliveryMode, QueueEntryId, QueueName};

pub type BoxError = Box<dyn StdError + Send + Sync>;

/// A payload type with a stable name - the `message_payload_type` written for it.
///
/// For a queue shared with Java, `TYPE_NAME` must be the FQCN of the Java class the payload deserializes into
/// (nested classes with `$`, e.g. `com.acme.OrderEvent$OrderAdded`), and the JSON must be what Jackson expects.
/// Implementing this trait (plus serde) makes the type a [`QueueMessage`].
pub trait MessageType: Serialize + DeserializeOwned + Send + 'static {
    const TYPE_NAME: &'static str;
}

/// How a queue's messages map to and from the `message_payload` / `message_payload_type` columns.
///
/// Implemented automatically for every [`MessageType`]. A queue that carries several payload types - common with
/// Java, where the type is chosen per row - implements it by hand on an enum, dispatching on `payload_type`.
pub trait QueueMessage: Sized + Send + 'static {
    /// The `message_payload_type` to store for this message.
    fn payload_type(&self) -> &str;
    fn to_payload(&self) -> serde_json::Result<Value>;
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
    UnknownPayloadType(String),
    #[error("payload does not match its type: {0}")]
    InvalidPayload(#[from] serde_json::Error),
    #[error("meta_data is not a JSON object of strings")]
    InvalidMetaData,
}

/// Java's `MessageMetaData`: string keys to string values, stored as a flat JSON object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageMetaData(BTreeMap<String, String>);

impl MessageMetaData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) -> Option<String> {
        self.0.insert(key.into(), value.into())
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

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
    pub payload: M,
    pub metadata: MessageMetaData,
}

impl<M> Message<M> {
    pub fn new(payload: M) -> Self {
        Self {
            payload,
            metadata: MessageMetaData::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// A message claimed for delivery (Java's `QueuedMessage`).
#[derive(Debug, Clone, PartialEq)]
pub struct QueuedMessage<M> {
    pub id: QueueEntryId,
    pub queue_name: QueueName,
    pub message: Message<M>,
    pub added_ts: DateTime<Utc>,
    pub delivery_ts: Option<DateTime<Utc>>,
    /// Includes this delivery.
    pub total_attempts: i32,
    pub redelivery_attempts: i32,
    pub last_delivery_error: Option<String>,
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

/// A failed delivery. Built from any error with `?`. Phase 3 adds the transient/permanent distinction.
pub struct HandlerError(BoxError);

impl HandlerError {
    pub fn new(error: impl Into<BoxError>) -> Self {
        Self(error.into())
    }

    pub fn into_inner(self) -> BoxError {
        self.0
    }
}

impl<E: Into<BoxError>> From<E> for HandlerError {
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl fmt::Debug for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
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
