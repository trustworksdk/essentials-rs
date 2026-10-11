use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// Name of a queue - the `queue_name` column. Many queues share one table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueueName(String);

impl QueueName {
    /// A queue name - any text, used verbatim.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for QueueName {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for QueueName {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl fmt::Display for QueueName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Id of a queued message - the `id` column. Opaque text: Java writes UUID v1 or v4 strings, this crate writes
/// UUID v7 strings (time-ordered, like Java's default). Never parse it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueueEntryId(String);

impl QueueEntryId {
    /// Wraps an existing id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// A new UUID v7 id.
    pub fn random() -> Self {
        Self(uuid::Uuid::now_v7().to_string())
    }

    /// The id text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for QueueEntryId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

impl fmt::Display for QueueEntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The `delivery_mode` column: Java's `QueuedMessage.DeliveryMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryMode {
    /// A plain message; `key` is NULL and `key_order` is -1.
    Normal,
    /// An `OrderedMessage`, delivered in `key_order` sequence per `key`.
    InOrder,
}

impl DeliveryMode {
    /// Java's enum name, as stored in `delivery_mode`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::InOrder => "IN_ORDER",
        }
    }
}

#[derive(Debug, Error)]
#[error("unknown delivery mode '{0}', expected NORMAL or IN_ORDER")]
/// A `delivery_mode` value other than `NORMAL` or `IN_ORDER`.
pub struct UnknownDeliveryMode(
    /// The value found.
    pub String,
);

impl FromStr for DeliveryMode {
    type Err = UnknownDeliveryMode;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "NORMAL" => Ok(Self::Normal),
            "IN_ORDER" => Ok(Self::InOrder),
            other => Err(UnknownDeliveryMode(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_ids_are_unique_time_ordered_uuids() {
        let first = QueueEntryId::random();
        let second = QueueEntryId::random();
        assert_ne!(first, second);
        assert!(first < second, "UUID v7 strings sort by creation time");
        assert_eq!(
            uuid::Uuid::parse_str(first.as_str())
                .unwrap()
                .get_version_num(),
            7
        );
    }

    #[test]
    fn delivery_mode_round_trips_java_names() {
        for mode in [DeliveryMode::Normal, DeliveryMode::InOrder] {
            assert_eq!(mode.as_str().parse::<DeliveryMode>().unwrap(), mode);
        }
        assert!("normal".parse::<DeliveryMode>().is_err());
    }
}
