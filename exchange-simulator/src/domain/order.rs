use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::{InstrumentId, ParticipantId};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown {kind} {value:?}")]
pub struct ParseEnumError {
    kind: &'static str,
    value: String,
}

/// Declares a fieldless enum with its stable database/wire spelling in both directions.
macro_rules! wire_enum {
    ($(#[$meta:meta])* $name:ident, $kind:literal { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum $name { $($variant),+ }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $text),+ }
            }
        }

        impl std::str::FromStr for $name {
            type Err = ParseEnumError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($text => Ok(Self::$variant),)+
                    _ => Err(ParseEnumError { kind: $kind, value: s.to_string() }),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

wire_enum!(Side, "side" { Buy => "BUY", Sell => "SELL" });
wire_enum!(OrderType, "order type" { Limit => "LIMIT", Market => "MARKET" });
wire_enum!(
    /// `Cancelled` keeps the unfilled `remaining_quantity` it had when cancelled, so filled = quantity - remaining.
    OrderStatus, "order status" {
        Open => "OPEN",
        PartiallyFilled => "PARTIALLY_FILLED",
        Filled => "FILLED",
        Cancelled => "CANCELLED",
        Rejected => "REJECTED",
    }
);

impl Side {
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

impl OrderStatus {
    /// Whether an order in this status can still trade (and, for a limit order, rests on the book).
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Open | Self::PartiallyFilled)
    }
}

/// An order. `sequence` is the exchange-assigned command sequence and the time component of price-time priority;
/// wall-clock timestamps play no part in matching.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Order {
    pub id: Uuid,
    pub instrument_id: InstrumentId,
    pub participant_id: ParticipantId,
    pub side: Side,
    pub order_type: OrderType,
    /// Present for limit orders, absent for market orders.
    pub limit_price_ticks: Option<i64>,
    pub quantity: u64,
    pub remaining_quantity: u64,
    pub sequence: u64,
    pub status: OrderStatus,
}

impl Order {
    pub fn filled_quantity(&self) -> u64 {
        self.quantity - self.remaining_quantity
    }
}

/// An immutable fill between one buy and one sell order. Its price is the resting (maker) order's limit price.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Execution {
    pub id: Uuid,
    pub instrument_id: InstrumentId,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub buyer_id: ParticipantId,
    pub seller_id: ParticipantId,
    pub price_ticks: i64,
    pub quantity: u64,
    pub executed_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_names_round_trip() {
        for status in [
            OrderStatus::Open,
            OrderStatus::PartiallyFilled,
            OrderStatus::Filled,
            OrderStatus::Cancelled,
            OrderStatus::Rejected,
        ] {
            assert_eq!(status.as_str().parse::<OrderStatus>(), Ok(status));
        }
        assert_eq!("SELL".parse::<Side>(), Ok(Side::Sell));
        assert_eq!("MARKET".parse::<OrderType>(), Ok(OrderType::Market));
        assert!("sell".parse::<Side>().is_err());
    }
}
