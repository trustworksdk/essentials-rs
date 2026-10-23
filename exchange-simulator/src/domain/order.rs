use chrono::{DateTime, NaiveDate, Utc};
use uuid::Uuid;

use super::{InstrumentId, ParticipantId};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown {kind} {value:?}")]
pub struct ParseEnumError {
    pub(crate) kind: &'static str,
    pub(crate) value: String,
}

/// Declares a fieldless enum with its stable database/wire spelling in both directions, including serde.
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
            type Err = $crate::domain::ParseEnumError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($text => Ok(Self::$variant),)+
                    _ => Err($crate::domain::ParseEnumError { kind: $kind, value: s.to_string() }),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
                text.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

pub(crate) use wire_enum;

wire_enum!(Side, "side" { Buy => "BUY", Sell => "SELL" });
wire_enum!(OrderType, "order type" { Limit => "LIMIT", Market => "MARKET" });
wire_enum!(
    /// How long an order stays on the book (SR-17): until the session closes, until cancelled, or not at all.
    TimeInForce, "time in force" { Day => "DAY", Gtc => "GTC", Ioc => "IOC" }
);
wire_enum!(
    /// `Cancelled` keeps the unfilled `remaining_quantity` it had when cancelled, so filled = quantity - remaining.
    /// `Expired` is a `DAY` order still open when its session closed (SR-17), likewise keeping its remainder.
    OrderStatus, "order status" {
        Open => "OPEN",
        PartiallyFilled => "PARTIALLY_FILLED",
        Filled => "FILLED",
        Cancelled => "CANCELLED",
        Rejected => "REJECTED",
        Expired => "EXPIRED",
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
    /// Participant-chosen ID, unique per participant; resubmitting it returns the original order.
    pub client_order_id: Option<String>,
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
    pub time_in_force: TimeInForce,
    /// Quantity whose fills were busted or corrected away (SR-36). It is not reinstated: it is neither filled nor
    /// remaining, as FIX's `CumQty` drops on a trade cancel while `LeavesQty` stays.
    pub busted_quantity: u64,
}

impl Order {
    pub fn filled_quantity(&self) -> u64 {
        self.quantity - self.remaining_quantity - self.busted_quantity
    }
}

wire_enum!(
    /// Whether an execution stands as executed, was corrected, or was busted (SR-36).
    ExecutionStatus, "execution status" {
        Executed => "EXECUTED",
        Corrected => "CORRECTED",
        Busted => "BUSTED",
    }
);

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
    /// The business day the trade belongs to (FIX `TradeDate`), and the day it is meant to settle (`SettlDate`).
    pub trade_date: NaiveDate,
    pub intended_settlement_date: NaiveDate,
    pub status: ExecutionStatus,
    /// What each side pays the exchange for this execution (SR-37), in minor units of the instrument's currency.
    pub buyer_fee_minor: i64,
    pub seller_fee_minor: i64,
}

/// The dates an execution carries (SR-29), the same for every fill of one command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TradeDates {
    pub trade_date: NaiveDate,
    pub intended_settlement_date: NaiveDate,
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
