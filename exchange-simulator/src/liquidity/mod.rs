//! The built-in liquidity provider (plan item 6.7, SR-6, SR-7, SR-8, SR-10): a participant the simulator creates that
//! quotes a ladder of limit orders around each instrument's reference price ([`crate::pricing`]), so a demo whose
//! orders all come from one participant always has a counterparty.
//!
//! This module is the pure part: the settings and the ladder they give for a reference price and an inventory. The
//! exchange places, replaces and accounts for the orders ([`crate::engine::Exchange`]).
//!
//! - **Ladder:** `levels` price levels a side. The best bid and ask are `spread_bps` from the (skewed) mid, rounded away
//!   from it to the tick, so rounding never narrows the spread; deeper levels step `level_step_ticks` further out.
//!   Each level holds `level_quantity` lots, times `level_quantity_growth` per level deeper.
//! - **Inventory skew:** the provider's net position, in lots, moves the mid by `skew_bps_per_lot` per lot against the
//!   position: a long provider lowers both sides, a short one raises them.
//! - **Limit:** at `max_inventory` lots long it stops bidding, at `max_inventory` short it stops offering.
//!
//! An instrument's own settings (`liquidity`) may leave fields out; they come from `liquidity.default`, field by field,
//! then from the built-in defaults. `mode` defaults to `off`, so nothing is quoted unless asked for.

use serde::{Deserialize, Serialize};

use crate::domain::Side;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LiquidityMode {
    On,
    Off,
}

/// Liquidity settings as configured: on an instrument (`liquidity`, API and configuration) or as the default
/// (`liquidity.default`). Every field is optional.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LiquiditySpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<LiquidityMode>,
    /// Price levels a side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub levels: Option<u32>,
    /// From the mid to the best bid and to the best ask, in basis points.
    #[serde(default, alias = "spread_bps", skip_serializing_if = "Option::is_none")]
    pub spread_bps: Option<f64>,
    /// Between levels, in ticks.
    #[serde(
        default,
        alias = "level_step_ticks",
        skip_serializing_if = "Option::is_none"
    )]
    pub level_step_ticks: Option<u32>,
    /// Lots at the best level.
    #[serde(
        default,
        alias = "level_quantity",
        skip_serializing_if = "Option::is_none"
    )]
    pub level_quantity: Option<u64>,
    /// Each deeper level holds this many times the one before (1 for flat depth).
    #[serde(
        default,
        alias = "level_quantity_growth",
        skip_serializing_if = "Option::is_none"
    )]
    pub level_quantity_growth: Option<f64>,
    /// How far the reference price must move from the last quote, in ticks, before the ladder is quoted again.
    #[serde(
        default,
        alias = "requote_threshold_ticks",
        skip_serializing_if = "Option::is_none"
    )]
    pub requote_threshold_ticks: Option<u32>,
    /// How long after a fill, in simulated milliseconds, the ladder is refilled (0: at once).
    #[serde(
        default,
        alias = "refill_delay_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub refill_delay_ms: Option<u64>,
    /// How far each lot of inventory moves the mid against the position, in basis points.
    #[serde(
        default,
        alias = "skew_bps_per_lot",
        skip_serializing_if = "Option::is_none"
    )]
    pub skew_bps_per_lot: Option<f64>,
    /// Lots of inventory, long or short, past which the side that would add to it is not quoted.
    #[serde(
        default,
        alias = "max_inventory",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_inventory: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum LiquidityError {
    #[error("liquidity.{field} must be {range}")]
    OutOfRange {
        field: &'static str,
        range: &'static str,
    },
}

/// The settings with every field resolved.
#[derive(Clone, Debug, PartialEq)]
pub struct LiquiditySettings {
    pub mode: LiquidityMode,
    pub levels: u32,
    pub spread_bps: f64,
    pub level_step_ticks: u32,
    pub level_quantity: u64,
    pub level_quantity_growth: f64,
    pub requote_threshold_ticks: u32,
    pub refill_delay_ms: u64,
    pub skew_bps_per_lot: f64,
    pub max_inventory: u64,
}

impl Default for LiquiditySettings {
    fn default() -> Self {
        Self {
            mode: LiquidityMode::Off,
            levels: 5,
            spread_bps: 10.0,
            level_step_ticks: 1,
            level_quantity: 100,
            level_quantity_growth: 1.0,
            requote_threshold_ticks: 1,
            refill_delay_ms: 0,
            skew_bps_per_lot: 0.0,
            max_inventory: 10_000,
        }
    }
}

impl LiquiditySpec {
    /// Checks the fields given.
    pub fn validate(&self) -> Result<(), LiquidityError> {
        let out = |field, range| Err(LiquidityError::OutOfRange { field, range });
        if self.levels.is_some_and(|l| !(1..=50).contains(&l)) {
            return out("levels", "between 1 and 50");
        }
        if self
            .spread_bps
            .is_some_and(|s| !s.is_finite() || !(0.0..=10_000.0).contains(&s))
        {
            return out("spreadBps", "between 0 and 10000");
        }
        if self.level_step_ticks == Some(0) {
            return out("levelStepTicks", "at least 1");
        }
        if self
            .level_quantity
            .is_some_and(|q| !(1..=1_000_000_000).contains(&q))
        {
            return out("levelQuantity", "between 1 and 1000000000 lots");
        }
        if self
            .level_quantity_growth
            .is_some_and(|g| !g.is_finite() || !(0.1..=10.0).contains(&g))
        {
            return out("levelQuantityGrowth", "between 0.1 and 10");
        }
        if self.refill_delay_ms.is_some_and(|d| d > 86_400_000) {
            return out("refillDelayMs", "at most 86400000");
        }
        if self
            .skew_bps_per_lot
            .is_some_and(|s| !s.is_finite() || !(0.0..=10_000.0).contains(&s))
        {
            return out("skewBpsPerLot", "between 0 and 10000");
        }
        if self.max_inventory == Some(0) {
            return out("maxInventory", "at least 1 lot");
        }
        Ok(())
    }

    /// This spec with its missing fields taken from `default`.
    pub fn or(&self, default: &Self) -> Self {
        Self {
            mode: self.mode.or(default.mode),
            levels: self.levels.or(default.levels),
            spread_bps: self.spread_bps.or(default.spread_bps),
            level_step_ticks: self.level_step_ticks.or(default.level_step_ticks),
            level_quantity: self.level_quantity.or(default.level_quantity),
            level_quantity_growth: self.level_quantity_growth.or(default.level_quantity_growth),
            requote_threshold_ticks: self
                .requote_threshold_ticks
                .or(default.requote_threshold_ticks),
            refill_delay_ms: self.refill_delay_ms.or(default.refill_delay_ms),
            skew_bps_per_lot: self.skew_bps_per_lot.or(default.skew_bps_per_lot),
            max_inventory: self.max_inventory.or(default.max_inventory),
        }
    }

    /// The settings, with the built-in defaults for whatever is still missing.
    pub fn resolve(&self) -> LiquiditySettings {
        let d = LiquiditySettings::default();
        LiquiditySettings {
            mode: self.mode.unwrap_or(d.mode),
            levels: self.levels.unwrap_or(d.levels),
            spread_bps: self.spread_bps.unwrap_or(d.spread_bps),
            level_step_ticks: self.level_step_ticks.unwrap_or(d.level_step_ticks),
            level_quantity: self.level_quantity.unwrap_or(d.level_quantity),
            level_quantity_growth: self
                .level_quantity_growth
                .unwrap_or(d.level_quantity_growth),
            requote_threshold_ticks: self
                .requote_threshold_ticks
                .unwrap_or(d.requote_threshold_ticks),
            refill_delay_ms: self.refill_delay_ms.unwrap_or(d.refill_delay_ms),
            skew_bps_per_lot: self.skew_bps_per_lot.unwrap_or(d.skew_bps_per_lot),
            max_inventory: self.max_inventory.unwrap_or(d.max_inventory),
        }
    }
}

/// One order of a ladder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quote {
    pub side: Side,
    pub price_ticks: i64,
    /// In units, a whole number of lots.
    pub quantity: u64,
}

impl LiquiditySettings {
    /// The ladder for `reference` (ticks) and `inventory` (lots, positive long), bids best first then asks best
    /// first. Levels that would be priced below one tick are left out.
    pub fn ladder(&self, reference: i64, inventory: i64, lot_size: u64) -> Vec<Quote> {
        let reference = reference as f64;
        let mid = reference * (1.0 - self.skew_bps_per_lot * inventory as f64 / 10_000.0);
        let half = reference * self.spread_bps / 10_000.0;
        // A millionth of a tick absorbs floating-point error: 10000 × 1.003 is 10029.999999999998, which must count
        // as the whole tick it is, not round a tick away.
        const EPSILON: f64 = 1e-6;
        let best_bid = (mid - half + EPSILON).floor() as i64;
        let best_ask = ((mid + half - EPSILON).ceil() as i64).max(best_bid + 1);
        let max = i64::try_from(self.max_inventory).unwrap_or(i64::MAX);
        let quantity = |level: u32| {
            let lots = (self.level_quantity as f64 * self.level_quantity_growth.powi(level as i32))
                .round()
                .max(1.0) as u64;
            lots.saturating_mul(lot_size)
        };
        let step = i64::from(self.level_step_ticks);
        let mut ladder = Vec::new();
        for (side, quoted, best, direction) in [
            (Side::Buy, inventory < max, best_bid, -1),
            (Side::Sell, inventory > -max, best_ask, 1),
        ] {
            if !quoted {
                continue;
            }
            for level in 0..self.levels {
                let price_ticks = best + direction * step * i64::from(level);
                if price_ticks >= 1 {
                    ladder.push(Quote {
                        side,
                        price_ticks,
                        quantity: quantity(level),
                    });
                }
            }
        }
        ladder
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(adjust: impl FnOnce(&mut LiquiditySettings)) -> LiquiditySettings {
        let mut s = LiquiditySettings {
            mode: LiquidityMode::On,
            levels: 3,
            ..Default::default()
        };
        adjust(&mut s);
        s
    }

    fn prices(ladder: &[Quote], side: Side) -> Vec<i64> {
        ladder
            .iter()
            .filter(|q| q.side == side)
            .map(|q| q.price_ticks)
            .collect()
    }

    #[test]
    fn the_spread_is_measured_from_the_reference_and_rounded_away_from_it() {
        // R = 100.00, tick 0.01, 10 bps: 99.90 / 100.10 (SR-7's acceptance).
        let ladder = settings(|_| {}).ladder(10_000, 0, 1);
        assert_eq!(prices(&ladder, Side::Buy), [9_990, 9_989, 9_988]);
        assert_eq!(prices(&ladder, Side::Sell), [10_010, 10_011, 10_012]);
        // 1.5 ticks either side rounds out to 2, never in to 1.
        let ladder = settings(|s| s.spread_bps = 15.0).ladder(1_000, 0, 1);
        assert_eq!(
            (
                prices(&ladder, Side::Buy)[0],
                prices(&ladder, Side::Sell)[0]
            ),
            (998, 1_002)
        );
        // No spread still leaves a tick between the sides.
        let ladder = settings(|s| s.spread_bps = 0.0).ladder(1_000, 0, 1);
        assert_eq!(
            (
                prices(&ladder, Side::Buy)[0],
                prices(&ladder, Side::Sell)[0]
            ),
            (1_000, 1_001)
        );
    }

    #[test]
    fn levels_step_out_and_grow_in_whole_lots() {
        let ladder = settings(|s| {
            s.level_step_ticks = 5;
            s.level_quantity = 10;
            s.level_quantity_growth = 2.0;
        })
        .ladder(10_000, 0, 100);
        assert_eq!(prices(&ladder, Side::Sell), [10_010, 10_015, 10_020]);
        let quantities: Vec<u64> = ladder
            .iter()
            .filter(|q| q.side == Side::Sell)
            .map(|q| q.quantity)
            .collect();
        assert_eq!(quantities, [1_000, 2_000, 4_000]);
    }

    #[test]
    fn inventory_skews_both_sides_against_the_position_up_to_the_limit() {
        let s = settings(|s| {
            s.skew_bps_per_lot = 1.0;
            s.max_inventory = 20;
        });
        let flat = s.ladder(10_000, 0, 1);
        let long = s.ladder(10_000, 10, 1);
        let short = s.ladder(10_000, -10, 1);
        assert_eq!(
            prices(&long, Side::Buy)[0],
            prices(&flat, Side::Buy)[0] - 10,
            "long lowers the bid"
        );
        assert_eq!(
            prices(&long, Side::Sell)[0],
            prices(&flat, Side::Sell)[0] - 10,
            "and the ask"
        );
        assert_eq!(
            prices(&short, Side::Sell)[0],
            prices(&flat, Side::Sell)[0] + 10,
            "short raises them"
        );
        let short_30 = settings(|s| s.skew_bps_per_lot = 1.0).ladder(10_000, -30, 1);
        assert_eq!(
            (
                prices(&short_30, Side::Buy)[0],
                prices(&short_30, Side::Sell)[0]
            ),
            (10_020, 10_040),
            "an exact tick stays exact despite floating-point error"
        );
        let at_limit_long = s.ladder(10_000, 20, 1);
        assert!(
            prices(&at_limit_long, Side::Buy).is_empty(),
            "no more buying"
        );
        assert_eq!(prices(&at_limit_long, Side::Sell).len(), 3);
        let at_limit_short = s.ladder(10_000, -20, 1);
        assert!(
            prices(&at_limit_short, Side::Sell).is_empty(),
            "no more selling"
        );
    }

    #[test]
    fn levels_below_one_tick_are_left_out() {
        let ladder = settings(|s| s.spread_bps = 0.0).ladder(2, 0, 1);
        assert_eq!(prices(&ladder, Side::Buy), [2, 1]);
    }

    #[test]
    fn specs_merge_field_by_field_and_are_validated() {
        let default = LiquiditySpec {
            mode: Some(LiquidityMode::On),
            spread_bps: Some(20.0),
            ..Default::default()
        };
        let own = LiquiditySpec {
            levels: Some(2),
            ..Default::default()
        };
        let resolved = own.or(&default).resolve();
        assert_eq!(
            (
                resolved.mode,
                resolved.levels,
                resolved.spread_bps,
                resolved.level_quantity
            ),
            (LiquidityMode::On, 2, 20.0, 100)
        );
        assert_eq!(LiquiditySpec::default().resolve().mode, LiquidityMode::Off);
        for (bad, field) in [
            (
                LiquiditySpec {
                    levels: Some(0),
                    ..Default::default()
                },
                "levels",
            ),
            (
                LiquiditySpec {
                    spread_bps: Some(f64::NAN),
                    ..Default::default()
                },
                "spreadBps",
            ),
            (
                LiquiditySpec {
                    level_step_ticks: Some(0),
                    ..Default::default()
                },
                "levelStepTicks",
            ),
            (
                LiquiditySpec {
                    level_quantity: Some(0),
                    ..Default::default()
                },
                "levelQuantity",
            ),
            (
                LiquiditySpec {
                    max_inventory: Some(0),
                    ..Default::default()
                },
                "maxInventory",
            ),
        ] {
            assert!(
                matches!(bad.validate(), Err(LiquidityError::OutOfRange { field: f, .. }) if f == field),
                "{field}"
            );
        }
    }
}
