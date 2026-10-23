//! Exchange fees (plan item 6.22, SR-37): what each side of an execution pays the exchange, a share of the
//! execution's value in basis points, the maker's (the resting order's) and the taker's (the incoming order's) rate
//! apart, with a minimum per execution and side. A participant may have rates of its own or be exempt, such as the
//! liquidity provider. Fees are in minor units of the instrument's currency, rounded half up, and never negative.

use std::collections::BTreeMap;

use serde::Deserialize;

/// The fee schedule (`fees:`); free when every rate and the minimum are zero, the default.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeeSchedule {
    pub maker_bps: f64,
    pub taker_bps: f64,
    /// Per execution and side, in major units of the instrument's currency, e.g. `"1.00"`.
    pub minimum: Option<String>,
    /// Rates of their own, by participant ID; a field left out is the schedule's.
    pub participants: BTreeMap<String, FeeRates>,
    /// Participants who pay nothing, e.g. the liquidity provider (`LP`).
    pub exempt: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeeRates {
    pub maker_bps: Option<f64>,
    pub taker_bps: Option<f64>,
    pub minimum: Option<String>,
}

impl FeeSchedule {
    pub fn validate(&self) -> Result<(), String> {
        let rate = |name: &str, bps: f64| {
            if (0.0..=10_000.0).contains(&bps) {
                Ok(())
            } else {
                Err(format!(
                    "fees{name} must be between 0 and 10000 basis points"
                ))
            }
        };
        let minimum = |name: &str, text: &Option<String>| match text {
            Some(text) if parse_minor(text, 8).is_none() => Err(format!(
                "fees{name}.minimum must be a non-negative decimal, got {text:?}"
            )),
            _ => Ok(()),
        };
        rate(".maker_bps", self.maker_bps)?;
        rate(".taker_bps", self.taker_bps)?;
        minimum("", &self.minimum)?;
        for (id, rates) in &self.participants {
            if let Some(bps) = rates.maker_bps {
                rate(&format!(".participants.{id}.maker_bps"), bps)?;
            }
            if let Some(bps) = rates.taker_bps {
                rate(&format!(".participants.{id}.taker_bps"), bps)?;
            }
            minimum(&format!(".participants.{id}"), &rates.minimum)?;
        }
        Ok(())
    }

    /// The fee `participant` pays on an execution worth `value_minor`, as maker or taker, in a currency with `scale`
    /// decimals: the rate's share, rounded half up, but at least the minimum. Zero when exempt, or when the rate and
    /// the minimum are both zero.
    pub fn fee(&self, participant: &str, maker: bool, value_minor: i64, scale: u8) -> i64 {
        if self.exempt.iter().any(|p| p == participant) {
            return 0;
        }
        let own = self.participants.get(participant);
        let bps = match maker {
            true => own.and_then(|r| r.maker_bps).unwrap_or(self.maker_bps),
            false => own.and_then(|r| r.taker_bps).unwrap_or(self.taker_bps),
        };
        let minimum = own
            .and_then(|r| r.minimum.as_ref())
            .or(self.minimum.as_ref())
            .and_then(|m| parse_minor(m, scale))
            .unwrap_or(0);
        let share = (value_minor as f64 * bps / 10_000.0).round() as i64;
        share.max(minimum).max(0)
    }
}

/// A non-negative decimal in major units as minor units with `scale` decimals; decimals beyond the scale are cut.
fn parse_minor(text: &str, scale: u8) -> Option<i64> {
    let (whole, fraction) = text.trim().split_once('.').unwrap_or((text.trim(), ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole
            .chars()
            .chain(fraction.chars())
            .all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let mut digits: String = fraction.chars().take(usize::from(scale)).collect();
    while digits.len() < usize::from(scale) {
        digits.push('0');
    }
    format!("{whole}{digits}").parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schedule() -> FeeSchedule {
        FeeSchedule {
            maker_bps: 1.0,
            taker_bps: 2.5,
            minimum: Some("1.00".into()),
            participants: [(
                "B".to_string(),
                FeeRates {
                    taker_bps: Some(0.5),
                    ..Default::default()
                },
            )]
            .into(),
            exempt: vec!["LP".into()],
        }
    }

    #[test]
    fn fees_are_a_share_of_the_value_with_a_minimum() {
        let s = schedule();
        // 100,000.00 DKK: 1 bp is 10.00, 2.5 bp 25.00.
        assert_eq!(s.fee("A", true, 10_000_000, 2), 1_000);
        assert_eq!(s.fee("A", false, 10_000_000, 2), 2_500);
        assert_eq!(s.fee("A", false, 10_000, 2), 100, "the minimum");
        assert_eq!(
            s.fee("A", false, 1_000_020, 2),
            250,
            "rounded half up: 250.005"
        );
        assert_eq!(s.fee("B", false, 10_000_000, 2), 500, "B's own taker rate");
        assert_eq!(
            s.fee("B", true, 10_000_000, 2),
            1_000,
            "the schedule's maker rate"
        );
        assert_eq!(s.fee("LP", false, 10_000_000, 2), 0, "exempt");
        assert_eq!(
            FeeSchedule::default().fee("A", false, 10_000_000, 2),
            0,
            "free"
        );
        assert_eq!(
            s.fee("A", false, 10, 0),
            1,
            "the minimum in a currency without decimals"
        );
    }

    #[test]
    fn schedules_are_validated() {
        assert!(schedule().validate().is_ok());
        for bad in [
            FeeSchedule {
                maker_bps: -1.0,
                ..Default::default()
            },
            FeeSchedule {
                taker_bps: 10_001.0,
                ..Default::default()
            },
            FeeSchedule {
                minimum: Some("-1".into()),
                ..Default::default()
            },
            FeeSchedule {
                minimum: Some("abc".into()),
                ..Default::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        assert_eq!(parse_minor("1.005", 2), Some(100));
        assert_eq!(parse_minor("12", 2), Some(1_200));
        assert_eq!(parse_minor(".5", 2), Some(50));
    }
}
