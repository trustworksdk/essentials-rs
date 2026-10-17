//! Amounts: decimal strings on the wire, integer minor units inside, with each currency's number of decimals.

use std::collections::BTreeMap;

/// The accepted currencies and their decimals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Currencies(BTreeMap<String, u8>);

impl Currencies {
    /// From `code → decimals`; codes are uppercased, must be three letters, and have at most 4 decimals.
    pub fn new(configured: &BTreeMap<String, u8>) -> Result<Self, String> {
        let mut currencies = BTreeMap::new();
        for (code, scale) in configured {
            let code = code.to_ascii_uppercase();
            if code.len() != 3 || !code.bytes().all(|b| b.is_ascii_uppercase()) {
                return Err(format!(
                    "payments.currencies: {code} is not a three-letter code"
                ));
            }
            if *scale > 4 {
                return Err(format!(
                    "payments.currencies: {code} has more than 4 decimals"
                ));
            }
            currencies.insert(code, *scale);
        }
        if currencies.is_empty() {
            return Err("payments.currencies must not be empty".into());
        }
        Ok(Self(currencies))
    }

    pub fn scale(&self, code: &str) -> Option<u8> {
        self.0.get(code).copied()
    }
}

/// Parses a decimal amount such as `"1500.00"` or `"12.5"` into minor units with `scale` decimals. It must not have
/// more decimals than the currency, and must be positive unless `allow_negative`.
pub fn parse_amount(text: &str, scale: u8, allow_negative: bool) -> Result<i64, String> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) if allow_negative => (true, rest),
        Some(_) => return Err("amount must be positive".into()),
        None => (false, text),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let valid = !whole.is_empty()
        && whole.bytes().all(|b| b.is_ascii_digit())
        && fraction.bytes().all(|b| b.is_ascii_digit())
        && (digits.contains('.') <= !fraction.is_empty());
    if !valid {
        return Err(format!("amount must be a decimal number, got {text:?}"));
    }
    if fraction.len() > usize::from(scale) {
        return Err(format!("amount has more than {scale} decimals"));
    }
    let padded = format!("{whole}{fraction:0<width$}", width = usize::from(scale));
    let minor: i64 = padded
        .parse()
        .map_err(|_| "amount is too large".to_string())?;
    if minor == 0 && !allow_negative {
        return Err("amount must be positive".into());
    }
    Ok(if negative { -minor } else { minor })
}

/// Formats minor units with `scale` decimals, e.g. 150000 at scale 2 is `"1500.00"`.
pub fn format_amount(minor: i64, scale: u8) -> String {
    let scale = u32::from(scale);
    if scale == 0 {
        return minor.to_string();
    }
    let divisor = 10_i128.pow(scale);
    let value = i128::from(minor);
    let sign = if value < 0 { "-" } else { "" };
    let value = value.abs();
    format!(
        "{sign}{}.{:0width$}",
        value / divisor,
        value % divisor,
        width = scale as usize
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amounts_parse_into_minor_units_and_format_back() {
        assert_eq!(parse_amount("1500.00", 2, false), Ok(150_000));
        assert_eq!(parse_amount("12.5", 2, false), Ok(1250));
        assert_eq!(parse_amount("7", 0, false), Ok(7));
        assert_eq!(parse_amount("-3.25", 2, true), Ok(-325));
        assert_eq!(parse_amount("0.00", 2, true), Ok(0));
        for bad in [
            "",
            "1.",
            ".5",
            "1.234",
            "1,00",
            "abc",
            "-1.00",
            "0",
            "0.00",
            "99999999999999999999",
        ] {
            assert!(parse_amount(bad, 2, false).is_err(), "{bad:?}");
        }
        assert_eq!(format_amount(150_000, 2), "1500.00");
        assert_eq!(format_amount(-5, 2), "-0.05");
        assert_eq!(format_amount(42, 0), "42");
    }

    #[test]
    fn currencies_are_validated() {
        let ok: BTreeMap<String, u8> = [("dkk".to_string(), 2)].into();
        assert_eq!(Currencies::new(&ok).unwrap().scale("DKK"), Some(2));
        for bad in [("DK", 2), ("DKK", 5), ("D1K", 2)] {
            let map: BTreeMap<String, u8> = [(bad.0.to_string(), bad.1)].into();
            assert!(Currencies::new(&map).is_err(), "{bad:?}");
        }
        assert!(Currencies::new(&BTreeMap::new()).is_err());
    }
}
