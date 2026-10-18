use super::InstrumentId;
use super::identifiers::{
    IdentifierError, check_length, validate_cusip, validate_isin, validate_mic,
};
use crate::pricing::PriceSourceSpec;

type Validator = fn(&str) -> Result<(), IdentifierError>;

/// How strictly external identifiers are checked at the boundary. The SQL schema enforces only lengths.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IdentifierPolicy {
    /// Character sets and check digits are validated.
    #[default]
    Strict,
    /// Only lengths are checked: for explicitly permissive test fixtures.
    Permissive,
}

/// A tradable listing. `symbol` is scoped by `mic`; ISIN and CUSIP identify the security and may be shared by several
/// listings. Prices are integer ticks of `tick_size_minor` minor currency units, where a major unit has
/// `currency_scale` decimal places (100.25 DKK with scale 2 and tick 1 is 10025 ticks).
#[derive(Clone, Debug, PartialEq)]
pub struct Instrument {
    pub id: InstrumentId,
    pub symbol: String,
    pub mic: Option<String>,
    pub isin: Option<String>,
    pub cusip: Option<String>,
    pub currency: String,
    pub currency_scale: u8,
    pub tick_size_minor: u64,
    pub lot_size: u64,
    /// Whether the instrument trades. Orders for a disabled instrument are rejected; cancels still work.
    pub enabled: bool,
    /// The symbol a price source quotes this instrument under (e.g. `AAPL`), which may differ from `symbol`.
    pub reference_symbol: Option<String>,
    /// The instrument's own price source; `None` takes `price_sources.default` (see [`crate::pricing`]).
    pub price_source: Option<PriceSourceSpec>,
}

/// A change to a listed instrument: only trading status, the reference symbol and the price source may change after
/// listing. `None` leaves a field as it is; `Some(None)` clears an optional one.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InstrumentChange {
    pub enabled: Option<bool>,
    pub reference_symbol: Option<Option<String>>,
    pub price_source: Option<Option<PriceSourceSpec>>,
}

impl InstrumentChange {
    /// Whether the change names no field.
    pub fn is_empty(&self) -> bool {
        self.enabled.is_none() && self.reference_symbol.is_none() && self.price_source.is_none()
    }

    /// Applies the change and returns the wire names of the fields whose value it changed, in a fixed order.
    pub fn apply(&self, instrument: &mut Instrument) -> Vec<&'static str> {
        let mut changed = Vec::new();
        if let Some(enabled) = self.enabled
            && enabled != instrument.enabled
        {
            instrument.enabled = enabled;
            changed.push("enabled");
        }
        if let Some(reference_symbol) = &self.reference_symbol
            && *reference_symbol != instrument.reference_symbol
        {
            instrument.reference_symbol = reference_symbol.clone();
            changed.push("referenceSymbol");
        }
        if let Some(price_source) = &self.price_source
            && *price_source != instrument.price_source
        {
            instrument.price_source = price_source.clone();
            changed.push("priceSource");
        }
        changed
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InstrumentError {
    #[error("{0} must not be blank")]
    Blank(&'static str),
    #[error("currency must be a three-letter uppercase ISO 4217 code")]
    Currency,
    #[error("currency_scale must be between 0 and 9")]
    CurrencyScale,
    #[error("reference_symbol must be 1 to 64 printable ASCII characters without spaces")]
    ReferenceSymbol,
    #[error("{0} must be between 1 and {max}", max = i64::MAX)]
    NotPositive(&'static str),
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PriceError {
    #[error("price is not a decimal number")]
    Syntax,
    #[error("price has more than {0} decimal places")]
    Scale(u8),
    #[error("price must be positive")]
    NotPositive,
    #[error("price is not a multiple of the tick size")]
    OffTick,
    #[error("price is out of range")]
    Overflow,
}

impl Instrument {
    /// Checks the registry invariants. Identifier format and check digits are checked only under
    /// [`IdentifierPolicy::Strict`]; lengths always.
    pub fn validate(&self, policy: IdentifierPolicy) -> Result<(), InstrumentError> {
        if self.id.0.trim().is_empty() {
            return Err(InstrumentError::Blank("id"));
        }
        if self.symbol.trim().is_empty() {
            return Err(InstrumentError::Blank("symbol"));
        }
        if self.currency.len() != 3 || !self.currency.bytes().all(|b| b.is_ascii_uppercase()) {
            return Err(InstrumentError::Currency);
        }
        if self.currency_scale > 9 {
            return Err(InstrumentError::CurrencyScale);
        }
        for (name, value) in [
            ("tick_size_minor", self.tick_size_minor),
            ("lot_size", self.lot_size),
        ] {
            if value == 0 || i64::try_from(value).is_err() {
                return Err(InstrumentError::NotPositive(name));
            }
        }
        if let Some(symbol) = &self.reference_symbol
            && (!(1..=64).contains(&symbol.len()) || !symbol.bytes().all(|b| b.is_ascii_graphic()))
        {
            return Err(InstrumentError::ReferenceSymbol);
        }
        let checks: [(Option<&String>, Validator, &'static str, usize); 3] = [
            (self.mic.as_ref(), validate_mic, "MIC", 4),
            (self.isin.as_ref(), validate_isin, "ISIN", 12),
            (self.cusip.as_ref(), validate_cusip, "CUSIP", 9),
        ];
        for (value, validate, kind, length) in checks {
            match (value, policy) {
                (None, _) => {}
                (Some(value), IdentifierPolicy::Strict) => validate(value)?,
                (Some(value), IdentifierPolicy::Permissive) => check_length(kind, value, length)?,
            }
        }
        Ok(())
    }

    /// Converts a decimal price in major currency units (e.g. `"100.25"`) to integer ticks, rejecting prices that are
    /// not positive or not on the tick grid.
    pub fn price_to_ticks(&self, price: &str) -> Result<i64, PriceError> {
        let (whole, fraction) = price.split_once('.').unwrap_or((price, ""));
        let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
        if whole.is_empty()
            || !all_digits(whole)
            || !all_digits(fraction)
            || (price.contains('.') && fraction.is_empty())
        {
            return Err(PriceError::Syntax);
        }
        let scale = usize::from(self.currency_scale);
        if fraction.len() > scale {
            return Err(PriceError::Scale(self.currency_scale));
        }
        let mut minor: i64 = 0;
        for digit in whole
            .bytes()
            .chain(fraction.bytes())
            .chain(std::iter::repeat_n(b'0', scale - fraction.len()))
        {
            minor = minor
                .checked_mul(10)
                .and_then(|m| m.checked_add(i64::from(digit - b'0')))
                .ok_or(PriceError::Overflow)?;
        }
        if minor == 0 {
            return Err(PriceError::NotPositive);
        }
        let tick = self.tick_size_i64();
        if minor % tick != 0 {
            return Err(PriceError::OffTick);
        }
        Ok(minor / tick)
    }

    /// Formats integer ticks as a decimal price in major currency units with exactly `currency_scale` decimals.
    pub fn ticks_to_price(&self, ticks: i64) -> String {
        let minor = i128::from(ticks) * i128::from(self.tick_size_i64());
        let scale = u32::from(self.currency_scale);
        if scale == 0 {
            return minor.to_string();
        }
        let divisor = 10_i128.pow(scale);
        let sign = if minor < 0 { "-" } else { "" };
        let minor = minor.abs();
        format!(
            "{sign}{}.{:0width$}",
            minor / divisor,
            minor % divisor,
            width = scale as usize
        )
    }

    /// Whether `quantity` is a positive whole number of lots.
    pub fn is_valid_quantity(&self, quantity: u64) -> bool {
        quantity > 0 && quantity.is_multiple_of(self.lot_size) && i64::try_from(quantity).is_ok()
    }

    fn tick_size_i64(&self) -> i64 {
        i64::try_from(self.tick_size_minor).expect("validated tick size fits i64")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acme() -> Instrument {
        Instrument {
            id: InstrumentId("ACME".into()),
            symbol: "ACME".into(),
            mic: Some("XCSE".into()),
            isin: None,
            cusip: None,
            currency: "DKK".into(),
            currency_scale: 2,
            tick_size_minor: 1,
            lot_size: 1,
            enabled: true,
            reference_symbol: None,
            price_source: None,
        }
    }

    #[test]
    fn validates_registry_fields() {
        assert_eq!(acme().validate(IdentifierPolicy::Strict), Ok(()));
        let blank_symbol = Instrument {
            symbol: " ".into(),
            ..acme()
        };
        assert_eq!(
            blank_symbol.validate(IdentifierPolicy::Strict),
            Err(InstrumentError::Blank("symbol"))
        );
        let bad_currency = Instrument {
            currency: "dkk".into(),
            ..acme()
        };
        assert_eq!(
            bad_currency.validate(IdentifierPolicy::Strict),
            Err(InstrumentError::Currency)
        );
        let zero_tick = Instrument {
            tick_size_minor: 0,
            ..acme()
        };
        assert_eq!(
            zero_tick.validate(IdentifierPolicy::Strict),
            Err(InstrumentError::NotPositive("tick_size_minor"))
        );
        let zero_lot = Instrument {
            lot_size: 0,
            ..acme()
        };
        assert_eq!(
            zero_lot.validate(IdentifierPolicy::Strict),
            Err(InstrumentError::NotPositive("lot_size"))
        );
    }

    #[test]
    fn reference_symbols_are_short_printable_and_without_spaces() {
        for ok in ["AAPL", "BINANCE:BTCUSDT", "OANDA:EUR_USD", &"X".repeat(64)] {
            let listed = Instrument {
                reference_symbol: Some(ok.to_string()),
                ..acme()
            };
            assert_eq!(listed.validate(IdentifierPolicy::Strict), Ok(()), "{ok}");
        }
        for bad in ["", "A B", "AAPL\n", "ÆØÅ", &"X".repeat(65)] {
            let listed = Instrument {
                reference_symbol: Some(bad.to_string()),
                ..acme()
            };
            assert_eq!(
                listed.validate(IdentifierPolicy::Permissive),
                Err(InstrumentError::ReferenceSymbol),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_change_reports_only_the_fields_whose_value_changed() {
        let mut instrument = acme();
        let unchanged = InstrumentChange {
            enabled: Some(true),
            reference_symbol: Some(None),
            price_source: Some(None),
        };
        assert!(unchanged.apply(&mut instrument).is_empty());
        let change = InstrumentChange {
            enabled: Some(false),
            reference_symbol: Some(Some("ACME.CO".into())),
            price_source: None,
        };
        assert_eq!(
            change.apply(&mut instrument),
            ["enabled", "referenceSymbol"]
        );
        assert!(!instrument.enabled);
        assert_eq!(instrument.reference_symbol.as_deref(), Some("ACME.CO"));
        let clear = InstrumentChange {
            reference_symbol: Some(None),
            ..Default::default()
        };
        assert_eq!(clear.apply(&mut instrument), ["referenceSymbol"]);
        assert_eq!(instrument.reference_symbol, None);
        assert!(InstrumentChange::default().is_empty());
        assert!(!clear.is_empty());
    }

    #[test]
    fn strict_policy_checks_check_digits_and_permissive_only_lengths() {
        let with_isin = |isin: &str| Instrument {
            isin: Some(isin.into()),
            ..acme()
        };
        assert_eq!(
            with_isin("US0378331005").validate(IdentifierPolicy::Strict),
            Ok(())
        );
        assert!(matches!(
            with_isin("US0378331006").validate(IdentifierPolicy::Strict),
            Err(InstrumentError::Identifier(
                IdentifierError::CheckDigit { .. }
            ))
        ));
        assert_eq!(
            with_isin("US0378331006").validate(IdentifierPolicy::Permissive),
            Ok(())
        );
        assert!(
            with_isin("US037833100")
                .validate(IdentifierPolicy::Permissive)
                .is_err()
        );

        let with_cusip = |cusip: &str| Instrument {
            cusip: Some(cusip.into()),
            ..acme()
        };
        assert_eq!(
            with_cusip("037833100").validate(IdentifierPolicy::Strict),
            Ok(())
        );
        assert!(
            with_cusip("037833101")
                .validate(IdentifierPolicy::Strict)
                .is_err()
        );
        assert_eq!(
            with_cusip("037833101").validate(IdentifierPolicy::Permissive),
            Ok(())
        );
    }

    #[test]
    fn converts_decimal_prices_to_ticks_and_back() {
        let acme = acme();
        assert_eq!(acme.price_to_ticks("100.25"), Ok(10025));
        assert_eq!(acme.price_to_ticks("100.2"), Ok(10020));
        assert_eq!(acme.price_to_ticks("100"), Ok(10000));
        assert_eq!(acme.ticks_to_price(10025), "100.25");
        assert_eq!(acme.ticks_to_price(5), "0.05");

        let five_ore = Instrument {
            tick_size_minor: 5,
            ..acme.clone()
        };
        assert_eq!(five_ore.price_to_ticks("100.25"), Ok(2005));
        assert_eq!(five_ore.ticks_to_price(2005), "100.25");
        assert_eq!(five_ore.price_to_ticks("100.26"), Err(PriceError::OffTick));

        let whole_units = Instrument {
            currency_scale: 0,
            ..acme.clone()
        };
        assert_eq!(whole_units.ticks_to_price(42), "42");
    }

    #[test]
    fn rejects_malformed_prices() {
        let acme = acme();
        assert_eq!(acme.price_to_ticks("100.255"), Err(PriceError::Scale(2)));
        assert_eq!(acme.price_to_ticks("0.00"), Err(PriceError::NotPositive));
        for bad in ["", ".5", "5.", "-1", "1e3", "1,5", " 1"] {
            assert_eq!(acme.price_to_ticks(bad), Err(PriceError::Syntax), "{bad:?}");
        }
        assert_eq!(
            acme.price_to_ticks("99999999999999999999"),
            Err(PriceError::Overflow)
        );
    }

    #[test]
    fn quantities_must_be_whole_lots() {
        let round_lots = Instrument {
            lot_size: 100,
            ..acme()
        };
        assert!(round_lots.is_valid_quantity(200));
        assert!(!round_lots.is_valid_quantity(150));
        assert!(!round_lots.is_valid_quantity(0));
    }
}
