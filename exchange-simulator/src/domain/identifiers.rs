//! Format and check-digit validation for external instrument identifiers (MIC, ISIN, CUSIP).

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IdentifierError {
    #[error("{kind} must be {expected} characters, got {actual}")]
    Length {
        kind: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("{kind} has an invalid character at position {position}")]
    Character { kind: &'static str, position: usize },
    #[error("{kind} check digit is wrong: expected {expected}")]
    CheckDigit { kind: &'static str, expected: char },
}

/// ISO 10383 market identifier code: four uppercase alphanumeric characters.
pub fn validate_mic(mic: &str) -> Result<(), IdentifierError> {
    check_length("MIC", mic, 4)?;
    check_chars("MIC", mic, |_, c| {
        c.is_ascii_uppercase() || c.is_ascii_digit()
    })
}

/// ISO 6166 ISIN: two-letter country prefix, nine alphanumerics, and a Luhn check digit computed over the
/// letters-expanded-to-numbers form.
pub fn validate_isin(isin: &str) -> Result<(), IdentifierError> {
    const KIND: &str = "ISIN";
    check_length(KIND, isin, 12)?;
    check_chars(KIND, isin, |i, c| match i {
        0 | 1 => c.is_ascii_uppercase(),
        11 => c.is_ascii_digit(),
        _ => c.is_ascii_uppercase() || c.is_ascii_digit(),
    })?;
    let digits: Vec<u32> = isin[..11]
        .chars()
        .flat_map(|c| {
            let value = c.to_digit(36).expect("checked alphanumeric");
            if value >= 10 {
                vec![value / 10, value % 10]
            } else {
                vec![value]
            }
        })
        .collect();
    // Luhn: double every second digit counting from the rightmost payload digit (the check digit sits right of it).
    let sum: u32 = digits
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &d)| if i % 2 == 0 { digit_sum(d * 2) } else { d })
        .sum();
    expect_check_digit(KIND, isin, (10 - sum % 10) % 10)
}

/// CUSIP: eight characters from `0-9A-Z*@#` and a modulus-10 "double-add-double" check digit.
pub fn validate_cusip(cusip: &str) -> Result<(), IdentifierError> {
    const KIND: &str = "CUSIP";
    check_length(KIND, cusip, 9)?;
    check_chars(KIND, cusip, |i, c| match i {
        8 => c.is_ascii_digit(),
        _ => c.is_ascii_uppercase() || c.is_ascii_digit() || matches!(c, '*' | '@' | '#'),
    })?;
    let sum: u32 = cusip[..8]
        .chars()
        .enumerate()
        .map(|(i, c)| {
            let value = match c {
                '*' => 36,
                '@' => 37,
                '#' => 38,
                _ => c.to_digit(36).expect("checked alphanumeric"),
            };
            let value = if i % 2 == 1 { value * 2 } else { value };
            value / 10 + value % 10
        })
        .sum();
    expect_check_digit(KIND, cusip, (10 - sum % 10) % 10)
}

pub(super) fn check_length(
    kind: &'static str,
    value: &str,
    expected: usize,
) -> Result<(), IdentifierError> {
    // Counted in chars so a multi-byte character reports a length, not a slicing panic later.
    let actual = value.chars().count();
    if actual != expected || value.len() != expected {
        return Err(IdentifierError::Length {
            kind,
            expected,
            actual,
        });
    }
    Ok(())
}

fn check_chars(
    kind: &'static str,
    value: &str,
    allowed: impl Fn(usize, char) -> bool,
) -> Result<(), IdentifierError> {
    match value.chars().enumerate().find(|&(i, c)| !allowed(i, c)) {
        Some((position, _)) => Err(IdentifierError::Character { kind, position }),
        None => Ok(()),
    }
}

fn digit_sum(n: u32) -> u32 {
    n / 10 + n % 10
}

fn expect_check_digit(
    kind: &'static str,
    value: &str,
    expected: u32,
) -> Result<(), IdentifierError> {
    let expected = char::from_digit(expected, 10).expect("single digit");
    if value.ends_with(expected) {
        Ok(())
    } else {
        Err(IdentifierError::CheckDigit { kind, expected })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_isins() {
        for isin in [
            "US0378331005",
            "DK0010274414",
            "GB0002634946",
            "US5949181045",
            "DE000BAY0017",
        ] {
            assert_eq!(validate_isin(isin), Ok(()), "{isin}");
        }
    }

    #[test]
    fn invalid_isins() {
        assert!(matches!(
            validate_isin("US0378331006"),
            Err(IdentifierError::CheckDigit { expected: '5', .. })
        ));
        assert!(matches!(
            validate_isin("US037833100"),
            Err(IdentifierError::Length { .. })
        ));
        assert!(matches!(
            validate_isin("1S0378331005"),
            Err(IdentifierError::Character { position: 0, .. })
        ));
        assert!(matches!(
            validate_isin("us0378331005"),
            Err(IdentifierError::Character { position: 0, .. })
        ));
        assert!(matches!(
            validate_isin("US03783310ü"),
            Err(IdentifierError::Length { .. })
        ));
    }

    #[test]
    fn valid_cusips() {
        for cusip in ["037833100", "594918104", "38259P508", "931142103"] {
            assert_eq!(validate_cusip(cusip), Ok(()), "{cusip}");
        }
    }

    #[test]
    fn invalid_cusips() {
        assert!(matches!(
            validate_cusip("037833101"),
            Err(IdentifierError::CheckDigit { expected: '0', .. })
        ));
        assert!(matches!(
            validate_cusip("03783310"),
            Err(IdentifierError::Length { .. })
        ));
        assert!(matches!(
            validate_cusip("03783310A"),
            Err(IdentifierError::Character { position: 8, .. })
        ));
    }

    #[test]
    fn mics() {
        assert_eq!(validate_mic("XNAS"), Ok(()));
        assert_eq!(validate_mic("XCSE"), Ok(()));
        assert!(validate_mic("xnas").is_err());
        assert!(validate_mic("XNA").is_err());
    }
}
