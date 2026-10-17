//! IBAN validation (ISO 13616): country code, check digits, then a country-specific account number of up to 30
//! letters and digits; the whole, rearranged, is 1 modulo 97.

/// The compact form: spaces removed, letters uppercased (`dk50 0040 ...` becomes `DK500040...`).
pub fn normalize(iban: &str) -> String {
    iban.chars()
        .filter(|c| !c.is_whitespace())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

/// Checks a compact IBAN: 15 to 34 characters, two letters, two digits, letters and digits after, and the mod-97 check.
pub fn validate(iban: &str) -> Result<(), String> {
    let bytes = iban.as_bytes();
    if !(15..=34).contains(&bytes.len()) {
        return Err(format!(
            "an IBAN has 15 to 34 characters, got {}",
            bytes.len()
        ));
    }
    if !bytes[..2].iter().all(u8::is_ascii_uppercase) || !bytes[2..4].iter().all(u8::is_ascii_digit)
    {
        return Err("an IBAN starts with a country code and two check digits".into());
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return Err("an IBAN has only letters and digits".into());
    }
    let remainder = bytes[4..].iter().chain(&bytes[..4]).fold(0_u32, |acc, &b| {
        let value = u32::from(if b.is_ascii_digit() {
            b - b'0'
        } else {
            b - b'A' + 10
        });
        if value >= 10 {
            (acc * 100 + value) % 97
        } else {
            (acc * 10 + value) % 97
        }
    });
    if remainder != 1 {
        return Err("the IBAN check digits are wrong".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ibans_pass_and_damaged_ones_fail() {
        for iban in [
            "DK5000400440116243",
            "DE89370400440532013000",
            "GB29NWBK60161331926819",
            "NO9386011117947",
        ] {
            assert_eq!(validate(iban), Ok(()), "{iban}");
        }
        assert_eq!(
            normalize("gb29 nwbk 6016 1331 9268 19"),
            "GB29NWBK60161331926819"
        );
        assert!(validate("DK5000400440116244").is_err(), "a changed digit");
        assert!(
            validate("DK0500400440116243").is_err(),
            "swapped check digits"
        );
        assert!(validate("DK50").is_err());
        assert!(validate("5K5000400440116243").is_err());
        assert!(validate("DK50-0400440116243").is_err());
    }
}
