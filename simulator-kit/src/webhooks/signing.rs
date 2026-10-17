//! HMAC-SHA256 request signing for webhook deliveries.
//!
//! For a subscription registered with a secret, each delivery attempt carries:
//! - `X-Signature-Timestamp: <unix seconds>`, the time of the attempt;
//! - `X-Signature: sha256=<lowercase hex>`, the HMAC-SHA256 under the secret of `"{timestamp}.{raw request body}"`.
//!   During the grace period after a secret rotation the header carries one value per secret, comma-separated and the
//!   current secret first (`sha256=<new>, sha256=<previous>`); a consumer accepts the request if any value matches.
//!
//! A consumer recomputes the HMAC over the raw body bytes as received (not a re-serialization), compares in constant
//! time, and rejects timestamps too far from its own clock to stop replays. Retries are signed again with a new
//! timestamp; the body, and so the `eventId`, are unchanged.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

pub const SIGNATURE_HEADER: &str = "X-Signature";
pub const TIMESTAMP_HEADER: &str = "X-Signature-Timestamp";

/// The `X-Signature` value for `body` sent at `timestamp` (unix seconds).
pub fn sign(secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    let mut signature = String::with_capacity(7 + 2 * digest.len());
    signature.push_str("sha256=");
    for byte in digest {
        signature.push_str(&format!("{byte:02x}"));
    }
    signature
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_an_independent_hmac_sha256() {
        // python3 -c "import hmac,hashlib; print(hmac.new(b'whsec_test_0123456789',
        //   b'1760443200.{\"eventId\":\"e-1\"}', hashlib.sha256).hexdigest())"
        assert_eq!(
            sign(
                "whsec_test_0123456789",
                1_760_443_200,
                br#"{"eventId":"e-1"}"#
            ),
            "sha256=cde596a7a23fd7c03b27e1cf91e6fba42e15936f765508360d753019cd935bc1"
        );
    }

    #[test]
    fn timestamp_body_and_secret_all_change_the_signature() {
        let base = sign("secret-0123456789", 1, b"{}");
        assert_ne!(base, sign("secret-0123456789", 2, b"{}"));
        assert_ne!(base, sign("secret-0123456789", 1, b"{ }"));
        assert_ne!(base, sign("secret-0123456789X", 1, b"{}"));
    }
}
