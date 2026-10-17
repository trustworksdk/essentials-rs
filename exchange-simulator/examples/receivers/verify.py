"""Verifies exchange-simulator webhook signatures. Standard library only; Python 3.10+.

Run it to check it against a known signature: python3 verify.py
"""

import hashlib
import hmac
import time


def verify(secret: bytes, timestamp: str, signature: str, raw_body: bytes, now: float | None = None,
           tolerance: int = 300) -> bool:
    """True if `signature` (the X-Signature header) holds a valid HMAC-SHA256 of the raw body under `secret`, and
    `timestamp` (the X-Signature-Timestamp header) is within `tolerance` seconds of now.

    During the grace period after a secret rotation the header carries several comma-separated values; one match is
    enough. Pass the body exactly as received, before any JSON parsing.
    """
    expected = "sha256=" + hmac.new(secret, timestamp.encode() + b"." + raw_body, hashlib.sha256).hexdigest()
    matches = any(hmac.compare_digest(expected, value.strip()) for value in signature.split(","))
    current = time.time() if now is None else now
    return matches and abs(current - int(timestamp)) <= tolerance


if __name__ == "__main__":
    secret, timestamp, body = b"whsec_test_0123456789", "1760443200", b'{"eventId":"e-1"}'
    valid = "sha256=cde596a7a23fd7c03b27e1cf91e6fba42e15936f765508360d753019cd935bc1"
    now = 1760443200 + 10
    checks = {
        "a valid signature": verify(secret, timestamp, valid, body, now),
        "a rotation's second value": verify(secret, timestamp, "sha256=00, " + valid, body, now),
        "not a changed body": not verify(secret, timestamp, valid, body + b" ", now),
        "not another secret": not verify(b"another-secret-0123", timestamp, valid, body, now),
        "not an old timestamp": not verify(secret, timestamp, valid, body, now + 3600),
    }
    for name, ok in checks.items():
        print(("ok    " if ok else "FAIL  ") + name)
    raise SystemExit(0 if all(checks.values()) else 1)
