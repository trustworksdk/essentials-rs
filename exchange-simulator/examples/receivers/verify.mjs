// Verifies exchange-simulator webhook signatures. Node.js 18+, no dependencies.
// Run it to check it against a known signature: node verify.mjs
import { createHmac, timingSafeEqual } from "node:crypto";

/**
 * True if `signature` (the X-Signature header) holds a valid HMAC-SHA256 of the raw body under `secret`, and
 * `timestamp` (the X-Signature-Timestamp header) is within `toleranceSeconds` of now. During the grace period after
 * a secret rotation the header carries several comma-separated values; one match is enough. Pass the body exactly as
 * received (a Buffer), before any JSON parsing; with Express, use express.raw({ type: "application/json" }).
 */
export function verify(secret, timestamp, signature, rawBody, nowSeconds = Date.now() / 1000, toleranceSeconds = 300) {
  const expected = Buffer.from(
    "sha256=" + createHmac("sha256", secret).update(`${timestamp}.`).update(rawBody).digest("hex"),
  );
  const matches = signature
    .split(",")
    .map((value) => Buffer.from(value.trim()))
    .some((value) => value.length === expected.length && timingSafeEqual(value, expected));
  return matches && Math.abs(nowSeconds - Number(timestamp)) <= toleranceSeconds;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const secret = "whsec_test_0123456789";
  const timestamp = "1760443200";
  const body = Buffer.from('{"eventId":"e-1"}');
  const valid = "sha256=cde596a7a23fd7c03b27e1cf91e6fba42e15936f765508360d753019cd935bc1";
  const now = 1760443200 + 10;
  const checks = {
    "a valid signature": verify(secret, timestamp, valid, body, now),
    "a rotation's second value": verify(secret, timestamp, `sha256=00, ${valid}`, body, now),
    "not a changed body": !verify(secret, timestamp, valid, Buffer.concat([body, Buffer.from(" ")]), now),
    "not another secret": !verify("another-secret-0123", timestamp, valid, body, now),
    "not an old timestamp": !verify(secret, timestamp, valid, body, now + 3600),
  };
  for (const [name, ok] of Object.entries(checks)) console.log(`${ok ? "ok  " : "FAIL"}  ${name}`);
  process.exit(Object.values(checks).every(Boolean) ? 0 : 1);
}
