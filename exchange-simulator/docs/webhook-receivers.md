# Writing a webhook receiver

## The contract

For every event a subscription wants, the exchange sends a `POST` with the event envelope as a JSON body. The
[README](../README.md#get-apieventsafterlimiteventtype) describes the envelope and every event type and payload. The
request carries these headers:

| Header | |
|---|---|
| `X-Event-Id` | The event's ID: deduplicate on it |
| `X-Event-Type` | e.g. `TradeExecuted` |
| `X-Event-Sequence` | The event's position in the exchange-wide log |
| `X-Subscription-Id` | Which subscription this delivery is for |
| `X-Delivery-Attempt` | 1 for the first attempt, then 2, 3, … |
| `X-Signature-Timestamp` | Signed subscriptions only: unix seconds at the time of this attempt |
| `X-Signature` | Signed subscriptions only: `sha256=<hex>`, or during a secret rotation `sha256=<new>, sha256=<previous>` |

**How the exchange treats your answer:**
- **A 2xx answer** means delivered.
- **Anything else is retried with backoff:** any other status (4xx included), a connection error, or no answer
  within `webhooks.request_timeout_ms` (5 s).
- **Dead letters:** after `webhooks.max_attempts` (10) the delivery is dead-lettered. It can still be retried later.
  The retry span is in [Configuration](configuration.md#webhooks).

**Delivery is at least once.** Every committed event is delivered, but possibly more than once: after a retry whose
first answer was lost, after a restart, or after a replay.

## A correct receiver, in five rules

1. **Verify the signature over the raw body, before parsing it.** Re-serialized JSON does not match. Accept the
   request if *any* value in `X-Signature` matches; during a secret rotation there are two. Reject timestamps far
   from your clock (5 minutes is usual) to stop replays.
2. **Process each `eventId` once.** Record the IDs you have processed, in the same transaction as the effect if you
   can. Answer a duplicate with 2xx and do nothing.
   - Record an ID only when the processing succeeded. A delivery you answered with an error comes back, and must not
     be skipped as a duplicate then.
3. **Answer quickly.** Persist the event, or its effect, and answer. Don't do slow work before answering: an attempt
   that takes longer than the request timeout counts as failed and is sent again.
4. **Answer 2xx for events you will never process.** Since everything that isn't 2xx is retried, a permanently
   unprocessable event would otherwise be retried until it is dead-lettered. Park it on your side instead, and
   answer with an error only when trying again later could help.
5. **Use `sequence` for order.** Deliveries of one subscription are sent concurrently and can arrive out of order.
   - **Strict order:** declare the subscription with `ordered: true`. It then has one delivery in flight at a time,
     in `sequence` order. A failing delivery holds up the ones behind it until it succeeds or is dead-lettered.
   - **Gaps:** after a dead letter, an ordered consumer sees a gap in `sequence`. Fill it from
     `GET /api/events?after=`.

The sample receiver in [`examples/compose/receiver.py`](../examples/compose/receiver.py) follows these rules in about
a hundred lines of Python.

## Verifying signatures

The signature is the HMAC-SHA256, under the shared secret, of the timestamp header, a `.`, and the raw body. Each of
these functions is in [`examples/receivers`](../examples/receivers), and running the file checks it against a known
signature. The containerized tests run all three.

**Python** (3.10+, standard library), [`verify.py`](../examples/receivers/verify.py):

```python
import hashlib, hmac, time

def verify(secret: bytes, timestamp: str, signature: str, raw_body: bytes, now: float | None = None,
           tolerance: int = 300) -> bool:
    expected = "sha256=" + hmac.new(secret, timestamp.encode() + b"." + raw_body, hashlib.sha256).hexdigest()
    matches = any(hmac.compare_digest(expected, value.strip()) for value in signature.split(","))
    current = time.time() if now is None else now
    return matches and abs(current - int(timestamp)) <= tolerance
```

**Java** (17+, JDK only), [`Verify.java`](../examples/receivers/Verify.java):

```java
public static boolean verify(byte[] secret, String timestamp, String signature, byte[] rawBody, long nowSeconds,
                             long toleranceSeconds) throws Exception {
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(secret, "HmacSHA256"));
    mac.update((timestamp + ".").getBytes(StandardCharsets.US_ASCII));
    byte[] expected = ("sha256=" + HexFormat.of().formatHex(mac.doFinal(rawBody)))
            .getBytes(StandardCharsets.US_ASCII);
    boolean matches = false;
    for (String value : signature.split(",")) {
        matches |= MessageDigest.isEqual(expected, value.trim().getBytes(StandardCharsets.US_ASCII));
    }
    return matches && Math.abs(nowSeconds - Long.parseLong(timestamp)) <= toleranceSeconds;
}
```

In a Spring Boot controller, take the body as bytes so it reaches you unparsed. This is a sketch; `processOnce`
stands for your idempotent handling:

```java
@PostMapping(path = "/webhooks/trades", consumes = "application/json")
ResponseEntity<Void> trade(@RequestHeader("X-Signature-Timestamp") String timestamp,
                           @RequestHeader("X-Signature") String signature,
                           @RequestBody byte[] body) throws Exception {
    if (!Verify.verify(secret, timestamp, signature, body)) {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
    }
    JsonNode event = objectMapper.readTree(body);
    processOnce(event.get("eventId").asText(), event);   // no-op for an eventId already processed
    return ResponseEntity.noContent().build();
}
```

**Node.js** (18+, no dependencies), [`verify.mjs`](../examples/receivers/verify.mjs). With Express, read the body
with `express.raw({ type: "application/json" })`:

```js
import { createHmac, timingSafeEqual } from "node:crypto";

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
```

**Secrets from files:** the exchange trims whitespace around a secret it reads from a file (`secret_file`), so
`echo "…" > secret.txt` works. A receiver that reads the same file must trim it too.

## Rotating a secret

During the grace period the exchange signs every delivery with both the new and the previous secret. A receiver
holding either one accepts the request, so the two sides can switch at different moments.

**A subscription created through the API:**
1. Rotate: `POST /api/webhooks/{id}/secret` with `{ "secret": "<new>", "graceSeconds": 3600 }`.
2. Within the grace period, give the receiver the new secret.
3. Afterwards only the new secret signs. The response's `previousSecretValidUntil` shows when that happens.

**A subscription declared in configuration:**
1. Change the secret where the exchange reads it (the file, the variable or the YAML), then restart the exchange.
   The previous secret keeps signing for `webhooks.secret_grace_seconds` (an hour by default), across further
   restarts too.
2. Within that period, give the receiver the new secret.

With the Compose example's shared secret file, both containers read the same file. Change it and restart both.

## Catching up and replaying

**To catch up after losing data, or to start a new consumer from history:**
- **Pull:** read `GET /api/events?after=<last sequence you have>` page by page until it returns `[]`. This is the
  log itself and needs no subscription. A consumer of a few event types filters with `eventType=A,B` and continues
  from each answer's `Event-Log-Scanned-Through` header, not from the last event it got, so it never scans the same
  stretch of the log twice.
- **Push:** `POST /api/webhooks/{id}/redeliver?after=<sequence>` queues every event after that sequence for the
  subscription again, including events from before it existed. They arrive as ordinary deliveries, so your
  deduplication applies.

## Testing your receiver

A test deployment, with `test_control.enabled: true`, can damage the copies of events it sends to a subscription:
- missing fields, explicit nulls, invalid values;
- duplicates, delays, and events held back to arrive out of order.

The canonical events are never changed. See the README's [Fault injection](../README.md#fault-injection). Together
with [scenarios](../README.md#scenarios), which give reproducible runs on a virtual clock, this checks rules 1 to 5
above before production does.
