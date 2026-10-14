# exchange-simulator

A lightweight stock exchange for integration and performance testing. It does real price-time matching over integer
ticks, keeps SQLite as the authoritative store, and delivers canonical events through durable, retried webhooks. Design:
[`rust_exchange_simulator_plan.md`](rust_exchange_simulator_plan.md) and
[`milestone_1_domain_schema_events.md`](milestone_1_domain_schema_events.md), whose "Implementation progress" section
tracks what is built.

## Running

```sh
# from the workspace root
cargo run -p exchange-simulator                       # http://127.0.0.1:8080, database in ./data/exchange.db
docker compose -f exchange-simulator/compose.yaml up --build   # http://localhost:8080, database in a volume
```

Configuration (`config/default.yaml` is compiled in) can be overridden with a YAML file named by `EXCHANGE_CONFIG`, or
with environment variables such as `EXCHANGE__SERVER__BIND=0.0.0.0:9000`. Settings:

| Key | Default | Meaning |
|---|---|---|
| `server.bind` | `127.0.0.1:8080` | Listen address (`0.0.0.0:8080` in the image) |
| `exchange.identifier_policy` | `strict` | `strict` checks MIC/ISIN/CUSIP check digits; `permissive` checks lengths only |
| `exchange.command_queue_capacity` | `10000` | Commands waiting for the exchange thread; when full, requests get `503 overloaded` |
| `events.simulation_id` | unset | Stamped on every event as `simulationId` |
| `webhooks.poll_interval_ms` | `250` | How often due deliveries (retries) are looked for; new events are sent at once |
| `webhooks.batch_size` / `concurrency` | `100` / `16` | Deliveries claimed per round / concurrent requests |
| `webhooks.request_timeout_ms` | `5000` | Per request; a timeout is a failed attempt |
| `webhooks.max_attempts` | `10` | Attempts in total; then the delivery is dead-lettered (`DEAD`) |
| `webhooks.initial_backoff_ms` / `max_backoff_ms` | `500` / `300000` | Exponential backoff between attempts, with jitter |
| `database.path` | `data/exchange.db` | SQLite file (WAL mode) |
| `database.busy_timeout_ms` | `5000` | SQLite busy timeout |
| `logging.filter` / `logging.format` | `info` / `pretty` | Used when `RUST_LOG` is unset; `json` for structured logs |

## Model

- **Prices** are integers in *ticks*. An instrument has `currencyScale` decimals per major unit and `tickSizeMinor`
  minor units per tick, so with DKK, scale 2 and tick 1, `100.25` is `10025` ticks. Requests take `priceTicks`.
  Responses add a decimal `price` string.
- **Matching** follows price-time priority. The execution price is the resting order's price. A `LIMIT` remainder
  rests on the book; a `MARKET` remainder is cancelled (status `CANCELLED`, `remainingQuantity` keeps the unfilled
  part).
- **Self-trade:** an incoming order that would trade against its own participant's resting order is rejected whole.
  It is recorded with status `REJECTED` and `rejectReason: "SELF_TRADE"`. Orders for a disabled instrument are recorded
  the same way, with `rejectReason: "INSTRUMENT_DISABLED"`.
- **Durability:** a successful response means the order, any resting orders it traded with, its executions, its events
  and their pending webhook deliveries are committed in one SQLite transaction. Books are rebuilt from SQLite on
  startup.
- **Sequence:** every accepted or rejected order gets the next exchange-wide `sequence`, which is its time priority.
  Malformed requests get no sequence.

## API

All bodies are JSON with camelCase fields. Errors look like `{"error": "<code>", "message": "..."}`:

| Status | `error` | When |
|---|---|---|
| 400 | `invalid_json` | Malformed JSON, wrong types, unknown fields, unknown enum values |
| 400 | `invalid_request` / `invalid_order` | Field validation (identifiers, blank names, quantity/lot, price presence) |
| 404 | `not_found` | Unknown order or instrument in the path |
| 409 | `already_exists`, `client_order_id_conflict`, `order_not_active` | Conflicts with current state |
| 422 | `unknown_instrument`, `unknown_participant` | A request body references something not registered |
| 503 | `overloaded`, `unavailable` | Command queue full, or the exchange thread stopped |
| 500 | `internal_error` | Storage failure; nothing was applied |

### `POST /api/instruments`

```json
{ "id": "ACME", "symbol": "ACME", "mic": "XCSE", "isin": "DK0010274414", "cusip": null,
  "currency": "DKK", "currencyScale": 2, "tickSizeMinor": 1, "lotSize": 1, "enabled": true }
```

`mic`, `isin`, `cusip` are optional; `tickSizeMinor` and `lotSize` default to 1, and `enabled` to `true`. `id` is the immutable key that
orders reference. `(mic, symbol)` must be unique when `mic` is given; ISIN and CUSIP may be shared by several
listings. Returns `201` with the instrument, plus `tickSize` as a decimal string.

### `GET /api/instruments?symbol=&mic=&isin=&cusip=`

All filters are optional and combined with AND. External identifiers can match several listings.

### `PATCH /api/instruments/{id}`

`{ "enabled": false }` halts trading and `{ "enabled": true }` resumes it. It returns `200` with the instrument, or
`404` for an unknown ID; `enabled` is the only field that can change. While an instrument is disabled:
- **Orders are recorded rejections:** every new order is sequenced and stored with status `REJECTED` and
  `rejectReason: "INSTRUMENT_DISABLED"`, and an `OrderRejected` event is emitted (`201`, like a self-trade).
- **Cancels keep working.**
- **Resting orders stay** on the book, and trade again once the instrument is re-enabled.

The flag survives restarts. Switching it emits no event (there is no instrument-status event type yet).

### `POST /api/participants`

`{ "id": "participant-a", "name": "Participant A" }` → `201`.

### `POST /api/orders`

```json
{ "clientOrderId": "c-1", "instrumentId": "ACME", "participantId": "participant-a",
  "side": "BUY", "type": "LIMIT", "priceTicks": 10025, "quantity": 150 }
```

`side` is `BUY`/`SELL`; `type` is `LIMIT` (needs `priceTicks`) or `MARKET` (no `priceTicks`). `clientOrderId` is
optional, 1–64 characters, and unique per participant.

- `201`: the order was processed (including `REJECTED`). Body: `{ "order": Order, "executions": [Trade], "duplicate": false }`.
- `200`: `clientOrderId` was already used with the same terms. Body: the existing order in its current state, with
  `"duplicate": true` and no executions.
- `409 client_order_id_conflict`: `clientOrderId` was already used with different terms.

`Order`: `orderId`, `clientOrderId`, `instrumentId`, `participantId`, `side`, `type`, `priceTicks`, `price`,
`quantity`, `remainingQuantity`, `filledQuantity`, `status` (`OPEN`, `PARTIALLY_FILLED`, `FILLED`, `CANCELLED`,
`REJECTED`), `sequence`, and `rejectReason` when rejected.

### `GET /api/orders/{id}`

The stored order, plus `createdAt` and `updatedAt`.

### `DELETE /api/orders/{id}`

Cancels the remaining quantity of an `OPEN` or `PARTIALLY_FILLED` order. Returns `200` with the same body as
`POST /api/orders`; `409 order_not_active` if the order is already done.

### `GET /api/orderbooks/{instrumentId}`

```json
{ "instrumentId": "ACME", "currency": "DKK", "enabled": true, "lastSequence": 2,
  "bids": [], "asks": [{ "priceTicks": 10025, "price": "100.25", "quantity": 50, "orders": 1 }] }
```

Levels are listed best first. `enabled` shows whether the instrument is trading. `lastSequence` is the sequence of the
last order processed before the snapshot.

### `GET /api/trades?instrumentId=&participantId=&orderId=&limit=`

Executions in the order they were created. `participantId` matches either buyer or seller, and `orderId` either
order. `limit` is 1–1000 (default 100). `Trade`: `executionId`, `instrumentId`, `buyOrderId`, `sellOrderId`,
`buyerId`, `sellerId`, `priceTicks`, `price`, `quantity`, `currency`, `executedAt`.

### `GET /health`

`{"status": "ok", "schemaVersion": <n>}` (the number of the last applied migration in `migrations/`), or `503` if
the database does not answer.

### `GET /api/events?after=&limit=&eventType=`

Canonical events with `sequence > after` (default 0), in sequence order. `limit` is 1–1000 (default 100). To replay
everything, pass the last `sequence` you received as `after` until the result is empty. Envelope:

```json
{ "eventId": "0199…", "sequence": 3, "eventType": "TradeExecuted", "schemaVersion": 1, "source": "exchange",
  "simulationId": null, "aggregateId": "<executionId>", "correlationId": "<incoming orderId>",
  "occurredAt": "2026-10-14T12:00:00.000000Z", "payload": { … } }
```

Event types and payloads (`schemaVersion` 1):

| Event | Aggregate | Payload |
|---|---|---|
| `OrderAccepted` | order | order as accepted (status `OPEN`, nothing filled) |
| `OrderRejected` | order | order with `rejectReason` |
| `TradeExecuted` | execution | `executionId`, `instrumentId`, `instrument` {`id`, `symbol`, `mic`, `isin`, `cusip`, `currency`}, `buyOrderId`, `sellOrderId`, `buyerId`, `sellerId`, `priceTicks`, `price`, `quantity`, `currency`, `currencyScale`, `tickSizeMinor`, `executedAt` |
| `OrderPartiallyFilled` / `OrderFilled` | order | order after the fill; for a resting order also `executionId` |
| `OrderCancelled` | order | order with `cancelReason`: `REQUESTED` or `MARKET_REMAINDER` |

Order payloads carry `orderId`, `clientOrderId`, `instrumentId`, `participantId`, `side`, `type`, `priceTicks`, `price`,
`quantity`, `remainingQuantity`, `filledQuantity`, `status` and `sequence`. Within one command, events come in this
order: `OrderAccepted`, then for each fill `TradeExecuted` followed by the resting order's fill event, then the
incoming order's fill event, then `OrderCancelled` for a market remainder. A command's events all share
`correlationId`, the incoming (or cancelled) order's ID.

### `POST /api/webhooks`

`{ "url": "http://consumer:8080/trades", "eventTypes": ["TradeExecuted"], "secret": "…", "ordered": false }` → `201`
`{ "id", "url", "eventTypes", "enabled", "signed", "ordered", "createdAt" }`. The URL must be `http` or `https`, and the event
types must be ones listed above. `secret` is optional (16–256 characters). When it is given, deliveries are signed. It
is never returned; responses show `signed: true` instead. A subscription receives events committed after it was registered; replay earlier events with
`/api/events`.

Delivery is a `POST` of the event envelope with headers `X-Event-Id`, `X-Event-Type`, `X-Event-Sequence`,
`X-Subscription-Id` and `X-Delivery-Attempt`.
- **Retries:** any 2xx response counts as delivered. Anything else, including a timeout or a connection error, is
  retried with exponential backoff and jitter, up to `webhooks.max_attempts`; then the delivery is marked `DEAD`.
- **At least once:** deliveries are durable and survive restarts, and a consumer may receive the same event more than
  once. Deduplicate by `eventId`.
- **Ordering:** by default deliveries are sent concurrently and may arrive out of order; use `sequence` for canonical
  order. With `"ordered": true`:
  - The subscription has at most one delivery in flight, always its unfinished event with the lowest `sequence`, so
    events arrive in `sequence` order.
  - A failing delivery blocks the ones behind it while it is retried.
  - If it is dead-lettered, delivery continues with the next event. The consumer sees a gap in `sequence` and can fill
    it from `/api/events`.
  - Throughput per ordered subscription is one round trip per event.
- **Signing:** for a subscription with a secret, each attempt also carries `X-Signature-Timestamp` (unix seconds, the
  time of the attempt) and `X-Signature: sha256=<hex>`, the HMAC-SHA256 under the secret of
  `"{X-Signature-Timestamp}.{raw body}"`. A retry is signed again with a new timestamp; its body is byte-identical.
  Subscriptions without a secret are unsigned, so use them on test networks only.

To verify a signature, recompute the HMAC over the raw body *as received*, not over a re-serialization, compare in
constant time, and reject timestamps far from your clock (e.g. more than 5 minutes) to stop replays:

```python
import hashlib, hmac, time

def verify(secret: bytes, headers, raw_body: bytes, tolerance=300) -> bool:
    timestamp = headers["X-Signature-Timestamp"]
    expected = "sha256=" + hmac.new(secret, timestamp.encode() + b"." + raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, headers["X-Signature"]) and abs(time.time() - int(timestamp)) <= tolerance
```

### `GET /api/webhooks`

Lists the subscriptions.

### `GET /metrics`

Prometheus text format, with these series:
- **Commands:** `exchange_commands_total{command,outcome}` and `exchange_command_duration_seconds{command}`.
- **Hot path, split into stages:** `exchange_match_duration_seconds` and `exchange_commit_duration_seconds`.
- **Queue:** `exchange_command_queue_depth` and `exchange_command_queue_rejections_total`.
- **Results:** `exchange_orders_total{status}`, `exchange_executions_total`, `exchange_traded_quantity_total` and
  `exchange_events_total{event_type}`.
- **Webhooks:** `webhook_delivery_attempts_total{outcome}`, `webhook_delivery_duration_seconds`, and the outbox
  backlog `webhook_outbox_deliveries{status}`, read from SQLite at scrape time.

## Testing

```sh
cargo test -p exchange-simulator                          # unit, integration and HTTP tests; tests/mockserver.rs needs Docker
cargo test -p exchange-simulator --features e2e --test e2e   # the acceptance test in containers (builds the image)
```

`tests/e2e.rs` runs the Milestone 1 acceptance scenario against the real image, with MockServer as the settlement
consumer on one Docker network. It covers registration, the 200/150 trade, the webhook with a stable event ID, a
`docker restart` that restores the book without repeating events, and a consumer outage whose retries create no
trades.

## Benchmarks

[`bench/README.md`](bench/README.md) covers how to run the harness and the baseline results.
