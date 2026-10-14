# exchange-simulator

A lightweight stock exchange for integration and performance testing. It does real price-time matching over integer
ticks and keeps SQLite as the authoritative store. Durable trade webhooks are planned. Design:
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
  It is recorded with status `REJECTED` and `rejectReason: "SELF_TRADE"`.
- **Durability:** a successful response means the order, any resting orders it traded with, and its executions are
  committed in one SQLite transaction. Books are rebuilt from SQLite on startup.
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
  "currency": "DKK", "currencyScale": 2, "tickSizeMinor": 1, "lotSize": 1 }
```

`mic`, `isin`, `cusip` are optional; `tickSizeMinor` and `lotSize` default to 1. `id` is the immutable key that
orders reference. `(mic, symbol)` must be unique when `mic` is given; ISIN and CUSIP may be shared by several
listings. Returns `201` with the instrument, plus `tickSize` as a decimal string.

### `GET /api/instruments?symbol=&mic=&isin=&cusip=`

All filters are optional and combined with AND. External identifiers can match several listings.

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
{ "instrumentId": "ACME", "currency": "DKK", "lastSequence": 2,
  "bids": [], "asks": [{ "priceTicks": 10025, "price": "100.25", "quantity": 50, "orders": 1 }] }
```

Levels are listed best first. `lastSequence` is the sequence of the last order processed before the snapshot.

### `GET /api/trades?instrumentId=&participantId=&orderId=&limit=`

Executions in the order they were created. `participantId` matches either buyer or seller, and `orderId` either
order. `limit` is 1–1000 (default 100). `Trade`: `executionId`, `instrumentId`, `buyOrderId`, `sellOrderId`,
`buyerId`, `sellerId`, `priceTicks`, `price`, `quantity`, `currency`, `executedAt`.

### `GET /health`

`{"status": "ok", "schemaVersion": 1}`, or `503` if the database does not answer.

Not yet available: `GET /api/events` and `/api/webhooks` (step 5), `GET /metrics` (step 6).
