# Rust Exchange Simulator — Milestone 1 Domain, Schema and Event Contracts

**Status:** Proposed implementation baseline, 2026-10-14  
**Companion:** `rust_exchange_simulator_plan.md`

## Scope

A single Rust binary provides instrument and participant registration, limit/market order matching, partial fills, cancellations, SQLite durability, and trade webhooks. The settlement lifecycle is a separate module introduced after this foundation; execution events already carry the fields required to drive external settlement.

## Invariants and decisions

1. SQLite is authoritative. A successful order response means the corresponding order state, executions, canonical events and webhook outbox entries are committed together.
2. Matching uses price-time priority; command processing is serialized per instrument. An engine worker may own several instruments.
3. Order IDs and execution IDs are distinct UUIDs. A single order may produce multiple executions.
4. Quantities are positive integers. Prices are integer *instrument ticks*, with tick size expressed as a rational minor-unit configuration or a fixed decimal scale; never floating point. For the MVP, store `price_ticks` and `tick_size_minor` (positive integer minor currency units per tick), and `currency_scale` (e.g. 2 for DKK). Thus 100.25 DKK with tick size 0.01 DKK is 10025 ticks.
5. A limit order may rest. A market order never rests; any unmatched remainder is cancelled (IOC-like behavior). Execution price is the resting order's limit price.
6. No self-trades by default; reject incoming crossing orders that would match the same participant (configurable later).
7. Every accepted command gets a monotonic command sequence. Order priority is based on this sequence, not wall-clock timestamps.
8. All canonical events are valid and immutable. Fault injection applies only to outbound webhook representations in a later milestone.
9. Webhooks are at-least-once, may duplicate, and must have stable event IDs for consumer idempotency. HTTP 2xx means delivered; other outcomes retry.
10. SQLite's single writer is the serialization point for durability. Benchmark durable order acknowledgements separately from in-memory matching throughput; 1k–10k orders/s is a target, not a guarantee.

## Rust domain types

```rust
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InstrumentId(pub String);
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ParticipantId(pub String);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side { Buy, Sell }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderType { Limit, Market }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderStatus { Open, PartiallyFilled, Filled, Cancelled, Rejected }

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
}

pub struct Participant { pub id: ParticipantId, pub name: String }

pub struct Order {
    pub id: Uuid,
    pub instrument_id: InstrumentId,
    pub participant_id: ParticipantId,
    pub side: Side,
    pub order_type: OrderType,
    pub limit_price_ticks: Option<i64>,
    pub quantity: u64,
    pub remaining_quantity: u64,
    pub sequence: u64,
    pub status: OrderStatus,
}

pub struct Execution {
    pub id: Uuid,
    pub instrument_id: InstrumentId,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub buyer_id: ParticipantId,
    pub seller_id: ParticipantId,
    pub price_ticks: i64,
    pub quantity: u64,
    pub executed_at: String,
}
```

The engine owns `BTreeMap<Reverse<i64>, VecDeque<Uuid>>` bids, `BTreeMap<i64, VecDeque<Uuid>>` asks, and a `HashMap<Uuid, Order>` per instrument. Cancellation must remove the order from its price-level FIFO and remove empty levels. If performance warrants it, add an index for O(1) lookup of each order's level.

## SQLite migration 001

```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE instruments (
  id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  mic TEXT,
  isin TEXT,
  cusip TEXT,
  currency TEXT NOT NULL,
  currency_scale INTEGER NOT NULL CHECK (currency_scale BETWEEN 0 AND 9),
  tick_size_minor INTEGER NOT NULL CHECK (tick_size_minor > 0),
  lot_size INTEGER NOT NULL CHECK (lot_size > 0),
  CHECK (mic IS NULL OR length(mic) = 4),
  CHECK (isin IS NULL OR length(isin) = 12),
  CHECK (cusip IS NULL OR length(cusip) = 9)
);
CREATE UNIQUE INDEX idx_instruments_mic_symbol ON instruments(mic, symbol) WHERE mic IS NOT NULL;
CREATE INDEX idx_instruments_isin ON instruments(isin) WHERE isin IS NOT NULL;
CREATE INDEX idx_instruments_cusip ON instruments(cusip) WHERE cusip IS NOT NULL;
CREATE TABLE participants (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL
);
CREATE TABLE orders (
  id TEXT PRIMARY KEY,
  instrument_id TEXT NOT NULL REFERENCES instruments(id),
  participant_id TEXT NOT NULL REFERENCES participants(id),
  side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
  order_type TEXT NOT NULL CHECK (order_type IN ('LIMIT', 'MARKET')),
  limit_price_ticks INTEGER,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  remaining_quantity INTEGER NOT NULL CHECK (remaining_quantity >= 0 AND remaining_quantity <= quantity),
  sequence INTEGER NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK (status IN ('OPEN', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK ((order_type = 'MARKET' AND limit_price_ticks IS NULL)
      OR (order_type = 'LIMIT' AND limit_price_ticks > 0))
);
CREATE INDEX idx_orders_active ON orders(instrument_id, side, limit_price_ticks, sequence)
  WHERE status IN ('OPEN', 'PARTIALLY_FILLED');
CREATE TABLE executions (
  id TEXT PRIMARY KEY,
  instrument_id TEXT NOT NULL REFERENCES instruments(id),
  buy_order_id TEXT NOT NULL REFERENCES orders(id),
  sell_order_id TEXT NOT NULL REFERENCES orders(id),
  buyer_id TEXT NOT NULL REFERENCES participants(id),
  seller_id TEXT NOT NULL REFERENCES participants(id),
  price_ticks INTEGER NOT NULL CHECK (price_ticks > 0),
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  executed_at TEXT NOT NULL
);
CREATE INDEX idx_executions_instrument_time ON executions(instrument_id, executed_at);
CREATE TABLE events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  event_type TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  aggregate_id TEXT NOT NULL,
  correlation_id TEXT,
  payload TEXT NOT NULL CHECK (json_valid(payload)),
  occurred_at TEXT NOT NULL
);
CREATE TABLE webhook_subscriptions (
  id TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  event_types TEXT NOT NULL CHECK (json_valid(event_types)),
  created_at TEXT NOT NULL
);
CREATE TABLE webhook_deliveries (
  id TEXT PRIMARY KEY,
  subscription_id TEXT NOT NULL REFERENCES webhook_subscriptions(id),
  event_sequence INTEGER NOT NULL REFERENCES events(sequence),
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'IN_FLIGHT', 'DELIVERED', 'DEAD')),
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  last_error TEXT,
  delivered_at TEXT,
  UNIQUE (subscription_id, event_sequence)
);
CREATE INDEX idx_webhook_pending ON webhook_deliveries(status, next_attempt_at);
```

SQLite pragmas are connection settings, not permanent migration state: apply `foreign_keys=ON` for every connection, configure WAL at startup, and set a `busy_timeout`. Do not use one connection concurrently from multiple engine workers.

## Canonical event envelope

```json
{
  "eventId": "4db26a9b-8f53-4b1c-b1ad-32a812cf6c42",
  "sequence": 1024,
  "schemaVersion": 1,
  "eventType": "TradeExecuted",
  "aggregateId": "trade-789",
  "correlationId": "order-456",
  "occurredAt": "2026-10-14T12:00:00Z",
  "payload": {
    "executionId": "trade-789",
    "instrumentId": "ACME",
    "buyOrderId": "buy-123",
    "sellOrderId": "sell-456",
    "buyerId": "participant-a",
    "sellerId": "participant-b",
    "priceTicks": 10025,
    "quantity": 150,
    "currency": "DKK",
    "currencyScale": 2,
    "tickSizeMinor": 1
  }
}
```

Initial event types: `OrderAccepted`, `OrderRejected`, `OrderPartiallyFilled`, `OrderFilled`, `OrderCancelled`, `TradeExecuted`. For a match, write the execution and order-state changes and all events in **one transaction**. Settlement events (`SettlementPending`, `SettlementCompleted`, `SettlementFailed`, etc.) follow in a later milestone, with a settlement instruction linked to an execution ID and explicit origin (`simulator` vs external service).

## Consistency and failure handling

- Process an instrument's next command only after the prior command's persistence outcome is known. Keep a proposed in-memory state transition isolated until commit, or roll it back on persistence failure; never acknowledge a state that exists only in memory.
- Restore books from active limit orders ordered by command sequence on startup. Market orders never appear as active resting orders.
- Register webhook deliveries transactionally alongside each canonical event, for all matching subscriptions. On restart, recover expired `IN_FLIGHT` claims as pending.
- Consumers deduplicate using `eventId` (or subscription ID + event sequence); retries reuse the same event identity.
- Keep HTTP requests outside matching and SQLite transactions.
- Use bounded queues; when saturated, reject or backpressure incoming requests rather than silently dropping commands or events.

## API for milestone 1

| Method | Route | Purpose |
|---|---|---|
| POST | `/api/instruments` | Register instrument |
| GET | `/api/instruments` | List instruments |
| POST | `/api/participants` | Register participant |
| POST | `/api/orders` | Submit order |
| GET | `/api/orders/{id}` | Order status |
| DELETE | `/api/orders/{id}` | Cancel open remainder |
| GET | `/api/orderbooks/{instrumentId}` | Book snapshot |
| GET | `/api/trades` | Query executions |
| GET | `/api/events?after=123` | Cursor-based event replay |
| POST | `/api/webhooks` | Register subscriber |
| GET | `/api/webhooks` | List subscribers |
| GET | `/health` | Health |
| GET | `/metrics` | Metrics |

A future test-control API will support seeded scenarios, virtual time, fault injection, and resetting state. Keep it separated from exchange-facing endpoints.

## First implementation sequence

1. Bootstrap Cargo project, Dockerfile, compose.yaml, Axum health endpoint, tracing, SQLite migration.
2. Implement domain types and pure order-book matching tests (crossing limit orders, FIFO, partial fills, cancellations, market remainder, self-match policy).
3. Add transactional repository and startup reconstruction; verify restart consistency.
4. Add REST endpoints and HTTP integration tests.
5. Add event envelope, transactional outbox, webhook registration, dispatcher, retry/recovery, and MockServer integration test.
6. Add Prometheus-compatible metrics and benchmark harness; measure durable vs in-memory throughput separately.

## Acceptance scenario

1. Start Docker container with empty SQLite database.
2. Create participant A, participant B, and instrument ACME (DKK, scale 2, tick size 1 minor unit).
3. Register a `TradeExecuted` webhook subscriber.
4. A submits a sell limit order for 200 shares at 10025 ticks.
5. B submits a buy limit order for 150 shares at 10025 ticks.
6. Exactly one execution of 150 shares is created; sell order has 50 remaining, buy order is filled.
7. The trade, orders, event, and delivery are committed atomically.
8. Subscriber receives the trade event; failed delivery retries without a new execution.
9. Restart and confirm the remaining sell order is reconstructed, events can be replayed, and no completed execution is repeated.

## Open implementation choices

- Exact self-trade policy: default reject incoming crossing order; optionally later support cancel-maker/cancel-taker.
- REST acknowledgement: respond after durable commit (recommended).
- Webhook subscription security: local/test networks only in MVP; restrict destinations and support signing before deployment to shared infrastructure.
- Event ordering: global SQLite event sequence is a replay cursor, not a guarantee that concurrent webhook POSTs arrive in order.

## Instrument identifiers (Milestone 1 addition)

- The immutable `InstrumentId` is the canonical FK for orders, executions, and order books; never use symbol/ISIN/CUSIP as the primary key.
- `symbol` is required and scoped by `mic` (ISO 10383); `mic` is optional in the MVP, but strongly recommended for realistic multi-market fixtures.
- `isin` (12 characters, ISO 6166) and `cusip` (9 characters) are optional. Validate check digits and character sets at the API/domain boundary in strict mode; SQL length checks are only a first line of defense.
- Do not make ISIN or CUSIP globally unique: multiple listings can represent the same security. Unique `(mic, symbol)` applies when MIC is provided. Define a default simulator MIC or require explicit IDs for ambiguous symbol-only lookups.
- Add lookup filters `GET /api/instruments?symbol=...&mic=...`, `?isin=...`, and `?cusip=...`; external identifier queries can return multiple listings.
- `TradeExecuted` payload should include an `instrument` object containing `id`, `symbol`, `mic`, `isin`, `cusip`, and `currency` as a snapshot at execution time. Existing `instrumentId` remains for stable joins.
- Missing/invalid identifiers for resilience testing belong in scenario payload transformations or explicitly permissive test fixtures, not ordinary canonical records.

Example payload addition:

```json
"instrument": {
  "id": "inst-acme-xnas",
  "symbol": "ACME",
  "mic": "XNAS",
  "isin": "US0378331005",
  "cusip": null,
  "currency": "USD"
}
```

The example ISIN is illustrative only; production test fixtures should use coherent synthetic instrument identities.

Acceptance tests: same symbol on different MICs; shared ISIN across listings; optional CUSIP; invalid check digit rejected; ambiguous symbol-only lookup handled; webhook includes identifier snapshot.

## Implementation progress

Steps refer to "First implementation sequence" above.

### Steps 1–2 — bootstrap, domain types, pure matching core ✅ done (2026-10-14)

**Delivered**
- Crate `exchange-simulator` as a workspace member (root `Cargo.toml`), lib + binary.
- `config.rs`: defaults compiled in from `config/default.yaml`, optional file via `EXCHANGE_CONFIG`, overrides via `EXCHANGE__SECTION__KEY` env vars. Tracing with `RUST_LOG` or the configured filter, pretty or JSON output.
- `migrations/001_initial.sql` plus `persistence::open/migrate`: versioned by `PRAGMA user_version`, one transaction per migration; `foreign_keys`, WAL and `busy_timeout` applied per connection.
- Axum server with `/health` (probes the database, returns the schema version) and graceful SIGTERM shutdown.
- `Dockerfile` (built from the workspace root to share `Cargo.lock`; non-root user, `/var/lib/exchange` volume, `HEALTHCHECK`), `Dockerfile.dockerignore`, `compose.yaml`. Smoke-tested: up, health, restart without re-migrating.
- `domain`: the types from this document; `Side`/`OrderType`/`OrderStatus` with their DB spellings; MIC/ISIN/CUSIP validation (format and check digit) under `IdentifierPolicy::Strict`, lengths only under `Permissive`; decimal price ↔ ticks conversion with scale and tick-grid checks; lot-size check.
- `engine::OrderBook`: price-time priority over the `BTreeMap`/`VecDeque`/`HashMap` layout above. Every command is split into `plan_*` (no mutation, returns a `BookUpdate`) and `apply`, so step 3 can commit before applying; dropping a plan is the rollback. An update carries the book version it was planned against and is refused if stale. `restore` rebuilds a book from active limit orders in sequence order. IDs and clock are injected (`IdGenerator`, `MatchContext`).
- Tests: 39 in total. 23 matching tests cover crossing, FIFO, price priority, multi-level sweeps, partial fills, cancels, market remainder, self-trade, validation, sequence and stale-update guards, and restore. A seeded random flow (20 seeds × 2,000 commands) checks that the book never crosses, that fills reconcile with executions, and that priority order holds.
- Mutation checks, each made a test fail: LIFO instead of FIFO; self-trade check removed; rejected order's quantity not reset; stale-update guard removed; limit crossing off by one in either direction; empty level not removed.

**Decisions**
- Matching semantics: execution price is the maker's price. A market order with no or partial liquidity ends `CANCELLED`, not `REJECTED`, and keeps its unfilled `remaining_quantity`. A user cancel also keeps `remaining_quantity` (filled = quantity − remaining).
- Self-trade: the incoming order is rejected whole if it would *reach* a resting order of the same participant within its quantity and limit. Own orders it would not reach do not trigger a rejection.
- Input errors (`InvalidOrder`: bad quantity/lot, price presence, non-positive price) are refused before sequencing and never persisted. Exchange rejections (`RejectReason::SelfTrade`) are sequenced and recorded.

**Deviations from this document**
- Migration: the `orders` CHECK reads `limit_price_ticks IS NOT NULL AND limit_price_ticks > 0`. The original admitted a LIMIT order with a NULL price, because a NULL CHECK result passes; a schema test caught it.
- Migration: the `PRAGMA` lines moved out of the SQL file into `persistence::open`. Pragmas are per-connection, and `journal_mode` cannot change inside the migration transaction.
- `Execution::executed_at` is `chrono::DateTime<Utc>`, not `String`.
- `rusqlite` is pinned to 0.39 (not 0.40): the workspace's `sqlx` restricts `libsqlite3-sys` to `< 0.38`, and only one crate may link `sqlite3`.

**Open, from the plan but not in this document** (not built; decide before step 3)
- Plan §8 asks for a unique constraint for idempotent order submission (e.g. client order ID). `orders` has no such column.
- Plan §2/§5 envelopes carry `source` and `simulationId`; the `events` table here does not.
- Plan §3 lists an instrument `enabled` flag and participant settlement metadata; the domain types here do not.
