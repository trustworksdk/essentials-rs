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

**Open, from the plan but not in this document** — resolved 2026-10-14 (see step 3)
- Idempotent order submission: added `orders.client_order_id`, unique per participant.
- `events.source` and `events.simulation_id`: added.
- Plan §3 instrument `enabled` flag and participant settlement metadata: still not built.

### Step 3 — transactional repository and startup reconstruction ✅ done (2026-10-14)

**Delivered**
- Migration 001 amended in place (nothing deployed yet): `orders.client_order_id` (1–64 chars) with a partial unique index on `(participant_id, client_order_id)`; `events.source TEXT NOT NULL` and `events.simulation_id TEXT`. The event columns are written from step 5.
- `persistence::Store` wraps the single writer connection. It inserts and lists instruments (filters on `symbol`/`mic`/`isin`/`cusip`) and participants, and reads active orders in sequence order, the max sequence, and orders by ID or by `(participant, client order ID)`. `commit(&BookUpdate)` writes the subject order (upsert), the makers it traded with, and its executions in one transaction. Timestamps are RFC 3339 UTC with microseconds.
- `engine::Exchange`, the persistence coordinator, owns the books, the store, the participant set and the global sequence. Each command is planned on the book, committed, and only then applied. A failed commit drops the plan and does not consume a sequence. `recover` rebuilds every book from active limit orders in sequence order and resumes sequencing after the highest stored sequence, rejected orders included. Registration is validated with the configured `exchange.identifier_policy`.
- `main` recovers the exchange at startup and logs the instrument count, resting orders and last sequence; `/health` reads through it.
- Tests (`tests/recovery.rs`, 7): the acceptance trade, committed and restored after restart; a forced commit failure (SQLite trigger) leaves memory and storage unchanged and the retry succeeds; client order ID duplicates and conflicts, also across restart; rejected, market and cancelled orders are stored but never restored; unknown instrument or participant; the registration rules (same symbol on two MICs, shared ISIN, optional CUSIP, bad check digits in strict but not permissive mode, filters, ambiguous symbol-only lookup). A seeded run of 600 commands × 3 seeds that restarts the exchange every 97 commands produces exactly the same outcomes, trades and final book as an uninterrupted run.
- Mutation checks, each made a test fail: applying the update even when the commit fails; restoring in reverse sequence order; not updating a maker's remaining quantity; reusing a rejected order's sequence; skipping the client order ID lookup; skipping book restore; skipping the participant check.

**Decisions**
- A repeated client order ID with the same terms (instrument, side, type, price, quantity) returns `SubmitOutcome::Duplicate` with the order's *current* state. With different terms it fails with `ClientOrderIdConflict`. The unique index is the backstop.
- Instrument and participant existence and duplicate listings are checked against in-memory state the coordinator owns (loaded at startup), not by querying SQLite per command. The client order ID check does query SQLite through an index.
- No default simulator MIC. `mic` may be absent, and symbol-only lookups can return several listings; callers disambiguate with the instrument ID.
- The clock reading is truncated to microseconds, so in-memory and stored timestamps agree.
- Concurrency is not wired yet: `AppState` holds the `Exchange` behind a mutex. The bounded command queue and instrument sharding come with the REST endpoints (step 4).

### Step 4 — REST endpoints and HTTP integration tests ✅ done (2026-10-14)

**Delivered**
- Endpoints from the API table: `POST/GET /api/instruments` (filters `symbol`, `mic`, `isin`, `cusip`), `POST /api/participants`, `POST /api/orders`, `GET/DELETE /api/orders/{id}`, `GET /api/orderbooks/{instrumentId}`, `GET /api/trades` (filters `instrumentId`, `participantId`, `orderId`, `limit`), and `/health`. They are documented in `README.md`, with the error-code table. `/api/events` and `/api/webhooks` are step 5; `/metrics` is step 6.
- `engine::Dispatcher`: one named OS thread owns the `Exchange` (and so the single SQLite writer) and executes commands from a bounded `tokio::mpsc` queue (`exchange.command_queue_capacity`, default 10,000). A full queue fails fast with `503 overloaded`. Callers get their answer after the commit. On shutdown the thread drains the commands it already accepted (`App::shutdown`).
- Reads (orders, trades, instruments, health) use a separate read-only connection (`persistence::open_reader`), so they do not queue behind writes. In WAL mode a reader sees each commit as soon as the response is sent. Book snapshots come from the exchange thread, because the books live there.
- `app::App` assembles the service from `Config` (database, recovery, exchange thread, router) for both `main` and the tests.
- Tests (`tests/api.rs`, 8, through the real router and exchange thread on a SQLite file):
  - the acceptance scenario over HTTP, including a restart
  - validation and error codes
  - self-trade rejection, market remainder, and client order ID duplicates and conflicts
  - cancel and lookup errors
  - registration and filters
  - trade filters and ordering
  - 200 concurrent submissions: sequences 1–200 with no gaps, response and stored trade quantities agree, and every unit submitted is traded, resting or rejected
  - plus a unit test for queue overload and a stopped exchange thread
- Container smoke: the acceptance steps 1–6 with `curl`, then `docker compose restart`; the book and trade are unchanged.
- Mutation checks, each made a test fail: a duplicate answered `201`; an unknown participant answered `404`; the `duplicate` flag dropped; the participant trade filter matching buyers only; trades returned out of order.

**Decisions**
- Status codes:
  - `201` for every processed order, including exchange rejections (recorded with `rejectReason`)
  - `200` with `duplicate: true` for a repeated client order ID
  - `422` when the body references an unknown instrument or participant; `404` for unknown path resources
  - `409` for conflicts
  - `400` for malformed input, including unknown JSON fields (`deny_unknown_fields`)
- Requests carry `priceTicks` only; responses add the decimal `price`. No decimal-price input yet.
- `lastSequence` in a book snapshot is the exchange-wide sequence at snapshot time. A book's own high-water mark would read lower after a restart, because only resting orders are restored. The HTTP restart test caught that.
- Instrument sharding is not done: one exchange thread serves all instruments. The plan schedules sharded workers in Milestone 4.
- `GET /api/trades` returns the oldest trades first with a `limit`. It has no cursor yet; ordered replay is `/api/events` (step 5).

### Step 5 — events, transactional outbox, webhooks ✅ done (2026-10-14)

**Delivered**
- `events`: the envelope (`eventId`, `sequence`, `eventType`, `schemaVersion` 1, `source` `"exchange"`, `simulationId` from `events.simulation_id`, `aggregateId`, `correlationId`, `occurredAt`, `payload`) and the six initial event types. `events::for_update` derives a command's events in a fixed order: `OrderAccepted`, then per fill `TradeExecuted` plus the resting order's `OrderPartiallyFilled`/`OrderFilled`, then the incoming order's fill event, then `OrderCancelled` (`MARKET_REMAINDER`). A cancel gives `OrderCancelled` (`REQUESTED`), a self-trade gives `OrderRejected`. `TradeExecuted` carries the instrument identifier snapshot, settlement fields (currency, scale, tick size) and a decimal `price`.
- Outbox: `Store::commit` writes events and one `PENDING` `webhook_deliveries` row per matching subscription in the same transaction as the orders and executions.
- Webhooks:
  - `POST/GET /api/webhooks`: the URL must be http(s), and event types must be known and non-empty.
  - `DeliveryWorker` (Tokio task) claims due deliveries through the exchange thread, so there is still one SQLite writer. It POSTs the envelope with `X-Event-Id`, `X-Event-Type`, `X-Event-Sequence`, `X-Subscription-Id` and `X-Delivery-Attempt` headers, with bounded concurrency, and records the outcomes in one batch.
  - 2xx counts as delivered. Anything else retries with exponential backoff and equal jitter until `max_attempts`, then `DEAD`.
  - A claim is a lease, sized from timeout × batch / concurrency + 30 s; startup releases all claims. A result for a claim that was meanwhile re-claimed and finished is ignored.
  - Commits that create deliveries wake the worker at once; retries are picked up by the poll (`webhooks.poll_interval_ms`).
- `GET /api/events?after=&limit=&eventType=`: cursor replay by sequence, via the read connection.
- `App::shutdown` stops the worker (after its batch), then drains the exchange thread.
- Tests:
  - `tests/events.rs` (6): event order and payloads per scenario.
  - `tests/recovery.rs` (+2): a failure inserting a delivery row rolls back the trade, events and orders; claims are leases and late results do not overwrite.
  - `tests/webhooks.rs` (6, in-process HTTP receiver): delivery with a stable `eventId` that equals the replayed event; per-subscription filtering; two failures then success, with no new trades; dead-lettering; resume after restart from a claim left `IN_FLIGHT`; replay paging, stable across restart; registration validation.
  - `tests/mockserver.rs` (1, Testcontainers `mockserver/mockserver:5.15.0`): acceptance steps 3 and 8–10 with a consumer that answers 500, 500, then 202. Three identical deliveries, one execution, the book restored after restart, no re-delivery.
- Mutation checks, each made a test fail: attempts not counted; claims not released at startup; subscription filter ignored; any status treated as delivered; lease ignored; a late result overwrites; the `TradeExecuted` type wrong; one attempt too many.

**Decisions**
- Delivery bookkeeping goes through the exchange thread rather than a second SQLite writer. The cost is extra commands on the matching queue (two per batch); the gain is no writer contention. When the queue is full, results are retried briefly, then left to lease expiry (at-least-once).
- A subscription receives only events committed after it registered; `/api/events` is the way to backfill.
- Deliveries for one subscription run concurrently, so they can arrive out of order. Serialized per-subscription delivery (plan §5, "where ordering matters") is not built.
- HMAC signing (plan §5, optional) is not built; subscribers belong on test networks only, as the "open implementation choices" above say.
- `config/default.yaml` uses `events: {}`. A section holding only comments parses as null and failed startup; the config unit test caught it.

**Remaining for Milestone 1** (step 6 and the definition of done)
- Prometheus `/metrics` and a benchmark harness: durable throughput measured separately from in-memory throughput.
- A Docker-based end-to-end run of the full acceptance test: the simulator *container* plus MockServer on one compose network, covering the container restart. Today MockServer runs in a container and the simulator in-process.

### Step 6 — metrics and benchmark harness ✅ done (2026-10-14)

**Delivered**
- `GET /metrics` (Prometheus text format via `metrics` + `metrics-exporter-prometheus`; one process-wide recorder, `metrics::install`):
  - command counts and latency per command type
  - matching and SQLite commit latency, measured separately
  - queue depth and queue rejections
  - orders by status, executions, traded quantity, events by type
  - webhook attempts by outcome and attempt latency
  - outbox rows by status, read from SQLite at scrape time
- `src/bin/bench.rs` (`cargo run --release -p exchange-simulator --bin bench -- all --dir <disk>`): one seeded workload measured at three levels:
  - in-memory `OrderBook`
  - durable `Exchange` on SQLite, with and without an outbox subscriber
  - the HTTP service with 32 clients, with and without a live webhook consumer

  Each report records the SQLite version and pragmas, and the measured fsync latency of the database directory. `default-run` keeps `cargo run` starting the service.
- `bench/README.md` and `bench/results/`: baseline on the devcontainer, which is a Docker Desktop VM (aarch64, 8 CPUs):

  | Scenario | Throughput |
  |---|---|
  | in-memory matching | 2.9 M orders/s |
  | durable, VM disk | 5.3–5.6 k orders/s (p50 140 µs, p99 1.2 ms) |
  | durable with outbox | 4.3–4.4 k orders/s |
  | HTTP end to end | 4.85 k orders/s (p50 6.4 ms) |
  | HTTP with webhook consumer | 3.7 k orders/s; every trade event delivered within the run |

  On tmpfs, with no real fsync: 13–26 k/s.
- Tests: `metrics_expose_commands_trades_latencies_and_the_outbox` (in `tests/webhooks.rs`). Mutation checks: dropping the commit histogram, or zeroing the outbox gauge, made it fail.

**Findings**
- `/tmp` in the devcontainer is tmpfs (fsync ≈ 1 µs). The first "durable" figures (26 k/s) were SQLite in RAM, which the fsync probe exposed. Durable figures must state where the database lived; the harness now reports it.
- One SQLite writer that fsyncs every commit is the limit, roughly 500× below in-memory matching. The 1k–10k orders/s target is met at about 4–5k on this VM's disk, not at 10k. Bare-metal numbers will differ; measure before quoting.
- Commit maxima of 57–180 ms recur on the VM disk. A likely cause is the automatic WAL checkpoint in the committing transaction; not investigated.
- Milestone 4 levers (not built): group commit, checkpointing off the hot path, sharded matching, and the plan's non-durable fast mode.

### Milestone 1 status

Steps 1–6 are done. Definition of done:
- Docker startup is reproducible.
- The API is documented in `README.md`.
- Correctness, restart and webhook tests pass, including MockServer.
- The performance baseline is measured.

The containerized acceptance test is now in place too (below), so Milestone 1 is complete.

### Containerized acceptance test ✅ done (2026-10-14)

`tests/e2e.rs`, behind the `e2e` feature (`cargo test -p exchange-simulator --features e2e --test e2e`, about 5 s with a warm image cache), follows durable-queues' `interop` pattern. It builds the service image from `Dockerfile` and starts it with MockServer 5.15.0 on a private Docker network; the service reaches MockServer by container name. It runs acceptance steps 1–10 of the plan:
1. Fresh container and volume.
2. ACME with tick size `0.01`, and two participants.
3. A `TradeExecuted` subscription pointing at MockServer.
4. and 5. Sell 200 at 100.25, then buy 150.
6. One execution of 150; the sell has 50 left.
7. The execution and its event are both stored.
8. MockServer receives the canonical envelope with the replayed `eventId`.
9. `docker restart` (SIGTERM, then start): the same book, the same replay, no re-delivery.
10. With the consumer answering 500, a second trade is retried with the same `eventId` (4+ attempts). After recovery it is delivered, with two trades in total and two distinct event IDs.

The run was repeated twice with no leftover containers or networks. Mutation check: with `MAX_ATTEMPTS=1` the delivery is dead-lettered and the test fails.
