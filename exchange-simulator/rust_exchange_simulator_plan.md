# Rust Exchange Simulator — Implementation Plan

**Status:** Architecture agreed; ready for milestone 1  
**Purpose:** Lightweight stock exchange/order-book simulator for integration and performance testing, including external settlement integration and optional simulated settlement lifecycle.

## 1. Confirmed decisions

| Area | Decision |
|---|---|
| Language | Rust, single application/binary initially |
| Deployment | Docker / Docker Compose |
| Persistence | SQLite, authoritative source of truth in durable mode |
| Exchange | Real matching, simplified exchange rules |
| Participants | Explicit participant IDs on orders and both sides of executions |
| Settlement | Both external trade-driven settlement and optional internal lifecycle simulation |
| Event delivery | HTTP webhooks, durable outbox, retry and replay |
| Performance target | 1,000–10,000 orders/sec (target, not yet benchmarked) |
| Testing | Deterministic scenarios, seeded randomness, partial/missing fields, fault injection |

## 2. Architecture

A single Rust process with separately testable modules:

```text
REST API / Test Controller       Scenario / Random Generator
             \                     /
              Command Dispatcher
                       |
              Matching Workers
            (instrument-sharded)
                       |
          Persistence Coordinator
           SQLite + Event Outbox
                       |
                Webhook Workers ------> External settlement service
                       |
           Optional Settlement Simulator
                       |
            Settlement lifecycle events
```

The settlement simulator consumes canonical trade executions and produces its own lifecycle events. In combined mode, distinguish simulator-originated lifecycle events from externally reported outcomes using `source`, `simulation_id`, and correlation IDs; avoid treating both as a single authoritative settlement state.

### Technology choices

- **axum** for HTTP; **tokio** for async runtime and bounded channels.
- **serde** for JSON and configuration; **tracing** for structured logs.
- **rusqlite** for SQLite; migrations stored as SQL files.
- **rand / rand_chacha** for seeded, reproducible workload generation.
- **metrics** with Prometheus exposition for observability.
- **uuid** for externally visible identifiers where useful; monotonically increasing database event sequence for replay.

## 3. Domain model and matching rules

### Entities

- **Instrument:** Stable internal ID, symbol, optional ISIN/CUSIP, market MIC, currency, tick size, lot size, enabled state. Internal ID is the foreign-key identity; external identifiers are metadata, not primary keys.
- **Participant:** ID and optional settlement metadata (no cash/positions ledger in MVP).
- **Order:** ID, client order ID (optional), instrument, participant, side, type, limit price (optional), original quantity, remaining quantity, status, accepted sequence, timestamps.
- **Execution:** Immutable execution ID, instrument, buy/sell order IDs, buyer/seller participant IDs, execution price, quantity, trade time, event correlation IDs.
- **Event:** Unique ID, monotonic sequence, event type, occurred-at time, source, schema version, payload, correlation ID, simulation ID.
- **Webhook subscription/delivery:** Endpoint, event filters, delivery status, attempt count, next retry time, response details.


### Instrument identifiers (Milestone 1)

- `id`: required immutable internal instrument/listing ID; all orders and executions reference it.
- `symbol`: required trading symbol, scoped to a market; do not assume global uniqueness.
- `mic`: optional ISO 10383 market identifier initially (e.g. `XNAS`), recommended for multi-market testing. If provided, enforce unique `(mic, symbol)`; when absent, allow duplicates only through explicit test setup or assign a default simulator MIC.
- `isin`: optional 12-character ISO 6166 identifier; validate format and check digit when provided in strict mode. The same ISIN may legitimately appear on multiple listings, so do not enforce global uniqueness.
- `cusip`: optional 9-character identifier; validate format and check digit when provided in strict mode. Do not require it for non-US/Canadian instruments.
- `currency`: required trading currency. ISIN/CUSIP identify securities, while `symbol + MIC` identifies a listing; instrument IDs distinguish simulator books.
- Treat missing, invalid, or conflicting identifiers as **scenario-driven outbound payload mutations** or explicitly marked permissive fixtures, never silently corrupt the canonical registry.
- `GET /api/instruments` should support filters for `symbol`, `mic`, `isin`, and `cusip`; webhook execution payloads should carry an `instrument` identity snapshot (`id`, `symbol`, `mic`, `isin`, `cusip`, `currency`) to help settlement consumers resolve instruments consistently.

**Acceptance tests:** two markets with the same symbol; two listings sharing an ISIN; optional CUSIP; invalid ISIN/CUSIP rejected in strict mode; execution webhook contains instrument identifiers; settlement consumer can resolve by internal ID or external identifiers.

### Matching

- Price-time priority; bids descending, asks ascending; FIFO at each price level.
- Limit and market orders; partial fills; cancellation of remaining quantity.
- Integer ticks for price and integer quantity (never floating-point matching).
- One execution for each matched resting/aggressing quantity pair; one order may produce multiple executions.
- Commands for an instrument processed serially; independent instruments can run on sharded workers.
- Matching core performs no HTTP requests and no direct synchronous database queries.
- Validation: tick/lot conformity, positive quantity, instrument existence, valid side/type, participant IDs; define behavior for unfilled market-order remainder (cancel it).
- MVP: no stop orders, replace, auctions, market sessions, clearing/netting, positions, or cash accounting.

### Durability and correctness

SQLite is the authoritative source of truth in durable mode. Persist order state transitions, executions, and corresponding outbox events atomically before acknowledging durable acceptance. Restore active order books from SQLite on restart. Coordinate matching worker state and commit acknowledgement so database failures do not leave acknowledged but uncommitted trades. Use WAL, prepared statements, indexes, batched writes where safe, and one controlled SQLite writer. Backpressure via bounded queues; reject or slow producers explicitly rather than silently dropping commands.

## 4. Settlement integration

### Mode A — External settlement

Emit immutable `TradeExecuted` events through subscribed webhooks. Include execution ID, instrument, quantity, price, currency, buyer/seller IDs, trade timestamp, and optional settlement instructions. The external service owns settlement outcomes.

### Mode B — Internal settlement simulation

Optional engine transitions executions through configurable pending, settled, failed, rejected, delayed, corrected, or cancelled outcomes. Support T+N rules using a virtual clock, outcome distributions, and controllable processing delays. Publish lifecycle events through the same outbox/webhook infrastructure.

### Combined mode

Allow external consumers to process trades while the internal simulator emits independently attributed lifecycle events. Keep lifecycle authority and event source explicit; do not silently merge potentially conflicting statuses.

## 5. Webhook contract and delivery

Example canonical event envelope:

```json
{
  "eventId": "evt-12345",
  "sequence": 12345,
  "eventType": "TradeExecuted",
  "schemaVersion": 1,
  "source": "exchange",
  "occurredAt": "2026-10-14T12:00:00Z",
  "simulationId": "sim-001",
  "correlationId": "order-456",
  "payload": {
    "executionId": "trade-789",
    "instrumentId": "ACME",
    "currency": "DKK",
    "quantity": 150,
    "price": "100.25",
    "buyerId": "participant-1",
    "sellerId": "participant-2"
  }
}
```

- Transactional outbox: event and trade commit together.
- Webhook subscriptions with endpoint and event filters.
- At-least-once delivery; 2xx marks success; timeout/non-2xx triggers configurable retry with exponential backoff and jitter.
- Delivery attempts and dead-letter state persisted; replay by event sequence.
- Idempotency via stable `eventId`; consumers must handle duplicates.
- Optional HMAC signing; configurable concurrency and timeouts.
- Normal webhook delivery does not guarantee ordering across parallel requests. Offer serialized delivery where ordering matters.
- Fault injection acts on delivered copies, **not** on canonical persisted events: missing fields, explicit null, invalid values, duplicates, delays, and out-of-order delivery.

## 6. Simulation and testing

Three modes:

1. **Random market:** seeded order arrival, side, price, size, cancellation and liquidity distributions.
2. **Deterministic scenarios:** ordered commands with asserted outcomes and a fixed seed.
3. **Performance mode:** target order rate, instrument count, duration, persistence policy, and metrics.

A partial fill is produced by matching and available liquidity, not randomly fabricated after execution. Reproducibility requires deterministic event ordering and clock control, not only a random seed.

Example later-stage YAML:

```yaml
simulation:
  seed: 12345
  instruments: 100
  orders_per_second: 5000
  duration_seconds: 300
  order_distribution:
    buy_probability: 0.5
    market_order_probability: 0.2
    cancellation_probability: 0.1
  events:
    missing_optional_fields_probability: 0.15
    duplicate_delivery_probability: 0.02
    out_of_order_delivery_probability: 0.05
settlement:
  mode: both
  settlement_days: 2
  virtual_clock: true
```

## 7. API outline

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/instruments` | Register instrument |
| GET | `/api/instruments` | List instruments |
| POST | `/api/participants` | Register participant |
| POST | `/api/orders` | Submit order |
| GET | `/api/orders/{id}` | Query order |
| DELETE | `/api/orders/{id}` | Cancel remaining order quantity |
| GET | `/api/orderbooks/{instrumentId}` | Order book snapshot |
| GET | `/api/trades` | Query executions |
| GET | `/api/events?after={sequence}` | Replay canonical events |
| POST | `/api/webhooks` | Register webhook subscription |
| GET | `/api/webhooks` | List subscriptions |
| GET | `/health` | Liveness/readiness |
| GET | `/metrics` | Operational metrics |

Later: `/api/simulations`, `/api/scenarios`, test reset, virtual clock controls, and delivery fault policies. Test-control endpoints should be isolated from normal exchange APIs and disabled/protected outside test deployments.

## 8. SQLite schema outline

Initial tables:

- `instruments`
- `participants`
- `orders`
- `executions`
- `events` (append-only canonical log with integer sequence)
- `webhook_subscriptions`
- `webhook_deliveries` (subscription/event pair, status, attempts, next attempt)

Later: `simulation_runs`, settlement state/events, scenario execution records.

Indexes should cover instrument and order lookups, active orders, event sequence, pending deliveries, and execution trade lookup. Define unique constraints for idempotent order submission and webhook event delivery.

## 9. Suggested repository structure

```text
exchange-simulator/
├── Cargo.toml
├── Dockerfile
├── compose.yaml
├── config/default.yaml
├── migrations/001_initial.sql
├── scenarios/
├── src/
│   ├── main.rs
│   ├── config.rs
│   ├── api/
│   ├── domain/
│   ├── engine/          # book, matching, dispatch
│   ├── persistence/     # SQLite, transaction coordinator
│   ├── events/          # event contracts and outbox
│   ├── webhooks/        # subscriptions, delivery, retries
│   ├── settlement/      # optional simulator
│   ├── simulation/      # random and scenario generators
│   └── metrics.rs
└── tests/
```

Keep one Cargo package initially; extract crates only if modularity or reuse justifies it.

## 10. Implementation milestones

### Milestone 1 — Executable exchange (first implementation)

**Goal:** Single Dockerized Rust service with real matching, authoritative SQLite state, and durable trade webhooks.

- [ ] Create Cargo project, Axum server, configuration, tracing, Dockerfile, Compose.
- [ ] Define domain types, matching invariants, REST/event contracts, and SQLite migration.
- [ ] Implement instrument and participant registration, including symbol, optional ISIN/CUSIP, MIC, identifier validation, and lookup filters.
- [ ] Implement limit/market order matching, price-time priority, partial fills, cancellation.
- [ ] Persist orders, executions, and outbox events transactionally.
- [ ] Restore active books from SQLite after restart.
- [ ] Implement webhook subscriptions, durable delivery, retry, and attempt tracking.
- [ ] Expose basic order book, trade, event, health, and metrics endpoints.
- [ ] Add Rust unit/integration tests and Docker-based end-to-end tests (MockServer consumer).

**Acceptance test:**

1. Start container with fresh SQLite database.
2. Register `ACME` (tick size `0.01`) and two participants.
3. Register MockServer webhook endpoint for `TradeExecuted`.
4. Place sell limit order for 200 at 100.25.
5. Place buy limit order for 150 at 100.25.
6. Assert one execution for 150; resting sell has 50 remaining.
7. Assert execution and corresponding event persisted atomically.
8. Assert successful webhook delivery with stable event ID.
9. Restart container; assert active order book restored.
10. Simulate webhook failure and recovery; assert retries do not generate additional trades.

**Definition of done:** Reproducible Docker startup, documented API, passing correctness/restart/webhook tests, and baseline performance measurements. Do not claim the 1k–10k/sec target before measurement.

### Milestone 2 — Settlement simulation

- [ ] Internal settlement lifecycle engine and event contracts.
- [ ] External-only, simulated-only, and combined modes.
- [ ] Virtual clock, T+N settlement rules, failure/delay/correction scenarios.
- [ ] Correlation and explicit event authority/source.

### Milestone 3 — Scenario and fault-injection engine

- [ ] YAML scenario DSL, seeded randomness, deterministic sequencing.
- [ ] Configurable order distributions and market activity.
- [ ] Missing/null/invalid webhook fields, duplicate/out-of-order/delayed delivery.
- [ ] Scenario assertions and reset/replay controls.

### Milestone 4 — Performance testing

- [ ] Target-rate generator, bounded queues and backpressure measurements.
- [ ] Separate match, persistence, acknowledgement, and webhook latency metrics.
- [ ] Durable versus explicitly non-durable fast mode.
- [ ] Instrument-sharded workers, batch write tuning, load reports.
- [ ] Benchmark against 1,000–10,000 orders/sec on specified hardware.

## 11. Risks and trade-offs

1. **SQLite single writer:** May constrain durable throughput. Benchmark before optimizing or replacing it.
2. **In-memory/DB consistency:** Never acknowledge durable trades before commit; reconcile failed commits and restart recovery.
3. **Webhook throughput:** Consumer speed must not block matching; bounded queues and outbox backlog metrics required.
4. **Ordering and replay:** Per-instrument matching is ordered; parallel webhook delivery may not be. Document both guarantees.
5. **Determinism:** Fixed seeds alone are insufficient with concurrent scheduling; record clock and command sequence.
6. **Settlement semantics:** Avoid conflating synthetic settlement statuses with authoritative external settlement outcomes.

## 12. Immediate next step

Design and review the **Rust domain types**, **SQLite migration**, and **event contracts** together, then implement matching as a pure/testable core before wiring HTTP, persistence coordination, and webhooks.
