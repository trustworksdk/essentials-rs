# Durable Queues in Rust — Phased Implementation Plan

Implements the experiment described in [`rust_durable_queues_experiment.md`](./rust_durable_queues_experiment.md).
Each phase ends with a verifiable exit gate; do not start the next phase until the gate is green.

**Reference version:** Java Essentials **0.60.0** (`postgresql-queue`). The behaviours that matter:

- Every queue operation (enqueue, fetch, ack, retry, dead-letter) runs in **its own transaction**. A handler's
  failure never rolls back its own retry count. `TransactionalMode` was removed in 0.60.
- Delivery is at-least-once, so handlers must be idempotent.
- Claiming uses `FOR UPDATE SKIP LOCKED`.
- A message stuck in delivery is reset after `messageHandlingTimeout` (default 30 s).
- The 0.60 index set is used, and there is no queue-statistics table.

---

## Phase 0 — Capture the Java behavioural contract ✅ delivered, awaiting review

**Deliverables:**
- [`docs/CONTRACT.md`](docs/CONTRACT.md): schema, every SQL statement verbatim, column conventions, state machine,
  retry/dead-letter decision, redelivery formulas, stuck-message reset, interop hazards, Java test checklist
- [`docs/fixtures/java_rows.sql`](docs/fixtures/java_rows.sql): synthetic Java-shaped rows

All of it was extracted from `essentials-project` tag `0.60.0`.

Key answers (details in CONTRACT.md):
- **Clock:** all timestamps come from the app clock (bound `:now`), never DB `now()`.
- **Dead-letter cap:** a failed message is dead-lettered when `total_attempts >= max_redeliveries + 1`, i.e. at
  most N+1 deliveries. `total_attempts` is incremented by the claim.
- **Retry delay** uses `redelivery_attempts` before the increment.
- **`message_payload_type`** is the Java FQCN, `meta_data` is a flat string→string JSON object, and `id` is an
  opaque UUID string.
- **Default Java fetch:** the centralized fetcher runs the stuck reset plus, per queue, the ordered claim and then
  (only if empty) the unordered claim, limited to free worker slots, in one transaction.

**Deviation:** the golden fixtures are synthetic, because there is no JDK here. Phase 6 diffs them against rows
from the real Java harness.

**Exit gate:** CONTRACT.md reviewed. Open decisions are in CONTRACT.md §15 (ordered messages in scope; resurrect
keeps counters; UUID v7 ids).

---

## Phase 1 — Crate skeleton, schema, test harness ✅ done

**Delivered:**
- `TableName`: Java's validation and reserved list, generated from the Java source
- `QueueName`, `QueueEntryId` (UUID v7), `DeliveryMode`
- `PostgresQueueStorage` with `SchemaOwnership::{Component, External}`
- `QueueEntry` row decoding with Java's NULL semantics
- Testcontainers harness (`tests/common`) and 15 tests

**Decisions taken during Phase 1:**
- sqlx 0.9 only accepts runtime-built SQL wrapped in `AssertSqlSafe`, so `QueueSql` is built from a validated
  `TableName` and is the single place that wrapper is used.
- Schema-qualified table names are rejected: Java's `idx_{table}_…` index names make them invalid DDL anyway.
- Schema creation takes Java's bootstrap advisory lock (`0xE55E4711B007DD15`), so Java and Rust nodes starting
  together cannot race. Without the lock, 290 of 300 concurrent initialisations failed. The test reproduces that
  race and fails if the lock is removed.
- Java's schema-history ledger, legacy index drops and statistics cleanup are not reproduced; Rust never drops or
  records anything.
- The `QueueStorage` trait is deferred to Phase 2, when it has operations to abstract. sqlx is built without TLS
  for now; add `tls-rustls-*` when a deployment needs it.
- One PostgreSQL container per test (about 1.7 s for the suite). Revisit if Phase 2's suite gets slow.

**Original plan:**


**Goal:** a compiling library and a green integration-test loop against a real PostgreSQL.

- Dependencies:
  - `tokio`
  - `sqlx` (postgres, runtime-tokio, tls-rustls, json, uuid, chrono)
  - `serde` / `serde_json`
  - `uuid`
  - `chrono` (UTC, microsecond precision in PG)
  - `tracing`
  - `thiserror`
  - dev: `testcontainers` + `testcontainers-modules` (postgres), `tracing-subscriber`
- Use runtime-checked `sqlx::query(...)`, not the `query!` macros. The table name is configurable, and the macros
  would require a live `DATABASE_URL` at compile time.
- Module layout (mirrors the suggested design):
  ```text
  src/
    lib.rs            public API re-exports
    types.rs          QueueName, QueueEntryId (newtypes), QueuedMessage<T>, MessageMetaData
    error.rs          DurableQueuesError (thiserror)
    storage/mod.rs    trait QueueStorage
    storage/postgres.rs  PostgresQueueStorage (SQL from CONTRACT.md)
    schema.rs         idempotent CREATE TABLE/INDEX IF NOT EXISTS, table-name validation
  tests/common/mod.rs Testcontainers PG fixture (one container per test binary)
  ```
- **Table-name validation.** Port `PostgresqlUtil.checkIsValidTableOrColumnName` (CONTRACT.md §9): lower-case,
  then `^[A-Za-z_][A-Za-z0-9_]{0,62}$` per part, plus a reserved-word rejection. The table name is concatenated into SQL, so this is the injection surface.
- Schema creation is optional (`SchemaOwnership::Library | External`), so it can share a DB whose schema Java
  already created.

**Exit gate:** `cargo test` passes from the root, including a test that creates the schema, and a test that runs
against a schema Java created (from the Phase 0 DDL) without altering it.

---

## Phase 2 — Vertical slice: enqueue → claim → handle → acknowledge ✅ done

**Delivered:**
- **Enqueueing.** `DurableQueues` has `queue_message(s)` and `queue_message(s)_in`; the `_in` variants take the
  caller's transaction (Outbox). A batch is one `UNNEST` insert.
- **Message typing.** The `QueueMessage` codec trait has a blanket impl for single `MessageType` structs and can be
  implemented by hand on an enum, for queues that mix Java payload types.
- **Message model.** `Message`, `MessageMetaData` (a flat string map; Java always writes `{}`) and
  `QueuedMessage`.
- **Consumer.** `ConsumerConfig`, a `QueueConsumer` handle, and a `MessageHandler` trait that also accepts plain
  async closures.
- **Tests.** 9 integration tests, and the consume suite passed 20 out of 20 runs.

**Behaviour, all per CONTRACT.md:**
- **Claiming.** The claim is the verbatim unordered statement, so ordered rows are never claimed. Its limit is the
  number of free worker slots: a claimed message never waits in a local buffer.
- **Settling.** Claim, handler and ack are separate transactions. An undecodable payload or metadata is
  dead-lettered on the spot without reading the row back (Java's `markAsDeadLetterMessageDirect`).
- **Concurrency.** One consumer per queue per `DurableQueues`. `stop()` stops claiming, waits for in-flight
  handlers up to `shutdown_timeout`, then abandons them to stuck-message recovery.

**Decisions:**
- **No `QueueStorage` trait.** There is one backend; `DurableQueues` (typed API and consumers) sits directly on
  `PostgresQueueStorage` (row-level SQL). Extract the trait when a second backend or a test double needs it.
- **Per-queue fetcher.** Each consumer has its own fetch loop instead of Java's single fetcher across all queues.
  The SQL per queue is identical; the difference is one poll per queue rather than one per instance. Revisit in
  Phase 7 if polling load matters.
- **Deferred to Phase 3, marked `TODO(phase 3)`:** a handler error or panic leaves the message claimed, so it
  waits for stuck-message recovery, which Phase 3 also adds.

**Mutation check:** without `FOR UPDATE SKIP LOCKED`, the exactly-once test fails in every run, with messages
delivered 2–3 times.

**Original plan:**


**Goal:** the happy path, with correct concurrency.

- `DurableQueues::queue_message(queue, payload: &T, meta) -> QueueEntryId`, plus `queue_messages` (batch).
  - Overload taking `&mut PgConnection` / `&mut Transaction`, so the enqueue joins the caller's transaction (the
    Outbox use case, matching Java's "joins the caller's UnitOfWork").
- `message_payload_type`: Java readers need a type name they can resolve. Use a `MessageType` trait / registry
  that maps a Rust type ⇄ the type-name string (default `std::any::type_name`, overridable to a Java FQCN for
  interop).
- Claim with the unordered claim SQL (CONTRACT.md §4.1: CTE `SELECT … FOR UPDATE SKIP LOCKED LIMIT :limit` +
  `UPDATE … RETURNING`). Like Java's centralized fetcher, `:limit` = free worker slots, so claimed messages never
  wait in a local buffer.
- `QueueConsumer`:
  - one fetcher task per queue, gated by a `Semaphore(parallel_consumers)`; each claimed message runs in its own
    task
  - fixed polling interval for now
  - handler trait: `async fn handle(&self, msg: QueuedMessage<T>) -> Result<(), HandlerError>`
  - ack = the Java ack SQL, in its own transaction **after** the handler returns. No shared transaction with the
    handler.
- **Ordered-message safety.** Until Phase 5, the Rust claim query only takes rows with `key IS NULL`. Rust then
  never claims ordered messages that Java put in a shared queue, so it cannot reorder them.
- Graceful shutdown with a `CancellationToken`: stop claiming, let in-flight handlers finish (with a timeout), and
  leave unfinished messages to stuck-message recovery.

**Tests (Testcontainers):**
- enqueue → consumed → row gone
- payload/metadata round-trip
- a Java golden-fixture row is consumed and deserialized
- 4 consumers × 1,000 messages: each handled **exactly once** (no failures injected) and none lost
- enqueue inside a rolled-back transaction leaves nothing behind

**Exit gate:** all of the above green and repeatable (run the concurrency test 20× in a loop).

---

## Phase 3 — Retries, delayed delivery, dead letters, stuck messages

**Goal:** the full failure path of the experiment's initial scope.

- `RedeliveryPolicy` enum: `Fixed`, `Linear`, `Exponential`, with `max_redeliveries`. Implement the formulas
  in CONTRACT.md §8 as **pure functions with unit tests**, including the cap-with-`>=` rule and the worked example
  (500ms, 500ms, 1s, 2s, … for exponential).
- `HandlerError::{Transient, Permanent}`, plus a `MessageDeliveryErrorHandler`-like classifier. A deserialization
  failure is always permanent and is dead-lettered directly, without re-deserializing (Java's
  `markAsDeadLetterMessageDirect`).
- Retry (CONTRACT.md §5): `redelivery_attempts += 1`, `next_delivery_ts = now + policy.delay(redelivery_attempts as
  claimed)`, `last_delivery_error`, and clear `is_being_delivered`/`delivery_ts`.
- Dead letter (§7): a permanent error, or `total_attempts >= max_redeliveries + 1`.
- Delayed delivery: `queue_message_with_delay(…, Duration)`.
- **Stuck-message recovery** (§6): run the multi-queue reset statement at the start of a fetch, throttled to once
  per `message_handling_timeout` (default 30 s) per queue, like Java. Use the queue-scoped variant only.
- DLQ operations: `get_dead_letter_messages`, `resurrect_dead_letter_message(id, delay)`,
  `mark_as_dead_letter_message(id, reason)`.
- Polling backoff: an optional `PollingOptimizer`, exponential with jitter, like `CentralizedQueuePollingOptimizer`.
  LISTEN/NOTIFY is deferred.

**Tests:**
- retry counts and `next_delivery_ts` for each policy
- dead letter after exactly N+1 deliveries (`max_redeliveries = N`)
- a permanent error dead-letters on the first attempt
- a delayed message is not delivered early
- a crashed consumer (task aborted mid-handler) is redelivered after the timeout
- resurrect puts a message back into delivery

**Exit gate:** the Phase 3 tests are green, and every scenario in the experiment's "Initial Scope" list is covered.

---

## Phase 4 — Hardening: tracing, ported Java tests, edge cases

- `tracing` spans per message (`queue_name`, `queue_entry_id`, `attempt`) and per SQL operation. Structured
  fields only; payloads are never logged.
- Port or reproduce the critical tests from Java's `DurableQueuesIT` / `PostgresqlDurableQueuesIT`. Make a
  checklist from the Java test names so coverage gaps are visible.
- Edge cases:
  - duplicate delivery (handler succeeds but the ack fails, e.g. the connection dropped)
  - DB restart mid-run (the pool recovers)
  - pool exhaustion (`parallel_consumers` > pool size)
  - a long-running handler that exceeds the timeout (expected duplicate; document it)
  - shutdown with in-flight messages
- `cargo clippy -D warnings`, `cargo fmt --check` and rustdoc on the public API.

**Exit gate:** checklist complete, CI-style `cargo test && cargo clippy` green.

---

## Phase 5 — Ordered messages (optional; decided in Phase 0)

- `OrderedMessage { key, order }` maps to `delivery_mode = 'IN_ORDER'`, `key`, `key_order`.
- Claim the ordered head using the Java SQL and the `_ordered_head` index. Locally, track in-flight keys
  (`DashSet` / `Mutex<HashSet>`) so the same key is never processed concurrently on one node.
- Same caveat as Java: ordering holds **per node** only. Multi-node ordering requires a single global consumer
  (FencedLock), which is out of scope.
- Remove the `key IS NULL` restriction from Phase 2.

**Exit gate:** per-key order holds under 10 parallel consumers. A failing head blocks later messages for its key
exactly as Java does.

---

## Phase 6 — Java ↔ Rust interoperability and crash recovery

**Setup:** this devcontainer has no JDK, so the Java side runs in Docker. Build a minimal Java harness app
(`interop/java-harness/`, Maven, Essentials 0.60.0, `eclipse-temurin` image). It takes CLI/env commands
(produce N, consume, consume-and-crash-after-K) and runs against the same Testcontainers PostgreSQL over a shared
Docker network. The Rust side gets an equivalent `interop` binary (or an example under `examples/`).

Scenarios, from the experiment doc:

| # | Scenario | Assert |
|---|---|---|
| 1 | Java producer → Rust consumer | all consumed, payload/metadata intact |
| 2 | Rust producer → Java consumer | Java deserializes `message_payload_type` and payload |
| 3 | Java + Rust competing consumers | union = all messages, no loss; duplicates only from timeouts |
| 4 | Java consumer killed (`docker kill`) → Rust recovers | after the timeout, Rust redelivers |
| 5 | Rust consumer killed → Java recovers | same |
| 6 | Retry/delay/DLQ written by one side, observed by the other | counters and DLQ flags identical |
| 7 | Clock skew between nodes (only if Phase 0 found app-clock timestamps) | document the behaviour |

**Exit gate:** all scenarios automated (a `cargo test --features interop`, ignored by default) and green.
Discrepancies are written down, not patched around.

---

## Phase 7 — Benchmarks

- Workload generator plus harnesses for both languages, run as **containers with equal CPU and memory limits**
  against one dedicated PostgreSQL.
- Hold constant: payload size, `parallel_consumers`, pool size, polling interval, number of queues, duration,
  warm-up.
- Load levels: idle, 100, 1k and 5k msg/s, and saturation.
- Measure the metrics in the experiment table:
  - idle and loaded RSS (`docker stats`)
  - startup time to first message consumed
  - throughput
  - p50/p99 enqueue→handle latency (timestamp in the payload)
  - CPU
  - DB connections and `pg_stat_statements` load
  - image/binary size
- Record SQL differences explicitly. Any batching or query change that is not in Java invalidates a
  language-to-language comparison unless it is reported separately.

**Exit gate:** a reproducible `bench/` script and a filled-in results table with raw data committed.

---

## Phase 8 — Evaluation report

Fill in the experiment's comparison table: effort (hours per phase, tracked from Phase 1 onwards), code size
(`tokei`), test count, bugs found in each implementation, operational notes, and a recommendation. This answers the
experiment's success criteria. Replacing Java is not a goal.

---

## Cross-phase notes

- **Workspace:** start with the single `durable-queues-rs` crate. Split out `durable-queues-interop` and
  `durable-queues-bench` as workspace members only when Phases 6–7 need binaries with their own dependencies.
- **Out of scope until the core is done** (per the experiment doc): admin API, statistics, TTL jobs,
  LISTEN/NOTIFY, Inbox/Outbox, a Spring-style config layer.
- **Biggest risks:**
  1. Phase 0 SQL subtleties: the claim predicate, stuck-message reset, `>` vs `>=` on the retry limit.
  2. `message_payload_type` / JSON shape compatibility with Jackson.
  3. Timestamp precision and clock source.
  4. Benchmark fairness.
