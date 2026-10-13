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
from the real Java harness (done: they match).

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
- **No `QueueStorage` trait in Phase 2.** Superseded after Phase 3: the trait was added to match the
  experiment's design (see the Phase 3 notes).
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

## Phase 3 — Retries, delayed delivery, dead letters, stuck messages ✅ done

**Delivered, item by item against the list below:**
- **`RedeliveryPolicy`:** fixed, linear and exponential backoff; `max_redeliveries`; Java's cap-with-`>=` and NaN
  rules. The §7 decision and the §8 formulas are pure functions, unit-tested against the Java worked example.
- **`HandlerError::{Transient, Permanent}`:** `?` gives `Transient`. `RedeliveryPolicy::stop_redelivery_on`
  matches anywhere in the `source()` chain (Java's `stopRedeliveryOn`). A handler panic, raised either while the
  handler is called or inside its future, is a transient failure (Java catches every `Throwable`).
- **Retry and dead letter:** the verbatim §5 statements. `last_delivery_error` holds the error and its
  `Caused by:` chain.
- **Delayed delivery:** `queue_message_with_delay`.
- **Stuck-message recovery:** the multi-queue reset, scoped to the consumer's queue. It runs in the same
  transaction as the claim, on the first poll and then at most once per `message_handling_timeout`, which is set
  on `DurableQueues` (default 30 s).
- **Dead-letter operations:** `get_dead_letter_messages`, `resurrect_dead_letter_message` (counters kept, as in
  Java) and `mark_as_dead_letter_message`.
- **`PollingBackoff`:** Java's `CentralizedQueuePollingOptimizer` algorithm, opt-in.
- **Tests:** 9 integration tests, one per item of the test list, plus panics and a backed-off idle consumer. The
  Phase 3 suite passed 15 out of 15 runs.

**Decisions:**
- **`ConsumerConfig::new(queue, policy)` requires a policy.** Java has no default either.
- **`RedeliveryPolicy` is a struct.** It wraps a private backoff enum, so the error classifier and
  `max_redeliveries` are not repeated per variant.
- **Dead-letter operations return stored rows (`QueueEntry`),** not typed messages: dead letters are often the
  undecodable ones.
- **Dead-letter listing is sorted by `(added_ts, id)`.** Java ignores its sort-order parameter and pages without
  `ORDER BY` (CONTRACT.md §5).
- **Not in this phase:** a handler asking for its own redelivery (Java's `markForRedeliveryIn`; CONTRACT.md §14
  lists it under Phase 3 but this phase's list does not). It moves to Phase 4 with the test porting.

**`QueueStorage` trait (added at the end of Phase 3, per the experiment's design):**
- **The trait.** Every storage operation the queues use (insert, claim with stuck reset, ack, retry, dead-letter,
  resurrect, list, find) is a `QueueStorage` method, with its contract section cited.
- **Backend-neutral types.** `QueueEntry`, `NewEntries`, `ClaimResult`, `StuckMessageReset` and
  `QueueingSortOrder` live in `storage`.
- **Generic over storage.** `DurableQueues<S = PostgresQueueStorage>` and the consumer are generic over it.
- **Postgres-only methods.** `queue_message(s)_in` (joining the caller's transaction) is only on
  `DurableQueues<PostgresQueueStorage>`, since a `PgConnection` is not backend-neutral.

**Bug found and fixed in testing:** the first panic guard wrapped only the handler's future. A closure handler
that panics before returning its future escaped it and left its message claimed. The panic test fails with that
version.

**Original plan:**


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

## Phase 4 — Hardening: tracing, ported Java tests, edge cases ✅ done

**Delivered, item by item against the list below:**
- **Tracing:**
  - spans `durable_queue_consumer` (queue) and `deliver_message` (queue_name, queue_entry_id, attempt,
    redelivery_attempts, and a recorded `outcome`)
  - one debug span per storage operation (`durable_queues.claim`, `.acknowledge`, `.retry`, …) with table,
    queue, id and count fields; arguments are skipped
  - `tests/tracing.rs` captures every span and event and asserts that no payload value appears
- **Java tests:** [`docs/JAVA_TEST_CHECKLIST.md`](docs/JAVA_TEST_CHECKLIST.md) maps every Java test method.
  - 23 are ported or reproduced (`tests/java_ports.rs` and earlier suites), with Java's numbers.
  - 7 wait for Phase 5 (ordered).
  - The rest are N/A, each with a reason: interceptors, batched fetch, observer/metrics, schema migration,
    Java utilities.
  - Java's `*_with_db_connectivity_issues` tests use `docker pause` for 5 s; so does the port.
- **Edge cases:** `tests/edge_cases.rs`
  - a failed ack is redelivered after the timeout, through a `QueueStorage` test double
  - a real container stop and start
  - a pool smaller than the parallelism
  - a handler outliving the timeout (documented duplicate)
  - shutdown with in-flight messages is covered by Phase 2's tests
- **Moved here from Phase 3:** handler-requested redelivery is `Err(HandlerError::redeliver_in(delay))`. Like
  Java's `markForRedeliveryIn`, it bypasses the policy and stores "Manually requested redelivery".
- **Lint and docs:** `#![warn(missing_docs)]` with every public item documented.
  `RUSTDOCFLAGS="-D warnings" cargo doc`, `cargo fmt --check` and `cargo clippy --all-targets -D warnings` are
  all clean.

**Bug found and fixed:** serde_json's error messages quote the offending input, so the "cannot decode message" log
line leaked payload values. Logs now get `DecodeError::redacted()` (error category and position). The full message
is still stored in `last_delivery_error`, next to the payload it describes. The tracing test fails if the
redaction is removed.

**Not ported (by the experiment's scope):** Java's query and admin API (queue names, counts,
`getQueuedMessages`, `purgeQueue`, `queryForMessagesSoonReadyForDelivery`), `queueMessageAsDeadLetterMessage`
and interceptors. The checklist notes which Java assertions this leaves out.

**Original plan:**


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

## Phase 5 — Ordered messages ✅ done

**Delivered, item by item against the list below:**
- **Ordered messages:** `Message::in_order(key, order)` writes `IN_ORDER`, `key`, `key_order`. Java's order ≥ 0
  rule is enforced at enqueue (`InvalidMessage`), and a batch can mix ordered and normal messages. Claimed
  messages carry their `MessageOrder`. `has_ordered_message_queued_for_key` is Java's query, verbatim.
- **Claiming:** `QueueStorage::claim` (renamed from `claim_unordered`) runs Java's fetch sequence in one
  transaction: stuck reset, the verbatim ordered claim with this consumer's in-flight keys excluded, then normal
  messages only if no ordered one was claimed. In-flight keys are a per-consumer set; a lease releases each key
  once its message is settled.
- **Per-node caveat:** kept, as in Java. CONTRACT.md §4.3 now explains which mechanism protects what, and why
  ordering is per node.
- **The Phase 2 restriction:** Rust now claims ordered rows; the Phase 2 Java-fixture test asserts they arrive in
  key order.
- **Tests:** `tests/ordered.rs`, 11 tests. They cover the 7 Java ordered tests (checklist updated, now 30 Java tests
  ported or reproduced) and the exit gate: 10 parallel consumers, a retried head holds back only its own key, and a
  node never runs one key twice at once.

**Mutation checks:**
- Without the per-key barrier, ordering and per-key exclusivity fail in 4 tests.
- Without the key exclusion, a message outliving the handling timeout is handled twice concurrently.

**Fixed along the way:** the "key already in flight" invariant check was a `debug_assert!`. In debug builds it
panicked and killed the consumer's fetch loop. It now logs an error, because a `QueueStorage` that ignores
`exclude_keys` is not a reason to stop consuming.

**Build note:** a full `cargo test` links about 9 test binaries in parallel, and once ran out of memory in this
container (the linker was killed with signal 9). `cargo test -j 2` avoids it.

**Original plan:**


- `OrderedMessage { key, order }` maps to `delivery_mode = 'IN_ORDER'`, `key`, `key_order`.
- Claim the ordered head using the Java SQL and the `_ordered_head` index. Locally, track in-flight keys
  (`DashSet` / `Mutex<HashSet>`) so the same key is never processed concurrently on one node.
- Same caveat as Java: ordering holds **per node** only. Multi-node ordering requires a single global consumer
  (FencedLock), which is out of scope.
- Remove the `key IS NULL` restriction from Phase 2.

**Exit gate:** per-key order holds under 10 parallel consumers. A failing head blocks later messages for its key
exactly as Java does.

---

## Phase 6 — Java ↔ Rust interoperability and crash recovery ✅ done

**Delivered, item by item against the list below:**
- **Java harness:** `interop/java-harness/`, built from Essentials 0.60.0 on Maven Central, with the pinned versions.
  - `Harness.java` runs `PostgresqlDurableQueues` with default settings (centralized fetcher, Essentials' JSON
    serializer) through a `JdbiUnitOfWorkFactory`. It offers `produce`, `consume` and `consume-and-crash-after K`.
  - Payload classes are named as in the fixture: `com.acme.orders.OrderPlaced`, with `CharSequenceType` ids, a
    `BigDecimal` and an `Instant`; `OrderEvent$OrderAdded` for ordered messages; `OrderEvent$OrderCancelled`;
    `com.acme.billing.InvoiceIssued`.
  - The POM declares JDBI, the PostgreSQL driver, Jackson 3, reactor-core and SLF4J itself (S2.1).
  - The Dockerfile has two stages, a `~/.m2` cache mount, and a fat jar on `eclipse-temurin:25-jre`. The image
    builds in about 35 s cold; the tests rebuild it on first use (cached).
- **Rust node:** the `interop` binary (`src/bin/interop.rs`). It has the same commands, options and JSON-lines
  events as the Java harness, so a test drives either side the same way. Its payload enum dispatches on the Java
  FQCNs.
- **Tests:** `tests/interop.rs`, 13 tests, all green, plus 5 consecutive full-suite runs (all 13 in parallel, about
  21 s each). Each test gets its own PostgreSQL on a private Docker network. Java nodes are containers; Rust nodes
  are child processes. A Java crash is a `docker kill`, a Rust crash a SIGKILL. The scenarios:
  1. Java → Rust and 2. Rust → Java: 20 normal plus 3 keys × 5 ordered. Every message is handled once, and type,
     payload and metadata equal the stored row. On the Java side the payload is Jackson's re-serialization of the
     deserialized class, so Jackson read every field. Each key is handled in order.
  3. Competing consumers: Java and Rust (2 workers each) share 200 messages from both producers. The union is all
     200, there are no duplicates, and both nodes handled some.
  4. / 5. Crash recovery, both directions: the crashed node hangs on its 4th message and is killed. The other side
     redelivers it with `total_attempts` 2 and `redelivery_attempts` 1, no earlier than its own handling timeout
     (3 s) and within 2 × timeout + 5 s.
  6. Written by one side, observed by the other:
     - **Dead letters:** each side exhausts the other's message (max 2 redeliveries, 3 deliveries). The rows have
       identical counters and flags, `(3, 2, dead, not delivering, NULL, NULL)`. The opposite side resurrects each
       dead letter and handles it with counters 4 / 2.
     - **Delay and retry:** a delay written by one side and a retry written by the other are both honoured, in
       both directions.
  7. Clock skew: libfaketime moves the Java wall clock.
     - A producer 10 s ahead stamps `added_ts` 10 s ahead, so its messages wait 10 s on every consumer.
     - A consumer 60 s ahead (default 30 s timeout) resets a Rust node's fresh claim at its first poll and handles
       the message while Rust is still running it: a duplicate.
     - CONTRACT.md §2 has the details.
- **Golden fixtures (CONTRACT.md §13):** the Java harness queues the fixture's messages (`produce-fixture`).
  `the_synthetic_java_fixture_matches_what_java_writes` compares them with `java_rows.sql` on every column except
  ids and absolute timestamps. That covers payloads, metadata, delivery mode, key / order, counters, the 1 h delay,
  and the direct dead letter (NULL `next_delivery_ts`, the first line of the error). They matched on the first run,
  so no contract bug was found.
- **Hazard 1 (CONTRACT.md §12):** `unknown_payload_types_are_dead_lettered_by_either_side`. Java dead-letters a
  Rust-only type (`DurableQueueDeserializationException`), and Rust dead-letters a Java-only type.

**Mutation checks:**
- Without Rust's stuck-message reset, scenario 4 never recovers.
- With Rust resetting at `now` instead of `now − timeout`, scenario 4 redelivers after 140 ms and fails.
- Without `next_delivery_ts <= now` in Rust's claim, the Java-delayed message is delivered early.

**Found in Java (for Phase 8):**
- **Bug:** `DefaultQueuedMessage.getDeliveryMode()` returns a hard-coded `NORMAL` (`DefaultQueuedMessage.java:111`).
  The row mapper builds an `OrderedMessage` for `IN_ORDER` rows, but a handler asking the queued message for its
  delivery mode is told `NORMAL`. Ordering is unaffected. Pinned by `java_reports_every_queued_message_as_normal`;
  not patched around.
- **Contract detail:** for a failed handler, `last_delivery_error` is the stack trace of a `UnitOfWorkException`
  wrapping the handler's exception. That wrapper comes from 0.60's `SingleOperationTransactionDurableQueuesInterceptor`.
  CONTRACT.md §2 updated.

**No Rust bugs found.** Two test-side races were fixed. Nodes print `handled` before the ack, so "table empty" is
awaited, and a consumer is kept running until its ack has landed.

**Deviations, all additive:**
- **Rust node:** an `[[bin]]` in this crate rather than a new workspace member. It needs no dependencies of its
  own, the condition the cross-phase note sets for splitting.
- **Gating:** the binary and `tests/interop.rs` use `required-features = ["interop"]` rather than `#[ignore]`.
  Plain `cargo test` skips them; `cargo test --features interop --test interop` runs them.
- **Harness extras** beyond produce / consume / crash: `produce-fixture` (for §13), `resurrect` (scenario 6), and
  the `--fail-attempts`, `--handle-ms` and `--timeout-ms` options.
- **Clock skew:** libfaketime is in the Java runtime image, inactive unless `LD_PRELOAD` and `FAKETIME` are set.
- **Fixture file:** `java_rows.sql` is kept rather than replaced by a dump. The Phase 2 tests depend on its ids and
  on row 7, which Java's serializer cannot produce. The interop test diffs it against real Java rows on every run.
- **Versions:** `reactor-core` 3.8.4 is not in stack-pins.md (Spring Boot manages it in an application), and
  Jackson 3.1.5 and PostgreSQL JDBC 42.7.13 come from the Essentials 0.60.0 root POM. The tests use
  `postgres:17-alpine` like the rest of the suite; stack-pins.md lists 18.4.
- **Two extra tests:** the Java delivery-mode bug, and hazard 1.

**Original plan:**

**Agreed approach (decided before starting):**
- **Getting Essentials.** Essentials 0.60.0 is published on Maven Central (`dk.trustworks.essentials.components`;
  checked with `curl` against `repo1.maven.org/.../maven-metadata.xml`). Download it there; do not build it from
  source.
- **Harness location.** The Java harness lives in `durable-queues-rs/interop/java-harness/`: a `pom.xml` and a
  small main class taking produce / consume / consume-and-crash-after-K arguments plus a JDBC URL.
- **Dockerfile, two stages.**
  - Build: `maven:3.9-eclipse-temurin-25` runs `mvn package`, with a BuildKit cache mount for `~/.m2`.
  - Runtime: `eclipse-temurin:25-jre` with a single fat jar.
  - Nothing Java is installed in the devcontainer.
- **Versions** come from the Essentials plugin's `references/stack/stack-pins.md`: Java 25, `essentials.version`
  0.60.0, JDBI BOM 3.55.0, Maven 3.9.16.
- **Dependencies.** Essentials' framework dependencies are `provided` and not transitive (stack contract S2.1). The
  harness POM must declare JDBI, the PostgreSQL JDBC driver, Jackson 3 and an SLF4J binding explicitly.
- **Running it.** The Rust interop tests start the harness image through Testcontainers on the same Docker network
  as PostgreSQL. Crashes use `docker kill`, and connectivity loss uses `docker pause`, as Java's own tests do.
- **Java source for reference.** The 0.60.0 source used for CONTRACT.md is not in the repo. Re-clone it when
  needed into a scratch directory:
  `git clone --depth 1 --branch 0.60.0 https://github.com/trustworksdk/essentials-project.git`.


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

## Phase 7 — Benchmarks ✅ done

**Delivered:**
- `bench/run.py` (Python standard library only) and `bench/README.md`. One PostgreSQL 17 per run, the generator and
  the consumer under test as containers. The consumer gets 2 CPUs and 1 GiB for both languages. Every metric in the
  list below is measured. The generator is the same Rust `bench-produce` for both languages.
- `interop/rust-node/Dockerfile`: the Rust node as an image. Both harnesses got `bench-consume`, and the Rust node
  `bench-produce`, both with `--queues N`.
- Results: `bench/results/20261012T064824Z/`, holding `results.md`, `summary.json` and raw JSON lines. The full run
  takes about 50 min.

**Deviations (named, as the plan asks):**
- **`FetchPacing::FixedRate`** (new public API in `consumer.rs`). Java's fetcher polls at a fixed rate. That caps a
  consumer at `parallel_consumers / polling_interval`: 400/s with 8 slots, and 4000/s with 10 queues × 8. Rust's
  loop (now `FetchPacing::Eager`, still the default) has no such cap. `FixedRate` makes the language-to-language
  comparison possible; `rust-eager` is reported separately as an algorithm difference. A mutation check routed
  `FixedRate` to the eager loop, and `fixed_rate_pacing_claims_once_per_polling_interval_like_java` failed.
- **10 queues** added at the user's request, beside the plan's single queue. `--queues 1,10`; each count is a full
  run with `parallel_consumers` 8 and 128.
- **Planner statistics pinned** for the benchmark (autovacuum off for the table, a fixed VACUUM + ANALYZE
  routine). Without this, run-to-run variance reached 5×. See the next item.

**Bugs and findings:**
- **Stale statistics degrade the claim (Java and Rust alike, shared SQL).** When the statistics were taken while the
  table was empty, the `UPDATE … FROM cte_unordered` join becomes a nested loop over the queue. That costs 10–50 ms
  per claim with 20 000 queued. Recorded in CONTRACT.md §4.1 and measured as `saturation_stale_stats`.
- **At equal pacing the languages issue identical SQL** (same calls per second per statement). Throughput and
  latency match. Rust differs in resources:
  - about 2.5 MiB idle against about 110 MiB;
  - startup 70 ms against about 500 ms;
  - roughly half the CPU under load;
  - an image of 135 against 496 MiB.
- **With 10 queues** two implementation differences show:
  - At 128 slots per queue, Java's consumer is CPU-bound (199 % of 2 CPUs) and drains a backlog at 6.6k/s, against
    16.9k/s for Rust fixed-rate.
  - Rust's 10 independent fetchers make every statement on the database slower. The claim and even the shared
    generator's INSERT run about 2× slower per call than with Java's single fetcher at the same call count. Rust's
    idle CPU is also higher (14 % against 5 %). A likely cause, not verified: sqlx sends BEGIN and COMMIT as
    separate round trips (they show up in `pg_stat_statements`), and 10 loops claim concurrently.
- **Measurement limit:** a drain under about 2 s (`rust-eager` at 128 slots) falls between `docker stats` samples.
  Its CPU and memory columns read 0 and are not valid.

**Plan, as written:**

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

## Phase 8 — Evaluation report ✅ done

**Delivered:** [`docs/EVALUATION.md`](docs/EVALUATION.md). It has:
- the experiment's comparison table, filled in from the Phase 7 run;
- effort per phase;
- code size (`tokei`, against the Java 0.60.0 source);
- test counts and the Java checklist tally;
- bugs found in each implementation;
- operational notes, ergonomics and a recommendation.

**Deviation:** effort was not tracked as hours while the work happened. It is reconstructed from the Claude Code
session transcripts: active time with gaps capped at 15 min, cut at each phase's commit. Work before the first
transcript (14:23 UTC, 2026-10-11) is not counted.

**Plan, as written:**

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
