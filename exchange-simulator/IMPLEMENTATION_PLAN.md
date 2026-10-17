# exchange-simulator — implementation plan for the remaining work

**Status:** agreed 2026-10-14 (decisions D1, D2 and D5 settled, plus the priority question in D4). D3 and the rest of D4 must be settled before Milestones 3 and 4.
**Inputs:**
- [`rust_exchange_simulator_plan.md`](rust_exchange_simulator_plan.md) (§10 milestones, §11 risks)
- [`milestone_1_domain_schema_events.md`](milestone_1_domain_schema_events.md) (open choices, implementation progress)
- [`bench/README.md`](bench/README.md) (baseline)

Milestone 1 is done (steps 1–6 plus the containerized acceptance test). This plan covers what is left: Milestone 1
loose ends, then Milestones 2–4 broken into steps the size of the Milestone 1 steps. Each step ends the same way:

- **Gates:** `cargo fmt --check`, `cargo clippy --all-targets --features e2e -D warnings`,
  `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`, `cargo test -j 2`, and the `e2e` test.
- **Checks:** repeat runs of any new timing-sensitive suite, and mutation checks for the step's key guarantees.
- **Record:** an entry in the milestone's progress section (delivered, decisions, deviations), then a commit after
  review.

Sizes: **S** is under a step of Milestone 1, **M** about one, **L** two or more.

## Milestone 1 wrap-up

| # | Task | Size | Notes |
|---|---|---|---|
| 1.1 | Commit the containerized acceptance test; push `exchange-simulator-m1`; open a PR to `main` | S | |
| 1.2 ✅ | Investigate the 57–180 ms commit spikes | S | **Done 2026-10-14.** The cause is the disk, not SQLite. `bench checkpoint` replaces SQLite's auto-checkpoint with an identical observed one: checkpoint commits peak at 8.1 ms, and every commit over 10 ms is an ordinary one. Raw fsync on the same VM disk stalls for 141–950 ms on its own. Checkpoints still cost about 10 % of durable time and most of the p99, so 4.3 is re-scoped. Turning auto-checkpointing off halves throughput. Results: `bench/results/2026-10-14-checkpoint.md`. |
| 1.3 | Backlog from plan §3/§5 that Milestone 1 skipped ✅ (1.3a–d) | M | Instrument `enabled` flag (a disabled instrument rejects orders); participant settlement metadata (free-form JSON, carried into `TradeExecuted`); HMAC-SHA256 webhook signing (`X-Signature`, a per-subscription secret); an optional serialized-delivery flag per subscription (one in-flight delivery per subscription, in `sequence` order). All four are needed soon (D5), so they come before Milestone 2; split into 1.3a signing, 1.3b ordered delivery, 1.3c instrument `enabled`, 1.3d participant metadata. Each 1.3 item adds a migration to the schema, starting with `002`, so Milestone 2's settlement migration takes the next free number. |

### Progress

**1.3a — HMAC webhook signing ✅ (2026-10-14).**
- **Delivered:**
  - Migration `002_webhook_signing.sql` adds `webhook_subscriptions.secret`: nullable, 16–256 characters (CHECK),
    and validated by the API as well.
  - `POST /api/webhooks` accepts an optional `secret`. Responses never include it and show `signed` instead
    (`SubscriptionView`).
  - The worker serializes the envelope once and signs and sends those exact bytes. It adds `X-Signature-Timestamp`
    (attempt time) and `X-Signature: sha256=hex(HMAC-SHA256(secret, "{timestamp}.{body}"))` (`webhooks::signing`,
    using RustCrypto `hmac` 0.13 and `sha2` 0.11).
  - The README documents verification, with a Python example.
- **Tests:**
  - A signing vector computed with Python's `hmac` module.
  - An end-to-end test with signed and unsigned receivers: each attempt verifies, using the `hmac` crate directly,
    over the raw bytes received. The retry body is byte-identical, unsigned subscriptions carry no signature headers,
    the secret is never listed, a short secret gets 400, and the secret survives a restart.
  - Upgrading a version-1 database keeps existing subscriptions unsigned.
- **Mutation checks**, each made a test fail: the timestamp left out of the MAC; sent bytes different from the signed
  ones; the secret not stored; the length check skipped.
- **Decisions:**
  - The secret is client-supplied, as GitHub and Stripe do it, rather than generated, so tests can know it.
  - It is stored in plain text in SQLite, which is acceptable for a simulator. Rotation is not supported yet:
    register a new subscription.
  - Signing stays opt-in per subscription, so existing consumers keep working.
- `/health` `schemaVersion` is now 2.

**1.3b — ordered delivery ✅ (2026-10-14).**
- **Delivered:**
  - Migration `003_ordered_delivery.sql` adds `webhook_subscriptions.ordered`, plus a partial index on unfinished
    deliveries by `(subscription_id, event_sequence)`.
  - `POST /api/webhooks` accepts `"ordered": true`, and responses show it.
  - Ordering is enforced in the claim query alone: an ordered subscription's only claimable delivery is its unfinished
    one with the lowest sequence. While that one is leased, or backing off after a failure, nothing else for the
    subscription is due. That gives at most one in flight, sent in order, with no worker-side state.
  - The worker now claims again until a claim comes back empty, rather than stopping after a partial batch. Otherwise
    each next ordered event would wait a full poll interval.
  - `register_webhook` takes a `NewSubscription` struct instead of a growing argument list.
- **Tests:**
  - Store level, with an ordered and an unordered subscription: only the ordered head is claimable; a leased or
    backing-off head blocks the rest; after a dead letter the next event is claimable.
  - End to end: a slow receiver (15 ms per request) failing two attempts, 11 events, an ordered and an unordered
    subscriber. The ordered one never had two requests at once, received the events in `sequence` order, and was
    attempted exactly twice more than the event count. The unordered one received everything.
  - The webhook suite was repeated five times without failure.
- **Mutation checks**, each made a test fail: the ordering predicate ignored; the head computed over finished
  deliveries; an in-flight head not blocking; the flag not stored.
- **Decisions:**
  - After a dead letter, an ordered subscription *continues* with the next event, leaving a visible gap in
    `sequence`, rather than blocking for good.
  - Ordered throughput is one claim-POST-record round trip per event. It has not been measured; batching consecutive
    events per request is a possible later improvement.
  - The early-return change to the worker loop is a performance change. No test fails without it, because the tests'
    20 ms poll hides it.
- `/health` `schemaVersion` is now 3. The README shows it as `<n>`, so it no longer goes stale.

**1.3c — instrument `enabled` flag ✅ (2026-10-14).**
- **Delivered:**
  - Migration `004_instrument_enabled.sql` adds `instruments.enabled` (default 1), and `Instrument` gets an `enabled`
    field.
  - Registration accepts `enabled` (default `true`). `PATCH /api/instruments/{id}` with `{ "enabled": bool }` (only
    that field) toggles it durably: the store first, then the book, through the exchange thread.
  - `OrderBook::set_enabled` bumps the book version, so updates planned before a toggle are stale.
  - A disabled book turns every incoming order into a recorded rejection before matching, with the new
    `RejectReason::InstrumentDisabled` (`INSTRUMENT_DISABLED`): sequenced, stored `REJECTED`, `OrderRejected` emitted,
    `201`. Cancels still work and resting orders stay.
  - Instrument listings and the book snapshot show `enabled`.
- **Tests:**
  - Book level: limit and market orders rejected with nothing traded or rested; resting orders kept; cancel works;
    trading resumes; toggling makes earlier plans stale.
  - Store and coordinator: the flag survives restart (also when set at registration); the rejection and its event are
    stored; the rejected order's sequence is not reused; unknown instrument.
  - API: halt, reject, cancel, restart, resume; `400` for a bad body or a field other than `enabled`; `404` for an
    unknown instrument.
- **Mutation checks**, each made a test fail: the disabled check removed; no version bump; the toggle not persisted;
  registration ignoring `enabled`. The last one first survived, since tests only looked at the in-memory instrument,
  and a restart assertion was added to catch it.
- **Decisions:**
  - Orders on a halted instrument are *recorded rejections*, not input errors: a halt is a business outcome that
    consumers should see as an `OrderRejected` event.
  - Resting orders are kept, not cancelled, on a halt.
  - Toggling emits no event; an `InstrumentStatusChanged` event type would be a contract addition, left for later.
- The schema test now names its columns, so later migrations do not break it. `/health` `schemaVersion` is now 4.

**1.3d — participant settlement details ✅ (2026-10-14).**
- **Delivered:**
  - Migration `005_participant_settlement.sql` adds `participants.settlement`. It is nullable, and the database
    checks that it is a JSON *object* (`json_valid` and `json_type = 'object'`). `Participant` gets a `settlement`
    field.
  - `POST /api/participants` accepts it and echoes it back; the coordinator also checks that it is an object of at
    most 4 KiB.
  - The coordinator keeps participants' details in memory (loaded at startup) and passes a lookup to
    `events::for_update`. `TradeExecuted` therefore carries `buyerSettlement` and `sellerSettlement` as of execution
    time, `null` when absent, with no database read on the hot path.
  - New payload fields are additive, so the event `schemaVersion` stays 1.
- **Tests:**
  - Payload with and without details.
  - Details durable across restart and present in `TradeExecuted` before and after it.
  - A non-object or oversized value rejected and not stored.
  - The database refuses a non-object.
  - API registration echo, `400` for a string, array or number, and the replayed trade event carrying the details.
- **Mutation checks**, each made a test fail: buyer and seller details swapped; not persisted; dropped on recovery;
  the object check skipped; the size check skipped.
- **Decisions:**
  - Details are a snapshot taken at registration. There is no update endpoint yet; the plan's API table has none, and
    changing them would need to settle whether past trades are affected (they are not: events are immutable).
  - Still no `GET /api/participants`.
- `ExchangeError::InvalidParticipant` now carries a reason. `/health` `schemaVersion` is now 5.

## Milestone 2 — settlement simulation

**Goal:** an optional settlement engine. It turns each `TradeExecuted` into a settlement lifecycle under T+N rules on a
virtual clock, with seeded outcome distributions. It publishes lifecycle events through the existing outbox and
webhooks, and keeps simulator-originated and externally reported outcomes explicitly apart.

**Architecture** (proposal):
- **A consumer of the event log.** The engine reads `TradeExecuted` from `events` by sequence cursor; the cursor is
  stored in a new `consumer_offsets` table. Matching stays untouched and settlement is replayable. The matching core
  never waits for settlement.
- **A pure state machine plus a driver**, the same split as `OrderBook` and `Exchange`. The state machine plans
  transitions. The driver commits them, with their events and outbox rows, in one transaction through the single
  writer thread (a new dispatcher command).
- **Order-independent outcomes.** Each instruction's outcome is drawn from an RNG seeded by
  `(simulation seed, execution_id)`, so it does not depend on processing order or restarts.
- **Explicit authority.** Lifecycle events carry `source: "settlement-simulator"`. Externally reported outcomes (if
  D1 says so) carry `source: "external"` and are stored separately. The simulator never merges the two into one
  status.

| # | Step | Size | Delivers |
|---|---|---|---|
| 2.1 ✅ | Virtual clock and test-control API | M | A `VirtualClock` implementing `Clock` (set, advance, freeze), selected by config. Test-control routes under `/test/…`: `GET/POST /test/clock` (now, advance by, set to). They are mounted only when `test_control.enabled=true` (default false), on the same port. Every timestamp the service uses goes through the clock: order timestamps, `occurredAt`, delivery due times. Milestone 3 needs this too. |
| 2.2 ✅ | Settlement domain, migration, event contracts | M | `SettlementInstruction` (execution ID, intended settlement date, status, attempt, outcome history). Statuses `PENDING → SETTLED \| FAILED \| REJECTED`, plus `DELAYED`, `CORRECTED`, `CANCELLED`. A business-day T+N calculator that skips weekends (D2: no holiday calendars for now). Events: `SettlementPending`, `SettlementCompleted`, `SettlementFailed`, `SettlementRejected`, `SettlementDelayed`, `SettlementCorrected`, `SettlementCancelled`, with schemas documented the way `TradeExecuted` is. The pure state machine and its tests come first, as matching did. |
| 2.3 ✅ | Settlement driver and configuration | M | `settlement.mode: external \| simulated \| both` (`external` = today's behaviour). Settings: `settlement_days`, outcome probabilities, delay distribution, seed. The driver consumes trades, creates instructions, and fires due transitions when the virtual or real clock passes them. It restarts from the stored cursor and pending instructions. |
| 2.4 ✅ | Combined mode and inbound external outcomes | S–M | Agreed (D1). `POST /api/settlements/{executionId}/outcomes` stores an external outcome (`source: external`) beside the simulated lifecycle. `GET /api/settlements/{executionId}` shows both streams side by side. |
| 2.5 ✅ | Tests and acceptance | M | Deterministic: the same seed and the same clock moves give identical lifecycles across restarts. T+2 across a weekend. Failure, delay and correction scenarios. In combined mode, an external consumer and the simulator act on the same trade without conflict. Extend the `e2e` test to cover a simulated settlement driven by clock moves. |

**Acceptance:** with mode `both`, the seed fixed and the virtual clock frozen, one trade gives `SettlementPending` at
once and `SettlementCompleted` (or the seeded failure) exactly when the clock reaches T+2. Webhooks carry
`source: "settlement-simulator"` and the execution ID as correlation ID. A restart in the middle neither changes nor
duplicates the outcome.

### Milestone 2 progress

**2.1 — virtual clock and test-control API ✅ (2026-10-14).**
- **Delivered:**
  - `clock` module: `Clock`, `SystemClock`, `FixedClock` (moved from `engine`), and `VirtualClock`. It can be set,
    advanced, frozen or resumed; it never moves backwards; clones share its state.
  - Config `clock.mode: system | virtual` (plus `start` and `running`), and `test_control.enabled` (default false).
  - Migration `006_virtual_clock.sql` stores the clock's reading and running flag. Each change is persisted through
    the writer thread, and recovery resumes from the stored reading, never jumping back to the configured start.
  - Routes `GET /test/clock` and `POST /test/clock/{advance,set,freeze,resume}`, merged into the router only when
    test control is enabled. Errors: `409 clock_not_virtual`, `409 clock_backwards`, `400` for bad input.
- **Refactors:**
  - `ExchangeOptions` gained `Default`.
  - Event and outbox writing is now `store::write_events(tx, events, Outbox)`, reusable by settlement.
  - `NewEvent` carries its `source`.
- **Deviation from this plan's 2.1 text ("every timestamp goes through the clock"):** webhook delivery scheduling
  (`next_attempt_at`, leases, `delivered_at`, claim release at startup) uses **real time**. Only domain timestamps
  use the configured clock. Under simulated time, retries would either fire at once (due dates in the "past") or
  never (frozen clock), and a clock jump would expire in-flight leases and duplicate deliveries.
- **Tests:**
  - `VirtualClock` unit tests.
  - `tests/clock.rs` (5): virtual time stamps orders, trades and events; forward-only, freeze and resume; restart
    resumes; the system clock refuses changes; the routes are absent when disabled.
  - Webhooks: with simulated time in 2100 and a 30-day jump mid-delivery, the retry still waits its real-time
    backoff and nothing is delivered twice.
- **Mutation checks**, each made a test fail: deliveries due on simulated time; claims on simulated time (which first
  survived; arrival times were then recorded and the retry gap asserted); the stored clock not restored; clock
  changes not persisted; `set` allowed backwards.

**2.2 — settlement domain, migration, event contracts ✅ (2026-10-14).**
- **Delivered:**
  - `settlement` module:
    - `calendar::add_business_days`: Mon–Fri; a weekend trade counts from Monday.
    - `SettlementStatus`: the seven statuses, as a wire enum (`wire_enum!` is now shared inside the crate).
    - `Instruction`, and `SettlementPolicy` (seed, `settlement_days`, five probabilities, `max_delay_days`,
      `max_delays`, `correction_days`) with validation.
    - `TradeFacts::from_event`: reads a canonical `TradeExecuted`.
    - `format_amount`, and `draw`: a SplitMix64 hash of (seed, trade sequence, attempt, purpose) giving [0, 1).
  - `SettlementSimulator` is a pure plan-then-apply state machine, like `OrderBook`:
    - It holds open instructions in a queue ordered by (due at, trade sequence).
    - `plan(trades, now)` takes in new trades past the cursor, then runs every due transition in global time order,
      so one instruction may move several times in a single step.
    - The plan carries the new trade cursor.
  - Seven event types (`SettlementPending`, `…Completed`, `…Failed`, `…Rejected`, `…Delayed`, `…Corrected`,
    `…Cancelled`) with `source: "settlement-simulator"`, documented in the README. `EVENT_TYPES` is now a slice, so
    webhooks can subscribe to them.
  - Migration `007_settlement.sql`: `settlement_instructions` (with a due index), `consumer_offsets`, and an index on
    `events(aggregate_id)`.
- **Semantics (decided here; nothing needed the user):**
  - Due times are 00:00 UTC on the intended settlement date; T+0 is due at the trade itself.
  - Event `occurredAt` is the scheduled instant, so the same clock moves give the same timestamps whatever the tick
    timing.
  - One draw at each due date decides the outcome, with cancel, fail and delay as cumulative thresholds; settle is
    the rest.
  - Delays are 1..=`max_delay_days` business days, at most `max_delays` times; after that the instruction must
    resolve.
  - Corrections are scheduled at settlement, `correction_days` business days later, and move the amount by one tick
    per unit (a simulated price correction).
  - An amount that does not fit `i64` minor units is rejected (`AMOUNT_OUT_OF_RANGE`).
- **Tests:**
  - `tests/settlement.rs` (12):
    - T+2 pending then settled, with scheduled stamps; cursor skipping.
    - Every outcome; delay dates and the cap; a delay across a weekend; correction; T+0; amount overflow.
    - The same outcomes whether time moves in one jump or in five-hour steps, and different ones for another seed.
    - A simulator restored from storage continues identically.
    - Outcome rates within 2 % of their probabilities over 5,000 trades.
    - Planning does not mutate, and events come in global time order.
  - Unit tests for the calendar, the draws (uniform within 1 %, sensitive to every input), amounts and policy
    validation.
- **Mutation checks**, each made a test fail: weekends counted as business days; transitions processed a day early;
  delays uncapped; wall-clock stamps; cancel and fail thresholds mixed up; `apply` losing the queue.

**2.3 — settlement driver and configuration ✅ (2026-10-14).**
- **Delivered:**
  - A `settlement:` config section: `mode` (`external` default, `simulated`, `both`), `tick_interval_ms`, and the
    policy fields. `SettlementConfig::simulator_policy()` is `Some` only for `simulated` and `both`. An invalid
    policy stops startup.
  - The simulator lives on the exchange thread (`Exchange.settlement`), so there is still one SQLite writer. It is
    restored from `open_settlements()` and the `consumer_offsets` cursor.
  - `Exchange::settlement_tick()` reads `TradeExecuted` events past the cursor in pages of 500, plans, and commits
    via `Store::commit_settlement` (instruction upserts, events, outbox rows and cursor in one transaction) before
    applying. It wakes the delivery worker and updates `settlement_open_instructions`.
  - `change_clock` runs a tick before it replies, so `POST /test/clock/advance` returns with every transition due by
    the new time already done.
  - `App` runs a settlement ticker task every `tick_interval_ms` that shares the delivery worker's shutdown signal.
    `App::shutdown` stops all background tasks before draining the exchange thread.
- **Tests:**
  - `tests/settlement_flow.rs` (6):
    - T+2 is processed by the clock change itself, with scheduled stamps, source and correlation.
    - A restart mid-lifecycle neither re-takes trades nor loses delayed instructions; everything resolves exactly
      once.
    - External mode simulates nothing.
    - Webhook subscribers receive `SettlementPending` and `SettlementCompleted`.
    - Two fresh runs with the same seed, orders and clock moves give identical lifecycles, covering at least four
      kinds of outcome.
    - An invalid policy stops startup.
  - `tests/recovery.rs`: a failed settlement commit leaves no events and no cursor change; the retry takes the trade
    in exactly once.
  - The new flow suite was repeated three times without failure.
- **Mutation checks**, each made a test fail: no tick on a clock change; the cursor not persisted; open instructions
  not restored; event source ignored by the writer; the plan not applied after commit.
- **Decisions:**
  - Intake starts from the beginning of the event log, so enabling simulation on an existing database settles past
    trades too, with past dates and immediately.
  - Ticks are periodic and run after clock changes, not after every trade, which keeps the trading hot path free of
    settlement writes.

**2.4 — combined mode and inbound external outcomes ✅ (2026-10-14).**
- **Delivered:**
  - Migration `008_settlement_reports.sql` adds `settlement_reports`: a status CHECK, a unique
    `(execution_id, reference)`, and `details` as a JSON object.
  - `Exchange::report_settlement(NewReport)`:
    - It is accepted only when `external_outcomes` is set (modes `external` and `both`).
    - It validates the reference length and that `details` is an object of at most 4 KiB, and that the execution
      exists.
    - It is idempotent by reference: `NewReport::repeats` compares `occurredAt` only when the retry gives one.
    - It commits the report and a `SettlementOutcomeReported` event (`source: "external"`, aggregate and correlation
      = execution ID) in one transaction.
  - Routes: `POST /api/settlements/{id}/outcomes` (`201`, or `200` with `duplicate`); `GET /api/settlements/{id}`
    (`simulated`, `simulatedEvents`, `external` side by side); `GET /api/settlements?status=&limit=`.
  - `Store::events_for(aggregate, source)` uses the aggregate index from 007.
- **Tests:**
  - `tests/settlement_reports.rs` (5):
    - In `both` mode, an external `SETTLED` and a simulated `FAILED` stay apart: neither overwrites the other, the
      external report emits no `SettlementCompleted`, and the simulated history holds no external events.
    - Idempotency: a repeat, a repeat with the same `occurredAt`, a conflicting status or time, reports without a
      reference; restart survival.
    - `simulated` mode refuses reports with `409`.
    - Validation: unknown execution, bad status, missing status, non-object details, reference length, unknown
      fields, bad UUID.
    - Listing by status.
  - The `/health` test now derives the expected schema version from the files in `migrations/`, so it no longer
    needs editing per migration.
- **Mutation checks**, each made a test fail: the mode check removed; `occurredAt` ignored in idempotency; a
  duplicate answered `201`; the external source mislabelled; the simulated history not filtered by source.
- **Decisions:**
  - External reports get their own event type rather than reusing `Settlement*` types with another source, so a
    subscriber to `SettlementCompleted` only ever sees the simulator's verdict.
  - No state ordering is imposed on external reports.
  - In `external` mode reports are accepted too: there they are the only lifecycle record.

**2.5 — tests and acceptance ✅ (2026-10-14).**
- **Delivered**, covering the parts of the 2.5 list earlier steps had not:
  - `tests/settlement_flow.rs` (+4):
    - Settlement happens *exactly* when the clock reaches 00:00 UTC on the date: one second before Monday nothing
      happens, and a Thursday trade waits over the weekend.
    - A correction lands on the configured business day with a one-tick amount change.
    - The same script with two restarts in the middle gives the same lifecycle as without.
    - An external consumer receives `TradeExecuted` by webhook and reports `SETTLED` back over real HTTP while the
      simulator settles the same trade; each source's events stay its own.
  - `tests/e2e.rs`: `milestone_2_settlement_in_containers` runs the plan's Milestone 2 acceptance on the real image
    with MockServer:
    - Combined mode on a frozen virtual clock.
    - `SettlementPending` delivered with `source: "settlement-simulator"` and the execution ID as correlation ID.
    - A `docker restart` mid-lifecycle: the clock resumes, and the trade is not taken in again.
    - `POST /test/clock/advance` by 2 days settles it once, at `2030-01-09T00:00:00Z`.
    - An external report is published as `external:SettlementOutcomeReported`.

    The image build now runs once per test binary (`std::sync::Once`).
- **Found:** `App::shutdown` waits for every router clone to drop, because each holds a handle to the exchange
  thread. A test that served the router on an extra port hung on stop until it aborted that server first. `main` is
  unaffected, since its server finishes before shutdown. The constraint is documented at the call site.
- **Stability:** the settlement suites passed three runs in a row and the e2e suite two, with no leftover containers
  or networks.

### Milestone 2 status

Steps 2.1–2.5 are done.
- **Acceptance met** (plan: "with mode `both`, the seed fixed and the virtual clock frozen…"):
  - One trade gives `SettlementPending` at once.
  - `SettlementCompleted` comes exactly when the clock reaches T+2, or the seeded failure instead.
  - Webhooks carry `source: "settlement-simulator"` and the execution ID as correlation ID.
  - A restart in the middle neither changes nor duplicates the outcome.

  All of it is checked in-process and in containers.
- **Deviation:** delivery scheduling runs on real time (see 2.1).
- **Open for later:**
  - Holiday calendars (D2).
  - An `InstrumentStatusChanged` event.
  - Participant detail updates.
  - Simulator intake from "now" instead of from the start of the log, if wanted.
  - Reproducible *IDs* across runs (now decision D6, step 3.0). Today outcomes, dates and timestamps are
    reproducible, but event and execution IDs are random UUIDv7.

## Milestone 3 — scenarios and fault injection

**Goal:** reproducible test runs. These are YAML scenarios with ordered commands, clock moves and assertions; seeded
random markets; and faults injected into *delivered copies* of events, never into canonical ones.

| # | Step | Size | Delivers |
|---|---|---|---|
| 3.0 ✅ | Reproducible IDs (D6) | M | Config `ids: { mode: random \| deterministic, seed }`, default `random`. In `deterministic` mode every ID is a UUIDv8 built from a keyed hash of `(seed, kind, natural key)`. The keys already follow from the command order: order → its sequence; execution → (taker order sequence, fill index); event → its event sequence (the single writer knows the next one, and checks it against SQLite after insert); webhook delivery → (subscription, event sequence); subscription → its ordinal; external report → (execution, ordinal). No extra counters are stored, so a restart continues identically. Acceptance: the same commands, seed and clock moves give a byte-identical `/api/events` log and identical API responses (only real-time delivery fields differ), including with restarts in between. |
| 3.1 ✅ | Scenario DSL and runner | L | YAML: `seed`, `instruments`, `participants`, `subscriptions`, then `steps:` (`order`, `cancel`, `advance_clock`, `expect` on order state, trades, events, book, deliveries). It runs against an in-process service with a virtual clock and deterministic IDs. Per D3, both a CLI (`exchange-simulator scenario run file.yaml`, exit code = verdict) and `POST /test/scenarios` (test control only; returns the verdict and each failed expectation). Both use one runner. Example scenarios go in `scenarios/`. |
| 3.2 ✅ | Random market generator | M | Seeded distributions from plan §6: arrival rate, buy probability, market-order and cancellation probabilities, and price and size distributions per instrument. It generalizes the `bench` workload. Reproducibility comes from the seed plus deterministic sequencing: one generator feeds commands in order, under the virtual clock. |
| 3.3 ✅ | Delivery fault policies | M | Per-subscription policy, configured under `/test/…` or in a scenario: missing fields, explicit nulls, invalid values (a JSON-path list or a random choice by probability), duplicates, delays, out-of-order (hold and release). Faults apply to the outgoing copy only. A new `webhook_attempts` table records each attempt with the mutation applied, so a test can assert on what was actually sent. |
| 3.4 ✅ | Reset and replay controls | S | `POST /test/reset` clears all state (test mode only). `POST /test/webhooks/{id}/redeliver?after=` re-enqueues deliveries from a sequence. Dead letters can be listed and retried. |
| 3.5 ✅ | Tests and acceptance | M | A scenario suite in CI: the same file and seed give byte-identical event logs over two runs. Each fault type is visible at the receiver and absent from `/api/events`. Reset gives a clean state. |

### Milestone 3 progress

**3.0 — reproducible IDs ✅ (2026-10-15).**
- **Delivered:**
  - `ids` module: `IdScheme::{Random, Deterministic { seed }}`. Deterministic IDs are UUIDv8 from
    SHA-256(`"exchange-simulator/ids/v1"`, seed, kind, length-prefixed natural key), with one function per kind:
    `order`, `execution`, `event`, `delivery`, `subscription`, `report`. It replaces the old `IdGenerator`,
    `UuidV7Ids` and `SequentialIds`.
  - `MatchContext` carries the scheme, and an execution ID is (taker sequence, fill index).
  - Config `ids: { mode: random | deterministic, seed }`, default `random`.
  - **Event sequences come from the writer.** `Exchange.next_event_sequence` starts at max(`MAX(sequence)`,
    `sqlite_sequence`) + 1 and moves on only after a successful commit. Events are built unstamped, then
    `NewEvent::stamp` gives each its sequence and the ID derived from it, and they are inserted with explicit
    sequences. Delivery IDs derive from (subscription, event sequence).
  - **Settlement intake moved into the trade's transaction** (revising 2.3's tick-based intake). `commit_and_apply`
    plans intake for the trade's executions and commits trade, events, settlement instructions, settlement events
    and cursor together. Recovery runs one settlement step before serving, to catch up trades from before the
    simulator was on. The ticker remains for due transitions under a running clock.
  - `Store::commit` takes an optional `SettlementWrite`. Event and settlement writing are shared helpers.
- **Tests:**
  - `ids` unit tests: stable and seed-dependent; one ID pinned to a value computed independently with Python's
    hashlib; no collisions across kinds and keys; random mode gives distinct v7s.
  - `tests/reproducible.rs` (3), over a script touching every ID kind and all three event sources:
    - Two runs are byte-identical in `/api/events`, trades, webhooks, settlements, the book, every order and
      settlement detail, the delivery IDs and every command response.
    - The same holds with two restarts in the middle.
    - Random mode differs only in IDs.
  - Event IDs equal `ids.event(sequence)`, and sequences have no gaps.
  - The settlement failure test now covers both paths: a failed intake rolls the whole trade back; a failed tick
    leaves transitions due. Neither leaves a gap in the sequence.
  - The reproducibility suite was repeated five times without failure.
- **Mutation checks**, each made a test fail: random execution IDs; random delivery IDs; random subscription IDs;
  intake left to the ticker (the log order then varied); the sequence counter not advanced.
- **Decisions:**
  - A trade whose settlement intake cannot be written is not committed either. Trade and intake are one unit, and
    both live in the same database.
  - In simulated modes, the trade transaction grows by one instruction row and one event per execution. The
    benchmark runs in `external` mode, so its numbers are unaffected.
  - Hash-derived IDs are not time-ordered. Nothing relies on ID order: lists order by sequence or rowid.

**3.1 — scenario DSL and runner ✅ (2026-10-15).**
- **Delivered:**
  - `scenario` module: `Scenario::parse` (YAML via `serde-saphyr`; JSON works too), a containment matcher, and one
    runner, `scenario::run(&Scenario, &Config) -> Report`.
  - Setup: `seed`, `start`, `settlement` (any policy field), `instruments`, `participants`, `webhooks` (the plan's
    `subscriptions`), all as API request bodies.
  - Steps: `order`, `cancel`, `advance_clock`, `set_clock`, `report_settlement`, `set_instrument`, and `expect` on an
    order, a book, a settlement, or a filter over trades, events or deliveries, with `count`. Command steps can check
    their own response (`status`, `matches`).
  - References: `as: name` names an order, `name#i` is its `i`-th execution.
  - Every step runs even after a failure. The report lists each step with its failures; step 0 is the setup.
  - **Sandbox:** each run starts a fresh in-process `App` from the service's configuration, with a temporary
    database, a frozen virtual clock at `start`, deterministic IDs from `seed`, test control on and
    `simulationId` = the scenario name. Commands go through the router (`tower::oneshot`), so a scenario exercises
    the real API. The sandbox is removed afterwards.
  - **CLI:** `exchange-simulator scenario run <file>... [--json]`. Exit 0 if all pass, 1 if any fails, 2 for an
    unreadable or invalid file. `serve` (or no arguments) runs the service as before.
  - **API:** `POST /test/scenarios` (test control only): `200` with the report, `400 invalid_scenario` with the
    parse error and its line.
  - **`GET /test/deliveries`:** every webhook delivery, with its URL, event, status and attempts. It backs the
    `deliveries` expectation and is there for tests generally.
  - Examples in `scenarios/`: the Milestone 1 acceptance trade, T+2 settlement over a weekend with an external report,
    and trading rules (self-trade, market remainder, idempotent client order IDs, cancels, a halt).
  - A README section on scenarios.
- **Tests:**
  - Unit tests: parsing (the documented example; errors with location, unknown steps, typos, wrong expectation
    targets) and the matcher (paths, arrays, numbers by value).
  - `tests/scenarios.rs` (6):
    - Every example passes.
    - A deliberately wrong scenario reports exactly the expected failures per step, and the steps after them run.
    - The same scenario gives an identical report and the deterministic event ID twice.
    - The CLI's exit codes and its `--json` output.
    - The API gives the same report as the CLI runner, leaves the live service's state untouched, answers `400` to
      a bad scenario, and `404` with test control off.
    - Deliveries against a real local consumer: `DELIVERED` on the first attempt, `DEAD` after five for a failing
      one, and a failing `wait_ms: 100` expectation reported as such. The consumer got the deterministic event ID.
  - The suite was repeated five times without failure.
- **Mutation checks**, each made a test fail: random IDs in the sandbox; no wait on deliveries; missing fields
  ignored by the matcher; stopping at the first failure; CLI exit 0 on failure; the sandbox on the base database
  path; no default `instrumentId`.
- **Decisions:**
  - **`deliveries` expectations wait** (`wait_ms`, default 5000). Delivery runs on real time (2.1), so it is the
    only state that changes without a command. Every other expectation is checked once. `wait_ms` anywhere else is a
    parse error.
  - **Sandboxes, not the live service.** `POST /test/scenarios` never touches the running service's state, so it
    is safe to call against a shared test deployment, and API and CLI give identical reports. Running a scenario
    *against* the live state could come later with reset (3.4).
  - **The API answers `200` for a failed scenario.** The verdict is `passed` in the body; `4xx` is reserved for a
    scenario that cannot be read.
  - Matching is containment: named fields must be equal, others are ignored, arrays match element by element and in
    full length. Numbers compare by value.
  - `http-body-util` and `tower` moved from dev-dependencies to dependencies, since the runner drives the router
    in-process.
- **Deviations:**
  - The plan names the setup list `subscriptions`; the file uses `webhooks`, after the API resource.
  - Besides the plan's steps there are also `set_clock`, `report_settlement` and `set_instrument`, so the
    Milestone 2 behaviour can be scripted too.

**3.2 — random market generator ✅ (2026-10-15).**
- **Delivered:**
  - `market` module: `MarketConfig`, which `validate`s with errors naming the field, and a pure `generate()`
    iterator of `MarketCommand { at_micros, action: Submit | Cancel { submit_index } }`.
  - The distributions from plan §6:
    - Poisson arrivals in simulated time (`orders_per_second`; 0 puts every command at one instant).
    - Buy, market-order and cancellation probabilities. A cancel picks a random earlier limit order, never one
      twice.
    - Participants as a count (`P01`…) or a list.
    - Per instrument: an arrival `weight`, limit prices around `mid_ticks` with `width_ticks` and `skew_ticks`
      (buys below the mid, sells above), `uniform` or `normal`, and quantities as uniform whole lots.
  - **Its own RNG (SplitMix64).** It is written out in the module, so a dependency upgrade can never change a
    recorded stream. Probabilities always draw, so changing one does not shift the rest of the stream.
  - **Scenario `market` step.** It registers the market's participants (existing ones are fine), then sends each
    command in order, first setting the virtual clock to its arrival time (start + offset, so no drift builds up).
    Cancels address orders by their submit index, through the IDs returned. The seed defaults to the scenario's.
    Exchange rejections and refused cancels of orders that have meanwhile filled are summarized, not failures; any
    other refusal is a failure, with at most 10 reported. The step's summary (orders, rejections, executions,
    cancels, refusals, final clock) is part of the report.
  - **Event-log fingerprint in every scenario report:** the count and SHA-256 of `/api/events` as JSON lines.
    Equal fingerprints mean byte-identical logs. 3.5's acceptance can use it directly.
  - **The bench workload is now this generator,** with a configuration matching the old one. The stream differs
    from the hand-written one, but the shape is the same (0.89 executions and 3.27 events per order against about
    0.9 and 3.3). Noted in `bench/README.md`.
  - `scenarios/random-market.yaml`: 600 commands on two instruments (one normal, one with lots of 10), with cancels
    and simulated settlement with failures, then a week forward.
  - A README section on random markets.
- **Tests:**
  - `market` unit tests (6):
    - The same seed gives the same stream; another seed gives another.
    - The stream's first three commands are pinned. The first was recomputed independently in Python.
    - The draws follow the configured shares, bounds, lots and skew: every uniform tick is used, about 68 % of a
      normal falls within 1σ, the mean arrival gap is 1/rate, and the gaps' coefficient of variation is about 1.
    - Cancels name earlier limit orders, each once.
    - No rate means one instant; named participants work.
    - Invalid configurations name the field.
  - `tests/scenarios.rs` (+3):
    - The random market passes, and two runs give identical reports and event-log fingerprints. Another seed gives
      a different market that still passes.
    - The clock ends about two minutes in, as 600 commands at 5/s should.
    - The summary's execution count equals the trades and `TradeExecuted` events.
    - A market on an unknown instrument reports 10 failures plus a "further failures" line.
    - Two logs of equal length differing only in their last event get different fingerprints.
  - The scenario suite was repeated five times without failure.
- **Mutation checks**, each made a test fail:
  - generator: seed ignored; normal drawn as uniform; skew sign flipped; no cancellations; evenly spaced arrivals
    with the same draws;
  - runner: clock not moved; a refused cancel counted as a failure; the scenario seed not used as the default; a
    fingerprint over the first event only.
- **Decisions:**
  - **The generator does not react to the exchange.** The stream depends only on the configuration, which is what
    makes a market reproducible from its seed. A cancel can therefore name an order that has meanwhile filled, and
    the exchange refuses it (counted as refused).
  - **Prices move around a fixed mid.** There is no drift or random walk of the mid yet; it can be added as a price
    option later.
  - **No market endpoint of its own.** Markets run as scenario steps, so they come through both the CLI and
    `POST /test/scenarios`. A market against the live service waits for reset (3.4), if it is wanted at all.
- **Deviation:** the plan's example config has `instruments: 100` as a count. Here instruments are listed (each
  needs its registration and distributions), so many instruments mean a long list.

**3.3 — delivery fault policies ✅ (2026-10-15).**
- **Delivered:**
  - `webhooks::faults`: `FaultPolicy` with `missing`, `nulls` and `invalid` (listed JSON paths, or one random leaf
    field), `duplicateProbability`, `delay` and `outOfOrder`, each with a probability. `validate` errors name the
    field. `plan()` turns a policy into a `FaultPlan` for one attempt.
  - **Reproducible decisions.** Each is drawn from SplitMix64 seeded by SHA-256(policy seed, delivery ID, attempt).
    With deterministic IDs the same deliveries get the same faults on every run and across restarts. Every draw is
    always made, so adding one fault leaves the others' decisions unchanged.
  - **Worker:**
    - a hold returns a new `DeliveryOutcome::Held { at }`;
    - a delay sleeps in the request's slot;
    - the damaged body (or the canonical one) is serialized once, signed and sent, and sent twice for a duplicate;
    - the first response decides the outcome;
    - headers stay canonical.
  - **Migration 009:**
    - `webhook_subscriptions.fault_policy` (JSON) holds the policy;
    - `webhook_deliveries.held` marks a delivery as held once, so it is never held again;
    - the new `webhook_attempts` table has one row per request sent (a duplicate adds one) and one per hold, with
      the attempt number, outcome (`SENT`/`HELD`), faults applied, the body when a fault changed it, HTTP status,
      error and time.
  - **A hold uses no attempt.** It sets the delivery back to `PENDING` with `next_attempt_at` = now + `holdMs`, so
    later events are claimed and overtake it. Rows are written in `record_deliveries`, in the same transaction as the
    outcome.
  - **Test control:**
    - `PUT`/`DELETE /test/webhooks/{id}/faults` (through the exchange thread; `404` for an unknown subscription);
    - `GET /test/attempts?subscriptionId=`;
    - `GET /api/webhooks` shows a subscription's policy when it has one.
  - **Scenarios:**
    - a `faults` key on webhook entries;
    - a `set_faults: { url, faults }` step (`null` clears);
    - an `attempts` expectation, which waits like `deliveries`;
    - `all: true` on list expectations: every item matches, and there is at least one. It is used to wait for the
      outbox to drain.
  - A README section on fault injection.
- **Tests:**
  - `faults` unit tests (5):
    - Listed paths are removed, nulled or made invalid, and missing paths are skipped.
    - Decisions are reproducible per delivery and attempt, change with the seed and on a retry, and fire at the
      configured rates.
    - A random field fault removes exactly one leaf.
    - A hold sends nothing and happens once.
    - Adding a fault does not change the others.
    - Invalid policies name the field.
  - `tests/faults.rs` (6), against a real receiver:
    - **Field faults:** the received body is exactly the configured damage, while `/api/events` keeps the canonical
      event. The signature covers the damaged bytes, and `/test/attempts` records the faults and the same body.
    - **Duplicates and delays:** two identical requests as one attempt, at least 300 ms late, with no body stored.
      The delivery is `DELIVERED` after 1 attempt.
    - **Holds:** a held trade is overtaken by a later one, and the hold uses no attempt.
    - **Restarts:** the policy survives a restart, and a held delivery is not held again after one.
    - **Validation:** invalid policies, unknown fields, `outOfOrder` on an ordered subscription, an unknown
      subscription, and test control off are all refused.
    - **Reproducibility:** a scenario with a random market and random faults, run twice, sends byte-identical copies,
      damage and duplicates included.
  - `tests/scenarios.rs` (+1): `all` fails on an empty list and on a partial match.
  - The fault, scenario and webhook suites were repeated five times without failure.
- **Mutation checks**, each made a test fail:
  - the canonical body sent instead of the damaged one;
  - the hold not remembered;
  - a hold counted as an attempt;
  - no duplicate;
  - no delay;
  - the fault generator ignoring the delivery ID;
  - holds allowed on ordered subscriptions;
  - the signature over the canonical body;
  - only faulted attempts recorded;
  - `all` passing on an empty list.
- **Decisions:**
  - **Faults are decided per attempt.** A retry draws anew, as a flaky network would.
  - **Every attempt is recorded,** faults or not, but the body only when a fault changed it. That makes
    `webhook_attempts` the record of what was sent. It costs one more row per attempt in the transaction that
    already records the outcome; the cost on webhook throughput is not yet measured (4.1/4.6).
  - **A duplicate is the same attempt,** with identical bytes and headers. The first response decides the outcome.
  - **A signed subscription signs the damaged bytes.** The consumer then sees a valid signature over a faulty event,
    which tests payload validation rather than signature checks. A forged-signature fault could come later.
  - **A delay sleeps in place.** It holds up the worker's batch, like a slow network, so it is capped at 10 s, well
    inside the claim lease. Holds are the way to reorder.
  - **Holds are refused on ordered subscriptions,** since an ordered subscription by definition never reorders.
- **Deviation:** the plan says "configured under `/test/…` or in a scenario". Both are there, but the policy lives
  on the subscription itself rather than in a separate test-control store. That way it survives restarts and is
  visible in `GET /api/webhooks`.

**3.4 — reset and replay controls ✅ (2026-10-16).**
- **Delivered:**
  - **`POST /test/reset`** (`204`) runs `Exchange::reset` on the exchange thread.
    - `Store::reset` empties every table found in `sqlite_master`, so tables added later are covered too, plus
      `sqlite_sequence`. It does this in one transaction with deferred foreign-key checks.
    - The in-memory state is then as on an empty database: no books, participants or subscriptions, sequences from
      1, and a fresh settlement simulator with the same policy.
    - The virtual clock goes back to its first reading, through a new `VirtualClock::reset`, the one way it can move
      backwards.
  - **Reset epoch:** a counter that each reset bumps. Delivery jobs and results carry it, and `record_deliveries`
    drops results claimed before a reset. Without it, a late result could land on a new delivery with the same
    deterministic ID, and its attempt row would fail the foreign-key check.
  - **`POST /test/webhooks/{id}/redeliver?after=`** upserts a pending delivery, with no attempts and not held, for
    every event after `after` of the types the subscription wants. Events from before the subscription existed are
    included, so this also replays history to a new consumer. It returns `{ queued }` and wakes the worker.
  - **Dead letters:**
    - `GET /test/deliveries` gains `subscriptionId` and `status` filters (`status=DEAD` lists the dead letters).
    - `POST /test/dead-letters/retry` with an optional `{ subscriptionId, deliveryIds }` resets dead deliveries to
      pending with a fresh attempt budget and returns `{ retried }`.
  - **Scenario steps:** `reset` (it also forgets order and webhook references), `register` (instruments,
    participants and webhooks mid-scenario, as in the setup), `redeliver` and `retry_dead_letters`.
  - A README section on reset and redelivery.
- **Tests:** `tests/reset.rs` (5):
  - **Reset:** after a script touching every kind of state (registration, a webhook, a trade, settlement at T+2, an
    external report), a reset empties every table. The APIs show nothing and the clock is back at its start. Running
    the same script again gives a byte-identical `/api/events`, and a restart afterwards keeps it.
  - **Stale results:** a delivery in flight during a reset reaches the consumer, but its result is dropped: only the
    new attempt is recorded, and the new delivery shows 1 attempt.
  - **Redelivery:** it covers events from before the subscription and goes out in order. Repeating it from a
    sequence re-sends only the later events, with fresh attempt counts. A sequence at the end queues nothing. An
    unknown subscription is `404`, and a bad `after` is `400`.
  - **Dead letters:** they are listed per status and subscription and retried per subscription, per ID or all.
    Others stay dead. A bad status is `400`, an unknown subscription `404`, unknown fields `400`, and with test
    control off it is `404`.
  - **Scenarios:** one that resets halfway and repeats its first half ends with the same event-log fingerprint as
    one that ran the first half only. References are forgotten on reset.
  - The reset, fault and webhook suites were repeated five times without failure.
- **Mutation checks**, each made a test fail:
  - stale results kept;
  - the epoch not advanced;
  - the autoincrement counter not reset;
  - the clock not reset;
  - the settlement simulator kept;
  - the books kept;
  - redelivery keeping attempts;
  - redelivery including `after` itself;
  - retry touching more than dead letters;
  - retry ignoring the subscription;
  - scenario references kept after a reset.
- **Decisions:**
  - **A reset clears everything except the configuration.** Subscriptions and their fault policies go too, and the
    clock goes back to its first reading. Metrics counters are not reset.
  - **A reset does not interrupt a request in flight,** so the consumer may still get it. Only its result is
    discarded.
  - **Redelivery is a fresh delivery** (attempts 0, not held). The old attempts stay in `webhook_attempts`, ordered
    by their ID.
  - **No new migration** was needed: the epoch lives in memory, and a restart cannot have claims from before a
    reset, since startup releases all claims.
- **Note:** this time the e2e suite took 96 s (17 s before). The result was unchanged; the extra time was most likely
  the image build.

**3.5 — tests and acceptance ✅ (2026-10-16).**
- **Delivered:**
  - **`tests/milestone3.rs`**, the acceptance in process:
    - **The scenario suite:** every file in `scenarios/` is run twice through the CLI, as separate processes, with
      `--json`. All pass, and the two runs give identical reports, event-log fingerprints included.
    - **Faults:** seven subscriptions on one deterministic service: clean, `missing`, `nulls`, `invalid`,
      duplicate, delay and out-of-order, all fed the same two trades.
      - Each fault is visible at a real receiver: only the price differs for the field faults; the duplicate
        subscription gets each trade twice; delayed copies arrive at least 300 ms after the clean ones; and the
        held first trade arrives after the second.
      - The clean subscription gets the canonical events.
      - `/api/events` is byte-identical to a reference run with no subscriptions at all, so the faults are absent
        from the log.
    - **Reset:** it empties the tables, and the same commands then give the reference log again.
  - **`scenarios/faults-and-reset.yaml`:** a self-contained fault and reset scenario. Its consumer is unreachable,
    and it checks the damaged copies in `/test/attempts` against the intact canonical event, then that a reset
    leaves no events, trades, deliveries or attempts.
  - **`tests/e2e.rs` `milestone_3_scenarios_in_containers`:**
    - The image's CLI runs the suite twice, from a read-only mount with `--network none`.
    - A running container runs each file through `POST /test/scenarios`.
    - Every report equals the local build's, and the container's own state stays empty.
  - **The scenario CLI logs to stderr.** Its stdout carries the reports, so `--json` output stays parseable at any
    `RUST_LOG`. The service still logs to stdout, which the container tests wait on.
  - README: Testing covers Milestone 3; a fault-injection note explains how delays and holds interact.
- **Tests:**
  - The in-process tests above, plus the e2e test.
  - `tests/scenarios.rs` now names the Milestone 1 file instead of taking the alphabetically first one.
  - The acceptance and scenario suites were repeated five times without failure.
- **Mutation checks**, each made an acceptance test fail: random IDs in the sandbox; no delay; no holds; invalid
  values written as null; a reset that keeps events.
- **Finding:** a delay holds up its whole delivery batch, and with it the recording of a hold in the same batch.
  Since a hold counts from its decision, a hold no longer than a concurrent delay has no effect (seen in the
  acceptance test with 400 ms each). This is documented: make `holdMs` longer than any delay. Running delays outside
  the batch could be revisited with the group-commit work in 4.2.
- **Deviation, CI not enabled yet (decision of 2026-10-16):** the repository has no CI. A workflow was drafted but
  left out on request. Until CI is enabled, the suite runs locally through `cargo test` (`tests/milestone3.rs`) and
  the e2e tests. A later workflow should run the step gates, `cargo test`, the scenario suite through the CLI, and
  the e2e tests.

### Milestone 3 status

Steps 3.0–3.5 are done.
- **Acceptance met** (plan: "the same file and seed give byte-identical event logs over two runs. Each fault type is
  visible at the receiver and absent from `/api/events`. Reset gives a clean state"). It is checked in process and in
  containers. CI is not enabled yet, by decision.
- **Open for later:**
  - Enabling CI for the scenario suite and the gates.
  - A price drift (random walk) for random markets.
  - A forged-signature fault.
  - The cost of recording every attempt, to be measured in 4.1 and 4.6.
  - Delays that do not hold up their batch.

## Milestone 4 — performance

**Goal:** measured, explained throughput against 1k–10k orders/s, with durable mode and an explicit non-durable mode.
The baseline says the per-order fsync is the limit (≈4–5k/s on the devcontainer disk; matching runs at 2.9M/s). So
the order below deliberately puts group commit before sharding.

| # | Step | Size | Delivers |
|---|---|---|---|
| 4.1 ✅ | Latency breakdown and an open-loop load generator | M | New histograms for queue wait (enqueue → start) and acknowledgement latency (enqueue → reply). The bench gains a target-rate, open-loop mode that avoids coordinated omission, reporting achieved rate, latency at that rate, and 503 counts. |
| 4.2 ✅ | Group commit | M–L | The writer drains up to N queued commands (or waits up to T µs) and plans them in order on the books. It commits them in one transaction and replies to each after the commit. If the commit fails, all plans are dropped and every caller gets the error; nothing is applied. Ordering and determinism are unchanged. Expected: several times today's durable throughput. |
| 4.3 ✅ | Background checkpointing | S | Turn off automatic WAL checkpointing on the writer *only together with* a background connection that runs `PRAGMA wal_checkpoint(PASSIVE)` when the WAL passes a size threshold. Without that, the WAL grows without bound and throughput halves (1.2). Target: the roughly 10 % of durable time and the p99 now spent in checkpoint commits. The occasional > 10 ms spikes come from the disk (1.2), and this will not remove them. Measure with `bench checkpoint`. |
| 4.4 ✅ | Explicit non-durable modes | S | `persistence.mode: durable \| fast \| memory` (D4).<br>• **`fast`**: WAL with `synchronous=NORMAL`. Every commit still reaches the database file, and fsync happens only at checkpoints. All state survives a process or container restart (books, events, outbox, settlement instructions, simulator cursor, virtual clock), so settlement keeps working across restarts. Only a host or node crash, or power loss, can lose the last commits, and the database is never corrupted. (`synchronous=OFF` was rejected: it can corrupt the database on a host crash.)<br>• **`memory`**: an in-memory SQLite database for pure throughput runs. Nothing survives a restart (orders, trades, pending settlements, undelivered webhooks), so settlement works only while the process runs.<br>`/health` and `/metrics` report the mode, and responses carry a header in non-durable modes. The README documents the trade-offs, including that durability in Kubernetes also needs a persistent volume (`emptyDir` loses everything when the pod is rescheduled, even in `durable` mode). |
| 4.5 ⏭ | Instrument-sharded matching (only if 4.1 shows matching as a bottleneck) | M | **Skipped (D7).** Matching workers per instrument shard in front of the single writer. Measured in 4.1–4.4: matching takes about 0.5 µs of the exchange thread's roughly 46 µs per order; the rest is SQLite writes. Revisit only if a later profile shows matching as the limit. |
| 4.6 | Containerized benchmark and load report | M | There is no single target machine (D4): deployments are mostly Docker Compose or Kubernetes, and their hardware varies. So the benchmark runs as containers: the service image plus a load-generator container, with explicit CPU and memory limits and a named storage backing (volume or storage class). It ships as a compose profile and as a Kubernetes Job manifest. Each report records its environment: limits, storage, measured fsync latency, and node type where known. It compares `durable`, `fast` and `memory`, with and without outbox and webhooks, across instrument counts. Figures against the 1k–10k target are quoted per environment, never as one universal number. Extended by D7 into the sub-steps below; Docker Compose comes first. |
| 4.6a ✅ | Webhook subscriptions declared in config | M | `webhooks.subscriptions`: a list of `{ name, url, event_types, secret \| secret_env \| secret_file, ordered, enabled }`. At startup, and after `/test/reset`, the service reconciles them by `name`: creates the missing ones and updates changed ones. Ones removed from the config are disabled, not deleted, so their history stays. Config-declared subscriptions are marked `source: config` and are read-only through the API; API-created ones work as before. A migration adds `name` (unique) and `source`. |
| 4.6b ✅ | Webhook maintenance API | M | Under `/api/webhooks/{id}`: `GET` with delivery stats (pending, in flight, delivered, dead, oldest pending); `PATCH` (url, eventTypes, ordered, enabled); `DELETE` (pending deliveries are dropped, history kept); `POST …/secret` to rotate the secret, with a grace period during which both signatures are sent; `GET …/deliveries?status=` for deliveries and dead letters with their last error; `POST …/redeliver?after=` and `POST …/dead-letters/retry`. The `/test` equivalents stay as aliases. |
| 4.6c ✅ | Usage guide and Compose setup | M | A `docs/` guide for operators and client developers: Compose quick start, configuration reference, writing a webhook receiver (signature check, idempotency by `eventId`, ordering, retries, dead letters, secret rotation), maintenance and troubleshooting, and benchmarking. A runnable Compose example with a sample receiver and config-declared subscriptions. The README stays the API reference and links to the guide. |
| 4.6d | Containerized benchmark and load report | M | As described for 4.6 above: the bench in a container beside the service, with CPU and memory limits and a named volume, as a Compose profile first and a Kubernetes Job manifest after. The report takes its environment from the container's cgroup limits (added in 4.4), and runs across modes, webhooks on and off, and instrument counts. |

### Milestone 4 progress

**4.1 — latency breakdown and an open-loop load generator ✅ (2026-10-16).**
- **Delivered:**
  - **Two new histograms,** labelled by `command`:
    - `exchange_command_queue_wait_seconds`: enqueue to start on the exchange thread.
    - `exchange_command_ack_seconds`: enqueue until the caller has the answer.
    - Each queued command now carries its enqueue `Instant`. The command name comes from the request itself
      (`Request::name`), so the caller can label the acknowledgement before the exchange thread sees the request.
    - The latency buckets now apply to every `_seconds` histogram, not just `*duration_seconds`.
  - **`bench open-loop`:**
    - For each target rate in `--rates` it runs for `--seconds`. Order `i` is due at `start + i / rate` and is sent
      then, whatever the earlier responses. Latency is measured from the due time, so coordinated omission is
      avoided.
    - Each row reports: the achieved rate, the latency of accepted orders, 503s and other failures, the
      generator's own lag, and the server-side breakdown per order (mean and p99 bucket bound of queue wait,
      exchange-thread time, commit and acknowledgement), taken from `/metrics` before and after the run.
    - New flags: `--queue-capacity` and `--max-in-flight`.
  - **The closed-loop `http` rows** carry the same breakdown. Their service setup is shared with `open-loop`
    (`Service`).
  - **Docs:** README metrics list; `bench/README.md` gains an open-loop section with results in
    `bench/results/2026-10-16-open-loop-*.md`.
- **Results (devcontainer):**
  - **VM disk, fsync about 300 µs:** 2,000/s held at p99 7 ms; saturated at about 3,850/s.
  - **tmpfs:** 15,000/s held at p99 25 ms; at 20,000/s about 15,050/s accepted and 39,010 of 200,000 refused with
    503.
  - **Breakdown:** below capacity the service accounts for under 1 ms of the 1–2 ms p50. At capacity the
    exchange thread still takes about 250 µs per order (commit 230 µs), and the queue wait grows to hundreds of
    milliseconds. The commit is the limit, which supports doing group commit (4.2) before sharding (4.5).
- **Tests:**
  - A dispatcher unit test: a request held 100 ms in the queue records exactly one queue wait of at least 100 ms
    and one acknowledgement, which covers the wait plus the execution.
  - The metrics integration test (`tests/webhooks.rs`) checks that both histograms are exposed with HELP text and
    latency buckets.
  - The dispatcher tests were repeated five times without failure. All gates pass, e2e included.
- **Mutation checks**, each made a test fail:
  - queue wait measured from the start of execution;
  - queue wait recorded without its `command` label;
  - acknowledgement not recorded;
  - acknowledgement shortened, so it no longer covers wait plus execution;
  - latency buckets only for `*duration_seconds` again.
- **Decisions:**
  - **The acknowledgement is measured at the caller,** when the answer arrives, so it includes the hand-back
    from the exchange thread. A caller that gives up (for example when the HTTP client disconnects) records none.
    A command refused with 503 records neither histogram; `exchange_command_queue_rejections_total` counts it.
  - **`open-loop` is not part of `all`,** like `checkpoint`: it runs 10 s per rate.
  - **At `--max-in-flight` the generator waits** rather than dropping orders. The wait counts as latency, so
    results stay honest, and `generator lag` shows it.
- **Deviation:** the generator still runs in the service's process and Tokio runtime, as the `http` rows did.
  Step 4.6 moves it into its own container.
- **Finding:** the VM disk is slower than on 2026-10-14: fsync took 300–500 µs, against 200 µs then. The first run
  on a cold disk also stalled the whole process (p99 452 ms at 500/s, against 4.4 ms on the rerun). Compare figures
  only within one report.

**4.2 — group commit ✅ (2026-10-16).**
- **Delivered:**
  - **`Exchange::begin_group` / `commit_group`.**
    - In a group, each order or cancellation is planned on the books, written, and applied to memory in turn, so the
      next command sees it.
    - Its writes run in a SAVEPOINT inside the group's `BEGIN IMMEDIATE` transaction. `Store::commit` now always
      uses a savepoint; on its own that is one transaction, exactly as before.
    - The group becomes durable at one `COMMIT`. Metrics and the delivery worker's wake-up are deferred until then.
  - **If the `COMMIT` fails:**
    - SQLite rolls the group back.
    - `Exchange::reload` rebuilds the books, both sequences and the settlement simulator from storage.
    - `ExchangeError::GroupCommit` tells every caller in the group that nothing was stored (HTTP 500).
    - If even the reload fails, the exchange thread panics rather than run with memory and storage apart.
  - **A failed statement inside a group** rolls back to that command's savepoint, so only that command fails, as
    without groups.
    - If SQLite instead rolled back the whole transaction (as it can on disk full or I/O errors), the next command
      refuses to write, so it does not commit on its own. The group's `COMMIT` then fails and reloads.
  - **The dispatcher (`GroupCommit`):**
    - After taking an order or a cancellation, the thread keeps taking queued orders and cancellations, up to
      `max_commands`. While the queue is empty it waits until `max_wait` after the group opened.
    - Any other request ends the group, then runs: a snapshot, a registration, a delivery claim.
    - Answers are held until the commit. A failed commit answers every caller with the error.
    - `max_commands: 1` keeps the old path, with no group at all.
  - **Config:** `exchange.group_commit: { max_commands: 256, max_wait_us: 0 }`, so it is on by default.
  - **Metrics:**
    - `exchange_commit_duration_seconds` is now per commit group (the `COMMIT`).
    - New: `exchange_group_commit_commands`, the commands per group.
    - Per command, `exchange_command_duration_seconds` covers planning and writing, but not the shared commit.
  - **Bench:**
    - `--group-commit N` and `--group-wait-us N`.
    - Grouped durable rows (the `Exchange` group API, groups of N).
    - The breakdown shows the commits and the average group size.
  - **Docs:** README (config, model, metrics); `bench/README.md` gains a group-commit section; results are in
    `bench/results/2026-10-16-group-commit-*.md`.
- **Results (devcontainer):**
  - **VM disk (fsync 208–266 µs):**
    - The open loop holds 12,000 orders/s at p99 11 ms and saturates at about 15,600/s. With group commit off, the
      same day saturated at 3,725/s.
    - HTTP with 32 clients: 13,060/s (4,850/s on 2026-10-14). With a webhook consumer: 9,650/s.
    - `Exchange` alone in groups of 256: 20,700/s, against 2,200–2,800/s.
  - **Groups follow the load:** 3.5 orders at 2,000/s, 19 at 12,000/s, and 92 at saturation. Latency at low load is
    unchanged.
  - **tmpfs:** saturation at about 19,000/s. The exchange thread then spends about 46 µs per order on planning and
    SQL writes, and the commit about 4 µs.
  - **Against the plan's expectation** ("several times today's durable throughput"): met, 4–9×, depending on the row.
- **Tests:**
  - **`tests/recovery.rs` (3 new):**
    - A group is invisible to another connection until its commit, and each command sees the one before it (a trade
      against an order in the same group, then cancelling it). There are no event-sequence gaps, and a restart
      rebuilds the same book.
    - A failed group `COMMIT`, forced through a deferred foreign-key violation that a temp trigger plants, changes
      neither memory nor storage. This is with the settlement simulator on. A retry gets the same sequences, and the
      trade is taken into settlement exactly once.
    - A failed write inside a group fails only that command. It consumes no sequence, and the others commit.
  - **Dispatcher unit tests (3 new), with commands queued before the thread starts:**
    - 330 orders and cancellations give identical answers and a byte-identical event log with grouping off and in
      groups of 64. A commit hook counts 6 commits against one per stored command.
    - With a commit hook that delays each commit by 30 ms, every caller finds its order stored when it hears back.
    - A failing group answers both of its callers with `GroupCommit`. A snapshot queued behind them ended the group
      and sees an empty book, and the next order gets sequence 1.
  - The metrics integration test checks `exchange_group_commit_commands`.
  - The existing suites (reproducibility, scenarios, faults, reset, e2e) pass with group commit on by default.
  - The dispatcher, recovery, reproducibility, Milestone 3, webhook and reset suites were repeated five times
    without failure.
- **Mutation checks**, each made a test fail:
  - answering before the commit;
  - a failed group answering with each command's own result;
  - a snapshot run inside the group;
  - `max_commands` ignored;
  - no reload after a failed commit;
  - a reload that keeps the last sequence;
  - a reload that keeps the settlement simulator;
  - `Store::commit` opening a transaction instead of a savepoint.

  A no-op control mutation passed.
- **Decisions:**
  - **Memory runs ahead of storage inside a group.** It is never observable: answers are held, and every request
    that reads or changes other state ends the group first. A failed commit reloads memory from storage, rather than
    undoing book changes, which could not restore FIFO positions. A reload is rare and costs a book rebuild.
  - **Only orders and cancellations are grouped.** They are the hot path; everything else keeps its own transaction.
  - **`max_wait_us` defaults to 0.** A group takes what is already queued and never waits, so latency at low load does
    not change. A wait would only help when commands arrive just after a commit, and it adds that wait to every
    group's first order.
  - **Default `max_commands` is 256.** At saturation groups reached 92 on the VM disk and about 253 on tmpfs, close
    to the cap. A larger cap mainly lengthens the latency of the orders at the front of a group.
- **Finding for 4.5 (sharded matching):** with the fsync amortized, the exchange thread's limit is about 46 µs of
  work per order, mostly SQLite statements (order, executions, about 3.3 events with JSON payloads, outbox rows).
  Matching is about 0.5 µs. Sharding matching would not raise throughput; cheaper writes would. 4.5 stays
  measure-first, and the measurement now argues against it.

**4.3 — background checkpointing ✅ (2026-10-16).**
- **Delivered:**
  - **`persistence/checkpoint.rs`:**
    - The writer registers a WAL hook that only records the WAL size (in a thread-local, since rusqlite's hook is a
      plain `fn`). Registering it turns off SQLite's automatic checkpoint on the writer.
    - A `checkpoint` thread with its own connection runs `PRAGMA wal_checkpoint(PASSIVE)` when asked. Requests made
      during a run coalesce into one more.
  - **`Store::checkpoint_in_background(busy_timeout, pages)` and `Store::request_checkpoint_if_due()`.** The exchange
    thread calls the latter after every request or commit group.
    - It asks for a background checkpoint each time the WAL has grown by `pages`.
    - At 4× `pages` the writer checkpoints itself, once a background checkpoint on the current WAL has left at most
      `pages / 4` to copy, or regardless at 8×. A generation counter makes the writer ignore background results from
      before a WAL restart.
    - Dropping the store stops the thread after its current checkpoint. The writer closes first, so the checkpointer's
      connection, the last one, removes the WAL.
  - **Config:** `database.checkpoint: background | inline` (default `background`) and `database.checkpoint_pages`
    (default 1000). A database without a file keeps the automatic checkpoint.
  - **Metrics:** `exchange_wal_checkpoints_total{by,outcome}`, `exchange_wal_checkpoint_duration_seconds{by}` and
    `exchange_wal_frames`.
  - **Bench:** `--checkpoint background|inline`. `bench checkpoint` gains the background row, with counts of
    background and writer checkpoints, their duration and the WAL size.
  - **Docs:** README (config, model, metrics); a `bench/README.md` section; results in
    `bench/results/2026-10-16-background-checkpoint-vm-disk.md` and
    `bench/results/2026-10-16-checkpoint-*-open-loop-vm-disk.md`.
- **Results (VM disk, fsync about 200 µs):**
  - **Per-order commits** (`bench checkpoint`, 50k orders, three runs):
    - Background: p99 0.45–0.51 ms against 1.38–1.51 ms inline, about 3× lower.
    - p50 rises by 20–25 µs, throughput falls 6–8 %, and p99.9 goes from 2.1–2.2 to 2.7–2.9 ms.
    - The WAL stays at 16 MiB, against 4 MiB inline.
  - **The service with group commit (open loop):**
    - 12,000/s: p99 8.0–9.3 ms against 11.6–14.0 ms inline.
    - 16,000/s: p50 about 4.1 ms against 6.8–9.5 ms, and p99 17–20 ms against 22–70 ms.
  - Spikes above 10 ms remain, from the disk, as 1.2 predicted.
- **Tests:**
  - **`tests/checkpoint.rs` (4):**
    - With background checkpoints from 100 pages, 400 orders leave the WAL under 2.5 MiB, checkpoints complete, and
      a restart sees every order and execution.
    - With a threshold that is never reached, the WAL grows past 8 MiB, so the writer does not checkpoint by itself.
    - Inline mode stays under 6 MiB.
    - Through the service, the dispatcher's requests keep the WAL under 2.5 MiB.
    - The bounds sit between measured sizes: 0.5–1.2 MB background, 4.2 MB inline, 19 MB with no checkpoint.
  - **Unit tests (3):**
    - With the background thread stopped, the writer still bounds the WAL at its cap.
    - 2000 writes cause at most 100 background checkpoints (917 without the throttle).
    - The writer's own checkpoint triggers when little is left to copy, never on a result from an earlier WAL, and
      always at the cap.
  - All suites pass with background checkpointing on by default. The checkpoint, recovery, reproducibility,
    Milestone 3, webhook, reset and library suites were repeated five times without failure, and the checkpoint
    suite 40 more times after the fix below.
- **Mutation checks**, each made a test fail:
  - the hook not registered (the writer auto-checkpoints);
  - requests ignored;
  - `complete` reported as `partial`;
  - the dispatcher never requesting;
  - the service ignoring `background`;
  - no throttle;
  - no writer checkpoint;
  - the writer only at the cap;
  - no cap;
  - the generation ignored;
  - a lenient remainder.
- **Found and fixed along the way:**
  - **The WAL rarely restarts by itself under load.** A restart needs a checkpoint to have copied every frame before
    the next commit. Under constant commits a background checkpoint loses that race, and the first version's
    checkpoint test failed once in 55 runs with a 3.3 MB WAL. Hence the writer's own checkpoint.
  - **Asking after every commit past the threshold** kept the background thread checkpointing nonstop: 36,500
    checkpoints per 50k orders, with p50 up about 40 %. Asking once per threshold of growth brought that down to
    about 660.
- **Decisions:**
  - **`background` is the default,** since the service runs with group commit, where it clearly wins. Per-order
    commits trade 6–8 % throughput and p99.9 for a 3× lower p99; `inline` remains for that case.
  - **The writer's own checkpoint is the price of a bounded WAL.** It runs about once per 4000 pages, and since the
    background has already copied most frames, it copies little. Its fsync of the database file is what remains in
    p99.9. A `RESTART` or `TRUNCATE` checkpoint from the background connection would block the writer anyway.
  - **WAL size:** about 16 MiB with the defaults, against 4 MiB inline. The plan's concern was unbounded growth
    (1.2: 2.2 GiB per 50k orders with checkpoints off). It is bounded, and the cap is twice that size.

**4.4 — explicit non-durable modes ✅ (2026-10-16).**
- **Delivered:**
  - **`database.mode: durable | fast | memory`** (`persistence::PersistenceMode`, default `durable`), opened by
    `persistence::open_mode`:
    - `durable`: WAL, with `synchronous=FULL` now set explicitly rather than inherited from SQLite's default.
    - `fast`: WAL with `synchronous=NORMAL`.
    - `memory`: SQLite's `memdb` VFS under a fresh name per start (`persistence::memory_location`), with
      `synchronous=OFF`. The writer, the API reader and test helpers share it, and it disappears with the last
      connection. Background checkpointing is skipped.
  - **`database.memory_max_mib`** (default 4096) replaces `memdb`'s 1 GiB cap through
    `sqlite3_file_control(SQLITE_FCNTL_SIZE_LIMIT)`. This is the crate's first `unsafe` block (see Decisions).
  - **Reporting:**
    - `GET /health` includes `persistence`.
    - `/metrics` has `exchange_persistence_mode{mode}`, 1 for the active mode and 0 for the others.
    - In `fast` and `memory` mode every response, test-control and 404s included, carries
      `Exchange-Persistence: <mode>`.
    - A warning is logged at startup in non-durable modes.
  - **`App::database_location()`:** the file path, or the in-memory URI, for tools and tests that read the database
    directly.
  - **Bench:**
    - `--persistence durable|fast|memory`, named in the report header.
    - Non-durable rows are prefixed with their mode.
    - The per-order rows report the database size.
  - **Docs:** a README section on persistence modes (trade-offs, RAM sizing, and Kubernetes `emptyDir` and Docker
    volumes); a `bench/README.md` section; results in `bench/results/2026-10-16-persistence-*.md`.
- **Results (VM disk):**
  - **`Exchange`, one commit per order:** durable 2,380/s, fast 7,210/s, memory 26,400/s.
  - **Groups of 256:** 22,900, 23,500 and 34,500/s.
  - **HTTP, 32 clients:** 13,950, 16,770 and 22,380/s.
  - **Open-loop saturation:** about 16,400, 18,600 and 25,000/s.
  - **Database size:** 2.6 KiB per order (3.7 KiB with an outbox subscriber to every event type).
- **Tests:**
  - **`tests/persistence_modes.rs` (5):**
    - Each mode opens with its promised journal and sync settings.
    - The in-memory database is shared with a read-only reader, gone after the last connection, separate per
      location, and writes no files.
    - The size cap holds at 4 MiB, and a larger configured cap replaces SQLite's default.
    - Durable responses carry no header.
    - `fast` mode reports itself in `/health` and in a header on every kind of response. Trades, the book and the
      event log are identical after a restart.
    - `memory` mode trades and settles at T+2 under the virtual clock while running, writes no file, and is empty
      after a restart.
  - **`tests/persistence_metrics.rs` (1),** in its own binary because the gauge is process-wide: the gauge is right
    in each mode.
  - The existing `/health` test expects `persistence: durable`.
  - The two new suites were repeated five times without failure.
- **Mutation checks**, each made a test fail:
  - `fast` keeping `FULL`;
  - no memory cap;
  - one shared memory name;
  - no header;
  - a header in durable mode too;
  - the gauge set before the recorder;
  - `/health` always durable;
  - `memory` mode using the file path.
- **Found and fixed:** the mode gauge was set before `App::start` installed the metrics recorder, so the first
  service in a process lost it. The recorder is now installed first. The isolated gauge test caught this, and its
  mutation check confirms it.
- **Decisions:**
  - **The key is `database.mode`, not the plan's `persistence.mode` (deviation).** It sits beside `database.path`
    and the checkpoint settings it governs.
  - **`memory` uses SQLite's `memdb` VFS, not `:memory:`.** `:memory:` gives each connection its own database, and
    the service has three connections. `memdb` has no WAL, so reads and the writer take turns, which is documented.
  - **`unsafe` for the size limit.** rusqlite exposes no safe call for `SQLITE_FCNTL_SIZE_LIMIT` (its own code has
    it as a TODO). The alternative, a compile-time `SQLITE_MEMDB_DEFAULT_MAXSIZE` through `LIBSQLITE3_FLAGS`, would
    depend silently on the build environment. The block is a few lines, has a `SAFETY` comment, and runs once per
    start.
  - **The header is `Exchange-Persistence`.** It is on every response, so a client sees it whichever endpoint it
    uses.
- **Finding, memory limits:** one `memory` run at 30,000/s collapsed (2,170/s, p99 87 s). The devcontainer is
  limited to **8 GiB** (`docker stats` showed 98 %), about 4 GiB of it held at rest by rust-analyzer, the VS Code
  server and dockerd. `/proc/meminfo` shows the VM's 15.6 GiB instead, so the bench header understated the pressure.
  The header now also reports the cgroup memory and CPU limits, which 4.6 needs anyway. Reruns with rust-analyzer
  idle saturated normally.
- **Finding:** with group commit, `fast` gains little over `durable` at the service level (about 15 % at
  saturation). `memory` is what lifts the ceiling, to about 25k/s. The next limit is the exchange thread's
  per-order work and the in-process HTTP stack (4.2, 4.6).

**4.5 — skipped (D7, 2026-10-16).** 4.1–4.4 measured matching at about 0.5 µs of the exchange thread's roughly 46 µs
per order. Sharding it would not raise throughput.

**4.6a — webhook subscriptions declared in config ✅ (2026-10-16).**
- **Delivered:**
  - **Config:** `webhooks.subscriptions`, a list of `{ name, url, event_types, secret | secret_env | secret_file,
    ordered, enabled, paused }` (`config::SubscriptionConfig`). Unknown keys are refused.
    `SubscriptionConfig::resolve` reads the secret from the value, an environment variable or a file (whitespace
    trimmed). Setting more than one source is an error.
  - **`Exchange::reconcile_webhooks`** runs at recovery and after a reset, by name:
    - It creates missing subscriptions and updates changed ones in place (same ID, pending deliveries kept, new URL
      used from the next claim).
    - It disables configuration-owned subscriptions that are no longer declared, keeping their history.
    - It leaves API subscriptions alone, and fails if a declared name belongs to one.
    - `webhooks::validate_declared` checks names (1–64 of `[A-Za-z0-9._-]`, unique). Every entry goes through the
      same validation as `POST /api/webhooks`, and errors name the entry. An invalid configuration stops startup.
  - **Migration 010:** `name` (unique where set), `source` (`api` | `config`) and `paused`.
  - **Paused versus disabled:**
    - A paused subscription still gets a delivery for each matching event, but the claim query skips it
      (`s.paused = 0`).
    - A disabled one gets none.
    - The claim query now also skips disabled subscriptions (`s.enabled = 1`). Until now nothing could disable a
      subscription, so their pending deliveries would have been sent.
  - **API:** subscriptions show `paused` and `source`, and `name` when declared.
  - **Scenarios:** the sandbox clears `webhooks.subscriptions`, so sandbox events never reach real consumers.
  - **Docs:** a README section "Declared subscriptions" and a commented example in `config/default.yaml`.
- **Tests:**
  - **`tests/webhook_config.rs` (4):**
    - A declared subscription is created with `source: config`, signed with a `secret_file` secret (trailing newline
      ignored) and delivered.
    - A paused subscription holds its two deliveries, and a disabled one records none.
    - **A restart with a changed configuration:**
      - It updates `trades` in place (same ID, new URL and event types).
      - It disables the removed `orders`.
      - It unpauses `later`, whose held deliveries then reach its new URL.
      - It creates `fresh` and leaves the API subscription byte-identical.
    - A reset removes API subscriptions and brings declared ones back.
    - Seven invalid declarations each stop startup with a message naming the problem.
  - **Unit tests:** the YAML overlay parses, and a typo is refused; secrets resolve from a value, a variable or a
    file, with a clear error for a missing file; the scenario sandbox has no declared subscriptions.
  - The new and the webhook, reset and scenario suites were repeated five times without failure. All gates pass,
    e2e included.
- **Mutation checks**, each made a test fail:
  - paused deliveries sent;
  - no reconcile after a reset;
  - removed subscriptions left enabled;
  - updates not stored;
  - the secret file not trimmed;
  - duplicate names allowed;
  - the sandbox inheriting subscriptions;
  - several secret sources accepted.
- **Decisions:**
  - **`paused` is new and separate from `enabled`.** The user asked for webhook clients that are easy to maintain:
    pausing a consumer for maintenance must not lose events, while turning one off (or removing it from the
    configuration) must not let a backlog grow without limit. So `paused` holds deliveries, and `enabled: false`
    records none.
  - **Removed means disabled, not deleted.** History, attempts and dead letters stay inspectable (4.6b). Declaring
    the name again re-enables the same subscription.
  - **Declared subscriptions are changed only through the configuration file and a restart.** Hot reload was left
    out; under Compose, a restart is one command.
  - **Lists cannot be set through `EXCHANGE__…` environment variables** (a limit of the `config` crate), so the
    documented route is a mounted YAML file.

**4.6b — webhook maintenance API ✅ (2026-10-16).**
- **Delivered** (`api/webhooks.rs`, which also takes over registration and listing from `api/mod.rs`):
  - **`GET /api/webhooks/{id}`:** the subscription with `deliveries: { pending, inFlight, delivered, dead,
    oldestUnfinished }` (`Store::delivery_stats`).
  - **`PATCH /api/webhooks/{id}`:** `url`, `eventTypes`, `ordered`, `enabled`, `paused` (`Exchange::update_webhook`),
    validated as for `POST`. Unpausing wakes the worker.
  - **`DELETE /api/webhooks/{id}`:** a soft delete (`deleted` column). The subscription is hidden from the API (unless
    `?includeDeleted=true`) and treated as unknown by every operation. `wants()` and the claim query skip it, so its
    pending deliveries and retries stop. Rows stay because deliveries reference them, and deterministic subscription
    IDs count every subscription ever created.
  - **`POST /api/webhooks/{id}/secret` `{ secret, graceSeconds }`** (default 3600, at most a week):
    - The previous secret is kept with an expiry (`previous_secret`, `previous_secret_until`).
    - The claim query returns it only while it is valid.
    - The worker then sends `X-Signature: sha256=<new>, sha256=<previous>`.
  - **Config-driven secret changes** get the same grace (`webhooks.secret_grace_seconds`, default 3600). A restart
    with an unchanged secret keeps the running grace period rather than starting a new one.
  - **`GET /api/webhooks/{id}/deliveries?status=&after=&limit=`** (paged, at most 1000), with `nextAttemptAt` and
    `deliveredAt`.
  - **`POST /api/webhooks/{id}/dead-letters/retry`** and **`POST /api/webhooks/{id}/redeliver?after=`**. The `/test`
    routes share the handlers and views.
  - **Configuration-owned subscriptions:** only `paused` can be changed. Anything else, a delete or a rotation gets
    `409 managed_by_configuration`, with a message saying to edit the file. The API pause lasts until a restart
    reapplies the file.
  - **Migration 011:** `previous_secret`, `previous_secret_until`, `deleted`.
  - **Docs:** README sections "Maintaining a subscription" and rotation, an updated Python verification example
    (accepts any listed signature), and the new config key.
- **Tests:** `tests/webhook_maintenance.rs` (4):
  - **Dead letters:** two deliveries go dead after five attempts. The stats and `?status=DEAD` show them with the
    error, and paging by `after` and `limit` works. One is retried by ID and the rest all at once, and a redelivery
    through the regular API works. Bad status, limit or query keys are `400`; unknown IDs are `404`.
  - **One subscription changed step by step:**
    - It is moved, and the failing delivery goes to the new URL.
    - It is paused: two deliveries are held, then sent in order.
    - It is narrowed to other event types, then disabled and re-enabled.
    - Invalid changes are refused and change nothing.
    - It is deleted while a delivery is mid-retry. That retry never goes out, the subscription is hidden, every
      operation on it is `404`, and its history is kept.
  - **Rotation:**
    - During the grace period both signatures are sent, the new one first.
    - With `graceSeconds: 0` only the new one is sent.
    - A 1-second grace expires.
    - An unsigned subscription becomes signed.
    - Invalid requests are refused.
  - **Declared subscriptions:**
    - URL, enable, delete and rotate requests get `409` naming the subscription.
    - An API pause holds deliveries until a restart reapplies the configuration.
    - A secret changed in the configuration signs with both secrets, and a second restart keeps the same grace
      deadline.
  - The maintenance, config, webhook and reset suites were repeated five times without failure. All gates pass, e2e
    included.
- **Mutation checks**, each made a test fail:
  - config subscriptions editable;
  - a config pause refused;
  - the previous secret never sent;
  - a grace period that never ends;
  - retries of a deleted subscription sent;
  - the grace period restarted on every restart;
  - deleted subscriptions listed;
  - only the new signature sent.

  Not caught, as expected: unpausing without waking the worker. The poll interval delivers anyway, so it only adds
  latency.
- **Decisions:**
  - **Rotation sends both signatures in one header** (`sha256=<new>, sha256=<previous>`), the pattern Stripe uses,
    rather than a second header. Outside a grace period the header is unchanged, so existing receivers are
    unaffected. Receivers that want to support rotation check every value; the README example does.
  - **The service does not generate secrets.** The crate has no cryptographic RNG (only `fastrand`), and the
    consumer must know the secret to verify it anyway. The client supplies it.
  - **Delete is soft.** A hard delete would have to remove deliveries and attempts, losing history, and would break
    the deterministic subscription ordinal.
  - **An API pause on a declared subscription is temporary by design.** It covers incidents without editing files;
    the file stays the source of truth.

**4.6c — usage guide and Compose setup ✅ (2026-10-16).**
- **Delivered:**
  - **`docs/`, the usage guide** (the README stays the API reference and now links to it):
    - `README.md`: an index.
    - `getting-started.md`: the Compose example step by step (trade, watch deliveries, watch a retry), then adding the
      simulator to your own Compose project, including where a consumer's URL points (same network, another project,
      the host).
    - `configuration.md`:
      - how settings are layered, including that the image's environment variables beat the file;
      - persistence modes;
      - webhook settings, with a table of how long a consumer may be down for each `max_attempts`;
      - settlement;
      - four recipes.
    - `webhook-receivers.md`:
      - the delivery contract and five rules for a correct receiver;
      - verification in Python, Java (with a Spring Boot sketch) and Node.js;
      - secret rotation for both kinds of subscription;
      - catching up and replay;
      - testing with fault injection.
    - `operations.md`:
      - declared versus API-created subscriptions, pausing, consumer lag, dead letters and replay;
      - monitoring, with the series worth an alert;
      - a troubleshooting table;
      - backups, upgrades and reset.
    - `benchmarking.md`: how to measure, a summary of the figures, and the memory-limit caveat (extended in 4.6d).
  - **`examples/compose`:**
    - `compose.yaml`: the exchange built from the workspace and a `python:3.13-slim` receiver, one shared Docker
      secret, a volume, health checks, and `EXCHANGE_PORT`, `RECEIVER_PORT` and `EXCHANGE_IMAGE` overrides.
    - `exchange.yaml`: declares `settlement` (ordered `TradeExecuted`) and `orders` (the order lifecycle), both
      signed through `secret_file`.
    - `receiver.py` (standard library only): it verifies signatures, deduplicates by `eventId`, logs one line per
      event, and lists what it received at `/received`. It can be switched to fail with `/control?status=`.
  - **`examples/receivers`:** `verify.py`, `Verify.java` and `verify.mjs`. Each holds the verification function plus
    a self-check against the signature already pinned in `signing.rs`: a valid signature, a rotation's second value,
    a changed body, another secret, and an old timestamp.
- **Tests:**
  - **`tests/e2e.rs`, the Compose example:** `the_compose_example_delivers_verified_webhooks` runs it under its own
    project name and ports:
    - the two declared subscriptions come up signed;
    - a trade's five events arrive, all verified;
    - with the receiver failing, the delivery is retried, and once accepted it is not taken for a duplicate;
    - after a container restart the subscriptions are unchanged and deliveries continue;
    - `down -v` cleans up.
  - **`tests/e2e.rs`, the snippets:** `the_signature_snippets_verify_in_their_runtimes` runs the three files in
    `python:3.13-slim`, `node:24-slim` and `eclipse-temurin:21-jdk` with no network.
  - **`config::tests::documented_configuration_examples_parse`:** every YAML configuration block in the README, the
    guide and the example parses as an overlay, with known event types and http URLs (7 blocks).
  - All relative links and anchors in the README, the guide and the example READMEs were checked with a script. The
    two new e2e tests passed three runs in total and left no containers or volumes behind.
- **Mutation checks**, each made a test fail:
  - the receiver marking retries as duplicates (see Found);
  - a documented example with an unknown event type;
  - a documented example with an invalid mode.
- **Found and fixed:** the first version of the sample receiver remembered an `eventId` on arrival, even when it
  answered with an error. A retry then counted as a duplicate, and a consumer built the same way would have dropped
  the retried event. It now records an event only when it acknowledges it. The guide states this as part of rule 2.
- **Decisions:**
  - **The demo secret is committed** (`examples/compose/secrets/webhook-secret.txt`), marked as a demo value in the
    example's README and the compose file. That way `docker compose up` works without setup.
  - **The receiver uses only Python's standard library** in `python:3.13-slim`, so nothing extra is built.
  - **Verification code is maintained as runnable files.** The guide quotes them, and the e2e suite runs them, so the
    documented code is tested code.
- **Deviation:** none from the 4.6c entry. The Kubernetes Job manifest remains part of 4.6d.

## Milestone 5 — back-office counterparties (draft, 2026-10-17)

**Context:** the exchange simulator backs a trading demo app. The app is a showcase for an internal CQRS and
event-sourcing framework: it models a trading account domain with event sourcing. The demo is front office today
(place orders, see fills, watch settlement). Back-office features will follow: deposits and withdrawals, settlement,
reconciliation.

**Principle (D8):** the simulators play only the outside parties: exchange, clearinghouse, payment gateway. Client
cash balances, positions, buying power, and the reconciliation of the firm's books belong to the demo's own
event-sourced domain; that is what the showcase demonstrates. So no simulator keeps a positions or cash ledger.
Clearing obligations are computed from the day's trades, not from stored balances.

**Goal:**
- settlement that completes within a short demo;
- realistic settlement cycles (T+1 by default, per market where it differs);
- end-of-day clearing statements to reconcile against;
- a payment gateway for deposits and withdrawals, with the same character as the exchange: an asynchronous API,
  signed webhooks, seeded outcomes and faults, the virtual clock.

| # | Step | Size | Delivers |
|---|---|---|---|
| 5.1 | Settlement cycle: T+1 by default, per market where it differs | S | `settlement.settlement_days` defaults to 1. T+1 is the cycle in the US, Canada and Mexico since May 2024, and the EU, UK and Switzerland move to it on 2027-10-11. A per-instrument override (`settlementDays` on the instrument, or a per-MIC table in config) lets a market keep T+2, e.g. XCSE until 2027-10-11. Tests and docs that assume T+2 by default are updated. |
| 5.2 | Accelerated virtual clock for short demos | M | `clock.speed`: simulated seconds per real second while the virtual clock runs (default 1; 1440 makes a day pass in a minute). Everything on domain time follows: order and event timestamps, T+N business days and weekends, and end-of-day cutoffs (5.3). Webhook retries, leases and timeouts stay on real time. With `clock.mode: virtual`, `running: true` and `speed: 1440`, a T+1 trade settles about a minute after it executes, with no clock calls. Speed is also settable at runtime (D11). The guide gets a "demo setup" recipe. The demo app must take dates from event timestamps, not its own clock. |
| 5.3 | End-of-day clearing statements | L | At a configurable cutoff on the domain clock (e.g. 17:30 in the market's time zone, business days only), one statement per participant: the day's cleared trades, and their net obligations per instrument and settlement date (securities and cash, computed from the trades). Delivered as a webhook event (`ClearingStatementIssued`) and by `GET /api/clearing/statements?date=&participant=` (JSON, maybe CSV). Settlement events reference the statement and obligation they settle. **Reconciliation breaks** are opt-in and seeded, in the style of the webhook faults: a trade left out, a quantity or price changed, a trade that does not exist, or a statement issued late. The canonical trades never change. The granularity depends on D9. |
| 5.4 | Payment gateway simulator (5.4a: extract the shared crate, D10; 5.4b: the gateway) | L | A new workspace member, not a mode of the exchange. **API:** initiate a deposit or withdrawal for an account, with an idempotency key, amount and currency. **Webhooks:** signed `PaymentPending`, then `PaymentCompleted`, `PaymentFailed`, or later `PaymentReturned` (a reversal after completion). **Outcomes:** seeded probabilities and delays on the domain clock. **Statements:** an end-of-day bank statement for reconciliation. It reuses the exchange simulator's outbox, webhook delivery and maintenance, fault injection, scenarios, virtual clock and persistence modes. Where to share that code is D10. |
| 5.5 | Tests, acceptance and guide | M | A demo-shaped acceptance on an accelerated clock: deposit, trade, T+1 settlement, statement, reconciliation inputs, a withdrawal, with and without breaks. The guide gets a back-office chapter: how the demo's domain consumes trades, settlements, statements and payments, and what each break looks like. |

**Order within the milestone:** 5.1 and 5.2 first. They help the front-office demo now (short demos that settle) and
are small. Then 5.3 and 5.4 when back-office work starts, in the order the demo needs them. 5.5 runs alongside.

### Milestone 5 progress

**5.1 — settlement cycle: T+1 by default, per market where it differs ✅ done (2026-10-17)**
- **Delivered:**
  - `settlement.settlement_days` defaults to 1 (was 2), in `config/default.yaml` and in `SettlementPolicy::default`.
  - `settlement.market_settlement_days`: a table by MIC (D12), e.g. `{ XCSE: 2 }`, or
    `EXCHANGE__SETTLEMENT__MARKET_SETTLEMENT_DAYS__XCSE=2`. Keys are uppercased when the policy is built, because
    environment variables arrive lowercased. Each key must be a valid MIC and each cycle at most 30 days; startup
    refuses anything else.
  - The cycle is chosen at intake from the instrument's `mic` in the `TradeExecuted` snapshot, so it is a function of
    the event log (reproducible, unaffected by later instrument changes). `SettlementPending.settlementDays` reports
    the cycle actually applied.
  - Scenarios can set `settlement.market_settlement_days` too (it replaces the service's table).
  - README (keys table, Settlement section with the T+1 dates), `docs/configuration.md` (per-market example; the
    simulated-settlement recipe no longer pins T+2), `config/default.yaml` comments.
- **Tests:**
  - `the_cycle_is_t_plus_1_unless_the_market_has_its_own` and MIC/limit validation (unit).
  - `a_market_with_its_own_cycle_keeps_it` (machine): XCSE at T+2, another MIC and no MIC at T+1.
  - `config::tests::market_settlement_cycles_come_from_the_file_and_the_environment`: file and environment layers
    combine, and the lowercased environment key matches.
  - `a_trade_settles_on_t_plus_1_by_default` and `a_trade_on_a_t_plus_2_market_settles_when_the_clock_gets_there`
    (service, the latter with a lowercase key).
  - Updated for T+1: the machine tests (dates one day earlier; the weekend case now trades on a Friday), the weekend
    and correction flow tests, the Milestone 2 e2e (advance one day), and comments. The `settlement-t-plus-2`
    scenario now demonstrates the per-market table (`mic: XCSE`, `market_settlement_days: { XCSE: 2 }`) and keeps its
    dates.
- **Mutation checks**, each made a test fail: intake ignoring the table; keys not uppercased; the MIC read from the
  wrong place in the payload.
- **Decisions:** D9, D11 and D12 settled (see Decisions). The shipped table is empty: XCSE is on T+2 in reality until
  2027-10-11, but the plan's default is T+1 everywhere, and the configuration comment shows how to keep T+2.
- **Deviation:** none. The scenario `settlement` section gained `market_settlement_days`, so scenarios can cover the
  new key like every other policy key.

**5.2 — accelerated virtual clock for short demos ✅ done (2026-10-17)**
- **Delivered:**
  - `clock.speed`: simulated seconds per real second while the virtual clock runs, default 1, more than 0 and at
    most 1 000 000. `VirtualClock` keeps the speed in its state; a speed change re-anchors at the current reading,
    so time already passed is kept, and a frozen clock stays frozen. Speed 1 keeps the exact real-time arithmetic.
  - Startup refuses an out-of-range speed, and any speed other than 1 with `clock.mode: system`.
  - Runtime control through test control only (D11): `POST /test/clock/speed { "speed": 1440 }`, `400
    invalid_clock_speed` out of range, `409 clock_not_virtual` on the system clock. `GET /test/clock` and every clock
    response report `speed`. A reset goes back to the configured speed.
  - Scenario sandboxes run at speed 1, whatever the service's configuration (their clock is frozen anyway).
  - README (keys table, test-control table, a Speed bullet, restart and reset behaviour), `config/default.yaml`, and a
    "Short demo" recipe in `docs/configuration.md`: `virtual`, `running: true`, `speed: 1440`, simulated settlement;
    the demo app must take dates from event timestamps, not its own clock.
  - Nothing else needed to change: order and event stamps, T+N and weekends already come from the exchange clock,
    and the settlement tick (real time) fires whatever became due, stamped with its scheduled instant. Webhook
    retries, leases and timeouts were already on real time.
- **Tests:**
  - `a_fast_clock_moves_speed_times_real_time_and_keeps_its_reading_on_a_change` (unit): bounds on the reading
    against real elapsed time × speed, a change keeps the reading, bad speeds refused, speed does not move a frozen
    clock.
  - `a_fast_clock_speeds_up_domain_time_and_its_speed_can_change` (API): speed reported, time moves an hour per real
    second, runtime change, 400s, reset restores the configured speed.
  - `a_restart_on_a_fast_clock_continues_from_the_latest_stamp`, `startup_refuses_a_speed_it_cannot_honour`, and the
    system clock refusing a speed change.
  - `on_a_fast_running_clock_a_t_plus_1_trade_settles_without_clock_calls` (service, 108 000×: Monday 09:00 to Tuesday
    00:00 in half a second): settles at exactly 00:00, with every event stamp in order.
  - The clock, settlement-flow and clock unit suites passed 5 repeat runs.
- **Mutation checks**, each made a test fail: the speed ignored; a speed change without re-anchoring; a reset keeping
  the runtime speed; recovery ignoring the latest stamp; the system-clock speed check removed.
- **Found and fixed:** a running virtual clock's stored reading is only written at startup and on clock changes, so
  after a restart the clock resumed from that stale reading and stamped new events *before* ones already recorded.
  This was true at speed 1 too (by the real time since the last change), but at 1440× it means hours or days
  backwards. Recovery now continues from the later of the stored reading and the latest `occurredAt` the exchange
  or the simulator stamped (`Store::latest_clock_stamp`; external reports carry the reporter's own time and are left
  out). The mutation check showed the test clock 2 h 22 min backwards without the fix.
- **Decisions:**
  - **The speed is configuration, not stored state.** A restart runs at `clock.speed` again; a speed changed through
    `/test/clock/speed` lasts until a restart or reset. Storing it like `running` would make a changed `clock.speed`
    silently ignored after a restart, and a showcase deployment (D11: no `/test`) could only undo that by deleting its
    volume. No migration was needed.
  - **Upper bound 1 000 000** (about 11.5 simulated days per real second): at that speed a 250 ms settlement tick
    already spans about three simulated days, so faster runs should use `/test/clock/advance` instead.
- **Deviation:** none from the 5.2 entry.

**5.3 — end-of-day clearing statements ✅ done (2026-10-17)**
- **Delivered:**
  - `src/clearing`: a clearinghouse with no ledger (D8). `clearing.enabled` (off by default), `cutoff` (`HH:MM`) and
    `time_zone` (IANA, through `chrono-tz` 0.10.4, which bundles the zone data, so the image needs no tzdata).
    Cutoffs are business days only and follow daylight saving.
  - At each cutoff: one statement per participant that traded in the window `[previous business day's cutoff,
    cutoff)` (D9: per participant, mapping-agnostic). It lists the participant's side of each trade and nets
    obligations per instrument and intended settlement date: `securitiesNet` and `cashNetMinor`/`cashNet`, positive
    meaning receive. Across a day, obligations net to zero.
  - Delivered as `ClearingStatementIssued` (`source: clearinghouse`, aggregate ID the statement ID), through the
    regular outbox and webhooks. Served by `GET /api/clearing/statements?date=&participant=` and
    `GET /api/clearing/statements/{id}`.
  - IDs are natural keys: `CS:<date>:<participant>` and `<statementId>:<instrument>:<settlementDate>`. Settlement
    events (and `/api/settlements`) carry `clearing: { businessDate, buyer, seller }` with both sides' statement and
    obligation from intake on, before the statement exists.
  - Seeded breaks under `clearing.breaks`: `omit`, `alter` (quantity or price ±1), `phantom` (a copy under an
    execution ID that does not exist) and `late` (1..=`max_late_minutes` of domain time). A broken statement nets what
    it lists. Trades and the log never change. `GET /test/clearing/breaks` lists each break with the canonical and
    the reported value, including late statements still waiting.
  - `Exchange::tick` (was `settlement_tick`) runs everything due on domain time in time order. Before each cutoff
    or late statement, the settlement transitions due by that instant run first, so a large clock move gives a
    time-ordered log. Statements carry their scheduled instant, like settlement events.
  - Persistence (migration 012): `clearing_statements` (body, breaks, issued, event sequence), `clearing_cursor`,
    and a `clearing` column on settlement instructions. Waiting statements survive restarts. Cutoffs passed while
    the service was down are issued at startup; a reset starts clearing again from the reset clock.
  - Scenarios: a `clearing:` section, and `scenarios/clearing-statements.yaml` (now part of the suite and of the e2e
    run in the image).
  - README Clearing section (format, breaks, API, test route, keys), `docs/configuration.md` Clearing section and a
    back-office demo recipe, `config/default.yaml`.
- **Tests:**
  - Unit (`clearing::tests`): cutoffs through DST, business-date assignment at the boundary and over weekends and
    time zones, netting (including zero-sum across participants), references, each break kind with the statement
    and its record agreeing, seeded reproducibility, validation.
  - `tests/clearing.rs`: issue exactly at the cutoff, the API and filters, 404/400, the event payload equal to the
    API, webhook deliveries; a seven-day clock move with settlement in time order (no weekend statements, Friday
    after the cutoff dated Monday); late and omitted breaks across a restart; a cutoff passed while down issued once;
    reset; off by default; invalid time zone, cutoff and probability refuse startup.
  - `tests/recovery.rs`: a trade stamped after a cutoff but committed before the tick that handles it goes on the
    next statement, and one move past a settlement instant and a cutoff gives events in time order.
  - The configuration-examples test now covers `clearing:` blocks and validates their policy (11 blocks).
  - The clearing, checkpoint, scenario and Milestone 3 suites passed 6 repeat runs.
- **Mutation checks**, each made a test fail: an inclusive cutoff boundary; a window that ignores weekends;
  statements before the settlement transitions due by their instant; the trade cursor jumping to the latest event;
  late statements issued early; waiting statements dropped at restart; the cursor not restored; the settlement trade
  date not following the clearing date; the cash sign flipped; an altered line not repriced.
- **Found and fixed:**
  - **A trade after the cutoff settled before its statement.** The first version kept the settlement trade date as
    the UTC date of execution, so a Monday 18:00 trade was on Tuesday's statement but settled T+1 at Tuesday 00:00,
    before that statement was issued. The scenario showed it. With clearing enabled, the trade date is now the
    clearing business date in both places, as at a real exchange, where late trades are dated the next business day.
    Without clearing nothing changes.
  - **An index on `events(event_type, occurred_at)` made the hot path slower.** The checkpoint test's WAL bound
    failed 1 in 8 runs with it and 0 in 8 without. A window now reads events after the last trade sequence the
    previous window took (a primary-key range), which is correct because exchange stamps never go backwards (5.2).
- **Decisions:**
  - **One clearing time zone** for the clearinghouse, not one per market: a single statement per participant per day
    covers all its markets.
  - **Statements only for participants that traded in the window.** Participants have no registration time, so
    "registered by the cutoff" is not reproducible on a running clock.
  - **Breaks are configuration** (and a scenario section), not a runtime test route like webhook faults: a showcase
    deployment without `/test` can still have them (D11). Their records are readable under `/test` only.
  - The ticker that ran only with the simulator now also runs with clearing, at `settlement.tick_interval_ms`.
- **Deviations:**
  - **No CSV.** The plan said "JSON, maybe CSV". JSON only for now, since every consumer so far reads the webhook.
  - **Statements are stamped at their scheduled instant.** On a running clock a statement can follow events
    stamped up to one tick after it, as settlement events can (documented).

**5.4a — extract the shared crate (D10) ✅ done (2026-10-17)**
- **Delivered:** a new workspace member, `simulator-kit` (with a README), that `exchange-simulator` depends on. No
  behaviour change. Moved, with history where a file moved whole:
  - `clock` (the virtual clock, `ClockConfig`, `wall_now`), `rng` (the SplitMix64 `Rng` from `market`), `matcher`
    (scenario "contains" matching);
  - `ids`: `IdScheme` with the outbox kinds (event, delivery, subscription) and a public `derive`. The exchange's
    kinds (order, execution, report) are the `ExchangeIds` extension trait;
  - `events`: `NewEvent` and `EventEnvelope`. Event types, sources and payloads stay in the exchange;
  - `persistence`: persistence modes, connection setup, `migrate` over the caller's migration list, background WAL
    checkpoints, column conversions (`sql`). The exchange keeps its migrations and wrappers with the old signatures;
  - `outbox`: the SQL for events, subscriptions, deliveries, attempts, redelivery, dead letters and the stored
    virtual clock, as free functions on a connection, plus `SCHEMA` (the shared tables as they stand). The
    exchange's `Store` methods delegate, so its call sites did not change;
  - `webhooks`: types, signing, faults, `WebhooksConfig`, the `DeliveryWorker` over a `DeliveryBackend` trait, and
    `WebhookHub`, which holds the subscriptions and all their maintenance logic (lifted from `Exchange`: register,
    reconcile declared ones, update, delete, rotate, faults, claim, record, redeliver, retry);
  - `api`: `ApiError`, and the `/api/webhooks` and `/test` webhook routes over a `WebhookBackend` trait.
  - The exchange implements `DeliveryBackend` for its `Dispatcher` and `WebhookBackend` for its `AppState`, and
    re-exports `clock` and `webhooks` under their old paths. `ExchangeError` keeps its variants: a `WebhookError`
    converts into them, so every HTTP status, error code and message is unchanged.
- **Tests:**
  - The existing suites pass unchanged, apart from imports of the `ExchangeIds` trait in two places: 231 test
    functions before, 233 run plus the 5 e2e tests now. The e2e run builds the image from the workspace with the
    new member.
  - New: `the_migrations_build_the_shared_outbox_schema` (the exchange's migrated tables equal `SCHEMA`, comments and
    whitespace aside); the kit's ID derivation pinned with a value computed independently in Python, which also
    reproduces the exchange's existing pin; `SCHEMA` creates usable tables; the worker waits out an overloaded
    writer and then gives up; the dispatcher reports an overloaded or stopped exchange to the worker as such.
  - The webhook, fault, maintenance, config, MockServer and checkpoint suites passed 3 repeat runs.
- **Mutation checks**, each made a test fail: `SCHEMA` drifting from the migrations; a hub refusal mapped to the
  wrong exchange error; the test webhook routes not mounted; the outbox stamping the wrong schema version; the ID
  derivation's domain changed; an overloaded exchange reported as a failure; the worker giving up on the first
  overload. Two of these first survived: the schema-version mutation had been applied to one write path only, and
  no test covered the worker waiting out an overloaded writer (true before the extraction too). The new tests cover
  the second.
- **Decisions:**
  - **The exchange's hash domains stay** (`exchange-simulator/ids/v1`, `exchange-simulator/faults/v1`), so its IDs and
    fault streams are unchanged. The gateway shares them; its IDs live in its own database.
  - **Metric names stay**, `exchange_` prefix included on the checkpoint and persistence-mode gauges, so dashboards
    keep working. Renaming them would be a behaviour change for a later step.
  - **Store methods delegate** to the kit instead of the exchange calling the kit directly: no call-site churn.
  - **The scenario runner stays in the exchange.** Its steps are exchange commands; only the matcher is shared. The
    gateway's scenarios (5.4b) decide how much of the runner to share.
  - `/api/webhooks` handlers moved to the kit with the exchange's file (`git mv`).
- **Deviation:** the D10 entry listed the parts to move; the scenario matcher was added (pure, 100 lines, needed by
  any simulator's scenarios), and `ClockConfig` and `WebhooksConfig` moved with the clock and webhooks.

**5.4b — payment gateway simulator ✅ done (2026-10-17)**
- **Delivered:** a new workspace member, `payment-gateway` (with a README), built on `simulator-kit`.
  - **API:** `POST /api/payments` (`idempotencyKey`, `type` `DEPOSIT`/`WITHDRAWAL`, the demo's opaque `accountId`, the
    counterparty `bankAccount` with a mod-97-checked IBAN, `amount` as a decimal string, `currency`, `reference`);
    a repeated key with the same details returns the payment with `duplicate: true`, with different details `409
    idempotency_conflict`. `GET /api/payments/{id}`, `GET /api/payments?accountId=&status=&after=&limit=`,
    `GET /api/events`, `/health`, `/metrics`.
  - **Lifecycle:** `PENDING`, then after a seeded delay per type (domain time) `COMPLETED`, booked on the firm's
    account, or `FAILED` with a reason. A completed payment may be `RETURNED` 1..=`max_return_days` business days
    later, which books the reversal. Draws are a function of the seed and the payment's sequence.
  - **Events:** `PaymentPending`, `PaymentCompleted`, `PaymentFailed`, `PaymentReturned` (`source:
    payment-gateway`) and `BankStatementIssued` (`source: bank`), in the shared envelope and outbox.
  - **Statements (D14):** at the cutoff on business days, one per currency the account has booked anything in,
    `BS:<date>:<currency>`: entries with end-to-end IDs (the idempotency keys), opening balance (configured plus all
    earlier bookings), credits, debits, closing balance. Payment transitions due by a cutoff run before it, as in the
    exchange's clearing.
  - **Breaks (D16):** omitted entry, amount off by one minor unit, phantom entry, late statement; seeded, listed under
    `/test/statements/breaks`. A broken statement is consistent with itself; the next day opens at the true balance.
  - **Shared parts:** webhooks and their maintenance API, signing, faults, declared subscriptions, the virtual clock
    with speed, persistence modes and deterministic IDs all come from the kit. `/test` has the clock routes, reset,
    statement breaks and the kit's webhook test routes.
  - **Kit additions:** `calendar` (business days, and the cutoff calendar that was the exchange's
    `ClearingCalendar`, now a type alias there) and `rng::draw` (moved from the exchange's settlement module). The
    exchange re-exports both; its error messages are unchanged.
- **Tests:** 28 in the gateway:
  - unit tests for IBANs, amounts, currencies, the lifecycle (delays, credits and debits, returns on business days,
    failure rates, seeding), statements (balances, directions, each break, no sign change), and configuration;
  - `tests/payments.rs`: pending then completed on the clock, idempotency, validation, failures and returns, a
    restart, deterministic IDs giving the same log twice (and in time order over a week), signed webhooks verified
    on the raw body plus maintenance, clock/reset/health, invalid configuration;
  - `tests/statements.rs`: balances carried from day to day and a second currency, returns as reversals over a week
    in time order (Copenhagen cutoff, Friday after the cutoff dated Monday), late and broken statements across a
    restart, statements off, reset.
  - The gateway suite passed 5 repeat runs; all 261 tests of the three crates pass, and the exchange e2e passed
    after the kit changes.
- **Mutation checks**, each made a test fail: the opening balance ignoring earlier bookings; a statement before the
  transitions due by its cutoff; a transition page spanning more than a day; idempotency ignoring the amount;
  withdrawals credited; IBANs unchecked; late statements lost at restart; failures never drawn.
- **Found and fixed:**
  - **Events could leave time order when a payment takes over a day.** A page of due transitions could create a
    return due before later items of the same page (for example, a three-day withdrawal and a deposit returned the
    next day). Pages now span less than a day: a return is at least a business day after its completion, so it
    always falls after the page. `a_return_due_before_a_slow_payment_completes_comes_first_in_the_log` covers it.
  - Two test mistakes, fixed in the tests: a signature checked against re-serialized JSON (receivers must verify
    the raw body, as the guide says), and a Friday payment placed before the Copenhagen cutoff.
- **Decisions:**
  - **One writer behind a mutex** instead of the exchange's dispatcher thread: the gateway has no hot path or group
    commit, so commands and ticks run on the gateway under a lock, off the async runtime.
  - **No balance checks** on withdrawals. Whether a client may withdraw is the demo's decision (D8).
  - **Statements per currency**, from the first booking in it. A currency with no movements that day still gets its
    statement, because a bank statement shows the balance.
  - **WAL checkpoints stay SQLite's automatic ones** (no `database.checkpoint` setting): the gateway writes little.
  - Payment IDs come from the kit's `derive` with the kind `payment`.
- **Deviations:** none from the 5.4 entry and D13–D16. Scenarios are deferred (D15). The Docker image and a
  Compose example move to 5.5, with the demo-shaped acceptance that runs both services.

**5.5 — tests, acceptance and guide ✅ done (2026-10-17)**
- **Delivered:**
  - **`payment-gateway/Dockerfile`** (and its `.dockerignore`), the same layout as the exchange's: non-root user, a
    volume at `/var/lib/gateway`, JSON logs, a health check on `/health`, port 8081.
  - **`examples/back-office`:** Compose with the exchange (simulated T+1 settlement, clearing at 17:30 Copenhagen time),
    the gateway (payments that always complete, bank statements at the same cutoff, a DKK opening balance) and the
    sample receiver, one ordered signed subscription per service. Both clocks start frozen at Monday 08:00 at speed
    1440 (`SPEED` overrides it) and are resumed together. `compose.breaks.yaml` turns on every break kind in both
    statements. README with a `curl` walkthrough.
  - **The sample receiver** now keeps each event's body in `/received`, so a reconciliation can work from what the
    demo actually received. It stays standard-library only.
  - **`docs/back-office.md`**, a new guide chapter: the parties and their events, taking dates from events, trading
    and settlement (T+1, clearing references), reconciling clearing statements (steps, and the D9 mapping question),
    payments (credit on completion, reserve withdrawals yourself, returns), reconciling bank statements, a table of
    every break and how it shows, and running the example. Linked from the guide index, the README's Clearing
    section and the gateway README.
- **Acceptance:** `the_back_office_demo_reconciles_on_an_accelerated_clock` (`tests/e2e.rs`) builds both images and
  plays the demo app over two business days at 4320×: six deposits and nine trades on Monday, two withdrawals on
  Tuesday morning, on the gateway's clock. From the webhooks the receiver got (all verified):
  - **Without breaks:** reconciling the clearing and bank statements against the trades and payments finds nothing;
    the day's obligations net to zero; every `SettlementCompleted` is T+1 and names obligations on the statements;
    Tuesday's bank statement opens at Monday's closing and lists the two withdrawals as debits.
  - **With breaks:** the reconciliation's findings equal the breaks `/test/clearing/breaks` and
    `/test/statements/breaks` list, one for one, and each of phantom, late and missing-or-changed occurs in both.
  - It takes about 95 seconds, images aside. It passed three runs and left no containers or volumes behind; the
    full e2e suite (6 tests) passed too.
- **Mutation checks** on the acceptance, each made it fail: the gateway listing withdrawals without their sign (the
  clean reconciliation finds both Tuesday entries changed); settlement events without their clearing references.
- **Tests:** the example configurations parse and validate (`exchange.yaml` in the documented-examples test, now
  13 blocks; `gateway.yaml` in a new gateway test). All relative links and anchors in the docs, READMEs and example
  READMEs were checked with a script.
- **Decisions:**
  - **D15 — no shared scenario runner.** The acceptance runs both services as the demo would, through Compose, and
    reconciles from the webhooks. A YAML scenario format for two services would add a second way to express the
    same thing. The exchange keeps its runner, and the gateway has none.
  - **Separate clocks, resumed together**, not a shared clock service: each simulator stays independent, and
    the drift (a fraction of a simulated minute) does not matter to reconciliation, which uses each statement's own
    window.
  - **Test control is on in the example**, because resuming the clocks and listing breaks need it. The example and
    its README call it a demo environment.
- **Deviation:** none from the 5.5 entry. The gateway image and Compose example, moved here from 5.4b, are done.

### Milestone 5 status

Done on 2026-10-17: 5.1 (T+1, per-market cycles), 5.2 (accelerated clock), 5.3 (clearing statements), 5.4a
(`simulator-kit`), 5.4b (payment gateway) and 5.5 (back-office acceptance and guide). Decisions D8–D16 are recorded
below. Still on hold from Milestone 4: 4.6d (the containerized benchmark), until the user asks for it.

## Beyond the milestones

**OpenAPI specifications ✅ done (2026-10-17, requested by the user)**
- **Delivered:** `exchange-simulator/openapi.yaml` and `payment-gateway/openapi.yaml`, OpenAPI 3.1, written by hand,
  served by each service at `GET /openapi.yaml`. Each describes every route, the `/test` routes under a
  `test-control` tag, every error response, and every event type in the top-level `webhooks` section with its
  payload schema. Response and event schemas are strict (`additionalProperties`/`unevaluatedProperties: false`).
  The webhook, clock and reset parts are identical in both documents.
- **Checks:** `simulator-kit` gains a `contract` feature (dev-dependency only: `jsonschema` =0.58.5, the newest
  release at least two weeks old, plus `serde-saphyr` and `regex`). Each service's `tests/openapi.rs`:
  - compares the routes mounted in its sources with the document's operations;
  - exercises every operation (`uncovered()` must be empty), validating each request expected to succeed and every
    response, its status included, against the document;
  - emits every event type and validates each against its `webhooks` schema;
  - in the exchange's suite, checks the shared paths and components are identical in the two documents.
  Both documents also pass `openapi-spec-validator` 0.7.1.
- **Mutation checks**, each made a test fail: a response field renamed; a `TradeExecuted` payload field renamed;
  an undocumented route; a required payment field dropped; an undocumented status (`202`); the shared part edited in
  one document only.
- **Change:** the gateway's `/health` now answers `503 {"status": "unavailable"}` when the database does not answer,
  as the exchange's does; it was a `500` error before.
- **Note:** the first build of the contract tests compiles `jsonschema`'s dependencies, which took about ten
  minutes in the devcontainer.

## Suggested order

1. Milestone 1 wrap-up: 1.1 now, then 1.2, then 1.3a–d.
2. 2.1 virtual clock and test control. Milestones 2 and 3 both depend on it.
3. Milestone 2, steps 2.2–2.5.
4. Milestone 3, steps 3.0–3.5.
5. Milestone 4, steps 4.1–4.6 (settlement before performance, D4). 4.5 skipped and 4.6 extended (D7). 4.6d (the
   containerized benchmark) is on hold until the user asks for it.
6. Milestone 5: 5.1 and 5.2 when the front-office demo needs short settlement cycles; 5.3–5.5 with the demo's
   back-office features.

## Decisions

Settled on 2026-10-14:
- **D1 — combined settlement mode:** yes, there is an inbound API. External outcomes are stored with
  `source: external` beside the simulated lifecycle and never merged into it (step 2.4).
- **D2 — T+N calendar:** weekends only. Holiday calendars can come later.
- **D4 (priority part):** the plan's order holds: Milestone 2 settlement, then Milestone 3, then Milestone 4.
- **D5 — Milestone 1 backlog:** all four items are needed soon: HMAC signing, ordered delivery, instrument `enabled`,
  participant settlement metadata (1.3a–d).

Settled on 2026-10-15:
- **D3 — scenario runner surface:** both, a CLI and an API (step 3.1).
- **D4 (rest) — benchmark environment and fast mode:**
  - There is no fixed hardware: the service mostly runs in Docker Compose or Kubernetes, so the benchmark is
    containerized and reports per environment (4.6).
  - Fast mode can skip disk syncing or run fully in memory, as long as it is documented. The plan offers both: `fast`
    (`synchronous=NORMAL`), where settlement survives restarts, and `memory`, where everything, settlement included,
    lasts only while the process runs (4.4).
- **D6 — reproducible IDs:** event and execution IDs (in fact all IDs) must be stable and reproducible across runs.
  This is a new step, 3.0, the first of Milestone 3.

Settled on 2026-10-16:
- **D7 — client setup and maintenance (extends 4.6):** the service will mostly run under Docker Compose to begin
  with, so webhook clients must be easy to set up and maintain, and usage must be documented in detail.
  - Webhook subscriptions are declared in config and reconciled at startup by name (4.6a).
  - The regular API gains every maintenance operation: update, pause, delete, secret rotation, delivery inspection,
    retry and redelivery (4.6b).
  - Usage documentation is a separate guide under `docs/`; the README stays the API reference (4.6c).
  - 4.5 (instrument-sharded matching) is skipped. 4.1–4.4 showed matching is not the bottleneck.

Settled on 2026-10-17:
- **D8 — what the simulators own:** only the outside parties (exchange, clearinghouse, payment gateway). Client
  positions, cash balances, buying power and reconciliation live in the trading demo's event-sourced domain, which is
  the point of the showcase. Clearing obligations are computed from trades, so no simulator keeps a ledger.

Settled on 2026-10-17 (Milestone 5, second round):
- **D9 — how demo accounts map to exchange participants:** the simulator stays agnostic. Statements (5.3) are per
  participant, and the demo may map one participant per client account now and a firm clearing member later. The
  demo's domain must not assume a participant ID is an account ID.
- **D11 — controlling clock speed at runtime:** through the test-control API only (`/test/clock`). A showcase
  deployment sets `clock.speed` in its configuration instead of exposing a control switch.
- **D12 — per-market settlement cycles:** a table per MIC in configuration (`settlement.market_settlement_days`), no
  instrument field. Cycles are set per market in reality, and instrument payloads stay unchanged.

Settled on 2026-10-17 (Milestone 5, third round):
- **D10 — sharing code with the payment gateway:** extract a shared crate first. Before 5.4, the outbox, webhook
  delivery, signing, fault injection and maintenance, the virtual clock and the persistence-mode plumbing move into a
  new workspace member that both simulators depend on. That refactor is its own step (5.4a, no behaviour change, the
  exchange's whole suite as its gate); the gateway itself is 5.4b.

Settled on 2026-10-17 (5.4b, the payment gateway):
- **D13 — accounts:** a payment carries the demo's opaque `accountId` (never assumed to mean anything) and the
  counterparty's `bankAccount` (`iban`, `holderName`). There is no registration step; IBAN check digits are
  validated.
- **D14 — bank statement:** the bank's end-of-day statement of the firm's client-money account, per currency: every
  movement booked that day (deposit credits, withdrawal debits, returns), with opening and closing balances derived
  from all booked movements (and a configurable opening balance). As with clearing, it is computed from the
  gateway's own payments; there is no separate ledger.
- **D15 — gateway scenarios:** deferred to 5.5. 5.4b is tested through its API; 5.5's acceptance decides whether a
  shared scenario runner is worth extracting. Settled in 5.5: no shared runner; the back-office acceptance runs both
  services through Compose.
- **D16 — statement breaks:** opt-in and seeded, in the clearing style: a movement left out, an amount off by one minor
  unit, a movement that never happened, a statement issued late; listed under `/test`.
