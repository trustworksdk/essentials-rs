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
| 1.2 | Investigate the 57–180 ms commit spikes | S | Turn on `wal_autocheckpoint` tracing and correlate with `exchange_commit_duration_seconds`. If the checkpoint is the cause, fix it in step 4.2; for now, record the finding in `bench/README.md`. |
| 1.3 | Backlog from plan §3/§5 that Milestone 1 skipped | M | Instrument `enabled` flag (a disabled instrument rejects orders); participant settlement metadata (free-form JSON, carried into `TradeExecuted`); HMAC-SHA256 webhook signing (`X-Signature`, a per-subscription secret); an optional serialized-delivery flag per subscription (one in-flight delivery per subscription, in `sequence` order). All four are needed soon (D5), so they come before Milestone 2; split into 1.3a signing, 1.3b ordered delivery, 1.3c instrument `enabled`, 1.3d participant metadata. Each 1.3 item adds a migration to the schema, starting with `002`, so Milestone 2's settlement migration takes the next free number. |

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
| 2.1 | Virtual clock and test-control API | M | A `VirtualClock` implementing `Clock` (set, advance, freeze), selected by config. Test-control routes under `/test/…`: `GET/POST /test/clock` (now, advance by, set to). They are mounted only when `test_control.enabled=true` (default false), on the same port. Every timestamp the service uses goes through the clock: order timestamps, `occurredAt`, delivery due times. Milestone 3 needs this too. |
| 2.2 | Settlement domain, migration, event contracts | M | `SettlementInstruction` (execution ID, intended settlement date, status, attempt, outcome history). Statuses `PENDING → SETTLED \| FAILED \| REJECTED`, plus `DELAYED`, `CORRECTED`, `CANCELLED`. A business-day T+N calculator that skips weekends (D2: no holiday calendars for now). Events: `SettlementPending`, `SettlementCompleted`, `SettlementFailed`, `SettlementRejected`, `SettlementDelayed`, `SettlementCorrected`, `SettlementCancelled`, with schemas documented the way `TradeExecuted` is. The pure state machine and its tests come first, as matching did. |
| 2.3 | Settlement driver and configuration | M | `settlement.mode: external \| simulated \| both` (`external` = today's behaviour). Settings: `settlement_days`, outcome probabilities, delay distribution, seed. The driver consumes trades, creates instructions, and fires due transitions when the virtual or real clock passes them. It restarts from the stored cursor and pending instructions. |
| 2.4 | Combined mode and inbound external outcomes | S–M | Agreed (D1). `POST /api/settlements/{executionId}/outcomes` stores an external outcome (`source: external`) beside the simulated lifecycle. `GET /api/settlements/{executionId}` shows both streams side by side. |
| 2.5 | Tests and acceptance | M | Deterministic: the same seed and the same clock moves give identical lifecycles across restarts. T+2 across a weekend. Failure, delay and correction scenarios. In combined mode, an external consumer and the simulator act on the same trade without conflict. Extend the `e2e` test to cover a simulated settlement driven by clock moves. |

**Acceptance:** with mode `both`, the seed fixed and the virtual clock frozen, one trade gives `SettlementPending` at
once and `SettlementCompleted` (or the seeded failure) exactly when the clock reaches T+2. Webhooks carry
`source: "settlement-simulator"` and the execution ID as correlation ID. A restart in the middle neither changes nor
duplicates the outcome.

## Milestone 3 — scenarios and fault injection

**Goal:** reproducible test runs. These are YAML scenarios with ordered commands, clock moves and assertions; seeded
random markets; and faults injected into *delivered copies* of events, never into canonical ones.

| # | Step | Size | Delivers |
|---|---|---|---|
| 3.1 | Scenario DSL and runner | L | YAML: `seed`, `instruments`, `participants`, `subscriptions`, then `steps:` (`order`, `cancel`, `advance_clock`, `expect` on order state, trades, events, book, deliveries). It runs against an in-process service with a virtual clock. Per D3, a CLI (`exchange-simulator scenario run file.yaml`, exit code = verdict) and/or `POST /test/scenarios`. Example scenarios go in `scenarios/`. |
| 3.2 | Random market generator | M | Seeded distributions from plan §6: arrival rate, buy probability, market-order and cancellation probabilities, and price and size distributions per instrument. It generalizes the `bench` workload. Reproducibility comes from the seed plus deterministic sequencing: one generator feeds commands in order, under the virtual clock. |
| 3.3 | Delivery fault policies | M | Per-subscription policy, configured under `/test/…` or in a scenario: missing fields, explicit nulls, invalid values (a JSON-path list or a random choice by probability), duplicates, delays, out-of-order (hold and release). Faults apply to the outgoing copy only. A new `webhook_attempts` table records each attempt with the mutation applied, so a test can assert on what was actually sent. |
| 3.4 | Reset and replay controls | S | `POST /test/reset` clears all state (test mode only). `POST /test/webhooks/{id}/redeliver?after=` re-enqueues deliveries from a sequence. Dead letters can be listed and retried. |
| 3.5 | Tests and acceptance | M | A scenario suite in CI: the same file and seed give byte-identical event logs over two runs. Each fault type is visible at the receiver and absent from `/api/events`. Reset gives a clean state. |

## Milestone 4 — performance

**Goal:** measured, explained throughput against 1k–10k orders/s, with durable mode and an explicit non-durable mode.
The baseline says the per-order fsync is the limit (≈4–5k/s on the devcontainer disk; matching runs at 2.9M/s). So
the order below deliberately puts group commit before sharding.

| # | Step | Size | Delivers |
|---|---|---|---|
| 4.1 | Latency breakdown and an open-loop load generator | M | New histograms for queue wait (enqueue → start) and acknowledgement latency (enqueue → reply). The bench gains a target-rate, open-loop mode that avoids coordinated omission, reporting achieved rate, latency at that rate, and 503 counts. |
| 4.2 | Group commit | M–L | The writer drains up to N queued commands (or waits up to T µs) and plans them in order on the books. It commits them in one transaction and replies to each after the commit. If the commit fails, all plans are dropped and every caller gets the error; nothing is applied. Ordering and determinism are unchanged. Expected: several times today's durable throughput. |
| 4.3 | Checkpoint control | S | Turn off automatic WAL checkpointing on the writer and run `PRAGMA wal_checkpoint(PASSIVE)` from a separate connection on a timer and a size threshold. Target: remove the 57–180 ms spikes (see 1.2). |
| 4.4 | Explicit non-durable fast mode | S | `persistence.mode: durable \| fast`. `fast` uses `synchronous=OFF`, or in-memory with periodic snapshots per D4. `/health` and `/metrics` say which mode is running, and responses carry a header in fast mode, so nobody mistakes it for durable. |
| 4.5 | Instrument-sharded matching (only if 4.1 shows matching as a bottleneck) | M | Matching workers per instrument shard in front of the single writer. Today's data says matching is not the limit, so this is a measure-first step. |
| 4.6 | Benchmark on the target hardware and load report | M | Run on agreed hardware (D4). Report durable vs fast, with and without outbox and webhooks, across instrument counts. Compare against the 1k–10k target with numbers and their environment, as `bench/README.md` does today. |

## Suggested order

1. Milestone 1 wrap-up: 1.1 now, then 1.2, then 1.3a–d.
2. 2.1 virtual clock and test control. Milestones 2 and 3 both depend on it.
3. Milestone 2, steps 2.2–2.5.
4. Milestone 3, steps 3.1–3.5.
5. Milestone 4, steps 4.1–4.6 (settlement before performance, D4).

## Decisions

Settled on 2026-10-14:
- **D1 — combined settlement mode:** yes, there is an inbound API. External outcomes are stored with
  `source: external` beside the simulated lifecycle and never merged into it (step 2.4).
- **D2 — T+N calendar:** weekends only. Holiday calendars can come later.
- **D4 (priority part):** the plan's order holds: Milestone 2 settlement, then Milestone 3, then Milestone 4.
- **D5 — Milestone 1 backlog:** all four items are needed soon: HMAC signing, ordered delivery, instrument `enabled`,
  participant settlement metadata (1.3a–d).

Still open:
- **D3 — scenario runner surface:** CLI only, API only, or both? Needed before 3.1.
- **D4 (rest):** what is the benchmark target hardware, and is "fast mode" `synchronous=OFF` or fully in-memory?
  Needed before 4.4 and 4.6.
