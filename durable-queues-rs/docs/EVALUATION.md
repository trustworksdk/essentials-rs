# Durable Queues in Rust — Evaluation Report (Phase 8)

The answer to [`rust_durable_queues_experiment.md`](../rust_durable_queues_experiment.md): a Rust port of a focused
subset of Java Essentials 0.60.0 Durable Queues (`postgresql-queue`), built phase by phase from
[`IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md). The plan's phase sections hold the details this report
summarizes. Replacing Java is not a goal, and this report does not recommend it.

## Success criteria

| Criterion (experiment doc) | Result |
|---|---|
| Reproduces the selected Durable Queues semantics | **Met.** Enqueue, claim (`FOR UPDATE SKIP LOCKED`, the SQL verbatim from Java), bounded concurrent handling, ack, retry with delayed delivery, dead letters, resurrection, stuck-message reset and ordered messages. The behaviour is pinned in [`CONTRACT.md`](CONTRACT.md). Java's own tests were ported or reproduced: see [Tests](#tests). |
| Interoperates safely with Java against the same schema | **Met.** 13 automated interop tests against the real Java 0.60.0 cover both producer directions, competing consumers, a crash on either side recovered by the other, retry, delay and DLQ written by one side and read by the other, clock skew, and golden fixtures. All pass. |
| Provides evidence to evaluate the trade-offs | **Met**, with one gap: effort was not tracked as it happened. It is reconstructed below from session timestamps. |

## The comparison table

Filled in from the benchmark run `bench/results/20261012T064824Z/` (method: [`bench/README.md`](../bench/README.md)).
The consumers ran with 2 CPUs and 1 GiB each against one PostgreSQL 17, with a pool of 10 connections, 20 ms polling and
a no-op handler. "Rust" is `rust-fixed-rate`, which polls like Java's centralized fetcher, so it issues the same SQL at
the same rate. Rust's default `Eager` pacing is an algorithm difference, reported in its own row.

| Metric | Java Essentials 0.60.0 | Rust |
|---|---|---|
| Idle memory | 108–125 MiB | 2.4–3.6 MiB |
| Memory under load | 138 MiB (1 queue × 8) to 425 MiB (10 queues × 128) | 3.3 to 18 MiB |
| Startup to first message handled | 484–554 ms | 68–72 ms |
| Messages/second (same pacing) | Identical at every level that fits under the polling ceiling. Saturation drain: 5.5k/s vs 6.1k/s (1 queue × 128 consumers); **6.6k/s vs 16.9k/s** (10 queues × 128), where Java is CPU-bound at 199 % of its 2 CPUs | see left |
| Messages/second, Rust `Eager` (algorithm difference) | — | No polling ceiling: 1 queue × 8 consumers takes 5000/s offered (4.1k/s handled) where both fixed-rate consumers cap at 400/s. Saturation drain up to 16.8k/s |
| p50 / p99 enqueue→handle latency | 11.8–16.6 / 25–29 ms | 11.5–14.3 / 22–29 ms |
| CPU | at 5000 msg/s, 1 queue × 128: 60 %; 10 queues × 128: 66 % | 34 %; 37 % |
| Database connections | 10 always (HikariCP keeps `minimumIdle` = max) | 1–2 idle with one queue, 10 under load (sqlx opens on demand) |
| Database load | Same statements, same calls/s per statement | Same. With 10 queues, Rust's 10 independent fetchers make every statement about 2× slower per call (see Operations) |
| Container image / artifact | 496 MiB / 8.1 MiB fat jar (needs a JRE) | 135 MiB / 4.1 MiB static release binary |
| Implementation effort | Not comparable: built and hardened over years, and the reference for this port | about 7.5 h of active time over two days, AI-assisted (see Effort) |
| Code size | about 4.6k lines in scope, 7.0k in all (see Code size) | 2.4k lines of library, 4.3k lines of tests |

"In scope" means the parts of Java that correspond to what was ported. All ranges cover the four configurations: 1 or
10 queues × 8 or 128 consumers per queue.

**Conclusion from the numbers:** throughput and latency are decided by the shared SQL and the polling design, not by
the language. With the same SQL at the same pace the two match. The language/runtime shows in footprint: about 40×
less idle memory, 7× faster startup, roughly half the CPU at load, and an image a quarter the size. The large
throughput gaps (polling ceiling, 10-queue saturation) come from algorithm and design, and are reported as such.

## Effort

Effort was not recorded as hours while the work happened; the plan asked for it from Phase 1 onwards. The figures
below are reconstructed from the timestamps of the Claude Code session transcripts, cut at each phase's commit.
*Active* time sums the gaps between consecutive transcript events, capping each gap at 15 minutes, so a coffee break
or an overnight pause does not count. One developer drove the work with Claude Code, which wrote the code; the
developer steered, reviewed and approved each phase.

| Phase | Active | Elapsed | Prompts |
|---|---|---|---|
| 0 Contract + 1 Skeleton and schema | 1.1 h | 1.3 h | 17 |
| 2 Enqueue → claim → handle → ack | 0.2 h | 0.2 h | 2 |
| 3 Retries, delays, DLQ, stuck messages, `QueueStorage` | 0.6 h | 1.3 h | 1 |
| 4 Tracing, Java test port, edge cases | 0.6 h | 0.6 h | 3 |
| 5 Ordered messages | 0.6 h | 0.8 h | 3 |
| 6 Java ↔ Rust interop and crash recovery | 0.8 h | 0.9 h | 2 |
| 7 Benchmarks | 3.4 h | 12.5 h (overnight) | 13 |
| **Total, Phases 0–7** | **about 7.3 h** | | |

Caveats:
- The earliest transcript starts at 14:23 UTC on 2026-10-11. Any Phase 0 work before then (the workspace commit
  is from 09:07 UTC) is not counted.
- The time subagents ran in parallel is not added on top.
- Phase 7 includes about 1–2 h of benchmark runs. It also includes runs that were discarded once uncontrolled
  planner statistics turned out to vary 5× between runs.

These hours measure an AI-assisted port that had a complete, readable reference implementation and its test suite
to work from. They say little about writing the component from scratch in either language.

## Code size

Counted with `tokei` (code lines only, no comments or blanks).

| | Java 0.60.0 | Rust port |
|---|---|---|
| Library, everything | 6 997: `postgresql-queue` 2 203 + `foundation/messaging/queue` 4 794 | 2 430 (`src/`, without `#[cfg(test)]` modules) |
| Library, roughly the ported scope | about 4 600 (without the admin/query API, operations, statistics, Micrometer, observability, health, interceptors, LISTEN/NOTIFY) | 2 430 |
| Tests | 5 235: `postgresql-queue/src/test` 3 464 + `foundation-test` queue bases 1 771 | 4 308: `tests/` 3 970 + unit tests in `src/` about 340 |
| Interop and benchmark tooling | — | `interop` binary 427, Java harness 343, `bench/run.py` 463 |

The Java "ported scope" figure is an estimate by exclusion. It still contains two fetchers (centralized and
traditional) and the LISTEN/NOTIFY polling optimizer, which the port does not have, so the like-for-like ratio is
smaller than 4.6k to 2.4k suggests.

## Tests

- **Rust:** 98 tests, all green: 26 unit tests and 72 integration tests in 8 suites, each test with its own
  Testcontainers PostgreSQL. 13 of the integration tests are Java ↔ Rust interop tests against the real Java
  harness.
- **Java's suites, mapped** in [`JAVA_TEST_CHECKLIST.md`](JAVA_TEST_CHECKLIST.md). It lists 34 test methods: 18
  ported with the same scenario and assertions, 9 reproduced with a different setup, 1 ported in Phase 5, and 6
  N/A (features outside the scope, such as interceptors and the admin API).
- **Mutation checks** for the key guarantees. The guarantee was broken on purpose and the test failed each time: the
  claim's delay predicate, stuck-message reset (removed, and at the wrong time), the ordered key exclusion, the panic
  guard, log redaction, and `FetchPacing::FixedRate`.

## Bugs found

**In Java Essentials 0.60.0:**
1. **`DefaultQueuedMessage.getDeliveryMode()` always returns `NORMAL`** (`DefaultQueuedMessage.java:111`), even for
   ordered messages, whose `getMessage()` is a correct `OrderedMessage`. Ordering is unaffected; only a handler that
   asks for the delivery mode gets the wrong answer. Pinned by the interop test
   `java_reports_every_queued_message_as_normal`. Not yet reported upstream.

**In the shared SQL** (affects Java and Rust alike; a performance property rather than a correctness bug):
2. **The unordered claim degrades with stale planner statistics.** When the table was analyzed or vacuumed while
   empty, PostgreSQL plans `UPDATE … FROM cte_unordered u WHERE q.id = u.id` as a nested loop over the whole queue.
   Each claim then costs time proportional to the backlog: 40–54 ms per claim with 20 000 queued on one queue,
   against 1–2 ms. Draining is 5× slower until autoanalyze catches up. A queue that is usually empty and suddenly
   fills up is exactly this case. Recorded in CONTRACT.md §4.1.

**A Java design property, not a bug:**
3. **Polling ceiling.** Java's fetcher polls at a fixed rate, so one consumer handles at most
   `parallelConsumers / pollingInterval` messages per second: 400/s with 8 slots and 20 ms. Above that the backlog
   grows without bound (the 1000 msg/s level left 18k messages behind). Raising `parallelConsumers` is the remedy.
   Documented in CONTRACT.md.

**In the Rust port** (each caught by its own tests during the phase, none reached a later phase):
1. The first panic guard wrapped only the handler's future. A closure that panicked before returning its future
   escaped it, and its message stayed claimed. (Phase 3)
2. The "cannot decode message" log line leaked payload values, because serde_json's errors quote the input. Logs now
   carry a redacted error. (Phase 4)
3. A `debug_assert!` for the "key already in flight" invariant killed the fetch loop in debug builds. It now logs
   an error. (Phase 5)

The Phase 6 interop tests found no Rust bugs: the golden fixtures matched Java's rows on the first run. Two races
in the test code were fixed.

## Operational notes

- **Footprint is where Rust wins clearly.** Memory: 3–18 MiB against 110–425 MiB. Startup: 70 ms against 500 ms.
  Image: 135 MiB against 496 MiB. For many small worker processes, or tight container limits, that is material. The
  JVM ran with default settings (heap capped at 25 % of the container), so its memory could be tuned down, at the
  cost of tuning work.
- **Java's thread-per-slot model costs memory as slots grow.** At 10 queues × 128 slots Java runs a worker thread per slot, 1 280 in all,
  using 400+ MiB and 2 CPUs while draining. Rust's tasks cost about 16 MiB in the same setup.
- **Fetcher design.** Java has one centralized fetcher for all queues; Rust has one fetcher per consumer. With 10
  queues, Rust's independent fetchers contend on the pool and the database. Per call, its statements, and even the
  shared generator's inserts, ran about 2× slower than with Java's single loop. Rust's idle CPU was also higher, 14 %
  against 5 %. A likely contributor, not verified: sqlx sends `BEGIN` / `COMMIT` as separate round trips, and they show
  in `pg_stat_statements`. A shared fetcher would be the Rust follow-up if many queues per process is a real use.
- **Connection pools behave differently by default.** HikariCP keeps all 10 connections open; sqlx opens them on
  demand. Count this when sizing `max_connections` for a mix of nodes.
- **Mixed Java/Rust queues are safe** if every consumer on a queue knows every payload type. An unknown
  `message_payload_type` is dead-lettered by whichever node claims it; this is equally true between two Java nodes
  (CONTRACT.md §12).
- **What Java has and the port does not:**
  - the admin and query API, queue statistics and TTL jobs (all out of scope by the experiment);
  - Micrometer metrics and health indicators;
  - interceptors, LISTEN/NOTIFY and Spring Boot configuration.
  The port has `tracing` spans per storage operation, and nothing else for operations yet. An operations team gets a
  much richer toolkit from Java today.

## Ergonomics and maintainability

Assessed as the people who did the port, not measured:
- **Explicit wiring** (`DurableQueues::new(PostgresQueueStorage::new(pool, …))`, `ConsumerConfig` builders) was
  enough. Nothing like Spring's dependency injection was missed for a library.
- **The `QueueStorage` trait** gave a clean seam between queue logic and SQL. `DurableQueues` and the consumer are
  generic over it, and only the Postgres-specific "join the caller's transaction" methods are tied to Postgres.
- **The type system** helped in specific places:
  - `QueueMessage` dispatches on Java's FQCNs.
  - Decode errors are typed and redacted.
  - Bounded concurrency is a `JoinSet` sized to free slots, so claimed messages never sit in a local buffer and age
    towards the stuck timeout.
  No typestate was introduced, as the experiment advised.
- **The hard parts were the same in both languages:** SQL semantics, `>` vs `>=` on the retry limit, timestamp
  precision, clock source, planner behaviour. They needed CONTRACT.md and tests against the real Java, whatever the
  language. The experiment doc's caveat held: the database dominated.
- **Team cost:** the port is about a third the size of Java's library, but the team knows Java and Spring. Maintaining
  a second implementation means keeping the shared SQL and JSON shape in sync with Java. The interop suite exists to
  guard that, and it needs Docker plus the Java harness to run.

## Recommendation

1. **Keep Java Essentials as the reference implementation.** Nothing here argues for replacing it, and the experiment
   did not set out to.
2. **The Rust port is a credible option where footprint matters:** many small consumers, sidecars, edge or batch
   workers, or services already written in Rust that need to join an Essentials queue. On the same database it is
   proven interoperable and at least as fast at the same pace, with a far smaller footprint. Before production use it
   needs:
   - operational surface: metrics and the minimum of the admin API an operator needs, such as counts and resurrecting
     dead letters;
   - a shared fetcher if processes consume many queues.
3. **Act on the findings for Java, independent of Rust:**
   - Report the `getDeliveryMode()` bug upstream.
   - Investigate the stale-statistics claim plan in the shared SQL. Candidates: a rewrite that keeps the join on
     the primary key regardless of estimates, or guidance on autovacuum and analyze settings for queue tables.
     Production queues that idle empty then burst are exposed to it today.
   - Document the polling ceiling (`parallelConsumers / pollingInterval`) for users who size consumers.
4. **If the experiment continues,** the next useful evidence is a realistic handler (I/O-bound, a few ms per
   message) and a multi-node mix of Java and Rust. Either would show whether the footprint advantage holds once
   handler work, not the queue, dominates.
