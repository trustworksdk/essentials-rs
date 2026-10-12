# Benchmarks (Phase 7)

Java Essentials 0.60.0 against the Rust port: the same queue, the same SQL, the same load, under equal container
limits. The procedure is `run.py` (Python standard library only); each run writes `results/<UTC timestamp>/`.

```sh
python3 durable-queues-rs/bench/run.py           # full run, about 25 min
python3 durable-queues-rs/bench/run.py --quick   # the same procedure with short windows, about 6 min
python3 durable-queues-rs/bench/run.py --help    # levels, durations, variants, parallelism
```

Needs Docker with BuildKit. The images are built first: `interop/java-harness` (Java) and `interop/rust-node` (the
Rust `interop` binary, built in Docker from the workspace).

## What runs

| Container | Image | Limits | Role |
|---|---|---|---|
| `dq-bench-pg` | `postgres:17-alpine` with `pg_stat_statements` | 2 CPUs, 2 GiB | the one database, fresh for every run |
| `dq-bench-gen` | Rust node, `bench-produce` | 2 CPUs, 512 MiB | the workload generator, the same for both languages |
| `dq-bench-<variant>` | Java or Rust node, `bench-consume` | **2 CPUs, 1 GiB** | the consumer under test |

The consumer's handler does no work. It measures the latency since the generator sent the message (`placedAt`, set
at send time; every container reads the same host clock) and reports once a second.

**Variants:**
- `java`: `PostgresqlDurableQueues` with its defaults (centralized fetcher), JDBI on HikariCP.
- `rust-fixed-rate`: the Rust crate with `FetchPacing::FixedRate`, which polls like Java's centralized fetcher: one
  poll every 20 ms, claiming at most the free worker slots. **This is the language-to-language comparison.**
- `rust-eager`: the Rust crate's default `FetchPacing::Eager`. After a poll that filled every free slot, it polls
  again as soon as a slot frees up. It's an algorithm difference, reported apart.

**Held constant:** payload (one `OrderPlaced`, about 100 bytes of JSON), one queue, `parallel_consumers` (8 and
128, each a full run), a pool of at most 10 connections, a 20 ms polling interval, a 10 s warm-up and a 30 s
measurement per level, and the container limits above. The JVM runs with its defaults (heap: 25 % of the
container's memory).

**Procedure per variant:**
1. **Startup:** 1000 messages are queued; the time from `docker run` to the first message handled. Repeated 3 times,
   median reported.
2. **Idle:** CPU and memory over 15 s with an empty queue, after a 10 s settle.
3. **Levels:** 100, 1000 and 5000 msg/s, open loop.
   - The generator inserts what the rate owes every 10 ms, in one multi-row statement.
   - Measured: throughput, latency p50 / p90 / p99 / max, CPU and memory (`docker stats`), the consumer's
     connections (`pg_stat_activity`, by `application_name`), and `pg_stat_statements`, reset per level.
   - If the consumer fell behind, its backlog is recorded after a 15 s grace period and purged before the next
     level.
4. **Saturation:** 20 000 messages are queued, then a fresh consumer drains them. Reported: messages per second from
   the first one handled until the table is empty. It runs twice:
   - with the run's representative statistics (below);
   - with **stale statistics**: the table is analyzed while empty and autovacuum is off for it during the drain
     (see "Finding").

**Planner statistics are pinned.** Autovacuum is off for the queue table for the whole run. Before every level and
saturation run the table goes through the same reset: a manual `VACUUM` (keeping bloat bounded), then 20 000
messages are queued, the table is analyzed, and the messages are deleted. The stale-statistics run then analyzes the
emptied table again.

Pinning is benchmark-only, applied to both languages alike. It is needed because the claim's plan depends on the
statistics (see "Finding"), and autovacuum would otherwise reset them at its own moments. A `VACUUM` resets the
planner's row estimate to the rows left, which between bursts means none. Earlier runs without pinning were
discarded:
- With no control, two runs of the same variant differed 5× in drain rate.
- Analyzing at the start of every (empty) level made each level that built a backlog collapse.
- Switching off only autoanalyze still let autovacuum's `VACUUM` zero the estimate.

## SQL and settings: what differs

Recorded as the plan requires. Anything here that is not in Java is reported separately.

- **Claim, acknowledge, retry, stuck reset:** the same statements, verbatim from Java (CONTRACT.md §4–§6). Java's
  ordered-then-normal claim is also kept: the ordered claim runs first, and the normal one only if it returned
  nothing (Phase 5). The `consumer SQL` rows in the results are those statements only. At equal pacing, Java and
  Rust issue the same number of calls per message.
- **Fetch pacing:** `rust-fixed-rate` matches Java; `rust-eager` does not (above).
- **Fetcher:** Java has one fetcher per `DurableQueues` for all its queues, Rust one per consumer. With the one queue
  used here they poll the same way.
- **Connection pool:** HikariCP keeps all 10 connections open (its default `minimumIdle` = `maximumPoolSize`). sqlx
  opens connections on demand, up to 10. The idle connection counts differ for that reason, not because of the
  language.
- **The generator** batches its inserts. It is not the component under test; both languages consume the same load.

## Finding: the claim degrades with stale planner statistics (Java and Rust alike)

Java's unordered claim, used verbatim by both, ends in `UPDATE durable_queues q … FROM cte_unordered u WHERE q.id =
u.id AND q.queue_name = $1`. When the table's statistics say it is empty while its pages are still allocated (`reltuples` 0, `relpages` > 0: an
analyze or vacuum right after a burst was deleted; `rows=1` in the plan), PostgreSQL
plans that join as a nested loop over an index scan of **every row of the queue**, filtered on `u.id = q.id`. The
plan is captured with `--auto-explain-ms` (`raw/postgres-*.log`). Each claim then costs time proportional to the
backlog: about 42 ms with 20 000 queued, against 1–2 ms with current statistics, and a backlog drains about 5×
slower until autoanalyze catches up. Java's plan picks a different index for the CTE, because it binds the boolean
filters as parameters, but has the same nested loop.

A queue that is usually empty and suddenly fills up is exactly this case. It is a property of the shared SQL, not of
either language. `saturation_stale_stats` measures it for both.

## Reading the results

- `results/<run>/results.md`: the tables.
- `results/<run>/summary.json`: every number, plus the top `pg_stat_statements` per level.
- `results/<run>/raw/*.jsonl.gz`: every event the consumers printed (including each message's latency), every
  `docker stats` sample and every connection count, timestamped.

`docker stats` reports CPU as a percentage of one core, so 200 % is both of the container's CPUs.
