# Benchmarks (Milestone 1 baseline)

`src/bin/bench.rs` measures one seeded workload at three levels, so in-memory matching and durable throughput are
reported separately, as the plan requires:

| Scenario | What runs | What it measures |
|---|---|---|
| in-memory matching | `OrderBook::submit` in a loop | pure price-time matching; no sequencing service, no I/O |
| durable | `Exchange::submit` on a SQLite file, one order after the other | plan, commit (orders, executions, events, outbox), apply, for each order: the single-writer ceiling |
| durable + outbox | the same, plus one subscription to all six event types | the extra cost of writing a delivery row per event |
| HTTP end to end | the service on a TCP port, N concurrent `reqwest` clients posting `POST /api/orders` | acknowledged-after-commit throughput and client-observed latency |
| HTTP + webhook | the same, plus a local consumer subscribed to `TradeExecuted` | the cost of delivery running alongside, and when the last trade event was delivered |
| `checkpoint` (run on its own, not part of `all`) | durable with an observed auto-checkpoint, durable with auto-checkpoint off, raw fsync | which commits are slow, and why (item 1.2) |

```sh
# from the workspace root; always --release
cargo run --release -p exchange-simulator --bin bench -- all --dir /path/on/real/disk --out results.md
cargo run --release -p exchange-simulator --bin bench -- all --quick        # 200k / 10k / 10k orders
cargo run --release -p exchange-simulator --bin bench -- --help
```

**The workload:** 80 % limit orders priced uniformly within ±50 ticks of 100.00, 20 % market orders, buy and sell
equally likely, quantities 1–100, 50 participants, one instrument, seed 42. It produces about 0.9 executions and 3.3
events per order. Self-trades are rejected, which is why the HTTP runs end with slightly different
execution counts: concurrent clients interleave orders differently.

Since step 3.2 the workload is a random market from `exchange_simulator::market`, with this configuration (no
arrival rate, no cancellations). Its seeded stream differs from the hand-written generator behind the 2026-10-14
figures, but it has the same distributions and the same shape: 0.89 executions and 3.27 events per order at seed 42.

**`--dir` matters.** Each report states the measured cost of a 4 KiB write plus fsync in the database directory. On
tmpfs that cost is about 1 µs, and the "durable" rows then measure SQLite in RAM. In this devcontainer `/tmp` is tmpfs;
`/home/vscode` is overlay on the Docker VM's disk, which is what a Docker volume uses.

## Results, 2026-10-14

Measured inside the devcontainer: Docker Desktop VM (linuxkit kernel, aarch64), 8 CPUs, 15 GiB. Full reports are in
[`results/`](results/).

| | VM disk (fsync ≈ 200 µs) | tmpfs (no real fsync) |
|---|---:|---:|
| in-memory matching | **2.9 M orders/s** | 2.9 M orders/s |
| durable, no subscribers | **5,300–5,600 orders/s** (p50 140 µs, p99 1.2 ms) | 25,900 orders/s |
| durable + outbox (1 subscriber, all events) | **4,300–4,400 orders/s** (p50 178 µs, p99 1.6 ms) | 17,900 orders/s |
| HTTP, 32 clients | **4,850 orders/s** (p50 6.4 ms, p99 9.3 ms) | 19,000 orders/s |
| HTTP + `TradeExecuted` consumer | **3,740 orders/s** (p50 8.3 ms); every trade event delivered within the run | 13,200 orders/s |

The durable rows were repeated three times on the VM disk. The ranges are those runs. The very first run on a cold
disk was slower (1,857 orders/s) and is not counted.

**What this says:**
- **Durability is the limit.** Matching is about 500× faster than a durable commit. One SQLite writer with
  `synchronous=FULL` commits each order separately, so throughput is bounded by commits per second: roughly 5k/s on
  this virtual disk.
- **Against the 1k–10k orders/s target:** on this hardware the durable service sustains about 4–5k orders/s,
  acknowledged after commit, with outbox and webhook delivery running. That is inside the target range but well below
  10k. Without fsync cost (tmpfs) the same code does 13–19k/s end to end. The numbers will differ on bare-metal NVMe;
  measure there before quoting a figure for a specific deployment.
- **The outbox costs about 20 %** with a subscriber to every event type (3.3 extra rows per order). Webhook delivery
  through the same writer thread costs another 20 % of HTTP throughput.
- **Latency spikes come from the disk, not SQLite** (item 1.2, `bench checkpoint`,
  [`results/2026-10-14-checkpoint.md`](results/2026-10-14-checkpoint.md)). Maximum commit latencies of 57–180 ms recur
  on the VM disk. The experiment replaced SQLite's auto-checkpoint with an identical one that records which commits
  run a checkpoint:
  - **Checkpoint commits** (570 per 50k orders) took 1.7–2.9 ms p50 and at most 8.1 ms. None crossed 10 ms in three
    runs.
  - **Every commit over 10 ms** was an ordinary commit.
  - **Raw 4 KiB write + fsync on the same disk, without SQLite,** shows the same pattern: 6–52 fsyncs over 10 ms per
    50k, with maxima of 141–950 ms.

  The VM's virtual disk occasionally stalls an fsync. Re-measure on the target hardware before treating spikes as an
  application problem.
- **Checkpoints still cost something:** about 1.1 % of commits run one at roughly 1.7 ms, about 10 % of a durable
  run's time and most of its p99. Turning auto-checkpointing off is *not* the fix: the WAL grows without bound
  (2.2 GiB per 50k orders), the p50 commit latency more than doubles, and throughput halves (≈2.5k/s), because every
  commit then extends the WAL file instead of reusing it.

**Levers for Milestone 4** (not built): group commit, meaning several orders per transaction while still
acknowledging only after commit; moving the WAL checkpoint to a background connection, which keeps the WAL bounded
but takes it off the commit path; instrument-sharded matching in front of the writer; and the plan's explicitly
non-durable fast mode.
