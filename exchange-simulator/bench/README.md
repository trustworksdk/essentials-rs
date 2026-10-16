# Benchmarks (Milestone 1 baseline)

`src/bin/bench.rs` measures one seeded workload at three levels, so in-memory matching and durable throughput are
reported separately, as the plan requires:

| Scenario | What runs | What it measures |
|---|---|---|
| in-memory matching | `OrderBook::submit` in a loop | pure price-time matching; no sequencing service, no I/O |
| durable | `Exchange::submit` on a SQLite file, one order after the other | plan, commit (orders, executions, events, outbox), apply, for each order: the single-writer ceiling |
| durable + outbox | the same, plus one subscription to all six event types | the extra cost of writing a delivery row per event |
| durable, groups of N | the same two, committed in groups of `--group-commit` orders (default 256) | the commit ceiling with group commit; an order's latency is its group's |
| HTTP end to end | the service on a TCP port, N concurrent `reqwest` clients posting `POST /api/orders` | acknowledged-after-commit throughput and client-observed latency |
| HTTP + webhook | the same, plus a local consumer subscribed to `TradeExecuted` | the cost of delivery running alongside, and when the last trade event was delivered |
| `open-loop` (run on its own, not part of `all`) | the service on a TCP port, orders sent on a fixed schedule at each target rate | achieved rate, latency at that rate, 503s, and where the time went inside the service |
| `checkpoint` (run on its own, not part of `all`) | durable with an observed auto-checkpoint, durable with auto-checkpoint off, raw fsync | which commits are slow, and why (item 1.2) |

```sh
# from the workspace root; always --release
cargo run --release -p exchange-simulator --bin bench -- all --dir /path/on/real/disk --out results.md
cargo run --release -p exchange-simulator --bin bench -- all --quick        # 200k / 10k / 10k orders
cargo run --release -p exchange-simulator --bin bench -- open-loop --rates 1000,2000,4000 --seconds 10 --dir /path/on/real/disk
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

## Open loop and latency breakdown (step 4.1)

The `http` rows are closed loop: 32 clients each wait for an answer before sending the next order. When the service
slows down, the clients slow down with it, and the stall never shows up in the latencies (coordinated omission).
`open-loop` avoids that:
- **Fixed schedule:** order `i` is due at `start + i / rate` and is sent then, whether or not earlier orders have
  been answered. Its latency is measured from that due time, not from the actual send.
- **Generator health:** `generator lag` reports how late sends actually went out. Tokio's timer has 1 ms
  resolution, so about 1–2 ms is normal. A lag far above that means the generator, not the service, was the limit.
- **Client bound:** `--max-in-flight` (default 16384) caps open requests. At the cap the generator waits, and the
  wait counts as latency.
- **Shared process:** the generator and the service run in one process on one Tokio runtime, as in the `http` rows.
  4.6 moves the load generator into its own container.
- **Each row reports:**
  - the accepted orders and the achieved rate (accepted orders over the time to the last answer);
  - the client latency of accepted orders;
  - 503s (queue full) and other failures;
  - a server-side breakdown per submitted order, from `GET /metrics` before and after the run: mean and p99
    bucket bound of queue wait, exchange-thread time (of which the SQLite commit) and acknowledgement (enqueue
    to answer received).
- **Queue bound and 503s:** the service queue holds `--queue-capacity` commands (default 10000, as in
  `config/default.yaml`). 503s therefore start only once that many orders are waiting, which takes seconds at
  rates just above capacity. A smaller queue turns overload into 503s sooner and caps the latency.

### Results, 2026-10-16

Same devcontainer as above, 10 s per rate. Full reports:
[`results/2026-10-16-open-loop-vm-disk.md`](results/2026-10-16-open-loop-vm-disk.md) and
[`results/2026-10-16-open-loop-tmpfs.md`](results/2026-10-16-open-loop-tmpfs.md).

| Target | VM disk (fsync ≈ 300 µs) | tmpfs |
|---:|---|---|
| 1,000/s | 1,000/s · p50 1.8 ms · p99 5.0 ms | – |
| 2,000/s | 2,000/s · p50 1.4 ms · p99 7.1 ms | 2,000/s · p50 1.2 ms · p99 14.6 ms |
| 4,000/s | **saturated:** 3,850/s · p50 106 ms · p99 393 ms | – |
| 5,000/s | – | 5,000/s · p50 1.5 ms · p99 3.8 ms |
| 10,000/s | – | 10,000/s · p50 1.6 ms · p99 96 ms |
| 15,000/s | – | 15,000/s · p50 1.9 ms · p99 25 ms |
| 20,000/s | – | **saturated:** 15,050/s, 39,010 × 503 of 200,000 · p50 657 ms |

**What this says:**
- **Below capacity, latency stays low.** The p50 is 1–2 ms, of which the service accounts for well under 1 ms
  (acknowledgement mean 0.4–0.9 ms). The rest is HTTP and the client in the same process.
- **At capacity, queue wait is the whole story.** At 4,000/s on the VM disk the exchange thread still takes 244 µs
  per order (229 µs of it commit), but orders wait 145 ms on average in the queue. Latency then grows with the
  backlog until the queue is full. After that, orders are refused with 503 rather than queued, as at 20,000/s on
  tmpfs.
- **The durable ceiling is the commit, as in the baseline.** At 300 µs per fsync, about 3.9k orders/s; on tmpfs,
  about 15k/s. That is what group commit (4.2) targets: one commit for many orders.
- **Commit time per order falls as the rate rises** (958 µs at 500/s, 229 µs at 4,000/s on the VM disk). One
  plausible cause is that a busy writer keeps the disk and the caches warm. Measure again after 4.2 before relying
  on it.
- **A cold disk distorts the first run.** The first 500/s run in a fresh container had p99 452 ms and a generator
  lag of up to 158 ms, so the whole process stalled. A rerun gave p99 4.4 ms. The table uses the rerun.
- **The disk is slower than on 2026-10-14** (fsync 300–500 µs today, 200 µs then). Compare rows only within one
  report.

## Group commit (step 4.2)

The exchange thread commits the orders and cancellations already queued together, up to
`exchange.group_commit.max_commands` (default 256), and answers each caller after that commit. `--group-commit N` sets
it for the service rows and the grouped durable rows; `--group-commit 1` gives the earlier one-commit-per-order
behaviour. The breakdown now reports the commits separately: the time of each `COMMIT`, and how many groups ran and
their average size.

### Results, 2026-10-16

Same devcontainer, the same day as the open-loop results above, so the fsync cost differs from run to run (208–266 µs on
the VM disk). Full reports: `results/2026-10-16-group-commit-*.md`.

| | VM disk, group commit off | VM disk, group commit on | tmpfs, group commit on |
|---|---|---|---|
| `Exchange` alone, no subscribers | 2,200–2,800/s | **20,700/s** (groups of 256) | 24,700/s (21,100/s alone) |
| `Exchange` alone + outbox | 2,100–3,700/s | **13,800/s** (groups of 256) | 16,400/s (14,700/s alone) |
| HTTP, 32 clients | 4,850/s (2026-10-14) | **13,060/s**, p99 6.8 ms, groups of 29 | 17,340/s, p99 3.4 ms |
| HTTP + `TradeExecuted` consumer | 3,740/s (2026-10-14) | **9,650/s**, p99 7.5 ms; every webhook delivered | 12,270/s |
| Open loop, highest rate held | 2,000/s at p99 400 ms; 4,000/s saturated at 3,725/s | **12,000/s** at p99 11 ms; 16,000/s saturated at 15,600/s | 15,000/s at p99 17 ms; saturated at about 19,000/s |

**Group sizes follow the load.** At 2,000/s on the VM disk groups held 3.5 orders on average; at 8,000/s, 10; at
12,000/s, 19; at saturation, 92 (and 253 on tmpfs, close to the cap). At low load an order mostly commits alone, so
its latency is as before (p50 about 1.9 ms over HTTP). Nothing waits for a group to fill (`max_wait_us: 0`).

**What this says:**
- **The fsync is no longer the ceiling.** With group commit, the VM disk sustains 12,000 orders/s in the open loop
  (target 1k–10k), where it saturated below 4,000/s before. The `Exchange` alone does 4–9× more orders per second.
- **The next limit is the exchange thread's own work.** At saturation on tmpfs it spends about 46 µs per order,
  while the commit adds about 4 µs. Matching takes about 0.5 µs (2 M orders/s), so the 46 µs is mostly the SQLite
  statements: the order, its executions, about 3.3 events with their JSON payloads, and outbox rows. Sharding matching
  (4.5) would not help with that. Fewer or cheaper statements per order would.
- **The load generator shares the process,** so near saturation the HTTP stack and the generator compete with the
  exchange thread for CPU. 4.6 separates them.
- **Stalls of the whole process** still occur on the VM disk: one 2,000/s run had a generator lag of up to 194 ms, and
  its rerun had p99 5.4 ms. The first durable row of the `all` run was slower than the row with an outbox after it, a
  cold disk again. Each report notes this.

