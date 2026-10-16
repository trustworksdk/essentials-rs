# exchange-simulator benchmark

- Date: 2026-10-16 18:52 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, database.mode=fast, journal_mode=wal, synchronous=NORMAL, one writer thread, checkpoints in the background from 1000 pages
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 233 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| HTTP open loop, target 15000 orders/s | 150000 | 10.00 s | 14996 | p50 3007 µs · p99 91.2 ms · p99.9 103.3 ms · max 112.4 ms | target 15000/s for 10 s: 150000 sent, 150000 accepted, 0 × 503, 0 other failures; generator lag p99 3401 µs max 13.7 ms; server per order (mean / p99): queue wait 6764 µs / ≤ 100.0 ms, exchange thread 49 µs / ≤ 250 µs, ack 9999 µs / ≤ 100.0 ms; commit 136 µs / ≤ 1000 µs per commit, 5469 groups of 27.4 orders on average |
| HTTP open loop, target 20000 orders/s | 186527 | 10.59 s | 17618 | p50 534.6 ms · p99 728.6 ms · p99.9 770.3 ms · max 985.0 ms | target 20000/s for 10 s: 200000 sent, 186527 accepted, 13473 × 503, 0 other failures; generator lag p99 3626 µs max 36.2 ms; server per order (mean / p99): queue wait 402.3 ms / ≤ 1.00 s, exchange thread 50 µs / ≤ 250 µs, ack 410.1 ms / ≤ 1.00 s; commit 795 µs / ≤ 10.0 ms per commit, 792 groups of 235.5 orders on average |
| HTTP open loop, target 25000 orders/s | 193971 | 10.53 s | 18426 | p50 543.8 ms · p99 625.6 ms · p99.9 635.9 ms · max 639.6 ms | target 25000/s for 10 s: 250000 sent, 193971 accepted, 56029 × 503, 0 other failures; generator lag p99 2248 µs max 5086 µs; server per order (mean / p99): queue wait 480.5 ms / ≤ 1.00 s, exchange thread 47 µs / ≤ 250 µs, ack 488.1 ms / ≤ 1.00 s; commit 917 µs / ≤ 10.0 ms per commit, 767 groups of 252.9 orders on average |
| HTTP open loop, target 30000 orders/s | 195336 | 10.52 s | 18571 | p50 537.0 ms · p99 824.3 ms · p99.9 835.3 ms · max 849.6 ms | target 30000/s for 10 s: 300000 sent, 195336 accepted, 104664 × 503, 0 other failures; generator lag p99 2449 µs max 7171 µs; server per order (mean / p99): queue wait 497.0 ms / ≤ 1.00 s, exchange thread 48 µs / ≤ 250 µs, ack 504.3 ms / ≤ 1.00 s; commit 723 µs / ≤ 2500 µs per commit, 772 groups of 253.0 orders on average |
