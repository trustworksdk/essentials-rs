# exchange-simulator benchmark

- Date: 2026-10-16 18:19 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread, checkpoints in the background from 1000 pages
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 200 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| HTTP open loop, target 8000 orders/s | 80000 | 10.00 s | 7997 | p50 2014 µs · p99 9061 µs · p99.9 38.8 ms · max 47.3 ms | target 8000/s for 10 s: 80000 sent, 80000 accepted, 0 × 503, 0 other failures; generator lag p99 2094 µs max 9329 µs; server per order (mean / p99): queue wait 392 µs / ≤ 5000 µs, exchange thread 47 µs / ≤ 250 µs, ack 1305 µs / ≤ 10.0 ms; commit 454 µs / ≤ 2500 µs per commit, 8315 groups of 9.6 orders on average |
| HTTP open loop, target 12000 orders/s | 120000 | 10.01 s | 11994 | p50 2520 µs · p99 9349 µs · p99.9 13.3 ms · max 17.0 ms | target 12000/s for 10 s: 120000 sent, 120000 accepted, 0 × 503, 0 other failures; generator lag p99 1930 µs max 3792 µs; server per order (mean / p99): queue wait 603 µs / ≤ 5000 µs, exchange thread 44 µs / ≤ 250 µs, ack 1765 µs / ≤ 10.0 ms; commit 491 µs / ≤ 5000 µs per commit, 7304 groups of 16.4 orders on average |
| HTTP open loop, target 16000 orders/s | 160000 | 10.00 s | 15996 | p50 4050 µs · p99 17.3 ms · p99.9 20.9 ms · max 24.1 ms | target 16000/s for 10 s: 160000 sent, 160000 accepted, 0 × 503, 0 other failures; generator lag p99 2004 µs max 4357 µs; server per order (mean / p99): queue wait 1224 µs / ≤ 5000 µs, exchange thread 43 µs / ≤ 250 µs, ack 3932 µs / ≤ 25.0 ms; commit 744 µs / ≤ 5000 µs per commit, 3412 groups of 46.9 orders on average |
