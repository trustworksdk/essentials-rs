# exchange-simulator benchmark

- Date: 2026-10-16 18:51 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, database.mode=durable, journal_mode=wal, synchronous=FULL, one writer thread, checkpoints in the background from 1000 pages
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 203 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| HTTP open loop, target 15000 orders/s | 150000 | 10.01 s | 14988 | p50 4141 µs · p99 45.5 ms · p99.9 55.7 ms · max 221.7 ms | target 15000/s for 10 s: 150000 sent, 150000 accepted, 0 × 503, 0 other failures; generator lag p99 2012 µs max 9116 µs; server per order (mean / p99): queue wait 2777 µs / ≤ 50.0 ms, exchange thread 46 µs / ≤ 250 µs, ack 6259 µs / ≤ 50.0 ms; commit 747 µs / ≤ 5000 µs per commit, 3433 groups of 43.7 orders on average |
| HTTP open loop, target 20000 orders/s | 170433 | 10.57 s | 16125 | p50 579.8 ms · p99 834.5 ms · p99.9 860.9 ms · max 951.9 ms | target 20000/s for 10 s: 200000 sent, 170433 accepted, 29567 × 503, 0 other failures; generator lag p99 3313 µs max 11.5 ms; server per order (mean / p99): queue wait 519.8 ms / ≤ 1.00 s, exchange thread 50 µs / ≤ 250 µs, ack 529.0 ms / ≤ 1.00 s; commit 2291 µs / ≤ 25.0 ms per commit, 677 groups of 251.7 orders on average |
| HTTP open loop, target 25000 orders/s | 173461 | 10.55 s | 16436 | p50 577.1 ms · p99 811.7 ms · p99.9 825.6 ms · max 830.3 ms | target 25000/s for 10 s: 250000 sent, 173461 accepted, 76539 × 503, 0 other failures; generator lag p99 2532 µs max 6822 µs; server per order (mean / p99): queue wait 554.3 ms / ≤ 1.00 s, exchange thread 46 µs / ≤ 250 µs, ack 563.6 ms / ≤ 1.00 s; commit 2851 µs / ≤ 25.0 ms per commit, 686 groups of 252.9 orders on average |
| HTTP open loop, target 30000 orders/s | 155202 | 10.55 s | 14707 | p50 707.5 ms · p99 903.8 ms · p99.9 922.5 ms · max 926.2 ms | target 30000/s for 10 s: 300000 sent, 155202 accepted, 144798 × 503, 0 other failures; generator lag p99 2330 µs max 4738 µs; server per order (mean / p99): queue wait 630.8 ms / ≤ 1.00 s, exchange thread 46 µs / ≤ 250 µs, ack 642.0 ms / ≤ 1.00 s; commit 4770 µs / ≤ 25.0 ms per commit, 614 groups of 252.8 orders on average |
