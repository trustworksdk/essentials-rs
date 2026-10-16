# exchange-simulator benchmark

- Date: 2026-10-16 17:14 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread
- Database directory: `/tmp/bench-tmp`; 4 KiB write + fsync there: 1 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| HTTP open loop, target 15000 orders/s | 150000 | 10.00 s | 14997 | p50 2200 µs · p99 17.3 ms · p99.9 32.3 ms · max 49.1 ms | target 15000/s for 10 s: 150000 sent, 150000 accepted, 0 × 503, 0 other failures; generator lag p99 2034 µs max 9122 µs; server per order (mean / p99): queue wait 551 µs / ≤ 5000 µs, exchange thread 46 µs / ≤ 250 µs, ack 1463 µs / ≤ 25.0 ms; commit 123 µs / ≤ 2500 µs per commit, 8393 groups of 17.9 orders on average |
| HTTP open loop, target 20000 orders/s | 199275 | 10.51 s | 18957 | p50 134.7 ms · p99 553.5 ms · p99.9 561.8 ms · max 577.4 ms | target 20000/s for 10 s: 200000 sent, 199275 accepted, 725 × 503, 0 other failures; generator lag p99 2424 µs max 8008 µs; server per order (mean / p99): queue wait 152.4 ms / ≤ 1.00 s, exchange thread 46 µs / ≤ 250 µs, ack 159.6 ms / ≤ 1.00 s; commit 911 µs / ≤ 5000 µs per commit, 904 groups of 220.4 orders on average |
| HTTP open loop, target 25000 orders/s | 191052 | 10.54 s | 18126 | p50 548.9 ms · p99 625.5 ms · p99.9 632.3 ms · max 635.0 ms | target 25000/s for 10 s: 250000 sent, 191052 accepted, 58948 × 503, 0 other failures; generator lag p99 2302 µs max 6131 µs; server per order (mean / p99): queue wait 492.1 ms / ≤ 1.00 s, exchange thread 48 µs / ≤ 250 µs, ack 499.9 ms / ≤ 1.00 s; commit 1096 µs / ≤ 5000 µs per commit, 755 groups of 253.0 orders on average |
| HTTP open loop, target 30000 orders/s | 196818 | 10.57 s | 18627 | p50 537.7 ms · p99 643.3 ms · p99.9 653.5 ms · max 661.2 ms | target 30000/s for 10 s: 300000 sent, 196818 accepted, 103182 × 503, 0 other failures; generator lag p99 2056 µs max 6669 µs; server per order (mean / p99): queue wait 496.6 ms / ≤ 1.00 s, exchange thread 47 µs / ≤ 250 µs, ack 504.2 ms / ≤ 1.00 s; commit 1047 µs / ≤ 5000 µs per commit, 777 groups of 253.3 orders on average |
