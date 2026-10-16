# exchange-simulator benchmark

- Date: 2026-10-16 16:35 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread
- Database directory: `/tmp/bench-tmp`; 4 KiB write + fsync there: 1 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| HTTP open loop, target 2000 orders/s | 20000 | 10.00 s | 2000 | p50 1195 µs · p99 14.6 ms · p99.9 24.5 ms · max 29.6 ms | target 2000/s for 10 s: 20000 sent, 20000 accepted, 0 × 503, 0 other failures; generator lag p99 10.0 ms max 29.3 ms; server per order (mean / p99): queue wait 254 µs / ≤ 5000 µs, exchange thread 93 µs / ≤ 500 µs (commit 74 µs / ≤ 500 µs), ack 379 µs / ≤ 5000 µs |
| HTTP open loop, target 5000 orders/s | 50000 | 10.00 s | 4999 | p50 1497 µs · p99 3758 µs · p99.9 20.1 ms · max 30.1 ms | target 5000/s for 10 s: 50000 sent, 50000 accepted, 0 × 503, 0 other failures; generator lag p99 2154 µs max 15.5 ms; server per order (mean / p99): queue wait 377 µs / ≤ 2500 µs, exchange thread 85 µs / ≤ 500 µs (commit 67 µs / ≤ 500 µs), ack 485 µs / ≤ 2500 µs |
| HTTP open loop, target 10000 orders/s | 100000 | 10.00 s | 9997 | p50 1597 µs · p99 96.3 ms · p99.9 105.6 ms · max 107.3 ms | target 10000/s for 10 s: 100000 sent, 100000 accepted, 0 × 503, 0 other failures; generator lag p99 2037 µs max 15.2 ms; server per order (mean / p99): queue wait 4142 µs / ≤ 100.0 ms, exchange thread 69 µs / ≤ 500 µs (commit 55 µs / ≤ 500 µs), ack 4230 µs / ≤ 100.0 ms |
| HTTP open loop, target 15000 orders/s | 150000 | 10.00 s | 14997 | p50 1859 µs · p99 25.2 ms · p99.9 26.8 ms · max 27.6 ms | target 15000/s for 10 s: 150000 sent, 150000 accepted, 0 × 503, 0 other failures; generator lag p99 1701 µs max 6144 µs; server per order (mean / p99): queue wait 1980 µs / ≤ 25.0 ms, exchange thread 63 µs / ≤ 500 µs (commit 51 µs / ≤ 500 µs), ack 2057 µs / ≤ 25.0 ms |
| HTTP open loop, target 20000 orders/s | 160990 | 10.70 s | 15049 | p50 656.9 ms · p99 802.1 ms · p99.9 810.5 ms · max 980.1 ms | target 20000/s for 10 s: 200000 sent, 160990 accepted, 39010 × 503, 0 other failures; generator lag p99 3276 µs max 53.8 ms; server per order (mean / p99): queue wait 553.4 ms / ≤ 1.00 s, exchange thread 65 µs / ≤ 500 µs (commit 53 µs / ≤ 500 µs), ack 553.5 ms / ≤ 1.00 s |
