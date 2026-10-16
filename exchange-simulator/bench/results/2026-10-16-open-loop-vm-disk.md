# exchange-simulator benchmark

- Date: 2026-10-16 16:35 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 307 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| HTTP open loop, target 500 orders/s | 5000 | 10.00 s | 500 | p50 1843 µs · p99 451.9 ms · p99.9 461.3 ms · max 465.1 ms | target 500/s for 10 s: 5000 sent, 5000 accepted, 0 × 503, 0 other failures; generator lag p99 58.9 ms max 158.2 ms; server per order (mean / p99): queue wait 47.6 ms / ≤ 500.0 ms, exchange thread 992 µs / ≤ 10.0 ms (commit 958 µs / ≤ 10.0 ms), ack 48.7 ms / ≤ 500.0 ms |
| HTTP open loop, target 1000 orders/s | 10000 | 10.00 s | 1000 | p50 1845 µs · p99 5047 µs · p99.9 20.2 ms · max 25.5 ms | target 1000/s for 10 s: 10000 sent, 10000 accepted, 0 × 503, 0 other failures; generator lag p99 1835 µs max 7550 µs; server per order (mean / p99): queue wait 174 µs / ≤ 5000 µs, exchange thread 416 µs / ≤ 2500 µs (commit 395 µs / ≤ 2500 µs), ack 609 µs / ≤ 5000 µs |
| HTTP open loop, target 1500 orders/s | 15000 | 10.00 s | 1500 | p50 1575 µs · p99 10.7 ms · p99.9 19.3 ms · max 21.7 ms | target 1500/s for 10 s: 15000 sent, 15000 accepted, 0 × 503, 0 other failures; generator lag p99 1894 µs max 3917 µs; server per order (mean / p99): queue wait 475 µs / ≤ 10.0 ms, exchange thread 333 µs / ≤ 2500 µs (commit 316 µs / ≤ 2500 µs), ack 821 µs / ≤ 10.0 ms |
| HTTP open loop, target 2000 orders/s | 20000 | 10.01 s | 1999 | p50 1442 µs · p99 7126 µs · p99.9 16.5 ms · max 20.3 ms | target 2000/s for 10 s: 20000 sent, 20000 accepted, 0 × 503, 0 other failures; generator lag p99 1661 µs max 3122 µs; server per order (mean / p99): queue wait 549 µs / ≤ 10.0 ms, exchange thread 291 µs / ≤ 2500 µs (commit 276 µs / ≤ 2500 µs), ack 856 µs / ≤ 10.0 ms |
| HTTP open loop, target 4000 orders/s | 40000 | 10.38 s | 3853 | p50 105.8 ms · p99 393.1 ms · p99.9 396.7 ms · max 397.6 ms | target 4000/s for 10 s: 40000 sent, 40000 accepted, 0 × 503, 0 other failures; generator lag p99 1864 µs max 4722 µs; server per order (mean / p99): queue wait 145.1 ms / ≤ 500.0 ms, exchange thread 244 µs / ≤ 2500 µs (commit 229 µs / ≤ 2500 µs), ack 145.4 ms / ≤ 500.0 ms |

Note: the 500 orders/s row was the first run on a cold disk in a fresh container (see `bench/README.md`). A rerun gave p50 1737 µs, p99 4404 µs, max 6968 µs, with a generator lag of at most 3 ms.
