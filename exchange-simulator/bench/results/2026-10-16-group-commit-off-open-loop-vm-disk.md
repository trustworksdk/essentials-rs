# exchange-simulator benchmark

- Date: 2026-10-16 17:12 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 229 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| HTTP open loop, target 2000 orders/s | 20000 | 10.00 s | 1999 | p50 1972 µs · p99 399.8 ms · p99.9 419.4 ms · max 422.9 ms | target 2000/s for 10 s: 20000 sent, 20000 accepted, 0 × 503, 0 other failures; generator lag p99 2169 µs max 16.5 ms; server per order (mean / p99): queue wait 36.8 ms / ≤ 500.0 ms, exchange thread 313 µs / ≤ 2500 µs, ack 37.1 ms / ≤ 500.0 ms; commit 299 µs / ≤ 2500 µs per commit, no groups |
| HTTP open loop, target 4000 orders/s | 40000 | 10.74 s | 3725 | p50 99.9 ms · p99 825.4 ms · p99.9 832.0 ms · max 834.8 ms | target 4000/s for 10 s: 40000 sent, 40000 accepted, 0 × 503, 0 other failures; generator lag p99 1952 µs max 6367 µs; server per order (mean / p99): queue wait 179.6 ms / ≤ 1.00 s, exchange thread 254 µs / ≤ 2500 µs, ack 179.8 ms / ≤ 1.00 s; commit 239 µs / ≤ 2500 µs per commit, no groups |
