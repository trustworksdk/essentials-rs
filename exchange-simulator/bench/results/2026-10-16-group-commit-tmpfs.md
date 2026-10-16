# exchange-simulator benchmark

- Date: 2026-10-16 17:13 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread
- Database directory: `/tmp/bench-tmp`; 4 KiB write + fsync there: 1 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| in-memory matching (`OrderBook`) | 1000000 | 0.50 s | 1993393 | – | 903447 executions, 68292 resting at end |
| durable, no subscribers (`Exchange` + SQLite) | 50000 | 2.37 s | 21127 | p50 40 µs · p99 206 µs · p99.9 363 µs · max 2505 µs | 45145 executions, 165273 events, 0 outbox rows |
| durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite) | 50000 | 3.39 s | 14743 | p50 59 µs · p99 262 µs · p99.9 396 µs · max 1046 µs | 45145 executions, 165273 events, 165273 outbox rows |
| durable, no subscribers (`Exchange` + SQLite), groups of 256 | 50000 | 2.03 s | 24675 | p50 10.0 ms · p99 13.7 ms · p99.9 15.7 ms · max 15.7 ms | 45145 executions, 165273 events, 0 outbox rows |
| durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite), groups of 256 | 50000 | 3.04 s | 16439 | p50 15.3 ms · p99 20.6 ms · p99.9 22.1 ms · max 22.1 ms | 45145 executions, 165273 events, 165273 outbox rows |
| HTTP end to end (`POST /api/orders`) | 50000 | 2.88 s | 17338 | p50 1777 µs · p99 3419 µs · p99.9 3932 µs · max 7341 µs | 32 clients, 0 non-2xx, 45119 executions; server per order (mean / p99): queue wait 706 µs / ≤ 2500 µs, exchange thread 43 µs / ≤ 250 µs, ack 1655 µs / ≤ 5000 µs; commit 165 µs / ≤ 2500 µs per commit, 1702 groups of 29.4 orders on average |
| HTTP end to end + `TradeExecuted` webhook consumer | 50000 | 4.08 s | 12268 | p50 2524 µs · p99 5005 µs · p99.9 6952 µs · max 9050 µs | 32 clients, 0 non-2xx, 45092 executions; server per order (mean / p99): queue wait 1247 µs / ≤ 5000 µs, exchange thread 58 µs / ≤ 250 µs, ack 2403 µs / ≤ 5000 µs; commit 182 µs / ≤ 2500 µs per commit, 1640 groups of 30.5 orders on average; 45092 of 45092 `TradeExecuted` webhooks delivered 4.11 s after the first order |
