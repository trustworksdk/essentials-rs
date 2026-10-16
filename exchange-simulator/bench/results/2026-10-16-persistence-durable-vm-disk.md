# exchange-simulator benchmark

- Date: 2026-10-16 18:50 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, database.mode=durable, journal_mode=wal, synchronous=FULL, one writer thread, checkpoints in the background from 1000 pages
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 204 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| in-memory matching (`OrderBook`) | 1000000 | 0.41 s | 2410031 | – | 903447 executions, 68292 resting at end |
| durable, no subscribers (`Exchange` + SQLite) | 50000 | 21.03 s | 2378 | p50 154 µs · p99 2170 µs · p99.9 5027 µs · max 138.4 ms | 45145 executions, 165273 events, 0 outbox rows |
| durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite) | 50000 | 16.19 s | 3089 | p50 216 µs · p99 1971 µs · p99.9 3789 µs · max 35.3 ms | 45145 executions, 165273 events, 165273 outbox rows |
| durable, no subscribers (`Exchange` + SQLite), groups of 256 | 50000 | 2.18 s | 22889 | p50 10.9 ms · p99 15.4 ms · p99.9 15.5 ms · max 15.5 ms | 45145 executions, 165273 events, 0 outbox rows |
| durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite), groups of 256 | 50000 | 3.30 s | 15148 | p50 16.6 ms · p99 22.6 ms · p99.9 23.1 ms · max 23.1 ms | 45145 executions, 165273 events, 165273 outbox rows |
| HTTP end to end (`POST /api/orders`) | 50000 | 3.58 s | 13948 | p50 2173 µs · p99 4803 µs · p99.9 6632 µs · max 8368 µs | 32 clients, 0 non-2xx, 44967 executions; server per order (mean / p99): queue wait 731 µs / ≤ 2500 µs, exchange thread 42 µs / ≤ 250 µs, ack 2100 µs / ≤ 5000 µs; commit 582 µs / ≤ 5000 µs per commit, 1769 groups of 28.3 orders on average |
| HTTP end to end + `TradeExecuted` webhook consumer | 50000 | 5.11 s | 9784 | p50 3105 µs · p99 8907 µs · p99.9 10.9 ms · max 12.1 ms | 32 clients, 0 non-2xx, 45112 executions; server per order (mean / p99): queue wait 1536 µs / ≤ 10.0 ms, exchange thread 54 µs / ≤ 250 µs, ack 3073 µs / ≤ 10.0 ms; commit 606 µs / ≤ 2500 µs per commit, 1563 groups of 32.0 orders on average; 45112 of 45112 `TradeExecuted` webhooks delivered 5.14 s after the first order |
