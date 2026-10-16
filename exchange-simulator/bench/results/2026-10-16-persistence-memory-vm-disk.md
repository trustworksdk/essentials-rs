# exchange-simulator benchmark

- Date: 2026-10-16 18:50 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, database.mode=memory, journal_mode=memory, synchronous=OFF, one writer thread, checkpoints in the background from 1000 pages
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 243 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| in-memory matching (`OrderBook`) | 1000000 | 0.58 s | 1722561 | – | 903447 executions, 68292 resting at end |
| durable, no subscribers (`Exchange` + SQLite) | 50000 | 1.89 s | 26396 | p50 32 µs · p99 123 µs · p99.9 217 µs · max 2333 µs | 45145 executions, 165273 events, 0 outbox rows |
| durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite) | 50000 | 2.37 s | 21119 | p50 44 µs · p99 135 µs · p99.9 177 µs · max 2491 µs | 45145 executions, 165273 events, 165273 outbox rows |
| durable, no subscribers (`Exchange` + SQLite), groups of 256 | 50000 | 1.45 s | 34522 | p50 7403 µs · p99 8774 µs · p99.9 9155 µs · max 9155 µs | 45145 executions, 165273 events, 0 outbox rows |
| durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite), groups of 256 | 50000 | 2.20 s | 22682 | p50 11.3 ms · p99 13.6 ms · p99.9 14.8 ms · max 14.8 ms | 45145 executions, 165273 events, 165273 outbox rows |
| HTTP end to end (`POST /api/orders`) | 50000 | 2.23 s | 22376 | p50 1414 µs · p99 2009 µs · p99.9 2896 µs · max 6224 µs | 32 clients, 0 non-2xx, 45127 executions; server per order (mean / p99): queue wait 562 µs / ≤ 2500 µs, exchange thread 35 µs / ≤ 250 µs, ack 1231 µs / ≤ 2500 µs; commit 26 µs / ≤ 100 µs per commit, 1692 groups of 29.6 orders on average |
| HTTP end to end + `TradeExecuted` webhook consumer | 50000 | 3.56 s | 14029 | p50 2230 µs · p99 4143 µs · p99.9 5470 µs · max 6661 µs | 32 clients, 0 non-2xx, 45096 executions; server per order (mean / p99): queue wait 1149 µs / ≤ 5000 µs, exchange thread 52 µs / ≤ 250 µs, ack 2066 µs / ≤ 5000 µs; commit 31 µs / ≤ 250 µs per commit, 1626 groups of 30.8 orders on average; 45096 of 45096 `TradeExecuted` webhooks delivered 3.58 s after the first order |
