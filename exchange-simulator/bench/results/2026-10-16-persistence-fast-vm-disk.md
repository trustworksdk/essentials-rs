# exchange-simulator benchmark

- Date: 2026-10-16 18:50 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, database.mode=fast, journal_mode=wal, synchronous=NORMAL, one writer thread, checkpoints in the background from 1000 pages
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 229 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| in-memory matching (`OrderBook`) | 1000000 | 0.53 s | 1883235 | – | 903447 executions, 68292 resting at end |
| durable, no subscribers (`Exchange` + SQLite) | 50000 | 6.93 s | 7211 | p50 61 µs · p99 1393 µs · p99.9 7795 µs · max 367.9 ms | 45145 executions, 165273 events, 0 outbox rows |
| durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite) | 50000 | 7.10 s | 7041 | p50 84 µs · p99 749 µs · p99.9 6098 µs · max 361.2 ms | 45145 executions, 165273 events, 165273 outbox rows |
| durable, no subscribers (`Exchange` + SQLite), groups of 256 | 50000 | 2.13 s | 23484 | p50 10.8 ms · p99 14.8 ms · p99.9 15.1 ms · max 15.1 ms | 45145 executions, 165273 events, 0 outbox rows |
| durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite), groups of 256 | 50000 | 3.19 s | 15689 | p50 16.1 ms · p99 24.7 ms · p99.9 25.8 ms · max 25.8 ms | 45145 executions, 165273 events, 165273 outbox rows |
| HTTP end to end (`POST /api/orders`) | 50000 | 2.98 s | 16767 | p50 1798 µs · p99 8269 µs · p99.9 10.3 ms · max 11.1 ms | 32 clients, 0 non-2xx, 45151 executions; server per order (mean / p99): queue wait 794 µs / ≤ 10.0 ms, exchange thread 43 µs / ≤ 250 µs, ack 1714 µs / ≤ 10.0 ms; commit 136 µs / ≤ 500 µs per commit, 1718 groups of 29.1 orders on average |
| HTTP end to end + `TradeExecuted` webhook consumer | 50000 | 4.20 s | 11914 | p50 2519 µs · p99 9095 µs · p99.9 10.8 ms · max 11.4 ms | 32 clients, 0 non-2xx, 45139 executions; server per order (mean / p99): queue wait 1326 µs / ≤ 10.0 ms, exchange thread 57 µs / ≤ 250 µs, ack 2479 µs / ≤ 10.0 ms; commit 174 µs / ≤ 500 µs per commit, 1620 groups of 30.9 orders on average; 45139 of 45139 `TradeExecuted` webhooks delivered 4.23 s after the first order |
