# exchange-simulator benchmark

- Date: 2026-10-16 17:11 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 266 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| in-memory matching (`OrderBook`) | 1000000 | 0.44 s | 2270197 | – | 903447 executions, 68292 resting at end |
| durable, no subscribers (`Exchange` + SQLite) | 50000 | 22.45 s | 2227 | p50 159 µs · p99 2356 µs · p99.9 5065 µs · max 10.1 ms | 45145 executions, 165273 events, 0 outbox rows |
| durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite) | 50000 | 13.66 s | 3660 | p50 207 µs · p99 1911 µs · p99.9 3796 µs · max 7083 µs | 45145 executions, 165273 events, 165273 outbox rows |
| durable, no subscribers (`Exchange` + SQLite), groups of 256 | 50000 | 2.42 s | 20683 | p50 11.0 ms · p99 21.5 ms · p99.9 27.4 ms · max 27.4 ms | 45145 executions, 165273 events, 0 outbox rows |
| durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite), groups of 256 | 50000 | 3.61 s | 13842 | p50 16.8 ms · p99 28.0 ms · p99.9 31.0 ms · max 31.0 ms | 45145 executions, 165273 events, 165273 outbox rows |
| HTTP end to end (`POST /api/orders`) | 50000 | 3.83 s | 13062 | p50 2192 µs · p99 6849 µs · p99.9 8874 µs · max 9391 µs | 32 clients, 0 non-2xx, 45130 executions; server per order (mean / p99): queue wait 722 µs / ≤ 2500 µs, exchange thread 43 µs / ≤ 250 µs, ack 2266 µs / ≤ 10.0 ms; commit 740 µs / ≤ 10.0 ms per commit, 1733 groups of 28.9 orders on average |
| HTTP end to end + `TradeExecuted` webhook consumer | 50000 | 5.18 s | 9651 | p50 3026 µs · p99 7457 µs · p99.9 8200 µs · max 8586 µs | 32 clients, 0 non-2xx, 45163 executions; server per order (mean / p99): queue wait 1403 µs / ≤ 5000 µs, exchange thread 53 µs / ≤ 250 µs, ack 3119 µs / ≤ 10.0 ms; commit 795 µs / ≤ 5000 µs per commit, 1565 groups of 31.9 orders on average; 45163 of 45163 `TradeExecuted` webhooks delivered 5.21 s after the first order |

Note: the first durable row ran on a cold disk (it is slower than the row with an outbox after it).
