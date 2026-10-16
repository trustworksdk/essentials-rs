# exchange-simulator benchmark

- Date: 2026-10-16 18:20 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread, checkpoints in the background from 1000 pages
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 203 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| durable, auto-checkpoint every 1000 pages (inline, observed) | 50000 | 9.29 s | 5385 | p50 142 µs · p99 1458 µs · p99.9 2193 µs · max 9096 µs | 664 checkpoints; commits running one: 0 spikes > 10 ms, p50 1693 µs · p99 2509 µs · p99.9 9096 µs · max 9096 µs; other commits: 0 spikes, max 8904 µs; WAL 4 MiB |
| durable, background checkpoint from 1000 pages (4.3) | 50000 | 9.95 s | 5026 | p50 165 µs · p99 471 µs · p99.9 2755 µs · max 6971 µs | 659 background checkpoints (620 µs / ≤ 2500 µs), 164 by the writer; 0 spikes > 10 ms; WAL 16 MiB |
| durable, auto-checkpoint off | 50000 | 20.20 s | 2475 | p50 357 µs · p99 1142 µs · p99.9 4099 µs · max 21.9 ms | 9 spikes > 10 ms; WAL grew to 2629 MiB |
| raw 4 KiB write + fsync, no SQLite | 50000 | 12.67 s | 3946 | p50 235 µs · p99 366 µs · p99.9 3368 µs · max 106.1 ms | 8 spikes > 10 ms |
