# exchange-simulator benchmark — commit spike attribution (item 1.2)

- Date: 2026-10-14
- Environment: Docker Desktop VM (linuxkit kernel, aarch64), 8 CPUs, 15 GiB RAM
- Build: release
- Command: `bench checkpoint --dir /home/vscode/exchange-bench` (the VM's own disk; 4 KiB write + fsync ≈ 200 µs)
- Workload: seed 42, 50,000 orders, the durable workload with no subscribers
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread
- Spike: a commit slower than 10 ms

The three arms, run in sequence:
- **Default, observed:** SQLite's auto-checkpoint replaced by an identical WAL hook that also counts. It runs a PASSIVE
  checkpoint in the committing call once the WAL holds at least 1000 frames, and ignores the result, as SQLite's own
  does.
- **Off:** `PRAGMA wal_autocheckpoint=0`.
- **Raw fsync:** 50,000 × (4 KiB append + fsync) in the same directory, without SQLite.

## Run 1

| Scenario | Time | Orders/s | Latency | Notes |
|---|---:|---:|---|---|
| durable, auto-checkpoint every 1000 pages (default, observed) | 27.05 s | 1849 | p50 162 µs · p99 2831 µs · p99.9 5156 µs · max 148.6 ms | 570 checkpoints; commits running one: 0 spikes > 10 ms, p50 2934 µs · p99 6343 µs · p99.9 8118 µs · max 8118 µs; other commits: 4 spikes, max 148.6 ms; WAL 4 MiB |
| durable, auto-checkpoint off | 24.29 s | 2059 | p50 379 µs · p99 3548 µs · p99.9 5895 µs · max 36.3 ms | 15 spikes > 10 ms; WAL grew to 2255 MiB |
| raw 4 KiB write + fsync, no SQLite | 16.10 s | 3105 | p50 213 µs · p99 421 µs · p99.9 11.4 ms · max 949.8 ms | 52 spikes > 10 ms |

## Run 2

| Scenario | Time | Orders/s | Latency | Notes |
|---|---:|---:|---|---|
| durable, auto-checkpoint every 1000 pages (default, observed) | 9.51 s | 5257 | p50 147 µs · p99 1366 µs · p99.9 2022 µs · max 9343 µs | 570 checkpoints; commits running one: 0 spikes > 10 ms, p50 1677 µs · p99 3047 µs · p99.9 9343 µs · max 9343 µs; other commits: 0 spikes, max 6427 µs; WAL 4 MiB |
| durable, auto-checkpoint off | 19.62 s | 2548 | p50 354 µs · p99 726 µs · p99.9 3809 µs · max 55.2 ms | 4 spikes > 10 ms; WAL grew to 2255 MiB |
| raw 4 KiB write + fsync, no SQLite | 13.81 s | 3621 | p50 252 µs · p99 389 µs · p99.9 701 µs · max 166.2 ms | 7 spikes > 10 ms |

## Run 3

| Scenario | Time | Orders/s | Latency | Notes |
|---|---:|---:|---|---|
| durable, auto-checkpoint every 1000 pages (default, observed) | 9.50 s | 5260 | p50 147 µs · p99 1257 µs · p99.9 2058 µs · max 2617 µs | 570 checkpoints; commits running one: 0 spikes > 10 ms, p50 1721 µs · p99 2377 µs · p99.9 2488 µs · max 2488 µs; other commits: 0 spikes, max 2617 µs; WAL 4 MiB |
| durable, auto-checkpoint off | 20.18 s | 2478 | p50 388 µs · p99 647 µs · p99.9 948 µs · max 9142 µs | 0 spikes > 10 ms; WAL grew to 2255 MiB |
| raw 4 KiB write + fsync, no SQLite | 12.81 s | 3903 | p50 238 µs · p99 387 µs · p99.9 2803 µs · max 141.4 ms | 6 spikes > 10 ms |
