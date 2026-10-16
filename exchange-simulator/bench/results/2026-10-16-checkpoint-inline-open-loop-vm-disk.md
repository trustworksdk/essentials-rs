# exchange-simulator benchmark

- Date: 2026-10-16 18:19 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread, checkpoints inline (SQLite's automatic checkpoint)
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 207 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| HTTP open loop, target 8000 orders/s | 80000 | 10.01 s | 7995 | p50 2438 µs · p99 75.6 ms · p99.9 123.0 ms · max 132.5 ms | target 8000/s for 10 s: 80000 sent, 80000 accepted, 0 × 503, 0 other failures; generator lag p99 2010 µs max 8858 µs; server per order (mean / p99): queue wait 2686 µs / ≤ 100.0 ms, exchange thread 48 µs / ≤ 250 µs, ack 5565 µs / ≤ 100.0 ms; commit 749 µs / ≤ 10.0 ms per commit, 6233 groups of 12.8 orders on average |
| HTTP open loop, target 12000 orders/s | 120000 | 10.01 s | 11992 | p50 2980 µs · p99 14.0 ms · p99.9 18.9 ms · max 24.3 ms | target 12000/s for 10 s: 120000 sent, 120000 accepted, 0 × 503, 0 other failures; generator lag p99 2006 µs max 5105 µs; server per order (mean / p99): queue wait 1067 µs / ≤ 10.0 ms, exchange thread 45 µs / ≤ 250 µs, ack 2951 µs / ≤ 25.0 ms; commit 698 µs / ≤ 10.0 ms per commit, 5658 groups of 21.2 orders on average |
| HTTP open loop, target 16000 orders/s | 160000 | 10.01 s | 15987 | p50 9480 µs · p99 69.6 ms · p99.9 78.9 ms · max 82.5 ms | target 16000/s for 10 s: 160000 sent, 160000 accepted, 0 × 503, 0 other failures; generator lag p99 2037 µs max 4555 µs; server per order (mean / p99): queue wait 5657 µs / ≤ 100.0 ms, exchange thread 44 µs / ≤ 250 µs, ack 11.4 ms / ≤ 100.0 ms; commit 1494 µs / ≤ 10.0 ms per commit, 1785 groups of 89.6 orders on average |
