# exchange-simulator benchmark

- Date: 2026-10-16 18:54 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, database.mode=memory, journal_mode=memory, synchronous=OFF, one writer thread, checkpoints none (in memory)
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 427 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| HTTP open loop, target 15000 orders/s | 150000 | 10.00 s | 14997 | p50 1965 µs · p99 28.5 ms · p99.9 45.6 ms · max 51.6 ms | target 15000/s for 10 s: 150000 sent, 150000 accepted, 0 × 503, 0 other failures; generator lag p99 2198 µs max 7964 µs; server per order (mean / p99): queue wait 799 µs / ≤ 25.0 ms, exchange thread 40 µs / ≤ 250 µs, ack 1773 µs / ≤ 25.0 ms; commit 26 µs / ≤ 500 µs per commit, 7993 groups of 18.8 orders on average |
| HTTP open loop, target 20000 orders/s | 200000 | 10.00 s | 19996 | p50 2284 µs · p99 7483 µs · p99.9 15.2 ms · max 19.1 ms | target 20000/s for 10 s: 200000 sent, 200000 accepted, 0 × 503, 0 other failures; generator lag p99 1969 µs max 6154 µs; server per order (mean / p99): queue wait 481 µs / ≤ 2500 µs, exchange thread 37 µs / ≤ 250 µs, ack 1237 µs / ≤ 5000 µs; commit 26 µs / ≤ 250 µs per commit, 8393 groups of 23.8 orders on average |
| HTTP open loop, target 25000 orders/s | 250000 | 10.00 s | 24991 | p50 8885 µs · p99 20.3 ms · p99.9 23.3 ms · max 25.3 ms | target 25000/s for 10 s: 250000 sent, 250000 accepted, 0 × 503, 0 other failures; generator lag p99 2322 µs max 5925 µs; server per order (mean / p99): queue wait 2872 µs / ≤ 25.0 ms, exchange thread 36 µs / ≤ 250 µs, ack 7168 µs / ≤ 25.0 ms; commit 100 µs / ≤ 1000 µs per commit, 1697 groups of 147.3 orders on average |
| HTTP open loop, target 30000 orders/s | 208860 | 96.25 s | 2170 | p50 402.3 ms · p99 87.22 s · p99.9 87.27 s · max 88.68 s | target 30000/s for 10 s: 300000 sent, 208860 accepted, 90606 × 503, 534 other failures; generator lag p99 85.81 s max 85.83 s; server per order (mean / p99): queue wait 4.48 s / > 10 s, exchange thread 451 µs / ≤ 250 µs, ack 4.29 s / > 10 s; commit 294 µs / ≤ 2500 µs per commit, 835 groups of 250.7 orders on average |

Note: the 30000 orders/s row collapsed because the devcontainer hit its 8 GiB memory limit (98 % in docker stats), while rust-analyzer was re-indexing. Two reruns of the same sequence saturated normally at 24,234 and 25,388 orders/s, with 503s. See bench/README.md.
