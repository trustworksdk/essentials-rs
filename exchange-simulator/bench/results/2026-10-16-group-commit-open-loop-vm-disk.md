# exchange-simulator benchmark

- Date: 2026-10-16 17:12 UTC
- Environment: unknown CPU, 8 CPUs available, 15 GiB RAM, linux aarch64 (kernel 7.0.14-linuxkit)
- Build: release
- Seed: 42; workload: 80 % limit orders within ±50 ticks of mid, 20 % market, quantities 1–100, 50 participants, one instrument
- SQLite: 3.51.3, journal_mode=wal, synchronous=FULL, one writer thread
- Database directory: `/home/vscode/bench-tmp`; 4 KiB write + fsync there: 208 µs

| Scenario | Orders | Time | Orders/s | Latency per order | Notes |
|---|---:|---:|---:|---|---|
| HTTP open loop, target 2000 orders/s | 20000 | 10.11 s | 1978 | p50 2343 µs · p99 272.9 ms · p99.9 359.2 ms · max 369.0 ms | target 2000/s for 10 s: 20000 sent, 20000 accepted, 0 × 503, 0 other failures; generator lag p99 98.4 ms max 193.9 ms; server per order (mean / p99): queue wait 11.2 ms / ≤ 250.0 ms, exchange thread 68 µs / ≤ 500 µs, ack 14.9 ms / ≤ 500.0 ms; commit 872 µs / ≤ 10.0 ms per commit, 5643 groups of 3.5 orders on average |
| HTTP open loop, target 4000 orders/s | 40000 | 10.00 s | 3999 | p50 1863 µs · p99 5301 µs · p99.9 12.5 ms · max 18.5 ms | target 4000/s for 10 s: 40000 sent, 40000 accepted, 0 × 503, 0 other failures; generator lag p99 2195 µs max 2348 µs; server per order (mean / p99): queue wait 242 µs / ≤ 2500 µs, exchange thread 49 µs / ≤ 250 µs, ack 916 µs / ≤ 5000 µs; commit 436 µs / ≤ 5000 µs per commit, 7139 groups of 5.6 orders on average |
| HTTP open loop, target 8000 orders/s | 80000 | 10.00 s | 7998 | p50 2122 µs · p99 6794 µs · p99.9 8629 µs · max 11.3 ms | target 8000/s for 10 s: 80000 sent, 80000 accepted, 0 × 503, 0 other failures; generator lag p99 2156 µs max 2398 µs; server per order (mean / p99): queue wait 428 µs / ≤ 5000 µs, exchange thread 45 µs / ≤ 250 µs, ack 1350 µs / ≤ 10.0 ms; commit 521 µs / ≤ 5000 µs per commit, 7976 groups of 10.0 orders on average |
| HTTP open loop, target 12000 orders/s | 120000 | 10.00 s | 11998 | p50 2761 µs · p99 11.2 ms · p99.9 14.2 ms · max 19.2 ms | target 12000/s for 10 s: 120000 sent, 120000 accepted, 0 × 503, 0 other failures; generator lag p99 2012 µs max 4365 µs; server per order (mean / p99): queue wait 868 µs / ≤ 5000 µs, exchange thread 44 µs / ≤ 250 µs, ack 2436 µs / ≤ 10.0 ms; commit 619 µs / ≤ 5000 µs per commit, 6337 groups of 18.9 orders on average |
| HTTP open loop, target 16000 orders/s | 160000 | 10.25 s | 15602 | p50 33.6 ms · p99 304.3 ms · p99.9 309.0 ms · max 311.7 ms | target 16000/s for 10 s: 160000 sent, 160000 accepted, 0 × 503, 0 other failures; generator lag p99 2164 µs max 12.1 ms; server per order (mean / p99): queue wait 118.2 ms / ≤ 500.0 ms, exchange thread 45 µs / ≤ 250 µs, ack 125.3 ms / ≤ 500.0 ms; commit 1500 µs / ≤ 25.0 ms per commit, 1735 groups of 92.2 orders on average |

Note: the 2000 orders/s row stalled the whole process (generator lag up to 194 ms), as a first run on a cold disk did in 4.1. A rerun gave p50 1863 µs, p99 5421 µs, max 15.2 ms, generator lag at most 7.9 ms, groups of 3.5 orders on average.
