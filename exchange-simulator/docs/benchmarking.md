# Benchmarking

Throughput depends on where the service runs: the disk's fsync latency, the CPU and memory limits, and the
persistence mode. So figures are measured per environment, never quoted as one number. The harness and every
measurement so far are in [`bench/README.md`](../bench/README.md). The short version:

```sh
# from the workspace root; always --release; --dir on the disk you want to measure (not tmpfs)
cargo run --release -p exchange-simulator --bin bench -- all --dir /path/on/real/disk --out report.md
cargo run --release -p exchange-simulator --bin bench -- open-loop --rates 2000,8000,16000 --dir /path/on/real/disk
cargo run --release -p exchange-simulator --bin bench -- --help
```

- **`open-loop`** sends orders on a fixed schedule and measures latency from each order's scheduled time, so a
  stalled service shows up as latency.
- **`provider`** runs the open loop at each rate twice, without and with the liquidity provider quoting a replayed
  price, so the cost of re-quoting shows in the order-ack latency at the same client rate (SR-20). Measured: about
  40–50 µs of exchange-thread time per order; invisible in the ack up to 2,000 orders/s, about +0.3–0.6 ms p50 and
  +2.5–5 ms p99 at 4,000/s.
- **Each report** records the environment: CPUs, memory, the container's cgroup limits, and the measured fsync cost
  of the database directory.
- **Each row** splits the service's time into queue wait, exchange-thread time, commit, and the size of commit
  groups.

**Measured in the devcontainer** (VM disk, fsync about 200 µs, 8 GiB container limit): about 12,000 orders/s held at
p99 11 ms, saturating near 15,600/s in `durable` mode and about 25,000/s in `memory` mode. The plan's target is
1,000–10,000 orders/s.

**Watch the memory limit.** In a container, `free` and `/proc/meminfo` show the host's memory, not the container's
limit. A `memory`-mode run near saturation can reach the limit and stall. Report headers show the cgroup limit; leave
headroom.

A containerized benchmark is the next step (plan item 4.6d): a Compose profile that runs the load generator in its
own container with explicit CPU and memory limits.
