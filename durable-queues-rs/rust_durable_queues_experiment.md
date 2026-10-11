# Rust Port of Essentials Durable Queues — Experiment Summary

## Goal

Port a focused subset of the Java Essentials **Durable Queues** component to Rust to compare developer experience, architecture, correctness, operational footprint, and performance.

The objective is **not** a line-by-line translation. Preserve the queue's observable behaviour and PostgreSQL semantics while implementing an idiomatic Rust library.

## Why Durable Queues?

It is a manageable but realistic component that exercises:

- PostgreSQL transactions and concurrent consumers
- Async execution, polling, backoff, and shutdown
- Message serialization and typed handlers
- Acknowledgement, retries, delayed delivery, and dead-letter handling
- Error handling, tracing, and integration testing

## Proposed Rust Stack

| Concern | Rust option |
|---|---|
| Async runtime and task execution | Tokio |
| PostgreSQL and connection pooling | SQLx |
| Serialization | Serde + serde_json |
| Identifiers and timestamps | uuid + chrono/time |
| Logging and telemetry | tracing |
| Library errors | thiserror |
| Integration testing | testcontainers-rs |

An HTTP framework such as Axum is **not needed** for the initial experiment: build a library first.

## Initial Scope: One Vertical Slice

```text
enqueue
   |
   v
PostgreSQL durable_queues
   |
   v
claim message safely (multiple consumers)
   |
   v
async Rust handler
   |
   +--> success: acknowledge
   |
   +--> failure: retry with next delivery time
                     |
                     +--> dead letter after retry limit
```

Implement:

1. Enqueue JSON messages.
2. Claim available messages with the same concurrency and locking guarantees as Java.
3. Execute handlers concurrently with bounded concurrency.
4. Acknowledge successful processing.
5. Schedule retries and support delayed delivery.
6. Move exhausted messages to dead letter.
7. Add tracing and integration tests.

Defer admin APIs, statistics, TTL jobs, Spring-style configuration, and additional features until the core is working.

## Suggested Design

```text
DurableQueues
  |-- QueueStorage (trait)
  |     `-- PostgresQueueStorage
  |-- QueueConsumer
  |-- RetryPolicy
  `-- QueuedMessage<T>
```

Use explicit dependency wiring rather than reproducing Spring dependency injection. SQLx is a natural fit because the existing implementation relies on PostgreSQL-specific behaviour.

Rust ownership may help enforce message lifecycle rules, but avoid introducing elaborate typestate abstractions unless they demonstrably simplify the design.

## Database Compatibility and Cross-Language Tests

Initially retain the existing `durable_queues` schema and SQL semantics. This makes it possible to run both implementations against the same PostgreSQL database.

Test these combinations:

- Java producer → Rust consumer
- Rust producer → Java consumer
- Java and Rust consumers competing for messages
- Java consumer crash → Rust recovery after the appropriate timeout
- Rust consumer crash → Java recovery after the appropriate timeout
- Retry, delayed delivery, dead-letter, and duplicate-delivery edge cases

Preserve the existing **at-least-once delivery** expectations; do not assume exactly-once processing. Compare transaction boundaries and claim/acknowledgement behaviour carefully.

## Benchmark Plan

| Metric | Java Essentials | Rust |
|---|---|---|
| Idle memory | TBD | TBD |
| Memory under load | TBD | TBD |
| Startup time | TBD | TBD |
| Messages/second | TBD | TBD |
| p50 / p99 delivery latency | TBD | TBD |
| CPU utilization | TBD | TBD |
| Database connections / load | TBD | TBD |
| Container or executable size | TBD | TBD |
| Implementation effort | TBD | TBD |
| Code size and maintainability | TBD | TBD |

Keep the database, workload, payloads, concurrency, connection limits, hardware, and test duration comparable. Measure throughput and latency across multiple load levels; database bottlenecks may dominate differences between languages.

## What the Comparison Should Reveal

**Potential Rust advantages:** lower runtime memory overhead, fast startup, explicit ownership, strong compile-time guarantees, and lightweight async tasks.

**Potential Java advantages:** faster initial development for the existing team, Spring integration, established operational tooling, and familiar maintenance patterns.

**Important caveat:** Rust is not automatically faster than modern Java. A fair result must distinguish language/runtime effects from changes in algorithms, SQL, batching, and connection-pool settings.

## Recommended Execution Order

1. Document the Java component's behavioural contract and key SQL queries.
2. Create a small Rust library with Tokio, SQLx, and Testcontainers.
3. Implement enqueue → claim → handle → acknowledge.
4. Add retries, delayed delivery, and dead-letter behaviour.
5. Port or reproduce the critical Java integration tests.
6. Test Java/Rust interoperability and crash recovery.
7. Benchmark under equivalent conditions.
8. Review code complexity, ergonomics, and maintainability—not just speed.

## Success Criteria

The experiment succeeds if the Rust implementation reproduces the selected Durable Queues semantics, interoperates safely with Java against the same schema, and provides enough evidence to evaluate the engineering trade-offs. Replacing the Java implementation is **not** a prerequisite.
