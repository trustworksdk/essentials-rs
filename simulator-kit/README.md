# simulator-kit

The parts the simulators share (decision D10 in `exchange-simulator/IMPLEMENTATION_PLAN.md`). `exchange-simulator` is
the first user; the payment gateway (plan item 5.4b) is the second. Each simulator keeps its own domain, schema
migrations, event types and payloads, and its single-writer thread. The kit gives it:

| Module | What it is |
|---|---|
| `clock` | `Clock`, `SystemClock`, `FixedClock`, the controllable `VirtualClock` (set, advance, freeze, speed), `ClockConfig`, `wall_now` |
| `rng` | `Rng`, the SplitMix64 stream behind every seeded draw |
| `ids` | `IdScheme`: random UUIDv7, or deterministic UUIDv8 from a seed and a natural key; `derive` for a simulator's own kinds |
| `events` | `NewEvent` and `EventEnvelope`, the canonical event log's records |
| `persistence` | persistence modes (`durable`, `fast`, `memory`), connection setup, migrations over the caller's list, background WAL checkpoints, column conversions (`sql`) |
| `outbox` | the event log and its transactional outbox in SQLite: writing events with their deliveries, event queries, subscriptions, claiming and recording deliveries, attempts, redelivery, dead letters, the virtual clock's stored reading. `SCHEMA` holds the tables |
| `webhooks` | subscription types, HMAC `signing`, fault injection (`faults`), the `DeliveryWorker` over a `DeliveryBackend`, `WebhookHub` (registration, reconciliation of declared subscriptions, updates, deletion, secret rotation, fault policies), `WebhooksConfig` |
| `api` | `ApiError` (`{"error", "message"}` bodies), and the `/api/webhooks` and `/test` webhook routes over a `WebhookBackend` |
| `matcher` | "contains" matching for scenario expectations |
| `metrics` | names of the metrics the kit records |

## Using it in a simulator

1. Put `outbox::SCHEMA` in the first migration, next to the simulator's own tables, and open the database with
   `persistence::open_mode(location, mode, busy_timeout, memory_max_bytes, MIGRATIONS)`.
2. On the writer thread, hold a `WebhookHub` (`WebhookHub::load` with the declared subscriptions and the simulator's
   event types). In each transaction that commits events, stamp them with `NewEvent::stamp` and write them with
   `outbox::write_events` and an `Outbox` built from `hub.subscriptions()`.
3. Implement `DeliveryBackend` (claim and record through the writer) and spawn `DeliveryWorker`.
4. Implement `WebhookBackend` for the HTTP state and merge `api::webhooks::router`, plus `test_router` where test
   control is enabled.

`exchange-simulator` does all four: see its `engine::Exchange`, `engine::Dispatcher` and `api::AppState`.

## Compatibility notes

- Deterministic IDs and fault draws keep the `exchange-simulator/...` hash domains they had before the extraction, so
  the exchange's IDs and fault streams are unchanged.
- The checkpoint and persistence-mode metric names keep their `exchange_` prefix (`exchange_wal_frames`, ...).
- `exchange-simulator` checks that its migrations still produce exactly `outbox::SCHEMA`
  (`persistence::tests::the_migrations_build_the_shared_outbox_schema`).

## Testing

```sh
cargo test -p simulator-kit -j 2
```

Most behaviour (delivery, retries, signing, faults, maintenance, reconciliation) is tested end to end through
`exchange-simulator`'s suite.
