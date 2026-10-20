# Operations

## Subscriptions

### Declared or API-created

| | Declared in configuration | Created through the API |
|---|---|---|
| Where it lives | `webhooks.subscriptions` in the file named by `EXCHANGE_CONFIG` | the database |
| Identity | its `name` | its `id` |
| Created | at startup (and after `/test/reset`) | `POST /api/webhooks` |
| Changed | edit the file and restart | `PATCH /api/webhooks/{id}` |
| Paused | `paused: true` in the file, or `PATCH … {"paused": true}` until the next restart | `PATCH … {"paused": true}` |
| Removed | delete the entry and restart: it is disabled, with history kept | `DELETE /api/webhooks/{id}`: hidden, with history kept |
| Secret rotation | change it in the file and restart: the old one keeps signing for `secret_grace_seconds` | `POST /api/webhooks/{id}/secret` |
| Survives `memory` mode restarts and resets | yes | no |

**Declare every standing consumer.** The subscription list is then reviewed and versioned with the deployment, and it
is back after every restart. Keep API-created subscriptions for short-lived listeners: a test harness, or a debugging
session.

A change to a declared subscription that reaches the API gets `409 managed_by_configuration`. The exception is
`paused`.

**Applying a file change under Compose:** `docker compose up -d` does not restart a container when only a mounted
file changed. Run `docker compose restart exchange`.

### Finding a subscription's ID

```sh
curl -s localhost:8080/api/webhooks                       # all, with name (if declared), source, enabled, paused, signed
curl -s 'localhost:8080/api/webhooks?includeDeleted=true' # deleted ones too
```

### Pausing for consumer maintenance

```sh
curl -s -XPATCH localhost:8080/api/webhooks/$ID -H 'content-type: application/json' -d '{"paused": true}'
# … consumer down for maintenance; events keep being recorded for it …
curl -s -XPATCH localhost:8080/api/webhooks/$ID -H 'content-type: application/json' -d '{"paused": false}'
```

- **While paused,** every matching event gets a delivery that waits. On resuming they go out in order.
- **`enabled: false` is different:** it turns the subscription off, so events in the meantime are not recorded for
  it at all.
- **Long maintenance windows:** prefer pausing to letting deliveries fail. Failed deliveries are dead-lettered after
  `max_attempts`.

### How far behind is a consumer?

```sh
curl -s localhost:8080/api/webhooks/$ID
# … "deliveries": { "pending": 12, "inFlight": 1, "delivered": 5310, "dead": 0,
#                   "oldestUnfinished": { "eventSequence": 18402, "occurredAt": "…" } }
```

`oldestUnfinished.occurredAt` is the age of the oldest event the consumer has not received yet.

### Dead letters

A delivery that failed `max_attempts` times stops being retried; it becomes a dead letter. It stays there until you
retry it:

```sh
curl -s "localhost:8080/api/webhooks/$ID/deliveries?status=DEAD"           # with attempts and lastError
curl -s -XPOST localhost:8080/api/webhooks/$ID/dead-letters/retry          # all of this subscription's
curl -s -XPOST localhost:8080/api/webhooks/$ID/dead-letters/retry \
  -H 'content-type: application/json' -d '{"deliveryIds": ["…"]}'          # or some
```

A retried dead letter starts with a fresh attempt budget. `lastError` usually says why it failed: `HTTP 500 …`, a
connection error (wrong URL, consumer down), or a timeout.

### Replaying events

```sh
curl -s -XPOST "localhost:8080/api/webhooks/$ID/redeliver?after=18000"
```

This queues every event after sequence 18000 for the subscription again, including events from before it existed.
Use it when a consumer lost data, or to backfill a new consumer.

## Monitoring

**Health:** `GET /health` gives `200 {"status":"ok","schemaVersion":…,"persistence":"durable"}`. The image's Docker
`HEALTHCHECK` uses it, on the port in `EXCHANGE__SERVER__BIND`, so `docker compose up --wait` and
`depends_on: condition: service_healthy` work.

**Metrics:** `GET /metrics` is in Prometheus text format. The [README](../README.md#get-metrics) lists every series.
The ones worth an alert:

| Series | Alert when | Means |
|---|---|---|
| `webhook_outbox_deliveries{status="DEAD"}` | above 0 | Deliveries gave up: retry them once the consumer is fixed |
| `webhook_outbox_deliveries{status="PENDING"}` | keeps growing | A consumer is down, slow or paused |
| `rate(webhook_delivery_attempts_total{outcome="retry"}[5m])` | sustained | A consumer is failing requests |
| `rate(exchange_command_queue_rejections_total[5m])` | above 0 | Overload: callers get `503 overloaded` |
| `exchange_command_ack_seconds` (p99) | above your budget | Orders wait in the queue; see [Benchmarking](benchmarking.md) |
| `exchange_wal_frames` | stays far above `checkpoint_pages` | Checkpoints cannot finish, usually because of a long-running read |
| `exchange_persistence_mode{mode="durable"}` | 0 in an environment that must be durable | The mode is set wrong |
| `exchange_reference_stale` | 1 for minutes | A live price source is down or the symbol is quiet; the liquidity provider applies its stale policy |
| `exchange_price_feed_connected` | 0 for minutes | The Finnhub websocket cannot connect: key, network, or Finnhub |
| `rate(exchange_market_data_streams_closed_total[5m])` | sustained | A market-data consumer cannot keep up; raise `conflation_interval_ms` or `stream_buffer` |

**Logs:** the image writes JSON lines (`logging.format: json`). Startup logs the database, its mode, the recovered
books, and every declared subscription created, updated or disabled. Set `RUST_LOG=debug` for more.

## Troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| The service exits at startup | Invalid configuration: the message names it (subscription, key, secret file) | Fix the file; nothing was changed |
| The consumer receives nothing | Subscription paused, disabled, deleted, or not wanting that event type | `GET /api/webhooks/{id}`: check `paused`, `enabled`, `eventTypes` |
| | Wrong URL or network | `GET …/deliveries?status=PENDING`: `lastError` shows connection or DNS errors. Inside Compose, use the service name, not `localhost` |
| The consumer answers 401 (bad signature) | Different secrets, e.g. a trailing newline in a secret file the consumer did not trim | Compare the secrets; the exchange trims file secrets |
| | Verifying a re-serialized body | Verify the raw bytes as received |
| | Clock skew beyond the consumer's tolerance | Sync clocks (NTP), or widen the tolerance |
| | Only the first value of `X-Signature` checked during a rotation | Accept any value in the comma-separated list |
| The same event arrives twice | At-least-once delivery: a retry, a restart, a redelivery | Deduplicate by `eventId` ([rules](webhook-receivers.md#a-correct-receiver-in-five-rules)) |
| Events arrive out of order | Concurrent delivery | Order by `sequence`, or declare the subscription `ordered: true` |
| An ordered subscription is stuck | Its oldest delivery keeps failing and holds up the rest | `GET …/deliveries?status=PENDING` for the error; fix the consumer, or wait for the dead letter and let the rest flow |
| API answers `503 overloaded` | The command queue is full | Slow down, or raise `exchange.command_queue_capacity` (requests then wait longer instead of failing fast) |
| API answers `409 managed_by_configuration` | Changing a declared subscription through the API | Change it in the file and restart, or only pause it |
| `memory` mode fails orders with a storage error | The in-memory database reached `memory_max_mib` | Raise it (check the container's memory limit), or use `fast` |

## Data

**Backups** (`durable` and `fast`): the database is one SQLite file plus its WAL in the volume. Stop the exchange so
the WAL is folded into the file, then copy the volume:

```sh
docker compose stop exchange
docker run --rm -v exchange-example_exchange-data:/data:ro -v "$PWD":/backup debian:bookworm-slim \
  tar czf /backup/exchange-$(date +%F).tgz -C /data .
docker compose start exchange
```

Restoring is the same in reverse, into an empty volume with the exchange stopped. The volume is named
`<project>_exchange-data`; `docker volume ls` lists it.

**Upgrades:** schema migrations run automatically at startup, and they are forward-only. Back up before starting a
newer image on an existing volume. An older image refuses nothing, but it does not know the newer schema, so don't go
back without restoring the backup.

**Resetting a test environment:** with `test_control.enabled: true`, `POST /test/reset` empties every table. Declared
subscriptions come back at once. Never enable test control in an environment others depend on.
