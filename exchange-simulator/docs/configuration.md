# Configuration

## How settings are layered

Three sources, each overriding the one before:
1. **Built-in defaults:** [`config/default.yaml`](../config/default.yaml), compiled into the binary. It is commented,
   and it is the complete list of keys.
2. **A YAML file** named by the `EXCHANGE_CONFIG` environment variable. It only needs the keys that differ.
3. **Environment variables** `EXCHANGE__<SECTION>__<KEY>`, for example `EXCHANGE__DATABASE__MODE=fast` or
   `EXCHANGE__WEBHOOKS__MAX_ATTEMPTS=20`. Lists, such as `webhooks.subscriptions`, cannot be set this way; use the
   file.

`RUST_LOG`, when set, overrides `logging.filter`.

**The image sets three variables, and they beat your file:** `EXCHANGE__SERVER__BIND=0.0.0.0:8080`,
`EXCHANGE__DATABASE__PATH=/var/lib/exchange/exchange.db` and `EXCHANGE__LOGGING__FORMAT=json`. To change one of these
in a container, set the variable rather than the file key.

The service checks its configuration at startup and refuses to start, with a message, when something is wrong: an
invalid webhook declaration, an unreadable secret file, settlement cancel, fail and delay probabilities that add up to more than 1.

The [README's settings table](../README.md#running) gives every key with its default. The sections below are the ones
that matter in practice.

## Persistence

| Key | Default | |
|---|---|---|
| `database.mode` | `durable` | `durable`, `fast` or `memory` |
| `database.path` | `data/exchange.db` (`/var/lib/exchange/exchange.db` in the image) | unused in `memory` |
| `database.checkpoint` | `background` | `inline` brings back SQLite's checkpoint inside commits |
| `database.memory_max_mib` | `4096` | `memory` only |

What each mode survives:

| Mode | Process or container restart | Host crash or power loss |
|---|---|---|
| `durable` | everything | everything that was acknowledged |
| `fast` | everything | all but the last commits; the database is never corrupted |
| `memory` | nothing | nothing |

- **`durable`** is the right default.
- **`fast`** is for load tests that still need state across restarts. With group commit on, it gains little over
  `durable` (see the [benchmarks](../bench/README.md)).
- **`memory`** is for throughput runs where nothing needs to survive. It needs RAM for about 3–4 KiB per order, and
  more while the database grows.

The mode shows in `GET /health`, in the `exchange_persistence_mode` metric, and, in `fast` and `memory` mode, in an
`Exchange-Persistence` header on every response.

**Persistence also depends on where the file lives.** Without a volume, a new container starts empty in any mode. In
Kubernetes an `emptyDir` is lost when the pod is rescheduled; use a persistent volume.

## Webhooks

| Key | Default | |
|---|---|---|
| `webhooks.subscriptions` | `[]` | Subscriptions declared by name; see [Operations](operations.md#declared-or-api-created) |
| `webhooks.max_attempts` | `10` | Attempts before a delivery is dead-lettered |
| `webhooks.initial_backoff_ms` / `max_backoff_ms` | `500` / `300000` | The first retry delay; it doubles up to the cap, with jitter |
| `webhooks.request_timeout_ms` | `5000` | A slower answer counts as a failed attempt |
| `webhooks.concurrency` / `batch_size` | `16` / `100` | Requests in flight, and deliveries claimed per round |
| `webhooks.poll_interval_ms` | `250` | How often due retries are looked for (new events are sent at once) |
| `webhooks.secret_grace_seconds` | `3600` | How long a declared subscription's previous secret keeps signing after the file changes it |

**How long a consumer can be down before deliveries are dead-lettered**, with the default backoff (each wait is drawn
between half and all of its doubling step, capped at `max_backoff_ms`):

| `max_attempts` | Retries span about |
|---:|---|
| 10 (default) | 2–4 minutes |
| 15 | 14–29 minutes |
| 20 | 27–54 minutes |
| 30 | 52–104 minutes |

Dead letters are not lost: they can be retried at any time ([Operations](operations.md#dead-letters)). For planned
downtime, pausing the subscription is better than relying on retries.

## Settlement

`settlement.mode` decides who settles trades:
- **`external`** (default): your service settles from the `TradeExecuted` webhook.
- **`simulated`:** the built-in simulator runs the T+N lifecycle and emits `Settlement*` events.
- **`both`:** the simulator runs, and your service can also report outcomes back.

The [README's Settlement section](../README.md#settlement) covers the policy keys: probabilities, delays and
`settlement_days`.

## Recipes

A file for each common setup, used with `EXCHANGE_CONFIG`.

**Integration environment, settlement handled by your service** (the defaults plus your consumers):

```yaml
webhooks:
  max_attempts: 20                     # rides out a consumer restart of up to half an hour
  subscriptions:
    - name: settlement
      url: http://settlement-service:8080/webhooks/trades
      event_types: [TradeExecuted]
      secret_file: /run/secrets/settlement_webhook_secret
      ordered: true
```

**Simulated settlement, for exercising consumers of lifecycle events:**

```yaml
settlement:
  mode: simulated
  settlement_days: 2
  fail_probability: 0.05
  delay_probability: 0.10
webhooks:
  subscriptions:
    - name: lifecycle
      url: http://ledger:8080/webhooks/settlement
      event_types: [SettlementPending, SettlementCompleted, SettlementFailed, SettlementDelayed,
                    SettlementRejected, SettlementCancelled, SettlementCorrected]
      secret_env: LEDGER_WEBHOOK_SECRET
```

**Reproducible test runs:** identical IDs and event logs run after run, and time under the test's control:

```yaml
ids: { mode: deterministic, seed: 42 }
clock: { mode: virtual, start: "2030-01-07T09:00:00Z" }
test_control: { enabled: true }        # mounts /test; never expose it outside test deployments
```

**Load test without durability:**

```yaml
database: { mode: memory, memory_max_mib: 8192 }
exchange: { command_queue_capacity: 20000 }
```
