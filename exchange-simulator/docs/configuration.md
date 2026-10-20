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

**Ports in a container:** to reach the service on another port of the host, change the mapping:
`EXCHANGE_PORT=9080 docker compose -f exchange-simulator/compose.yaml up` (the example Compose files take
`EXCHANGE_PORT` and `GATEWAY_PORT` too). To change the port inside the container, set
`EXCHANGE__SERVER__BIND=0.0.0.0:<port>` and map to that port; the image's health check reads the port from that
variable. The payment gateway works the same way with `GATEWAY__SERVER__BIND` (default `0.0.0.0:8081`).

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

## Instruments

Instruments can be declared in the configuration file, so the market exists before any client connects. Each entry
has the fields of `POST /api/instruments`, in snake_case:

```yaml
instruments:
  - id: AAPL
    symbol: AAPL
    mic: XNAS
    currency: USD
    currency_scale: 2
    tick_size_minor: 1        # default 1
    lot_size: 1               # default 1
    enabled: true             # default true
    reference_symbol: AAPL    # optional: the symbol a price source quotes it under
    price_source: { kind: synthetic, initial_price: "187.50" }   # optional; see Price sources
```

- **At startup**, after the declared webhook subscriptions, each missing instrument is listed (`InstrumentListed`).
  A listed one whose `enabled`, `reference_symbol` or `price_source` differs is updated (`InstrumentUpdated`), so the configuration
  wins over API changes at each start. Its other fields never change once listed: a difference stops startup with
  an error naming the field.
- **After `POST /test/reset`** the declared instruments are listed again, with their events, as on a new database.
- **Removed from the list**, an instrument stays listed, since orders and trades refer to it.
- IDs must be unique, and values are validated as for the API (under `exchange.identifier_policy`).
- API-registered instruments keep working alongside declared ones.

## Price sources

Each instrument's reference price comes from its price source: `none` (the default), `fixed`, `synthetic` or
`finnhub`. The
[README](../README.md#price-sources) describes each kind and the synthetic walk. Fields an instrument's
`price_source` leaves out come from `price_sources.default`, so a market of synthetic instruments is set once:

```yaml
price_sources:
  seed: 0                    # base of each instrument's derived synthetic seed
  default:
    kind: synthetic
    drift: 0.0               # annualized
    volatility: 0.3          # annualized
    tick_interval_ms: 1000   # simulated time per step, at least 100
instruments:
  - { id: AAPL, symbol: AAPL, mic: XNAS, currency: USD, currency_scale: 2, price_source: { initial_price: "187.50" } }
  - { id: MSFT, symbol: MSFT, mic: XNAS, currency: USD, currency_scale: 2, price_source: { initial_price: "410.00" } }
```

A source that cannot resolve stops startup with an error naming the instrument, e.g. a `synthetic` default with no
`initial_price`. A changed default applies at the next start to the instruments that rely on it, and their walks are
recomputed from when their source was set, with the new parameters, so their reference prices jump. Change an
instrument's own `price_source` instead to restart its walk from the current time.

### Live prices from Finnhub

`kind: finnhub` follows real trades ([README](../README.md#finnhub-live-prices)). The API key comes from the
`FINNHUB_API_KEY` environment variable. Both Compose files (`compose.yaml` and `examples/compose/compose.yaml`) pass
it through from the shell that runs `docker compose`, so the key never lands in a file in the repository:

```bash
FINNHUB_API_KEY=<your key> docker compose -f exchange-simulator/compose.yaml up --build
```

Unset or empty, the feed does not run. `api_key_file` reads the key from a file instead, and wins over the variable
when set.

**A ready-made market:** [`config/finnhub-market.yaml`](../config/finnhub-market.yaml) lists 40 instruments on
Finnhub prices: 37 large US stocks and ETFs (Nasdaq, NYSE, NYSE Arca) and 3 crypto pairs that trade around the clock.
The image ships it at `/etc/exchange/finnhub-market.yaml`, and `compose.yaml` passes `EXCHANGE_CONFIG` through:

```bash
FINNHUB_API_KEY=<your key> EXCHANGE_CONFIG=/etc/exchange/finnhub-market.yaml \
  docker compose -f exchange-simulator/compose.yaml up --build
```

Without a key that configuration refuses to start, since every instrument needs the feed. Copy the file to change
the market. A hand-written setup looks like this:

```yaml
price_sources:
  finnhub:
    stale_after_ms: 120000               # stale after two minutes without an update
    max_updates_per_second: 4
instruments:
  - { id: AAPL, symbol: AAPL, mic: XNAS, currency: USD, currency_scale: 2, price_source: { kind: finnhub } }
  - { id: BTC, symbol: BTC, currency: USD, currency_scale: 2, reference_symbol: "BINANCE:BTCUSDT",
      price_source: { kind: finnhub } }
liquidity:
  default: { mode: on, stale_policy: widen, stale_widen_factor: 3 }
```

The variable is visible to anyone who can run `docker inspect` on the container; use `api_key_file` with a Docker
secret where that matters. The defaults for
`max_symbols` (50) and `rest_calls_per_minute` (60) are Finnhub's free tier; raise them for a paid plan.
`stale_after_ms` must be longer than `refresh_after_ms`, or a closed market's prices would go stale between REST
refreshes. Automated tests never call Finnhub; they run against a local mock (`tests/finnhub.rs`).

### Replaying recorded prices

Record a live session, then replay it on an accelerated clock ([README](../README.md#replay)):

```yaml
# Recording: a live run
price_sources:
  finnhub:
    record_to: recordings/2026-10-20.csv    # recordings/ is gitignored
```

```yaml
# Replaying it: no key, no network, the same path at speed 60
clock: { mode: virtual, running: true, speed: 60 }
price_sources:
  default: { kind: replay }
  replay:
    file: recordings/2026-10-20.csv
    loop: true
```

The replay's instruments must use the recorded symbols (`reference_symbol`, else `symbol`). Never commit a recording.

## Liquidity provider

A built-in participant quotes a ladder around each instrument's reference price ([README](../README.md#liquidity-provider)).
It needs a price source, and is off until `mode: on`. A market where every instrument has a counterparty:

```yaml
price_sources:
  default: { kind: synthetic, volatility: 0.3 }
liquidity:
  participant_id: LP            # the participant it trades as
  emit_order_events: false      # its order lifecycle stays out of the log; its trades never do
  default: { mode: on, levels: 5, spread_bps: 10, level_quantity: 100, skew_bps_per_lot: 0.5, max_inventory: 2000 }
instruments:
  - { id: AAPL, symbol: AAPL, mic: XNAS, currency: USD, currency_scale: 2, price_source: { initial_price: "187.50" } }
```

An instrument's own `liquidity` overrides the default field by field; set `liquidity: { mode: off }` on one to leave
it to client orders only. `stale_policy` (`pull` by default, `widen` with `stale_widen_factor`, or `keep`) decides
what happens to the ladder while a live reference price is stale.

## Trading sessions

Opening hours per market, by MIC, in each market's own time zone ([README](../README.md#trading-sessions-and-time-in-force)):

```yaml
sessions:
  XNAS: { open: "09:30", close: "16:00", time_zone: America/New_York }
  XNYS: { open: "09:30", close: "16:00", time_zone: America/New_York }
  XCSE: { open: "09:00", close: "17:00", time_zone: Europe/Copenhagen }
liquidity:
  default: { mode: on, closed_policy: widen, closed_widen_factor: 3 }   # quote wider while clients cannot trade
```

Outside its session an instrument refuses client orders (`MARKET_CLOSED`) and expires its `DAY` orders at the close;
`DAY` becomes the default time in force there. Markets not listed stay always open, so adding sessions changes nothing
for them. Sessions are weekdays only; holidays, half days and auctions are not modelled. With a virtual clock,
`POST /test/clock/set` across an open or a close behaves as if the time had passed: the provider re-quotes, and
`DAY` orders expire with `occurredAt` at the close.

## Market data

The snapshot (`GET /api/marketdata`) needs no setting. The stream (`GET /api/marketdata/stream`, [README](../README.md#get-apimarketdatastreaminstruments))
has two:

```yaml
market_data:
  conflation_interval_ms: 250   # per subscriber, instrument and kind, at most one message per 250 ms; 0 sends all
  stream_buffer: 1024           # messages a subscriber may fall behind before its stream is closed
```

A browser chart wants a few updates a second, not every quote: 100–500 ms is a good conflation interval. A
subscriber more than half of `stream_buffer` behind is caught up at once (its backlog collapses to the latest message
per instrument and kind); a whole buffer behind, its stream is closed and it reconnects. The exchange never waits for
a subscriber either way.

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

**Two demo stacks on one exchange:** give each stack's subscription a `participants` filter, so it receives only its
own participants' order, trade, settlement and clearing events (and every event that involves no participant, such as
instrument listings). The [README](../README.md#post-apiwebhooks) has the exact rules.

```yaml
webhooks:
  subscriptions:
    - { name: stack-a, url: "http://demo-a:8080/exchange", event_types: [TradeExecuted, OrderAccepted], participants: [DEMO-A] }
    - { name: stack-b, url: "http://demo-b:8080/exchange", event_types: [TradeExecuted, OrderAccepted], participants: [DEMO-B] }
```

## Settlement

`settlement.mode` decides who settles trades:
- **`external`** (default): your service settles from the `TradeExecuted` webhook.
- **`simulated`:** the built-in simulator runs the T+N lifecycle and emits `Settlement*` events.
- **`both`:** the simulator runs, and your service can also report outcomes back.

The cycle is T+1 by default (`settlement_days: 1`). Markets still on another cycle are listed by MIC:

```yaml
settlement:
  mode: simulated
  market_settlement_days: { XCSE: 2, XSTO: 2 }   # T+2 until the EU moves to T+1 on 2027-10-11
```

The [README's Settlement section](../README.md#settlement) covers the policy keys: probabilities, delays,
`settlement_days` and `market_settlement_days`.

## Clearing

`clearing.cutoff` and `clearing.time_zone` also define the business days: each one ends at the cutoff with
`BusinessDayClosed`, whether or not clearing is enabled ([README](../README.md#business-days)).

`clearing.enabled: true` adds end-of-day clearing statements: at the cutoff on each business day, one
`ClearingStatementIssued` per participant that traded, with its trades and net obligations per instrument and
settlement date. The cutoff is local time in an IANA time zone, so it follows daylight saving:

```yaml
clearing:
  enabled: true
  cutoff: "17:30"
  time_zone: Europe/Copenhagen
```

Statements are for reconciling against, so they can be made wrong on purpose. `clearing.breaks` leaves trades out,
changes a quantity or price by one, adds a trade that does not exist, or issues a statement late, each with a seeded
probability. Every break is listed under `GET /test/clearing/breaks`. The
[README's Clearing section](../README.md#clearing) has the statement format and each break.

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

**Short demo: a simulated day per real minute,** so settlement completes while the audience watches:

```yaml
clock:
  mode: virtual
  start: "2030-01-07T08:00:00Z"        # a Monday morning; any business day works
  running: true
  speed: 1440                          # a simulated day per real minute
settlement:
  mode: simulated
  fail_probability: 0.0                # every trade settles, for a clean run
  delay_probability: 0.0
```

At this speed a trade made at 09:00 is settled at 00:00 the next day (T+1), about 37 real seconds later; a Friday
trade settles on Monday, under 3 minutes later. Webhook retries and timeouts stay on real time, so a consumer that
is briefly down catches up as usual. Points to keep in mind:
- **Take dates from the events.** A consumer, such as the demo app, must take dates and times from `occurredAt`,
  `executedAt`, `tradeDate` and `intendedSettlementDate`, never from its own clock, which runs a thousand times
  slower than the exchange's.
- **A restart keeps simulated time** (it continues from the latest event) and the configured speed. For a fresh run,
  remove the database volume, or call `POST /test/reset` where test control is enabled.
- **To change the speed during a demo**, set `clock.operator_control: true` and call `POST /api/clock/speed` (or
  `/freeze` and `/resume`). This does not expose `/test`, so a showcase deployment can use it.
- **To follow the exchange's time**, read `GET /api/clock` at startup and subscribe to `ClockChanged`; the
  [README](../README.md#the-clock-api) has the formula.

**Back-office demo: statements to reconcile, with a few breaks.** Add to the short-demo recipe:

```yaml
clearing:
  enabled: true
  cutoff: "17:30"
  time_zone: Europe/Copenhagen
  breaks: { seed: 11, omit_probability: 0.05, alter_probability: 0.05, late_probability: 0.2 }
```

At `speed: 1440` a statement arrives once a simulated business day, about every real minute. Trades after the cutoff
are dated the next business day, on its statement, and settle T+1 from it.

**Load test without durability:**

```yaml
database: { mode: memory, memory_max_mib: 8192 }
exchange: { command_queue_capacity: 20000 }
```
