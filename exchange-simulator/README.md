# exchange-simulator

A lightweight stock exchange for integration and performance testing. It does real price-time matching over integer
ticks, keeps SQLite as the authoritative store, and delivers canonical events through durable, retried webhooks. Design:
[`rust_exchange_simulator_plan.md`](rust_exchange_simulator_plan.md) and
[`milestone_1_domain_schema_events.md`](milestone_1_domain_schema_events.md), whose "Implementation progress" section
tracks what is built.

**New here?** The [usage guide](docs/README.md) starts with a Compose example and covers configuration, writing a
webhook receiver, and operations. This README is the API reference.

## Running

```sh
# from the workspace root
cargo run -p exchange-simulator                       # http://127.0.0.1:8080, database in ./data/exchange.db
docker compose -f exchange-simulator/compose.yaml up --build   # http://localhost:8080, database in a volume
```

Configuration (`config/default.yaml` is compiled in) can be overridden with a YAML file named by `EXCHANGE_CONFIG`, or
with environment variables such as `EXCHANGE__SERVER__BIND=0.0.0.0:9000`. Settings:

| Key | Default | Meaning |
|---|---|---|
| `server.bind` | `127.0.0.1:8080` | Listen address (`0.0.0.0:8080` in the image) |
| `exchange.identifier_policy` | `strict` | `strict` checks MIC/ISIN/CUSIP check digits; `permissive` checks lengths only |
| `exchange.command_queue_capacity` | `10000` | Commands waiting for the exchange thread; when full, requests get `503 overloaded` |
| `exchange.group_commit.max_commands` / `max_wait_us` | `256` / `0` | Group commit: most orders and cancellations per commit (`1` commits each on its own); how long to wait for more once the queue is empty |
| `events.simulation_id` | unset | Stamped on every event as `simulationId` |
| `price_sources.finnhub.api_key_file` | unset | File holding the Finnhub API key; else `FINNHUB_API_KEY`. Without a key the feed does not run (see Finnhub live prices) |
| `price_sources.finnhub.stale_after_ms` / `refresh_after_ms` | `120000` / `60000` | Real time without an update before a live price is stale / before a symbol is refreshed over REST |
| `price_sources.finnhub.max_updates_per_second`, `max_symbols`, `rest_calls_per_minute` | `4`, `50`, `60` | Live price conflation per instrument; Finnhub free-tier limits |
| `price_sources.finnhub.ws_url`, `rest_url`, `request_timeout_ms`, `reconnect_initial_ms` / `reconnect_max_ms` | Finnhub's, `10000`, `1000` / `60000` | Endpoints, timeout and reconnect backoff |
| `market_data.conflation_interval_ms` / `stream_buffer` | `0` / `1024` | Market-data stream: at most one message per instrument and kind per interval (`0`: every message); how far a subscriber may fall behind before its stream is closed (see `GET /api/marketdata/stream`) |
| `webhooks.poll_interval_ms` | `250` | How often due deliveries (retries) are looked for; new events are sent at once |
| `webhooks.batch_size` / `concurrency` | `100` / `16` | Deliveries claimed per round / concurrent requests |
| `webhooks.request_timeout_ms` | `5000` | Per request; a timeout is a failed attempt |
| `webhooks.max_attempts` | `10` | Attempts in total; then the delivery is dead-lettered (`DEAD`) |
| `webhooks.initial_backoff_ms` / `max_backoff_ms` | `500` / `300000` | Exponential backoff between attempts, with jitter |
| `webhooks.subscriptions` | `[]` | Subscriptions declared in configuration, reconciled by name at startup (see Declared subscriptions) |
| `webhooks.secret_grace_seconds` | `3600` | When the configuration changes a declared subscription's secret, how long the previous one keeps signing too |
| `ids.mode` / `ids.seed` | `random` / `0` | `deterministic` makes every ID reproducible from the seed (see Reproducible runs) |
| `settlement.mode` | `external` | `external`, `simulated` or `both` (see Settlement) |
| `settlement.tick_interval_ms` | `250` | How often the simulator takes in trades and fires due transitions, and clearing issues due statements |
| `settlement.seed`, `settlement_days`, `*_probability`, `max_delay_days`, `max_delays`, `correction_days` | `1`, `1`, fail 0.02, delay 0.05, others 0, `3`, `2`, `1` | Simulation policy (see Settlement) |
| `settlement.market_settlement_days` | `{}` | Settlement cycle per market, by MIC, for markets not on `settlement_days`, e.g. `{ XCSE: 2 }` |
| `clearing.enabled`, `cutoff`, `time_zone` | `false`, `"17:30"`, `"UTC"` | End-of-day clearing statements at the cutoff, local time in an IANA zone (see Clearing) |
| `clearing.breaks.seed`, `*_probability`, `max_late_minutes` | `1`, all 0, `60` | Seeded reconciliation breaks: `omit`, `alter`, `phantom`, `late` (see Clearing) |
| `clock.mode` | `system` | `virtual` gives a controllable clock for domain time (see Test control) |
| `clock.start` / `clock.running` | unset / `false` | Virtual clock only: first reading (default: real time at first start); start running instead of frozen |
| `clock.speed` | `1` | Virtual clock only: simulated seconds per real second while it runs, more than 0 and at most 1000000 (1440: a day a minute). The system clock refuses any other value than 1 |
| `test_control.enabled` | `false` | Mounts `/test/...`. Never enable outside test deployments |
| `database.mode` | `durable` | `durable`, `fast` or `memory` (see Persistence modes) |
| `database.path` | `data/exchange.db` | SQLite file (WAL mode); unused in `memory` mode |
| `database.memory_max_mib` | `4096` | `memory` mode only: the most the in-memory database may grow to |
| `database.busy_timeout_ms` | `5000` | SQLite busy timeout |
| `database.checkpoint` / `checkpoint_pages` | `background` / `1000` | Where WAL checkpoints run: `background` (a separate connection, from `checkpoint_pages` pages; see Persistence) or `inline` (SQLite's automatic checkpoint inside a commit) |
| `logging.filter` / `logging.format` | `info` / `pretty` | Used when `RUST_LOG` is unset; `json` for structured logs |

## Persistence modes

`database.mode` (environment: `EXCHANGE__DATABASE__MODE`) trades durability for speed. In every mode a caller is
answered only after its command is committed, and memory never holds state the database does not.

| Mode | Storage | Survives a process or container restart | Survives a host crash or power loss |
|---|---|---|---|
| `durable` (default) | WAL, `synchronous=FULL`: each commit is synced to disk | everything | everything acknowledged |
| `fast` | WAL, `synchronous=NORMAL`: synced only at checkpoints | everything | all but the last commits before the crash; never a corrupt database |
| `memory` | in-memory SQLite (`memdb`), nothing on disk | nothing | nothing |

- **`fast`:** orders, books, trades, events, the webhook outbox, settlement instructions, the settlement simulator's
  position and the virtual clock are all in the database file, so everything carries over a restart, settlement
  included. (`synchronous=OFF` was rejected: it can corrupt the database on a host crash.)
- **`memory`:** for pure throughput runs. Orders, trades, pending settlements and undelivered webhooks are lost when
  the process stops, so settlement works only while it runs.
  - The database lives in RAM. It needs about 2.6 KiB per order with no webhook subscribers, and 3.7 KiB with a
    subscriber to every event type. The default cap of 4 GiB (`memory_max_mib`) holds roughly a million orders;
    beyond it, commands fail with a storage error.
  - SQLite grows it by doubling one buffer, so leave RAM for two to three times its size.
  - There is no WAL in memory: an API read waits for the current commit, and the writer waits for a read in
    progress.
- **How the mode is reported:** `GET /health` includes `"persistence": "<mode>"`. `/metrics` has
  `exchange_persistence_mode{mode}`, which is 1 for the active mode. In `fast` and `memory` mode every response
  carries `Exchange-Persistence: <mode>`, so no client mistakes the service for a durable one.
- **Kubernetes and Docker:** durability also depends on where the file lives. With `durable` or `fast` in Kubernetes,
  put `database.path` on a persistent volume. An `emptyDir` is lost when the pod is rescheduled, so it then keeps
  nothing across pods even in `durable` mode. The same holds for a container without a volume in Docker; the compose
  file mounts one.

## Model

- **Prices** are integers in *ticks*. An instrument has `currencyScale` decimals per major unit and `tickSizeMinor`
  minor units per tick, so with DKK, scale 2 and tick 1, `100.25` is `10025` ticks. Requests take `priceTicks`.
  Responses add a decimal `price` string.
- **Matching** follows price-time priority. The execution price is the resting order's price. A `LIMIT` remainder
  rests on the book; a `MARKET` remainder is cancelled (status `CANCELLED`, `remainingQuantity` keeps the unfilled
  part).
- **Self-trade:** an incoming order that would trade against its own participant's resting order is rejected whole.
  It is recorded with status `REJECTED` and `rejectReason: "SELF_TRADE"`. Orders for a disabled instrument are recorded
  the same way, with `rejectReason: "INSTRUMENT_DISABLED"`.
- **Durability:** a successful response means the order, any resting orders it traded with, its executions, its events
  and their pending webhook deliveries are committed to SQLite. Books are rebuilt from SQLite on startup.
- **Group commit:** orders and cancellations already waiting in the queue are committed together, up to
  `exchange.group_commit.max_commands` per commit. Each is still planned and written in queue order and sees the
  ones before it, so outcomes, sequences and the event log are exactly as if each were committed alone. Every caller
  is answered only after the shared commit. If one command's write fails, only that command fails. If the commit
  itself fails, none of the group is stored, every caller in it gets `500`, and the books are reloaded from SQLite.
  Any other request (a book snapshot, a registration) ends the group and runs after it. Under light load groups are
  small (often one order), so latency is unchanged; under heavy load one fsync serves many orders.
- **WAL checkpoints:** SQLite appends commits to a write-ahead log (WAL) and copies them into the database file in
  checkpoints. By default (`database.checkpoint: background`) the writer never checkpoints inside a commit:
  - A separate connection runs a `PASSIVE` checkpoint each time the WAL has grown by `checkpoint_pages` pages
    (default 1000, SQLite's own threshold). It never blocks the writer.
  - Under load the WAL cannot restart while commits keep arriving. So once it holds four times the threshold, the
    writer runs one checkpoint itself, when the background has left little to copy, or regardless at eight times.
  - The WAL therefore stays at about 16 MiB with the defaults. `inline` restores SQLite's automatic checkpoint
    (4 MiB of WAL, and a checkpoint inside the commit that fills each 1000 pages).
- **Sequence:** every accepted or rejected order gets the next exchange-wide `sequence`, which is its time priority.
  Malformed requests get no sequence.

## API

The OpenAPI 3.1 specification is [`openapi.yaml`](openapi.yaml), also served at `GET /openapi.yaml`. It describes
every endpoint, the `/test` routes under the `test-control` tag, and every event type in its `webhooks` section;
`tests/openapi.rs` checks it against the service (see Testing).

All bodies are JSON with camelCase fields. Errors look like `{"error": "<code>", "message": "..."}`:

| Status | `error` | When |
|---|---|---|
| 400 | `invalid_json` | Malformed JSON, wrong types, unknown fields, unknown enum values |
| 400 | `invalid_request` / `invalid_order` | Field validation (identifiers, blank names, quantity/lot, price presence) |
| 404 | `not_found` | Unknown order or instrument in the path |
| 409 | `already_exists`, `client_order_id_conflict`, `order_not_active` | Conflicts with current state |
| 422 | `unknown_instrument`, `unknown_participant` | A request body references something not registered |
| 503 | `overloaded`, `unavailable` | Command queue full, or the exchange thread stopped |
| 500 | `internal_error` | Storage failure; nothing was applied |

### `POST /api/instruments`

```json
{ "id": "ACME", "symbol": "ACME", "mic": "XCSE", "isin": "DK0010274414", "cusip": null,
  "currency": "DKK", "currencyScale": 2, "tickSizeMinor": 1, "lotSize": 1, "enabled": true,
  "referenceSymbol": null, "priceSource": { "kind": "fixed", "price": "100.25" } }
```

`mic`, `isin`, `cusip` are optional; `tickSizeMinor` and `lotSize` default to 1, and `enabled` to `true`. `id` is the immutable key that
orders reference. `(mic, symbol)` must be unique when `mic` is given; ISIN and CUSIP may be shared by several
listings. `referenceSymbol` (optional, 1 to 64 printable characters without spaces) is the symbol a price source
quotes the instrument under, e.g. `AAPL`, when it differs from `symbol`. Returns `201` with the instrument, plus
`tickSize` as a decimal string, and publishes `InstrumentListed`. `priceSource` (optional) is the instrument's own
price source; see [Price sources](#price-sources).

Instruments can also be declared in configuration (`instruments:`, see
[Configuration](docs/configuration.md#instruments)); they are listed at startup and again after a reset.

### `GET /api/instruments?symbol=&mic=&isin=&cusip=`

All filters are optional and combined with AND. External identifiers can match several listings.

### `PATCH /api/instruments/{id}`

Changes `enabled`, `referenceSymbol`, `priceSource`, or any of them; absent fields stay as they are, `null` clears
`referenceSymbol` or `priceSource` (which then falls back to the default), and an empty body is a `400`. A new
`priceSource` restarts the source at once. `{ "enabled": false }` halts trading and `{ "enabled": true }` resumes it. It returns
`200` with the instrument, or `404` for an unknown ID; no other field can change. A change that alters a value
publishes `InstrumentUpdated`; one that alters nothing publishes nothing. A declared instrument can be changed too,
but the configuration wins again at the next start. While an instrument is disabled:
- **Orders are recorded rejections:** every new order is sequenced and stored with status `REJECTED` and
  `rejectReason: "INSTRUMENT_DISABLED"`, and an `OrderRejected` event is emitted (`201`, like a self-trade).
- **Cancels keep working.**
- **Resting orders stay** on the book, and trade again once the instrument is re-enabled.

The flag survives restarts.

### `POST /api/participants`

`{ "id": "participant-a", "name": "Participant A", "settlement": { "account": "DK-0001", "bic": "DABADKKK" } }` → `201`
with the participant. `settlement` is optional free-form JSON: an object of at most 4 KiB, for example account, BIC
or custodian. It is copied into every `TradeExecuted` the participant is party to, as `buyerSettlement` or
`sellerSettlement`. It cannot be changed after registration yet.

### `POST /api/orders`

```json
{ "clientOrderId": "c-1", "instrumentId": "ACME", "participantId": "participant-a",
  "side": "BUY", "type": "LIMIT", "priceTicks": 10025, "quantity": 150 }
```

`side` is `BUY`/`SELL`; `type` is `LIMIT` (needs `priceTicks`) or `MARKET` (no `priceTicks`). `clientOrderId` is
optional, 1–64 characters, and unique per participant.

- `201`: the order was processed (including `REJECTED`). Body: `{ "order": Order, "executions": [Trade], "duplicate": false }`.
- `200`: `clientOrderId` was already used with the same terms. Body: the existing order in its current state, with
  `"duplicate": true` and no executions.
- `409 client_order_id_conflict`: `clientOrderId` was already used with different terms.

`Order`: `orderId`, `clientOrderId`, `instrumentId`, `participantId`, `side`, `type`, `priceTicks`, `price`,
`quantity`, `remainingQuantity`, `filledQuantity`, `status` (`OPEN`, `PARTIALLY_FILLED`, `FILLED`, `CANCELLED`,
`REJECTED`), `sequence`, and `rejectReason` when rejected.

### `GET /api/orders/{id}`

The stored order, plus `createdAt` and `updatedAt`.

### `DELETE /api/orders/{id}`

Cancels the remaining quantity of an `OPEN` or `PARTIALLY_FILLED` order. Returns `200` with the same body as
`POST /api/orders`; `409 order_not_active` if the order is already done.

### `GET /api/orderbooks/{instrumentId}`

```json
{ "instrumentId": "ACME", "currency": "DKK", "enabled": true, "lastSequence": 2,
  "bids": [], "asks": [{ "priceTicks": 10025, "price": "100.25", "quantity": 50, "orders": 1 }] }
```

Levels are listed best first. `enabled` shows whether the instrument is trading. `lastSequence` is the sequence of the
last order processed before the snapshot.

### Price sources

Each instrument has a price source that produces its **reference price**, the price the built-in liquidity provider
will quote around and the market data publishes. The demo never pushes prices into the exchange; it reads them.

| `kind` | Produces | Fields |
|---|---|---|
| `none` | No reference price; the instrument trades only on client orders (the default) | – |
| `fixed` | A constant price | `price` |
| `synthetic` | A seeded geometric Brownian motion on simulated time | `initialPrice`, `drift`, `volatility`, `tickIntervalMs`, `seed` |
| `finnhub` | Live trade prices from Finnhub for the instrument's `referenceSymbol` (else its `symbol`) | – (the service needs an API key, below) |

```json
"priceSource": { "kind": "synthetic", "initialPrice": "187.50", "drift": 0.05, "volatility": 0.3,
                 "tickIntervalMs": 1000 }
```

- **Defaults:** fields an instrument leaves out come from `price_sources.default` in the configuration, field by
  field, then from the built-in defaults: kind `none`, drift 0, volatility 0.2, a step a second. A configuration that
  sets `default: { kind: synthetic, volatility: 0.3 }` needs only an `initialPrice` per instrument.
- **The synthetic walk** steps on a grid of `tickIntervalMs` of simulated time, starting when the source was set. Each
  step multiplies the price by `exp((μ − σ²/2)Δt + σ√Δt·Z)`, with `drift` μ and `volatility` σ annualized (a year of
  365.25 simulated days). Prices are rounded to the nearest tick and never fall below one tick.
- **Reproducible:** the path depends only on the parameters, the seed and when the source was set, never on when it
  is read. With deterministic IDs and a virtual clock, two runs give the same prices, across restarts and resets.
  `seed` defaults to one derived from `price_sources.seed` and the instrument ID.
- **Runtime changes:** `PATCH /api/instruments/{id}` with a new `priceSource` restarts the source without a restart of
  the service. A switch to `synthetic` with no `initialPrice` continues from the current reference price, which is
  stored as its `initialPrice`.
- **Clock jumps** catch the walk up step by step on the exchange thread: about 50 ms per instrument for 30 simulated
  days of one-second steps. `tickIntervalMs` is at least 100.

#### Finnhub live prices

`finnhub` streams real trades for `referenceSymbol` (`AAPL`, `BINANCE:BTCUSDT`; the exchange `symbol` when unset)
from Finnhub's trades websocket, and seeds or refreshes each symbol from its REST quote endpoint on connect and
whenever it has been quiet for `price_sources.finnhub.refresh_after_ms` (a closed market streams no trades).
- **The API key** comes from the `FINNHUB_API_KEY` environment variable, which both Compose files pass through from
  the host (`FINNHUB_API_KEY=… docker compose up`); `price_sources.finnhub.api_key_file` reads it from a file instead.
  Without one the feed does not run and `finnhub` sources are refused (a startup error for a declared instrument, a
  `400` from the API). The key is never logged, returned or used in a metric label.
- **Time:** a price takes effect at the exchange's current time, also on a virtual or accelerated clock, and keeps
  Finnhub's own time as `sourceTimestamp`. Live prices are not reproducible; `source: "finnhub"` says so.
- **Conflation:** each instrument takes in at most `max_updates_per_second` new prices a second, the latest winning.
- **Stale:** with nothing from Finnhub for `stale_after_ms` (real time; the connection or REST down), the last price
  stays, marked `stale: true` in the market data, and the liquidity provider applies its `stalePolicy`. The feed
  reconnects with backoff and jitter, subscribes every symbol again, and the price is fresh with the next update. A
  feed that is down never stops the exchange.
- **Limits:** at most `max_symbols` distinct symbols (Finnhub's free tier: 50) and `rest_calls_per_minute` REST calls
  (free tier: 60). A source past the symbol limit is refused like a missing key; a REST call past the rate limit is
  skipped and counted. Finnhub's free tier is for personal, non-commercial use.
- **Changing `referenceSymbol`** of a `finnhub` instrument unsubscribes the old symbol and subscribes the new one.
- **Metrics:** `exchange_price_feed_connected`, `exchange_price_feed_reconnects_total`,
  `exchange_price_feed_updates_total{via}` (`websocket`, `rest`), `exchange_price_feed_rest_calls_total{outcome}`.

### Liquidity provider

A built-in participant (`liquidity.participant_id`, default `LP`) quotes a ladder of limit orders around each
instrument's reference price, so a demo whose orders all come from one participant always has a counterparty. It is
off unless an instrument's `liquidity` (or `liquidity.default`) sets `mode: on`:

```json
"liquidity": { "mode": "on", "levels": 5, "spreadBps": 10, "levelStepTicks": 1, "levelQuantity": 100,
               "levelQuantityGrowth": 1, "requoteThresholdTicks": 1, "refillDelayMs": 0, "skewBpsPerLot": 0,
               "maxInventory": 10000, "stalePolicy": "pull", "staleWidenFactor": 2 }
```

- **Ladder:** `levels` limit orders a side. The best bid and ask are `spreadBps` from the mid, rounded away from it to
  the tick (with R = 100.00, 10 bps and a 0.01 tick: 99.90 and 100.10); each deeper level is `levelStepTicks`
  further out and holds `levelQuantity` lots, times `levelQuantityGrowth` per level. Its orders are ordinary limit
  orders: a market order sweeps the ladder level by level and its remainder is cancelled (`MARKET_REMAINDER`) when
  the ladder runs out; a crossing limit order fills at the ladder's prices; a limit order inside the spread rests.
- **When it quotes:** after every order and cancellation, after an instrument change, and on every tick. It quotes
  again when the reference price moved `requoteThresholdTicks` from its last quote, and refills `refillDelayMs` of
  simulated time after a fill. The old ladder is cancelled and the new one placed in one commit, inside the exchange
  thread, so no order sees half a ladder. With mode `off`, trading disabled or no reference price, it pulls its ladder.
- **Inventory:** its net position, in lots, moves the mid `skewBpsPerLot` per lot against the position: a long
  provider lowers both sides, so one-sided client buying pushes the price up. At `maxInventory` lots it stops quoting
  the side that would add to the position.
- **Stale reference price** (a live source gone quiet): `stalePolicy` `pull` (the default) cancels the ladder until
  the price is fresh, `widen` quotes it with the spread times `staleWidenFactor`, `keep` leaves it as it was quoted.
  The market data's `liquidity` says which applies: `kept`, `widened` or `pulled`.
- **Events:** its order lifecycle (accepted, filled, cancelled) stays out of the event log and webhooks unless
  `liquidity.emit_order_events` is true. Its trades are always published (`TradeExecuted` with it as buyer or seller),
  and it settles and appears on clearing statements like any participant. Metrics count its quotes
  (`exchange_liquidity_quotes_total`), ladders replaced (`exchange_liquidity_requotes_total`) and fills
  (`exchange_liquidity_fills_total`) either way.
- **After a restart or reset** it quotes afresh: its inventory comes from its trades, its old ladder is replaced
  (decision D18). On a frozen clock what it does depends only on the commands, so reproducible runs stay byte-identical.

### `GET /api/marketdata` and `GET /api/marketdata/{instrumentId}`

```json
{ "instrumentId": "AAPL", "mdSequence": 41,
  "reference": { "price": "187.52", "priceTicks": 18752, "source": "synthetic",
                 "sourceTimestamp": "2030-01-07T09:01:30Z", "stale": false },
  "bestBid": { "priceTicks": 18733, "price": "187.33", "quantity": 100 },
  "bestAsk": { "priceTicks": 18771, "price": "187.71", "quantity": 100 },
  "lastTrade": { "priceTicks": 18771, "price": "187.71", "quantity": 25, "executedAt": "2030-01-07T09:01:12Z" },
  "sessionVolume": 1250, "liquidity": "quoting" }
```

Each instrument's market data, in instrument ID order; `404` for an unknown instrument.
- `reference` is `null` for a `none` source. `sourceTimestamp` is when the price was produced, on the exchange's
  clock: when a fixed source was set, or the synthetic step's grid time. `stale` is for live sources (always `false`
  for `fixed` and `synthetic`).
- `bestBid` and `bestAsk` (`null` for an empty side) carry the quantity resting at the best price. `lastTrade` has no
  participants; `null` before the first trade.
- `sessionVolume` is the quantity traded on the current business date, which ends at `clearing.cutoff`.
- `liquidity` is what the liquidity provider did when it last quoted: `off` (no provider, mode `off`, or trading
  disabled), `no-reference`, `quoting`, or under a stale price `kept`, `widened` or `pulled`.
- `mdSequence` is that of the last stream message about the instrument (below), so the snapshot and the stream line
  up.

The last trade and the session volume survive a restart; `mdSequence` starts again from 0 with the service.

### `GET /api/marketdata/stream?instruments=`

Server-sent events for the listed instruments (comma-separated; all when left out; `400` for an unknown one):

```text
event: quote
id: 42
data: {"instrumentId":"AAPL","mdSequence":42,"bestBid":{"priceTicks":18733,"price":"187.33","quantity":75},"bestAsk":{...}}

event: trade
id: 43
data: {"instrumentId":"AAPL","mdSequence":43,"priceTicks":18733,"price":"187.33","quantity":25,"executedAt":"..."}

event: reference
id: 44
data: {"instrumentId":"AAPL","mdSequence":44,"reference":{"price":"187.40",...,"stale":false}}
```

- **`quote`** when the best bid or ask (price or quantity) changes, **`trade`** for each print (anonymous), and
  **`reference`** when the reference price changes, goes stale or goes away. Each carries the instrument's
  `mdSequence`, which counts its messages from 1 and is the event's `id:`.
- Messages are published once a whole command is done: an order, with the matching and the liquidity provider's
  re-quote it caused; a cancellation; an instrument change; a clock tick. A trade's prints come before the quote
  they leave behind. Intermediate states never reach the feed.
- **To start:** open the stream, then read the snapshot, and skip messages at or below the snapshot's `mdSequence`.
- **Conflation** (`market_data.conflation_interval_ms`, off by default): at most one message per instrument and
  kind per interval, the latest. `quote` and `reference` carry their whole state, so nothing is lost but
  intermediate values; prints left out remain in `GET /api/trades`.
- **Slow subscribers** never slow the exchange. More than half of `market_data.stream_buffer` behind, a subscriber's
  backlog collapses to the latest message per instrument and kind; a whole buffer behind, its stream is closed.
  Reconnect and start again from the snapshot. Metrics: `exchange_market_data_messages_total{kind}`,
  `exchange_market_data_catch_ups_total`, `exchange_market_data_streams_closed_total`.
- Market data is not in the event log and not delivered over webhooks.

```bash
curl -N 'localhost:8080/api/marketdata/stream?instruments=AAPL'
```

### `GET /api/trades?instrumentId=&participantId=&orderId=&limit=`

Executions in the order they were created. `participantId` matches either buyer or seller, and `orderId` either
order. `limit` is 1–1000 (default 100). `Trade`: `executionId`, `instrumentId`, `buyOrderId`, `sellOrderId`,
`buyerId`, `sellerId`, `priceTicks`, `price`, `quantity`, `currency`, `executedAt`.

### `GET /health`

`{"status": "ok", "schemaVersion": <n>, "persistence": "<mode>"}` (`schemaVersion` is the number of the last applied
migration in `migrations/`), or `503` with `{"status": "unavailable"}` if the database does not answer.

### `GET /openapi.yaml`

This API's OpenAPI 3.1 specification ([`openapi.yaml`](openapi.yaml)), as `application/yaml`.

### `GET /api/events?after=&limit=&eventType=`

Canonical events with `sequence > after` (default 0), in sequence order. `limit` is 1–1000 (default 100). To replay
everything, pass the last `sequence` you received as `after` until the result is empty. Envelope:

```json
{ "eventId": "0199…", "sequence": 3, "eventType": "TradeExecuted", "schemaVersion": 1, "source": "exchange",
  "simulationId": null, "aggregateId": "<executionId>", "correlationId": "<incoming orderId>",
  "occurredAt": "2026-10-14T12:00:00.000000Z", "payload": { … } }
```

Event types and payloads (`schemaVersion` 1):

| Event | Aggregate | Payload |
|---|---|---|
| `OrderAccepted` | order | order as accepted (status `OPEN`, nothing filled) |
| `OrderRejected` | order | order with `rejectReason` |
| `TradeExecuted` | execution | `executionId`, `instrumentId`, `instrument` {`id`, `symbol`, `mic`, `isin`, `cusip`, `currency`}, `buyOrderId`, `sellOrderId`, `buyerId`, `sellerId`, `buyerSettlement`, `sellerSettlement` (each party's settlement details at execution time, or `null`), `priceTicks`, `price`, `quantity`, `currency`, `currencyScale`, `tickSizeMinor`, `executedAt` |
| `OrderPartiallyFilled` / `OrderFilled` | order | order after the fill; for a resting order also `executionId` |
| `OrderCancelled` | order | order with `cancelReason`: `REQUESTED` or `MARKET_REMAINDER` |

Settlement lifecycle events come from the optional settlement simulator (`source: "settlement-simulator"`). Their
aggregate and correlation ID is the `executionId`, and their `occurredAt` is the scheduled instant: the trade time at
intake, otherwise 00:00 UTC of the relevant date. Every one carries:
- `executionId` and `tradeSequence`, the `TradeExecuted` event's `sequence`
- `instrumentId`, `buyerId`, `sellerId`, `quantity`, `priceTicks`, `currency`
- `settlementAmount` (decimal string) and `amountMinor`
- `tradeDate` and `intendedSettlementDate`
- `status` and `attempt`
- with clearing enabled, `clearing`: `businessDate`, and for `buyer` and `seller` the `statementId` and
  `obligationId` the trade appears on (see Clearing)

| Event | Extra payload | Status after |
|---|---|---|
| `SettlementPending` | `settlementDays` | `PENDING` (due at 00:00 UTC on the intended settlement date, T+N business days) |
| `SettlementRejected` | `reason`: `SIMULATED_REJECTION` or `AMOUNT_OUT_OF_RANGE` | `REJECTED` |
| `SettlementDelayed` | `previousIntendedSettlementDate`, `delayBusinessDays` | `DELAYED` (due on the new date) |
| `SettlementCompleted` | `settledAt` | `SETTLED` |
| `SettlementFailed` | `reason`: `SIMULATED_FAILURE` | `FAILED` |
| `SettlementCancelled` | `reason`: `SIMULATED_CANCELLATION` | `CANCELLED` |
| `SettlementOutcomeReported` | `source: "external"`: `reportId`, `buyerId`, `sellerId`, `status`, `reference`, `reason`, `details`, `receivedAt` (not a lifecycle step of the simulator) | – |
| `SettlementCorrected` | `reason`: `SIMULATED_CORRECTION`, `previousSettlementAmount`, `previousAmountMinor` (the amount moves by one tick per unit) | `CORRECTED` |

`ClearingStatementIssued` (`source: "clearinghouse"`, aggregate ID the statement ID, correlation ID the participant)
carries the whole statement, as `GET /api/clearing/statements/{id}` returns it.

`InstrumentListed` and `InstrumentUpdated` (`source: "exchange"`, aggregate ID the instrument ID, no correlation ID)
carry the whole instrument after the change, as `GET /api/instruments` shows it, with `instrumentId` for `id`.
`InstrumentUpdated` adds `changes`, the fields whose value changed (`enabled`, `referenceSymbol`). Every listing is an
event, declared instruments included, so a consumer learns the instruments from the log alone.

Order payloads carry `orderId`, `clientOrderId`, `instrumentId`, `participantId`, `side`, `type`, `priceTicks`, `price`,
`quantity`, `remainingQuantity`, `filledQuantity`, `status` and `sequence`. Within one command, events come in this
order: `OrderAccepted`, then for each fill `TradeExecuted` followed by the resting order's fill event, then the
incoming order's fill event, then `OrderCancelled` for a market remainder. A command's events all share
`correlationId`, the incoming (or cancelled) order's ID.

### `POST /api/webhooks`

`{ "url": "http://consumer:8080/trades", "eventTypes": ["TradeExecuted"], "secret": "…", "ordered": false }` → `201`
`{ "id", "url", "eventTypes", "enabled", "paused", "signed", "ordered", "createdAt", "source": "api" }`. Subscriptions
declared in configuration also show their `name`, with `source: "config"`. The URL must be `http` or `https`, and the event
types must be ones listed above. `secret` is optional (16–256 characters). When it is given, deliveries are signed. It
is never returned; responses show `signed: true` instead. A subscription receives events committed after it was registered; replay earlier events with
`/api/events`.

**Participant filter:** `"participants": ["A", "B"]` (1 to 100 participant IDs; shown in responses when set) narrows
a subscription to those participants, so two independent demo stacks can share one exchange:
- order events (`OrderAccepted`, `OrderRejected`, fills, `OrderCancelled`) only for orders of a listed participant;
- `TradeExecuted`, settlement events (`SettlementOutcomeReported` included) and `ClearingStatementIssued` when either
  side, or the statement's participant, is listed;
- events that involve no participant, such as `InstrumentListed`, always.

Redelivery applies the same filter. `PATCH /api/webhooks/{id}` with `"participants": null` removes it. The payment
gateway has no participants and refuses the field.

Delivery is a `POST` of the event envelope with headers `X-Event-Id`, `X-Event-Type`, `X-Event-Sequence`,
`X-Subscription-Id` and `X-Delivery-Attempt`.
- **Retries:** any 2xx response counts as delivered. Anything else, including a timeout or a connection error, is
  retried with exponential backoff and jitter, up to `webhooks.max_attempts`; then the delivery is marked `DEAD`.
- **At least once:** deliveries are durable and survive restarts, and a consumer may receive the same event more than
  once. Deduplicate by `eventId`.
- **Ordering:** by default deliveries are sent concurrently and may arrive out of order; use `sequence` for canonical
  order. With `"ordered": true`:
  - The subscription has at most one delivery in flight, always its unfinished event with the lowest `sequence`, so
    events arrive in `sequence` order.
  - A failing delivery blocks the ones behind it while it is retried.
  - If it is dead-lettered, delivery continues with the next event. The consumer sees a gap in `sequence` and can fill
    it from `/api/events`.
  - Throughput per ordered subscription is one round trip per event.
- **Signing:** for a subscription with a secret, each attempt also carries `X-Signature-Timestamp` (unix seconds, the
  time of the attempt) and `X-Signature: sha256=<hex>`, the HMAC-SHA256 under the secret of
  `"{X-Signature-Timestamp}.{raw body}"`. A retry is signed again with a new timestamp; its body is byte-identical.
  Subscriptions without a secret are unsigned, so use them on test networks only.
- **Rotation:** for the grace period after a secret rotation, `X-Signature` carries one value per secret,
  comma-separated with the current secret first: `sha256=<new>, sha256=<previous>`. A request is authentic if any
  value matches. A consumer can therefore switch to the new secret at any point in the grace period without rejecting
  a delivery.

To verify a signature, recompute the HMAC over the raw body *as received*, not over a re-serialization, compare in
constant time against each value in the header, and reject timestamps far from your clock (e.g. more than 5 minutes)
to stop replays:

```python
import hashlib, hmac, time

def verify(secret: bytes, headers, raw_body: bytes, tolerance=300) -> bool:
    timestamp = headers["X-Signature-Timestamp"]
    expected = "sha256=" + hmac.new(secret, timestamp.encode() + b"." + raw_body, hashlib.sha256).hexdigest()
    candidates = [value.strip() for value in headers["X-Signature"].split(",")]
    matches = any(hmac.compare_digest(expected, candidate) for candidate in candidates)
    return matches and abs(time.time() - int(timestamp)) <= tolerance
```

### Declared subscriptions

For a standing consumer, declare the subscription in the configuration file (named by `EXCHANGE_CONFIG`) rather than
registering it through the API. That way it is in place on every start, comes back after `/test/reset`, and is
reviewed with the rest of the deployment:

```yaml
webhooks:
  subscriptions:
    - name: settlement                       # its identity: 1-64 letters, digits, '.', '_' or '-'
      url: http://settlement-service:8080/trades
      event_types: [TradeExecuted]
      secret_env: SETTLEMENT_WEBHOOK_SECRET  # or secret: "...", or secret_file: /run/secrets/settlement
      ordered: true                          # default false
    - name: audit
      url: http://audit:9000/events
      event_types: [OrderAccepted, OrderCancelled, TradeExecuted]
      paused: true                           # default false
```

- **Startup:** the service reconciles the list by `name`.
  - A missing subscription is created. One whose URL, event types, secret, ordering or flags changed is updated in
    place, keeping its ID and its pending deliveries, which then go to the new URL.
  - One that is no longer listed is disabled, not deleted, so its history stays.
  - Subscriptions created through the API are left alone.
- **Startup fails** with a message naming the subscription if an entry is invalid: a duplicate or malformed name, an
  unknown event type, a non-http URL, a secret of the wrong length, a missing `secret_env` variable or `secret_file`,
  or more than one secret source. A YAML key that is not listed above is refused too, so typos do not pass silently.
- **`paused: true`** records a delivery for every matching event but sends none until it is unpaused. They then go
  out in order. Use it for a consumer under maintenance.
- **`enabled: false`** turns the subscription off: no deliveries are recorded at all.
- **Secrets:** `secret_env` names an environment variable and `secret_file` a file, e.g. a Docker secret under
  `/run/secrets`. Surrounding whitespace is ignored. The secret is never returned by the API.
- **Changing a declared subscription:** edit the file and restart the service. The configuration is its source of
  truth: through the API (below) only `paused` can be changed, and only until the next restart reapplies the file.
  Every other change, a delete or a secret rotation gets `409 managed_by_configuration`. A secret changed in the file
  keeps the previous one signing for `webhooks.secret_grace_seconds` (default an hour), across restarts.
- **Scenarios:** the scenario sandbox (`/test/scenarios`, `scenario run`) never inherits declared subscriptions, so
  sandbox events never reach real consumers.
- **Environment variables:** a list cannot be set through `EXCHANGE__…` variables; use the file.

### `GET /api/webhooks?includeDeleted=`

Lists the subscriptions; deleted ones only with `includeDeleted=true` (shown with `deleted: true`).

### Maintaining a subscription

For subscriptions created through the API. One declared in configuration takes only `paused`; the rest get `409`.
Inspecting, listing deliveries, retrying and redelivering work for every subscription. A deleted or unknown ID gets
`404`.

- **`GET /api/webhooks/{id}`:** the subscription and how its deliveries stand:
  `"deliveries": { "pending", "inFlight", "delivered", "dead", "oldestUnfinished": { "eventSequence", "occurredAt" } }`.
  `oldestUnfinished` is the oldest event not yet delivered, which shows how far behind the consumer is.
- **`PATCH /api/webhooks/{id}`** with any of `{ "url", "eventTypes", "ordered", "enabled", "paused" }` → `200` and the
  subscription.
  - Pending deliveries keep their place and go to the new URL.
  - `paused: true` holds deliveries; they go out in order when it is unpaused.
  - `enabled: false` records no new deliveries at all.
  - Validation is as for `POST`.
- **`DELETE /api/webhooks/{id}`** → `204`. The subscription disappears from the API, gets no new deliveries, and sends
  none of its pending ones, retries included. Its deliveries and attempts stay in the database.
- **`POST /api/webhooks/{id}/secret`** with `{ "secret", "graceSeconds" }` → `200`.
  - It replaces the signing secret (16–256 characters; an unsigned subscription becomes signed).
  - For `graceSeconds` (default 3600, at most 604800; 0 for none) the previous secret signs too (see Rotation above).
    The response shows `previousSecretValidUntil`.
  - Secrets are never returned.
- **`GET /api/webhooks/{id}/deliveries?status=&after=&limit=`:** the deliveries in event order, with
  `{ "deliveryId", "eventSequence", "eventId", "eventType", "status", "attempts", "lastError", "nextAttemptAt",
  "deliveredAt" }`.
  - `status` is `PENDING`, `IN_FLIGHT`, `DELIVERED` or `DEAD` (the dead letters).
  - Page with `after` (an event sequence) and `limit` (default 100, at most 1000).
- **`POST /api/webhooks/{id}/dead-letters/retry`** with an optional `{ "deliveryIds": [...] }` → `{ "retried" }`. It
  retries all of the subscription's dead letters, or the listed ones, with a fresh attempt budget.
- **`POST /api/webhooks/{id}/redeliver?after=`** → `{ "queued" }`. It sends every event after sequence `after` again,
  including events from before the subscription existed: a replay for a consumer that lost data.

### `GET /metrics`

Prometheus text format, with these series:
- **Commands:** `exchange_commands_total{command,outcome}` and `exchange_command_duration_seconds{command}`.
- **Hot path, split into stages:** `exchange_match_duration_seconds` and `exchange_commit_duration_seconds` (per
  commit group, the `COMMIT`; with group commit off, each command's whole transaction), and the group sizes in
  `exchange_group_commit_commands`.
- **WAL checkpoints** (with `database.checkpoint: background`): `exchange_wal_checkpoints_total{by,outcome}`,
  `exchange_wal_checkpoint_duration_seconds{by}` (`by` is `background` or `writer`) and `exchange_wal_frames`.
- **Queue:** `exchange_command_queue_depth` and `exchange_command_queue_rejections_total`.
- **Latency breakdown per command:** `exchange_command_queue_wait_seconds{command}` (enqueued to started on the
  exchange thread) and `exchange_command_ack_seconds{command}` (enqueued to answer received by the caller: queue
  wait, execution and commit, and the hand-back). With `exchange_command_duration_seconds` they split an
  acknowledgement into waiting and working. A command refused with 503 was never queued and records neither.
- **Results:** `exchange_orders_total{status}`, `exchange_executions_total`, `exchange_traded_quantity_total` and
  `exchange_events_total{event_type}`.
- **Webhooks:** `webhook_delivery_attempts_total{outcome}`, `webhook_delivery_duration_seconds`, and the outbox
  backlog `webhook_outbox_deliveries{status}`, read from SQLite at scrape time.

## Settlement

`settlement.mode` decides who owns settlement outcomes:
- **`external`** (default): an external service settles trades from the `TradeExecuted` webhook. Nothing is
  simulated.
- **`simulated`**: the built-in simulator creates one instruction per execution and emits `Settlement*` lifecycle
  events with `source: "settlement-simulator"`.
- **`both`**: the simulator runs, and external outcomes may be reported back. They are kept apart, never merged.

**How the simulator works:**
- **Intake:** each trade's instruction and `SettlementPending` are written in the trade's own transaction, so a
  trade and its intake commit together or not at all. The simulator's position in the event log is stored too.
  Trades committed while it was off (or before it was enabled) are taken in at startup, from the beginning of the
  log.
- **Lifecycle:** an instruction is `PENDING` until 00:00 UTC on its intended settlement date: T+N business days,
  Monday to Friday, with no holiday calendars. On that date one draw decides the outcome:
  - `CANCELLED`, `FAILED`, or `DELAYED` by 1..=`max_delay_days` business days (at most `max_delays` times),
    according to `cancel_probability`, `fail_probability` and `delay_probability`
  - otherwise `SETTLED`

  N is the instrument's market entry in `market_settlement_days` (matched on its `mic`), or else `settlement_days`,
  which defaults to 1. T+1 is the cycle in the US, Canada and Mexico since May 2024; the EU, UK and Switzerland move
  to it on 2027-10-11. Until then a European market can keep T+2 with, for example,
  `market_settlement_days: { XCSE: 2 }` (or `EXCHANGE__SETTLEMENT__MARKET_SETTLEMENT_DAYS__XCSE=2`). The cycle is
  chosen at intake and reported as `settlementDays` on `SettlementPending`; changing the configuration does not move
  instructions already pending.

  `reject_probability` rejects at intake. `correct_probability` issues a `SettlementCorrected` `correction_days`
  business days after settlement.
- **Reproducibility:** every draw is a hash of (`seed`, the trade's event sequence, the attempt, the decision). The
  same trades in the same order with the same seed and clock moves give the same lifecycle, independent of tick
  timing and restarts.
- **Timing:** it runs on the exchange clock. With `clock.mode: virtual`, nothing settles until simulated time reaches
  the date, and `POST /test/clock/advance` returns only after everything due by the new time has happened.
- **Durability:** instruction changes, their events, webhook deliveries and the simulator's position commit in one
  transaction.

### `POST /api/settlements/{executionId}/outcomes`

An external settlement service reports an outcome, accepted in `external` and `both` modes; in `simulated` mode it
returns `409 external_outcomes_disabled`. The body:

```json
{ "status": "SETTLED", "reference": "csd-12345", "reason": null,
  "occurredAt": "2030-01-09T10:00:00Z", "details": { "csd": "VP Securities" } }
```

- **Fields:** `status` is one of the settlement statuses (`PENDING`, `DELAYED`, `SETTLED`, `FAILED`, `REJECTED`,
  `CANCELLED`, `CORRECTED`). `reference` (1–128 characters), `reason`, `occurredAt` (default: time of receipt) and
  `details` (a JSON object, at most 4 KiB) are optional.
- **Response:** `201` with the stored report (`reportId`, `receivedAt` on the exchange clock), and it publishes a
  `SettlementOutcomeReported` event with `source: "external"`.
- **Idempotent by `reference`:** the same reference with the same outcome returns the stored report with `200` and
  `"duplicate": true`. With a different outcome it returns `409 report_conflict`.
- **No ordering rules:** the service is the authority for its own reports, so the simulator imposes no state order on
  them.
- **Never merged:** a report never changes a simulated instruction, and a simulated outcome never changes a report.
- **Errors:** `404` for an unknown execution.

### `GET /api/settlements/{executionId}`

Everything known about one execution's settlement, side by side:
- `simulated`: the instruction, or `null` if the simulator is off
- `simulatedEvents`: its lifecycle events
- `external`: the reports in order of receipt

### `GET /api/settlements?status=&limit=`

Simulated instructions in trade order, optionally of one status. `limit` is 1–1000, default 100.

## Business days

At `clearing.cutoff`, local time in `clearing.time_zone`, every weekday, the exchange closes the business day with
**`BusinessDayClosed`** (`source: "exchange"`, aggregate ID the business date, `occurredAt` the cutoff):
`businessDate`, `cutoffAt`, `nextBusinessDate` and `timeZone`. It is the authoritative end-of-day signal for a
consumer's end-of-day work (statements, valuation, closing books):
- **Always:** whether or not clearing is enabled, and also on a day without trades. Weekends are skipped.
- **Before the day's statements:** at the cutoff the day closes first, then that day's `ClearingStatementIssued`.
- **Every day, in order:** a clock move past several cutoffs closes each day in turn, in time order with the
  settlement events around it. A running clock closes days on its own (the background tick, every
  `settlement.tick_interval_ms`). Cutoffs passed while the service was down are closed at startup, once.
- **From now on:** a new database, or a reset, starts with the current business day; no earlier day is closed.

## Clearing

With `clearing.enabled: true`, a clearinghouse issues end-of-day statements on the exchange clock. It plays an outside
party only (decision D8): it keeps no positions or cash, and computes every statement from the trades alone.
How a demo consumes and reconciles these statements, together with the payment gateway's bank statements, is in
the guide's [Back office](docs/back-office.md) chapter.

- **When:** at `clearing.cutoff`, local time in `clearing.time_zone` (daylight saving included), Monday to Friday.
  With `clock.mode: virtual` nothing is issued until simulated time reaches the cutoff, and a clock move issues every
  cutoff it passes, each in time order with the settlement events around it. A statement's `occurredAt` is its
  scheduled instant, so on a running clock it can follow events stamped up to one `settlement.tick_interval_ms` after
  it, as settlement events can. Cutoffs passed while the service was
  down are issued at startup; a cutoff before clearing was first enabled is not.
- **Which trades:** a trade's business date is the first business day whose cutoff is after it. Trades after the
  cutoff, or on a weekend, are on the next business day's statement, and that date is also their trade date: T+N
  counts from it, in the statement and in the settlement simulator, so nothing settles before the statement that
  clears it (T+0 aside). Without clearing, the trade date is the UTC date of execution.
- **What:** one statement per participant that traded since the previous cutoff, ID `CS:<businessDate>:<participant>`:
  `statementId`, `businessDate`, `participantId`, `timeZone`, `tradesFrom` and `cutoffAt` (the window of execution
  times, start included), `issuedAt`, then
  - `trades`: `executionId`, `tradeSequence`, `instrumentId`, `side` (the participant's), `quantity`, `priceTicks`,
    `currency`, `currencyScale`, `amountMinor`, `amount`, `executedAt`, `tradeDate`, `intendedSettlementDate`,
    `obligationId`;
  - `obligations`, one per instrument and intended settlement date, ID `<statementId>:<instrument>:<date>`:
    `securitiesNet` (units; positive receives, negative delivers), `cashNetMinor` and `cashNet` (positive receives),
    `currency`, `currencyScale`, `tradeCount`. Across all participants of a day, obligations net to zero.
- **Settlement references:** with the simulator running, each settlement event's `clearing` field names both sides'
  statement and obligation, from intake on (the IDs are natural keys, so they are known before the statement is).
  A delayed settlement keeps its original obligation.
- **Reconciliation breaks** are opt-in under `clearing.breaks`, seeded and reproducible, like the webhook faults.
  They change only what a statement says (its obligations net what it lists); the trades, the event log and the
  settlement simulator never change:
  - `omit_probability` (per trade line): the trade is left out (`OMITTED_TRADE`);
  - `alter_probability` (per trade line): its quantity or price is off by one unit or tick (`QUANTITY_CHANGED`,
    `PRICE_CHANGED`);
  - `phantom_probability` (per statement with trades): a copy of one of its trades under an execution ID that does
    not exist (`PHANTOM_TRADE`);
  - `late_probability` (per statement): issued 1..=`max_late_minutes` minutes of domain time after the cutoff
    (`LATE_STATEMENT`). Until then it is neither listed nor delivered.

  `GET /test/clearing/breaks` lists what each broken statement says differently, for tests that check a
  reconciliation finds it.

```yaml
clearing:
  enabled: true
  cutoff: "17:30"
  time_zone: Europe/Copenhagen
  breaks: { seed: 7, omit_probability: 0.02, late_probability: 0.1, max_late_minutes: 45 }
```

### `GET /api/clearing/statements?date=&participant=`

Issued statements, optionally of one business date (`YYYY-MM-DD`) and one participant, in date and participant order.

### `GET /api/clearing/statements/{statementId}`

One issued statement, or `404` (also for a late statement not issued yet).

## Reproducible runs

With `ids.mode: deterministic`, every ID is a UUIDv8 derived from `ids.seed` and the natural key of what it names:
- an order, from its sequence
- an execution, from the incoming order's sequence and the fill index
- an event, from its event sequence
- a webhook delivery, from its subscription and event sequence
- a subscription, from its ordinal
- an external report, from its execution and ordinal

With a frozen virtual clock (`clock.mode: virtual`) the same commands then give a byte-identical event log and
identical API responses, run after run, and also when the service is restarted in between. Only real-time delivery
bookkeeping differs: attempts, `deliveredAt`, retry times. `random` (the default) gives UUIDv7s.

Event sequences are assigned by the single writer, and event IDs follow from them. Settlement intake for a trade
happens in the trade's own transaction, so where `SettlementPending` lands in the log depends on the commands alone,
not on a background tick.

## The clock API

`GET /api/clock` is always mounted: `{ "mode": "virtual"|"system", "running", "speed", "now" }`, for a consumer that
follows the exchange's simulated time. With `clock.operator_control: true` (decision D17), operators can also change
it without enabling test control, which would expose reset and fault injection:

| Method | Path | Body | Effect |
|---|---|---|---|
| POST | `/api/clock/speed` | `{ "speed": 1440 }` | Change the speed from the current reading on; out of range is `400 invalid_clock_speed` |
| POST | `/api/clock/freeze` / `/api/clock/resume` | – | Stop the clock, or let it run at its speed |

Setting and advancing the clock stay under `/test`. With the system clock every change is `409 clock_not_virtual`.

**`ClockChanged`** (`source: "exchange"`, aggregate ID `clock`) is published for every change that alters the clock,
from either API: `change` (`advance`, `set`, `freeze`, `resume`, `speed`), `mode`, `running`, `speed`, `now` (the
reading at the change) and `effectiveAt` (the real time of it). A follower computes simulated time as
`now + (real time − effectiveAt) × speed` while `running`, and `now` while frozen, without polling. Details:
- **No-ops publish nothing:** freezing a frozen clock, resuming a running one, the speed it already has.
- **`effectiveAt` is `null` while frozen**, when the follower does not need it, so real time stays out of the
  reproducible log of a run on a frozen clock.
- **What became due on the way comes first:** a clock move first issues everything due by the new time (settlement
  transitions, statements), then `ClockChanged`, so the log stays in time order.
- **A reset publishes nothing:** read `GET /api/clock` after one.

## Test control

With `test_control.enabled: true`, routes under `/test` control simulated time, show webhook deliveries and attempts,
inject delivery faults and run scenarios. They return `404` otherwise.

| Method | Path | Body | Effect |
|---|---|---|---|
| GET | `/test/clock` | – | `{ "mode": "virtual"\|"system", "running", "speed", "now" }` |
| POST | `/test/clock/advance` | `{ "days", "hours", "minutes", "seconds" }` (any, summed, > 0) | Move forward |
| POST | `/test/clock/set` | `{ "to": "2030-01-07T09:00:00Z" }` | Move to an instant; earlier than now is `409 clock_backwards` |
| POST | `/test/clock/freeze` / `/test/clock/resume` | – | Stop the clock, or let it run at its speed |
| POST | `/test/clock/speed` | `{ "speed": 1440 }` | Change the speed from the current reading on (a frozen clock stays frozen); out of range is `400 invalid_clock_speed` |
| GET | `/test/clearing/breaks?date=&participant=` | – | Statements with reconciliation breaks, issued or still waiting (late): `statementId`, `businessDate`, `participantId`, `issued`, `issuedAt` (or when it will be), `breaks` [{ `kind`, `executionId`, `canonical`, `reported` }] |
| POST | `/test/reset` | – | `204`: clears all state, as on a new database (see below) |
| GET | `/test/deliveries?subscriptionId=&status=` | – | Webhook deliveries in event order, optionally one subscription's or one status (`DEAD` lists the dead letters): `deliveryId`, `subscriptionId`, `url`, `eventSequence`, `eventId`, `eventType`, `status`, `attempts`, `lastError`, `nextAttemptAt`, `deliveredAt`. Across all subscriptions; the regular API has the per-subscription equivalents (Maintaining a subscription) |
| POST | `/test/webhooks/{id}/redeliver?after=` | – | Queues the subscription's events after sequence `after` (default 0: all) for delivery again: `{ "queued": n }`. Same as `POST /api/webhooks/{id}/redeliver` |
| POST | `/test/dead-letters/retry` | Optional `{ "subscriptionId", "deliveryIds": [...] }` | Retries dead letters (all, one subscription's, or the listed ones) with a fresh attempt budget: `{ "retried": n }` |
| PUT / DELETE | `/test/webhooks/{id}/faults` | A fault policy / – | Set or clear the subscription's delivery faults (see Fault injection); returns the subscription |
| GET | `/test/attempts?subscriptionId=` | – | Every request sent and every hold, oldest first: `deliveryId`, `subscriptionId`, `url`, `eventSequence`, `eventId`, `eventType`, `attempt`, `outcome` (`SENT`\|`HELD`), `faults`, `body` (only when a fault changed it), `httpStatus`, `error`, `at` |
| POST | `/test/scenarios` | A scenario, YAML or JSON | Runs it in a sandbox (see Scenarios); `200` with the report, `400 invalid_scenario` if it does not parse |

**Reset** empties every table and gives the in-memory state of a new database: no instruments, participants,
orders, trades, events, subscriptions, deliveries, attempts, settlements or clearing statements; clearing starts
again from the clock's new reading. Sequences restart at 1, so with
deterministic IDs the same commands after a reset give the same log and IDs as on a fresh service. The virtual clock
goes back to its first reading (the configured `clock.start`, or the time the service started), and to the configured
`running` and `speed`. Only the configuration
remains, and metrics counters are not reset. A delivery in flight during the reset may still reach its consumer, but
its result is dropped.

**Redelivery** re-queues events, with sequences after `after` and of the types the subscription wants, as fresh
deliveries: pending, no attempts so far, due now. Events from before the subscription existed get a delivery too, so
this also replays history to a new consumer. Deliveries already in flight are sent again.

The `/test/clock` routes change the clock only with `clock.mode: virtual`; with the system clock they return
`409 clock_not_virtual`.
- **What the virtual clock stamps:** domain time: order `createdAt`/`updatedAt`, `executedAt`, event `occurredAt`,
  settlement dates and clearing cutoffs.
- **It never moves backwards.** Its state is stored, so a restart resumes simulated time where it was: at the stored
  reading or the latest event the exchange or the simulator stamped, whichever is later. Running or frozen is kept;
  the speed is the configured one again (a speed changed through `/api/clock/speed` or `/test/clock/speed` lasts until
  a restart or reset).
- **Speed:** a running virtual clock moves `clock.speed` simulated seconds per real second. Everything on domain time
  follows: order and event timestamps, T+N business days and weekends. With `speed: 1440`, `running: true` and the
  T+1 default, a trade made at 09:00 settles at the next midnight, about 37 real seconds later, with no clock calls.
  The simulator fires due transitions every `settlement.tick_interval_ms` of real time, stamped with their scheduled
  instant, so a fast clock changes when they are seen, never what they say.
- **Webhook delivery stays on real time** (retry backoff, leases, timeouts, `delivered_at`), so jumping or speeding
  the clock neither stalls nor duplicates deliveries.

## Scenarios

A scenario is a YAML file: setup (instruments, participants, webhooks), then ordered steps: commands, clock moves and
expectations. It runs against a fresh in-process service of its own, with a temporary database, a frozen virtual
clock, deterministic IDs from its `seed` and test control on. The same file therefore gives the same events, IDs and
verdict every time. Examples are in [`scenarios/`](scenarios/).

```sh
cargo run -p exchange-simulator -- scenario run exchange-simulator/scenarios/*.yaml   # exit 0 pass, 1 fail, 2 bad file
cargo run -p exchange-simulator -- scenario run --json some.yaml                      # reports as a JSON array
curl -X POST --data-binary @some.yaml localhost:8080/test/scenarios                   # the same runner, over HTTP
```

The sandbox starts from the service's configuration (webhook timings, identifier policy). The scenario sets the
clock, the IDs and, under `settlement:`, any settlement policy fields; `simulationId` is the scenario's name. The
live service's own state is never touched.

```yaml
name: partial fill
seed: 7                          # deterministic IDs, and the settlement seed unless settlement.seed is set
start: 2030-01-07T09:00:00Z      # the virtual clock's first reading (this is the default)
settlement: { mode: both, fail_probability: 0 }
instruments:  [ { id: ACME, symbol: ACME, currency: DKK, currencyScale: 2 } ]   # request bodies, as for POST
participants: [ { id: A, name: Seller }, { id: B, name: Buyer } ]
webhooks:     [ { url: "http://consumer:8080/hook", eventTypes: [TradeExecuted],
                  faults: { duplicateProbability: 0.1 } } ]                         # optional, see Fault injection
steps:
  - order: { as: sell, participantId: A, side: SELL, type: LIMIT, priceTicks: 10025, quantity: 200 }
  - order: { as: buy, participantId: B, side: BUY, type: LIMIT, priceTicks: 10025, quantity: 150,
             expect: { status: 201, matches: { order: { status: FILLED } } } }
  - expect: { order: sell, matches: { status: PARTIALLY_FILLED, remainingQuantity: 50 } }
  - advance_clock: { days: 2 }
  - expect: { settlement: "buy#0", matches: { simulated: { status: SETTLED } } }
  - expect: { events: { eventType: SettlementCompleted }, count: 1 }
  - expect: { deliveries: { eventType: TradeExecuted, status: DELIVERED }, count: 1, wait_ms: 2000 }
```

| Step | Does |
|---|---|
| `order: { as?, expect?, ...body }` | `POST /api/orders`. `instrumentId` may be left out when there is one instrument |
| `cancel: { order, expect? }` | `DELETE /api/orders/{id}` |
| `advance_clock: { days, hours, minutes, seconds }` / `set_clock: { to }` | Move the virtual clock |
| `report_settlement: { execution, expect?, ...body }` | `POST /api/settlements/{executionId}/outcomes` |
| `set_instrument: { instrument, enabled, expect? }` | `PATCH /api/instruments/{id}` |
| `register: { instruments, participants, webhooks }` | Registers more, as in the setup (e.g. after a reset) |
| `reset: {}` | `POST /test/reset`; order and webhook references are forgotten too |
| `redeliver: { url, after, expect? }` | Redelivers the webhook's events after `after` |
| `retry_dead_letters: { url?, expect? }` | Retries the webhook's dead letters, or all |
| `set_faults: { url, faults }` | Set the fault policy of the webhook registered for `url`; `faults: null` clears it |
| `market: { ...random market }` | A seeded random market; see below |
| `expect: { order \| book \| settlement: ref, matches }` | `GET` the one resource; it must contain `matches` |
| `expect: { trades \| events \| deliveries \| attempts: filter, count? \| all? }` | The items containing `filter`: exactly `count`, at least one, or with `all: true` every item (and at least one) |

- **References:** `as: name` names an order. Later steps use `name` for its order ID and `name#i` for the ID of the
  `i`-th execution in its submit response. Literal UUIDs work too.
- **Command checks:** `expect: { status, matches }` checks the command's own response. Without it, any 2xx passes.
- **Matching is containment:** every field named must be present and equal; fields left out are ignored. Arrays must
  have the same length and match element by element.
- **Deliveries run on real time.** So `deliveries` and `attempts` expectations are retried until they hold or
  `wait_ms` has passed (default 5000). `{ deliveries: { status: DELIVERED }, all: true }` waits for the outbox to
  drain. Every other expectation is checked once, since the state depends only on the commands and the clock.
- **Reports:** every step runs, even after a failure, so one run shows every problem. Step 0 is the setup. Each
  report ends with a fingerprint of the final event log, its event count and SHA-256. Equal fingerprints mean
  byte-identical logs.

### Random markets

A `market` step generates orders and cancels from seeded distributions, then sends them in order. Before each
command, the virtual clock is set to that command's arrival time. The command stream depends only on the
configuration, so the same seed gives the same orders, trades, events and IDs. A different seed gives a different
market with the same statistics. [`scenarios/random-market.yaml`](scenarios/random-market.yaml) is an example.

```yaml
- market:
    seed: 12345                     # default: the scenario's seed
    orders: 600                     # commands in total, submits and cancels
    orders_per_second: 5            # Poisson arrivals in simulated time; 0 (default) puts them all at one instant
    participants: 8                 # P01..P08, registered if new; or a list of existing participant IDs
    buy_probability: 0.5            # default 0.5
    market_order_probability: 0.15  # default 0
    cancellation_probability: 0.1   # chance a command cancels an earlier limit order instead; default 0
    instruments:                    # must be registered in the setup
      - { id: ACME, weight: 2, price: { mid_ticks: 10000, width_ticks: 30, skew_ticks: 5 } }
      - { id: BETA, weight: 1, price: { mid_ticks: 2500, width_ticks: 8, distribution: normal },
          quantity: { min: 10, max: 500, lot: 10 } }
```

- **Instruments:** each gets a share of the arrivals in proportion to its `weight`.
- **Limit prices:** buys centre on `mid_ticks - skew_ticks` and sells on `mid_ticks + skew_ticks`. A positive skew
  builds resting liquidity; a negative one makes orders cross.
- **Price spread:** `width_ticks` is the half-width of a `uniform` distribution (the default), or the standard
  deviation of a `normal` one.
- **Quantities:** uniform whole lots between `min` and `max` (default 1–100, lot 1).
- **What counts as normal:** exchange rejections (self-trades, for example) and cancels of orders that have filled
  meanwhile are part of a market. The step summarizes them: orders, rejections, executions, cancels and refused
  cancels, and the final clock reading. Any other refusal is a failure.

The benchmark's workload is the same generator (see `bench/README.md`).

## Fault injection

With test control on, a subscription can be given a fault policy. It damages, duplicates, delays or reorders the
copies sent to that subscription. The canonical events in `/api/events`, and every other subscription's copies, are
never changed.

```sh
curl -X PUT localhost:8080/test/webhooks/$SUBSCRIPTION/faults -H 'content-type: application/json' -d '{
  "seed": 7,
  "missing":    { "probability": 0.15 },
  "nulls":      { "paths": ["payload.price"] },
  "invalid":    { "paths": ["payload.quantity", "payload.executedAt"], "probability": 0.5 },
  "duplicateProbability": 0.02,
  "delay":      { "probability": 0.1, "ms": 500 },
  "outOfOrder": { "probability": 0.05, "holdMs": 1000 }
}'
```

| Fault | Effect on the copy sent |
|---|---|
| `missing` | Removes the listed `paths`, or one random field without `paths` |
| `nulls` | Sets them to `null` |
| `invalid` | Replaces them with a value of the wrong type: a string becomes `-12345`; a number, boolean, null, array or object becomes a string |
| `duplicateProbability` | Sends the same bytes twice, as the same attempt |
| `delay` | Waits `ms` (at most 10 000) before sending |
| `outOfOrder` | Holds the delivery back for `holdMs` without sending it or using an attempt; later events overtake it. A delivery is held at most once. Not allowed on an `ordered` subscription |

- **Paths** are dot-separated, with array indexes as numbers: `payload.price`, `payload.instrument.isin`. A path
  that does not exist in an event is skipped.
- **Probabilities** default to 1 for field faults, delays and holds, and to 0 for duplicates. Each fault is decided
  per attempt, so a retry can come out differently.
- **Reproducible:** each decision is drawn from a generator seeded by (`seed`, delivery ID, attempt). With
  `ids.mode: deterministic` (always so in scenarios), the same deliveries get the same faults on every run, restarts
  included.
- **Signatures and headers:** a signed subscription's signature covers the damaged bytes, as sent. Headers
  (`X-Event-Id`, `X-Event-Sequence`, …) stay canonical.
- **What was sent** is in `/test/attempts`: one row per request (a duplicate adds one) and one per hold, with the
  faults applied and, when they changed it, the exact body.
- **Delays and holds interact:** a delay holds up the worker's whole batch, the recording of a hold included, and a
  hold counts from when it was decided. So for reordering, make `holdMs` longer than any `delay.ms` on the same
  service.
- **Scope:** policies are stored with the subscription and survive restarts. Clear one with `DELETE`. Every attempt
  is recorded, faults or not; the body only when a fault changed it.

## Testing

```sh
cargo test -p exchange-simulator                          # unit, integration and HTTP tests; tests/mockserver.rs needs Docker
cargo test -p exchange-simulator --features e2e --test e2e   # the acceptance test in containers (builds the image)
```

`tests/openapi.rs` checks [`openapi.yaml`](openapi.yaml) against the service:
- the documented routes are exactly the mounted ones;
- every operation is exercised, and each request and response is validated against the document's schemas (strict:
  an undocumented field or status fails);
- every event type is emitted and validated against its schema in the `webhooks` section;
- the parts shared with the payment gateway's specification (webhooks, clock, reset) are identical.

`tests/e2e.rs` runs the acceptance tests against the real image, with MockServer on one Docker network:
- **Milestone 1:** registration, the 200/150 trade, the webhook with a stable event ID, a `docker restart` that
  restores the book without repeating events, and a consumer outage whose retries create no trades.
- **Milestone 2:** combined settlement mode on a virtual clock. `SettlementPending` reaches MockServer, the trade
  survives a `docker restart` mid-lifecycle, a clock advance to T+1 settles it exactly once, and an external report
  is published apart from the simulator's events.
- **Milestone 3:** the scenario suite through the image's CLI (twice, without network) and through
  `POST /test/scenarios` on a running container. Every report, event-log fingerprint included, equals the local
  build's.

`tests/milestone3.rs` holds the Milestone 3 acceptance in process:
- **The scenario suite:** run twice as separate processes, it gives identical reports.
- **Faults:** every fault type is visible at a real receiver, while `/api/events` stays byte-identical to a run
  without subscriptions.
- **Reset:** it leaves a clean state.

## Benchmarks

[`bench/README.md`](bench/README.md) covers how to run the harness and the baseline results.
