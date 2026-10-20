# Running with your app

How to run the exchange simulator and the payment gateway next to an app that lives in another project, from images
built on the same machine. The runnable version is [`examples/with-your-app`](../examples/with-your-app).

## 1. Build the images

Both images build from the root of this repository (they share `Cargo.lock` and the `simulator-kit` crate):

```sh
scripts/build-images.sh              # exchange-simulator:dev and payment-gateway:dev
TAG=demo scripts/build-images.sh     # other tags
scripts/build-images.sh exchange     # one image: exchange or gateway
```

Run it again after pulling changes; the cargo caches in the Dockerfiles keep rebuilds incremental. The first build
is a full Rust release build of each service and takes several minutes.

Your project refers to the images by tag and must never try to pull them, since they exist only on this machine:

```yaml
    image: exchange-simulator:dev
    pull_policy: never
```

Without the image, `docker compose up` then fails with a clear "no such image" instead of a failed pull from Docker
Hub.

## 2. Try the example

```sh
cd exchange-simulator/examples/with-your-app
docker compose up -d --wait
curl -s localhost:8080/api/marketdata/AAPL           # a synthetic price, quoted by the liquidity provider
curl -s -X POST localhost:8080/api/participants -H 'content-type: application/json' \
  -d '{"id": "demo", "name": "Demo"}'
curl -s -X POST localhost:8080/api/orders -H 'content-type: application/json' \
  -d '{"instrumentId": "AAPL", "participantId": "demo", "side": "BUY", "type": "MARKET", "quantity": 10}'
curl -s localhost:9000/received | head -c 600       # the webhooks the stand-in app received
docker compose down                                  # add -v to drop the databases too
```

It runs three services on one Compose network:

| Service | Image | Reached by your app at | On the host |
|---|---|---|---|
| `exchange` | `exchange-simulator:dev` | `http://exchange:8080` | `EXCHANGE_PORT`, default 8080 |
| `gateway` | `payment-gateway:dev` | `http://gateway:8081` | `GATEWAY_PORT`, default 8081 |
| `app` | the sample receiver, standing in for your app | – | `APP_PORT`, default 9000 |

## 3. Move it into your project

Copy `compose.yaml`, `exchange.yaml`, `gateway.yaml` and `secrets/` into your project (or merge the `exchange` and
`gateway` services into your existing `compose.yaml`), then:

1. **Replace the `app` service with yours.** Keep the name `app`, or change it in the webhook URLs of
   `exchange.yaml` and `gateway.yaml` (`http://app:9000/hooks/...`) to your service's name, port and paths.
2. **Give your app the services' URLs:** `http://exchange:8080` and `http://gateway:8081` inside the network. From a
   browser or the host, use `localhost` and the host ports.
3. **Share the webhook secret.** Both services sign every delivery with the secret in `secrets/webhook-secret.txt`;
   mount the same secret into your app to verify them ([Writing a webhook receiver](webhook-receivers.md)). Replace
   the example's value with a long random one.
4. **Choose your market** in `exchange.yaml`: the example lists three instruments on synthetic prices, with the
   liquidity provider quoting each. For live prices take the instruments from
   [`config/finnhub-market.yaml`](../config/finnhub-market.yaml), set `price_sources.default.kind: finnhub`, and start
   with `FINNHUB_API_KEY=<key> docker compose up` ([Configuration](configuration.md#live-prices-from-finnhub)).
5. **Choose the events** your app subscribes to in each `webhooks.subscriptions` list. The exchange's are listed
   under [`GET /api/events`](../README.md#get-apieventsafterlimiteventtype) in its README, the gateway's under
   [Events](../../payment-gateway/README.md#events) in its own.

## Keeping the services in their own Compose project

To start and stop the simulators separately from your app, keep this directory as its own project and join the
two over a shared network instead of merging the files:

```sh
docker network create sim-net
```

```yaml
# in both compose files
networks:
  default:
    name: sim-net
    external: true
```

Service names resolve across projects on the shared network, so the URLs above stay the same. Drop the `app`
service from this project, and start this project first.

## Clocks

The exchange runs a virtual clock in real time with `operator_control` on, so your app can speed it up, freeze and
resume it through `POST /api/clock/speed`, `/freeze` and `/resume`. The gateway follows the exchange's clock
(`clock.mode: follow`, polled every 250 ms), which is why it waits for the exchange to be healthy. A follower never
runs backwards: after a freeze or a slow-down it can be up to one poll interval ahead, and holds there until the
exchange's clock catches up. Settlement, clearing statements, bank
statements and business-day cutoffs all run on that one clock.

## Ports

`EXCHANGE_PORT`, `GATEWAY_PORT` and `APP_PORT` move the host ports. To move a port inside the network as well, set
`EXCHANGE__SERVER__BIND=0.0.0.0:<port>` (or `GATEWAY__SERVER__BIND`) on the service, change its mapping and every URL
that uses it; the images' health checks follow the bind variable.
