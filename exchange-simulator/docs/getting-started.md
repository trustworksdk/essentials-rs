# Getting started

## The Compose example

[`examples/compose`](../examples/compose) runs the simulator next to a sample webhook receiver. Two subscriptions
are declared in [`exchange.yaml`](../examples/compose/exchange.yaml): `settlement` gets trades, one at a time in
order, and `orders` gets the order lifecycle. Both are signed with a secret both containers read from
[`secrets/webhook-secret.txt`](../examples/compose/secrets/webhook-secret.txt), a Docker secret.

You need Docker with Compose v2. From `exchange-simulator/examples/compose`:

```sh
docker compose up --build -d --wait     # the first build takes a few minutes; --wait returns once both are healthy
docker compose logs -f receiver         # in a second terminal: one line per delivery
```

`EXCHANGE_PORT` and `RECEIVER_PORT` change the host ports (default 8080 and 9000).

Register an instrument and two participants, then trade:

```sh
curl -s localhost:8080/api/instruments -H 'content-type: application/json' \
  -d '{"id":"ACME","symbol":"ACME","mic":"XCSE","currency":"DKK","currencyScale":2}'
for p in A B; do
  curl -s localhost:8080/api/participants -H 'content-type: application/json' -d "{\"id\":\"$p\",\"name\":\"$p\"}"
done
curl -s localhost:8080/api/orders -H 'content-type: application/json' \
  -d '{"instrumentId":"ACME","participantId":"A","side":"SELL","type":"LIMIT","priceTicks":10025,"quantity":100}'
curl -s localhost:8080/api/orders -H 'content-type: application/json' \
  -d '{"instrumentId":"ACME","participantId":"B","side":"BUY","type":"LIMIT","priceTicks":10025,"quantity":40}'
```

The receiver logs each event with its sequence and attempt, and the status it answered:

```text
receiver-1  | orders: #1 OrderAccepted 01a1…c5 attempt 1 -> 204
receiver-1  | orders: #2 OrderAccepted 01a1…f8 attempt 1 -> 204
receiver-1  | settlement: #3 TradeExecuted 01a1…79 attempt 1 -> 204
receiver-1  | orders: #4 OrderPartiallyFilled 01a1…b9 attempt 1 -> 204
receiver-1  | orders: #5 OrderFilled 01a1…9c attempt 1 -> 204
```

`curl -s localhost:9000/received` shows the same as JSON, with `"verified": true` for each signature it checked.

### Watch a retry

Make the receiver fail, place an order, and look at the subscription:

```sh
curl -s -XPOST 'localhost:9000/control?status=503'
curl -s localhost:8080/api/orders -H 'content-type: application/json' \
  -d '{"instrumentId":"ACME","participantId":"A","side":"SELL","type":"LIMIT","priceTicks":10050,"quantity":5}'
ID=$(curl -s localhost:8080/api/webhooks | python3 -c 'import json,sys; print([s["id"] for s in json.load(sys.stdin) if s["name"]=="orders"][0])')
curl -s localhost:8080/api/webhooks/$ID                              # "deliveries": {"pending": 1, …, "oldestUnfinished": {…}}
curl -s "localhost:8080/api/webhooks/$ID/deliveries?status=PENDING"  # attempts, lastError "HTTP 503 …", nextAttemptAt
curl -s -XPOST 'localhost:9000/control?status=204'                   # back to normal: the next attempt is delivered
```

Attempts back off exponentially (0.5 s, 1 s, 2 s, … with jitter). With the defaults a delivery is dead-lettered
after 10 failed attempts, about 2–4 minutes; [Operations](operations.md#dead-letters) shows how to retry dead
letters.

### Stop and clean up

```sh
docker compose down        # keeps the database volume: the next `up` continues where this one stopped
docker compose down -v     # also deletes it
```

## Adding the simulator to your own Compose project

Build the image once from the workspace root (or let Compose build it, as the example does):

```sh
docker build -f exchange-simulator/Dockerfile -t exchange-simulator:dev .
```

Then add a service. Put your consumers on the same Compose network and address them by service name:

```yaml
services:
  exchange:
    image: exchange-simulator:dev
    ports: ["8080:8080"]
    environment:
      EXCHANGE_CONFIG: /etc/exchange/exchange.yaml
    volumes:
      - ./exchange.yaml:/etc/exchange/exchange.yaml:ro
      - exchange-data:/var/lib/exchange      # durable state; leave it out only for throwaway runs
    secrets: [settlement_webhook_secret]

  settlement-service:                          # your consumer
    image: my-org/settlement-service:latest
    secrets: [settlement_webhook_secret]

secrets:
  settlement_webhook_secret:
    file: ./secrets/settlement-webhook-secret.txt

volumes:
  exchange-data:
```

```yaml
# exchange.yaml
webhooks:
  subscriptions:
    - name: settlement
      url: http://settlement-service:8080/webhooks/trades
      event_types: [TradeExecuted]
      secret_file: /run/secrets/settlement_webhook_secret
      ordered: true
```

**Where your consumer runs decides the URL:**
- **On the same Compose network:** use the service name, as above.
- **In another Compose project:** create a network once with `docker network create exchange-net`, declare it
  `external: true` in both projects, and attach both services to it.
- **On the host, outside Docker:** use `http://host.docker.internal:<port>/…`. On Linux, add
  `extra_hosts: ["host.docker.internal:host-gateway"]` to the `exchange` service.

**What to read next:**
- To write the consumer, see [Writing a webhook receiver](webhook-receivers.md).
- To change, pause or inspect subscriptions later, see [Operations](operations.md).
- For durability and settings, see [Configuration](configuration.md).
