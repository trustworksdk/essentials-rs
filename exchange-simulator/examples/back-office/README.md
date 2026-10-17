# Back-office example

The exchange simulator (with settlement and clearing) and the payment gateway on an accelerated clock, with the
sample receiver from [`../compose`](../compose) standing in for the demo app. The how-to is in
[Back office](../../docs/back-office.md).

| File | |
|---|---|
| `compose.yaml` | `exchange`, `gateway` and `receiver`, built from the workspace; a shared secret; a volume each |
| `exchange.yaml` | Virtual clock (frozen at Monday 2030-01-07 08:00 UTC, speed 1440), simulated T+1 settlement, clearing statements at 17:30 Copenhagen time, test control, one ordered subscription `back-office` |
| `gateway.yaml` | The same clock, payments that always complete, bank statements at 17:30 Copenhagen time, a DKK opening balance, one ordered subscription `payments` |
| `compose.breaks.yaml` | Overlay that turns on reconciliation breaks in both services |

Test control is enabled because the clocks are resumed through it and the breaks are listed under it: this is a demo
environment. The receiver verifies signatures with `../compose/secrets/webhook-secret.txt`, a demo value.

## Walkthrough

```sh
docker compose up --build -d --wait
E=localhost:8080; G=localhost:8081; H='content-type: application/json'

# While the clocks are frozen: an instrument and two participants.
curl -s -XPOST $E/api/instruments -H "$H" -d '{"id":"ACME","symbol":"ACME","mic":"XCSE","currency":"DKK","currencyScale":2}'
curl -s -XPOST $E/api/participants -H "$H" -d '{"id":"client-a","name":"Client A"}'
curl -s -XPOST $E/api/participants -H "$H" -d '{"id":"client-b","name":"Client B"}'

# Start both clocks together: a simulated day now takes a real minute.
curl -s -XPOST $E/test/clock/resume; curl -s -XPOST $G/test/clock/resume

# A deposit, then a trade.
curl -s -XPOST $G/api/payments -H "$H" -d '{"idempotencyKey":"dep-1","type":"DEPOSIT","accountId":"client-a",
  "bankAccount":{"iban":"DE89370400440532013000","holderName":"Client A"},"amount":"10000.00","currency":"DKK"}'
curl -s -XPOST $E/api/orders -H "$H" -d '{"instrumentId":"ACME","participantId":"client-b","side":"SELL","type":"LIMIT","priceTicks":10000,"quantity":50}'
curl -s -XPOST $E/api/orders -H "$H" -d '{"instrumentId":"ACME","participantId":"client-a","side":"BUY","type":"LIMIT","priceTicks":10000,"quantity":50}'

docker compose logs -f receiver
```

Within about a real minute the receiver logs, in simulated time: `PaymentPending` and `PaymentCompleted` (within
half an hour), `TradeExecuted` and `SettlementPending`, at 17:30 Copenhagen time (16:30 UTC) the
`ClearingStatementIssued` for each client and the `BankStatementIssued` for DKK, and at 00:00 UTC the
`SettlementCompleted`. `curl localhost:9000/received` lists every event in full.

For the run with breaks, start over with the overlay:

```sh
docker compose down -v
docker compose -f compose.yaml -f compose.breaks.yaml up --build -d --wait
# ... the same steps ...
curl -s $E/test/clearing/breaks; curl -s $G/test/statements/breaks
```

`SPEED` changes the clock speed for both services, e.g. `SPEED=4320 docker compose up -d --wait` (the e2e test
uses it). `EXCHANGE_PORT`, `GATEWAY_PORT` and `RECEIVER_PORT` change the host ports.
