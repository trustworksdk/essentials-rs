# payment-gateway

A payment gateway simulator for the trading demo (plan item 5.4b in `exchange-simulator/IMPLEMENTATION_PLAN.md`).
It moves money in (deposits) and out (withdrawals) for the demo's accounts, with seeded outcomes and delays on a
controllable clock, signed webhooks, and the bank's end-of-day statements to reconcile against. Like the exchange
simulator it plays an outside party only (D8): client balances and buying power belong to the demo's own domain.
The clock, the event outbox, webhook delivery and maintenance, fault injection and persistence modes come from
[`simulator-kit`](../simulator-kit/README.md), so they behave exactly as in the exchange simulator.

## Running

```sh
cargo run -p payment-gateway          # from the workspace root; listens on 127.0.0.1:8081
cargo test -p payment-gateway -j 2
```

Settings come from `config/default.yaml`, then a YAML file named by `GATEWAY_CONFIG`, then `GATEWAY__*` environment
variables (`__` between levels, e.g. `GATEWAY__SERVER__BIND=0.0.0.0:8081`).

| Key | Default | Meaning |
|---|---|---|
| `payments.seed` | `1` | Seeds every outcome, delay and return |
| `payments.currencies` | DKK, EUR, USD, SEK, NOK, GBP, CHF (2 decimals), JPY (0) | Accepted currencies and their decimals |
| `payments.deposit` / `withdrawal` | fail 0.02, 60–1800 s / fail 0.02, 600–14400 s | `fail_probability`, `min_delay_seconds`, `max_delay_seconds` (domain time) |
| `payments.return_probability`, `max_return_days` | `0.01`, `3` | A completed payment is returned 1..=`max_return_days` business days later |
| `statement.enabled`, `cutoff`, `time_zone` | `true`, `"17:30"`, `"UTC"` | End-of-day statements at the cutoff, local time in an IANA zone |
| `statement.account_iban` | `DK5000400440116243` | The firm's account at the bank, on every statement |
| `statement.opening_balances` | `{}` | Balance before the first booking per currency, e.g. `{ DKK: "250000.00" }` |
| `statement.breaks.*` | all 0 | Seeded reconciliation breaks (see Statements) |
| `statement.tick_interval_ms` | `250` | How often due transitions and statements are looked for |
| `webhooks.*` | as in the exchange simulator | Delivery timing and declared subscriptions |
| `ids.mode` / `ids.seed` | `random` / `0` | `deterministic` makes every ID reproducible |
| `clock.mode`, `start`, `running`, `speed` | `system` | As in the exchange simulator; `speed: 1440` makes a day pass in a minute |
| `test_control.enabled` | `false` | Mounts `/test`; never in a real deployment |
| `database.mode`, `path` | `durable`, `data/payment-gateway.db` | `durable`, `fast` or `memory`, as in the exchange simulator |

## Payments

### `POST /api/payments`

```json
{
  "idempotencyKey": "dep-2030-0001",
  "type": "DEPOSIT",
  "accountId": "client-42",
  "bankAccount": { "iban": "DE89 3704 0044 0532 0130 00", "holderName": "A. Customer" },
  "amount": "1500.00",
  "currency": "DKK",
  "reference": "top-up"
}
```

- `201` with the payment, `PENDING`. A repeated `idempotencyKey` with the same details returns the same payment with
  `200` and `"duplicate": true`; with different details `409 idempotency_conflict`.
- `type` is `DEPOSIT` (money in from the customer's bank account) or `WITHDRAWAL` (money out to it).
- `accountId` is the demo's own account, 1–64 characters. The gateway never interprets it (D13).
- The IBAN's check digits are validated; it is stored in compact form. `holderName` is 1–140 characters.
- `amount` is a positive decimal string with at most the currency's decimals. `reference` is optional, at most 140
  characters.
- Invalid requests get `400 invalid_request`. There is no balance check: whether a client may withdraw is the demo's
  decision.

A payment carries `paymentId`, `sequence`, `idempotencyKey`, `type`, `accountId`, `bankAccount`, `amount`,
`amountMinor`, `currency`, `currencyScale`, `reference`, `status`, `reason`, `createdAt`, `completedAt`,
`failedAt`, `returnedAt` and `updatedAt`.

### `GET /api/payments/{id}` and `GET /api/payments?accountId=&status=&after=&limit=`

One payment, or payments in sequence order. `status` is `PENDING`, `COMPLETED`, `FAILED` or `RETURNED`; `after` pages
by sequence; `limit` is 1–1000, default 100.

### Lifecycle

- **Pending:** from initiation, for a seeded delay between the type's `min_delay_seconds` and `max_delay_seconds` of
  domain time.
- **Then** `COMPLETED`, which books the amount on the firm's account (a credit for a deposit, a debit for a
  withdrawal), or `FAILED` with `fail_probability`, with a `reason`: `ACCOUNT_CLOSED`, `INSUFFICIENT_FUNDS` (deposits)
  or `INVALID_ACCOUNT` (withdrawals), or `REFUSED_BY_BANK`.
- **Returned:** with `return_probability`, a completed payment is reversed 1..=`max_return_days` business days later
  at the same time of day, `reason: RETURNED_BY_BANK`, and the reversal is booked.
- Outcomes are a function of the seed and the payment's sequence, so they do not depend on tick timing or restarts.
  Events carry the scheduled instant as `occurredAt`.

## Events

Every event is in the canonical envelope the exchange simulator uses (`eventId`, `sequence`, `eventType`,
`schemaVersion`, `source`, `simulationId`, `aggregateId`, `correlationId`, `occurredAt`, `payload`), replayable from
`GET /api/events?after=&limit=&eventType=` and delivered to webhooks.

| Event | Source | Aggregate / correlation ID | Payload |
|---|---|---|---|
| `PaymentPending` | `payment-gateway` | payment ID / `accountId` | the payment |
| `PaymentCompleted` | `payment-gateway` | payment ID / `accountId` | the payment |
| `PaymentFailed` | `payment-gateway` | payment ID / `accountId` | the payment, with `reason` |
| `PaymentReturned` | `payment-gateway` | payment ID / `accountId` | the payment, with `reason` |
| `BankStatementIssued` | `bank` | statement ID / currency | the statement |

## Statements

At `statement.cutoff` on each business day the bank issues one statement per currency the firm's account has
booked anything in (D14). ID `BS:<businessDate>:<currency>`:

- `businessDate`, `accountIban`, `currency`, `currencyScale`, `timeZone`, `entriesFrom` and `cutoffAt` (the window of
  booking times, start included), `issuedAt`;
- `openingBalance(Minor)`: the configured opening balance plus every booking before the window; `totalCreditsMinor`,
  `totalDebitsMinor`, `closingBalance(Minor)`;
- `entries`: `entryId` (`<statementId>:<n>`), `paymentId`, `endToEndId` (the idempotency key), `accountId`,
  `counterparty`, `kind` (`DEPOSIT`, `WITHDRAWAL`, `DEPOSIT_RETURN`, `WITHDRAWAL_RETURN`), `direction` (`CREDIT` or
  `DEBIT`), `amountMinor` (signed), `amount`, `bookedAt`, `valueDate`.

Before each cutoff the payment transitions due by it run first, so a statement holds every booking made before its
cutoff, and a clock move issues every business day in time order with the payments around it.
`GET /api/statements?date=&currency=` lists issued statements; `GET /api/statements/{statementId}` returns one
(`404` until issued).

**Breaks** (D16) are opt-in under `statement.breaks`, seeded like the exchange's clearing breaks:
`omit_probability` (an entry left out, `OMITTED_ENTRY`), `alter_probability` (an amount off by one minor unit,
`AMOUNT_CHANGED`), `phantom_probability` (an entry for a payment that does not exist, `PHANTOM_ENTRY`),
`late_probability` (issued 1..=`max_late_minutes` minutes after the cutoff, `LATE_STATEMENT`). A broken statement is
consistent with itself, closing equals opening plus its entries, but the next day opens at the true balance.
`GET /test/statements/breaks?date=&currency=` lists each break with the canonical and the reported value.

## Webhooks

`/api/webhooks` and its maintenance operations are the exchange simulator's, unchanged: registration, signing with
`X-Signature`, ordered delivery, pausing, secret rotation, deliveries, dead letters, redelivery, and subscriptions
declared under `webhooks.subscriptions`. See the exchange simulator's README and its webhook receiver guide; the
verification snippets there work for both services.

## Test control

With `test_control.enabled: true`: `GET /test/clock`, `POST /test/clock/advance`, `/set`, `/freeze`, `/resume` and
`/speed` (as in the exchange simulator); `POST /test/reset`; `GET /test/statements/breaks`; and the shared webhook
routes (`/test/deliveries`, `/test/attempts`, `/test/dead-letters/retry`, `/test/webhooks/{id}/redeliver`,
`/test/webhooks/{id}/faults`).

## Running it with the exchange

`payment-gateway/Dockerfile` builds the image from the workspace root (`docker build -f payment-gateway/Dockerfile .`).
[`exchange-simulator/examples/back-office`](../exchange-simulator/examples/back-office) runs it with the exchange
simulator on an accelerated clock, and the
[back-office chapter](../exchange-simulator/docs/back-office.md) of the exchange simulator's guide describes how a
demo consumes and reconciles both.

There are no gateway scenarios (D15): the back-office acceptance in the exchange simulator's e2e suite covers the two
services together.
