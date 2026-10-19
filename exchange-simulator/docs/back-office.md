# Back office: settlement, statements and payments

The exchange simulator and the payment gateway play the outside parties a broker's back office deals with: the
exchange, the clearinghouse, and the bank or payment provider. Client positions, cash balances, buying power and the
reconciliation of the firm's books are not in either simulator. They belong to your own domain (decision D8); for the
trading demo, that is its event-sourced account model. This chapter is about building that side: which events to
consume, what to do with each, how to reconcile, and what each reconciliation break looks like.

The [`examples/back-office`](../examples/back-office) Compose project runs both services on an accelerated clock
with a receiver standing in for your app. The containerized test suite plays the demo app against it and reconciles
everything (`the_back_office_demo_reconciles_on_an_accelerated_clock` in `tests/e2e.rs`), so what this page
describes is what the services do.

## The parties and their events

| Party | Service | Events | What your domain does with them |
|---|---|---|---|
| Exchange | exchange simulator | `TradeExecuted` | Books the trade on the client's account: the position, and the cash it will cost or bring at settlement |
| Settlement | exchange simulator (`settlement.mode: simulated`) | `SettlementPending`, `SettlementCompleted`, `SettlementFailed`, `SettlementDelayed`, `SettlementRejected`, `SettlementCancelled`, `SettlementCorrected` | Moves the trade from pending to settled (or failed); a settled trade's cash and securities are final |
| Clearinghouse | exchange simulator (`clearing.enabled`) | `ClearingStatementIssued` | Reconciles the day's trades and net obligations per participant |
| Payment provider | payment gateway | `PaymentPending`, `PaymentCompleted`, `PaymentFailed`, `PaymentReturned` | Credits a deposit once it completes, debits a withdrawal, reverses a return |
| Bank | payment gateway | `BankStatementIssued` | Reconciles the firm's client-money account: every movement and the balances |

Consume them as described in [Writing a webhook receiver](webhook-receivers.md): verify every signature, deduplicate
by `eventId`, and use ordered subscriptions where order matters (the example declares both subscriptions ordered).

## Dates come from the events

Both services run on their own domain clock, which in a demo runs fast (`clock.speed: 1440` is a day per minute).
Take every date and time from the events: `executedAt`, `tradeDate`, `intendedSettlementDate`, `occurredAt`,
`completedAt`, `businessDate`. Never use your own clock to decide what day a trade or payment belongs to, or whether
a statement is due. The end of a business day is `BusinessDayClosed` from the exchange: start end-of-day work on it,
before the day's clearing statements arrive. It is published on days without trades too.

The two services keep separate clocks. The example starts both frozen at the same instant and resumes them together
(`POST /test/clock/resume` on each), which keeps them within a fraction of a simulated minute of each other. A
restart of one service continues its clock from where it was, so they may drift apart by the restart's duration.

## Trading and settlement

- **`TradeExecuted`** carries both parties, quantity, price (`priceTicks` and `price`), currency and `executedAt`.
  A buy commits cash and adds a position; a sell the reverse. Until settlement these are pending.
- **The settlement cycle is T+1 by default**, per market where it differs (`settlement.market_settlement_days`).
  With clearing enabled, a trade after the cutoff or on a weekend is dated the next business day, so it settles a
  business day after that.
- **`SettlementPending`** comes in the same transaction as the trade, with the `intendedSettlementDate`.
  `SettlementCompleted` arrives at 00:00 UTC on that date; `SettlementFailed`, `SettlementDelayed` (with a new date)
  and the others follow their probabilities (all 0 in the example).
- **Every settlement event names the clearing statement and obligation it settles**, for both sides:
  `clearing.buyer.statementId`, `clearing.buyer.obligationId`, and the same for `seller`. Use them to tie a settled
  trade to the obligation the clearinghouse reported.

## Reconciling clearing statements

At the cutoff (`clearing.cutoff` in `clearing.time_zone`, business days), each participant that traded gets a
`ClearingStatementIssued`: its trades in the window `[tradesFrom, cutoffAt)`, from its side, and its net
obligations per instrument and settlement date. The full format is in the
[README's Clearing section](../README.md#clearing).

To reconcile a statement:
1. Take the trades you booked for that participant with `executedAt` in `[tradesFrom, cutoffAt)`.
2. Every one of them should be a line, with the same `executionId`, `quantity` and `priceTicks`.
3. Every line should be a trade you booked.
4. Summing your trades per instrument and settlement date should give the statement's `obligations`:
   `securitiesNet` (bought minus sold) and `cashNetMinor` (received minus paid).
5. `issuedAt` should equal `cutoffAt`.

Across all participants of a day, obligations net to zero.

**Which participants?** Statements are per participant (D9). If your accounts map one-to-one to participants, a
statement is one client's. If the firm is one clearing member for many clients, its statement nets all of them, and
reconciling means summing your client bookings first. Do not assume a participant ID is an account ID either way.

## Payments

- **`POST /api/payments`** with your own `accountId`, the client's bank account (`iban`, `holderName`), the amount and
  currency, and an `idempotencyKey`. Repeating a request with the same key and details is safe: you get the same
  payment back with `duplicate: true`. Use a key you can regenerate, such as your own deposit or withdrawal ID.
- **A deposit is not money yet.** `PaymentPending` says it was initiated. Credit the client's cash only on
  `PaymentCompleted`; on `PaymentFailed` (with a `reason`) there is nothing to credit.
- **A withdrawal is checked by you, not the gateway.** The gateway has no balances (D8), so decide on buying power
  before you call it. Reserve the amount when you initiate, release it on `PaymentFailed`, and settle it on
  `PaymentCompleted`.
- **`PaymentReturned`** can arrive business days after completion: the bank reversed the payment. Reverse what
  `PaymentCompleted` booked.

## Reconciling bank statements

At the cutoff (`statement.cutoff` in `statement.time_zone`, business days) the bank issues one `BankStatementIssued`
per currency the firm's account has booked anything in. It lists every movement booked in `[entriesFrom, cutoffAt)`,
with the opening and closing balance. The full format is in the
[gateway README](../../payment-gateway/README.md#statements).

To reconcile a statement:
1. Take the payments you saw complete or return with that time in `[entriesFrom, cutoffAt)` and that currency.
   Expect a credit for a completed deposit and a debit for a completed withdrawal; returns are the reverse.
2. Every one should be an entry with the same `paymentId` (or `endToEndId`, your idempotency key) and `amountMinor`.
3. Every entry should be a payment you know.
4. `openingBalance` should be the previous statement's `closingBalance`, and `closingBalance` the opening plus the
   entries.
5. `issuedAt` should equal `cutoffAt`.

The firm's account is not the clients' cash: it is where the clients' money is held. Your client balances should add
up to it, minus what is in flight.

## Breaks: what can go wrong, and how it shows

Both statements can be made wrong on purpose, seeded and reproducible, to exercise reconciliation:
`clearing.breaks` in the exchange simulator and `statement.breaks` in the gateway. The example's
`compose.breaks.yaml` turns them on. The trades, payments and event log never change; only what a statement says.

| Break | Clearing statement | Bank statement | How a reconciliation sees it |
|---|---|---|---|
| Left out | A trade is not listed (`OMITTED_TRADE`) | A movement is not listed (`OMITTED_ENTRY`) | A trade or payment you booked has no line; the obligations or the balance differ by it |
| Changed | A quantity or price is off by one (`QUANTITY_CHANGED`, `PRICE_CHANGED`) | An amount is off by one minor unit (`AMOUNT_CHANGED`) | The line exists but does not match your booking |
| Phantom | A copy of a trade under an execution ID that does not exist (`PHANTOM_TRADE`) | An entry for a payment that does not exist (`PHANTOM_ENTRY`) | A line you cannot match to anything |
| Late | Issued 1..`max_late_minutes` after the cutoff (`LATE_STATEMENT`) | The same | `issuedAt` is after `cutoffAt`; until then there is no statement for the day |

A broken statement is consistent with itself: its obligations net its own lines, and a bank statement's closing
balance is its opening plus its entries. The break shows only against your books. For a bank statement it also shows
the next day, which opens at the true balance, not at the broken closing.

`GET /test/clearing/breaks` and `GET /test/statements/breaks` list every break with the canonical and the reported
value, so a test can check that the reconciliation found exactly those. The e2e test does this: in the breaks run,
the reconciliation's findings must equal the listed breaks, one for one.

## Running the example

```sh
cd exchange-simulator/examples/back-office
docker compose up --build -d --wait
curl -X POST localhost:8080/test/clock/resume; curl -X POST localhost:8081/test/clock/resume
```

Then register an instrument and participants, make deposits, trade, and watch the receiver. The
[example's README](../examples/back-office/README.md) has a walkthrough with `curl`. Add `-f compose.yaml -f
compose.breaks.yaml` to every `docker compose` command for the run with breaks, and `docker compose down -v` to start
again.
