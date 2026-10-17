# exchange-simulator guide

How to run the exchange simulator and build against it. The [README](../README.md) is the API reference: every
endpoint, field and event type. This guide is the how-to around it.

| Page | For | What it covers |
|---|---|---|
| [Getting started](getting-started.md) | everyone | The Compose example in five minutes, then adding the simulator to your own Compose project |
| [Configuration](configuration.md) | operators | How settings are layered, the ones that matter in practice, persistence modes, recipes |
| [Writing a webhook receiver](webhook-receivers.md) | client developers | The delivery contract, signature verification (Python, Java, Node.js), idempotency, ordering, retries, secret rotation, replay |
| [Operations](operations.md) | operators | Managing subscriptions, monitoring and alerts, troubleshooting, backups and upgrades |
| [Benchmarking](benchmarking.md) | operators | Measuring throughput and latency in your own environment |
| [Back office](back-office.md) | demo developers | Settlement, clearing statements, payments and bank statements with the payment gateway: what to consume, how to reconcile, what each break looks like |

Runnable material:
- [`examples/compose`](../examples/compose): the simulator, two subscriptions declared in configuration, a shared
  secret, and a sample receiver that verifies every delivery.
- [`examples/receivers`](../examples/receivers): signature verification in Python, Java and Node.js. Each file checks
  itself against a known signature when run.
- [`examples/back-office`](../examples/back-office): the exchange simulator and the payment gateway on an accelerated
  clock, with settlement, clearing and bank statements, and an overlay that adds reconciliation breaks.

All three are exercised by the containerized test suite (`cargo test -p exchange-simulator --features e2e --test e2e`), so
what this guide shows is what the service does.
