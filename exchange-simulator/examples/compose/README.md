# Compose example

The exchange simulator with a sample webhook receiver: the walkthrough is in
[Getting started](../../docs/getting-started.md).

| File | |
|---|---|
| `compose.yaml` | The `exchange` (built from the workspace) and `receiver` services, a shared Docker secret, a volume |
| `exchange.yaml` | The service configuration: two declared webhook subscriptions, `settlement` and `orders` |
| `secrets/webhook-secret.txt` | The shared signing secret. **A demo value: change it** for anything beyond a demo |
| `receiver.py` | A receiver that verifies signatures, deduplicates by `eventId`, logs one line per event, and can be told to fail (`POST /control?status=503`) |

```sh
docker compose up --build -d --wait
docker compose logs -f receiver
docker compose down -v
```
