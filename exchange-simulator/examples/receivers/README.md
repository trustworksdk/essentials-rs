# Signature verification

The webhook signature check in three languages. Each file is a function to copy into a receiver, plus a `main`
that checks it against a known signature: a valid one, a rotation's second value, a changed body, another secret,
and an old timestamp.

| File | Run |
|---|---|
| `verify.py` | `python3 verify.py` (3.10+) |
| `Verify.java` | `java Verify.java` (17+) |
| `verify.mjs` | `node verify.mjs` (18+) |

[Writing a webhook receiver](../../docs/webhook-receivers.md) explains the scheme and the rules around it. The
containerized tests run all three files (`the_signature_snippets_verify_in_their_runtimes` in `tests/e2e.rs`).
