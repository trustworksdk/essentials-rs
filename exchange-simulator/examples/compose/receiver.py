"""A sample webhook receiver for the exchange simulator. Standard library only; Python 3.10+.

POST /hooks/<name>  a delivery: the signature is verified (401 if it fails), duplicates are recognized by eventId,
                    and one line per event is logged. Answers with the configured status (default 204).
GET  /received      everything received, as JSON.
POST /control?status=503   answer deliveries with this status from now on (to watch retries and dead letters);
                           /control?status=204 restores normal operation.
GET  /health        200.

WEBHOOK_SECRET_FILE (or WEBHOOK_SECRET) holds the shared secret; without one, signatures are not checked.
"""

import hashlib
import hmac
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


def load_secret() -> bytes | None:
    path = os.environ.get("WEBHOOK_SECRET_FILE")
    if path:
        with open(path, "rb") as file:
            return file.read().strip()
    value = os.environ.get("WEBHOOK_SECRET")
    return value.encode() if value else None


def verify(secret: bytes, timestamp: str, signature: str, raw_body: bytes, tolerance: int = 300) -> bool:
    """See examples/receivers/verify.py: any of the comma-separated values may match (secret rotation)."""
    expected = "sha256=" + hmac.new(secret, timestamp.encode() + b"." + raw_body, hashlib.sha256).hexdigest()
    matches = any(hmac.compare_digest(expected, value.strip()) for value in signature.split(","))
    return matches and abs(time.time() - int(timestamp)) <= tolerance


SECRET = load_secret()
LOCK = threading.Lock()
RECEIVED: list[dict] = []
SEEN: set[str] = set()
STATE = {"status": 204}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        url = urlparse(self.path)
        if url.path == "/control":
            status = int(parse_qs(url.query).get("status", ["204"])[0])
            STATE["status"] = status
            print(f"control: answering deliveries with {status}", flush=True)
            return self.reply(200, {"status": status})
        if not url.path.startswith("/hooks/"):
            return self.reply(404, {"error": "not found"})
        name = url.path.removeprefix("/hooks/")
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        verified = None
        if SECRET is not None:
            verified = verify(
                SECRET,
                self.headers.get("X-Signature-Timestamp", "0"),
                self.headers.get("X-Signature", ""),
                body,
            )
            if not verified:
                print(f"{name}: rejected a delivery with a bad signature", flush=True)
                return self.reply(401, {"error": "bad signature"})
        event = json.loads(body)
        accepted = 200 <= STATE["status"] < 300
        with LOCK:
            duplicate = event["eventId"] in SEEN
            # Remember an event only once it is processed and acknowledged: a delivery answered with an error comes
            # back as a retry, and that retry is not a duplicate.
            if accepted:
                SEEN.add(event["eventId"])
            RECEIVED.append({
                "hook": name,
                "eventId": event["eventId"],
                "eventType": event["eventType"],
                "sequence": event["sequence"],
                "attempt": int(self.headers.get("X-Delivery-Attempt", "1")),
                "verified": verified,
                "duplicate": duplicate,
                "answered": STATE["status"],
            })
        print(
            f"{name}: #{event['sequence']} {event['eventType']} {event['eventId']}"
            f" attempt {self.headers.get('X-Delivery-Attempt')}"
            f"{' (duplicate)' if duplicate else ''} -> {STATE['status']}",
            flush=True,
        )
        self.reply(STATE["status"], None)

    def do_GET(self):
        if self.path == "/health":
            return self.reply(200, {"status": "ok"})
        if self.path == "/received":
            with LOCK:
                return self.reply(200, list(RECEIVED))
        self.reply(404, {"error": "not found"})

    def reply(self, status: int, payload):
        body = b"" if payload is None else json.dumps(payload).encode()
        self.send_response(status)
        if body:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # one line per event is logged above instead
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "9000"))
    print(f"receiver listening on :{port}; signatures {'checked' if SECRET else 'NOT checked'}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
