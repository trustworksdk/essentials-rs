-- Plan item 3.3: delivery fault injection. A subscription may carry a fault policy (JSON, test control only) that
-- damages, duplicates, delays or holds back the outgoing copies of its events; canonical events are never changed.
ALTER TABLE webhook_subscriptions
  ADD COLUMN fault_policy TEXT CHECK (fault_policy IS NULL OR (json_valid(fault_policy) AND json_type(fault_policy) = 'object'));
-- Whether the delivery was held back once by an out-of-order fault (it is never held twice).
ALTER TABLE webhook_deliveries ADD COLUMN held INTEGER NOT NULL DEFAULT 0 CHECK (held IN (0, 1));
-- Every request sent for a delivery (duplicates included), and every hold, with the faults applied. `body` is the
-- exact body sent when a fault changed it; NULL means the canonical envelope.
CREATE TABLE webhook_attempts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_id TEXT NOT NULL REFERENCES webhook_deliveries(id),
  attempt INTEGER NOT NULL CHECK (attempt >= 1),
  outcome TEXT NOT NULL CHECK (outcome IN ('SENT', 'HELD')),
  faults TEXT NOT NULL CHECK (json_valid(faults) AND json_type(faults) = 'array'),
  body TEXT,
  http_status INTEGER,
  error TEXT,
  at TEXT NOT NULL
);
CREATE INDEX idx_webhook_attempts_delivery ON webhook_attempts(delivery_id);
