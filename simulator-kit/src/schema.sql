-- The shared tables as they stand: the canonical event log, webhook subscriptions, deliveries and attempts (the
-- transactional outbox), and the virtual clock's stored reading. A simulator applies this in its first migration;
-- exchange-simulator built the same tables up over its migrations 001-011 and checks they still match.
CREATE TABLE events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  event_type TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  -- Which component produced the event, so authority stays explicit when several parties write to one log.
  source TEXT NOT NULL,
  -- Optional identifier of the simulation run the event belongs to.
  simulation_id TEXT,
  aggregate_id TEXT NOT NULL,
  correlation_id TEXT,
  payload TEXT NOT NULL CHECK (json_valid(payload)),
  occurred_at TEXT NOT NULL
);

CREATE TABLE webhook_subscriptions (
  id TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  event_types TEXT NOT NULL CHECK (json_valid(event_types)),
  created_at TEXT NOT NULL,
  secret TEXT CHECK (secret IS NULL OR length(secret) BETWEEN 16 AND 256),
  ordered INTEGER NOT NULL DEFAULT 0 CHECK (ordered IN (0, 1)),
  fault_policy TEXT CHECK (fault_policy IS NULL OR (json_valid(fault_policy) AND json_type(fault_policy) = 'object')),
  name TEXT CHECK (name IS NULL OR length(name) BETWEEN 1 AND 64),
  source TEXT NOT NULL DEFAULT 'api' CHECK (source IN ('api', 'config')),
  paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
  previous_secret TEXT CHECK (previous_secret IS NULL OR length(previous_secret) BETWEEN 16 AND 256),
  previous_secret_until TEXT,
  deleted INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0, 1))
);

CREATE TABLE webhook_deliveries (
  id TEXT PRIMARY KEY,
  subscription_id TEXT NOT NULL REFERENCES webhook_subscriptions(id),
  event_sequence INTEGER NOT NULL REFERENCES events(sequence),
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'IN_FLIGHT', 'DELIVERED', 'DEAD')),
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  last_error TEXT,
  delivered_at TEXT,
  held INTEGER NOT NULL DEFAULT 0 CHECK (held IN (0, 1)),
  UNIQUE (subscription_id, event_sequence)
);

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

CREATE TABLE virtual_clock (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  now TEXT NOT NULL,
  running INTEGER NOT NULL CHECK (running IN (0, 1))
);

CREATE INDEX idx_events_aggregate ON events(aggregate_id);
CREATE INDEX idx_webhook_attempts_delivery ON webhook_attempts(delivery_id);
CREATE INDEX idx_webhook_pending ON webhook_deliveries(status, next_attempt_at);
CREATE INDEX idx_webhook_unfinished ON webhook_deliveries(subscription_id, event_sequence)
  WHERE status IN ('PENDING', 'IN_FLIGHT');
CREATE UNIQUE INDEX idx_webhook_subscriptions_name ON webhook_subscriptions(name) WHERE name IS NOT NULL;
