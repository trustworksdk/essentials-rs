-- Milestone 1 schema. Connection pragmas (foreign_keys, journal_mode = WAL, busy_timeout) are applied per connection
-- by persistence::open, not here: they are not migration state, and journal_mode cannot change inside the
-- migration's transaction.

CREATE TABLE instruments (
  id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  mic TEXT,
  isin TEXT,
  cusip TEXT,
  currency TEXT NOT NULL,
  currency_scale INTEGER NOT NULL CHECK (currency_scale BETWEEN 0 AND 9),
  tick_size_minor INTEGER NOT NULL CHECK (tick_size_minor > 0),
  lot_size INTEGER NOT NULL CHECK (lot_size > 0),
  CHECK (mic IS NULL OR length(mic) = 4),
  CHECK (isin IS NULL OR length(isin) = 12),
  CHECK (cusip IS NULL OR length(cusip) = 9)
);
CREATE UNIQUE INDEX idx_instruments_mic_symbol ON instruments(mic, symbol) WHERE mic IS NOT NULL;
CREATE INDEX idx_instruments_isin ON instruments(isin) WHERE isin IS NOT NULL;
CREATE INDEX idx_instruments_cusip ON instruments(cusip) WHERE cusip IS NOT NULL;

CREATE TABLE participants (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE orders (
  id TEXT PRIMARY KEY,
  -- Optional participant-chosen ID; makes order submission idempotent per participant.
  client_order_id TEXT CHECK (client_order_id IS NULL OR length(client_order_id) BETWEEN 1 AND 64),
  instrument_id TEXT NOT NULL REFERENCES instruments(id),
  participant_id TEXT NOT NULL REFERENCES participants(id),
  side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
  order_type TEXT NOT NULL CHECK (order_type IN ('LIMIT', 'MARKET')),
  limit_price_ticks INTEGER,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  remaining_quantity INTEGER NOT NULL CHECK (remaining_quantity >= 0 AND remaining_quantity <= quantity),
  sequence INTEGER NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK (status IN ('OPEN', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  -- IS NOT NULL is required: a NULL comparison makes a CHECK pass, so `limit_price_ticks > 0` alone admits a LIMIT
  -- order without a price.
  CHECK ((order_type = 'MARKET' AND limit_price_ticks IS NULL)
      OR (order_type = 'LIMIT' AND limit_price_ticks IS NOT NULL AND limit_price_ticks > 0))
);
CREATE UNIQUE INDEX idx_orders_client_order_id ON orders(participant_id, client_order_id)
  WHERE client_order_id IS NOT NULL;
CREATE INDEX idx_orders_active ON orders(instrument_id, side, limit_price_ticks, sequence)
  WHERE status IN ('OPEN', 'PARTIALLY_FILLED');

CREATE TABLE executions (
  id TEXT PRIMARY KEY,
  instrument_id TEXT NOT NULL REFERENCES instruments(id),
  buy_order_id TEXT NOT NULL REFERENCES orders(id),
  sell_order_id TEXT NOT NULL REFERENCES orders(id),
  buyer_id TEXT NOT NULL REFERENCES participants(id),
  seller_id TEXT NOT NULL REFERENCES participants(id),
  price_ticks INTEGER NOT NULL CHECK (price_ticks > 0),
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  executed_at TEXT NOT NULL
);
CREATE INDEX idx_executions_instrument_time ON executions(instrument_id, executed_at);

CREATE TABLE events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  event_type TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  -- Which component produced the event (e.g. 'exchange'; later the settlement simulator), so lifecycle authority
  -- stays explicit when external and simulated settlement run side by side.
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
  created_at TEXT NOT NULL
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
  UNIQUE (subscription_id, event_sequence)
);
CREATE INDEX idx_webhook_pending ON webhook_deliveries(status, next_attempt_at);
