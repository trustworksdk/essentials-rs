-- foreign_keys: off
-- Plan item 6.10 (SR-16, SR-17): an order's time in force, and the EXPIRED status of a DAY order still open at the
-- close. SQLite cannot change a CHECK constraint, so the table is rebuilt (with foreign keys off; the runner checks
-- them before committing). Orders from before this migration were good till cancelled: they become GTC.
CREATE TABLE orders_new (
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
  status TEXT NOT NULL
    CHECK (status IN ('OPEN', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  time_in_force TEXT NOT NULL DEFAULT 'GTC' CHECK (time_in_force IN ('DAY', 'GTC', 'IOC')),
  -- IS NOT NULL is required: a NULL comparison makes a CHECK pass, so `limit_price_ticks > 0` alone admits a LIMIT
  -- order without a price.
  CHECK ((order_type = 'MARKET' AND limit_price_ticks IS NULL)
      OR (order_type = 'LIMIT' AND limit_price_ticks IS NOT NULL AND limit_price_ticks > 0))
);
INSERT INTO orders_new (id, client_order_id, instrument_id, participant_id, side, order_type, limit_price_ticks,
                        quantity, remaining_quantity, sequence, status, created_at, updated_at, time_in_force)
  SELECT id, client_order_id, instrument_id, participant_id, side, order_type, limit_price_ticks, quantity,
         remaining_quantity, sequence, status, created_at, updated_at,
         CASE order_type WHEN 'MARKET' THEN 'IOC' ELSE 'GTC' END
  FROM orders;
DROP TABLE orders;
ALTER TABLE orders_new RENAME TO orders;
CREATE UNIQUE INDEX idx_orders_client_order_id ON orders(participant_id, client_order_id)
  WHERE client_order_id IS NOT NULL;
CREATE INDEX idx_orders_active ON orders(instrument_id, side, limit_price_ticks, sequence)
  WHERE status IN ('OPEN', 'PARTIALLY_FILLED');
