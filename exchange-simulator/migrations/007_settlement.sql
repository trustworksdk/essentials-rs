-- Plan item 2.2: simulated settlement. One instruction per execution; open ones have a due_at. The simulator takes
-- TradeExecuted events in by sequence, tracked in consumer_offsets.
CREATE TABLE settlement_instructions (
  execution_id TEXT PRIMARY KEY REFERENCES executions(id),
  trade_sequence INTEGER NOT NULL UNIQUE REFERENCES events(sequence),
  instrument_id TEXT NOT NULL REFERENCES instruments(id),
  buyer_id TEXT NOT NULL REFERENCES participants(id),
  seller_id TEXT NOT NULL REFERENCES participants(id),
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  price_ticks INTEGER NOT NULL,
  currency TEXT NOT NULL,
  currency_scale INTEGER NOT NULL,
  tick_size_minor INTEGER NOT NULL,
  amount_minor INTEGER NOT NULL,
  trade_date TEXT NOT NULL,
  intended_settlement_date TEXT NOT NULL,
  status TEXT NOT NULL
    CHECK (status IN ('PENDING', 'DELAYED', 'SETTLED', 'FAILED', 'REJECTED', 'CANCELLED', 'CORRECTED')),
  attempts INTEGER NOT NULL DEFAULT 0,
  delays INTEGER NOT NULL DEFAULT 0,
  due_at TEXT,
  settled_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE INDEX idx_settlement_due ON settlement_instructions(due_at, trade_sequence) WHERE due_at IS NOT NULL;

CREATE TABLE consumer_offsets (
  consumer TEXT PRIMARY KEY,
  last_sequence INTEGER NOT NULL
);

-- Settlement history and lookups go by execution ID, which is the settlement events' aggregate ID.
CREATE INDEX idx_events_aggregate ON events(aggregate_id);
