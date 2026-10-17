-- Payments, the bookings they make on the firm's account, and the bank statements built from those bookings. (Migration
-- 001 is the shared outbox schema, simulator_kit::outbox::SCHEMA.)
CREATE TABLE payments (
  id TEXT PRIMARY KEY,
  sequence INTEGER NOT NULL UNIQUE CHECK (sequence >= 1),
  idempotency_key TEXT NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 64),
  type TEXT NOT NULL CHECK (type IN ('DEPOSIT', 'WITHDRAWAL')),
  account_id TEXT NOT NULL CHECK (length(account_id) BETWEEN 1 AND 64),
  iban TEXT NOT NULL,
  holder_name TEXT NOT NULL,
  amount_minor INTEGER NOT NULL CHECK (amount_minor > 0),
  currency TEXT NOT NULL,
  currency_scale INTEGER NOT NULL,
  reference TEXT,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED', 'RETURNED')),
  reason TEXT,
  created_at TEXT NOT NULL,
  due_at TEXT,
  completed_at TEXT,
  failed_at TEXT,
  returned_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE INDEX idx_payments_due ON payments(due_at, sequence) WHERE due_at IS NOT NULL;
CREATE INDEX idx_payments_account ON payments(account_id, sequence);

CREATE TABLE bookings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  payment_id TEXT NOT NULL REFERENCES payments(id),
  kind TEXT NOT NULL CHECK (kind IN ('DEPOSIT', 'WITHDRAWAL', 'DEPOSIT_RETURN', 'WITHDRAWAL_RETURN')),
  amount_minor INTEGER NOT NULL CHECK (amount_minor <> 0),
  currency TEXT NOT NULL,
  booked_at TEXT NOT NULL
);
CREATE INDEX idx_bookings_time ON bookings(currency, booked_at);

CREATE TABLE bank_statements (
  statement_id TEXT PRIMARY KEY,
  business_date TEXT NOT NULL,
  currency TEXT NOT NULL,
  due_at TEXT NOT NULL,
  issued INTEGER NOT NULL CHECK (issued IN (0, 1)),
  event_sequence INTEGER REFERENCES events(sequence),
  body TEXT NOT NULL CHECK (json_valid(body)),
  breaks TEXT NOT NULL CHECK (json_valid(breaks))
);
CREATE INDEX idx_bank_statements_date ON bank_statements(business_date, currency);

-- The last business date whose statement cutoff was handled.
CREATE TABLE statement_cursor (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  business_date TEXT NOT NULL
);
