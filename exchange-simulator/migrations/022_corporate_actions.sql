-- Plan item 6.23 (SR-38): corporate actions, announced from configuration or by an operator, paid or applied when due.
CREATE TABLE corporate_actions (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('CASH_DIVIDEND', 'SPLIT')),
  instrument_id TEXT NOT NULL REFERENCES instruments(id),
  ex_date TEXT,
  record_date TEXT,
  pay_date TEXT,
  amount_per_share_minor INTEGER NOT NULL DEFAULT 0,
  ratio_new INTEGER NOT NULL DEFAULT 1 CHECK (ratio_new > 0),
  ratio_old INTEGER NOT NULL DEFAULT 1 CHECK (ratio_old > 0),
  effective_date TEXT,
  announced_at TEXT NOT NULL,
  done_at TEXT
);
