-- Plan item 5.3: end-of-day clearing statements. One row per statement: its body as delivered in
-- ClearingStatementIssued, the breaks it carries (never delivered), and whether it was issued yet (a late one waits
-- for due_at). The cursor is the last business date whose cutoff was handled, and the sequence of the last trade a
-- statement window took: the next window reads only events after it, with no index on the hot insert path.
CREATE TABLE clearing_statements (
  statement_id TEXT PRIMARY KEY,
  business_date TEXT NOT NULL,
  participant_id TEXT NOT NULL REFERENCES participants(id),
  due_at TEXT NOT NULL,
  issued INTEGER NOT NULL CHECK (issued IN (0, 1)),
  event_sequence INTEGER REFERENCES events(sequence),
  body TEXT NOT NULL CHECK (json_valid(body)),
  breaks TEXT NOT NULL CHECK (json_valid(breaks))
);
CREATE INDEX idx_clearing_statements_date ON clearing_statements(business_date, participant_id);
CREATE INDEX idx_clearing_statements_pending ON clearing_statements(due_at) WHERE issued = 0;

CREATE TABLE clearing_cursor (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  business_date TEXT NOT NULL,
  after_sequence INTEGER NOT NULL
);

-- Which statement and obligation each side of a simulated instruction appears on (JSON), when clearing runs.
ALTER TABLE settlement_instructions ADD COLUMN clearing TEXT CHECK (clearing IS NULL OR json_valid(clearing));
