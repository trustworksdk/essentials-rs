-- Plan item 2.4: settlement outcomes reported by an external service. Stored apart from the simulator's
-- instructions and never merged into them.
CREATE TABLE settlement_reports (
  id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL REFERENCES executions(id),
  status TEXT NOT NULL
    CHECK (status IN ('PENDING', 'DELAYED', 'SETTLED', 'FAILED', 'REJECTED', 'CANCELLED', 'CORRECTED')),
  -- The reporter's own identifier; makes reporting idempotent per execution.
  reference TEXT CHECK (reference IS NULL OR length(reference) BETWEEN 1 AND 128),
  reason TEXT,
  details TEXT CHECK (details IS NULL OR (json_valid(details) AND json_type(details) = 'object')),
  -- When the external service says the outcome happened, and when the exchange received the report.
  occurred_at TEXT NOT NULL,
  received_at TEXT NOT NULL
);
CREATE UNIQUE INDEX idx_settlement_reports_reference ON settlement_reports(execution_id, reference)
  WHERE reference IS NOT NULL;
CREATE INDEX idx_settlement_reports_execution ON settlement_reports(execution_id, received_at);
