-- Plan item 6.21 (SR-36): trade corrections and busts. An order's busted quantity is neither filled nor remaining; an
-- execution is corrected in place or marked busted; seeded corrections and busts wait in trade_adjustments.
ALTER TABLE orders ADD COLUMN busted_quantity INTEGER NOT NULL DEFAULT 0 CHECK (busted_quantity >= 0);
ALTER TABLE executions ADD COLUMN status TEXT NOT NULL DEFAULT 'EXECUTED'
  CHECK (status IN ('EXECUTED', 'CORRECTED', 'BUSTED'));
CREATE TABLE trade_adjustments (
  execution_id TEXT PRIMARY KEY REFERENCES executions(id),
  due_at TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('CORRECT', 'BUST'))
);
CREATE INDEX idx_trade_adjustments_due ON trade_adjustments(due_at, execution_id);
