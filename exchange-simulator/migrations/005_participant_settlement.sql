-- Plan item 1.3d: optional free-form settlement details per participant (a JSON object), copied into TradeExecuted.
ALTER TABLE participants ADD COLUMN settlement TEXT
  CHECK (settlement IS NULL OR (json_valid(settlement) AND json_type(settlement) = 'object'));
