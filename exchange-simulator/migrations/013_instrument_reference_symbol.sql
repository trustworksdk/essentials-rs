-- Plan item 6.1: the provider symbol a price source quotes the instrument under (e.g. AAPL), which may differ from its
-- exchange symbol. Optional; at most 64 characters.
ALTER TABLE instruments ADD COLUMN reference_symbol TEXT
  CHECK (reference_symbol IS NULL OR length(reference_symbol) BETWEEN 1 AND 64);
