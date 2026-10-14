-- Plan item 1.3c: trading can be switched off per instrument. Orders for a disabled instrument are rejected
-- (INSTRUMENT_DISABLED); cancels still work and resting orders stay on the book.
ALTER TABLE instruments ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1));
