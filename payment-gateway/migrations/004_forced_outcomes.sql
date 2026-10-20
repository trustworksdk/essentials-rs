-- Plan item 6.6: an outcome a test forced on a payment (POST /test/payments/{id}/outcome, decision D20), as JSON. It
-- replaces the seeded draws when the payment's outcome is due, so a test gets the same path every time.
ALTER TABLE payments ADD COLUMN forced_outcome TEXT
  CHECK (forced_outcome IS NULL OR (json_valid(forced_outcome) AND json_type(forced_outcome) = 'object'));
