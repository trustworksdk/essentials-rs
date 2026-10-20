-- Plan item 6.9a (SR-29): every execution's trade date and intended settlement date (ISO dates), set when it is
-- committed. Executions stored before this migration get them at the next startup, on the then-current calendar.
ALTER TABLE executions ADD COLUMN trade_date TEXT;
ALTER TABLE executions ADD COLUMN intended_settlement_date TEXT;
