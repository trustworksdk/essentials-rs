-- Plan item 6.18 (SR-33): the value date of a payment's latest booking, the business date the booking falls in. Set
-- when a payment completes or is returned; payments stored before this column are dated at the next startup.
ALTER TABLE payments ADD COLUMN value_date TEXT;
