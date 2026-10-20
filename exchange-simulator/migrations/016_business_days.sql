-- Plan item 6.5: the last business date closed with BusinessDayClosed. Closing runs whether or not clearing does, so it
-- keeps its own cursor beside clearing_cursor.
CREATE TABLE business_day_cursor (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  business_date TEXT NOT NULL
);
