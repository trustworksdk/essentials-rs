-- Plan item 2.1: the virtual clock's state, so a restart resumes simulated time instead of jumping back.
CREATE TABLE virtual_clock (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  now TEXT NOT NULL,
  running INTEGER NOT NULL CHECK (running IN (0, 1))
);
