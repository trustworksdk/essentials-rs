-- Plan item 6.2: an instrument's own price source (JSON, as given to the API; NULL takes price_sources.default) and
-- when its current source was started. A synthetic walk is stepped on a grid from that instant, so storing it lets a
-- restart reproduce the walk.
ALTER TABLE instruments ADD COLUMN price_source TEXT CHECK (price_source IS NULL OR json_valid(price_source));
ALTER TABLE instruments ADD COLUMN price_source_since TEXT;
