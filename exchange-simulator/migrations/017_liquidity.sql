-- Plan item 6.7: an instrument's own liquidity settings (JSON, as given to the API; NULL takes liquidity.default).
ALTER TABLE instruments ADD COLUMN liquidity TEXT CHECK (liquidity IS NULL OR json_valid(liquidity));
