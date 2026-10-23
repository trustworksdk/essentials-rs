-- Plan item 6.22 (SR-37): exchange fees per execution and side, in minor units of the instrument's currency.
ALTER TABLE executions ADD COLUMN buyer_fee_minor INTEGER NOT NULL DEFAULT 0 CHECK (buyer_fee_minor >= 0);
ALTER TABLE executions ADD COLUMN seller_fee_minor INTEGER NOT NULL DEFAULT 0 CHECK (seller_fee_minor >= 0);
