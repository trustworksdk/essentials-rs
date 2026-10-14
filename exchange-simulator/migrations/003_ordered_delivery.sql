-- Plan item 1.3b: optional strictly ordered delivery per subscription. An ordered subscription has at most one
-- delivery in flight: the unfinished one with the lowest event sequence.
ALTER TABLE webhook_subscriptions ADD COLUMN ordered INTEGER NOT NULL DEFAULT 0 CHECK (ordered IN (0, 1));
CREATE INDEX idx_webhook_unfinished ON webhook_deliveries(subscription_id, event_sequence)
  WHERE status IN ('PENDING', 'IN_FLIGHT');
