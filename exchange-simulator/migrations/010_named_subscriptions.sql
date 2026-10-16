-- Plan item 4.6a: webhook subscriptions declared in configuration. `source` says who owns a subscription: `api`
-- (created through POST /api/webhooks) or `config` (declared under webhooks.subscriptions and reconciled at startup
-- by `name`). Names are unique where set.
ALTER TABLE webhook_subscriptions
  ADD COLUMN name TEXT CHECK (name IS NULL OR length(name) BETWEEN 1 AND 64);
ALTER TABLE webhook_subscriptions
  ADD COLUMN source TEXT NOT NULL DEFAULT 'api' CHECK (source IN ('api', 'config'));
CREATE UNIQUE INDEX idx_webhook_subscriptions_name ON webhook_subscriptions(name) WHERE name IS NOT NULL;
-- A paused subscription still gets a delivery for every event it wants, but none is sent until it is unpaused (a
-- disabled one gets no deliveries at all).
ALTER TABLE webhook_subscriptions ADD COLUMN paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1));
