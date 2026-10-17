-- Plan item 4.6b: webhook maintenance. A rotated secret stays valid for a grace period: until previous_secret_until,
-- deliveries are signed with both the new and the previous secret. A deleted subscription is kept (its deliveries and
-- attempts reference it) but is hidden from the API, gets no new deliveries and sends none.
ALTER TABLE webhook_subscriptions
  ADD COLUMN previous_secret TEXT CHECK (previous_secret IS NULL OR length(previous_secret) BETWEEN 16 AND 256);
ALTER TABLE webhook_subscriptions ADD COLUMN previous_secret_until TEXT;
ALTER TABLE webhook_subscriptions ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0, 1));
