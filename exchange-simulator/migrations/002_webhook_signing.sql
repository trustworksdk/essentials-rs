-- Plan item 1.3a: optional per-subscription secret for HMAC-SHA256 webhook signatures. NULL means unsigned.
ALTER TABLE webhook_subscriptions
  ADD COLUMN secret TEXT CHECK (secret IS NULL OR length(secret) BETWEEN 16 AND 256);
