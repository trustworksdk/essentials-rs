-- Plan item 6.3: the webhook subscription's participant filter (JSON array of participant IDs; NULL receives every
-- event of its types). The same statement as simulator_kit::outbox::PARTICIPANT_FILTER, which the payment gateway runs;
-- the shared-schema test keeps them equal.
ALTER TABLE webhook_subscriptions ADD COLUMN participants TEXT
  CHECK (participants IS NULL OR (json_valid(participants) AND json_type(participants) = 'array'));
