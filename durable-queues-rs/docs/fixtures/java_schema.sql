-- Verbatim DDL from Essentials 0.60.0 DurableQueuesSql (getCreateQueueTableSql + the three index statements),
-- {:tableName} = durable_queues. Used by the tests as "a schema Java created".

CREATE TABLE IF NOT EXISTS durable_queues (
                    id                     TEXT PRIMARY KEY,
                    queue_name             TEXT NOT NULL,
                    message_payload        JSONB NOT NULL,
                    message_payload_type   TEXT NOT NULL,
                    added_ts               TIMESTAMPTZ NOT NULL,
                    next_delivery_ts       TIMESTAMPTZ,
                    delivery_ts            TIMESTAMPTZ DEFAULT NULL,
                    total_attempts         INTEGER DEFAULT 0,
                    redelivery_attempts    INTEGER DEFAULT 0,
                    last_delivery_error    TEXT DEFAULT NULL,
                    is_being_delivered     BOOLEAN DEFAULT FALSE,
                    is_dead_letter_message BOOLEAN NOT NULL DEFAULT FALSE,
                    meta_data              JSONB DEFAULT NULL,
                    delivery_mode          TEXT NOT NULL,
                    key                    TEXT DEFAULT NULL,
                    key_order              BIGINT DEFAULT -1
                    );

CREATE INDEX IF NOT EXISTS idx_durable_queues_ordered_msg ON durable_queues (queue_name, key, key_order);

CREATE INDEX IF NOT EXISTS idx_durable_queues_unordered_ready
                      ON durable_queues (queue_name, next_delivery_ts)
                      INCLUDE (id)
                      WHERE key IS NULL
                        AND NOT is_dead_letter_message
                        AND NOT is_being_delivered;

CREATE INDEX IF NOT EXISTS idx_durable_queues_ordered_head
                      ON durable_queues (queue_name, key_order, next_delivery_ts)
                      INCLUDE (id)
                      WHERE key IS NOT NULL
                        AND is_dead_letter_message = FALSE
                        AND is_being_delivered     = FALSE;
