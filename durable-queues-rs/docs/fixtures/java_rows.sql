-- SYNTHETIC golden fixtures: rows shaped exactly as Essentials 0.60.0 PostgresqlDurableQueues#queueMessage
-- inserts them (see docs/CONTRACT.md §2). Phase 6 verified them against the real Java harness (CONTRACT.md §13):
-- tests/interop.rs diffs every row except 7 with what Essentials 0.60.0 writes for the same messages.
-- Assumes the table from CONTRACT.md §1 exists with the default name `durable_queues`.
-- Timestamps are relative to now() so "ready" rows are ready and "delayed" rows are in the future whenever loaded.

INSERT INTO durable_queues
    (id, queue_name, message_payload, message_payload_type, added_ts, next_delivery_ts,
     last_delivery_error, is_dead_letter_message, meta_data, delivery_mode, key, key_order)
VALUES
-- 1. Plain message, ready now, with metadata (MessageMetaData = flat string map)
('2f1c7c6e-6a4b-11f1-9a2e-0242ac120002', 'Orders',
 '{"orderId": "order-1", "customerId": "customer-1", "amount": 125.5, "placedAt": "2026-10-11T12:00:00Z"}',
 'com.acme.orders.OrderPlaced',
 now() - interval '1 second', now() - interval '1 second',
 NULL, FALSE, '{"correlationId": "corr-1", "essentials.causedByEventId": "evt-42"}', 'NORMAL', NULL, -1),

-- 2. Plain message, empty metadata (Java always writes {} - never NULL)
('2f1c7c6e-6a4b-11f1-9a2e-0242ac120003', 'Orders',
 '{"orderId": "order-2", "customerId": "customer-2", "amount": 10, "placedAt": "2026-10-11T12:00:01Z"}',
 'com.acme.orders.OrderPlaced',
 now() - interval '1 second', now() - interval '1 second',
 NULL, FALSE, '{}', 'NORMAL', NULL, -1),

-- 3. Delayed message (deliveryDelay = 1 hour) - must NOT be claimed yet
('2f1c7c6e-6a4b-11f1-9a2e-0242ac120004', 'Orders',
 '{"orderId": "order-3", "reason": "customer request"}',
 'com.acme.orders.OrderEvent$OrderCancelled',
 now(), now() + interval '1 hour',
 NULL, FALSE, '{}', 'NORMAL', NULL, -1),

-- 4+5. Ordered messages for one key (Rust before Phase 5 must never claim these)
('2f1c7c6e-6a4b-11f1-9a2e-0242ac120005', 'Orders',
 '{"orderId": "order-4", "step": 0}', 'com.acme.orders.OrderEvent$OrderAdded',
 now() - interval '1 second', now() - interval '1 second',
 NULL, FALSE, '{}', 'IN_ORDER', 'order-4', 0),
('2f1c7c6e-6a4b-11f1-9a2e-0242ac120006', 'Orders',
 '{"orderId": "order-4", "step": 1}', 'com.acme.orders.OrderEvent$OrderAdded',
 now() - interval '1 second', now() - interval '1 second',
 NULL, FALSE, '{}', 'IN_ORDER', 'order-4', 1),

-- 6. Queued directly as a dead letter (queueMessageAsDeadLetterMessage: next_delivery_ts NULL, stack trace as error)
('2f1c7c6e-6a4b-11f1-9a2e-0242ac120007', 'Orders',
 '{"orderId": "order-5", "customerId": "customer-5", "amount": 1, "placedAt": "2026-10-11T12:00:02Z"}',
 'com.acme.orders.OrderPlaced',
 now(), NULL,
 E'java.lang.IllegalStateException: boom\n\tat com.acme.orders.Handler.handle(Handler.java:42)',
 TRUE, '{}', 'NORMAL', NULL, -1),

-- 7. Unknown payload shape for the type -> deserialization failure -> direct dead letter at fetch
('2f1c7c6e-6a4b-11f1-9a2e-0242ac120008', 'Orders',
 '{"unexpected": true}', 'com.acme.orders.OrderPlaced',
 now() - interval '1 second', now() - interval '1 second',
 NULL, FALSE, '{}', 'NORMAL', NULL, -1),

-- 8. Other queue - must be untouched by consumers of 'Orders'
('2f1c7c6e-6a4b-11f1-9a2e-0242ac120009', 'Invoices',
 '{"invoiceId": "inv-1"}', 'com.acme.billing.InvoiceIssued',
 now() - interval '1 second', now() - interval '1 second',
 NULL, FALSE, '{}', 'NORMAL', NULL, -1);
