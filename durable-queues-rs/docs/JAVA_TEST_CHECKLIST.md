# Java Test Checklist (Phase 4)

Every test method of Essentials 0.60.0's durable-queue test suites, mapped to the Rust test that covers it, or to the
reason it is not ported. Sources: `foundation-test/.../messaging/queue/` (the shared bases) and
`postgresql-queue/src/test/` (their PostgreSQL leaves and module-specific tests).

The PostgreSQL leaves run each base twice, once with Java's centralized fetcher and once with its traditional
per-consumer fetcher. Rust has one fetch loop per consumer, so each base test maps to one Rust test.

**Status key**
- **Ported:** same scenario and numbers, same assertions.
- **Reproduced:** same behaviour under test, set up differently; the note says how.
- **Phase 5:** ordered messages, ported with that phase.
- **N/A:** tests a feature outside the experiment's scope; the note says which.

Rust test files: `tests/java_ports.rs` (jp), `tests/consume.rs` (c), `tests/redelivery.rs` (r),
`tests/edge_cases.rs` (e), `tests/schema.rs` (s), `tests/ordered.rs` (o), unit tests in `src/` (u).

## foundation-test: DurableQueuesIT

| Java test | Status | Rust test | Notes |
|---|---|---|---|
| `test_simple_enqueueing_and_afterwards_querying_queued_messages` | Reproduced (core) | c `queued_rows_have_the_shape_java_writes`, c `a_queued_message_is_delivered_with_its_metadata_and_removed_once_handled` | Row shape, counters, metadata and timestamps are checked. Java's query and admin API (`getQueueNames`, `getQueueNameFor`, counts, `getQueuedMessages`, `queryForMessagesSoonReadyForDelivery`, `purgeQueue`) is deferred by the experiment ("defer admin APIs, statistics"). |
| `verify_queued_messages_are_dequeued_in_order` | Ported | jp `verify_queued_messages_are_dequeued_in_order` | The `getQueueNames` assertions are skipped (admin API). |
| `verify_a_message_queues_as_a_dead_letter_message_is_marked_as_such_and_will_not_be_delivered_to_the_consumer` | Reproduced | jp `verify_a_message_queued_as_a_dead_letter_message_is_not_delivered` | `queueMessageAsDeadLetterMessage` is not ported; queue-then-mark gives the same row state. |
| `verify_a_that_as_long_as_an_ordered_message_with_same_key_and_a_lower_key_order_exists_as_a_dead_letter_message…` | Reproduced | o `verify_that_an_ordered_dead_letter_blocks_its_key_until_it_is_resurrected` | Key1Msg3 is queued then marked as a dead letter before delivery starts, instead of being queued as one. Same 7-then-3 deliveries, with parallel 2. |
| `verify_hasOrderedMessageQueuedForKey` | Reproduced | o `verify_has_ordered_message_queued_for_key` | Messages are acknowledged by consuming them instead of by hand. Dead letters count. Java's final `deleteMessage` step is not ported (no delete API). |
| `verify_failed_messages_are_redelivered` | Ported | jp `verify_failed_messages_are_redelivered` | |
| `verify_a_message_that_failed_too_many_times_is_marked_as_dead_letter_message_AND_the_message_can_be_resurrected` | Ported | jp `verify_a_message_that_failed_too_many_times_is_marked_as_dead_letter_message_and_can_be_resurrected` | 6 deliveries; the dead letter has total 6 and redelivery 5; resurrection gives a 7th delivery. |
| `test_two_stage_redelivery_where_a_message_about_to_be_marked_as_a_deadletter_message_is_queued_with_a_redelivery_delay` | N/A | (r `a_resurrected_dead_letter_is_delivered_again_with_its_counters_kept` covers resurrection) | Built on a `DurableQueuesInterceptor` that resurrects inside `markAsDeadLetter`. Interceptors are not ported. |
| `verify_a_message_can_manually_be_marked_as_dead_letter_message_AND_the_message_can_afterwards_be_resurrected` | Ported | jp `verify_a_message_can_manually_be_marked_as_dead_letter_message_and_afterwards_be_resurrected` | Java passes no reason and asserts a NULL error; the Rust API takes a reason. |
| `test_messagehandler_with_call_to_markForRedeliveryIn` | Ported | jp `test_messagehandler_with_call_to_mark_for_redelivery_in`, u `a_requested_redelivery_ignores_the_cap_and_the_policy_delay` | Rust handlers request it with `Err(HandlerError::redeliver_in(delay))`. Errors are None, then "Manually requested redelivery" twice. The test also checks what Java's code does but its test does not: the cap is never applied. |
| `verify_json_deserialization_problem_causes_message_to_be_marked_as_dead_letter_message` | Reproduced | jp `verify_json_deserialization_problem_causes_message_to_be_marked_as_dead_letter_message` | Java corrupts the JSON in a proxy serializer; Rust stores an undecodable row. Metadata is kept. The error text differs: no Java exception name. |
| `verify_deserialization_failure_in_one_queue_does_not_block_other_queues` | Reproduced | jp `verify_deserialization_failure_in_one_queue_does_not_block_other_queues` | Same corruption difference. |

## foundation-test: competing consumers, duplicates, load

| Java test | Status | Rust test | Notes |
|---|---|---|---|
| `LocalCompetingConsumersDurableQueueIT.verify_queued_messages_are_dequeued_in_order` | Ported | jp `local_competing_consumers_verify_queued_messages_are_dequeued` | 2000 messages, 20 parallel, `fixedBackoff(1 ms, 5)`, no late duplicates within 1990 ms. Like Java, order is not checked. |
| `DistributedCompetingConsumersDurableQueuesIT.verify_queued_messages_are_dequeued_in_order` | Ported | jp `distributed_competing_consumers_verify_queued_messages_are_dequeued` | 2 instances × 10 parallel, 1000 messages; each instance consumes. |
| `…_with_db_connectivity_issues` | Ported | jp `…_with_db_connectivity_issues` | `docker pause` for 5 s after 2 s, as Java does. Handlers take 50 ms so that the pause hits in-flight work; Java's instant handlers finish before it. |
| `DuplicateConsumptionDurableQueuesIT.verify_no_duplicate_message_consumption` | Ported | jp `verify_no_duplicate_message_consumption` | 40 messages, 2 instances with their own pools, 1 s handlers, 10 s timeout, second instance 200 ms later; each message exactly once. |
| `…_with_db_connectivity_issues` | Ported | jp `…_with_db_connectivity_issues` | Same pause. |
| `DurableQueuesLoadIT.queue_a_large_number_of_messages` | Ported | jp `queue_a_large_number_of_messages` | 20 000 single-statement enqueues in one transaction, then more than 10 handled within 5 s. Java asserts exactly 20 000 queued although its consumer is already running; Rust asserts queued + handled = 20 000. |
| `LocalOrderedMessagesDurableQueueIT.verify_queued_ordered_messages_are_dequeued_in_order_per_key` | Ported | o `verify_queued_ordered_messages_are_dequeued_in_order_per_key` | 2000 messages, 45 keys, 20 parallel; per key, order i is the i-th handled. |
| `LocalOrderedMessagesRedeliveryDurableQueueIT.verify_…_even_if_some_messages_are_redelivered` | Ported | o `verify_queued_ordered_messages_are_dequeued_in_order_per_key_even_if_some_messages_are_redelivered` | Java's failure rule (fail while the attempts trail a global counter, up to 5 redeliveries); no dead letters. |

## postgresql-queue module

| Java test | Status | Rust test | Notes |
|---|---|---|---|
| `SingleOperationTransaction…DurableQueuesIT.test_SingleOperationTransaction` | Reproduced | c `a_queued_message_is_delivered_with_its_metadata_and_removed_once_handled`, c `competing_nodes_handle_every_message_exactly_once` | Java drives `getNextMessageReadyForDelivery` by hand: fetch, a second fetch returns empty, acknowledge. Rust has no public single-fetch API; consumers cover claim-once and acknowledge. |
| `…test_SingleOperationTransaction_timeout_messages_gets_automatically_retried` | Reproduced | r `a_message_left_claimed_by_a_crashed_node_is_redelivered_after_the_handling_timeout` | |
| `PostgresqlDurableQueuesTest.initializeWithDefaultTableName` / `…OverriddenTableName` / `…InvalidOverriddenTableName` | Ported | u `default_is_durable_queues`, u `rejects_the_names_java_tests_reject` | |
| `PostgresqlDurableQueuesBuilderDefaultsTest.test_the_default_message_handling_timeout_is_thirty_seconds` | Ported | u `the_default_message_handling_timeout_is_thirty_seconds` | |
| `PostgresqlDurableQueuesBuilderDefaultsTest.test_the_builder_needs_nothing_but_a_unitOfWorkFactory` | Reproduced | s, c (every test builds from a pool and the default config) | |
| `PostgresqlDurableQueuesMixedBatchIT.*` (2 tests) | Ported | o `a_batch_starting_with_an_ordered_message_can_also_contain_unordered_messages`, o `a_batch_starting_with_an_unordered_message_can_also_contain_ordered_messages` | Also checks that the mixed queue drains. |
| `CentralizedFetcherDurableQueueIT.test_multiple_queues_with_different_thread_configurations` | Ported | jp `test_multiple_queues_with_different_thread_configurations` | 1000 messages over three queues with 10/5/2 parallel; no message crosses queues. |
| `CentralizedFetcherDurableQueueIT.verify_ordered_messages_are_processed_in_order` | Ported | o `verify_ordered_messages_are_processed_in_order` | 10 keys × 20 orders, queued shuffled, 15 parallel, 10 ms per message. |
| `BatchedFetchStrategyIT.*` (20 tests) | N/A | | Java's opt-in batched fetch (`useBatchedFetch`, off by default) is out of scope (CONTRACT.md §4.3). |
| `DurableQueueMessageObserverIT.*` (5 tests) | N/A | | Message observer, statistics and admin API are deferred by the experiment. |
| `LegacyQueueStatisticsRemovalIT.*` (3 tests) | N/A | | Java's 0.60 migration; Rust never drops or migrates schema (Phase 1 decision). |
| `IOExceptionUtilTest`, `ImmutablePayloadSerializationTest`, `TestMessageFactoryTest` | N/A | | Java utilities and test fixtures. |
| `PostgresqlDurableQueuesLatencyIT`, `benchmark/*` | Phase 7 | | Benchmarks. |
| `DurableLocalCommandBus*IT` | N/A | | Command bus, not a queue feature. |

## Phase 4 edge cases (from the plan, not from Java)

| Edge case | Rust test |
|---|---|
| A handler succeeds but the acknowledgement fails | e `a_failed_acknowledgement_leads_to_a_duplicate_delivery_after_the_handling_timeout`. A `QueueStorage` test double fails the first acknowledgement. |
| The database restarts mid-run | e `consumers_survive_a_database_restart_and_every_message_is_handled`. Real container stop and start on a fixed port. |
| More parallel consumers than pooled connections | e `more_parallel_consumers_than_pooled_connections_still_handle_every_message_once` |
| A handler runs longer than the handling timeout | e `a_handler_that_outlives_the_handling_timeout_sees_its_message_delivered_again`. A duplicate is expected; see the note in the test. |
| Shutdown with in-flight messages | c `stop_waits_for_in_flight_handlers_and_acknowledges_them`, c `stop_abandons_handlers_still_running_at_the_shutdown_timeout` |

## Summary

| Status | Java tests |
|---|---|
| Ported or reproduced | 30, plus the 5 edge cases from the plan |
| N/A, feature out of scope | 29: `test_two_stage_redelivery…` (interceptors), `BatchedFetchStrategyIT` (20), `DurableQueueMessageObserverIT` (5), `LegacyQueueStatisticsRemovalIT` (3) |
| N/A, Java-only utilities and fixtures | `IOExceptionUtilTest`, `ImmutablePayloadSerializationTest`, `TestMessageFactoryTest`, `DurableLocalCommandBus*IT` (methods not counted) |
| Phase 7 | the benchmark classes |
