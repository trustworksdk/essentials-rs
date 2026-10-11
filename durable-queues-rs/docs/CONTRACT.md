# Durable Queues — Java Behavioural Contract (Essentials 0.60.0)

Phase 0 deliverable of [`IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md). Everything here was extracted from
`trustworksdk/essentials-project` at tag **`0.60.0`**. Source files are cited per section, with paths relative to
`components/`:

| Short name | File |
|---|---|
| `DurableQueuesSql` | `postgresql-queue/.../queue/postgresql/DurableQueuesSql.java` |
| `PostgresqlDurableQueues` | `postgresql-queue/.../queue/postgresql/PostgresqlDurableQueues.java` |
| `QueuedMessageRowMapper` | `postgresql-queue/.../queue/postgresql/QueuedMessageRowMapper.java` |
| `DefaultDurableQueueConsumer` | `foundation/.../messaging/queue/DefaultDurableQueueConsumer.java` |
| `CentralizedMessageFetcher` | `foundation/.../messaging/queue/CentralizedMessageFetcher.java` |
| `MessageDeliveryClassifier` | `foundation/.../messaging/queue/MessageDeliveryClassifier.java` |
| `RedeliveryPolicy` | `foundation/.../messaging/RedeliveryPolicy.java` |
| `PostgresqlUtil` | `foundation/.../postgresql/PostgresqlUtil.java` |

Wherever this contract and the bundled LLM docs disagree, **this contract wins**: it was taken from the code. The
known disagreements are listed in §11.

`{table}` below is the configured table name (default `durable_queues`). Java lower-cases it, then validates it
(§9).

---

## 1. Schema

```sql
CREATE TABLE IF NOT EXISTS {table} (
    id                     TEXT PRIMARY KEY,
    queue_name             TEXT NOT NULL,
    message_payload        JSONB NOT NULL,
    message_payload_type   TEXT NOT NULL,
    added_ts               TIMESTAMPTZ NOT NULL,
    next_delivery_ts       TIMESTAMPTZ,              -- NULL while being delivered and for dead letters
    delivery_ts            TIMESTAMPTZ DEFAULT NULL, -- set when claimed
    total_attempts         INTEGER DEFAULT 0,
    redelivery_attempts    INTEGER DEFAULT 0,
    last_delivery_error    TEXT DEFAULT NULL,
    is_being_delivered     BOOLEAN DEFAULT FALSE,
    is_dead_letter_message BOOLEAN NOT NULL DEFAULT FALSE,
    meta_data              JSONB DEFAULT NULL,
    delivery_mode          TEXT NOT NULL,            -- 'NORMAL' | 'IN_ORDER'
    key                    TEXT DEFAULT NULL,        -- OrderedMessage key; NULL for NORMAL
    key_order              BIGINT DEFAULT -1         -- OrderedMessage order (>= 0); -1 for NORMAL
);

CREATE INDEX IF NOT EXISTS idx_{table}_ordered_msg ON {table} (queue_name, key, key_order);

CREATE INDEX IF NOT EXISTS idx_{table}_unordered_ready
  ON {table} (queue_name, next_delivery_ts)
  INCLUDE (id)
  WHERE key IS NULL
    AND NOT is_dead_letter_message
    AND NOT is_being_delivered;

CREATE INDEX IF NOT EXISTS idx_{table}_ordered_head
  ON {table} (queue_name, key_order, next_delivery_ts)
  INCLUDE (id)
  WHERE key IS NOT NULL
    AND is_dead_letter_message = FALSE
    AND is_being_delivered     = FALSE;
```

Java also runs repeatable cleanup on startup:

- `DROP INDEX IF EXISTS` for the legacy indexes `_queue_name`, `_next_delivery_ts`, `_is_dead_letter_message`,
  `_is_being_delivered`, `_next_msg`, `_ready` and `_ordered_ready`
- a `DO $$` block that drops the pre-0.60 queue-statistics trigger, function and table

With LISTEN/NOTIFY enabled it also installs a notification trigger. **The Rust port only needs the CREATE
statements.** The cleanup belongs to Java's migration path; Rust must never drop anything.

## 2. Column-value conventions

| Column | Value written by Java | Source |
|---|---|---|
| `id` | `RandomIdGenerator.generate()`: a UUID string. Time-based v1 if `com.fasterxml.uuid` is on the classpath, otherwise v4. **Opaque TEXT**: readers must not parse it. | `QueueEntryId.random()` |
| `queue_name` | `QueueName.toString()`, verbatim | |
| `message_payload` | Jackson 3 JSON of the payload object (Essentials object mapper) | `queueMessage` |
| `message_payload_type` | `payload.getClass().getName()`: the **Java FQCN**, nested classes with `$` (e.g. `com.acme.OrderEvent$OrderAdded`). Java deserializes with `jsonSerializer.deserialize(json, typeName)`, so the name must resolve on the Java classpath. | `queueMessage`, `DurableQueuesSerialization` |
| `meta_data` | `MessageMetaData` implements `Map<String,String>`, so it is a flat JSON object of string → string. Java always writes it (`{}` when empty). On read, NULL means empty. | `MessageMetaData`, `QueuedMessageRowMapper` |
| `delivery_mode` | `'NORMAL'`, or `'IN_ORDER'` for an `OrderedMessage` | |
| `key`, `key_order` | NORMAL: `NULL`, `-1`. IN_ORDER: `key` non-null and `order >= 0` (both validated) | |
| `added_ts` | `Instant.now()` on the **app clock** | |
| `next_delivery_ts` | `added_ts + deliveryDelay` (0 by default); `NULL` when enqueued directly as a dead letter | |
| `last_delivery_error` | **The full Java stack trace** (`Exceptions.getStackTrace(e)`), the `markAsDeadLetter` reason string, the literal `"Handler Processing of the Message was determined to have Timed Out"` for a stuck reset, or `RetryMessage.MANUALLY_REQUESTED_REDELIVERY` for a manual retry | |
| `is_being_delivered` | Not set on insert (DEFAULT FALSE) | |

**Clock source: every timestamp, including `:now` in every claim/reset predicate, comes from the application clock**
(`Instant.now()` bound as a parameter). No statement uses DB `now()`. Consequences for mixed Java/Rust nodes:

- Clock skew between nodes shifts delayed delivery by that skew.
- A node whose clock runs ahead resets other nodes' in-flight messages early (§6). Keep nodes NTP-synced; Phase 6
  scenario 7 measures this.
- Precision: PG stores microseconds. Rust must write UTC `TIMESTAMPTZ` values; chrono's nanoseconds are truncated
  by PG, the same as Java's.

## 3. Message lifecycle (state machine)

```text
              enqueue (delay d)
                    │  next_delivery_ts = added_ts + d
                    ▼
 ┌──────────────► READY  (is_being_delivered=F, is_dead_letter=F, next_delivery_ts <= now)
 │                  │ claim  (§4): total_attempts += 1, next_delivery_ts = NULL,
 │                  │          is_being_delivered = T, delivery_ts = now
 │                  ▼
 │             BEING DELIVERED ──────── handler OK ──► ack: DELETE row
 │                  │       │
 │                  │       └── delivery_ts <= now − timeout  (stuck reset, §6)
 │ retry (§5)       │             redelivery_attempts += 1, next_delivery_ts = now,
 │ redelivery_      │             is_being_delivered = F, delivery_ts = NULL,
 │ attempts += 1    │             last_delivery_error = 'Handler Processing … Timed Out'
 │◄─────────────────┤
 │                  │ permanent error OR attempts exhausted (§7)
 │                  ▼
 │             DEAD LETTER (is_dead_letter=T, next_delivery_ts=NULL, is_being_delivered=F)
 └── resurrect ─────┘  next_delivery_ts = now + d, is_dead_letter = F (counters NOT reset)
```

## 4. Claiming (fetch)

### 4.1 Unordered claim (verbatim)

```sql
WITH cte_unordered AS (
  SELECT id
  FROM {table} q
  WHERE queue_name = :queueName
    AND is_dead_letter_message = FALSE
    AND is_being_delivered     = FALSE
    AND next_delivery_ts <= :now
    AND key IS NULL
  ORDER BY next_delivery_ts
  LIMIT :limit
  FOR UPDATE SKIP LOCKED
)
UPDATE {table} q
SET total_attempts     = q.total_attempts + 1,
    next_delivery_ts   = NULL,
    is_being_delivered = TRUE,
    delivery_ts        = :now
FROM cte_unordered u
WHERE q.id = u.id
  AND q.queue_name = :queueName
RETURNING q.*;
```

### 4.2 Ordered claim (verbatim)

```sql
WITH cte_ordered AS (
  SELECT id
  FROM {table} q
  WHERE queue_name             = :queueName
    AND is_dead_letter_message = FALSE
    AND is_being_delivered     = FALSE
    AND next_delivery_ts      <= :now
    AND key IS NOT NULL
    -- full per-key barrier:
    AND NOT EXISTS (
      SELECT 1 FROM {table} q2
      WHERE q2.key        = q.key
        AND q2.queue_name = q.queue_name
        AND q2.key_order  < q.key_order
    )
    AND key NOT IN (<excludeKeys>)        -- only present when the exclusion list is non-empty
  ORDER BY key_order, next_delivery_ts
  LIMIT :limit
  FOR UPDATE SKIP LOCKED
)
UPDATE {table} q
SET total_attempts = q.total_attempts + 1, next_delivery_ts = NULL,
    is_being_delivered = TRUE, delivery_ts = :now
FROM cte_ordered o
WHERE q.id = o.id AND q.queue_name = :queueName
RETURNING q.*;
```

The barrier matches *any* row with a lower `key_order`: being delivered, waiting for a retry, **or a dead letter**.
So a dead-lettered ordered message blocks its key until it is resurrected or deleted. This is intended
(`DurableQueuesIT.verify_a_that_as_long_as_an_ordered_message_with_same_key_and_a_lower_key_order_exists_as_a_dead_letter_message…`).

`excludeKeys` holds the keys this node is currently processing (`CentralizedMessageFetcher.inProcessOrderedKeys`).
This is the "per node" part of ordering.

### 4.3 How Java drives the claims (default, centralized fetcher)

`PostgresqlDurableQueues.fetchNextBatchOfMessages` runs every 20 ms by default, **in a single transaction**:

1. `resetMessagesStuckBeingDeliveredAcrossMultipleQueues(queues)` (§6)
2. `now = Instant.now()`
3. For each consumed queue with free worker slots (`limit = free slots`), unless the polling optimizer says skip:
   - run the ordered claim (4.2) with `limit`
   - **only if it returned nothing**, run the unordered claim (4.1) with `limit`
4. Commit, then dispatch the messages to the worker threads.

A row that fails to deserialize is dead-lettered with `markAsDeadLetterMessageDirect` and is not dispatched
(`handleFailedMappings`). The opt-in `useBatchedFetch` path (`buildBatchedSqlStatement`) is disabled by default and
**not part of the Rust scope**. `getQueueMessageSqlOptimized` is never called.

The traditional consumer (`getNextMessageReadyForDelivery`) does the same with `limit = 1`, plus a per-queue stuck
reset (§6).

**Rust (Phase 5):** `QueueStorage::claim` runs the same sequence in one transaction: stuck reset, the ordered claim
excluding this consumer's in-flight keys, then the unordered claim only if the ordered one returned nothing. The
in-flight key set is per consumer, as Java's is per node. The key exclusion is written `AND NOT (key = ANY($4))`,
which also covers the empty list Java leaves out.

**What carries the ordering guarantee (verified by mutation tests):**
- **The per-key barrier (`NOT EXISTS` a lower `key_order`)** keeps a key's messages in sequence. A later order
  cannot be claimed while any earlier row of the key exists, whether it is being delivered, waiting for a retry, or
  a dead letter. Because the barrier lives in the database, this also holds across nodes.
- **The in-flight key exclusion** matters when a handler outlives `messageHandlingTimeout`. Stuck-message recovery
  then returns the head row to the queue while its handler still runs, and the barrier does not stop a head. The
  exclusion keeps that node from claiming it again and running the key twice at once. Another node does not have
  the key in its exclusion list, so it can redeliver the head. That is a per-key duplicate across nodes, and it is
  why Java documents ordering as per node unless a single global consumer is used.

## 5. Handler outcome → SQL (each in its own transaction, after the claim transaction committed)

From `DefaultDurableQueueConsumer.handleMessage` and the operations in `PostgresqlDurableQueues`. The centralized
consumer behaves the same way.

| Outcome | Statement |
|---|---|
| Success | `DELETE FROM {table} WHERE id = :id AND is_dead_letter_message = FALSE`. If 0 rows were deleted and the row exists as a dead letter, it was dead-lettered during handling, which counts as acknowledged. Otherwise an error is logged and `false` returned. |
| Handler requested redelivery (`markForRedeliveryIn`) | the retry statement, with the delay the handler asked for and `last_delivery_error = MANUALLY_REQUESTED_REDELIVERY` |
| Failure → retry | `UPDATE {table} SET next_delivery_ts = :next, last_delivery_error = :stackTrace, redelivery_attempts = redelivery_attempts + 1, is_being_delivered = FALSE, delivery_ts = NULL WHERE id = :id RETURNING *` with `:next = Instant.now() + policy.calculateNextRedeliveryDelay(row.redelivery_attempts)`. The *pre*-increment value is used. |
| Failure → dead letter | `UPDATE {table} SET next_delivery_ts = NULL, last_delivery_error = :err, is_dead_letter_message = TRUE, is_being_delivered = FALSE, delivery_ts = NULL WHERE id = :id AND is_dead_letter_message = FALSE RETURNING *`. The "Direct" variant is the same without `RETURNING`. |

Neither ack nor retry checks `is_being_delivered`. A handler that outlived the timeout still deletes the row on
success, even if another node redelivered it in the meantime: an accepted duplicate.

Other operations:

| Operation | SQL |
|---|---|
| Resurrect | `UPDATE … SET next_delivery_ts = :now+d, is_dead_letter_message = FALSE WHERE id = :id AND is_dead_letter_message = TRUE RETURNING *`. The counters are **not** reset, so the next failure dead-letters again immediately if `total_attempts` is already over the cap. |
| Get dead letters | Java: `SELECT * … WHERE queue_name = :q AND is_dead_letter_message = TRUE LIMIT :pageSize OFFSET :offset`. Its `QueueingSortOrder` parameter is **ignored** and there is no `ORDER BY`, so pages are nondeterministic. Rust orders by `added_ts, id` in the requested direction (read-only, no interop impact). |
| Get by id | `SELECT * FROM {table} WHERE id = :id AND is_dead_letter_message = :isDeadLetterMessage` |
| Delete | `DELETE FROM {table} WHERE id = :id` |
| Purge | `DELETE FROM {table} WHERE queue_name = :queueName` |
| Counts | `SELECT count(*) … WHERE queue_name = :q AND is_dead_letter_message = FALSE` (or `TRUE` for dead letters) |

## 6. Stuck-message reset (crash recovery)

```sql
UPDATE {table} SET
  is_being_delivered  = FALSE,
  delivery_ts         = NULL,
  redelivery_attempts = redelivery_attempts + 1,
  next_delivery_ts    = :now,
  last_delivery_error = :error          -- 'Handler Processing of the Message was determined to have Timed Out'
WHERE is_being_delivered = TRUE
  AND delivery_ts <= :threshold         -- :threshold = now - messageHandlingTimeout (default 30 s)
  AND queue_name IN (<queueNames>)      -- multi-queue variant (centralized fetcher) only
```

- **Throttle:** this runs at most once per `messageHandlingTimeout` per queue name, tracked in memory per instance
  (`lastResetStuckMessagesCheckTimestamps`).
- **Quirk:** the single-queue variant used by the traditional consumer has **no `queue_name` filter**, so it resets
  stuck rows of *every* queue in the table. Rust uses the multi-queue variant only.
- `redelivery_attempts` grows, but `total_attempts` does not (only claiming increments it). The redelivery cap
  (§7) counts `total_attempts`, so a reset message still moves towards the dead-letter limit through its next
  claim.
- Worst-case recovery latency after a crash is about `timeout` (row age) plus `timeout` (throttle) = **≤ 2 ×
  messageHandlingTimeout**.

## 7. Retry vs dead-letter decision

`MessageDeliveryClassifier.classify(queuedMessage, error, policy)`. `attempts` is `total_attempts` **as returned by
the claim**, so it already includes the current delivery.

1. If `policy.verdict(msg, error) == PERMANENT_ERROR` (the policy's `MessageDeliveryErrorHandler`) → dead letter.
2. Else, walk the cause chain (max depth 100, cycle-safe) looking for a **built-in permanent error**:
   - `DurableQueueDeserializationException`, `tools.jackson.databind.exc.MismatchedInputException` (or a
     subclass), `NoClassDefFoundError` → dead letter. Cannot be overridden.
   - `IllegalArgumentException` (and subclasses), `ClassCastException` → dead letter, **unless** the verdict is
     `RETRY` (`alwaysRetryOn`), in which case go to step 3.
3. **Redelivery cap:** if `attempts >= maximumNumberOfRedeliveries + 1` → dead letter (`REDELIVERIES_EXHAUSTED`),
   otherwise retry.

So `maximumNumberOfRedeliveries = N` means at most **N + 1 total deliveries**. The final failure dead-letters; it
is not retried.

**Rust mapping:**

- Handlers return `Result<(), HandlerError>` where `HandlerError` is `Transient(e)` or `Permanent(e)`.
- Payload/metadata deserialization failure is always permanent and handled at fetch time with the Direct
  dead-letter statement, like Java.
- The Java exception classes have no Rust counterpart. A Rust handler marks the error `Permanent` itself.
- `last_delivery_error` gets the error's `Display` plus its `source()` chain, one cause per line. Rust does not
  imitate the Java stack-trace format.

## 8. Redelivery delay

`RedeliveryPolicy.calculateNextRedeliveryDelay(n)`, where `n = redelivery_attempts` read at claim time (0 after
the first failure):

| | n = 0 | n ≥ 1 | cap |
|---|---|---|---|
| fixed(`delay`) | `delay` | `delay` | `maximumFollowupRedeliveryThreshold` |
| linear(`delay`, `max`) | `delay` | `delay + delay × n` = `delay × (n+1)` | `max` |
| exponential(`initial`, `followup`, `m`, `max`) | `initial` | `followup × m^(n−1)`; `m ≤ 1` or NaN means 1 | `max` |

The cap applies with `>=`: if the computed delay is at least the threshold, the threshold is returned. Computation
is done in `double` nanoseconds. Worked example `exponential(500ms, 500ms, 2.0, 1min, 8)` → 500ms, 500ms, 1s, 2s,
4s, 8s, 16s, 32s.

## 9. Table-name validation (`PostgresqlUtil.checkIsValidTableOrColumnName`)

- Must not be null or blank.
- Unqualified: matches `^[A-Za-z_][A-Za-z0-9_]{0,62}$` (≤ 63 chars) and is **not** in `RESERVED_NAMES` (case
  insensitive). That list holds PG data types, SQL keywords, etc.; port it from `PostgresqlUtil.RESERVED_NAMES`.
- Qualified (`schema.table`): each part is validated the same way; total length ≤ 127.
- Java lower-cases the name before use. Rust does the same.

## 10. Consumer runtime behaviour (non-SQL)

- **Polling:** the centralized fetcher polls every 20 ms by default. An optional optimizer (exponential backoff +
  jitter) is only active with LISTEN/NOTIFY. Rust Phase 3 uses a simple adaptive backoff; LISTEN/NOTIFY is out of
  scope.
- **Concurrency:** `parallelConsumers` worker slots per queue. The fetcher claims at most the number of free slots,
  so claimed messages never sit in a local buffer, where they would age towards the stuck timeout.
- **Ordered keys:** a key being handled on this node is excluded from the next ordered claim (`excludeKeys`).
- **Shutdown:** stop fetching, then stop the workers. Messages in flight at a hard stop are recovered by §6.
- **Transactions:** the claim, the ack/retry/dead-letter, and the handler's own work are **separate
  transactions**. A crash between the handler committing and the ack produces a redelivery (at-least-once).
  `queueMessage` inside an existing unit of work joins it (Outbox pattern).

## 11. Corrections to the bundled LLM docs / original plan

| Docs/plan said | Code says |
|---|---|
| column `last_error` | `last_delivery_error` |
| `next_delivery_ts … NOT NULL` | nullable (NULL while delivering and for dead letters) |
| `delivery_mode` nullable | `NOT NULL` |
| `key_order` NULL for normal messages | `-1` |
| linear `delay × (n+1)` | `initial + followup × n` (the same as long as linear uses one delay for both) |
| stuck reset scoped to a queue | the single-queue variant is **not** scoped (quirk, §6) |

## 12. Interop hazards to test in Phase 6

1. **Unknown payload types are dead-lettered, not skipped.** On a queue with Java *and* Rust consumers, every
   consumer must understand every `message_payload_type` on that queue. Otherwise the node that claims the message
   dead-letters it. This is true in pure Java as well (class missing on one node). Mixed queues need the same type
   registry on both sides.
2. **Rust payloads must deserialize in Jackson.** A Rust producer must write `message_payload_type` = a Java FQCN
   the Java consumer has, and JSON that Jackson 3 + Essentials' mapper accepts (field names, `SingleValueType`s
   serialized as bare strings or numbers, `Instant`s as ISO-8601 strings).
3. **Clock skew** (§2): resets and delayed delivery depend on each node's clock.
4. **Ordered rows** are never claimed by Rust before Phase 5 (§4.3).

## 13. Golden fixtures

There is no JDK in this devcontainer, so the fixtures in [`fixtures/java_rows.sql`](fixtures/java_rows.sql) are
**synthetic**: they are written to match exactly what `queueMessage` / `queueMessages` insert (§2) and are used by
the Phase 2 compatibility tests. Phase 6 replaces them with rows dumped from the real Java harness and diffs the
two. A mismatch is a contract bug.

## 14. Java test checklist (Phase 4 porting source)

From `foundation-test/.../messaging/queue/`:

| Test (Java) | Rust phase |
|---|---|
| `DurableQueuesIT.test_simple_enqueueing_and_afterwards_querying_queued_messages` | 2 |
| `DurableQueuesIT.verify_queued_messages_are_dequeued_in_order` | 2 |
| `DurableQueuesIT.verify_a_message_queues_as_a_dead_letter_message_is_marked_as_such_and_will_not_be_delivered_to_the_consumer` | 3 |
| `DurableQueuesIT.verify_failed_messages_are_redelivered` | 3 |
| `DurableQueuesIT.verify_a_message_that_failed_too_many_times_is_marked_as_dead_letter_message_AND_the_message_can_be_resurrected` | 3 |
| `DurableQueuesIT.test_two_stage_redelivery_where_a_message_about_to_be_marked_as_a_deadletter_message_is_queued_with_a_redelivery_delay` | 3 |
| `DurableQueuesIT.verify_a_message_can_manually_be_marked_as_dead_letter_message_AND_the_message_can_afterwards_be_resurrected` | 3 |
| `DurableQueuesIT.test_messagehandler_with_call_to_markForRedeliveryIn` | 3 |
| `DurableQueuesIT.verify_json_deserialization_problem_causes_message_to_be_marked_as_dead_letter_message` | 3 |
| `DurableQueuesIT.verify_deserialization_failure_in_one_queue_does_not_block_other_queues` | 3 |
| `DurableQueuesIT.verify_a_that_as_long_as_an_ordered_message_with_same_key_and_a_lower_key_order_exists_as_a_dead_letter_message…` | 5 |
| `DurableQueuesIT.verify_hasOrderedMessageQueuedForKey` | 5 |
| `LocalCompetingConsumersDurableQueueIT.verify_queued_messages_are_dequeued_in_order` | 2 |
| `DistributedCompetingConsumersDurableQueuesIT.verify_queued_messages_are_dequeued_in_order` (+ `_with_db_connectivity_issues`) | 2 / 4 |
| `DuplicateConsumptionDurableQueuesIT.verify_no_duplicate_message_consumption` (+ `_with_db_connectivity_issues`) | 2 / 4 |
| `LocalOrderedMessagesDurableQueueIT.verify_queued_ordered_messages_are_dequeued_in_order_per_key` | 5 |
| `LocalOrderedMessagesRedeliveryDurableQueueIT.verify_queued_ordered_messages_are_dequeued_in_order_per_key_even_if_some_messages_are_redelivered` | 5 |
| `DurableQueuesLoadIT.queue_a_large_number_of_messages` | 4 |

## 15. Open decisions (for review)

Phase 1 proceeded on the recommendations below (ordered messages as Phase 5, Java's resurrect behaviour, UUID v7).
Revisit them at the Phase 0 review.

1. **Ordered messages in scope?** Recommendation: **yes, as Phase 5**. The SQL is already captured (§4.2), and
   without it a Rust node cannot fully replace a Java node on a mixed queue.
2. **Resurrect does not reset counters** (§5). Keep Java's behaviour (recommended, for interop) or reset in Rust?
3. **ID format:** Rust writes UUID v7 strings, which are time-ordered like Java's v1 default and valid opaque TEXT.
   Any objection?
