//! Phase 5: ordered messages. The Java ports keep Java's numbers; the exit-gate test runs 10 parallel consumers.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{TestDatabase, eventually};
use durable_queues_rs::{
    ConsumerConfig, DeliveryMode, DurableQueues, DurableQueuesError, HandlerError, Message,
    MessageOrder, MessageType, QueueEntryId, QueueName, QueueStorage, QueuedMessage,
    QueueingSortOrder, RedeliveryPolicy,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Event {
    label: String,
}

impl MessageType for Event {
    const TYPE_NAME: &'static str = "com.acme.Event";
}

fn event(label: impl Into<String>) -> Event {
    Event {
        label: label.into(),
    }
}

fn fixed(delay_ms: u64, max_redeliveries: u32) -> RedeliveryPolicy {
    RedeliveryPolicy::fixed_backoff(Duration::from_millis(delay_ms), max_redeliveries)
}

/// Orders recorded per key, in the sequence the handler saw them.
type PerKey = Arc<Mutex<HashMap<String, Vec<i64>>>>;

fn record(per_key: &PerKey, message: &QueuedMessage<Event>) {
    let order = message.message.order.as_ref().expect("an ordered message");
    per_key
        .lock()
        .unwrap()
        .entry(order.key.clone())
        .or_default()
        .push(order.order);
}

fn assert_in_order_per_key(per_key: &PerKey, expected_per_key: &HashMap<String, i64>) {
    let per_key = per_key.lock().unwrap();
    assert_eq!(per_key.len(), expected_per_key.len(), "every key delivered");
    for (key, count) in expected_per_key {
        let expected: Vec<i64> = (0..*count).collect();
        assert_eq!(
            per_key[key], expected,
            "key {key}: each order exactly once, in order"
        );
    }
}

/// Java's `createOrderedMessages`: `count` messages round-robin over `keys` keys, each key's orders 0, 1, 2, ...
fn round_robin(count: usize, keys: usize) -> (Vec<Message<Event>>, HashMap<String, i64>) {
    let mut next_order: HashMap<String, i64> = HashMap::new();
    let messages = (0..count)
        .map(|i| {
            let key = format!("account-{}", i % keys);
            let order = next_order.entry(key.clone()).or_insert(0);
            let message = Message::new(event(format!("message-{i}"))).in_order(key, *order);
            *order += 1;
            message
        })
        .collect();
    (messages, next_order)
}

async fn queue_empty(db: &TestDatabase) -> bool {
    db.count("SELECT count(*) FROM durable_queues").await == 0
}

#[tokio::test]
async fn ordered_messages_are_stored_like_java_and_invalid_orders_are_rejected() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("Orders");

    let id = queues
        .queue_message(&queue, &Message::new(event("a")).in_order("order-1", 7))
        .await
        .unwrap();
    let row = queues.storage().find_entry(&id).await.unwrap().unwrap();
    assert_eq!(
        (row.delivery_mode, row.key.as_deref(), row.key_order),
        (DeliveryMode::InOrder, Some("order-1"), 7)
    );

    let batch = [
        Message::new(event("ok")).in_order("order-2", 0),
        Message::new(event("bad")).in_order("order-2", -1),
    ];
    let rejected = queues.queue_messages(&queue, &batch).await;
    assert!(
        matches!(rejected, Err(DurableQueuesError::InvalidMessage(_))),
        "{rejected:?}"
    );
    assert_eq!(
        db.count("SELECT count(*) FROM durable_queues").await,
        1,
        "nothing of the rejected batch is queued"
    );
}

// --- PostgresqlDurableQueuesMixedBatchIT -----------------------------------------------------------------------

async fn mixed_batch(queue: &str, batch: Vec<Message<Event>>, expected_ordered: usize) {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from(queue);
    let mut tx = db.pool.begin().await.unwrap();
    let ids = queues
        .queue_messages_in(&mut tx, &queue, &batch)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(ids.len(), 5);

    let mut ordered = 0;
    for (id, message) in ids.iter().zip(&batch) {
        let row = queues.storage().find_entry(id).await.unwrap().unwrap();
        match &message.order {
            Some(MessageOrder { key, order }) => {
                ordered += 1;
                assert_eq!(
                    (row.delivery_mode, row.key.as_ref(), row.key_order),
                    (DeliveryMode::InOrder, Some(key), *order)
                );
            }
            None => assert_eq!(
                (row.delivery_mode, row.key, row.key_order),
                (DeliveryMode::Normal, None, -1)
            ),
        }
    }
    assert_eq!(ordered, expected_ordered);

    // Not in Java's test: the mixed queue drains, ordered and normal messages alike.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = {
        let seen = seen.clone();
        move |m: QueuedMessage<Event>| {
            seen.lock().unwrap().push(m.message.payload.label);
            async { Ok(()) }
        }
    };
    let consumer = queues.consume_from_queue(
        ConsumerConfig::new(queue, fixed(10, 3)).with_parallel_consumers(4),
        handler,
    );
    let consumer = consumer.unwrap();
    eventually("the mixed batch to be consumed", || queue_empty(&db)).await;
    consumer.stop().await;
    assert_eq!(seen.lock().unwrap().len(), 5);
}

#[tokio::test]
async fn a_batch_starting_with_an_ordered_message_can_also_contain_unordered_messages() {
    let batch = vec![
        Message::new(event("ord-a-0")).in_order("key-a", 0),
        Message::new(event("unord-1")),
        Message::new(event("ord-a-1")).in_order("key-a", 1),
        Message::new(event("unord-2")),
        Message::new(event("ord-b-0")).in_order("key-b", 0),
    ];
    mixed_batch("MixedBatchOrderedFirst", batch, 3).await;
}

#[tokio::test]
async fn a_batch_starting_with_an_unordered_message_can_also_contain_ordered_messages() {
    let batch = vec![
        Message::new(event("unord-1")),
        Message::new(event("ord-a-0")).in_order("key-a", 0),
        Message::new(event("unord-2")),
        Message::new(event("ord-b-0")).in_order("key-b", 0),
        Message::new(event("unord-3")),
    ];
    mixed_batch("MixedBatchUnorderedFirst", batch, 2).await;
}

// --- DurableQueuesIT -----------------------------------------------------------------------------------------

#[tokio::test]
async fn verify_has_ordered_message_queued_for_key() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let (test_queue, other_queue) = (
        QueueName::from("TestQueue"),
        QueueName::from("OtherQueueName"),
    );
    let has = |queues: DurableQueues| async move {
        let mut found = Vec::new();
        for queue in ["TestQueue", "OtherQueueName"] {
            for key in ["Key1", "Key2", "NonExistentKey"] {
                if queues
                    .has_ordered_message_queued_for_key(&queue.into(), key)
                    .await
                    .unwrap()
                {
                    found.push(format!("{queue}/{key}"));
                }
            }
        }
        found
    };
    assert!(has(queues.clone()).await.is_empty());

    queues
        .queue_message(
            &test_queue,
            &Message::new(event("Message1")).in_order("Key1", 0),
        )
        .await
        .unwrap();
    assert_eq!(has(queues.clone()).await, ["TestQueue/Key1"]);
    queues
        .queue_message(
            &test_queue,
            &Message::new(event("Message2")).in_order("Key2", 0),
        )
        .await
        .unwrap();
    assert_eq!(
        has(queues.clone()).await,
        ["TestQueue/Key1", "TestQueue/Key2"]
    );

    // Java acknowledges both by hand; consuming them does the same.
    let consumer = queues
        .consume_from_queue(
            ConsumerConfig::new(test_queue.clone(), fixed(10, 3)),
            |_: QueuedMessage<Event>| async { Ok(()) },
        )
        .unwrap();
    eventually("both to be handled", || queue_empty(&db)).await;
    consumer.stop().await;
    assert!(has(queues.clone()).await.is_empty());

    // Dead letters count. Java queues one directly as a dead letter; queue-then-mark is the same row.
    let id = queues
        .queue_message_with_delay(
            &test_queue,
            &Message::new(event("DeadLetter")).in_order("Key1", 1),
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
    queues
        .mark_as_dead_letter_message(&id, "On purpose")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(has(queues.clone()).await, ["TestQueue/Key1"]);
    assert!(
        !queues
            .has_ordered_message_queued_for_key(&other_queue, "Key1")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn verify_that_an_ordered_dead_letter_blocks_its_key_until_it_is_resurrected() {
    // DurableQueuesIT.verify_a_that_as_long_as_an_ordered_message_with_same_key_and_a_lower_key_order_exists_as_a_
    // dead_letter_message_then_no_further_messages_with_the_same_key_will_be_delivered
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("TestQueue");
    let mut ids = HashMap::new();
    for key in ["Key1", "Key2"] {
        for order in 0..5 {
            let label = format!("{key}Msg{}", order + 1);
            let message = Message::new(event(label.clone())).in_order(key, order);
            // Delayed so Key1Msg3 is a dead letter before anything is delivered (Java queues it as one directly).
            ids.insert(
                label,
                queues
                    .queue_message_with_delay(&queue, &message, Duration::from_millis(500))
                    .await
                    .unwrap(),
            );
        }
    }
    queues
        .mark_as_dead_letter_message(&ids["Key1Msg3"], "On purpose")
        .await
        .unwrap()
        .unwrap();

    let delivered = Arc::new(Mutex::new(Vec::new()));
    let handler = {
        let delivered = delivered.clone();
        move |m: QueuedMessage<Event>| {
            delivered.lock().unwrap().push(m.message.payload.label);
            async { Ok(()) }
        }
    };
    let config = ConsumerConfig::new(queue.clone(), fixed(200, 1)).with_parallel_consumers(2);
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    eventually("everything but the blocked key to be delivered", || async {
        delivered.lock().unwrap().len() == 7
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let first_phase: HashSet<String> = std::mem::take(&mut *delivered.lock().unwrap())
        .into_iter()
        .collect();
    let expected: HashSet<String> = [
        "Key1Msg1", "Key1Msg2", "Key2Msg1", "Key2Msg2", "Key2Msg3", "Key2Msg4", "Key2Msg5",
    ]
    .map(String::from)
    .into();
    assert_eq!(
        first_phase, expected,
        "Key1Msg4 and Key1Msg5 wait behind the dead letter"
    );

    queues
        .resurrect_dead_letter_message(&ids["Key1Msg3"], Duration::from_millis(10))
        .await
        .unwrap()
        .unwrap();
    eventually("the rest of Key1 to be delivered", || async {
        delivered.lock().unwrap().len() == 3
    })
    .await;
    consumer.stop().await;
    assert_eq!(
        *delivered.lock().unwrap(),
        ["Key1Msg3", "Key1Msg4", "Key1Msg5"],
        "in key order"
    );
    assert!(
        queues
            .get_dead_letter_messages(&queue, QueueingSortOrder::Ascending, 0, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

// --- Exit gate: 10 parallel consumers, a failing head blocks only its own key ---------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_retried_head_holds_back_its_key_while_other_keys_proceed_under_10_parallel_consumers() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("Orders");
    let mut batch = Vec::new();
    for key in ["slow", "fast"] {
        for order in 0..5 {
            batch.push(Message::new(event(format!("{key}-{order}"))).in_order(key, order));
        }
    }
    queues.queue_messages(&queue, &batch).await.unwrap();

    // `slow`'s head fails twice, 300 ms apart; everything else succeeds at once.
    let handled = Arc::new(Mutex::new(Vec::new()));
    let handler = {
        let handled = handled.clone();
        move |m: QueuedMessage<Event>| {
            let fails = m.message.payload.label == "slow-0" && m.total_attempts <= 2;
            if !fails {
                handled
                    .lock()
                    .unwrap()
                    .push(m.message.payload.label.clone());
            }
            async move {
                match fails {
                    true => Err(HandlerError::transient("head not ready yet")),
                    false => Ok(()),
                }
            }
        }
    };
    let config = ConsumerConfig::new(queue, fixed(300, 5)).with_parallel_consumers(10);
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    eventually("both keys to be handled", || queue_empty(&db)).await;
    consumer.stop().await;

    let handled = handled.lock().unwrap();
    let position = |label: &str| handled.iter().position(|l| l == label).unwrap();
    let slow: Vec<_> = handled
        .iter()
        .filter(|l| l.starts_with("slow"))
        .cloned()
        .collect();
    let fast: Vec<_> = handled
        .iter()
        .filter(|l| l.starts_with("fast"))
        .cloned()
        .collect();
    assert_eq!(
        slow,
        (0..5).map(|o| format!("slow-{o}")).collect::<Vec<_>>()
    );
    assert_eq!(
        fast,
        (0..5).map(|o| format!("fast-{o}")).collect::<Vec<_>>()
    );
    assert!(
        position("fast-4") < position("slow-0"),
        "the other key is not held back by the retrying head: {handled:?}"
    );
}

// --- LocalOrderedMessagesDurableQueueIT / …RedeliveryDurableQueueIT / CentralizedFetcherDurableQueueIT --------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn verify_queued_ordered_messages_are_dequeued_in_order_per_key() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.node_pool(20).await).await;
    let queue = QueueName::from("TestQueue");
    let (messages, per_key_count) = round_robin(2000, 45);
    queues.queue_messages(&queue, &messages).await.unwrap();

    let per_key = PerKey::default();
    let handler = {
        let per_key = per_key.clone();
        move |m: QueuedMessage<Event>| {
            record(&per_key, &m);
            async { Ok(()) }
        }
    };
    let config = ConsumerConfig::new(queue, fixed(1, 5)).with_parallel_consumers(20);
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    eventually("2000 ordered messages to be handled", || queue_empty(&db)).await;
    tokio::time::sleep(Duration::from_millis(1990)).await;
    consumer.stop().await;

    assert_in_order_per_key(&per_key, &per_key_count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn verify_queued_ordered_messages_are_dequeued_in_order_per_key_even_if_some_messages_are_redelivered()
 {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.node_pool(20).await).await;
    let queue = QueueName::from("TestQueue");
    let (messages, per_key_count) = round_robin(2000, 45);
    queues.queue_messages(&queue, &messages).await.unwrap();

    // Java's rule: fail while the delivery count trails a global counter, until 5 redeliveries - so most messages
    // are redelivered a few times, none is dead-lettered, and order must survive the retries.
    let per_key = PerKey::default();
    let counter = Arc::new(AtomicUsize::new(0));
    let handler = {
        let (per_key, counter) = (per_key.clone(), counter.clone());
        move |m: QueuedMessage<Event>| {
            let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
            let fail = (m.total_attempts as usize) < count && m.redelivery_attempts < 5;
            if !fail {
                record(&per_key, &m);
            }
            async move {
                match fail {
                    true => Err(HandlerError::transient("Thrown on purpose")),
                    false => Ok(()),
                }
            }
        }
    };
    let config = ConsumerConfig::new(queue, fixed(1, 5)).with_parallel_consumers(20);
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    eventually("2000 ordered messages to be handled", || queue_empty(&db)).await;
    consumer.stop().await;

    assert!(
        counter.load(Ordering::SeqCst) > 2000,
        "messages were redelivered"
    );
    assert_eq!(
        db.count("SELECT count(*) FROM durable_queues WHERE is_dead_letter_message")
            .await,
        0
    );
    assert_in_order_per_key(&per_key, &per_key_count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn verify_ordered_messages_are_processed_in_order() {
    // CentralizedFetcherDurableQueueIT: 10 keys x 20 orders, queued shuffled, 15 parallel, 10 ms per message.
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.node_pool(21).await).await;
    let queue = QueueName::from("OrderedQueue");
    let mut messages: Vec<_> = (0..10)
        .flat_map(|k| {
            (0..20).map(move |o| {
                Message::new(event(format!("Message key-{k}:{o}"))).in_order(format!("key-{k}"), o)
            })
        })
        .collect();
    fastrand::shuffle(&mut messages);
    queues.queue_messages(&queue, &messages).await.unwrap();

    let per_key = PerKey::default();
    let handler = {
        let per_key = per_key.clone();
        move |m: QueuedMessage<Event>| {
            record(&per_key, &m);
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(())
            }
        }
    };
    let config = ConsumerConfig::new(queue, fixed(10, 3)).with_parallel_consumers(15);
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    eventually("200 ordered messages to be handled", || queue_empty(&db)).await;
    consumer.stop().await;

    let expected: HashMap<String, i64> = (0..10).map(|k| (format!("key-{k}"), 20)).collect();
    assert_in_order_per_key(&per_key, &expected);
}

#[tokio::test]
async fn a_node_never_handles_two_messages_of_one_key_at_once() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let batch: Vec<_> = (0..10)
        .map(|o| Message::new(event(format!("m{o}"))).in_order("only-key", o))
        .collect();
    let ids: Vec<QueueEntryId> = queues
        .queue_messages(&"Orders".into(), &batch)
        .await
        .unwrap();
    assert_eq!(ids.len(), 10);

    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let handler = {
        let (running, peak) = (running.clone(), peak.clone());
        move |_: QueuedMessage<Event>| {
            let (running, peak) = (running.clone(), peak.clone());
            async move {
                peak.fetch_max(running.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(30)).await;
                running.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        }
    };
    let config = ConsumerConfig::new("Orders", fixed(10, 3)).with_parallel_consumers(10);
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    eventually("the key to be drained", || queue_empty(&db)).await;
    consumer.stop().await;
    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "10 free slots, but one key is handled one message at a time"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordered_message_outliving_the_timeout_is_not_redelivered_while_its_key_is_in_flight() {
    // Stuck-message recovery returns the slow head to the queue after 1 s, while its handler still runs. The barrier
    // does not stop that row - it is the head - so only the in-flight key exclusion keeps this node from handling
    // the key twice at once (the plain-message equivalent in edge_cases.rs is delivered twice).
    let db = TestDatabase::start().await;
    let queues = db
        .durable_queues(db.pool.clone())
        .await
        .with_message_handling_timeout(Duration::from_secs(1));
    queues
        .queue_message(
            &"Orders".into(),
            &Message::new(event("slow")).in_order("k", 0),
        )
        .await
        .unwrap();

    let deliveries = Arc::new(AtomicUsize::new(0));
    let handler = {
        let deliveries = deliveries.clone();
        move |_: QueuedMessage<Event>| {
            deliveries.fetch_add(1, Ordering::SeqCst);
            async {
                tokio::time::sleep(Duration::from_millis(3000)).await;
                Ok(())
            }
        }
    };
    let config = ConsumerConfig::new("Orders", fixed(10, 3)).with_parallel_consumers(2);
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    eventually("the slow message to be handled", || queue_empty(&db)).await;
    consumer.stop().await;

    assert_eq!(
        deliveries.load(Ordering::SeqCst),
        1,
        "never redelivered while its key was in flight on this node"
    );
}
