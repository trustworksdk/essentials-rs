//! Phase 4: Java's durable-queue integration tests, ported. Each test names its Java original and keeps its numbers
//! (messages, policies, parallelism, timeouts); deviations are noted inline. The full mapping, including the Java
//! tests that are not ported and why, is in `docs/JAVA_TEST_CHECKLIST.md`.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{TestDatabase, eventually};
use durable_queues_rs::{
    ConsumerConfig, DecodeError, DurableQueues, HandlerError, MANUALLY_REQUESTED_REDELIVERY,
    Message, MessageHandler, QueueEntryId, QueueMessage, QueueName, QueueStorage, QueuedMessage,
    QueueingSortOrder, RedeliveryPolicy,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc::{self, UnboundedReceiver};

// --- Java's test_data: OrderEvent.OrderAdded / ProductAddedToOrder / OrderAccepted ------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderAdded {
    order_id: String,
    customer_id: String,
    order_number: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProductAddedToOrder {
    order_id: String,
    product_id: String,
    quantity: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderAccepted {
    order_id: String,
}

#[derive(Debug, Clone, PartialEq)]
enum OrderEvent {
    Added(OrderAdded),
    ProductAdded(ProductAddedToOrder),
    Accepted(OrderAccepted),
}

const ORDER_ADDED: &str = "dk.trustworks.essentials.components.foundation.test.messaging.queue.test_data.OrderEvent$OrderAdded";
const PRODUCT_ADDED: &str = "dk.trustworks.essentials.components.foundation.test.messaging.queue.test_data.OrderEvent$ProductAddedToOrder";
const ORDER_ACCEPTED: &str = "dk.trustworks.essentials.components.foundation.test.messaging.queue.test_data.OrderEvent$OrderAccepted";

impl QueueMessage for OrderEvent {
    fn payload_type(&self) -> &str {
        match self {
            Self::Added(_) => ORDER_ADDED,
            Self::ProductAdded(_) => PRODUCT_ADDED,
            Self::Accepted(_) => ORDER_ACCEPTED,
        }
    }

    fn to_payload(&self) -> serde_json::Result<Value> {
        match self {
            Self::Added(e) => serde_json::to_value(e),
            Self::ProductAdded(e) => serde_json::to_value(e),
            Self::Accepted(e) => serde_json::to_value(e),
        }
    }

    fn from_payload(payload_type: &str, payload: Value) -> Result<Self, DecodeError> {
        Ok(match payload_type {
            ORDER_ADDED => Self::Added(serde_json::from_value(payload)?),
            PRODUCT_ADDED => Self::ProductAdded(serde_json::from_value(payload)?),
            ORDER_ACCEPTED => Self::Accepted(serde_json::from_value(payload)?),
            other => return Err(DecodeError::UnknownPayloadType(other.to_owned())),
        })
    }
}

fn added(i: i64) -> OrderEvent {
    OrderEvent::Added(OrderAdded {
        order_id: format!("order-{i}"),
        customer_id: "customer-1".into(),
        order_number: i,
    })
}

/// Java's `msg1`, `msg2`, `msg3`: three payload types, the first two with metadata.
fn three_messages() -> [Message<OrderEvent>; 3] {
    [
        Message::new(added(1))
            .with_metadata("correlation_id", "corr-1")
            .with_metadata("trace_id", "trace-1"),
        Message::new(OrderEvent::ProductAdded(ProductAddedToOrder {
            order_id: "order-1".into(),
            product_id: "product-1".into(),
            quantity: 2,
        }))
        .with_metadata("correlation_id", "corr-1")
        .with_metadata("trace_id", "trace-2"),
        Message::new(OrderEvent::Accepted(OrderAccepted {
            order_id: "order-1".into(),
        })),
    ]
}

/// The competing-consumer tests' message mix: `i % 2 == 0` OrderAdded, else `i % 3 == 0` ProductAddedToOrder,
/// else OrderAccepted - all with metadata.
fn mixed_messages(count: usize) -> Vec<Message<OrderEvent>> {
    (0..count)
        .map(|i| {
            let payload = if i % 2 == 0 {
                added(i as i64)
            } else if i % 3 == 0 {
                OrderEvent::ProductAdded(ProductAddedToOrder {
                    order_id: format!("order-{i}"),
                    product_id: format!("product-{i}"),
                    quantity: i as i32,
                })
            } else {
                OrderEvent::Accepted(OrderAccepted {
                    order_id: format!("order-{i}"),
                })
            };
            Message::new(payload).with_metadata("correlation_id", format!("corr-{i}"))
        })
        .collect()
}

fn fixed(delay_ms: u64, max_redeliveries: u32) -> RedeliveryPolicy {
    RedeliveryPolicy::fixed_backoff(Duration::from_millis(delay_ms), max_redeliveries)
}

/// Java's `RecordingQueuedMessageHandler`: records each delivery, then settles it with `outcome`.
fn recording(
    outcome: impl Fn(&QueuedMessage<OrderEvent>) -> Result<(), HandlerError> + Send + Sync + 'static,
) -> (
    impl MessageHandler<OrderEvent>,
    UnboundedReceiver<QueuedMessage<OrderEvent>>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let handler = move |message: QueuedMessage<OrderEvent>| {
        let result = outcome(&message);
        sender.send(message).unwrap();
        async move { result }
    };
    (handler, receiver)
}

/// Waits until `count` deliveries were recorded, within `within`, and returns them.
async fn deliveries<M>(
    receiver: &mut UnboundedReceiver<QueuedMessage<M>>,
    count: usize,
    within: Duration,
) -> Vec<QueuedMessage<M>> {
    let mut received = Vec::with_capacity(count);
    tokio::time::timeout(within, async {
        while received.len() < count {
            received.push(receiver.recv().await.expect("handler alive"));
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected {count} deliveries within {within:?}"));
    received
}

async fn queue_is_empty(db: &TestDatabase) -> bool {
    db.count("SELECT count(*) FROM durable_queues").await == 0
}

async fn dead_letter(
    queues: &DurableQueues,
    queue: &QueueName,
) -> Vec<durable_queues_rs::QueueEntry> {
    queues
        .get_dead_letter_messages(queue, QueueingSortOrder::Ascending, 0, 20)
        .await
        .unwrap()
}

// --- DurableQueuesIT -----------------------------------------------------------------------------------------

#[tokio::test]
async fn verify_queued_messages_are_dequeued_in_order() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("TestQueue");
    let sent = three_messages();
    for message in &sent {
        queues.queue_message(&queue, message).await.unwrap();
    }

    let (handler, mut received) = recording(|_| Ok(()));
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new(queue.clone(), fixed(200, 5)), handler)
        .unwrap();
    let delivered = deliveries(&mut received, 3, Duration::from_secs(2)).await;
    eventually("the queue to be empty", || queue_is_empty(&db)).await;
    consumer.stop().await;

    let delivered: Vec<_> = delivered.into_iter().map(|d| d.message).collect();
    assert_eq!(
        delivered, sent,
        "delivered in queue order, payload and metadata intact"
    );
    assert!(dead_letter(&queues, &queue).await.is_empty());
}

#[tokio::test]
async fn verify_a_message_queued_as_a_dead_letter_message_is_not_delivered() {
    // Java queues it with queueMessageAsDeadLetterMessage, which is not ported; queueing then marking it before any
    // consumer runs produces the same row state.
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("TestQueue");
    let [message, ..] = three_messages();
    let id = queues.queue_message(&queue, &message).await.unwrap();
    queues
        .mark_as_dead_letter_message(&id, "On purpose")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        dead_letter(&queues, &queue)
            .await
            .iter()
            .map(|e| &e.id)
            .collect::<Vec<_>>(),
        [&id]
    );

    let (handler, mut received) = recording(|_| Ok(()));
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new(queue.clone(), fixed(200, 5)), handler)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1990)).await;
    consumer.stop().await;

    assert!(
        received.try_recv().is_err(),
        "a dead letter is never delivered"
    );
    assert_eq!(dead_letter(&queues, &queue).await.len(), 1);
}

#[tokio::test]
async fn verify_failed_messages_are_redelivered() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let [message, ..] = three_messages();
    queues
        .queue_message(&"TestQueue".into(), &message)
        .await
        .unwrap();

    let (handler, mut received) = recording(|m| match m.total_attempts {
        1..=3 => Err(HandlerError::transient("Thrown on purpose")),
        _ => Ok(()),
    });
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new("TestQueue", fixed(200, 5)), handler)
        .unwrap();
    let delivered = deliveries(&mut received, 4, Duration::from_secs(2)).await;
    eventually("the queue to be empty", || queue_is_empty(&db)).await;
    consumer.stop().await;

    assert!(delivered.iter().all(|d| d.message == message));
}

#[tokio::test]
async fn verify_a_message_that_failed_too_many_times_is_marked_as_dead_letter_message_and_can_be_resurrected()
 {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("TestQueue");
    let [message, ..] = three_messages();
    let id = queues.queue_message(&queue, &message).await.unwrap();

    let (handler, mut received) = recording(|m| match m.total_attempts {
        1..=6 => Err(HandlerError::transient("Thrown on purpose")),
        _ => Ok(()),
    });
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new(queue.clone(), fixed(200, 5)), handler)
        .unwrap();
    let delivered = deliveries(&mut received, 6, Duration::from_secs(4)).await;
    assert!(delivered.iter().all(|d| d.message == message));
    eventually("the message to be dead-lettered", || async {
        dead_letter(&queues, &queue).await.len() == 1
    })
    .await;

    let dead = queues.storage().find_entry(&id).await.unwrap().unwrap();
    assert!(dead.is_dead_letter_message);
    assert_eq!((dead.total_attempts, dead.redelivery_attempts), (6, 5));
    assert_eq!(dead_letter(&queues, &queue).await, [dead]);

    assert!(
        queues
            .resurrect_dead_letter_message(&id, Duration::from_millis(1000))
            .await
            .unwrap()
            .is_some()
    );
    let redelivered = deliveries(&mut received, 1, Duration::from_secs(4)).await;
    assert_eq!(redelivered[0].message, message);
    eventually("the queue to be empty", || queue_is_empty(&db)).await;
    consumer.stop().await;
}

#[tokio::test]
async fn verify_a_message_can_manually_be_marked_as_dead_letter_message_and_afterwards_be_resurrected()
 {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("TestQueue");
    let [message, ..] = three_messages();
    let id = queues.queue_message(&queue, &message).await.unwrap();

    // On its first delivery the handler dead-letters the message itself, then returns normally: the acknowledgement
    // finds a dead letter and counts it as handled, as in Java.
    let (sender, mut received) = mpsc::unbounded_channel::<QueuedMessage<OrderEvent>>();
    let handler = {
        let queues = queues.clone();
        move |m: QueuedMessage<OrderEvent>| {
            let (queues, sender) = (queues.clone(), sender.clone());
            async move {
                if m.total_attempts == 1 {
                    queues
                        .mark_as_dead_letter_message(&m.id, "Marked by the handler")
                        .await?;
                }
                sender.send(m).unwrap();
                Ok(())
            }
        }
    };
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new(queue.clone(), fixed(200, 5)), handler)
        .unwrap();
    deliveries(&mut received, 1, Duration::from_secs(4)).await;
    eventually("the message to be a dead letter", || async {
        dead_letter(&queues, &queue).await.len() == 1
    })
    .await;

    let dead = queues.storage().find_entry(&id).await.unwrap().unwrap();
    assert_eq!((dead.total_attempts, dead.redelivery_attempts), (1, 0));
    // Java passes no reason here and asserts a NULL error; the Rust API always takes one.
    assert_eq!(
        dead.last_delivery_error.as_deref(),
        Some("Marked by the handler")
    );

    assert!(
        queues
            .resurrect_dead_letter_message(&id, Duration::from_millis(1000))
            .await
            .unwrap()
            .is_some()
    );
    deliveries(&mut received, 1, Duration::from_secs(4)).await;
    eventually("the queue to be empty", || queue_is_empty(&db)).await;
    consumer.stop().await;
    assert!(dead_letter(&queues, &queue).await.is_empty());
}

#[tokio::test]
async fn test_messagehandler_with_call_to_mark_for_redelivery_in() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("TestQueue");
    let [message, ..] = three_messages();
    queues.queue_message(&queue, &message).await.unwrap();

    let (handler, mut received) = recording(|m| match m.total_attempts < 3 {
        true => Err(HandlerError::redeliver_in(Duration::from_millis(150))),
        false => Ok(()),
    });
    // Java uses fixedBackoff(200 ms, 5). A cap of 1 shows what Java's code does but its test does not pin: a
    // requested redelivery never consults the policy, so it never dead-letters.
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new(queue.clone(), fixed(200, 1)), handler)
        .unwrap();
    let delivered = deliveries(&mut received, 3, Duration::from_secs(2)).await;
    eventually("the queue to be empty", || queue_is_empty(&db)).await;
    consumer.stop().await;

    let errors: Vec<_> = delivered
        .iter()
        .map(|d| d.last_delivery_error.as_deref())
        .collect();
    assert_eq!(
        errors,
        [
            None,
            Some(MANUALLY_REQUESTED_REDELIVERY),
            Some(MANUALLY_REQUESTED_REDELIVERY)
        ]
    );
    let attempts: Vec<_> = delivered
        .iter()
        .map(|d| (d.total_attempts, d.redelivery_attempts))
        .collect();
    assert_eq!(attempts, [(1, 0), (2, 1), (3, 2)]);
    assert!(
        delivered[1].delivery_ts.unwrap() - delivered[0].delivery_ts.unwrap()
            >= chrono::TimeDelta::milliseconds(150)
    );
    assert!(dead_letter(&queues, &queue).await.is_empty());
}

/// Inserts a row whose payload cannot be decoded as `type_name` - Java corrupts the JSON during deserialization
/// instead (`ProxyJSONSerializer`), which has the same effect on the consumer.
async fn queue_undecodable(db: &TestDatabase, queue: &str, type_name: &str) -> QueueEntryId {
    let id = QueueEntryId::random();
    sqlx::query(
        "INSERT INTO durable_queues (id, queue_name, message_payload, message_payload_type, added_ts, next_delivery_ts,
                                     delivery_mode, meta_data)
         VALUES ($1, $2, '{\"corrupted\": true}', $3, now(), now(), 'NORMAL', '{\"correlation_id\": \"corr-x\"}')",
    )
    .bind(id.as_str())
    .bind(queue)
    .bind(type_name)
    .execute(&db.pool)
    .await
    .unwrap();
    id
}

#[tokio::test]
async fn verify_json_deserialization_problem_causes_message_to_be_marked_as_dead_letter_message() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let id = queue_undecodable(&db, "TestQueue", ORDER_ADDED).await;

    let (handler, mut received) = recording(|_| Ok(()));
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new("TestQueue", fixed(200, 5)), handler)
        .unwrap();
    eventually("the message to be dead-lettered", || async {
        queues
            .storage()
            .find_entry(&id)
            .await
            .unwrap()
            .unwrap()
            .is_dead_letter_message
    })
    .await;
    consumer.stop().await;

    assert!(received.try_recv().is_err(), "the handler never sees it");
    let dead = queues.storage().find_entry(&id).await.unwrap().unwrap();
    assert_eq!(
        dead.meta_data,
        Some(serde_json::json!({"correlation_id": "corr-x"})),
        "metadata kept"
    );
    // Java's text names DurableQueueDeserializationException; the Rust one says what failed.
    assert!(dead.last_delivery_error.unwrap().starts_with(&format!(
        "Failed to deserialize message payload of type {ORDER_ADDED}"
    )));
}

#[tokio::test]
async fn verify_deserialization_failure_in_one_queue_does_not_block_other_queues() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let accepted = Message::new(OrderEvent::Accepted(OrderAccepted {
        order_id: "order-b".into(),
    }));
    queues
        .queue_message(&"QueueB".into(), &accepted)
        .await
        .unwrap();
    queue_undecodable(&db, "QueueA", ORDER_ADDED).await;

    let (handler_a, mut received_a) = recording(|_| Ok(()));
    let (handler_b, mut received_b) = recording(|_| Ok(()));
    let consumer_a = queues
        .consume_from_queue(ConsumerConfig::new("QueueA", fixed(100, 3)), handler_a)
        .unwrap();
    let consumer_b = queues
        .consume_from_queue(ConsumerConfig::new("QueueB", fixed(100, 3)), handler_b)
        .unwrap();
    let delivered_b = deliveries(&mut received_b, 1, Duration::from_secs(5)).await;
    eventually("QueueA's message to be dead-lettered", || async {
        db.count("SELECT count(*) FROM durable_queues WHERE queue_name = 'QueueA' AND is_dead_letter_message").await == 1
    })
    .await;
    consumer_a.stop().await;
    consumer_b.stop().await;

    assert!(matches!(
        delivered_b[0].message.payload,
        OrderEvent::Accepted(_)
    ));
    assert!(received_a.try_recv().is_err());
    assert_eq!(db.count("SELECT count(*) FROM durable_queues WHERE queue_name = 'QueueA' AND NOT is_dead_letter_message").await, 0);
}

// --- LocalCompetingConsumersDurableQueueIT / DistributedCompetingConsumersDurableQueuesIT --------------------

/// Deliveries per instance: message id -> times delivered.
type Deliveries = Arc<Mutex<Vec<HashMap<QueueEntryId, usize>>>>;

/// Starts one consumer per instance on `queue`; every delivery is recorded per instance.
fn competing_consumers(
    instances: &[DurableQueues],
    queue: &QueueName,
    policy: &RedeliveryPolicy,
    parallel_consumers: usize,
    handling_time: Duration,
) -> (Vec<durable_queues_rs::QueueConsumer>, Deliveries) {
    let deliveries = Arc::new(Mutex::new(vec![HashMap::new(); instances.len()]));
    let consumers = instances
        .iter()
        .enumerate()
        .map(|(instance, queues)| {
            let deliveries = deliveries.clone();
            let handler = move |m: QueuedMessage<OrderEvent>| {
                *deliveries.lock().unwrap()[instance]
                    .entry(m.id)
                    .or_insert(0) += 1;
                async move {
                    tokio::time::sleep(handling_time).await;
                    Ok(())
                }
            };
            let config = ConsumerConfig::new(queue.clone(), policy.clone())
                .with_parallel_consumers(parallel_consumers);
            queues.consume_from_queue(config, handler).unwrap()
        })
        .collect();
    (consumers, deliveries)
}

fn total_deliveries(deliveries: &Mutex<Vec<HashMap<QueueEntryId, usize>>>) -> usize {
    deliveries
        .lock()
        .unwrap()
        .iter()
        .flat_map(|per_instance| per_instance.values())
        .sum()
}

/// Every sent id delivered exactly once across all instances, and nothing more arrives during `quiet`.
async fn assert_each_delivered_exactly_once(
    deliveries: &Mutex<Vec<HashMap<QueueEntryId, usize>>>,
    sent: &HashSet<QueueEntryId>,
    quiet: Duration,
) {
    tokio::time::sleep(quiet).await;
    let deliveries = deliveries.lock().unwrap();
    let mut merged: HashMap<&QueueEntryId, usize> = HashMap::new();
    for per_instance in deliveries.iter() {
        for (id, count) in per_instance {
            *merged.entry(id).or_insert(0) += count;
        }
    }
    let delivered: HashSet<QueueEntryId> = merged.keys().map(|id| (*id).clone()).collect();
    assert_eq!(
        &delivered, sent,
        "every sent message delivered, nothing else"
    );
    let duplicates: Vec<_> = merged.iter().filter(|(_, count)| **count != 1).collect();
    assert!(
        duplicates.is_empty(),
        "messages consumed more than once: {duplicates:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn local_competing_consumers_verify_queued_messages_are_dequeued() {
    const MESSAGES: usize = 2000;
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.node_pool(20).await).await;
    let queue = QueueName::from("TestQueue");
    let sent: HashSet<_> = queues
        .queue_messages(&queue, &mixed_messages(MESSAGES))
        .await
        .unwrap()
        .into_iter()
        .collect();

    let (consumers, deliveries) = competing_consumers(
        std::slice::from_ref(&queues),
        &queue,
        &fixed(1, 5),
        20,
        Duration::ZERO,
    );
    eventually("2000 deliveries", || async {
        total_deliveries(&deliveries) >= MESSAGES
    })
    .await;
    assert_each_delivered_exactly_once(&deliveries, &sent, Duration::from_millis(1990)).await;
    for consumer in consumers {
        consumer.stop().await;
    }
}

async fn distributed_competing_consumers(disrupt_database: bool) {
    const MESSAGES: usize = 1000;
    let db = TestDatabase::start().await;
    let instances = [
        db.durable_queues(db.node_pool(10).await).await,
        db.durable_queues(db.node_pool(10).await).await,
    ];
    let queue = QueueName::from("TestQueue");
    let sent: HashSet<_> = instances[0]
        .queue_messages(&queue, &mixed_messages(MESSAGES))
        .await
        .unwrap()
        .into_iter()
        .collect();

    // Java's handlers are instant and the whole run fits before its pause. 50 ms per message spreads the work over
    // about 2.5 s, so the 5 s pause at the 2 s mark lands on in-flight claims and acknowledgements.
    let handling_time = Duration::from_millis(if disrupt_database { 50 } else { 0 });
    let (consumers, deliveries) =
        competing_consumers(&instances, &queue, &fixed(1, 5), 10, handling_time);
    if disrupt_database {
        tokio::time::sleep(Duration::from_secs(2)).await;
        db.pause_for(Duration::from_secs(5)).await;
    }
    eventually("1000 deliveries", || async {
        total_deliveries(&deliveries) >= MESSAGES
    })
    .await;
    assert_each_delivered_exactly_once(&deliveries, &sent, Duration::from_millis(1990)).await;
    for consumer in consumers {
        consumer.stop().await;
    }
    let per_instance: Vec<usize> = deliveries
        .lock()
        .unwrap()
        .iter()
        .map(HashMap::len)
        .collect();
    assert!(
        per_instance.iter().all(|&n| n > 0),
        "both instances consumed: {per_instance:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn distributed_competing_consumers_verify_queued_messages_are_dequeued() {
    distributed_competing_consumers(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn distributed_competing_consumers_verify_queued_messages_are_dequeued_with_db_connectivity_issues()
 {
    distributed_competing_consumers(true).await;
}

// --- DuplicateConsumptionDurableQueuesIT ---------------------------------------------------------------------

async fn no_duplicate_message_consumption(disrupt_database: bool) {
    const MESSAGES: usize = 40;
    let db = TestDatabase::start().await;
    let timeout = Duration::from_millis(10_000);
    let instance = |pool| async {
        db.durable_queues(pool)
            .await
            .with_message_handling_timeout(timeout)
    };
    let first = instance(db.node_pool(12).await).await;
    let second = instance(db.node_pool(12).await).await;
    let queue = QueueName::from("DuplicateConsumptionTestQueue");
    let sent: HashSet<_> = first
        .queue_messages(&queue, &mixed_messages(MESSAGES))
        .await
        .unwrap()
        .into_iter()
        .collect();

    let processing = Duration::from_millis(1000);
    let (mut consumers, deliveries) = competing_consumers(
        std::slice::from_ref(&first),
        &queue,
        &fixed(1, 5),
        1,
        processing,
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The second instance's deliveries go into its own map; merge both before asserting.
    let (second_consumers, second_deliveries) = competing_consumers(
        std::slice::from_ref(&second),
        &queue,
        &fixed(1, 5),
        1,
        processing,
    );
    consumers.extend(second_consumers);
    if disrupt_database {
        tokio::time::sleep(Duration::from_secs(2)).await;
        db.pause_for(Duration::from_secs(5)).await;
    }
    let all = || {
        let mut merged = deliveries.lock().unwrap().clone();
        merged.extend(second_deliveries.lock().unwrap().iter().cloned());
        Mutex::new(merged)
    };
    eventually("40 messages consumed", || async {
        total_deliveries(&all()) >= MESSAGES
    })
    .await;
    assert_each_delivered_exactly_once(&all(), &sent, Duration::from_millis(1990)).await;
    for consumer in consumers {
        consumer.stop().await;
    }
    let per_instance = [
        deliveries.lock().unwrap()[0].len(),
        second_deliveries.lock().unwrap()[0].len(),
    ];
    assert!(
        per_instance.iter().all(|&n| n > 0),
        "both instances consumed: {per_instance:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_no_duplicate_message_consumption() {
    no_duplicate_message_consumption(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_no_duplicate_message_consumption_with_db_connectivity_issues() {
    no_duplicate_message_consumption(true).await;
}

// --- DurableQueuesLoadIT --------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_a_large_number_of_messages() {
    const MESSAGES: i64 = 20_000;
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("TestQueue");
    let handled = Arc::new(AtomicUsize::new(0));
    let handler = {
        let handled = handled.clone();
        move |_: QueuedMessage<OrderEvent>| {
            handled.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }
    };
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new(queue.clone(), fixed(100, 0)), handler)
        .unwrap();

    // One message per statement, all in one transaction - as Java does inside one UnitOfWork.
    let mut tx = db.pool.begin().await.unwrap();
    for i in 0..MESSAGES {
        queues
            .queue_message_in(&mut tx, &queue, &Message::new(added(i)))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    // Java asserts 20000 queued right after the commit; the running consumer may already have taken some. A message
    // is deleted only after its handler returns, so a handled message can still be in the table: with one consumer,
    // at most one. Each bound reads `handled` on the side of the count that makes it hold.
    let handled_before = handled.load(Ordering::SeqCst) as i64;
    let queued = db.count("SELECT count(*) FROM durable_queues").await;
    let handled_after = handled.load(Ordering::SeqCst) as i64;
    assert!(
        queued + handled_after >= MESSAGES,
        "none lost: {queued} queued + {handled_after} handled"
    );
    assert!(
        queued + handled_before <= MESSAGES + 1,
        "none extra: {queued} queued + {handled_before} handled, at most one handled but not yet deleted"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while handled.load(Ordering::SeqCst) <= 10 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("more than 10 messages handled within 5 s");
    consumer.stop().await;
}

// --- CentralizedFetcherDurableQueueIT ------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_multiple_queues_with_different_thread_configurations() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.node_pool(21).await).await;
    let lanes = [
        ("HighPriorityQueue", 10),
        ("MediumPriorityQueue", 5),
        ("LowPriorityQueue", 2),
    ];
    let mut sent: HashMap<&str, HashSet<QueueEntryId>> = HashMap::new();
    for (lane, (queue, _)) in lanes.iter().enumerate() {
        let messages: Vec<_> = (0..1000)
            .filter(|i| i % 3 == lane)
            .map(|i| Message::new(added(i as i64)))
            .collect();
        sent.insert(
            queue,
            queues
                .queue_messages(&(*queue).into(), &messages)
                .await
                .unwrap()
                .into_iter()
                .collect(),
        );
    }
    assert_eq!(
        sent.values()
            .map(HashSet::len)
            .collect::<Vec<_>>()
            .iter()
            .sum::<usize>(),
        1000
    );

    let mut consumers = Vec::new();
    let mut received = HashMap::new();
    for (queue, parallel) in lanes {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler = {
            let seen = seen.clone();
            move |m: QueuedMessage<OrderEvent>| {
                seen.lock().unwrap().push(m.id);
                async { Ok(()) }
            }
        };
        let config = ConsumerConfig::new(queue, fixed(1, 5)).with_parallel_consumers(parallel);
        consumers.push(queues.consume_from_queue(config, handler).unwrap());
        received.insert(queue, seen);
    }
    eventually("every queue to be drained", || queue_is_empty(&db)).await;
    for consumer in consumers {
        consumer.stop().await;
    }

    for (queue, _) in lanes {
        let seen = received[queue].lock().unwrap();
        let unique: HashSet<_> = seen.iter().cloned().collect();
        assert_eq!(seen.len(), unique.len(), "{queue}: no duplicates");
        assert_eq!(unique, sent[queue], "{queue}: exactly its own messages");
    }
}
