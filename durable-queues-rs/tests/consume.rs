//! Phase 2: enqueue -> claim -> handle -> acknowledge.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{JAVA_ROWS, JAVA_SCHEMA, TestDatabase, eventually};
use durable_queues_rs::{
    ConsumerConfig, DecodeError, DeliveryMode, DurableQueuesError, HandlerError, Message,
    MessageOrder, MessageType, QueueEntryId, QueueMessage, QueueName, QueueStorage, QueuedMessage,
    RedeliveryPolicy,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderPlaced {
    order_id: String,
    customer_id: String,
    amount: f64,
    placed_at: String,
}

impl MessageType for OrderPlaced {
    const TYPE_NAME: &'static str = "com.acme.orders.OrderPlaced";
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderCancelled {
    order_id: String,
    reason: String,
}

/// A queue carrying several Java payload types, dispatched on `message_payload_type`.
#[derive(Debug, Clone, PartialEq)]
enum OrderMessage {
    Placed(OrderPlaced),
    Cancelled(OrderCancelled),
    Added(OrderAdded),
}

const ORDER_ADDED: &str = "com.acme.orders.OrderEvent$OrderAdded";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderAdded {
    order_id: String,
    step: u32,
}

impl QueueMessage for OrderMessage {
    fn payload_type(&self) -> &str {
        match self {
            Self::Placed(_) => OrderPlaced::TYPE_NAME,
            Self::Cancelled(_) => "com.acme.orders.OrderEvent$OrderCancelled",
            Self::Added(_) => ORDER_ADDED,
        }
    }

    fn to_payload(&self) -> serde_json::Result<Value> {
        match self {
            Self::Placed(m) => serde_json::to_value(m),
            Self::Cancelled(m) => serde_json::to_value(m),
            Self::Added(m) => serde_json::to_value(m),
        }
    }

    fn from_payload(payload_type: &str, payload: Value) -> Result<Self, DecodeError> {
        match payload_type {
            OrderPlaced::TYPE_NAME => Ok(Self::Placed(serde_json::from_value(payload)?)),
            "com.acme.orders.OrderEvent$OrderCancelled" => {
                Ok(Self::Cancelled(serde_json::from_value(payload)?))
            }
            ORDER_ADDED => Ok(Self::Added(serde_json::from_value(payload)?)),
            other => Err(DecodeError::UnknownPayloadType(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: usize,
}

impl MessageType for Job {
    const TYPE_NAME: &'static str = "com.acme.Job";
}

fn policy() -> RedeliveryPolicy {
    RedeliveryPolicy::fixed_backoff(Duration::from_millis(100), 3)
}

fn order(id: &str) -> OrderPlaced {
    OrderPlaced {
        order_id: id.into(),
        customer_id: "customer-1".into(),
        amount: 12.5,
        placed_at: "2026-10-11T12:00:00Z".into(),
    }
}

/// A handler that forwards every message to a channel.
fn forward<M: Send + 'static>(
    sender: mpsc::UnboundedSender<QueuedMessage<M>>,
) -> impl Fn(QueuedMessage<M>) -> std::future::Ready<Result<(), HandlerError>> + Send + Sync + 'static
{
    move |message| {
        sender.send(message).unwrap();
        std::future::ready(Ok(()))
    }
}

#[tokio::test]
async fn a_queued_message_is_delivered_with_its_metadata_and_removed_once_handled() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("Orders");
    let id = queues
        .queue_message(
            &queue,
            &Message::new(order("order-1")).with_metadata("correlationId", "corr-1"),
        )
        .await
        .unwrap();

    let (sender, mut received) = mpsc::unbounded_channel::<QueuedMessage<OrderPlaced>>();
    let consumer = queues
        .consume_from_queue(
            ConsumerConfig::new(queue.clone(), policy()),
            forward(sender),
        )
        .unwrap();
    let delivered = tokio::time::timeout(Duration::from_secs(10), received.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(delivered.id, id);
    assert_eq!(delivered.queue_name, queue);
    assert_eq!(delivered.message.payload, order("order-1"));
    assert_eq!(
        delivered.message.metadata.get("correlationId"),
        Some("corr-1")
    );
    assert_eq!(
        (delivered.total_attempts, delivered.redelivery_attempts),
        (1, 0)
    );
    assert_eq!(delivered.delivery_mode, DeliveryMode::Normal);
    assert!(delivered.delivery_ts.is_some());
    eventually("the handled message to be deleted", || async {
        db.count("SELECT count(*) FROM durable_queues").await == 0
    })
    .await;
    consumer.stop().await;
}

#[tokio::test]
async fn queued_rows_have_the_shape_java_writes() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let messages = [
        Message::new(Job { n: 1 }),
        Message::new(Job { n: 2 }).with_metadata("k", "v"),
    ];

    let ids = queues
        .queue_messages(&"Jobs".into(), &messages)
        .await
        .unwrap();

    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    let expected_meta = [serde_json::json!({}), serde_json::json!({"k": "v"})];
    for ((id, n), meta) in ids.iter().zip([1, 2]).zip(expected_meta) {
        let row = queues.storage().find_entry(id).await.unwrap().unwrap();
        assert_eq!(row.message_payload, serde_json::json!({ "n": n }));
        assert_eq!(row.message_payload_type, "com.acme.Job");
        assert_eq!(
            row.meta_data,
            Some(meta),
            "Java always writes meta_data, {{}} when empty"
        );
        assert_eq!(
            (row.delivery_mode, row.key, row.key_order),
            (DeliveryMode::Normal, None, -1)
        );
        assert_eq!((row.total_attempts, row.redelivery_attempts), (0, 0));
        assert!(
            !row.is_being_delivered
                && !row.is_dead_letter_message
                && row.last_delivery_error.is_none()
        );
        assert_eq!(
            row.next_delivery_ts,
            Some(row.added_ts),
            "no delivery delay"
        );
        assert_eq!(row.delivery_ts, None);
    }
}

#[tokio::test]
async fn java_rows_are_consumed_ordered_rows_in_order_and_delayed_and_foreign_rows_are_left_alone()
{
    let db = TestDatabase::start().await;
    db.run_script(JAVA_SCHEMA).await;
    db.run_script(JAVA_ROWS).await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let fixture =
        |suffix: &str| QueueEntryId::new(format!("2f1c7c6e-6a4b-11f1-9a2e-0242ac12{suffix}"));

    let (sender, mut received) = mpsc::unbounded_channel::<QueuedMessage<OrderMessage>>();
    let consumer = queues.consume_from_queue(
        ConsumerConfig::new("Orders", policy()).with_parallel_consumers(4),
        forward(sender),
    );
    let consumer = consumer.unwrap();
    let mut handled = HashMap::new();
    let mut arrival = Vec::new();
    for _ in 0..4 {
        let message = tokio::time::timeout(Duration::from_secs(10), received.recv())
            .await
            .unwrap()
            .unwrap();
        arrival.push(message.id.clone());
        handled.insert(message.id.clone(), message);
    }
    eventually("the undecodable row to be dead-lettered", || async {
        db.count(
            "SELECT count(*) FROM durable_queues WHERE id LIKE '%0008' AND is_dead_letter_message",
        )
        .await
            == 1
    })
    .await;
    eventually("the handled rows to be deleted", || async {
        db.count("SELECT count(*) FROM durable_queues WHERE right(id, 4) IN ('0002', '0003', '0005', '0006')")
            .await
            == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await; // a few more polls
    consumer.stop().await;

    let placed = &handled[&fixture("0002")];
    assert_eq!(
        placed.message.metadata.get("essentials.causedByEventId"),
        Some("evt-42")
    );
    let OrderMessage::Placed(payload) = &placed.message.payload else {
        panic!("expected OrderPlaced")
    };
    assert_eq!(
        (payload.order_id.as_str(), payload.amount),
        ("order-1", 125.5)
    );
    assert!(handled.contains_key(&fixture("0003")));
    // The two ordered rows of key order-4, in key order.
    let ordered: Vec<_> = arrival
        .iter()
        .filter(|id| [fixture("0005"), fixture("0006")].contains(id))
        .collect();
    assert_eq!(ordered, [&fixture("0005"), &fixture("0006")]);
    let head = &handled[&fixture("0005")];
    assert_eq!(
        head.message.order,
        Some(MessageOrder {
            key: "order-4".into(),
            order: 0
        })
    );
    assert_eq!(head.delivery_mode, DeliveryMode::InOrder);
    assert!(matches!(
        head.message.payload,
        OrderMessage::Added(OrderAdded { step: 0, .. })
    ));
    assert!(
        received.try_recv().is_err(),
        "nothing else may be delivered"
    );

    let storage = queues.storage();
    let dead = storage.find_entry(&fixture("0008")).await.unwrap().unwrap();
    assert!(
        dead.last_delivery_error.unwrap().starts_with(
            "Failed to deserialize message payload of type com.acme.orders.OrderPlaced"
        )
    );
    assert_eq!(
        (
            dead.is_being_delivered,
            dead.next_delivery_ts,
            dead.delivery_ts
        ),
        (false, None, None)
    );
    for untouched in ["0004", "0009"] {
        let row = storage
            .find_entry(&fixture(untouched))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (row.total_attempts, row.is_being_delivered),
            (0, false),
            "row {untouched} must not be claimed"
        );
    }
    let preexisting_dead_letter = storage.find_entry(&fixture("0007")).await.unwrap().unwrap();
    assert!(
        preexisting_dead_letter.is_dead_letter_message
            && preexisting_dead_letter.total_attempts == 0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn competing_nodes_handle_every_message_exactly_once() {
    const MESSAGES: usize = 1000;
    const NODES: usize = 4;
    let db = TestDatabase::start().await;
    let producer = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("Jobs");
    let jobs: Vec<_> = (0..MESSAGES).map(|n| Message::new(Job { n })).collect();
    let queued: HashSet<QueueEntryId> = producer
        .queue_messages(&queue, &jobs)
        .await
        .unwrap()
        .into_iter()
        .collect();

    let deliveries = Arc::new(Mutex::new(HashMap::<QueueEntryId, usize>::new()));
    let mut consumers = Vec::new();
    for _ in 0..NODES {
        let node = db.durable_queues(db.node_pool(4).await).await;
        let deliveries = deliveries.clone();
        let handler = move |message: QueuedMessage<Job>| {
            let deliveries = deliveries.clone();
            async move {
                *deliveries.lock().unwrap().entry(message.id).or_default() += 1;
                tokio::time::sleep(Duration::from_millis(message.message.payload.n as u64 % 3))
                    .await;
                Ok(())
            }
        };
        let config = ConsumerConfig::new(queue.clone(), policy()).with_parallel_consumers(4);
        consumers.push(node.consume_from_queue(config, handler).unwrap());
    }

    eventually("every message to be handled and acknowledged", || async {
        db.count("SELECT count(*) FROM durable_queues").await == 0
    })
    .await;
    for consumer in consumers {
        consumer.stop().await;
    }

    let deliveries = deliveries.lock().unwrap();
    let delivered: HashSet<_> = deliveries.keys().cloned().collect();
    assert_eq!(delivered, queued, "every queued message is delivered");
    let duplicates: Vec<_> = deliveries
        .iter()
        .filter(|(_, count)| **count != 1)
        .collect();
    assert!(
        duplicates.is_empty(),
        "duplicate deliveries: {duplicates:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn never_more_messages_are_claimed_or_handled_than_parallel_consumers() {
    const SLOTS: usize = 3;
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let jobs: Vec<_> = (0..20).map(|n| Message::new(Job { n })).collect();
    queues.queue_messages(&"Jobs".into(), &jobs).await.unwrap();

    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let handler = {
        let (running, peak) = (running.clone(), peak.clone());
        move |_: QueuedMessage<Job>| {
            let (running, peak) = (running.clone(), peak.clone());
            async move {
                peak.fetch_max(running.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                running.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
        }
    };
    let consumer = queues.consume_from_queue(
        ConsumerConfig::new("Jobs", policy()).with_parallel_consumers(SLOTS),
        handler,
    );
    let consumer = consumer.unwrap();

    let mut peak_claimed = 0;
    while db.count("SELECT count(*) FROM durable_queues").await > 0 {
        peak_claimed = peak_claimed.max(
            db.count("SELECT count(*) FROM durable_queues WHERE is_being_delivered")
                .await,
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    consumer.stop().await;

    assert_eq!(
        peak.load(Ordering::SeqCst),
        SLOTS,
        "handlers run in parallel up to, never beyond, the slot count"
    );
    assert!(
        peak_claimed as usize <= SLOTS,
        "claimed {peak_claimed} rows with {SLOTS} slots"
    );
}

#[tokio::test]
async fn a_message_queued_in_a_rolled_back_transaction_is_never_delivered() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("Outbox");

    let mut tx = db.pool.begin().await.unwrap();
    queues
        .queue_message_in(&mut tx, &queue, &Message::new(Job { n: 1 }))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(db.count("SELECT count(*) FROM durable_queues").await, 0);

    let mut tx = db.pool.begin().await.unwrap();
    let committed = queues
        .queue_message_in(&mut tx, &queue, &Message::new(Job { n: 2 }))
        .await
        .unwrap();
    assert_eq!(
        db.count("SELECT count(*) FROM durable_queues").await,
        0,
        "invisible until commit"
    );
    tx.commit().await.unwrap();

    let (sender, mut received) = mpsc::unbounded_channel::<QueuedMessage<Job>>();
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new(queue, policy()), forward(sender))
        .unwrap();
    let delivered = tokio::time::timeout(Duration::from_secs(10), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((delivered.id, delivered.message.payload.n), (committed, 2));
    consumer.stop().await;
}

#[tokio::test]
async fn stop_waits_for_in_flight_handlers_and_acknowledges_them() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    queues
        .queue_message(&"Jobs".into(), &Message::new(Job { n: 1 }))
        .await
        .unwrap();

    let (started, mut handler_started) = mpsc::unbounded_channel();
    let handler = move |_: QueuedMessage<Job>| {
        let started = started.clone();
        async move {
            started.send(()).unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(())
        }
    };
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new("Jobs", policy()), handler)
        .unwrap();
    handler_started.recv().await.unwrap();

    consumer.stop().await;

    assert_eq!(
        db.count("SELECT count(*) FROM durable_queues").await,
        0,
        "the in-flight message was acknowledged"
    );
}

#[tokio::test]
async fn stop_abandons_handlers_still_running_at_the_shutdown_timeout() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    queues
        .queue_message(&"Jobs".into(), &Message::new(Job { n: 1 }))
        .await
        .unwrap();

    let (started, mut handler_started) = mpsc::unbounded_channel();
    let handler = move |_: QueuedMessage<Job>| {
        let started = started.clone();
        async move {
            started.send(()).unwrap();
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(())
        }
    };
    let config =
        ConsumerConfig::new("Jobs", policy()).with_shutdown_timeout(Duration::from_millis(200));
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    handler_started.recv().await.unwrap();

    let stopping = Instant::now();
    consumer.stop().await;

    assert!(stopping.elapsed() < Duration::from_secs(2));
    assert_eq!(
        db.count("SELECT count(*) FROM durable_queues WHERE is_being_delivered")
            .await,
        1,
        "left claimed for stuck-message recovery"
    );
}

#[tokio::test]
async fn one_consumer_per_queue_until_it_is_stopped() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let handler = |_: QueuedMessage<Job>| async { Ok(()) };

    let first = queues
        .consume_from_queue(ConsumerConfig::new("Jobs", policy()), handler)
        .unwrap();
    let second = queues.consume_from_queue(ConsumerConfig::new("Jobs", policy()), handler);
    assert!(
        matches!(second, Err(DurableQueuesError::ConsumerAlreadyExists(q)) if q.as_str() == "Jobs")
    );
    let other_queue = queues
        .consume_from_queue(ConsumerConfig::new("Other", policy()), handler)
        .unwrap();

    first.stop().await;
    let replacement = queues
        .consume_from_queue(ConsumerConfig::new("Jobs", policy()), handler)
        .unwrap();

    let zero = queues.consume_from_queue(
        ConsumerConfig::new("Zero", policy()).with_parallel_consumers(0),
        handler,
    );
    assert!(matches!(
        zero,
        Err(DurableQueuesError::InvalidConsumerConfig(_))
    ));
    replacement.stop().await;
    other_queue.stop().await;
}
