//! Phase 3: retries, delayed delivery, dead letters and stuck-message recovery.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use common::{TestDatabase, eventually};
use durable_queues_rs::{
    ConsumerConfig, DurableQueues, HandlerError, Message, MessageHandler, MessageType,
    PollingBackoff, QueueEntry, QueueEntryId, QueueName, QueueStorage, QueuedMessage,
    QueueingSortOrder, RedeliveryPolicy,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, UnboundedReceiver};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: usize,
}

impl MessageType for Job {
    const TYPE_NAME: &'static str = "com.acme.Job";
}

const STUCK_MESSAGE_ERROR: &str =
    "Handler Processing of the Message was determined to have Timed Out";

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn hours(hours: u64) -> Duration {
    Duration::from_secs(hours * 3600)
}

/// A handler that reports every delivery and settles it with `outcome`.
fn recording(
    outcome: impl Fn(&QueuedMessage<Job>) -> Result<(), HandlerError> + Send + Sync + 'static,
) -> (
    impl MessageHandler<Job>,
    UnboundedReceiver<QueuedMessage<Job>>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let handler = move |message: QueuedMessage<Job>| {
        let result = outcome(&message);
        sender.send(message).unwrap();
        async move { result }
    };
    (handler, receiver)
}

fn always_fail(_: &QueuedMessage<Job>) -> Result<(), HandlerError> {
    Err(HandlerError::transient("boom"))
}

async fn next_delivery(
    deliveries: &mut UnboundedReceiver<QueuedMessage<Job>>,
) -> QueuedMessage<Job> {
    tokio::time::timeout(Duration::from_secs(15), deliveries.recv())
        .await
        .expect("a delivery within 15 s")
        .expect("handler alive")
}

async fn entry(queues: &DurableQueues, id: &QueueEntryId) -> QueueEntry {
    queues
        .storage()
        .find_entry(id)
        .await
        .unwrap()
        .expect("row exists")
}

/// Waits until a failed delivery has been settled, i.e. the row is no longer claimed.
async fn settled(queues: &DurableQueues, id: &QueueEntryId) -> QueueEntry {
    eventually("the failed delivery to be settled", || async {
        !entry(queues, id).await.is_being_delivered
    })
    .await;
    entry(queues, id).await
}

fn assert_delay(from: DateTime<Utc>, until: Option<DateTime<Utc>>, delay: Duration) {
    let actual = until.expect("next_delivery_ts set") - from;
    let expected = TimeDelta::from_std(delay).unwrap();
    assert!(
        actual >= expected && actual < expected + TimeDelta::seconds(5),
        "expected a delay of {expected}, got {actual}"
    );
}

#[tokio::test]
async fn each_policy_schedules_redeliveries_from_the_redelivery_count_before_the_failure() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let cases = [
        (
            "Fixed",
            RedeliveryPolicy::fixed_backoff(hours(1), 5),
            hours(1),
            hours(1),
        ),
        (
            "Linear",
            RedeliveryPolicy::linear_backoff(hours(1), hours(10), 5),
            hours(1),
            hours(2),
        ),
        (
            "Exponential",
            RedeliveryPolicy::exponential_backoff(hours(1), hours(3), 2.0, hours(10), 5),
            hours(1),
            hours(3),
        ),
    ];

    for (queue, policy, first_delay, second_delay) in cases {
        let id = queues
            .queue_message(&queue.into(), &Message::new(Job { n: 1 }))
            .await
            .unwrap();
        let (handler, mut deliveries) = recording(always_fail);
        let consumer = queues
            .consume_from_queue(ConsumerConfig::new(queue, policy), handler)
            .unwrap();

        let first = next_delivery(&mut deliveries).await;
        let row = settled(&queues, &id).await;
        assert_eq!(
            (row.total_attempts, row.redelivery_attempts),
            (1, 1),
            "{queue}"
        );
        assert_eq!(
            (row.delivery_ts, row.last_delivery_error.as_deref()),
            (None, Some("boom")),
            "{queue}"
        );
        assert_delay(
            first.delivery_ts.unwrap(),
            row.next_delivery_ts,
            first_delay,
        );

        sqlx::query("UPDATE durable_queues SET next_delivery_ts = now() WHERE id = $1")
            .bind(id.as_str())
            .execute(&db.pool)
            .await
            .unwrap();
        let second = next_delivery(&mut deliveries).await;
        assert_eq!(
            (second.total_attempts, second.redelivery_attempts),
            (2, 1),
            "{queue}"
        );
        let row = settled(&queues, &id).await;
        assert_eq!(
            (row.total_attempts, row.redelivery_attempts),
            (2, 2),
            "{queue}"
        );
        assert_delay(
            second.delivery_ts.unwrap(),
            row.next_delivery_ts,
            second_delay,
        );
        consumer.stop().await;
    }
}

#[tokio::test]
async fn a_message_is_dead_lettered_after_exactly_max_redeliveries_plus_one_deliveries() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let id = queues
        .queue_message(&"Jobs".into(), &Message::new(Job { n: 1 }))
        .await
        .unwrap();
    let (handler, mut deliveries) = recording(always_fail);
    let policy = RedeliveryPolicy::fixed_backoff(ms(50), 2);
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new("Jobs", policy), handler)
        .unwrap();

    for attempt in 1..=3 {
        let delivery = next_delivery(&mut deliveries).await;
        assert_eq!(
            (delivery.total_attempts, delivery.redelivery_attempts),
            (attempt, attempt - 1)
        );
        if attempt > 1 {
            assert_eq!(delivery.last_delivery_error.as_deref(), Some("boom"));
        }
    }
    eventually("the message to be dead-lettered", || async {
        entry(&queues, &id).await.is_dead_letter_message
    })
    .await;
    tokio::time::sleep(ms(300)).await;
    consumer.stop().await;

    assert!(
        deliveries.try_recv().is_err(),
        "a dead letter is not delivered again"
    );
    let row = entry(&queues, &id).await;
    assert_eq!((row.total_attempts, row.redelivery_attempts), (3, 2));
    assert_eq!(
        (
            row.is_being_delivered,
            row.next_delivery_ts,
            row.delivery_ts
        ),
        (false, None, None)
    );
    assert_eq!(row.last_delivery_error.as_deref(), Some("boom"));
}

#[derive(Debug, thiserror::Error)]
#[error("order total must be positive")]
struct ValidationError;

#[tokio::test]
async fn permanent_errors_are_dead_lettered_on_the_first_attempt() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let explicit = queues
        .queue_message(&"Explicit".into(), &Message::new(Job { n: 1 }))
        .await
        .unwrap();
    let classified = queues
        .queue_message(&"Classified".into(), &Message::new(Job { n: 2 }))
        .await
        .unwrap();
    let generous = RedeliveryPolicy::fixed_backoff(ms(10), 10);

    let (handler, mut explicit_deliveries) =
        recording(|_| Err(HandlerError::permanent("unknown customer")));
    let first = queues
        .consume_from_queue(ConsumerConfig::new("Explicit", generous.clone()), handler)
        .unwrap();
    // Returned through `?`, so transient - the policy's classifier makes it permanent.
    let (handler, mut classified_deliveries) = recording(|_| Err(ValidationError)?);
    let policy = generous.stop_redelivery_on(|e| e.is::<ValidationError>());
    let second = queues
        .consume_from_queue(ConsumerConfig::new("Classified", policy), handler)
        .unwrap();

    for (id, reason) in [
        (&explicit, "unknown customer"),
        (&classified, "order total must be positive"),
    ] {
        eventually("the message to be dead-lettered", || async {
            entry(&queues, id).await.is_dead_letter_message
        })
        .await;
        let row = entry(&queues, id).await;
        assert_eq!((row.total_attempts, row.redelivery_attempts), (1, 0));
        assert_eq!(row.last_delivery_error.as_deref(), Some(reason));
    }
    first.stop().await;
    second.stop().await;
    for deliveries in [&mut explicit_deliveries, &mut classified_deliveries] {
        assert!(deliveries.try_recv().is_ok());
        assert!(deliveries.try_recv().is_err(), "exactly one delivery");
    }
}

#[tokio::test]
async fn a_panicking_handler_is_retried_like_a_transient_failure() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let jobs = [Message::new(Job { n: 1 }), Message::new(Job { n: 2 })];
    queues.queue_messages(&"Jobs".into(), &jobs).await.unwrap();
    let (sender, mut deliveries) = mpsc::unbounded_channel::<QueuedMessage<Job>>();
    // Job 1 panics while the handler is called, job 2 inside the future it returns - both on the first attempt only.
    let handler = move |message: QueuedMessage<Job>| {
        let first_attempt = message.total_attempts == 1;
        let n = message.message.payload.n;
        if first_attempt && n == 1 {
            panic!("kaboom before the future");
        }
        sender.send(message).unwrap();
        async move {
            if first_attempt && n == 2 {
                panic!("kaboom inside the future");
            }
            Ok(())
        }
    };
    let policy = RedeliveryPolicy::fixed_backoff(ms(50), 3);
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new("Jobs", policy), handler)
        .unwrap();

    let mut redelivered = Vec::new();
    while redelivered.len() < 2 {
        let delivery = next_delivery(&mut deliveries).await;
        if delivery.total_attempts == 2 {
            redelivered.push(delivery);
        }
    }
    eventually("both messages to be acknowledged", || async {
        db.count("SELECT count(*) FROM durable_queues").await == 0
    })
    .await;
    consumer.stop().await;

    redelivered.sort_by_key(|d| d.message.payload.n);
    for (delivery, panic) in redelivered
        .iter()
        .zip(["kaboom before the future", "kaboom inside the future"])
    {
        assert_eq!(delivery.redelivery_attempts, 1);
        assert_eq!(
            delivery.last_delivery_error.as_deref(),
            Some(&*format!("message handler panicked: {panic}"))
        );
    }
}

#[tokio::test]
async fn a_delayed_message_is_not_delivered_before_its_delay() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let delay = ms(1000);
    let id = queues
        .queue_message_with_delay(&"Jobs".into(), &Message::new(Job { n: 1 }), delay)
        .await
        .unwrap();
    let row = entry(&queues, &id).await;
    assert_eq!(
        row.next_delivery_ts,
        Some(row.added_ts + TimeDelta::from_std(delay).unwrap())
    );

    let (handler, mut deliveries) = recording(|_| Ok(()));
    let policy = RedeliveryPolicy::fixed_backoff(ms(50), 0);
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new("Jobs", policy), handler)
        .unwrap();
    let delivery = next_delivery(&mut deliveries).await;
    consumer.stop().await;

    assert!(delivery.delivery_ts.unwrap() >= row.next_delivery_ts.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_message_left_claimed_by_a_crashed_node_is_redelivered_after_the_handling_timeout() {
    let timeout = ms(1000);
    let db = TestDatabase::start().await;
    let queue = QueueName::from("Jobs");
    let policy = RedeliveryPolicy::fixed_backoff(ms(50), 5);

    // Node A claims the message and "crashes": its handler hangs and is abandoned at shutdown.
    let node_a = db
        .durable_queues(db.node_pool(2).await)
        .await
        .with_message_handling_timeout(timeout);
    let id = node_a
        .queue_message(&queue, &Message::new(Job { n: 1 }))
        .await
        .unwrap();
    let (started, mut hanging) = mpsc::unbounded_channel::<QueuedMessage<Job>>();
    let hang = move |message: QueuedMessage<Job>| {
        started.send(message).unwrap();
        std::future::pending::<Result<(), HandlerError>>()
    };
    let config = ConsumerConfig::new(queue.clone(), policy.clone()).with_shutdown_timeout(ms(50));
    let crashed = node_a.consume_from_queue(config, hang).unwrap();
    let first = next_delivery(&mut hanging).await;
    crashed.stop().await;
    assert!(
        entry(&node_a, &id).await.is_being_delivered,
        "left claimed by the crashed node"
    );

    // Node B recovers it once it has been claimed for longer than the timeout.
    let node_b = db
        .durable_queues(db.node_pool(2).await)
        .await
        .with_message_handling_timeout(timeout);
    let (handler, mut deliveries) = recording(|_| Ok(()));
    let recovering = node_b
        .consume_from_queue(ConsumerConfig::new(queue, policy), handler)
        .unwrap();
    let redelivery = next_delivery(&mut deliveries).await;
    eventually("the recovered message to be acknowledged", || async {
        db.count("SELECT count(*) FROM durable_queues").await == 0
    })
    .await;
    recovering.stop().await;

    assert_eq!(redelivery.id, id);
    assert_eq!(
        (redelivery.total_attempts, redelivery.redelivery_attempts),
        (2, 1)
    );
    assert_eq!(
        redelivery.last_delivery_error.as_deref(),
        Some(STUCK_MESSAGE_ERROR)
    );
    let claimed_for = redelivery.delivery_ts.unwrap() - first.delivery_ts.unwrap();
    assert!(
        claimed_for >= TimeDelta::from_std(timeout).unwrap(),
        "recovered after only {claimed_for}"
    );
}

#[tokio::test]
async fn a_resurrected_dead_letter_is_delivered_again_with_its_counters_kept() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let id = queues
        .queue_message(&"Jobs".into(), &Message::new(Job { n: 1 }))
        .await
        .unwrap();
    let fail = Arc::new(AtomicBool::new(true));
    let (handler, mut deliveries) = {
        let fail = fail.clone();
        recording(move |_| match fail.load(Ordering::SeqCst) {
            true => Err(HandlerError::permanent("downstream rejected the job")),
            false => Ok(()),
        })
    };
    let policy = RedeliveryPolicy::fixed_backoff(ms(50), 3);
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new("Jobs", policy), handler)
        .unwrap();
    next_delivery(&mut deliveries).await;
    eventually("the message to be dead-lettered", || async {
        entry(&queues, &id).await.is_dead_letter_message
    })
    .await;

    let dead_letters = queues
        .get_dead_letter_messages(&"Jobs".into(), QueueingSortOrder::Ascending, 0, 10)
        .await;
    assert_eq!(
        dead_letters
            .unwrap()
            .iter()
            .map(|e| &e.id)
            .collect::<Vec<_>>(),
        [&id]
    );
    fail.store(false, Ordering::SeqCst);
    let resurrected = queues
        .resurrect_dead_letter_message(&id, Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert!(!resurrected.is_dead_letter_message && resurrected.next_delivery_ts.is_some());

    let redelivery = next_delivery(&mut deliveries).await;
    assert_eq!(
        (redelivery.total_attempts, redelivery.redelivery_attempts),
        (2, 0),
        "counters are not reset"
    );
    assert_eq!(
        redelivery.last_delivery_error.as_deref(),
        Some("downstream rejected the job")
    );
    eventually("the message to be acknowledged", || async {
        db.count("SELECT count(*) FROM durable_queues").await == 0
    })
    .await;
    consumer.stop().await;
    assert!(
        queues
            .resurrect_dead_letter_message(&id, Duration::ZERO)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn dead_letters_can_be_marked_by_hand_and_listed_in_either_order() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("Jobs");
    let mut ids = Vec::new();
    for n in 0..4 {
        ids.push(
            queues
                .queue_message_with_delay(&queue, &Message::new(Job { n }), hours(1))
                .await
                .unwrap(),
        );
    }

    for id in &ids[..3] {
        let marked = queues
            .mark_as_dead_letter_message(id, "parked by operator")
            .await
            .unwrap()
            .unwrap();
        assert!(marked.is_dead_letter_message && marked.next_delivery_ts.is_none());
        assert_eq!(
            marked.last_delivery_error.as_deref(),
            Some("parked by operator")
        );
    }
    assert!(
        queues
            .mark_as_dead_letter_message(&ids[0], "again")
            .await
            .unwrap()
            .is_none()
    );

    let page = |order, offset, limit| {
        let (queues, queue) = (queues.clone(), queue.clone());
        async move {
            let entries = queues
                .get_dead_letter_messages(&queue, order, offset, limit)
                .await
                .unwrap();
            entries.into_iter().map(|e| e.id).collect::<Vec<_>>()
        }
    };
    assert_eq!(page(QueueingSortOrder::Ascending, 0, 10).await, ids[..3]);
    assert_eq!(
        page(QueueingSortOrder::Descending, 0, 10).await,
        [ids[2].clone(), ids[1].clone(), ids[0].clone()]
    );
    assert_eq!(
        page(QueueingSortOrder::Ascending, 1, 1).await,
        [ids[1].clone()]
    );
}

#[tokio::test]
async fn a_backed_off_idle_consumer_still_picks_up_new_messages() {
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let (handler, mut deliveries) = recording(|_| Ok(()));
    let config = ConsumerConfig::new("Jobs", RedeliveryPolicy::fixed_backoff(ms(50), 0))
        .with_polling_backoff(PollingBackoff::new(ms(10), ms(200), 2.0, 0.1).unwrap());
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    tokio::time::sleep(ms(600)).await; // long enough to reach the 200 ms cap

    let queued_at = tokio::time::Instant::now();
    queues
        .queue_message(&"Jobs".into(), &Message::new(Job { n: 1 }))
        .await
        .unwrap();
    next_delivery(&mut deliveries).await;
    consumer.stop().await;

    assert!(
        queued_at.elapsed() < ms(1000),
        "picked up within the backoff cap, took {:?}",
        queued_at.elapsed()
    );
}
