//! Phase 4 edge cases: a failed acknowledgement, a database restart, an exhausted connection pool and a handler that
//! outlives the handling timeout. (Shutdown with in-flight messages is covered in `consume.rs`.)

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::{TestDatabase, eventually};
use durable_queues_rs::{
    ClaimResult, ConsumerConfig, DurableQueues, DurableQueuesError, Message, MessageType,
    NewEntries, PostgresQueueStorage, QueueEntry, QueueEntryId, QueueName, QueueStorage,
    QueuedMessage, QueueingSortOrder, RedeliveryPolicy, Result, STUCK_MESSAGE_ERROR,
    StuckMessageReset,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    n: usize,
}

impl MessageType for Job {
    const TYPE_NAME: &'static str = "com.acme.Job";
}

fn policy() -> RedeliveryPolicy {
    RedeliveryPolicy::fixed_backoff(Duration::from_millis(50), 5)
}

/// Real PostgreSQL storage whose first `failures` acknowledgements fail as if the connection had dropped after the
/// handler finished.
#[derive(Clone)]
struct FailingAcknowledgements {
    inner: PostgresQueueStorage,
    failures: Arc<AtomicUsize>,
}

impl QueueStorage for FailingAcknowledgements {
    async fn insert_messages(
        &self,
        queue_name: &QueueName,
        entries: NewEntries,
        added_ts: DateTime<Utc>,
        next_delivery_ts: DateTime<Utc>,
    ) -> Result<u64> {
        self.inner
            .insert_messages(queue_name, entries, added_ts, next_delivery_ts)
            .await
    }

    async fn claim(
        &self,
        queue_name: &QueueName,
        limit: i64,
        now: DateTime<Utc>,
        reset_stuck: Option<StuckMessageReset>,
        exclude_keys: &[String],
    ) -> Result<ClaimResult> {
        self.inner
            .claim(queue_name, limit, now, reset_stuck, exclude_keys)
            .await
    }

    async fn has_ordered_message_queued_for_key(
        &self,
        queue_name: &QueueName,
        key: &str,
    ) -> Result<bool> {
        self.inner
            .has_ordered_message_queued_for_key(queue_name, key)
            .await
    }

    async fn acknowledge(&self, id: &QueueEntryId) -> Result<bool> {
        let fail = self
            .failures
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if fail {
            return Err(DurableQueuesError::Database(sqlx::Error::Protocol(
                "connection reset (simulated)".into(),
            )));
        }
        self.inner.acknowledge(id).await
    }

    async fn retry(
        &self,
        id: &QueueEntryId,
        next_delivery_ts: DateTime<Utc>,
        error: &str,
    ) -> Result<bool> {
        self.inner.retry(id, next_delivery_ts, error).await
    }

    async fn mark_as_dead_letter(
        &self,
        id: &QueueEntryId,
        reason: &str,
    ) -> Result<Option<QueueEntry>> {
        self.inner.mark_as_dead_letter(id, reason).await
    }

    async fn mark_as_dead_letter_direct(&self, id: &QueueEntryId, reason: &str) -> Result<bool> {
        self.inner.mark_as_dead_letter_direct(id, reason).await
    }

    async fn resurrect_dead_letter(
        &self,
        id: &QueueEntryId,
        next: DateTime<Utc>,
    ) -> Result<Option<QueueEntry>> {
        self.inner.resurrect_dead_letter(id, next).await
    }

    async fn dead_letters(
        &self,
        queue_name: &QueueName,
        order: QueueingSortOrder,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<QueueEntry>> {
        self.inner
            .dead_letters(queue_name, order, offset, limit)
            .await
    }

    async fn find_entry(&self, id: &QueueEntryId) -> Result<Option<QueueEntry>> {
        self.inner.find_entry(id).await
    }
}

#[tokio::test]
async fn a_failed_acknowledgement_leads_to_a_duplicate_delivery_after_the_handling_timeout() {
    let db = TestDatabase::start().await;
    let storage = PostgresQueueStorage::new(db.pool.clone(), Default::default())
        .await
        .unwrap();
    let failing = FailingAcknowledgements {
        inner: storage,
        failures: Arc::new(AtomicUsize::new(1)),
    };
    let queues = DurableQueues::new(failing).with_message_handling_timeout(Duration::from_secs(1));
    let id = queues
        .queue_message(&"Jobs".into(), &Message::new(Job { n: 1 }))
        .await
        .unwrap();

    let (sender, mut deliveries) = mpsc::unbounded_channel::<QueuedMessage<Job>>();
    let handler = move |m: QueuedMessage<Job>| {
        sender.send(m).unwrap();
        async { Ok(()) }
    };
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new("Jobs", policy()), handler)
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
        .await
        .unwrap()
        .unwrap();
    eventually(
        "the second acknowledgement to delete the message",
        || async { db.count("SELECT count(*) FROM durable_queues").await == 0 },
    )
    .await;
    consumer.stop().await;

    // At-least-once: the handler ran twice for one message; its side effects must be idempotent.
    assert_eq!((first.id.clone(), second.id.clone()), (id.clone(), id));
    assert_eq!((second.total_attempts, second.redelivery_attempts), (2, 1));
    assert_eq!(
        second.last_delivery_error.as_deref(),
        Some(STUCK_MESSAGE_ERROR)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumers_survive_a_database_restart_and_every_message_is_handled() {
    const MESSAGES: usize = 200;
    let db = TestDatabase::start_restartable().await;
    let queues = db
        .durable_queues(db.node_pool(4).await)
        .await
        .with_message_handling_timeout(Duration::from_secs(2));
    let queue = QueueName::from("Jobs");
    let jobs: Vec<_> = (0..MESSAGES).map(|n| Message::new(Job { n })).collect();
    let sent = queues.queue_messages(&queue, &jobs).await.unwrap();

    let handled = Arc::new(Mutex::new(HashMap::<QueueEntryId, usize>::new()));
    let handler = {
        let handled = handled.clone();
        move |m: QueuedMessage<Job>| {
            *handled.lock().unwrap().entry(m.id).or_insert(0) += 1;
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            }
        }
    };
    let config = ConsumerConfig::new(queue, policy()).with_parallel_consumers(4);
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    eventually("some messages to be handled before the restart", || async {
        handled.lock().unwrap().len() >= 20
    })
    .await;

    db.restart(Duration::from_secs(2)).await;

    eventually(
        "every message to be handled and the queue drained",
        || async {
            handled.lock().unwrap().len() == MESSAGES
                && db.count("SELECT count(*) FROM durable_queues").await == 0
        },
    )
    .await;
    consumer.stop().await;

    let handled = handled.lock().unwrap();
    assert!(
        sent.iter().all(|id| handled.contains_key(id)),
        "every message handled at least once"
    );
    // Messages in flight when the database went away may be handled twice (their acknowledgement failed);
    // everything else exactly once.
    let duplicates = handled.values().filter(|&&n| n > 1).count();
    assert!(
        duplicates <= 4,
        "only in-flight messages may repeat, got {duplicates}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn more_parallel_consumers_than_pooled_connections_still_handle_every_message_once() {
    const MESSAGES: usize = 100;
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.node_pool(2).await).await;
    let jobs: Vec<_> = (0..MESSAGES).map(|n| Message::new(Job { n })).collect();
    queues.queue_messages(&"Jobs".into(), &jobs).await.unwrap();

    let handled = Arc::new(Mutex::new(HashMap::<QueueEntryId, usize>::new()));
    let handler = {
        let handled = handled.clone();
        move |m: QueuedMessage<Job>| {
            *handled.lock().unwrap().entry(m.id).or_insert(0) += 1;
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            }
        }
    };
    let config = ConsumerConfig::new("Jobs", policy()).with_parallel_consumers(8);
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    eventually("the queue to be drained", || async {
        db.count("SELECT count(*) FROM durable_queues").await == 0
    })
    .await;
    consumer.stop().await;

    let handled = handled.lock().unwrap();
    assert_eq!(handled.len(), MESSAGES);
    assert!(
        handled.values().all(|&n| n == 1),
        "a starved pool delays acknowledgements but never duplicates"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handler_that_outlives_the_handling_timeout_sees_its_message_delivered_again() {
    // Expected at-least-once behaviour, the same in Java: after `message_handling_timeout` the claim counts as stuck
    // and the message is delivered again - here to the same consumer's second slot - while the first handler still
    // runs. Whichever handler finishes first deletes the row; the other acknowledgement finds nothing.
    let db = TestDatabase::start().await;
    let queues = db
        .durable_queues(db.pool.clone())
        .await
        .with_message_handling_timeout(Duration::from_secs(1));
    let id = queues
        .queue_message(&"Jobs".into(), &Message::new(Job { n: 1 }))
        .await
        .unwrap();

    let (sender, mut deliveries) = mpsc::unbounded_channel::<QueuedMessage<Job>>();
    let handler = move |m: QueuedMessage<Job>| {
        let slow = m.total_attempts == 1;
        sender.send(m).unwrap();
        async move {
            if slow {
                tokio::time::sleep(Duration::from_millis(3500)).await;
            }
            Ok(())
        }
    };
    let config = ConsumerConfig::new("Jobs", policy()).with_parallel_consumers(2);
    let consumer = queues.consume_from_queue(config, handler).unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(10), deliveries.recv())
        .await
        .unwrap()
        .unwrap();
    consumer.stop().await;

    assert_eq!((first.id.clone(), second.id.clone()), (id.clone(), id));
    assert_eq!(
        second.last_delivery_error.as_deref(),
        Some(STUCK_MESSAGE_ERROR)
    );
    assert!(
        second.delivery_ts.unwrap() - first.delivery_ts.unwrap() >= chrono::TimeDelta::seconds(1)
    );
    assert_eq!(db.count("SELECT count(*) FROM durable_queues").await, 0);
}
