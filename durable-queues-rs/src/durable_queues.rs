use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgConnection;

use crate::consumer::{ConsumerConfig, MessageHandler, QueueConsumer};
use crate::error::{DurableQueuesError, Result};
use crate::message::{Message, QueueMessage};
use crate::storage::postgres::PostgresQueueStorage;
use crate::storage::{NewEntries, QueueEntry, QueueStorage, QueueingSortOrder};
use crate::types::{QueueEntryId, QueueName};

/// Queues messages and runs consumers on one [`QueueStorage`] - the Rust counterpart of Java's `DurableQueues`.
/// Cheap to clone; clones share the same consumers.
#[derive(Debug, Clone)]
pub struct DurableQueues<S = PostgresQueueStorage> {
    storage: S,
    active_consumers: Arc<Mutex<HashSet<QueueName>>>,
    message_handling_timeout: Duration,
}

impl<S: QueueStorage> DurableQueues<S> {
    /// Java's `DEFAULT_MESSAGE_HANDLING_TIMEOUT`.
    pub const DEFAULT_MESSAGE_HANDLING_TIMEOUT: Duration = Duration::from_secs(30);

    pub fn new(storage: S) -> Self {
        Self {
            storage,
            active_consumers: Arc::default(),
            message_handling_timeout: Self::DEFAULT_MESSAGE_HANDLING_TIMEOUT,
        }
    }

    /// How long a message may stay claimed before stuck-message recovery returns it to the queue
    /// (Java's `messageHandlingTimeout`, default 30 s). Applies to consumers started afterwards.
    /// A handler that runs longer than this may see its message delivered again elsewhere -
    /// handlers must be idempotent.
    pub fn with_message_handling_timeout(mut self, timeout: Duration) -> Self {
        self.message_handling_timeout = timeout;
        self
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Queues one message for immediate delivery, in its own transaction.
    pub async fn queue_message<M: QueueMessage>(
        &self,
        queue_name: &QueueName,
        message: &Message<M>,
    ) -> Result<QueueEntryId> {
        self.queue_message_with_delay(queue_name, message, Duration::ZERO)
            .await
    }

    /// Queues one message, deliverable once `delay` has passed.
    pub async fn queue_message_with_delay<M: QueueMessage>(
        &self,
        queue_name: &QueueName,
        message: &Message<M>,
        delay: Duration,
    ) -> Result<QueueEntryId> {
        let mut ids = self
            .insert(queue_name, std::slice::from_ref(message), delay)
            .await?;
        Ok(ids.remove(0))
    }

    /// Queues messages in one statement; all are inserted or none. Ids are returned in input order.
    pub async fn queue_messages<M: QueueMessage>(
        &self,
        queue_name: &QueueName,
        messages: &[Message<M>],
    ) -> Result<Vec<QueueEntryId>> {
        self.insert(queue_name, messages, Duration::ZERO).await
    }

    async fn insert<M: QueueMessage>(
        &self,
        queue_name: &QueueName,
        messages: &[Message<M>],
        delay: Duration,
    ) -> Result<Vec<QueueEntryId>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        let (entries, ids) = new_entries(messages)?;
        let now = Utc::now();
        self.storage
            .insert_messages(queue_name, entries, now, after(now, delay))
            .await?;
        tracing::debug!(queue = %queue_name, count = ids.len(), "queued messages");
        Ok(ids)
    }

    /// Starts consuming `config.queue_name`. One consumer per queue per `DurableQueues`, as in Java; stop the
    /// returned consumer before starting another on the same queue.
    pub fn consume_from_queue<M, H>(
        &self,
        config: ConsumerConfig,
        handler: H,
    ) -> Result<QueueConsumer>
    where
        M: QueueMessage,
        H: MessageHandler<M>,
    {
        config.validate()?;
        let registration =
            ConsumerRegistration::register(&self.active_consumers, &config.queue_name)?;
        Ok(QueueConsumer::start(
            self.storage.clone(),
            config,
            handler,
            self.message_handling_timeout,
            registration,
        ))
    }

    /// A page of `queue_name`'s dead letters, oldest first for [`QueueingSortOrder::Ascending`]. Returned as stored
    /// rows: a dead letter is often one that cannot be decoded.
    pub async fn get_dead_letter_messages(
        &self,
        queue_name: &QueueName,
        order: QueueingSortOrder,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<QueueEntry>> {
        self.storage
            .dead_letters(queue_name, order, offset, limit)
            .await
    }

    /// Makes a dead letter deliverable again after `delay`. Its delivery counters are kept, as in Java, so a message
    /// whose redeliveries were exhausted is dead-lettered again by its next failure. `None` if `id` is not a dead letter.
    pub async fn resurrect_dead_letter_message(
        &self,
        id: &QueueEntryId,
        delay: Duration,
    ) -> Result<Option<QueueEntry>> {
        self.storage
            .resurrect_dead_letter(id, after(Utc::now(), delay))
            .await
    }

    /// Dead-letters a message by hand. `None` if it does not exist or already is a dead letter.
    pub async fn mark_as_dead_letter_message(
        &self,
        id: &QueueEntryId,
        reason: &str,
    ) -> Result<Option<QueueEntry>> {
        self.storage.mark_as_dead_letter(id, reason).await
    }
}

/// Joining the caller's transaction is a PostgreSQL capability, so it is not part of [`QueueStorage`].
impl DurableQueues<PostgresQueueStorage> {
    /// Queues one message as part of the caller's transaction, so it commits or rolls back with the caller's own
    /// writes - the Outbox use case. Pass `&mut *tx` for a `sqlx::Transaction`.
    pub async fn queue_message_in<M: QueueMessage>(
        &self,
        connection: &mut PgConnection,
        queue_name: &QueueName,
        message: &Message<M>,
    ) -> Result<QueueEntryId> {
        let mut ids = self
            .queue_messages_in(connection, queue_name, std::slice::from_ref(message))
            .await?;
        Ok(ids.remove(0))
    }

    /// [`Self::queue_messages`] as part of the caller's transaction.
    pub async fn queue_messages_in<M: QueueMessage>(
        &self,
        connection: &mut PgConnection,
        queue_name: &QueueName,
        messages: &[Message<M>],
    ) -> Result<Vec<QueueEntryId>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        let (entries, ids) = new_entries(messages)?;
        let now = Utc::now();
        self.storage
            .insert_messages_with(connection, queue_name, entries, now, now)
            .await?;
        tracing::debug!(queue = %queue_name, count = ids.len(), "queued messages in caller's transaction");
        Ok(ids)
    }
}

fn new_entries<M: QueueMessage>(
    messages: &[Message<M>],
) -> Result<(NewEntries, Vec<QueueEntryId>)> {
    let mut entries = NewEntries::default();
    let mut ids = Vec::with_capacity(messages.len());
    for message in messages {
        let id = QueueEntryId::random();
        entries.ids.push(id.as_str().to_owned());
        entries.payloads.push(message.payload.to_payload()?);
        entries
            .payload_types
            .push(message.payload.payload_type().to_owned());
        entries.meta_data.push(message.metadata.to_json());
        ids.push(id);
    }
    Ok((entries, ids))
}

/// A queue's slot in [`DurableQueues`]' consumer set, released when the consumer's task ends.
pub(crate) struct ConsumerRegistration {
    active_consumers: Arc<Mutex<HashSet<QueueName>>>,
    queue_name: QueueName,
}

impl ConsumerRegistration {
    fn register(
        active_consumers: &Arc<Mutex<HashSet<QueueName>>>,
        queue_name: &QueueName,
    ) -> Result<Self> {
        if !active_consumers.lock().unwrap().insert(queue_name.clone()) {
            return Err(DurableQueuesError::ConsumerAlreadyExists(
                queue_name.clone(),
            ));
        }
        Ok(Self {
            active_consumers: active_consumers.clone(),
            queue_name: queue_name.clone(),
        })
    }
}

impl Drop for ConsumerRegistration {
    fn drop(&mut self) {
        self.active_consumers
            .lock()
            .unwrap()
            .remove(&self.queue_name);
    }
}

/// `now + delay`, saturating instead of overflowing.
pub(crate) fn after(now: DateTime<Utc>, delay: Duration) -> DateTime<Utc> {
    TimeDelta::from_std(delay)
        .ok()
        .and_then(|delay| now.checked_add_signed(delay))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}
