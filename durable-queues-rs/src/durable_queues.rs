use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use sqlx::PgConnection;

use crate::consumer::{ConsumerConfig, MessageHandler, QueueConsumer};
use crate::error::{DurableQueuesError, Result};
use crate::message::{Message, QueueMessage};
use crate::storage::postgres::{NewEntries, PostgresQueueStorage};
use crate::types::{QueueEntryId, QueueName};

/// Queues messages and runs consumers on one queue table - the Rust counterpart of Java's `DurableQueues`.
/// Cheap to clone; clones share the same consumers.
#[derive(Debug, Clone)]
pub struct DurableQueues {
    storage: PostgresQueueStorage,
    active_consumers: Arc<Mutex<HashSet<QueueName>>>,
}

impl DurableQueues {
    pub fn new(storage: PostgresQueueStorage) -> Self {
        Self {
            storage,
            active_consumers: Arc::default(),
        }
    }

    pub fn storage(&self) -> &PostgresQueueStorage {
        &self.storage
    }

    /// Queues one message for immediate delivery, in its own transaction.
    pub async fn queue_message<M: QueueMessage>(
        &self,
        queue_name: &QueueName,
        message: &Message<M>,
    ) -> Result<QueueEntryId> {
        let mut ids = self
            .queue_messages(queue_name, std::slice::from_ref(message))
            .await?;
        Ok(ids.remove(0))
    }

    /// Queues messages in one statement; all are inserted or none. Ids are returned in input order.
    pub async fn queue_messages<M: QueueMessage>(
        &self,
        queue_name: &QueueName,
        messages: &[Message<M>],
    ) -> Result<Vec<QueueEntryId>> {
        self.insert(self.storage.pool(), queue_name, messages).await
    }

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
        self.insert(connection, queue_name, messages).await
    }

    async fn insert<M: QueueMessage>(
        &self,
        executor: impl sqlx::PgExecutor<'_>,
        queue_name: &QueueName,
        messages: &[Message<M>],
    ) -> Result<Vec<QueueEntryId>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
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
        self.storage
            .insert_messages(executor, queue_name, entries, Utc::now())
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
            registration,
        ))
    }
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
