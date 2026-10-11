use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::durable_queues::ConsumerRegistration;
use crate::error::{DurableQueuesError, Result};
use crate::message::{HandlerError, QueueMessage, QueuedMessage};
use crate::storage::postgres::{PostgresQueueStorage, QueueEntry};
use crate::types::QueueName;

/// Handles messages claimed from a queue. Delivery is at-least-once, so handlers must be idempotent.
///
/// Implemented for any `Fn(QueuedMessage<M>) -> impl Future<Output = Result<(), HandlerError>>`.
pub trait MessageHandler<M>: Send + Sync + 'static {
    fn handle(
        &self,
        message: QueuedMessage<M>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send;
}

impl<M, F, Fut> MessageHandler<M> for F
where
    F: Fn(QueuedMessage<M>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send,
{
    fn handle(
        &self,
        message: QueuedMessage<M>,
    ) -> impl Future<Output = Result<(), HandlerError>> + Send {
        self(message)
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub queue_name: QueueName,
    /// Messages handled concurrently - Java's `parallelConsumers`. Never more are claimed than there are free slots.
    pub parallel_consumers: usize,
    /// Pause after a poll that did not fill every free slot - Java's centralized fetcher polls every 20 ms.
    pub polling_interval: Duration,
    /// How long [`QueueConsumer::stop`] waits for in-flight handlers. Handlers still running after it are abandoned;
    /// their messages stay claimed until stuck-message recovery redelivers them.
    pub shutdown_timeout: Duration,
}

impl ConsumerConfig {
    pub fn new(queue_name: impl Into<QueueName>) -> Self {
        Self {
            queue_name: queue_name.into(),
            parallel_consumers: 1,
            polling_interval: Duration::from_millis(20),
            shutdown_timeout: Duration::from_secs(10),
        }
    }

    pub fn with_parallel_consumers(mut self, parallel_consumers: usize) -> Self {
        self.parallel_consumers = parallel_consumers;
        self
    }

    pub fn with_polling_interval(mut self, polling_interval: Duration) -> Self {
        self.polling_interval = polling_interval;
        self
    }

    pub fn with_shutdown_timeout(mut self, shutdown_timeout: Duration) -> Self {
        self.shutdown_timeout = shutdown_timeout;
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        // A semaphore holds at most `usize::MAX >> 3` permits; a `u32` is far beyond any sensible consumer count.
        if self.parallel_consumers == 0 || self.parallel_consumers > u32::MAX as usize {
            return Err(DurableQueuesError::InvalidConsumerConfig(
                "parallel_consumers must be between 1 and u32::MAX",
            ));
        }
        Ok(())
    }
}

/// A running consumer. Dropping it stops claiming new messages; [`Self::stop`] also waits for in-flight handlers.
#[derive(Debug)]
pub struct QueueConsumer {
    queue_name: QueueName,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl QueueConsumer {
    pub(crate) fn start<M, H>(
        storage: PostgresQueueStorage,
        config: ConsumerConfig,
        handler: H,
        registration: ConsumerRegistration,
    ) -> Self
    where
        M: QueueMessage,
        H: MessageHandler<M>,
    {
        let cancel = CancellationToken::new();
        let queue_name = config.queue_name.clone();
        let fetcher = Fetcher {
            storage,
            handler: Arc::new(handler),
            slots: Arc::new(Semaphore::new(config.parallel_consumers)),
            config,
            cancel: cancel.clone(),
            _registration: registration,
            _message: PhantomData,
        };
        Self {
            queue_name,
            cancel,
            task: Some(tokio::spawn(fetcher.run())),
        }
    }

    pub fn queue_name(&self) -> &QueueName {
        &self.queue_name
    }

    /// Stops claiming messages and waits up to `shutdown_timeout` for in-flight handlers to finish.
    pub async fn stop(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take()
            && let Err(e) = task.await
        {
            error!(queue = %self.queue_name, error = %e, "consumer task failed");
        }
    }
}

impl Drop for QueueConsumer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

struct Fetcher<M, H> {
    storage: PostgresQueueStorage,
    handler: Arc<H>,
    /// One permit per worker slot; a delivery task holds its permit until the message is settled.
    slots: Arc<Semaphore>,
    config: ConsumerConfig,
    cancel: CancellationToken,
    _registration: ConsumerRegistration,
    _message: PhantomData<fn() -> M>,
}

impl<M: QueueMessage, H: MessageHandler<M>> Fetcher<M, H> {
    async fn run(self) {
        let queue_name = &self.config.queue_name;
        debug!(queue = %queue_name, parallel_consumers = self.config.parallel_consumers, "consumer started");
        let mut in_flight = JoinSet::new();
        loop {
            reap(&mut in_flight, queue_name);
            let mut slots = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => break,
                permit = self.slots.clone().acquire_owned() => permit.expect("the slot semaphore is never closed"),
            };
            let free = self.slots.available_permits();
            if free > 0
                && let Ok(more) = self.slots.clone().try_acquire_many_owned(free as u32)
            {
                slots.merge(more);
            }

            let wanted = slots.num_permits();
            let claimed = match self
                .storage
                .claim_unordered(queue_name, wanted as i64, Utc::now())
                .await
            {
                Ok(entries) => entries,
                Err(e) => {
                    error!(queue = %queue_name, error = %e, "failed to claim messages");
                    Vec::new()
                }
            };
            let filled_every_slot = claimed.len() == wanted;
            for entry in claimed {
                let slot = slots
                    .split(1)
                    .expect("never more messages claimed than slots held");
                in_flight.spawn(deliver(
                    self.storage.clone(),
                    self.handler.clone(),
                    entry,
                    slot,
                ));
            }
            drop(slots);

            if !filled_every_slot {
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    _ = tokio::time::sleep(self.config.polling_interval) => {}
                }
            }
        }
        self.shutdown(in_flight).await;
    }

    async fn shutdown(&self, mut in_flight: JoinSet<()>) {
        let queue_name = &self.config.queue_name;
        let drained = tokio::time::timeout(self.config.shutdown_timeout, async {
            while let Some(result) = in_flight.join_next().await {
                log_task_failure(result, queue_name);
            }
        })
        .await;
        if drained.is_err() {
            warn!(
                queue = %queue_name,
                abandoned = in_flight.len(),
                "handlers still running at shutdown timeout; their messages stay claimed until stuck-message recovery"
            );
            in_flight.abort_all();
        }
        debug!(queue = %queue_name, "consumer stopped");
    }
}

fn reap(in_flight: &mut JoinSet<()>, queue_name: &QueueName) {
    while let Some(result) = in_flight.try_join_next() {
        log_task_failure(result, queue_name);
    }
}

fn log_task_failure(
    result: std::result::Result<(), tokio::task::JoinError>,
    queue_name: &QueueName,
) {
    if let Err(e) = result
        && e.is_panic()
    {
        // TODO(phase 3): treat a panic as a transient handler failure and schedule a retry.
        error!(queue = %queue_name, "message handler panicked; the message stays claimed until stuck-message recovery");
    }
}

/// Decodes, handles and settles one claimed message. Each step that touches the database is its own transaction.
async fn deliver<M: QueueMessage, H: MessageHandler<M>>(
    storage: PostgresQueueStorage,
    handler: Arc<H>,
    entry: QueueEntry,
    _slot: OwnedSemaphorePermit,
) {
    let id = entry.id.clone();
    let queue_name = entry.queue_name.clone();
    let payload_type = entry.message_payload_type.clone();

    let message = match QueuedMessage::<M>::decode(entry) {
        Ok(message) => message,
        Err(e) => {
            // Permanent: dead-letter without reading the row back, like Java's markAsDeadLetterMessageDirect.
            error!(queue = %queue_name, id = %id, payload_type, error = %e, "cannot decode message; dead-lettering it");
            let reason =
                format!("Failed to deserialize message payload of type {payload_type}: {e}");
            if let Err(e) = storage.mark_as_dead_letter_direct(&id, &reason).await {
                error!(queue = %queue_name, id = %id, error = %e, "failed to dead-letter undecodable message");
            }
            return;
        }
    };

    match handler.handle(message).await {
        Ok(()) => match storage.acknowledge(&id).await {
            Ok(true) => debug!(queue = %queue_name, id = %id, "message handled and acknowledged"),
            Ok(false) => {
                error!(queue = %queue_name, id = %id, "could not acknowledge message - it may already have been deleted")
            }
            Err(e) => {
                error!(queue = %queue_name, id = %id, error = %e, "failed to acknowledge message; it will be redelivered")
            }
        },
        // TODO(phase 3): retry with the RedeliveryPolicy or dead-letter (CONTRACT.md §5, §7).
        Err(e) => {
            warn!(queue = %queue_name, id = %id, error = %e, "message handler failed; the message stays claimed until stuck-message recovery")
        }
    }
}
