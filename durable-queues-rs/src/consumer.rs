use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use std::panic::AssertUnwindSafe;

use chrono::Utc;
use futures_util::FutureExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, error, field, info_span, warn};

use crate::durable_queues::{ConsumerRegistration, after};
use crate::error::{DurableQueuesError, Result};
use crate::message::{HandlerError, QueueMessage, QueuedMessage};
use crate::polling::{PollingBackoff, PollingBackoffState};
use crate::redelivery::{DeadLetterReason, FailureOutcome, RedeliveryPolicy};
use crate::storage::{QueueEntry, QueueStorage, StuckMessageReset};
use crate::types::QueueName;

/// Handles messages claimed from a queue. Delivery is at-least-once, so handlers must be idempotent.
///
/// Implemented for any `Fn(QueuedMessage<M>) -> impl Future<Output = Result<(), HandlerError>>`.
pub trait MessageHandler<M>: Send + Sync + 'static {
    /// Handles one delivery: `Ok` acknowledges it, an error settles it as described on [`HandlerError`].
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
/// How a consumer runs - Java's `ConsumeFromQueue`.
pub struct ConsumerConfig {
    /// The queue to consume.
    pub queue_name: QueueName,
    /// Required, as in Java: what happens to a message whose handler failed.
    pub redelivery_policy: RedeliveryPolicy,
    /// Messages handled concurrently - Java's `parallelConsumers`. Never more are claimed than there are free slots.
    pub parallel_consumers: usize,
    /// Pause after a poll that did not fill every free slot - Java's centralized fetcher polls every 20 ms.
    pub polling_interval: Duration,
    /// How long [`QueueConsumer::stop`] waits for in-flight handlers. Handlers still running after it are abandoned;
    /// their messages stay claimed until stuck-message recovery redelivers them.
    pub shutdown_timeout: Duration,
    /// Optional adaptive pause for idle queues; replaces `polling_interval` after polls that found nothing.
    pub polling_backoff: Option<PollingBackoff>,
}

impl ConsumerConfig {
    /// A consumer of `queue_name` with one worker slot, Java's 20 ms polling interval and a 10 s shutdown timeout.
    pub fn new(queue_name: impl Into<QueueName>, redelivery_policy: RedeliveryPolicy) -> Self {
        Self {
            queue_name: queue_name.into(),
            redelivery_policy,
            parallel_consumers: 1,
            polling_interval: Duration::from_millis(20),
            shutdown_timeout: Duration::from_secs(10),
            polling_backoff: None,
        }
    }

    /// Enables adaptive polling for idle queues; see [`PollingBackoff`].
    pub fn with_polling_backoff(mut self, polling_backoff: PollingBackoff) -> Self {
        self.polling_backoff = Some(polling_backoff);
        self
    }

    /// Sets how many messages are handled concurrently.
    pub fn with_parallel_consumers(mut self, parallel_consumers: usize) -> Self {
        self.parallel_consumers = parallel_consumers;
        self
    }

    /// Sets the pause after a poll that did not fill every free slot.
    pub fn with_polling_interval(mut self, polling_interval: Duration) -> Self {
        self.polling_interval = polling_interval;
        self
    }

    /// Sets how long [`QueueConsumer::stop`] waits for in-flight handlers.
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
    pub(crate) fn start<M, H, S>(
        storage: S,
        config: ConsumerConfig,
        handler: H,
        message_handling_timeout: Duration,
        registration: ConsumerRegistration,
    ) -> Self
    where
        M: QueueMessage,
        H: MessageHandler<M>,
        S: QueueStorage,
    {
        let cancel = CancellationToken::new();
        let queue_name = config.queue_name.clone();
        let queue_name_for_span = queue_name.clone();
        let fetcher = Fetcher {
            storage,
            handler: Arc::new(handler),
            slots: Arc::new(Semaphore::new(config.parallel_consumers)),
            redelivery_policy: Arc::new(config.redelivery_policy.clone()),
            backoff: config.polling_backoff.map(PollingBackoffState::new),
            config,
            message_handling_timeout,
            last_stuck_reset: None,
            cancel: cancel.clone(),
            _registration: registration,
            _message: PhantomData,
        };
        Self {
            queue_name,
            cancel,
            task: Some(tokio::spawn(fetcher.run().instrument(info_span!(
                "durable_queue_consumer",
                queue_name = %queue_name_for_span,
            )))),
        }
    }

    /// The queue this consumer consumes.
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

struct Fetcher<M, H, S> {
    storage: S,
    handler: Arc<H>,
    /// One permit per worker slot; a delivery task holds its permit until the message is settled.
    slots: Arc<Semaphore>,
    redelivery_policy: Arc<RedeliveryPolicy>,
    backoff: Option<PollingBackoffState>,
    config: ConsumerConfig,
    /// Java's `messageHandlingTimeout`: a message claimed longer ago than this is considered stuck.
    message_handling_timeout: Duration,
    last_stuck_reset: Option<Instant>,
    cancel: CancellationToken,
    _registration: ConsumerRegistration,
    _message: PhantomData<fn() -> M>,
}

impl<M: QueueMessage, H: MessageHandler<M>, S: QueueStorage> Fetcher<M, H, S> {
    async fn run(mut self) {
        let queue_name = self.config.queue_name.clone();
        debug!(queue = %queue_name, parallel_consumers = self.config.parallel_consumers, "consumer started");
        let mut in_flight = JoinSet::new();
        loop {
            reap(&mut in_flight, &queue_name);
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
            let now = Utc::now();
            let reset_stuck = self.stuck_message_reset_due(now);
            let claimed = match self
                .storage
                .claim_unordered(&queue_name, wanted as i64, now, reset_stuck)
                .await
            {
                Ok(result) => {
                    if reset_stuck.is_some() {
                        self.last_stuck_reset = Some(Instant::now());
                        if result.reset > 0 {
                            warn!(queue = %queue_name, count = result.reset, "returned messages stuck in delivery to the queue");
                        }
                    }
                    result.claimed
                }
                Err(e) => {
                    error!(queue = %queue_name, error = %e, "failed to claim messages");
                    Vec::new()
                }
            };
            let found = claimed.len();
            for entry in claimed {
                let slot = slots
                    .split(1)
                    .expect("never more messages claimed than slots held");
                let span = info_span!(
                    "deliver_message",
                    queue_name = %entry.queue_name,
                    queue_entry_id = %entry.id,
                    attempt = entry.total_attempts,
                    redelivery_attempts = entry.redelivery_attempts,
                    outcome = field::Empty,
                );
                let delivery = deliver(
                    self.storage.clone(),
                    self.handler.clone(),
                    self.redelivery_policy.clone(),
                    entry,
                    slot,
                );
                in_flight.spawn(delivery.instrument(span));
            }
            drop(slots);

            if found < wanted {
                let pause = self.pause_after_poll(found);
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    _ = tokio::time::sleep(pause) => {}
                }
            } else if let Some(backoff) = &mut self.backoff {
                backoff.reset();
            }
        }
        self.shutdown(in_flight).await;
    }

    /// Java throttles the stuck-message reset to once per `messageHandlingTimeout` per queue; the first poll always
    /// resets, so a restarted node immediately recovers what a crashed one left claimed.
    fn stuck_message_reset_due(&self, now: chrono::DateTime<Utc>) -> Option<StuckMessageReset> {
        let due = self
            .last_stuck_reset
            .is_none_or(|last| last.elapsed() > self.message_handling_timeout);
        let timeout = chrono::TimeDelta::from_std(self.message_handling_timeout)
            .unwrap_or(chrono::TimeDelta::MAX);
        due.then(|| StuckMessageReset {
            claimed_before: now.checked_sub_signed(timeout).unwrap_or(now),
        })
    }

    fn pause_after_poll(&mut self, found: usize) -> Duration {
        match &mut self.backoff {
            Some(backoff) if found == 0 => backoff.after_empty_poll(),
            Some(backoff) => {
                backoff.reset();
                self.config.polling_interval
            }
            None => self.config.polling_interval,
        }
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
        // Handler panics are caught in `deliver`; this is a panic while settling, e.g. in a log formatter.
        error!(queue = %queue_name, "delivery task panicked; its message stays claimed until stuck-message recovery");
    }
}

/// Decodes, handles and settles one claimed message. Each step that touches the database is its own transaction.
/// Runs inside the `deliver_message` span, which carries the queue, the message id and the attempt, and records
/// the outcome. Nothing here logs payloads.
async fn deliver<M: QueueMessage, H: MessageHandler<M>, S: QueueStorage>(
    storage: S,
    handler: Arc<H>,
    redelivery_policy: Arc<RedeliveryPolicy>,
    entry: QueueEntry,
    _slot: OwnedSemaphorePermit,
) {
    let id = entry.id.clone();
    let payload_type = entry.message_payload_type.clone();
    let (total_attempts, redelivery_attempts) = (entry.total_attempts, entry.redelivery_attempts);

    let message = match QueuedMessage::<M>::decode(entry) {
        Ok(message) => message,
        Err(e) => {
            // Permanent: dead-letter without reading the row back, like Java's markAsDeadLetterMessageDirect.
            // The stored reason keeps serde's full message; the log gets a redacted one, which cannot quote the payload.
            record_outcome("dead_lettered_undecodable");
            error!(payload_type, error = %e.redacted(), "cannot decode message; dead-lettering it");
            let reason =
                format!("Failed to deserialize message payload of type {payload_type}: {e}");
            if let Err(e) = storage.mark_as_dead_letter_direct(&id, &reason).await {
                error!(error = %e, "failed to dead-letter undecodable message");
            }
            return;
        }
    };

    // The call to `handle` sits inside the guarded future: a closure handler can panic before it returns its future.
    let handled = AssertUnwindSafe(async move { handler.handle(message).await });
    let error = match handled.catch_unwind().await {
        Ok(Ok(())) => {
            match storage.acknowledge(&id).await {
                Ok(true) => {
                    record_outcome("acknowledged");
                    debug!("message handled and acknowledged");
                }
                Ok(false) => {
                    record_outcome("acknowledge_missed");
                    error!("could not acknowledge message - it may already have been deleted");
                }
                Err(e) => {
                    record_outcome("acknowledge_failed");
                    error!(error = %e, "failed to acknowledge message; it will be redelivered");
                }
            }
            return;
        }
        Ok(Err(error)) => error,
        // Java catches every Throwable from a handler; a panic is the Rust equivalent of an unchecked exception.
        Err(panic) => HandlerError::transient(format!(
            "message handler panicked: {}",
            panic_message(&*panic)
        )),
    };

    let report = error.report();
    let settled = match redelivery_policy.decide(&error, total_attempts, redelivery_attempts) {
        FailureOutcome::Retry { delay } => {
            record_outcome("retry");
            debug!(error = %error, ?delay, "message handling failed; will redeliver");
            storage
                .retry(&id, after(Utc::now(), delay), &report)
                .await
                .map(|_| ())
        }
        FailureOutcome::RedeliveryRequested { delay } => {
            record_outcome("redelivery_requested");
            debug!(?delay, "handler requested redelivery");
            storage
                .retry(&id, after(Utc::now(), delay), &report)
                .await
                .map(|_| ())
        }
        FailureOutcome::DeadLetter { reason } => {
            record_outcome("dead_lettered");
            match reason {
                DeadLetterReason::PermanentError => {
                    error!(error = %error, "permanent error; marking message as dead letter")
                }
                DeadLetterReason::RedeliveriesExhausted => {
                    warn!(error = %error, "too many deliveries; marking message as dead letter")
                }
            }
            storage.mark_as_dead_letter(&id, &report).await.map(|_| ())
        }
    };
    if let Err(e) = settled {
        error!(error = %e, "failed to settle failed message; stuck-message recovery will redeliver it");
    }
}

fn record_outcome(outcome: &'static str) {
    Span::current().record("outcome", outcome);
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<non-string panic payload>")
}
