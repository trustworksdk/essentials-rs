use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
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
    /// With [`FetchPacing::Eager`], the pause after a poll that did not fill every free slot; with
    /// [`FetchPacing::FixedRate`], the time between polls. Java's centralized fetcher polls every 20 ms.
    pub polling_interval: Duration,
    /// When the consumer polls; see [`FetchPacing`].
    pub fetch_pacing: FetchPacing,
    /// How long [`QueueConsumer::stop`] waits for in-flight handlers. Handlers still running after it are abandoned;
    /// their messages stay claimed until stuck-message recovery redelivers them.
    pub shutdown_timeout: Duration,
    /// Optional adaptive pause for idle queues; replaces `polling_interval` after polls that found nothing. Only with
    /// [`FetchPacing::Eager`].
    pub polling_backoff: Option<PollingBackoff>,
}

/// When a consumer polls for messages. Both claim at most the free worker slots, with the same SQL; they differ in
/// throughput once a queue has a backlog. With fixed-rate pacing a consumer handles at most
/// `parallel_consumers / polling_interval` messages a second (400/s with 8 slots and 20 ms); eager pacing has no such
/// ceiling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FetchPacing {
    /// Polls again as soon as a slot frees up after a poll that filled every free slot, and pauses for
    /// `polling_interval` after a poll that did not. Not what Java does.
    #[default]
    Eager,
    /// Java's centralized fetcher (`scheduleAtFixedRate`): one poll every `polling_interval`, skipped while no slot is
    /// free. A poll that overruns its interval is followed by the missed ones at once, as in Java.
    FixedRate,
}

impl ConsumerConfig {
    /// A consumer of `queue_name` with one worker slot, Java's 20 ms polling interval and a 10 s shutdown timeout.
    pub fn new(queue_name: impl Into<QueueName>, redelivery_policy: RedeliveryPolicy) -> Self {
        Self {
            queue_name: queue_name.into(),
            redelivery_policy,
            parallel_consumers: 1,
            polling_interval: Duration::from_millis(20),
            fetch_pacing: FetchPacing::default(),
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

    /// Sets the polling interval; what it means depends on the [`FetchPacing`].
    pub fn with_polling_interval(mut self, polling_interval: Duration) -> Self {
        self.polling_interval = polling_interval;
        self
    }

    /// Sets when the consumer polls; see [`FetchPacing`].
    pub fn with_fetch_pacing(mut self, fetch_pacing: FetchPacing) -> Self {
        self.fetch_pacing = fetch_pacing;
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
        if self.polling_backoff.is_some() && self.fetch_pacing == FetchPacing::FixedRate {
            return Err(DurableQueuesError::InvalidConsumerConfig(
                "polling_backoff requires FetchPacing::Eager",
            ));
        }
        if self.polling_interval.is_zero() && self.fetch_pacing == FetchPacing::FixedRate {
            return Err(DurableQueuesError::InvalidConsumerConfig(
                "FetchPacing::FixedRate needs a polling_interval greater than zero",
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
            in_flight_keys: InFlightKeys::default(),
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

/// The ordering keys of the messages a consumer is handling. Java tracks the same set per node
/// (`inProcessOrderedKeys`) and excludes it from its ordered claims.
#[derive(Debug, Clone, Default)]
struct InFlightKeys(Arc<Mutex<HashSet<String>>>);

impl InFlightKeys {
    fn snapshot(&self) -> Vec<String> {
        self.0.lock().unwrap().iter().cloned().collect()
    }

    /// Marks `key` as in flight until the returned lease drops. `None` if it already is - which the claim's key
    /// exclusion and per-key barrier rule out; it is handled rather than trusted.
    fn lease(&self, key: &str) -> Option<KeyLease> {
        let inserted = self.0.lock().unwrap().insert(key.to_owned());
        if !inserted {
            // A storage that ignores `exclude_keys` - not a reason to stop consuming.
            error!("claimed an ordered message for a key already in flight on this node");
        }
        inserted.then(|| KeyLease {
            keys: self.clone(),
            key: key.to_owned(),
        })
    }
}

/// Keeps an ordering key in flight; held by the delivery task until its message is settled.
struct KeyLease {
    keys: InFlightKeys,
    key: String,
}

impl Drop for KeyLease {
    fn drop(&mut self) {
        self.keys.0.lock().unwrap().remove(&self.key);
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
    /// Ordering keys of the ordered messages this consumer is handling - excluded from its claims, as in Java.
    in_flight_keys: InFlightKeys,
    cancel: CancellationToken,
    _registration: ConsumerRegistration,
    _message: PhantomData<fn() -> M>,
}

impl<M: QueueMessage, H: MessageHandler<M>, S: QueueStorage> Fetcher<M, H, S> {
    async fn run(mut self) {
        let queue_name = self.config.queue_name.clone();
        debug!(queue = %queue_name, parallel_consumers = self.config.parallel_consumers, pacing = ?self.config.fetch_pacing, "consumer started");
        let mut in_flight = JoinSet::new();
        match self.config.fetch_pacing {
            FetchPacing::Eager => self.run_eager(&mut in_flight).await,
            FetchPacing::FixedRate => self.run_fixed_rate(&mut in_flight).await,
        }
        self.shutdown(in_flight).await;
    }

    async fn run_eager(&mut self, in_flight: &mut JoinSet<()>) {
        loop {
            reap(in_flight, &self.config.queue_name);
            let mut slots = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return,
                permit = self.slots.clone().acquire_owned() => permit.expect("the slot semaphore is never closed"),
            };
            let free = self.slots.available_permits();
            if free > 0
                && let Ok(more) = self.slots.clone().try_acquire_many_owned(free as u32)
            {
                slots.merge(more);
            }

            let wanted = slots.num_permits();
            let found = self.poll(slots, in_flight).await;
            if found < wanted {
                let pause = self.pause_after_poll(found);
                tokio::select! {
                    _ = self.cancel.cancelled() => return,
                    _ = tokio::time::sleep(pause) => {}
                }
            } else if let Some(backoff) = &mut self.backoff {
                backoff.reset();
            }
        }
    }

    async fn run_fixed_rate(&mut self, in_flight: &mut JoinSet<()>) {
        // tokio's default `MissedTickBehavior::Burst` catches up on missed ticks, like `scheduleAtFixedRate`
        let mut ticks = tokio::time::interval(self.config.polling_interval);
        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return,
                _ = ticks.tick() => {}
            }
            reap(in_flight, &self.config.queue_name);
            let free = self.slots.available_permits();
            // Java: "No worker capacity available, skipping fetch and distribute"
            if free == 0 {
                continue;
            }
            if let Ok(slots) = self.slots.clone().try_acquire_many_owned(free as u32) {
                self.poll(slots, in_flight).await;
            }
        }
    }

    /// One claim for the `slots` held, each claimed message spawned as a delivery holding one of them. Returns the
    /// number claimed; unused slots are released.
    async fn poll(
        &mut self,
        mut slots: OwnedSemaphorePermit,
        in_flight: &mut JoinSet<()>,
    ) -> usize {
        let queue_name = &self.config.queue_name;
        let wanted = slots.num_permits();
        let now = Utc::now();
        let reset_stuck = self.stuck_message_reset_due(now);
        let exclude_keys = self.in_flight_keys.snapshot();
        let claimed = match self
            .storage
            .claim(queue_name, wanted as i64, now, reset_stuck, &exclude_keys)
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
            let key = entry
                .key
                .as_deref()
                .and_then(|key| self.in_flight_keys.lease(key));
            let delivery = deliver(
                self.storage.clone(),
                self.handler.clone(),
                self.redelivery_policy.clone(),
                entry,
                slot,
                key,
            );
            in_flight.spawn(delivery.instrument(span));
        }
        found
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
    _key: Option<KeyLease>,
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
