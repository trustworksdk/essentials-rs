use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::StreamExt;
use tokio::sync::{Notify, watch};

use super::{DeliveryJob, DeliveryOutcome, DeliveryResult};
use crate::engine::{DispatchError, Dispatcher};

/// Exponential backoff with jitter, and the attempt budget before a delivery is dead-lettered.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Total attempts, the first included.
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl RetryPolicy {
    /// Delay after the `failed`-th failed attempt: `initial * 2^(failed-1)`, capped, then drawn uniformly from its
    /// upper half ("equal jitter") so retries of many deliveries spread out.
    pub fn backoff(&self, failed: u32) -> Duration {
        let exponent = failed.saturating_sub(1).min(31);
        let base = self
            .initial_backoff
            .saturating_mul(1 << exponent)
            .min(self.max_backoff);
        let half = base / 2;
        half + half.mul_f64(fastrand::f64())
    }

    /// The outcome of a failed attempt, given the attempts made before it.
    pub fn after_failure(&self, attempts_before: u32, error: String) -> DeliveryOutcome {
        let failed = attempts_before + 1;
        if failed >= self.max_attempts {
            DeliveryOutcome::Dead { error }
        } else {
            let delay =
                chrono::Duration::from_std(self.backoff(failed)).unwrap_or(chrono::Duration::MAX);
            DeliveryOutcome::Retry {
                at: Utc::now() + delay,
                error,
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// How often to look for due deliveries when no commit signalled new ones (retries become due this way).
    pub poll_interval: Duration,
    /// Deliveries claimed per round.
    pub batch_size: u32,
    /// Concurrent HTTP requests.
    pub concurrency: usize,
    pub request_timeout: Duration,
    pub retry: RetryPolicy,
}

impl WorkerConfig {
    /// Claim lease: long enough for a whole batch to time out request by request, plus a margin. A delivery whose
    /// result is never recorded becomes due again after it.
    pub fn lease(&self) -> Duration {
        let rounds = u32::try_from((self.batch_size as usize).div_ceil(self.concurrency.max(1)))
            .unwrap_or(u32::MAX);
        self.request_timeout.saturating_mul(rounds) + Duration::from_secs(30)
    }
}

/// Background task that delivers due webhook deliveries.
pub struct DeliveryWorker;

impl DeliveryWorker {
    /// Runs until `shutdown` turns true (finishing the batch in hand) or the exchange stops.
    pub fn spawn(
        dispatcher: Dispatcher,
        signal: Arc<Notify>,
        config: WorkerConfig,
        mut shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let client = match reqwest::Client::builder()
                .timeout(config.request_timeout)
                .build()
            {
                Ok(client) => client,
                Err(error) => {
                    tracing::error!(%error, "cannot build webhook HTTP client; webhook delivery disabled");
                    return;
                }
            };
            loop {
                if *shutdown.borrow() {
                    break;
                }
                match deliver_due(&dispatcher, &client, &config).await {
                    Ok(()) => {}
                    Err(DispatchError::Stopped) => break,
                    Err(error) => {
                        tracing::warn!(%error, "webhook delivery round failed; retrying on next poll")
                    }
                }
                tokio::select! {
                    _ = shutdown.changed() => {}
                    () = signal.notified() => {}
                    () = tokio::time::sleep(config.poll_interval) => {}
                }
            }
            tracing::info!("webhook delivery worker stopped");
        })
    }
}

/// Claims and delivers batches until nothing is due.
async fn deliver_due(
    dispatcher: &Dispatcher,
    client: &reqwest::Client,
    config: &WorkerConfig,
) -> Result<(), DispatchError> {
    loop {
        let jobs = dispatcher
            .claim_deliveries(config.batch_size, config.lease())
            .await?;
        if jobs.is_empty() {
            return Ok(());
        }
        let claimed = jobs.len();
        let results: Vec<DeliveryResult> = futures_util::stream::iter(jobs)
            .map(|job| deliver(client, &config.retry, job))
            .buffer_unordered(config.concurrency.max(1))
            .collect()
            .await;
        record(dispatcher, results).await?;
        if claimed < config.batch_size as usize {
            return Ok(());
        }
    }
}

/// Records results, waiting out a full command queue for a while. If they still cannot be recorded the claims expire
/// and the deliveries are retried: at-least-once, never lost.
async fn record(
    dispatcher: &Dispatcher,
    results: Vec<DeliveryResult>,
) -> Result<(), DispatchError> {
    let mut wait = Duration::from_millis(10);
    for _ in 0..10 {
        match dispatcher.record_deliveries(results.clone()).await {
            Err(DispatchError::Overloaded) => {
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(Duration::from_secs(1));
            }
            other => return other,
        }
    }
    Err(DispatchError::Overloaded)
}

async fn deliver(
    client: &reqwest::Client,
    retry: &RetryPolicy,
    job: DeliveryJob,
) -> DeliveryResult {
    let attempt = job.attempts + 1;
    let started = std::time::Instant::now();
    let response = client
        .post(&job.url)
        .header("X-Event-Id", job.event.event_id.to_string())
        .header("X-Event-Type", &job.event.event_type)
        .header("X-Event-Sequence", job.event.sequence.to_string())
        .header("X-Subscription-Id", job.subscription_id.to_string())
        .header("X-Delivery-Attempt", attempt.to_string())
        .json(&job.event)
        .send()
        .await;
    let outcome = match response {
        Ok(response) if response.status().is_success() => DeliveryOutcome::Delivered,
        Ok(response) => retry.after_failure(job.attempts, format!("HTTP {}", response.status())),
        Err(error) => retry.after_failure(job.attempts, error.to_string()),
    };
    ::metrics::histogram!(crate::metrics::WEBHOOK_DURATION).record(started.elapsed());
    let label = match &outcome {
        DeliveryOutcome::Delivered => "delivered",
        DeliveryOutcome::Retry { .. } => "retry",
        DeliveryOutcome::Dead { .. } => "dead",
    };
    ::metrics::counter!(crate::metrics::WEBHOOK_ATTEMPTS, "outcome" => label).increment(1);
    match &outcome {
        DeliveryOutcome::Delivered => {
            tracing::debug!(delivery = %job.delivery_id, event = %job.event.event_id, attempt, "webhook delivered");
        }
        DeliveryOutcome::Retry { error, at } => {
            tracing::info!(delivery = %job.delivery_id, url = %job.url, attempt, %error, retry_at = %at, "webhook failed");
        }
        DeliveryOutcome::Dead { error } => {
            tracing::warn!(delivery = %job.delivery_id, url = %job.url, attempt, %error, "webhook dead-lettered");
        }
    }
    DeliveryResult {
        delivery_id: job.delivery_id,
        outcome,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(500),
        }
    }

    #[test]
    fn backoff_doubles_with_jitter_and_is_capped() {
        let policy = policy();
        for _ in 0..100 {
            let first = policy.backoff(1);
            assert!(
                first >= Duration::from_millis(50) && first <= Duration::from_millis(100),
                "{first:?}"
            );
            let third = policy.backoff(3);
            assert!(
                third >= Duration::from_millis(200) && third <= Duration::from_millis(400),
                "{third:?}"
            );
            let capped = policy.backoff(30);
            assert!(
                capped >= Duration::from_millis(250) && capped <= Duration::from_millis(500),
                "{capped:?}"
            );
        }
    }

    #[test]
    fn the_last_allowed_attempt_dead_letters() {
        let policy = policy();
        assert!(matches!(
            policy.after_failure(0, "x".into()),
            DeliveryOutcome::Retry { .. }
        ));
        assert!(matches!(
            policy.after_failure(2, "x".into()),
            DeliveryOutcome::Retry { .. }
        ));
        assert_eq!(
            policy.after_failure(3, "x".into()),
            DeliveryOutcome::Dead { error: "x".into() }
        );
    }

    #[test]
    fn lease_covers_a_batch_of_timeouts() {
        let config = WorkerConfig {
            poll_interval: Duration::from_millis(250),
            batch_size: 100,
            concurrency: 16,
            request_timeout: Duration::from_secs(5),
            retry: policy(),
        };
        assert_eq!(config.lease(), Duration::from_secs(7 * 5 + 30));
    }
}
