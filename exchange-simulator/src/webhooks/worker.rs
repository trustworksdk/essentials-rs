use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::StreamExt;
use tokio::sync::{Notify, watch};

use super::{AttemptRecord, DeliveryJob, DeliveryOutcome, DeliveryResult, signing};
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

/// Claims and delivers batches until a claim comes back empty.
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
        let results: Vec<DeliveryResult> = futures_util::stream::iter(jobs)
            .map(|job| deliver(client, &config.retry, job))
            .buffer_unordered(config.concurrency.max(1))
            .collect()
            .await;
        record(dispatcher, results).await?;
        // Claim again even after a partial batch: an ordered subscription's next event only becomes eligible once
        // the one before it is recorded. The loop ends when nothing is due.
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
    let plan = job
        .faults
        .as_ref()
        .map(|policy| {
            let event = serde_json::to_value(&job.event).expect("envelopes serialize");
            policy.plan(job.delivery_id, attempt, job.held, &event)
        })
        .unwrap_or_default();
    if let Some(hold) = plan.hold {
        let now = Utc::now();
        let at = now + chrono::Duration::from_std(hold).unwrap_or(chrono::Duration::MAX);
        ::metrics::counter!(crate::metrics::WEBHOOK_ATTEMPTS, "outcome" => "held").increment(1);
        tracing::debug!(delivery = %job.delivery_id, until = %at, "webhook held back by an out-of-order fault");
        return DeliveryResult {
            delivery_id: job.delivery_id,
            epoch: job.epoch,
            outcome: DeliveryOutcome::Held { at },
            sent: vec![AttemptRecord {
                attempt,
                held: true,
                faults: plan.applied,
                body: None,
                http_status: None,
                error: None,
                at: now,
            }],
        };
    }
    if let Some(delay) = plan.delay {
        tokio::time::sleep(delay).await;
    }
    // Serialized once: these exact bytes are signed and sent (twice for a duplicate).
    let body = match &plan.body {
        Some(damaged) => damaged.clone(),
        None => serde_json::to_vec(&job.event).expect("envelopes serialize"),
    };
    let recorded_body = plan
        .body
        .as_ref()
        .map(|b| String::from_utf8_lossy(b).into_owned());
    let mut sent = Vec::new();
    let mut outcome = None;
    for _ in 0..if plan.duplicate { 2 } else { 1 } {
        let at = Utc::now();
        let (http_status, error) = match send(client, &job, attempt, &body).await {
            Ok(status) if status.is_success() => (Some(status.as_u16()), None),
            Ok(status) => (Some(status.as_u16()), Some(format!("HTTP {status}"))),
            Err(error) => (None, Some(error.to_string())),
        };
        // The first request decides; a duplicate only shows the consumer the same event again.
        outcome.get_or_insert_with(|| match &error {
            None => DeliveryOutcome::Delivered,
            Some(error) => retry.after_failure(job.attempts, error.clone()),
        });
        sent.push(AttemptRecord {
            attempt,
            held: false,
            faults: plan.applied.clone(),
            body: recorded_body.clone(),
            http_status,
            error,
            at,
        });
    }
    let outcome = outcome.expect("at least one request");
    ::metrics::histogram!(crate::metrics::WEBHOOK_DURATION).record(started.elapsed());
    let label = match &outcome {
        DeliveryOutcome::Delivered => "delivered",
        DeliveryOutcome::Retry { .. } => "retry",
        DeliveryOutcome::Dead { .. } => "dead",
        DeliveryOutcome::Held { .. } => "held",
    };
    ::metrics::counter!(crate::metrics::WEBHOOK_ATTEMPTS, "outcome" => label).increment(1);
    match &outcome {
        DeliveryOutcome::Delivered | DeliveryOutcome::Held { .. } => {
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
        epoch: job.epoch,
        outcome,
        sent,
    }
}

/// One POST of `body` for the job, signed when the subscription has a secret.
async fn send(
    client: &reqwest::Client,
    job: &DeliveryJob,
    attempt: u32,
    body: &[u8],
) -> Result<reqwest::StatusCode, reqwest::Error> {
    let mut request = client
        .post(&job.url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header("X-Event-Id", job.event.event_id.to_string())
        .header("X-Event-Type", &job.event.event_type)
        .header("X-Event-Sequence", job.event.sequence.to_string())
        .header("X-Subscription-Id", job.subscription_id.to_string())
        .header("X-Delivery-Attempt", attempt.to_string());
    if let Some(secret) = &job.secret {
        let timestamp = Utc::now().timestamp();
        let mut signature = signing::sign(secret, timestamp, body);
        if let Some(previous) = &job.previous_secret {
            // During a rotation's grace period: one signature per secret, the current one first.
            signature.push_str(", ");
            signature.push_str(&signing::sign(previous, timestamp, body));
        }
        request = request
            .header(signing::TIMESTAMP_HEADER, timestamp.to_string())
            .header(signing::SIGNATURE_HEADER, signature);
    }
    Ok(request.body(body.to_vec()).send().await?.status())
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
