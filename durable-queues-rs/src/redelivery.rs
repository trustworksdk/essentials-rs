use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::message::HandlerError;

type PermanentErrorClassifier = Arc<dyn Fn(&(dyn StdError + 'static)) -> bool + Send + Sync>;

/// When and how often a failed message is redelivered - Java's `RedeliveryPolicy` (CONTRACT.md §7, §8).
///
/// `max_redeliveries = N` allows at most N + 1 deliveries; the failure of the last one dead-letters the message.
#[derive(Clone)]
pub struct RedeliveryPolicy {
    backoff: Backoff,
    max_redeliveries: u32,
    permanent_error_classifier: Option<PermanentErrorClassifier>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Backoff {
    Fixed {
        delay: Duration,
    },
    Linear {
        delay: Duration,
        max_delay: Duration,
    },
    Exponential {
        initial: Duration,
        followup: Duration,
        multiplier: f64,
        max_delay: Duration,
    },
}

/// What to do with a message whose delivery failed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum FailureOutcome {
    Retry { delay: Duration },
    DeadLetter { reason: DeadLetterReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeadLetterReason {
    PermanentError,
    RedeliveriesExhausted,
}

impl RedeliveryPolicy {
    /// The same `delay` before every redelivery.
    pub fn fixed_backoff(delay: Duration, max_redeliveries: u32) -> Self {
        Self::with_backoff(Backoff::Fixed { delay }, max_redeliveries)
    }

    /// `delay × (n + 1)` before redelivery `n` (0-based), capped at `max_delay`.
    pub fn linear_backoff(delay: Duration, max_delay: Duration, max_redeliveries: u32) -> Self {
        Self::with_backoff(Backoff::Linear { delay, max_delay }, max_redeliveries)
    }

    /// `initial` before the first redelivery, then `followup × multiplier^(n - 1)`, capped at `max_delay`.
    /// A multiplier of 1.0 or less (or NaN) gives a constant `followup`.
    pub fn exponential_backoff(
        initial: Duration,
        followup: Duration,
        multiplier: f64,
        max_delay: Duration,
        max_redeliveries: u32,
    ) -> Self {
        Self::with_backoff(
            Backoff::Exponential {
                initial,
                followup,
                multiplier,
                max_delay,
            },
            max_redeliveries,
        )
    }

    fn with_backoff(backoff: Backoff, max_redeliveries: u32) -> Self {
        Self {
            backoff,
            max_redeliveries,
            permanent_error_classifier: None,
        }
    }

    /// Dead-letters immediately on errors `is_permanent` accepts, even when the handler returned them as
    /// [`HandlerError::Transient`] - Java's `MessageDeliveryErrorHandler.stopRedeliveryOn(...)`. Every error in the
    /// `source()` chain is offered, so `|e| e.is::<ValidationError>()` also matches a wrapped `ValidationError`.
    pub fn stop_redelivery_on(
        mut self,
        is_permanent: impl Fn(&(dyn StdError + 'static)) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.permanent_error_classifier = Some(Arc::new(is_permanent));
        self
    }

    pub fn max_redeliveries(&self) -> u32 {
        self.max_redeliveries
    }

    /// The delay before the next redelivery of a message that has been redelivered `redelivery_attempts` times
    /// (0 after its first delivery failed). Java's `calculateNextRedeliveryDelay`, computed in f64 nanoseconds; a
    /// result at or above the cap returns the cap.
    pub fn next_redelivery_delay(&self, redelivery_attempts: u32) -> Duration {
        let (initial, cap) = match self.backoff {
            Backoff::Fixed { delay } => (delay, delay),
            Backoff::Linear { delay, max_delay } => (delay, max_delay),
            Backoff::Exponential {
                initial, max_delay, ..
            } => (initial, max_delay),
        };
        if redelivery_attempts == 0 {
            return initial;
        }
        let n = f64::from(redelivery_attempts);
        let delay_nanos = match self.backoff {
            Backoff::Fixed { delay } => nanos(delay),
            Backoff::Linear { delay, .. } => nanos(delay) + nanos(delay) * n,
            Backoff::Exponential {
                followup,
                multiplier,
                ..
            } => {
                // NaN also means "no growth", as in Java's `!(x > 1.0)`.
                let multiplier = if multiplier.is_nan() || multiplier <= 1.0 {
                    1.0
                } else {
                    multiplier
                };
                nanos(followup) * multiplier.powf(n - 1.0)
            }
        };
        if delay_nanos >= nanos(cap) {
            cap
        } else {
            Duration::from_nanos(delay_nanos as u64)
        }
    }

    /// Java's `MessageDeliveryClassifier.classify` (CONTRACT.md §7). `total_attempts` includes the failed delivery.
    pub(crate) fn decide(
        &self,
        error: &HandlerError,
        total_attempts: i32,
        redelivery_attempts: i32,
    ) -> FailureOutcome {
        if error.is_permanent() || self.classifies_as_permanent(error) {
            return FailureOutcome::DeadLetter {
                reason: DeadLetterReason::PermanentError,
            };
        }
        // Java: `attempts >= maximumNumberOfRedeliveries + 1`.
        if i64::from(total_attempts) > i64::from(self.max_redeliveries) {
            return FailureOutcome::DeadLetter {
                reason: DeadLetterReason::RedeliveriesExhausted,
            };
        }
        FailureOutcome::Retry {
            delay: self.next_redelivery_delay(redelivery_attempts.max(0) as u32),
        }
    }

    fn classifies_as_permanent(&self, error: &HandlerError) -> bool {
        let Some(is_permanent) = &self.permanent_error_classifier else {
            return false;
        };
        let mut current: Option<&(dyn StdError + 'static)> = Some(error.error());
        // Bounded like Java's cause-chain walk, in case a source chain is cyclic.
        for _ in 0..100 {
            let Some(e) = current else { break };
            if is_permanent(e) {
                return true;
            }
            current = e.source();
        }
        false
    }
}

impl fmt::Debug for RedeliveryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedeliveryPolicy")
            .field("backoff", &self.backoff)
            .field("max_redeliveries", &self.max_redeliveries)
            .field(
                "stop_redelivery_on",
                &self.permanent_error_classifier.is_some(),
            )
            .finish()
    }
}

fn nanos(duration: Duration) -> f64 {
    duration.as_nanos() as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn delays(policy: &RedeliveryPolicy, count: u32) -> Vec<Duration> {
        (0..count)
            .map(|n| policy.next_redelivery_delay(n))
            .collect()
    }

    #[test]
    fn fixed_backoff_is_constant() {
        assert_eq!(
            delays(&RedeliveryPolicy::fixed_backoff(ms(500), 5), 4),
            [ms(500); 4]
        );
    }

    #[test]
    fn linear_backoff_grows_by_the_delay_and_stops_at_the_cap() {
        let policy = RedeliveryPolicy::linear_backoff(ms(1000), ms(3500), 10);
        assert_eq!(
            delays(&policy, 5),
            [ms(1000), ms(2000), ms(3000), ms(3500), ms(3500)]
        );
    }

    #[test]
    fn exponential_backoff_matches_the_java_worked_example() {
        // RedeliveryPolicy.exponentialBackoff(500ms, 500ms, 2.0, 1min, 8) in LLM-foundation.md
        let policy = RedeliveryPolicy::exponential_backoff(
            ms(500),
            ms(500),
            2.0,
            Duration::from_secs(60),
            8,
        );
        let expected = [500, 500, 1000, 2000, 4000, 8000, 16000, 32000].map(ms);
        assert_eq!(delays(&policy, 8), expected);
    }

    #[test]
    fn exponential_backoff_uses_initial_then_followup() {
        let policy = RedeliveryPolicy::exponential_backoff(
            ms(100),
            ms(1000),
            3.0,
            Duration::from_secs(60),
            5,
        );
        assert_eq!(delays(&policy, 4), [ms(100), ms(1000), ms(3000), ms(9000)]);
    }

    #[test]
    fn a_delay_at_or_above_the_cap_returns_the_cap() {
        let policy = RedeliveryPolicy::exponential_backoff(ms(10), ms(1000), 2.0, ms(2000), 50);
        assert_eq!(policy.next_redelivery_delay(2), ms(2000), "exactly the cap");
        assert_eq!(
            policy.next_redelivery_delay(40),
            ms(2000),
            "far beyond, no overflow"
        );
    }

    #[test]
    fn a_multiplier_of_one_or_less_or_nan_does_not_grow() {
        for multiplier in [1.0, 0.5, 0.0, -2.0, f64::NAN] {
            let policy =
                RedeliveryPolicy::exponential_backoff(ms(10), ms(700), multiplier, ms(60_000), 5);
            assert_eq!(
                delays(&policy, 4),
                [ms(10), ms(700), ms(700), ms(700)],
                "multiplier {multiplier}"
            );
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("invalid order")]
    struct ValidationError;

    #[derive(Debug, thiserror::Error)]
    #[error("command failed")]
    struct Wrapper(#[source] ValidationError);

    fn transient(e: impl Into<crate::message::BoxError>) -> HandlerError {
        HandlerError::Transient(e.into())
    }

    #[test]
    fn redeliveries_are_exhausted_after_max_plus_one_deliveries() {
        let policy = RedeliveryPolicy::fixed_backoff(ms(200), 2);
        let error = transient("boom");
        assert_eq!(
            policy.decide(&error, 1, 0),
            FailureOutcome::Retry { delay: ms(200) }
        );
        assert_eq!(
            policy.decide(&error, 2, 1),
            FailureOutcome::Retry { delay: ms(200) }
        );
        assert_eq!(
            policy.decide(&error, 3, 2),
            FailureOutcome::DeadLetter {
                reason: DeadLetterReason::RedeliveriesExhausted
            }
        );
    }

    #[test]
    fn the_retry_delay_uses_the_redelivery_count_from_before_the_failure() {
        let policy = RedeliveryPolicy::linear_backoff(ms(100), ms(10_000), 10);
        assert_eq!(
            policy.decide(&transient("boom"), 3, 2),
            FailureOutcome::Retry { delay: ms(300) }
        );
    }

    #[test]
    fn permanent_errors_dead_letter_on_the_first_attempt() {
        let policy = RedeliveryPolicy::fixed_backoff(ms(200), 10);
        let permanent = FailureOutcome::DeadLetter {
            reason: DeadLetterReason::PermanentError,
        };
        assert_eq!(
            policy.decide(&HandlerError::Permanent("bad".into()), 1, 0),
            permanent
        );

        let policy = policy.stop_redelivery_on(|e| e.is::<ValidationError>());
        assert_eq!(policy.decide(&transient(ValidationError), 1, 0), permanent);
        assert_eq!(
            policy.decide(&transient(Wrapper(ValidationError)), 1, 0),
            permanent,
            "matched in the source chain"
        );
        assert!(matches!(
            policy.decide(&transient("other"), 1, 0),
            FailureOutcome::Retry { .. }
        ));
    }
}
