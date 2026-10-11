use std::time::Duration;

use crate::error::{DurableQueuesError, Result};

/// Adaptive polling for idle queues - Java's `CentralizedQueuePollingOptimizer`: every poll that finds nothing grows
/// the pause by `factor` (± `jitter_fraction`) up to `max_delay`; a poll that finds messages resets it.
///
/// Off by default. Java only enables it together with LISTEN/NOTIFY, which wakes an idle queue at once; without
/// NOTIFY (not ported yet) a message queued on an idle queue waits for up to `max_delay`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollingBackoff {
    initial_delay: Duration,
    max_delay: Duration,
    factor: f64,
    jitter_fraction: f64,
}

impl PollingBackoff {
    /// Validated like Java: `initial_delay > 0`, `max_delay >= initial_delay`, `factor >= 1`, `jitter_fraction` in [0, 1).
    pub fn new(
        initial_delay: Duration,
        max_delay: Duration,
        factor: f64,
        jitter_fraction: f64,
    ) -> Result<Self> {
        let invalid = |reason| Err(DurableQueuesError::InvalidConsumerConfig(reason));
        if initial_delay.is_zero() {
            return invalid("polling backoff initial_delay must be > 0");
        }
        if max_delay < initial_delay {
            return invalid("polling backoff max_delay must be >= initial_delay");
        }
        if factor.is_nan() || factor < 1.0 {
            return invalid("polling backoff factor must be >= 1.0");
        }
        if !(0.0..1.0).contains(&jitter_fraction) {
            return invalid("polling backoff jitter_fraction must be in [0.0, 1.0)");
        }
        Ok(Self {
            initial_delay,
            max_delay,
            factor,
            jitter_fraction,
        })
    }
}

/// The current pause of one consumer.
#[derive(Debug)]
pub(crate) struct PollingBackoffState {
    config: PollingBackoff,
    current: Duration,
}

impl PollingBackoffState {
    pub fn new(config: PollingBackoff) -> Self {
        Self {
            config,
            current: config.initial_delay,
        }
    }

    /// Java's `queuePollingReturnedNoMessages`: the pause before the next poll.
    pub fn after_empty_poll(&mut self) -> Duration {
        let PollingBackoff {
            initial_delay,
            max_delay,
            factor,
            jitter_fraction,
        } = self.config;
        let capped = self.current.mul_f64(factor).min(max_delay);
        let jitter = capped.mul_f64(jitter_fraction).as_nanos() as i128;
        let delta = if jitter == 0 {
            0
        } else {
            fastrand::i128(-jitter..=jitter)
        };
        let jittered = (capped.as_nanos() as i128 + delta).max(0) as u64;
        self.current = Duration::from_nanos(jittered).max(initial_delay);
        self.current
    }

    /// Java's `queuePollingReturnedMessages`.
    pub fn reset(&mut self) {
        self.current = self.config.initial_delay;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[test]
    fn empty_polls_back_off_up_to_the_cap_and_messages_reset() {
        let mut state =
            PollingBackoffState::new(PollingBackoff::new(ms(100), ms(1000), 2.0, 0.0).unwrap());
        let pauses: Vec<_> = (0..5).map(|_| state.after_empty_poll()).collect();
        assert_eq!(pauses, [ms(200), ms(400), ms(800), ms(1000), ms(1000)]);
        state.reset();
        assert_eq!(state.after_empty_poll(), ms(200));
    }

    #[test]
    fn jitter_stays_within_its_fraction_and_never_below_the_initial_delay() {
        let mut state =
            PollingBackoffState::new(PollingBackoff::new(ms(100), ms(1000), 1.0, 0.5).unwrap());
        for _ in 0..1000 {
            let pause = state.after_empty_poll();
            assert!(pause >= ms(100) && pause <= ms(150), "{pause:?}");
            state.reset();
        }
    }

    #[test]
    fn invalid_settings_are_rejected() {
        assert!(PollingBackoff::new(Duration::ZERO, ms(10), 2.0, 0.1).is_err());
        assert!(PollingBackoff::new(ms(10), ms(5), 2.0, 0.1).is_err());
        assert!(PollingBackoff::new(ms(10), ms(50), 0.5, 0.1).is_err());
        assert!(PollingBackoff::new(ms(10), ms(50), f64::NAN, 0.1).is_err());
        assert!(PollingBackoff::new(ms(10), ms(50), 2.0, 1.0).is_err());
    }
}
