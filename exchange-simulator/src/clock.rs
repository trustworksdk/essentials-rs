//! Clocks. The exchange stamps every domain timestamp (orders, executions, events, settlement dates) from one
//! [`Clock`]: the system clock in normal operation, or a [`VirtualClock`] that tests and simulations set, advance and
//! freeze. Webhook delivery scheduling (retries, leases) always uses real time, because it deals with real consumers.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono::{DateTime, Utc};

/// Source of the current time.
pub trait Clock {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock stuck at one instant.
#[derive(Clone, Copy, Debug)]
pub struct FixedClock(pub DateTime<Utc>);

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ClockError {
    #[error("the virtual clock cannot move backwards (now {now}, requested {requested})")]
    Backwards {
        now: DateTime<Utc>,
        requested: DateTime<Utc>,
    },
}

/// A controllable clock. Frozen, it reads the same instant until set or advanced; running, it moves with real time
/// from the instant it was last set to. It never moves backwards. Clones share the same state.
#[derive(Clone, Debug)]
pub struct VirtualClock {
    state: Arc<Mutex<State>>,
}

#[derive(Debug)]
struct State {
    /// The virtual time at `anchor`.
    at_anchor: DateTime<Utc>,
    anchor: Instant,
    running: bool,
}

impl VirtualClock {
    pub fn new(start: DateTime<Utc>, running: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                at_anchor: start,
                anchor: Instant::now(),
                running,
            })),
        }
    }

    pub fn is_running(&self) -> bool {
        self.state.lock().expect("clock lock").running
    }

    /// Moves the clock to `to`, which must not be before the current reading.
    pub fn set(&self, to: DateTime<Utc>) -> Result<(), ClockError> {
        let mut state = self.state.lock().expect("clock lock");
        let now = state.now();
        if to < now {
            return Err(ClockError::Backwards { now, requested: to });
        }
        state.at_anchor = to;
        state.anchor = Instant::now();
        Ok(())
    }

    /// Moves the clock forward by `by`.
    pub fn advance(&self, by: chrono::Duration) -> Result<(), ClockError> {
        let mut state = self.state.lock().expect("clock lock");
        let now = state.now();
        let to =
            now.checked_add_signed(by)
                .filter(|to| *to >= now)
                .ok_or(ClockError::Backwards {
                    now,
                    requested: now,
                })?;
        state.at_anchor = to;
        state.anchor = Instant::now();
        Ok(())
    }

    /// Puts the clock at `to`, even backwards, and freezes or runs it. Only for a reset, which clears every state
    /// stamped with the old readings.
    pub fn reset(&self, to: DateTime<Utc>, running: bool) {
        let mut state = self.state.lock().expect("clock lock");
        state.at_anchor = to;
        state.anchor = Instant::now();
        state.running = running;
    }

    /// Stops the clock at its current reading.
    pub fn freeze(&self) {
        let mut state = self.state.lock().expect("clock lock");
        state.at_anchor = state.now();
        state.anchor = Instant::now();
        state.running = false;
    }

    /// Lets the clock move with real time again, from its current reading.
    pub fn resume(&self) {
        let mut state = self.state.lock().expect("clock lock");
        state.at_anchor = state.now();
        state.anchor = Instant::now();
        state.running = true;
    }
}

impl State {
    fn now(&self) -> DateTime<Utc> {
        if self.running {
            let elapsed =
                chrono::Duration::from_std(self.anchor.elapsed()).unwrap_or(chrono::Duration::MAX);
            self.at_anchor
                .checked_add_signed(elapsed)
                .unwrap_or(self.at_anchor)
        } else {
            self.at_anchor
        }
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> DateTime<Utc> {
        self.state.lock().expect("clock lock").now()
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn start() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 14, 12, 0, 0).unwrap()
    }

    #[test]
    fn a_frozen_clock_moves_only_when_told() {
        let clock = VirtualClock::new(start(), false);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(clock.now(), start());
        clock.advance(chrono::Duration::days(2)).unwrap();
        assert_eq!(clock.now(), start() + chrono::Duration::days(2));
        let later = start() + chrono::Duration::days(3);
        clock.set(later).unwrap();
        assert_eq!(clock.now(), later);
        assert_eq!(
            clock.set(start()),
            Err(ClockError::Backwards {
                now: later,
                requested: start()
            }),
            "never backwards"
        );
        assert!(clock.advance(chrono::Duration::seconds(-1)).is_err());
        assert_eq!(clock.now(), later);
    }

    #[test]
    fn a_running_clock_follows_real_time_and_can_be_frozen() {
        let clock = VirtualClock::new(start(), true);
        std::thread::sleep(std::time::Duration::from_millis(20));
        let moved = clock.now();
        assert!(
            moved >= start() + chrono::Duration::milliseconds(20),
            "{moved}"
        );
        clock.freeze();
        let frozen = clock.now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(clock.now(), frozen);
        assert!(!clock.is_running());
        clock.resume();
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(clock.now() > frozen);
    }

    #[test]
    fn clones_share_one_clock() {
        let clock = VirtualClock::new(start(), false);
        let other = clock.clone();
        clock.advance(chrono::Duration::hours(1)).unwrap();
        assert_eq!(other.now(), start() + chrono::Duration::hours(1));
    }
}
