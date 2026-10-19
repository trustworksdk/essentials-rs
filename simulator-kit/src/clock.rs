//! Clocks. The exchange stamps every domain timestamp (orders, executions, events, settlement dates) from one
//! [`Clock`]: the system clock in normal operation, or a [`VirtualClock`] that tests and simulations set, advance,
//! freeze and speed up. Webhook delivery scheduling (retries, leases, timeouts) always uses real time, because it
//! deals with real consumers.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono::{DateTime, SubsecRound, Utc};
use serde::Deserialize;

/// Real time at the precision the database stores (microseconds), for scheduling against real consumers.
pub fn wall_now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

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

#[derive(Clone, Copy, Debug, PartialEq, thiserror::Error)]
pub enum ClockError {
    #[error("the virtual clock cannot move backwards (now {now}, requested {requested})")]
    Backwards {
        now: DateTime<Utc>,
        requested: DateTime<Utc>,
    },
    #[error("clock speed must be more than 0 and at most {MAX_SPEED}, got {0}")]
    Speed(f64),
}

/// The fastest a virtual clock may run: about eleven and a half simulated days per real second.
pub const MAX_SPEED: f64 = 1_000_000.0;

/// Checks a speed: simulated seconds per real second, more than 0 and at most [`MAX_SPEED`].
pub fn validate_speed(speed: f64) -> Result<(), ClockError> {
    if speed > 0.0 && speed <= MAX_SPEED {
        Ok(())
    } else {
        Err(ClockError::Speed(speed))
    }
}

/// A controllable clock. Frozen, it reads the same instant until set or advanced; running, it moves from the instant
/// it was last set to at `speed` simulated seconds per real second (1 is real time). It never moves backwards. Clones
/// share the same state.
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
    speed: f64,
}

impl VirtualClock {
    /// A clock at real-time speed.
    pub fn new(start: DateTime<Utc>, running: bool) -> Self {
        Self::with_speed(start, running, 1.0)
    }

    /// A clock that runs at `speed`, which must pass [`validate_speed`].
    pub fn with_speed(start: DateTime<Utc>, running: bool, speed: f64) -> Self {
        debug_assert!(validate_speed(speed).is_ok(), "speed {speed}");
        Self {
            state: Arc::new(Mutex::new(State {
                at_anchor: start,
                anchor: Instant::now(),
                running,
                speed,
            })),
        }
    }

    pub fn is_running(&self) -> bool {
        self.state.lock().expect("clock lock").running
    }

    /// The reading at the clock's last change and the real time of that change: the exact pair a follower
    /// extrapolates from. Every change (set, advance, freeze, resume, speed) re-anchors the clock.
    pub fn anchor(&self) -> (DateTime<Utc>, DateTime<Utc>) {
        let state = self.state.lock().expect("clock lock");
        let since = chrono::Duration::from_std(state.anchor.elapsed()).unwrap_or_default();
        (state.at_anchor, (Utc::now() - since).trunc_subsecs(6))
    }

    /// Simulated seconds per real second while running.
    pub fn speed(&self) -> f64 {
        self.state.lock().expect("clock lock").speed
    }

    /// Changes the speed from the current reading on; the time already passed is kept.
    pub fn set_speed(&self, speed: f64) -> Result<(), ClockError> {
        validate_speed(speed)?;
        let mut state = self.state.lock().expect("clock lock");
        state.at_anchor = state.now();
        state.anchor = Instant::now();
        state.speed = speed;
        Ok(())
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

    /// Puts the clock at `to`, even backwards, and freezes or runs it at `speed`. Only for a reset, which clears
    /// every state stamped with the old readings.
    pub fn reset(&self, to: DateTime<Utc>, running: bool, speed: f64) {
        let mut state = self.state.lock().expect("clock lock");
        state.at_anchor = to;
        state.anchor = Instant::now();
        state.running = running;
        state.speed = speed;
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
            let real = self.anchor.elapsed();
            let simulated = if self.speed == 1.0 {
                Some(real)
            } else {
                std::time::Duration::try_from_secs_f64(real.as_secs_f64() * self.speed).ok()
            };
            let elapsed = simulated
                .and_then(|d| chrono::Duration::from_std(d).ok())
                .unwrap_or(chrono::Duration::MAX);
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

/// How a simulator tells time, as the clock API shows it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClockState {
    pub virtual_clock: bool,
    pub running: bool,
    /// Simulated seconds per real second while running; 1 for the system clock.
    pub speed: f64,
    pub now: DateTime<Utc>,
}

/// A change to the virtual clock.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ClockChange {
    Advance(chrono::Duration),
    Set(DateTime<Utc>),
    Freeze,
    Resume,
    Speed(f64),
}

impl ClockChange {
    /// Its name in `ClockChanged`: `advance`, `set`, `freeze`, `resume` or `speed`.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Advance(_) => "advance",
            Self::Set(_) => "set",
            Self::Freeze => "freeze",
            Self::Resume => "resume",
            Self::Speed(_) => "speed",
        }
    }

    /// Applies the change to `clock`. Returns whether it changed anything a follower must know: the reading jumped,
    /// or the clock froze, resumed or changed speed. Freezing a frozen clock, resuming a running one, or setting the
    /// speed it already has changes nothing.
    pub fn apply(&self, clock: &VirtualClock) -> Result<bool, ClockError> {
        let (running, speed) = (clock.is_running(), clock.speed());
        match *self {
            Self::Advance(by) => clock.advance(by)?,
            Self::Set(to) => {
                let before = clock.now();
                clock.set(to)?;
                return Ok(to != before);
            }
            Self::Freeze => clock.freeze(),
            Self::Resume => clock.resume(),
            Self::Speed(to) => clock.set_speed(to)?,
        }
        Ok(matches!(self, Self::Advance(_))
            || clock.is_running() != running
            || clock.speed() != speed)
    }
}

/// The event every clock change publishes, so a consumer can follow simulated time without polling.
pub const CLOCK_CHANGED: &str = "ClockChanged";

/// `ClockChanged` for `change`, just applied to `clock`: its state after the change, with `now` the reading at the
/// change and `effectiveAt` the real time of it ([`VirtualClock::anchor`]). A follower computes simulated time as
/// `now + (real time − effectiveAt) × speed` while `running`. A frozen clock reads `now` whenever it is asked, so
/// its `effectiveAt` is `null`: real time stays out of the event log of a reproducible run, whose clock is frozen
/// (decision D6). The aggregate ID is `clock`; `source` is the simulator's own.
pub fn changed_event(
    change: &ClockChange,
    clock: &VirtualClock,
    source: &'static str,
) -> crate::events::NewEvent {
    let (now, effective_at) = clock.anchor();
    let running = clock.is_running();
    crate::events::NewEvent {
        event_id: uuid::Uuid::nil(),
        sequence: 0,
        event_type: CLOCK_CHANGED,
        source,
        aggregate_id: "clock".into(),
        correlation_id: None,
        occurred_at: now,
        payload: serde_json::json!({
            "change": change.name(),
            "mode": "virtual",
            "running": running,
            "speed": clock.speed(),
            "now": now,
            "effectiveAt": running.then_some(effective_at),
        }),
    }
}

/// The clock settings (`clock:` in a simulator's configuration).
#[derive(Clone, Debug, Deserialize)]
pub struct ClockConfig {
    pub mode: ClockMode,
    /// Virtual clock only: the first reading. Default: the real time at first startup.
    #[serde(default)]
    pub start: Option<chrono::DateTime<chrono::Utc>>,
    /// Virtual clock only: start running instead of frozen.
    #[serde(default)]
    pub running: bool,
    /// Virtual clock only: simulated seconds per real second while it runs (1440 makes a day pass in a minute).
    #[serde(default = "real_time")]
    pub speed: f64,
    /// Mounts speed, freeze and resume under `/api/clock` (decision D17): runtime control for a showcase deployment
    /// without test control's reset and fault injection. `GET /api/clock` is always mounted.
    #[serde(default)]
    pub operator_control: bool,
}

fn real_time() -> f64 {
    1.0
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClockMode {
    System,
    Virtual,
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
    fn a_fast_clock_moves_speed_times_real_time_and_keeps_its_reading_on_a_change() {
        let before = std::time::Instant::now();
        let clock = VirtualClock::with_speed(start(), true, 3600.0);
        std::thread::sleep(std::time::Duration::from_millis(50));
        let moved = clock.now() - start();
        let real = chrono::Duration::from_std(before.elapsed()).unwrap();
        assert!(
            moved >= chrono::Duration::seconds(180) && moved <= real * 3600,
            "an hour per second: {moved} after {real}"
        );
        clock.set_speed(1.0).unwrap();
        let slowed = clock.now();
        assert!(slowed - start() >= moved, "the change keeps the reading");
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(
            clock.now() - slowed < chrono::Duration::seconds(5),
            "real time again"
        );
        assert_eq!(clock.speed(), 1.0);
        for bad in [0.0, -1.0, f64::NAN, MAX_SPEED * 2.0] {
            assert!(clock.set_speed(bad).is_err(), "{bad}");
        }
        assert_eq!(clock.speed(), 1.0);
        clock.freeze();
        clock.set_speed(1440.0).unwrap();
        let frozen = clock.now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(clock.now(), frozen, "speed does not move a frozen clock");
    }

    #[test]
    fn clones_share_one_clock() {
        let clock = VirtualClock::new(start(), false);
        let other = clock.clone();
        clock.advance(chrono::Duration::hours(1)).unwrap();
        assert_eq!(other.now(), start() + chrono::Duration::hours(1));
    }
}
