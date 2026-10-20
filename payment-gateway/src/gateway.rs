//! The gateway's state and its single writer. Every command and every tick runs here, one at a time (the service
//! holds it behind a mutex). Each change is committed to SQLite before memory follows it.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, SubsecRound, Utc};
use rusqlite::Connection;
use simulator_kit::calendar::{next_business_day, previous_business_day};
use simulator_kit::clock::{Clock, ClockError, SystemClock, VirtualClock, wall_now};
pub use simulator_kit::clock::{ClockChange, ClockState};
use simulator_kit::events::NewEvent;
use simulator_kit::ids::IdScheme;
use simulator_kit::outbox::{self, Outbox};
use simulator_kit::webhooks::{
    DeclaredSubscription, DeliveryJob, DeliveryResult, FaultPolicy, NewSubscription, Subscription,
    WebhookChange, WebhookError, WebhookHub,
};
use uuid::Uuid;

use crate::iban;
use crate::money::{Currencies, parse_amount};
use crate::payments::{
    BankAccount, Booking, ForcedOutcome, PAYMENT_COMPLETED, PAYMENT_FAILED, PAYMENT_PENDING,
    PAYMENT_RETURNED, Payment, PaymentPolicy, PaymentStatus, PaymentType,
};
use crate::statement::{self, StatementPolicy, StoredStatement};
use crate::store;

/// The version of the payload schemas the gateway's events carry.
pub const SCHEMA_VERSION: u32 = 1;

/// Every event type the gateway emits, for validating subscriptions.
pub const EVENT_TYPES: &[&str] = &[
    PAYMENT_PENDING,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    PAYMENT_RETURNED,
    statement::BANK_STATEMENT_ISSUED,
    simulator_kit::clock::CLOCK_CHANGED,
];

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("invalid payment: {0}")]
    Invalid(String),
    #[error("idempotency key {key} was already used by payment {existing} with different details")]
    IdempotencyConflict { key: String, existing: Uuid },
    #[error("unknown payment {0}")]
    UnknownPayment(Uuid),
    #[error("payment {id} is {status}: {why}")]
    PaymentSettled {
        id: Uuid,
        status: &'static str,
        why: &'static str,
    },
    #[error("the gateway uses the system clock; configure clock.mode = virtual to control time")]
    ClockNotVirtual,
    #[error(
        "the gateway's clock follows the exchange (clock.mode = follow); change the exchange's clock instead"
    )]
    ClockFollows,
    #[error(transparent)]
    Clock(#[from] ClockError),
    #[error(transparent)]
    Webhook(WebhookError),
    #[error("storage failure: {0}")]
    Storage(#[from] rusqlite::Error),
}

impl From<WebhookError> for GatewayError {
    fn from(error: WebhookError) -> Self {
        match error {
            WebhookError::Storage(error) => Self::Storage(error),
            other => Self::Webhook(other),
        }
    }
}

/// A request to move money (`POST /api/payments`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaymentRequest {
    pub idempotency_key: String,
    pub payment_type: PaymentType,
    pub account_id: String,
    pub bank_account: BankAccount,
    /// Decimal string, e.g. `"1500.00"`.
    pub amount: String,
    pub currency: String,
    pub reference: Option<String>,
}

/// Construction-time dependencies of a [`Gateway`].
pub struct GatewayOptions {
    pub ids: IdScheme,
    /// Domain time; ignored when `virtual_clock` is set.
    pub clock: Box<dyn Clock + Send>,
    pub virtual_clock: Option<VirtualClock>,
    pub currencies: Currencies,
    pub payments: PaymentPolicy,
    pub statements: Option<StatementPolicy>,
    pub simulation_id: Option<String>,
    pub delivery_signal: Option<Arc<tokio::sync::Notify>>,
    pub webhooks: Vec<DeclaredSubscription>,
    pub webhook_secret_grace: chrono::Duration,
    /// The virtual clock follows another simulator's ([`Gateway::follow`]) and refuses changes through the clock API.
    pub follow: bool,
}

/// Another simulator's clock as read from its `GET /api/clock`, `now` already corrected for the request's latency.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RemoteClock {
    pub running: bool,
    pub speed: f64,
    pub now: DateTime<Utc>,
}

/// The bank's statements: their policy, the last business date whose cutoff was handled, and late statements waiting.
struct Statements {
    policy: StatementPolicy,
    cursor: NaiveDate,
    pending: Vec<StoredStatement>,
}

impl Statements {
    fn due(&self) -> DateTime<Utc> {
        let cutoff = self
            .policy
            .calendar
            .cutoff_at(next_business_day(self.cursor));
        self.pending
            .first()
            .map_or(cutoff, |p| p.draft.statement.issued_at.min(cutoff))
    }

    fn first_cursor(policy: &StatementPolicy, now: DateTime<Utc>) -> NaiveDate {
        previous_business_day(policy.calendar.business_date(now))
    }
}

pub struct Gateway {
    conn: Connection,
    webhooks: WebhookHub,
    ids: IdScheme,
    clock: Box<dyn Clock + Send>,
    virtual_clock: Option<VirtualClock>,
    clock_start: Option<(DateTime<Utc>, bool, f64)>,
    currencies: Currencies,
    policy: PaymentPolicy,
    statements: Option<Statements>,
    /// Payments with a transition due, by (due, sequence).
    queue: BTreeSet<(DateTime<Utc>, u64)>,
    open: HashMap<u64, Payment>,
    last_payment: u64,
    next_event_sequence: u64,
    simulation_id: Option<String>,
    delivery_signal: Option<Arc<tokio::sync::Notify>>,
    /// Counts resets; delivery results claimed before one are dropped.
    epoch: u64,
    /// The virtual clock follows another simulator's.
    follow: bool,
    /// Whether the followed clock was last seen behind ours, so the warning is logged once per episode.
    behind: bool,
}

impl Gateway {
    /// Opens the gateway on a migrated writer connection: loads open payments and subscriptions, resumes the clock,
    /// releases webhook claims from a previous process, and runs whatever came due while it was down.
    pub fn recover(conn: Connection, options: GatewayOptions) -> Result<Self, GatewayError> {
        options.payments.validate().map_err(GatewayError::Invalid)?;
        let released = outbox::release_claims(&conn, wall_now())?;
        if released > 0 {
            tracing::info!(
                released,
                "released webhook deliveries claimed before restart"
            );
        }
        let webhooks = WebhookHub::load(
            &conn,
            options.webhooks,
            options.webhook_secret_grace,
            EVENT_TYPES,
            None,
        )?;
        let mut clock = options.clock;
        let clock_start = options
            .virtual_clock
            .as_ref()
            .map(|c| (c.now(), c.is_running(), c.speed()));
        if let Some(virtual_clock) = &options.virtual_clock {
            // Simulated time continues where it was: from the stored reading, or the latest event stamped since.
            if let Some((stored, running)) = outbox::virtual_clock(&conn)? {
                let latest =
                    outbox::latest_clock_stamp(&conn, "")?.map_or(stored, |at| at.max(stored));
                if latest > virtual_clock.now() {
                    virtual_clock.set(latest)?;
                }
                if running {
                    virtual_clock.resume();
                } else {
                    virtual_clock.freeze();
                }
            }
            outbox::save_virtual_clock(&conn, virtual_clock.now(), virtual_clock.is_running())?;
            clock = Box::new(virtual_clock.clone());
        }
        let open = store::open_payments(&conn)?;
        let mut gateway = Self {
            last_payment: store::last_payment_sequence(&conn)?,
            next_event_sequence: outbox::next_event_sequence(&conn)?,
            queue: open
                .iter()
                .map(|p| (p.due_at.expect("open"), p.sequence))
                .collect(),
            open: open.into_iter().map(|p| (p.sequence, p)).collect(),
            conn,
            webhooks,
            ids: options.ids,
            clock,
            virtual_clock: options.virtual_clock,
            clock_start,
            currencies: options.currencies,
            policy: options.payments,
            statements: None,
            simulation_id: options.simulation_id,
            delivery_signal: options.delivery_signal,
            epoch: 0,
            follow: options.follow,
            behind: false,
        };
        gateway.reconcile_webhooks()?;
        if let Some(policy) = options.statements {
            let cursor = match store::statement_cursor(&gateway.conn)? {
                Some(cursor) => cursor,
                None => {
                    let cursor = Statements::first_cursor(&policy, gateway.now());
                    store::save_statement_cursor(&gateway.conn, cursor)?;
                    cursor
                }
            };
            let pending = store::statements(&gateway.conn, None, None, true)?;
            gateway.statements = Some(Statements {
                policy,
                cursor,
                pending,
            });
            gateway.sort_pending();
        }
        gateway.tick()?;
        Ok(gateway)
    }

    /// The writer connection (tests).
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Initiates a deposit or withdrawal. A repeated idempotency key with the same details returns the existing
    /// payment and `true`; with different details it is refused.
    pub fn initiate(&mut self, request: PaymentRequest) -> Result<(Payment, bool), GatewayError> {
        let invalid = |m: String| GatewayError::Invalid(m);
        let PaymentRequest {
            idempotency_key,
            payment_type,
            account_id,
            bank_account,
            amount,
            currency,
            reference,
        } = request;
        if !(1..=64).contains(&idempotency_key.chars().count()) {
            return Err(invalid("idempotencyKey must be 1 to 64 characters".into()));
        }
        if !(1..=64).contains(&account_id.chars().count()) {
            return Err(invalid("accountId must be 1 to 64 characters".into()));
        }
        let iban = iban::normalize(&bank_account.iban);
        iban::validate(&iban).map_err(|e| invalid(format!("bankAccount.iban: {e}")))?;
        let holder_name = bank_account.holder_name.trim().to_string();
        if !(1..=140).contains(&holder_name.chars().count()) {
            return Err(invalid(
                "bankAccount.holderName must be 1 to 140 characters".into(),
            ));
        }
        if reference.as_ref().is_some_and(|r| r.chars().count() > 140) {
            return Err(invalid("reference must be at most 140 characters".into()));
        }
        let currency = currency.to_ascii_uppercase();
        let scale = self
            .currencies
            .scale(&currency)
            .ok_or_else(|| invalid(format!("currency {currency} is not accepted")))?;
        let amount_minor = parse_amount(&amount, scale, false).map_err(invalid)?;
        let bank_account = BankAccount { iban, holder_name };
        if let Some(existing) = store::payment_by_key(&self.conn, &idempotency_key)? {
            let same = existing.payment_type == payment_type
                && existing.account_id == account_id
                && existing.bank_account == bank_account
                && existing.amount_minor == amount_minor
                && existing.currency == currency
                && existing.reference == reference;
            return if same {
                Ok((existing, true))
            } else {
                Err(GatewayError::IdempotencyConflict {
                    key: idempotency_key,
                    existing: existing.id,
                })
            };
        }
        let now = self.now();
        let sequence = self.last_payment + 1;
        let mut payment = Payment {
            id: self.ids.derive(b"payment", &[&sequence.to_le_bytes()]),
            sequence,
            idempotency_key,
            payment_type,
            account_id,
            bank_account,
            amount_minor,
            currency,
            currency_scale: scale,
            reference,
            status: PaymentStatus::Pending,
            reason: None,
            created_at: now,
            due_at: None,
            completed_at: None,
            failed_at: None,
            returned_at: None,
            updated_at: now,
            forced: None,
        };
        payment.due_at = Some(self.policy.outcome_due(&payment));
        let mut events = vec![self.policy.initiated(&payment)];
        self.commit_payments(&[&payment], &[], &mut events)?;
        self.last_payment = sequence;
        self.schedule(payment.clone());
        ::metrics::counter!(crate::metrics::PAYMENTS, "type" => payment.payment_type.as_str())
            .increment(1);
        Ok((payment, false))
    }

    fn schedule(&mut self, payment: Payment) {
        if let Some(old) = self.open.remove(&payment.sequence)
            && let Some(due) = old.due_at
        {
            self.queue.remove(&(due, old.sequence));
        }
        if let Some(due) = payment.due_at {
            self.queue.insert((due, payment.sequence));
            self.open.insert(payment.sequence, payment);
        }
    }

    /// Forces a payment's outcome for a test (decision D20): `outcome` replaces its seeded draws. A pending payment
    /// takes any outcome, due at its scheduled time or `delay` from now; a completed one not yet returned takes only
    /// `RETURNED`, due `returnAfterSeconds` from now. Whatever is due at once happens before this returns the payment.
    pub fn force_outcome(
        &mut self,
        id: Uuid,
        outcome: ForcedOutcome,
        delay: Option<chrono::Duration>,
    ) -> Result<Payment, GatewayError> {
        if let ForcedOutcome::Failed { reason } = &outcome
            && (reason.trim().is_empty() || reason.chars().count() > 64)
        {
            return Err(GatewayError::Invalid(
                "reason must be 1 to 64 characters".into(),
            ));
        }
        let mut payment =
            store::payment(&self.conn, id)?.ok_or(GatewayError::UnknownPayment(id))?;
        let now = self.now();
        let settled = |why| GatewayError::PaymentSettled {
            id,
            status: payment.status.as_str(),
            why,
        };
        match (payment.status, &outcome) {
            (PaymentStatus::Pending, _) => {
                if let Some(delay) = delay {
                    payment.due_at = Some(now + delay);
                }
            }
            (
                PaymentStatus::Completed,
                ForcedOutcome::Returned {
                    return_after_seconds,
                },
            ) if delay.is_none() => {
                payment.due_at =
                    Some(now + chrono::Duration::seconds(*return_after_seconds as i64));
            }
            (PaymentStatus::Completed, ForcedOutcome::Returned { .. }) => {
                return Err(GatewayError::Invalid(
                    "a completed payment takes returnAfterSeconds, not delaySeconds".into(),
                ));
            }
            (PaymentStatus::Completed, _) => {
                return Err(settled("only RETURNED can still apply"));
            }
            (PaymentStatus::Failed | PaymentStatus::Returned, _) => {
                return Err(settled("its outcome is final"));
            }
        }
        payment.forced = Some(outcome);
        self.commit_payments(&[&payment], &[], &mut [])?;
        self.schedule(payment);
        self.tick()?;
        Ok(store::payment(&self.conn, id)?.expect("just stored"))
    }

    /// Stamps `events`, commits them with the payments and bookings, and moves the event sequence on.
    fn commit_payments(
        &mut self,
        payments: &[&Payment],
        bookings: &[&Booking],
        events: &mut [NewEvent],
    ) -> Result<(), GatewayError> {
        let next = NewEvent::stamp(events, self.next_event_sequence, self.ids);
        let outbox = Outbox {
            subscriptions: self.webhooks.subscriptions(),
            simulation_id: self.simulation_id.as_deref(),
            ids: self.ids,
            due: wall_now(),
            schema_version: SCHEMA_VERSION,
            parties: None,
        };
        store::commit_payments(&mut self.conn, payments, bookings, events, outbox)?;
        self.next_event_sequence = next;
        for event in events.iter() {
            ::metrics::counter!(crate::metrics::EVENTS, "event_type" => event.event_type)
                .increment(1);
        }
        self.signal_deliveries(self.webhooks.delivers(events));
        Ok(())
    }

    /// Everything due by now on domain time, in time order: payment transitions, and statements at each cutoff (and
    /// late ones when due). Before each statement, the transitions due by its instant run first, so a statement holds
    /// every booking made before its cutoff. Returns the number of events.
    pub fn tick(&mut self) -> Result<usize, GatewayError> {
        let mut total = 0;
        loop {
            let now = self.now();
            match self.statements.as_ref().map(Statements::due) {
                Some(due) if due <= now => {
                    total += self.payment_step(due)?;
                    total += self.statement_step(due)?;
                }
                _ => return Ok(total + self.payment_step(now)?),
            }
        }
    }

    /// Runs every payment transition due by `until`, in (due, sequence) order, in pages.
    fn payment_step(&mut self, until: DateTime<Utc>) -> Result<usize, GatewayError> {
        const PAGE: usize = 500;
        let mut total = 0;
        loop {
            // A page spans less than a day: a return is due at least a business day after its completion, so one
            // made in this page is due after everything in it, and events stay in time order.
            let Some(&(first, _)) = self.queue.first().filter(|(at, _)| *at <= until) else {
                return Ok(total);
            };
            let horizon =
                until.min(first + chrono::Duration::hours(24) - chrono::Duration::microseconds(1));
            let due: Vec<u64> = self
                .queue
                .iter()
                .take_while(|(at, _)| *at <= horizon)
                .take(PAGE)
                .map(|(_, sequence)| *sequence)
                .collect();
            let transitions: Vec<_> = due
                .iter()
                .map(|sequence| self.policy.transition(&self.open[sequence]))
                .collect();
            let payments: Vec<&Payment> = transitions.iter().map(|t| &t.payment).collect();
            let bookings: Vec<&Booking> = transitions
                .iter()
                .filter_map(|t| t.booking.as_ref())
                .collect();
            let mut events: Vec<NewEvent> = transitions.iter().map(|t| t.event.clone()).collect();
            self.commit_payments(&payments, &bookings, &mut events)?;
            total += transitions.len();
            for transition in transitions {
                ::metrics::counter!(crate::metrics::PAYMENT_OUTCOMES, "status" => transition.payment.status.as_str())
                    .increment(1);
                self.schedule(transition.payment);
            }
        }
    }

    /// One statement step at `at`: the cutoff's statements (those not drafted late are issued at once), and late ones
    /// due by `at`, committed together.
    fn statement_step(&mut self, at: DateTime<Utc>) -> Result<usize, GatewayError> {
        let statements = self.statements.as_ref().expect("statements run");
        let date = next_business_day(statements.cursor);
        let policy = &statements.policy;
        let mut issue: Vec<StoredStatement> = statements
            .pending
            .iter()
            .filter(|p| p.draft.statement.issued_at <= at)
            .cloned()
            .collect();
        let mut waiting = Vec::new();
        let mut cursor = None;
        if policy.calendar.cutoff_at(date) <= at {
            let (from, until) = policy.calendar.window(date);
            for currency in store::booked_currencies(&self.conn, until)? {
                let scale = self.currencies.scale(&currency).unwrap_or(2);
                let opening = policy.opening_balances.get(&currency).copied().unwrap_or(0)
                    + store::booked_balance(&self.conn, &currency, from)?;
                let movements = store::movements(&self.conn, &currency, from, until)?;
                let stored = StoredStatement {
                    draft: policy.draft(date, &currency, scale, opening, &movements),
                    issued: false,
                    event_sequence: None,
                };
                if stored.draft.statement.issued_at <= at {
                    issue.push(stored);
                } else {
                    waiting.push(stored);
                }
            }
            cursor = Some(date);
        }
        issue.sort_by(|a, b| issue_order(a).cmp(&issue_order(b)));
        let mut events: Vec<NewEvent> = issue
            .iter()
            .map(|s| statement::statement_event(&s.draft.statement))
            .collect();
        let next = NewEvent::stamp(&mut events, self.next_event_sequence, self.ids);
        for (statement, event) in issue.iter_mut().zip(&events) {
            statement.issued = true;
            statement.event_sequence = Some(event.sequence);
        }
        let outbox = Outbox {
            subscriptions: self.webhooks.subscriptions(),
            simulation_id: self.simulation_id.as_deref(),
            ids: self.ids,
            due: wall_now(),
            schema_version: SCHEMA_VERSION,
            parties: None,
        };
        let rows: Vec<&StoredStatement> = issue.iter().chain(&waiting).collect();
        store::commit_statements(&mut self.conn, &rows, cursor, &events, outbox)?;
        self.next_event_sequence = next;
        for event in &events {
            ::metrics::counter!(crate::metrics::EVENTS, "event_type" => event.event_type)
                .increment(1);
        }
        let statements = self.statements.as_mut().expect("statements run");
        if let Some(date) = cursor {
            statements.cursor = date;
        }
        statements
            .pending
            .retain(|p| p.draft.statement.issued_at > at);
        statements.pending.extend(waiting);
        self.sort_pending();
        self.signal_deliveries(self.webhooks.delivers(&events));
        Ok(events.len())
    }

    fn sort_pending(&mut self) {
        if let Some(statements) = &mut self.statements {
            statements
                .pending
                .sort_by(|a, b| issue_order(a).cmp(&issue_order(b)));
        }
    }

    pub fn clock_state(&self) -> ClockState {
        ClockState {
            virtual_clock: self.virtual_clock.is_some(),
            running: self
                .virtual_clock
                .as_ref()
                .is_none_or(VirtualClock::is_running),
            speed: self.virtual_clock.as_ref().map_or(1.0, VirtualClock::speed),
            now: self.now(),
        }
    }

    /// Changes the virtual clock, persists it, and runs whatever became due before answering. A following clock
    /// refuses: it moves with the clock it follows.
    pub fn change_clock(&mut self, change: ClockChange) -> Result<ClockState, GatewayError> {
        if self.follow {
            return Err(GatewayError::ClockFollows);
        }
        let clock = self
            .virtual_clock
            .clone()
            .ok_or(GatewayError::ClockNotVirtual)?;
        let changed = change.apply(&clock)?;
        self.clock_changed(change.name(), &clock, changed)?;
        Ok(self.clock_state())
    }

    /// Brings the following clock to `remote`: its speed and running state, and its reading when that is ahead. The
    /// clock never moves backwards, so when the followed clock is behind (it slowed or froze between two reads, or
    /// was reset), this one holds its reading and a warning is logged. A change of speed or running state, or a jump
    /// of more than a simulated minute (or a real second at the followed speed), publishes `ClockChanged` with
    /// `change: follow`; small corrections between reads publish nothing. Returns whether it published.
    pub fn follow(&mut self, remote: RemoteClock) -> Result<bool, GatewayError> {
        let clock = self
            .virtual_clock
            .clone()
            .ok_or(GatewayError::ClockNotVirtual)?;
        let (running, speed, now) = (clock.is_running(), clock.speed(), clock.now());
        if remote.speed != speed {
            clock.set_speed(remote.speed)?;
        }
        match (remote.running, running) {
            (true, false) => clock.resume(),
            (false, true) => clock.freeze(),
            _ => {}
        }
        let mut jumped = false;
        if remote.now > now {
            clock.set(remote.now.max(clock.now()))?;
            let threshold = chrono::Duration::minutes(1).max(chrono::Duration::milliseconds(
                (remote.speed * 1_000.0) as i64,
            ));
            jumped = remote.now - now > threshold;
            self.behind = false;
        } else if now - remote.now > chrono::Duration::seconds(1) && !self.behind {
            self.behind = true;
            tracing::warn!(followed = %remote.now, ours = %now, "the followed clock is behind this one (it slowed, froze or was reset); holding until it catches up");
        }
        let changed = jumped || remote.speed != speed || remote.running != running;
        self.clock_changed("follow", &clock, changed)?;
        Ok(changed)
    }

    /// After a clock change: runs whatever became due by the new time, then (when `changed`) commits the new reading
    /// with its `ClockChanged`, so the log stays in time order.
    fn clock_changed(
        &mut self,
        change: &'static str,
        clock: &VirtualClock,
        changed: bool,
    ) -> Result<(), GatewayError> {
        self.tick()?;
        if changed {
            let mut event =
                simulator_kit::clock::changed_event(change, clock, crate::payments::SOURCE);
            let next = NewEvent::stamp(
                std::slice::from_mut(&mut event),
                self.next_event_sequence,
                self.ids,
            );
            let outbox = Outbox {
                subscriptions: self.webhooks.subscriptions(),
                simulation_id: self.simulation_id.as_deref(),
                ids: self.ids,
                due: wall_now(),
                schema_version: SCHEMA_VERSION,
                parties: None,
            };
            store::commit_clock(
                &mut self.conn,
                clock.now(),
                clock.is_running(),
                &event,
                outbox,
            )?;
            self.next_event_sequence = next;
            ::metrics::counter!(crate::metrics::EVENTS, "event_type" => event.event_type)
                .increment(1);
            self.signal_deliveries(self.webhooks.delivers(std::slice::from_ref(&event)));
        }
        Ok(())
    }

    /// Clears all state, as on a new database; the virtual clock goes back to its first reading.
    pub fn reset(&mut self) -> Result<(), GatewayError> {
        store::reset(&mut self.conn)?;
        self.webhooks.clear();
        self.queue.clear();
        self.open.clear();
        self.last_payment = 0;
        self.next_event_sequence = outbox::next_event_sequence(&self.conn)?;
        if let (Some(clock), Some((start, running, speed))) =
            (&self.virtual_clock, self.clock_start)
        {
            clock.reset(start, running, speed);
            outbox::save_virtual_clock(&self.conn, clock.now(), clock.is_running())?;
        }
        let now = self.now();
        if let Some(statements) = &mut self.statements {
            statements.cursor = Statements::first_cursor(&statements.policy, now);
            statements.pending.clear();
            store::save_statement_cursor(&self.conn, statements.cursor)?;
        }
        self.epoch += 1;
        self.reconcile_webhooks()?;
        tracing::info!(epoch = self.epoch, "gateway state reset");
        Ok(())
    }

    fn reconcile_webhooks(&mut self) -> Result<(), GatewayError> {
        let now = self.now();
        let wake = self.webhooks.reconcile(&self.conn, self.ids, now)?;
        self.signal_deliveries(wake);
        Ok(())
    }

    pub fn register_webhook(
        &mut self,
        request: NewSubscription,
    ) -> Result<Subscription, GatewayError> {
        let now = self.now();
        Ok(self.webhooks.register(&self.conn, request, self.ids, now)?)
    }

    pub fn update_webhook(
        &mut self,
        id: Uuid,
        change: WebhookChange,
    ) -> Result<Subscription, GatewayError> {
        let (subscription, wake) = self.webhooks.update(&self.conn, id, change)?;
        self.signal_deliveries(wake);
        Ok(subscription)
    }

    pub fn delete_webhook(&mut self, id: Uuid) -> Result<(), GatewayError> {
        Ok(self.webhooks.delete(&self.conn, id)?)
    }

    pub fn rotate_webhook_secret(
        &mut self,
        id: Uuid,
        secret: String,
        grace: chrono::Duration,
    ) -> Result<Subscription, GatewayError> {
        Ok(self.webhooks.rotate_secret(&self.conn, id, secret, grace)?)
    }

    pub fn set_webhook_faults(
        &mut self,
        id: Uuid,
        policy: Option<FaultPolicy>,
    ) -> Result<Subscription, GatewayError> {
        Ok(self.webhooks.set_faults(&self.conn, id, policy)?)
    }

    pub fn redeliver(&mut self, id: Uuid, after: u64) -> Result<usize, GatewayError> {
        let queued = self
            .webhooks
            .redeliver(&mut self.conn, id, after, self.ids)?;
        self.signal_deliveries(queued > 0);
        Ok(queued)
    }

    pub fn retry_dead_letters(
        &mut self,
        subscription_id: Option<Uuid>,
        delivery_ids: &[Uuid],
    ) -> Result<usize, GatewayError> {
        let retried =
            self.webhooks
                .retry_dead_letters(&mut self.conn, subscription_id, delivery_ids)?;
        self.signal_deliveries(retried > 0);
        Ok(retried)
    }

    pub fn claim_deliveries(
        &mut self,
        limit: u32,
        lease: Duration,
    ) -> Result<Vec<DeliveryJob>, GatewayError> {
        Ok(self
            .webhooks
            .claim(&mut self.conn, limit, lease, self.epoch)?)
    }

    pub fn record_deliveries(&mut self, results: &[DeliveryResult]) -> Result<(), GatewayError> {
        Ok(self.webhooks.record(&mut self.conn, results, self.epoch)?)
    }

    fn signal_deliveries(&self, any: bool) {
        if any && let Some(signal) = &self.delivery_signal {
            signal.notify_one();
        }
    }

    /// The clock reading at the precision the database stores.
    fn now(&self) -> DateTime<Utc> {
        self.clock.now().trunc_subsecs(6)
    }
}

impl Default for GatewayOptions {
    fn default() -> Self {
        let config = crate::config::Config::defaults();
        Self {
            ids: IdScheme::Random,
            clock: Box::new(SystemClock),
            virtual_clock: None,
            currencies: config.currencies().expect("valid defaults"),
            payments: config.payment_policy().expect("valid defaults"),
            statements: None,
            simulation_id: None,
            delivery_signal: None,
            webhooks: Vec::new(),
            webhook_secret_grace: chrono::Duration::hours(1),
            follow: false,
        }
    }
}

fn issue_order(s: &StoredStatement) -> (DateTime<Utc>, &str) {
    (s.draft.statement.issued_at, &s.draft.statement.statement_id)
}
