use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SubsecRound, Utc};
use uuid::Uuid;

use super::{BookError, BookUpdate, Clock, IdGenerator, MatchContext, OrderBook};
use crate::clock::{ClockError, SystemClock, VirtualClock};
use crate::domain::{
    IdentifierPolicy, Instrument, InstrumentError, InstrumentId, Order, OrderStatus, OrderType,
    Participant, ParticipantId, Side,
};
use crate::events::{self, Command};
use crate::persistence::{Outbox, Store};
use crate::settlement::{
    ExternalReport, NewReport, SettlementPolicy, SettlementSimulator, TradeFacts,
};
use crate::webhooks::{DeliveryJob, DeliveryResult, NewSubscription, Subscription};

/// The settlement simulator's name in `consumer_offsets`.
pub const SETTLEMENT_CONSUMER: &str = "settlement-simulator";

/// Upper bound on a participant's settlement details, serialized.
pub const MAX_SETTLEMENT_BYTES: usize = 4096;

/// A request to place an order; the exchange assigns its ID and sequence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmitOrder {
    pub client_order_id: Option<String>,
    pub instrument_id: InstrumentId,
    pub participant_id: ParticipantId,
    pub side: Side,
    pub order_type: OrderType,
    pub limit_price_ticks: Option<i64>,
    pub quantity: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// The order was sequenced, matched and committed.
    Processed(BookUpdate),
    /// The participant already submitted this client order ID with the same terms; this is that order, now.
    Duplicate(Order),
}

#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    #[error("unknown instrument {0}")]
    UnknownInstrument(InstrumentId),
    #[error("unknown participant {0}")]
    UnknownParticipant(ParticipantId),
    #[error("unknown order {0}")]
    UnknownOrder(Uuid),
    #[error("order {id} is {status} and cannot be cancelled")]
    OrderNotActive { id: Uuid, status: OrderStatus },
    #[error("instrument {0} already exists")]
    DuplicateInstrument(InstrumentId),
    #[error("symbol {symbol} is already listed on {mic}")]
    DuplicateListing { mic: String, symbol: String },
    #[error("participant {0} already exists")]
    DuplicateParticipant(ParticipantId),
    #[error(
        "client order ID {client_order_id} was already used by order {existing} with different terms"
    )]
    ClientOrderIdConflict {
        client_order_id: String,
        existing: Uuid,
    },
    #[error("client order ID must be 1 to 64 characters")]
    InvalidClientOrderId,
    #[error("invalid participant: {0}")]
    InvalidParticipant(String),
    #[error("invalid webhook subscription: {0}")]
    InvalidWebhook(String),
    #[error("the exchange uses the system clock; configure clock.mode = virtual to control time")]
    ClockNotVirtual,
    #[error(transparent)]
    Clock(#[from] ClockError),
    #[error("settlement simulation: {0}")]
    Settlement(String),
    #[error("settlement mode is simulated; externally reported outcomes are not accepted")]
    ExternalOutcomesDisabled,
    #[error("unknown execution {0}")]
    UnknownExecution(Uuid),
    #[error("invalid settlement report: {0}")]
    InvalidReport(String),
    #[error(
        "reference {reference} was already reported for execution {execution_id} with a different outcome"
    )]
    ReportConflict {
        execution_id: Uuid,
        reference: String,
    },
    #[error(transparent)]
    Instrument(#[from] InstrumentError),
    #[error(transparent)]
    Book(#[from] BookError),
    #[error("storage failure: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Construction-time dependencies of an [`Exchange`]. `Default` gives strict identifiers, UUIDv7 IDs and the
/// system clock.
pub struct ExchangeOptions {
    pub identifier_policy: IdentifierPolicy,
    pub ids: Box<dyn IdGenerator + Send>,
    /// Domain time (orders, executions, events). Ignored when `virtual_clock` is set.
    pub clock: Box<dyn Clock + Send>,
    /// A controllable clock for domain time. Its state is persisted, and recovery resumes from the stored reading.
    pub virtual_clock: Option<VirtualClock>,
    /// Stamped on every event as `simulationId`.
    pub simulation_id: Option<String>,
    /// Notified after a commit that created webhook deliveries, so the delivery worker need not wait for its poll.
    pub delivery_signal: Option<Arc<tokio::sync::Notify>>,
    /// Runs the settlement simulator with this policy (settlement mode `simulated` or `both`).
    pub settlement: Option<SettlementPolicy>,
    /// Accepts settlement outcomes reported by an external service (settlement mode `external` or `both`).
    pub external_outcomes: bool,
}

impl Default for ExchangeOptions {
    fn default() -> Self {
        Self {
            identifier_policy: IdentifierPolicy::Strict,
            ids: Box::new(super::UuidV7Ids),
            clock: Box::new(SystemClock),
            virtual_clock: None,
            simulation_id: None,
            delivery_signal: None,
            settlement: None,
            external_outcomes: true,
        }
    }
}

/// How the exchange tells time, for the test-control API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockState {
    pub virtual_clock: bool,
    pub running: bool,
    pub now: DateTime<Utc>,
}

/// A change to the virtual clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockChange {
    Advance(chrono::Duration),
    Set(DateTime<Utc>),
    Freeze,
    Resume,
}

/// The persistence coordinator: owns the order books and the single SQLite writer, and serializes every command.
///
/// Each command is planned on the book, committed to SQLite, and only then applied to memory. When the commit fails
/// the plan is dropped, so memory never holds state the database does not; a caller only sees success after commit.
pub struct Exchange {
    store: Store,
    books: HashMap<InstrumentId, OrderBook>,
    /// Registered participants and their settlement details.
    participants: HashMap<ParticipantId, Option<serde_json::Value>>,
    last_sequence: u64,
    identifier_policy: IdentifierPolicy,
    ids: Box<dyn IdGenerator + Send>,
    clock: Box<dyn Clock + Send>,
    virtual_clock: Option<VirtualClock>,
    settlement: Option<SettlementSimulator>,
    external_outcomes: bool,
    subscriptions: Vec<Subscription>,
    simulation_id: Option<String>,
    delivery_signal: Option<Arc<tokio::sync::Notify>>,
}

impl Exchange {
    /// Loads instruments, participants and webhook subscriptions, rebuilds every book from the active orders in
    /// acceptance sequence, and releases webhook delivery claims left by a previous process.
    pub fn recover(store: Store, options: ExchangeOptions) -> Result<Self, ExchangeError> {
        let mut books: HashMap<_, _> = store
            .instruments(&Default::default())?
            .into_iter()
            .map(|instrument| (instrument.id.clone(), OrderBook::new(instrument)))
            .collect();
        for order in store.active_orders()? {
            let book = books
                .get_mut(&order.instrument_id)
                .ok_or_else(|| ExchangeError::UnknownInstrument(order.instrument_id.clone()))?;
            book.restore(order)?;
        }
        let participants = store
            .participants()?
            .into_iter()
            .map(|p| (p.id, p.settlement))
            .collect();
        let last_sequence = store.max_sequence()?;
        let subscriptions = store.subscriptions()?;
        let released = store.release_claims(wall_now())?;
        let settlement = match options.settlement {
            Some(policy) => {
                policy.validate().map_err(ExchangeError::Settlement)?;
                let cursor = store.consumer_offset(SETTLEMENT_CONSUMER)?;
                Some(SettlementSimulator::new(
                    policy,
                    store.open_settlements()?,
                    cursor,
                ))
            }
            None => None,
        };
        let mut clock = options.clock;
        if let Some(virtual_clock) = &options.virtual_clock {
            // Simulated time continues where it was; it never jumps back to the configured start.
            if let Some((stored, running)) = store.virtual_clock()? {
                if stored > virtual_clock.now() {
                    virtual_clock.set(stored)?;
                }
                if running {
                    virtual_clock.resume()
                } else {
                    virtual_clock.freeze()
                }
            }
            store.save_virtual_clock(virtual_clock.now(), virtual_clock.is_running())?;
            clock = Box::new(virtual_clock.clone());
        }
        if released > 0 {
            tracing::info!(
                released,
                "released webhook deliveries claimed before restart"
            );
        }
        Ok(Self {
            store,
            books,
            participants,
            last_sequence,
            identifier_policy: options.identifier_policy,
            ids: options.ids,
            clock,
            virtual_clock: options.virtual_clock,
            settlement,
            external_outcomes: options.external_outcomes,
            subscriptions,
            simulation_id: options.simulation_id,
            delivery_signal: options.delivery_signal,
        })
    }

    pub fn register_instrument(&mut self, instrument: Instrument) -> Result<(), ExchangeError> {
        instrument.validate(self.identifier_policy)?;
        if self.books.contains_key(&instrument.id) {
            return Err(ExchangeError::DuplicateInstrument(instrument.id));
        }
        if let Some(mic) = &instrument.mic
            && self.books.values().any(|b| {
                b.instrument().mic.as_ref() == Some(mic)
                    && b.instrument().symbol == instrument.symbol
            })
        {
            return Err(ExchangeError::DuplicateListing {
                mic: mic.clone(),
                symbol: instrument.symbol,
            });
        }
        self.store.insert_instrument(&instrument)?;
        self.books
            .insert(instrument.id.clone(), OrderBook::new(instrument));
        Ok(())
    }

    /// Switches trading in an instrument on or off, durably, and returns the updated instrument.
    pub fn set_instrument_enabled(
        &mut self,
        id: &InstrumentId,
        enabled: bool,
    ) -> Result<Instrument, ExchangeError> {
        let book = self
            .books
            .get_mut(id)
            .ok_or_else(|| ExchangeError::UnknownInstrument(id.clone()))?;
        self.store.set_instrument_enabled(id, enabled)?;
        book.set_enabled(enabled);
        Ok(book.instrument().clone())
    }

    pub fn register_participant(&mut self, participant: Participant) -> Result<(), ExchangeError> {
        if participant.id.0.trim().is_empty() || participant.name.trim().is_empty() {
            return Err(ExchangeError::InvalidParticipant(
                "ID and name must not be blank".into(),
            ));
        }
        if let Some(settlement) = &participant.settlement {
            if !settlement.is_object() {
                return Err(ExchangeError::InvalidParticipant(
                    "settlement must be a JSON object".into(),
                ));
            }
            if settlement.to_string().len() > MAX_SETTLEMENT_BYTES {
                return Err(ExchangeError::InvalidParticipant(format!(
                    "settlement must be at most {MAX_SETTLEMENT_BYTES} bytes of JSON"
                )));
            }
        }
        if self.participants.contains_key(&participant.id) {
            return Err(ExchangeError::DuplicateParticipant(participant.id));
        }
        self.store.insert_participant(&participant)?;
        self.participants
            .insert(participant.id, participant.settlement);
        Ok(())
    }

    /// Sequences, matches and commits an order. Malformed orders are refused without a sequence; orders the exchange
    /// rejects (self-trade) are sequenced and stored as `REJECTED`. A repeated client order ID returns the original
    /// order if the terms match and fails otherwise.
    pub fn submit(&mut self, command: SubmitOrder) -> Result<SubmitOutcome, ExchangeError> {
        let book = self
            .books
            .get(&command.instrument_id)
            .ok_or_else(|| ExchangeError::UnknownInstrument(command.instrument_id.clone()))?;
        if !self.participants.contains_key(&command.participant_id) {
            return Err(ExchangeError::UnknownParticipant(command.participant_id));
        }
        if let Some(client_order_id) = &command.client_order_id {
            if client_order_id.is_empty() || client_order_id.chars().count() > 64 {
                return Err(ExchangeError::InvalidClientOrderId);
            }
            if let Some(existing) = self
                .store
                .order_by_client_id(&command.participant_id, client_order_id)?
            {
                return if same_terms(&existing.order, &command) {
                    Ok(SubmitOutcome::Duplicate(existing.order))
                } else {
                    Err(ExchangeError::ClientOrderIdConflict {
                        client_order_id: client_order_id.clone(),
                        existing: existing.order.id,
                    })
                };
            }
        }

        let now = self.now();
        let order = Order {
            id: self.ids.next_id(),
            client_order_id: command.client_order_id,
            instrument_id: command.instrument_id,
            participant_id: command.participant_id,
            side: command.side,
            order_type: command.order_type,
            limit_price_ticks: command.limit_price_ticks,
            quantity: command.quantity,
            remaining_quantity: command.quantity,
            sequence: self.last_sequence + 1,
            status: OrderStatus::Open,
        };
        let started = std::time::Instant::now();
        let update = book.plan_submit(
            order,
            &mut MatchContext {
                now,
                ids: self.ids.as_mut(),
            },
        )?;
        ::metrics::histogram!(crate::metrics::MATCH_DURATION).record(started.elapsed());
        self.commit_and_apply(Command::Submit, update, now)
    }

    /// Cancels the remaining quantity of an active order.
    pub fn cancel(&mut self, order_id: Uuid) -> Result<BookUpdate, ExchangeError> {
        let Some(book) = self
            .books
            .values()
            .find(|book| book.order(order_id).is_some())
        else {
            return Err(match self.store.order(order_id)? {
                Some(record) => ExchangeError::OrderNotActive {
                    id: order_id,
                    status: record.order.status,
                },
                None => ExchangeError::UnknownOrder(order_id),
            });
        };
        let update = book.plan_cancel(order_id)?;
        let now = self.now();
        match self.commit_and_apply(Command::Cancel, update, now)? {
            SubmitOutcome::Processed(update) => Ok(update),
            SubmitOutcome::Duplicate(_) => {
                unreachable!("commit_and_apply only reports processed updates")
            }
        }
    }

    pub fn book(&self, instrument_id: &InstrumentId) -> Option<&OrderBook> {
        self.books.get(instrument_id)
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The sequence of the last order accepted or rejected.
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// Registers a webhook endpoint for the given event types. It receives events committed from now on; earlier
    /// events can be replayed from the events API. With a `secret` (16–256 characters) every delivery is signed (see
    /// [`crate::webhooks::signing`]); with `ordered`, deliveries go out one at a time in sequence order.
    pub fn register_webhook(
        &mut self,
        request: NewSubscription,
    ) -> Result<Subscription, ExchangeError> {
        let NewSubscription {
            url,
            mut event_types,
            secret,
            ordered,
        } = request;
        let parsed = reqwest::Url::parse(&url)
            .map_err(|e| ExchangeError::InvalidWebhook(format!("url: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(ExchangeError::InvalidWebhook(
                "url must be http or https".into(),
            ));
        }
        if event_types.is_empty() {
            return Err(ExchangeError::InvalidWebhook(
                "eventTypes must not be empty".into(),
            ));
        }
        if let Some(unknown) = event_types
            .iter()
            .find(|t| !events::EVENT_TYPES.contains(&t.as_str()))
        {
            return Err(ExchangeError::InvalidWebhook(format!(
                "unknown event type {unknown}"
            )));
        }
        if let Some(secret) = &secret
            && !(16..=256).contains(&secret.chars().count())
        {
            return Err(ExchangeError::InvalidWebhook(
                "secret must be 16 to 256 characters".into(),
            ));
        }
        event_types.sort();
        event_types.dedup();
        let subscription = Subscription {
            id: self.ids.next_id(),
            url,
            event_types,
            enabled: true,
            created_at: self.now(),
            secret,
            ordered,
        };
        self.store.insert_subscription(&subscription)?;
        self.subscriptions.push(subscription.clone());
        Ok(subscription)
    }

    /// Claims up to `limit` due webhook deliveries, leased for `lease`.
    pub fn claim_deliveries(
        &mut self,
        limit: u32,
        lease: Duration,
    ) -> Result<Vec<DeliveryJob>, ExchangeError> {
        let now = wall_now();
        let lease_until = now + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::MAX);
        Ok(self.store.claim_deliveries(now, lease_until, limit)?)
    }

    pub fn record_deliveries(&mut self, results: &[DeliveryResult]) -> Result<(), ExchangeError> {
        let now = wall_now();
        Ok(self.store.record_deliveries(results, now)?)
    }

    /// One settlement simulator step: takes in `TradeExecuted` events past the stored cursor (in pages), then runs
    /// every lifecycle transition due by now. Each page is committed (instructions, events, outbox, cursor) before it
    /// is applied. Returns the number of transitions; 0 when the simulator is off.
    pub fn settlement_tick(&mut self) -> Result<usize, ExchangeError> {
        const PAGE: u32 = 500;
        let mut total = 0;
        loop {
            let Some(simulator) = &self.settlement else {
                return Ok(0);
            };
            let page = self
                .store
                .events(simulator.cursor(), PAGE, Some(events::TRADE_EXECUTED))?;
            let trades = page
                .iter()
                .map(TradeFacts::from_event)
                .collect::<Result<Vec<_>, _>>()
                .map_err(ExchangeError::Settlement)?;
            let now = self.now();
            let plan = simulator.plan(&trades, now, self.ids.as_mut());
            if plan.transitions.is_empty() && plan.cursor == simulator.cursor() {
                return Ok(total);
            }
            let outbox = Outbox {
                subscriptions: &self.subscriptions,
                simulation_id: self.simulation_id.as_deref(),
                ids: self.ids.as_mut(),
                due: wall_now(),
            };
            self.store.commit_settlement(
                &plan.transitions,
                SETTLEMENT_CONSUMER,
                plan.cursor,
                outbox,
            )?;
            for transition in &plan.transitions {
                ::metrics::counter!(crate::metrics::EVENTS, "event_type" => transition.event.event_type)
                    .increment(1);
            }
            let delivers = plan.transitions.iter().any(|t| {
                self.subscriptions
                    .iter()
                    .any(|s| s.wants(t.event.event_type))
            });
            let simulator = self.settlement.as_mut().expect("checked above");
            simulator.apply(&plan);
            ::metrics::gauge!(crate::metrics::SETTLEMENT_OPEN).set(simulator.open() as f64);
            total += plan.transitions.len();
            if delivers && let Some(signal) = &self.delivery_signal {
                signal.notify_one();
            }
            if page.len() < PAGE as usize {
                return Ok(total);
            }
        }
    }

    /// Stores a settlement outcome reported by an external service and publishes `SettlementOutcomeReported`
    /// (`source: "external"`). It never touches a simulated instruction. A repeated `reference` with the same outcome
    /// returns the stored report with `true`; with a different outcome it fails.
    pub fn report_settlement(
        &mut self,
        report: NewReport,
    ) -> Result<(ExternalReport, bool), ExchangeError> {
        if !self.external_outcomes {
            return Err(ExchangeError::ExternalOutcomesDisabled);
        }
        if let Some(reference) = &report.reference
            && !(1..=128).contains(&reference.chars().count())
        {
            return Err(ExchangeError::InvalidReport(
                "reference must be 1 to 128 characters".into(),
            ));
        }
        if let Some(details) = &report.details
            && (!details.is_object() || details.to_string().len() > MAX_SETTLEMENT_BYTES)
        {
            return Err(ExchangeError::InvalidReport(format!(
                "details must be a JSON object of at most {MAX_SETTLEMENT_BYTES} bytes"
            )));
        }
        if !self.store.execution_exists(report.execution_id)? {
            return Err(ExchangeError::UnknownExecution(report.execution_id));
        }
        if let Some(reference) = &report.reference
            && let Some(existing) = self
                .store
                .report_by_reference(report.execution_id, reference)?
        {
            return if report.repeats(&existing) {
                Ok((existing, true))
            } else {
                Err(ExchangeError::ReportConflict {
                    execution_id: report.execution_id,
                    reference: reference.clone(),
                })
            };
        }
        let received_at = self.now();
        let stored = ExternalReport {
            id: self.ids.next_id(),
            execution_id: report.execution_id,
            status: report.status,
            reference: report.reference,
            reason: report.reason,
            details: report.details,
            occurred_at: report.occurred_at.unwrap_or(received_at),
            received_at,
        };
        let event = events::NewEvent {
            event_id: self.ids.next_id(),
            event_type: events::SETTLEMENT_OUTCOME_REPORTED,
            source: events::EXTERNAL_SOURCE,
            aggregate_id: stored.execution_id.to_string(),
            correlation_id: Some(stored.execution_id.to_string()),
            occurred_at: stored.occurred_at,
            payload: serde_json::json!({
                "reportId": stored.id,
                "executionId": stored.execution_id,
                "status": stored.status,
                "reference": stored.reference,
                "reason": stored.reason,
                "details": stored.details,
                "receivedAt": stored.received_at,
            }),
        };
        let outbox = Outbox {
            subscriptions: &self.subscriptions,
            simulation_id: self.simulation_id.as_deref(),
            ids: self.ids.as_mut(),
            due: wall_now(),
        };
        self.store.commit_report(&stored, &event, outbox)?;
        ::metrics::counter!(crate::metrics::EVENTS, "event_type" => event.event_type).increment(1);
        if self.subscriptions.iter().any(|s| s.wants(event.event_type))
            && let Some(signal) = &self.delivery_signal
        {
            signal.notify_one();
        }
        Ok((stored, false))
    }

    /// Whether the settlement simulator runs.
    pub fn simulates_settlement(&self) -> bool {
        self.settlement.is_some()
    }

    /// The clock the exchange uses for domain time.
    pub fn clock_state(&self) -> ClockState {
        ClockState {
            virtual_clock: self.virtual_clock.is_some(),
            running: self
                .virtual_clock
                .as_ref()
                .is_none_or(VirtualClock::is_running),
            now: self.now(),
        }
    }

    /// Changes the virtual clock and persists its new state. It never moves backwards.
    pub fn change_clock(&mut self, change: ClockChange) -> Result<ClockState, ExchangeError> {
        let clock = self
            .virtual_clock
            .as_ref()
            .ok_or(ExchangeError::ClockNotVirtual)?;
        match change {
            ClockChange::Advance(by) => clock.advance(by)?,
            ClockChange::Set(to) => clock.set(to)?,
            ClockChange::Freeze => clock.freeze(),
            ClockChange::Resume => clock.resume(),
        }
        self.store
            .save_virtual_clock(clock.now(), clock.is_running())?;
        // Whatever became due by the new time happens before the caller hears back.
        self.settlement_tick()?;
        Ok(self.clock_state())
    }

    fn commit_and_apply(
        &mut self,
        command: Command,
        update: BookUpdate,
        now: DateTime<Utc>,
    ) -> Result<SubmitOutcome, ExchangeError> {
        let instrument = self.books[&update.order.instrument_id].instrument();
        let participants = &self.participants;
        let settlement = |id: &ParticipantId| participants.get(id).cloned().flatten();
        let new_events = events::for_update(
            command,
            &update,
            instrument,
            &settlement,
            now,
            self.ids.as_mut(),
        );
        let started = std::time::Instant::now();
        let outbox = Outbox {
            subscriptions: &self.subscriptions,
            simulation_id: self.simulation_id.as_deref(),
            ids: self.ids.as_mut(),
            due: wall_now(),
        };
        self.store.commit(&update, &new_events, outbox, now)?;
        ::metrics::histogram!(crate::metrics::COMMIT_DURATION).record(started.elapsed());
        record_committed(command, &update, &new_events);
        let book = self
            .books
            .get_mut(&update.order.instrument_id)
            .expect("planned on this book");
        book.apply(&update)
            .expect("nothing else touched the book between plan and apply");
        self.last_sequence = self.last_sequence.max(update.order.sequence);
        let delivers = new_events
            .iter()
            .any(|e| self.subscriptions.iter().any(|s| s.wants(e.event_type)));
        if delivers && let Some(signal) = &self.delivery_signal {
            signal.notify_one();
        }
        Ok(SubmitOutcome::Processed(update))
    }

    /// The clock reading at the precision the database stores, so memory and storage agree.
    fn now(&self) -> DateTime<Utc> {
        self.clock.now().trunc_subsecs(6)
    }
}

/// Real time, at the precision the database stores, for webhook delivery scheduling.
fn wall_now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

fn record_committed(command: Command, update: &BookUpdate, new_events: &[events::NewEvent]) {
    use crate::metrics::{EVENTS, EXECUTIONS, ORDERS, TRADED_QUANTITY};
    if command == Command::Submit {
        ::metrics::counter!(ORDERS, "status" => update.order.status.as_str()).increment(1);
    }
    ::metrics::counter!(EXECUTIONS).increment(update.executions.len() as u64);
    ::metrics::counter!(TRADED_QUANTITY)
        .increment(update.executions.iter().map(|e| e.quantity).sum());
    for event in new_events {
        ::metrics::counter!(EVENTS, "event_type" => event.event_type).increment(1);
    }
}

fn same_terms(order: &Order, command: &SubmitOrder) -> bool {
    order.instrument_id == command.instrument_id
        && order.side == command.side
        && order.order_type == command.order_type
        && order.limit_price_ticks == command.limit_price_ticks
        && order.quantity == command.quantity
}
