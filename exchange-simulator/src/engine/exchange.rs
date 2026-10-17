use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SubsecRound, Utc};
use uuid::Uuid;

use super::{BookError, BookUpdate, Clock, IdScheme, MatchContext, OrderBook};
use crate::clock::{ClockError, SystemClock, VirtualClock};
use crate::domain::{
    IdentifierPolicy, Instrument, InstrumentError, InstrumentId, Order, OrderStatus, OrderType,
    Participant, ParticipantId, Side,
};
use crate::events::NewEvent;
use crate::events::{self, Command};
use crate::persistence::{Outbox, SettlementWrite, Store};
use crate::settlement::{
    ExternalReport, NewReport, SettlementPolicy, SettlementSimulator, TradeFacts,
};
use crate::webhooks::{
    DeclaredSubscription, DeliveryJob, DeliveryResult, FaultPolicy, NewSubscription, Subscription,
    SubscriptionSource, WebhookChange,
};

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
    #[error("unknown webhook subscription {0}")]
    UnknownSubscription(Uuid),
    #[error(
        "webhook subscription {name} ({id}) is declared in the configuration (webhooks.subscriptions): change it there \
         and restart; through the API only `paused` can be changed, until the next restart"
    )]
    ManagedByConfiguration { id: Uuid, name: String },
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
    /// The commit of a group of commands failed (see [`Exchange::commit_group`]); none of them took effect.
    #[error("storage failure, nothing was stored: {0}")]
    GroupCommit(String),
}

/// Construction-time dependencies of an [`Exchange`]. `Default` gives strict identifiers, UUIDv7 IDs and the
/// system clock.
pub struct ExchangeOptions {
    pub identifier_policy: IdentifierPolicy,
    /// How IDs are made; `Deterministic` makes every ID reproducible from the seed and the command order.
    pub ids: IdScheme,
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
    /// Webhook subscriptions declared in configuration, reconciled at recovery and after a reset. Names must be
    /// unique (see [`crate::webhooks::validate_declared`]).
    pub webhooks: Vec<DeclaredSubscription>,
    /// How long a declared subscription's previous secret stays valid after the configuration changes it.
    pub webhook_secret_grace: chrono::Duration,
}

impl Default for ExchangeOptions {
    fn default() -> Self {
        Self {
            identifier_policy: IdentifierPolicy::Strict,
            ids: IdScheme::Random,
            clock: Box::new(SystemClock),
            virtual_clock: None,
            simulation_id: None,
            delivery_signal: None,
            settlement: None,
            external_outcomes: true,
            webhooks: Vec::new(),
            webhook_secret_grace: chrono::Duration::hours(1),
        }
    }
}

/// How the exchange tells time, for the test-control API.
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

/// The persistence coordinator: owns the order books and the single SQLite writer, and serializes every command.
///
/// Each command is planned on the book, committed to SQLite, and only then applied to memory. When the commit fails
/// the plan is dropped, so memory never holds state the database does not; a caller only sees success after commit.
///
/// Orders and cancellations can also be committed as a group ([`Exchange::begin_group`]): each is planned, written
/// and applied in turn, so the next one sees its effect, and the group becomes durable in one commit
/// ([`Exchange::commit_group`]). Until then memory is ahead of storage, so the caller must hold every answer back and
/// run nothing else in between. If the group's commit fails, memory is reloaded from storage, which then holds none of
/// the group.
pub struct Exchange {
    store: Store,
    books: HashMap<InstrumentId, OrderBook>,
    /// Registered participants and their settlement details.
    participants: HashMap<ParticipantId, Option<serde_json::Value>>,
    last_sequence: u64,
    identifier_policy: IdentifierPolicy,
    ids: IdScheme,
    /// The sequence the next event gets. The single writer assigns sequences (and with them event IDs) before commit,
    /// and moves this on only once the commit succeeded.
    next_event_sequence: u64,
    clock: Box<dyn Clock + Send>,
    virtual_clock: Option<VirtualClock>,
    settlement: Option<SettlementSimulator>,
    external_outcomes: bool,
    subscriptions: Vec<Subscription>,
    simulation_id: Option<String>,
    delivery_signal: Option<Arc<tokio::sync::Notify>>,
    /// The virtual clock's first reading and whether it started running: where a reset puts it.
    clock_start: Option<(DateTime<Utc>, bool, f64)>,
    /// Counts resets. Delivery claims carry it, so results for deliveries claimed before a reset are dropped.
    epoch: u64,
    /// The open commit group: what its commands wrote, reported once the group is durable.
    group: Option<Vec<Committed>>,
    /// Webhook subscriptions declared in configuration (see [`Exchange::reconcile_webhooks`]).
    declared_webhooks: Vec<DeclaredSubscription>,
    webhook_secret_grace: chrono::Duration,
}

/// What one committed command wrote, for metrics and the delivery worker.
struct Committed {
    order_status: Option<&'static str>,
    executions: u64,
    quantity: u64,
    event_types: Vec<&'static str>,
    delivers: bool,
}

impl Committed {
    fn record(&self) {
        use crate::metrics::{EVENTS, EXECUTIONS, ORDERS, TRADED_QUANTITY};
        if let Some(status) = self.order_status {
            ::metrics::counter!(ORDERS, "status" => status).increment(1);
        }
        ::metrics::counter!(EXECUTIONS).increment(self.executions);
        ::metrics::counter!(TRADED_QUANTITY).increment(self.quantity);
        for event_type in &self.event_types {
            ::metrics::counter!(EVENTS, "event_type" => *event_type).increment(1);
        }
    }
}

impl Exchange {
    /// Loads instruments, participants and webhook subscriptions, rebuilds every book from the active orders in
    /// acceptance sequence, and releases webhook delivery claims left by a previous process.
    pub fn recover(store: Store, options: ExchangeOptions) -> Result<Self, ExchangeError> {
        let books = load_books(&store)?;
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
        let clock_start = options
            .virtual_clock
            .as_ref()
            .map(|c| (c.now(), c.is_running(), c.speed()));
        if let Some(virtual_clock) = &options.virtual_clock {
            // Simulated time continues where it was; it never jumps back to the configured start. A running clock's
            // stored reading is from its last change, so it can be behind what was stamped since (by a lot on a fast
            // clock): it continues from the latest stamp instead.
            if let Some((stored, running)) = store.virtual_clock()? {
                let latest = store
                    .latest_clock_stamp()?
                    .map_or(stored, |at| at.max(stored));
                if latest > virtual_clock.now() {
                    virtual_clock.set(latest)?;
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
        let next_event_sequence = store.next_event_sequence()?;
        let mut exchange = Self {
            store,
            books,
            participants,
            last_sequence,
            identifier_policy: options.identifier_policy,
            ids: options.ids,
            next_event_sequence,
            clock,
            virtual_clock: options.virtual_clock,
            settlement,
            external_outcomes: options.external_outcomes,
            subscriptions,
            simulation_id: options.simulation_id,
            delivery_signal: options.delivery_signal,
            clock_start,
            epoch: 0,
            group: None,
            declared_webhooks: options.webhooks,
            webhook_secret_grace: options.webhook_secret_grace,
        };
        crate::webhooks::validate_declared(&exchange.declared_webhooks)
            .map_err(ExchangeError::InvalidWebhook)?;
        exchange.reconcile_webhooks()?;
        // Trades committed while the simulator was off (or before it existed) are taken in now, before any command,
        // so intake order does not depend on when a background tick happens to run.
        exchange.settlement_tick()?;
        Ok(exchange)
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
            id: self.ids.order(self.last_sequence + 1),
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
        let update = book.plan_submit(order, &mut MatchContext { now, ids: self.ids })?;
        ::metrics::histogram!(crate::metrics::MATCH_DURATION).record(started.elapsed());
        self.commit_and_apply(Command::Submit, update, now)
    }

    /// Opens a commit group: the following [`Exchange::submit`]s and [`Exchange::cancel`]s become durable together
    /// at [`Exchange::commit_group`]. Nothing else may run while the group is open.
    pub fn begin_group(&mut self) -> Result<(), ExchangeError> {
        assert!(self.group.is_none(), "a commit group is already open");
        self.store.begin_group()?;
        self.group = Some(Vec::new());
        Ok(())
    }

    /// Makes the open group durable in one commit. If that fails, nothing of the group is stored, and memory is
    /// reloaded from storage so it holds none of the group either.
    ///
    /// # Panics
    /// If memory cannot be reloaded after a failed commit: memory and storage would then disagree, so the exchange
    /// must not go on.
    pub fn commit_group(&mut self) -> Result<(), ExchangeError> {
        let committed = self.group.take().expect("a commit group is open");
        let started = std::time::Instant::now();
        if let Err(error) = self.store.commit_group() {
            tracing::error!(%error, commands = committed.len(), "group commit failed; reloading from storage");
            if let Err(reload) = self.reload() {
                panic!("cannot reload the exchange after a failed group commit: {reload}");
            }
            return Err(error.into());
        }
        ::metrics::histogram!(crate::metrics::COMMIT_DURATION).record(started.elapsed());
        for command in &committed {
            command.record();
        }
        self.signal_deliveries(committed.iter().any(|c| c.delivers));
        Ok(())
    }

    /// Rebuilds what commands change in memory (books, sequences, the settlement simulator) from storage.
    fn reload(&mut self) -> Result<(), ExchangeError> {
        self.books = load_books(&self.store)?;
        self.last_sequence = self.store.max_sequence()?;
        self.next_event_sequence = self.store.next_event_sequence()?;
        if let Some(simulator) = &mut self.settlement {
            *simulator = SettlementSimulator::new(
                simulator.policy().clone(),
                self.store.open_settlements()?,
                self.store.consumer_offset(SETTLEMENT_CONSUMER)?,
            );
        }
        Ok(())
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
        let request = validate_subscription(request)?;
        self.create_webhook(request, None, SubscriptionSource::Api, (true, false))
    }

    /// Stores a new, already validated subscription.
    fn create_webhook(
        &mut self,
        request: NewSubscription,
        name: Option<String>,
        source: SubscriptionSource,
        (enabled, paused): (bool, bool),
    ) -> Result<Subscription, ExchangeError> {
        let NewSubscription {
            url,
            event_types,
            secret,
            ordered,
        } = request;
        let subscription = Subscription {
            id: self.ids.subscription(self.subscriptions.len() as u64 + 1),
            url,
            event_types,
            enabled,
            created_at: self.now(),
            secret,
            ordered,
            faults: None,
            name,
            source,
            paused,
            previous_secret: None,
            deleted: false,
        };
        self.store.insert_subscription(&subscription)?;
        self.subscriptions.push(subscription.clone());
        Ok(subscription)
    }

    /// Brings the subscriptions declared in configuration (`webhooks.subscriptions`) into the database, by name:
    /// creates the missing ones, updates the ones whose url, event types, secret, ordering, enabled or paused flag
    /// changed,
    /// and disables configuration-owned ones no longer declared (keeping their history). Subscriptions created through
    /// the API are left alone. Runs at recovery and after a reset.
    fn reconcile_webhooks(&mut self) -> Result<(), ExchangeError> {
        let declared = self.declared_webhooks.clone();
        let mut wake = false;
        for entry in &declared {
            let wanted =
                validate_subscription(entry.subscription.clone()).map_err(|e| match e {
                    ExchangeError::InvalidWebhook(message) => ExchangeError::InvalidWebhook(
                        format!("webhooks.subscriptions {}: {message}", entry.name),
                    ),
                    other => other,
                })?;
            let existing = self
                .subscriptions
                .iter()
                .position(|s| s.name.as_deref() == Some(entry.name.as_str()));
            match existing {
                None => {
                    let created = self.create_webhook(
                        wanted,
                        Some(entry.name.clone()),
                        SubscriptionSource::Config,
                        (entry.enabled, entry.paused),
                    )?;
                    tracing::info!(name = %entry.name, id = %created.id, url = %created.url, "webhook subscription created from configuration");
                    wake |= created.enabled && !created.paused;
                }
                Some(index) => {
                    let current = &self.subscriptions[index];
                    if current.source != SubscriptionSource::Config {
                        return Err(ExchangeError::InvalidWebhook(format!(
                            "webhooks.subscriptions {}: the name belongs to a subscription created through the API",
                            entry.name
                        )));
                    }
                    let previous_secret = if wanted.secret == current.secret {
                        current.previous_secret.clone()
                    } else {
                        self.grace(current.secret.clone(), wanted.secret.is_some())
                    };
                    let updated = Subscription {
                        url: wanted.url,
                        event_types: wanted.event_types,
                        secret: wanted.secret,
                        ordered: wanted.ordered,
                        enabled: entry.enabled,
                        paused: entry.paused,
                        previous_secret,
                        ..current.clone()
                    };
                    if updated != *current {
                        self.store.update_subscription(&updated)?;
                        tracing::info!(name = %entry.name, id = %updated.id, url = %updated.url, enabled = updated.enabled, paused = updated.paused, "webhook subscription updated from configuration");
                        wake |= updated.enabled && !updated.paused;
                        self.subscriptions[index] = updated;
                    }
                }
            }
        }
        for subscription in &mut self.subscriptions {
            let still_declared = declared
                .iter()
                .any(|d| subscription.name.as_deref() == Some(d.name.as_str()));
            if subscription.source == SubscriptionSource::Config
                && subscription.enabled
                && !still_declared
            {
                subscription.enabled = false;
                self.store.update_subscription(subscription)?;
                tracing::warn!(name = ?subscription.name, id = %subscription.id, "webhook subscription no longer in the configuration; disabled, its history kept");
            }
        }
        self.signal_deliveries(wake);
        Ok(())
    }

    /// The previous secret to keep signing with after a change away from `old`: for `webhook_secret_grace`, when there
    /// was an old secret and there still is a new one.
    fn grace(&self, old: Option<String>, signed: bool) -> Option<(String, DateTime<Utc>)> {
        old.filter(|_| signed && self.webhook_secret_grace > chrono::Duration::zero())
            .map(|old| (old, wall_now() + self.webhook_secret_grace))
    }

    /// The index of a subscription that exists and is not deleted.
    fn live_subscription(&self, id: Uuid) -> Result<usize, ExchangeError> {
        self.subscriptions
            .iter()
            .position(|s| s.id == id && !s.deleted)
            .ok_or(ExchangeError::UnknownSubscription(id))
    }

    /// Refuses to change a configuration-owned subscription through the API.
    fn api_owned(&self, index: usize) -> Result<(), ExchangeError> {
        let subscription = &self.subscriptions[index];
        match subscription.source {
            SubscriptionSource::Api => Ok(()),
            SubscriptionSource::Config => Err(ExchangeError::ManagedByConfiguration {
                id: subscription.id,
                name: subscription.name.clone().unwrap_or_default(),
            }),
        }
    }

    /// Changes a subscription's URL, event types, ordering, or enabled and paused flags; unset fields stay. Pending
    /// deliveries keep their place and go to the new URL. A subscription declared in configuration only takes
    /// `paused`, which lasts until the next restart reapplies the configuration.
    pub fn update_webhook(
        &mut self,
        id: Uuid,
        change: WebhookChange,
    ) -> Result<Subscription, ExchangeError> {
        let index = self.live_subscription(id)?;
        let WebhookChange {
            url,
            event_types,
            ordered,
            enabled,
            paused,
        } = change;
        if url.is_some() || event_types.is_some() || ordered.is_some() || enabled.is_some() {
            self.api_owned(index)?;
        }
        let current = &self.subscriptions[index];
        let wanted = validate_subscription(NewSubscription {
            url: url.unwrap_or_else(|| current.url.clone()),
            event_types: event_types.unwrap_or_else(|| current.event_types.clone()),
            secret: current.secret.clone(),
            ordered: ordered.unwrap_or(current.ordered),
        })?;
        let updated = Subscription {
            url: wanted.url,
            event_types: wanted.event_types,
            ordered: wanted.ordered,
            enabled: enabled.unwrap_or(current.enabled),
            paused: paused.unwrap_or(current.paused),
            ..current.clone()
        };
        if updated != *current {
            self.store.update_subscription(&updated)?;
            self.signal_deliveries(updated.enabled && !updated.paused);
            self.subscriptions[index] = updated;
        }
        Ok(self.subscriptions[index].clone())
    }

    /// Deletes a subscription created through the API: it disappears from the API, gets no new deliveries and sends
    /// none of its pending ones. Its deliveries and attempts stay in the database.
    pub fn delete_webhook(&mut self, id: Uuid) -> Result<(), ExchangeError> {
        let index = self.live_subscription(id)?;
        self.api_owned(index)?;
        let mut deleted = self.subscriptions[index].clone();
        deleted.deleted = true;
        self.store.update_subscription(&deleted)?;
        self.subscriptions[index] = deleted;
        Ok(())
    }

    /// Replaces a subscription's signing secret (API-created subscriptions only). For `grace` the previous secret stays
    /// valid: deliveries carry signatures under both, so the consumer can switch without rejecting any.
    pub fn rotate_webhook_secret(
        &mut self,
        id: Uuid,
        secret: String,
        grace: chrono::Duration,
    ) -> Result<Subscription, ExchangeError> {
        let index = self.live_subscription(id)?;
        self.api_owned(index)?;
        let current = &self.subscriptions[index];
        let wanted = validate_subscription(NewSubscription {
            url: current.url.clone(),
            event_types: current.event_types.clone(),
            secret: Some(secret),
            ordered: current.ordered,
        })?;
        let previous_secret = current
            .secret
            .clone()
            .filter(|old| Some(old) != wanted.secret.as_ref() && grace > chrono::Duration::zero())
            .map(|old| (old, wall_now() + grace));
        let updated = Subscription {
            secret: wanted.secret,
            previous_secret,
            ..current.clone()
        };
        self.store.update_subscription(&updated)?;
        self.subscriptions[index] = updated;
        Ok(self.subscriptions[index].clone())
    }

    /// Sets (or with `None` clears) a subscription's delivery fault policy. It applies to attempts from the next claim
    /// on. Out-of-order holds are refused for an ordered subscription, which by definition never reorders.
    pub fn set_webhook_faults(
        &mut self,
        subscription_id: Uuid,
        policy: Option<FaultPolicy>,
    ) -> Result<Subscription, ExchangeError> {
        let subscription = self
            .subscriptions
            .iter_mut()
            .find(|s| s.id == subscription_id && !s.deleted)
            .ok_or(ExchangeError::UnknownSubscription(subscription_id))?;
        if let Some(policy) = &policy {
            policy.validate().map_err(ExchangeError::InvalidWebhook)?;
            if subscription.ordered && policy.out_of_order.is_some() {
                return Err(ExchangeError::InvalidWebhook(
                    "outOfOrder cannot apply to an ordered subscription".into(),
                ));
            }
        }
        self.store
            .set_fault_policy(subscription_id, policy.as_ref())?;
        subscription.faults = policy;
        Ok(subscription.clone())
    }

    /// Claims up to `limit` due webhook deliveries, leased for `lease`.
    pub fn claim_deliveries(
        &mut self,
        limit: u32,
        lease: Duration,
    ) -> Result<Vec<DeliveryJob>, ExchangeError> {
        let now = wall_now();
        let lease_until = now + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::MAX);
        let mut jobs = self.store.claim_deliveries(now, lease_until, limit)?;
        for job in &mut jobs {
            job.epoch = self.epoch;
        }
        Ok(jobs)
    }

    /// Records delivery results, except those claimed before a reset: their deliveries are gone (or, with
    /// deterministic IDs, a new delivery may have the same ID).
    pub fn record_deliveries(&mut self, results: &[DeliveryResult]) -> Result<(), ExchangeError> {
        let now = wall_now();
        let current: Vec<DeliveryResult> = results
            .iter()
            .filter(|r| r.epoch == self.epoch)
            .cloned()
            .collect();
        if current.len() < results.len() {
            tracing::debug!(
                dropped = results.len() - current.len(),
                "dropped delivery results claimed before a reset"
            );
        }
        Ok(self.store.record_deliveries(&current, now)?)
    }

    /// Clears all state: every table emptied, books, participants, subscriptions and the settlement simulator back to
    /// nothing, sequences starting at 1, and the virtual clock back at its first reading (the configured start, or
    /// without one the time the service started). The service then behaves
    /// as on a new database, so a deterministic run after a reset repeats a fresh one, IDs included. Only the
    /// configuration remains.
    pub fn reset(&mut self) -> Result<(), ExchangeError> {
        self.store.reset()?;
        self.books.clear();
        self.participants.clear();
        self.subscriptions.clear();
        self.last_sequence = 0;
        self.next_event_sequence = self.store.next_event_sequence()?;
        if let Some(simulator) = &mut self.settlement {
            *simulator = SettlementSimulator::new(simulator.policy().clone(), Vec::new(), 0);
        }
        if let (Some(clock), Some((start, running, speed))) =
            (&self.virtual_clock, self.clock_start)
        {
            clock.reset(start, running, speed);
            self.store
                .save_virtual_clock(clock.now(), clock.is_running())?;
        }
        self.epoch += 1;
        // Configuration-declared subscriptions come back, as on a new database.
        self.reconcile_webhooks()?;
        tracing::info!(epoch = self.epoch, "exchange state reset");
        Ok(())
    }

    /// Queues the subscription's events after sequence `after` for delivery again (see [`Store::redeliver`]).
    pub fn redeliver(&mut self, subscription_id: Uuid, after: u64) -> Result<usize, ExchangeError> {
        let subscription = self
            .subscriptions
            .iter()
            .find(|s| s.id == subscription_id && !s.deleted)
            .ok_or(ExchangeError::UnknownSubscription(subscription_id))?;
        let queued = self
            .store
            .redeliver(subscription, after, wall_now(), &self.ids)?;
        self.signal_deliveries(queued > 0);
        Ok(queued)
    }

    /// Gives dead-lettered deliveries a fresh attempt budget: all, one subscription's, or the listed ones.
    pub fn retry_dead_letters(
        &mut self,
        subscription_id: Option<Uuid>,
        delivery_ids: &[Uuid],
    ) -> Result<usize, ExchangeError> {
        if let Some(id) = subscription_id
            && !self.subscriptions.iter().any(|s| s.id == id && !s.deleted)
        {
            return Err(ExchangeError::UnknownSubscription(id));
        }
        let retried = self
            .store
            .retry_dead_letters(subscription_id, delivery_ids, wall_now())?;
        self.signal_deliveries(retried > 0);
        Ok(retried)
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
            let plan = simulator.plan(&trades, now);
            if plan.transitions.is_empty() && plan.cursor == simulator.cursor() {
                return Ok(total);
            }
            let mut stamped: Vec<NewEvent> =
                plan.transitions.iter().map(|t| t.event.clone()).collect();
            let next = NewEvent::stamp(&mut stamped, self.next_event_sequence, self.ids);
            let outbox = Outbox {
                subscriptions: &self.subscriptions,
                simulation_id: self.simulation_id.as_deref(),
                ids: self.ids,
                due: wall_now(),
            };
            let write = SettlementWrite {
                instructions: plan.transitions.iter().map(|t| &t.instruction).collect(),
                consumer: SETTLEMENT_CONSUMER,
                cursor: plan.cursor,
            };
            self.store.commit_settlement(&stamped, write, outbox)?;
            self.next_event_sequence = next;
            for event in &stamped {
                ::metrics::counter!(crate::metrics::EVENTS, "event_type" => event.event_type)
                    .increment(1);
            }
            let simulator = self.settlement.as_mut().expect("checked above");
            simulator.apply(&plan);
            ::metrics::gauge!(crate::metrics::SETTLEMENT_OPEN).set(simulator.open() as f64);
            total += plan.transitions.len();
            self.wake_delivery(&stamped);
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
        let ordinal = self.store.report_count(report.execution_id)? + 1;
        let stored = ExternalReport {
            id: self.ids.report(report.execution_id, ordinal),
            execution_id: report.execution_id,
            status: report.status,
            reference: report.reference,
            reason: report.reason,
            details: report.details,
            occurred_at: report.occurred_at.unwrap_or(received_at),
            received_at,
        };
        let mut event = events::NewEvent {
            event_id: Uuid::nil(),
            sequence: 0,
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
        let next = NewEvent::stamp(
            std::slice::from_mut(&mut event),
            self.next_event_sequence,
            self.ids,
        );
        let outbox = Outbox {
            subscriptions: &self.subscriptions,
            simulation_id: self.simulation_id.as_deref(),
            ids: self.ids,
            due: wall_now(),
        };
        self.store.commit_report(&stored, &event, outbox)?;
        self.next_event_sequence = next;
        ::metrics::counter!(crate::metrics::EVENTS, "event_type" => event.event_type).increment(1);
        self.wake_delivery(std::slice::from_ref(&event));
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
            speed: self.virtual_clock.as_ref().map_or(1.0, VirtualClock::speed),
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
            ClockChange::Speed(speed) => clock.set_speed(speed)?,
        }
        self.store
            .save_virtual_clock(clock.now(), clock.is_running())?;
        // Whatever became due by the new time happens before the caller hears back.
        self.settlement_tick()?;
        Ok(self.clock_state())
    }

    /// Commits a planned book update with its events, and (if the simulator runs) the settlement intake of its
    /// trades in the same transaction, then applies both to memory. Intake here rather than on a later tick keeps
    /// the event log's order a function of the commands alone.
    fn commit_and_apply(
        &mut self,
        command: Command,
        update: BookUpdate,
        now: DateTime<Utc>,
    ) -> Result<SubmitOutcome, ExchangeError> {
        if self.group.is_some() && self.store.connection().is_autocommit() {
            // SQLite rolled the whole group back after a failed statement (e.g. disk full). Writing on would commit
            // this command on its own; the group's commit will fail and reload instead.
            return Err(ExchangeError::GroupCommit(
                "the group's transaction was rolled back".into(),
            ));
        }
        let instrument = self.books[&update.order.instrument_id].instrument();
        let participants = &self.participants;
        let lookup = |id: &ParticipantId| participants.get(id).cloned().flatten();
        let mut new_events = events::for_update(command, &update, instrument, &lookup, now);
        let mut next = NewEvent::stamp(&mut new_events, self.next_event_sequence, self.ids);
        let settlement_plan = match &self.settlement {
            Some(simulator) if !update.executions.is_empty() => {
                let trades = new_events
                    .iter()
                    .filter(|e| e.event_type == events::TRADE_EXECUTED)
                    .map(|e| TradeFacts::from_payload(e.sequence, &e.payload))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(ExchangeError::Settlement)?;
                let mut plan = simulator.plan(&trades, now);
                let mut settlement_events: Vec<NewEvent> =
                    plan.transitions.iter().map(|t| t.event.clone()).collect();
                next = NewEvent::stamp(&mut settlement_events, next, self.ids);
                for (transition, event) in plan.transitions.iter_mut().zip(&settlement_events) {
                    transition.event = event.clone();
                }
                new_events.extend(settlement_events);
                Some(plan)
            }
            _ => None,
        };
        let started = std::time::Instant::now();
        let outbox = Outbox {
            subscriptions: &self.subscriptions,
            simulation_id: self.simulation_id.as_deref(),
            ids: self.ids,
            due: wall_now(),
        };
        let settlement_write = settlement_plan.as_ref().map(|plan| SettlementWrite {
            instructions: plan.transitions.iter().map(|t| &t.instruction).collect(),
            consumer: SETTLEMENT_CONSUMER,
            cursor: plan.cursor,
        });
        self.store
            .commit(&update, &new_events, settlement_write, outbox, now)?;
        self.next_event_sequence = next;
        let book = self
            .books
            .get_mut(&update.order.instrument_id)
            .expect("planned on this book");
        book.apply(&update)
            .expect("nothing else touched the book between plan and apply");
        self.last_sequence = self.last_sequence.max(update.order.sequence);
        if let (Some(plan), Some(simulator)) = (&settlement_plan, self.settlement.as_mut()) {
            simulator.apply(plan);
            ::metrics::gauge!(crate::metrics::SETTLEMENT_OPEN).set(simulator.open() as f64);
        }
        let committed = Committed {
            order_status: (command == Command::Submit).then(|| update.order.status.as_str()),
            executions: update.executions.len() as u64,
            quantity: update.executions.iter().map(|e| e.quantity).sum(),
            event_types: new_events.iter().map(|e| e.event_type).collect(),
            delivers: self.delivers(&new_events),
        };
        match &mut self.group {
            Some(group) => group.push(committed),
            None => {
                ::metrics::histogram!(crate::metrics::COMMIT_DURATION).record(started.elapsed());
                committed.record();
                self.signal_deliveries(committed.delivers);
            }
        }
        Ok(SubmitOutcome::Processed(update))
    }

    /// Wakes the delivery worker if any of `events` has a subscriber.
    fn wake_delivery(&self, events: &[NewEvent]) {
        self.signal_deliveries(self.delivers(events));
    }

    /// Whether any of `events` has a subscriber.
    fn delivers(&self, events: &[NewEvent]) -> bool {
        events
            .iter()
            .any(|e| self.subscriptions.iter().any(|s| s.wants(e.event_type)))
    }

    /// Wakes the delivery worker when there is something new to deliver.
    fn signal_deliveries(&self, any: bool) {
        if any && let Some(signal) = &self.delivery_signal {
            signal.notify_one();
        }
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

/// Every instrument's book, with its active orders restored in acceptance sequence.
fn load_books(store: &Store) -> Result<HashMap<InstrumentId, OrderBook>, ExchangeError> {
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
    Ok(books)
}

/// Checks a subscription request (http(s) URL, known event types, secret length) and normalizes its event types
/// (sorted, without duplicates).
fn validate_subscription(mut request: NewSubscription) -> Result<NewSubscription, ExchangeError> {
    let parsed = reqwest::Url::parse(&request.url)
        .map_err(|e| ExchangeError::InvalidWebhook(format!("url: {e}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(ExchangeError::InvalidWebhook(
            "url must be http or https".into(),
        ));
    }
    if request.event_types.is_empty() {
        return Err(ExchangeError::InvalidWebhook(
            "eventTypes must not be empty".into(),
        ));
    }
    if let Some(unknown) = request
        .event_types
        .iter()
        .find(|t| !events::EVENT_TYPES.contains(&t.as_str()))
    {
        return Err(ExchangeError::InvalidWebhook(format!(
            "unknown event type {unknown}"
        )));
    }
    if let Some(secret) = &request.secret
        && !(16..=256).contains(&secret.chars().count())
    {
        return Err(ExchangeError::InvalidWebhook(
            "secret must be 16 to 256 characters".into(),
        ));
    }
    request.event_types.sort();
    request.event_types.dedup();
    Ok(request)
}

fn same_terms(order: &Order, command: &SubmitOrder) -> bool {
    order.instrument_id == command.instrument_id
        && order.side == command.side
        && order.order_type == command.order_type
        && order.limit_price_ticks == command.limit_price_ticks
        && order.quantity == command.quantity
}
