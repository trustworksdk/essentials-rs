use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SubsecRound, Utc};
use uuid::Uuid;

use super::{BookError, BookUpdate, Clock, IdScheme, MatchContext, OrderBook};
use crate::clearing::{ClearingPolicy, StoredStatement};
use crate::clock::{ClockError, SystemClock, VirtualClock};
use crate::domain::{
    IdentifierPolicy, Instrument, InstrumentChange, InstrumentError, InstrumentId, Order,
    OrderStatus, OrderType, Participant, ParticipantId, Side, TimeInForce,
};
use crate::events::NewEvent;
use crate::events::{self, Command};
use crate::ids::ExchangeIds;
use crate::liquidity::{LiquiditySettings, StalePolicy};
use crate::persistence::{Outbox, SettlementWrite, Store};
use crate::pricing::{PriceSourceError, PriceSources, Reference};
use crate::settlement::{
    ExternalReport, NewReport, SettlementPolicy, SettlementSimulator, TradeFacts,
};
use crate::webhooks::{
    DeclaredSubscription, DeliveryJob, DeliveryResult, FaultPolicy, NewSubscription, Subscription,
    WebhookChange, WebhookError, WebhookHub,
};
pub use simulator_kit::clock::{ClockChange, ClockState};

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
    /// `None` takes the default: `IOC` for a market order, `DAY` on a market with a session, else `GTC` (SR-17).
    pub time_in_force: Option<TimeInForce>,
}

/// The order a cancel names: the exchange's order ID, or the participant's own client order ID (SR-30).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CancelTarget {
    Order(Uuid),
    Client {
        participant_id: ParticipantId,
        client_order_id: String,
    },
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
    #[error("participant {participant_id} has no order with client order ID {client_order_id}")]
    UnknownClientOrderId {
        participant_id: ParticipantId,
        client_order_id: String,
    },
    #[error("order {id} is {status} and cannot be cancelled")]
    OrderNotActive { id: Uuid, status: OrderStatus },
    #[error("instrument {0} already exists")]
    DuplicateInstrument(InstrumentId),
    #[error("symbol {symbol} is already listed on {mic}")]
    DuplicateListing { mic: String, symbol: String },
    #[error(
        "instruments {id}: {field} differs from the listed instrument; only enabled, reference_symbol, \
         price_source and liquidity can change"
    )]
    DeclaredInstrumentConflict {
        id: InstrumentId,
        field: &'static str,
    },
    #[error("instruments {id}: {error}")]
    DeclaredInstrument {
        id: InstrumentId,
        error: Box<ExchangeError>,
    },
    #[error(transparent)]
    PriceSource(#[from] PriceSourceError),
    #[error(transparent)]
    Liquidity(#[from] crate::liquidity::LiquidityError),
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
    #[error("a market order is always IOC; timeInForce {0} is not allowed")]
    InvalidTimeInForce(TimeInForce),
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
    #[error("clearing: {0}")]
    Clearing(String),
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

/// The hub's refusals keep the exchange's own variants (and with them the API's responses); its storage failures are
/// the exchange's storage failures.
impl From<WebhookError> for ExchangeError {
    fn from(error: WebhookError) -> Self {
        match error {
            WebhookError::Invalid(message) => Self::InvalidWebhook(message),
            WebhookError::Unknown(id) => Self::UnknownSubscription(id),
            WebhookError::ManagedByConfiguration { id, name } => {
                Self::ManagedByConfiguration { id, name }
            }
            WebhookError::Storage(error) => Self::Storage(error),
        }
    }
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
    /// Issues end-of-day clearing statements with this policy (`clearing.enabled`).
    pub clearing: Option<ClearingPolicy>,
    /// Webhook subscriptions declared in configuration, reconciled at recovery and after a reset. Names must be
    /// unique (see [`crate::webhooks::validate_declared`]).
    pub webhooks: Vec<DeclaredSubscription>,
    /// How long a declared subscription's previous secret stays valid after the configuration changes it.
    pub webhook_secret_grace: chrono::Duration,
    /// Instruments declared in configuration, listed or updated at recovery and after a reset, in this order. IDs
    /// must be unique (see [`crate::config::InstrumentConfig`]).
    pub instruments: Vec<Instrument>,
    /// What an instrument's own price source leaves out, and the base of derived synthetic seeds.
    pub price_sources: PriceSources,
    /// Closes every business day at this calendar's cutoff with `BusinessDayClosed`, whether or not clearing runs.
    /// `None` closes no days.
    pub business_days: Option<simulator_kit::calendar::CutoffCalendar>,
    /// Runs the built-in liquidity provider (plan item 6.7); `None` quotes nothing.
    pub liquidity: Option<LiquidityPolicy>,
    /// Where market-data messages go (plan item 6.8); `None` publishes nothing, the snapshot still works.
    pub market_data: Option<crate::market_data::Sender>,
    /// How executions are dated (SR-29). The default dates by UTC date and settles T+1.
    pub trade_dating: crate::settlement::TradeDating,
    /// Trading sessions by market (plan item 6.10). The default has none: every market is always open.
    pub sessions: crate::sessions::TradingSessions,
}

/// How the liquidity provider runs (`liquidity:` in the configuration).
#[derive(Clone, Debug, PartialEq)]
pub struct LiquidityPolicy {
    /// The participant it trades as, registered the first time it quotes.
    pub participant: ParticipantId,
    /// Publish its order lifecycle (accepted, filled, cancelled) in the event log and over webhooks. Its trades are
    /// published either way: they are real clients' counterparties.
    pub emit_order_events: bool,
    /// What an instrument's own settings leave out.
    pub default: crate::liquidity::LiquiditySpec,
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
            clearing: None,
            webhooks: Vec::new(),
            webhook_secret_grace: chrono::Duration::hours(1),
            instruments: Vec::new(),
            price_sources: PriceSources::new(Default::default(), 0)
                .expect("the empty default is valid"),
            business_days: None,
            liquidity: None,
            market_data: None,
            trade_dating: Default::default(),
            sessions: Default::default(),
        }
    }
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
    clearing: Option<Clearing>,
    /// Webhook subscriptions, declared ones included, and the rules for changing them.
    webhooks: WebhookHub,
    simulation_id: Option<String>,
    delivery_signal: Option<Arc<tokio::sync::Notify>>,
    /// The virtual clock's first reading and whether it started running: where a reset puts it.
    clock_start: Option<(DateTime<Utc>, bool, f64)>,
    /// Counts resets. Delivery claims carry it, so results for deliveries claimed before a reset are dropped.
    epoch: u64,
    /// The open commit group: what its commands wrote, reported once the group is durable.
    group: Option<Vec<Committed>>,
    /// Instruments declared in configuration.
    declared_instruments: Vec<Instrument>,
    /// Every listed instrument's running price source.
    prices: PriceSources,
    /// The business-day calendar and the last date closed.
    days: Option<BusinessDays>,
    /// The liquidity provider and its state per instrument.
    provider: Option<Provider>,
    /// Market data per instrument, and where to publish it.
    market_data: crate::market_data::States,
    market_data_sender: Option<crate::market_data::Sender>,
    /// Instruments to publish once the open commit group is durable.
    market_data_due: std::collections::BTreeSet<InstrumentId>,
    trade_dating: crate::settlement::TradeDating,
    sessions: crate::sessions::TradingSessions,
    /// Resting `DAY` orders by when they expire: (expiry, sequence, instrument, order, the business day that ends
    /// then). An entry whose order is no longer active when it comes due is dropped.
    expiries:
        std::collections::BTreeSet<(DateTime<Utc>, u64, InstrumentId, Uuid, chrono::NaiveDate)>,
}

/// The liquidity provider in one instrument, as `GET /api/liquidity` shows it (SR-11).
#[derive(Clone, Debug)]
pub struct LiquidityStatus {
    pub instrument: Instrument,
    /// The participant it quotes as; `None` when the service runs no provider.
    pub provider: Option<ParticipantId>,
    /// Its settings, resolved (own, then `liquidity.default`, then built-in).
    pub settings: Option<LiquiditySettings>,
    /// What it did when it last quoted (see `Exchange::liquidity_state`).
    pub state: &'static str,
    /// Its net position in units (positive long).
    pub inventory: i64,
    /// The reference price its current ladder was quoted around.
    pub quoted_at: Option<i64>,
    /// Its resting orders, bids best first, then asks best first.
    pub ladder: Vec<Order>,
    pub reference: Option<Reference>,
    pub session_open: bool,
}

/// A change to one instrument's liquidity (`PATCH /api/liquidity/{id}`): settings by wire name (`null` clears one),
/// and optionally a new price source.
#[derive(Clone, Debug, Default)]
pub struct LiquidityPatch {
    pub fields: serde_json::Map<String, serde_json::Value>,
    pub price_source: Option<Option<crate::pricing::PriceSourceSpec>>,
}

/// The liquidity provider: its policy, and per instrument what it last quoted around and holds.
struct Provider {
    policy: LiquidityPolicy,
    quoting: HashMap<InstrumentId, Quoting>,
}

/// The provider's state in one instrument. Not stored: after a restart or reset it is rebuilt from the trades, and
/// the ladder is quoted afresh (decision D18).
#[derive(Clone, Debug, Default)]
struct Quoting {
    /// Net position in units: positive long.
    inventory: i64,
    /// The reference price the current ladder was quoted around; `None` before the first quote or once pulled.
    quoted_at: Option<i64>,
    /// When it last traded, until the ladder is refilled.
    filled_at: Option<DateTime<Utc>>,
    /// Whether the reference price was stale, and the market closed, when the current ladder was quoted (or pulled).
    quoted_stale: bool,
    quoted_closed: bool,
    /// The settings the current ladder was quoted with: a change re-quotes it at once.
    quoted_settings: Option<LiquiditySettings>,
    /// What it is doing, as the market data shows it (see [`Exchange::liquidity_state`]); `None` reads `off`.
    state: Option<&'static str>,
}

/// Business days and the last one closed with `BusinessDayClosed`.
struct BusinessDays {
    calendar: simulator_kit::calendar::CutoffCalendar,
    cursor: chrono::NaiveDate,
}

impl BusinessDays {
    /// When the next business day closes.
    fn due(&self) -> DateTime<Utc> {
        self.calendar
            .cutoff_at(self.calendar.days.next_business_day(self.cursor))
    }

    /// The cursor for days starting at `now`: the business day before the one now belongs to, so the first day closed
    /// is the current one and no past day is.
    fn first_cursor(
        calendar: &simulator_kit::calendar::CutoffCalendar,
        now: DateTime<Utc>,
    ) -> chrono::NaiveDate {
        calendar
            .days
            .previous_business_day(calendar.business_date(now))
    }
}

/// The clearinghouse: its policy, the last business date whose cutoff was handled, and late statements waiting to be
/// issued, in issue order.
struct Clearing {
    policy: ClearingPolicy,
    cursor: chrono::NaiveDate,
    /// The last trade sequence a window took. Exchange stamps never go backwards, so every trade of a later window
    /// comes after it.
    after: u64,
    pending: Vec<StoredStatement>,
}

impl Clearing {
    /// When the next statement is due: the next cutoff, or a late statement before it.
    fn due(&self) -> DateTime<Utc> {
        let cutoff = self
            .policy
            .calendar
            .cutoff_at(self.policy.calendar.days.next_business_day(self.cursor));
        self.pending
            .first()
            .map_or(cutoff, |p| p.draft.statement.issued_at.min(cutoff))
    }

    /// The cursor for a clearinghouse starting at `now`: the business day before the one now belongs to, so the next
    /// cutoff is the first one after now and no past day is issued.
    fn first_cursor(policy: &ClearingPolicy, now: DateTime<Utc>) -> chrono::NaiveDate {
        policy
            .calendar
            .days
            .previous_business_day(policy.calendar.business_date(now))
    }
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
        let webhooks = WebhookHub::load(
            store.connection(),
            options.webhooks,
            options.webhook_secret_grace,
            events::EVENT_TYPES,
            Some(events::parties),
        )?;
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
            clearing: None,
            webhooks,
            simulation_id: options.simulation_id,
            delivery_signal: options.delivery_signal,
            clock_start,
            epoch: 0,
            group: None,
            declared_instruments: options.instruments,
            prices: options.price_sources,
            days: None,
            provider: options.liquidity.map(|policy| Provider {
                policy,
                quoting: HashMap::new(),
            }),
            market_data: HashMap::new(),
            market_data_sender: options.market_data,
            market_data_due: Default::default(),
            trade_dating: options.trade_dating,
            sessions: options.sessions,
            expiries: Default::default(),
        };
        exchange.start_price_sources()?;
        exchange.reconcile_webhooks()?;
        exchange.schedule_expiries()?;
        exchange.start_liquidity()?;
        exchange.reconcile_instruments()?;
        if let Some(policy) = options.clearing {
            policy.breaks.validate().map_err(ExchangeError::Clearing)?;
            let (cursor, after) = match exchange.store.clearing_cursor()? {
                Some(cursor) => cursor,
                None => {
                    let cursor = Clearing::first_cursor(&policy, exchange.now());
                    exchange.store.save_clearing_cursor(cursor, 0)?;
                    (cursor, 0)
                }
            };
            let pending = exchange.store.clearing_statements(None, None, true)?;
            exchange.clearing = Some(Clearing {
                policy,
                cursor,
                after,
                pending,
            });
            exchange.sort_pending();
        }
        if let Some(calendar) = options.business_days {
            let cursor = match exchange.store.business_day_cursor()? {
                Some(cursor) => cursor,
                None => {
                    let cursor = BusinessDays::first_cursor(&calendar, exchange.now());
                    exchange.store.save_business_day_cursor(cursor)?;
                    cursor
                }
            };
            exchange.days = Some(BusinessDays { calendar, cursor });
        }
        exchange.date_old_executions()?;
        exchange.load_market_data()?;
        // Trades committed while the simulator was off (or before it existed) are taken in now, before any command,
        // so intake order does not depend on when a background tick happens to run. Cutoffs passed while the service
        // was down are issued too.
        exchange.tick()?;
        Ok(exchange)
    }

    /// Lists an instrument and publishes `InstrumentListed`.
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
        let mut instrument = instrument;
        let id = instrument.id.clone();
        self.commit_instrument(&mut instrument, None)?;
        self.maintain_liquidity(Some(&id))?;
        self.publish_market_data(Some(&id));
        Ok(())
    }

    /// Switches trading in an instrument on or off, durably, and returns the updated instrument.
    pub fn set_instrument_enabled(
        &mut self,
        id: &InstrumentId,
        enabled: bool,
    ) -> Result<Instrument, ExchangeError> {
        self.update_instrument(
            id,
            &InstrumentChange {
                enabled: Some(enabled),
                ..Default::default()
            },
        )
    }

    /// Changes a listed instrument and returns it. A change that alters a value publishes `InstrumentUpdated` with the
    /// fields it changed; one that alters nothing publishes nothing.
    pub fn update_instrument(
        &mut self,
        id: &InstrumentId,
        change: &InstrumentChange,
    ) -> Result<Instrument, ExchangeError> {
        let mut instrument = self
            .books
            .get(id)
            .ok_or_else(|| ExchangeError::UnknownInstrument(id.clone()))?
            .instrument()
            .clone();
        let changes = change.apply(&mut instrument);
        if changes.is_empty() {
            return Ok(instrument);
        }
        instrument.validate(self.identifier_policy)?;
        self.commit_instrument(&mut instrument, Some(&changes))?;
        // A new price source, liquidity setting or trading status changes what the provider quotes.
        self.maintain_liquidity(Some(id))?;
        self.publish_market_data(Some(id));
        Ok(instrument)
    }

    /// Writes a new (`changes: None`) or changed instrument with its event, then applies it to memory. A new price
    /// source is prepared first (see [`PriceSources::prepare`]), so the instrument stored and published is the one
    /// whose source then starts, with its grid at now.
    fn commit_instrument(
        &mut self,
        instrument: &mut Instrument,
        changes: Option<&[&'static str]>,
    ) -> Result<(), ExchangeError> {
        if let Some(liquidity) = &instrument.liquidity {
            liquidity.validate()?;
        }
        let now = self.now();
        // A live source streams its reference symbol, so a new one restarts it too.
        let restarts = changes.is_none_or(|c| {
            c.contains(&"priceSource")
                || (c.contains(&"referenceSymbol") && self.prices.is_live(instrument))
        });
        if restarts {
            let current = self.prices.reference(&instrument.id, now);
            self.prices.prepare(instrument, current)?;
        }
        let mut event = events::instrument_event(instrument, changes, now);
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
            schema_version: events::SCHEMA_VERSION,
            parties: self.webhooks.parties(),
        };
        self.store.commit_instrument(
            instrument,
            changes.is_none(),
            restarts.then_some(now),
            &event,
            outbox,
        )?;
        self.next_event_sequence = next;
        ::metrics::counter!(crate::metrics::EVENTS, "event_type" => event.event_type).increment(1);
        if restarts {
            self.prices.start(instrument, now)?;
        }
        match self.books.get_mut(&instrument.id) {
            Some(book) => book.set_instrument(instrument.clone()),
            None => {
                self.books
                    .insert(instrument.id.clone(), OrderBook::new(instrument.clone()));
            }
        }
        self.wake_delivery(std::slice::from_ref(&event));
        Ok(())
    }

    /// Brings the instruments declared in configuration into the registry. Runs at recovery and after a reset, after
    /// the declared webhook subscriptions, so they receive the events. A missing instrument is listed; a listed one
    /// whose `enabled`, `reference_symbol` or `price_source` differs is updated, so configuration wins over API
    /// changes, as for subscriptions. Any other difference fails: those fields never change once listed. Instruments
    /// no longer declared stay listed, since orders and trades refer to them.
    fn reconcile_instruments(&mut self) -> Result<(), ExchangeError> {
        for declared in self.declared_instruments.clone() {
            let id = declared.id.clone();
            let wrap = |error| match error {
                error @ ExchangeError::Storage(_) => error,
                error => ExchangeError::DeclaredInstrument {
                    id: id.clone(),
                    error: Box::new(error),
                },
            };
            let Some(book) = self.books.get(&declared.id) else {
                tracing::info!(id = %declared.id.0, "instrument listed from configuration");
                self.register_instrument(declared).map_err(wrap)?;
                continue;
            };
            let listed = book.instrument();
            let fixed = [
                ("symbol", listed.symbol == declared.symbol),
                ("mic", listed.mic == declared.mic),
                ("isin", listed.isin == declared.isin),
                ("cusip", listed.cusip == declared.cusip),
                ("currency", listed.currency == declared.currency),
                (
                    "currency_scale",
                    listed.currency_scale == declared.currency_scale,
                ),
                (
                    "tick_size_minor",
                    listed.tick_size_minor == declared.tick_size_minor,
                ),
                ("lot_size", listed.lot_size == declared.lot_size),
            ];
            if let Some((field, _)) = fixed.iter().find(|(_, same)| !same) {
                return Err(ExchangeError::DeclaredInstrumentConflict {
                    id: declared.id,
                    field,
                });
            }
            let change = InstrumentChange {
                enabled: Some(declared.enabled),
                reference_symbol: Some(declared.reference_symbol.clone()),
                price_source: Some(declared.price_source.clone()),
                liquidity: Some(declared.liquidity.clone()),
            };
            self.update_instrument(&declared.id, &change)
                .map_err(wrap)?;
        }
        Ok(())
    }

    /// Starts every listed instrument's price source at recovery, each with its grid where it was. An instrument with
    /// no stored start (listed before price sources existed) starts now, and that is stored.
    fn start_price_sources(&mut self) -> Result<(), ExchangeError> {
        let mut since = self.store.price_source_since()?;
        let now = self.now();
        let mut instruments: Vec<Instrument> = self
            .books
            .values()
            .map(|b| b.instrument().clone())
            .collect();
        instruments.sort_by(|a, b| a.id.cmp(&b.id));
        for instrument in instruments {
            let at = match since.remove(&instrument.id) {
                Some(at) => at,
                None => {
                    self.store.save_price_source_since(&instrument.id, now)?;
                    now
                }
            };
            self.prices.start(&instrument, at).map_err(|error| {
                ExchangeError::DeclaredInstrument {
                    id: instrument.id.clone(),
                    error: Box::new(error.into()),
                }
            })?;
        }
        Ok(())
    }

    /// Sets the liquidity provider up, at recovery and after a reset: its inventory per instrument from its trades,
    /// and no ladder yet, so each is quoted afresh (decision D18). Its resting orders, if any survived a restart, are
    /// replaced at the first quote.
    fn start_liquidity(&mut self) -> Result<(), ExchangeError> {
        let Some(provider) = &mut self.provider else {
            return Ok(());
        };
        provider.quoting = self
            .store
            .net_positions(&provider.policy.participant)?
            .into_iter()
            .map(|(id, inventory)| {
                (
                    id,
                    Quoting {
                        inventory,
                        ..Default::default()
                    },
                )
            })
            .collect();
        Ok(())
    }

    /// Lets the liquidity provider quote what is due, in one instrument or all (in ID order): the first ladder, a
    /// new one when the reference price moved `requote_threshold_ticks` from the last quote, a refill once
    /// `refill_delay_ms` passed since a fill, or no ladder when the instrument is off, disabled or without a reference
    /// price. Runs after every order and cancellation, after instrument changes, and on every tick, so on a frozen
    /// clock what it does depends only on the commands.
    fn maintain_liquidity(&mut self, only: Option<&InstrumentId>) -> Result<(), ExchangeError> {
        if self.provider.is_none() {
            return Ok(());
        }
        let mut ids: Vec<InstrumentId> = match only {
            Some(id) => vec![id.clone()],
            None => self.books.keys().cloned().collect(),
        };
        ids.sort();
        let now = self.now();
        for id in ids {
            self.requote(&id, now)?;
        }
        Ok(())
    }

    /// Quotes one instrument if due (see [`Exchange::maintain_liquidity`]). The old ladder is cancelled and the new
    /// one placed as one commit group, inside the caller's group if one is open, so no order ever sees half a ladder.
    fn requote(&mut self, id: &InstrumentId, now: DateTime<Utc>) -> Result<(), ExchangeError> {
        let Some(book) = self.books.get(id) else {
            return Ok(());
        };
        let instrument = book.instrument().clone();
        let mut settings = self
            .liquidity_settings(&instrument)
            .expect("checked by the caller");
        let provider = self.provider.as_ref().expect("checked by the caller");
        let lp = provider.policy.participant.clone();
        let state = provider.quoting.get(id).cloned().unwrap_or_default();
        let mut resting: Vec<Order> = book
            .orders()
            .filter(|o| o.participant_id == lp)
            .cloned()
            .collect();
        resting.sort_by_key(|o| o.sequence);
        let reference = match (settings.mode, instrument.enabled) {
            (crate::liquidity::LiquidityMode::On, true) => self.prices.reference(id, now),
            _ => None,
        };
        let closed = !self.sessions.is_open(instrument.mic.as_deref(), now);
        let shown = Self::liquidity_state(&settings, &instrument, reference.as_ref(), closed);
        let configured = settings.clone();
        // SR-9: in a closed market, or under a stale price, the ladder is quoted as it is (keep), with a wider spread
        // (widen), or not at all (pull). A closed market's policy takes precedence.
        let stale = reference.is_some_and(|r| r.stale);
        let degraded = if closed {
            Some((settings.closed_policy, settings.closed_widen_factor))
        } else if stale {
            Some((settings.stale_policy, settings.stale_widen_factor))
        } else {
            None
        };
        if let Some((StalePolicy::Widen, factor)) = degraded {
            settings.spread_bps *= factor;
        }
        let reference = reference
            .filter(|_| !matches!(degraded, Some((StalePolicy::Pull, _))))
            .map(|r| r.ticks);
        let lot = instrument.lot_size;
        let wanted =
            reference.map(|r| settings.ladder(r, state.inventory / lot.max(1) as i64, lot));
        let due = match (reference, state.quoted_at) {
            (None, _) => !resting.is_empty(),
            (Some(_), None) => true,
            (Some(r), Some(quoted)) => {
                (r - quoted).abs() >= i64::from(settings.requote_threshold_ticks)
                    || stale != state.quoted_stale
                    || closed != state.quoted_closed
                    || state.quoted_settings.as_ref() != Some(&configured)
                    || state.filled_at.is_some_and(|at| {
                        now >= at + chrono::Duration::milliseconds(settings.refill_delay_ms as i64)
                    })
            }
        };
        if !due {
            self.quoting(id).state = Some(shown);
            return Ok(());
        }
        let wanted = wanted.unwrap_or_default();
        let unchanged = resting.len() == wanted.len()
            && resting.iter().zip(&wanted).all(|(order, quote)| {
                order.side == quote.side
                    && order.limit_price_ticks == Some(quote.price_ticks)
                    && order.remaining_quantity == quote.quantity
            });
        if !unchanged {
            if !wanted.is_empty() && !self.participants.contains_key(&lp) {
                self.register_participant(Participant {
                    id: lp.clone(),
                    name: "Liquidity provider".into(),
                    settlement: None,
                })?;
            }
            let own_group = self.group.is_none();
            if own_group {
                self.begin_group()?;
            }
            let replaced = self.replace_ladder(&instrument.id, &lp, &resting, &wanted);
            if own_group {
                // What was written is applied in memory too, so commit it even when a step failed.
                self.commit_group()?;
            }
            replaced?;
            ::metrics::counter!(crate::metrics::LIQUIDITY_REQUOTES, "instrument" => id.0.clone())
                .increment(1);
            ::metrics::counter!(crate::metrics::LIQUIDITY_QUOTES).increment(wanted.len() as u64);
        }
        let quoting = self.quoting(id);
        quoting.quoted_at = reference;
        quoting.filled_at = None;
        quoting.quoted_stale = stale;
        quoting.quoted_closed = closed;
        quoting.quoted_settings = Some(configured);
        quoting.state = Some(shown);
        Ok(())
    }

    fn quoting(&mut self, id: &InstrumentId) -> &mut Quoting {
        self.provider
            .as_mut()
            .expect("checked by the caller")
            .quoting
            .entry(id.clone())
            .or_default()
    }

    /// The liquidity provider's status in every listed instrument (or only `id`'s), in ID order (SR-11).
    pub fn liquidity(&mut self, id: Option<&InstrumentId>) -> Vec<LiquidityStatus> {
        let now = self.now();
        let mut ids: Vec<InstrumentId> = self
            .books
            .keys()
            .filter(|i| id.is_none_or(|id| id == *i))
            .cloned()
            .collect();
        ids.sort();
        ids.into_iter()
            .map(|id| {
                let book = &self.books[&id];
                let instrument = book.instrument().clone();
                let provider = self.provider.as_ref().map(|p| p.policy.participant.clone());
                let quoting = self
                    .provider
                    .as_ref()
                    .and_then(|p| p.quoting.get(&id))
                    .cloned()
                    .unwrap_or_default();
                let mut ladder: Vec<Order> = book
                    .orders()
                    .filter(|o| provider.as_ref() == Some(&o.participant_id))
                    .cloned()
                    .collect();
                ladder.sort_by_key(|o| match o.side {
                    Side::Buy => (0, -o.limit_price_ticks.unwrap_or(0)),
                    Side::Sell => (1, o.limit_price_ticks.unwrap_or(0)),
                });
                LiquidityStatus {
                    settings: self.liquidity_settings(&instrument),
                    provider,
                    state: quoting.state.unwrap_or("off"),
                    inventory: quoting.inventory,
                    quoted_at: quoting.quoted_at,
                    ladder,
                    reference: self.prices.reference(&id, now),
                    session_open: self.sessions.is_open(instrument.mic.as_deref(), now),
                    instrument,
                }
            })
            .collect()
    }

    /// Changes one instrument's liquidity settings field by field, and its price source if given, durably (an
    /// `InstrumentUpdated`), then returns the provider's status there. The merge runs here, on the exchange thread, so
    /// concurrent patches never lose each other's fields.
    pub fn patch_liquidity(
        &mut self,
        id: &InstrumentId,
        patch: &LiquidityPatch,
    ) -> Result<LiquidityStatus, ExchangeError> {
        let book = self
            .books
            .get(id)
            .ok_or_else(|| ExchangeError::UnknownInstrument(id.clone()))?;
        let liquidity = if patch.fields.is_empty() {
            None
        } else {
            let merged = book
                .instrument()
                .liquidity
                .clone()
                .unwrap_or_default()
                .patched(&patch.fields)?;
            Some((merged != Default::default()).then_some(merged))
        };
        let change = InstrumentChange {
            enabled: None,
            reference_symbol: None,
            price_source: patch.price_source.clone(),
            liquidity,
        };
        self.update_instrument(id, &change)?;
        Ok(self
            .liquidity(Some(id))
            .pop()
            .expect("the instrument is listed"))
    }

    /// The provider's settings for `instrument`, resolved; `None` when there is no provider.
    fn liquidity_settings(&self, instrument: &Instrument) -> Option<LiquiditySettings> {
        let provider = self.provider.as_ref()?;
        Some(
            instrument
                .liquidity
                .clone()
                .unwrap_or_default()
                .or(&provider.policy.default)
                .resolve(),
        )
    }

    /// What the provider does in an instrument, for the market data: `off` (mode off or trading disabled),
    /// `no-reference`, `quoting`, or in a closed market or under a stale price that policy: `kept`, `widened` or
    /// `pulled`. Recorded when it quotes, so the snapshot never shows a state whose ladder is not in the book yet.
    fn liquidity_state(
        settings: &LiquiditySettings,
        instrument: &Instrument,
        reference: Option<&Reference>,
        closed: bool,
    ) -> &'static str {
        if settings.mode == crate::liquidity::LiquidityMode::Off || !instrument.enabled {
            return "off";
        }
        let policy = match reference {
            None => return "no-reference",
            Some(_) if closed => settings.closed_policy,
            Some(r) if r.stale => settings.stale_policy,
            Some(_) => return "quoting",
        };
        match policy {
            StalePolicy::Keep => "kept",
            StalePolicy::Widen => "widened",
            StalePolicy::Pull => "pulled",
        }
    }

    /// Cancels the provider's resting orders and places `wanted`, bids then asks, best first.
    fn replace_ladder(
        &mut self,
        instrument: &InstrumentId,
        lp: &ParticipantId,
        resting: &[Order],
        wanted: &[crate::liquidity::Quote],
    ) -> Result<(), ExchangeError> {
        for order in resting {
            self.cancel_order(order.id)?;
        }
        for quote in wanted {
            self.submit_order(SubmitOrder {
                client_order_id: None,
                instrument_id: instrument.clone(),
                participant_id: lp.clone(),
                side: quote.side,
                order_type: OrderType::Limit,
                limit_price_ticks: Some(quote.price_ticks),
                quantity: quote.quantity,
                time_in_force: None,
            })?;
        }
        Ok(())
    }

    /// Every listed instrument's market data now, in ID order (or only `id`'s). What changed since the last
    /// publication is published first, so the snapshot's `mdSequence` is that of the last message about it.
    pub fn market_data(&mut self, id: Option<&InstrumentId>) -> Vec<crate::market_data::Snapshot> {
        self.publish_market_data(id);
        let now = self.now();
        let today = self.business_date(now);
        let mut ids: Vec<InstrumentId> = self
            .books
            .keys()
            .filter(|i| id.is_none_or(|id| id == *i))
            .cloned()
            .collect();
        ids.sort();
        ids.into_iter()
            .map(|id| {
                let reference = self.prices.reference(&id, now);
                let (best_bid, best_ask) = self.top_of_book(&id);
                let state = self.market_data.get(&id).cloned().unwrap_or_default();
                let instrument = self.books[&id].instrument().clone();
                let instrument_mic = instrument.mic.clone();
                crate::market_data::Snapshot {
                    liquidity: self
                        .provider
                        .as_ref()
                        .and_then(|p| p.quoting.get(&id))
                        .and_then(|q| q.state)
                        .unwrap_or("off"),
                    instrument,
                    md_sequence: state.md_sequence,
                    reference,
                    best_bid,
                    best_ask,
                    last_trade: state.last_trade,
                    session_volume: state.volume_on(today),
                    session_open: self.sessions.is_open(instrument_mic.as_deref(), now),
                }
            })
            .collect()
    }

    /// Publishes what changed in one instrument's market data, or every instrument's, since the last publication:
    /// trade prints, then the quote, then the reference price. Inside a commit group, nothing is published until the
    /// group is durable (see [`Self::commit_group`]).
    fn publish_market_data(&mut self, only: Option<&InstrumentId>) {
        let ids: Vec<InstrumentId> = match only {
            Some(id) => vec![id.clone()],
            None => self.books.keys().cloned().collect(),
        };
        if self.group.is_some() {
            self.market_data_due.extend(ids);
            return;
        }
        let now = self.now();
        let mut ids = ids;
        ids.sort();
        for id in ids {
            let Some(book) = self.books.get(&id) else {
                continue;
            };
            let instrument = book.instrument().clone();
            let quote = self.top_of_book(&id);
            let reference = self.prices.reference(&id, now);
            let state = self.market_data.entry(id).or_default();
            let before = state.reference.flatten().map(|(ticks, ..)| ticks);
            let messages = state.publish(&instrument, quote, reference);
            record_reference(&instrument.id, before, reference, now);
            for message in messages {
                ::metrics::counter!(crate::metrics::MARKET_DATA_MESSAGES, "instrument" => message.instrument_id.0.clone(), "kind" => message.kind)
                    .increment(1);
                if let Some(sender) = &self.market_data_sender {
                    // No subscribers is not an error; a full subscriber loses its place, never blocks this.
                    let _ = sender.send(std::sync::Arc::new(message));
                }
            }
        }
    }

    /// The best bid and ask of an instrument's book, with the quantity resting there.
    fn top_of_book(
        &self,
        id: &InstrumentId,
    ) -> (
        Option<crate::market_data::Top>,
        Option<crate::market_data::Top>,
    ) {
        let top = |side| {
            self.books[id]
                .depth(side)
                .first()
                .map(|level| crate::market_data::Top {
                    price_ticks: level.price_ticks,
                    quantity: level.quantity,
                })
        };
        (top(Side::Buy), top(Side::Sell))
    }

    /// The business date an instant belongs to: on the business-day calendar when there is one, else its UTC date.
    fn business_date(&self, at: DateTime<Utc>) -> chrono::NaiveDate {
        self.days
            .as_ref()
            .map_or_else(|| at.date_naive(), |d| d.calendar.business_date(at))
    }

    /// Dates the executions stored before executions carried dates (migration 018), on the current calendar and
    /// cycles, so every execution has both dates from here on.
    fn date_old_executions(&mut self) -> Result<(), ExchangeError> {
        let undated = self.store.executions_without_dates()?;
        if undated.is_empty() {
            return Ok(());
        }
        let dated: Vec<_> = undated
            .into_iter()
            .map(|e| {
                (
                    e.id,
                    self.trade_dating.dates(e.executed_at, e.mic.as_deref()),
                )
            })
            .collect();
        tracing::info!(
            executions = dated.len(),
            "dating executions stored before they carried dates"
        );
        self.store.set_execution_dates(&dated)?;
        Ok(())
    }

    /// Rebuilds the market data a restart lost: each instrument's last trade and its volume on the current business
    /// date. The `mdSequence` starts again from 0.
    fn load_market_data(&mut self) -> Result<(), ExchangeError> {
        let now = self.now();
        let today = self.business_date(now);
        let since = match &self.days {
            Some(days) => days.calendar.window(today).0,
            None => today.and_hms_opt(0, 0, 0).expect("midnight").and_utc(),
        };
        for summary in self.store.trade_summary(since)? {
            let state = self.market_data.entry(summary.instrument_id).or_default();
            state.last_trade = Some(crate::market_data::Print {
                price_ticks: summary.last_price_ticks,
                quantity: summary.last_quantity,
                executed_at: summary.last_executed_at,
            });
            state.volume_date = Some(today);
            state.volume = summary.volume_since;
        }
        Ok(())
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
    /// Matches an order, commits it, and then lets the liquidity provider react (refill, re-quote) to what it did.
    pub fn submit(&mut self, command: SubmitOrder) -> Result<SubmitOutcome, ExchangeError> {
        let instrument_id = command.instrument_id.clone();
        let outcome = self.submit_order(command)?;
        self.maintain_liquidity(Some(&instrument_id))?;
        self.publish_market_data(Some(&instrument_id));
        Ok(outcome)
    }

    fn submit_order(&mut self, command: SubmitOrder) -> Result<SubmitOutcome, ExchangeError> {
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
        let mic = book.instrument().mic.clone();
        let time_in_force = match (command.order_type, command.time_in_force) {
            (OrderType::Market, None | Some(TimeInForce::Ioc)) => TimeInForce::Ioc,
            (OrderType::Market, Some(other)) => {
                return Err(ExchangeError::InvalidTimeInForce(other));
            }
            (OrderType::Limit, Some(given)) => given,
            (OrderType::Limit, None) if self.sessions.session(mic.as_deref()).is_some() => {
                TimeInForce::Day
            }
            (OrderType::Limit, None) => TimeInForce::Gtc,
        };
        // The liquidity provider is not a client: it follows its closed-market policy instead (SR-9).
        let provider = self
            .provider
            .as_ref()
            .is_some_and(|p| p.policy.participant == command.participant_id);
        let market_closed = !provider && !self.sessions.is_open(mic.as_deref(), now);
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
            time_in_force,
        };
        let started = std::time::Instant::now();
        let mut ctx = MatchContext {
            now,
            ids: self.ids,
            dates: self.trade_dating.dates(now, mic.as_deref()),
            market_closed,
        };
        let update = book.plan_submit(order, &mut ctx)?;
        ::metrics::histogram!(crate::metrics::MATCH_DURATION).record(started.elapsed());
        let outcome = self.commit_and_apply(Command::Submit, update, now)?;
        if let SubmitOutcome::Processed(update) = &outcome
            && update.order.time_in_force == TimeInForce::Day
            && update.order.status.is_active()
        {
            self.schedule_expiry(&update.order, mic.as_deref(), now);
        }
        Ok(outcome)
    }

    /// Schedules a resting `DAY` order's expiry (SR-17).
    fn schedule_expiry(&mut self, order: &Order, mic: Option<&str>, accepted_at: DateTime<Utc>) {
        if let Some((due, business_date)) = self.sessions.day_expiry(mic, accepted_at) {
            self.expiries.insert((
                due,
                order.sequence,
                order.instrument_id.clone(),
                order.id,
                business_date,
            ));
        }
    }

    /// Rebuilds the expiry schedule from the stored active `DAY` orders, at recovery and after a failed commit.
    fn schedule_expiries(&mut self) -> Result<(), ExchangeError> {
        self.expiries.clear();
        for record in self.store.active_day_orders()? {
            let mic = self
                .books
                .get(&record.order.instrument_id)
                .and_then(|b| b.instrument().mic.clone());
            self.schedule_expiry(&record.order, mic.as_deref(), record.created_at);
        }
        Ok(())
    }

    /// Expires the `DAY` orders due at `due` that are still active, each in its own commit with `OrderExpired` stamped
    /// at `due`, the close. Returns the number of events.
    fn expire_due(&mut self, due: DateTime<Utc>) -> Result<usize, ExchangeError> {
        let mut events = 0;
        while let Some(entry) = self.expiries.first().cloned() {
            if entry.0 > due {
                break;
            }
            self.expiries.remove(&entry);
            let (_, _, instrument, order_id, business_date) = entry;
            let Some(book) = self.books.get(&instrument) else {
                continue;
            };
            if book.order(order_id).is_none() {
                continue; // Filled or cancelled since.
            }
            let update = book.plan_expire(order_id)?;
            self.commit_and_apply(Command::Expire { business_date }, update, due)?;
            events += 1;
            self.maintain_liquidity(Some(&instrument))?;
            self.publish_market_data(Some(&instrument));
        }
        Ok(events)
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
            // Nothing of the group was published; what is due now compares against the reloaded books.
            self.publish_due_market_data();
            return Err(error.into());
        }
        ::metrics::histogram!(crate::metrics::COMMIT_DURATION).record(started.elapsed());
        for command in &committed {
            command.record();
        }
        self.signal_deliveries(committed.iter().any(|c| c.delivers));
        self.publish_due_market_data();
        Ok(())
    }

    fn publish_due_market_data(&mut self) {
        for id in std::mem::take(&mut self.market_data_due) {
            self.publish_market_data(Some(&id));
        }
    }

    /// Rebuilds what commands change in memory (books, sequences, the settlement simulator) from storage.
    fn reload(&mut self) -> Result<(), ExchangeError> {
        self.books = load_books(&self.store)?;
        self.schedule_expiries()?;
        // The group's trades are gone: forget their prints, and take the last trade and volume from storage again.
        for state in self.market_data.values_mut() {
            state.pending.clear();
            state.last_trade = None;
            state.volume_date = None;
            state.volume = 0;
        }
        self.load_market_data()?;
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

    /// Cancels the remaining quantity of an active order, then lets the liquidity provider react.
    pub fn cancel(&mut self, order_id: Uuid) -> Result<BookUpdate, ExchangeError> {
        let update = self.cancel_order(order_id)?;
        self.maintain_liquidity(Some(&update.order.instrument_id))?;
        self.publish_market_data(Some(&update.order.instrument_id));
        Ok(update)
    }

    /// The ID of the order `target` names. A client order ID is looked up within its participant, among the orders
    /// stored so far, those of an open commit group included.
    pub fn order_id_of(&self, target: CancelTarget) -> Result<Uuid, ExchangeError> {
        match target {
            CancelTarget::Order(order_id) => Ok(order_id),
            CancelTarget::Client {
                participant_id,
                client_order_id,
            } => match self
                .store
                .order_by_client_id(&participant_id, &client_order_id)?
            {
                Some(record) => Ok(record.order.id),
                None => Err(ExchangeError::UnknownClientOrderId {
                    participant_id,
                    client_order_id,
                }),
            },
        }
    }

    fn cancel_order(&mut self, order_id: Uuid) -> Result<BookUpdate, ExchangeError> {
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

    /// Registers a webhook endpoint for the given event types (see [`WebhookHub::register`]).
    pub fn register_webhook(
        &mut self,
        request: NewSubscription,
    ) -> Result<Subscription, ExchangeError> {
        let now = self.now();
        Ok(self
            .webhooks
            .register(self.store.connection(), request, self.ids, now)?)
    }

    /// Brings the subscriptions declared in configuration into the database (see [`WebhookHub::reconcile`]). Runs at
    /// recovery and after a reset.
    fn reconcile_webhooks(&mut self) -> Result<(), ExchangeError> {
        let now = self.now();
        let wake = self
            .webhooks
            .reconcile(self.store.connection(), self.ids, now)?;
        self.signal_deliveries(wake);
        Ok(())
    }

    /// Changes a subscription (see [`WebhookHub::update`]).
    pub fn update_webhook(
        &mut self,
        id: Uuid,
        change: WebhookChange,
    ) -> Result<Subscription, ExchangeError> {
        let (subscription, wake) = self.webhooks.update(self.store.connection(), id, change)?;
        self.signal_deliveries(wake);
        Ok(subscription)
    }

    /// Deletes a subscription created through the API (see [`WebhookHub::delete`]).
    pub fn delete_webhook(&mut self, id: Uuid) -> Result<(), ExchangeError> {
        Ok(self.webhooks.delete(self.store.connection(), id)?)
    }

    /// Replaces a subscription's signing secret (see [`WebhookHub::rotate_secret`]).
    pub fn rotate_webhook_secret(
        &mut self,
        id: Uuid,
        secret: String,
        grace: chrono::Duration,
    ) -> Result<Subscription, ExchangeError> {
        Ok(self
            .webhooks
            .rotate_secret(self.store.connection(), id, secret, grace)?)
    }

    /// Sets (or with `None` clears) a subscription's delivery fault policy (see [`WebhookHub::set_faults`]).
    pub fn set_webhook_faults(
        &mut self,
        subscription_id: Uuid,
        policy: Option<FaultPolicy>,
    ) -> Result<Subscription, ExchangeError> {
        Ok(self
            .webhooks
            .set_faults(self.store.connection(), subscription_id, policy)?)
    }

    /// Claims up to `limit` due webhook deliveries, leased for `lease`.
    pub fn claim_deliveries(
        &mut self,
        limit: u32,
        lease: Duration,
    ) -> Result<Vec<DeliveryJob>, ExchangeError> {
        Ok(self
            .webhooks
            .claim(self.store.connection_mut(), limit, lease, self.epoch)?)
    }

    /// Records delivery results, except those claimed before a reset.
    pub fn record_deliveries(&mut self, results: &[DeliveryResult]) -> Result<(), ExchangeError> {
        Ok(self
            .webhooks
            .record(self.store.connection_mut(), results, self.epoch)?)
    }

    /// Clears all state: every table emptied, books, participants, subscriptions and the settlement simulator back to
    /// nothing, sequences starting at 1, and the virtual clock back at its first reading (the configured start, or
    /// without one the time the service started). The service then behaves
    /// as on a new database, so a deterministic run after a reset repeats a fresh one, IDs included. Only the
    /// configuration remains.
    pub fn reset(&mut self) -> Result<(), ExchangeError> {
        self.store.reset()?;
        self.books.clear();
        self.expiries.clear();
        self.prices.clear();
        self.market_data.clear();
        self.participants.clear();
        self.webhooks.clear();
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
        let now = self.now();
        if let Some(days) = &mut self.days {
            days.cursor = BusinessDays::first_cursor(&days.calendar, now);
            self.store.save_business_day_cursor(days.cursor)?;
        }
        if let Some(clearing) = &mut self.clearing {
            clearing.cursor = Clearing::first_cursor(&clearing.policy, now);
            clearing.after = 0;
            clearing.pending.clear();
            self.store.save_clearing_cursor(clearing.cursor, 0)?;
        }
        self.epoch += 1;
        // Configuration-declared subscriptions and instruments come back, as on a new database.
        self.reconcile_webhooks()?;
        self.start_liquidity()?;
        self.reconcile_instruments()?;
        tracing::info!(epoch = self.epoch, "exchange state reset");
        Ok(())
    }

    /// Queues the subscription's events after sequence `after` for delivery again (see [`WebhookHub::redeliver`]).
    pub fn redeliver(&mut self, subscription_id: Uuid, after: u64) -> Result<usize, ExchangeError> {
        let queued = self.webhooks.redeliver(
            self.store.connection_mut(),
            subscription_id,
            after,
            self.ids,
        )?;
        self.signal_deliveries(queued > 0);
        Ok(queued)
    }

    /// Gives dead-lettered deliveries a fresh attempt budget: all, one subscription's, or the listed ones.
    pub fn retry_dead_letters(
        &mut self,
        subscription_id: Option<Uuid>,
        delivery_ids: &[Uuid],
    ) -> Result<usize, ExchangeError> {
        let retried = self.webhooks.retry_dead_letters(
            self.store.connection_mut(),
            subscription_id,
            delivery_ids,
        )?;
        self.signal_deliveries(retried > 0);
        Ok(retried)
    }

    /// Everything due by now on domain time, in time order: settlement transitions, each business day's close, and
    /// clearing statements at each cutoff (and late ones when due). Before each instant, the settlement transitions
    /// due by it run first, so the event log stays in time order across a large clock move; at a cutoff the day closes
    /// before its statements are issued. Returns the number of events.
    pub fn tick(&mut self) -> Result<usize, ExchangeError> {
        let mut total = 0;
        loop {
            let now = self.now();
            let close = self.days.as_ref().map(BusinessDays::due);
            let clearing = self.clearing.as_ref().map(Clearing::due);
            let expiry = self.expiries.first().map(|e| e.0);
            match close.into_iter().chain(clearing).chain(expiry).min() {
                Some(due) if due <= now => {
                    total += self.settlement_step(due)?;
                    // DAY orders end with their trading day, before the business day closes.
                    if expiry == Some(due) {
                        total += self.expire_due(due)?;
                    }
                    if close == Some(due) {
                        total += self.close_business_day()?;
                    }
                    if clearing == Some(due) {
                        total += self.clearing_step(due)?;
                    }
                }
                _ => {
                    total += self.settlement_step(now)?;
                    // Time moved: reference prices with it, and refill delays ran out.
                    self.maintain_liquidity(None)?;
                    self.publish_market_data(None);
                    return Ok(total);
                }
            }
        }
    }

    /// Closes the next business day with `BusinessDayClosed`, committed with the moved cursor.
    fn close_business_day(&mut self) -> Result<usize, ExchangeError> {
        let days = self.days.as_ref().expect("business days run");
        let date = days.calendar.days.next_business_day(days.cursor);
        let calendar = days.calendar.clone();
        let mut new_events = vec![events::business_day_closed(date, &calendar)];
        new_events.extend(self.closing_prices(date, &calendar)?);
        let next = NewEvent::stamp(&mut new_events, self.next_event_sequence, self.ids);
        let outbox = Outbox {
            subscriptions: self.webhooks.subscriptions(),
            simulation_id: self.simulation_id.as_deref(),
            ids: self.ids,
            due: wall_now(),
            schema_version: events::SCHEMA_VERSION,
            parties: self.webhooks.parties(),
        };
        self.store.commit_business_day(date, &new_events, outbox)?;
        self.next_event_sequence = next;
        for event in &new_events {
            ::metrics::counter!(crate::metrics::EVENTS, "event_type" => event.event_type)
                .increment(1);
        }
        self.days.as_mut().expect("business days run").cursor = date;
        self.wake_delivery(&new_events);
        Ok(new_events.len())
    }

    /// `ClosingPriceIssued` for business date `date`, in instrument ID order (SR-14): every listed instrument that
    /// traded in the day's window closes at its last trade then; one that did not, at its reference price at the
    /// cutoff; one with neither has no closing price. The reference is read as of the cutoff without moving the
    /// source, so the event is the same however late the cutoff is handled.
    fn closing_prices(
        &self,
        date: chrono::NaiveDate,
        calendar: &simulator_kit::calendar::CutoffCalendar,
    ) -> Result<Vec<NewEvent>, ExchangeError> {
        let (from, cutoff_at) = calendar.window(date);
        let traded: HashMap<InstrumentId, crate::persistence::DayTrading> = self
            .store
            .day_trading(from, cutoff_at)?
            .into_iter()
            .map(|t| (t.instrument_id.clone(), t))
            .collect();
        let mut ids: Vec<&InstrumentId> = self.books.keys().collect();
        ids.sort();
        Ok(ids
            .into_iter()
            .filter_map(|id| {
                let instrument = self.books[id].instrument();
                let (price, basis, volume) = match traded.get(id) {
                    Some(day) => (
                        day.last_price_ticks,
                        events::ClosingBasis::LastTrade,
                        day.volume,
                    ),
                    None => (
                        self.prices.reference_at(id, cutoff_at)?.ticks,
                        events::ClosingBasis::Reference,
                        0,
                    ),
                };
                Some(events::closing_price(
                    instrument, date, cutoff_at, price, basis, volume,
                ))
            })
            .collect())
    }

    /// One clearing step at `at`, the earliest instant something is due: the cutoff's statements (those not drafted
    /// late are issued at once), and late statements due by `at`. Committed in one transaction, then applied.
    fn clearing_step(&mut self, at: DateTime<Utc>) -> Result<usize, ExchangeError> {
        let clearing = self.clearing.as_ref().expect("clearing runs");
        let date = clearing
            .policy
            .calendar
            .days
            .next_business_day(clearing.cursor);
        let calendar = clearing.policy.calendar.clone();
        let mut issue: Vec<StoredStatement> = clearing
            .pending
            .iter()
            .filter(|p| p.draft.statement.issued_at <= at)
            .cloned()
            .collect();
        let mut waiting = Vec::new();
        let mut cursor = None;
        if calendar.cutoff_at(date) <= at {
            let (from, until) = calendar.window(date);
            let trades = self
                .store
                .trades_between(clearing.after, from, until)?
                .iter()
                .map(TradeFacts::from_event)
                .collect::<Result<Vec<_>, _>>()
                .map_err(ExchangeError::Clearing)?;
            for draft in clearing.policy.draft(date, &trades) {
                let stored = StoredStatement {
                    draft,
                    issued: false,
                    event_sequence: None,
                };
                if stored.draft.statement.issued_at <= at {
                    issue.push(stored);
                } else {
                    waiting.push(stored);
                }
            }
            let after = trades.last().map_or(clearing.after, |t| t.sequence);
            cursor = Some((date, after));
        }
        issue.sort_by(|a, b| issue_order(a).cmp(&issue_order(b)));
        let mut stamped: Vec<NewEvent> = issue
            .iter()
            .map(|s| crate::clearing::statement_event(&s.draft.statement))
            .collect();
        let next = NewEvent::stamp(&mut stamped, self.next_event_sequence, self.ids);
        for (statement, event) in issue.iter_mut().zip(&stamped) {
            statement.issued = true;
            statement.event_sequence = Some(event.sequence);
        }
        let outbox = Outbox {
            subscriptions: self.webhooks.subscriptions(),
            simulation_id: self.simulation_id.as_deref(),
            ids: self.ids,
            due: wall_now(),
            schema_version: events::SCHEMA_VERSION,
            parties: self.webhooks.parties(),
        };
        let rows: Vec<&StoredStatement> = issue.iter().chain(&waiting).collect();
        self.store
            .commit_clearing(&stamped, &rows, cursor, outbox)?;
        self.next_event_sequence = next;
        for event in &stamped {
            ::metrics::counter!(crate::metrics::EVENTS, "event_type" => event.event_type)
                .increment(1);
        }
        let clearing = self.clearing.as_mut().expect("clearing runs");
        if let Some((date, after)) = cursor {
            clearing.cursor = date;
            clearing.after = after;
        }
        clearing
            .pending
            .retain(|p| p.draft.statement.issued_at > at);
        clearing.pending.extend(waiting);
        self.sort_pending();
        self.wake_delivery(&stamped);
        Ok(stamped.len())
    }

    fn sort_pending(&mut self) {
        if let Some(clearing) = &mut self.clearing {
            clearing
                .pending
                .sort_by(|a, b| issue_order(a).cmp(&issue_order(b)));
        }
    }

    /// One settlement simulator step: takes in `TradeExecuted` events past the stored cursor (in pages), then runs
    /// every lifecycle transition due by `until`. Each page is committed (instructions, events, outbox, cursor)
    /// before it is applied. Returns the number of transitions; 0 when the simulator is off.
    fn settlement_step(&mut self, until: DateTime<Utc>) -> Result<usize, ExchangeError> {
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
            let plan = simulator.plan(&trades, until);
            if plan.transitions.is_empty() && plan.cursor == simulator.cursor() {
                return Ok(total);
            }
            let mut stamped: Vec<NewEvent> =
                plan.transitions.iter().map(|t| t.event.clone()).collect();
            let next = NewEvent::stamp(&mut stamped, self.next_event_sequence, self.ids);
            let outbox = Outbox {
                subscriptions: self.webhooks.subscriptions(),
                simulation_id: self.simulation_id.as_deref(),
                ids: self.ids,
                due: wall_now(),
                schema_version: events::SCHEMA_VERSION,
                parties: self.webhooks.parties(),
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
        let Some((buyer_id, seller_id)) = self.store.execution_parties(report.execution_id)? else {
            return Err(ExchangeError::UnknownExecution(report.execution_id));
        };
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
                "buyerId": buyer_id,
                "sellerId": seller_id,
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
            subscriptions: self.webhooks.subscriptions(),
            simulation_id: self.simulation_id.as_deref(),
            ids: self.ids,
            due: wall_now(),
            schema_version: events::SCHEMA_VERSION,
            parties: self.webhooks.parties(),
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
            .clone()
            .ok_or(ExchangeError::ClockNotVirtual)?;
        let changed = change.apply(&clock)?;
        // Whatever became due by the new time happens first, so the log stays in time order; then the new reading
        // commits with its ClockChanged.
        self.tick()?;
        if changed {
            let mut event =
                simulator_kit::clock::changed_event(change.name(), &clock, events::EXCHANGE_SOURCE);
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
                schema_version: events::SCHEMA_VERSION,
                parties: self.webhooks.parties(),
            };
            self.store
                .commit_clock(clock.now(), clock.is_running(), &event, outbox)?;
            self.next_event_sequence = next;
            ::metrics::counter!(crate::metrics::EVENTS, "event_type" => event.event_type)
                .increment(1);
            self.wake_delivery(std::slice::from_ref(&event));
        }
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
        if let Some(provider) = &self.provider
            && !provider.policy.emit_order_events
        {
            // The provider's order lifecycle stays out of the log (SR-10); its trades stay in.
            let lp = provider.policy.participant.0.as_str();
            new_events.retain(|e| {
                e.event_type == events::TRADE_EXECUTED || e.payload["participantId"] != lp
            });
        }
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
            subscriptions: self.webhooks.subscriptions(),
            simulation_id: self.simulation_id.as_deref(),
            ids: self.ids,
            due: wall_now(),
            schema_version: events::SCHEMA_VERSION,
            parties: self.webhooks.parties(),
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
        for execution in &update.executions {
            let date = self.business_date(execution.executed_at);
            self.market_data
                .entry(update.order.instrument_id.clone())
                .or_default()
                .trade(
                    crate::market_data::Print {
                        price_ticks: execution.price_ticks,
                        quantity: execution.quantity,
                        executed_at: execution.executed_at,
                    },
                    date,
                );
        }
        if let Some(provider) = &mut self.provider {
            let lp = &provider.policy.participant;
            for execution in &update.executions {
                let signed = match (&execution.buyer_id, &execution.seller_id) {
                    (buyer, _) if buyer == lp => execution.quantity as i64,
                    (_, seller) if seller == lp => -(execution.quantity as i64),
                    _ => continue,
                };
                let quoting = provider
                    .quoting
                    .entry(update.order.instrument_id.clone())
                    .or_default();
                quoting.inventory += signed;
                quoting.filled_at = Some(now);
                ::metrics::counter!(crate::metrics::LIQUIDITY_FILLS, "instrument" => update.order.instrument_id.0.clone())
                    .increment(1);
            }
        }
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
        self.webhooks.delivers(events)
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

/// Statements are issued in time order, then by statement ID.
fn issue_order(s: &StoredStatement) -> (DateTime<Utc>, &str) {
    (s.draft.statement.issued_at, &s.draft.statement.statement_id)
}

use simulator_kit::clock::wall_now;

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

fn same_terms(order: &Order, command: &SubmitOrder) -> bool {
    order.instrument_id == command.instrument_id
        && order.side == command.side
        && order.order_type == command.order_type
        && order.limit_price_ticks == command.limit_price_ticks
        && order.quantity == command.quantity
        && command
            .time_in_force
            .is_none_or(|t| t == order.time_in_force)
}

/// The reference-price metrics after one publication: an update when the published price moved from `before`, and
/// the staleness and age of what is published now.
fn record_reference(
    id: &InstrumentId,
    before: Option<i64>,
    reference: Option<Reference>,
    now: DateTime<Utc>,
) {
    use crate::metrics::{REFERENCE_AGE, REFERENCE_STALE, REFERENCE_UPDATES};
    let Some(reference) = reference else {
        ::metrics::gauge!(REFERENCE_STALE, "instrument" => id.0.clone()).set(0.0);
        return;
    };
    if before != Some(reference.ticks) {
        ::metrics::counter!(REFERENCE_UPDATES, "instrument" => id.0.clone()).increment(1);
    }
    ::metrics::gauge!(REFERENCE_STALE, "instrument" => id.0.clone())
        .set(f64::from(u8::from(reference.stale)));
    let age = (now - reference.source_timestamp).num_milliseconds().max(0) as f64 / 1000.0;
    ::metrics::gauge!(REFERENCE_AGE, "instrument" => id.0.clone()).set(age);
}
