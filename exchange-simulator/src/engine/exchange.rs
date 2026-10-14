use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SubsecRound, Utc};
use uuid::Uuid;

use super::{BookError, BookUpdate, Clock, IdGenerator, MatchContext, OrderBook};
use crate::domain::{
    IdentifierPolicy, Instrument, InstrumentError, InstrumentId, Order, OrderStatus, OrderType,
    Participant, ParticipantId, Side,
};
use crate::events::{self, Command};
use crate::persistence::Store;
use crate::webhooks::{DeliveryJob, DeliveryResult, Subscription};

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
    #[error("participant ID and name must not be blank")]
    InvalidParticipant,
    #[error("invalid webhook subscription: {0}")]
    InvalidWebhook(String),
    #[error(transparent)]
    Instrument(#[from] InstrumentError),
    #[error(transparent)]
    Book(#[from] BookError),
    #[error("storage failure: {0}")]
    Storage(#[from] rusqlite::Error),
}

/// Construction-time dependencies of an [`Exchange`].
pub struct ExchangeOptions {
    pub identifier_policy: IdentifierPolicy,
    pub ids: Box<dyn IdGenerator + Send>,
    pub clock: Box<dyn Clock + Send>,
    /// Stamped on every event as `simulationId`.
    pub simulation_id: Option<String>,
    /// Notified after a commit that created webhook deliveries, so the delivery worker need not wait for its poll.
    pub delivery_signal: Option<Arc<tokio::sync::Notify>>,
}

/// The persistence coordinator: owns the order books and the single SQLite writer, and serializes every command.
///
/// Each command is planned on the book, committed to SQLite, and only then applied to memory. When the commit fails
/// the plan is dropped, so memory never holds state the database does not; a caller only sees success after commit.
pub struct Exchange {
    store: Store,
    books: HashMap<InstrumentId, OrderBook>,
    participants: HashSet<ParticipantId>,
    last_sequence: u64,
    identifier_policy: IdentifierPolicy,
    ids: Box<dyn IdGenerator + Send>,
    clock: Box<dyn Clock + Send>,
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
        let participants = store.participants()?.into_iter().map(|p| p.id).collect();
        let last_sequence = store.max_sequence()?;
        let subscriptions = store.subscriptions()?;
        let released = store.release_claims(options.clock.now())?;
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
            clock: options.clock,
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

    pub fn register_participant(&mut self, participant: Participant) -> Result<(), ExchangeError> {
        if participant.id.0.trim().is_empty() || participant.name.trim().is_empty() {
            return Err(ExchangeError::InvalidParticipant);
        }
        if self.participants.contains(&participant.id) {
            return Err(ExchangeError::DuplicateParticipant(participant.id));
        }
        self.store.insert_participant(&participant)?;
        self.participants.insert(participant.id);
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
        if !self.participants.contains(&command.participant_id) {
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
        let update = book.plan_submit(
            order,
            &mut MatchContext {
                now,
                ids: self.ids.as_mut(),
            },
        )?;
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
    /// events can be replayed from the events API.
    pub fn register_webhook(
        &mut self,
        url: String,
        event_types: Vec<String>,
    ) -> Result<Subscription, ExchangeError> {
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
        let mut event_types = event_types;
        event_types.sort();
        event_types.dedup();
        let subscription = Subscription {
            id: self.ids.next_id(),
            url,
            event_types,
            enabled: true,
            created_at: self.now(),
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
        let now = self.now();
        let lease_until = now + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::MAX);
        Ok(self.store.claim_deliveries(now, lease_until, limit)?)
    }

    pub fn record_deliveries(&mut self, results: &[DeliveryResult]) -> Result<(), ExchangeError> {
        let now = self.now();
        Ok(self.store.record_deliveries(results, now)?)
    }

    fn commit_and_apply(
        &mut self,
        command: Command,
        update: BookUpdate,
        now: DateTime<Utc>,
    ) -> Result<SubmitOutcome, ExchangeError> {
        let instrument = self.books[&update.order.instrument_id].instrument();
        let new_events = events::for_update(command, &update, instrument, now, self.ids.as_mut());
        self.store.commit(
            &update,
            &new_events,
            &self.subscriptions,
            self.simulation_id.as_deref(),
            self.ids.as_mut(),
            now,
        )?;
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

fn same_terms(order: &Order, command: &SubmitOrder) -> bool {
    order.instrument_id == command.instrument_id
        && order.side == command.side
        && order.order_type == command.order_type
        && order.limit_price_ticks == command.limit_price_ticks
        && order.quantity == command.quantity
}
