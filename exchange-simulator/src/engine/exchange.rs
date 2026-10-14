use std::collections::{HashMap, HashSet};

use chrono::{DateTime, SubsecRound, Utc};
use uuid::Uuid;

use super::{BookError, BookUpdate, Clock, IdGenerator, MatchContext, OrderBook};
use crate::domain::{
    IdentifierPolicy, Instrument, InstrumentError, InstrumentId, Order, OrderStatus, OrderType,
    Participant, ParticipantId, Side,
};
use crate::persistence::Store;

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
}

impl Exchange {
    /// Loads instruments and participants and rebuilds every book from the active orders, in acceptance sequence.
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
        Ok(Self {
            store,
            books,
            participants,
            last_sequence,
            identifier_policy: options.identifier_policy,
            ids: options.ids,
            clock: options.clock,
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
        self.commit_and_apply(update, now)
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
        match self.commit_and_apply(update, now)? {
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

    fn commit_and_apply(
        &mut self,
        update: BookUpdate,
        now: DateTime<Utc>,
    ) -> Result<SubmitOutcome, ExchangeError> {
        self.store.commit(&update, now)?;
        let book = self
            .books
            .get_mut(&update.order.instrument_id)
            .expect("planned on this book");
        book.apply(&update)
            .expect("nothing else touched the book between plan and apply");
        self.last_sequence = self.last_sequence.max(update.order.sequence);
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
