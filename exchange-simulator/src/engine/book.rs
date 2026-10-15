use std::cmp::{Reverse, min};
use std::collections::{BTreeMap, HashMap, VecDeque};

use uuid::Uuid;

use super::MatchContext;
use crate::domain::{Execution, Instrument, InstrumentId, Order, OrderStatus, OrderType, Side};

/// Why an order is malformed. These are input errors: such an order is never accepted, sequenced or persisted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidOrder {
    #[error("quantity must be a positive multiple of the lot size")]
    Quantity,
    #[error("a limit order needs a limit price")]
    MissingLimitPrice,
    #[error("a market order must not have a limit price")]
    MarketOrderWithPrice,
    #[error("limit price must be positive")]
    NonPositivePrice,
    #[error("a new order must be OPEN with remaining quantity equal to its quantity")]
    NotNew,
}

/// Why a well-formed order was rejected by the exchange. Rejected orders are sequenced and recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RejectReason {
    #[error("order would trade against a resting order of the same participant")]
    SelfTrade,
    #[error("trading in the instrument is disabled")]
    InstrumentDisabled,
}

impl RejectReason {
    /// Stable machine-readable code for APIs and events.
    pub const fn code(self) -> &'static str {
        match self {
            Self::SelfTrade => "SELF_TRADE",
            Self::InstrumentDisabled => "INSTRUMENT_DISABLED",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BookError {
    #[error(transparent)]
    Invalid(#[from] InvalidOrder),
    #[error("order is for instrument {actual}, book is {expected}")]
    WrongInstrument {
        expected: InstrumentId,
        actual: InstrumentId,
    },
    #[error("order {0} already exists in the book")]
    DuplicateOrder(Uuid),
    #[error("order sequence {got} is not after the last accepted sequence {last}")]
    SequenceNotIncreasing { last: u64, got: u64 },
    #[error("no active order {0} in the book")]
    UnknownOrder(Uuid),
    #[error("update was planned against book version {planned}, book is at {current}")]
    StaleUpdate { planned: u64, current: u64 },
}

/// A proposed book transition: the final state of every order it touches and the executions it creates. Planning
/// leaves the book unchanged; [`OrderBook::apply`] makes it current. An update applies only to the book version it
/// was planned against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookUpdate {
    base_version: u64,
    /// The order the command was about (the incoming order, or the cancelled one), in its resulting state.
    pub order: Order,
    /// Resting orders that traded, in their resulting state: one per execution, in the same (match) order.
    pub fills: Vec<Order>,
    /// Executions in match order; empty unless the incoming order traded.
    pub executions: Vec<Execution>,
    /// Set when the exchange rejected the order; the update then contains no fills or executions.
    pub rejection: Option<RejectReason>,
}

/// Aggregated resting quantity at one price.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Level {
    pub price_ticks: i64,
    pub quantity: u64,
    pub orders: usize,
}

/// Limit order book for one instrument. Bids are ordered best (highest) first, asks best (lowest) first, and each
/// level is FIFO by acceptance sequence. Only active limit orders rest here.
#[derive(Clone, Debug)]
pub struct OrderBook {
    instrument: Instrument,
    bids: BTreeMap<Reverse<i64>, VecDeque<Uuid>>,
    asks: BTreeMap<i64, VecDeque<Uuid>>,
    orders: HashMap<Uuid, Order>,
    last_sequence: u64,
    version: u64,
}

impl OrderBook {
    pub fn new(instrument: Instrument) -> Self {
        Self {
            instrument,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
            last_sequence: 0,
            version: 0,
        }
    }

    pub fn instrument(&self) -> &Instrument {
        &self.instrument
    }

    /// Switches trading on or off. A disabled book rejects every incoming order (`INSTRUMENT_DISABLED`) but still
    /// cancels; resting orders stay. Updates planned before the switch become stale.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.instrument.enabled = enabled;
        self.version += 1;
    }

    /// Checks an incoming order's shape against the instrument, without consulting book state.
    pub fn validate(&self, order: &Order) -> Result<(), BookError> {
        if order.instrument_id != self.instrument.id {
            return Err(BookError::WrongInstrument {
                expected: self.instrument.id.clone(),
                actual: order.instrument_id.clone(),
            });
        }
        if order.status != OrderStatus::Open || order.remaining_quantity != order.quantity {
            return Err(InvalidOrder::NotNew.into());
        }
        if !self.instrument.is_valid_quantity(order.quantity) {
            return Err(InvalidOrder::Quantity.into());
        }
        match (order.order_type, order.limit_price_ticks) {
            (OrderType::Limit, None) => Err(InvalidOrder::MissingLimitPrice.into()),
            (OrderType::Limit, Some(price)) if price <= 0 => {
                Err(InvalidOrder::NonPositivePrice.into())
            }
            (OrderType::Market, Some(_)) => Err(InvalidOrder::MarketOrderWithPrice.into()),
            _ => Ok(()),
        }
    }

    /// Plans matching an incoming order (status `OPEN`, nothing filled) against the book without changing it.
    ///
    /// The order trades against the best opposite prices it crosses, oldest first within a price, each fill at the
    /// resting order's price. A limit remainder rests; a market remainder is cancelled. If the order would trade
    /// against a resting order of the same participant it is rejected whole, with no executions. While the instrument
    /// is disabled every order is rejected before matching.
    #[must_use = "a planned update changes nothing until it is applied"]
    pub fn plan_submit(
        &self,
        order: Order,
        ctx: &mut MatchContext,
    ) -> Result<BookUpdate, BookError> {
        self.validate(&order)?;
        if self.orders.contains_key(&order.id) {
            return Err(BookError::DuplicateOrder(order.id));
        }
        if order.sequence <= self.last_sequence {
            return Err(BookError::SequenceNotIncreasing {
                last: self.last_sequence,
                got: order.sequence,
            });
        }
        if !self.instrument.enabled {
            return Ok(self.rejection(order, RejectReason::InstrumentDisabled));
        }

        let mut taker = order;
        let mut fills = Vec::new();
        let mut executions = Vec::new();
        'levels: for (price, queue) in self.opposite_levels(taker.side) {
            if taker.remaining_quantity == 0 || !crosses(&taker, price) {
                break;
            }
            for id in queue {
                if taker.remaining_quantity == 0 {
                    break 'levels;
                }
                let maker = &self.orders[id];
                if maker.participant_id == taker.participant_id {
                    return Ok(self.rejection(taker, RejectReason::SelfTrade));
                }
                let quantity = min(taker.remaining_quantity, maker.remaining_quantity);
                let mut maker = maker.clone();
                maker.remaining_quantity -= quantity;
                maker.status = if maker.remaining_quantity == 0 {
                    OrderStatus::Filled
                } else {
                    OrderStatus::PartiallyFilled
                };
                taker.remaining_quantity -= quantity;
                let (buy, sell) = match taker.side {
                    Side::Buy => (&taker, &maker),
                    Side::Sell => (&maker, &taker),
                };
                executions.push(Execution {
                    id: ctx.ids.execution(taker.sequence, executions.len()),
                    instrument_id: self.instrument.id.clone(),
                    buy_order_id: buy.id,
                    sell_order_id: sell.id,
                    buyer_id: buy.participant_id.clone(),
                    seller_id: sell.participant_id.clone(),
                    price_ticks: price,
                    quantity,
                    executed_at: ctx.now,
                });
                fills.push(maker);
            }
        }

        taker.status = match (taker.remaining_quantity, taker.order_type) {
            (0, _) => OrderStatus::Filled,
            (_, OrderType::Market) => OrderStatus::Cancelled,
            (remaining, OrderType::Limit) if remaining == taker.quantity => OrderStatus::Open,
            (_, OrderType::Limit) => OrderStatus::PartiallyFilled,
        };
        Ok(BookUpdate {
            base_version: self.version,
            order: taker,
            fills,
            executions,
            rejection: None,
        })
    }

    /// Plans cancelling the remaining quantity of an active resting order.
    #[must_use = "a planned update changes nothing until it is applied"]
    pub fn plan_cancel(&self, order_id: Uuid) -> Result<BookUpdate, BookError> {
        let mut order = self
            .orders
            .get(&order_id)
            .ok_or(BookError::UnknownOrder(order_id))?
            .clone();
        order.status = OrderStatus::Cancelled;
        Ok(BookUpdate {
            base_version: self.version,
            order,
            fills: Vec::new(),
            executions: Vec::new(),
            rejection: None,
        })
    }

    /// Makes a planned update current. Fails, changing nothing, if the book moved on since the update was planned.
    pub fn apply(&mut self, update: &BookUpdate) -> Result<(), BookError> {
        if update.base_version != self.version {
            return Err(BookError::StaleUpdate {
                planned: update.base_version,
                current: self.version,
            });
        }
        for order in update.fills.iter().chain(std::iter::once(&update.order)) {
            self.reconcile(order);
        }
        self.last_sequence = self.last_sequence.max(update.order.sequence);
        self.version += 1;
        Ok(())
    }

    /// Plans and applies in one step, for callers with nothing to persist in between.
    pub fn submit(
        &mut self,
        order: Order,
        ctx: &mut MatchContext,
    ) -> Result<BookUpdate, BookError> {
        let update = self.plan_submit(order, ctx)?;
        self.apply(&update)?;
        Ok(update)
    }

    /// Plans and applies a cancellation in one step.
    pub fn cancel(&mut self, order_id: Uuid) -> Result<BookUpdate, BookError> {
        let update = self.plan_cancel(order_id)?;
        self.apply(&update)?;
        Ok(update)
    }

    /// Re-inserts an already-accepted resting order when rebuilding the book from storage. Orders must be restored in
    /// ascending sequence so each price level keeps its original FIFO order.
    pub fn restore(&mut self, order: Order) -> Result<(), BookError> {
        if order.instrument_id != self.instrument.id {
            return Err(BookError::WrongInstrument {
                expected: self.instrument.id.clone(),
                actual: order.instrument_id.clone(),
            });
        }
        if order.order_type != OrderType::Limit
            || !order.status.is_active()
            || order.remaining_quantity == 0
        {
            return Err(InvalidOrder::NotNew.into());
        }
        if order.limit_price_ticks.is_none_or(|price| price <= 0) {
            return Err(InvalidOrder::NonPositivePrice.into());
        }
        if self.orders.contains_key(&order.id) {
            return Err(BookError::DuplicateOrder(order.id));
        }
        if order.sequence <= self.last_sequence {
            return Err(BookError::SequenceNotIncreasing {
                last: self.last_sequence,
                got: order.sequence,
            });
        }
        self.last_sequence = order.sequence;
        self.reconcile(&order);
        self.version += 1;
        Ok(())
    }

    /// The active resting order with this ID.
    pub fn order(&self, order_id: Uuid) -> Option<&Order> {
        self.orders.get(&order_id)
    }

    /// Number of resting orders.
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    /// The highest sequence the book has accepted or restored.
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub fn best_bid(&self) -> Option<i64> {
        self.bids.keys().next().map(|Reverse(price)| *price)
    }

    pub fn best_ask(&self) -> Option<i64> {
        self.asks.keys().next().copied()
    }

    /// Price levels of one side, best first.
    pub fn depth(&self, side: Side) -> Vec<Level> {
        self.side_levels(side)
            .map(|(price_ticks, queue)| Level {
                price_ticks,
                quantity: queue
                    .iter()
                    .map(|id| self.orders[id].remaining_quantity)
                    .sum(),
                orders: queue.len(),
            })
            .collect()
    }

    /// Resting orders of one side in priority order.
    pub fn resting_orders(&self, side: Side) -> Vec<&Order> {
        self.side_levels(side)
            .flat_map(|(_, queue)| queue.iter().map(|id| &self.orders[id]))
            .collect()
    }

    /// The rejected form of an incoming order. Matching may have walked part of the book before finding the reason,
    /// so the order's remaining quantity is reset: none of that walk happened.
    fn rejection(&self, mut order: Order, reason: RejectReason) -> BookUpdate {
        order.status = OrderStatus::Rejected;
        order.remaining_quantity = order.quantity;
        BookUpdate {
            base_version: self.version,
            order,
            fills: Vec::new(),
            executions: Vec::new(),
            rejection: Some(reason),
        }
    }

    fn opposite_levels(&self, side: Side) -> Box<dyn Iterator<Item = (i64, &VecDeque<Uuid>)> + '_> {
        self.side_levels(side.opposite())
    }

    fn side_levels(&self, side: Side) -> Box<dyn Iterator<Item = (i64, &VecDeque<Uuid>)> + '_> {
        match side {
            Side::Buy => Box::new(
                self.bids
                    .iter()
                    .map(|(Reverse(price), queue)| (*price, queue)),
            ),
            Side::Sell => Box::new(self.asks.iter().map(|(price, queue)| (*price, queue))),
        }
    }

    /// Brings the book in line with an order's new state: active limit orders rest (new ones join the back of their
    /// level, known ones keep their place), anything else leaves the book.
    fn reconcile(&mut self, order: &Order) {
        let rests = order.status.is_active() && order.order_type == OrderType::Limit;
        let known = self.orders.contains_key(&order.id);
        if !known && !rests {
            return;
        }
        let price = order
            .limit_price_ticks
            .expect("orders in the book are limit orders");
        match (known, rests) {
            (true, true) => {
                self.orders.insert(order.id, order.clone());
            }
            (true, false) => {
                self.orders.remove(&order.id);
                self.remove_from_level(order.side, price, order.id);
            }
            (false, true) => {
                self.orders.insert(order.id, order.clone());
                match order.side {
                    Side::Buy => self
                        .bids
                        .entry(Reverse(price))
                        .or_default()
                        .push_back(order.id),
                    Side::Sell => self.asks.entry(price).or_default().push_back(order.id),
                }
            }
            (false, false) => unreachable!("returned above"),
        }
    }

    fn remove_from_level(&mut self, side: Side, price: i64, id: Uuid) {
        fn remove(queue: &mut VecDeque<Uuid>, id: Uuid) -> bool {
            if let Some(position) = queue.iter().position(|queued| *queued == id) {
                queue.remove(position);
            }
            queue.is_empty()
        }
        match side {
            Side::Buy => {
                if self
                    .bids
                    .get_mut(&Reverse(price))
                    .is_some_and(|queue| remove(queue, id))
                {
                    self.bids.remove(&Reverse(price));
                }
            }
            Side::Sell => {
                if self
                    .asks
                    .get_mut(&price)
                    .is_some_and(|queue| remove(queue, id))
                {
                    self.asks.remove(&price);
                }
            }
        }
    }
}

/// Whether a resting price is acceptable to the taker: market orders take any price.
fn crosses(taker: &Order, resting_price: i64) -> bool {
    match (taker.side, taker.limit_price_ticks) {
        (_, None) => true,
        (Side::Buy, Some(limit)) => resting_price <= limit,
        (Side::Sell, Some(limit)) => resting_price >= limit,
    }
}
