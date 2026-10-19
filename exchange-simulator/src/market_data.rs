//! The market-data channel (plan item 6.8, SR-12, SR-13): what the exchange publishes about each instrument, as a
//! snapshot (`GET /api/marketdata`) and as a live stream of server-sent events (`GET /api/marketdata/stream`).
//!
//! - **Messages:** `quote` (best bid or ask changed), `trade` (an anonymous print: price, quantity, time) and
//!   `reference` (the reference price changed or became stale). Each carries its instrument's `mdSequence`, which
//!   counts the instrument's messages from 1 since the service started.
//! - **Published at command boundaries:** after a whole order or cancellation (the liquidity provider's re-quote
//!   included), after an instrument change, and on every tick. Intermediate states, such as a half-replaced ladder,
//!   never reach the feed.
//! - **Never slows the exchange:** the exchange thread sends into a broadcast channel, which never waits. A
//!   subscriber more than half of `market_data.stream_buffer` messages behind is conflated harder: its whole backlog
//!   collapses to the latest message per instrument and kind. One that still falls a whole buffer behind loses its
//!   place, and its stream is closed; it reconnects and resynchronises from the snapshot.
//! - **Conflation:** per subscriber, per instrument and kind, at most one message every `conflation_interval_ms`;
//!   the latest wins. `quote` and `reference` messages hold the whole state of their kind, so only intermediate
//!   states are skipped; skipped trade prints remain in `/api/trades` and the event log.
//! - **Not durable:** the feed is not written to the event log or delivered over webhooks.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::domain::{Instrument, InstrumentId};
use crate::pricing::Reference;

/// One market-data message, serialized once for every subscriber.
#[derive(Debug)]
pub struct Message {
    pub instrument_id: InstrumentId,
    /// `quote`, `trade` or `reference`.
    pub kind: &'static str,
    pub md_sequence: u64,
    /// The JSON body, `instrumentId` and `mdSequence` included.
    pub data: String,
}

pub type Sender = broadcast::Sender<Arc<Message>>;

/// The best price on one side and the quantity resting there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Top {
    pub price_ticks: i64,
    pub quantity: u64,
}

/// A trade print, without participants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Print {
    pub price_ticks: i64,
    pub quantity: u64,
    pub executed_at: DateTime<Utc>,
}

/// An instrument's market data at one moment, for the snapshot.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub instrument: Instrument,
    pub md_sequence: u64,
    pub reference: Option<Reference>,
    pub best_bid: Option<Top>,
    pub best_ask: Option<Top>,
    pub last_trade: Option<Print>,
    /// Traded quantity on the current business date.
    pub session_volume: u64,
}

/// What the exchange keeps per instrument: what it last published, and the trades since.
#[derive(Clone, Debug, Default)]
pub struct State {
    pub md_sequence: u64,
    /// The quote last published.
    pub quote: Option<(Option<Top>, Option<Top>)>,
    /// The reference last published: price, source and staleness, or `Some(None)` for none.
    pub reference: Option<Option<(i64, &'static str, bool)>>,
    pub last_trade: Option<Print>,
    /// The business date `volume` is for.
    pub volume_date: Option<NaiveDate>,
    pub volume: u64,
    /// Prints not yet published.
    pub pending: Vec<Print>,
}

impl State {
    /// Records a trade: the last print, the session volume, and a print to publish.
    pub fn trade(&mut self, print: Print, business_date: NaiveDate) {
        if self.volume_date != Some(business_date) {
            self.volume_date = Some(business_date);
            self.volume = 0;
        }
        self.volume += print.quantity;
        self.last_trade = Some(print);
        self.pending.push(print);
    }

    /// The session volume on `business_date`: 0 when the last trade was on an earlier one.
    pub fn volume_on(&self, business_date: NaiveDate) -> u64 {
        if self.volume_date == Some(business_date) {
            self.volume
        } else {
            0
        }
    }

    /// The messages that bring a subscriber from the last published state to `quote` and `reference`, after the
    /// pending prints, each with the next `mdSequence`. Updates the published state.
    pub fn publish(
        &mut self,
        instrument: &Instrument,
        quote: (Option<Top>, Option<Top>),
        reference: Option<Reference>,
    ) -> Vec<Message> {
        let mut messages = Vec::new();
        let mut next = |state: &mut Self, kind: &'static str, mut body: Value| {
            state.md_sequence += 1;
            body["instrumentId"] = json!(instrument.id);
            body["mdSequence"] = json!(state.md_sequence);
            messages.push(Message {
                instrument_id: instrument.id.clone(),
                kind,
                md_sequence: state.md_sequence,
                data: body.to_string(),
            });
        };
        for print in std::mem::take(&mut self.pending) {
            next(self, "trade", print_json(instrument, &print));
        }
        if self.quote != Some(quote) {
            self.quote = Some(quote);
            next(
                self,
                "quote",
                json!({ "bestBid": top_json(instrument, quote.0), "bestAsk": top_json(instrument, quote.1) }),
            );
        }
        let published = reference.map(|r| (r.ticks, r.kind.as_str(), r.stale));
        if self.reference != Some(published) {
            self.reference = Some(published);
            next(
                self,
                "reference",
                json!({ "reference": reference.map(|r| reference_json(instrument, &r)) }),
            );
        }
        messages
    }
}

impl Snapshot {
    /// The snapshot as `GET /api/marketdata` shows it.
    pub fn to_json(&self) -> Value {
        let i = &self.instrument;
        json!({
            "instrumentId": i.id,
            "mdSequence": self.md_sequence,
            "reference": self.reference.as_ref().map(|r| reference_json(i, r)),
            "bestBid": top_json(i, self.best_bid),
            "bestAsk": top_json(i, self.best_ask),
            "lastTrade": self.last_trade.as_ref().map(|p| print_json(i, p)),
            "sessionVolume": self.session_volume,
        })
    }
}

pub fn top_json(instrument: &Instrument, top: Option<Top>) -> Value {
    top.map_or(Value::Null, |t| {
        json!({ "priceTicks": t.price_ticks, "price": instrument.ticks_to_price(t.price_ticks), "quantity": t.quantity })
    })
}

pub fn print_json(instrument: &Instrument, print: &Print) -> Value {
    json!({
        "priceTicks": print.price_ticks,
        "price": instrument.ticks_to_price(print.price_ticks),
        "quantity": print.quantity,
        "executedAt": print.executed_at,
    })
}

pub fn reference_json(instrument: &Instrument, reference: &Reference) -> Value {
    json!({
        "price": instrument.ticks_to_price(reference.ticks),
        "priceTicks": reference.ticks,
        "source": reference.kind.as_str(),
        "sourceTimestamp": reference.source_timestamp,
        "stale": reference.stale,
    })
}

/// How subscribers are served (`market_data:` in the configuration).
#[derive(Clone, Copy, Debug)]
pub struct StreamOptions {
    /// Per instrument and kind, at most one message this often; zero sends every message.
    pub conflation: Duration,
    /// The channel's capacity: a subscriber this far behind is closed, half as far behind is caught up.
    pub buffer: usize,
}

/// One subscriber's place in the channel and its conflation state.
struct Subscriber {
    receiver: broadcast::Receiver<Arc<Message>>,
    instruments: Option<Vec<InstrumentId>>,
    conflation: Duration,
    /// A backlog longer than this is collapsed at once.
    catch_up_at: usize,
    /// Conflated messages waiting for the next flush, by instrument and kind.
    pending: BTreeMap<(InstrumentId, &'static str), Arc<Message>>,
    /// Flushed messages to hand out, oldest first.
    ready: std::collections::VecDeque<Arc<Message>>,
    flush_at: Option<tokio::time::Instant>,
    done: bool,
}

/// A subscriber's view of the channel: messages for `instruments` (all when `None`), conflated per instrument and
/// kind to one per `options.conflation`. Ends when the subscriber lagged behind the buffer, or the exchange stopped.
pub fn subscribe(
    receiver: broadcast::Receiver<Arc<Message>>,
    instruments: Option<Vec<InstrumentId>>,
    options: StreamOptions,
) -> impl futures_util::Stream<Item = Arc<Message>> {
    let subscriber = Subscriber {
        receiver,
        instruments,
        conflation: options.conflation,
        catch_up_at: options.buffer / 2,
        pending: BTreeMap::new(),
        ready: Default::default(),
        flush_at: None,
        done: false,
    };
    futures_util::stream::unfold(subscriber, |mut s| async move {
        loop {
            if let Some(message) = s.ready.pop_front() {
                return Some((message, s));
            }
            if s.done {
                return None;
            }
            let received = match s.flush_at {
                Some(at) => tokio::select! {
                    received = s.receiver.recv() => Some(received),
                    () = tokio::time::sleep_until(at) => None,
                },
                None => Some(s.receiver.recv().await),
            };
            match received {
                None => s.flush(),
                Some(Ok(message)) => {
                    if s.receiver.len() > s.catch_up_at {
                        // Behind: collapse everything queued to the latest per instrument and kind, and send that.
                        ::metrics::counter!(crate::metrics::MARKET_DATA_CATCH_UPS).increment(1);
                        s.conflate(message);
                        while let Ok(message) = s.receiver.try_recv() {
                            s.conflate(message);
                        }
                        s.flush();
                        continue;
                    }
                    if s.conflation.is_zero() {
                        if s.wants(&message) {
                            return Some((message, s));
                        }
                        continue;
                    }
                    s.conflate(message);
                    if !s.pending.is_empty() {
                        s.flush_at
                            .get_or_insert_with(|| tokio::time::Instant::now() + s.conflation);
                    }
                }
                Some(Err(broadcast::error::RecvError::Lagged(skipped))) => {
                    ::metrics::counter!(crate::metrics::MARKET_DATA_STREAMS_CLOSED).increment(1);
                    tracing::info!(
                        skipped,
                        "market-data subscriber fell behind the buffer; closing its stream"
                    );
                    s.done = true;
                    s.pending.clear();
                }
                Some(Err(broadcast::error::RecvError::Closed)) => {
                    s.done = true;
                    s.flush();
                }
            }
        }
    })
}

impl Subscriber {
    fn wants(&self, message: &Message) -> bool {
        self.instruments
            .as_ref()
            .is_none_or(|ids| ids.contains(&message.instrument_id))
    }

    /// Keeps `message` for the next flush, replacing an older one of its instrument and kind.
    fn conflate(&mut self, message: Arc<Message>) {
        if self.wants(&message) {
            self.pending
                .insert((message.instrument_id.clone(), message.kind), message);
        }
    }

    /// Hands out the conflated messages, per instrument in `mdSequence` order.
    fn flush(&mut self) {
        let mut flushed: Vec<Arc<Message>> =
            std::mem::take(&mut self.pending).into_values().collect();
        flushed.sort_by_key(|m| (m.instrument_id.clone(), m.md_sequence));
        self.ready.extend(flushed);
        self.flush_at = None;
    }
}

/// Market-data state per instrument, kept by the exchange.
pub type States = HashMap<InstrumentId, State>;
