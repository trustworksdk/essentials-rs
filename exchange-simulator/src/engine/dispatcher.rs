use std::thread::JoinHandle;

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::{BookUpdate, Exchange, ExchangeError, Level, SubmitOrder, SubmitOutcome};
use crate::domain::{Instrument, InstrumentId, Participant, Side};

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    /// The command queue is full. The command was not enqueued; the caller may retry later.
    #[error("the exchange is overloaded; retry later")]
    Overloaded,
    /// The exchange thread has stopped.
    #[error("the exchange is not running")]
    Stopped,
    #[error(transparent)]
    Exchange(#[from] ExchangeError),
}

/// Point-in-time view of one instrument's book.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookSnapshot {
    pub instrument: Instrument,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    /// Exchange-wide sequence of the last order processed when the snapshot was taken; the snapshot reflects every
    /// command up to it. (Not the book's own high-water mark, which after a restart only covers resting orders.)
    pub last_sequence: u64,
}

type Reply<T> = oneshot::Sender<Result<T, ExchangeError>>;

enum Request {
    RegisterInstrument(Instrument, Reply<Instrument>),
    RegisterParticipant(Participant, Reply<Participant>),
    Submit(SubmitOrder, Reply<(SubmitOutcome, Instrument)>),
    Cancel(Uuid, Reply<(BookUpdate, Instrument)>),
    Snapshot(InstrumentId, oneshot::Sender<Option<BookSnapshot>>),
}

/// Async handle to the single exchange thread. Commands are queued on a bounded channel and executed one at a time
/// by the thread that owns the [`Exchange`] (and with it the SQLite writer). A full queue fails fast with
/// [`DispatchError::Overloaded`] instead of buffering without limit or dropping commands silently.
#[derive(Clone)]
pub struct Dispatcher {
    tx: mpsc::Sender<Request>,
}

impl Dispatcher {
    /// Starts the exchange thread. It runs until every `Dispatcher` clone is dropped, finishing queued commands first.
    pub fn spawn(exchange: Exchange, capacity: usize) -> std::io::Result<(Self, JoinHandle<()>)> {
        let (tx, rx) = mpsc::channel(capacity);
        let handle = std::thread::Builder::new()
            .name("exchange".into())
            .spawn(move || run(exchange, rx))?;
        Ok((Self { tx }, handle))
    }

    pub async fn register_instrument(
        &self,
        instrument: Instrument,
    ) -> Result<Instrument, DispatchError> {
        self.call(|reply| Request::RegisterInstrument(instrument, reply))
            .await
    }

    pub async fn register_participant(
        &self,
        participant: Participant,
    ) -> Result<Participant, DispatchError> {
        self.call(|reply| Request::RegisterParticipant(participant, reply))
            .await
    }

    /// Submits an order; resolves once the outcome is committed. Also returns the instrument, for presentation.
    pub async fn submit(
        &self,
        order: SubmitOrder,
    ) -> Result<(SubmitOutcome, Instrument), DispatchError> {
        self.call(|reply| Request::Submit(order, reply)).await
    }

    pub async fn cancel(&self, order_id: Uuid) -> Result<(BookUpdate, Instrument), DispatchError> {
        self.call(|reply| Request::Cancel(order_id, reply)).await
    }

    pub async fn snapshot(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<Option<BookSnapshot>, DispatchError> {
        let (reply, response) = oneshot::channel();
        self.enqueue(Request::Snapshot(instrument_id, reply))?;
        response.await.map_err(|_| DispatchError::Stopped)
    }

    async fn call<T>(&self, request: impl FnOnce(Reply<T>) -> Request) -> Result<T, DispatchError> {
        let (reply, response) = oneshot::channel();
        self.enqueue(request(reply))?;
        Ok(response.await.map_err(|_| DispatchError::Stopped)??)
    }

    fn enqueue(&self, request: Request) -> Result<(), DispatchError> {
        self.tx.try_send(request).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => DispatchError::Overloaded,
            mpsc::error::TrySendError::Closed(_) => DispatchError::Stopped,
        })
    }
}

fn run(mut exchange: Exchange, mut rx: mpsc::Receiver<Request>) {
    // A send fails only when the caller gave up waiting (e.g. the HTTP client disconnected). The command still took
    // effect; there is no one left to tell.
    while let Some(request) = rx.blocking_recv() {
        match request {
            Request::RegisterInstrument(instrument, reply) => {
                let _ = reply.send(
                    exchange
                        .register_instrument(instrument.clone())
                        .map(|()| instrument),
                );
            }
            Request::RegisterParticipant(participant, reply) => {
                let _ = reply.send(
                    exchange
                        .register_participant(participant.clone())
                        .map(|()| participant),
                );
            }
            Request::Submit(order, reply) => {
                let result = exchange.submit(order).map(|outcome| {
                    let instrument_id = match &outcome {
                        SubmitOutcome::Processed(update) => &update.order.instrument_id,
                        SubmitOutcome::Duplicate(order) => &order.instrument_id,
                    };
                    let instrument = instrument_of(&exchange, instrument_id);
                    (outcome, instrument)
                });
                let _ = reply.send(result);
            }
            Request::Cancel(order_id, reply) => {
                let result = exchange.cancel(order_id).map(|update| {
                    let instrument = instrument_of(&exchange, &update.order.instrument_id);
                    (update, instrument)
                });
                let _ = reply.send(result);
            }
            Request::Snapshot(instrument_id, reply) => {
                let _ = reply.send(exchange.book(&instrument_id).map(|book| BookSnapshot {
                    instrument: book.instrument().clone(),
                    bids: book.depth(Side::Buy),
                    asks: book.depth(Side::Sell),
                    last_sequence: exchange.last_sequence(),
                }));
            }
        }
    }
    tracing::info!("exchange thread stopped");
}

fn instrument_of(exchange: &Exchange, id: &InstrumentId) -> Instrument {
    exchange
        .book(id)
        .expect("orders reference registered instruments")
        .instrument()
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_full_queue_fails_fast_and_a_stopped_thread_reports_stopped() {
        let (tx, rx) = mpsc::channel(1);
        let dispatcher = Dispatcher { tx };
        // Nothing consumes the queue, so the first request fills it.
        let first = tokio::spawn({
            let dispatcher = dispatcher.clone();
            async move { dispatcher.snapshot(InstrumentId("ACME".into())).await }
        });
        tokio::task::yield_now().await;
        while dispatcher.tx.capacity() > 0 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            dispatcher.snapshot(InstrumentId("ACME".into())).await,
            Err(DispatchError::Overloaded)
        ));

        drop(rx);
        assert!(
            matches!(first.await.unwrap(), Err(DispatchError::Stopped)),
            "queued request is abandoned"
        );
        assert!(matches!(
            dispatcher.snapshot(InstrumentId("ACME".into())).await,
            Err(DispatchError::Stopped)
        ));
    }
}
