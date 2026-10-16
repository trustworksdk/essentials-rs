use std::thread::JoinHandle;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::{
    BookUpdate, ClockChange, ClockState, Exchange, ExchangeError, Level, SubmitOrder, SubmitOutcome,
};
use crate::domain::{Instrument, InstrumentId, Participant, Side};
use crate::settlement::{ExternalReport, NewReport};
use crate::webhooks::{DeliveryJob, DeliveryResult, FaultPolicy, NewSubscription, Subscription};

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
    RegisterWebhook(NewSubscription, Reply<Subscription>),
    SetWebhookFaults(Uuid, Option<FaultPolicy>, Reply<Subscription>),
    Redeliver(Uuid, u64, Reply<usize>),
    RetryDeadLetters(Option<Uuid>, Vec<Uuid>, Reply<usize>),
    Reset(Reply<()>),
    SetInstrumentEnabled(InstrumentId, bool, Reply<Instrument>),
    ClockState(oneshot::Sender<ClockState>),
    SettlementTick(Reply<usize>),
    ReportSettlement(NewReport, Reply<(ExternalReport, bool)>),
    ChangeClock(ClockChange, Reply<ClockState>),
    ClaimDeliveries(u32, std::time::Duration, Reply<Vec<DeliveryJob>>),
    RecordDeliveries(Vec<DeliveryResult>, Reply<()>),
}

impl Request {
    /// The `command` label in metrics.
    fn name(&self) -> &'static str {
        match self {
            Self::RegisterInstrument(..) => "register_instrument",
            Self::RegisterParticipant(..) => "register_participant",
            Self::Submit(..) => "submit",
            Self::Cancel(..) => "cancel",
            Self::Snapshot(..) => "snapshot",
            Self::RegisterWebhook(..) => "register_webhook",
            Self::SetWebhookFaults(..) => "set_webhook_faults",
            Self::Redeliver(..) => "redeliver",
            Self::RetryDeadLetters(..) => "retry_dead_letters",
            Self::Reset(..) => "reset",
            Self::SetInstrumentEnabled(..) => "set_instrument_enabled",
            Self::ClockState(..) => "clock_state",
            Self::SettlementTick(..) => "settlement_tick",
            Self::ReportSettlement(..) => "report_settlement",
            Self::ChangeClock(..) => "change_clock",
            Self::ClaimDeliveries(..) => "claim_deliveries",
            Self::RecordDeliveries(..) => "record_deliveries",
        }
    }
}

/// A request on the queue, stamped when it was enqueued so the exchange thread can measure how long it waited.
struct Queued {
    request: Request,
    enqueued: Instant,
}

/// Async handle to the single exchange thread. Commands are queued on a bounded channel and executed one at a time
/// by the thread that owns the [`Exchange`] (and with it the SQLite writer). A full queue fails fast with
/// [`DispatchError::Overloaded`] instead of buffering without limit or dropping commands silently.
#[derive(Clone)]
pub struct Dispatcher {
    tx: mpsc::Sender<Queued>,
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
        self.ask(|reply| Request::Snapshot(instrument_id, reply))
            .await
    }

    /// Runs one settlement simulator step; resolves with the number of transitions.
    pub async fn settlement_tick(&self) -> Result<usize, DispatchError> {
        self.call(Request::SettlementTick).await
    }

    /// Stores an externally reported settlement outcome; `true` when it repeats a stored report.
    pub async fn report_settlement(
        &self,
        report: NewReport,
    ) -> Result<(ExternalReport, bool), DispatchError> {
        self.call(|reply| Request::ReportSettlement(report, reply))
            .await
    }

    pub async fn clock_state(&self) -> Result<ClockState, DispatchError> {
        self.ask(Request::ClockState).await
    }

    pub async fn change_clock(&self, change: ClockChange) -> Result<ClockState, DispatchError> {
        self.call(|reply| Request::ChangeClock(change, reply)).await
    }

    pub async fn set_instrument_enabled(
        &self,
        id: InstrumentId,
        enabled: bool,
    ) -> Result<Instrument, DispatchError> {
        self.call(|reply| Request::SetInstrumentEnabled(id, enabled, reply))
            .await
    }

    pub async fn register_webhook(
        &self,
        request: NewSubscription,
    ) -> Result<Subscription, DispatchError> {
        self.call(|reply| Request::RegisterWebhook(request, reply))
            .await
    }

    pub async fn set_webhook_faults(
        &self,
        subscription_id: Uuid,
        policy: Option<FaultPolicy>,
    ) -> Result<Subscription, DispatchError> {
        self.call(|reply| Request::SetWebhookFaults(subscription_id, policy, reply))
            .await
    }

    /// Queues a subscription's events after sequence `after` for delivery again; returns how many.
    pub async fn redeliver(
        &self,
        subscription_id: Uuid,
        after: u64,
    ) -> Result<usize, DispatchError> {
        self.call(|reply| Request::Redeliver(subscription_id, after, reply))
            .await
    }

    /// Retries dead-lettered deliveries (all, one subscription's, or the listed ones); returns how many.
    pub async fn retry_dead_letters(
        &self,
        subscription_id: Option<Uuid>,
        delivery_ids: Vec<Uuid>,
    ) -> Result<usize, DispatchError> {
        self.call(|reply| Request::RetryDeadLetters(subscription_id, delivery_ids, reply))
            .await
    }

    /// Clears all state (see [`Exchange::reset`]).
    pub async fn reset(&self) -> Result<(), DispatchError> {
        self.call(Request::Reset).await
    }

    /// Claims up to `limit` due webhook deliveries, leased for `lease`.
    pub async fn claim_deliveries(
        &self,
        limit: u32,
        lease: std::time::Duration,
    ) -> Result<Vec<DeliveryJob>, DispatchError> {
        self.call(|reply| Request::ClaimDeliveries(limit, lease, reply))
            .await
    }

    pub async fn record_deliveries(
        &self,
        results: Vec<DeliveryResult>,
    ) -> Result<(), DispatchError> {
        self.call(|reply| Request::RecordDeliveries(results, reply))
            .await
    }

    async fn call<T>(&self, request: impl FnOnce(Reply<T>) -> Request) -> Result<T, DispatchError> {
        Ok(self.ask(request).await??)
    }

    /// Enqueues a request and waits for its answer. The acknowledgement latency (enqueue to answer received) is
    /// recorded whatever the answer; a refused request was never enqueued, and a caller that gives up waiting
    /// records nothing.
    async fn ask<T>(
        &self,
        request: impl FnOnce(oneshot::Sender<T>) -> Request,
    ) -> Result<T, DispatchError> {
        let (reply, response) = oneshot::channel();
        let request = request(reply);
        let command = request.name();
        let enqueued = self.enqueue(request)?;
        let answer = response.await.map_err(|_| DispatchError::Stopped)?;
        ::metrics::histogram!(crate::metrics::ACK_LATENCY, "command" => command)
            .record(enqueued.elapsed());
        Ok(answer)
    }

    fn enqueue(&self, request: Request) -> Result<Instant, DispatchError> {
        let enqueued = Instant::now();
        let queued = Queued { request, enqueued };
        self.tx
            .try_send(queued)
            .map(|()| enqueued)
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => {
                    ::metrics::counter!(crate::metrics::QUEUE_REJECTIONS).increment(1);
                    DispatchError::Overloaded
                }
                mpsc::error::TrySendError::Closed(_) => DispatchError::Stopped,
            })
    }
}

fn run(mut exchange: Exchange, mut rx: mpsc::Receiver<Queued>) {
    use crate::metrics::{COMMAND_DURATION, COMMANDS, QUEUE_DEPTH, QUEUE_WAIT};
    while let Some(Queued { request, enqueued }) = rx.blocking_recv() {
        ::metrics::gauge!(QUEUE_DEPTH).set(rx.len() as f64);
        let started = Instant::now();
        let command = request.name();
        ::metrics::histogram!(QUEUE_WAIT, "command" => command).record(started - enqueued);
        let outcome = handle(&mut exchange, request);
        ::metrics::histogram!(COMMAND_DURATION, "command" => command).record(started.elapsed());
        ::metrics::counter!(COMMANDS, "command" => command, "outcome" => outcome).increment(1);
    }
    tracing::info!("exchange thread stopped");
}

/// Executes one request and answers it; returns the outcome for metrics.
///
/// A reply fails only when the caller gave up waiting (e.g. the HTTP client disconnected). The command still took
/// effect; there is no one left to tell.
fn handle(exchange: &mut Exchange, request: Request) -> &'static str {
    fn answer<T>(reply: Reply<T>, result: Result<T, ExchangeError>) -> &'static str {
        let outcome = match &result {
            Ok(_) => "ok",
            Err(ExchangeError::Storage(_) | ExchangeError::Book(_))
                if !is_invalid_order(&result) =>
            {
                "error"
            }
            Err(_) => "invalid",
        };
        let _ = reply.send(result);
        outcome
    }
    fn is_invalid_order<T>(result: &Result<T, ExchangeError>) -> bool {
        matches!(
            result,
            Err(ExchangeError::Book(super::BookError::Invalid(_)))
        )
    }

    match request {
        Request::RegisterInstrument(instrument, reply) => {
            let result = exchange
                .register_instrument(instrument.clone())
                .map(|()| instrument);
            answer(reply, result)
        }
        Request::RegisterParticipant(participant, reply) => {
            let result = exchange
                .register_participant(participant.clone())
                .map(|()| participant);
            answer(reply, result)
        }
        Request::Submit(order, reply) => {
            let result = exchange.submit(order).map(|outcome| {
                let instrument_id = match &outcome {
                    SubmitOutcome::Processed(update) => &update.order.instrument_id,
                    SubmitOutcome::Duplicate(order) => &order.instrument_id,
                };
                let instrument = instrument_of(exchange, instrument_id);
                (outcome, instrument)
            });
            answer(reply, result)
        }
        Request::Cancel(order_id, reply) => {
            let result = exchange.cancel(order_id).map(|update| {
                let instrument = instrument_of(exchange, &update.order.instrument_id);
                (update, instrument)
            });
            answer(reply, result)
        }
        Request::SettlementTick(reply) => answer(reply, exchange.settlement_tick()),
        Request::ReportSettlement(report, reply) => {
            answer(reply, exchange.report_settlement(report))
        }
        Request::ClockState(reply) => {
            let _ = reply.send(exchange.clock_state());
            "ok"
        }
        Request::ChangeClock(change, reply) => answer(reply, exchange.change_clock(change)),
        Request::SetInstrumentEnabled(id, enabled, reply) => {
            answer(reply, exchange.set_instrument_enabled(&id, enabled))
        }
        Request::RegisterWebhook(request, reply) => {
            answer(reply, exchange.register_webhook(request))
        }
        Request::SetWebhookFaults(id, policy, reply) => {
            answer(reply, exchange.set_webhook_faults(id, policy))
        }
        Request::Redeliver(id, after, reply) => answer(reply, exchange.redeliver(id, after)),
        Request::RetryDeadLetters(subscription, ids, reply) => {
            answer(reply, exchange.retry_dead_letters(subscription, &ids))
        }
        Request::Reset(reply) => answer(reply, exchange.reset()),
        Request::ClaimDeliveries(limit, lease, reply) => {
            answer(reply, exchange.claim_deliveries(limit, lease))
        }
        Request::RecordDeliveries(results, reply) => {
            answer(reply, exchange.record_deliveries(&results))
        }
        Request::Snapshot(instrument_id, reply) => {
            let _ = reply.send(exchange.book(&instrument_id).map(|book| BookSnapshot {
                instrument: book.instrument().clone(),
                bids: book.depth(Side::Buy),
                asks: book.depth(Side::Sell),
                last_sequence: exchange.last_sequence(),
            }));
            "ok"
        }
    }
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

    /// The `_sum` and `_count` of a histogram series, as rendered by the process-wide recorder.
    fn histogram(name: &str, labels: &str) -> (f64, f64) {
        let text = crate::metrics::install().render();
        let value = |suffix: &str| {
            let series = format!("{name}_{suffix}{{{labels}}} ");
            text.lines()
                .find_map(|line| line.strip_prefix(&series)?.parse().ok())
                .unwrap_or(0.0)
        };
        (value("sum"), value("count"))
    }

    #[tokio::test]
    async fn queue_wait_and_acknowledgement_latency_are_measured_from_enqueueing() {
        use crate::metrics::{ACK_LATENCY, COMMAND_DURATION, QUEUE_WAIT};
        let labels = r#"command="clock_state""#;
        let before = [QUEUE_WAIT, ACK_LATENCY, COMMAND_DURATION].map(|m| histogram(m, labels));

        // The request sits in the queue for 100 ms before the exchange thread starts.
        let (tx, rx) = mpsc::channel(1);
        let dispatcher = Dispatcher { tx };
        let asked = tokio::spawn({
            let dispatcher = dispatcher.clone();
            async move { dispatcher.clock_state().await }
        });
        while dispatcher.tx.capacity() > 0 {
            tokio::task::yield_now().await;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        let conn = crate::persistence::open(":memory:", std::time::Duration::from_secs(1)).unwrap();
        let exchange =
            Exchange::recover(crate::persistence::Store::new(conn), Default::default()).unwrap();
        let thread = std::thread::spawn(move || run(exchange, rx));
        asked.await.unwrap().unwrap();
        drop(dispatcher);
        thread.join().unwrap();

        let after = [QUEUE_WAIT, ACK_LATENCY, COMMAND_DURATION].map(|m| histogram(m, labels));
        let delta = |i: usize| (after[i].0 - before[i].0, after[i].1 - before[i].1);
        let (wait, waits) = delta(0);
        let (ack, acks) = delta(1);
        let (duration, _) = delta(2);
        assert_eq!(
            (waits, acks),
            (1.0, 1.0),
            "one wait and one acknowledgement"
        );
        assert!(
            wait >= 0.1,
            "queue wait {wait} s covers the 100 ms in the queue"
        );
        assert!(
            ack >= wait + duration,
            "acknowledgement {ack} s covers the wait {wait} s and the execution {duration} s"
        );
    }
}
