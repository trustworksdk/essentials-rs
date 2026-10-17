use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::{
    BookUpdate, ClockChange, ClockState, Exchange, ExchangeError, Level, SubmitOrder, SubmitOutcome,
};
use crate::domain::{Instrument, InstrumentId, Participant, Side};
use crate::settlement::{ExternalReport, NewReport};
use crate::webhooks::{
    DeliveryJob, DeliveryResult, FaultPolicy, NewSubscription, Subscription, WebhookChange,
};

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
    UpdateWebhook(Uuid, WebhookChange, Reply<Subscription>),
    DeleteWebhook(Uuid, Reply<()>),
    RotateWebhookSecret(Uuid, String, chrono::Duration, Reply<Subscription>),
    Redeliver(Uuid, u64, Reply<usize>),
    RetryDeadLetters(Option<Uuid>, Vec<Uuid>, Reply<usize>),
    Reset(Reply<()>),
    SetInstrumentEnabled(InstrumentId, bool, Reply<Instrument>),
    ClockState(oneshot::Sender<ClockState>),
    Tick(Reply<usize>),
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
            Self::UpdateWebhook(..) => "update_webhook",
            Self::DeleteWebhook(..) => "delete_webhook",
            Self::RotateWebhookSecret(..) => "rotate_webhook_secret",
            Self::Redeliver(..) => "redeliver",
            Self::RetryDeadLetters(..) => "retry_dead_letters",
            Self::Reset(..) => "reset",
            Self::SetInstrumentEnabled(..) => "set_instrument_enabled",
            Self::ClockState(..) => "clock_state",
            Self::Tick(..) => "tick",
            Self::ReportSettlement(..) => "report_settlement",
            Self::ChangeClock(..) => "change_clock",
            Self::ClaimDeliveries(..) => "claim_deliveries",
            Self::RecordDeliveries(..) => "record_deliveries",
        }
    }
}

impl Request {
    /// Whether the request joins a commit group: orders and cancellations do; anything else ends the group first.
    fn groups(&self) -> bool {
        matches!(self, Self::Submit(..) | Self::Cancel(..))
    }
}

/// How the exchange thread groups orders and cancellations into one commit.
///
/// Once it takes an order or a cancellation, the thread opens a group and keeps taking queued orders and
/// cancellations, up to `max_commands`, and while the queue is empty, until `max_wait` has passed since the group
/// opened. Any other request ends the group, and runs after it. Every command is planned and written in queue order,
/// seeing the effects of those before it, and every caller is answered only once the group is committed. Grouping
/// therefore changes neither the outcomes nor the event log, only how many commands share one durable commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupCommit {
    pub max_commands: usize,
    pub max_wait: Duration,
}

impl GroupCommit {
    /// Every command committed on its own, as before group commit.
    pub const OFF: Self = Self {
        max_commands: 1,
        max_wait: Duration::ZERO,
    };
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
    pub fn spawn(
        exchange: Exchange,
        capacity: usize,
        grouping: GroupCommit,
    ) -> std::io::Result<(Self, JoinHandle<()>)> {
        let (tx, rx) = mpsc::channel(capacity);
        let handle = std::thread::Builder::new()
            .name("exchange".into())
            .spawn(move || run(exchange, rx, grouping))?;
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

    /// Runs everything due on domain time (settlement transitions, clearing statements); resolves with the number
    /// of events.
    pub async fn tick(&self) -> Result<usize, DispatchError> {
        self.call(Request::Tick).await
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

    pub async fn update_webhook(
        &self,
        id: Uuid,
        change: WebhookChange,
    ) -> Result<Subscription, DispatchError> {
        self.call(|reply| Request::UpdateWebhook(id, change, reply))
            .await
    }

    pub async fn delete_webhook(&self, id: Uuid) -> Result<(), DispatchError> {
        self.call(|reply| Request::DeleteWebhook(id, reply)).await
    }

    pub async fn rotate_webhook_secret(
        &self,
        id: Uuid,
        secret: String,
        grace: chrono::Duration,
    ) -> Result<Subscription, DispatchError> {
        self.call(|reply| Request::RotateWebhookSecret(id, secret, grace, reply))
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

fn run(mut exchange: Exchange, mut rx: mpsc::Receiver<Queued>, grouping: GroupCommit) {
    let mut next = None;
    loop {
        let Some(queued) = next.take().or_else(|| rx.blocking_recv()) else {
            break;
        };
        if grouping.max_commands > 1 && queued.request.groups() {
            next = run_group(&mut exchange, &mut rx, queued, grouping);
        } else {
            let (command, started) = start(&queued, &rx);
            let outcome = handle(&mut exchange, queued.request);
            finish(command, started, outcome);
        }
        exchange.store().request_checkpoint_if_due();
    }
    tracing::info!("exchange thread stopped");
}

/// Records the queue metrics for a request the thread is about to run; returns its name and start time.
fn start(queued: &Queued, rx: &mpsc::Receiver<Queued>) -> (&'static str, Instant) {
    use crate::metrics::{QUEUE_DEPTH, QUEUE_WAIT};
    ::metrics::gauge!(QUEUE_DEPTH).set(rx.len() as f64);
    let started = Instant::now();
    let command = queued.request.name();
    ::metrics::histogram!(QUEUE_WAIT, "command" => command).record(started - queued.enqueued);
    (command, started)
}

fn finish(command: &'static str, started: Instant, outcome: &'static str) {
    use crate::metrics::{COMMAND_DURATION, COMMANDS};
    ::metrics::histogram!(COMMAND_DURATION, "command" => command).record(started.elapsed());
    ::metrics::counter!(COMMANDS, "command" => command, "outcome" => outcome).increment(1);
}

/// An answer held back until its group is committed.
enum Held {
    Submit(
        Reply<(SubmitOutcome, Instrument)>,
        Result<(SubmitOutcome, Instrument), ExchangeError>,
    ),
    Cancel(
        Reply<(BookUpdate, Instrument)>,
        Result<(BookUpdate, Instrument), ExchangeError>,
    ),
}

impl Held {
    /// Answers the caller: with the command's own result if the group was committed, else with the group's failure.
    fn answer(self, committed: &Result<(), String>) -> &'static str {
        fn send<T>(
            reply: Reply<T>,
            result: Result<T, ExchangeError>,
            committed: &Result<(), String>,
        ) -> &'static str {
            let result = match committed {
                Ok(()) => result,
                Err(error) => Err(ExchangeError::GroupCommit(error.clone())),
            };
            answer(reply, result)
        }
        match self {
            Self::Submit(reply, result) => send(reply, result, committed),
            Self::Cancel(reply, result) => send(reply, result, committed),
        }
    }
}

/// Runs a commit group starting with `first` (see [`GroupCommit`]). Returns the request that ended the group, if
/// any, for the caller to run next.
fn run_group(
    exchange: &mut Exchange,
    rx: &mut mpsc::Receiver<Queued>,
    first: Queued,
    grouping: GroupCommit,
) -> Option<Queued> {
    let opened = Instant::now();
    if let Err(error) = exchange.begin_group() {
        let (command, started) = start(&first, rx);
        let outcome = match first.request {
            Request::Submit(_, reply) => answer(reply, Err(error)),
            Request::Cancel(_, reply) => answer(reply, Err(error)),
            _ => unreachable!("only orders and cancellations open a group"),
        };
        finish(command, started, outcome);
        return None;
    }
    let mut held = Vec::new();
    let mut queued = first;
    let ended_by = loop {
        let (command, started) = start(&queued, rx);
        let answer = match queued.request {
            Request::Submit(order, reply) => Held::Submit(reply, submit(exchange, order)),
            Request::Cancel(order_id, reply) => Held::Cancel(reply, cancel(exchange, order_id)),
            _ => unreachable!("only orders and cancellations join a group"),
        };
        ::metrics::histogram!(crate::metrics::COMMAND_DURATION, "command" => command)
            .record(started.elapsed());
        held.push((command, answer));
        if held.len() >= grouping.max_commands {
            break None;
        }
        match next_in_group(rx, opened + grouping.max_wait) {
            Some(next) if next.request.groups() => queued = next,
            other => break other,
        }
    };
    let size = held.len();
    let committed = exchange.commit_group().map_err(|e| e.to_string());
    ::metrics::histogram!(crate::metrics::GROUP_SIZE).record(size as f64);
    for (command, answer) in held {
        let outcome = answer.answer(&committed);
        ::metrics::counter!(crate::metrics::COMMANDS, "command" => command, "outcome" => outcome)
            .increment(1);
    }
    ended_by
}

/// The next queued request, waiting while the queue is empty until `deadline`.
fn next_in_group(rx: &mut mpsc::Receiver<Queued>, deadline: Instant) -> Option<Queued> {
    loop {
        match rx.try_recv() {
            Ok(queued) => return Some(queued),
            Err(mpsc::error::TryRecvError::Disconnected) => return None,
            Err(mpsc::error::TryRecvError::Empty) => {
                let now = Instant::now();
                if now >= deadline {
                    return None;
                }
                std::thread::sleep((deadline - now).min(Duration::from_micros(20)));
            }
        }
    }
}

/// Executes one request and answers it; returns the outcome for metrics.
///
/// A reply fails only when the caller gave up waiting (e.g. the HTTP client disconnected). The command still took
/// effect; there is no one left to tell.
fn handle(exchange: &mut Exchange, request: Request) -> &'static str {
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
        Request::Submit(order, reply) => answer(reply, submit(exchange, order)),
        Request::Cancel(order_id, reply) => answer(reply, cancel(exchange, order_id)),
        Request::Tick(reply) => answer(reply, exchange.tick()),
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
        Request::UpdateWebhook(id, change, reply) => {
            answer(reply, exchange.update_webhook(id, change))
        }
        Request::DeleteWebhook(id, reply) => answer(reply, exchange.delete_webhook(id)),
        Request::RotateWebhookSecret(id, secret, grace, reply) => {
            answer(reply, exchange.rotate_webhook_secret(id, secret, grace))
        }
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

/// Sends the result to the caller; returns the outcome for metrics.
fn answer<T>(reply: Reply<T>, result: Result<T, ExchangeError>) -> &'static str {
    let outcome = match &result {
        Ok(_) => "ok",
        Err(ExchangeError::Book(super::BookError::Invalid(_))) => "invalid",
        Err(ExchangeError::Storage(_) | ExchangeError::Book(_) | ExchangeError::GroupCommit(_)) => {
            "error"
        }
        Err(_) => "invalid",
    };
    let _ = reply.send(result);
    outcome
}

fn submit(
    exchange: &mut Exchange,
    order: SubmitOrder,
) -> Result<(SubmitOutcome, Instrument), ExchangeError> {
    exchange.submit(order).map(|outcome| {
        let instrument_id = match &outcome {
            SubmitOutcome::Processed(update) => &update.order.instrument_id,
            SubmitOutcome::Duplicate(order) => &order.instrument_id,
        };
        let instrument = instrument_of(exchange, instrument_id);
        (outcome, instrument)
    })
}

fn cancel(
    exchange: &mut Exchange,
    order_id: Uuid,
) -> Result<(BookUpdate, Instrument), ExchangeError> {
    exchange.cancel(order_id).map(|update| {
        let instrument = instrument_of(exchange, &update.order.instrument_id);
        (update, instrument)
    })
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
        let thread = std::thread::spawn(move || run(exchange, rx, GroupCommit::OFF));
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

    mod grouping {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use chrono::TimeZone;

        use super::*;
        use crate::domain::{OrderType, ParticipantId};
        use crate::engine::{ExchangeOptions, FixedClock, IdScheme};
        use crate::persistence::Store;

        const IDS: IdScheme = IdScheme::Deterministic { seed: 7 };

        /// A database file in its own directory, removed on drop.
        struct TempDb(std::path::PathBuf);

        impl TempDb {
            fn new() -> Self {
                Self(std::env::temp_dir().join(format!("exchange-group-{}", Uuid::now_v7())))
            }
            fn path(&self) -> std::path::PathBuf {
                self.0.join("exchange.db")
            }
            fn count(&self, sql: &str) -> i64 {
                rusqlite::Connection::open(self.path())
                    .unwrap()
                    .query_row(sql, [], |r| r.get(0))
                    .unwrap()
            }
        }

        impl Drop for TempDb {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        fn acme() -> Instrument {
            Instrument {
                id: InstrumentId("ACME".into()),
                symbol: "ACME".into(),
                mic: Some("XCSE".into()),
                isin: None,
                cusip: None,
                currency: "DKK".into(),
                currency_scale: 2,
                tick_size_minor: 1,
                lot_size: 1,
                enabled: true,
            }
        }

        /// An exchange with deterministic IDs and a fixed clock, ACME and participants P0 to P4 registered, and a
        /// commit hook counting commits from here on.
        fn exchange(db: &TempDb) -> (Exchange, Arc<AtomicUsize>) {
            let conn =
                crate::persistence::open(db.path(), std::time::Duration::from_secs(5)).unwrap();
            let mut exchange = Exchange::recover(
                Store::new(conn),
                ExchangeOptions {
                    ids: IDS,
                    clock: Box::new(FixedClock(
                        chrono::Utc
                            .with_ymd_and_hms(2026, 10, 16, 12, 0, 0)
                            .unwrap(),
                    )),
                    ..Default::default()
                },
            )
            .unwrap();
            exchange.register_instrument(acme()).unwrap();
            for i in 0..5 {
                exchange
                    .register_participant(Participant {
                        id: ParticipantId(format!("P{i}")),
                        name: format!("P{i}"),
                        settlement: None,
                    })
                    .unwrap();
            }
            let commits = Arc::new(AtomicUsize::new(0));
            let counter = commits.clone();
            exchange
                .store()
                .connection()
                .commit_hook(Some(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    false
                }))
                .unwrap();
            (exchange, commits)
        }

        fn order(i: u64, quantity: u64) -> SubmitOrder {
            SubmitOrder {
                client_order_id: None,
                instrument_id: InstrumentId("ACME".into()),
                participant_id: ParticipantId(format!("P{}", i % 5)),
                side: if i.is_multiple_of(2) {
                    Side::Buy
                } else {
                    Side::Sell
                },
                order_type: OrderType::Limit,
                limit_price_ticks: Some(10_000 + (i * 7 % 11) as i64 - 5),
                quantity,
            }
        }

        /// Starts `request` and waits until it is on the queue, so requests queue in the order they are started.
        /// The exchange thread must not be running yet.
        async fn queue<T: Send + 'static>(
            dispatcher: &Dispatcher,
            request: impl Future<Output = T> + Send + 'static,
        ) -> tokio::task::JoinHandle<T> {
            let before = dispatcher.tx.capacity();
            let handle = tokio::spawn(request);
            while dispatcher.tx.capacity() == before {
                tokio::task::yield_now().await;
            }
            handle
        }

        /// Queues 300 orders and 30 cancellations of resting orders, then runs them; returns every answer (as
        /// text), the event log, and the number of commits.
        async fn script(grouping: GroupCommit) -> (Vec<String>, Vec<String>, usize) {
            let db = TempDb::new();
            let (exchange, commits) = exchange(&db);
            let (tx, rx) = mpsc::channel(1024);
            let dispatcher = Dispatcher { tx };
            let mut answers = Vec::new();
            for i in 0..330u64 {
                let d = dispatcher.clone();
                answers.push(if i % 11 == 10 {
                    // A cancellation of an order queued earlier; it may have traded away, and then fails.
                    let id = IDS.order(i - 5);
                    queue(
                        &dispatcher,
                        async move { format!("{:?}", d.cancel(id).await) },
                    )
                    .await
                } else {
                    queue(&dispatcher, async move {
                        format!("{:?}", d.submit(order(i, 10 + i % 40)).await)
                    })
                    .await
                });
            }
            commits.store(0, Ordering::SeqCst);
            let thread = std::thread::spawn(move || run(exchange, rx, grouping));
            let mut replies = Vec::new();
            for answer in answers {
                replies.push(answer.await.unwrap());
            }
            drop(dispatcher);
            thread.join().unwrap();
            let conn = rusqlite::Connection::open(db.path()).unwrap();
            let events = conn
                .prepare(
                    "SELECT sequence, event_id, event_type, payload FROM events ORDER BY sequence",
                )
                .unwrap()
                .query_map([], |r| {
                    Ok(format!(
                        "{} {} {} {}",
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            (replies, events, commits.load(Ordering::SeqCst))
        }

        #[tokio::test]
        async fn grouping_changes_only_the_number_of_commits() {
            let (alone, alone_log, alone_commits) = script(GroupCommit::OFF).await;
            let grouping = GroupCommit {
                max_commands: 64,
                max_wait: std::time::Duration::ZERO,
            };
            let (grouped, grouped_log, grouped_commits) = script(grouping).await;
            assert_eq!(
                alone_commits,
                330 - alone.iter().filter(|r| r.starts_with("Err")).count()
            );
            assert_eq!(
                grouped_commits, 6,
                "330 queued commands in groups of at most 64"
            );
            assert!(
                alone.iter().any(|r| r.contains("executions: [Execution")),
                "orders trade"
            );
            assert!(
                alone
                    .iter()
                    .any(|r| r.starts_with("Ok") && r.contains("Cancelled")),
                "cancellations land"
            );
            assert!(
                alone.iter().any(|r| r.contains("OrderNotActive")),
                "some cancellations fail"
            );
            assert_eq!(grouped, alone, "every answer is the same");
            assert_eq!(
                grouped_log, alone_log,
                "the event log is the same, byte for byte"
            );
        }

        #[tokio::test]
        async fn callers_are_answered_only_after_their_group_is_committed() {
            let db = TempDb::new();
            let (exchange, _) = exchange(&db);
            // Each commit takes 30 ms before it is durable, so an early answer would see nothing stored.
            exchange
                .store()
                .connection()
                .commit_hook(Some(|| {
                    std::thread::sleep(std::time::Duration::from_millis(30));
                    false
                }))
                .unwrap();
            let (tx, rx) = mpsc::channel(1024);
            let dispatcher = Dispatcher { tx };
            let mut answers = Vec::new();
            for i in 0..20 {
                let d = dispatcher.clone();
                let path = db.path();
                answers.push(
                    queue(&dispatcher, async move {
                        let (outcome, _) = d.submit(order(i, 10)).await.unwrap();
                        let SubmitOutcome::Processed(update) = outcome else {
                            panic!("not a duplicate")
                        };
                        rusqlite::Connection::open(path)
                            .unwrap()
                            .query_row(
                                "SELECT COUNT(*) FROM orders WHERE id = ?1",
                                [update.order.id.to_string()],
                                |r| r.get::<_, i64>(0),
                            )
                            .unwrap()
                    })
                    .await,
                );
            }
            let grouping = GroupCommit {
                max_commands: 8,
                max_wait: std::time::Duration::ZERO,
            };
            let thread = std::thread::spawn(move || run(exchange, rx, grouping));
            for answer in answers {
                assert_eq!(
                    answer.await.unwrap(),
                    1,
                    "the order is stored when its caller hears back"
                );
            }
            drop(dispatcher);
            thread.join().unwrap();
        }

        #[tokio::test]
        async fn a_failed_group_answers_every_caller_with_the_error_and_other_requests_end_a_group()
        {
            let db = TempDb::new();
            let (exchange, _) = exchange(&db);
            exchange
                .store()
                .connection()
                .execute_batch(
                    "CREATE TEMP TABLE fail_parent (id INTEGER PRIMARY KEY);
                     CREATE TEMP TABLE fail_child (parent INTEGER REFERENCES fail_parent (id) DEFERRABLE INITIALLY DEFERRED);
                     CREATE TEMP TRIGGER fail_commit AFTER INSERT ON main.orders WHEN NEW.quantity = 7
                     BEGIN INSERT INTO fail_child VALUES (1); END;",
                )
                .unwrap();
            let (tx, rx) = mpsc::channel(1024);
            let dispatcher = Dispatcher { tx };
            let d = dispatcher.clone();
            let rests = queue(&dispatcher, async move { d.submit(order(1, 10)).await }).await;
            let d = dispatcher.clone();
            let fails = queue(&dispatcher, async move { d.submit(order(3, 7)).await }).await;
            let d = dispatcher.clone();
            let snapshot = queue(&dispatcher, async move {
                d.snapshot(InstrumentId("ACME".into())).await
            })
            .await;
            let d = dispatcher.clone();
            let after = queue(&dispatcher, async move { d.submit(order(5, 5)).await }).await;
            let grouping = GroupCommit {
                max_commands: 64,
                max_wait: std::time::Duration::ZERO,
            };
            let thread = std::thread::spawn(move || run(exchange, rx, grouping));

            for answer in [rests.await.unwrap(), fails.await.unwrap()] {
                assert!(
                    matches!(
                        answer,
                        Err(DispatchError::Exchange(ExchangeError::GroupCommit(_)))
                    ),
                    "{answer:?}"
                );
            }
            let snapshot = snapshot.await.unwrap().unwrap().unwrap();
            assert!(
                snapshot.bids.is_empty() && snapshot.asks.is_empty(),
                "the snapshot ended the group and saw none of it: {snapshot:?}"
            );
            assert_eq!(snapshot.last_sequence, 0);
            let (outcome, _) = after.await.unwrap().unwrap();
            let SubmitOutcome::Processed(update) = outcome else {
                panic!("not a duplicate")
            };
            assert_eq!(
                update.order.sequence, 1,
                "the failed group consumed no sequence"
            );
            drop(dispatcher);
            thread.join().unwrap();
            assert_eq!(db.count("SELECT COUNT(*) FROM orders"), 1);
        }
    }
}
