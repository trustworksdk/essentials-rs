//! Prometheus metrics. A process-wide recorder is installed once by [`install`]; `GET /metrics` renders it. Without a
//! recorder (e.g. when the engine is used as a library) every recording is a no-op.

use std::sync::OnceLock;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

pub use simulator_kit::metrics::{
    CHECKPOINT_DURATION, CHECKPOINTS, OUTBOX, PERSISTENCE_MODE, WAL_FRAMES, WEBHOOK_ATTEMPTS,
    WEBHOOK_DURATION, set_persistence_mode,
};

/// Commands executed by the exchange thread. Labels: `command`, `outcome` (`ok`, `invalid`, `error`).
pub const COMMANDS: &str = "exchange_commands_total";
/// Time the exchange thread spent on one command, matching and commit included. Label: `command`.
pub const COMMAND_DURATION: &str = "exchange_command_duration_seconds";
/// Time a command waited in the queue before the exchange thread started it (enqueue to start). Label: `command`.
pub const QUEUE_WAIT: &str = "exchange_command_queue_wait_seconds";
/// Time from enqueueing a command until its caller received the answer (queue wait, execution and commit, and the
/// hand-back). Label: `command`.
pub const ACK_LATENCY: &str = "exchange_command_ack_seconds";
/// Time spent planning a match on the in-memory book (no I/O).
pub const MATCH_DURATION: &str = "exchange_match_duration_seconds";
/// Time spent making orders and cancellations durable: per commit group, the `COMMIT`; with group commit off, the
/// whole transaction of one command.
pub const COMMIT_DURATION: &str = "exchange_commit_duration_seconds";
/// Commands (orders and cancellations) per commit group.
pub const GROUP_SIZE: &str = "exchange_group_commit_commands";
/// Commands waiting for the exchange thread.
pub const QUEUE_DEPTH: &str = "exchange_command_queue_depth";
/// Commands refused because the queue was full.
pub const QUEUE_REJECTIONS: &str = "exchange_command_queue_rejections_total";
/// Committed orders by resulting status. Label: `status`.
pub const ORDERS: &str = "exchange_orders_total";
pub const EXECUTIONS: &str = "exchange_executions_total";
pub const TRADED_QUANTITY: &str = "exchange_traded_quantity_total";
/// Canonical events written. Label: `event_type`.
pub const EVENTS: &str = "exchange_events_total";
/// Orders the liquidity provider placed (each level of each ladder), whether or not its order events are published.
pub const LIQUIDITY_QUOTES: &str = "exchange_liquidity_quotes_total";
/// Ladders the liquidity provider replaced (or pulled). Label: `instrument`.
pub const LIQUIDITY_REQUOTES: &str = "exchange_liquidity_requotes_total";
/// Executions with the liquidity provider on one side. Label: `instrument`.
pub const LIQUIDITY_FILLS: &str = "exchange_liquidity_fills_total";
/// Settlement instructions with a transition still to come.
pub const SETTLEMENT_OPEN: &str = "settlement_open_instructions";

const GROUP_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0,
];

const LATENCY_BUCKETS: &[f64] = &[
    0.000_01, 0.000_025, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025,
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Installs the process-wide Prometheus recorder on first use and returns a handle to render it.
pub fn install() -> PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            let recorder = PrometheusBuilder::new()
                .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), LATENCY_BUCKETS)
                .expect("buckets are non-empty")
                .set_buckets_for_metric(Matcher::Full(GROUP_SIZE.into()), GROUP_BUCKETS)
                .expect("buckets are non-empty")
                .build_recorder();
            let handle = recorder.handle();
            if ::metrics::set_global_recorder(recorder).is_err() {
                tracing::warn!("a metrics recorder was already installed; /metrics will be empty");
            }
            describe();
            handle
        })
        .clone()
}

fn describe() {
    use ::metrics::{Unit, describe_counter, describe_gauge, describe_histogram};
    describe_counter!(COMMANDS, "Commands executed by the exchange thread");
    describe_histogram!(
        COMMAND_DURATION,
        Unit::Seconds,
        "Exchange thread time per command, commit included"
    );
    describe_histogram!(
        QUEUE_WAIT,
        Unit::Seconds,
        "Time a command waited in the queue before the exchange thread started it"
    );
    describe_histogram!(
        ACK_LATENCY,
        Unit::Seconds,
        "Time from enqueueing a command until its caller received the answer"
    );
    describe_histogram!(
        MATCH_DURATION,
        Unit::Seconds,
        "In-memory matching time per order"
    );
    describe_histogram!(
        COMMIT_DURATION,
        Unit::Seconds,
        "Time to make orders and cancellations durable, per commit group"
    );
    describe_histogram!(GROUP_SIZE, "Orders and cancellations per commit group");
    describe_counter!(CHECKPOINTS, "WAL checkpoints by who ran them and outcome");
    describe_histogram!(
        CHECKPOINT_DURATION,
        Unit::Seconds,
        "Duration of one WAL checkpoint"
    );
    describe_gauge!(
        WAL_FRAMES,
        "Frames in the WAL after the writer's last commit"
    );
    describe_gauge!(QUEUE_DEPTH, "Commands waiting for the exchange thread");
    describe_counter!(
        QUEUE_REJECTIONS,
        "Commands refused because the command queue was full"
    );
    describe_counter!(ORDERS, "Committed orders by resulting status");
    describe_counter!(EXECUTIONS, "Executions committed");
    describe_counter!(TRADED_QUANTITY, "Quantity traded");
    describe_counter!(EVENTS, "Canonical events written");
    describe_counter!(WEBHOOK_ATTEMPTS, "Webhook delivery attempts by outcome");
    describe_histogram!(
        WEBHOOK_DURATION,
        Unit::Seconds,
        "Webhook HTTP attempt duration"
    );
    describe_gauge!(OUTBOX, "Webhook deliveries by status");
    describe_gauge!(
        PERSISTENCE_MODE,
        "1 for the persistence mode the service runs in (durable, fast, memory)"
    );
    describe_gauge!(
        SETTLEMENT_OPEN,
        "Settlement instructions with a transition still to come"
    );
}
