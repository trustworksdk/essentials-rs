//! Prometheus metrics: the gateway's own, plus the shared ones the kit records (webhook delivery, persistence mode).

use std::sync::OnceLock;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

pub use simulator_kit::metrics::{
    OUTBOX, PERSISTENCE_MODE, WEBHOOK_ATTEMPTS, WEBHOOK_DURATION, set_persistence_mode,
};

/// Payments initiated. Label: `type` (`DEPOSIT`, `WITHDRAWAL`).
pub const PAYMENTS: &str = "gateway_payments_total";
/// Payment transitions by resulting status (`COMPLETED`, `FAILED`, `RETURNED`). Label: `status`.
pub const PAYMENT_OUTCOMES: &str = "gateway_payment_outcomes_total";
/// Canonical events written. Label: `event_type`.
pub const EVENTS: &str = "gateway_events_total";

const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Installs the process-wide Prometheus recorder on first use and returns a handle to render it.
pub fn install() -> PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            let recorder = PrometheusBuilder::new()
                .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), LATENCY_BUCKETS)
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
    describe_counter!(PAYMENTS, "Payments initiated");
    describe_counter!(PAYMENT_OUTCOMES, "Payment transitions by resulting status");
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
        "1 for the persistence mode the service runs in"
    );
}
