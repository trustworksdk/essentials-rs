//! Names of the metrics the shared parts record. A simulator installs the Prometheus recorder and describes these
//! with its own. The checkpoint and persistence names keep the `exchange_` prefix they had before the extraction, so
//! the exchange's dashboards keep working.

use crate::persistence::PersistenceMode;

/// WAL checkpoints with background checkpointing. Labels: `by` (`background`, or `writer` when the background
/// checkpoints fell behind) and `outcome` (`complete`, `partial` when frames remained, `error`).
pub const CHECKPOINTS: &str = "exchange_wal_checkpoints_total";
/// Duration of one WAL checkpoint with background checkpointing. Label: `by`.
pub const CHECKPOINT_DURATION: &str = "exchange_wal_checkpoint_duration_seconds";
/// Frames (pages) in the WAL after the writer's last commit, with background checkpoints.
pub const WAL_FRAMES: &str = "exchange_wal_frames";
/// Webhook delivery attempts by outcome (`delivered`, `retry`, `dead`).
pub const WEBHOOK_ATTEMPTS: &str = "webhook_delivery_attempts_total";
/// Duration of one webhook HTTP attempt.
pub const WEBHOOK_DURATION: &str = "webhook_delivery_duration_seconds";
/// 1 for the persistence mode the service runs in, 0 for the others. Label: `mode`.
pub const PERSISTENCE_MODE: &str = "exchange_persistence_mode";
/// Outbox rows by status (`PENDING`, `IN_FLIGHT`, `DELIVERED`, `DEAD`), read from SQLite at scrape time.
pub const OUTBOX: &str = "webhook_outbox_deliveries";

/// Records the persistence mode the service runs in.
pub fn set_persistence_mode(mode: PersistenceMode) {
    for each in PersistenceMode::ALL {
        ::metrics::gauge!(PERSISTENCE_MODE, "mode" => each.as_str()).set(if each == mode {
            1.0
        } else {
            0.0
        });
    }
}
