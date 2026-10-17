//! Background WAL checkpointing. SQLite's automatic checkpoint runs inside the commit that crosses its threshold, so
//! that commit (and every command queued behind it) waits for the database file to be written and synced. Here the
//! writer only records how large the WAL has grown, and a checkpointer thread with its own connection copies the WAL
//! back into the database file with `PRAGMA wal_checkpoint(PASSIVE)`, alongside the writer's commits.
//!
//! A checkpoint that runs alongside commits rarely lets the WAL restart: the WAL restarts from its beginning only after
//! a checkpoint has copied every frame, and under load a commit lands first and adds frames that were not copied. So:
//! - The writer asks for a background checkpoint each time the WAL has grown by the threshold. These do the bulk of
//!   the copying.
//! - Once the WAL holds [`WRITER_FACTOR`] times the threshold, the writer runs one checkpoint itself, as soon as a
//!   background checkpoint has left at most a quarter of the threshold to copy, or regardless at twice that size.
//!   Nothing appends while it runs, so it completes, and the WAL restarts at the next commit.
//!
//! The WAL thus stays near [`WRITER_FACTOR`] times the threshold (at most twice that), and the writer's own
//! checkpoints copy little.

use std::cell::Cell;
use std::ffi::c_int;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rusqlite::Connection;

thread_local! {
    /// Frames in the WAL after the last commit on this thread, as reported by [`note_wal_frames`].
    static WAL_FRAMES: Cell<c_int> = const { Cell::new(0) };
}

/// The writer's WAL hook. Registering any WAL hook turns off SQLite's automatic checkpoint for the connection; this one
/// only records the WAL size. It runs inside the commit on the committing thread, so that thread reads it right after.
fn note_wal_frames(_wal: &rusqlite::hooks::Wal, frames: c_int) -> rusqlite::Result<()> {
    WAL_FRAMES.with(|f| f.set(frames));
    Ok(())
}

/// Frames in the WAL after the last commit on this thread by a writer with background checkpointing.
pub(crate) fn wal_frames() -> c_int {
    WAL_FRAMES.with(Cell::get)
}

/// The writer checkpoints itself once the WAL holds this many times the threshold and little is left to copy.
pub const WRITER_FACTOR: c_int = 4;

#[derive(Default)]
struct State {
    requested: bool,
    stop: bool,
    /// The WAL generation (see [`Checkpointer::generation`]) of the latest request.
    generation: u64,
    /// The generation and WAL frames copied by the last background checkpoint that completed without error.
    copied: Option<(u64, i64)>,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    /// Background checkpoints run so far.
    runs: std::sync::atomic::AtomicU64,
}

/// A thread that checkpoints the WAL when asked. Requests made while a checkpoint runs are coalesced into one more.
/// Dropping it stops the thread after its current checkpoint.
pub struct Checkpointer {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
    /// The WAL size, in frames (pages), from which the writer asks for a checkpoint.
    pages: c_int,
    /// The WAL size at which to ask again: one request per `pages` of growth, so the thread does not checkpoint after
    /// every commit while it trails the writer.
    next_request: Cell<c_int>,
    /// The WAL size from which the writer checkpoints itself once little is left to copy.
    next_writer: Cell<c_int>,
    /// The WAL size from which the writer checkpoints itself regardless.
    writer_cap: Cell<c_int>,
    /// Counts WAL restarts, so a background result from an earlier WAL is not mistaken for progress on this one.
    generation: Cell<u64>,
    /// The WAL size after the previous write; a smaller one means the WAL restarted.
    last_frames: Cell<c_int>,
}

impl Checkpointer {
    /// Turns off automatic checkpoints on `writer` and starts a checkpointer for the database at `path`.
    pub fn start(
        writer: &Connection,
        path: &Path,
        busy_timeout: Duration,
        pages: u32,
    ) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        super::configure(&conn, busy_timeout)?;
        writer.wal_hook(Some(note_wal_frames));
        let shared = Arc::new(Shared::default());
        let thread = std::thread::Builder::new()
            .name("checkpoint".into())
            .spawn({
                let shared = shared.clone();
                move || run(&conn, &shared)
            })
            .map_err(|e| {
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                    Some(format!("starting the checkpoint thread: {e}")),
                )
            })?;
        let pages = c_int::try_from(pages.max(1)).unwrap_or(c_int::MAX);
        Ok(Self {
            shared,
            thread: Some(thread),
            pages,
            next_request: Cell::new(pages),
            next_writer: Cell::new(pages.saturating_mul(WRITER_FACTOR)),
            writer_cap: Cell::new(pages.saturating_mul(2 * WRITER_FACTOR)),
            generation: Cell::new(0),
            last_frames: Cell::new(0),
        })
    }

    /// Called on the writer's thread after it wrote (outside a transaction): asks for a background checkpoint each
    /// time the WAL has grown by `pages`, and checkpoints on `writer` itself once the WAL is [`WRITER_FACTOR`] times
    /// that and little is left to copy, or twice that regardless (see the module documentation).
    pub fn after_write(&self, writer: &Connection) {
        let frames = wal_frames();
        ::metrics::gauge!(crate::metrics::WAL_FRAMES).set(f64::from(frames));
        if frames < self.last_frames.get() {
            self.generation.set(self.generation.get() + 1);
            self.next_request.set(self.pages);
            self.next_writer
                .set(self.pages.saturating_mul(WRITER_FACTOR));
            self.writer_cap
                .set(self.pages.saturating_mul(2 * WRITER_FACTOR));
        }
        self.last_frames.set(frames);
        if frames >= self.next_writer.get() && self.writer_checkpoint_due(frames) {
            checkpoint(writer, "writer");
            // If a reader kept the checkpoint from finishing, try again after more growth rather than on every write.
            self.next_writer.set(frames.saturating_add(self.pages));
            self.writer_cap.set(frames.saturating_add(self.pages));
        } else if frames >= self.next_request.get() {
            self.request();
            self.next_request.set(frames.saturating_add(self.pages));
        }
    }

    /// Whether the writer should checkpoint now: a background checkpoint on this WAL left at most a quarter of the
    /// threshold to copy, or the WAL has reached the cap.
    fn writer_checkpoint_due(&self, frames: c_int) -> bool {
        if frames >= self.writer_cap.get() {
            return true;
        }
        let copied = self.shared.state.lock().expect("not poisoned").copied;
        copied.is_some_and(|(generation, copied)| {
            generation == self.generation.get()
                && i64::from(frames) - copied <= i64::from(self.pages / 4)
        })
    }

    fn request(&self) {
        let mut state = self.shared.state.lock().expect("not poisoned");
        state.requested = true;
        state.generation = self.generation.get();
        drop(state);
        self.shared.wake.notify_one();
    }
}

/// For tests of a store that uses checkpoints.
impl Checkpointer {
    /// Stops the background thread, as if it had fallen behind for good; requests then do nothing.
    #[doc(hidden)]
    pub fn stop_for_test(&mut self) {
        self.shared.state.lock().expect("not poisoned").stop = true;
        self.shared.wake.notify_one();
        if let Some(thread) = self.thread.take() {
            thread.join().expect("checkpoint thread");
        }
    }

    /// Background checkpoints run so far.
    #[doc(hidden)]
    pub fn runs(&self) -> u64 {
        self.shared.runs.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        self.shared.state.lock().expect("not poisoned").stop = true;
        self.shared.wake.notify_one();
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            tracing::error!("checkpoint thread panicked");
        }
    }
}

/// Runs a PASSIVE checkpoint on `conn` and records it, labelled `by` (`background` or `writer`). Returns how many WAL
/// frames have been copied, if it ran.
pub(crate) fn checkpoint(conn: &Connection, by: &'static str) -> Option<i64> {
    use crate::metrics::{CHECKPOINT_DURATION, CHECKPOINTS};
    let started = Instant::now();
    // PASSIVE never blocks the writer or readers. It copies what it can; frames a reader still needs stay in the
    // WAL until a later checkpoint. Once a checkpoint has copied everything, the writer's next commit restarts the
    // WAL from its beginning, so the file stops growing.
    let result = conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
        ))
    });
    ::metrics::histogram!(CHECKPOINT_DURATION, "by" => by).record(started.elapsed());
    let outcome = match &result {
        Ok((0, log, copied)) if copied >= log => "complete",
        Ok(_) => "partial",
        Err(error) => {
            tracing::warn!(%error, by, "WAL checkpoint failed; retrying on the next request");
            "error"
        }
    };
    ::metrics::counter!(CHECKPOINTS, "by" => by, "outcome" => outcome).increment(1);
    result.ok().map(|(_, _, copied)| copied)
}

fn run(conn: &Connection, shared: &Shared) {
    loop {
        let generation = {
            let mut state = shared
                .wake
                .wait_while(shared.state.lock().expect("not poisoned"), |s| {
                    !s.requested && !s.stop
                })
                .expect("not poisoned");
            if state.stop {
                break;
            }
            state.requested = false;
            state.generation
        };
        let copied = checkpoint(conn, "background");
        shared
            .runs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(copied) = copied {
            // Tagged with the generation of the request: if the WAL restarted meanwhile, the writer ignores it.
            shared.state.lock().expect("not poisoned").copied = Some((generation, copied));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_writer_trusts_only_background_progress_on_the_current_wal() {
        let dir = std::env::temp_dir().join(format!("exchange-ckpt-{}", uuid::Uuid::now_v7()));
        let path = dir.join("exchange.db");
        let writer = super::super::open(&path, Duration::from_secs(5), &[]).unwrap();
        let mut checkpointer =
            Checkpointer::start(&writer, &path, Duration::from_secs(5), 100).unwrap();
        checkpointer.stop_for_test();
        // Drives `after_write` as if the last commit left `frames` in the WAL; true if the writer checkpointed.
        let write = |frames: c_int| {
            WAL_FRAMES.with(|f| f.set(frames));
            checkpointer.after_write(&writer);
            // A writer checkpoint moves the next one to `pages` beyond the current size.
            checkpointer.next_writer.get() == frames + 100
        };

        assert!(!write(390), "below 4 x 100 pages");
        // A background checkpoint copied almost everything on this WAL: the writer finishes the job.
        checkpointer.shared.state.lock().unwrap().copied = Some((0, 390));
        assert!(write(410), "little left to copy");

        // The WAL restarts. A result from the old WAL says nothing about this one, however large.
        assert!(!write(20));
        assert_eq!(checkpointer.generation.get(), 1);
        checkpointer.shared.state.lock().unwrap().copied = Some((0, 10_000));
        assert!(!write(410), "a stale result does not count");
        checkpointer.shared.state.lock().unwrap().copied = Some((1, 300));
        assert!(!write(420), "too much left to copy");
        assert!(write(800), "the cap: 8 x 100 pages");
        drop(checkpointer);
        drop(writer);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
