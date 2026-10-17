//! SQLite plumbing: persistence modes, connection setup, schema migrations, background WAL checkpoints, and the
//! column conversions every store uses.

pub mod checkpoint;
pub mod sql;

use std::path::Path;
use std::time::Duration;

use rusqlite::Connection;

/// How much of a simulator's state survives what (`database.mode`).
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceMode {
    /// WAL with `synchronous=FULL`: every acknowledged command is on disk, and survives a host crash or power loss.
    #[default]
    Durable,
    /// WAL with `synchronous=NORMAL`: commits reach the database file but are synced only at checkpoints. Everything
    /// survives a process or container restart; a host crash or power loss can lose the last commits, but never
    /// corrupts the database.
    Fast,
    /// An in-memory database (SQLite's `memdb`): nothing survives the process.
    Memory,
}

impl PersistenceMode {
    pub const ALL: [Self; 3] = [Self::Durable, Self::Fast, Self::Memory];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Fast => "fast",
            Self::Memory => "memory",
        }
    }
}

/// A fresh, process-wide name for an in-memory database, starting with `name`. Every connection opened with it (by
/// [`open_mode`] and [`open_reader`]) shares the database, which disappears when the last of them closes.
pub fn memory_location(name: &str) -> String {
    format!("file:/{name}-{}?vfs=memdb", uuid::Uuid::now_v7())
}

/// Opens (creating if needed) the database at `location` for `mode`, applies connection pragmas and runs pending
/// `migrations`. `location` is a file path, or with [`PersistenceMode::Memory`] a name from [`memory_location`].
/// `memory_max_bytes` caps an in-memory database (SQLite's own default is 1 GiB).
pub fn open_mode(
    location: &str,
    mode: PersistenceMode,
    busy_timeout: Duration,
    memory_max_bytes: u64,
    migrations: &[&str],
) -> rusqlite::Result<Connection> {
    match mode {
        PersistenceMode::Durable => open(location, busy_timeout, migrations),
        PersistenceMode::Fast => {
            let conn = open(location, busy_timeout, migrations)?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            Ok(conn)
        }
        PersistenceMode::Memory => {
            let mut conn = Connection::open_with_flags(
                location,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                    | rusqlite::OpenFlags::SQLITE_OPEN_URI
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            configure(&conn, busy_timeout)?;
            // There is no disk to sync. memdb keeps its rollback journal in memory and has no WAL.
            conn.pragma_update(None, "synchronous", "OFF")?;
            set_memory_limit(&conn, memory_max_bytes)?;
            migrate(&mut conn, migrations)?;
            Ok(conn)
        }
    }
}

/// Raises (or lowers) the size limit of the in-memory database `conn` uses.
fn set_memory_limit(conn: &Connection, max_bytes: u64) -> rusqlite::Result<()> {
    let mut limit = i64::try_from(max_bytes).unwrap_or(i64::MAX);
    // SAFETY: `conn.handle()` is the live connection, `c"main"` a NUL-terminated schema name, and for
    // SQLITE_FCNTL_SIZE_LIMIT the argument must point to an `sqlite3_int64`, which `limit` is; SQLite only reads and
    // writes it during the call. rusqlite has no safe wrapper for this file control.
    let rc = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            conn.handle(),
            c"main".as_ptr(),
            rusqlite::ffi::SQLITE_FCNTL_SIZE_LIMIT,
            (&raw mut limit).cast(),
        )
    };
    if rc != rusqlite::ffi::SQLITE_OK {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rc),
            Some("setting the in-memory database size limit".into()),
        ));
    }
    Ok(())
}

/// Opens (creating if needed) the database file at `path` for durable use (WAL, `synchronous=FULL`), applies
/// connection pragmas and runs pending `migrations`.
pub fn open(
    path: impl AsRef<Path>,
    busy_timeout: Duration,
    migrations: &[&str],
) -> rusqlite::Result<Connection> {
    let path = path.as_ref();
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|e| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
                Some(e.to_string()),
            )
        })?;
    }
    let mut conn = Connection::open(path)?;
    configure(&conn, busy_timeout)?;
    let mode: String =
        conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        tracing::warn!(journal_mode = %mode, "SQLite did not switch to WAL");
    }
    conn.pragma_update(None, "synchronous", "FULL")?;
    migrate(&mut conn, migrations)?;
    Ok(conn)
}

/// Opens an extra read-only connection to a database [`open`] or [`open_mode`] already created. In WAL mode readers see
/// every committed transaction without blocking the writer. An in-memory database has no WAL: a reader there waits
/// while a write transaction is open, and the writer waits for a read to finish (up to the busy timeout).
pub fn open_reader(path: impl AsRef<Path>, busy_timeout: Duration) -> rusqlite::Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_URI
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    configure(&conn, busy_timeout)?;
    Ok(conn)
}

/// Per-connection settings. SQLite does not persist these, so every connection must apply them.
pub fn configure(conn: &Connection, busy_timeout: Duration) -> rusqlite::Result<()> {
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.busy_timeout(busy_timeout)
}

/// Applies every migration newer than the database's `user_version`, each in its own transaction. `migrations` is the
/// simulator's ordered list; the index + 1 is the version recorded in `PRAGMA user_version`.
pub fn migrate(conn: &mut Connection, migrations: &[&str]) -> rusqlite::Result<()> {
    let current = schema_version(conn)?;
    for (version, sql) in (1_u32..).zip(migrations).skip(current as usize) {
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", version)?;
        tx.commit()?;
        tracing::info!(version, "applied schema migration");
    }
    Ok(())
}

/// The schema version recorded in the database.
pub fn schema_version(conn: &Connection) -> rusqlite::Result<u32> {
    conn.pragma_query_value(None, "user_version", |row| row.get(0))
}
