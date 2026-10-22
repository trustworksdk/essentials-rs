//! The gateway's tables: payments, bookings and bank statements. The event log, outbox and webhook tables are the
//! shared ones (`simulator_kit::outbox`).

use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, params};
use simulator_kit::events::NewEvent;
use simulator_kit::outbox::{Outbox, write_events};
use simulator_kit::persistence::sql::{from_sql, parsed, timestamp, to_sql};
use uuid::Uuid;

use crate::payments::{BankAccount, Booking, Payment};
use crate::statement::{BookedMovement, Draft, StoredStatement};

/// The gateway's migrations: the shared outbox schema, then its own tables.
pub const MIGRATIONS: &[&str] = &[
    simulator_kit::outbox::SCHEMA,
    include_str!("../migrations/002_payments.sql"),
    simulator_kit::outbox::PARTICIPANT_FILTER,
    include_str!("../migrations/004_forced_outcomes.sql"),
    include_str!("../migrations/005_value_dates.sql"),
];

const PAYMENT_COLUMNS: &str = "id, sequence, idempotency_key, type, account_id, iban, holder_name, amount_minor, \
    currency, currency_scale, reference, status, reason, created_at, due_at, completed_at, failed_at, returned_at, \
    updated_at, forced_outcome, value_date";

/// A text column holding one of the wire enums (type, status, kind).
fn wire<T: std::str::FromStr<Err = String>>(row: &Row<'_>, index: usize) -> rusqlite::Result<T> {
    let text: String = row.get(index)?;
    text.parse()
        .map_err(|e: String| rusqlite::Error::FromSqlConversionFailure(index, Type::Text, e.into()))
}

fn optional_time(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<DateTime<Utc>>> {
    row.get::<_, Option<String>>(index)?
        .map(|_| parsed(row, index))
        .transpose()
}

fn payment_row(row: &Row<'_>) -> rusqlite::Result<Payment> {
    Ok(Payment {
        id: parsed(row, 0)?,
        sequence: from_sql(row, 1)?,
        idempotency_key: row.get(2)?,
        payment_type: wire(row, 3)?,
        account_id: row.get(4)?,
        bank_account: BankAccount {
            iban: row.get(5)?,
            holder_name: row.get(6)?,
        },
        amount_minor: row.get(7)?,
        currency: row.get(8)?,
        currency_scale: row.get(9)?,
        reference: row.get(10)?,
        status: wire(row, 11)?,
        reason: row.get(12)?,
        created_at: parsed(row, 13)?,
        due_at: optional_time(row, 14)?,
        completed_at: optional_time(row, 15)?,
        failed_at: optional_time(row, 16)?,
        returned_at: optional_time(row, 17)?,
        updated_at: parsed(row, 18)?,
        forced: row
            .get::<_, Option<String>>(19)?
            .map(|json| {
                serde_json::from_str(&json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(19, Type::Text, Box::new(e))
                })
            })
            .transpose()?,
        value_date: row
            .get::<_, Option<String>>(20)?
            .map(|_| parsed(row, 20))
            .transpose()?,
    })
}

fn upsert_payment(conn: &Connection, p: &Payment) -> rusqlite::Result<()> {
    conn.prepare_cached(&format!(
        "INSERT INTO payments ({PAYMENT_COLUMNS})
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
         ON CONFLICT (id) DO UPDATE SET status = excluded.status, reason = excluded.reason, due_at = excluded.due_at,
           completed_at = excluded.completed_at, failed_at = excluded.failed_at, returned_at = excluded.returned_at,
           updated_at = excluded.updated_at, forced_outcome = excluded.forced_outcome,
           value_date = excluded.value_date"
    ))?
    .execute(params![
        p.id.to_string(),
        to_sql(p.sequence),
        p.idempotency_key,
        p.payment_type.as_str(),
        p.account_id,
        p.bank_account.iban,
        p.bank_account.holder_name,
        p.amount_minor,
        p.currency,
        p.currency_scale,
        p.reference,
        p.status.as_str(),
        p.reason,
        timestamp(p.created_at),
        p.due_at.map(timestamp),
        p.completed_at.map(timestamp),
        p.failed_at.map(timestamp),
        p.returned_at.map(timestamp),
        timestamp(p.updated_at),
        p.forced
            .as_ref()
            .map(|f| serde_json::to_string(f).expect("an outcome serializes")),
        p.value_date.map(|d| d.to_string()),
    ])
    .map(drop)
}

/// Stores the virtual clock's reading with its `ClockChanged` (and outbox rows), atomically.
pub fn commit_clock(
    conn: &mut Connection,
    now: DateTime<Utc>,
    running: bool,
    event: &NewEvent,
    outbox: Outbox<'_>,
) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    simulator_kit::outbox::save_virtual_clock(&tx, now, running)?;
    simulator_kit::outbox::write_events(&tx, std::slice::from_ref(event), &outbox)?;
    tx.commit()
}

/// Writes payments' new states, the bookings they make and their events (with outbox rows), atomically.
pub fn commit_payments(
    conn: &mut Connection,
    payments: &[&Payment],
    bookings: &[&Booking],
    events: &[NewEvent],
    outbox: Outbox<'_>,
) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    for payment in payments {
        upsert_payment(&tx, payment)?;
    }
    {
        let mut insert = tx.prepare_cached(
            "INSERT INTO bookings (payment_id, kind, amount_minor, currency, booked_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for b in bookings {
            insert.execute(params![
                b.payment_id.to_string(),
                b.kind.as_str(),
                b.amount_minor,
                b.currency,
                timestamp(b.booked_at),
            ])?;
        }
    }
    write_events(&tx, events, &outbox)?;
    tx.commit()
}

pub fn payment(conn: &Connection, id: Uuid) -> rusqlite::Result<Option<Payment>> {
    conn.prepare_cached(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE id = ?1"
    ))?
    .query_row([id.to_string()], payment_row)
    .optional()
}

pub fn payment_by_key(conn: &Connection, key: &str) -> rusqlite::Result<Option<Payment>> {
    conn.prepare_cached(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE idempotency_key = ?1"
    ))?
    .query_row([key], payment_row)
    .optional()
}

/// Payments in sequence order, optionally of one account and one status, after a sequence, at most `limit`.
pub fn payments(
    conn: &Connection,
    account_id: Option<&str>,
    status: Option<&str>,
    after: u64,
    limit: u32,
) -> rusqlite::Result<Vec<Payment>> {
    conn.prepare_cached(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments
         WHERE (?1 IS NULL OR account_id = ?1) AND (?2 IS NULL OR status = ?2) AND sequence > ?3
         ORDER BY sequence LIMIT ?4"
    ))?
    .query_map(
        params![account_id, status, to_sql(after), limit],
        payment_row,
    )?
    .collect()
}

/// Gives the completed and returned payments stored before payments carried value dates (migration 005) the value date
/// of their latest booking on `calendar`. Returns how many it dated.
pub fn date_undated_payments(
    conn: &Connection,
    calendar: &simulator_kit::calendar::CutoffCalendar,
) -> rusqlite::Result<usize> {
    let undated: Vec<(String, DateTime<Utc>)> = conn
        .prepare(
            "SELECT id, COALESCE(returned_at, completed_at) FROM payments
             WHERE value_date IS NULL AND completed_at IS NOT NULL",
        )?
        .query_map([], |row| Ok((row.get(0)?, parsed(row, 1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut update = conn.prepare("UPDATE payments SET value_date = ?2 WHERE id = ?1")?;
    for (id, booked_at) in &undated {
        update.execute(params![id, calendar.business_date(*booked_at).to_string()])?;
    }
    Ok(undated.len())
}

/// Payments with a transition still to come, for resuming the lifecycle.
pub fn open_payments(conn: &Connection) -> rusqlite::Result<Vec<Payment>> {
    conn.prepare_cached(&format!(
        "SELECT {PAYMENT_COLUMNS} FROM payments WHERE due_at IS NOT NULL ORDER BY due_at, sequence"
    ))?
    .query_map([], payment_row)?
    .collect()
}

/// The highest payment sequence so far, 0 if none.
pub fn last_payment_sequence(conn: &Connection) -> rusqlite::Result<u64> {
    conn.query_row(
        "SELECT COALESCE(MAX(sequence), 0) FROM payments",
        [],
        |row| from_sql(row, 0),
    )
}

/// The currencies with a booking before `until`: the firm's accounts that exist by then.
pub fn booked_currencies(conn: &Connection, until: DateTime<Utc>) -> rusqlite::Result<Vec<String>> {
    conn.prepare_cached(
        "SELECT DISTINCT currency FROM bookings WHERE booked_at < ?1 ORDER BY currency",
    )?
    .query_map([timestamp(until)], |row| row.get(0))?
    .collect()
}

/// The sum of a currency's bookings before `until`, in minor units.
pub fn booked_balance(
    conn: &Connection,
    currency: &str,
    until: DateTime<Utc>,
) -> rusqlite::Result<i64> {
    conn.query_row(
        "SELECT COALESCE(SUM(amount_minor), 0) FROM bookings WHERE currency = ?1 AND booked_at < ?2",
        params![currency, timestamp(until)],
        |row| row.get(0),
    )
}

/// A currency's bookings in `[from, until)` with their payments, in booking order.
pub fn movements(
    conn: &Connection,
    currency: &str,
    from: DateTime<Utc>,
    until: DateTime<Utc>,
) -> rusqlite::Result<Vec<BookedMovement>> {
    conn.prepare_cached(
        "SELECT b.id, b.payment_id, p.idempotency_key, p.account_id, p.iban, p.holder_name, b.kind, b.amount_minor,
                b.booked_at
         FROM bookings b JOIN payments p ON p.id = b.payment_id
         WHERE b.currency = ?1 AND b.booked_at >= ?2 AND b.booked_at < ?3
         ORDER BY b.booked_at, b.id",
    )?
    .query_map(params![currency, timestamp(from), timestamp(until)], |row| {
        Ok(BookedMovement {
            booking_id: from_sql(row, 0)?,
            payment_id: parsed(row, 1)?,
            idempotency_key: row.get(2)?,
            account_id: row.get(3)?,
            counterparty: BankAccount {
                iban: row.get(4)?,
                holder_name: row.get(5)?,
            },
            kind: wire(row, 6)?,
            amount_minor: row.get(7)?,
            booked_at: parsed(row, 8)?,
        })
    })?
    .collect()
}

pub fn statement_cursor(conn: &Connection) -> rusqlite::Result<Option<NaiveDate>> {
    conn.query_row(
        "SELECT business_date FROM statement_cursor WHERE id = 1",
        [],
        |row| parsed(row, 0),
    )
    .optional()
}

pub fn save_statement_cursor(conn: &Connection, date: NaiveDate) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO statement_cursor (id, business_date) VALUES (1, ?1)
         ON CONFLICT (id) DO UPDATE SET business_date = excluded.business_date",
        [date.to_string()],
    )
    .map(drop)
}

/// Writes a statement step atomically: new and newly issued statements, their events with outbox rows, the cursor.
pub fn commit_statements(
    conn: &mut Connection,
    statements: &[&StoredStatement],
    cursor: Option<NaiveDate>,
    events: &[NewEvent],
    outbox: Outbox<'_>,
) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    write_events(&tx, events, &outbox)?;
    {
        let mut upsert = tx.prepare_cached(
            "INSERT INTO bank_statements (statement_id, business_date, currency, due_at, issued, event_sequence, body,
                                          breaks)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT (statement_id) DO UPDATE SET issued = excluded.issued, event_sequence = excluded.event_sequence",
        )?;
        for stored in statements {
            let Draft { statement, breaks } = &stored.draft;
            upsert.execute(params![
                statement.statement_id,
                statement.business_date.to_string(),
                statement.currency,
                timestamp(statement.issued_at),
                stored.issued,
                stored.event_sequence.map(to_sql),
                serde_json::to_string(statement).expect("a statement serializes"),
                serde_json::to_string(breaks).expect("breaks serialize"),
            ])?;
        }
    }
    if let Some(date) = cursor {
        save_statement_cursor(&tx, date)?;
    }
    tx.commit()
}

fn statement_row(row: &Row<'_>) -> rusqlite::Result<StoredStatement> {
    let invalid = |index: usize| {
        move |e: serde_json::Error| {
            rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(e))
        }
    };
    let body: String = row.get(0)?;
    let breaks: String = row.get(1)?;
    let sequence: Option<i64> = row.get(3)?;
    Ok(StoredStatement {
        draft: Draft {
            statement: serde_json::from_str(&body).map_err(invalid(0))?,
            breaks: serde_json::from_str(&breaks).map_err(invalid(1))?,
        },
        issued: row.get(2)?,
        event_sequence: sequence.map(|s| s as u64),
    })
}

/// Statements, optionally of one business date and one currency, in date and currency order: the issued ones, or
/// with `pending` the late ones still waiting.
pub fn statements(
    conn: &Connection,
    date: Option<NaiveDate>,
    currency: Option<&str>,
    pending: bool,
) -> rusqlite::Result<Vec<StoredStatement>> {
    conn.prepare_cached(
        "SELECT body, breaks, issued, event_sequence FROM bank_statements
         WHERE (?1 IS NULL OR business_date = ?1) AND (?2 IS NULL OR currency = ?2) AND issued = ?3
         ORDER BY business_date, currency",
    )?
    .query_map(
        params![date.map(|d| d.to_string()), currency, !pending],
        statement_row,
    )?
    .collect()
}

pub fn statement(conn: &Connection, id: &str) -> rusqlite::Result<Option<StoredStatement>> {
    conn.query_row(
        "SELECT body, breaks, issued, event_sequence FROM bank_statements WHERE statement_id = ?1",
        [id],
        statement_row,
    )
    .optional()
}

/// Deletes every row of every table, in one transaction: the empty state of a new database (the schema stays).
pub fn reset(conn: &mut Connection) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    tx.pragma_update(None, "defer_foreign_keys", "ON")?;
    let tables = tx
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for table in tables {
        tx.execute(&format!("DELETE FROM \"{table}\""), [])?;
    }
    tx.execute("DELETE FROM sqlite_sequence", [])?;
    tx.commit()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_apply_on_top_of_the_shared_schema() {
        let mut conn = Connection::open_in_memory().unwrap();
        simulator_kit::persistence::migrate(&mut conn, MIGRATIONS).unwrap();
        assert_eq!(
            simulator_kit::persistence::schema_version(&conn).unwrap() as usize,
            MIGRATIONS.len()
        );
        assert_eq!(last_payment_sequence(&conn).unwrap(), 0);
        assert!(statement_cursor(&conn).unwrap().is_none());
        assert_eq!(
            simulator_kit::outbox::next_event_sequence(&conn).unwrap(),
            1
        );
    }
}
