//! The canonical event log and its transactional outbox, in SQLite: events, webhook subscriptions, deliveries and
//! attempts, and the virtual clock's stored reading. The tables are the ones in [`SCHEMA`]; a simulator creates them
//! in its own migrations. Every function runs on the caller's connection (the single writer, or a reader for queries).

use chrono::{DateTime, Utc};
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, params};
use uuid::Uuid;

use crate::events::{EventEnvelope, NewEvent};
use crate::ids::IdScheme;
use crate::persistence::sql::{from_sql, json_column, parsed, timestamp, to_sql};
use crate::webhooks::{
    DeliveryJob, DeliveryOutcome, DeliveryResult, FaultPolicy, Parties, Subscription,
    SubscriptionSource,
};

/// The shared tables as they stood when the kit was extracted, for a simulator's first migration: `events`,
/// `webhook_subscriptions`, `webhook_deliveries`, `webhook_attempts` and `virtual_clock`. Later shared changes are
/// migrations of their own, run after it: [`PARTICIPANT_FILTER`].
pub const SCHEMA: &str = include_str!("schema.sql");

/// A migration both services run after [`SCHEMA`]'s tables exist: the subscription's participant filter (JSON array
/// of participant IDs; NULL receives every event of its types).
pub const PARTICIPANT_FILTER: &str = "ALTER TABLE webhook_subscriptions ADD COLUMN participants TEXT \
     CHECK (participants IS NULL OR (json_valid(participants) AND json_type(participants) = 'array'));";

/// A recorded webhook attempt with its delivery's subscription and event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttemptRow {
    pub id: u64,
    pub delivery_id: Uuid,
    pub subscription_id: Uuid,
    pub url: String,
    pub event_sequence: u64,
    pub event_id: Uuid,
    pub event_type: String,
    pub attempt: u32,
    pub held: bool,
    pub faults: Vec<String>,
    pub body: Option<String>,
    pub http_status: Option<u16>,
    pub error: Option<String>,
    pub at: DateTime<Utc>,
}

/// A webhook delivery as test control shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryRecord {
    pub delivery_id: Uuid,
    pub subscription_id: Uuid,
    pub url: String,
    pub event_sequence: u64,
    pub event_id: Uuid,
    pub event_type: String,
    /// `PENDING`, `IN_FLIGHT`, `DELIVERED` or `DEAD`.
    pub status: String,
    pub attempts: u64,
    pub last_error: Option<String>,
    /// When the next attempt is due (pending), or the lease ends (in flight).
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub delivered_at: Option<DateTime<Utc>>,
}

/// One subscription's deliveries by status.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeliveryStats {
    pub pending: u64,
    pub in_flight: u64,
    pub delivered: u64,
    /// Dead letters.
    pub dead: u64,
    /// The event sequence and time of the oldest pending or in-flight delivery: how far behind the consumer is.
    pub oldest_unfinished: Option<(u64, DateTime<Utc>)>,
}

/// Where a transaction's events go out: the subscriptions to queue deliveries for, the run's simulation ID, the ID
/// scheme for delivery rows, the real time at which the deliveries become due, and the payload schema version the
/// events are stamped with.
pub struct Outbox<'a> {
    pub subscriptions: &'a [Subscription],
    pub simulation_id: Option<&'a str>,
    pub ids: IdScheme,
    pub due: DateTime<Utc>,
    pub schema_version: u32,
    /// The participants an event involves, for subscriptions' participant filters (see [`crate::webhooks::WebhookHub::parties`]).
    pub parties: Option<Parties>,
}

/// Appends stamped events to the canonical log at their assigned sequences, and queues one pending delivery per event
/// and interested subscription, inside the caller's transaction.
pub fn write_events(
    tx: &Connection,
    new_events: &[NewEvent],
    outbox: &Outbox<'_>,
) -> rusqlite::Result<()> {
    let due = timestamp(outbox.due);
    let mut insert_event = tx.prepare_cached(
        "INSERT INTO events (sequence, event_id, event_type, schema_version, source, simulation_id, aggregate_id,
                             correlation_id, payload, occurred_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?;
    let mut insert_delivery = tx.prepare_cached(
        "INSERT INTO webhook_deliveries (id, subscription_id, event_sequence, status, attempts, next_attempt_at)
         VALUES (?1, ?2, ?3, 'PENDING', 0, ?4)",
    )?;
    for event in new_events {
        assert!(
            event.sequence > 0,
            "events are stamped before they are written"
        );
        insert_event.execute(params![
            to_sql(event.sequence),
            event.event_id.to_string(),
            event.event_type,
            outbox.schema_version,
            event.source,
            outbox.simulation_id,
            event.aggregate_id,
            event.correlation_id,
            event.payload.to_string(),
            timestamp(event.occurred_at),
        ])?;
        let parties = outbox
            .parties
            .map_or_else(Vec::new, |f| f(event.event_type, &event.payload));
        for subscription in outbox
            .subscriptions
            .iter()
            .filter(|s| s.wants(event.event_type, &parties))
        {
            insert_delivery.execute(params![
                outbox
                    .ids
                    .delivery(subscription.id, event.sequence)
                    .to_string(),
                subscription.id.to_string(),
                to_sql(event.sequence),
                due
            ])?;
        }
    }
    Ok(())
}

/// The `events` columns [`event_row`] reads, aliased `e`.
pub const EVENT_COLUMNS: &str = "e.event_id, e.sequence, e.event_type, e.schema_version, e.source, e.simulation_id, \
                             e.aggregate_id, e.correlation_id, e.occurred_at, e.payload";

/// Reads the [`EVENT_COLUMNS`] starting at column `at`.
pub fn event_row(row: &Row<'_>, at: usize) -> rusqlite::Result<EventEnvelope> {
    let payload: String = row.get(at + 9)?;
    Ok(EventEnvelope {
        event_id: parsed(row, at)?,
        sequence: from_sql(row, at + 1)?,
        event_type: row.get(at + 2)?,
        schema_version: row.get(at + 3)?,
        source: row.get(at + 4)?,
        simulation_id: row.get(at + 5)?,
        aggregate_id: row.get(at + 6)?,
        correlation_id: row.get(at + 7)?,
        occurred_at: parsed(row, at + 8)?,
        payload: serde_json::from_str(&payload).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(at + 9, Type::Text, Box::new(e))
        })?,
    })
}

/// Canonical events with `sequence > after`, in sequence order, optionally of one type.
pub fn events(
    conn: &Connection,
    after: u64,
    limit: u32,
    event_type: Option<&str>,
) -> rusqlite::Result<Vec<EventEnvelope>> {
    let types = event_type.map(|t| vec![t.to_string()]);
    Ok(read_events(conn, after, limit, types.as_deref())?.events)
}

/// One page of the event log, and how far the read looked (SR-34).
#[derive(Clone, Debug, PartialEq)]
pub struct EventPage {
    pub events: Vec<EventEnvelope>,
    /// The highest sequence the read looked at: the last returned event's when the page is full, else the log's head
    /// when the read began (or `after`, if that is further). A reader may continue from here.
    pub scanned_through: u64,
}

/// Canonical events with `sequence > after`, in sequence order, of the given types (`None`: every type). The head is
/// read first and the page is bounded by it, so an event committed during the read is neither returned nor skipped.
pub fn read_events(
    conn: &Connection,
    after: u64,
    limit: u32,
    event_types: Option<&[String]>,
) -> rusqlite::Result<EventPage> {
    let head = conn.query_row("SELECT COALESCE(MAX(sequence), 0) FROM events", [], |row| {
        from_sql(row, 0)
    })?;
    let types = event_types.map(|t| serde_json::to_string(t).expect("strings serialize"));
    let events: Vec<EventEnvelope> = conn
        .prepare_cached(&format!(
            "SELECT {EVENT_COLUMNS} FROM events e
                 WHERE e.sequence > ?1 AND e.sequence <= ?2
                   AND (?3 IS NULL OR e.event_type IN (SELECT value FROM json_each(?3)))
                 ORDER BY e.sequence LIMIT ?4"
        ))?
        .query_map(params![to_sql(after), to_sql(head), types, limit], |row| {
            event_row(row, 0)
        })?
        .collect::<rusqlite::Result<_>>()?;
    let scanned_through = match events.last() {
        Some(last) if events.len() == limit as usize => last.sequence,
        _ => head.max(after),
    };
    Ok(EventPage {
        events,
        scanned_through,
    })
}

pub fn insert_subscription(conn: &Connection, subscription: &Subscription) -> rusqlite::Result<()> {
    let event_types = serde_json::to_string(&subscription.event_types).expect("strings serialize");
    conn
        .prepare_cached(
            "INSERT INTO webhook_subscriptions (id, url, enabled, event_types, created_at, secret, ordered, name, source,
                                                paused, previous_secret, previous_secret_until, deleted, participants)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        )?
        .execute(params![
            subscription.id.to_string(),
            subscription.url,
            subscription.enabled,
            event_types,
            timestamp(subscription.created_at),
            subscription.secret,
            subscription.ordered,
            subscription.name,
            subscription.source.as_str(),
            subscription.paused,
            subscription.previous_secret.as_ref().map(|(secret, _)| secret),
            subscription.previous_secret.as_ref().map(|(_, until)| timestamp(*until)),
            subscription.deleted,
            participants_json(subscription),
        ])
        .map(drop)
}

/// Stores a subscription's url, event types, secrets, ordering, and enabled, paused and deleted flags; false when
/// there is no such subscription. Its pending deliveries keep their place and go to the new URL; while it is disabled or paused
/// they wait.
fn participants_json(subscription: &Subscription) -> Option<String> {
    subscription
        .participants
        .as_ref()
        .map(|p| serde_json::to_string(p).expect("strings serialize"))
}

pub fn update_subscription(
    conn: &Connection,
    subscription: &Subscription,
) -> rusqlite::Result<bool> {
    let event_types = serde_json::to_string(&subscription.event_types).expect("strings serialize");
    conn
        .prepare_cached(
            "UPDATE webhook_subscriptions SET url = ?2, enabled = ?3, event_types = ?4, secret = ?5, ordered = ?6,
                                             paused = ?7, previous_secret = ?8, previous_secret_until = ?9,
                                             deleted = ?10, participants = ?11
             WHERE id = ?1",
        )?
        .execute(params![
            subscription.id.to_string(),
            subscription.url,
            subscription.enabled,
            event_types,
            subscription.secret,
            subscription.ordered,
            subscription.paused,
            subscription.previous_secret.as_ref().map(|(secret, _)| secret),
            subscription.previous_secret.as_ref().map(|(_, until)| timestamp(*until)),
            subscription.deleted,
            participants_json(subscription),
        ])
        .map(|n| n == 1)
}

pub fn subscriptions(conn: &Connection) -> rusqlite::Result<Vec<Subscription>> {
    conn
        .prepare_cached(
            "SELECT id, url, enabled, event_types, created_at, secret, ordered, fault_policy, name, source, paused,
                    previous_secret, previous_secret_until, deleted, participants
             FROM webhook_subscriptions ORDER BY created_at, id",
        )?
        .query_map([], |row| {
            let event_types: String = row.get(3)?;
            Ok(Subscription {
                id: parsed(row, 0)?,
                url: row.get(1)?,
                enabled: row.get(2)?,
                event_types: serde_json::from_str(&event_types).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(3, Type::Text, Box::new(e))
                })?,
                created_at: parsed(row, 4)?,
                secret: row.get(5)?,
                ordered: row.get(6)?,
                faults: json_column(row, 7)?,
                name: row.get(8)?,
                source: match row.get::<_, String>(9)?.as_str() {
                    "config" => SubscriptionSource::Config,
                    _ => SubscriptionSource::Api,
                },
                paused: row.get(10)?,
                previous_secret: match row.get::<_, Option<String>>(11)? {
                    Some(secret) => Some((secret, parsed(row, 12)?)),
                    None => None,
                },
                deleted: row.get(13)?,
                participants: json_column(row, 14)?,
            })
        })?
        .collect()
}

/// Sets or clears a subscription's fault policy; false when there is no such subscription.
pub fn set_fault_policy(
    conn: &Connection,
    subscription_id: Uuid,
    policy: Option<&FaultPolicy>,
) -> rusqlite::Result<bool> {
    let policy = policy.map(|p| serde_json::to_string(p).expect("policies serialize"));
    conn.prepare_cached("UPDATE webhook_subscriptions SET fault_policy = ?2 WHERE id = ?1")?
        .execute(params![subscription_id.to_string(), policy])
        .map(|n| n == 1)
}

/// Recorded webhook attempts, oldest first, optionally for one subscription.
pub fn attempts(
    conn: &Connection,
    subscription_id: Option<Uuid>,
) -> rusqlite::Result<Vec<AttemptRow>> {
    conn
        .prepare_cached(
            "SELECT a.id, a.delivery_id, d.subscription_id, s.url, d.event_sequence, e.event_id, e.event_type,
                    a.attempt, a.outcome, a.faults, a.body, a.http_status, a.error, a.at
             FROM webhook_attempts a
             JOIN webhook_deliveries d ON d.id = a.delivery_id
             JOIN webhook_subscriptions s ON s.id = d.subscription_id
             JOIN events e ON e.sequence = d.event_sequence
             WHERE ?1 IS NULL OR d.subscription_id = ?1
             ORDER BY a.id",
        )?
        .query_map(params![subscription_id.map(|id| id.to_string())], |row| {
            Ok(AttemptRow {
                id: from_sql(row, 0)?,
                delivery_id: parsed(row, 1)?,
                subscription_id: parsed(row, 2)?,
                url: row.get(3)?,
                event_sequence: from_sql(row, 4)?,
                event_id: parsed(row, 5)?,
                event_type: row.get(6)?,
                attempt: row.get(7)?,
                held: row.get::<_, String>(8)? == "HELD",
                faults: json_column(row, 9)?.unwrap_or_default(),
                body: row.get(10)?,
                http_status: row.get(11)?,
                error: row.get(12)?,
                at: parsed(row, 13)?,
            })
        })?
        .collect()
}

/// Claims up to `limit` deliveries that are due at `now` (pending, or in flight with an expired lease), oldest
/// due first, and leases them until `lease_until`. For an ordered subscription only its unfinished delivery with
/// the lowest sequence is eligible, so it never has two in flight and later events wait behind a failing one.
pub fn claim_deliveries(
    conn: &mut Connection,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    limit: u32,
) -> rusqlite::Result<Vec<DeliveryJob>> {
    let tx = conn.transaction()?;
    let jobs = {
        let mut select = tx.prepare_cached(&format!(
            "SELECT d.id, d.subscription_id, s.url, d.attempts, {EVENT_COLUMNS}, s.secret, s.fault_policy, d.held,
                    CASE WHEN s.previous_secret_until > ?1 THEN s.previous_secret END
             FROM webhook_deliveries d
             JOIN webhook_subscriptions s ON s.id = d.subscription_id
             JOIN events e ON e.sequence = d.event_sequence
             WHERE d.status IN ('PENDING', 'IN_FLIGHT') AND d.next_attempt_at <= ?1 AND s.enabled = 1 AND s.paused = 0 AND s.deleted = 0
               AND (s.ordered = 0 OR d.event_sequence = (
                     SELECT MIN(u.event_sequence) FROM webhook_deliveries u
                     WHERE u.subscription_id = d.subscription_id AND u.status IN ('PENDING', 'IN_FLIGHT')))
             ORDER BY d.next_attempt_at, d.event_sequence
             LIMIT ?2"
        ))?;
        let jobs = select
            .query_map(params![timestamp(now), limit], |row| {
                Ok(DeliveryJob {
                    delivery_id: parsed(row, 0)?,
                    subscription_id: parsed(row, 1)?,
                    url: row.get(2)?,
                    attempts: row.get(3)?,
                    event: event_row(row, 4)?,
                    secret: row.get(14)?,
                    faults: json_column(row, 15)?,
                    held: row.get(16)?,
                    previous_secret: row.get(17)?,
                    epoch: 0,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut lease = tx.prepare_cached(
            "UPDATE webhook_deliveries SET status = 'IN_FLIGHT', next_attempt_at = ?2 WHERE id = ?1",
        )?;
        for job in &jobs {
            lease.execute(params![job.delivery_id.to_string(), timestamp(lease_until)])?;
        }
        jobs
    };
    tx.commit()?;
    Ok(jobs)
}

/// Records attempt outcomes. Each counts as one attempt, except a hold. Only in-flight deliveries are updated, so a
/// late result for a delivery that was meanwhile re-claimed and finished does not overwrite it. What was sent is
/// recorded in `webhook_attempts` either way: it happened.
pub fn record_deliveries(
    conn: &mut Connection,
    results: &[DeliveryResult],
    now: DateTime<Utc>,
) -> rusqlite::Result<()> {
    let now = timestamp(now);
    let tx = conn.transaction()?;
    {
        let mut update = tx.prepare_cached(
            "UPDATE webhook_deliveries
             SET status = ?2, attempts = attempts + 1, next_attempt_at = ?3, last_error = ?4, delivered_at = ?5
             WHERE id = ?1 AND status = 'IN_FLIGHT'",
        )?;
        let mut hold = tx.prepare_cached(
            "UPDATE webhook_deliveries SET status = 'PENDING', next_attempt_at = ?2, held = 1
             WHERE id = ?1 AND status = 'IN_FLIGHT'",
        )?;
        let mut attempt = tx.prepare_cached(
            "INSERT INTO webhook_attempts (delivery_id, attempt, outcome, faults, body, http_status, error, at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for result in results {
            let (status, next, error, delivered) = match &result.outcome {
                DeliveryOutcome::Delivered => ("DELIVERED", None, None, Some(now.clone())),
                DeliveryOutcome::Retry { at, error } => {
                    ("PENDING", Some(timestamp(*at)), Some(error.as_str()), None)
                }
                DeliveryOutcome::Dead { error } => ("DEAD", None, Some(error.as_str()), None),
                DeliveryOutcome::Held { .. } => ("PENDING", None, None, None),
            };
            if let DeliveryOutcome::Held { at } = &result.outcome {
                hold.execute(params![result.delivery_id.to_string(), timestamp(*at)])?;
            } else {
                update.execute(params![
                    result.delivery_id.to_string(),
                    status,
                    next,
                    error,
                    delivered
                ])?;
            }
            for sent in &result.sent {
                attempt.execute(params![
                    result.delivery_id.to_string(),
                    sent.attempt,
                    if sent.held { "HELD" } else { "SENT" },
                    serde_json::to_string(&sent.faults).expect("strings serialize"),
                    sent.body,
                    sent.http_status,
                    sent.error,
                    timestamp(sent.at),
                ])?;
            }
        }
    }
    tx.commit()
}

/// The stored virtual clock reading and whether it was running, if a virtual clock was ever used.
pub fn virtual_clock(conn: &Connection) -> rusqlite::Result<Option<(DateTime<Utc>, bool)>> {
    conn.query_row(
        "SELECT now, running FROM virtual_clock WHERE id = 1",
        [],
        |row| Ok((parsed(row, 0)?, row.get(1)?)),
    )
    .optional()
}

/// The latest `occurred_at` of the events the service stamped from its clock. Events from `external_source` carry
/// a reporter's own time, so they are left out.
pub fn latest_clock_stamp(
    conn: &Connection,
    external_source: &str,
) -> rusqlite::Result<Option<DateTime<Utc>>> {
    let latest: Option<String> = conn.query_row(
        "SELECT MAX(occurred_at) FROM events WHERE source <> ?1",
        [external_source],
        |row| row.get(0),
    )?;
    latest
        .map(|text| {
            DateTime::parse_from_rfc3339(&text)
                .map(|at| at.with_timezone(&Utc))
                .map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })
        })
        .transpose()
}

pub fn save_virtual_clock(
    conn: &Connection,
    now: DateTime<Utc>,
    running: bool,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO virtual_clock (id, now, running) VALUES (1, ?1, ?2)
             ON CONFLICT (id) DO UPDATE SET now = excluded.now, running = excluded.running",
        params![timestamp(now), running],
    )
    .map(drop)
}

/// The sequence the next event will get: one past the highest ever assigned (AUTOINCREMENT never reuses one).
pub fn next_event_sequence(conn: &Connection) -> rusqlite::Result<u64> {
    conn.query_row(
        "SELECT MAX(COALESCE((SELECT MAX(sequence) FROM events), 0),
                    COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0)) + 1",
        [],
        |row| from_sql(row, 0),
    )
}

/// Events about one aggregate (e.g. an execution's settlement lifecycle), optionally from one source.
pub fn events_for(
    conn: &Connection,
    aggregate_id: &str,
    source: Option<&str>,
) -> rusqlite::Result<Vec<EventEnvelope>> {
    conn.prepare_cached(&format!(
        "SELECT {EVENT_COLUMNS} FROM events e
             WHERE e.aggregate_id = ?1 AND (?2 IS NULL OR e.source = ?2) ORDER BY e.sequence"
    ))?
    .query_map(params![aggregate_id, source], |row| event_row(row, 0))?
    .collect()
}

/// Number of webhook deliveries per status.
pub fn delivery_counts(conn: &Connection) -> rusqlite::Result<Vec<(String, u64)>> {
    conn.prepare_cached(
        "SELECT status, COUNT(*) FROM webhook_deliveries GROUP BY status ORDER BY status",
    )?
    .query_map([], |row| Ok((row.get(0)?, from_sql(row, 1)?)))?
    .collect()
}

/// Webhook deliveries with their subscription URL and event, in event order, then subscription; optionally only
/// one subscription's, or only those with one status (e.g. `DEAD`, the dead letters).
/// One subscription's deliveries by status, and its oldest unfinished one.
pub fn delivery_stats(conn: &Connection, subscription_id: Uuid) -> rusqlite::Result<DeliveryStats> {
    let id = subscription_id.to_string();
    let mut stats = DeliveryStats::default();
    let mut counts = conn.prepare_cached(
        "SELECT status, COUNT(*) FROM webhook_deliveries WHERE subscription_id = ?1 GROUP BY status",
    )?;
    let rows = counts.query_map([&id], |row| {
        Ok((row.get::<_, String>(0)?, from_sql(row, 1)?))
    })?;
    for row in rows {
        let (status, count) = row?;
        match status.as_str() {
            "PENDING" => stats.pending = count,
            "IN_FLIGHT" => stats.in_flight = count,
            "DELIVERED" => stats.delivered = count,
            "DEAD" => stats.dead = count,
            _ => {}
        }
    }
    stats.oldest_unfinished = conn
        .prepare_cached(
            "SELECT d.event_sequence, e.occurred_at FROM webhook_deliveries d
             JOIN events e ON e.sequence = d.event_sequence
             WHERE d.subscription_id = ?1 AND d.status IN ('PENDING', 'IN_FLIGHT')
             ORDER BY d.event_sequence LIMIT 1",
        )?
        .query_row([&id], |row| Ok((from_sql(row, 0)?, parsed(row, 1)?)))
        .optional()?;
    Ok(stats)
}

/// Webhook deliveries in event order: all, or one subscription's, of one status, after an event sequence, at most
/// `limit`.
pub fn deliveries(
    conn: &Connection,
    subscription_id: Option<Uuid>,
    status: Option<&str>,
    after: u64,
    limit: u32,
) -> rusqlite::Result<Vec<DeliveryRecord>> {
    conn
        .prepare_cached(
            "SELECT d.id, d.subscription_id, s.url, d.event_sequence, e.event_id, e.event_type, d.status,
                    d.attempts, d.last_error, d.next_attempt_at, d.delivered_at
             FROM webhook_deliveries d
             JOIN webhook_subscriptions s ON s.id = d.subscription_id
             JOIN events e ON e.sequence = d.event_sequence
             WHERE (?1 IS NULL OR d.subscription_id = ?1) AND (?2 IS NULL OR d.status = ?2)
               AND d.event_sequence > ?3
             ORDER BY d.event_sequence, s.created_at, s.id
             LIMIT ?4",
        )?
        .query_map(params![subscription_id.map(|id| id.to_string()), status, to_sql(after), limit], |row| {
            Ok(DeliveryRecord {
                delivery_id: parsed(row, 0)?,
                subscription_id: parsed(row, 1)?,
                url: row.get(2)?,
                event_sequence: from_sql(row, 3)?,
                event_id: parsed(row, 4)?,
                event_type: row.get(5)?,
                status: row.get(6)?,
                attempts: from_sql(row, 7)?,
                last_error: row.get(8)?,
                next_attempt_at: row
                    .get::<_, Option<String>>(9)?
                    .map(|_| parsed(row, 9))
                    .transpose()?,
                delivered_at: row
                    .get::<_, Option<String>>(10)?
                    .map(|_| parsed(row, 10))
                    .transpose()?,
            })
        })?
        .collect()
}

/// Queues the subscription's events after sequence `after` (of the types it wants, and through its participant
/// filter) for delivery again, due at `now`: existing deliveries start over (pending, no attempts, never held), and
/// events from before the subscription existed get a delivery. Returns how many were queued.
pub fn redeliver(
    conn: &mut Connection,
    subscription: &Subscription,
    after: u64,
    now: DateTime<Utc>,
    ids: &IdScheme,
    parties: Option<Parties>,
) -> rusqlite::Result<usize> {
    let event_types = serde_json::to_string(&subscription.event_types).expect("strings serialize");
    let tx = conn.transaction()?;
    let queued = {
        let sequences = tx
            .prepare_cached(
                "SELECT sequence, event_type, payload FROM events
                 WHERE sequence > ?1 AND event_type IN (SELECT value FROM json_each(?2))
                 ORDER BY sequence",
            )?
            .query_map(params![to_sql(after), event_types], |row| {
                Ok((
                    from_sql(row, 0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .filter_map(|row| match row {
                Err(error) => Some(Err(error)),
                Ok((sequence, event_type, payload)) => {
                    let involved = match (parties, &subscription.participants) {
                        (Some(parties), Some(_)) => {
                            let payload = serde_json::from_str(&payload).unwrap_or_default();
                            subscription.involves(&parties(&event_type, &payload))
                        }
                        _ => true,
                    };
                    involved.then_some(Ok(sequence))
                }
            })
            .collect::<rusqlite::Result<Vec<u64>>>()?;
        let mut upsert = tx.prepare_cached(
            "INSERT INTO webhook_deliveries (id, subscription_id, event_sequence, status, attempts, next_attempt_at)
             VALUES (?1, ?2, ?3, 'PENDING', 0, ?4)
             ON CONFLICT (subscription_id, event_sequence) DO UPDATE
             SET status = 'PENDING', attempts = 0, next_attempt_at = ?4, last_error = NULL, delivered_at = NULL,
                 held = 0",
        )?;
        for sequence in &sequences {
            upsert.execute(params![
                ids.delivery(subscription.id, *sequence).to_string(),
                subscription.id.to_string(),
                to_sql(*sequence),
                timestamp(now),
            ])?;
        }
        sequences.len()
    };
    tx.commit()?;
    Ok(queued)
}

/// Gives dead-lettered deliveries a fresh attempt budget, due at `now`: all of them, one subscription's, or the
/// listed ones. Returns how many.
pub fn retry_dead_letters(
    conn: &mut Connection,
    subscription_id: Option<Uuid>,
    delivery_ids: &[Uuid],
    now: DateTime<Utc>,
) -> rusqlite::Result<usize> {
    let delivery_ids =
        serde_json::to_string(&delivery_ids.iter().map(Uuid::to_string).collect::<Vec<_>>())
            .expect("strings serialize");
    conn
        .prepare_cached(
            "UPDATE webhook_deliveries SET status = 'PENDING', attempts = 0, next_attempt_at = ?1, last_error = NULL
             WHERE status = 'DEAD' AND (?2 IS NULL OR subscription_id = ?2)
               AND (?3 = '[]' OR id IN (SELECT value FROM json_each(?3)))",
        )?
        .execute(params![
            timestamp(now),
            subscription_id.map(|id| id.to_string()),
            delivery_ids
        ])
}

/// Makes every in-flight delivery due now. Used at startup, when no claim can still be in progress.
pub fn release_claims(conn: &Connection, now: DateTime<Utc>) -> rusqlite::Result<usize> {
    conn.execute(
        "UPDATE webhook_deliveries SET status = 'PENDING', next_attempt_at = ?1 WHERE status = 'IN_FLIGHT'",
        [timestamp(now)],
    )
}

/// The `CREATE` statements of the shared tables and their indexes in `conn`, comments dropped and whitespace
/// normalized, in name order: for checking that a simulator's migrations produce [`SCHEMA`].
pub fn shared_schema(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    const TABLES: [&str; 5] = [
        "events",
        "webhook_subscriptions",
        "webhook_deliveries",
        "webhook_attempts",
        "virtual_clock",
    ];
    let mut statements = conn
        .prepare(
            "SELECT tbl_name, sql FROM sqlite_master WHERE type IN ('table', 'index') AND sql IS NOT NULL
             ORDER BY type DESC, name",
        )?
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    statements.retain(|(table, _)| TABLES.contains(&table.as_str()));
    Ok(statements
        .into_iter()
        .map(|(_, sql)| normalize_sql(&sql))
        .collect())
}

fn normalize_sql(sql: &str) -> String {
    let without_comments: String = sql
        .lines()
        .map(|line| line.split("--").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join(" ");
    let mut out = String::new();
    for word in without_comments.split_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
    out.replace("( ", "(").replace(" )", ")").replace(" ,", ",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_schema_and_later_migrations_create_the_shared_tables() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute_batch(PARTICIPANT_FILTER).unwrap();
        let schema = shared_schema(&conn).unwrap();
        assert_eq!(schema.len(), 10, "{schema:#?}");
        assert!(schema.iter().all(|s| !s.contains("--")));
        assert_eq!(next_event_sequence(&conn).unwrap(), 1);
        assert!(subscriptions(&conn).unwrap().is_empty());
    }
}
