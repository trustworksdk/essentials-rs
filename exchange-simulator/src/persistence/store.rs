use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, params};
use uuid::Uuid;

use crate::domain::{Execution, Instrument, InstrumentId, Order, Participant, ParticipantId};
use crate::engine::BookUpdate;
use crate::events::{self, EventEnvelope, NewEvent};
use crate::ids::IdScheme;
use crate::settlement::{ExternalReport, Instruction, SettlementStatus};
use crate::webhooks::{
    DeliveryJob, DeliveryOutcome, DeliveryResult, FaultPolicy, Subscription, SubscriptionSource,
};

/// Repository over the single writer connection. Every state change a matching step produces is written by
/// [`Store::commit`] in one transaction.
pub struct Store {
    conn: Connection,
    /// Set when WAL checkpoints run in the background (see [`Store::checkpoint_in_background`]). Declared after
    /// `conn`, so the writer closes first and the checkpointer's connection, the last one, removes the WAL.
    checkpointer: Option<super::checkpoint::Checkpointer>,
}

/// Optional equality filters for listing instruments; unset fields match everything.
#[derive(Clone, Debug, Default)]
pub struct InstrumentFilter {
    pub symbol: Option<String>,
    pub mic: Option<String>,
    pub isin: Option<String>,
    pub cusip: Option<String>,
}

/// Optional filters for listing executions; unset fields match everything. Results come in insertion order.
#[derive(Clone, Debug, Default)]
pub struct ExecutionFilter {
    pub instrument_id: Option<InstrumentId>,
    /// Matches executions where the participant is buyer or seller.
    pub participant_id: Option<ParticipantId>,
    /// Matches executions where the order is the buy or the sell order.
    pub order_id: Option<Uuid>,
    pub limit: u32,
}

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

/// An order with the timestamps stored alongside it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderRecord {
    pub order: Order,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

const ORDER_COLUMNS: &str = "id, client_order_id, instrument_id, participant_id, side, order_type, limit_price_ticks, \
                             quantity, remaining_quantity, sequence, status, created_at, updated_at";

impl Store {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            checkpointer: None,
        }
    }

    /// Moves WAL checkpoints off the writer. SQLite's automatic checkpoint, which runs inside the commit that crosses
    /// 1000 pages, is turned off; instead, once the WAL holds `pages` pages, [`Store::request_checkpoint_if_due`]
    /// asks a background connection to checkpoint (`PASSIVE`, which never blocks the writer). A database without a
    /// file (in memory) keeps the automatic checkpoint.
    pub fn checkpoint_in_background(
        &mut self,
        busy_timeout: std::time::Duration,
        pages: u32,
    ) -> rusqlite::Result<()> {
        let Some(path) = self.conn.path().filter(|p| !p.is_empty()) else {
            return Ok(());
        };
        let path = std::path::PathBuf::from(path);
        self.checkpointer = Some(super::checkpoint::Checkpointer::start(
            &self.conn,
            &path,
            busy_timeout,
            pages,
        )?);
        Ok(())
    }

    /// With background checkpoints, asks for one each time the WAL has grown by the threshold. Once the WAL holds four
    /// times the threshold, the writer checkpoints itself, which bounds the WAL (see `persistence/checkpoint.rs`). Call it on the writer's thread after writing, outside a
    /// transaction; the exchange thread does so after every command or commit group.
    pub fn request_checkpoint_if_due(&self) {
        if let Some(checkpointer) = &self.checkpointer {
            checkpointer.after_write(&self.conn);
        }
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Opens the transaction of a commit group: the [`Store::commit`]s until [`Store::commit_group`] become durable
    /// together. Other writes must not run while a group is open (they would fail to start their transaction).
    pub fn begin_group(&mut self) -> rusqlite::Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")
    }

    /// Commits the open group. If the commit fails, the group is rolled back: none of its writes are stored.
    pub fn commit_group(&mut self) -> rusqlite::Result<()> {
        let result = self.conn.execute_batch("COMMIT");
        if result.is_err() && !self.conn.is_autocommit() {
            // Some failures (a deferred constraint, SQLITE_BUSY) leave the transaction open.
            let _ = self.conn.execute_batch("ROLLBACK");
        }
        result
    }

    pub fn insert_instrument(&self, instrument: &Instrument) -> rusqlite::Result<()> {
        self.conn
            .prepare_cached(
                "INSERT INTO instruments (id, symbol, mic, isin, cusip, currency, currency_scale, tick_size_minor,
                                          lot_size, enabled)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )?
            .execute(params![
                instrument.id.0,
                instrument.symbol,
                instrument.mic,
                instrument.isin,
                instrument.cusip,
                instrument.currency,
                instrument.currency_scale,
                to_sql(instrument.tick_size_minor),
                to_sql(instrument.lot_size),
                instrument.enabled,
            ])
            .map(drop)
    }

    pub fn set_instrument_enabled(
        &self,
        id: &InstrumentId,
        enabled: bool,
    ) -> rusqlite::Result<usize> {
        self.conn
            .prepare_cached("UPDATE instruments SET enabled = ?2 WHERE id = ?1")?
            .execute(params![id.0, enabled])
    }

    pub fn instruments(&self, filter: &InstrumentFilter) -> rusqlite::Result<Vec<Instrument>> {
        self.conn
            .prepare_cached(
                "SELECT id, symbol, mic, isin, cusip, currency, currency_scale, tick_size_minor, lot_size, enabled
                 FROM instruments
                 WHERE (?1 IS NULL OR symbol = ?1) AND (?2 IS NULL OR mic = ?2)
                   AND (?3 IS NULL OR isin = ?3) AND (?4 IS NULL OR cusip = ?4)
                 ORDER BY id",
            )?
            .query_map(params![filter.symbol, filter.mic, filter.isin, filter.cusip], instrument_row)?
            .collect()
    }

    pub fn instrument(&self, id: &InstrumentId) -> rusqlite::Result<Option<Instrument>> {
        self.conn
            .prepare_cached(
                "SELECT id, symbol, mic, isin, cusip, currency, currency_scale, tick_size_minor, lot_size, enabled
                 FROM instruments WHERE id = ?1",
            )?
            .query_row([&id.0], instrument_row)
            .optional()
    }

    pub fn executions(&self, filter: &ExecutionFilter) -> rusqlite::Result<Vec<Execution>> {
        self.conn
            .prepare_cached(
                "SELECT id, instrument_id, buy_order_id, sell_order_id, buyer_id, seller_id, price_ticks, quantity,
                        executed_at
                 FROM executions
                 WHERE (?1 IS NULL OR instrument_id = ?1)
                   AND (?2 IS NULL OR buyer_id = ?2 OR seller_id = ?2)
                   AND (?3 IS NULL OR buy_order_id = ?3 OR sell_order_id = ?3)
                 ORDER BY rowid
                 LIMIT ?4",
            )?
            .query_map(
                params![
                    filter.instrument_id.as_ref().map(|i| &i.0),
                    filter.participant_id.as_ref().map(|p| &p.0),
                    filter.order_id.map(|o| o.to_string()),
                    filter.limit,
                ],
                |row| {
                    Ok(Execution {
                        id: parsed(row, 0)?,
                        instrument_id: InstrumentId(row.get(1)?),
                        buy_order_id: parsed(row, 2)?,
                        sell_order_id: parsed(row, 3)?,
                        buyer_id: ParticipantId(row.get(4)?),
                        seller_id: ParticipantId(row.get(5)?),
                        price_ticks: row.get(6)?,
                        quantity: from_sql(row, 7)?,
                        executed_at: parsed(row, 8)?,
                    })
                },
            )?
            .collect()
    }

    pub fn insert_participant(&self, participant: &Participant) -> rusqlite::Result<()> {
        self.conn
            .prepare_cached("INSERT INTO participants (id, name, settlement) VALUES (?1, ?2, ?3)")?
            .execute(params![
                participant.id.0,
                participant.name,
                participant.settlement.as_ref().map(|s| s.to_string()),
            ])
            .map(drop)
    }

    pub fn participants(&self) -> rusqlite::Result<Vec<Participant>> {
        self.conn
            .prepare_cached("SELECT id, name, settlement FROM participants ORDER BY id")?
            .query_map([], |row| {
                let settlement: Option<String> = row.get(2)?;
                Ok(Participant {
                    id: ParticipantId(row.get(0)?),
                    name: row.get(1)?,
                    settlement: settlement
                        .map(|text| serde_json::from_str(&text))
                        .transpose()
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(2, Type::Text, Box::new(e))
                        })?,
                })
            })?
            .collect()
    }

    /// Orders that rest on a book (`OPEN` or `PARTIALLY_FILLED`), in acceptance sequence, for rebuilding books.
    pub fn active_orders(&self) -> rusqlite::Result<Vec<Order>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {ORDER_COLUMNS} FROM orders WHERE status IN ('OPEN', 'PARTIALLY_FILLED') ORDER BY sequence"
            ))?
            .query_map([], |row| order_record(row).map(|record| record.order))?
            .collect()
    }

    /// The highest order sequence ever accepted (rejected orders included), or 0 for an empty database.
    pub fn max_sequence(&self) -> rusqlite::Result<u64> {
        self.conn
            .query_row("SELECT COALESCE(MAX(sequence), 0) FROM orders", [], |row| {
                from_sql(row, 0)
            })
    }

    pub fn order(&self, id: Uuid) -> rusqlite::Result<Option<OrderRecord>> {
        self.conn
            .prepare_cached(&format!("SELECT {ORDER_COLUMNS} FROM orders WHERE id = ?1"))?
            .query_row([id.to_string()], order_record)
            .optional()
    }

    pub fn order_by_client_id(
        &self,
        participant: &ParticipantId,
        client_order_id: &str,
    ) -> rusqlite::Result<Option<OrderRecord>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {ORDER_COLUMNS} FROM orders WHERE participant_id = ?1 AND client_order_id = ?2"
            ))?
            .query_row(params![participant.0, client_order_id], order_record)
            .optional()
    }

    /// Writes a planned book update atomically: the subject order (inserted, or updated if it exists), every resting
    /// order it traded with, its executions, its events, and a pending delivery of each event to every subscription
    /// that wants it (the outbox). Order rows are stamped `now` (domain time). Nothing is written if any statement
    /// fails.
    ///
    /// The writes run in a savepoint: on their own they are one transaction, and inside a group (see
    /// [`Store::begin_group`]) they become part of the group's transaction, durable only at [`Store::commit_group`].
    pub fn commit(
        &mut self,
        update: &BookUpdate,
        new_events: &[NewEvent],
        settlement: Option<SettlementWrite<'_>>,
        outbox: Outbox<'_>,
        now: DateTime<Utc>,
    ) -> rusqlite::Result<()> {
        let now = timestamp(now);
        let tx = self.conn.savepoint()?;
        {
            let mut upsert = tx.prepare_cached(&format!(
                "INSERT INTO orders ({ORDER_COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12)
                 ON CONFLICT (id) DO UPDATE SET remaining_quantity = excluded.remaining_quantity,
                                                status = excluded.status,
                                                updated_at = excluded.updated_at"
            ))?;
            // The subject first: its executions reference it, and a new order must exist before them.
            for order in std::iter::once(&update.order).chain(&update.fills) {
                upsert.execute(params![
                    order.id.to_string(),
                    order.client_order_id,
                    order.instrument_id.0,
                    order.participant_id.0,
                    order.side.as_str(),
                    order.order_type.as_str(),
                    order.limit_price_ticks,
                    to_sql(order.quantity),
                    to_sql(order.remaining_quantity),
                    to_sql(order.sequence),
                    order.status.as_str(),
                    now,
                ])?;
            }
            let mut insert_execution = tx.prepare_cached(
                "INSERT INTO executions (id, instrument_id, buy_order_id, sell_order_id, buyer_id, seller_id,
                                         price_ticks, quantity, executed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?;
            for execution in &update.executions {
                insert_execution.execute(params![
                    execution.id.to_string(),
                    execution.instrument_id.0,
                    execution.buy_order_id.to_string(),
                    execution.sell_order_id.to_string(),
                    execution.buyer_id.0,
                    execution.seller_id.0,
                    execution.price_ticks,
                    to_sql(execution.quantity),
                    timestamp(execution.executed_at),
                ])?;
            }
            write_events(&tx, new_events, &outbox)?;
            if let Some(settlement) = &settlement {
                write_settlement(&tx, settlement)?;
            }
        }
        tx.commit()
    }

    /// Canonical events with `sequence > after`, in sequence order, optionally of one type.
    pub fn events(
        &self,
        after: u64,
        limit: u32,
        event_type: Option<&str>,
    ) -> rusqlite::Result<Vec<EventEnvelope>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {EVENT_COLUMNS} FROM events e
                 WHERE e.sequence > ?1 AND (?2 IS NULL OR e.event_type = ?2)
                 ORDER BY e.sequence LIMIT ?3"
            ))?
            .query_map(params![to_sql(after), event_type, limit], |row| {
                event_row(row, 0)
            })?
            .collect()
    }

    pub fn insert_subscription(&self, subscription: &Subscription) -> rusqlite::Result<()> {
        let event_types =
            serde_json::to_string(&subscription.event_types).expect("strings serialize");
        self.conn
            .prepare_cached(
                "INSERT INTO webhook_subscriptions (id, url, enabled, event_types, created_at, secret, ordered, name, source,
                                                    paused, previous_secret, previous_secret_until, deleted)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
            ])
            .map(drop)
    }

    /// Stores a subscription's url, event types, secrets, ordering, and enabled, paused and deleted flags; false when
    /// there is no such subscription. Its pending deliveries keep their place and go to the new URL; while it is disabled or paused
    /// they wait.
    pub fn update_subscription(&self, subscription: &Subscription) -> rusqlite::Result<bool> {
        let event_types =
            serde_json::to_string(&subscription.event_types).expect("strings serialize");
        self.conn
            .prepare_cached(
                "UPDATE webhook_subscriptions SET url = ?2, enabled = ?3, event_types = ?4, secret = ?5, ordered = ?6,
                                                 paused = ?7, previous_secret = ?8, previous_secret_until = ?9,
                                                 deleted = ?10
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
            ])
            .map(|n| n == 1)
    }

    pub fn subscriptions(&self) -> rusqlite::Result<Vec<Subscription>> {
        self.conn
            .prepare_cached(
                "SELECT id, url, enabled, event_types, created_at, secret, ordered, fault_policy, name, source, paused,
                        previous_secret, previous_secret_until, deleted
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
                })
            })?
            .collect()
    }

    /// Sets or clears a subscription's fault policy; false when there is no such subscription.
    pub fn set_fault_policy(
        &self,
        subscription_id: Uuid,
        policy: Option<&FaultPolicy>,
    ) -> rusqlite::Result<bool> {
        let policy = policy.map(|p| serde_json::to_string(p).expect("policies serialize"));
        self.conn
            .prepare_cached("UPDATE webhook_subscriptions SET fault_policy = ?2 WHERE id = ?1")?
            .execute(params![subscription_id.to_string(), policy])
            .map(|n| n == 1)
    }

    /// Recorded webhook attempts, oldest first, optionally for one subscription.
    pub fn attempts(&self, subscription_id: Option<Uuid>) -> rusqlite::Result<Vec<AttemptRow>> {
        self.conn
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
        &mut self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> rusqlite::Result<Vec<DeliveryJob>> {
        let tx = self.conn.transaction()?;
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
        &mut self,
        results: &[DeliveryResult],
        now: DateTime<Utc>,
    ) -> rusqlite::Result<()> {
        let now = timestamp(now);
        let tx = self.conn.transaction()?;
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
    pub fn virtual_clock(&self) -> rusqlite::Result<Option<(DateTime<Utc>, bool)>> {
        self.conn
            .query_row(
                "SELECT now, running FROM virtual_clock WHERE id = 1",
                [],
                |row| Ok((parsed(row, 0)?, row.get(1)?)),
            )
            .optional()
    }

    /// The latest `occurred_at` of the events the exchange and the simulator stamped from the clock. External
    /// reports carry the reporter's own time, so they are left out.
    pub fn latest_clock_stamp(&self) -> rusqlite::Result<Option<DateTime<Utc>>> {
        let latest: Option<String> = self.conn.query_row(
            "SELECT MAX(occurred_at) FROM events WHERE source <> ?1",
            [crate::events::EXTERNAL_SOURCE],
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

    pub fn save_virtual_clock(&self, now: DateTime<Utc>, running: bool) -> rusqlite::Result<()> {
        self.conn
            .execute(
                "INSERT INTO virtual_clock (id, now, running) VALUES (1, ?1, ?2)
                 ON CONFLICT (id) DO UPDATE SET now = excluded.now, running = excluded.running",
                params![timestamp(now), running],
            )
            .map(drop)
    }

    /// Settlement instructions with a transition still to come, for resuming the simulator.
    pub fn open_settlements(&self) -> rusqlite::Result<Vec<Instruction>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {INSTRUCTION_COLUMNS} FROM settlement_instructions WHERE due_at IS NOT NULL
                 ORDER BY due_at, trade_sequence"
            ))?
            .query_map([], instruction_row)?
            .collect()
    }

    pub fn settlement(&self, execution_id: Uuid) -> rusqlite::Result<Option<Instruction>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {INSTRUCTION_COLUMNS} FROM settlement_instructions WHERE execution_id = ?1"
            ))?
            .query_row([execution_id.to_string()], instruction_row)
            .optional()
    }

    /// Settlement instructions, optionally of one status, in trade order.
    pub fn settlements(
        &self,
        status: Option<SettlementStatus>,
        limit: u32,
    ) -> rusqlite::Result<Vec<Instruction>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {INSTRUCTION_COLUMNS} FROM settlement_instructions
                 WHERE ?1 IS NULL OR status = ?1 ORDER BY trade_sequence LIMIT ?2"
            ))?
            .query_map(
                params![status.map(SettlementStatus::as_str), limit],
                instruction_row,
            )?
            .collect()
    }

    /// The last event sequence a consumer (e.g. the settlement simulator) has taken in; 0 if none.
    pub fn consumer_offset(&self, consumer: &str) -> rusqlite::Result<u64> {
        self.conn
            .query_row(
                "SELECT last_sequence FROM consumer_offsets WHERE consumer = ?1",
                [consumer],
                |row| from_sql(row, 0),
            )
            .optional()
            .map(Option::unwrap_or_default)
    }

    /// Writes a settlement step atomically: every instruction's new state, the lifecycle events with their outbox
    /// rows, and the consumer's new offset.
    pub fn commit_settlement(
        &mut self,
        events: &[NewEvent],
        settlement: SettlementWrite<'_>,
        outbox: Outbox<'_>,
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        write_settlement(&tx, &settlement)?;
        write_events(&tx, events, &outbox)?;
        tx.commit()
    }

    /// The sequence the next event will get: one past the highest ever assigned (AUTOINCREMENT never reuses one).
    pub fn next_event_sequence(&self) -> rusqlite::Result<u64> {
        self.conn.query_row(
            "SELECT MAX(COALESCE((SELECT MAX(sequence) FROM events), 0),
                        COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0)) + 1",
            [],
            |row| from_sql(row, 0),
        )
    }

    /// How many external reports an execution has.
    pub fn report_count(&self, execution_id: Uuid) -> rusqlite::Result<u64> {
        self.conn.query_row(
            "SELECT COUNT(*) FROM settlement_reports WHERE execution_id = ?1",
            [execution_id.to_string()],
            |row| from_sql(row, 0),
        )
    }

    pub fn execution_exists(&self, id: Uuid) -> rusqlite::Result<bool> {
        self.conn
            .prepare_cached("SELECT 1 FROM executions WHERE id = ?1")?
            .exists([id.to_string()])
    }

    pub fn report_by_reference(
        &self,
        execution_id: Uuid,
        reference: &str,
    ) -> rusqlite::Result<Option<ExternalReport>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {REPORT_COLUMNS} FROM settlement_reports WHERE execution_id = ?1 AND reference = ?2"
            ))?
            .query_row(params![execution_id.to_string(), reference], report_row)
            .optional()
    }

    /// External reports for an execution, in order of receipt.
    pub fn reports(&self, execution_id: Uuid) -> rusqlite::Result<Vec<ExternalReport>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {REPORT_COLUMNS} FROM settlement_reports WHERE execution_id = ?1 ORDER BY received_at, rowid"
            ))?
            .query_map([execution_id.to_string()], report_row)?
            .collect()
    }

    /// Stores an external report and publishes its event, atomically.
    pub fn commit_report(
        &mut self,
        report: &ExternalReport,
        event: &NewEvent,
        outbox: Outbox<'_>,
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        tx.prepare_cached(&format!(
            "INSERT INTO settlement_reports ({REPORT_COLUMNS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
        ))?
        .execute(params![
            report.id.to_string(),
            report.execution_id.to_string(),
            report.status.as_str(),
            report.reference,
            report.reason,
            report.details.as_ref().map(|d| d.to_string()),
            timestamp(report.occurred_at),
            timestamp(report.received_at),
        ])?;
        write_events(&tx, std::slice::from_ref(event), &outbox)?;
        tx.commit()
    }

    /// Events about one aggregate (e.g. an execution's settlement lifecycle), optionally from one source.
    pub fn events_for(
        &self,
        aggregate_id: &str,
        source: Option<&str>,
    ) -> rusqlite::Result<Vec<EventEnvelope>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {EVENT_COLUMNS} FROM events e
                 WHERE e.aggregate_id = ?1 AND (?2 IS NULL OR e.source = ?2) ORDER BY e.sequence"
            ))?
            .query_map(params![aggregate_id, source], |row| event_row(row, 0))?
            .collect()
    }

    /// Number of webhook deliveries per status.
    pub fn delivery_counts(&self) -> rusqlite::Result<Vec<(String, u64)>> {
        self.conn
            .prepare_cached(
                "SELECT status, COUNT(*) FROM webhook_deliveries GROUP BY status ORDER BY status",
            )?
            .query_map([], |row| Ok((row.get(0)?, from_sql(row, 1)?)))?
            .collect()
    }

    /// Webhook deliveries with their subscription URL and event, in event order, then subscription; optionally only
    /// one subscription's, or only those with one status (e.g. `DEAD`, the dead letters).
    /// One subscription's deliveries by status, and its oldest unfinished one.
    pub fn delivery_stats(&self, subscription_id: Uuid) -> rusqlite::Result<DeliveryStats> {
        let id = subscription_id.to_string();
        let mut stats = DeliveryStats::default();
        let mut counts = self.conn.prepare_cached(
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
        stats.oldest_unfinished = self
            .conn
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
        &self,
        subscription_id: Option<Uuid>,
        status: Option<&str>,
        after: u64,
        limit: u32,
    ) -> rusqlite::Result<Vec<DeliveryRecord>> {
        self.conn
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

    /// Queues the subscription's events after sequence `after` (of the types it wants) for delivery again, due at
    /// `now`: existing deliveries start over (pending, no attempts, never held), and events from before the
    /// subscription existed get a delivery. Returns how many were queued.
    pub fn redeliver(
        &mut self,
        subscription: &Subscription,
        after: u64,
        now: DateTime<Utc>,
        ids: &IdScheme,
    ) -> rusqlite::Result<usize> {
        let event_types =
            serde_json::to_string(&subscription.event_types).expect("strings serialize");
        let tx = self.conn.transaction()?;
        let queued = {
            let sequences = tx
                .prepare_cached(
                    "SELECT sequence FROM events
                     WHERE sequence > ?1 AND event_type IN (SELECT value FROM json_each(?2))
                     ORDER BY sequence",
                )?
                .query_map(params![to_sql(after), event_types], |row| from_sql(row, 0))?
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
        &mut self,
        subscription_id: Option<Uuid>,
        delivery_ids: &[Uuid],
        now: DateTime<Utc>,
    ) -> rusqlite::Result<usize> {
        let delivery_ids =
            serde_json::to_string(&delivery_ids.iter().map(Uuid::to_string).collect::<Vec<_>>())
                .expect("strings serialize");
        self.conn
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

    /// Deletes every row of every table, in one transaction: the empty state of a new database (the schema stays).
    pub fn reset(&mut self) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        // Rows reference each other; checked at commit, when nothing is left.
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
        // AUTOINCREMENT counters, so sequences start at 1 again.
        tx.execute("DELETE FROM sqlite_sequence", [])?;
        tx.commit()
    }

    /// Makes every in-flight delivery due now. Used at startup, when no claim can still be in progress.
    pub fn release_claims(&self, now: DateTime<Utc>) -> rusqlite::Result<usize> {
        self.conn.execute(
            "UPDATE webhook_deliveries SET status = 'PENDING', next_attempt_at = ?1 WHERE status = 'IN_FLIGHT'",
            [timestamp(now)],
        )
    }
}

/// Stored timestamp form: RFC 3339 UTC with microseconds, so text order is time order.
pub(crate) fn timestamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// Domain quantities and sequences are `u64` but validated to fit SQLite's signed 64-bit integers.
fn to_sql(value: u64) -> i64 {
    i64::try_from(value).expect("domain integers are validated to fit i64")
}

/// A nullable JSON text column.
fn json_column<T: serde::de::DeserializeOwned>(
    row: &Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<T>> {
    let Some(text) = row.get::<_, Option<String>>(index)? else {
        return Ok(None);
    };
    serde_json::from_str(&text)
        .map(Some)
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(e)))
}

fn from_sql(row: &Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value)
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(index, Type::Integer, Box::new(e)))
}

fn parsed<T>(row: &Row<'_>, index: usize) -> rusqlite::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let text: String = row.get(index)?;
    text.parse()
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(e)))
}

/// Where a transaction's events go out: the subscriptions to queue deliveries for, the run's simulation ID, the ID
/// scheme for delivery rows, and the real time at which the deliveries become due.
pub struct Outbox<'a> {
    pub subscriptions: &'a [Subscription],
    pub simulation_id: Option<&'a str>,
    pub ids: IdScheme,
    pub due: DateTime<Utc>,
}

/// Settlement work committed together with something else: instructions' new states and the simulator's cursor.
pub struct SettlementWrite<'a> {
    pub instructions: Vec<&'a Instruction>,
    pub consumer: &'a str,
    pub cursor: u64,
}

/// Appends stamped events to the canonical log at their assigned sequences, and queues one pending delivery per event
/// and interested subscription, inside the caller's transaction.
pub(crate) fn write_events(
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
            events::SCHEMA_VERSION,
            event.source,
            outbox.simulation_id,
            event.aggregate_id,
            event.correlation_id,
            event.payload.to_string(),
            timestamp(event.occurred_at),
        ])?;
        for subscription in outbox
            .subscriptions
            .iter()
            .filter(|s| s.wants(event.event_type))
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

fn write_settlement(tx: &Connection, write: &SettlementWrite<'_>) -> rusqlite::Result<()> {
    let mut upsert = tx.prepare_cached(&format!(
        "INSERT INTO settlement_instructions ({INSTRUCTION_COLUMNS})
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
         ON CONFLICT (execution_id) DO UPDATE SET
           intended_settlement_date = excluded.intended_settlement_date, status = excluded.status,
           attempts = excluded.attempts, delays = excluded.delays, due_at = excluded.due_at,
           settled_at = excluded.settled_at, amount_minor = excluded.amount_minor,
           updated_at = excluded.updated_at"
    ))?;
    for i in &write.instructions {
        upsert.execute(params![
            i.execution_id.to_string(),
            to_sql(i.trade_sequence),
            i.instrument_id.0,
            i.buyer_id.0,
            i.seller_id.0,
            to_sql(i.quantity),
            i.price_ticks,
            i.currency,
            i.currency_scale,
            to_sql(i.tick_size_minor),
            i.amount_minor,
            i.trade_date.to_string(),
            i.intended_settlement_date.to_string(),
            i.status.as_str(),
            i.attempts,
            i.delays,
            i.due_at.map(timestamp),
            i.settled_at.map(timestamp),
            timestamp(i.updated_at),
        ])?;
    }
    tx.execute(
        "INSERT INTO consumer_offsets (consumer, last_sequence) VALUES (?1, ?2)
         ON CONFLICT (consumer) DO UPDATE SET last_sequence = excluded.last_sequence",
        params![write.consumer, to_sql(write.cursor)],
    )?;
    Ok(())
}

const REPORT_COLUMNS: &str =
    "id, execution_id, status, reference, reason, details, occurred_at, received_at";

fn report_row(row: &Row<'_>) -> rusqlite::Result<ExternalReport> {
    let details: Option<String> = row.get(5)?;
    Ok(ExternalReport {
        id: parsed(row, 0)?,
        execution_id: parsed(row, 1)?,
        status: parsed(row, 2)?,
        reference: row.get(3)?,
        reason: row.get(4)?,
        details: details
            .map(|text| serde_json::from_str(&text))
            .transpose()
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(e)))?,
        occurred_at: parsed(row, 6)?,
        received_at: parsed(row, 7)?,
    })
}

const INSTRUCTION_COLUMNS: &str = "execution_id, trade_sequence, instrument_id, buyer_id, seller_id, quantity, \
    price_ticks, currency, currency_scale, tick_size_minor, amount_minor, trade_date, intended_settlement_date, status, \
    attempts, delays, due_at, settled_at, updated_at";

fn instruction_row(row: &Row<'_>) -> rusqlite::Result<Instruction> {
    let date = |index: usize| -> rusqlite::Result<chrono::NaiveDate> { parsed(row, index) };
    let optional_time = |index: usize| -> rusqlite::Result<Option<DateTime<Utc>>> {
        let text: Option<String> = row.get(index)?;
        text.map(|t| {
            t.parse().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(e))
            })
        })
        .transpose()
    };
    Ok(Instruction {
        execution_id: parsed(row, 0)?,
        trade_sequence: from_sql(row, 1)?,
        instrument_id: InstrumentId(row.get(2)?),
        buyer_id: ParticipantId(row.get(3)?),
        seller_id: ParticipantId(row.get(4)?),
        quantity: from_sql(row, 5)?,
        price_ticks: row.get(6)?,
        currency: row.get(7)?,
        currency_scale: row.get(8)?,
        tick_size_minor: from_sql(row, 9)?,
        amount_minor: row.get(10)?,
        trade_date: date(11)?,
        intended_settlement_date: date(12)?,
        status: parsed(row, 13)?,
        attempts: row.get(14)?,
        delays: row.get(15)?,
        due_at: optional_time(16)?,
        settled_at: optional_time(17)?,
        updated_at: parsed(row, 18)?,
    })
}

const EVENT_COLUMNS: &str = "e.event_id, e.sequence, e.event_type, e.schema_version, e.source, e.simulation_id, \
                             e.aggregate_id, e.correlation_id, e.occurred_at, e.payload";

/// Reads the [`EVENT_COLUMNS`] starting at column `at`.
fn event_row(row: &Row<'_>, at: usize) -> rusqlite::Result<EventEnvelope> {
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

fn instrument_row(row: &Row<'_>) -> rusqlite::Result<Instrument> {
    Ok(Instrument {
        id: InstrumentId(row.get(0)?),
        symbol: row.get(1)?,
        mic: row.get(2)?,
        isin: row.get(3)?,
        cusip: row.get(4)?,
        currency: row.get(5)?,
        currency_scale: row.get(6)?,
        tick_size_minor: from_sql(row, 7)?,
        lot_size: from_sql(row, 8)?,
        enabled: row.get(9)?,
    })
}

fn order_record(row: &Row<'_>) -> rusqlite::Result<OrderRecord> {
    Ok(OrderRecord {
        order: Order {
            id: parsed(row, 0)?,
            client_order_id: row.get(1)?,
            instrument_id: InstrumentId(row.get(2)?),
            participant_id: ParticipantId(row.get(3)?),
            side: parsed(row, 4)?,
            order_type: parsed(row, 5)?,
            limit_price_ticks: row.get(6)?,
            quantity: from_sql(row, 7)?,
            remaining_quantity: from_sql(row, 8)?,
            sequence: from_sql(row, 9)?,
            status: parsed(row, 10)?,
        },
        created_at: parsed(row, 11)?,
        updated_at: parsed(row, 12)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_writer_checkpoints_at_the_cap_when_background_checkpoints_stop() {
        let dir = std::env::temp_dir().join(format!("exchange-wal-{}", uuid::Uuid::now_v7()));
        let path = dir.join("exchange.db");
        let wal_bytes = || std::fs::metadata(dir.join("exchange.db-wal")).map_or(0, |m| m.len());
        for fallback in [true, false] {
            let _ = std::fs::remove_dir_all(&dir);
            let conn = super::super::open(&path, std::time::Duration::from_secs(5)).unwrap();
            let mut store = Store::new(conn);
            store
                .checkpoint_in_background(std::time::Duration::from_secs(5), 100)
                .unwrap();
            store.checkpointer.as_mut().unwrap().stop_for_test();
            store
                .conn
                .execute_batch("CREATE TABLE filler (data BLOB)")
                .unwrap();
            for _ in 0..2000 {
                store
                    .conn
                    .execute("INSERT INTO filler VALUES (zeroblob(4000))", [])
                    .unwrap();
                if fallback {
                    store.request_checkpoint_if_due();
                }
            }
            let wal = wal_bytes();
            if fallback {
                // With no background checkpoint to finish the job cheaply, the writer checkpoints at the cap:
                // 8 x 100 pages of 4 KiB, plus the commit that crossed it.
                assert!(wal < 4 << 20, "the writer bounded the WAL: {wal} bytes");
            } else {
                assert!(wal > 6 << 20, "without it the WAL grows: {wal} bytes");
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn background_checkpoints_are_asked_for_once_per_threshold_of_growth() {
        let dir = std::env::temp_dir().join(format!("exchange-wal-{}", uuid::Uuid::now_v7()));
        let conn =
            super::super::open(dir.join("exchange.db"), std::time::Duration::from_secs(5)).unwrap();
        let mut store = Store::new(conn);
        store
            .checkpoint_in_background(std::time::Duration::from_secs(5), 100)
            .unwrap();
        store
            .conn
            .execute_batch("CREATE TABLE filler (data BLOB)")
            .unwrap();
        // About two pages per insert: some 4000 frames in all, so about 40 requests of one per 100 pages. Asking after
        // every write past the threshold instead would checkpoint after most of the 2000 writes.
        for _ in 0..2000 {
            store
                .conn
                .execute("INSERT INTO filler VALUES (zeroblob(4000))", [])
                .unwrap();
            store.request_checkpoint_if_due();
        }
        let checkpointer = store.checkpointer.as_mut().unwrap();
        checkpointer.stop_for_test();
        let runs = checkpointer.runs();
        assert!((1..=100).contains(&runs), "{runs} background checkpoints");
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
