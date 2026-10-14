use chrono::{DateTime, SecondsFormat, Utc};
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, params};
use uuid::Uuid;

use crate::domain::{Execution, Instrument, InstrumentId, Order, Participant, ParticipantId};
use crate::engine::BookUpdate;
use crate::events::{self, EventEnvelope, NewEvent};
use crate::settlement::{Instruction, SettlementStatus, Transition};
use crate::webhooks::{DeliveryJob, DeliveryOutcome, DeliveryResult, Subscription};

/// Repository over the single writer connection. Every state change a matching step produces is written by
/// [`Store::commit`] in one transaction.
pub struct Store {
    conn: Connection,
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
        Self { conn }
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
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
    pub fn commit(
        &mut self,
        update: &BookUpdate,
        new_events: &[NewEvent],
        outbox: Outbox<'_>,
        now: DateTime<Utc>,
    ) -> rusqlite::Result<()> {
        let now = timestamp(now);
        let tx = self.conn.transaction()?;
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
            write_events(&tx, new_events, outbox)?;
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
                "INSERT INTO webhook_subscriptions (id, url, enabled, event_types, created_at, secret, ordered)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?
            .execute(params![
                subscription.id.to_string(),
                subscription.url,
                subscription.enabled,
                event_types,
                timestamp(subscription.created_at),
                subscription.secret,
                subscription.ordered,
            ])
            .map(drop)
    }

    pub fn subscriptions(&self) -> rusqlite::Result<Vec<Subscription>> {
        self.conn
            .prepare_cached(
                "SELECT id, url, enabled, event_types, created_at, secret, ordered
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
                "SELECT d.id, d.subscription_id, s.url, d.attempts, {EVENT_COLUMNS}, s.secret
                 FROM webhook_deliveries d
                 JOIN webhook_subscriptions s ON s.id = d.subscription_id
                 JOIN events e ON e.sequence = d.event_sequence
                 WHERE d.status IN ('PENDING', 'IN_FLIGHT') AND d.next_attempt_at <= ?1
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

    /// Records attempt outcomes. Each counts as one attempt. Only in-flight deliveries are updated, so a late result
    /// for a delivery that was meanwhile re-claimed and finished does not overwrite it.
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
            for result in results {
                let (status, next, error, delivered) = match &result.outcome {
                    DeliveryOutcome::Delivered => ("DELIVERED", None, None, Some(now.clone())),
                    DeliveryOutcome::Retry { at, error } => {
                        ("PENDING", Some(timestamp(*at)), Some(error.as_str()), None)
                    }
                    DeliveryOutcome::Dead { error } => ("DEAD", None, Some(error.as_str()), None),
                };
                update.execute(params![
                    result.delivery_id.to_string(),
                    status,
                    next,
                    error,
                    delivered
                ])?;
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
        transitions: &[Transition],
        consumer: &str,
        cursor: u64,
        outbox: Outbox<'_>,
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut upsert = tx.prepare_cached(&format!(
                "INSERT INTO settlement_instructions ({INSTRUCTION_COLUMNS})
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
                 ON CONFLICT (execution_id) DO UPDATE SET
                   intended_settlement_date = excluded.intended_settlement_date, status = excluded.status,
                   attempts = excluded.attempts, delays = excluded.delays, due_at = excluded.due_at,
                   settled_at = excluded.settled_at, amount_minor = excluded.amount_minor,
                   updated_at = excluded.updated_at"
            ))?;
            for transition in transitions {
                let i = &transition.instruction;
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
                params![consumer, to_sql(cursor)],
            )?;
            let events: Vec<NewEvent> = transitions.iter().map(|t| t.event.clone()).collect();
            write_events(&tx, &events, outbox)?;
        }
        tx.commit()
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
/// source for delivery rows, and the real time at which the deliveries become due.
pub struct Outbox<'a> {
    pub subscriptions: &'a [Subscription],
    pub simulation_id: Option<&'a str>,
    pub ids: &'a mut dyn crate::engine::IdGenerator,
    pub due: DateTime<Utc>,
}

/// Appends events to the canonical log and queues one pending delivery per event and interested subscription, inside
/// the caller's transaction.
pub(crate) fn write_events(
    tx: &rusqlite::Transaction<'_>,
    new_events: &[NewEvent],
    outbox: Outbox<'_>,
) -> rusqlite::Result<()> {
    let due = timestamp(outbox.due);
    let mut insert_event = tx.prepare_cached(
        "INSERT INTO events (event_id, event_type, schema_version, source, simulation_id, aggregate_id,
                             correlation_id, payload, occurred_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?;
    let mut insert_delivery = tx.prepare_cached(
        "INSERT INTO webhook_deliveries (id, subscription_id, event_sequence, status, attempts, next_attempt_at)
         VALUES (?1, ?2, ?3, 'PENDING', 0, ?4)",
    )?;
    for event in new_events {
        insert_event.execute(params![
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
        let sequence = tx.last_insert_rowid();
        for subscription in outbox
            .subscriptions
            .iter()
            .filter(|s| s.wants(event.event_type))
        {
            insert_delivery.execute(params![
                outbox.ids.next_id().to_string(),
                subscription.id.to_string(),
                sequence,
                due
            ])?;
        }
    }
    Ok(())
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
