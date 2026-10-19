use chrono::{DateTime, Utc};
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Row, params};
use simulator_kit::outbox::{
    self, AttemptRow, DeliveryRecord, DeliveryStats, EVENT_COLUMNS, Outbox, event_row, write_events,
};
use simulator_kit::persistence::sql::{from_sql, parsed, timestamp, to_sql};
use uuid::Uuid;

use crate::clearing::{Draft, Statement, StoredStatement};
use crate::domain::{Execution, Instrument, InstrumentId, Order, Participant, ParticipantId};
use crate::engine::BookUpdate;
use crate::events::{self, EventEnvelope, NewEvent};
use crate::ids::IdScheme;
use crate::settlement::{ExternalReport, Instruction, SettlementStatus};
use crate::webhooks::{DeliveryJob, DeliveryResult, FaultPolicy, Subscription};

/// Repository over the single writer connection. Every state change a matching step produces is written by
/// [`Store::commit`] in one transaction.
pub struct Store {
    conn: Connection,
    /// Set when WAL checkpoints run in the background (see [`Store::checkpoint_in_background`]). Declared after
    /// `conn`, so the writer closes first and the checkpointer's connection, the last one, removes the WAL.
    checkpointer: Option<simulator_kit::persistence::checkpoint::Checkpointer>,
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

/// An instrument's last trade and recent volume ([`Store::trade_summary`]).
#[derive(Debug)]
pub struct TradeSummary {
    pub instrument_id: InstrumentId,
    pub last_price_ticks: i64,
    pub last_quantity: u64,
    pub last_executed_at: DateTime<Utc>,
    /// Quantity traded since the time asked for.
    pub volume_since: u64,
}

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
        self.checkpointer = Some(simulator_kit::persistence::checkpoint::Checkpointer::start(
            &self.conn,
            &path,
            busy_timeout,
            pages,
        )?);
        Ok(())
    }

    /// With background checkpoints, asks for one each time the WAL has grown by the threshold. Once the WAL holds four
    /// times the threshold, the writer checkpoints itself, which bounds the WAL (see `simulator_kit::persistence::checkpoint`). Call it on the writer's thread after writing, outside a
    /// transaction; the exchange thread does so after every command or commit group.
    pub fn request_checkpoint_if_due(&self) {
        if let Some(checkpointer) = &self.checkpointer {
            checkpointer.after_write(&self.conn);
        }
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// The writer connection, for shared components that write through it (the webhook hub).
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
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
        insert_instrument(&self.conn, instrument)
    }

    /// Lists a new instrument (`insert`) or stores a listed one's changeable fields, together with its event, in one
    /// transaction. `price_source_since` is stored when the price source (re)started.
    pub fn commit_instrument(
        &mut self,
        instrument: &Instrument,
        insert: bool,
        price_source_since: Option<DateTime<Utc>>,
        event: &NewEvent,
        outbox: Outbox<'_>,
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        if insert {
            insert_instrument(&tx, instrument)?;
        } else {
            tx.prepare_cached(
                "UPDATE instruments SET enabled = ?2, reference_symbol = ?3, price_source = ?4, liquidity = ?5
                 WHERE id = ?1",
            )?
            .execute(params![
                instrument.id.0,
                instrument.enabled,
                instrument.reference_symbol,
                price_source_json(instrument),
                liquidity_json(instrument),
            ])?;
        }
        if let Some(since) = price_source_since {
            set_price_source_since(&tx, &instrument.id, since)?;
        }
        write_events(&tx, std::slice::from_ref(event), &outbox)?;
        tx.commit()
    }

    /// When each instrument's price source started; instruments listed before 6.2 have none.
    pub fn price_source_since(
        &self,
    ) -> rusqlite::Result<std::collections::HashMap<InstrumentId, DateTime<Utc>>> {
        let mut statement = self.conn.prepare_cached(
            "SELECT id, price_source_since FROM instruments WHERE price_source_since IS NOT NULL",
        )?;
        let rows =
            statement.query_map([], |row| Ok((InstrumentId(row.get(0)?), parsed(row, 1)?)))?;
        rows.collect()
    }

    pub fn save_price_source_since(
        &self,
        id: &InstrumentId,
        since: DateTime<Utc>,
    ) -> rusqlite::Result<()> {
        set_price_source_since(&self.conn, id, since)
    }

    pub fn instruments(&self, filter: &InstrumentFilter) -> rusqlite::Result<Vec<Instrument>> {
        self.conn
            .prepare_cached(
                "SELECT id, symbol, mic, isin, cusip, currency, currency_scale, tick_size_minor, lot_size, enabled,
                        reference_symbol, price_source, liquidity
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
                "SELECT id, symbol, mic, isin, cusip, currency, currency_scale, tick_size_minor, lot_size, enabled,
                        reference_symbol, price_source, liquidity
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
        outbox::events(&self.conn, after, limit, event_type)
    }

    pub fn insert_subscription(&self, subscription: &Subscription) -> rusqlite::Result<()> {
        outbox::insert_subscription(&self.conn, subscription)
    }

    /// Stores a subscription's url, event types, secrets, ordering, and enabled, paused and deleted flags; false when
    /// there is no such subscription. Its pending deliveries keep their place and go to the new URL; while it is disabled or paused
    /// they wait.
    pub fn update_subscription(&self, subscription: &Subscription) -> rusqlite::Result<bool> {
        outbox::update_subscription(&self.conn, subscription)
    }

    pub fn subscriptions(&self) -> rusqlite::Result<Vec<Subscription>> {
        outbox::subscriptions(&self.conn)
    }

    /// Sets or clears a subscription's fault policy; false when there is no such subscription.
    pub fn set_fault_policy(
        &self,
        subscription_id: Uuid,
        policy: Option<&FaultPolicy>,
    ) -> rusqlite::Result<bool> {
        outbox::set_fault_policy(&self.conn, subscription_id, policy)
    }

    /// Recorded webhook attempts, oldest first, optionally for one subscription.
    pub fn attempts(&self, subscription_id: Option<Uuid>) -> rusqlite::Result<Vec<AttemptRow>> {
        outbox::attempts(&self.conn, subscription_id)
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
        outbox::claim_deliveries(&mut self.conn, now, lease_until, limit)
    }

    /// Records attempt outcomes. Each counts as one attempt, except a hold. Only in-flight deliveries are updated, so a
    /// late result for a delivery that was meanwhile re-claimed and finished does not overwrite it. What was sent is
    /// recorded in `webhook_attempts` either way: it happened.
    pub fn record_deliveries(
        &mut self,
        results: &[DeliveryResult],
        now: DateTime<Utc>,
    ) -> rusqlite::Result<()> {
        outbox::record_deliveries(&mut self.conn, results, now)
    }

    /// The stored virtual clock reading and whether it was running, if a virtual clock was ever used.
    pub fn virtual_clock(&self) -> rusqlite::Result<Option<(DateTime<Utc>, bool)>> {
        outbox::virtual_clock(&self.conn)
    }

    /// The latest `occurred_at` of the events the exchange and the simulator stamped from the clock. External
    /// reports carry the reporter's own time, so they are left out.
    pub fn latest_clock_stamp(&self) -> rusqlite::Result<Option<DateTime<Utc>>> {
        outbox::latest_clock_stamp(&self.conn, events::EXTERNAL_SOURCE)
    }

    pub fn save_virtual_clock(&self, now: DateTime<Utc>, running: bool) -> rusqlite::Result<()> {
        outbox::save_virtual_clock(&self.conn, now, running)
    }

    /// Stores the virtual clock's reading with its `ClockChanged`, in one transaction.
    pub fn commit_clock(
        &mut self,
        now: DateTime<Utc>,
        running: bool,
        event: &NewEvent,
        outbox: Outbox<'_>,
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        outbox::save_virtual_clock(&tx, now, running)?;
        write_events(&tx, std::slice::from_ref(event), &outbox)?;
        tx.commit()
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

    /// `TradeExecuted` events after sequence `after` executed in `[from, until)`, in sequence order: one clearing
    /// window. Reading from `after` (a range of the primary key) keeps it cheap without an index on event times.
    pub fn trades_between(
        &self,
        after: u64,
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> rusqlite::Result<Vec<EventEnvelope>> {
        self.conn
            .prepare_cached(&format!(
                "SELECT {EVENT_COLUMNS} FROM events e
                 WHERE e.sequence > ?1 AND e.event_type = ?2 AND e.occurred_at >= ?3 AND e.occurred_at < ?4
                 ORDER BY e.sequence"
            ))?
            .query_map(
                params![
                    to_sql(after),
                    events::TRADE_EXECUTED,
                    timestamp(from),
                    timestamp(until)
                ],
                |row| event_row(row, 0),
            )?
            .collect()
    }

    /// The last business date whose clearing cutoff was handled and the last trade sequence its window took, if
    /// clearing ever ran.
    pub fn clearing_cursor(&self) -> rusqlite::Result<Option<(chrono::NaiveDate, u64)>> {
        self.conn
            .query_row(
                "SELECT business_date, after_sequence FROM clearing_cursor WHERE id = 1",
                [],
                |row| Ok((parsed(row, 0)?, from_sql(row, 1)?)),
            )
            .optional()
    }

    /// The last business date closed with `BusinessDayClosed`, once one was.
    pub fn business_day_cursor(&self) -> rusqlite::Result<Option<chrono::NaiveDate>> {
        self.conn
            .query_row(
                "SELECT business_date FROM business_day_cursor WHERE id = 1",
                [],
                |row| parsed(row, 0),
            )
            .optional()
    }

    pub fn save_business_day_cursor(&self, date: chrono::NaiveDate) -> rusqlite::Result<()> {
        save_business_day_cursor(&self.conn, date)
    }

    /// Closes a business day: its `BusinessDayClosed` and the moved cursor, in one transaction.
    pub fn commit_business_day(
        &mut self,
        date: chrono::NaiveDate,
        event: &NewEvent,
        outbox: Outbox<'_>,
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        save_business_day_cursor(&tx, date)?;
        write_events(&tx, std::slice::from_ref(event), &outbox)?;
        tx.commit()
    }

    pub fn save_clearing_cursor(
        &self,
        date: chrono::NaiveDate,
        after: u64,
    ) -> rusqlite::Result<()> {
        save_clearing_cursor(&self.conn, date, after)
    }

    /// Statements, optionally of one business date and one participant, in date and participant order. `pending`
    /// selects the issued ones (`false`) or the late ones still waiting (`true`).
    pub fn clearing_statements(
        &self,
        date: Option<chrono::NaiveDate>,
        participant: Option<&str>,
        pending: bool,
    ) -> rusqlite::Result<Vec<StoredStatement>> {
        self.conn
            .prepare_cached(
                "SELECT body, breaks, issued, event_sequence FROM clearing_statements
                 WHERE (?1 IS NULL OR business_date = ?1) AND (?2 IS NULL OR participant_id = ?2) AND issued = ?3
                 ORDER BY business_date, participant_id",
            )?
            .query_map(
                params![date.map(|d| d.to_string()), participant, !pending],
                statement_row,
            )?
            .collect()
    }

    pub fn clearing_statement(&self, id: &str) -> rusqlite::Result<Option<StoredStatement>> {
        self.conn
            .query_row(
                "SELECT body, breaks, issued, event_sequence FROM clearing_statements WHERE statement_id = ?1",
                [id],
                statement_row,
            )
            .optional()
    }

    /// Writes a clearing step atomically: new and newly issued statements, their events with outbox rows, and the
    /// cursor.
    pub fn commit_clearing(
        &mut self,
        events: &[NewEvent],
        statements: &[&StoredStatement],
        cursor: Option<(chrono::NaiveDate, u64)>,
        outbox: Outbox<'_>,
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        write_events(&tx, events, &outbox)?;
        {
            let mut upsert = tx.prepare_cached(
                "INSERT INTO clearing_statements
                   (statement_id, business_date, participant_id, due_at, issued, event_sequence, body, breaks)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT (statement_id) DO UPDATE SET issued = excluded.issued,
                                                          event_sequence = excluded.event_sequence",
            )?;
            for stored in statements {
                let Draft { statement, breaks } = &stored.draft;
                upsert.execute(params![
                    statement.statement_id,
                    statement.business_date.to_string(),
                    statement.participant_id.0,
                    timestamp(statement.issued_at),
                    stored.issued,
                    stored.event_sequence.map(to_sql),
                    serde_json::to_string(statement).expect("a statement serializes"),
                    serde_json::to_string(breaks).expect("breaks serialize"),
                ])?;
            }
        }
        if let Some((date, after)) = cursor {
            save_clearing_cursor(&tx, date, after)?;
        }
        tx.commit()
    }

    /// The sequence the next event will get: one past the highest ever assigned (AUTOINCREMENT never reuses one).
    pub fn next_event_sequence(&self) -> rusqlite::Result<u64> {
        outbox::next_event_sequence(&self.conn)
    }

    /// How many external reports an execution has.
    pub fn report_count(&self, execution_id: Uuid) -> rusqlite::Result<u64> {
        self.conn.query_row(
            "SELECT COUNT(*) FROM settlement_reports WHERE execution_id = ?1",
            [execution_id.to_string()],
            |row| from_sql(row, 0),
        )
    }

    /// Per instrument that traded: its last trade and the quantity traded since `since`, in instrument ID order.
    pub fn trade_summary(&self, since: DateTime<Utc>) -> rusqlite::Result<Vec<TradeSummary>> {
        self.conn
            .prepare_cached(
                "SELECT e.instrument_id, e.price_ticks, e.quantity, e.executed_at,
                        (SELECT COALESCE(SUM(v.quantity), 0) FROM executions v
                         WHERE v.instrument_id = e.instrument_id AND v.executed_at >= ?1)
                 FROM executions e
                 WHERE e.rowid = (SELECT MAX(l.rowid) FROM executions l WHERE l.instrument_id = e.instrument_id)
                 ORDER BY e.instrument_id",
            )?
            .query_map([timestamp(since)], |row| {
                Ok(TradeSummary {
                    instrument_id: InstrumentId(row.get(0)?),
                    last_price_ticks: row.get(1)?,
                    last_quantity: from_sql(row, 2)?,
                    last_executed_at: parsed(row, 3)?,
                    volume_since: from_sql(row, 4)?,
                })
            })?
            .collect()
    }

    /// A participant's net traded quantity per instrument (bought minus sold), for instruments it traded.
    pub fn net_positions(
        &self,
        participant: &ParticipantId,
    ) -> rusqlite::Result<Vec<(InstrumentId, i64)>> {
        self.conn
            .prepare_cached(
                "SELECT instrument_id,
                        SUM(CASE WHEN buyer_id = ?1 THEN quantity ELSE -quantity END)
                 FROM executions WHERE buyer_id = ?1 OR seller_id = ?1
                 GROUP BY instrument_id ORDER BY instrument_id",
            )?
            .query_map([&participant.0], |row| {
                Ok((InstrumentId(row.get(0)?), row.get(1)?))
            })?
            .collect()
    }

    /// The buyer and seller of an execution; `None` when there is no such execution.
    pub fn execution_parties(&self, id: Uuid) -> rusqlite::Result<Option<(String, String)>> {
        self.conn
            .prepare_cached("SELECT buyer_id, seller_id FROM executions WHERE id = ?1")?
            .query_row([id.to_string()], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()
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
        outbox::events_for(&self.conn, aggregate_id, source)
    }

    /// Number of webhook deliveries per status.
    pub fn delivery_counts(&self) -> rusqlite::Result<Vec<(String, u64)>> {
        outbox::delivery_counts(&self.conn)
    }

    /// Webhook deliveries with their subscription URL and event, in event order, then subscription; optionally only
    /// one subscription's, or only those with one status (e.g. `DEAD`, the dead letters).
    /// One subscription's deliveries by status, and its oldest unfinished one.
    pub fn delivery_stats(&self, subscription_id: Uuid) -> rusqlite::Result<DeliveryStats> {
        outbox::delivery_stats(&self.conn, subscription_id)
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
        outbox::deliveries(&self.conn, subscription_id, status, after, limit)
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
        outbox::redeliver(
            &mut self.conn,
            subscription,
            after,
            now,
            ids,
            Some(events::parties),
        )
    }

    /// Gives dead-lettered deliveries a fresh attempt budget, due at `now`: all of them, one subscription's, or the
    /// listed ones. Returns how many.
    pub fn retry_dead_letters(
        &mut self,
        subscription_id: Option<Uuid>,
        delivery_ids: &[Uuid],
        now: DateTime<Utc>,
    ) -> rusqlite::Result<usize> {
        outbox::retry_dead_letters(&mut self.conn, subscription_id, delivery_ids, now)
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
        outbox::release_claims(&self.conn, now)
    }
}

/// Settlement work committed together with something else: instructions' new states and the simulator's cursor.
pub struct SettlementWrite<'a> {
    pub instructions: Vec<&'a Instruction>,
    pub consumer: &'a str,
    pub cursor: u64,
}

fn write_settlement(tx: &Connection, write: &SettlementWrite<'_>) -> rusqlite::Result<()> {
    let mut upsert = tx.prepare_cached(&format!(
        "INSERT INTO settlement_instructions ({INSTRUCTION_COLUMNS})
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)
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
            i.clearing
                .as_ref()
                .map(|refs| serde_json::to_string(refs).expect("references serialize")),
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
    attempts, delays, due_at, settled_at, updated_at, clearing";

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
        clearing: row
            .get::<_, Option<String>>(19)?
            .map(|text| {
                serde_json::from_str(&text).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(19, Type::Text, Box::new(e))
                })
            })
            .transpose()?,
    })
}

fn save_business_day_cursor(conn: &Connection, date: chrono::NaiveDate) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO business_day_cursor (id, business_date) VALUES (1, ?1)
         ON CONFLICT (id) DO UPDATE SET business_date = excluded.business_date",
        [date.to_string()],
    )
    .map(drop)
}

fn save_clearing_cursor(
    conn: &Connection,
    date: chrono::NaiveDate,
    after: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO clearing_cursor (id, business_date, after_sequence) VALUES (1, ?1, ?2)
         ON CONFLICT (id) DO UPDATE SET business_date = excluded.business_date,
                                        after_sequence = excluded.after_sequence",
        params![date.to_string(), to_sql(after)],
    )
    .map(drop)
}

fn statement_row(row: &Row<'_>) -> rusqlite::Result<StoredStatement> {
    let json = |index: usize| -> rusqlite::Result<String> { row.get(index) };
    let invalid = |index: usize| {
        move |e: serde_json::Error| {
            rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(e))
        }
    };
    let statement: Statement = serde_json::from_str(&json(0)?).map_err(invalid(0))?;
    let breaks = serde_json::from_str(&json(1)?).map_err(invalid(1))?;
    let sequence: Option<i64> = row.get(3)?;
    Ok(StoredStatement {
        draft: Draft { statement, breaks },
        issued: row.get(2)?,
        event_sequence: sequence.map(|s| s as u64),
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
        reference_symbol: row.get(10)?,
        price_source: row
            .get::<_, Option<String>>(11)?
            .map(|json| {
                serde_json::from_str(&json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(11, Type::Text, Box::new(e))
                })
            })
            .transpose()?,
        liquidity: row
            .get::<_, Option<String>>(12)?
            .map(|json| {
                serde_json::from_str(&json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(12, Type::Text, Box::new(e))
                })
            })
            .transpose()?,
    })
}

fn liquidity_json(instrument: &Instrument) -> Option<String> {
    instrument
        .liquidity
        .as_ref()
        .map(|spec| serde_json::to_string(spec).expect("liquidity settings serialize"))
}

fn price_source_json(instrument: &Instrument) -> Option<String> {
    instrument
        .price_source
        .as_ref()
        .map(|spec| serde_json::to_string(spec).expect("a price source serializes"))
}

fn set_price_source_since(
    conn: &Connection,
    id: &InstrumentId,
    since: DateTime<Utc>,
) -> rusqlite::Result<()> {
    conn.prepare_cached("UPDATE instruments SET price_source_since = ?2 WHERE id = ?1")?
        .execute(params![id.0, timestamp(since)])
        .map(drop)
}

fn insert_instrument(conn: &Connection, instrument: &Instrument) -> rusqlite::Result<()> {
    conn.prepare_cached(
        "INSERT INTO instruments (id, symbol, mic, isin, cusip, currency, currency_scale, tick_size_minor, lot_size,
                                  enabled, reference_symbol, price_source, liquidity)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
        instrument.reference_symbol,
        price_source_json(instrument),
        liquidity_json(instrument),
    ])
    .map(drop)
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
