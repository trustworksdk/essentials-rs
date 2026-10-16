//! Benchmark harness: in-memory matching, durable (SQLite) order processing, and the HTTP service end to end, on one
//! seeded workload. See `bench/README.md`. Run with `--release`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use exchange_simulator::app::App;
use exchange_simulator::config::{
    CheckpointMode, ClockConfig, ClockMode, Config, DatabaseConfig, EventsConfig, ExchangeConfig,
    GroupCommitConfig, IdsConfig, LogFormat, LoggingConfig, PersistenceMode, ServerConfig,
    SettlementConfig, TestControlConfig, WebhooksConfig,
};
use exchange_simulator::domain::{
    IdentifierPolicy, Instrument, InstrumentId, Order, OrderStatus, Participant, ParticipantId,
};
use exchange_simulator::engine::{
    Exchange, ExchangeOptions, IdScheme, MatchContext, OrderBook, SubmitOrder,
};
use exchange_simulator::events::EVENT_TYPES;
use exchange_simulator::market::{
    Distribution, InstrumentMarket, MarketAction, MarketConfig, Participants, PriceDistribution,
    QuantityDistribution,
};
use exchange_simulator::persistence::{self, Store};
use exchange_simulator::webhooks::NewSubscription;

const USAGE: &str = "usage: bench [all|matching|durable|http|checkpoint|open-loop] [--quick] [--seed N] \
                     [--matching-orders N] [--durable-orders N] [--http-orders N] [--concurrency N] [--rates R,R,..] \
                     [--seconds S] [--queue-capacity N] [--max-in-flight N] [--group-commit N] [--group-wait-us N] \
                     [--checkpoint background|inline] [--persistence durable|fast|memory] [--dir DIR] [--out FILE]

--dir: where the SQLite files go (default: the system temp dir). Check the reported fsync latency: on tmpfs it is
       ~0 and the \"durable\" numbers measure SQLite in RAM.
open-loop (not part of all): for each target rate in --rates (orders/s, default 1000,2000,4000,8000), sends orders
       on a fixed schedule for --seconds (default 10; 3 with --quick) whatever the responses, and measures each
       latency from the order's scheduled send time. --queue-capacity is the service's command queue bound (default
       10000), --max-in-flight the client's limit on open requests (default 16384).
--group-commit: most orders per commit in the service, and the group size of the grouped durable row (default 256;
       1 commits each order on its own). --group-wait-us: how long the service waits for more orders (default 0).
--checkpoint: where WAL checkpoints run for the durable and service rows (default background, as in the service;
       inline is SQLite's automatic checkpoint inside the commit). The checkpoint mode compares both.
--persistence: database.mode for the durable and service rows (default durable). The checkpoint mode always runs
       durable.";

struct Options {
    mode: String,
    seed: u64,
    matching_orders: usize,
    durable_orders: usize,
    http_orders: usize,
    concurrency: usize,
    rates: Vec<f64>,
    seconds: f64,
    queue_capacity: usize,
    max_in_flight: usize,
    group_commit: GroupCommitConfig,
    checkpoint: CheckpointMode,
    persistence: PersistenceMode,
    dir: std::path::PathBuf,
    out: Option<String>,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let mut options = Self {
            mode: "all".into(),
            seed: 42,
            matching_orders: 1_000_000,
            durable_orders: 50_000,
            http_orders: 50_000,
            concurrency: 32,
            rates: vec![1000.0, 2000.0, 4000.0, 8000.0],
            seconds: 10.0,
            queue_capacity: 10_000,
            max_in_flight: 16_384,
            group_commit: GroupCommitConfig::default(),
            checkpoint: CheckpointMode::Background,
            persistence: PersistenceMode::Durable,
            dir: std::env::temp_dir(),
            out: None,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let mut value = |name: &str| args.next().ok_or(format!("{name} needs a value"));
            let number = |text: String| text.parse::<usize>().map_err(|e| format!("{text}: {e}"));
            match arg.as_str() {
                "all" | "matching" | "durable" | "http" | "checkpoint" | "open-loop" => {
                    options.mode = arg
                }
                "--quick" => {
                    options.matching_orders = 200_000;
                    options.durable_orders = 10_000;
                    options.http_orders = 10_000;
                    options.seconds = 3.0;
                }
                "--seed" => options.seed = number(value("--seed")?)? as u64,
                "--matching-orders" => {
                    options.matching_orders = number(value("--matching-orders")?)?
                }
                "--durable-orders" => options.durable_orders = number(value("--durable-orders")?)?,
                "--http-orders" => options.http_orders = number(value("--http-orders")?)?,
                "--concurrency" => options.concurrency = number(value("--concurrency")?)?.max(1),
                "--rates" => {
                    options.rates = value("--rates")?
                        .split(',')
                        .map(|r| match r.trim().parse::<f64>() {
                            Ok(rate) if rate > 0.0 && rate.is_finite() => Ok(rate),
                            _ => Err(format!("--rates: {r} is not a positive rate")),
                        })
                        .collect::<Result<_, _>>()?
                }
                "--seconds" => {
                    let text = value("--seconds")?;
                    options.seconds = text
                        .parse::<f64>()
                        .ok()
                        .filter(|s| *s > 0.0 && s.is_finite())
                        .ok_or(format!("--seconds: {text} is not a positive duration"))?
                }
                "--queue-capacity" => {
                    options.queue_capacity = number(value("--queue-capacity")?)?.max(1)
                }
                "--persistence" => {
                    let text = value("--persistence")?;
                    options.persistence = PersistenceMode::ALL
                        .into_iter()
                        .find(|m| m.as_str() == text)
                        .ok_or(format!(
                            "--persistence: {text} is not durable, fast or memory"
                        ))?
                }
                "--checkpoint" => {
                    options.checkpoint = match value("--checkpoint")?.as_str() {
                        "background" => CheckpointMode::Background,
                        "inline" => CheckpointMode::Inline,
                        other => {
                            return Err(format!(
                                "--checkpoint: {other} is not background or inline"
                            ));
                        }
                    }
                }
                "--group-commit" => {
                    options.group_commit.max_commands = number(value("--group-commit")?)?.max(1)
                }
                "--group-wait-us" => {
                    options.group_commit.max_wait_us = number(value("--group-wait-us")?)? as u64
                }
                "--max-in-flight" => {
                    options.max_in_flight = number(value("--max-in-flight")?)?.max(1)
                }
                "--out" => options.out = Some(value("--out")?),
                "--dir" => options.dir = value("--dir")?.into(),
                "-h" | "--help" => return Err(USAGE.into()),
                other => return Err(format!("unknown argument {other}\n{USAGE}")),
            }
        }
        Ok(options)
    }
}

fn acme() -> Instrument {
    Instrument {
        id: InstrumentId("ACME".into()),
        symbol: "ACME".into(),
        mic: Some("XCSE".into()),
        isin: None,
        cusip: None,
        currency: "DKK".into(),
        currency_scale: 2,
        tick_size_minor: 1,
        lot_size: 1,
        enabled: true,
    }
}

const PARTICIPANTS: usize = 50;

/// The workload, a random market (`exchange_simulator::market`): 80 % limit orders priced uniformly within ±50 ticks
/// of 100.00, 20 % market orders, sides even, quantities 1–100, 50 participants. Limit prices on both sides overlap,
/// so roughly half the orders trade.
fn market(seed: u64, n: usize) -> MarketConfig {
    MarketConfig {
        seed: Some(seed),
        orders: n,
        orders_per_second: 0.0,
        participants: Participants::Count(PARTICIPANTS),
        buy_probability: 0.5,
        market_order_probability: 0.2,
        cancellation_probability: 0.0,
        instruments: vec![InstrumentMarket {
            id: "ACME".into(),
            weight: 1.0,
            price: PriceDistribution {
                mid_ticks: 10_000,
                width_ticks: 50,
                skew_ticks: 0,
                distribution: Distribution::Uniform,
            },
            quantity: QuantityDistribution {
                min: 1,
                max: 100,
                lot: 1,
            },
        }],
    }
}

fn workload(seed: u64, n: usize) -> Vec<SubmitOrder> {
    let market = market(seed, n);
    market.validate().expect("valid workload");
    market
        .generate()
        .map(|command| match command.action {
            MarketAction::Submit(order) => order,
            MarketAction::Cancel { .. } => unreachable!("the workload has no cancellations"),
        })
        .collect()
}

fn participant_ids() -> Vec<String> {
    Participants::Count(PARTICIPANTS).ids()
}

#[derive(Default)]
struct Latencies(Vec<Duration>);

impl Latencies {
    fn percentile(&mut self, p: f64) -> Duration {
        self.0.sort_unstable();
        let index = ((self.0.len() as f64 * p).ceil() as usize).clamp(1, self.0.len()) - 1;
        self.0[index]
    }

    fn summary(&mut self) -> String {
        format!(
            "p50 {} · p99 {} · p99.9 {} · max {}",
            micros(self.percentile(0.50)),
            micros(self.percentile(0.99)),
            micros(self.percentile(0.999)),
            micros(self.percentile(1.0))
        )
    }
}

fn micros(d: Duration) -> String {
    if d >= Duration::from_secs(1) {
        format!("{:.2} s", d.as_secs_f64())
    } else if d >= Duration::from_millis(10) {
        format!("{:.1} ms", d.as_secs_f64() * 1e3)
    } else {
        format!("{} µs", d.as_micros())
    }
}

fn rate(n: usize, elapsed: Duration) -> String {
    format!("{:.0}", n as f64 / elapsed.as_secs_f64())
}

struct Report(Vec<String>);

impl Report {
    fn row(
        &mut self,
        scenario: &str,
        orders: usize,
        elapsed: Duration,
        latency: &str,
        notes: &str,
    ) {
        let line = format!(
            "| {scenario} | {orders} | {:.2} s | {} | {latency} | {notes} |",
            elapsed.as_secs_f64(),
            rate(orders, elapsed)
        );
        eprintln!("{line}");
        self.0.push(line);
    }
}

fn temp_dir(base: &std::path::Path, label: &str) -> std::path::PathBuf {
    let dir = base.join(format!("exchange-bench-{label}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Pure matching on one book: no I/O, no events, no sequencing beyond the book's own.
fn bench_matching(options: &Options, report: &mut Report) {
    let commands = workload(options.seed, options.matching_orders);
    let ids = IdScheme::Deterministic { seed: 0 };
    let orders: Vec<Order> = commands
        .into_iter()
        .enumerate()
        .map(|(i, c)| Order {
            id: uuid::Uuid::from_u128(i as u128 + 1),
            client_order_id: None,
            instrument_id: c.instrument_id,
            participant_id: c.participant_id,
            side: c.side,
            order_type: c.order_type,
            limit_price_ticks: c.limit_price_ticks,
            quantity: c.quantity,
            remaining_quantity: c.quantity,
            sequence: i as u64 + 1,
            status: OrderStatus::Open,
        })
        .collect();
    let mut book = OrderBook::new(acme());
    let now = Utc::now();
    let mut executions = 0;
    let started = Instant::now();
    for order in orders {
        let update = book
            .submit(order, &mut MatchContext { now, ids })
            .expect("valid order");
        executions += update.executions.len();
    }
    let elapsed = started.elapsed();
    report.row(
        "in-memory matching (`OrderBook`)",
        options.matching_orders,
        elapsed,
        "–",
        &format!("{executions} executions, {} resting at end", book.len()),
    );
}

/// The coordinator on a SQLite file: every order committed (orders, executions, events, outbox) before the next.
///
/// With `group` > 1, orders are committed in groups of that size (`Exchange::begin_group` / `commit_group`), as the
/// exchange thread does under load. An order's latency then runs from its group's start to the group's commit, when
/// its caller would be answered.
fn bench_durable(options: &Options, report: &mut Report, subscribed: bool, group: usize) {
    let dir = temp_dir(&options.dir, "durable");
    let mut exchange = durable_exchange(&dir, subscribed, options.checkpoint, options.persistence);
    let commands = workload(options.seed, options.durable_orders);
    let mut latencies = Latencies::default();
    let started = Instant::now();
    if group > 1 {
        let mut commands = commands.into_iter().peekable();
        while commands.peek().is_some() {
            let t = Instant::now();
            exchange.begin_group().expect("begin");
            let mut n = 0;
            for command in commands.by_ref().take(group) {
                exchange.submit(command).expect("submit");
                n += 1;
            }
            exchange.commit_group().expect("commit");
            exchange.store().request_checkpoint_if_due();
            latencies.0.extend(std::iter::repeat_n(t.elapsed(), n));
        }
    } else {
        for command in commands {
            let t = Instant::now();
            exchange.submit(command).expect("submit");
            exchange.store().request_checkpoint_if_due();
            latencies.0.push(t.elapsed());
        }
    }
    let elapsed = started.elapsed();
    let count = |sql: &str| -> i64 {
        exchange
            .store()
            .connection()
            .query_row(sql, [], |r| r.get(0))
            .unwrap()
    };
    let bytes = count("SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()");
    let notes = format!(
        "{} executions, {} events, {} outbox rows; database {} MiB ({:.1} KiB per order)",
        count("SELECT COUNT(*) FROM executions"),
        count("SELECT COUNT(*) FROM events"),
        count("SELECT COUNT(*) FROM webhook_deliveries"),
        bytes >> 20,
        bytes as f64 / 1024.0 / options.durable_orders as f64
    );
    let mut scenario = if subscribed {
        "durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite)"
    } else {
        "durable, no subscribers (`Exchange` + SQLite)"
    }
    .to_string();
    if group > 1 {
        scenario.push_str(&format!(", groups of {group}"));
    }
    if options.persistence != PersistenceMode::Durable {
        scenario = format!("{} mode: {scenario}", options.persistence.as_str());
    }
    report.row(
        &scenario,
        options.durable_orders,
        elapsed,
        &latencies.summary(),
        &notes,
    );
    drop(exchange);
    let _ = std::fs::remove_dir_all(dir);
}

/// An `Exchange` on a fresh SQLite file in `dir`, with ACME and the participants registered. With background
/// checkpoints the caller asks for them after each commit (`Store::request_checkpoint_if_due`), as the exchange thread
/// does.
fn durable_exchange(
    dir: &std::path::Path,
    subscribed: bool,
    checkpoint: CheckpointMode,
    mode: PersistenceMode,
) -> Exchange {
    let location = match mode {
        PersistenceMode::Memory => persistence::memory_location(),
        _ => dir.join("exchange.db").to_string_lossy().into_owned(),
    };
    let conn = persistence::open_mode(&location, mode, Duration::from_secs(5), 16 << 30).unwrap();
    let mut store = Store::new(conn);
    if checkpoint == CheckpointMode::Background && mode != PersistenceMode::Memory {
        store
            .checkpoint_in_background(Duration::from_secs(5), 1000)
            .unwrap();
    }
    let mut exchange = Exchange::recover(store, ExchangeOptions::default()).unwrap();
    exchange.register_instrument(acme()).unwrap();
    for id in participant_ids() {
        exchange
            .register_participant(Participant {
                settlement: None,
                id: ParticipantId(id.clone()),
                name: id,
            })
            .unwrap();
    }
    if subscribed {
        exchange
            .register_webhook(NewSubscription {
                url: "http://127.0.0.1:9/unused".into(),
                event_types: EVENT_TYPES.iter().map(|t| t.to_string()).collect(),
                ..Default::default()
            })
            .unwrap();
    }
    exchange
}

/// Commit latency above this counts as a spike (normal p99.9 is about 2 ms on the VM disk).
const SPIKE: Duration = Duration::from_millis(10);

/// Checkpoints run by [`observed_autocheckpoint`]; a plain `fn` hook cannot capture state.
static CHECKPOINTS: AtomicUsize = AtomicUsize::new(0);

/// A WAL hook that does exactly what SQLite's built-in auto-checkpoint does (a PASSIVE checkpoint, inside the
/// committing call, once the WAL holds at least 1000 frames) and counts it, so a slow commit can be attributed.
fn observed_autocheckpoint(
    wal: &rusqlite::hooks::Wal,
    frames: std::ffi::c_int,
) -> rusqlite::Result<()> {
    if frames >= 1000 {
        CHECKPOINTS.fetch_add(1, Ordering::Relaxed);
        // SQLite's default hook ignores the result too: a checkpoint that cannot finish is retried on a later commit.
        let _ = wal.checkpoint();
    }
    Ok(())
}

/// Items 1.2 and 4.3: attributes durable commit latency spikes and measures background checkpointing. Runs the
/// durable workload (no subscribers, one commit per order) with SQLite's auto-checkpoint replaced by an identical,
/// observed one, with checkpoints in the background (4.3), and with auto-checkpointing off, then measures raw fsync
/// latency in the same directory.
fn bench_checkpoint(options: &Options, report: &mut Report) {
    #[derive(PartialEq)]
    enum Variant {
        Observed,
        Background,
        Off,
    }
    let spikes = |latencies: &[Duration]| latencies.iter().filter(|l| **l > SPIKE).count();
    for variant in [Variant::Observed, Variant::Background, Variant::Off] {
        let observed = variant == Variant::Observed;
        let dir = temp_dir(&options.dir, "checkpoint");
        let mode = if variant == Variant::Background {
            CheckpointMode::Background
        } else {
            CheckpointMode::Inline
        };
        let mut exchange = durable_exchange(&dir, false, mode, PersistenceMode::Durable);
        let conn = exchange.store().connection();
        match variant {
            Variant::Observed => conn.wal_hook(Some(observed_autocheckpoint)),
            Variant::Off => conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap(),
            Variant::Background => {}
        }
        let before = Scrape(exchange_simulator::metrics::install().render());
        CHECKPOINTS.store(0, Ordering::Relaxed);
        let mut latencies = Latencies::default();
        let mut in_checkpoint = Vec::new();
        let mut without = Vec::new();
        let started = Instant::now();
        for command in workload(options.seed, options.durable_orders) {
            let before = CHECKPOINTS.load(Ordering::Relaxed);
            let t = Instant::now();
            exchange.submit(command).expect("submit");
            exchange.store().request_checkpoint_if_due();
            let latency = t.elapsed();
            latencies.0.push(latency);
            if CHECKPOINTS.load(Ordering::Relaxed) > before {
                in_checkpoint.push(latency);
            } else {
                without.push(latency);
            }
        }
        let elapsed = started.elapsed();
        let wal_mib =
            std::fs::metadata(dir.join("exchange.db-wal")).map_or(0, |m| m.len()) / (1 << 20);
        let after = Scrape(exchange_simulator::metrics::install().render());
        let notes = if variant == Variant::Background {
            let checkpoints = |by: &str| {
                ["complete", "partial", "error"]
                    .iter()
                    .map(|outcome| {
                        let series = format!(
                            "exchange_wal_checkpoints_total{{by=\"{by}\",outcome=\"{outcome}\"}}"
                        );
                        after.value(&series) - before.value(&series)
                    })
                    .sum::<f64>()
            };
            format!(
                "{:.0} background checkpoints ({}), {:.0} by the writer; {} spikes > {} ms; WAL {wal_mib} MiB",
                checkpoints("background"),
                histogram_delta(
                    &before,
                    &after,
                    "exchange_wal_checkpoint_duration_seconds",
                    r#"by="background""#
                ),
                checkpoints("writer"),
                spikes(&latencies.0),
                SPIKE.as_millis()
            )
        } else if observed {
            let mut checkpointed = Latencies(in_checkpoint.clone());
            format!(
                "{} checkpoints; commits running one: {} spikes > {} ms, {}; other commits: {} spikes, max {}; WAL {wal_mib} MiB",
                in_checkpoint.len(),
                spikes(&in_checkpoint),
                SPIKE.as_millis(),
                if in_checkpoint.is_empty() {
                    "–".into()
                } else {
                    checkpointed.summary()
                },
                spikes(&without),
                micros(without.iter().max().copied().unwrap_or_default()),
            )
        } else {
            format!(
                "{} spikes > {} ms; WAL grew to {wal_mib} MiB",
                spikes(&without),
                SPIKE.as_millis()
            )
        };
        let scenario = match variant {
            Variant::Observed => "durable, auto-checkpoint every 1000 pages (inline, observed)",
            Variant::Background => "durable, background checkpoint from 1000 pages (4.3)",
            Variant::Off => "durable, auto-checkpoint off",
        };
        report.row(
            scenario,
            options.durable_orders,
            elapsed,
            &latencies.summary(),
            &notes,
        );
        drop(exchange);
        let _ = std::fs::remove_dir_all(dir);
    }

    // The disk on its own: 4 KiB appends, each followed by fsync, as many as the durable run made commits.
    use std::io::Write;
    let dir = temp_dir(&options.dir, "fsync");
    let mut file = std::fs::File::create(dir.join("probe")).unwrap();
    let mut latencies = Latencies::default();
    let started = Instant::now();
    for _ in 0..options.durable_orders {
        let t = Instant::now();
        file.write_all(&[0u8; 4096]).unwrap();
        file.sync_all().unwrap();
        latencies.0.push(t.elapsed());
    }
    let elapsed = started.elapsed();
    let notes = format!("{} spikes > {} ms", spikes(&latencies.0), SPIKE.as_millis());
    report.row(
        "raw 4 KiB write + fsync, no SQLite",
        options.durable_orders,
        elapsed,
        &latencies.summary(),
        &notes,
    );
    drop(file);
    let _ = std::fs::remove_dir_all(dir);
}

/// The service started in-process on a TCP port, with ACME and the participants registered over HTTP.
struct Service {
    app: App,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
    base: String,
    client: reqwest::Client,
    database: String,
    dir: std::path::PathBuf,
}

impl Service {
    async fn start(options: &Options, pool: usize) -> Self {
        let dir = temp_dir(&options.dir, "http");
        let config = Config {
            server: ServerConfig {
                bind: "127.0.0.1:0".into(),
            },
            exchange: ExchangeConfig {
                identifier_policy: IdentifierPolicy::Strict,
                command_queue_capacity: options.queue_capacity,
                group_commit: options.group_commit,
            },
            events: EventsConfig::default(),
            webhooks: WebhooksConfig {
                poll_interval_ms: 250,
                batch_size: 100,
                concurrency: 16,
                request_timeout_ms: 5000,
                max_attempts: 10,
                initial_backoff_ms: 500,
                max_backoff_ms: 300_000,
            },
            database: DatabaseConfig {
                mode: options.persistence,
                memory_max_mib: 16 * 1024,
                checkpoint: options.checkpoint,
                checkpoint_pages: 1000,
                path: dir.join("exchange.db").to_string_lossy().into_owned(),
                busy_timeout_ms: 5000,
            },
            clock: ClockConfig {
                mode: ClockMode::System,
                start: None,
                running: false,
            },
            ids: IdsConfig::default(),
            settlement: SettlementConfig::default(),
            test_control: TestControlConfig::default(),
            logging: LoggingConfig {
                filter: "warn".into(),
                format: LogFormat::Pretty,
            },
        };
        let app = App::start(&config).unwrap();
        let database = app.database_location().to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(axum::serve(listener, app.router.clone()).into_future());
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(pool)
            .build()
            .unwrap();
        let service = Self {
            app,
            server,
            base,
            client,
            database,
            dir,
        };
        service.post("/api/instruments", serde_json::json!({ "id": "ACME", "symbol": "ACME", "mic": "XCSE", "currency": "DKK", "currencyScale": 2 })).await;
        for id in participant_ids() {
            service
                .post(
                    "/api/participants",
                    serde_json::json!({ "id": id, "name": id }),
                )
                .await;
        }
        service
    }

    async fn post(&self, path: &str, body: serde_json::Value) {
        let response = self
            .client
            .post(format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success(), "POST {path}");
    }

    async fn metrics(&self) -> Scrape {
        let url = format!("{}/metrics", self.base);
        Scrape(
            self.client
                .get(url)
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
        )
    }

    fn count(&self, sql: &str) -> i64 {
        let db = rusqlite::Connection::open(&self.database).unwrap();
        db.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    async fn stop(self) {
        self.server.abort();
        self.app.shutdown().await;
        let _ = std::fs::remove_dir_all(self.dir);
    }
}

/// One `GET /metrics` text. The recorder is process-wide, so a run is read as the difference of two scrapes.
struct Scrape(String);

impl Scrape {
    fn value(&self, series: &str) -> f64 {
        self.0
            .lines()
            .find_map(|line| line.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
            .unwrap_or(0.0)
    }

    /// The cumulative bucket counts `(le, count)` of a histogram series; `labels` without braces, or empty.
    fn buckets(&self, name: &str, labels: &str) -> Vec<(f64, f64)> {
        let prefix = if labels.is_empty() {
            format!("{name}_bucket{{le=\"")
        } else {
            format!("{name}_bucket{{{labels},le=\"")
        };
        self.0
            .lines()
            .filter_map(|line| {
                let (le, count) = line.strip_prefix(&prefix)?.split_once("\"} ")?;
                let le = if le == "+Inf" {
                    f64::INFINITY
                } else {
                    le.parse().ok()?
                };
                Some((le, count.parse().ok()?))
            })
            .collect()
    }
}

/// Mean and p99 bucket bound of one histogram between two scrapes, e.g. `180 µs / ≤ 1 ms`.
fn histogram_delta(before: &Scrape, after: &Scrape, name: &str, labels: &str) -> String {
    let braces = if labels.is_empty() {
        String::new()
    } else {
        format!("{{{labels}}}")
    };
    let delta = |suffix: &str| {
        let series = format!("{name}_{suffix}{braces}");
        after.value(&series) - before.value(&series)
    };
    let count = delta("count");
    if count < 1.0 {
        return "–".into();
    }
    let mean = Duration::from_secs_f64(delta("sum") / count);
    let earlier = before.buckets(name, labels);
    let p99 = after
        .buckets(name, labels)
        .into_iter()
        .find(|(le, cumulative)| {
            let previous = earlier
                .iter()
                .find(|(l, _)| l == le)
                .map_or(0.0, |(_, c)| *c);
            cumulative - previous >= 0.99 * count
        })
        .map_or("?".into(), |(le, _)| {
            if le.is_infinite() {
                "> 10 s".into()
            } else {
                format!("≤ {}", micros(Duration::from_secs_f64(le)))
            }
        });
    format!("{} / {p99}", micros(mean))
}

/// Where a submitted order's time went inside the service, between two scrapes: mean and p99 bucket bound of the
/// queue wait, the exchange thread's time per order (planning and writing; with group commit, the group's commit is
/// not part of it), the acknowledgement (enqueue to reply), and the commits with their group sizes.
fn breakdown(before: &Scrape, after: &Scrape) -> String {
    use exchange_simulator::metrics::{
        ACK_LATENCY, COMMAND_DURATION, COMMIT_DURATION, GROUP_SIZE, QUEUE_WAIT,
    };
    let submit = r#"command="submit""#;
    let groups =
        after.value(&format!("{GROUP_SIZE}_count")) - before.value(&format!("{GROUP_SIZE}_count"));
    let grouped =
        after.value(&format!("{GROUP_SIZE}_sum")) - before.value(&format!("{GROUP_SIZE}_sum"));
    let group = if groups > 0.0 {
        format!(
            "{:.0} groups of {:.1} orders on average",
            groups,
            grouped / groups
        )
    } else {
        "no groups".into()
    };
    format!(
        "server per order (mean / p99): queue wait {}, exchange thread {}, ack {}; commit {} per commit, {group}",
        histogram_delta(before, after, QUEUE_WAIT, submit),
        histogram_delta(before, after, COMMAND_DURATION, submit),
        histogram_delta(before, after, ACK_LATENCY, submit),
        histogram_delta(before, after, COMMIT_DURATION, ""),
    )
}

fn order_bodies(seed: u64, n: usize) -> Arc<Vec<serde_json::Value>> {
    Arc::new(
        workload(seed, n)
            .into_iter()
            .map(|c| {
                serde_json::json!({ "instrumentId": "ACME", "participantId": c.participant_id, "side": c.side,
                                    "type": c.order_type, "priceTicks": c.limit_price_ticks, "quantity": c.quantity })
            })
            .collect(),
    )
}

/// The full service over real TCP: `concurrency` HTTP clients submitting the workload; optionally a webhook consumer
/// subscribed to `TradeExecuted`, with the time until every delivery is done.
async fn bench_http(options: &Options, report: &mut Report, with_webhook: bool) {
    let service = Service::start(options, options.concurrency).await;
    let delivered = Arc::new(AtomicUsize::new(0));
    if with_webhook {
        let counter = delivered.clone();
        let consumer = axum::Router::new().route(
            "/hook",
            axum::routing::post(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                async { axum::http::StatusCode::NO_CONTENT }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        tokio::spawn(axum::serve(listener, consumer).into_future());
        service
            .post(
                "/api/webhooks",
                serde_json::json!({ "url": url, "eventTypes": ["TradeExecuted"] }),
            )
            .await;
    }

    let bodies = order_bodies(options.seed, options.http_orders);
    let next = Arc::new(AtomicUsize::new(0));
    let before = service.metrics().await;
    let started = Instant::now();
    let workers: Vec<_> = (0..options.concurrency)
        .map(|_| {
            let (client, bodies, next, url) = (
                service.client.clone(),
                bodies.clone(),
                next.clone(),
                format!("{}/api/orders", service.base),
            );
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                let mut failures = 0usize;
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(body) = bodies.get(i) else { break };
                    let t = Instant::now();
                    let ok = client
                        .post(&url)
                        .json(body)
                        .send()
                        .await
                        .is_ok_and(|r| r.status().is_success());
                    latencies.push(t.elapsed());
                    failures += usize::from(!ok);
                }
                (latencies, failures)
            })
        })
        .collect();
    let mut latencies = Latencies::default();
    let mut failures = 0;
    for worker in workers {
        let (l, f) = worker.await.unwrap();
        latencies.0.extend(l);
        failures += f;
    }
    let elapsed = started.elapsed();
    let after = service.metrics().await;

    let mut notes = format!(
        "{} clients, {failures} non-2xx, {} executions; {}",
        options.concurrency,
        service.count("SELECT COUNT(*) FROM executions"),
        breakdown(&before, &after)
    );
    let scenario = if with_webhook {
        let expected = service
            .count("SELECT COUNT(*) FROM events WHERE event_type = 'TradeExecuted'")
            as usize;
        while delivered.load(Ordering::Relaxed) < expected
            && started.elapsed() < Duration::from_secs(600)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let drained = started.elapsed();
        notes.push_str(&format!(
            "; {} of {expected} `TradeExecuted` webhooks delivered {:.2} s after the first order",
            delivered.load(Ordering::Relaxed),
            drained.as_secs_f64()
        ));
        "HTTP end to end + `TradeExecuted` webhook consumer"
    } else {
        "HTTP end to end (`POST /api/orders`)"
    };
    report.row(
        scenario,
        options.http_orders,
        elapsed,
        &latencies.summary(),
        &notes,
    );
    service.stop().await;
}

/// How one open-loop request ended.
enum Outcome {
    Accepted,
    Overloaded,
    Failed,
}

/// Open loop at `rate` orders/s: order `i` is due at `start + i / rate`, and is sent then whether or not earlier
/// orders were answered. Latency runs from the due time, not the actual send, so a stalled server or a lagging
/// generator shows up in the latency instead of quietly lowering the offered load (no coordinated omission).
/// `--max-in-flight` bounds the open requests; at that bound the generator waits, and the wait counts as latency.
async fn bench_open_loop(options: &Options, report: &mut Report, rate: f64) {
    let service = Service::start(options, options.max_in_flight).await;
    let n = ((rate * options.seconds).round() as usize).max(1);
    let bodies = order_bodies(options.seed, n);
    let url = format!("{}/api/orders", service.base);
    let in_flight = Arc::new(tokio::sync::Semaphore::new(options.max_in_flight));
    let before = service.metrics().await;
    let start = tokio::time::Instant::now() + Duration::from_millis(20);
    let mut lags = Latencies::default();
    let mut requests = Vec::with_capacity(n);
    for i in 0..n {
        let due = start + Duration::from_secs_f64(i as f64 / rate);
        tokio::time::sleep_until(due).await;
        let permit = in_flight.clone().acquire_owned().await.unwrap();
        lags.0.push(due.elapsed());
        let request = service.client.post(&url).json(&bodies[i]);
        requests.push(tokio::spawn(async move {
            let outcome = match request.send().await {
                Ok(r) if r.status().is_success() => Outcome::Accepted,
                Ok(r) if r.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE => {
                    Outcome::Overloaded
                }
                _ => Outcome::Failed,
            };
            drop(permit);
            (outcome, due.elapsed(), tokio::time::Instant::now())
        }));
    }
    let (mut accepted, mut overloaded, mut failed) = (Latencies::default(), 0, 0);
    let mut last = start;
    for request in requests {
        let (outcome, latency, done) = request.await.unwrap();
        last = last.max(done);
        match outcome {
            Outcome::Accepted => accepted.0.push(latency),
            Outcome::Overloaded => overloaded += 1,
            Outcome::Failed => failed += 1,
        }
    }
    let elapsed = last - start;
    let after = service.metrics().await;
    let ok = accepted.0.len();
    let latency = if ok == 0 {
        "–".into()
    } else {
        accepted.summary()
    };
    let notes = format!(
        "target {rate:.0}/s for {:.0} s: {n} sent, {ok} accepted, {overloaded} × 503, {failed} other failures; \
         generator lag p99 {} max {}; {}",
        options.seconds,
        micros(lags.percentile(0.99)),
        micros(lags.percentile(1.0)),
        breakdown(&before, &after),
    );
    report.row(
        &format!("HTTP open loop, target {rate:.0} orders/s"),
        ok,
        elapsed,
        &latency,
        &notes,
    );
    service.stop().await;
}

fn environment() -> String {
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|info| {
            info.lines()
                .find(|l| l.starts_with("model name"))
                .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
        })
        .unwrap_or_else(|| "unknown CPU".into());
    let cpus = std::thread::available_parallelism().map_or(0, |n| n.get());
    let memory = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|m| {
            m.lines().next().map(|l| {
                l.split_whitespace()
                    .nth(1)
                    .unwrap_or("0")
                    .parse::<u64>()
                    .unwrap_or(0)
                    / 1024
                    / 1024
            })
        })
        .unwrap_or(0);
    format!(
        "{cpu}, {cpus} CPUs available, {memory} GiB RAM on the host, {}; {} {} (kernel {})",
        container_limits(),
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .unwrap_or_default()
            .trim()
    )
}

/// The cgroup (v2) limits this process runs under. In a container `/proc/meminfo` shows the host's memory, not the
/// container's limit, which is what decides when a run starts to thrash.
fn container_limits() -> String {
    let read = |file: &str| {
        std::fs::read_to_string(format!("/sys/fs/cgroup/{file}"))
            .ok()
            .map(|s| s.trim().to_string())
    };
    let memory = match read("memory.max").as_deref() {
        Some("max") => "no memory limit".to_string(),
        Some(bytes) => match bytes.parse::<u64>() {
            Ok(bytes) => format!(
                "container memory limit {:.1} GiB",
                bytes as f64 / (1u64 << 30) as f64
            ),
            Err(_) => "memory limit unknown".into(),
        },
        None => "memory limit unknown".into(),
    };
    let cpu = match read("cpu.max") {
        Some(max) => match max.split_once(' ') {
            Some(("max", _)) => "no CPU limit".to_string(),
            Some((quota, period)) => match (quota.parse::<f64>(), period.parse::<f64>()) {
                (Ok(q), Ok(p)) if p > 0.0 => format!("CPU limit {:.1}", q / p),
                _ => "CPU limit unknown".into(),
            },
            None => "CPU limit unknown".into(),
        },
        None => "CPU limit unknown".into(),
    };
    format!("{memory}, {cpu}")
}

/// Average latency of a 4 KiB write plus `fsync` in `base`: what one durable commit costs at least.
fn fsync_latency(base: &std::path::Path) -> Duration {
    use std::io::Write;
    let dir = temp_dir(base, "fsync");
    let mut file = std::fs::File::create(dir.join("probe")).unwrap();
    let block = [0u8; 4096];
    let n = 300;
    let started = Instant::now();
    for _ in 0..n {
        file.write_all(&block).unwrap();
        file.sync_all().unwrap();
    }
    let average = started.elapsed() / n;
    drop(file);
    let _ = std::fs::remove_dir_all(dir);
    average
}

/// The journal and sync mode a database opened by the service actually uses.
fn sqlite_settings(base: &std::path::Path, mode: PersistenceMode) -> String {
    let dir = temp_dir(base, "pragma");
    let location = match mode {
        PersistenceMode::Memory => persistence::memory_location(),
        _ => dir.join("probe.db").to_string_lossy().into_owned(),
    };
    let conn = persistence::open_mode(&location, mode, Duration::from_secs(1), 1 << 30).unwrap();
    let journal: String = conn
        .pragma_query_value(None, "journal_mode", |r| r.get(0))
        .unwrap();
    let synchronous: i64 = conn
        .pragma_query_value(None, "synchronous", |r| r.get(0))
        .unwrap();
    let version: String = conn
        .query_row("SELECT sqlite_version()", [], |r| r.get(0))
        .unwrap();
    drop(conn);
    let _ = std::fs::remove_dir_all(dir);
    let sync = match synchronous {
        0 => "OFF",
        1 => "NORMAL",
        2 => "FULL",
        3 => "EXTRA",
        _ => "?",
    };
    format!(
        "{version}, database.mode={}, journal_mode={journal}, synchronous={sync}",
        mode.as_str()
    )
}

#[tokio::main]
async fn main() {
    let options = match Options::parse() {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    if cfg!(debug_assertions) {
        eprintln!("warning: debug build; run with --release for meaningful numbers");
    }
    let mut report = Report(vec![
        "| Scenario | Orders | Time | Orders/s | Latency per order | Notes |".into(),
        "|---|---:|---:|---:|---|---|".into(),
    ]);
    let all = options.mode == "all";
    if all || options.mode == "matching" {
        bench_matching(&options, &mut report);
    }
    if options.mode == "checkpoint" {
        bench_checkpoint(&options, &mut report);
    }
    if all || options.mode == "durable" {
        let group = options.group_commit.max_commands;
        bench_durable(&options, &mut report, false, 1);
        bench_durable(&options, &mut report, true, 1);
        if group > 1 {
            bench_durable(&options, &mut report, false, group);
            bench_durable(&options, &mut report, true, group);
        }
    }
    if all || options.mode == "http" {
        bench_http(&options, &mut report, false).await;
        bench_http(&options, &mut report, true).await;
    }
    if options.mode == "open-loop" {
        for rate in options.rates.clone() {
            bench_open_loop(&options, &mut report, rate).await;
        }
    }
    let text = format!(
        "# exchange-simulator benchmark\n\n- Date: {}\n- Environment: {}\n- Build: {}\n- Seed: {}; workload: 80 % limit \
         orders within ±50 ticks of mid, 20 % market, quantities 1–100, {PARTICIPANTS} participants, one instrument\n- \
         SQLite: {}, one writer thread, checkpoints {}\n- Database directory: `{}`; 4 KiB write + fsync there: {}\n\n{}\n",
        Utc::now().format("%Y-%m-%d %H:%M UTC"),
        environment(),
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        options.seed,
        sqlite_settings(&options.dir, options.persistence),
        match (options.persistence, options.checkpoint) {
            (PersistenceMode::Memory, _) => "none (in memory)",
            (_, CheckpointMode::Background) => "in the background from 1000 pages",
            (_, CheckpointMode::Inline) => "inline (SQLite's automatic checkpoint)",
        },
        options.dir.display(),
        micros(fsync_latency(&options.dir)),
        report.0.join("\n")
    );
    println!("{text}");
    if let Some(out) = options.out {
        std::fs::write(&out, &text).unwrap();
        eprintln!("wrote {out}");
    }
}
