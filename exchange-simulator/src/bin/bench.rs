//! Benchmark harness: in-memory matching, durable (SQLite) order processing, and the HTTP service end to end, on one
//! seeded workload. See `bench/README.md`. Run with `--release`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use exchange_simulator::app::App;
use exchange_simulator::config::{
    Config, DatabaseConfig, EventsConfig, ExchangeConfig, LogFormat, LoggingConfig, ServerConfig,
    WebhooksConfig,
};
use exchange_simulator::domain::{
    IdentifierPolicy, Instrument, InstrumentId, Order, OrderStatus, OrderType, Participant,
    ParticipantId, Side,
};
use exchange_simulator::engine::{
    Exchange, ExchangeOptions, MatchContext, OrderBook, SequentialIds, SubmitOrder, SystemClock,
    UuidV7Ids,
};
use exchange_simulator::events::EVENT_TYPES;
use exchange_simulator::persistence::{self, Store};
use exchange_simulator::webhooks::NewSubscription;

const USAGE: &str = "usage: bench [all|matching|durable|http|checkpoint] [--quick] [--seed N] [--matching-orders N] \
                     [--durable-orders N] [--http-orders N] [--concurrency N] [--dir DIR] [--out FILE]

--dir: where the SQLite files go (default: the system temp dir). Check the reported fsync latency: on tmpfs it is
       ~0 and the \"durable\" numbers measure SQLite in RAM.";

struct Options {
    mode: String,
    seed: u64,
    matching_orders: usize,
    durable_orders: usize,
    http_orders: usize,
    concurrency: usize,
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
            dir: std::env::temp_dir(),
            out: None,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let mut value = |name: &str| args.next().ok_or(format!("{name} needs a value"));
            let number = |text: String| text.parse::<usize>().map_err(|e| format!("{text}: {e}"));
            match arg.as_str() {
                "all" | "matching" | "durable" | "http" | "checkpoint" => options.mode = arg,
                "--quick" => {
                    options.matching_orders = 200_000;
                    options.durable_orders = 10_000;
                    options.http_orders = 10_000;
                }
                "--seed" => options.seed = number(value("--seed")?)? as u64,
                "--matching-orders" => {
                    options.matching_orders = number(value("--matching-orders")?)?
                }
                "--durable-orders" => options.durable_orders = number(value("--durable-orders")?)?,
                "--http-orders" => options.http_orders = number(value("--http-orders")?)?,
                "--concurrency" => options.concurrency = number(value("--concurrency")?)?.max(1),
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

/// Deterministic generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

/// The workload: 80 % limit orders priced uniformly within ±50 ticks of 100.00, 20 % market orders, sides even,
/// quantities 1–100, 50 participants. Limit prices on both sides overlap, so roughly half the orders trade.
fn workload(seed: u64, n: usize) -> Vec<SubmitOrder> {
    let mut rng = Rng(seed.max(1));
    (0..n)
        .map(|_| {
            let side = if rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let market = rng.below(5) == 0;
            SubmitOrder {
                client_order_id: None,
                instrument_id: InstrumentId("ACME".into()),
                participant_id: ParticipantId(format!("P{:02}", rng.below(PARTICIPANTS as u64))),
                side,
                order_type: if market {
                    OrderType::Market
                } else {
                    OrderType::Limit
                },
                limit_price_ticks: (!market).then(|| 9_950 + rng.below(101) as i64),
                quantity: 1 + rng.below(100),
            }
        })
        .collect()
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
    if d >= Duration::from_millis(10) {
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
    let mut ids = SequentialIds(1 << 64);
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
            .submit(order, &mut MatchContext { now, ids: &mut ids })
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
fn bench_durable(options: &Options, report: &mut Report, subscribed: bool) {
    let dir = temp_dir(&options.dir, "durable");
    let mut exchange = durable_exchange(&dir, subscribed);
    let commands = workload(options.seed, options.durable_orders);
    let mut latencies = Latencies::default();
    let started = Instant::now();
    for command in commands {
        let t = Instant::now();
        exchange.submit(command).expect("submit");
        latencies.0.push(t.elapsed());
    }
    let elapsed = started.elapsed();
    let count = |sql: &str| -> i64 {
        exchange
            .store()
            .connection()
            .query_row(sql, [], |r| r.get(0))
            .unwrap()
    };
    let notes = format!(
        "{} executions, {} events, {} outbox rows",
        count("SELECT COUNT(*) FROM executions"),
        count("SELECT COUNT(*) FROM events"),
        count("SELECT COUNT(*) FROM webhook_deliveries")
    );
    let scenario = if subscribed {
        "durable, outbox for 1 subscriber to all event types (`Exchange` + SQLite)"
    } else {
        "durable, no subscribers (`Exchange` + SQLite)"
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

/// An `Exchange` on a fresh SQLite file in `dir`, with ACME and the participants registered.
fn durable_exchange(dir: &std::path::Path, subscribed: bool) -> Exchange {
    let path = dir.join("exchange.db");
    let conn = persistence::open(&path, Duration::from_secs(5)).unwrap();
    let mut exchange = Exchange::recover(
        Store::new(conn),
        ExchangeOptions {
            identifier_policy: IdentifierPolicy::Strict,
            ids: Box::new(UuidV7Ids),
            clock: Box::new(SystemClock),
            simulation_id: None,
            delivery_signal: None,
        },
    )
    .unwrap();
    exchange.register_instrument(acme()).unwrap();
    for p in 0..PARTICIPANTS {
        exchange
            .register_participant(Participant {
                settlement: None,
                id: ParticipantId(format!("P{p:02}")),
                name: format!("P{p}"),
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

/// Item 1.2: attributes durable commit latency spikes. Runs the durable workload (no subscribers) with SQLite's
/// auto-checkpoint replaced by an identical, observed one, then with auto-checkpointing off, and measures raw fsync
/// latency in the same directory.
fn bench_checkpoint(options: &Options, report: &mut Report) {
    let spikes = |latencies: &[Duration]| latencies.iter().filter(|l| **l > SPIKE).count();
    for observed in [true, false] {
        let dir = temp_dir(&options.dir, "checkpoint");
        let mut exchange = durable_exchange(&dir, false);
        let conn = exchange.store().connection();
        if observed {
            conn.wal_hook(Some(observed_autocheckpoint));
        } else {
            conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        }
        CHECKPOINTS.store(0, Ordering::Relaxed);
        let mut latencies = Latencies::default();
        let mut in_checkpoint = Vec::new();
        let mut without = Vec::new();
        let started = Instant::now();
        for command in workload(options.seed, options.durable_orders) {
            let before = CHECKPOINTS.load(Ordering::Relaxed);
            let t = Instant::now();
            exchange.submit(command).expect("submit");
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
        let notes = if observed {
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
        let scenario = if observed {
            "durable, auto-checkpoint every 1000 pages (default, observed)"
        } else {
            "durable, auto-checkpoint off"
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

/// The full service over real TCP: `concurrency` HTTP clients submitting the workload; optionally a webhook consumer
/// subscribed to `TradeExecuted`, with the time until every delivery is done.
async fn bench_http(options: &Options, report: &mut Report, with_webhook: bool) {
    let dir = temp_dir(&options.dir, "http");
    let config = Config {
        server: ServerConfig {
            bind: "127.0.0.1:0".into(),
        },
        exchange: ExchangeConfig {
            identifier_policy: IdentifierPolicy::Strict,
            command_queue_capacity: 10_000,
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
            path: dir.join("exchange.db").to_string_lossy().into_owned(),
            busy_timeout_ms: 5000,
        },
        logging: LoggingConfig {
            filter: "warn".into(),
            format: LogFormat::Pretty,
        },
    };
    let app = App::start(&config).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app.router.clone()).into_future());
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(options.concurrency)
        .build()
        .unwrap();

    let post = |path: &str, body: serde_json::Value| {
        let request = client.post(format!("{base}{path}")).json(&body);
        async move { assert!(request.send().await.unwrap().status().is_success()) }
    };
    post("/api/instruments", serde_json::json!({ "id": "ACME", "symbol": "ACME", "mic": "XCSE", "currency": "DKK", "currencyScale": 2 })).await;
    for p in 0..PARTICIPANTS {
        post(
            "/api/participants",
            serde_json::json!({ "id": format!("P{p:02}"), "name": format!("P{p}") }),
        )
        .await;
    }
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
        post(
            "/api/webhooks",
            serde_json::json!({ "url": url, "eventTypes": ["TradeExecuted"] }),
        )
        .await;
    }

    let bodies: Arc<Vec<serde_json::Value>> = Arc::new(
        workload(options.seed, options.http_orders)
            .into_iter()
            .map(|c| {
                serde_json::json!({ "instrumentId": "ACME", "participantId": c.participant_id, "side": c.side,
                                    "type": c.order_type, "priceTicks": c.limit_price_ticks, "quantity": c.quantity })
            })
            .collect(),
    );
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let workers: Vec<_> = (0..options.concurrency)
        .map(|_| {
            let (client, bodies, next, url) = (
                client.clone(),
                bodies.clone(),
                next.clone(),
                format!("{base}/api/orders"),
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

    let db = rusqlite::Connection::open(&config.database.path).unwrap();
    let count = |sql: &str| -> i64 { db.query_row(sql, [], |r| r.get(0)).unwrap() };
    let mut notes = format!(
        "{} clients, {failures} non-2xx, {} executions",
        options.concurrency,
        count("SELECT COUNT(*) FROM executions")
    );
    let scenario = if with_webhook {
        let expected =
            count("SELECT COUNT(*) FROM events WHERE event_type = 'TradeExecuted'") as usize;
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
    server.abort();
    app.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
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
        "{cpu}, {cpus} CPUs available, {memory} GiB RAM, {} {} (kernel {})",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .unwrap_or_default()
            .trim()
    )
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
fn sqlite_settings(base: &std::path::Path) -> String {
    let dir = temp_dir(base, "pragma");
    let conn = persistence::open(dir.join("probe.db"), Duration::from_secs(1)).unwrap();
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
    format!("{version}, journal_mode={journal}, synchronous={sync}")
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
        bench_durable(&options, &mut report, false);
        bench_durable(&options, &mut report, true);
    }
    if all || options.mode == "http" {
        bench_http(&options, &mut report, false).await;
        bench_http(&options, &mut report, true).await;
    }
    let text = format!(
        "# exchange-simulator benchmark\n\n- Date: {}\n- Environment: {}\n- Build: {}\n- Seed: {}; workload: 80 % limit \
         orders within ±50 ticks of mid, 20 % market, quantities 1–100, {PARTICIPANTS} participants, one instrument\n- \
         SQLite: {}, one writer thread\n- Database directory: `{}`; 4 KiB write + fsync there: {}\n\n{}\n",
        Utc::now().format("%Y-%m-%d %H:%M UTC"),
        environment(),
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        options.seed,
        sqlite_settings(&options.dir),
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
