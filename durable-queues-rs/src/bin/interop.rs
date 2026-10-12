//! The Rust node of the Phase 6 interop tests - the counterpart of `interop/java-harness`, with the same commands and
//! the same one-JSON-object-per-line events on stdout:
//!
//! ```text
//! interop produce                    --count N --prefix P [--ordered-keys K] [--delay-ms D]
//! interop consume                    [--parallel P] [--fail-attempts F] [--redelivery-delay-ms D]
//!                                    [--max-redeliveries R] [--handle-ms H] [--timeout-ms T]
//! interop consume-and-crash-after K  (consume options)
//! interop resurrect                  --id ID [--delay-ms D]
//! interop bench-produce              (--rate R --duration-s D | --count N) [--tick-ms T] [--queues N]
//! interop bench-consume              [--parallel P] [--queues N]
//! consume options: [--fetch-pacing eager|fixed-rate]  (default eager; fixed-rate is Java's polling)
//! common: --database-url URL [--queue Q] [--node NAME] [--pool-size N]
//! ```
//!
//! A consumer runs until the process is killed. The `bench-*` commands are Phase 7's: `bench-produce` is the workload
//! generator for both languages, stamping each `OrderPlaced.placedAt` with its send time; `bench-consume` handles
//! messages without doing any work and reports, once a second, how many it handled and each one's latency since it
//! was sent.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use durable_queues_rs::{
    ConsumerConfig, DecodeError, DurableQueues, FetchPacing, HandlerError, Message,
    PostgresQueueStorage, QueueEntryId, QueueMessage, QueueName, QueuedMessage, RedeliveryPolicy,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

/// Java's `com.acme.orders.OrderPlaced`: `SingleValueType`s as bare strings, a `BigDecimal`, an `Instant`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderPlaced {
    order_id: String,
    customer_id: String,
    amount: serde_json::Number,
    placed_at: DateTime<Utc>,
}

/// Java's `com.acme.orders.OrderEvent$OrderAdded`, the ordered-message payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderAdded {
    order_id: String,
    step: i64,
}

/// The payload types an interop queue carries, dispatched on `message_payload_type`.
#[derive(Debug, Clone)]
enum Payload {
    OrderPlaced(OrderPlaced),
    OrderAdded(OrderAdded),
}

const ORDER_PLACED: &str = "com.acme.orders.OrderPlaced";
const ORDER_ADDED: &str = "com.acme.orders.OrderEvent$OrderAdded";

impl QueueMessage for Payload {
    fn payload_type(&self) -> &str {
        match self {
            Self::OrderPlaced(_) => ORDER_PLACED,
            Self::OrderAdded(_) => ORDER_ADDED,
        }
    }

    fn to_payload(&self) -> serde_json::Result<Value> {
        match self {
            Self::OrderPlaced(payload) => serde_json::to_value(payload),
            Self::OrderAdded(payload) => serde_json::to_value(payload),
        }
    }

    fn from_payload(payload_type: &str, payload: Value) -> Result<Self, DecodeError> {
        match payload_type {
            ORDER_PLACED => Ok(Self::OrderPlaced(serde_json::from_value(payload)?)),
            ORDER_ADDED => Ok(Self::OrderAdded(serde_json::from_value(payload)?)),
            other => Err(DecodeError::UnknownPayloadType(other.to_owned())),
        }
    }
}

struct Options {
    values: HashMap<String, String>,
    node: String,
    queue_name: QueueName,
}

impl Options {
    fn parse(command: &str, mut args: impl Iterator<Item = String>) -> Self {
        let mut values = HashMap::new();
        if command == "consume-and-crash-after" {
            values.insert(
                "hang-after".to_owned(),
                args.next().expect("consume-and-crash-after K"),
            );
        }
        while let Some(name) = args.next() {
            let name = name
                .strip_prefix("--")
                .unwrap_or_else(|| panic!("expected --option, got {name}"))
                .to_owned();
            let value = args
                .next()
                .unwrap_or_else(|| panic!("--{name} needs a value"));
            values.insert(name, value);
        }
        let node = values
            .get("node")
            .cloned()
            .unwrap_or_else(|| "rust".to_owned());
        let queue_name = QueueName::new(values.get("queue").map_or("Orders", String::as_str));
        Self {
            values,
            node,
            queue_name,
        }
    }

    fn string(&self, name: &str) -> &str {
        self.values
            .get(name)
            .unwrap_or_else(|| panic!("missing --{name}"))
    }

    fn int(&self, name: &str, default: i64) -> i64 {
        self.values.get(name).map_or(default, |value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("--{name} must be a number"))
        })
    }

    fn millis(&self, name: &str, default: u64) -> Duration {
        Duration::from_millis(self.int(name, default as i64) as u64)
    }

    fn fetch_pacing(&self) -> FetchPacing {
        match self.values.get("fetch-pacing").map(String::as_str) {
            None | Some("eager") => FetchPacing::Eager,
            Some("fixed-rate") => FetchPacing::FixedRate,
            Some(other) => panic!("--fetch-pacing must be eager or fixed-rate, not {other}"),
        }
    }

    /// The `bench-*` commands' queues: `--queue` itself, or with `--queues N` (N > 1) `<queue>-0` … `<queue>-<N-1>`.
    fn bench_queues(&self) -> Vec<QueueName> {
        match self.int("queues", 1) {
            1 => vec![self.queue_name.clone()],
            n => (0..n)
                .map(|i| QueueName::new(format!("{}-{i}", self.queue_name.as_str())))
                .collect(),
        }
    }

    fn event(&self, name: &str) -> serde_json::Map<String, Value> {
        let mut event = serde_json::Map::new();
        event.insert("event".to_owned(), json!(name));
        event.insert("node".to_owned(), json!(self.node));
        event
    }
}

fn emit(event: serde_json::Map<String, Value>) {
    // One `println!` per event: stdout is line-buffered and locked per call, so events never interleave.
    println!("{}", Value::Object(event));
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let command = args
        .next()
        .expect("usage: interop <command> [--option value]...");
    let options = Options::parse(&command, args);
    // The node name as `application_name`, so pg_stat_activity tells the nodes' connections apart (Phase 7)
    let connect_options = options
        .string("database-url")
        .parse::<PgConnectOptions>()
        .expect("--database-url")
        .application_name(&options.node);
    let pool = PgPoolOptions::new()
        .max_connections(options.int("pool-size", 10) as u32)
        .connect_with(connect_options)
        .await
        .expect("connect to PostgreSQL");
    let mut queues = DurableQueues::new(
        PostgresQueueStorage::new(pool, Default::default())
            .await
            .expect("init storage"),
    );
    if options.values.contains_key("timeout-ms") {
        queues = queues.with_message_handling_timeout(options.millis("timeout-ms", 0));
    }
    match command.as_str() {
        "produce" => produce(&queues, &options).await,
        "consume" | "consume-and-crash-after" => consume(&queues, options).await,
        "resurrect" => resurrect(&queues, &options).await,
        "bench-produce" => bench_produce(&queues, &options).await,
        "bench-consume" => bench_consume(&queues, options).await,
        other => panic!("unknown command {other}"),
    }
}

async fn produce(queues: &DurableQueues, options: &Options) {
    let count = options.int("count", 1);
    let prefix = options.values.get("prefix").map_or("rust", String::as_str);
    let ordered_keys = options.int("ordered-keys", 0);
    let delay = options.millis("delay-ms", 0);
    let mut messages = Vec::new();
    if ordered_keys == 0 {
        let placed_at = "2026-10-11T12:00:00.123456Z"
            .parse::<DateTime<Utc>>()
            .unwrap();
        for i in 0..count {
            let payload = Payload::OrderPlaced(OrderPlaced {
                order_id: format!("{prefix}-{i}"),
                customer_id: format!("customer-{i}"),
                amount: format!("{i}.5").parse().unwrap(),
                placed_at: placed_at + chrono::TimeDelta::seconds(i),
            });
            messages.push(metadata(
                Message::new(payload),
                &format!("{prefix}-{i}"),
                options,
            ));
        }
    } else {
        // Orders interleaved across keys, as the Java harness queues them
        for order in 0..count {
            for k in 0..ordered_keys {
                let key = format!("{prefix}-key-{k}");
                let payload = Payload::OrderAdded(OrderAdded {
                    order_id: key.clone(),
                    step: order,
                });
                let message = Message::new(payload).in_order(&key, order);
                messages.push(metadata(message, &format!("{key}-{order}"), options));
            }
        }
    }
    for message in &messages {
        let id = queues
            .queue_message_with_delay(&options.queue_name, message, delay)
            .await
            .expect("queue message");
        let mut event = options.event("queued");
        event.insert("id".to_owned(), json!(id.as_str()));
        emit(event);
    }
    emit(options.event("done"));
}

fn metadata(
    message: Message<Payload>,
    correlation_id: &str,
    options: &Options,
) -> Message<Payload> {
    message
        .with_metadata("correlationId", correlation_id)
        .with_metadata("producer", &options.node)
}

async fn consume(queues: &DurableQueues, options: Options) {
    let policy = RedeliveryPolicy::fixed_backoff(
        options.millis("redelivery-delay-ms", 100),
        options.int("max-redeliveries", 5) as u32,
    );
    let config = ConsumerConfig::new(options.queue_name.clone(), policy)
        .with_parallel_consumers(options.int("parallel", 1) as usize)
        .with_fetch_pacing(options.fetch_pacing());
    let fail_attempts = options.int("fail-attempts", 0);
    let handle_time = options.millis("handle-ms", 0);
    let hang_after = options.int("hang-after", -1);
    let handled = Arc::new(AtomicI64::new(0));
    let options = Arc::new(options);
    let handler_options = options.clone();
    let _consumer = queues
        .consume_from_queue(config, move |message: QueuedMessage<Payload>| {
            let options = handler_options.clone();
            let handled = handled.clone();
            async move {
                if hang_after >= 0 && handled.load(Ordering::SeqCst) >= hang_after {
                    let mut event = options.event("hung");
                    event.insert("id".to_owned(), json!(message.id.as_str()));
                    emit(event);
                    std::future::pending::<()>().await;
                }
                if i64::from(message.total_attempts) <= fail_attempts {
                    emit(describe(&options, "failed", &message));
                    return Err(HandlerError::transient(format!(
                        "interop: failing delivery attempt {}",
                        message.total_attempts
                    )));
                }
                tokio::time::sleep(handle_time).await;
                handled.fetch_add(1, Ordering::SeqCst);
                emit(describe(&options, "handled", &message));
                Ok(())
            }
        })
        .expect("start consumer");
    emit(options.event("ready"));
    std::future::pending::<()>().await;
}

/// Open-loop load at a fixed rate: every tick inserts as many messages as the rate owes since the start, in one
/// `queue_messages` statement, so a slow insert is caught up on rather than lowering the rate. With `--count`, queues
/// that many at once (a backlog) instead. With `--queues N`, every batch is spread evenly over the N queues.
async fn bench_produce(queues: &DurableQueues, options: &Options) {
    let bench_queues = options.bench_queues();
    let batch = |count: i64| -> Vec<Message<Payload>> {
        let placed_at = Utc::now();
        (0..count)
            .map(|i| {
                Message::new(Payload::OrderPlaced(OrderPlaced {
                    order_id: format!("bench-{i}"),
                    customer_id: "customer".to_owned(),
                    amount: 1.into(),
                    placed_at,
                }))
            })
            .collect()
    };
    let mut sent = 0;
    if options.values.contains_key("count") {
        let count = options.int("count", 0);
        while sent < count {
            let chunk = (count - sent).min(1000);
            queue_spread(queues, &bench_queues, chunk, &batch).await;
            sent += chunk;
        }
    } else {
        let rate = options.int("rate", 100) as f64;
        let duration = Duration::from_secs(options.int("duration-s", 10) as u64);
        let mut ticks = tokio::time::interval(options.millis("tick-ms", 10));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let start = tokio::time::Instant::now();
        let mut reported = (start, 0);
        while start.elapsed() < duration {
            ticks.tick().await;
            let owed = (rate * start.elapsed().as_secs_f64()) as i64 - sent;
            if owed > 0 {
                queue_spread(queues, &bench_queues, owed, &batch).await;
                sent += owed;
            }
            if reported.0.elapsed() >= Duration::from_secs(1) {
                let mut event = options.event("produced");
                event.insert("at".to_owned(), json!(Utc::now().timestamp_millis()));
                event.insert("count".to_owned(), json!(sent - reported.1));
                emit(event);
                reported = (tokio::time::Instant::now(), sent);
            }
        }
    }
    let mut event = options.event("done");
    event.insert("sent".to_owned(), json!(sent));
    emit(event);
}

/// Queues `count` messages spread evenly over `bench_queues`, one `queue_messages` statement per queue.
async fn queue_spread(
    queues: &DurableQueues,
    bench_queues: &[QueueName],
    count: i64,
    batch: &impl Fn(i64) -> Vec<Message<Payload>>,
) {
    let n = bench_queues.len() as i64;
    for (i, queue_name) in bench_queues.iter().enumerate() {
        // The remainder goes to the first queues
        let share = count / n + i64::from((i as i64) < count % n);
        if share > 0 {
            queues
                .queue_messages(queue_name, &batch(share))
                .await
                .expect("queue messages");
        }
    }
}

/// A consumer with a no-op handler, reporting once a second; with `--queues N`, one consumer per queue (each with
/// `--parallel` slots) sharing the report. The first message handled is also reported on its own,
/// for startup time.
async fn bench_consume(queues: &DurableQueues, options: Options) {
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
    let first = Arc::new(std::sync::Once::new());
    let options = Arc::new(options);
    let mut consumers = Vec::new();
    for queue_name in options.bench_queues() {
        let config = ConsumerConfig::new(
            queue_name,
            RedeliveryPolicy::fixed_backoff(Duration::from_millis(100), 5),
        )
        .with_parallel_consumers(options.int("parallel", 1) as usize)
        .with_fetch_pacing(options.fetch_pacing());
        let handler_options = options.clone();
        let handler_latencies = latencies.clone();
        let first = first.clone();
        let consumer = queues
            .consume_from_queue(config, move |message: QueuedMessage<Payload>| {
                let now = Utc::now();
                first.call_once(|| {
                    let mut event = handler_options.event("first");
                    event.insert("at".to_owned(), json!(now.timestamp_millis()));
                    emit(event);
                });
                if let Payload::OrderPlaced(payload) = &message.message.payload {
                    let latency = (now - payload.placed_at)
                        .num_microseconds()
                        .unwrap_or(i64::MAX);
                    handler_latencies.lock().unwrap().push(latency);
                }
                async { Ok(()) }
            })
            .expect("start consumer");
        consumers.push(consumer);
    }
    emit(options.event("ready"));
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    loop {
        ticks.tick().await;
        let latencies = std::mem::take(&mut *latencies.lock().unwrap());
        let mut event = options.event("tick");
        event.insert("at".to_owned(), json!(Utc::now().timestamp_millis()));
        event.insert("count".to_owned(), json!(latencies.len()));
        event.insert("latencies_us".to_owned(), json!(latencies));
        emit(event);
    }
}

async fn resurrect(queues: &DurableQueues, options: &Options) {
    let resurrected = queues
        .resurrect_dead_letter_message(
            &QueueEntryId::new(options.string("id")),
            options.millis("delay-ms", 0),
        )
        .await
        .expect("resurrect");
    let mut event = options.event("resurrected");
    event.insert("found".to_owned(), json!(resurrected.is_some()));
    emit(event);
}

fn describe(
    options: &Options,
    name: &str,
    message: &QueuedMessage<Payload>,
) -> serde_json::Map<String, Value> {
    let mut event = options.event(name);
    event.insert("id".to_owned(), json!(message.id.as_str()));
    event.insert(
        "type".to_owned(),
        json!(message.message.payload.payload_type()),
    );
    event.insert(
        "payload".to_owned(),
        message
            .message
            .payload
            .to_payload()
            .expect("serialize payload"),
    );
    let metadata: serde_json::Map<String, Value> = message
        .message
        .metadata
        .iter()
        .map(|(key, value)| (key.to_owned(), json!(value)))
        .collect();
    event.insert("metadata".to_owned(), Value::Object(metadata));
    event.insert("totalAttempts".to_owned(), json!(message.total_attempts));
    event.insert(
        "redeliveryAttempts".to_owned(),
        json!(message.redelivery_attempts),
    );
    event.insert(
        "deliveryMode".to_owned(),
        json!(message.delivery_mode.as_str()),
    );
    if let Some(order) = &message.message.order {
        event.insert("key".to_owned(), json!(order.key));
        event.insert("order".to_owned(), json!(order.order));
    }
    event
}
