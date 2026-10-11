//! Phase 6: Java <-> Rust interoperability and crash recovery, against the real Essentials 0.60.0 (scenarios from the
//! experiment doc, numbered as in IMPLEMENTATION_PLAN.md).
//!
//! Every test runs its own PostgreSQL on a private Docker network. A Java node is a container of the
//! `interop/java-harness` image (built on first use); a Rust node is the `interop` binary in a child process. Both speak
//! the same CLI and print the same JSON events, one per line, which the tests collect.
//!
//! Needs Docker. Run with `cargo test --features interop --test interop`.

mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::process::Stdio;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::{JAVA_ROWS, JAVA_SCHEMA, TestDatabase, eventually};
use durable_queues_rs::{Message, MessageType, QueueEntry, QueueName};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

const JAVA_IMAGE: &str = "durable-queues-java-harness";
const JAVA_IMAGE_TAG: &str = "interop";

/// Builds the Java harness image once per test run; BuildKit's layer cache makes a rebuild without changes quick.
fn ensure_java_image() {
    static BUILD: Once = Once::new();
    BUILD.call_once(|| {
        let context = concat!(env!("CARGO_MANIFEST_DIR"), "/interop/java-harness");
        let status = std::process::Command::new("docker")
            .args([
                "build",
                "-q",
                "-t",
                &format!("{JAVA_IMAGE}:{JAVA_IMAGE_TAG}"),
                context,
            ])
            .env("DOCKER_BUILDKIT", "1")
            .stdout(Stdio::null())
            .status()
            .expect("run docker build");
        assert!(status.success(), "building the Java harness image failed");
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Side {
    Java,
    Rust,
}

/// One test's PostgreSQL plus the nodes started against it.
struct Cluster {
    db: TestDatabase,
    network: String,
    pg_host: String,
}

impl Cluster {
    async fn start() -> Self {
        ensure_java_image();
        let suffix = uuid::Uuid::now_v7().simple().to_string();
        let network = format!("dq-interop-{suffix}");
        let pg_host = format!("dq-interop-pg-{suffix}");
        let db = TestDatabase::start_on_network(&network, &pg_host).await;
        Self {
            db,
            network,
            pg_host,
        }
    }

    async fn node(&self, side: Side, args: &[&str]) -> Node {
        match side {
            Side::Java => self.java(args, &[]).await,
            Side::Rust => self.rust(args),
        }
    }

    /// A Java node; `env` is for libfaketime (scenario 7).
    async fn java(&self, args: &[&str], env: &[(&str, &str)]) -> Node {
        let jdbc_url = format!("jdbc:postgresql://{}:5432/postgres", self.pg_host);
        let mut cmd: Vec<String> = args.iter().map(|arg| arg.to_string()).collect();
        cmd.extend(["--jdbc-url".to_owned(), jdbc_url]);
        let mut request = GenericImage::new(JAVA_IMAGE, JAVA_IMAGE_TAG)
            .with_cmd(cmd)
            .with_network(&self.network);
        for (name, value) in env {
            request = request.with_env_var(*name, *value);
        }
        let container = request.start().await.expect("start Java harness");
        let events = Events::default();
        events.collect(container.stdout(true));
        Node {
            side: Side::Java,
            process: Process::Java(Box::new(container)),
            events,
        }
    }

    fn rust(&self, args: &[&str]) -> Node {
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_interop"))
            .args(args)
            .args(["--database-url", &self.db.url])
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("start Rust interop binary");
        let events = Events::default();
        events.collect(tokio::io::BufReader::new(child.stdout.take().unwrap()));
        Node {
            side: Side::Rust,
            process: Process::Rust(child),
            events,
        }
    }

    /// Runs a node to completion: a producer until it reports `done`, a resurrect until it reports its result.
    async fn run(&self, side: Side, args: &[&str], last_event: &str) -> Node {
        let node = self.node(side, args).await;
        node.wait_for(last_event, 1).await;
        node
    }

    /// A consumer that has started consuming.
    async fn consumer(&self, side: Side, args: &[&str]) -> Node {
        let node = self.node(side, args).await;
        node.wait_for("ready", 1).await;
        node
    }

    async fn rows(&self) -> Vec<QueueEntry> {
        sqlx::query_as("SELECT * FROM durable_queues")
            .fetch_all(&self.db.pool)
            .await
            .expect("read rows")
    }

    async fn row(&self, id: &str) -> Option<QueueEntry> {
        self.rows()
            .await
            .into_iter()
            .find(|row| row.id.as_str() == id)
    }

    /// Waits for every message to be acknowledged. Nodes print `handled` from inside the handler, before the ack.
    async fn wait_until_empty(&self) {
        eventually("every message to be acknowledged", || async {
            self.db.count("SELECT count(*) FROM durable_queues").await == 0
        })
        .await;
    }
}

enum Process {
    Java(Box<ContainerAsync<GenericImage>>),
    Rust(tokio::process::Child),
}

struct Node {
    side: Side,
    process: Process,
    events: Events,
}

impl Node {
    fn events(&self, kind: &str) -> Vec<Value> {
        self.events.of(kind)
    }

    async fn wait_for(&self, kind: &str, count: usize) {
        let what = format!("{count} '{kind}' events from the {:?} node", self.side);
        eventually(&what, || async { self.events(kind).len() >= count }).await;
    }

    fn ids(&self, kind: &str) -> Vec<String> {
        self.events(kind)
            .iter()
            .map(|event| event["id"].as_str().unwrap().to_owned())
            .collect()
    }

    fn event_for(&self, kind: &str, id: &str) -> Option<Value> {
        self.events(kind)
            .into_iter()
            .find(|event| event["id"] == id)
    }

    /// When the first `kind` event arrived.
    fn arrival(&self, kind: &str) -> DateTime<Utc> {
        self.events.with_arrival(kind)[0].0
    }

    /// When the `kind` event for message `id` arrived.
    fn arrival_for(&self, kind: &str, id: &str) -> DateTime<Utc> {
        self.events
            .with_arrival(kind)
            .into_iter()
            .find(|(_, event)| event["id"] == id)
            .unwrap()
            .0
    }

    /// A crash: `docker kill` for Java, SIGKILL for Rust. Nothing runs on the way down.
    async fn kill(&mut self) {
        match &mut self.process {
            Process::Java(container) => {
                let status = tokio::process::Command::new("docker")
                    .args(["kill", container.id()])
                    .stdout(Stdio::null())
                    .status()
                    .await
                    .expect("run docker kill");
                assert!(status.success(), "docker kill failed");
            }
            Process::Rust(child) => child.kill().await.expect("kill Rust node"),
        }
    }
}

/// The JSON events a node printed, with the instant each arrived.
#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<Arrived>>>);

/// An event and the time it arrived.
type Arrived = (DateTime<Utc>, Value);

impl Events {
    fn collect(&self, stdout: impl AsyncBufRead + Unpin + Send + 'static) {
        let events = self.0.clone();
        tokio::spawn(async move {
            let mut lines = stdout.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                match serde_json::from_str::<Value>(&line) {
                    Ok(event) => events.lock().unwrap().push((Utc::now(), event)),
                    Err(_) => eprintln!("node: {line}"),
                }
            }
        });
    }

    fn of(&self, kind: &str) -> Vec<Value> {
        self.with_arrival(kind)
            .into_iter()
            .map(|(_, event)| event)
            .collect()
    }

    fn with_arrival(&self, kind: &str) -> Vec<Arrived> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, event)| event["event"] == kind)
            .cloned()
            .collect()
    }
}

/// The payload and metadata each message was stored with, by id - what a consumer must hand its handler.
async fn stored_messages(cluster: &Cluster) -> HashMap<String, QueueEntry> {
    cluster
        .rows()
        .await
        .into_iter()
        .map(|row| (row.id.as_str().to_owned(), row))
        .collect()
}

/// Asserts that `consumer` handled exactly the `stored` messages, each with its stored type, payload and metadata,
/// and every ordered key in order.
fn assert_handled_intact(consumer: &Node, stored: &HashMap<String, QueueEntry>) {
    let handled = consumer.events("handled");
    let ids: HashSet<_> = handled.iter().map(|e| e["id"].as_str().unwrap()).collect();
    assert_eq!(ids.len(), handled.len(), "no message is handled twice");
    assert_eq!(
        ids,
        stored.keys().map(String::as_str).collect(),
        "every message is handled"
    );
    let mut orders_per_key: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for event in &handled {
        let row = &stored[event["id"].as_str().unwrap()];
        assert_eq!(event["type"], row.message_payload_type.as_str());
        assert_eq!(
            event["payload"], row.message_payload,
            "payload of {}",
            row.id
        );
        assert_eq!(
            Some(&event["metadata"]),
            row.meta_data.as_ref(),
            "metadata of {}",
            row.id
        );
        assert_eq!(event["totalAttempts"], 1);
        assert_eq!(event["redeliveryAttempts"], 0);
        if let Some(key) = event["key"].as_str() {
            orders_per_key
                .entry(key.to_owned())
                .or_default()
                .push(event["order"].as_i64().unwrap());
        }
    }
    assert!(!orders_per_key.is_empty(), "ordered messages were handled");
    for (key, orders) in orders_per_key {
        let expected: Vec<i64> = (0..orders.len() as i64).collect();
        assert_eq!(orders, expected, "key {key} is handled in order");
    }
}

/// Scenarios 1 and 2: a producer on one side, a consumer on the other, normal and ordered messages.
async fn producer_to_consumer(producer: Side, consumer: Side) {
    let cluster = Cluster::start().await;
    cluster
        .run(
            producer,
            &["produce", "--count", "20", "--prefix", "plain"],
            "done",
        )
        .await;
    cluster
        .run(
            producer,
            &[
                "produce",
                "--count",
                "5",
                "--prefix",
                "ordered",
                "--ordered-keys",
                "3",
            ],
            "done",
        )
        .await;
    let stored = stored_messages(&cluster).await;
    assert_eq!(stored.len(), 35);

    let node = cluster
        .consumer(consumer, &["consume", "--parallel", "4"])
        .await;
    node.wait_for("handled", 35).await;
    assert_handled_intact(&node, &stored);
    cluster.wait_until_empty().await;
}

#[tokio::test]
async fn scenario_1_java_producer_to_rust_consumer() {
    producer_to_consumer(Side::Java, Side::Rust).await;
}

#[tokio::test]
async fn scenario_2_rust_producer_to_java_consumer() {
    // The Java node's `type` is the class Jackson deserialized the payload into, and its `payload` is that object
    // serialized again: equal to what Rust stored, so Jackson read every field.
    producer_to_consumer(Side::Rust, Side::Java).await;
}

/// Java's `DefaultQueuedMessage.getDeliveryMode()` returns `NORMAL` for every message, ordered ones included
/// (Essentials 0.60.0 `DefaultQueuedMessage.java:111`). Rust reports `IN_ORDER`. Recorded as a Java discrepancy.
#[tokio::test]
async fn java_reports_every_queued_message_as_normal() {
    let cluster = Cluster::start().await;
    cluster
        .run(
            Side::Rust,
            &[
                "produce",
                "--count",
                "1",
                "--prefix",
                "ordered",
                "--ordered-keys",
                "1",
            ],
            "done",
        )
        .await;
    let java = cluster.consumer(Side::Java, &["consume"]).await;
    java.wait_for("handled", 1).await;
    let handled = &java.events("handled")[0];
    assert_eq!(
        handled["key"], "ordered-key-0",
        "Java has the OrderedMessage"
    );
    assert_eq!(handled["deliveryMode"], "NORMAL", "but reports NORMAL");
}

#[tokio::test]
async fn scenario_3_java_and_rust_competing_consumers() {
    let cluster = Cluster::start().await;
    let java = cluster
        .consumer(
            Side::Java,
            &["consume", "--parallel", "2", "--handle-ms", "5"],
        )
        .await;
    let rust = cluster
        .consumer(
            Side::Rust,
            &["consume", "--parallel", "2", "--handle-ms", "5"],
        )
        .await;
    let java_producer = cluster.node(Side::Java, &["produce", "--count", "100", "--prefix", "j"]);
    let rust_producer = cluster.node(Side::Rust, &["produce", "--count", "100", "--prefix", "r"]);
    let (java_producer, rust_producer) = tokio::join!(java_producer, rust_producer);
    java_producer.wait_for("done", 1).await;
    rust_producer.wait_for("done", 1).await;
    let queued: HashSet<String> = java_producer
        .ids("queued")
        .into_iter()
        .chain(rust_producer.ids("queued"))
        .collect();
    assert_eq!(queued.len(), 200);

    eventually("all 200 messages to be handled", || async {
        java.events("handled").len() + rust.events("handled").len() >= 200
    })
    .await;
    let java_ids = java.ids("handled");
    let rust_ids = rust.ids("handled");
    let mut all = java_ids.clone();
    all.extend(rust_ids.iter().cloned());
    let union: HashSet<String> = all.iter().cloned().collect();
    assert_eq!(union, queued, "no message is lost");
    // Duplicates are only expected from handling timeouts, and every handler here finishes in milliseconds.
    assert_eq!(all.len(), union.len(), "no message is handled twice");
    assert!(
        !java_ids.is_empty() && !rust_ids.is_empty(),
        "both nodes competed: Java handled {}, Rust {}",
        java_ids.len(),
        rust_ids.len()
    );
    cluster.wait_until_empty().await;
}

/// Scenarios 4 and 5: a consumer hangs on its fourth message and is killed; a consumer on the other side redelivers
/// that message once it has been claimed for longer than its own handling timeout.
async fn crash_recovery(crashing: Side, recovering: Side) {
    let timeout = Duration::from_secs(3);
    let cluster = Cluster::start().await;
    cluster
        .run(
            crashing,
            &["produce", "--count", "10", "--prefix", "crash"],
            "done",
        )
        .await;

    let mut crashed = cluster
        .node(crashing, &["consume-and-crash-after", "3"])
        .await;
    crashed.wait_for("hung", 1).await;
    let hung_id = crashed.ids("hung").remove(0);
    let claimed = cluster.row(&hung_id).await.expect("hung row");
    assert!(claimed.is_being_delivered);
    let claimed_at = claimed.delivery_ts.expect("claimed row has a delivery_ts");
    crashed.kill().await;
    assert_eq!(crashed.events("handled").len(), 3);

    let timeout_ms = timeout.as_millis().to_string();
    let recovered = cluster
        .consumer(recovering, &["consume", "--timeout-ms", &timeout_ms])
        .await;
    recovered.wait_for("handled", 7).await;
    let redelivered = recovered
        .event_for("handled", &hung_id)
        .expect("the hung message is redelivered");
    // The stuck-message reset counts a redelivery; the new claim counts a delivery (CONTRACT.md §6).
    assert_eq!(redelivered["totalAttempts"], 2);
    assert_eq!(redelivered["redeliveryAttempts"], 1);

    // Not before the message has been claimed for `timeout`, and within the documented 2 x timeout (§6).
    let redelivered_at = recovered.arrival_for("handled", &hung_id);
    let waited = (redelivered_at - claimed_at).to_std().unwrap();
    assert!(
        waited >= timeout,
        "redelivered after {waited:?}, before the {timeout:?} timeout"
    );
    assert!(
        waited <= 2 * timeout + Duration::from_secs(5),
        "redelivered only after {waited:?}"
    );

    let mut all: Vec<String> = crashed.ids("handled");
    all.extend(recovered.ids("handled"));
    assert_eq!(all.len(), 10);
    assert_eq!(
        all.iter().collect::<HashSet<_>>().len(),
        10,
        "no duplicates besides the hung one"
    );
    cluster.wait_until_empty().await;
}

#[tokio::test]
async fn scenario_4_java_consumer_killed_rust_recovers() {
    crash_recovery(Side::Java, Side::Rust).await;
}

#[tokio::test]
async fn scenario_5_rust_consumer_killed_java_recovers() {
    crash_recovery(Side::Rust, Side::Java).await;
}

/// The row state the two implementations must agree on.
fn settled_state(row: &QueueEntry) -> (i32, i32, bool, bool, bool, bool) {
    (
        row.total_attempts,
        row.redelivery_attempts,
        row.is_dead_letter_message,
        row.is_being_delivered,
        row.next_delivery_ts.is_none(),
        row.delivery_ts.is_none(),
    )
}

/// Scenario 6, dead letters: each side exhausts the redeliveries of a message the other side produced; the dead
/// letters are identical. Each is then resurrected by the side that did not dead-letter it and handled by it too.
#[tokio::test]
async fn scenario_6_dead_letters_written_by_one_side_are_observed_by_the_other() {
    let cluster = Cluster::start().await;
    let failing = [
        "--fail-attempts",
        "100",
        "--max-redeliveries",
        "2",
        "--redelivery-delay-ms",
        "100",
    ];
    let mut dead_letters = HashMap::new();
    for (failer, other, queue) in [
        (Side::Java, Side::Rust, "JavaFails"),
        (Side::Rust, Side::Java, "RustFails"),
    ] {
        let produced = cluster
            .run(
                other,
                &["produce", "--count", "1", "--queue", queue],
                "done",
            )
            .await;
        let id = produced.ids("queued").remove(0);
        let mut args = vec!["consume", "--queue", queue];
        args.extend(failing);
        let mut consumer = cluster.consumer(failer, &args).await;
        eventually("the message to become a dead letter", || async {
            cluster
                .row(&id)
                .await
                .is_some_and(|row| row.is_dead_letter_message)
        })
        .await;
        consumer.kill().await;
        let failures: Vec<(i64, i64)> = consumer
            .events("failed")
            .iter()
            .map(|e| {
                (
                    e["totalAttempts"].as_i64().unwrap(),
                    e["redeliveryAttempts"].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            failures,
            [(1, 0), (2, 1), (3, 2)],
            "{failer:?}: max 2 redeliveries = 3 deliveries"
        );
        dead_letters.insert(failer, (id, other, queue));
    }

    let java_dead_letter = cluster.row(&dead_letters[&Side::Java].0).await.unwrap();
    let rust_dead_letter = cluster.row(&dead_letters[&Side::Rust].0).await.unwrap();
    assert_eq!(
        settled_state(&java_dead_letter),
        (3, 2, true, false, true, true)
    );
    assert_eq!(
        settled_state(&java_dead_letter),
        settled_state(&rust_dead_letter)
    );
    // The error text is each language's own (CONTRACT.md §7): a Java stack trace, a Rust error chain. Java's is the
    // trace of the UnitOfWorkException its single-operation transaction wraps the handler's exception in.
    let java_error = java_dead_letter.last_delivery_error.unwrap();
    assert!(
        java_error.starts_with(
            "dk.trustworks.essentials.components.foundation.transaction.UnitOfWorkException: \
             java.lang.IllegalStateException: interop: failing delivery attempt 3\n\tat "
        ),
        "{java_error}"
    );
    assert_eq!(
        rust_dead_letter.last_delivery_error.unwrap(),
        "interop: failing delivery attempt 3"
    );

    // Consumers stay up until the end: a node stopped between its `handled` event and the ack leaves the row claimed.
    let mut consumers = Vec::new();
    for (id, other, queue) in dead_letters.values() {
        let resurrected = cluster
            .run(*other, &["resurrect", "--id", id], "resurrected")
            .await;
        assert_eq!(resurrected.events("resurrected")[0]["found"], true);
        let consumer = cluster
            .consumer(*other, &["consume", "--queue", queue])
            .await;
        consumer.wait_for("handled", 1).await;
        // Resurrection keeps the counters (CONTRACT.md §5), on either side.
        let handled = consumer.event_for("handled", id).unwrap();
        assert_eq!(
            handled["totalAttempts"], 4,
            "{other:?} handles the resurrected message"
        );
        assert_eq!(handled["redeliveryAttempts"], 2);
        consumers.push(consumer);
    }
    cluster.wait_until_empty().await;
}

/// Scenario 6, delay and retry: `producer` queues a message with a delivery delay; the other side fails its first
/// delivery and schedules a retry; `producer`'s side then handles the retry. Neither delivery comes early.
async fn delay_and_retry(producer: Side, failer: Side) {
    let cluster = Cluster::start().await;
    let produced = cluster
        .run(
            producer,
            &["produce", "--count", "1", "--delay-ms", "2000"],
            "done",
        )
        .await;
    let id = produced.ids("queued").remove(0);
    let queued = cluster.row(&id).await.unwrap();
    let due = queued.next_delivery_ts.unwrap();
    assert_eq!(
        (due - queued.added_ts).num_milliseconds(),
        2000,
        "the delay is stored as written"
    );

    let mut failing = cluster
        .consumer(
            failer,
            &[
                "consume",
                "--fail-attempts",
                "1",
                "--redelivery-delay-ms",
                "2000",
            ],
        )
        .await;
    failing.wait_for("failed", 1).await;
    assert!(
        failing.arrival("failed") >= due,
        "the delayed message is not delivered early"
    );
    eventually("the retry to be written", || async {
        cluster
            .row(&id)
            .await
            .is_some_and(|row| row.redelivery_attempts == 1 && !row.is_being_delivered)
    })
    .await;
    failing.kill().await;
    let retried = cluster.row(&id).await.unwrap();
    assert_eq!(settled_state(&retried), (1, 1, false, false, false, true));
    assert!(
        retried
            .last_delivery_error
            .as_deref()
            .unwrap()
            .contains("interop: failing delivery attempt 1")
    );
    let retry_due = retried.next_delivery_ts.unwrap();

    let finishing = cluster.consumer(producer, &["consume"]).await;
    finishing.wait_for("handled", 1).await;
    assert!(
        finishing.arrival("handled") >= retry_due,
        "the retry is not delivered early"
    );
    let handled = &finishing.events("handled")[0];
    assert_eq!(handled["totalAttempts"], 2);
    assert_eq!(handled["redeliveryAttempts"], 1);
    cluster.wait_until_empty().await;
}

#[tokio::test]
async fn scenario_6_java_delay_rust_retry() {
    delay_and_retry(Side::Java, Side::Rust).await;
}

#[tokio::test]
async fn scenario_6_rust_delay_java_retry() {
    delay_and_retry(Side::Rust, Side::Java).await;
}

/// libfaketime in the Java image: the JVM's wall clock runs `offset` ahead; its monotonic clock is left alone, so its
/// timers and sleeps behave.
fn clock_ahead(offset: &'static str) -> [(&'static str, &'static str); 3] {
    [
        ("LD_PRELOAD", "/usr/local/lib/libfaketime.so.1"),
        ("FAKETIME", offset),
        ("FAKETIME_DONT_FAKE_MONOTONIC", "1"),
    ]
}

/// Scenario 7a: timestamps come from the producer's clock (CONTRACT.md §2), so a producer whose clock runs ahead
/// delays its messages by the skew on every consumer.
#[tokio::test]
async fn scenario_7_a_producer_clock_ahead_delays_delivery_by_the_skew() {
    let cluster = Cluster::start().await;
    let rust = cluster.consumer(Side::Rust, &["consume"]).await;
    let java = cluster
        .java(&["produce", "--count", "1"], &clock_ahead("+10"))
        .await;
    java.wait_for("done", 1).await;
    let produced_at = java.arrival("done");
    let row = cluster.row(&java.ids("queued")[0]).await.unwrap();
    let skew = row.added_ts - Utc::now();
    assert!(
        skew.num_seconds() >= 9,
        "added_ts is on the Java clock, {skew} ahead"
    );

    rust.wait_for("handled", 1).await;
    let waited = (rust.arrival("handled") - produced_at).to_std().unwrap();
    assert!(
        waited >= Duration::from_secs(8),
        "a message queued with no delay waited {waited:?} for the producer's clock"
    );
}

/// Scenario 7b: a consumer whose clock runs ahead by more than the handling timeout treats another node's in-flight
/// message as stuck as soon as it polls, and redelivers it while the first handler still runs: a duplicate.
#[tokio::test]
async fn scenario_7_a_consumer_clock_ahead_resets_in_flight_messages_early() {
    let cluster = Cluster::start().await;
    let rust = cluster
        .consumer(Side::Rust, &["consume", "--handle-ms", "8000"])
        .await;
    cluster
        .run(Side::Rust, &["produce", "--count", "1"], "done")
        .await;
    eventually("Rust to claim the message", || async {
        cluster
            .rows()
            .await
            .first()
            .is_some_and(|row| row.is_being_delivered)
    })
    .await;
    let id = cluster.rows().await[0].id.as_str().to_owned();

    // Java's default 30 s timeout, on a clock 60 s ahead: the 0 s old claim looks 60 s old.
    let java = cluster.java(&["consume"], &clock_ahead("+60")).await;
    java.wait_for("handled", 1).await;
    assert!(
        rust.events("handled").is_empty(),
        "Java redelivered while Rust was still handling"
    );
    let redelivered = java.event_for("handled", &id).unwrap();
    assert_eq!(redelivered["totalAttempts"], 2);
    assert_eq!(redelivered["redeliveryAttempts"], 1);
    rust.wait_for("handled", 1).await;
    assert_eq!(
        rust.ids("handled"),
        [id],
        "the message was handled by both nodes"
    );
}

/// A Rust-only payload type, unknown to the Java harness.
#[derive(Debug, Serialize, Deserialize)]
struct RustOnly {
    value: i32,
}

impl MessageType for RustOnly {
    const TYPE_NAME: &'static str = "rust.only.RustOnly";
}

/// CONTRACT.md §12, hazard 1: a node that claims a message whose type it does not know dead-letters it, Java and
/// Rust alike. Mixed queues need the same types on both sides.
#[tokio::test]
async fn unknown_payload_types_are_dead_lettered_by_either_side() {
    let cluster = Cluster::start().await;
    // Java-only: the fixture's InvoiceIssued, on the Invoices queue
    cluster.run(Side::Java, &["produce-fixture"], "done").await;
    let queues = cluster.db.durable_queues(cluster.db.pool.clone()).await;
    let rust_only = queues
        .queue_message(
            &QueueName::new("RustOnly"),
            &Message::new(RustOnly { value: 1 }),
        )
        .await
        .unwrap();

    let _rust = cluster
        .consumer(Side::Rust, &["consume", "--queue", "Invoices"])
        .await;
    let _java = cluster
        .consumer(Side::Java, &["consume", "--queue", "RustOnly"])
        .await;
    eventually("both messages to be dead letters", || async {
        let rows = cluster.rows().await;
        let dead = |queue: &str| {
            rows.iter()
                .any(|row| row.queue_name.as_str() == queue && row.is_dead_letter_message)
        };
        dead("Invoices") && dead("RustOnly")
    })
    .await;
    let java_reason = cluster
        .row(rust_only.as_str())
        .await
        .unwrap()
        .last_delivery_error
        .unwrap();
    assert!(
        java_reason.starts_with(
            "dk.trustworks.essentials.components.foundation.messaging.queue.DurableQueueDeserializationException: "
        ) && java_reason.contains("Failed to deserialize message payload of type rust.only.RustOnly"),
        "{java_reason}"
    );
    let invoice = cluster
        .rows()
        .await
        .into_iter()
        .find(|row| row.queue_name.as_str() == "Invoices")
        .unwrap();
    let rust_reason = invoice.last_delivery_error.unwrap();
    assert!(
        rust_reason.contains("com.acme.billing.InvoiceIssued"),
        "{rust_reason}"
    );
    // Dead-lettered at fetch, before any delivery attempt was counted as handled
    assert_eq!(invoice.redelivery_attempts, 0);
}

/// CONTRACT.md §13: the synthetic `java_rows.sql` against the rows the real Java harness writes for the same
/// messages. Ids and absolute timestamps differ by nature; everything else must match. The fixture's row 7 (a payload
/// that does not match its type) cannot be produced through Java's serializer and is left out.
#[tokio::test]
async fn the_synthetic_java_fixture_matches_what_java_writes() {
    let cluster = Cluster::start().await;
    cluster.db.run_script(JAVA_SCHEMA).await;
    cluster.db.run_script(JAVA_ROWS).await;
    cluster
        .db
        .run_script(
            "CREATE TABLE synthetic_rows AS SELECT * FROM durable_queues
               WHERE id <> '2f1c7c6e-6a4b-11f1-9a2e-0242ac120008';
             DELETE FROM durable_queues;",
        )
        .await;
    cluster.run(Side::Java, &["produce-fixture"], "done").await;

    let synthetic = normalized_rows(&cluster, "synthetic_rows").await;
    let java = normalized_rows(&cluster, "durable_queues").await;
    assert_eq!(java.len(), 7);
    assert_eq!(synthetic, java);
}

async fn normalized_rows(cluster: &Cluster, table: &str) -> Vec<Value> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT to_jsonb(r) FROM (
           SELECT queue_name, message_payload, message_payload_type, meta_data, delivery_mode, key, key_order,
                  total_attempts, redelivery_attempts, is_being_delivered, is_dead_letter_message,
                  delivery_ts IS NULL AS no_delivery_ts, next_delivery_ts IS NULL AS no_next_delivery_ts,
                  round(extract(epoch FROM next_delivery_ts - added_ts)) AS delay_seconds,
                  split_part(last_delivery_error, E'\\n', 1) AS error_first_line
           FROM {table}) r
         ORDER BY message_payload_type, message_payload::text"
    )))
    .fetch_all(&cluster.db.pool)
    .await
    .expect("read normalized rows")
}
