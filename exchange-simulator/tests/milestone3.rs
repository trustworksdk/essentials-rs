//! The Milestone 3 acceptance (plan item 3.5), in process:
//! - the scenario suite: every file in `scenarios/` passes, and two runs in separate processes give byte-identical
//!   event logs;
//! - each fault type is visible at the receiver, and absent from `/api/events`;
//! - a reset gives a clean state.
//!
//! `tests/e2e.rs` runs the scenario suite in the service image as well.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::post;
use common::TestApp;
use exchange_simulator::config::{ClockMode, IdMode};
use serde_json::{Value, json};

fn scenario_files() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios");
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "yaml"))
        .collect();
    files.sort();
    files
}

/// Runs the whole suite through the CLI, as CI does, and returns the JSON reports.
fn run_suite() -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_exchange-simulator"))
        .args(["scenario", "run", "--json"])
        .args(scenario_files())
        .env_remove("RUST_LOG")
        .output()
        .expect("run the CLI");
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn the_scenario_suite_passes_and_repeats_byte_for_byte() {
    let files = scenario_files();
    assert!(files.len() >= 5, "{files:?}");
    let first = run_suite();
    let second = run_suite();
    let reports = first.as_array().unwrap();
    assert_eq!(reports.len(), files.len());
    for report in reports {
        assert_eq!(report["passed"], true, "{report}");
    }
    assert_eq!(
        first, second,
        "identical reports, event-log fingerprints included"
    );
    let market = reports
        .iter()
        .find(|r| r["name"] == "Random market")
        .expect("the random market scenario");
    assert!(market["eventLog"]["events"].as_u64().unwrap() > 1000);
}

/// One endpoint per fault type, recording what each received.
#[derive(Clone, Default)]
struct Receiver {
    received: Arc<Mutex<Vec<(String, Value, Instant)>>>,
}

impl Receiver {
    async fn start(&self) -> String {
        let app = Router::new()
            .route(
                "/{fault}",
                post(
                    |State(receiver): State<Receiver>,
                     UrlPath(fault): UrlPath<String>,
                     _: HeaderMap,
                     body: axum::body::Bytes| async move {
                        let body = serde_json::from_slice(&body).unwrap();
                        receiver
                            .received
                            .lock()
                            .unwrap()
                            .push((fault, body, Instant::now()));
                        StatusCode::OK
                    },
                ),
            )
            .with_state(self.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    fn at(&self, fault: &str) -> Vec<(Value, Instant)> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .filter(|(f, ..)| f == fault)
            .map(|(_, body, at)| (body.clone(), *at))
            .collect()
    }
}

const FAULTS: [(&str, &str); 7] = [
    ("clean", r#"{}"#),
    (
        "missing",
        r#"{ "missing": { "paths": ["payload.price"] } }"#,
    ),
    ("nulls", r#"{ "nulls": { "paths": ["payload.price"] } }"#),
    (
        "invalid",
        r#"{ "invalid": { "paths": ["payload.price"] } }"#,
    ),
    ("duplicate", r#"{ "duplicateProbability": 1 }"#),
    ("delay", r#"{ "delay": { "ms": 400 } }"#),
    // Longer than the delay: a delay holds up its whole delivery batch, the hold's recording included, and a hold
    // counts from its decision.
    ("outOfOrder", r#"{ "outOfOrder": { "holdMs": 1500 } }"#),
];

async fn deterministic() -> TestApp {
    TestApp::start_with(|c| {
        c.ids.mode = IdMode::Deterministic;
        c.ids.seed = 33;
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
    })
    .await
}

/// Registers instruments and participants, then two trades.
async fn commands(app: &TestApp) {
    app.seed().await;
    for price in [10025, 10030] {
        trade(app, price).await;
    }
}

async fn trade(app: &TestApp, price: i64) {
    assert_eq!(
        app.order("A", "SELL", Some(price), 10).await.0,
        StatusCode::CREATED
    );
    assert_eq!(
        app.order("B", "BUY", Some(price), 10).await.0,
        StatusCode::CREATED
    );
}

async fn events(app: &TestApp) -> Value {
    app.get("/api/events?limit=1000").await.1
}

#[tokio::test]
async fn each_fault_reaches_the_receiver_and_never_the_log_and_reset_starts_clean() {
    // The reference: the same commands with no subscriptions at all.
    let mut reference = deterministic().await;
    commands(&reference).await;
    let canonical = events(&reference).await;
    reference.stop().await;
    let trades: Vec<Value> = canonical
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["eventType"] == "TradeExecuted")
        .cloned()
        .collect();
    assert_eq!(trades.len(), 2);

    let receiver = Receiver::default();
    let base = receiver.start().await;
    let mut app = deterministic().await;
    app.seed().await;
    faulted_trades(&app, &base).await;

    // Two trades per subscription, and two more copies from the duplicate one.
    let deadline = Instant::now() + Duration::from_secs(10);
    while receiver.received.lock().unwrap().len() < 2 * FAULTS.len() + 2
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Absent from the log: byte-identical to the run without any subscriptions or faults.
    assert_eq!(events(&app).await, canonical);

    let clean = receiver.at("clean");
    assert_eq!(
        clean.iter().map(|(b, _)| b.clone()).collect::<Vec<_>>(),
        trades,
        "the clean subscription gets the canonical events"
    );
    let bodies = |fault: &str| {
        receiver
            .at(fault)
            .into_iter()
            .map(|(b, _)| b)
            .collect::<Vec<_>>()
    };
    for body in bodies("missing") {
        assert!(body["payload"].get("price").is_none(), "{body}");
    }
    for body in bodies("nulls") {
        assert!(body["payload"]["price"].is_null(), "{body}");
    }
    for body in bodies("invalid") {
        assert_eq!(body["payload"]["price"], -12345, "{body}");
    }
    for fault in ["missing", "nulls", "invalid"] {
        let damaged = bodies(fault);
        assert_eq!(damaged.len(), 2, "{fault}");
        for (body, trade) in damaged.iter().zip(&trades) {
            let mut repaired = body.clone();
            repaired["payload"]["price"] = trade["payload"]["price"].clone();
            assert_eq!(&repaired, trade, "{fault}: only the price differs");
        }
    }
    let duplicated = bodies("duplicate");
    assert_eq!(
        duplicated,
        [
            trades[0].clone(),
            trades[0].clone(),
            trades[1].clone(),
            trades[1].clone()
        ]
    );
    let delayed = receiver.at("delay");
    assert_eq!(delayed.len(), 2);
    for ((_, at), (_, clean_at)) in delayed.iter().zip(&clean) {
        assert!(
            *at >= *clean_at + Duration::from_millis(300),
            "delayed against the clean copy"
        );
    }
    let reordered = bodies("outOfOrder");
    assert_eq!(
        reordered,
        [trades[1].clone(), trades[0].clone()],
        "the held first trade arrives last"
    );

    // A reset leaves a clean state, and the same commands then give the canonical log again.
    assert_eq!(
        app.call(Method::POST, "/test/reset", None).await.0,
        StatusCode::NO_CONTENT
    );
    for table in [
        "events",
        "orders",
        "webhook_subscriptions",
        "webhook_deliveries",
        "webhook_attempts",
    ] {
        assert_eq!(
            app.count(&format!("SELECT COUNT(*) FROM {table}")),
            0,
            "{table}"
        );
    }
    assert_eq!(events(&app).await, json!([]));
    commands(&app).await;
    assert_eq!(events(&app).await, canonical);
    app.stop().await;
}

/// The trades of `commands`, with one subscription per fault type. The out-of-order subscription holds the first
/// trade; its policy is then cleared, so the second goes out at once and overtakes it.
async fn faulted_trades(app: &TestApp, base: &str) {
    let mut hold_id = String::new();
    for (fault, policy) in FAULTS {
        let url = format!("{base}/{fault}");
        let (_, subscription) = app
            .post(
                "/api/webhooks",
                json!({ "url": url, "eventTypes": ["TradeExecuted"] }),
            )
            .await;
        let id = subscription["id"].as_str().unwrap().to_string();
        let (status, body) = app
            .call(
                Method::PUT,
                &format!("/test/webhooks/{id}/faults"),
                Some(serde_json::from_str(policy).unwrap()),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{fault}: {body}");
        if fault == "outOfOrder" {
            hold_id = id;
        }
    }
    trade(app, 10025).await;
    // Wait until the first trade is held, then let later ones through.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let attempts = app
            .get(&format!("/test/attempts?subscriptionId={hold_id}"))
            .await
            .1;
        if attempts.as_array().is_some_and(|a| !a.is_empty()) || Instant::now() > deadline {
            assert_eq!(attempts[0]["outcome"], "HELD", "{attempts}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (status, _) = app
        .call(
            Method::DELETE,
            &format!("/test/webhooks/{hold_id}/faults"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    trade(app, 10030).await;
}
