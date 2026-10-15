//! Scenarios (plan item 3.1): the example files pass, failures are reported precisely, runs are reproducible, and the
//! CLI and the test-control API give the same verdicts.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use common::TestApp;
use exchange_simulator::config::Config;
use exchange_simulator::ids::IdScheme;
use exchange_simulator::scenario::{self, Scenario};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

fn examples() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios");
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "yaml"))
        .collect();
    files.sort();
    assert!(files.len() >= 3);
    files
}

/// The service's default configuration, as scenarios start from it.
async fn base_config() -> Config {
    let app = TestApp::start().await;
    app.config.clone()
}

const FAILING: &str = r#"
name: deliberately wrong
instruments: [ { id: ACME, symbol: ACME, currency: DKK, currencyScale: 2 } ]
participants: [ { id: A, name: A }, { id: B, name: B } ]
steps:
  - order: { as: sell, participantId: A, side: SELL, type: LIMIT, priceTicks: 10025, quantity: 200 }
  - order: { as: buy, participantId: B, side: BUY, type: LIMIT, priceTicks: 10025, quantity: 150,
             expect: { status: 400 } }
  - expect: { order: sell, matches: { status: OPEN, remainingQuantity: 200 } }
  - expect: { trades: {}, count: 2 }
  - cancel: { order: nobody }
  - expect: { order: sell, matches: { status: PARTIALLY_FILLED } }
"#;

#[tokio::test]
async fn the_example_scenarios_pass() {
    let config = base_config().await;
    for file in examples() {
        let scenario = Scenario::parse(&std::fs::read_to_string(&file).unwrap()).unwrap();
        let report = scenario::run(&scenario, &config).await.unwrap();
        assert!(report.passed, "{}:\n{}", file.display(), report.render());
        assert_eq!(
            report.steps.len(),
            scenario.steps.len() + 1,
            "every step ran"
        );
    }
}

#[tokio::test]
async fn failures_are_reported_per_step_and_the_run_continues() {
    let report = scenario::run(&Scenario::parse(FAILING).unwrap(), &base_config().await)
        .await
        .unwrap();
    assert!(!report.passed);
    let failures: Vec<(usize, Vec<String>)> = report
        .steps
        .iter()
        .filter(|s| !s.passed)
        .map(|s| (s.index, s.failures.clone()))
        .collect();
    assert_eq!(failures.len(), 4, "{}", report.render());
    assert_eq!(failures[0].0, 2);
    assert!(
        failures[0].1[0].starts_with("expected HTTP 400, got 201"),
        "{:?}",
        failures[0]
    );
    assert_eq!(
        failures[1],
        (
            3,
            vec![
                "remainingQuantity: expected 200, got 50".to_string(),
                "status: expected \"OPEN\", got \"PARTIALLY_FILLED\"".to_string()
            ]
        )
    );
    assert_eq!(
        failures[2],
        (4, vec!["expected 2 matching trades, found 1".to_string()])
    );
    assert!(
        failures[3].1[0].contains("unknown order reference nobody"),
        "{:?}",
        failures[3]
    );
    assert!(
        report.steps[6].passed,
        "steps after failures still run: {}",
        report.render()
    );
    assert!(report.render().contains("FAIL   3 expect order sell"));
}

#[tokio::test]
async fn a_scenario_gives_the_same_report_and_ids_every_time() {
    let config = base_config().await;
    let text = format!(
        "{}\n  - expect: {{ events: {{ sequence: 1, eventId: \"{}\" }}, count: 1 }}\n",
        std::fs::read_to_string(&examples()[0]).unwrap().trim_end(),
        IdScheme::Deterministic { seed: 1 }.event(1)
    );
    let scenario = Scenario::parse(&text).unwrap();
    let first = scenario::run(&scenario, &config).await.unwrap();
    let second = scenario::run(&scenario, &config).await.unwrap();
    assert!(first.passed, "{}", first.render());
    assert_eq!(first, second);
}

fn cli(args: &[&str]) -> (i32, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_exchange-simulator"))
        .args(args)
        .env_remove("RUST_LOG")
        .output()
        .expect("run the binary");
    (
        output.status.code().unwrap(),
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap(),
    )
}

#[test]
fn the_cli_exit_code_is_the_verdict() {
    let files: Vec<String> = examples()
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    let args: Vec<&str> = ["scenario", "run"]
        .into_iter()
        .chain(files.iter().map(String::as_str))
        .collect();
    let (code, stdout, _) = cli(&args);
    assert_eq!(code, 0, "{stdout}");
    assert_eq!(stdout.matches(": PASSED").count(), files.len());

    let dir = std::env::temp_dir().join(format!("exchange-cli-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let failing = dir.join("failing.yaml");
    std::fs::write(&failing, FAILING).unwrap();
    let (code, stdout, _) = cli(&["scenario", "run", failing.to_str().unwrap()]);
    assert_eq!(code, 1, "{stdout}");
    assert!(
        stdout.contains("FAILED") && stdout.contains("expected 2 matching trades, found 1"),
        "{stdout}"
    );

    let (code, stdout, _) = cli(&[
        "scenario",
        "run",
        "--json",
        files[0].as_str(),
        failing.to_str().unwrap(),
    ]);
    assert_eq!(code, 1);
    let reports: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        (
            reports[0]["passed"].as_bool(),
            reports[1]["passed"].as_bool()
        ),
        (Some(true), Some(false))
    );

    let broken = dir.join("broken.yaml");
    std::fs::write(&broken, "name: x\nsteps:\n  - teleport: {}\n").unwrap();
    let (code, _, stderr) = cli(&["scenario", "run", broken.to_str().unwrap()]);
    assert_eq!(code, 2);
    assert!(stderr.contains("unknown variant `teleport`"), "{stderr}");
    let (code, _, _) = cli(&[
        "scenario",
        "run",
        dir.join("missing.yaml").to_str().unwrap(),
    ]);
    assert_eq!(code, 2);
    let (code, _, stderr) = cli(&["frobnicate"]);
    assert_eq!(code, 2);
    assert!(stderr.contains("usage"));
    let _ = std::fs::remove_dir_all(dir);
}

async fn post_scenario(app: &TestApp, body: &str) -> (StatusCode, Value) {
    let request = Request::post("/test/scenarios")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.router().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn the_api_runs_scenarios_in_a_sandbox() {
    let mut app = TestApp::start().await;
    let passing = std::fs::read_to_string(&examples()[0]).unwrap();
    let (status, report) = post_scenario(&app, &passing).await;
    assert_eq!(
        (status, &report["passed"]),
        (StatusCode::OK, &Value::Bool(true)),
        "{report}"
    );
    let cli_like = scenario::run(&Scenario::parse(&passing).unwrap(), &app.config)
        .await
        .unwrap();
    assert_eq!(
        report,
        serde_json::to_value(&cli_like).unwrap(),
        "the API and the CLI share one runner"
    );

    let (status, report) = post_scenario(&app, FAILING).await;
    assert_eq!(
        (status, &report["passed"]),
        (StatusCode::OK, &Value::Bool(false))
    );
    let (status, error) = post_scenario(&app, "name: x\nsteps:\n  - teleport: {}\n").await;
    assert_eq!(
        (status, &error["error"]),
        (StatusCode::BAD_REQUEST, &Value::from("invalid_scenario"))
    );
    assert!(error["message"].as_str().unwrap().contains("teleport"));

    assert_eq!(
        app.get("/api/trades").await.1,
        Value::Array(vec![]),
        "the live service is untouched"
    );
    assert_eq!(app.get("/api/instruments").await.1, Value::Array(vec![]));
    app.stop().await;

    let mut closed = TestApp::start_with(|c| c.test_control.enabled = false).await;
    assert_eq!(
        post_scenario(&closed, &passing).await.0,
        StatusCode::NOT_FOUND
    );
    closed.stop().await;
}

/// A webhook consumer that accepts everything on `/ok` and fails everything on `/down`, and records what `/ok` got.
async fn consumer() -> (String, Arc<Mutex<Vec<Value>>>) {
    let received = Arc::new(Mutex::new(Vec::new()));
    let app = axum::Router::new()
        .route(
            "/ok",
            post(
                |State(seen): State<Arc<Mutex<Vec<Value>>>>, Json(body): Json<Value>| async move {
                    seen.lock().unwrap().push(body);
                    StatusCode::OK
                },
            ),
        )
        .route(
            "/down",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .with_state(received.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, received)
}

#[tokio::test]
async fn deliveries_are_expected_on_real_time() {
    let (base, received) = consumer().await;
    // Test webhook timings: five attempts with backoff of at most 100 ms, so `/down` is dead-lettered quickly.
    let config = base_config().await;
    let text = format!(
        r#"
name: deliveries
seed: 5
instruments: [ {{ id: ACME, symbol: ACME, currency: DKK, currencyScale: 2 }} ]
participants: [ {{ id: A, name: A }}, {{ id: B, name: B }} ]
webhooks:
  - {{ url: "{base}/ok", eventTypes: [TradeExecuted] }}
  - {{ url: "{base}/down", eventTypes: [TradeExecuted] }}
steps:
  - order: {{ participantId: A, side: SELL, type: LIMIT, priceTicks: 10025, quantity: 10 }}
  - order: {{ participantId: B, side: BUY, type: LIMIT, priceTicks: 10025, quantity: 10 }}
  - expect: {{ deliveries: {{ eventType: TradeExecuted }}, count: 2, wait_ms: 0 }}
  - expect: {{ deliveries: {{ url: "{base}/ok", eventType: TradeExecuted, status: DELIVERED, attempts: 1 }},
              count: 1 }}
  - expect: {{ deliveries: {{ url: "{base}/down", status: DEAD, attempts: 5 }}, count: 1 }}
  - expect: {{ deliveries: {{ url: "{base}/ok", status: DEAD }}, count: 1, wait_ms: 100 }}
"#
    );
    let scenario = Scenario::parse(&text).unwrap();
    let report = scenario::run(&scenario, &config).await.unwrap();
    let failed: Vec<(usize, &Vec<String>)> = report
        .steps
        .iter()
        .filter(|s| !s.passed)
        .map(|s| (s.index, &s.failures))
        .collect();
    assert_eq!(
        failed,
        [(
            6,
            &vec!["expected 1 matching deliveries, found 0 after waiting 100 ms".to_string()]
        )],
        "{}",
        report.render()
    );
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 1);
    let sequence = received[0]["sequence"].as_u64().unwrap();
    assert_eq!(
        (&received[0]["eventType"], &received[0]["eventId"]),
        (
            &Value::from("TradeExecuted"),
            &Value::from(
                IdScheme::Deterministic { seed: 5 }
                    .event(sequence)
                    .to_string()
            )
        ),
        "the consumer got the event with its deterministic ID"
    );
}

fn random_market() -> String {
    std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios/random-market.yaml"),
    )
    .unwrap()
}

#[tokio::test]
async fn a_random_market_is_reproducible_from_its_seed() {
    let config = base_config().await;
    let text = random_market();
    let first = scenario::run(&Scenario::parse(&text).unwrap(), &config)
        .await
        .unwrap();
    let second = scenario::run(&Scenario::parse(&text).unwrap(), &config)
        .await
        .unwrap();
    assert!(first.passed, "{}", first.render());
    assert_eq!(first, second, "same report, same byte-identical event log");
    assert!(first.event_log.events > 1000, "{:?}", first.event_log);

    let other = scenario::run(
        &Scenario::parse(&text.replace("seed: 12345", "seed: 54321")).unwrap(),
        &config,
    )
    .await
    .unwrap();
    assert!(other.passed, "{}", other.render());
    assert_ne!(other.event_log, first.event_log);
    assert_ne!(other.steps[1].step, first.steps[1].step);

    // Arrivals move the virtual clock: 600 commands at 5 per second end about two minutes after the 09:00 start.
    let summary = &first.steps[1].step;
    let until: chrono::DateTime<chrono::Utc> =
        summary.rsplit("until ").next().unwrap().parse().unwrap();
    let elapsed = (until
        - "2030-01-07T09:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap())
    .num_seconds();
    assert!((100..140).contains(&elapsed), "{summary}");

    // The market's summary agrees with the trade log: every execution it saw is one trade.
    let executions: usize = summary
        .split(", ")
        .find_map(|part| part.strip_suffix(" executions"))
        .unwrap_or_else(|| panic!("{summary}"))
        .parse()
        .unwrap();
    let checked = format!(
        "{}\n  - expect: {{ trades: {{}}, count: {executions} }}\n  - expect: {{ events: {{ eventType: TradeExecuted }}, count: {executions} }}\n",
        text.trim_end()
    );
    let report = scenario::run(&Scenario::parse(&checked).unwrap(), &config)
        .await
        .unwrap();
    assert!(report.passed, "{}", report.render());
}

#[tokio::test]
async fn a_broken_market_reports_a_bounded_number_of_failures() {
    let text = r#"
name: unknown instrument
steps:
  - market: { orders: 50, instruments: [ { id: NOPE, price: { mid_ticks: 100 } } ] }
"#;
    let report = scenario::run(&Scenario::parse(text).unwrap(), &base_config().await)
        .await
        .unwrap();
    let failures = &report.steps[1].failures;
    assert!(!report.passed);
    assert_eq!(failures.len(), 11, "{}", report.render());
    assert!(
        failures[0].contains("HTTP 422") && failures[0].contains("unknown_instrument"),
        "{}",
        failures[0]
    );
    assert_eq!(failures[10], "… further failures not shown");
}

#[tokio::test]
async fn the_event_log_fingerprint_covers_every_event() {
    let config = base_config().await;
    let at = |price: u32| {
        format!(
            "name: fingerprint\ninstruments: [ {{ id: ACME, symbol: ACME, currency: DKK, currencyScale: 2 }} ]\n\
             participants: [ {{ id: A, name: A }} ]\nsteps:\n\
             - order: {{ participantId: A, side: SELL, type: LIMIT, priceTicks: 100, quantity: 1 }}\n\
             - order: {{ participantId: A, side: SELL, type: LIMIT, priceTicks: {price}, quantity: 1 }}\n"
        )
    };
    let run = |text: String| {
        let config = config.clone();
        async move {
            scenario::run(&Scenario::parse(&text).unwrap(), &config)
                .await
                .unwrap()
                .event_log
        }
    };
    let (a, b) = (run(at(101)).await, run(at(102)).await);
    assert_eq!(a.events, b.events);
    assert_ne!(a.sha256, b.sha256, "only the last event differs");
    assert_eq!(a, run(at(101)).await);
}

#[tokio::test]
async fn all_needs_every_item_and_at_least_one() {
    let text = r#"
name: all
instruments: [ { id: ACME, symbol: ACME, currency: DKK, currencyScale: 2 } ]
participants: [ { id: A, name: A }, { id: B, name: B } ]
steps:
  - expect: { deliveries: {}, all: true, wait_ms: 50 }
  - order: { participantId: A, side: SELL, type: LIMIT, priceTicks: 100, quantity: 5 }
  - order: { participantId: B, side: BUY, type: LIMIT, priceTicks: 99, quantity: 5 }
  - expect: { events: { eventType: OrderAccepted }, all: true }
  - expect: { events: { payload: { side: SELL } }, all: true }
"#;
    let report = scenario::run(&Scenario::parse(text).unwrap(), &base_config().await)
        .await
        .unwrap();
    let failures: Vec<(usize, Vec<String>)> = report
        .steps
        .iter()
        .filter(|s| !s.passed)
        .map(|s| (s.index, s.failures.clone()))
        .collect();
    assert_eq!(
        failures,
        [
            (
                1,
                vec!["expected all deliveries to match, 0 of 0 do after waiting 50 ms".to_string()]
            ),
            (
                5,
                vec!["expected all events to match, 1 of 2 do".to_string()]
            )
        ],
        "{}",
        report.render()
    );
}
