//! The settlement simulator wired into the service: intake from the trade log, lifecycle transitions on the virtual
//! clock, persistence across restarts, webhooks, and modes.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, SettlementMode};
use serde_json::{Value, json};

/// Monday 2030-01-07 09:00 UTC.
const START: &str = "2030-01-07T09:00:00Z";

fn simulated(adjust: impl FnOnce(&mut Config)) -> impl FnOnce(&mut Config) {
    move |c: &mut Config| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some(START.parse().unwrap());
        c.settlement.mode = SettlementMode::Simulated;
        c.settlement.tick_interval_ms = 20;
        c.settlement.fail_probability = 0.0;
        c.settlement.delay_probability = 0.0;
        adjust(c);
    }
}

async fn trade(app: &TestApp) -> Value {
    assert_eq!(
        app.order("A", "SELL", Some(10025), 200).await.0,
        StatusCode::CREATED
    );
    let (_, buy) = app.order("B", "BUY", Some(10025), 150).await;
    buy["executions"][0].clone()
}

async fn events(app: &TestApp, event_type: &str) -> Vec<Value> {
    app.get(&format!("/api/events?eventType={event_type}&limit=1000"))
        .await
        .1
        .as_array()
        .unwrap()
        .clone()
}

async fn wait_for(app: &TestApp, event_type: &str, count: usize) -> Vec<Value> {
    for _ in 0..300 {
        let found = events(app, event_type).await;
        if found.len() >= count {
            return found;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {count} {event_type}");
}

async fn advance(app: &TestApp, days: u32) {
    let (status, body) = app
        .post("/test/clock/advance", json!({ "days": days }))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn a_trade_settles_on_t_plus_2_when_the_clock_gets_there() {
    let mut app = TestApp::start_with(simulated(|_| {})).await;
    app.seed().await;
    let execution = trade(&app).await;
    let execution_id = execution["executionId"].as_str().unwrap();

    let pending = wait_for(&app, "SettlementPending", 1).await;
    let event = &pending[0];
    assert_eq!(
        (&event["source"], &event["aggregateId"]),
        (&json!("settlement-simulator"), &json!(execution_id))
    );
    assert_eq!(event["correlationId"], execution_id);
    assert_eq!(
        event["occurredAt"], execution["executedAt"],
        "intake is stamped with the trade time"
    );
    assert_eq!(event["payload"]["intendedSettlementDate"], "2030-01-09");
    assert_eq!(event["payload"]["settlementAmount"], "15037.50");

    advance(&app, 1).await;
    assert!(
        events(&app, "SettlementCompleted").await.is_empty(),
        "T+1 is too early"
    );
    advance(&app, 1).await;
    // No waiting: the clock change ran the settlement step before it returned.
    let completed = events(&app, "SettlementCompleted").await;
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0]["occurredAt"], "2030-01-09T00:00:00Z");
    assert_eq!(completed[0]["payload"]["status"], "SETTLED");
    let status: String = app
        .db()
        .query_row(
            "SELECT status FROM settlement_instructions WHERE execution_id = ?1",
            [execution_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "SETTLED");
    app.stop().await;
}

#[tokio::test]
async fn a_restart_in_the_middle_neither_repeats_nor_loses_anything() {
    let mut app = TestApp::start_with(simulated(|c| c.settlement.delay_probability = 1.0)).await;
    app.seed().await;
    trade(&app).await;
    trade(&app).await;
    let trades = events(&app, "TradeExecuted").await.len();
    assert_eq!(
        trades, 3,
        "the second buy also takes the first sell's remainder"
    );
    wait_for(&app, "SettlementPending", trades).await;
    advance(&app, 2).await;
    assert_eq!(events(&app, "SettlementDelayed").await.len(), trades);

    app.restart().await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        events(&app, "SettlementPending").await.len(),
        trades,
        "trades are not taken in twice"
    );
    advance(&app, 14).await;
    let all = app.get("/api/events?limit=1000").await.1;
    let lifecycle: Vec<_> = all
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["source"] == "settlement-simulator")
        .map(|e| e["eventType"].as_str().unwrap().to_string())
        .collect();
    let count = |t: &str| lifecycle.iter().filter(|x| *x == t).count();
    assert_eq!(
        (count("SettlementPending"), count("SettlementCompleted")),
        (trades, trades)
    );
    assert_eq!(
        count("SettlementDelayed"),
        2 * trades,
        "two delays each (max_delays), then settled"
    );
    let open: i64 = app
        .db()
        .query_row(
            "SELECT COUNT(*) FROM settlement_instructions WHERE due_at IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(open, 0);
    app.stop().await;
}

#[tokio::test]
async fn external_mode_simulates_nothing() {
    let mut app = TestApp::start_with(|c| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some(START.parse().unwrap());
    })
    .await;
    app.seed().await;
    trade(&app).await;
    advance(&app, 10).await;
    let all = app.get("/api/events?limit=1000").await.1;
    assert!(
        all.as_array()
            .unwrap()
            .iter()
            .all(|e| e["source"] == "exchange")
    );
    let rows: i64 = app
        .db()
        .query_row("SELECT COUNT(*) FROM settlement_instructions", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(rows, 0);
    app.stop().await;
}

#[tokio::test]
async fn settlement_events_reach_webhook_subscribers() {
    let mut app = TestApp::start_with(simulated(|_| {})).await;
    app.seed().await;
    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
    let sink = received.clone();
    let consumer = axum::Router::new().route(
        "/hook",
        axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            let sink = sink.clone();
            async move {
                sink.lock().unwrap().push(body);
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, consumer).await.unwrap() });
    let (status, _) = app
        .post(
            "/api/webhooks",
            json!({ "url": url, "eventTypes": ["SettlementPending", "SettlementCompleted"] }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    trade(&app).await;
    wait_for(&app, "SettlementPending", 1).await;
    advance(&app, 2).await;
    for _ in 0..300 {
        if received.lock().unwrap().len() == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let received = received.lock().unwrap().clone();
    let types: Vec<_> = received
        .iter()
        .map(|e| e["eventType"].as_str().unwrap())
        .collect();
    assert_eq!(types.len(), 2, "{types:?}");
    assert!(types.contains(&"SettlementPending") && types.contains(&"SettlementCompleted"));
    assert!(
        received
            .iter()
            .all(|e| e["source"] == "settlement-simulator")
    );
    app.stop().await;
}

/// The settlement lifecycle of a run, without IDs (which are random).
async fn lifecycle(app: &TestApp) -> Vec<(String, u64, String, String, String)> {
    let all = app.get("/api/events?limit=1000").await.1;
    all.as_array()
        .unwrap()
        .iter()
        .filter(|e| e["source"] == "settlement-simulator")
        .map(|e| {
            let p = &e["payload"];
            (
                e["eventType"].as_str().unwrap().to_string(),
                p["tradeSequence"].as_u64().unwrap(),
                p["intendedSettlementDate"].as_str().unwrap().to_string(),
                e["occurredAt"].as_str().unwrap().to_string(),
                p["settlementAmount"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[tokio::test]
async fn the_same_seed_and_clock_moves_give_the_same_lifecycle() {
    let run = || async {
        let mut app = TestApp::start_with(simulated(|c| {
            c.settlement.seed = 2024;
            c.settlement.reject_probability = 0.1;
            c.settlement.fail_probability = 0.15;
            c.settlement.delay_probability = 0.3;
            c.settlement.correct_probability = 0.2;
        }))
        .await;
        app.seed().await;
        for i in 0..20 {
            let price = 10_000 + i;
            assert_eq!(
                app.order("A", "SELL", Some(price), 10).await.0,
                StatusCode::CREATED
            );
            assert_eq!(
                app.order(["B", "C", "D"][i as usize % 3], "BUY", Some(price), 10)
                    .await
                    .0,
                StatusCode::CREATED
            );
            if i % 5 == 4 {
                advance(&app, 1).await;
            }
        }
        advance(&app, 20).await;
        let result = lifecycle(&app).await;
        app.stop().await;
        result
    };
    let first = run().await;
    let second = run().await;
    assert_eq!(first, second);
    let kinds: std::collections::HashSet<_> = first.iter().map(|e| e.0.as_str()).collect();
    assert!(
        kinds.len() >= 4,
        "the run exercises several outcomes: {kinds:?}"
    );
}

#[tokio::test]
async fn an_invalid_settlement_policy_stops_startup() {
    let dir =
        std::env::temp_dir().join(format!("exchange-sim-bad-policy-{}", uuid::Uuid::now_v7()));
    let mut config = TestApp::start().await.config.clone();
    config.database.path = dir.join("exchange.db").to_string_lossy().into_owned();
    config.settlement.mode = SettlementMode::Both;
    config.settlement.fail_probability = 0.9;
    config.settlement.delay_probability = 0.9;
    let error = exchange_simulator::app::App::start(&config)
        .err()
        .expect("refused");
    assert!(
        format!("{error:#}").contains("must not exceed 1"),
        "{error:#}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

async fn set_clock(app: &TestApp, to: &str) {
    let (status, body) = app.post("/test/clock/set", json!({ "to": to })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn settlement_happens_exactly_when_the_clock_reaches_the_date_and_skips_weekends() {
    // Thursday 2030-01-10: T+2 is Monday 2030-01-14.
    let mut app = TestApp::start_with(simulated(|c| {
        c.clock.start = Some("2030-01-10T15:00:00Z".parse().unwrap())
    }))
    .await;
    app.seed().await;
    trade(&app).await;
    let pending = wait_for(&app, "SettlementPending", 1).await;
    assert_eq!(
        pending[0]["payload"]["intendedSettlementDate"],
        "2030-01-14"
    );

    set_clock(&app, "2030-01-12T12:00:00Z").await; // Saturday
    set_clock(&app, "2030-01-13T23:59:59Z").await; // one second before Monday
    assert!(
        events(&app, "SettlementCompleted").await.is_empty(),
        "nothing settles over the weekend"
    );
    set_clock(&app, "2030-01-14T00:00:00Z").await;
    let completed = events(&app, "SettlementCompleted").await;
    assert_eq!(
        completed.len(),
        1,
        "exactly at 00:00 UTC on the settlement date"
    );
    assert_eq!(completed[0]["occurredAt"], "2030-01-14T00:00:00Z");
    app.stop().await;
}

#[tokio::test]
async fn a_settled_trade_is_corrected_on_the_configured_day() {
    let mut app = TestApp::start_with(simulated(|c| {
        c.settlement.correct_probability = 1.0;
        c.settlement.correction_days = 2;
    }))
    .await;
    app.seed().await;
    trade(&app).await;
    wait_for(&app, "SettlementPending", 1).await;
    advance(&app, 2).await; // Wednesday: settled
    assert_eq!(events(&app, "SettlementCompleted").await.len(), 1);
    advance(&app, 1).await; // Thursday: not yet
    assert!(events(&app, "SettlementCorrected").await.is_empty());
    advance(&app, 1).await; // Friday 2030-01-11: ISD + 2 business days
    let corrected = events(&app, "SettlementCorrected").await;
    assert_eq!(corrected.len(), 1);
    assert_eq!(corrected[0]["occurredAt"], "2030-01-11T00:00:00Z");
    let payload = &corrected[0]["payload"];
    assert_eq!(payload["previousSettlementAmount"], "15037.50");
    assert!(
        payload["settlementAmount"] == "15036.00" || payload["settlementAmount"] == "15039.00",
        "{payload}"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_restart_in_the_middle_does_not_change_the_outcome() {
    let policy = |c: &mut Config| {
        c.settlement.seed = 77;
        c.settlement.fail_probability = 0.2;
        c.settlement.delay_probability = 0.4;
        c.settlement.correct_probability = 0.3;
    };
    let script = |restart: bool| async move {
        let mut app = TestApp::start_with(simulated(policy)).await;
        app.seed().await;
        for i in 0..8 {
            app.order("A", "SELL", Some(10_000 + i), 10).await;
            app.order("B", "BUY", Some(10_000 + i), 10).await;
        }
        wait_for(&app, "SettlementPending", 8).await;
        advance(&app, 2).await;
        if restart {
            app.restart().await;
        }
        advance(&app, 3).await;
        if restart {
            app.restart().await;
        }
        advance(&app, 10).await;
        let result = lifecycle(&app).await;
        app.stop().await;
        result
    };
    let straight = script(false).await;
    let restarted = script(true).await;
    assert_eq!(straight, restarted);
    assert!(
        straight.len() > 16,
        "the script goes past pending and settled: {}",
        straight.len()
    );
}

#[tokio::test]
async fn an_external_consumer_and_the_simulator_act_on_the_same_trade() {
    let mut app =
        TestApp::start_with(simulated(|c| c.settlement.mode = SettlementMode::Both)).await;
    app.seed().await;
    // Serve the exchange on a real port, so the consumer can call back the way an external service would.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let exchange_url = format!("http://{}", listener.local_addr().unwrap());
    let router = app.router();
    // The extra server holds a router (and so a handle to the exchange thread); it must stop before the app can.
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    // The consumer settles every trade it hears about and reports SETTLED back with its own reference.
    let reported = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = reported.clone();
    let base = exchange_url.clone();
    let consumer = axum::Router::new().route(
        "/trades",
        axum::routing::post(move |axum::Json(event): axum::Json<Value>| {
            let (base, counter) = (base.clone(), counter.clone());
            async move {
                let execution = event["payload"]["executionId"]
                    .as_str()
                    .unwrap()
                    .to_string();
                let response = reqwest::Client::new()
                    .post(format!("{base}/api/settlements/{execution}/outcomes"))
                    .json(
                        &json!({ "status": "SETTLED", "reference": format!("ext-{execution}"),
                                   "details": { "via": "consumer" } }),
                    )
                    .send()
                    .await
                    .unwrap();
                assert!(response.status().is_success());
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let consumer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let consumer_url = format!("http://{}/trades", consumer_listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(consumer_listener, consumer).await.unwrap() });
    let (status, _) = app
        .post(
            "/api/webhooks",
            json!({ "url": consumer_url, "eventTypes": ["TradeExecuted"] }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let execution = trade(&app).await;
    let id = execution["executionId"].as_str().unwrap();
    for _ in 0..300 {
        if reported.load(std::sync::atomic::Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        reported.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the consumer reported back"
    );
    wait_for(&app, "SettlementPending", 1).await;
    advance(&app, 2).await;

    let detail = app.get(&format!("/api/settlements/{id}")).await.1;
    assert_eq!(detail["simulated"]["status"], "SETTLED");
    assert_eq!(detail["external"][0]["status"], "SETTLED");
    assert_eq!(detail["external"][0]["reference"], format!("ext-{id}"));
    let sources: std::collections::BTreeSet<_> = app
        .get("/api/events?limit=1000")
        .await
        .1
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["aggregateId"] == id || e["correlationId"] == id)
        .map(|e| {
            format!(
                "{}:{}",
                e["source"].as_str().unwrap(),
                e["eventType"].as_str().unwrap()
            )
        })
        .collect();
    assert_eq!(
        sources.into_iter().collect::<Vec<_>>(),
        [
            "exchange:TradeExecuted",
            "external:SettlementOutcomeReported",
            "settlement-simulator:SettlementCompleted",
            "settlement-simulator:SettlementPending",
        ],
        "each source speaks for itself"
    );
    server.abort();
    let _ = server.await;
    app.stop().await;
}
