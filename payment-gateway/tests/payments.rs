//! Payments through the service: initiation, idempotency, validation, outcomes on the domain clock, returns,
//! webhooks, restarts and reproducibility.

mod common;

use std::sync::{Arc, Mutex};

use axum::http::{HeaderMap, Method, StatusCode};
use common::{TestApp, payment};
use payment_gateway::config::IdMode;
use serde_json::{Value, json};

#[tokio::test]
async fn a_deposit_is_pending_until_its_delay_passes_then_completes() {
    let mut app = TestApp::start().await;
    let created = app.pay("dep-1", "DEPOSIT", "1500.00", "dkk").await;
    assert_eq!(created["status"], "PENDING");
    assert_eq!(created["currency"], "DKK");
    assert_eq!(created["amountMinor"], 150_000);
    assert_eq!(
        created["bankAccount"]["iban"], "DE89370400440532013000",
        "stored in compact form"
    );
    assert_eq!(created["createdAt"], common::START);
    let id = created["paymentId"].as_str().unwrap().to_string();
    let pending = app.events("PaymentPending").await;
    assert_eq!(pending.len(), 1);
    assert_eq!(
        (
            &pending[0]["aggregateId"],
            &pending[0]["correlationId"],
            &pending[0]["source"]
        ),
        (&json!(id), &json!("acc-1"), &json!("payment-gateway"))
    );

    // Deposits take 60 to 1800 seconds by default.
    app.advance(json!({ "seconds": 59 })).await;
    assert!(app.events("PaymentCompleted").await.is_empty());
    app.advance(json!({ "minutes": 30 })).await;
    let completed = app.events("PaymentCompleted").await;
    assert_eq!(completed.len(), 1);
    let (_, now) = app.get(&format!("/api/payments/{id}")).await;
    assert_eq!(now["status"], "COMPLETED");
    assert_eq!(
        completed[0]["occurredAt"], now["completedAt"],
        "the scheduled instant"
    );
    assert_eq!(completed[0]["payload"]["status"], "COMPLETED");
    assert_eq!(
        app.get("/api/payments/00000000-0000-0000-0000-000000000000")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    app.stop().await;
}

#[tokio::test]
async fn an_idempotency_key_returns_the_same_payment_or_refuses_different_details() {
    let mut app = TestApp::start().await;
    let first = app.pay("k-1", "WITHDRAWAL", "250.00", "EUR").await;
    let (status, again) = app
        .post(
            "/api/payments",
            payment("k-1", "WITHDRAWAL", "250.00", "EUR"),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        (&again["paymentId"], &again["duplicate"]),
        (&first["paymentId"], &json!(true))
    );
    let (status, conflict) = app
        .post(
            "/api/payments",
            payment("k-1", "WITHDRAWAL", "251.00", "EUR"),
        )
        .await;
    assert_eq!(
        (status, &conflict["error"]),
        (StatusCode::CONFLICT, &json!("idempotency_conflict"))
    );
    assert_eq!(app.events("PaymentPending").await.len(), 1, "one payment");
    app.stop().await;
}

#[tokio::test]
async fn invalid_payments_are_refused() {
    let mut app = TestApp::start().await;
    let base = payment("v-1", "DEPOSIT", "10.00", "DKK");
    let with = |field: &str, value: Value| {
        let mut body = base.clone();
        let mut target = &mut body;
        let path: Vec<&str> = field.split('.').collect();
        for key in &path[..path.len() - 1] {
            target = &mut target[*key];
        }
        target[path[path.len() - 1]] = value;
        body
    };
    for (body, expected) in [
        (
            with("bankAccount.iban", json!("DE89370400440532013001")),
            "check digits",
        ),
        (with("currency", json!("XXX")), "not accepted"),
        (with("amount", json!("10.001")), "decimals"),
        (with("amount", json!("0.00")), "positive"),
        (with("amount", json!("-5.00")), "positive"),
        (with("idempotencyKey", json!("")), "idempotencyKey"),
        (with("accountId", json!("x".repeat(65))), "accountId"),
        (with("bankAccount.holderName", json!("  ")), "holderName"),
    ] {
        let (status, error) = app.post("/api/payments", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            error["message"].as_str().unwrap().contains(expected),
            "{error}"
        );
    }
    assert_eq!(
        app.post("/api/payments", with("type", json!("REFUND")))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        app.post("/api/payments", json!({ "idempotencyKey": "x" }))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert!(app.all_events().await.is_empty());
    app.stop().await;
}

#[tokio::test]
async fn a_failed_payment_has_a_reason_and_a_returned_one_is_reversed_business_days_later() {
    let mut app = TestApp::start_with(|c| {
        c.payments.deposit.fail_probability = 1.0;
        c.payments.withdrawal.fail_probability = 0.0;
        c.payments.return_probability = 1.0;
        c.payments.max_return_days = 1;
    })
    .await;
    let deposit = app.pay("f-1", "DEPOSIT", "100.00", "DKK").await;
    let withdrawal = app.pay("r-1", "WITHDRAWAL", "40.00", "DKK").await;
    app.advance(json!({ "hours": 4 })).await;
    let (_, failed) = app
        .get(&format!(
            "/api/payments/{}",
            deposit["paymentId"].as_str().unwrap()
        ))
        .await;
    assert_eq!(failed["status"], "FAILED");
    assert!(
        ["ACCOUNT_CLOSED", "INSUFFICIENT_FUNDS", "REFUSED_BY_BANK"]
            .contains(&failed["reason"].as_str().unwrap()),
        "{failed}"
    );
    let id = withdrawal["paymentId"].as_str().unwrap().to_string();
    let (_, completed) = app.get(&format!("/api/payments/{id}")).await;
    assert_eq!(completed["status"], "COMPLETED");
    // One business day later, at the same time: Tuesday.
    app.advance(json!({ "days": 1 })).await;
    let (_, returned) = app.get(&format!("/api/payments/{id}")).await;
    assert_eq!(returned["status"], "RETURNED");
    assert_eq!(returned["reason"], "RETURNED_BY_BANK");
    let completed_at: chrono::DateTime<chrono::Utc> =
        completed["completedAt"].as_str().unwrap().parse().unwrap();
    let returned_at: chrono::DateTime<chrono::Utc> =
        returned["returnedAt"].as_str().unwrap().parse().unwrap();
    assert_eq!(returned_at - completed_at, chrono::Duration::days(1));
    let types: Vec<String> = app
        .all_events()
        .await
        .iter()
        .filter(|e| e["aggregateId"] == json!(id))
        .map(|e| e["eventType"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        types,
        ["PaymentPending", "PaymentCompleted", "PaymentReturned"]
    );
    let (_, listed) = app.get("/api/payments?status=RETURNED").await;
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(
        app.get("/api/payments?status=LOST").await.0,
        StatusCode::BAD_REQUEST
    );
    app.stop().await;
}

#[tokio::test]
async fn a_pending_payment_survives_a_restart_and_completes_once() {
    let mut app = TestApp::start().await;
    let created = app.pay("s-1", "DEPOSIT", "10.00", "DKK").await;
    app.restart().await;
    let (_, after) = app
        .get(&format!(
            "/api/payments/{}",
            created["paymentId"].as_str().unwrap()
        ))
        .await;
    assert_eq!(after["status"], "PENDING");
    app.advance(json!({ "hours": 1 })).await;
    app.restart().await;
    app.advance(json!({ "hours": 1 })).await;
    assert_eq!(app.events("PaymentCompleted").await.len(), 1);
    app.stop().await;
}

#[tokio::test]
async fn deterministic_ids_give_the_same_event_log_run_after_run() {
    let run = || async {
        let mut app = TestApp::start_with(|c| {
            c.ids.mode = IdMode::Deterministic;
            c.ids.seed = 7;
            c.payments.deposit.fail_probability = 0.3;
            c.payments.return_probability = 0.3;
        })
        .await;
        for i in 0..20 {
            let kind = if i % 3 == 0 { "WITHDRAWAL" } else { "DEPOSIT" };
            app.pay(&format!("d-{i}"), kind, &format!("{}.00", 10 + i), "DKK")
                .await;
        }
        app.advance(json!({ "days": 7 })).await;
        let log = app.all_events().await;
        app.stop().await;
        log
    };
    let first = run().await;
    assert!(first.len() > 40, "{}", first.len());
    assert_eq!(first, run().await);
    let stamps: Vec<&str> = first
        .iter()
        .map(|e| e["occurredAt"].as_str().unwrap())
        .collect();
    assert!(stamps.is_sorted(), "time order across a week: {stamps:#?}");
}

/// Each request's headers, raw body (what the signature covers) and parsed event.
type Received = Arc<Mutex<Vec<(HeaderMap, Vec<u8>, Value)>>>;

async fn receiver() -> (String, Received) {
    let received: Received = Arc::default();
    let sink = received.clone();
    let router = axum::Router::new().route(
        "/hook",
        axum::routing::post(move |headers: HeaderMap, body: axum::body::Bytes| {
            let sink = sink.clone();
            async move {
                let event = serde_json::from_slice(&body).unwrap();
                sink.lock().unwrap().push((headers, body.to_vec(), event));
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, received)
}

#[tokio::test]
async fn payment_events_reach_subscribers_signed_and_maintenance_works() {
    let (url, received) = receiver().await;
    let mut app = TestApp::start().await;
    let secret = "a-secret-of-at-least-16";
    let (status, subscription) = app
        .post(
            "/api/webhooks",
            json!({ "url": url, "eventTypes": ["PaymentPending", "PaymentCompleted"], "secret": secret, "ordered": true }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{subscription}");
    let (status, refused) = app
        .post(
            "/api/webhooks",
            json!({ "url": url, "eventTypes": ["TradeExecuted"] }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an exchange event type: {refused}"
    );
    app.pay("w-1", "DEPOSIT", "75.50", "EUR").await;
    app.advance(json!({ "hours": 1 })).await;
    for _ in 0..200 {
        if received.lock().unwrap().len() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let got = received.lock().unwrap().clone();
    let types: Vec<&str> = got
        .iter()
        .map(|(_, _, e)| e["eventType"].as_str().unwrap())
        .collect();
    assert_eq!(types, ["PaymentPending", "PaymentCompleted"]);
    for (headers, body, _) in &got {
        let timestamp: i64 = headers[simulator_kit::webhooks::signing::TIMESTAMP_HEADER]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let expected = simulator_kit::webhooks::signing::sign(secret, timestamp, body);
        assert_eq!(
            headers[simulator_kit::webhooks::signing::SIGNATURE_HEADER]
                .to_str()
                .unwrap(),
            expected
        );
    }
    let id = subscription["id"].as_str().unwrap();
    let (_, detail) = app.get(&format!("/api/webhooks/{id}")).await;
    assert_eq!(detail["deliveries"]["delivered"], 2);
    let (status, _) = app
        .call(Method::DELETE, &format!("/api/webhooks/{id}"), None)
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, deliveries) = app.get("/test/deliveries").await;
    assert_eq!(deliveries.as_array().unwrap().len(), 2);
    app.stop().await;
}

#[tokio::test]
async fn the_clock_reset_and_health_behave_as_in_the_exchange() {
    let mut app = TestApp::start_with(|c| {
        c.clock.running = true;
        c.clock.speed = 3600.0;
    })
    .await;
    let (_, health) = app.get("/health").await;
    assert_eq!(
        health,
        json!({ "status": "ok", "schemaVersion": 5, "persistence": "durable" })
    );
    let (_, clock) = app.get("/test/clock").await;
    assert_eq!(
        (&clock["mode"], &clock["speed"]),
        (&json!("virtual"), &json!(3600.0))
    );
    app.pay("c-1", "DEPOSIT", "1.00", "DKK").await;
    assert_eq!(
        app.post("/test/clock/set", json!({ "to": "2029-01-01T00:00:00Z" }))
            .await
            .1["error"],
        "clock_backwards"
    );
    assert_eq!(
        app.call(Method::POST, "/test/reset", None).await.0,
        StatusCode::NO_CONTENT
    );
    assert!(app.all_events().await.is_empty());
    assert_eq!(app.get("/api/payments").await.1, json!([]));
    let (_, metrics) = app.get("/metrics").await;
    assert!(
        metrics.as_str().unwrap().contains("gateway_payments_total"),
        "{metrics}"
    );
    app.stop().await;

    let mut config = app.config.clone();
    config.clock.mode = payment_gateway::config::ClockMode::System;
    config.clock.speed = 1.0;
    let system = payment_gateway::app::App::start(&config).await.unwrap();
    let router = system.router.clone();
    let response = tower::ServiceExt::oneshot(
        router,
        axum::http::Request::post("/test/clock/advance")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(r#"{"days":1}"#))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    system.shutdown().await;
}

#[tokio::test]
async fn invalid_configuration_stops_startup() {
    let base = TestApp::start().await.config.clone();
    for (adjust, message) in [
        (
            Box::new(|c: &mut payment_gateway::config::Config| {
                c.payments.deposit.fail_probability = 2.0
            }) as Box<dyn Fn(&mut payment_gateway::config::Config)>,
            "fail_probability",
        ),
        (
            Box::new(|c| c.statement.time_zone = "Nowhere/Land".into()),
            "statement.time_zone",
        ),
        (
            Box::new(|c| c.statement.account_iban = "DK00".into()),
            "account_iban",
        ),
        (Box::new(|c| c.clock.speed = 0.0), "clock.speed"),
        (
            Box::new(|c| c.calendar.mode = payment_gateway::config::CalendarMode::Follow),
            "calendar.mode: follow needs calendar.follow.url or clock.follow.url",
        ),
    ] {
        let mut config = base.clone();
        config.database.path = std::env::temp_dir()
            .join(format!("payment-gateway-bad-{}", uuid::Uuid::now_v7()))
            .join("g.db")
            .to_string_lossy()
            .into_owned();
        adjust(&mut config);
        let error = payment_gateway::app::App::start(&config)
            .await
            .err()
            .expect("refused");
        assert!(format!("{error:#}").contains(message), "{error:#}");
    }
}

#[tokio::test]
async fn a_return_due_before_a_slow_payment_completes_comes_first_in_the_log() {
    // Withdrawals take three days; a deposit completes in a minute and is returned the next business day. One clock
    // move covers all of it: the deposit's return (Tuesday) must come before the withdrawal's outcome (Thursday).
    let mut app = TestApp::start_with(|c| {
        c.payments.deposit.min_delay_seconds = 60;
        c.payments.deposit.max_delay_seconds = 60;
        c.payments.withdrawal.min_delay_seconds = 3 * 24 * 3600;
        c.payments.withdrawal.max_delay_seconds = 3 * 24 * 3600;
        c.payments.return_probability = 1.0;
        c.payments.max_return_days = 1;
        c.statement.enabled = false;
    })
    .await;
    app.pay("w-slow", "WITHDRAWAL", "10.00", "DKK").await;
    app.pay("d-fast", "DEPOSIT", "10.00", "DKK").await;
    app.advance(json!({ "days": 6 })).await;
    let log: Vec<(String, String)> = app
        .all_events()
        .await
        .iter()
        .map(|e| {
            (
                e["occurredAt"].as_str().unwrap().to_string(),
                format!(
                    "{} {}",
                    e["eventType"].as_str().unwrap(),
                    e["payload"]["idempotencyKey"].as_str().unwrap_or("-")
                ),
            )
        })
        .collect();
    assert!(log.is_sorted_by_key(|(at, _)| at.clone()), "{log:#?}");
    assert_eq!(log.len(), 7, "{log:#?}");
    assert_eq!(
        log.last().unwrap().1,
        "ClockChanged -",
        "the clock move comes after what became due on the way"
    );
    app.stop().await;
}

#[tokio::test]
async fn webhook_participant_filters_are_refused() {
    let mut app = TestApp::start().await;
    let (status, body) = app
        .post(
            "/api/webhooks",
            json!({ "url": "http://127.0.0.1:9/hook", "eventTypes": ["PaymentCompleted"], "participants": ["A"] }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["message"],
        json!(
            "invalid webhook subscription: participants: this service has no participants to filter by"
        )
    );
    app.stop().await;
}

/// SR-34 on the gateway: several event types, repeated or comma-separated, and the scanned-through header.
#[tokio::test]
async fn events_are_read_by_several_types_with_how_far_the_read_looked() {
    let mut app = TestApp::start().await;
    app.pay("d-1", "DEPOSIT", "100.00", "DKK").await;
    app.pay("w-1", "WITHDRAWAL", "50.00", "DKK").await;
    app.set_clock("2030-01-07T15:00:00Z").await;
    let all = app.all_events().await;
    let read = async |query: &str| {
        let (status, headers, body) = app
            .call_full(Method::GET, &format!("/api/events?{query}"), None)
            .await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        let scanned: u64 = headers["event-log-scanned-through"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let sequences: Vec<u64> = body
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["sequence"].as_u64().unwrap())
            .collect();
        (sequences, scanned)
    };
    let head = all.last().unwrap()["sequence"].as_u64().unwrap();
    let expected: Vec<u64> = all
        .iter()
        .filter(|e| e["eventType"] == "PaymentPending" || e["eventType"] == "PaymentCompleted")
        .map(|e| e["sequence"].as_u64().unwrap())
        .collect();
    assert_eq!(expected.len(), 4);
    assert_eq!(
        read("eventType=PaymentPending&eventType=PaymentCompleted").await,
        (expected.clone(), head)
    );
    assert_eq!(
        read("eventType=PaymentPending,PaymentCompleted").await,
        (expected, head)
    );
    assert_eq!(
        read("eventType=PaymentFailed&after=1").await,
        (vec![], head)
    );
    let (status, _) = app.get("/api/events?eventType=PaymentPending,Nope").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    app.stop().await;
}
