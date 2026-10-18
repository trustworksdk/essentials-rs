//! Instruments declared in configuration and instrument events (step 6.1): declared instruments are listed at startup
//! and after a reset, configuration wins over API changes at each start, fields that never change fail startup when
//! they differ, and every listing or change is an event in the canonical log.

mod common;

use axum::http::{Method, StatusCode};
use common::TestApp;
use exchange_simulator::app::App;
use exchange_simulator::config::{Config, InstrumentConfig, SubscriptionConfig};
use serde_json::{Value, json};

fn declared(id: &str, mic: &str) -> InstrumentConfig {
    InstrumentConfig {
        id: id.into(),
        symbol: id.into(),
        mic: Some(mic.into()),
        isin: None,
        cusip: None,
        currency: "USD".into(),
        currency_scale: 2,
        tick_size_minor: 1,
        lot_size: 1,
        enabled: true,
        reference_symbol: Some(id.into()),
        price_source: None,
    }
}

fn with_declared(c: &mut Config) {
    c.instruments = vec![declared("AAPL", "XNAS"), declared("MSFT", "XNAS")];
    c.webhooks.subscriptions.push(SubscriptionConfig {
        name: "demo".into(),
        url: "http://127.0.0.1:9/demo".into(),
        event_types: vec!["InstrumentListed".into(), "InstrumentUpdated".into()],
        secret: None,
        secret_env: None,
        secret_file: None,
        ordered: true,
        enabled: true,
        paused: true,
        participants: None,
    });
}

/// Every event in the log, oldest first.
async fn events(app: &TestApp) -> Vec<Value> {
    let (status, events) = app.get("/api/events?limit=1000").await;
    assert_eq!(status, StatusCode::OK);
    events.as_array().unwrap().clone()
}

/// `(eventType, aggregateId)` of every event.
async fn log(app: &TestApp) -> Vec<(String, String)> {
    events(app)
        .await
        .iter()
        .map(|e| {
            (
                e["eventType"].as_str().unwrap().to_string(),
                e["aggregateId"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn listed(id: &str) -> (String, String) {
    ("InstrumentListed".into(), id.into())
}

fn updated(id: &str) -> (String, String) {
    ("InstrumentUpdated".into(), id.into())
}

async fn patch(app: &TestApp, id: &str, body: Value) -> (StatusCode, Value) {
    app.call(Method::PATCH, &format!("/api/instruments/{id}"), Some(body))
        .await
}

#[tokio::test]
async fn declared_instruments_are_listed_at_startup_with_an_event_each() {
    let mut app = TestApp::start_with(with_declared).await;
    let (_, instruments) = app.get("/api/instruments").await;
    assert_eq!(
        instruments,
        json!([
            { "id": "AAPL", "symbol": "AAPL", "mic": "XNAS", "isin": null, "cusip": null, "currency": "USD",
              "currencyScale": 2, "tickSizeMinor": 1, "tickSize": "0.01", "lotSize": 1, "enabled": true,
              "referenceSymbol": "AAPL", "priceSource": null },
            { "id": "MSFT", "symbol": "MSFT", "mic": "XNAS", "isin": null, "cusip": null, "currency": "USD",
              "currencyScale": 2, "tickSizeMinor": 1, "tickSize": "0.01", "lotSize": 1, "enabled": true,
              "referenceSymbol": "MSFT", "priceSource": null }
        ])
    );
    let all = events(&app).await;
    assert_eq!(log(&app).await, [listed("AAPL"), listed("MSFT")]);
    assert_eq!(
        (
            &all[0]["sequence"],
            &all[0]["source"],
            &all[0]["correlationId"]
        ),
        (&json!(1), &json!("exchange"), &Value::Null)
    );
    assert_eq!(
        all[0]["payload"],
        json!({ "instrumentId": "AAPL", "symbol": "AAPL", "mic": "XNAS", "isin": null, "cusip": null,
                "currency": "USD", "currencyScale": 2, "tickSizeMinor": 1, "tickSize": "0.01", "lotSize": 1,
                "enabled": true, "referenceSymbol": "AAPL", "priceSource": null })
    );
    // The declared subscription exists before the instruments are listed, so it has their deliveries.
    assert_eq!(app.count("SELECT COUNT(*) FROM webhook_deliveries"), 2);

    app.restart().await;
    assert_eq!(
        log(&app).await,
        [listed("AAPL"), listed("MSFT")],
        "an unchanged configuration publishes nothing on restart"
    );
    app.stop().await;
}

#[tokio::test]
async fn configuration_wins_over_api_changes_at_each_start() {
    let mut app = TestApp::start_with(with_declared).await;
    let (status, aapl) = patch(&app, "AAPL", json!({ "enabled": false })).await;
    assert_eq!((status, &aapl["enabled"]), (StatusCode::OK, &json!(false)));

    app.restart().await;
    let (_, aapl) = app.get("/api/instruments?symbol=AAPL").await;
    assert_eq!(aapl[0]["enabled"], json!(true), "the configuration wins");
    let all = events(&app).await;
    assert_eq!(
        log(&app).await,
        [
            listed("AAPL"),
            listed("MSFT"),
            updated("AAPL"),
            updated("AAPL")
        ]
    );
    assert_eq!(
        (&all[2]["payload"]["changes"], &all[2]["payload"]["enabled"]),
        (&json!(["enabled"]), &json!(false))
    );
    assert_eq!(
        (&all[3]["payload"]["changes"], &all[3]["payload"]["enabled"]),
        (&json!(["enabled"]), &json!(true))
    );

    // Changing the configuration updates the instrument; removing one leaves it listed.
    app.config.instruments[1].enabled = false;
    app.config.instruments[1].reference_symbol = Some("MSFT.O".into());
    app.config.instruments.remove(0);
    app.restart().await;
    let (_, instruments) = app.get("/api/instruments").await;
    assert_eq!(
        instruments
            .as_array()
            .unwrap()
            .iter()
            .map(|i| (
                i["id"].clone(),
                i["enabled"].clone(),
                i["referenceSymbol"].clone()
            ))
            .collect::<Vec<_>>(),
        [
            (json!("AAPL"), json!(true), json!("AAPL")),
            (json!("MSFT"), json!(false), json!("MSFT.O"))
        ]
    );
    let all = events(&app).await;
    assert_eq!(all.len(), 5);
    assert_eq!(
        (&all[4]["eventType"], &all[4]["payload"]["changes"]),
        (
            &json!("InstrumentUpdated"),
            &json!(["enabled", "referenceSymbol"])
        )
    );
    app.stop().await;
}

#[tokio::test]
async fn a_declared_field_that_cannot_change_stops_startup() {
    let mut app = TestApp::start_with(with_declared).await;
    app.stop().await;
    let mut config = app.config.clone();
    config.instruments[0].tick_size_minor = 5;
    let error = App::start(&config).err().expect("startup fails");
    assert!(
        format!("{error:#}").contains(
            "instruments AAPL: tick_size_minor differs from the listed instrument; only enabled, \
             reference_symbol and price_source can change"
        ),
        "{error:#}"
    );
    config = app.config.clone();
    config.instruments[1].mic = Some("XNYS".into());
    let error = App::start(&config).err().expect("startup fails");
    assert!(
        format!("{error:#}").contains("instruments MSFT: mic differs"),
        "{error:#}"
    );

    // The unchanged configuration still starts, with nothing published by the failed attempts.
    app.restart().await;
    assert_eq!(log(&app).await, [listed("AAPL"), listed("MSFT")]);
    app.stop().await;
}

#[tokio::test]
async fn invalid_declarations_stop_startup() {
    let mut config = TestApp::config(
        &std::env::temp_dir().join(format!("exchange-sim-it-{}", uuid::Uuid::now_v7())),
    );
    config.instruments = vec![declared("AAPL", "XNAS"), declared("AAPL", "XNYS")];
    let error = App::start(&config).err().expect("a duplicate ID fails");
    assert!(
        format!("{error:#}").contains("instruments: AAPL is declared twice"),
        "{error:#}"
    );
    config.instruments = vec![InstrumentConfig {
        reference_symbol: Some("A B".into()),
        ..declared("AAPL", "XNAS")
    }];
    let error = App::start(&config).err().expect("an invalid field fails");
    assert!(
        format!("{error:#}").contains("reference_symbol"),
        "{error:#}"
    );
    let _ = std::fs::remove_dir_all(
        std::path::Path::new(&config.database.path)
            .parent()
            .unwrap(),
    );
}

#[tokio::test]
async fn a_reset_lists_the_declared_instruments_again() {
    let mut app = TestApp::start_with(|c| {
        with_declared(c);
        c.test_control.enabled = true;
    })
    .await;
    let (status, _) = app
        .post(
            "/api/instruments",
            json!({ "id": "TSLA", "symbol": "TSLA", "mic": "XNAS", "currency": "USD", "currencyScale": 2 }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    patch(&app, "MSFT", json!({ "enabled": false })).await;
    assert_eq!(events(&app).await.len(), 4);

    let (status, _) = app.post("/test/reset", json!({})).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, instruments) = app.get("/api/instruments").await;
    assert_eq!(
        instruments
            .as_array()
            .unwrap()
            .iter()
            .map(|i| (i["id"].clone(), i["enabled"].clone()))
            .collect::<Vec<_>>(),
        [(json!("AAPL"), json!(true)), (json!("MSFT"), json!(true))],
        "API-registered instruments and API changes are gone"
    );
    let all = events(&app).await;
    assert_eq!(log(&app).await, [listed("AAPL"), listed("MSFT")]);
    assert_eq!(all[0]["sequence"], json!(1));
    assert_eq!(app.count("SELECT COUNT(*) FROM webhook_deliveries"), 2);
    app.stop().await;
}

#[tokio::test]
async fn api_listings_and_changes_are_events() {
    let mut app = TestApp::start().await;
    let (status, created) = app
        .post(
            "/api/instruments",
            json!({ "id": "AAPL", "symbol": "AAPL", "mic": "XNAS", "currency": "USD", "currencyScale": 2,
                    "referenceSymbol": "AAPL", "priceSource": null }),
        )
        .await;
    assert_eq!(
        (status, &created["referenceSymbol"]),
        (StatusCode::CREATED, &json!("AAPL"))
    );

    let (status, changed) = patch(&app, "AAPL", json!({ "referenceSymbol": "NASDAQ:AAPL" })).await;
    assert_eq!(
        (status, &changed["referenceSymbol"], &changed["enabled"]),
        (StatusCode::OK, &json!("NASDAQ:AAPL"), &json!(true)),
        "absent fields stay as they are"
    );
    let (status, unchanged) = patch(&app, "AAPL", json!({ "enabled": true })).await;
    assert_eq!(
        (status, &unchanged["enabled"]),
        (StatusCode::OK, &json!(true))
    );
    let (status, cleared) = patch(&app, "AAPL", json!({ "referenceSymbol": null })).await;
    assert_eq!(
        (status, &cleared["referenceSymbol"]),
        (StatusCode::OK, &Value::Null)
    );

    let all = events(&app).await;
    assert_eq!(
        log(&app).await,
        [listed("AAPL"), updated("AAPL"), updated("AAPL")],
        "a change that alters nothing publishes nothing"
    );
    assert_eq!(
        (
            &all[1]["payload"]["changes"],
            &all[1]["payload"]["referenceSymbol"]
        ),
        (&json!(["referenceSymbol"]), &json!("NASDAQ:AAPL"))
    );
    assert_eq!(all[2]["payload"]["referenceSymbol"], Value::Null);

    for (body, why) in [
        (json!({}), "an empty change"),
        (json!({ "referenceSymbol": "" }), "a blank reference symbol"),
        (json!({ "referenceSymbol": "A B" }), "a space"),
        (json!({ "symbol": "NEW" }), "a field that cannot change"),
    ] {
        let (status, error) = patch(&app, "AAPL", body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{why}: {error}");
    }
    let (status, _) = app
        .post(
            "/api/instruments",
            json!({ "id": "MSFT", "symbol": "MSFT", "currency": "USD", "currencyScale": 2,
                    "referenceSymbol": "x".repeat(65) }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        events(&app).await.len(),
        3,
        "rejected requests publish nothing"
    );
    app.stop().await;
}
