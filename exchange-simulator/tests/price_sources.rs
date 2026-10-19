//! Price sources and reference prices (step 6.2, SR-1 and SR-3): `none`, `fixed` and `synthetic` sources per
//! instrument, defaults from configuration, runtime changes without a restart, and synthetic walks that reproduce
//! across runs, restarts and resets.

mod common;

use axum::http::{Method, StatusCode};
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, IdMode, InstrumentConfig};
use exchange_simulator::pricing::{PriceSourceKind, PriceSourceSpec};
use serde_json::{Value, json};

const START: &str = "2030-01-07T09:00:00Z";

fn instrument(id: &str, price_source: Option<PriceSourceSpec>) -> InstrumentConfig {
    InstrumentConfig {
        id: id.into(),
        symbol: id.into(),
        mic: None,
        isin: None,
        cusip: None,
        currency: "USD".into(),
        currency_scale: 2,
        tick_size_minor: 1,
        lot_size: 1,
        enabled: true,
        reference_symbol: None,
        price_source,
        liquidity: None,
    }
}

fn synthetic(initial_price: &str) -> PriceSourceSpec {
    PriceSourceSpec {
        kind: Some(PriceSourceKind::Synthetic),
        initial_price: Some(initial_price.into()),
        volatility: Some(0.6),
        ..Default::default()
    }
}

/// A reproducible setup: deterministic IDs, a frozen virtual clock, test control, and SYN on a synthetic walk.
fn reproducible(c: &mut Config) {
    c.ids.mode = IdMode::Deterministic;
    c.ids.seed = 1;
    c.clock.mode = ClockMode::Virtual;
    c.clock.start = Some(START.parse().unwrap());
    c.test_control.enabled = true;
    c.price_sources.seed = 42;
    c.instruments = vec![instrument("SYN", Some(synthetic("100.00")))];
}

async fn advance(app: &TestApp, seconds: u64) {
    let (status, body) = app
        .post("/test/clock/advance", json!({ "seconds": seconds }))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

async fn reference(app: &TestApp, id: &str) -> Value {
    let (status, body) = app.get(&format!("/api/marketdata/{id}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["reference"].clone()
}

async fn patch(app: &TestApp, id: &str, body: Value) -> (StatusCode, Value) {
    app.call(Method::PATCH, &format!("/api/instruments/{id}"), Some(body))
        .await
}

#[tokio::test]
async fn each_kind_produces_its_reference_price() {
    let mut app = TestApp::start_with(|c| {
        reproducible(c);
        c.instruments = vec![
            instrument(
                "FIX",
                Some(PriceSourceSpec {
                    kind: Some(PriceSourceKind::Fixed),
                    price: Some("12.34".into()),
                    ..Default::default()
                }),
            ),
            instrument("NONE", None),
            instrument("SYN", Some(synthetic("100.00"))),
        ];
    })
    .await;
    advance(&app, 90).await;
    let (status, all) = app.get("/api/marketdata").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        all[0],
        json!({ "instrumentId": "FIX", "reference": { "price": "12.34", "priceTicks": 1234, "source": "fixed",
                                                      "sourceTimestamp": START } })
    );
    assert_eq!(all[1], json!({ "instrumentId": "NONE", "reference": null }));
    let syn = &all[2]["reference"];
    assert_eq!(
        (&syn["source"], &syn["sourceTimestamp"]),
        (&json!("synthetic"), &json!("2030-01-07T09:01:30Z")),
        "the last step at or before now"
    );
    assert_eq!(
        syn["price"]
            .as_str()
            .unwrap()
            .split('.')
            .nth(1)
            .map(str::len),
        Some(2),
        "on the cent grid"
    );
    let (status, _) = app.get("/api/marketdata/NOPE").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    app.stop().await;
}

#[tokio::test]
async fn a_synthetic_walk_reproduces_however_often_it_is_read() {
    let mut often = TestApp::start_with(reproducible).await;
    let mut once = TestApp::start_with(reproducible).await;
    let mut seen = Vec::new();
    for _ in 0..12 {
        advance(&often, 300).await;
        seen.push(reference(&often, "SYN").await["priceTicks"].clone());
    }
    advance(&once, 3_600).await;
    assert_eq!(
        reference(&once, "SYN").await,
        reference(&often, "SYN").await
    );
    seen.dedup();
    assert!(seen.len() > 1, "the price moves over an hour: {seen:?}");

    let mut reseeded = TestApp::start_with(|c| {
        reproducible(c);
        c.price_sources.seed = 43;
    })
    .await;
    advance(&reseeded, 3_600).await;
    assert_ne!(
        reference(&reseeded, "SYN").await["priceTicks"],
        reference(&once, "SYN").await["priceTicks"],
        "another seed, another path"
    );
    for app in [&mut often, &mut once, &mut reseeded] {
        app.stop().await;
    }
}

#[tokio::test]
async fn a_restart_and_a_reset_continue_the_same_walk() {
    let mut straight = TestApp::start_with(reproducible).await;
    advance(&straight, 3_600).await;
    let expected = reference(&straight, "SYN").await;

    let mut restarted = TestApp::start_with(reproducible).await;
    advance(&restarted, 1_800).await;
    restarted.restart().await;
    advance(&restarted, 1_800).await;
    assert_eq!(
        reference(&restarted, "SYN").await,
        expected,
        "after a restart"
    );

    let (status, _) = straight.post("/test/reset", json!({})).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    advance(&straight, 3_600).await;
    assert_eq!(reference(&straight, "SYN").await, expected, "after a reset");
    straight.stop().await;
    restarted.stop().await;
}

#[tokio::test]
async fn the_source_can_change_at_runtime() {
    let mut app = TestApp::start_with(reproducible).await;
    let (status, created) = app
        .post(
            "/api/instruments",
            json!({ "id": "LIVE", "symbol": "LIVE", "currency": "USD", "currencyScale": 2,
                    "priceSource": { "kind": "fixed", "price": "50.00" } }),
        )
        .await;
    assert_eq!(
        (status, &created["priceSource"]),
        (
            StatusCode::CREATED,
            &json!({ "kind": "fixed", "price": "50.00" })
        )
    );
    advance(&app, 60).await;

    // A switch to synthetic with no initial price continues from the current price, and stores it.
    let (status, switched) = patch(
        &app,
        "LIVE",
        json!({ "priceSource": { "kind": "synthetic" } }),
    )
    .await;
    assert_eq!(
        (status, &switched["priceSource"]),
        (
            StatusCode::OK,
            &json!({ "kind": "synthetic", "initialPrice": "50.00" })
        )
    );
    assert_eq!(
        reference(&app, "LIVE").await,
        json!({ "price": "50.00", "priceTicks": 5000, "source": "synthetic",
                "sourceTimestamp": "2030-01-07T09:01:00Z" }),
        "the walk starts at the switch"
    );
    let (_, events) = app.get("/api/events?eventType=InstrumentUpdated").await;
    assert_eq!(
        (
            &events[0]["payload"]["changes"],
            &events[0]["payload"]["priceSource"]
        ),
        (
            &json!(["priceSource"]),
            &json!({ "kind": "synthetic", "initialPrice": "50.00" })
        )
    );

    let (status, _) = patch(&app, "LIVE", json!({ "priceSource": { "kind": "none" } })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reference(&app, "LIVE").await, Value::Null);
    let (status, cleared) = patch(&app, "LIVE", json!({ "priceSource": null })).await;
    assert_eq!(
        (status, &cleared["priceSource"]),
        (StatusCode::OK, &Value::Null)
    );
    assert_eq!(
        reference(&app, "LIVE").await,
        Value::Null,
        "the default, none"
    );
    app.stop().await;
}

#[tokio::test]
async fn the_default_fills_in_what_an_instrument_leaves_out() {
    let mut app = TestApp::start_with(|c| {
        reproducible(c);
        c.price_sources.default = PriceSourceSpec {
            kind: Some(PriceSourceKind::Synthetic),
            volatility: Some(0.4),
            tick_interval_ms: Some(250),
            ..Default::default()
        };
        c.instruments = vec![instrument(
            "AAPL",
            Some(PriceSourceSpec {
                initial_price: Some("187.50".into()),
                ..Default::default()
            }),
        )];
    })
    .await;
    advance(&app, 1).await;
    let aapl = reference(&app, "AAPL").await;
    assert_eq!(
        (&aapl["source"], &aapl["sourceTimestamp"]),
        (&json!("synthetic"), &json!("2030-01-07T09:00:01Z"))
    );
    let (status, error) = app
        .post(
            "/api/instruments",
            json!({ "id": "MSFT", "symbol": "MSFT", "currency": "USD", "currencyScale": 2 }),
        )
        .await;
    assert_eq!(
        (status, error["message"].as_str()),
        (
            StatusCode::BAD_REQUEST,
            Some("priceSource.initialPrice is required for kind synthetic")
        ),
        "the default's kind needs an initial price"
    );
    app.stop().await;
}

#[tokio::test]
async fn invalid_sources_are_rejected() {
    let mut app = TestApp::start_with(reproducible).await;
    for (price_source, why) in [
        (
            json!({ "kind": "fixed", "price": "1.005" }),
            "off the cent grid",
        ),
        (json!({ "kind": "fixed" }), "no price"),
        (
            json!({ "kind": "synthetic" }),
            "no initial price and no current price",
        ),
        (
            json!({ "kind": "synthetic", "initialPrice": "1.00", "volatility": -0.1 }),
            "negative volatility",
        ),
        (
            json!({ "kind": "synthetic", "initialPrice": "1.00", "tickIntervalMs": 50 }),
            "a step too short",
        ),
        (json!({ "kind": "finnhub" }), "an unknown kind"),
        (
            json!({ "kind": "fixed", "price": "1.00", "colour": "red" }),
            "an unknown field",
        ),
    ] {
        let (status, body) = app
            .post(
                "/api/instruments",
                json!({ "id": "BAD", "symbol": "BAD", "currency": "USD", "currencyScale": 2,
                        "priceSource": price_source }),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{why}: {body}");
    }
    let (status, _) = patch(&app, "SYN", json!({ "priceSource": { "kind": "fixed" } })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (_, instruments) = app.get("/api/instruments").await;
    assert_eq!(
        instruments.as_array().unwrap().len(),
        1,
        "nothing was listed"
    );
    let (_, events) = app.get("/api/events").await;
    assert_eq!(events.as_array().unwrap().len(), 1, "or published");

    // A declared source that cannot resolve stops startup, naming the instrument.
    app.stop().await;
    let mut config = app.config.clone();
    config.instruments[0].price_source = Some(PriceSourceSpec {
        kind: Some(PriceSourceKind::Fixed),
        ..Default::default()
    });
    let error = exchange_simulator::app::App::start(&config)
        .err()
        .expect("startup fails");
    assert!(
        format!("{error:#}")
            .contains("instruments SYN: priceSource.price is required for kind fixed"),
        "{error:#}"
    );
}
