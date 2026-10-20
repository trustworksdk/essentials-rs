//! Liquidity provider runtime control (step 6.12, SR-11): `GET /api/liquidity[/{id}]` shows settings, inventory,
//! ladder, source and staleness; `PATCH /api/liquidity/{id}` changes settings field by field and the price source, at
//! once and durably.

mod common;

use axum::http::{Method, StatusCode};
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, IdMode, InstrumentConfig};
use exchange_simulator::liquidity::{LiquidityMode, LiquiditySpec};
use exchange_simulator::pricing::{PriceSourceKind, PriceSourceSpec};
use serde_json::{Value, json};

fn instrument(id: &str, liquidity: Option<LiquiditySpec>) -> InstrumentConfig {
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
        price_source: Some(PriceSourceSpec {
            kind: Some(PriceSourceKind::Fixed),
            price: Some("100.00".into()),
            ..Default::default()
        }),
        liquidity,
    }
}

/// ACME quoted by the provider (two levels, 10 bps), PLAIN without liquidity settings; a frozen virtual clock.
fn market(c: &mut Config) {
    c.ids.mode = IdMode::Deterministic;
    c.clock.mode = ClockMode::Virtual;
    c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
    c.instruments = vec![
        instrument(
            "ACME",
            Some(LiquiditySpec {
                mode: Some(LiquidityMode::On),
                levels: Some(2),
                spread_bps: Some(10.0),
                level_quantity: Some(10),
                ..Default::default()
            }),
        ),
        instrument("PLAIN", None),
    ];
}

async fn patch(app: &TestApp, id: &str, body: Value) -> (StatusCode, Value) {
    app.call(Method::PATCH, &format!("/api/liquidity/{id}"), Some(body))
        .await
}

fn prices(ladder: &Value, side: &str) -> Vec<i64> {
    ladder[side]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["priceTicks"].as_i64().unwrap())
        .collect()
}

#[tokio::test]
async fn the_status_shows_settings_inventory_ladder_and_source() {
    let mut app = TestApp::start_with(market).await;
    let (status, all) = app.get("/api/liquidity").await;
    assert_eq!(status, StatusCode::OK);
    let ids: Vec<&str> = all
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["instrumentId"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["ACME", "PLAIN"], "every instrument, in ID order");

    let (status, acme) = app.get("/api/liquidity/ACME").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        (
            &acme["providerId"],
            &acme["state"],
            &acme["inventory"],
            &acme["session"]
        ),
        (&json!("LP"), &json!("quoting"), &json!(0), &json!("open"))
    );
    assert_eq!(acme["settings"]["levels"], 2, "resolved settings");
    assert_eq!(
        acme["settings"]["maxInventory"], 10_000,
        "the built-in default where nothing is set"
    );
    assert_eq!(
        acme["own"],
        json!({ "mode": "on", "levels": 2, "spreadBps": 10.0, "levelQuantity": 10 })
    );
    assert_eq!(
        acme["quotedAround"],
        json!({ "priceTicks": 10_000, "price": "100.00" })
    );
    assert_eq!(prices(&acme["ladder"], "bids"), [9_990, 9_989]);
    assert_eq!(prices(&acme["ladder"], "asks"), [10_010, 10_011]);
    assert_eq!(acme["ladder"]["asks"][0]["quantity"], 10);
    assert_eq!(
        acme["priceSource"],
        json!({ "kind": "fixed", "price": "100.00" })
    );
    assert_eq!(
        (&acme["reference"]["source"], &acme["reference"]["stale"]),
        (&json!("fixed"), &json!(false))
    );

    let (_, plain) = app.get("/api/liquidity/PLAIN").await;
    assert_eq!(
        (&plain["state"], &plain["own"], &plain["ladder"]["asks"]),
        (&json!("off"), &Value::Null, &json!([]))
    );
    let (status, _) = app.get("/api/liquidity/NOPE").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // A client buys from the provider: its inventory goes short.
    let (status, _) = app
        .post("/api/participants", json!({ "id": "A", "name": "A" }))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = app
        .post(
            "/api/orders",
            json!({ "instrumentId": "ACME", "participantId": "A", "side": "BUY", "type": "MARKET", "quantity": 4 }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(app.get("/api/liquidity/ACME").await.1["inventory"], -4);
    app.stop().await;
}

#[tokio::test]
async fn a_patch_changes_settings_field_by_field_and_requotes_at_once() {
    let mut app = TestApp::start_with(market).await;
    let (status, widened) = patch(&app, "ACME", json!({ "spreadBps": 50 })).await;
    assert_eq!(status, StatusCode::OK, "{widened}");
    assert_eq!(
        prices(&widened["ladder"], "asks"),
        [10_050, 10_051],
        "quoted with the new spread at once"
    );
    let (_, deeper) = patch(&app, "ACME", json!({ "levels": 3 })).await;
    assert_eq!(
        deeper["own"],
        json!({ "mode": "on", "levels": 3, "spreadBps": 50.0, "levelQuantity": 10 }),
        "one field changed, the others kept"
    );
    assert_eq!(prices(&deeper["ladder"], "asks"), [10_050, 10_051, 10_052]);
    let (_, cleared) = patch(&app, "ACME", json!({ "spreadBps": null })).await;
    assert_eq!(
        cleared["settings"]["spreadBps"], 10.0,
        "null clears: the default applies again"
    );
    assert!(cleared["own"].get("spreadBps").is_none());

    // PLAIN had no settings: a patch creates them.
    let (_, plain) = patch(&app, "PLAIN", json!({ "mode": "on" })).await;
    assert_eq!(
        (&plain["state"], &plain["own"]),
        (&json!("quoting"), &json!({ "mode": "on" }))
    );
    let (_, off) = patch(&app, "PLAIN", json!({ "mode": null })).await;
    assert_eq!(
        (&off["state"], &off["own"], &off["ladder"]["asks"]),
        (&json!("off"), &Value::Null, &json!([]))
    );

    // Durable: each change is an InstrumentUpdated, and the settings survive a restart.
    let (_, events) = app.get("/api/events?eventType=InstrumentUpdated").await;
    let changes: Vec<&Value> = events
        .as_array()
        .unwrap()
        .iter()
        .map(|e| &e["payload"]["changes"])
        .collect();
    assert_eq!(changes.len(), 5);
    assert!(
        changes.iter().all(|c| **c == json!(["liquidity"])),
        "{changes:?}"
    );
    // An instrument listed through the API keeps its runtime settings across a restart; a declared one gets its
    // configuration back, as for every declared field.
    let (status, _) = app
        .post(
            "/api/instruments",
            json!({ "id": "LISTED", "symbol": "LISTED", "currency": "USD", "currencyScale": 2,
                    "priceSource": { "kind": "fixed", "price": "100.00" }, "liquidity": { "mode": "on" } }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    patch(&app, "LISTED", json!({ "levels": 1 })).await;
    app.restart().await;
    let (_, listed) = app.get("/api/liquidity/LISTED").await;
    assert_eq!(listed["own"], json!({ "mode": "on", "levels": 1 }));
    assert_eq!(
        prices(&listed["ladder"], "asks"),
        [10_010],
        "quoted with it after the restart"
    );
    let (_, declared) = app.get("/api/liquidity/ACME").await;
    assert_eq!(
        declared["own"]["levels"], 2,
        "the configuration wins at startup"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_patch_can_switch_the_price_source() {
    let mut app = TestApp::start_with(market).await;
    let (status, switched) = patch(
        &app,
        "ACME",
        json!({ "priceSource": { "kind": "synthetic", "volatility": 0.4 }, "spreadBps": 20 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{switched}");
    assert_eq!(
        switched["priceSource"],
        json!({ "kind": "synthetic", "initialPrice": "100.00", "volatility": 0.4 }),
        "continues from the current reference price"
    );
    assert_eq!(switched["reference"]["source"], "synthetic");
    assert_eq!(switched["own"]["spreadBps"], 20.0);
    let (_, events) = app.get("/api/events?eventType=InstrumentUpdated").await;
    assert_eq!(
        events[0]["payload"]["changes"],
        json!(["priceSource", "liquidity"])
    );
    let (_, none) = patch(&app, "ACME", json!({ "priceSource": null })).await;
    assert_eq!(
        (&none["priceSource"], &none["reference"], &none["state"]),
        (&Value::Null, &Value::Null, &json!("no-reference"))
    );
    app.stop().await;
}

#[tokio::test]
async fn invalid_patches_are_refused_and_change_nothing() {
    let mut app = TestApp::start_with(market).await;
    for (body, why) in [
        (json!({}), "empty"),
        (json!({ "spreadBpz": 5 }), "an unknown field"),
        (json!({ "levels": 0 }), "out of range"),
        (json!({ "mode": "sideways" }), "an unknown mode"),
        (
            json!({ "priceSource": { "kind": "astrology" } }),
            "an unknown price source",
        ),
        (
            json!({ "priceSource": { "kind": "fixed" } }),
            "a fixed source without a price",
        ),
    ] {
        let (status, body) = patch(&app, "ACME", body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{why}: {body}");
    }
    let (status, _) = patch(&app, "NOPE", json!({ "levels": 3 })).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, acme) = app.get("/api/liquidity/ACME").await;
    assert_eq!(acme["own"]["levels"], 2);
    let (_, events) = app.get("/api/events?eventType=InstrumentUpdated").await;
    assert_eq!(events, json!([]));
    app.stop().await;
}
