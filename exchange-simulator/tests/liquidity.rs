//! The built-in liquidity provider (step 6.7, SR-6, SR-7, SR-8, SR-10, D18): a ladder around the reference price, swept
//! by market orders, refilled, skewed by inventory up to its limit, its order events kept out of the log unless asked
//! for, and quoted afresh after a restart.

mod common;

use axum::http::{Method, StatusCode};
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, IdMode, InstrumentConfig};
use exchange_simulator::liquidity::{LiquidityMode, LiquiditySpec};
use exchange_simulator::pricing::{PriceSourceKind, PriceSourceSpec};
use serde_json::{Value, json};

fn spec(adjust: impl FnOnce(&mut LiquiditySpec)) -> LiquiditySpec {
    let mut spec = LiquiditySpec {
        mode: Some(LiquidityMode::On),
        levels: Some(3),
        spread_bps: Some(10.0),
        level_quantity: Some(10),
        ..Default::default()
    };
    adjust(&mut spec);
    spec
}

/// ACME at a fixed 100.00, with the provider quoting `liquidity`; participant A; a frozen virtual clock.
fn market(liquidity: LiquiditySpec) -> impl FnOnce(&mut Config) {
    move |c: &mut Config| {
        c.ids.mode = IdMode::Deterministic;
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
        c.instruments = vec![InstrumentConfig {
            id: "ACME".into(),
            symbol: "ACME".into(),
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
            liquidity: Some(liquidity),
        }];
    }
}

async fn client(app: &TestApp) {
    let (status, _) = app
        .post(
            "/api/participants",
            json!({ "id": "A", "name": "Client A" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
}

/// `(price, quantity)` per level, best first.
async fn side(app: &TestApp, side: &str) -> Vec<(i64, u64)> {
    let (_, book) = app.get("/api/orderbooks/ACME").await;
    book[side]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| {
            (
                l["priceTicks"].as_i64().unwrap(),
                l["quantity"].as_u64().unwrap(),
            )
        })
        .collect()
}

async fn order(app: &TestApp, side: &str, price: Option<i64>, quantity: u64) -> Value {
    let (status, body) = app.order("A", side, price, quantity).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body
}

async fn events(app: &TestApp) -> Vec<Value> {
    app.get("/api/events?limit=1000")
        .await
        .1
        .as_array()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn the_ladder_is_quoted_around_the_reference_price_and_swept_by_a_market_order() {
    let mut app = TestApp::start_with(market(spec(|_| {}))).await;
    client(&app).await;
    // R = 100.00, 10 bps, tick 0.01 (SR-7's acceptance).
    assert_eq!(
        side(&app, "bids").await,
        [(9_990, 10), (9_989, 10), (9_988, 10)]
    );
    assert_eq!(
        side(&app, "asks").await,
        [(10_010, 10), (10_011, 10), (10_012, 10)]
    );

    // A market buy larger than the ladder fills level by level and cancels the remainder.
    let bought = order(&app, "BUY", None, 50).await;
    let prices: Vec<i64> = bought["executions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["priceTicks"].as_i64().unwrap())
        .collect();
    assert_eq!(prices, [10_010, 10_011, 10_012]);
    assert_eq!(
        (
            &bought["order"]["status"],
            &bought["order"]["remainingQuantity"]
        ),
        (&json!("CANCELLED"), &json!(20))
    );
    let trades = app.get("/api/trades").await.1;
    assert!(
        trades
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["sellerId"] == "LP")
    );
    // Refilled at once (refill delay 0), around the same reference.
    assert_eq!(
        side(&app, "asks").await,
        [(10_010, 10), (10_011, 10), (10_012, 10)]
    );

    // The provider's order lifecycle is not in the log; its trades are.
    let log = events(&app).await;
    assert!(
        log.iter().all(|e| e["payload"]["participantId"] != "LP"),
        "no provider order events"
    );
    assert_eq!(
        log.iter()
            .filter(|e| e["eventType"] == "TradeExecuted")
            .count(),
        3
    );
    app.stop().await;
}

#[tokio::test]
async fn client_limit_orders_cross_the_ladder_at_its_prices_or_rest_inside_the_spread() {
    let mut app = TestApp::start_with(market(spec(|_| {}))).await;
    client(&app).await;
    let crossing = order(&app, "SELL", Some(9_980), 15).await;
    let fills: Vec<(i64, u64)> = crossing["executions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["priceTicks"].as_i64().unwrap(),
                e["quantity"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        fills,
        [(9_990, 10), (9_989, 5)],
        "at the ladder's prices, best first"
    );
    let inside = order(&app, "BUY", Some(10_000), 5).await;
    assert_eq!(inside["order"]["status"], "OPEN", "rests inside the spread");
    assert_eq!(side(&app, "bids").await[0], (10_000, 5));
    app.stop().await;
}

#[tokio::test]
async fn inventory_skews_the_ladder_until_the_limit_leaves_one_side() {
    let mut app = TestApp::start_with(market(spec(|s| {
        s.levels = Some(1);
        s.skew_bps_per_lot = Some(1.0);
        s.max_inventory = Some(30);
    })))
    .await;
    client(&app).await;
    let mut best_asks = Vec::new();
    for _ in 0..3 {
        best_asks.push(side(&app, "asks").await[0].0);
        order(&app, "BUY", None, 10).await;
    }
    // Each buy leaves the provider 10 lots shorter, which raises its quotes by 10 bps of 100.00: 10 ticks.
    assert_eq!(best_asks, [10_010, 10_020, 10_030]);
    assert!(
        side(&app, "asks").await.is_empty(),
        "short 30 lots: no more selling"
    );
    assert_eq!(
        side(&app, "bids").await,
        [(10_020, 10)],
        "bids only, skewed up 30 ticks"
    );
    app.stop().await;
}

#[tokio::test]
async fn buying_from_clients_makes_the_provider_long_and_lowers_its_quotes() {
    let mut app = TestApp::start_with(market(spec(|s| {
        s.levels = Some(1);
        s.skew_bps_per_lot = Some(1.0);
    })))
    .await;
    client(&app).await;
    order(&app, "SELL", None, 10).await;
    assert_eq!(
        side(&app, "bids").await,
        [(9_980, 10)],
        "10 lots long: 10 ticks lower"
    );
    assert_eq!(side(&app, "asks").await, [(10_000, 10)]);
    app.stop().await;
}

#[tokio::test]
async fn the_ladder_moves_with_the_reference_beyond_the_threshold() {
    let mut app = TestApp::start_with(market(spec(|s| s.requote_threshold_ticks = Some(20)))).await;
    let patch = |price: &str| json!({ "priceSource": { "kind": "fixed", "price": price } });
    app.call(
        Method::PATCH,
        "/api/instruments/ACME",
        Some(patch("100.10")),
    )
    .await;
    assert_eq!(
        side(&app, "asks").await[0].0,
        10_010,
        "10 ticks: below the threshold"
    );
    app.call(
        Method::PATCH,
        "/api/instruments/ACME",
        Some(patch("101.00")),
    )
    .await;
    assert_eq!(
        side(&app, "asks").await[0].0,
        10_111,
        "100 ticks: quoted again (101.00 + 10 bps, rounded up)"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_refill_waits_for_its_delay_in_simulated_time() {
    let mut app = TestApp::start_with(market(spec(|s| s.refill_delay_ms = Some(60_000)))).await;
    client(&app).await;
    order(&app, "BUY", None, 10).await;
    assert_eq!(
        side(&app, "asks").await[0],
        (10_011, 10),
        "the best level is gone"
    );
    app.post("/test/clock/advance", json!({ "seconds": 59 }))
        .await;
    assert_eq!(side(&app, "asks").await[0].0, 10_011);
    app.post("/test/clock/advance", json!({ "seconds": 1 }))
        .await;
    assert_eq!(
        side(&app, "asks").await[0],
        (10_010, 10),
        "refilled after a minute"
    );
    app.stop().await;
}

#[tokio::test]
async fn the_ladder_is_pulled_when_off_disabled_or_without_a_reference() {
    let mut app = TestApp::start_with(market(spec(|_| {}))).await;
    for (change, why) in [
        (json!({ "liquidity": { "mode": "off" } }), "mode off"),
        (json!({ "enabled": false }), "trading disabled"),
        (
            json!({ "enabled": true, "priceSource": { "kind": "none" } }),
            "no reference price",
        ),
    ] {
        let (status, body) = app
            .call(Method::PATCH, "/api/instruments/ACME", Some(change))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(
            side(&app, "bids").await.is_empty() && side(&app, "asks").await.is_empty(),
            "{why}"
        );
        assert_eq!(
            app.count(
                "SELECT COUNT(*) FROM orders WHERE participant_id = 'LP' AND status = 'REJECTED'"
            ),
            0,
            "{why}: pulled, not refused order by order"
        );
        // Back on for the next case.
        app.call(
            Method::PATCH,
            "/api/instruments/ACME",
            Some(json!({ "liquidity": spec(|_| {}), "enabled": true,
                         "priceSource": { "kind": "fixed", "price": "100.00" } })),
        )
        .await;
        assert_eq!(
            side(&app, "asks").await.len(),
            3,
            "quoted again after {why}"
        );
    }
    app.stop().await;
}

#[tokio::test]
async fn after_a_restart_the_ladder_is_quoted_afresh_with_the_inventory_from_the_trades() {
    let mut app = TestApp::start_with(market(spec(|s| {
        s.levels = Some(1);
        s.skew_bps_per_lot = Some(1.0);
    })))
    .await;
    client(&app).await;
    order(&app, "BUY", None, 10).await;
    let before = (side(&app, "bids").await, side(&app, "asks").await);
    assert_eq!(before.1, [(10_020, 10)]);
    app.restart().await;
    assert_eq!((side(&app, "bids").await, side(&app, "asks").await), before);
    let resting: i64 = app.count(
        "SELECT COUNT(*) FROM orders WHERE participant_id = 'LP' AND status IN ('OPEN', 'PARTIALLY_FILLED')",
    );
    assert_eq!(resting, 2, "the old ladder was replaced, not added to");
    app.stop().await;
}

#[tokio::test]
async fn order_events_are_published_when_asked_for_and_runs_reproduce() {
    let run = || async {
        let mut app = TestApp::start_with(|c| {
            market(spec(|_| {}))(c);
            c.liquidity.emit_order_events = true;
        })
        .await;
        client(&app).await;
        order(&app, "BUY", None, 15).await;
        let log = events(&app).await;
        app.stop().await;
        log
    };
    let first = run().await;
    assert!(
        first
            .iter()
            .any(|e| e["eventType"] == "OrderAccepted" && e["payload"]["participantId"] == "LP"),
        "the provider's orders are published"
    );
    assert_eq!(
        run().await,
        first,
        "deterministic IDs and a frozen clock give the same log"
    );
}

#[tokio::test]
async fn invalid_settings_are_refused() {
    let mut app = TestApp::start_with(market(spec(|_| {}))).await;
    for (liquidity, why) in [
        (json!({ "levels": 0 }), "no levels"),
        (json!({ "spreadBps": -1 }), "a negative spread"),
        (json!({ "levelQuantity": 0 }), "an empty level"),
        (json!({ "mode": "sometimes" }), "an unknown mode"),
        (json!({ "colour": "red" }), "an unknown field"),
    ] {
        let (status, body) = app
            .call(
                Method::PATCH,
                "/api/instruments/ACME",
                Some(json!({ "liquidity": liquidity })),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{why}: {body}");
    }
    app.stop().await;
}

#[tokio::test]
async fn the_provider_clears_like_any_participant() {
    let mut app = TestApp::start_with(|c| {
        market(spec(|_| {}))(c);
        c.clearing.enabled = true;
    })
    .await;
    client(&app).await;
    order(&app, "BUY", None, 10).await;
    app.post("/test/clock/advance", json!({ "days": 1 })).await;
    let (_, statements) = app.get("/api/clearing/statements?date=2030-01-07").await;
    let participants: Vec<&str> = statements
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["participantId"].as_str().unwrap())
        .collect();
    assert_eq!(participants, ["A", "LP"]);
    // What A receives, the provider delivers: the obligations net to zero across the two.
    let securities: i64 = statements
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["obligations"].as_array().unwrap().clone())
        .map(|o| o["securitiesNet"].as_i64().unwrap())
        .sum();
    assert_eq!(securities, 0);
    app.stop().await;
}
