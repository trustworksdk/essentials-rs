//! Corporate actions (step 6.23, SR-38): cash dividends paid on contractual holdings at the record date, and splits
//! that scale resting orders and the reference price, announced by an operator or from configuration.

mod common;

use axum::http::{Method, StatusCode};
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config};
use exchange_simulator::corporate_actions::{ActionKind, ActionSpec};
use serde_json::{Value, json};

/// Monday 2030-01-07 09:00 UTC on a frozen virtual clock, T+1.
fn monday(c: &mut Config) {
    c.clock.mode = ClockMode::Virtual;
    c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
    c.test_control.enabled = true;
    c.settlement.settlement_days = 1;
}

fn declared(id: &str) -> exchange_simulator::config::InstrumentConfig {
    exchange_simulator::config::InstrumentConfig {
        id: id.into(),
        symbol: id.into(),
        mic: Some("XCSE".into()),
        isin: None,
        cusip: None,
        currency: "DKK".into(),
        currency_scale: 2,
        tick_size_minor: 1,
        lot_size: 1,
        enabled: true,
        reference_symbol: None,
        price_source: None,
        liquidity: None,
    }
}

async fn set_clock(app: &TestApp, to: &str) {
    let (status, body) = app.post("/test/clock/set", json!({ "to": to })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// `seller` sells `quantity` to `buyer` at 100.00.
async fn trade(app: &TestApp, buyer: &str, seller: &str, quantity: u64) {
    app.order(seller, "SELL", Some(10_000), quantity).await;
    let (_, buy) = app.order(buyer, "BUY", Some(10_000), quantity).await;
    assert_eq!(buy["executions"][0]["quantity"], quantity, "{buy}");
}

async fn reference(app: &TestApp) -> Value {
    app.get("/api/marketdata/ACME").await.1["reference"]["price"].clone()
}

async fn events(app: &TestApp, event_type: &str) -> Vec<Value> {
    app.get(&format!("/api/events?eventType={event_type}&limit=1000"))
        .await
        .1
        .as_array()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn a_dividend_is_paid_on_holdings_settled_by_the_record_date() {
    let mut app = TestApp::start_with(monday).await;
    app.seed().await;
    trade(&app, "A", "B", 100).await; // Settles Tuesday.
    let (status, announced) = app
        .post(
            "/api/corporate-actions",
            json!({ "kind": "CASH_DIVIDEND", "instrumentId": "ACME", "exDate": "2030-01-08",
                    "recordDate": "2030-01-09", "payDate": "2030-01-11", "amountPerShare": "0.50" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{announced}");
    assert_eq!(
        (&announced["corporateActionId"], &announced["status"]),
        (
            &json!("CA:ACME:CASH_DIVIDEND:2030-01-11"),
            &json!("ANNOUNCED")
        )
    );
    assert_eq!(events(&app, "CorporateActionAnnounced").await.len(), 1);

    set_clock(&app, "2030-01-08T10:00:00Z").await;
    trade(&app, "D", "B", 40).await; // Settles Wednesday, the record date.
    set_clock(&app, "2030-01-10T10:00:00Z").await;
    trade(&app, "C", "B", 50).await; // Settles Friday: after the record date.
    set_clock(&app, "2030-01-11T12:00:00Z").await;

    let paid = events(&app, "DividendPaid").await;
    let payments: Vec<(&str, &Value, &Value)> = paid
        .iter()
        .map(|e| {
            (
                e["payload"]["participantId"].as_str().unwrap(),
                &e["payload"]["quantity"],
                &e["payload"]["amount"],
            )
        })
        .collect();
    assert_eq!(
        payments,
        [
            ("A", &json!(100), &json!("50.00")),
            ("D", &json!(40), &json!("20.00"))
        ],
        "100 × 0.50 and 40 × 0.50; B is short and C settles after the record date"
    );
    assert_eq!(
        paid[0]["occurredAt"], "2030-01-11T00:00:00Z",
        "the pay date"
    );
    assert_eq!(paid[0]["correlationId"], "A");
    let (_, actions) = app.get("/api/corporate-actions").await;
    assert_eq!(actions[0]["status"], "DONE");

    for (body, expected) in [
        (
            json!({ "kind": "CASH_DIVIDEND", "instrumentId": "ACME", "exDate": "2030-01-08",
                    "recordDate": "2030-01-09", "payDate": "2030-01-11", "amountPerShare": "0.50" }),
            StatusCode::CONFLICT,
        ),
        (
            json!({ "kind": "CASH_DIVIDEND", "instrumentId": "ACME", "exDate": "2030-01-10",
                    "recordDate": "2030-01-09", "payDate": "2030-01-14", "amountPerShare": "0.50" }),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({ "kind": "CASH_DIVIDEND", "instrumentId": "ACME", "exDate": "2030-01-14",
                    "recordDate": "2030-01-14", "payDate": "2030-01-15", "amountPerShare": "0.505" }),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({ "kind": "SPLIT", "instrumentId": "ACME", "ratio": "1:1", "effectiveDate": "2030-01-15" }),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({ "kind": "SPLIT", "instrumentId": "NOPE", "ratio": "2:1", "effectiveDate": "2030-01-15" }),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let (status, answer) = app.post("/api/corporate-actions", body.clone()).await;
        assert_eq!(status, expected, "{body}: {answer}");
    }
    app.stop().await;
}

#[tokio::test]
async fn a_split_doubles_resting_orders_halves_their_prices_and_moves_the_reference() {
    let mut app = TestApp::start_with(monday).await;
    app.seed().await;
    let (status, body) = app
        .call(
            Method::PATCH,
            "/api/instruments/ACME",
            Some(json!({ "priceSource": { "kind": "fixed", "price": "100.00" } })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    trade(&app, "A", "B", 100).await; // Settles Tuesday.
    let (_, bid) = app.order("A", "BUY", Some(10_000), 10).await;
    let (_, ask) = app.order("C", "SELL", Some(10_101), 7).await;
    let (status, _) = app
        .post(
            "/api/corporate-actions",
            json!({ "kind": "SPLIT", "instrumentId": "ACME", "ratio": "2:1", "effectiveDate": "2030-01-09" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    set_clock(&app, "2030-01-09T01:00:00Z").await;

    let order = |result: &Value| result["order"]["orderId"].as_str().unwrap().to_string();
    let (_, bid) = app.get(&format!("/api/orders/{}", order(&bid))).await;
    assert_eq!(
        (
            &bid["quantity"],
            &bid["remainingQuantity"],
            &bid["priceTicks"]
        ),
        (&json!(20), &json!(20), &json!(5_000)),
        "doubled and halved"
    );
    let (_, ask) = app.get(&format!("/api/orders/{}", order(&ask))).await;
    assert_eq!(
        (&ask["remainingQuantity"], &ask["price"]),
        (&json!(14), &json!("50.51")),
        "101.01 / 2 to the nearest tick"
    );
    let (_, book) = app.get("/api/orderbooks/ACME").await;
    assert_eq!(
        (&book["bids"][0]["priceTicks"], &book["bids"][0]["quantity"]),
        (&json!(5_000), &json!(20))
    );
    assert_eq!(book["asks"][0]["priceTicks"], 5_051);

    let applied = events(&app, "SplitApplied").await;
    let by = |id: &str| {
        applied
            .iter()
            .find(|e| e["payload"]["participantId"] == id)
            .unwrap_or_else(|| panic!("no SplitApplied for {id}"))["payload"]
            .clone()
    };
    let a = by("A");
    assert_eq!(
        (&a["quantityBefore"], &a["quantityAfter"], &a["ratio"]),
        (&json!(100), &json!(200), &json!("2:1"))
    );
    assert_eq!(a["orders"][0]["previousPrice"], "100.00");
    assert_eq!(a["orders"][0]["price"], "50.00");
    assert_eq!(by("B")["quantityAfter"], -200);
    assert_eq!(by("C")["orders"][0]["remainingQuantity"], 14);

    assert_eq!(reference(&app).await, "50.00");
    app.restart().await;
    assert_eq!(
        reference(&app).await,
        "50.00",
        "the split survives a restart"
    );
    let (_, book) = app.get("/api/orderbooks/ACME").await;
    assert_eq!(book["bids"][0]["priceTicks"], 5_000);
    app.stop().await;
}

#[tokio::test]
async fn configured_actions_are_announced_once() {
    let mut app = TestApp::start_with(|c| {
        monday(c);
        c.instruments = vec![declared("ACME")];
        c.corporate_actions = vec![ActionSpec {
            kind: ActionKind::Split,
            instrument_id: exchange_simulator::domain::InstrumentId("ACME".into()),
            ex_date: None,
            record_date: None,
            pay_date: None,
            amount_per_share: None,
            ratio: Some("3:1".into()),
            effective_date: Some("2030-01-10".parse().unwrap()),
        }];
    })
    .await;
    app.restart().await;
    let announced = events(&app, "CorporateActionAnnounced").await;
    assert_eq!(announced.len(), 1, "{announced:?}");
    assert_eq!(announced[0]["payload"]["ratio"], "3:1");
    app.stop().await;
}
