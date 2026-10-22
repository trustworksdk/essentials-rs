//! Trading sessions and time in force (step 6.10, SR-16, SR-17, SR-9): client orders refused outside the session,
//! `DAY` orders expired at the close (or the business day's end), `IOC` remainders cancelled, market orders always
//! `IOC`, and the liquidity provider's closed-market policy.

mod common;

use axum::http::{Method, StatusCode};
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, InstrumentConfig, SessionConfig};
use exchange_simulator::liquidity::{LiquidityMode, LiquiditySpec, StalePolicy};
use exchange_simulator::pricing::{PriceSourceKind, PriceSourceSpec};
use serde_json::{Value, json};

/// Monday 2030-01-07 09:00 New York (14:00 UTC): before the 09:30-16:00 session, which is 14:30-21:00 UTC in winter.
const BEFORE_OPEN: &str = "2030-01-07T14:00:00Z";
const OPEN: &str = "2030-01-07T14:30:00Z";
const MIDDAY: &str = "2030-01-07T17:00:00Z";
const CLOSE: &str = "2030-01-07T21:00:00Z";

fn instrument(id: &str, mic: Option<&str>, liquidity: Option<LiquiditySpec>) -> InstrumentConfig {
    InstrumentConfig {
        id: id.into(),
        symbol: id.into(),
        mic: mic.map(str::to_string),
        isin: None,
        cusip: None,
        currency: "USD".into(),
        currency_scale: 2,
        tick_size_minor: 1,
        lot_size: 1,
        enabled: true,
        reference_symbol: None,
        price_source: liquidity.as_ref().map(|_| PriceSourceSpec {
            kind: Some(PriceSourceKind::Fixed),
            price: Some("100.00".into()),
            ..Default::default()
        }),
        liquidity,
    }
}

/// A frozen virtual clock before Nasdaq opens; AAPL on XNAS with a session, FREE on a market without one.
fn nasdaq(adjust: impl FnOnce(&mut Config)) -> impl FnOnce(&mut Config) {
    move |c: &mut Config| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some(BEFORE_OPEN.parse().unwrap());
        c.test_control.enabled = true;
        c.sessions = [(
            "XNAS".to_string(),
            SessionConfig {
                open: "09:30".into(),
                close: "16:00".into(),
                time_zone: "America/New_York".into(),
            },
        )]
        .into();
        c.instruments = vec![
            instrument("AAPL", Some("XNAS"), None),
            instrument("FREE", Some("XCSE"), None),
        ];
        adjust(c);
    }
}

async fn participants(app: &TestApp) {
    for id in ["A", "B"] {
        let (status, _) = app
            .post("/api/participants", json!({ "id": id, "name": id }))
            .await;
        assert_eq!(status, StatusCode::CREATED);
    }
}

async fn set_clock(app: &TestApp, to: &str) {
    let (status, body) = app.post("/test/clock/set", json!({ "to": to })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

async fn order(app: &TestApp, body: Value) -> (StatusCode, Value) {
    app.post("/api/orders", body).await
}

fn limit(instrument: &str, participant: &str, side: &str, price: i64, quantity: u64) -> Value {
    json!({ "instrumentId": instrument, "participantId": participant, "side": side, "type": "LIMIT",
            "priceTicks": price, "quantity": quantity })
}

fn with(mut body: Value, field: &str, value: Value) -> Value {
    body[field] = value;
    body
}

async fn placed(app: &TestApp, body: Value) -> Value {
    let (status, result) = order(app, body).await;
    assert_eq!(status, StatusCode::CREATED, "{result}");
    result
}

async fn events(app: &TestApp) -> Vec<Value> {
    app.get("/api/events?limit=1000")
        .await
        .1
        .as_array()
        .unwrap()
        .clone()
}

async fn of_type(app: &TestApp, event_type: &str) -> Vec<Value> {
    events(app)
        .await
        .into_iter()
        .filter(|e| e["eventType"] == event_type)
        .collect()
}

#[tokio::test]
async fn client_orders_are_refused_outside_the_session_and_cancels_still_work() {
    let mut app = TestApp::start_with(nasdaq(|_| {})).await;
    participants(&app).await;

    let early = placed(&app, limit("AAPL", "A", "BUY", 10_000, 5)).await;
    assert_eq!(
        (&early["order"]["status"], &early["order"]["rejectReason"]),
        (&json!("REJECTED"), &json!("MARKET_CLOSED"))
    );
    assert_eq!(
        of_type(&app, "OrderRejected").await[0]["payload"]["rejectReason"],
        "MARKET_CLOSED"
    );
    let (_, md) = app.get("/api/marketdata/AAPL").await;
    assert_eq!(md["session"], "closed");
    let free = placed(&app, limit("FREE", "A", "BUY", 10_000, 5)).await;
    assert_eq!(
        free["order"]["status"], "OPEN",
        "a market without a session is always open"
    );
    let (_, md) = app.get("/api/marketdata/FREE").await;
    assert_eq!(md["session"], "open");

    set_clock(&app, OPEN).await;
    let (_, md) = app.get("/api/marketdata/AAPL").await;
    assert_eq!(md["session"], "open", "09:30 New York opens");
    let gtc = placed(
        &app,
        with(
            limit("AAPL", "A", "BUY", 10_000, 5),
            "timeInForce",
            json!("GTC"),
        ),
    )
    .await;
    assert_eq!(gtc["order"]["status"], "OPEN");

    set_clock(&app, CLOSE).await;
    let late = placed(&app, limit("AAPL", "B", "SELL", 10_000, 5)).await;
    assert_eq!(
        late["order"]["rejectReason"], "MARKET_CLOSED",
        "16:00 closes; nothing traded"
    );
    let id = gtc["order"]["orderId"].as_str().unwrap();
    let (status, cancelled) = app.delete(&format!("/api/orders/{id}")).await;
    assert_eq!(
        (status, &cancelled["order"]["status"]),
        (StatusCode::OK, &json!("CANCELLED"))
    );

    set_clock(&app, "2030-01-12T16:00:00Z").await;
    let saturday = placed(&app, limit("AAPL", "A", "BUY", 10_000, 5)).await;
    assert_eq!(
        saturday["order"]["rejectReason"], "MARKET_CLOSED",
        "no session on a Saturday"
    );
    app.stop().await;
}

#[tokio::test]
async fn day_orders_expire_at_the_close_keeping_their_remainder_and_gtc_orders_stay() {
    let mut app = TestApp::start_with(nasdaq(|_| {})).await;
    participants(&app).await;
    set_clock(&app, MIDDAY).await;

    let day = placed(&app, limit("AAPL", "A", "BUY", 10_000, 10)).await;
    assert_eq!(
        day["order"]["timeInForce"], "DAY",
        "the default on a market with a session"
    );
    placed(&app, limit("AAPL", "B", "SELL", 10_000, 4)).await; // Fills 4 of the DAY order.
    let gtc = placed(
        &app,
        with(
            limit("AAPL", "A", "BUY", 9_000, 10),
            "timeInForce",
            json!("GTC"),
        ),
    )
    .await;
    let free = placed(&app, limit("FREE", "A", "BUY", 10_000, 10)).await;
    assert_eq!(
        free["order"]["timeInForce"], "GTC",
        "the default on a market without a session"
    );

    // A restart before the close: the expiry is scheduled again from the stored order.
    app.restart().await;
    set_clock(&app, "2030-01-08T15:00:00Z").await;

    let expired = of_type(&app, "OrderExpired").await;
    assert_eq!(expired.len(), 1, "{expired:?}");
    let event = &expired[0];
    assert_eq!(
        event["occurredAt"], CLOSE,
        "stamped at the close, not when the clock move ran"
    );
    assert_eq!(event["aggregateId"], day["order"]["orderId"]);
    assert_eq!(
        (
            &event["payload"]["status"],
            &event["payload"]["remainingQuantity"],
            &event["payload"]["filledQuantity"]
        ),
        (&json!("EXPIRED"), &json!(6), &json!(4))
    );
    assert_eq!(
        event["payload"]["businessDate"], "2030-01-07",
        "the day whose session closed"
    );
    let id = day["order"]["orderId"].as_str().unwrap();
    let (_, stored) = app.get(&format!("/api/orders/{id}")).await;
    assert_eq!(stored["status"], "EXPIRED");
    let (_, book) = app.get("/api/orderbooks/AAPL").await;
    assert_eq!(
        book["bids"].as_array().unwrap().len(),
        1,
        "only the GTC order rests: {book}"
    );
    assert_eq!(book["bids"][0]["priceTicks"], gtc["order"]["priceTicks"]);
    let (status, _) = app.delete(&format!("/api/orders/{id}")).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "an expired order cannot be cancelled"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_day_order_on_a_market_without_a_session_ends_with_the_business_day() {
    let mut app = TestApp::start_with(nasdaq(|c| {
        c.clearing.cutoff = "17:30".into();
        c.clearing.time_zone = "UTC".into();
    }))
    .await;
    participants(&app).await;
    let day = placed(
        &app,
        with(
            limit("FREE", "A", "BUY", 10_000, 10),
            "timeInForce",
            json!("DAY"),
        ),
    )
    .await;
    set_clock(&app, "2030-01-07T18:00:00Z").await;
    let all = events(&app).await;
    let position = |event_type: &str| {
        all.iter()
            .position(|e| e["eventType"] == event_type)
            .unwrap()
    };
    let expired = &all[position("OrderExpired")];
    assert_eq!(expired["aggregateId"], day["order"]["orderId"]);
    assert_eq!(
        expired["occurredAt"], "2030-01-07T17:30:00Z",
        "the business day's cutoff"
    );
    assert!(
        position("OrderExpired") < position("BusinessDayClosed"),
        "the order ends with the day"
    );
    assert_eq!(expired["payload"]["businessDate"], "2030-01-07");

    app.stop().await;
}

/// SR-32: a `DAY` order resting at Tuesday's cutoff is expired with `businessDate` Tuesday, the date that cutoff's
/// `BusinessDayClosed` names. The cutoff, 17:30 in Los Angeles, falls on Wednesday in UTC.
#[tokio::test]
async fn an_expired_order_names_the_business_day_that_ended() {
    let mut app = TestApp::start_with(nasdaq(|c| {
        c.clearing.cutoff = "17:30".into();
        c.clearing.time_zone = "America/Los_Angeles".into();
    }))
    .await;
    participants(&app).await;
    set_clock(&app, "2030-01-08T18:00:00Z").await; // Tuesday 10:00 in Los Angeles.
    let tuesday = placed(
        &app,
        with(
            limit("FREE", "A", "BUY", 10_000, 10),
            "timeInForce",
            json!("DAY"),
        ),
    )
    .await;
    set_clock(&app, "2030-01-09T02:00:00Z").await;
    let all = events(&app).await;
    let expired = all
        .iter()
        .find(|e| {
            e["eventType"] == "OrderExpired" && e["aggregateId"] == tuesday["order"]["orderId"]
        })
        .expect("Tuesday's DAY order expired");
    assert_eq!(expired["occurredAt"], "2030-01-09T01:30:00Z");
    let closed = all
        .iter()
        .find(|e| e["eventType"] == "BusinessDayClosed" && e["occurredAt"] == expired["occurredAt"])
        .expect("the BusinessDayClosed at the same cutoff");
    assert_eq!(
        (
            &expired["payload"]["businessDate"],
            &closed["payload"]["businessDate"]
        ),
        (&json!("2030-01-08"), &json!("2030-01-08"))
    );
    app.stop().await;
}

#[tokio::test]
async fn an_ioc_remainder_is_cancelled_and_market_orders_are_always_ioc() {
    let mut app =
        TestApp::start_with(nasdaq(|c| c.clock.start = Some(MIDDAY.parse().unwrap()))).await;
    participants(&app).await;
    placed(&app, limit("AAPL", "A", "SELL", 10_000, 5)).await;
    let ioc = placed(
        &app,
        with(
            limit("AAPL", "B", "BUY", 10_000, 8),
            "timeInForce",
            json!("IOC"),
        ),
    )
    .await;
    assert_eq!(
        (
            &ioc["order"]["status"],
            &ioc["order"]["filledQuantity"],
            &ioc["order"]["remainingQuantity"]
        ),
        (&json!("CANCELLED"), &json!(5), &json!(3))
    );
    let cancelled = of_type(&app, "OrderCancelled").await;
    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0]["payload"]["cancelReason"], "IOC_REMAINDER");
    let (_, book) = app.get("/api/orderbooks/AAPL").await;
    assert_eq!(book["bids"], json!([]), "an IOC order never rests");

    let nothing = placed(
        &app,
        with(
            limit("AAPL", "B", "BUY", 9_000, 8),
            "timeInForce",
            json!("IOC"),
        ),
    )
    .await;
    assert_eq!(
        nothing["order"]["status"], "CANCELLED",
        "nothing to trade with: cancelled at once"
    );

    let market = json!({ "instrumentId": "AAPL", "participantId": "B", "side": "BUY", "type": "MARKET",
                         "quantity": 1 });
    let (status, result) = order(&app, market.clone()).await;
    assert_eq!(
        (status, &result["order"]["timeInForce"]),
        (StatusCode::CREATED, &json!("IOC"))
    );
    for refused in ["DAY", "GTC"] {
        let (status, body) = order(&app, with(market.clone(), "timeInForce", json!(refused))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}: {body}");
    }
    let (status, _) = order(
        &app,
        with(
            limit("AAPL", "B", "BUY", 9_000, 1),
            "timeInForce",
            json!("FOK"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "an unknown time in force");
    app.stop().await;
}

fn provider(policy: StalePolicy) -> LiquiditySpec {
    LiquiditySpec {
        mode: Some(LiquidityMode::On),
        levels: Some(1),
        spread_bps: Some(10.0),
        level_quantity: Some(10),
        closed_policy: Some(policy),
        closed_widen_factor: Some(3.0),
        ..Default::default()
    }
}

async fn best_ask(app: &TestApp, id: &str) -> Value {
    app.get(&format!("/api/marketdata/{id}")).await.1["bestAsk"]["priceTicks"].clone()
}

#[tokio::test]
async fn the_provider_applies_its_closed_market_policy_per_instrument() {
    let mut app = TestApp::start_with(nasdaq(|c| {
        c.instruments = vec![
            instrument("PULL", Some("XNAS"), Some(provider(StalePolicy::Pull))),
            instrument("WIDE", Some("XNAS"), Some(provider(StalePolicy::Widen))),
            instrument("KEEP", Some("XNAS"), Some(provider(StalePolicy::Keep))),
        ];
    }))
    .await;
    // Closed: pulled, widened (placed although the market is closed: the provider is not a client), never quoted.
    let (_, pull) = app.get("/api/marketdata/PULL").await;
    assert_eq!(
        (&pull["liquidity"], &pull["bestAsk"]),
        (&json!("pulled"), &Value::Null)
    );
    let (_, wide) = app.get("/api/marketdata/WIDE").await;
    assert_eq!(
        (&wide["liquidity"], &wide["bestAsk"]["priceTicks"]),
        (&json!("widened"), &json!(10_030))
    );
    assert_eq!(app.get("/api/marketdata/KEEP").await.1["liquidity"], "kept");

    set_clock(&app, OPEN).await;
    for id in ["PULL", "WIDE", "KEEP"] {
        let (_, md) = app.get(&format!("/api/marketdata/{id}")).await;
        assert_eq!(
            (&md["liquidity"], &md["bestAsk"]["priceTicks"]),
            (&json!("quoting"), &json!(10_010)),
            "{id}"
        );
    }

    set_clock(&app, CLOSE).await;
    assert_eq!(
        best_ask(&app, "PULL").await,
        Value::Null,
        "pulled at the close"
    );
    assert_eq!(best_ask(&app, "WIDE").await, json!(10_030));
    assert_eq!(
        best_ask(&app, "KEEP").await,
        json!(10_010),
        "the open ladder kept"
    );
    let (status, _) = app
        .call(
            Method::PATCH,
            "/api/instruments/KEEP",
            Some(json!({ "liquidity": { "mode": "on", "levels": 1, "spreadBps": 10.0, "levelQuantity": 10,
                                         "closedPolicy": "pull" } })),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, keep) = app.get("/api/marketdata/KEEP").await;
    assert_eq!(
        (&keep["liquidity"], &keep["bestAsk"]),
        (&json!("pulled"), &Value::Null),
        "a policy change applies at once"
    );
    app.stop().await;
}
