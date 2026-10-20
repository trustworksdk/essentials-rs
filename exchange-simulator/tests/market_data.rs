//! The market-data channel (step 6.8, SR-12, SR-13): the snapshot (reference price, best bid and ask, last trade,
//! session volume, `mdSequence`), and the server-sent event stream with its instrument filter, conflation, catch-up
//! and closing of subscribers that fall a whole buffer behind.

mod common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config, IdMode, InstrumentConfig};
use exchange_simulator::pricing::{PriceSourceKind, PriceSourceSpec};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

/// One server-sent event: kind, `id:` and the JSON body.
type Message = (String, u64, Value);

/// An open `GET /api/marketdata/stream`.
struct Feed {
    body: Body,
    buffer: String,
}

impl Feed {
    async fn open(app: &TestApp, query: &str) -> Self {
        let response = app
            .router()
            .oneshot(
                Request::get(format!("/api/marketdata/stream{query}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"].to_str().unwrap(),
            "text/event-stream"
        );
        Self {
            body: response.into_body(),
            buffer: String::new(),
        }
    }

    /// The next event, or `None` when nothing arrives within `wait`.
    async fn next_within(&mut self, wait: Duration) -> Option<Message> {
        loop {
            if let Some(end) = self.buffer.find("\n\n") {
                let block: String = self.buffer.drain(..end + 2).collect();
                let (mut kind, mut id, mut data) = (None, None, None);
                for line in block.lines() {
                    match line.split_once(':') {
                        Some(("event", v)) => kind = Some(v.trim().to_string()),
                        Some(("id", v)) => id = Some(v.trim().parse().unwrap()),
                        Some(("data", v)) => data = Some(serde_json::from_str(v.trim()).unwrap()),
                        _ => {} // Keep-alive comments.
                    }
                }
                if let (Some(kind), Some(id), Some(data)) = (kind, id, data) {
                    return Some((kind, id, data));
                }
                continue;
            }
            match tokio::time::timeout(wait, self.body.frame()).await {
                Err(_) | Ok(None) => return None,
                Ok(Some(frame)) => {
                    if let Ok(bytes) = frame.unwrap().into_data() {
                        self.buffer.push_str(std::str::from_utf8(&bytes).unwrap());
                    }
                }
            }
        }
    }

    async fn next(&mut self) -> Message {
        self.next_within(Duration::from_secs(5))
            .await
            .expect("a market-data message")
    }

    /// Whether the stream ended (rather than merely being quiet) within `wait`.
    async fn ended_within(&mut self, wait: Duration) -> bool {
        tokio::time::timeout(wait, async {
            loop {
                match self.body.frame().await {
                    None => return true,
                    Some(Err(_)) => return true,
                    Some(Ok(_)) => {}
                }
            }
        })
        .await
        .unwrap_or(false)
    }
}

async fn order(app: &TestApp, participant: &str, side: &str, price: i64, quantity: u64) -> Value {
    let (status, body) = app.order(participant, side, Some(price), quantity).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body
}

async fn snapshot(app: &TestApp, id: &str) -> Value {
    let (status, body) = app.get(&format!("/api/marketdata/{id}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

fn synthetic(id: &str) -> InstrumentConfig {
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
            kind: Some(PriceSourceKind::Synthetic),
            initial_price: Some("100.00".into()),
            volatility: Some(0.6),
            tick_interval_ms: Some(1000),
            ..Default::default()
        }),
        liquidity: None,
    }
}

/// Deterministic IDs, a frozen virtual clock on a Monday morning, test control, and SYN on a synthetic walk.
fn frozen(c: &mut Config) {
    c.ids.mode = IdMode::Deterministic;
    c.clock.mode = ClockMode::Virtual;
    c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
    c.test_control.enabled = true;
    c.instruments = vec![synthetic("SYN")];
}

#[tokio::test]
async fn the_stream_carries_quotes_and_prints_in_md_sequence_order() {
    // Without background ticks (which publish every instrument), so each message comes from the command before it.
    let mut app = TestApp::start_with(|c| {
        frozen(c);
        c.instruments.clear();
        c.settlement.tick_interval_ms = 3_600_000;
    })
    .await;
    app.seed().await;
    // Listing ACME published its empty quote and its missing reference price.
    assert_eq!(snapshot(&app, "ACME").await["mdSequence"], 2);
    let mut feed = Feed::open(&app, "").await;

    order(&app, "A", "BUY", 100, 10).await;
    assert_eq!(
        feed.next().await,
        (
            "quote".into(),
            3,
            json!({ "instrumentId": "ACME", "mdSequence": 3,
                    "bestBid": { "priceTicks": 100, "price": "1.00", "quantity": 10 }, "bestAsk": null })
        )
    );
    order(&app, "B", "SELL", 100, 4).await;
    let (kind, id, print) = feed.next().await;
    assert_eq!(
        (kind.as_str(), id),
        ("trade", 4),
        "the print before the quote"
    );
    assert_eq!(
        (&print["priceTicks"], &print["price"], &print["quantity"]),
        (&json!(100), &json!("1.00"), &json!(4))
    );
    assert!(
        print.get("buyerId").is_none() && print.get("sellerId").is_none(),
        "anonymous: {print}"
    );
    let (kind, id, quote) = feed.next().await;
    assert_eq!((kind.as_str(), id), ("quote", 5));
    assert_eq!(quote["bestBid"]["quantity"], 6);
    assert!(
        feed.next_within(Duration::from_millis(200)).await.is_none(),
        "nothing changed, nothing sent"
    );

    let md = snapshot(&app, "ACME").await;
    assert_eq!(md["mdSequence"], 5, "the last message about it");
    assert_eq!(
        md["bestBid"],
        json!({ "priceTicks": 100, "price": "1.00", "quantity": 6 })
    );
    assert_eq!(md["bestAsk"], Value::Null);
    assert_eq!(md["lastTrade"]["quantity"], 4);
    assert_eq!(md["sessionVolume"], 4);

    // A cancellation moves the quote too.
    let resting = order(&app, "C", "SELL", 105, 3).await;
    let (kind, _, quote) = feed.next().await;
    assert_eq!(
        (kind.as_str(), &quote["bestAsk"]["priceTicks"]),
        ("quote", &json!(105))
    );
    let order_id = resting["order"]["orderId"].as_str().unwrap();
    let (status, body) = app.delete(&format!("/api/orders/{order_id}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (kind, id, quote) = feed.next().await;
    assert_eq!(
        (kind.as_str(), id, &quote["bestAsk"]),
        ("quote", 7, &Value::Null)
    );
    app.stop().await;
}

#[tokio::test]
async fn reference_prices_stream_as_time_moves() {
    let mut app = TestApp::start_with(frozen).await;
    let start = snapshot(&app, "SYN").await;
    assert_eq!(start["reference"]["price"], "100.00");
    assert_eq!(start["reference"]["stale"], false);
    let mut feed = Feed::open(&app, "?instruments=SYN").await;
    let (status, _) = app
        .post("/test/clock/advance", json!({ "seconds": 30 }))
        .await;
    assert_eq!(status, StatusCode::OK);
    let (kind, id, body) = feed.next().await;
    assert_eq!(kind, "reference");
    assert_eq!(id, start["mdSequence"].as_u64().unwrap() + 1);
    assert_eq!(body["reference"]["source"], "synthetic");
    assert_eq!(body["reference"]["sourceTimestamp"], "2030-01-07T09:00:30Z");
    assert_eq!(snapshot(&app, "SYN").await["reference"], body["reference"]);
    app.stop().await;
}

#[tokio::test]
async fn a_subscriber_gets_only_the_instruments_it_asked_for() {
    let mut app = TestApp::start_with(frozen).await;
    app.seed().await;
    let mut feed = Feed::open(&app, "?instruments=ACME").await;
    let (status, _) = app
        .post("/test/clock/advance", json!({ "seconds": 30 }))
        .await;
    assert_eq!(status, StatusCode::OK, "SYN's reference price moves");
    order(&app, "A", "BUY", 100, 10).await;
    let (_, _, body) = feed.next().await;
    assert_eq!(
        body["instrumentId"], "ACME",
        "SYN's reference is filtered out"
    );

    for (query, expected) in [
        ("?instruments=ACME,NOPE", StatusCode::BAD_REQUEST),
        ("?colour=red", StatusCode::BAD_REQUEST),
    ] {
        let (status, _) = app.get(&format!("/api/marketdata/stream{query}")).await;
        assert_eq!(status, expected, "{query}");
    }
    let (status, _) = app.get("/api/marketdata/NOPE").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    app.stop().await;
}

#[tokio::test]
async fn conflation_keeps_the_latest_message_per_instrument_and_kind() {
    let mut app = TestApp::start_with(|c| c.market_data.conflation_interval_ms = 300).await;
    app.seed().await;
    let mut feed = Feed::open(&app, "").await;
    for price in [100, 101, 102] {
        order(&app, "A", "BUY", price, 1).await;
    }
    let (kind, id, quote) = feed.next().await;
    assert_eq!(
        (kind.as_str(), id),
        ("quote", 5),
        "only the last of three quotes"
    );
    assert_eq!(quote["bestBid"]["priceTicks"], 102);
    assert!(feed.next_within(Duration::from_millis(500)).await.is_none());
    app.stop().await;
}

#[tokio::test]
async fn a_subscriber_half_a_buffer_behind_is_caught_up_and_a_whole_buffer_behind_is_closed() {
    let mut app = TestApp::start_with(|c| c.market_data.stream_buffer = 8).await;
    app.seed().await;
    let mut behind = Feed::open(&app, "").await;
    let mut closed = Feed::open(&app, "").await;
    // Six quotes, unread: more than half of the buffer.
    for price in 100..106 {
        order(&app, "A", "BUY", price, 1).await;
    }
    let (kind, id, quote) = behind.next().await;
    assert_eq!(
        (kind.as_str(), id),
        ("quote", 8),
        "the backlog collapsed to its latest quote"
    );
    assert_eq!(quote["bestBid"]["priceTicks"], 105);
    assert!(
        behind
            .next_within(Duration::from_millis(200))
            .await
            .is_none()
    );

    // Four more, still unread by the second subscriber: ten in all, more than the buffer.
    for price in 106..110 {
        order(&app, "A", "BUY", price, 1).await;
    }
    assert!(
        closed.ended_within(Duration::from_secs(5)).await,
        "closed, to resynchronise from the snapshot"
    );
    // The caught-up subscriber reads on.
    let (_, id, _) = behind.next().await;
    assert_eq!(id, 9);
    app.stop().await;
}

#[tokio::test]
async fn the_last_trade_and_session_volume_survive_a_restart_and_reset_on_a_new_business_day() {
    let mut app = TestApp::start_with(frozen).await;
    app.seed().await;
    order(&app, "A", "BUY", 100, 10).await;
    order(&app, "B", "SELL", 100, 4).await;
    order(&app, "B", "SELL", 99, 2).await;
    let before = snapshot(&app, "ACME").await;
    assert_eq!(before["sessionVolume"], 6);
    assert_eq!(before["lastTrade"]["quantity"], 2);

    app.restart().await;
    let after = snapshot(&app, "ACME").await;
    assert_eq!(after["lastTrade"], before["lastTrade"]);
    assert_eq!(after["sessionVolume"], 6);
    assert_eq!(after["bestBid"], before["bestBid"], "the book is recovered");
    assert_eq!(
        after["mdSequence"], 2,
        "the sequence starts again with the service"
    );

    // Past Monday's 17:30 cutoff: Tuesday's session has traded nothing yet.
    let (status, _) = app
        .post("/test/clock/advance", json!({ "seconds": 9 * 3600 }))
        .await;
    assert_eq!(status, StatusCode::OK);
    let tuesday = snapshot(&app, "ACME").await;
    assert_eq!(tuesday["sessionVolume"], 0);
    assert_eq!(tuesday["lastTrade"], before["lastTrade"]);
    order(&app, "B", "SELL", 100, 1).await;
    assert_eq!(snapshot(&app, "ACME").await["sessionVolume"], 1);

    // A restart on Tuesday counts Tuesday's trades only.
    app.restart().await;
    assert_eq!(snapshot(&app, "ACME").await["sessionVolume"], 1);
    app.stop().await;
}

#[tokio::test]
async fn market_data_is_not_an_event() {
    let mut app = TestApp::start().await;
    app.seed().await;
    let (_, before) = app.get("/api/events?limit=1000").await;
    let mut feed = Feed::open(&app, "").await;
    order(&app, "A", "BUY", 100, 10).await;
    feed.next().await;
    let (_, after) = app.get("/api/events?limit=1000").await;
    let new: Vec<&str> = after.as_array().unwrap()[before.as_array().unwrap().len()..]
        .iter()
        .map(|e| e["eventType"].as_str().unwrap())
        .collect();
    assert_eq!(new, ["OrderAccepted"]);
    app.stop().await;
}

#[tokio::test]
async fn the_snapshot_publishes_what_moved_since_the_last_tick_first() {
    let mut app = TestApp::start_with(|c| {
        frozen(c);
        c.clock.running = true;
        c.clock.speed = 1000.0;
        // No tick during the test: only the snapshot can notice that the walk moved.
        c.settlement.tick_interval_ms = 3_600_000;
    })
    .await;
    let mut feed = Feed::open(&app, "?instruments=SYN").await;
    tokio::time::sleep(Duration::from_millis(50)).await; // 50 simulated seconds.
    let md = snapshot(&app, "SYN").await;
    let (kind, id, body) = feed.next().await;
    assert_eq!(
        (kind.as_str(), id),
        ("reference", md["mdSequence"].as_u64().unwrap()),
        "the snapshot's mdSequence is the message carrying its reference price"
    );
    assert_eq!(body["reference"], md["reference"]);
    app.stop().await;
}

/// The value of one `/metrics` series, whatever the order of its labels; `None` when it is not there.
async fn metric(app: &TestApp, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let response = app
        .router()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    text.lines().find_map(|line| {
        let (series, value) = line.rsplit_once(' ')?;
        let (series_name, rest) = series.split_once('{').unwrap_or((series, "}"));
        let found: Vec<&str> = rest
            .trim_end_matches('}')
            .split(',')
            .filter(|l| !l.is_empty())
            .collect();
        let wanted: Vec<String> = labels.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
        (series_name == name
            && found.len() == wanted.len()
            && wanted.iter().all(|w| found.contains(&w.as_str())))
        .then(|| value.parse().unwrap())
    })
}

/// Lists an instrument like ACME under another ID, so its per-instrument metrics belong to one test alone.
async fn list(app: &TestApp, id: &str) {
    let mut instrument = common::acme();
    instrument["id"] = json!(id);
    instrument["symbol"] = json!(id);
    let (status, body) = app.post("/api/instruments", instrument).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

async fn order_on(app: &TestApp, id: &str, price: i64) {
    let (status, body) = app
        .post(
            "/api/orders",
            json!({ "instrumentId": id, "participantId": "A", "side": "BUY", "type": "LIMIT",
                    "priceTicks": price, "quantity": 1 }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

#[tokio::test]
async fn subscribers_and_messages_sent_and_conflated_are_counted_per_instrument() {
    use exchange_simulator::metrics::{
        MARKET_DATA_CONFLATED, MARKET_DATA_MESSAGES, MARKET_DATA_SENT, MARKET_DATA_SUBSCRIBERS,
    };
    let mut app = TestApp::start_with(|c| c.market_data.conflation_interval_ms = 300).await;
    app.seed().await;
    list(&app, "MDM1").await;
    let mdm1 = [("instrument", "MDM1")];
    let quotes = [("instrument", "MDM1"), ("kind", "quote")];
    let feed = Feed::open(&app, "?instruments=MDM1").await;
    let mut both = Feed::open(&app, "?instruments=MDM1,ACME").await;
    assert_eq!(
        metric(&app, MARKET_DATA_SUBSCRIBERS, &mdm1).await,
        Some(2.0)
    );
    assert_eq!(
        metric(&app, MARKET_DATA_SUBSCRIBERS, &[("instrument", "ACME")]).await,
        Some(1.0),
        "a stream for two instruments counts under each"
    );

    for price in [100, 101, 102] {
        order_on(&app, "MDM1", price).await;
    }
    let (kind, _, quote) = both.next().await;
    assert_eq!(
        (kind.as_str(), &quote["bestBid"]["priceTicks"]),
        ("quote", &json!(102))
    );
    assert_eq!(
        metric(&app, MARKET_DATA_MESSAGES, &quotes).await,
        Some(4.0),
        "the empty quote at listing, then three"
    );
    assert_eq!(
        metric(&app, MARKET_DATA_SENT, &quotes).await,
        Some(1.0),
        "only the subscriber that read was handed one"
    );
    assert_eq!(
        metric(&app, MARKET_DATA_CONFLATED, &quotes).await,
        Some(2.0),
        "two skipped by the subscriber that read; the other has read nothing yet, so skipped nothing"
    );

    drop(feed);
    assert_eq!(
        metric(&app, MARKET_DATA_SUBSCRIBERS, &mdm1).await,
        Some(1.0),
        "a closed stream no longer counts"
    );
    drop(both);
    assert_eq!(
        metric(&app, MARKET_DATA_SUBSCRIBERS, &mdm1).await,
        Some(0.0)
    );
    app.stop().await;
}

#[tokio::test]
async fn a_stream_closed_for_falling_behind_is_counted_under_its_instruments() {
    use exchange_simulator::metrics::{MARKET_DATA_STREAMS_CLOSED, MARKET_DATA_SUBSCRIBERS};
    let mut app = TestApp::start_with(|c| c.market_data.stream_buffer = 8).await;
    app.seed().await;
    list(&app, "MDM2").await;
    let mdm2 = [("instrument", "MDM2")];
    let mut closed = Feed::open(&app, "?instruments=MDM2").await;
    for price in 100..110 {
        order_on(&app, "MDM2", price).await;
    }
    assert!(closed.ended_within(Duration::from_secs(5)).await);
    assert_eq!(
        metric(&app, MARKET_DATA_STREAMS_CLOSED, &mdm2).await,
        Some(1.0)
    );
    assert_eq!(
        metric(&app, MARKET_DATA_SUBSCRIBERS, &mdm2).await,
        Some(0.0)
    );
    app.stop().await;
}

#[tokio::test]
async fn reference_updates_staleness_and_age_are_counted_per_instrument() {
    use exchange_simulator::metrics::{REFERENCE_AGE, REFERENCE_STALE, REFERENCE_UPDATES};
    let mut app = TestApp::start_with(|c| {
        frozen(c);
        c.instruments = vec![synthetic("MDWALK"), synthetic("MDFIX")];
        c.instruments[1].price_source = Some(PriceSourceSpec {
            kind: Some(PriceSourceKind::Fixed),
            price: Some("50.00".into()),
            ..Default::default()
        });
    })
    .await;
    let walk = [("instrument", "MDWALK")];
    let fixed = [("instrument", "MDFIX")];
    assert_eq!(
        metric(&app, REFERENCE_UPDATES, &walk).await,
        Some(1.0),
        "the first price"
    );
    let (status, _) = app
        .post("/test/clock/advance", json!({ "seconds": 30 }))
        .await;
    assert_eq!(status, StatusCode::OK);
    let moves = metric(&app, REFERENCE_UPDATES, &walk).await.unwrap();
    assert!(moves > 1.0, "the walk moved: {moves}");
    assert_eq!(
        metric(&app, REFERENCE_UPDATES, &fixed).await,
        Some(1.0),
        "a fixed price is one update however often it is published"
    );
    assert_eq!(metric(&app, REFERENCE_STALE, &fixed).await, Some(0.0));
    assert_eq!(
        metric(&app, REFERENCE_AGE, &fixed).await,
        Some(30.0),
        "set at the start, read 30 simulated seconds later"
    );
    assert_eq!(
        metric(&app, REFERENCE_AGE, &walk).await,
        Some(0.0),
        "a step on the second"
    );
    app.stop().await;
}
