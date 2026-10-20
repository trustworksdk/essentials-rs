//! The Finnhub live source and the stale policy (step 6.9, SR-2, SR-9, D19), against a local mock of Finnhub's trades
//! websocket and REST quote endpoint, never the real network: seeding over REST, following trades, conflation,
//! staleness and recovery without a restart, the provider's stale policy, re-subscribing, limits, and the key kept out
//! of every answer.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use common::TestApp;
use exchange_simulator::app::App;
use exchange_simulator::config::{Config, FinnhubConfig, InstrumentConfig};
use exchange_simulator::liquidity::{LiquidityMode, LiquiditySpec, StalePolicy};
use exchange_simulator::pricing::{PriceSourceKind, PriceSourceSpec};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{
    Request as WsRequest, Response as WsResponse,
};
use tower::ServiceExt;

const KEY: &str = "test-key-6f1c9a";

/// What the mock knows and saw.
#[derive(Default)]
struct MockState {
    up: bool,
    /// REST quotes: symbol → (price, epoch seconds).
    quotes: HashMap<String, (f64, i64)>,
    rest_calls: Vec<String>,
    /// Every token presented, in the websocket URL or the REST header.
    tokens: Vec<String>,
    /// `subscribe:AAPL`, `unsubscribe:AAPL`, in order.
    subscriptions: Vec<String>,
    clients: Vec<mpsc::UnboundedSender<Message>>,
}

/// A local Finnhub: a trades websocket and a REST quote endpoint, which can be taken down and brought back.
#[derive(Clone)]
struct Mock {
    ws: SocketAddr,
    rest: SocketAddr,
    state: Arc<Mutex<MockState>>,
}

impl Mock {
    async fn start() -> Self {
        let state = Arc::new(Mutex::new(MockState {
            up: true,
            ..Default::default()
        }));
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rest_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mock = Self {
            ws: ws_listener.local_addr().unwrap(),
            rest: rest_listener.local_addr().unwrap(),
            state: state.clone(),
        };
        let ws_state = state.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = ws_listener.accept().await {
                if !ws_state.lock().unwrap().up {
                    continue; // Refused: dropped before the handshake.
                }
                let state = ws_state.clone();
                tokio::spawn(async move {
                    let token_state = state.clone();
                    // The error type is tungstenite's `Callback` contract, not ours to shrink.
                    #[allow(clippy::result_large_err)]
                    let callback = move |request: &WsRequest, response: WsResponse| {
                        let query = request.uri().query().unwrap_or_default();
                        let token = query.strip_prefix("token=").unwrap_or_default();
                        token_state.lock().unwrap().tokens.push(token.to_string());
                        if token == KEY {
                            Ok(response)
                        } else {
                            Err(axum::http::Response::builder()
                                .status(StatusCode::UNAUTHORIZED)
                                .body(None)
                                .unwrap())
                        }
                    };
                    let Ok(socket) = tokio_tungstenite::accept_hdr_async(stream, callback).await
                    else {
                        return;
                    };
                    let (mut sink, mut source) = socket.split();
                    let (tx, mut rx) = mpsc::unbounded_channel();
                    state.lock().unwrap().clients.push(tx);
                    loop {
                        tokio::select! {
                            incoming = source.next() => match incoming {
                                Some(Ok(Message::Text(text))) => {
                                    let message: Value = serde_json::from_str(&text).unwrap();
                                    state.lock().unwrap().subscriptions.push(format!(
                                        "{}:{}",
                                        message["type"].as_str().unwrap(),
                                        message["symbol"].as_str().unwrap()
                                    ));
                                }
                                Some(Ok(_)) => {}
                                _ => return,
                            },
                            outgoing = rx.recv() => match outgoing {
                                Some(message) => { let _ = sink.send(message).await; }
                                None => { let _ = sink.close().await; return; }
                            },
                        }
                    }
                });
            }
        });
        let app = Router::new().route("/quote", get(quote)).with_state(state);
        tokio::spawn(async move { axum::serve(rest_listener, app).await.unwrap() });
        mock
    }

    fn config(&self, dir: &std::path::Path) -> FinnhubConfig {
        let key_file = dir.join("finnhub.key");
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(&key_file, format!("{KEY}\n")).unwrap();
        FinnhubConfig {
            api_key_file: Some(key_file.display().to_string()),
            ws_url: format!("ws://{}", self.ws),
            rest_url: format!("http://{}", self.rest),
            stale_after_ms: 800,
            refresh_after_ms: 400,
            max_updates_per_second: 1000,
            reconnect_initial_ms: 50,
            reconnect_max_ms: 200,
            request_timeout_ms: 1000,
            ..Default::default()
        }
    }

    fn quote(&self, symbol: &str, price: f64, at: i64) {
        self.state
            .lock()
            .unwrap()
            .quotes
            .insert(symbol.into(), (price, at));
    }

    /// Sends a trade to every connected client.
    fn trade(&self, symbol: &str, price: f64, at_ms: i64) {
        let message =
            json!({ "type": "trade", "data": [{ "s": symbol, "p": price, "t": at_ms, "v": 10 }] });
        for client in &self.state.lock().unwrap().clients {
            let _ = client.send(Message::text(message.to_string()));
        }
    }

    /// Takes the mock down: open connections are closed, new ones refused, REST answers 503.
    fn kill(&self) {
        let mut state = self.state.lock().unwrap();
        state.up = false;
        state.clients.clear();
    }

    fn revive(&self) {
        self.state.lock().unwrap().up = true;
    }

    fn subscriptions(&self) -> Vec<String> {
        self.state.lock().unwrap().subscriptions.clone()
    }

    /// Waits until the mock has seen `entry` (`subscribe:MSFT`), for up to five seconds.
    async fn until_seen(&self, entry: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !self.subscriptions().iter().any(|s| s == entry) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "never saw {entry}: {:?}",
                self.subscriptions()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn rest_calls(&self) -> Vec<String> {
        self.state.lock().unwrap().rest_calls.clone()
    }
}

async fn quote(
    State(state): State<Arc<Mutex<MockState>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> axum::response::Response {
    let mut state = state.lock().unwrap();
    if !state.up {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let token = headers
        .get("x-finnhub-token")
        .map(|t| t.to_str().unwrap().to_string())
        .unwrap_or_default();
    state.tokens.push(token.clone());
    if token != KEY {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let symbol = query["symbol"].clone();
    state.rest_calls.push(symbol.clone());
    let (c, t) = state.quotes.get(&symbol).copied().unwrap_or((0.0, 0));
    Json(json!({ "c": c, "d": 0, "dp": 0, "h": c, "l": c, "o": c, "pc": c, "t": t }))
        .into_response()
}

fn live(id: &str, symbol: &str, liquidity: Option<LiquiditySpec>) -> InstrumentConfig {
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
        reference_symbol: Some(symbol.into()),
        price_source: Some(PriceSourceSpec {
            kind: Some(PriceSourceKind::Finnhub),
            ..Default::default()
        }),
        liquidity,
    }
}

fn provider(policy: StalePolicy) -> LiquiditySpec {
    LiquiditySpec {
        mode: Some(LiquidityMode::On),
        levels: Some(2),
        spread_bps: Some(10.0),
        level_quantity: Some(10),
        stale_policy: Some(policy),
        stale_widen_factor: Some(3.0),
        ..Default::default()
    }
}

async fn start(mock: &Mock, adjust: impl FnOnce(&mut Config)) -> TestApp {
    let mock = mock.clone();
    TestApp::start_with(move |c| {
        let dir = std::path::PathBuf::from(&c.database.path)
            .parent()
            .unwrap()
            .to_path_buf();
        c.price_sources.finnhub = mock.config(&dir);
        adjust(c);
    })
    .await
}

async fn snapshot(app: &TestApp, id: &str) -> Value {
    let (status, body) = app.get(&format!("/api/marketdata/{id}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

/// Polls `id`'s snapshot until `done` holds, for up to five seconds.
async fn until(app: &TestApp, id: &str, why: &str, done: impl Fn(&Value) -> bool) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let md = snapshot(app, id).await;
        if done(&md) {
            return md;
        }
        assert!(tokio::time::Instant::now() < deadline, "{why}: {md}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn price(md: &Value) -> &str {
    md["reference"]["price"].as_str().unwrap_or("")
}

/// `(price, quantity)` per level of the provider's side of the book.
async fn side(app: &TestApp, id: &str, side: &str) -> Vec<(i64, u64)> {
    let (_, book) = app.get(&format!("/api/orderbooks/{id}")).await;
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

#[tokio::test]
async fn prices_are_seeded_over_rest_then_follow_the_trades_websocket() {
    let mock = Mock::start().await;
    mock.quote("AAPL", 187.5, 1_900_000_000);
    let mut app = start(&mock, |c| {
        c.price_sources.finnhub.refresh_after_ms = 10_000;
        c.price_sources.finnhub.stale_after_ms = 20_000;
        c.instruments = vec![live("APPLE", "AAPL", None)];
    })
    .await;

    let md = until(&app, "APPLE", "seeded over REST", |md| {
        price(md) == "187.50"
    })
    .await;
    assert_eq!(md["reference"]["source"], "finnhub");
    assert_eq!(
        md["reference"]["sourceTimestamp"], "2030-03-17T17:46:40Z",
        "the provider's time"
    );
    assert_eq!(md["reference"]["stale"], false);
    assert_eq!(
        mock.subscriptions(),
        ["subscribe:AAPL"],
        "the reference symbol, not the exchange symbol"
    );

    mock.trade("AAPL", 188.254, 1_900_000_100_500);
    let md = until(&app, "APPLE", "a websocket trade", |md| {
        price(md) == "188.25"
    })
    .await;
    assert_eq!(md["reference"]["priceTicks"], 18825, "rounded to the tick");
    assert_eq!(
        md["reference"]["sourceTimestamp"],
        "2030-03-17T17:48:20.500Z"
    );
    {
        let state = mock.state.lock().unwrap();
        assert!(
            !state.tokens.is_empty() && state.tokens.iter().all(|t| t == KEY),
            "{:?}",
            state.tokens
        );
    }
    app.stop().await;
}

#[tokio::test]
async fn a_quiet_symbol_is_refreshed_over_rest_and_stays_fresh() {
    let mock = Mock::start().await;
    mock.quote("AAPL", 100.0, 1_900_000_000);
    let mut app = start(&mock, |c| c.instruments = vec![live("APPLE", "AAPL", None)]).await;
    until(&app, "APPLE", "seeded", |md| price(md) == "100.00").await;
    // A closed market: no trades on the websocket, only the REST quote moves.
    mock.quote("AAPL", 100.5, 1_900_000_060);
    let md = until(&app, "APPLE", "refreshed over REST", |md| {
        price(md) == "100.50"
    })
    .await;
    assert_eq!(md["reference"]["sourceTimestamp"], "2030-03-17T17:47:40Z");
    tokio::time::sleep(Duration::from_millis(1_000)).await; // Longer than stale_after_ms (800).
    assert_eq!(
        snapshot(&app, "APPLE").await["reference"]["stale"],
        false,
        "refreshes keep it fresh"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_feed_that_goes_away_marks_prices_stale_and_recovers_without_a_restart() {
    let mock = Mock::start().await;
    mock.quote("AAPL", 100.0, 1_900_000_000);
    let mut app = start(&mock, |c| {
        c.instruments = vec![live("STALE", "AAPL", Some(provider(StalePolicy::Pull)))];
    })
    .await;
    let md = until(&app, "STALE", "quoting", |md| md["liquidity"] == "quoting").await;
    assert_eq!(md["bestAsk"]["priceTicks"], 10_010);

    mock.kill();
    let md = until(&app, "STALE", "stale once the feed is gone", |md| {
        md["reference"]["stale"] == true
    })
    .await;
    assert_eq!(price(&md), "100.00", "the last price is kept, marked stale");
    until_metric(&app, r#"exchange_reference_stale{instrument="STALE"} 1"#).await;
    let md = until(&app, "STALE", "the ladder pulled", |md| {
        md["liquidity"] == "pulled" && md["bestAsk"].is_null()
    })
    .await;
    assert!(md["bestBid"].is_null());

    mock.quote("AAPL", 101.0, 1_900_000_600);
    mock.revive();
    let md = until(&app, "STALE", "fresh again after a reconnect", |md| {
        md["reference"]["stale"] == false && price(md) == "101.00" && md["liquidity"] == "quoting"
    })
    .await;
    assert_eq!(
        md["bestAsk"]["priceTicks"], 10_111,
        "quoted around the new price"
    );
    until_metric(&app, r#"exchange_reference_stale{instrument="STALE"} 0"#).await;
    // 100.00, then 101.00.
    until_metric(
        &app,
        r#"exchange_reference_updates_total{instrument="STALE"} 2"#,
    )
    .await;
    let subscribed = mock
        .subscriptions()
        .iter()
        .filter(|s| *s == "subscribe:AAPL")
        .count();
    assert!(
        subscribed >= 2,
        "subscribed again after the reconnect: {:?}",
        mock.subscriptions()
    );
    app.stop().await;
}

#[tokio::test]
async fn a_stale_price_keeps_or_widens_the_ladder_per_instrument() {
    let mock = Mock::start().await;
    mock.quote("KEEP", 100.0, 1_900_000_000);
    mock.quote("WIDE", 100.0, 1_900_000_000);
    let mut app = start(&mock, |c| {
        c.instruments = vec![
            live("KEEP", "KEEP", Some(provider(StalePolicy::Keep))),
            live("WIDE", "WIDE", Some(provider(StalePolicy::Widen))),
        ];
    })
    .await;
    until(&app, "KEEP", "quoting", |md| md["liquidity"] == "quoting").await;
    until(&app, "WIDE", "quoting", |md| md["liquidity"] == "quoting").await;
    let kept = (
        side(&app, "KEEP", "bids").await,
        side(&app, "KEEP", "asks").await,
    );
    assert_eq!(kept.1[0], (10_010, 10));

    mock.kill();
    until(&app, "KEEP", "kept", |md| md["liquidity"] == "kept").await;
    let md = until(&app, "WIDE", "widened", |md| {
        md["liquidity"] == "widened" && md["bestAsk"]["priceTicks"] == 10_030
    })
    .await;
    assert_eq!(md["bestBid"]["priceTicks"], 9_970, "the spread times three");
    assert_eq!(
        (
            side(&app, "KEEP", "bids").await,
            side(&app, "KEEP", "asks").await
        ),
        kept,
        "the kept ladder is untouched"
    );
    app.stop().await;
}

#[tokio::test]
async fn updates_are_conflated_to_max_updates_per_second() {
    let mock = Mock::start().await;
    mock.quote("AAPL", 100.0, 1_900_000_000);
    let mut app = start(&mock, |c| {
        c.price_sources.finnhub.max_updates_per_second = 1;
        c.price_sources.finnhub.refresh_after_ms = 10_000;
        c.price_sources.finnhub.stale_after_ms = 20_000;
        c.instruments = vec![live("APPLE", "AAPL", None)];
    })
    .await;
    until(&app, "APPLE", "seeded", |md| price(md) == "100.00").await;
    for (i, p) in [101.0, 102.0, 103.0].into_iter().enumerate() {
        mock.trade("AAPL", p, 1_900_000_001_000 + i as i64);
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        price(&snapshot(&app, "APPLE").await),
        "100.00",
        "within a second of the last update"
    );
    let md = until(&app, "APPLE", "the latest after the interval", |md| {
        price(md) != "100.00"
    })
    .await;
    assert_eq!(
        price(&md),
        "103.00",
        "the latest trade wins, the ones before it are skipped"
    );
    app.stop().await;
}

#[tokio::test]
async fn a_new_reference_symbol_resubscribes() {
    let mock = Mock::start().await;
    mock.quote("AAPL", 100.0, 1_900_000_000);
    mock.quote("MSFT", 400.0, 1_900_000_000);
    let mut app = start(&mock, |c| {
        // The quiet-symbol check runs every 15 s: a symbol added later must be seeded when it is subscribed.
        c.price_sources.finnhub.refresh_after_ms = 60_000;
        c.price_sources.finnhub.stale_after_ms = 120_000;
        c.instruments = vec![live("TECH", "AAPL", None)];
    })
    .await;
    until(&app, "TECH", "AAPL", |md| price(md) == "100.00").await;
    let (status, body) = app
        .call(
            Method::PATCH,
            "/api/instruments/TECH",
            Some(json!({ "referenceSymbol": "MSFT" })),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    until(&app, "TECH", "MSFT", |md| price(md) == "400.00").await;
    mock.until_seen("subscribe:MSFT").await;
    let subscriptions = mock.subscriptions();
    assert!(
        subscriptions.ends_with(&["unsubscribe:AAPL".to_string(), "subscribe:MSFT".to_string()]),
        "{subscriptions:?}"
    );
    app.stop().await;
}

#[tokio::test]
async fn rest_calls_stay_within_the_limit() {
    let mock = Mock::start().await;
    mock.quote("AAPL", 100.0, 1_900_000_000);
    mock.quote("MSFT", 400.0, 1_900_000_000);
    let mut app = start(&mock, |c| {
        c.price_sources.finnhub.rest_calls_per_minute = 1;
        c.instruments = vec![live("APPLE", "AAPL", None), live("SOFT", "MSFT", None)];
    })
    .await;
    until(&app, "APPLE", "the one call allowed", |md| {
        !md["reference"].is_null()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(600)).await; // Past refresh_after: more refreshes are due.
    assert_eq!(mock.rest_calls(), ["AAPL"], "one call a minute");
    assert!(
        snapshot(&app, "SOFT").await["reference"].is_null(),
        "no price yet"
    );
    let metrics = text(&app, "/metrics").await;
    assert!(
        metrics.contains(r#"exchange_price_feed_rest_calls_total{outcome="rate_limited"}"#),
        "counted"
    );
    app.stop().await;
}

/// Waits until `/metrics` has the line `series value`; the metrics follow the next publication, not the snapshot.
async fn until_metric(app: &TestApp, line: &str) {
    for _ in 0..100 {
        if text(app, "/metrics").await.lines().any(|l| l == line) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no {line} in\n{}", text(app, "/metrics").await);
}

async fn text(app: &TestApp, uri: &str) -> String {
    let response = app
        .router()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn the_key_appears_in_no_answer() {
    let mock = Mock::start().await;
    mock.quote("AAPL", 100.0, 1_900_000_000);
    let mut app = start(&mock, |c| {
        c.test_control.enabled = true;
        c.instruments = vec![live("APPLE", "AAPL", Some(provider(StalePolicy::Pull)))];
    })
    .await;
    until(&app, "APPLE", "quoting", |md| md["liquidity"] == "quoting").await;
    mock.kill(); // Failures too: their errors are logged, never answered.
    until(&app, "APPLE", "stale", |md| {
        md["reference"]["stale"] == true
    })
    .await;
    for uri in [
        "/api/instruments",
        "/api/instruments/APPLE",
        "/api/marketdata",
        "/api/events?limit=1000",
        "/api/orderbooks/APPLE",
        "/api/participants",
        "/metrics",
        "/health",
    ] {
        assert!(!text(&app, uri).await.contains(KEY), "{uri} shows the key");
    }
    app.stop().await;
}

#[tokio::test]
async fn missing_keys_and_exceeded_limits_are_refused() {
    let mock = Mock::start().await;
    let dir = std::env::temp_dir().join(format!("exchange-sim-finnhub-{}", uuid::Uuid::now_v7()));
    let mut config = TestApp::config(&dir);
    config.price_sources.finnhub = mock.config(&dir);

    // No key: an empty key file (the environment is not consulted when a file is named).
    let mut no_key = config.clone();
    std::fs::write(dir.join("empty.key"), "").unwrap();
    no_key.price_sources.finnhub.api_key_file = Some(dir.join("empty.key").display().to_string());
    no_key.instruments = vec![live("APPLE", "AAPL", None)];
    let error = format!("{:#}", App::start(&no_key).err().expect("refused"));
    assert!(error.contains("needs an API key"), "{error}");

    let mut too_many = config.clone();
    too_many.price_sources.finnhub.max_symbols = 1;
    too_many.instruments = vec![live("APPLE", "AAPL", None), live("SOFT", "MSFT", None)];
    let error = format!("{:#}", App::start(&too_many).err().expect("refused"));
    assert!(error.contains("at most 1 symbols"), "{error}");

    let mut contradictory = config.clone();
    contradictory.price_sources.finnhub.stale_after_ms = 100;
    let error = format!("{:#}", App::start(&contradictory).err().expect("refused"));
    assert!(error.contains("stale_after_ms must be longer"), "{error}");

    // At runtime, the same limit refuses the change. Two instruments may share a symbol.
    let mut app = start(&mock, |c| {
        c.price_sources.finnhub.max_symbols = 1;
        c.instruments = vec![live("APPLE", "AAPL", None), live("APPLE2", "AAPL", None)];
    })
    .await;
    mock.until_seen("subscribe:AAPL").await;
    let (status, body) = app
        .call(
            Method::PATCH,
            "/api/instruments/APPLE2",
            Some(json!({ "referenceSymbol": "MSFT" })),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(mock.subscriptions(), ["subscribe:AAPL"]);
    app.stop().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn the_finnhub_market_file_starts_and_subscribes_every_symbol() {
    #[derive(serde::Deserialize)]
    struct MarketFile {
        instruments: Vec<InstrumentConfig>,
    }
    let file: MarketFile =
        serde_saphyr::from_str(include_str!("../config/finnhub-market.yaml")).unwrap();
    let count = file.instruments.len();
    let mock = Mock::start().await;
    let mut app = start(&mock, |c| {
        c.price_sources.default.kind = Some(PriceSourceKind::Finnhub);
        c.instruments = file.instruments;
    })
    .await;
    mock.until_seen("subscribe:BINANCE:BTCUSDT").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while mock.subscriptions().len() < count {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{:?}",
            mock.subscriptions()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(mock.subscriptions().contains(&"subscribe:AAPL".to_string()));
    let (_, listed) = app.get("/api/instruments").await;
    assert_eq!(listed.as_array().unwrap().len(), count);
    app.stop().await;
}

#[tokio::test]
async fn a_recorded_session_replays_its_price_path() {
    let mock = Mock::start().await;
    mock.quote("AAPL", 100.0, 1_900_000_000);
    let recording = std::env::temp_dir()
        .join(format!("exchange-recording-{}", uuid::Uuid::now_v7()))
        .join("recordings")
        .join("session.csv");
    let file = recording.display().to_string();
    let mut app = start(&mock, move |c| {
        c.price_sources.finnhub.record_to = Some(file);
        c.instruments = vec![live("APPLE", "AAPL", None)];
    })
    .await;
    until(&app, "APPLE", "seeded", |md| price(md) == "100.00").await;
    for (i, p) in [100.5, 101.25, 100.75].into_iter().enumerate() {
        mock.trade("AAPL", p, 1_900_000_001_000 + i as i64 * 1_000);
        let expected = format!("{p:.2}");
        until(&app, "APPLE", "a recorded trade", |md| {
            price(md) == expected
        })
        .await;
    }
    app.stop().await;

    // The recording holds the adopted prices, in order, in the replay format.
    let replay = exchange_simulator::pricing::replay::Replay::load(&recording, None).unwrap();
    let text = std::fs::read_to_string(&recording).unwrap();
    let rows: Vec<(chrono::DateTime<chrono::Utc>, f64)> = text
        .lines()
        .skip(1)
        .map(|l| {
            let fields: Vec<&str> = l.split(',').collect();
            assert_eq!(fields[1], "AAPL", "{l}");
            (fields[0].parse().unwrap(), fields[2].parse().unwrap())
        })
        .collect();
    let prices: Vec<f64> = rows.iter().map(|(_, p)| *p).collect();
    assert_eq!(prices, [100.0, 100.5, 101.25, 100.75], "{text}");
    assert!(!text.contains(KEY));

    // Replayed, it reproduces the path at the recorded offsets.
    let first = rows[0].0;
    for (at, price) in &rows {
        let offset = (*at - first).num_milliseconds();
        assert_eq!(
            replay.price_at("AAPL", offset).map(|(p, _)| p),
            Some(*price)
        );
    }
    let _ = std::fs::remove_dir_all(recording.parent().unwrap().parent().unwrap());
}
