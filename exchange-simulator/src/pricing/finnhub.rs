//! The Finnhub live source (plan item 6.9, SR-2): real trade prices for instruments whose price source is `finnhub`.
//!
//! - **Transport:** the trades websocket (`{ws_url}?token=`), one subscription per symbol (an instrument's
//!   `referenceSymbol`, else its `symbol`). On connect, and whenever a symbol has had no update for
//!   `refresh_after_ms`, its price is seeded or refreshed from the REST quote endpoint (`{rest_url}/quote?symbol=`),
//!   which also covers a closed market, when no trades stream.
//! - **Resilience:** the connection is retried with exponential backoff and jitter, and every symbol is subscribed
//!   again after a reconnect. The feed runs on its own task: a source that is down never stops the exchange, which
//!   keeps reading the last price and marks it stale.
//! - **The key** is read once, held in [`ApiKey`] (whose `Debug` is redacted), sent only in the websocket URL and the
//!   `X-Finnhub-Token` header, and scrubbed from every error before it is logged.
//! - **Limits:** at most `rest_calls_per_minute` REST calls; a refresh past the limit is skipped, logged and counted.
//!   The symbol limit is checked where a source is configured ([`super::PriceSources`]).
//!
//! The exchange thread never waits on this task: they share the latest price per symbol behind a mutex
//! ([`LiveFeed`]), and the exchange decides when to adopt it (conflation) and whether it is stale.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{Notify, watch};
use tokio_tungstenite::tungstenite::Message;

use crate::metrics;

/// The latest price received for a symbol.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LivePrice {
    /// In major currency units, as the provider sends it.
    pub price: f64,
    /// The provider's own time for the price.
    pub source_timestamp: DateTime<Utc>,
    /// When it arrived (real time): the measure of staleness.
    pub received: Instant,
}

/// What the feed task and the exchange share: the symbols wanted, and the latest price per symbol.
#[derive(Clone, Debug, Default)]
pub struct LiveFeed {
    shared: Arc<Shared>,
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<FeedState>,
    /// Woken when the wanted symbols change.
    changed: Notify,
}

#[derive(Debug, Default)]
struct FeedState {
    wanted: BTreeSet<String>,
    prices: HashMap<String, LivePrice>,
}

impl LiveFeed {
    fn state(&self) -> std::sync::MutexGuard<'_, FeedState> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Replaces the symbols to stream; the feed task subscribes and unsubscribes the difference.
    pub fn set_wanted(&self, symbols: BTreeSet<String>) {
        let mut state = self.state();
        if state.wanted != symbols {
            state.prices.retain(|symbol, _| symbols.contains(symbol));
            state.wanted = symbols;
            drop(state);
            self.shared.changed.notify_one();
        }
    }

    pub fn wanted(&self) -> BTreeSet<String> {
        self.state().wanted.clone()
    }

    pub fn latest(&self, symbol: &str) -> Option<LivePrice> {
        self.state().prices.get(symbol).copied()
    }

    /// Records a price for a wanted symbol (others are dropped: an unsubscribe may still be in flight).
    pub fn record(&self, symbol: &str, price: f64, source_timestamp: DateTime<Utc>) {
        if !price.is_finite() || price <= 0.0 {
            return;
        }
        let mut state = self.state();
        if state.wanted.contains(symbol) {
            state.prices.insert(
                symbol.to_string(),
                LivePrice {
                    price,
                    source_timestamp,
                    received: Instant::now(),
                },
            );
        }
    }
}

/// The API key. Never printed: `Debug` shows `ApiKey(***)`.
#[derive(Clone)]
pub struct ApiKey(String);

impl ApiKey {
    pub fn new(key: String) -> Option<Self> {
        let key = key.trim().to_string();
        (!key.is_empty()).then_some(Self(key))
    }

    /// `text` with every occurrence of the key replaced, for logging errors that may quote a URL.
    pub fn redact(&self, text: &str) -> String {
        text.replace(&self.0, "***")
    }
}

impl std::fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApiKey(***)")
    }
}

/// How the feed task connects and paces itself (`price_sources.finnhub` in the configuration).
#[derive(Clone, Debug)]
pub struct FeedSettings {
    pub ws_url: String,
    pub rest_url: String,
    pub refresh_after: Duration,
    pub rest_calls_per_minute: u32,
    pub request_timeout: Duration,
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
}

/// Runs the feed until `stop` turns true: connect, stream, and reconnect with backoff whenever the connection ends.
pub async fn run(
    feed: LiveFeed,
    settings: FeedSettings,
    key: ApiKey,
    mut stop: watch::Receiver<bool>,
) {
    let mut rest = Rest::new(&settings, key.clone());
    let mut backoff = settings.reconnect_initial;
    loop {
        let started = Instant::now();
        let ended = tokio::select! {
            ended = session(&feed, &settings, &key, &mut rest) => ended,
            _ = stop.changed() => return,
        };
        ::metrics::gauge!(metrics::PRICE_FEED_CONNECTED).set(0.0);
        match ended {
            Ok(()) => tracing::warn!("price feed connection closed; reconnecting"),
            Err(error) => {
                tracing::warn!(error = %key.redact(&error), "price feed connection failed; reconnecting")
            }
        }
        ::metrics::counter!(metrics::PRICE_FEED_RECONNECTS).increment(1);
        // A connection that lasted resets the backoff; repeated failures double it up to the maximum.
        if started.elapsed() > settings.reconnect_max {
            backoff = settings.reconnect_initial;
        }
        let jittered = backoff.mul_f64(0.5 + fastrand::f64() / 2.0);
        backoff = (backoff * 2).min(settings.reconnect_max);
        tokio::select! {
            () = tokio::time::sleep(jittered) => {}
            _ = stop.changed() => return,
        }
    }
}

/// One websocket connection, until it ends. `Ok` when the server closed it, `Err` (possibly quoting the URL, so
/// redact it) otherwise.
async fn session(
    feed: &LiveFeed,
    settings: &FeedSettings,
    key: &ApiKey,
    rest: &mut Rest,
) -> Result<(), String> {
    // Parsed, so a URL without a path (`wss://ws.finnhub.io`) still asks for `/`.
    let url = reqwest::Url::parse_with_params(&settings.ws_url, [("token", &key.0)])
        .map_err(|e| format!("price_sources.finnhub.ws_url: {e}"))?;
    let (socket, _) = tokio::time::timeout(
        settings.request_timeout,
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await
    .map_err(|_| "connect timed out".to_string())?
    .map_err(|e| e.to_string())?;
    tracing::info!("price feed connected");
    ::metrics::gauge!(metrics::PRICE_FEED_CONNECTED).set(1.0);
    let (mut sink, mut stream) = socket.split();
    let mut subscribed = BTreeSet::new();
    let mut check =
        tokio::time::interval((settings.refresh_after / 4).max(Duration::from_millis(50)));
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        // Subscriptions follow the wanted symbols; new ones (all of them after a reconnect) are seeded over REST.
        let wanted = feed.wanted();
        for symbol in subscribed.difference(&wanted) {
            send(&mut sink, "unsubscribe", symbol).await?;
        }
        let new: Vec<String> = wanted.difference(&subscribed).cloned().collect();
        for symbol in &new {
            send(&mut sink, "subscribe", symbol).await?;
        }
        subscribed = wanted;
        for symbol in &new {
            rest.refresh(feed, symbol).await;
        }
        tokio::select! {
            message = stream.next() => match message {
                None | Some(Ok(Message::Close(_))) => return Ok(()),
                Some(Err(error)) => return Err(error.to_string()),
                Some(Ok(Message::Text(text))) => receive(feed, &text),
                Some(Ok(_)) => {} // Pings are answered by the library.
            },
            () = feed.shared.changed.notified() => {}
            _ = check.tick() => {
                // No update for a while: a quiet symbol or a closed market. Ask REST.
                for symbol in &subscribed {
                    let quiet = feed
                        .latest(symbol)
                        .is_none_or(|p| p.received.elapsed() >= settings.refresh_after);
                    if quiet {
                        rest.refresh(feed, symbol).await;
                    }
                }
            }
        }
    }
}

async fn send<S>(sink: &mut S, kind: &str, symbol: &str) -> Result<(), String>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let message = serde_json::json!({ "type": kind, "symbol": symbol }).to_string();
    sink.send(Message::text(message))
        .await
        .map_err(|e| e.to_string())
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    data: Vec<Trade>,
    #[serde(default)]
    msg: Option<String>,
}

#[derive(Deserialize)]
struct Trade {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: f64,
    /// Milliseconds since the epoch.
    #[serde(rename = "t")]
    timestamp_ms: i64,
}

/// Handles one websocket text message: the latest trade per symbol of a batch becomes its price.
fn receive(feed: &LiveFeed, text: &str) {
    let Ok(envelope) = serde_json::from_str::<Envelope>(text) else {
        tracing::debug!("price feed sent a message it could not parse");
        return;
    };
    match envelope.kind.as_str() {
        "trade" => {
            let mut latest: BTreeMap<&str, &Trade> = BTreeMap::new();
            for trade in &envelope.data {
                let entry = latest.entry(&trade.symbol).or_insert(trade);
                if trade.timestamp_ms >= entry.timestamp_ms {
                    *entry = trade;
                }
            }
            for (symbol, trade) in latest {
                if let Some(at) = DateTime::from_timestamp_millis(trade.timestamp_ms) {
                    feed.record(symbol, trade.price, at);
                    ::metrics::counter!(metrics::PRICE_FEED_UPDATES, "via" => "websocket")
                        .increment(1);
                }
            }
        }
        "error" => tracing::warn!(
            message = envelope.msg.as_deref().unwrap_or(""),
            "price feed reported an error"
        ),
        _ => {} // `ping` and anything new.
    }
}

/// The REST quote endpoint, rate limited.
struct Rest {
    http: reqwest::Client,
    url: String,
    key: ApiKey,
    per_minute: usize,
    /// When the calls of the last minute were made.
    calls: VecDeque<Instant>,
}

#[derive(Deserialize)]
struct Quote {
    /// The current price; 0 for a symbol Finnhub does not know.
    c: f64,
    /// Seconds since the epoch.
    t: i64,
}

impl Rest {
    fn new(settings: &FeedSettings, key: ApiKey) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(settings.request_timeout)
                .build()
                .expect("a client with only a timeout builds"),
            url: format!("{}/quote", settings.rest_url.trim_end_matches('/')),
            key,
            per_minute: settings.rest_calls_per_minute as usize,
            calls: VecDeque::new(),
        }
    }

    /// Seeds or refreshes one symbol's price; failures are logged and counted, never fatal.
    async fn refresh(&mut self, feed: &LiveFeed, symbol: &str) {
        let now = Instant::now();
        while self
            .calls
            .front()
            .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(60))
        {
            self.calls.pop_front();
        }
        if self.calls.len() >= self.per_minute {
            ::metrics::counter!(metrics::PRICE_FEED_REST_CALLS, "outcome" => "rate_limited")
                .increment(1);
            tracing::debug!(symbol, "REST quote skipped: rest_calls_per_minute reached");
            return;
        }
        self.calls.push_back(now);
        let outcome = match self.quote(symbol).await {
            Ok(Some((price, at))) => {
                feed.record(symbol, price, at);
                ::metrics::counter!(metrics::PRICE_FEED_UPDATES, "via" => "rest").increment(1);
                "ok"
            }
            Ok(None) => {
                tracing::warn!(symbol, "Finnhub has no quote for the symbol");
                "no_quote"
            }
            Err((outcome, error)) => {
                tracing::warn!(symbol, error = %self.key.redact(&error), "REST quote failed");
                outcome
            }
        };
        ::metrics::counter!(metrics::PRICE_FEED_REST_CALLS, "outcome" => outcome).increment(1);
    }

    async fn quote(
        &self,
        symbol: &str,
    ) -> Result<Option<(f64, DateTime<Utc>)>, (&'static str, String)> {
        let url = reqwest::Url::parse_with_params(&self.url, [("symbol", symbol)])
            .map_err(|e| ("error", format!("price_sources.finnhub.rest_url: {e}")))?;
        let response = self
            .http
            .get(url)
            .header("X-Finnhub-Token", &self.key.0)
            .send()
            .await
            .map_err(|e| ("error", e.without_url().to_string()))?;
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(("throttled", "HTTP 429".into()));
        }
        if !status.is_success() {
            return Err(("error", format!("HTTP {status}")));
        }
        let quote: Quote = response
            .json()
            .await
            .map_err(|e| ("error", e.without_url().to_string()))?;
        if !quote.c.is_finite() || quote.c <= 0.0 {
            return Ok(None);
        }
        Ok(DateTime::from_timestamp(quote.t, 0).map(|at| (quote.c, at)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_never_prints() {
        let key = ApiKey::new(" secret-key \n".into()).unwrap();
        assert_eq!(format!("{key:?}"), "ApiKey(***)");
        assert_eq!(
            key.redact("GET wss://ws.example?token=secret-key failed"),
            "GET wss://ws.example?token=*** failed"
        );
        assert!(ApiKey::new("  ".into()).is_none());
    }

    #[test]
    fn a_batch_gives_each_symbol_its_latest_trade_and_ignores_unwanted_symbols() {
        let feed = LiveFeed::default();
        feed.set_wanted(["AAPL".to_string()].into());
        receive(
            &feed,
            r#"{"type":"trade","data":[{"s":"AAPL","p":187.5,"t":2000,"v":1},{"s":"AAPL","p":187.1,"t":1000,"v":1},
                {"s":"MSFT","p":400.0,"t":3000,"v":1}]}"#,
        );
        let price = feed.latest("AAPL").unwrap();
        assert_eq!(price.price, 187.5);
        assert_eq!(price.source_timestamp.timestamp_millis(), 2000);
        assert!(feed.latest("MSFT").is_none());
        receive(&feed, r#"{"type":"ping"}"#);
        receive(&feed, "not json");
        feed.set_wanted(BTreeSet::new());
        assert!(
            feed.latest("AAPL").is_none(),
            "an unwanted symbol's price is dropped"
        );
    }
}
