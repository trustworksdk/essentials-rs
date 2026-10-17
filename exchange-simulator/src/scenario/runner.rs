use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request};
use http_body_util::BodyExt;
use serde::Serialize;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use super::matcher::{contains, mismatches};
use super::{Expectation, ResponseCheck, Scenario, Step};
use crate::app::App;
use crate::config::{ClockConfig, ClockMode, Config, IdMode, IdsConfig};
use crate::market::{MarketAction, MarketConfig};

/// How long a `deliveries` or `attempts` expectation waits by default for webhook delivery, which runs on real time.
const DEFAULT_DELIVERY_WAIT_MS: u64 = 5000;
const DELIVERY_POLL: Duration = Duration::from_millis(20);
/// A market step reports at most this many failures; a broken market would otherwise fail every order.
const MAX_MARKET_FAILURES: usize = 10;

/// The outcome of a scenario run. Every step runs, even after a failure, so one run shows every problem.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    pub name: String,
    pub passed: bool,
    /// Index 0 is the setup (instruments, participants, webhooks); steps count from 1.
    pub steps: Vec<StepReport>,
    /// A fingerprint of the run's final event log: equal fingerprints mean byte-identical logs.
    pub event_log: EventLog,
}

/// The number of events and the SHA-256 of `/api/events` as JSON lines, one event per line, in sequence order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventLog {
    pub events: usize,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StepReport {
    pub index: usize,
    pub step: String,
    pub passed: bool,
    pub failures: Vec<String>,
}

impl Report {
    /// A human-readable rendering: one line per step, failures indented below.
    pub fn render(&self) -> String {
        let verdict = if self.passed { "PASSED" } else { "FAILED" };
        let mut out = format!("Scenario \"{}\": {verdict}\n", self.name);
        for step in &self.steps {
            let mark = if step.passed { "ok  " } else { "FAIL" };
            out.push_str(&format!("  {mark} {:>3} {}\n", step.index, step.step));
            for failure in &step.failures {
                out.push_str(&format!("           {failure}\n"));
            }
        }
        out.push_str(&format!(
            "  event log: {} events, sha256 {}\n",
            self.event_log.events, self.event_log.sha256
        ));
        out
    }
}

/// Runs a scenario against a fresh sandboxed service built from `base`: its own temporary database, a frozen virtual
/// clock at the scenario's start, deterministic IDs from its seed, test control on, and its settlement settings. The
/// sandbox is removed afterwards. Errors are reserved for a sandbox that cannot start; scenario failures are in the
/// report.
pub async fn run(scenario: &Scenario, base: &Config) -> anyhow::Result<Report> {
    let dir = std::env::temp_dir().join(format!("exchange-scenario-{}", Uuid::now_v7()));
    let config = sandbox_config(scenario, base, &dir);
    let app = App::start(&config)?;
    let mut runner = Runner {
        router: app.router.clone(),
        refs: HashMap::new(),
        instruments: Vec::new(),
        webhooks: HashMap::new(),
        seed: scenario.seed,
    };
    let mut steps = vec![runner.setup(scenario).await];
    for (i, step) in scenario.steps.iter().enumerate() {
        let (description, failures) = runner.step(step).await;
        steps.push(StepReport {
            index: i + 1,
            step: description,
            passed: failures.is_empty(),
            failures,
        });
    }
    let event_log = runner.event_log().await;
    // The runner's router holds a handle to the exchange thread; it must go before the app can shut down.
    drop(runner);
    app.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(Report {
        name: scenario.name.clone(),
        passed: steps.iter().all(|s| s.passed),
        steps,
        event_log,
    })
}

fn sandbox_config(scenario: &Scenario, base: &Config, dir: &std::path::Path) -> Config {
    let mut config = base.clone();
    config.database.path = dir.join("exchange.db").to_string_lossy().into_owned();
    config.clock = ClockConfig {
        mode: ClockMode::Virtual,
        start: Some(scenario.start),
        running: false,
        speed: 1.0,
    };
    config.ids = IdsConfig {
        mode: IdMode::Deterministic,
        seed: scenario.seed,
    };
    config.test_control.enabled = true;
    // A scenario declares its own webhooks; the service's real consumers must never receive sandbox events.
    config.webhooks.subscriptions.clear();
    config.events.simulation_id = Some(scenario.name.clone());
    if let Some(section) = &scenario.clearing {
        let c = &mut config.clearing;
        c.enabled = section.enabled.unwrap_or(true);
        if let Some(cutoff) = &section.cutoff {
            c.cutoff = cutoff.clone();
        }
        if let Some(zone) = &section.time_zone {
            c.time_zone = zone.clone();
        }
        if let Some(breaks) = &section.breaks {
            c.breaks = breaks.clone();
        }
    }
    let s = &mut config.settlement;
    s.seed = scenario.seed;
    if let Some(section) = &scenario.settlement {
        macro_rules! apply {
            ($($field:ident),*) => { $(if let Some(value) = section.$field { s.$field = value; })* };
        }
        if let Some(table) = &section.market_settlement_days {
            s.market_settlement_days = table.clone();
        }
        apply!(
            mode,
            seed,
            settlement_days,
            reject_probability,
            cancel_probability,
            fail_probability,
            delay_probability,
            correct_probability,
            max_delay_days,
            max_delays,
            correction_days
        );
    }
    config
}

struct Runner {
    router: Router,
    /// Named orders: their submit responses.
    refs: HashMap<String, Value>,
    instruments: Vec<String>,
    /// Registered webhooks: URL → subscription ID.
    webhooks: HashMap<String, String>,
    /// The scenario's seed: the default for market steps.
    seed: u64,
}

impl Runner {
    async fn call(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), String> {
        let request = Request::builder().method(method).uri(uri);
        let request = match body {
            Some(body) => request
                .header("content-type", "application/json")
                .body(Body::from(body.to_string())),
            None => request.body(Body::empty()),
        }
        .map_err(|e| e.to_string())?;
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .map_err(|e| e.to_string())?;
        let status = response.status().as_u16();
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|e| e.to_string())?
            .to_bytes();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        Ok((status, body))
    }

    async fn setup(&mut self, scenario: &Scenario) -> StepReport {
        let failures = self
            .register(
                &scenario.instruments,
                &scenario.participants,
                &scenario.webhooks,
            )
            .await;
        StepReport {
            index: 0,
            step: "setup".into(),
            passed: failures.is_empty(),
            failures,
        }
    }

    /// Registers instruments, participants and webhooks (with their fault policies), in that order.
    async fn register(
        &mut self,
        instruments: &[Value],
        participants: &[Value],
        webhooks: &[Value],
    ) -> Vec<String> {
        let mut failures = Vec::new();
        let groups = [
            ("instrument", "/api/instruments", instruments),
            ("participant", "/api/participants", participants),
            ("webhook", "/api/webhooks", webhooks),
        ];
        for (kind, uri, bodies) in groups {
            for body in bodies {
                // A webhook's `faults` is not part of its registration; it is set right after.
                let mut body = body.clone();
                let faults = body.as_object_mut().and_then(|b| b.remove("faults"));
                match self.call(Method::POST, uri, Some(body.clone())).await {
                    Ok((status, response)) if (200..300).contains(&status) => {
                        if kind == "instrument"
                            && let Some(id) = body["id"].as_str()
                        {
                            self.instruments.push(id.to_string());
                        }
                        if kind == "webhook"
                            && let (Some(url), Some(id)) =
                                (body["url"].as_str(), response["id"].as_str())
                        {
                            self.webhooks.insert(url.to_string(), id.to_string());
                            if let Some(faults) = faults {
                                failures.extend(self.set_faults(url, Some(faults)).await);
                            }
                        }
                    }
                    Ok((status, response)) => {
                        failures.push(format!("{kind} {body}: HTTP {status} {response}"))
                    }
                    Err(error) => failures.push(format!("{kind} {body}: {error}")),
                }
            }
        }
        failures
    }

    /// Runs one step; returns its description and failures.
    async fn step(&mut self, step: &Step) -> (String, Vec<String>) {
        match step {
            Step::Order(order) => {
                let mut body = order.body.clone();
                if !body.contains_key("instrumentId")
                    && let [only] = self.instruments.as_slice()
                {
                    body.insert("instrumentId".into(), json!(only));
                }
                let text = |key: &str| {
                    body.get(key)
                        .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
                };
                let price = text("priceTicks").unwrap_or_else(|| "MARKET".into());
                let description = format!(
                    "order{} {} {} {}@{price}",
                    order
                        .name
                        .as_ref()
                        .map(|n| format!(" {n}:"))
                        .unwrap_or_default(),
                    text("participantId").unwrap_or_default(),
                    text("side").unwrap_or_default(),
                    text("quantity").unwrap_or_default(),
                );
                let response = self
                    .call(Method::POST, "/api/orders", Some(Value::Object(body)))
                    .await;
                if let (Some(name), Ok((status, body))) = (&order.name, &response)
                    && (200..300).contains(status)
                {
                    self.refs.insert(name.clone(), body.clone());
                }
                (description, check_response(response, order.expect.as_ref()))
            }
            Step::Cancel(cancel) => {
                let description = format!("cancel {}", cancel.order);
                match self.order_id(&cancel.order) {
                    Ok(id) => {
                        let response = self
                            .call(Method::DELETE, &format!("/api/orders/{id}"), None)
                            .await;
                        (
                            description,
                            check_response(response, cancel.expect.as_ref()),
                        )
                    }
                    Err(failure) => (description, vec![failure]),
                }
            }
            Step::AdvanceClock(advance) => {
                let body = serde_json::to_value(advance).expect("serializable");
                let description = format!("advance_clock {body}");
                let response = self
                    .call(Method::POST, "/test/clock/advance", Some(body))
                    .await;
                (description, check_response(response, None))
            }
            Step::SetClock(set) => {
                let description = format!(
                    "set_clock {}",
                    set.to.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                );
                let response = self
                    .call(
                        Method::POST,
                        "/test/clock/set",
                        Some(json!({ "to": set.to })),
                    )
                    .await;
                (description, check_response(response, None))
            }
            Step::ReportSettlement(report) => {
                let description = format!("report_settlement {}", report.execution);
                match self.execution_id(&report.execution) {
                    Ok(id) => {
                        let uri = format!("/api/settlements/{id}/outcomes");
                        let response = self
                            .call(Method::POST, &uri, Some(Value::Object(report.body.clone())))
                            .await;
                        (
                            description,
                            check_response(response, report.expect.as_ref()),
                        )
                    }
                    Err(failure) => (description, vec![failure]),
                }
            }
            Step::SetInstrument(set) => {
                let description =
                    format!("set_instrument {} enabled={}", set.instrument, set.enabled);
                let uri = format!("/api/instruments/{}", set.instrument);
                let response = self
                    .call(Method::PATCH, &uri, Some(json!({ "enabled": set.enabled })))
                    .await;
                (description, check_response(response, set.expect.as_ref()))
            }
            Step::Market(market) => self.market(market).await,
            Step::Register(register) => {
                let description = format!(
                    "register {} instruments, {} participants, {} webhooks",
                    register.instruments.len(),
                    register.participants.len(),
                    register.webhooks.len()
                );
                let failures = self
                    .register(
                        &register.instruments,
                        &register.participants,
                        &register.webhooks,
                    )
                    .await;
                (description, failures)
            }
            Step::Reset(_) => {
                self.refs.clear();
                self.instruments.clear();
                self.webhooks.clear();
                let response = self.call(Method::POST, "/test/reset", None).await;
                ("reset".into(), check_response(response, None))
            }
            Step::Redeliver(redeliver) => {
                let description = format!("redeliver {} after {}", redeliver.url, redeliver.after);
                let Some(id) = self.webhooks.get(&redeliver.url) else {
                    return (
                        description,
                        vec![format!("no webhook registered for {}", redeliver.url)],
                    );
                };
                let uri = format!("/test/webhooks/{id}/redeliver?after={}", redeliver.after);
                let response = self.call(Method::POST, &uri, None).await;
                (
                    description,
                    check_response(response, redeliver.expect.as_ref()),
                )
            }
            Step::RetryDeadLetters(retry) => {
                let description = format!(
                    "retry_dead_letters {}",
                    retry.url.as_deref().unwrap_or("(all)")
                );
                let body = match &retry.url {
                    Some(url) => match self.webhooks.get(url) {
                        Some(id) => json!({ "subscriptionId": id }),
                        None => {
                            return (
                                description,
                                vec![format!("no webhook registered for {url}")],
                            );
                        }
                    },
                    None => json!({}),
                };
                let response = self
                    .call(Method::POST, "/test/dead-letters/retry", Some(body))
                    .await;
                (description, check_response(response, retry.expect.as_ref()))
            }
            Step::SetFaults(set) => (
                format!(
                    "set_faults {} {}",
                    set.url,
                    set.faults.as_ref().map_or("off".into(), Value::to_string)
                ),
                self.set_faults(&set.url, set.faults.clone()).await,
            ),
            Step::Expect(expectation) => self.expect(expectation).await,
        }
    }

    /// Registers the market's participants (existing ones are fine), then sends its commands in order, setting the
    /// virtual clock to each arrival time first. Exchange rejections and cancels of orders that have meanwhile filled
    /// are part of a market; any other refusal is a failure.
    async fn market(&mut self, market: &MarketConfig) -> (String, Vec<String>) {
        let mut market = market.clone();
        let seed = *market.seed.get_or_insert(self.seed);
        let mut failures = Vec::new();
        let mut fail = |failure: String| {
            if failures.len() < MAX_MARKET_FAILURES {
                failures.push(failure);
            } else if failures.len() == MAX_MARKET_FAILURES {
                failures.push("… further failures not shown".into());
            }
        };
        for id in market.participants.ids() {
            let body = json!({ "id": id, "name": id });
            match self
                .call(Method::POST, "/api/participants", Some(body))
                .await
            {
                Ok((201 | 409, _)) => {}
                Ok((status, body)) => fail(format!("participant {id}: HTTP {status} {body}")),
                Err(error) => fail(format!("participant {id}: {error}")),
            }
        }
        let start = match self.call(Method::GET, "/test/clock", None).await {
            Ok((200, clock)) => clock["now"]
                .as_str()
                .and_then(|now| now.parse::<DateTime<Utc>>().ok()),
            _ => None,
        };
        let Some(start) = start else {
            return (
                format!("market seed {seed}"),
                vec!["cannot read the virtual clock".into()],
            );
        };
        let mut now = start;
        let mut order_ids: Vec<Option<String>> = Vec::new();
        let (mut accepted, mut rejected, mut executions, mut cancelled, mut refused) =
            (0, 0, 0, 0, 0);
        for command in market.generate() {
            let at = start + chrono::Duration::microseconds(command.at_micros as i64);
            if at > now {
                match self
                    .call(Method::POST, "/test/clock/set", Some(json!({ "to": at })))
                    .await
                {
                    Ok((200, _)) => now = at,
                    Ok((status, body)) => fail(format!("set clock to {at}: HTTP {status} {body}")),
                    Err(error) => fail(format!("set clock to {at}: {error}")),
                }
            }
            match command.action {
                MarketAction::Submit(order) => {
                    let body = json!({
                        "instrumentId": order.instrument_id.0,
                        "participantId": order.participant_id.0,
                        "side": order.side,
                        "type": order.order_type,
                        "priceTicks": order.limit_price_ticks,
                        "quantity": order.quantity,
                    });
                    let response = self
                        .call(Method::POST, "/api/orders", Some(body.clone()))
                        .await;
                    let id = match response {
                        Ok((201, response)) => {
                            if response["order"]["status"] == "REJECTED" {
                                rejected += 1;
                            } else {
                                accepted += 1;
                            }
                            executions += response["executions"].as_array().map_or(0, Vec::len);
                            response["order"]["orderId"].as_str().map(str::to_string)
                        }
                        Ok((status, response)) => {
                            fail(format!("order {body}: HTTP {status} {response}"));
                            None
                        }
                        Err(error) => {
                            fail(format!("order {body}: {error}"));
                            None
                        }
                    };
                    order_ids.push(id);
                }
                MarketAction::Cancel { submit_index } => {
                    // An order that failed to submit has already been reported.
                    let Some(Some(id)) = order_ids.get(submit_index) else {
                        continue;
                    };
                    let uri = format!("/api/orders/{id}");
                    match self.call(Method::DELETE, &uri, None).await {
                        Ok((200, _)) => cancelled += 1,
                        Ok((409, _)) => refused += 1,
                        Ok((status, body)) => fail(format!("DELETE {uri}: HTTP {status} {body}")),
                        Err(error) => fail(format!("DELETE {uri}: {error}")),
                    }
                }
            }
        }
        let description = format!(
            "market seed {seed}: {} orders ({rejected} rejected), {executions} executions, {} cancels \
             ({refused} refused), until {}",
            accepted + rejected,
            cancelled + refused,
            now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
        );
        (description, failures)
    }

    /// Sets (or with `None` clears) the fault policy of the webhook registered for `url`.
    async fn set_faults(&self, url: &str, faults: Option<Value>) -> Vec<String> {
        let Some(id) = self.webhooks.get(url) else {
            return vec![format!("no webhook registered for {url}")];
        };
        let uri = format!("/test/webhooks/{id}/faults");
        let response = match faults {
            Some(faults) => self.call(Method::PUT, &uri, Some(faults)).await,
            None => self.call(Method::DELETE, &uri, None).await,
        };
        check_response(response, None)
    }

    /// The fingerprint of the current event log.
    async fn event_log(&self) -> EventLog {
        let events = self
            .collect("/api/events?limit=1000", true)
            .await
            .unwrap_or_default();
        let mut hash = Sha256::new();
        for event in &events {
            hash.update(event.to_string().as_bytes());
            hash.update(b"\n");
        }
        EventLog {
            events: events.len(),
            sha256: hash.finalize().iter().map(|b| format!("{b:02x}")).collect(),
        }
    }

    async fn expect(&self, e: &Expectation) -> (String, Vec<String>) {
        if let Some(order) = &e.order {
            let description = format!("expect order {order}");
            return match self.order_id(order) {
                Ok(id) => (
                    description,
                    self.check_single(&format!("/api/orders/{id}"), e.matches.as_ref())
                        .await,
                ),
                Err(failure) => (description, vec![failure]),
            };
        }
        if let Some(instrument) = &e.book {
            let description = format!("expect book {instrument}");
            let failures = self
                .check_single(&format!("/api/orderbooks/{instrument}"), e.matches.as_ref())
                .await;
            return (description, failures);
        }
        if let Some(execution) = &e.settlement {
            let description = format!("expect settlement {execution}");
            return match self.execution_id(execution) {
                Ok(id) => (
                    description,
                    self.check_single(&format!("/api/settlements/{id}"), e.matches.as_ref())
                        .await,
                ),
                Err(failure) => (description, vec![failure]),
            };
        }
        let real_time_wait = e.wait_ms.unwrap_or(DEFAULT_DELIVERY_WAIT_MS);
        let (kind, filter, uri, paged, wait) =
            match (&e.trades, &e.events, &e.deliveries, &e.attempts) {
                (Some(filter), ..) => ("trades", filter, "/api/trades?limit=1000", false, 0),
                (_, Some(filter), ..) => ("events", filter, "/api/events?limit=1000", true, 0),
                (_, _, Some(filter), _) => (
                    "deliveries",
                    filter,
                    "/test/deliveries",
                    false,
                    real_time_wait,
                ),
                (.., Some(filter)) => ("attempts", filter, "/test/attempts", false, real_time_wait),
                _ => unreachable!("Scenario::parse checks that an expectation has a target"),
            };
        let description = format!("expect {kind} {filter}");
        let deadline = Instant::now() + Duration::from_millis(wait);
        loop {
            let failure = match self.collect(uri, paged).await {
                Ok(items) => {
                    let found = items.iter().filter(|item| contains(filter, item)).count();
                    match e.count {
                        _ if e.all == Some(true) => {
                            (found < items.len() || items.is_empty()).then(|| {
                                format!(
                                    "expected all {kind} to match, {found} of {} do",
                                    items.len()
                                )
                            })
                        }
                        Some(count) if count != found => {
                            Some(format!("expected {count} matching {kind}, found {found}"))
                        }
                        None if found == 0 => {
                            Some(format!("expected at least one matching {kind}, found none"))
                        }
                        _ => None,
                    }
                }
                Err(failure) => Some(failure),
            };
            match failure {
                Some(_) if Instant::now() < deadline => tokio::time::sleep(DELIVERY_POLL).await,
                Some(failure) => {
                    let waited = if wait > 0 {
                        format!(" after waiting {wait} ms")
                    } else {
                        String::new()
                    };
                    return (description, vec![format!("{failure}{waited}")]);
                }
                None => return (description, Vec::new()),
            }
        }
    }

    /// GETs one resource and checks it contains `matches`.
    async fn check_single(&self, uri: &str, matches: Option<&Value>) -> Vec<String> {
        match self.call(Method::GET, uri, None).await {
            Ok((200, actual)) => matches
                .map(|expected| mismatches(expected, &actual))
                .unwrap_or_default(),
            Ok((status, body)) => vec![format!("GET {uri}: HTTP {status} {body}")],
            Err(error) => vec![format!("GET {uri}: {error}")],
        }
    }

    /// Every item of a list endpoint; events are paged through by sequence.
    async fn collect(&self, uri: &str, paged: bool) -> Result<Vec<Value>, String> {
        let mut items = Vec::new();
        let mut after = 0;
        loop {
            let page_uri = if paged {
                format!("{uri}&after={after}")
            } else {
                uri.to_string()
            };
            let page = match self.call(Method::GET, &page_uri, None).await? {
                (200, Value::Array(page)) => page,
                (status, body) => return Err(format!("GET {page_uri}: HTTP {status} {body}")),
            };
            let full = page.len() == 1000;
            if let Some(last) = page.last() {
                after = last["sequence"].as_u64().unwrap_or(after);
            }
            items.extend(page);
            if !paged || !full {
                return Ok(items);
            }
        }
    }

    /// A named order's ID, or a literal order ID.
    fn order_id(&self, reference: &str) -> Result<String, String> {
        if let Some(response) = self.refs.get(reference) {
            return response["order"]["orderId"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("{reference} has no order ID"));
        }
        Uuid::parse_str(reference)
            .map(|id| id.to_string())
            .map_err(|_| format!("unknown order reference {reference} (name orders with `as:`)"))
    }

    /// `order#i`: the `i`-th execution of a named order; otherwise a literal execution ID.
    fn execution_id(&self, reference: &str) -> Result<String, String> {
        if let Some((name, index)) = reference.split_once('#') {
            let response = self
                .refs
                .get(name)
                .ok_or_else(|| format!("unknown order reference {name}"))?;
            let index: usize = index
                .parse()
                .map_err(|_| format!("bad execution index in {reference}"))?;
            return response["executions"][index]["executionId"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("{name} has no execution {index}"));
        }
        Uuid::parse_str(reference)
            .map(|id| id.to_string())
            .map_err(|_| format!("unknown execution reference {reference} (use order#index)"))
    }
}

/// Checks a command's response: the expected status (default: any 2xx) and the contained fields.
fn check_response(
    response: Result<(u16, Value), String>,
    check: Option<&ResponseCheck>,
) -> Vec<String> {
    let (status, body) = match response {
        Ok(response) => response,
        Err(error) => return vec![error],
    };
    let mut failures = Vec::new();
    match check.and_then(|c| c.status) {
        Some(expected) if expected != status => {
            failures.push(format!("expected HTTP {expected}, got {status}: {body}"))
        }
        None if !(200..300).contains(&status) => failures.push(format!("HTTP {status}: {body}")),
        _ => {}
    }
    if let Some(expected) = check.and_then(|c| c.matches.as_ref()) {
        failures.extend(mismatches(expected, &body));
    }
    failures
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sandbox_never_inherits_the_services_webhook_subscriptions() {
        let mut base: Config = config::Config::builder()
            .add_source(config::File::from_str(
                include_str!("../../config/default.yaml"),
                config::FileFormat::Yaml,
            ))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        base.webhooks.subscriptions = vec![crate::config::SubscriptionConfig {
            name: "production-consumer".into(),
            url: "http://consumer/hook".into(),
            event_types: vec!["TradeExecuted".into()],
            secret: None,
            secret_env: None,
            secret_file: None,
            ordered: false,
            enabled: true,
            paused: false,
        }];
        let scenario: Scenario =
            serde_saphyr::from_str("name: s\nseed: 1\nstart: 2030-01-07T09:00:00Z\nsteps: []\n")
                .unwrap();
        let sandbox = sandbox_config(&scenario, &base, std::path::Path::new("/tmp/x"));
        assert!(sandbox.webhooks.subscriptions.is_empty());
    }
}
