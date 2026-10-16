//! The Milestone 1 acceptance test, fully containerized: the service image built from `Dockerfile` and MockServer as
//! the settlement consumer on one private Docker network, a fresh database volume, and a real container restart.
//!
//! A second test runs the Milestone 2 settlement acceptance the same way: combined settlement mode on a virtual
//! clock, lifecycle events and an external report delivered to MockServer, and a restart mid-lifecycle.
//!
//! A third runs the Milestone 3 scenario suite in the image: through its CLI, with no network, twice; and through
//! `POST /test/scenarios` on a running container. All reports must equal the ones the local build produces.
//!
//! Needs Docker. Run with `cargo test -p exchange-simulator --features e2e --test e2e`.

use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const IMAGE: &str = "exchange-simulator";
const TAG: &str = "e2e";
const HOOK_PATH: &str = "/settlement/trades";

/// Builds the service image from the workspace root, as compose.yaml does, once per test run; BuildKit's cache
/// makes reruns quick.
fn build_image() {
    static BUILD: std::sync::Once = std::sync::Once::new();
    BUILD.call_once(build_image_now);
}

fn build_image_now() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/..");
    let status = std::process::Command::new("docker")
        .args([
            "build",
            "-q",
            "-f",
            "exchange-simulator/Dockerfile",
            "-t",
            &format!("{IMAGE}:{TAG}"),
            ".",
        ])
        .current_dir(root)
        .env("DOCKER_BUILDKIT", "1")
        .stdout(Stdio::null())
        .status()
        .expect("run docker build");
    assert!(status.success(), "building the service image failed");
}

struct Http {
    client: reqwest::Client,
}

impl Http {
    async fn send(
        &self,
        method: reqwest::Method,
        url: String,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut request = self.client.request(method, &url);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .unwrap_or_else(|e| panic!("{url}: {e}"));
        let status = response.status().as_u16();
        let text = response.text().await.unwrap();
        (
            status,
            if text.is_empty() {
                Value::Null
            } else {
                serde_json::from_str(&text).unwrap_or(Value::String(text))
            },
        )
    }

    async fn get(&self, url: String) -> Value {
        let (status, body) = self.send(reqwest::Method::GET, url, None).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn post(&self, url: String, body: Value) -> Value {
        let (status, body) = self.send(reqwest::Method::POST, url, Some(body)).await;
        assert!((200..300).contains(&status), "{status}: {body}");
        body
    }

    async fn put(&self, url: String, body: Value) -> Value {
        let (status, body) = self.send(reqwest::Method::PUT, url, Some(body)).await;
        assert!((200..300).contains(&status), "{status}: {body}");
        body
    }
}

struct MockServer {
    container: ContainerAsync<GenericImage>,
}

impl MockServer {
    async fn base(&self) -> String {
        format!(
            "http://127.0.0.1:{}",
            self.container.get_host_port_ipv4(1080).await.unwrap()
        )
    }

    /// Answers `POST HOOK_PATH` with `status`, replacing earlier expectations.
    async fn respond(&self, http: &Http, status: u16) {
        self.respond_on(http, HOOK_PATH, status).await;
    }

    /// Answers `POST path` with `status`, replacing earlier expectations for that path.
    async fn respond_on(&self, http: &Http, path: &str, status: u16) {
        http.put(
            format!("{}/mockserver/clear?type=EXPECTATIONS", self.base().await),
            json!({ "path": path }),
        )
        .await;
        http.put(
            format!("{}/mockserver/expectation", self.base().await),
            json!({ "httpRequest": { "method": "POST", "path": path }, "httpResponse": { "statusCode": status },
                    "times": { "unlimited": true } }),
        )
        .await;
    }

    /// Bodies of the webhook requests received on `HOOK_PATH` so far, oldest first.
    async fn received(&self, http: &Http) -> Vec<Value> {
        self.received_on(http, HOOK_PATH).await
    }

    /// Bodies of the requests received on `path` so far, oldest first.
    async fn received_on(&self, http: &Http, path: &str) -> Vec<Value> {
        let requests = http
            .put(
                format!(
                    "{}/mockserver/retrieve?type=REQUESTS&format=JSON",
                    self.base().await
                ),
                json!({ "path": path }),
            )
            .await;
        requests
            .as_array()
            .unwrap()
            .iter()
            .map(|request| match &request["body"] {
                Value::Object(body) if body.contains_key("json") => body["json"].clone(),
                Value::Object(body) => {
                    serde_json::from_str(body["string"].as_str().unwrap()).unwrap()
                }
                other => other.clone(),
            })
            .collect()
    }
}

async fn eventually<T>(what: &str, mut probe: impl AsyncFnMut() -> Option<T>) -> T {
    for _ in 0..300 {
        if let Some(value) = probe().await {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn milestone_1_acceptance_in_containers() {
    build_image();
    let suffix = uuid::Uuid::now_v7().simple().to_string();
    let network = format!("exsim-e2e-{suffix}");
    let mock_host = format!("mockserver-{suffix}");
    let http = Http {
        client: reqwest::Client::new(),
    };

    let mock = MockServer {
        container: GenericImage::new("mockserver/mockserver", "5.15.0")
            .with_exposed_port(1080.tcp())
            .with_wait_for(WaitFor::message_on_stdout("started on port: 1080"))
            .with_network(&network)
            .with_container_name(&mock_host)
            .start()
            .await
            .expect("start MockServer"),
    };
    mock.respond(&http, 202).await;

    // 1. Start the service container with a fresh database volume.
    let service = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(8080.tcp())
        .with_wait_for(WaitFor::message_on_stdout("exchange simulator listening"))
        .with_network(&network)
        .with_env_var("EXCHANGE__WEBHOOKS__INITIAL_BACKOFF_MS", "100")
        .with_env_var("EXCHANGE__WEBHOOKS__MAX_BACKOFF_MS", "500")
        .with_env_var("EXCHANGE__WEBHOOKS__MAX_ATTEMPTS", "1000")
        .start()
        .await
        .expect("start service container");
    let base = async || {
        format!(
            "http://127.0.0.1:{}",
            service.get_host_port_ipv4(8080).await.unwrap()
        )
    };
    let api = base().await;
    assert_eq!(http.get(format!("{api}/health")).await["status"], "ok");

    // 2. ACME with tick size 0.01 DKK, and two participants.
    let acme = http
        .post(
            format!("{api}/api/instruments"),
            json!({ "id": "ACME", "symbol": "ACME", "mic": "XCSE", "currency": "DKK", "currencyScale": 2, "tickSizeMinor": 1 }),
        )
        .await;
    assert_eq!(acme["tickSize"], "0.01");
    for id in ["participant-a", "participant-b"] {
        http.post(
            format!("{api}/api/participants"),
            json!({ "id": id, "name": id }),
        )
        .await;
    }

    // 3. MockServer subscribed to TradeExecuted, addressed by its name on the Docker network.
    let hook_url = format!("http://{mock_host}:1080{HOOK_PATH}");
    http.post(
        format!("{api}/api/webhooks"),
        json!({ "url": hook_url, "eventTypes": ["TradeExecuted"] }),
    )
    .await;

    // 4. and 5. A sell of 200 at 100.25, then a buy of 150 at 100.25.
    let order = |participant: &str, side: &str, quantity: u64| {
        json!({ "instrumentId": "ACME", "participantId": participant, "side": side, "type": "LIMIT",
                "priceTicks": 10025, "quantity": quantity })
    };
    let sell = http
        .post(
            format!("{api}/api/orders"),
            order("participant-a", "SELL", 200),
        )
        .await;
    let sell_id = sell["order"]["orderId"].as_str().unwrap().to_string();
    let buy = http
        .post(
            format!("{api}/api/orders"),
            order("participant-b", "BUY", 150),
        )
        .await;

    // 6. Exactly one execution of 150; the sell has 50 remaining, the buy is filled.
    let executions = buy["executions"].as_array().unwrap();
    assert_eq!(executions.len(), 1);
    assert_eq!(
        (&executions[0]["quantity"], &executions[0]["price"]),
        (&json!(150), &json!("100.25"))
    );
    assert_eq!(buy["order"]["status"], "FILLED");
    let resting = http.get(format!("{api}/api/orders/{sell_id}")).await;
    assert_eq!(
        (&resting["status"], &resting["remainingQuantity"]),
        (&json!("PARTIALLY_FILLED"), &json!(50))
    );

    // 7. The execution and its event are both stored (they are committed in one transaction).
    let execution_id = executions[0]["executionId"].clone();
    let trades = http.get(format!("{api}/api/trades")).await;
    assert_eq!(trades.as_array().unwrap().len(), 1);
    let events = http
        .get(format!("{api}/api/events?eventType=TradeExecuted"))
        .await;
    assert_eq!(events.as_array().unwrap().len(), 1);
    assert_eq!(events[0]["payload"]["executionId"], execution_id);
    let event_id = events[0]["eventId"].clone();

    // 8. MockServer receives the trade with the stable event ID.
    let delivered = eventually("first delivery", async || {
        let received = mock.received(&http).await;
        (!received.is_empty()).then_some(received)
    })
    .await;
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0]["eventId"], event_id);
    assert_eq!(
        delivered[0], events[0],
        "the delivered envelope is the canonical event"
    );
    let book_before = http.get(format!("{api}/api/orderbooks/ACME")).await;

    // 9. Restart the container (SIGTERM, then start): the book is restored, replay is unchanged, nothing repeats.
    let status = tokio::process::Command::new("docker")
        .args(["restart", service.id()])
        .stdout(Stdio::null())
        .status()
        .await
        .expect("run docker restart");
    assert!(status.success());
    let api = base().await;
    eventually("service after restart", async || {
        let response = http.client.get(format!("{api}/health")).send().await.ok()?;
        response.status().is_success().then_some(())
    })
    .await;
    assert_eq!(
        http.get(format!("{api}/api/orderbooks/ACME")).await,
        book_before
    );
    assert_eq!(
        http.get(format!("{api}/api/events?eventType=TradeExecuted"))
            .await,
        events
    );
    assert_eq!(
        http.get(format!("{api}/api/trades"))
            .await
            .as_array()
            .unwrap()
            .len(),
        1
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        mock.received(&http).await.len(),
        1,
        "a delivered event is not sent again after restart"
    );

    // 10. The consumer fails, then recovers: the delivery is retried with the same event and no trade is repeated.
    mock.respond(&http, 500).await;
    let second = http
        .post(
            format!("{api}/api/orders"),
            order("participant-b", "BUY", 50),
        )
        .await;
    assert_eq!(second["executions"].as_array().unwrap().len(), 1);
    let failing = eventually("failed attempts", async || {
        let received = mock.received(&http).await;
        (received.len() >= 4).then_some(received)
    })
    .await;
    let second_event = failing[1]["eventId"].clone();
    assert!(
        failing[1..].iter().all(|e| e["eventId"] == second_event),
        "retries resend the same event"
    );
    mock.respond(&http, 202).await;
    eventually("delivery after recovery", async || {
        let metrics = http
            .client
            .get(format!("{api}/metrics"))
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()?;
        metrics
            .contains("webhook_outbox_deliveries{status=\"DELIVERED\"} 2")
            .then_some(())
    })
    .await;
    assert_eq!(
        http.get(format!("{api}/api/trades"))
            .await
            .as_array()
            .unwrap()
            .len(),
        2,
        "retries create no trades"
    );
    let ids: std::collections::HashSet<_> = mock
        .received(&http)
        .await
        .iter()
        .map(|e| e["eventId"].to_string())
        .collect();
    assert_eq!(
        ids.len(),
        2,
        "two trades, two distinct event IDs, however many attempts"
    );
    assert_eq!(
        http.get(format!("{api}/api/orderbooks/ACME")).await["asks"],
        json!([])
    );
}

#[tokio::test]
async fn milestone_2_settlement_in_containers() {
    const EVENTS_PATH: &str = "/settlement/events";
    build_image();
    let suffix = uuid::Uuid::now_v7().simple().to_string();
    let network = format!("exsim-e2e2-{suffix}");
    let mock_host = format!("mockserver-{suffix}");
    let http = Http {
        client: reqwest::Client::new(),
    };
    let mock = MockServer {
        container: GenericImage::new("mockserver/mockserver", "5.15.0")
            .with_exposed_port(1080.tcp())
            .with_wait_for(WaitFor::message_on_stdout("started on port: 1080"))
            .with_network(&network)
            .with_container_name(&mock_host)
            .start()
            .await
            .expect("start MockServer"),
    };
    mock.respond_on(&http, EVENTS_PATH, 202).await;

    // Combined mode, frozen virtual clock on Monday 2030-01-07 09:00 UTC, no random failures or delays.
    let service = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(8080.tcp())
        .with_wait_for(WaitFor::message_on_stdout("exchange simulator listening"))
        .with_network(&network)
        .with_env_var("EXCHANGE__CLOCK__MODE", "virtual")
        .with_env_var("EXCHANGE__CLOCK__START", "2030-01-07T09:00:00Z")
        .with_env_var("EXCHANGE__TEST_CONTROL__ENABLED", "true")
        .with_env_var("EXCHANGE__SETTLEMENT__MODE", "both")
        .with_env_var("EXCHANGE__SETTLEMENT__TICK_INTERVAL_MS", "50")
        .with_env_var("EXCHANGE__SETTLEMENT__FAIL_PROBABILITY", "0")
        .with_env_var("EXCHANGE__SETTLEMENT__DELAY_PROBABILITY", "0")
        .start()
        .await
        .expect("start service container");
    let base = async || {
        format!(
            "http://127.0.0.1:{}",
            service.get_host_port_ipv4(8080).await.unwrap()
        )
    };
    let api = base().await;
    http.post(
        format!("{api}/api/instruments"),
        json!({ "id": "ACME", "symbol": "ACME", "mic": "XCSE", "currency": "DKK", "currencyScale": 2 }),
    )
    .await;
    for id in ["participant-a", "participant-b"] {
        http.post(
            format!("{api}/api/participants"),
            json!({ "id": id, "name": id }),
        )
        .await;
    }
    http.post(
        format!("{api}/api/webhooks"),
        json!({ "url": format!("http://{mock_host}:1080{EVENTS_PATH}"),
                "eventTypes": ["SettlementPending", "SettlementCompleted", "SettlementOutcomeReported"] }),
    )
    .await;

    let order = |participant: &str, side: &str, quantity: u64| {
        json!({ "instrumentId": "ACME", "participantId": participant, "side": side, "type": "LIMIT",
                "priceTicks": 10025, "quantity": quantity })
    };
    http.post(
        format!("{api}/api/orders"),
        order("participant-a", "SELL", 200),
    )
    .await;
    let buy = http
        .post(
            format!("{api}/api/orders"),
            order("participant-b", "BUY", 150),
        )
        .await;
    let execution_id = buy["executions"][0]["executionId"]
        .as_str()
        .unwrap()
        .to_string();

    // SettlementPending reaches the subscriber, attributed to the simulator and correlated to the execution.
    let received = eventually("SettlementPending at MockServer", async || {
        let received = mock.received_on(&http, EVENTS_PATH).await;
        (!received.is_empty()).then_some(received)
    })
    .await;
    let pending = &received[0];
    assert_eq!(
        (&pending["eventType"], &pending["source"]),
        (&json!("SettlementPending"), &json!("settlement-simulator"))
    );
    assert_eq!(pending["correlationId"], execution_id.as_str());
    assert_eq!(pending["payload"]["intendedSettlementDate"], "2030-01-09");

    // Restart mid-lifecycle: simulated time and the pending instruction survive.
    let status = tokio::process::Command::new("docker")
        .args(["restart", service.id()])
        .stdout(Stdio::null())
        .status()
        .await
        .expect("run docker restart");
    assert!(status.success());
    let api = base().await;
    eventually("service after restart", async || {
        let response = http.client.get(format!("{api}/health")).send().await.ok()?;
        response.status().is_success().then_some(())
    })
    .await;
    assert_eq!(
        http.get(format!("{api}/test/clock")).await["now"],
        "2030-01-07T09:00:00Z"
    );

    // T+2: advancing the clock settles the trade before the call returns.
    let advanced = http
        .post(format!("{api}/test/clock/advance"), json!({ "days": 2 }))
        .await;
    assert_eq!(advanced["now"], "2030-01-09T09:00:00Z");
    let completed = http
        .get(format!("{api}/api/events?eventType=SettlementCompleted"))
        .await;
    assert_eq!(completed.as_array().unwrap().len(), 1);
    assert_eq!(completed[0]["occurredAt"], "2030-01-09T00:00:00Z");
    let pendings = http
        .get(format!("{api}/api/events?eventType=SettlementPending"))
        .await;
    assert_eq!(
        pendings.as_array().unwrap().len(),
        1,
        "the restart did not take the trade in twice"
    );

    // An external service reports its own outcome; it is published apart from the simulator's.
    let (status, _) = http
        .send(
            reqwest::Method::POST,
            format!("{api}/api/settlements/{execution_id}/outcomes"),
            Some(json!({ "status": "SETTLED", "reference": "csd-e2e-1" })),
        )
        .await;
    assert_eq!(status, 201);
    let all = eventually("three settlement events at MockServer", async || {
        let received = mock.received_on(&http, EVENTS_PATH).await;
        (received.len() >= 3).then_some(received)
    })
    .await;
    let seen: std::collections::BTreeSet<_> = all
        .iter()
        .map(|e| {
            format!(
                "{}:{}",
                e["source"].as_str().unwrap(),
                e["eventType"].as_str().unwrap()
            )
        })
        .collect();
    assert_eq!(
        seen.into_iter().collect::<Vec<_>>(),
        [
            "external:SettlementOutcomeReported",
            "settlement-simulator:SettlementCompleted",
            "settlement-simulator:SettlementPending",
        ]
    );
    let detail = http
        .get(format!("{api}/api/settlements/{execution_id}"))
        .await;
    assert_eq!(
        (
            &detail["simulated"]["status"],
            &detail["external"][0]["status"]
        ),
        (&json!("SETTLED"), &json!("SETTLED"))
    );
}

/// The scenario files, sorted, as paths relative to `scenarios/`.
fn scenario_names() -> Vec<String> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/scenarios");
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.ends_with(".yaml"))
        .collect();
    names.sort();
    names
}

/// Runs `exchange-simulator scenario run --json` on every scenario file and returns the reports.
fn cli_reports(program: &str, prefix: &[&str], dir: &str) -> Value {
    let files: Vec<String> = scenario_names()
        .iter()
        .map(|n| format!("{dir}/{n}"))
        .collect();
    let output = std::process::Command::new(program)
        .args(prefix)
        .args(["scenario", "run", "--json"])
        .args(&files)
        .env_remove("RUST_LOG")
        .output()
        .expect("run the scenario CLI");
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn milestone_3_scenarios_in_containers() {
    build_image();
    let local = cli_reports(
        env!("CARGO_BIN_EXE_exchange-simulator"),
        &[],
        concat!(env!("CARGO_MANIFEST_DIR"), "/scenarios"),
    );
    assert!(
        local
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["passed"] == true),
        "{local}"
    );

    // The CLI in the image, with the files mounted read-only and no network: twice, byte for byte the same.
    let mount = format!("{}/scenarios:/scenarios:ro", env!("CARGO_MANIFEST_DIR"));
    let image = format!("{IMAGE}:{TAG}");
    let docker = [
        "run",
        "--rm",
        "--network",
        "none",
        "-v",
        mount.as_str(),
        image.as_str(),
    ];
    let first = cli_reports("docker", &docker, "/scenarios");
    let second = cli_reports("docker", &docker, "/scenarios");
    assert_eq!(first, second, "two runs in the image");
    assert_eq!(
        first, local,
        "the image and the local build agree, event-log fingerprints included"
    );

    // The API of a running container gives the same reports.
    let http = Http {
        client: reqwest::Client::new(),
    };
    let service = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(8080.tcp())
        .with_wait_for(WaitFor::message_on_stdout("exchange simulator listening"))
        .with_env_var("EXCHANGE__TEST_CONTROL__ENABLED", "true")
        .start()
        .await
        .expect("start service container");
    let api = format!(
        "http://127.0.0.1:{}",
        service.get_host_port_ipv4(8080).await.unwrap()
    );
    for (name, expected) in scenario_names().iter().zip(local.as_array().unwrap()) {
        let text =
            std::fs::read_to_string(format!("{}/scenarios/{name}", env!("CARGO_MANIFEST_DIR")))
                .unwrap();
        let response = http
            .client
            .post(format!("{api}/test/scenarios"))
            .body(text)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200, "{name}");
        let report: Value = response.json().await.unwrap();
        assert_eq!(&report, expected, "{name} over the API");
    }
    // The scenarios ran in sandboxes: the service itself is still empty.
    assert_eq!(http.get(format!("{api}/api/events")).await, json!([]));
}
