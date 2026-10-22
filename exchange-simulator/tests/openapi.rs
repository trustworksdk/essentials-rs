//! The OpenAPI document (`openapi.yaml`) against the service: the routes it describes are exactly the routes
//! mounted; every operation is exercised, with each request and response validated against the document's schemas;
//! every event type is emitted and validated against its `webhooks` schema; and the parts shared with the payment
//! gateway's document are identical.

mod common;

use axum::body::Body;
use axum::http::{Method, Request};
use common::TestApp;
use exchange_simulator::config::{ClockMode, SettlementMode, SubscriptionConfig};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use simulator_kit::contract::{Spec, routes_in_source, without_parameter_names};
use tower::ServiceExt;

fn spec() -> Spec {
    Spec::from_yaml(exchange_simulator::api::OPENAPI)
}

/// One request through the router; the response is checked against the document, and with `valid` the request
/// too (requests meant to be refused are not). `body` is JSON unless `content_type` says otherwise.
async fn call(
    app: &TestApp,
    spec: &Spec,
    method: Method,
    uri: &str,
    body: Option<Value>,
    content_type: &str,
    valid: bool,
) -> (u16, Value) {
    let request = Request::builder().method(method.clone()).uri(uri);
    let request = match &body {
        Some(Value::String(text)) if content_type != "application/json" => request
            .header("content-type", content_type)
            .body(Body::from(text.clone())),
        Some(json) => {
            if valid {
                spec.check_request(method.as_str(), uri, json);
            }
            request
                .header("content-type", "application/json")
                .body(Body::from(json.to_string()))
        }
        None => request.body(Body::empty()),
    }
    .unwrap();
    let response = app.router().oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let json = response
        .headers()
        .get("content-type")
        .is_some_and(|v| v.to_str().unwrap().starts_with("application/json"));
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else if json {
        serde_json::from_slice(&bytes).unwrap()
    } else {
        Value::String(String::from_utf8_lossy(&bytes).into_owned())
    };
    spec.check_response(method.as_str(), uri, status, &value);
    (status, value)
}

/// [`call`] with a JSON body (or none), expecting `status`; the request is validated when it is expected to succeed.
async fn expect(
    app: &TestApp,
    spec: &Spec,
    status: u16,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> Value {
    let valid = status < 300;
    let (got, value) = call(
        app,
        spec,
        method.clone(),
        uri,
        body,
        "application/json",
        valid,
    )
    .await;
    assert_eq!(got, status, "{method} {uri}: {value}");
    value
}

/// Opens `GET uri`, a server-sent event stream, and checks it answered `200` as `text/event-stream`.
async fn open_stream(app: &TestApp, uri: &str) -> Body {
    let request = Request::get(uri).body(Body::empty()).unwrap();
    let response = app.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200, "GET {uri}");
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    response.into_body()
}

/// Reads what the stream sent until it is quiet for a moment, validates the response and each event's `data` against
/// the schema for its kind, and returns the kinds seen.
async fn drain_stream(
    spec: &Spec,
    uri: &str,
    mut body: Body,
) -> std::collections::BTreeSet<String> {
    let mut text = String::new();
    while let Ok(Some(frame)) =
        tokio::time::timeout(std::time::Duration::from_millis(300), body.frame()).await
    {
        if let Ok(bytes) = frame.unwrap().into_data() {
            text.push_str(std::str::from_utf8(&bytes).unwrap());
        }
    }
    spec.check_response("GET", uri, 200, &json!(text));
    let mut kinds = std::collections::BTreeSet::new();
    for block in text.split("\n\n").filter(|b| b.contains("data:")) {
        let field = |name: &str| {
            block
                .lines()
                .find_map(|l| l.strip_prefix(&format!("{name}:")))
                .unwrap()
                .trim()
                .to_string()
        };
        let (kind, data) = (field("event"), field("data"));
        let data: Value = serde_json::from_str(&data).unwrap();
        assert_eq!(
            field("id"),
            data["mdSequence"].to_string(),
            "`id:` is the mdSequence"
        );
        let schema = match kind.as_str() {
            "quote" => "MarketDataQuote",
            "trade" => "MarketDataTrade",
            "reference" => "MarketDataReference",
            other => panic!("undocumented market-data event {other}"),
        };
        spec.validate(
            &json!({ "$ref": format!("#/components/schemas/{schema}") }),
            &data,
            &format!("market-data event {kind}"),
        );
        kinds.insert(kind);
    }
    kinds
}

#[test]
fn the_document_describes_exactly_the_mounted_routes() {
    let sources = [
        include_str!("../src/api/mod.rs"),
        include_str!("../src/api/settlements.rs"),
        include_str!("../src/api/clearing.rs"),
        include_str!("../src/api/market_data.rs"),
        include_str!("../src/api/liquidity.rs"),
        include_str!("../src/api/test_control.rs"),
        include_str!("../../simulator-kit/src/api/webhooks.rs"),
        include_str!("../../simulator-kit/src/api/clock.rs"),
    ];
    // Path parameter names are the code's own (`{execution_id}` in a route is `{executionId}` in the document).
    assert_eq!(
        without_parameter_names(&routes_in_source(&sources)),
        without_parameter_names(&spec().operations())
    );
}

/// One version for the simulators' APIs: both crates, both documents and the `simulator-api-specs` artifact
/// (`api-specs/pom.xml`). The gateway's suite checks its crate against its document.
#[test]
fn the_simulators_share_one_version() {
    let version = env!("CARGO_PKG_VERSION");
    assert_eq!(spec().document()["info"]["version"], version);
    let gateway = Spec::from_yaml(include_str!("../../payment-gateway/openapi.yaml"));
    assert_eq!(gateway.document()["info"]["version"], version);
    let pom = include_str!("../../api-specs/pom.xml");
    let artifact = pom
        .split_once("<artifactId>simulator-api-specs</artifactId>")
        .and_then(|(_, rest)| rest.split_once("<version>"))
        .and_then(|(_, rest)| rest.split_once("</version>"))
        .map(|(version, _)| version.trim());
    assert_eq!(artifact, Some(version), "the version in api-specs/pom.xml");
}

/// The OpenAPI 3.0 rendition shipped for client generators (`api-specs/`) is the current document's.
#[test]
fn the_openapi_30_rendition_is_current() {
    simulator_kit::openapi30::check_rendition(
        exchange_simulator::api::OPENAPI,
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi-3.0.json"),
    );
}

#[test]
fn the_parts_shared_with_the_payment_gateway_are_identical() {
    let gateway = Spec::from_yaml(include_str!("../../payment-gateway/openapi.yaml"));
    let paths = [
        "/api/webhooks",
        "/api/webhooks/{id}",
        "/api/webhooks/{id}/secret",
        "/api/webhooks/{id}/deliveries",
        "/api/webhooks/{id}/redeliver",
        "/api/webhooks/{id}/dead-letters/retry",
        "/api/clock",
        "/api/clock/freeze",
        "/api/clock/resume",
        "/api/clock/speed",
        "/test/reset",
        "/test/clock",
        "/test/clock/advance",
        "/test/clock/set",
        "/test/clock/freeze",
        "/test/clock/resume",
        "/test/clock/speed",
        "/test/deliveries",
        "/test/dead-letters/retry",
        "/test/webhooks/{id}/redeliver",
        "/test/attempts",
        "/test/webhooks/{id}/faults",
    ];
    let components = [
        "parameters/Limit",
        "parameters/SubscriptionIdPath",
        "parameters/RedeliverAfter",
        "responses/BadRequest",
        "responses/NotFound",
        "responses/Conflict",
        "responses/Unavailable",
        "responses/Clock",
        "responses/WebhookAck",
        "schemas/Error",
        "schemas/Timestamp",
        "schemas/NullableTimestamp",
        "schemas/Clock",
        "schemas/ClockAdvance",
        "schemas/ClockChangedPayload",
        "schemas/EventEnvelope",
        "schemas/DeliveryStatus",
        "schemas/WebhookRequest",
        "schemas/WebhookUpdate",
        "schemas/SecretRotation",
        "schemas/SubscriptionFields",
        "schemas/Subscription",
        "schemas/SubscriptionDetail",
        "schemas/Delivery",
        "schemas/Attempt",
        "schemas/FaultPolicy",
        "schemas/FieldFault",
        "schemas/Queued",
        "schemas/Retried",
    ];
    let exchange = spec().sections(&paths, &components);
    assert!(exchange.values().all(|v| !v.is_null()), "{exchange:#?}");
    assert_eq!(exchange, gateway.sections(&paths, &components));
}

#[tokio::test]
async fn every_operation_and_event_matches_the_document() {
    let spec = spec();
    let mut app = TestApp::start_with(|c| {
        c.clock.mode = ClockMode::Virtual;
        c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
        c.clock.operator_control = true;
        c.settlement.mode = SettlementMode::Both;
        c.settlement.seed = 3;
        c.settlement.reject_probability = 0.15;
        c.settlement.cancel_probability = 0.15;
        c.settlement.fail_probability = 0.15;
        c.settlement.delay_probability = 0.25;
        c.settlement.correct_probability = 0.5;
        c.clearing.enabled = true;
        c.clearing.breaks.late_probability = 0.5;
        c.clearing.breaks.alter_probability = 0.3;
        c.webhooks.subscriptions.push(SubscriptionConfig {
            name: "declared".into(),
            url: "http://127.0.0.1:9/declared".into(),
            event_types: vec!["TradeExecuted".into()],
            secret: None,
            secret_env: None,
            secret_file: None,
            ordered: false,
            enabled: true,
            paused: false,
            participants: None,
        });
    })
    .await;
    let (a, s) = (&app, &spec);
    use Method as M;

    // Service.
    expect(a, s, 200, M::GET, "/health", None).await;
    expect(a, s, 200, M::GET, "/metrics", None).await;
    let served = expect(a, s, 200, M::GET, "/openapi.yaml", None).await;
    assert_eq!(served, json!(exchange_simulator::api::OPENAPI));

    // Instruments and participants.
    let acme = json!({ "id": "ACME", "symbol": "ACME", "mic": "XCSE", "currency": "DKK", "currencyScale": 2 });
    expect(a, s, 201, M::POST, "/api/instruments", Some(acme.clone())).await;
    expect(a, s, 409, M::POST, "/api/instruments", Some(acme)).await;
    let (status, _) = call(
        a,
        s,
        M::POST,
        "/api/instruments",
        Some(json!("not json")),
        "text/plain",
        false,
    )
    .await;
    assert_eq!(status, 400);
    expect(a, s, 200, M::GET, "/api/instruments?mic=XCSE", None).await;
    expect(a, s, 400, M::GET, "/api/instruments?colour=red", None).await;
    for patch in [
        json!({ "enabled": false }),
        json!({ "enabled": true, "referenceSymbol": "ACME.CO" }),
        json!({ "referenceSymbol": null }),
    ] {
        expect(a, s, 200, M::PATCH, "/api/instruments/ACME", Some(patch)).await;
    }
    let synthetic = json!({ "id": "SYN", "symbol": "SYN", "currency": "USD", "currencyScale": 2,
        "priceSource": { "kind": "synthetic", "initialPrice": "100.00", "drift": 0.05, "volatility": 0.4,
                         "tickIntervalMs": 1000, "seed": 3 } });
    expect(a, s, 201, M::POST, "/api/instruments", Some(synthetic)).await;
    let stream_uri = "/api/marketdata/stream?instruments=ACME,SYN";
    let stream = open_stream(a, stream_uri).await;
    expect(
        a,
        s,
        400,
        M::GET,
        "/api/marketdata/stream?instruments=NOPE",
        None,
    )
    .await;
    expect(
        a,
        s,
        200,
        M::PATCH,
        "/api/instruments/ACME",
        Some(json!({ "priceSource": { "kind": "fixed", "price": "100.25" } })),
    )
    .await;
    // Every liquidity field; mode off, so the orders below meet no provider.
    let liquidity = json!({ "mode": "off", "levels": 3, "spreadBps": 10.0, "levelStepTicks": 1, "levelQuantity": 10,
        "levelQuantityGrowth": 1.5, "requoteThresholdTicks": 2, "refillDelayMs": 500, "skewBpsPerLot": 0.5,
        "maxInventory": 100, "stalePolicy": "widen", "staleWidenFactor": 3.0 });
    expect(
        a,
        s,
        200,
        M::PATCH,
        "/api/instruments/ACME",
        Some(json!({ "liquidity": liquidity })),
    )
    .await;
    s.check_request(
        "PATCH",
        "/api/instruments/ACME",
        &json!({ "liquidity": { "mode": "on" } }),
    );
    expect(a, s, 200, M::GET, "/api/marketdata", None).await;
    expect(a, s, 200, M::GET, "/api/marketdata/SYN", None).await;
    expect(a, s, 404, M::GET, "/api/marketdata/NOPE", None).await;
    expect(a, s, 200, M::GET, "/api/liquidity", None).await;
    expect(a, s, 200, M::GET, "/api/liquidity/ACME", None).await;
    expect(a, s, 404, M::GET, "/api/liquidity/NOPE", None).await;
    expect(
        a,
        s,
        200,
        M::PATCH,
        "/api/liquidity/ACME",
        Some(json!({ "spreadBps": 12.0, "requoteThresholdTicks": null })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::PATCH,
        "/api/liquidity/ACME",
        Some(json!({ "spreadBpz": 1 })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::PATCH,
        "/api/liquidity/NOPE",
        Some(json!({ "levels": 2 })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::PATCH,
        "/api/instruments/NOPE",
        Some(json!({ "enabled": true })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::PATCH,
        "/api/instruments/ACME",
        Some(json!({})),
    )
    .await;
    for id in ["A", "B", "C"] {
        let body = json!({ "id": id, "name": format!("Participant {id}"), "settlement": { "account": format!("DK-{id}") } });
        expect(a, s, 201, M::POST, "/api/participants", Some(body)).await;
    }
    expect(
        a,
        s,
        409,
        M::POST,
        "/api/participants",
        Some(json!({ "id": "A", "name": "again" })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        "/api/participants",
        Some(json!({ "id": "D" })),
    )
    .await;

    // Webhooks, before trading, so deliveries and attempts exist.
    let hook = expect(
        a,
        s,
        201,
        M::POST,
        "/api/webhooks",
        Some(json!({ "url": "http://127.0.0.1:9/hook", "eventTypes": ["TradeExecuted", "OrderAccepted"],
                     "secret": "a-secret-of-at-least-16", "ordered": true })),
    )
    .await;
    let hook_id = hook["id"].as_str().unwrap().to_string();
    expect(
        a,
        s,
        400,
        M::POST,
        "/api/webhooks",
        Some(json!({ "url": "ftp://x", "eventTypes": ["TradeExecuted"] })),
    )
    .await;
    let listed = expect(a, s, 200, M::GET, "/api/webhooks", None).await;
    let declared = listed
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["name"] == "declared")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Orders: resting, trading, idempotent, rejected, cancelled, refused.
    let order = |participant: &str, side: &str, price: i64, quantity: u64| {
        json!({ "instrumentId": "ACME", "participantId": participant, "side": side, "type": "LIMIT",
                "priceTicks": price, "quantity": quantity })
    };
    let mut resting = Vec::new();
    for i in 0..24 {
        let price = 10_000 + i;
        let sell = expect(
            a,
            s,
            201,
            M::POST,
            "/api/orders",
            Some(order("A", "SELL", price, 20)),
        )
        .await;
        resting.push(sell["order"]["orderId"].as_str().unwrap().to_string());
        expect(
            a,
            s,
            201,
            M::POST,
            "/api/orders",
            Some(order("B", "BUY", price, 10)),
        )
        .await;
    }
    let mut keyed = order("C", "BUY", 9_000, 5);
    keyed["clientOrderId"] = json!("c-1");
    expect(a, s, 201, M::POST, "/api/orders", Some(keyed.clone())).await;
    let again = expect(a, s, 200, M::POST, "/api/orders", Some(keyed.clone())).await;
    assert_eq!(again["duplicate"], true);
    keyed["quantity"] = json!(6);
    expect(a, s, 409, M::POST, "/api/orders", Some(keyed)).await;
    let self_trade = expect(
        a,
        s,
        201,
        M::POST,
        "/api/orders",
        Some(order("A", "BUY", 10_023, 1)),
    )
    .await;
    assert_eq!(self_trade["order"]["status"], "REJECTED");
    let mut market = order("C", "BUY", 0, 500);
    market["type"] = json!("MARKET");
    market["priceTicks"] = Value::Null;
    expect(a, s, 201, M::POST, "/api/orders", Some(market)).await;
    assert_eq!(
        drain_stream(s, stream_uri, stream).await,
        ["quote", "reference", "trade"].map(String::from).into(),
        "every kind of market-data event"
    );
    // Time in force: a DAY order rests until the business day's cutoff (ACME's market has no session), where the
    // clock move below expires it; an IOC remainder is cancelled; a market order is always IOC.
    let mut day = order("C", "BUY", 9_001, 3);
    day["timeInForce"] = json!("DAY");
    let day = expect(a, s, 201, M::POST, "/api/orders", Some(day)).await;
    assert_eq!(day["order"]["status"], "OPEN");
    let mut ioc = order("C", "BUY", 9_002, 3);
    ioc["timeInForce"] = json!("IOC");
    let ioc = expect(a, s, 201, M::POST, "/api/orders", Some(ioc)).await;
    assert_eq!(ioc["order"]["status"], "CANCELLED");
    let mut gtc_market = order("C", "BUY", 0, 1);
    gtc_market["type"] = json!("MARKET");
    gtc_market["priceTicks"] = Value::Null;
    gtc_market["timeInForce"] = json!("GTC");
    expect(a, s, 400, M::POST, "/api/orders", Some(gtc_market)).await;
    let mut unknown = order("A", "SELL", 10_000, 1);
    unknown["instrumentId"] = json!("NOPE");
    expect(a, s, 422, M::POST, "/api/orders", Some(unknown)).await;
    expect(
        a,
        s,
        400,
        M::POST,
        "/api/orders",
        Some(order("A", "SELL", 10_000, 0)),
    )
    .await;
    let open = expect(
        a,
        s,
        201,
        M::POST,
        "/api/orders",
        Some(order("C", "SELL", 20_000, 3)),
    )
    .await;
    let open_id = open["order"]["orderId"].as_str().unwrap().to_string();
    expect(a, s, 200, M::GET, &format!("/api/orders/{open_id}"), None).await;
    expect(a, s, 400, M::GET, "/api/orders/not-a-uuid", None).await;
    expect(
        a,
        s,
        404,
        M::GET,
        "/api/orders/00000000-0000-0000-0000-000000000000",
        None,
    )
    .await;
    expect(
        a,
        s,
        200,
        M::DELETE,
        &format!("/api/orders/{open_id}"),
        None,
    )
    .await;
    expect(
        a,
        s,
        409,
        M::DELETE,
        &format!("/api/orders/{open_id}"),
        None,
    )
    .await;
    expect(
        a,
        s,
        404,
        M::DELETE,
        "/api/orders/00000000-0000-0000-0000-000000000000",
        None,
    )
    .await;
    let by_client = "/api/orders?participantId=C&clientOrderId=c-1";
    expect(a, s, 200, M::DELETE, by_client, None).await;
    expect(a, s, 409, M::DELETE, by_client, None).await;
    expect(
        a,
        s,
        404,
        M::DELETE,
        "/api/orders?participantId=C&clientOrderId=nope",
        None,
    )
    .await;
    expect(a, s, 400, M::DELETE, "/api/orders?clientOrderId=c-1", None).await;
    expect(a, s, 200, M::GET, "/api/orderbooks/ACME", None).await;
    expect(a, s, 404, M::GET, "/api/orderbooks/NOPE", None).await;
    let trades = expect(
        a,
        s,
        200,
        M::GET,
        "/api/trades?participantId=B&limit=1000",
        None,
    )
    .await;
    let execution = trades[0]["executionId"].as_str().unwrap().to_string();
    expect(a, s, 400, M::GET, "/api/trades?limit=0", None).await;

    // Trade corrections and busts (SR-36), on the last trade: corrected, then busted.
    let last = trades.as_array().unwrap().last().unwrap()["executionId"]
        .as_str()
        .unwrap()
        .to_string();
    let price = trades.as_array().unwrap().last().unwrap()["priceTicks"]
        .as_i64()
        .unwrap();
    let correction = format!("/api/trades/{last}/correction");
    let bust = format!("/api/trades/{last}/bust");
    expect(
        a,
        s,
        200,
        M::POST,
        &correction,
        Some(json!({ "priceTicks": price + 1, "reason": "FAT_FINGER" })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        &correction,
        Some(json!({ "quantity": 1_000_000 })),
    )
    .await;
    expect(a, s, 200, M::POST, &bust, Some(json!({}))).await;
    expect(a, s, 409, M::POST, &bust, Some(json!({}))).await;
    expect(
        a,
        s,
        404,
        M::POST,
        "/api/trades/00000000-0000-0000-0000-000000000000/bust",
        Some(json!({})),
    )
    .await;

    // Settlement, simulated and reported.
    expect(a, s, 200, M::GET, "/api/settlements?status=PENDING", None).await;
    expect(a, s, 400, M::GET, "/api/settlements?status=LOST", None).await;
    expect(
        a,
        s,
        200,
        M::GET,
        &format!("/api/settlements/{execution}"),
        None,
    )
    .await;
    expect(
        a,
        s,
        404,
        M::GET,
        "/api/settlements/00000000-0000-0000-0000-000000000000",
        None,
    )
    .await;
    expect(a, s, 400, M::GET, "/api/settlements/not-a-uuid", None).await;
    let outcome = format!("/api/settlements/{execution}/outcomes");
    let report = json!({ "status": "SETTLED", "reference": "csd-1", "details": { "csd": "VP" } });
    expect(a, s, 201, M::POST, &outcome, Some(report.clone())).await;
    expect(a, s, 200, M::POST, &outcome, Some(report)).await;
    expect(
        a,
        s,
        409,
        M::POST,
        &outcome,
        Some(json!({ "status": "FAILED", "reference": "csd-1" })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::POST,
        "/api/settlements/00000000-0000-0000-0000-000000000000/outcomes",
        Some(json!({ "status": "SETTLED" })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        &outcome,
        Some(json!({ "status": "LOST" })),
    )
    .await;

    // The clock: past the cutoff, the settlement dates and corrections.
    expect(a, s, 200, M::GET, "/test/clock", None).await;
    expect(a, s, 200, M::GET, "/api/clock", None).await;
    expect(
        a,
        s,
        200,
        M::POST,
        "/api/clock/speed",
        Some(json!({ "speed": 2 })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        "/api/clock/speed",
        Some(json!({ "speed": 0 })),
    )
    .await;
    expect(a, s, 200, M::POST, "/api/clock/resume", None).await;
    expect(a, s, 200, M::POST, "/api/clock/freeze", None).await;
    expect(
        a,
        s,
        200,
        M::POST,
        "/test/clock/set",
        Some(json!({ "to": "2030-01-07T18:00:00Z" })),
    )
    .await;
    expect(
        a,
        s,
        409,
        M::POST,
        "/test/clock/set",
        Some(json!({ "to": "2030-01-01T00:00:00Z" })),
    )
    .await;
    expect(a, s, 400, M::POST, "/test/clock/advance", Some(json!({}))).await;
    expect(
        a,
        s,
        200,
        M::POST,
        "/test/clock/advance",
        Some(json!({ "days": 12 })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        "/test/clock/speed",
        Some(json!({ "speed": 0 })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        "/test/clock/speed",
        Some(json!({ "speed": 2 })),
    )
    .await;
    expect(a, s, 200, M::POST, "/test/clock/resume", None).await;
    expect(a, s, 200, M::POST, "/test/clock/freeze", None).await;
    expect(a, s, 200, M::GET, "/api/settlements?limit=1000", None).await;
    let detail = expect(
        a,
        s,
        200,
        M::GET,
        &format!("/api/settlements/{execution}"),
        None,
    )
    .await;
    assert!(!detail["simulatedEvents"].as_array().unwrap().is_empty());

    // Clearing.
    let statements = expect(
        a,
        s,
        200,
        M::GET,
        "/api/clearing/statements?date=2030-01-07",
        None,
    )
    .await;
    let statement = statements[0]["statementId"].as_str().unwrap().to_string();
    expect(
        a,
        s,
        200,
        M::GET,
        &format!("/api/clearing/statements/{statement}"),
        None,
    )
    .await;
    expect(
        a,
        s,
        404,
        M::GET,
        "/api/clearing/statements/CS:2030-01-07:Z",
        None,
    )
    .await;
    expect(
        a,
        s,
        400,
        M::GET,
        "/api/clearing/statements?date=monday",
        None,
    )
    .await;
    expect(a, s, 200, M::GET, "/test/clearing/breaks", None).await;
    expect(a, s, 400, M::GET, "/test/clearing/breaks?date=monday", None).await;

    // Webhook maintenance and test control over deliveries.
    let hook_url = format!("/api/webhooks/{hook_id}");
    expect(a, s, 200, M::GET, &hook_url, None).await;
    expect(
        a,
        s,
        404,
        M::GET,
        "/api/webhooks/00000000-0000-0000-0000-000000000000",
        None,
    )
    .await;
    expect(a, s, 400, M::GET, "/api/webhooks/nope", None).await;
    expect(
        a,
        s,
        200,
        M::PATCH,
        &hook_url,
        Some(json!({ "paused": true })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::PATCH,
        &hook_url,
        Some(json!({ "eventTypes": ["Nonsense"] })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::PATCH,
        "/api/webhooks/00000000-0000-0000-0000-000000000000",
        Some(json!({ "paused": true })),
    )
    .await;
    expect(
        a,
        s,
        409,
        M::PATCH,
        &format!("/api/webhooks/{declared}"),
        Some(json!({ "url": "http://127.0.0.1:9/x" })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        &format!("{hook_url}/secret"),
        Some(json!({ "secret": "another-secret-of-16+", "graceSeconds": 60 })),
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        &format!("{hook_url}/secret"),
        Some(json!({ "secret": "short" })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::POST,
        "/api/webhooks/00000000-0000-0000-0000-000000000000/secret",
        Some(json!({ "secret": "another-secret-of-16+" })),
    )
    .await;
    expect(
        a,
        s,
        409,
        M::POST,
        &format!("/api/webhooks/{declared}/secret"),
        Some(json!({ "secret": "another-secret-of-16+" })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::GET,
        &format!("{hook_url}/deliveries?limit=5"),
        None,
    )
    .await;
    expect(
        a,
        s,
        400,
        M::GET,
        &format!("{hook_url}/deliveries?status=LOST"),
        None,
    )
    .await;
    expect(
        a,
        s,
        404,
        M::GET,
        "/api/webhooks/00000000-0000-0000-0000-000000000000/deliveries",
        None,
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        &format!("{hook_url}/redeliver?after=0"),
        None,
    )
    .await;
    expect(
        a,
        s,
        404,
        M::POST,
        "/api/webhooks/00000000-0000-0000-0000-000000000000/redeliver",
        None,
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        &format!("{hook_url}/dead-letters/retry"),
        Some(json!({ "deliveryIds": [] })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::POST,
        "/api/webhooks/00000000-0000-0000-0000-000000000000/dead-letters/retry",
        None,
    )
    .await;
    expect(
        a,
        s,
        400,
        M::POST,
        &format!("{hook_url}/dead-letters/retry"),
        Some(json!({ "deliveryIds": "all" })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::PATCH,
        &hook_url,
        Some(json!({ "paused": false })),
    )
    .await;
    expect(a, s, 200, M::GET, "/test/deliveries?status=PENDING", None).await;
    expect(a, s, 400, M::GET, "/test/deliveries?status=LOST", None).await;
    expect(a, s, 200, M::POST, "/test/dead-letters/retry", None).await;
    expect(
        a,
        s,
        404,
        M::POST,
        "/test/dead-letters/retry",
        Some(json!({ "subscriptionId": "00000000-0000-0000-0000-000000000000" })),
    )
    .await;
    expect(
        a,
        s,
        200,
        M::POST,
        &format!("/test/webhooks/{hook_id}/redeliver"),
        None,
    )
    .await;
    expect(
        a,
        s,
        404,
        M::POST,
        "/test/webhooks/00000000-0000-0000-0000-000000000000/redeliver",
        None,
    )
    .await;
    let faults = format!("/test/webhooks/{declared}/faults");
    let policy = json!({ "seed": 1, "missing": { "probability": 0.5, "paths": ["payload.price"] }, "duplicateProbability": 0.5,
                         "delay": { "ms": 10 }, "outOfOrder": { "holdMs": 100 } });
    expect(a, s, 200, M::PUT, &faults, Some(policy)).await;
    expect(
        a,
        s,
        400,
        M::PUT,
        &faults,
        Some(json!({ "duplicateProbability": 2 })),
    )
    .await;
    expect(
        a,
        s,
        404,
        M::PUT,
        "/test/webhooks/00000000-0000-0000-0000-000000000000/faults",
        Some(json!({})),
    )
    .await;
    let with_faults = expect(a, s, 200, M::GET, "/api/webhooks", None).await;
    assert!(
        with_faults
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["faults"].is_object())
    );
    expect(a, s, 200, M::DELETE, &faults, None).await;
    expect(
        a,
        s,
        404,
        M::DELETE,
        "/test/webhooks/00000000-0000-0000-0000-000000000000/faults",
        None,
    )
    .await;
    // Delivery attempts to the unreachable endpoints.
    let mut attempts = Value::Null;
    for _ in 0..200 {
        attempts = expect(a, s, 200, M::GET, "/test/attempts", None).await;
        if !attempts.as_array().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(!attempts.as_array().unwrap().is_empty());
    expect(
        a,
        s,
        200,
        M::GET,
        &format!("/test/attempts?subscriptionId={hook_id}"),
        None,
    )
    .await;
    expect(
        a,
        s,
        409,
        M::DELETE,
        &format!("/api/webhooks/{declared}"),
        None,
    )
    .await;
    expect(a, s, 204, M::DELETE, &hook_url, None).await;
    expect(a, s, 404, M::DELETE, &hook_url, None).await;
    expect(a, s, 200, M::GET, "/api/webhooks?includeDeleted=true", None).await;

    // Every event emitted matches its schema, and every event type the document describes occurred.
    let events = expect(a, s, 200, M::GET, "/api/events?limit=1000", None).await;
    expect(
        a,
        s,
        200,
        M::GET,
        "/api/events?eventType=TradeExecuted&after=3",
        None,
    )
    .await;
    expect(a, s, 400, M::GET, "/api/events?limit=5000", None).await;
    expect(
        a,
        s,
        200,
        M::GET,
        "/api/events?eventType=TradeExecuted,OrderFilled",
        None,
    )
    .await;
    expect(a, s, 400, M::GET, "/api/events?eventType=Nope", None).await;
    expect(
        a,
        s,
        200,
        M::GET,
        "/api/calendar?from=2030-01-07&to=2030-01-13",
        None,
    )
    .await;
    expect(a, s, 400, M::GET, "/api/calendar?from=2030-01-07", None).await;
    let mut seen = std::collections::BTreeSet::new();
    for event in events.as_array().unwrap() {
        spec.check_event(event);
        seen.insert(event["eventType"].as_str().unwrap().to_string());
    }
    assert_eq!(seen, spec.event_types(), "every event type occurs");

    // Scenarios and reset.
    let scenario = "name: tiny\nsteps:\n  - expect: { trades: {}, count: 0 }\n";
    let (status, report) = call(
        a,
        s,
        M::POST,
        "/test/scenarios",
        Some(json!(scenario)),
        "application/yaml",
        true,
    )
    .await;
    assert_eq!((status, &report["passed"]), (200, &json!(true)));
    let (status, _) = call(
        a,
        s,
        M::POST,
        "/test/scenarios",
        Some(json!("steps: [")),
        "application/yaml",
        false,
    )
    .await;
    assert_eq!(status, 400);
    expect(a, s, 204, M::POST, "/test/reset", None).await;

    assert_eq!(
        spec.uncovered(),
        Default::default(),
        "operations the test did not exercise"
    );
    app.stop().await;
}
