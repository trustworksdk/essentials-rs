//! Exchange fees (step 6.22, SR-37): each side of an execution pays a share of its value, the resting order's side
//! the maker rate and the incoming order's the taker rate, at least the minimum. The fees are on `TradeExecuted`, the
//! fill events, the order result and the clearing statement, whose obligations sum them as `feesNet`.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use exchange_simulator::config::{ClockMode, Config};
use exchange_simulator::fees::{FeeRates, FeeSchedule};
use serde_json::{Value, json};

/// Maker 1 bp, taker 2.5 bp, at least 1.00; D pays no taker fee beyond the minimum; C is exempt. Clearing at 17:30
/// UTC from Monday 2030-01-07 09:00.
fn priced(c: &mut Config) {
    c.clock.mode = ClockMode::Virtual;
    c.clock.start = Some("2030-01-07T09:00:00Z".parse().unwrap());
    c.test_control.enabled = true;
    c.clearing.enabled = true;
    c.clearing.cutoff = "17:30".into();
    c.clearing.time_zone = "UTC".into();
    c.fees = FeeSchedule {
        maker_bps: 1.0,
        taker_bps: 2.5,
        minimum: Some("1.00".into()),
        participants: [(
            "D".to_string(),
            FeeRates {
                taker_bps: Some(0.0),
                ..Default::default()
            },
        )]
        .into(),
        exempt: vec!["C".into()],
    };
}

async fn events(app: &TestApp, event_type: &str) -> Vec<Value> {
    app.get(&format!("/api/events?eventType={event_type}&limit=1000"))
        .await
        .1
        .as_array()
        .unwrap()
        .clone()
}

fn minor(fee: &Value) -> i64 {
    fee["amountMinor"].as_i64().unwrap()
}

#[tokio::test]
async fn fees_are_on_the_trade_the_fills_the_result_and_the_statement() {
    let mut app = TestApp::start_with(priced).await;
    app.seed().await;
    // 100 at 100.00 is worth 10,000.00: maker 1 bp is 1.00, taker 2.5 bp is 2.50.
    app.order("A", "SELL", Some(10_000), 100).await;
    let (status, buy) = app.order("B", "BUY", Some(10_000), 100).await;
    assert_eq!(status, StatusCode::CREATED, "{buy}");
    let execution = &buy["executions"][0];
    assert_eq!(
        (
            &execution["fee"]["amount"],
            &execution["buyerFee"]["amount"],
            &execution["sellerFee"]["amount"]
        ),
        (&json!("2.50"), &json!("2.50"), &json!("1.00")),
        "the result carries the buyer's own fee, and both sides'"
    );
    assert_eq!(execution["fee"]["currency"], "DKK");
    // 10 at 100.00 is worth 1,000.00: 2.5 bp would be 0.25, so the minimum; C pays nothing.
    app.order("C", "SELL", Some(10_000), 10).await;
    app.order("B", "BUY", Some(10_000), 10).await;
    // D's own taker rate is 0, but the minimum still applies.
    app.order("A", "BUY", Some(10_000), 10).await;
    app.order("D", "SELL", Some(10_000), 10).await;

    let trades = events(&app, "TradeExecuted").await;
    let fees: Vec<(i64, i64)> = trades
        .iter()
        .map(|t| {
            (
                minor(&t["payload"]["buyerFee"]),
                minor(&t["payload"]["sellerFee"]),
            )
        })
        .collect();
    assert_eq!(fees, [(250, 100), (100, 0), (100, 100)]);
    let fills: Vec<Value> = [
        events(&app, "OrderFilled").await,
        events(&app, "OrderPartiallyFilled").await,
    ]
    .concat();
    for trade in &trades {
        let p = &trade["payload"];
        for (side, fee) in [
            ("buyOrderId", &p["buyerFee"]),
            ("sellOrderId", &p["sellerFee"]),
        ] {
            let fill = fills
                .iter()
                .find(|f| {
                    f["aggregateId"] == p[side] && f["payload"]["executionId"] == p["executionId"]
                })
                .expect("the order's fill event for the execution");
            assert_eq!(
                &fill["payload"]["lastFee"], fee,
                "lastFee is the order's side's"
            );
        }
    }

    let (status, _) = app
        .post("/test/clock/set", json!({ "to": "2030-01-07T18:00:00Z" }))
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, statements) = app.get("/api/clearing/statements?date=2030-01-07").await;
    let b = statements
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["participantId"] == "B")
        .unwrap();
    let line_fees: Vec<&Value> = b["trades"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| &t["fee"])
        .collect();
    assert_eq!(line_fees, [&json!("2.50"), &json!("1.00")]);
    let obligation = &b["obligations"][0];
    assert_eq!(
        (&obligation["feesNet"], &obligation["feesNetMinor"]),
        (&json!("3.50"), &json!(350)),
        "the lines' fees summed"
    );
    assert_eq!(
        obligation["cashNet"], "-11000.00",
        "fees are apart from the cash settled"
    );
    let c = statements
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["participantId"] == "C")
        .unwrap();
    assert_eq!(c["obligations"][0]["feesNet"], "0.00", "exempt");
    app.stop().await;
}

#[tokio::test]
async fn a_correction_reprices_the_fees() {
    let mut app = TestApp::start_with(priced).await;
    app.seed().await;
    app.order("A", "SELL", Some(10_000), 100).await;
    let (_, buy) = app.order("B", "BUY", Some(10_000), 100).await;
    let id = buy["executions"][0]["executionId"].as_str().unwrap();
    let (status, corrected) = app
        .post(
            &format!("/api/trades/{id}/correction"),
            json!({ "priceTicks": 20_000 }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{corrected}");
    assert_eq!(
        (
            &corrected["trade"]["buyerFee"]["amount"],
            &corrected["trade"]["sellerFee"]["amount"]
        ),
        (&json!("5.00"), &json!("2.00")),
        "the incoming buy stays the taker"
    );
    let event = &events(&app, "TradeCorrected").await[0]["payload"];
    assert_eq!(minor(&event["buyerFee"]), 500);
    let fills = events(&app, "OrderFillCorrected").await;
    assert_eq!(minor(&fills[0]["payload"]["lastFee"]), 500);
    assert_eq!(minor(&fills[1]["payload"]["lastFee"]), 200);
    app.stop().await;
}
