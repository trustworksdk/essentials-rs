//! End-of-day bank statements through the service (D14, D16): issued at each cutoff per booked currency, balances
//! carried from day to day, returns as reversals, in time order with payments, exactly once across restarts, with
//! seeded breaks.

mod common;

use axum::http::StatusCode;
use common::TestApp;
use payment_gateway::statement::BreakPolicy;
use serde_json::{Value, json};

async fn statements(app: &TestApp, query: &str) -> Vec<Value> {
    let (status, body) = app.get(&format!("/api/statements{query}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body.as_array().unwrap().clone()
}

#[tokio::test]
async fn a_statement_lists_the_days_bookings_between_its_balances_and_balances_carry_over() {
    let mut app = TestApp::start_with(|c| {
        c.statement
            .opening_balances
            .insert("DKK".into(), "10000.00".into());
        c.payments.return_probability = 0.0;
    })
    .await;
    app.pay("d-1", "DEPOSIT", "1500.00", "DKK").await;
    app.pay("w-1", "WITHDRAWAL", "400.00", "DKK").await;
    app.set_clock("2030-01-07T17:29:59Z").await;
    assert!(
        statements(&app, "").await.is_empty(),
        "not before the cutoff (17:30 UTC)"
    );
    app.set_clock("2030-01-07T17:30:00Z").await;
    let monday = statements(&app, "?date=2030-01-07").await;
    assert_eq!(monday.len(), 1, "one per booked currency");
    let s = &monday[0];
    assert_eq!(s["statementId"], "BS:2030-01-07:DKK");
    assert_eq!(s["accountIban"], "DK5000400440116243");
    assert_eq!(s["issuedAt"], "2030-01-07T17:30:00Z");
    assert_eq!(s["openingBalance"], "10000.00");
    assert_eq!(
        (&s["totalCreditsMinor"], &s["totalDebitsMinor"]),
        (&json!(150_000), &json!(-40_000))
    );
    assert_eq!(s["closingBalance"], "11100.00");
    let entries = s["entries"].as_array().unwrap();
    let kinds: Vec<(&str, &str)> = entries
        .iter()
        .map(|e| {
            (
                e["kind"].as_str().unwrap(),
                e["direction"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(kinds, [("DEPOSIT", "CREDIT"), ("WITHDRAWAL", "DEBIT")]);
    assert_eq!(entries[0]["endToEndId"], "d-1");
    assert_eq!(entries[0]["counterparty"]["iban"], "DE89370400440532013000");
    let (_, one) = app.get("/api/statements/BS:2030-01-07:DKK").await;
    assert_eq!(&one, s);
    let issued = app.events("BankStatementIssued").await;
    assert_eq!(issued.len(), 1);
    assert_eq!(issued[0]["payload"], *s);
    assert_eq!(issued[0]["source"], "bank");

    // Tuesday: a EUR deposit opens a second account; DKK has no movements but still gets its statement.
    app.set_clock("2030-01-08T09:00:00Z").await;
    app.pay("d-2", "DEPOSIT", "20.00", "EUR").await;
    app.set_clock("2030-01-08T17:30:00Z").await;
    let tuesday = statements(&app, "?date=2030-01-08").await;
    let ids: Vec<&str> = tuesday
        .iter()
        .map(|s| s["statementId"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["BS:2030-01-08:DKK", "BS:2030-01-08:EUR"]);
    assert_eq!(tuesday[0]["openingBalance"], "11100.00", "Monday's closing");
    assert_eq!(tuesday[0]["entries"], json!([]));
    assert_eq!(tuesday[1]["closingBalance"], "20.00");
    assert_eq!(statements(&app, "?currency=eur").await.len(), 1);
    assert_eq!(
        app.get("/api/statements/BS:2030-01-09:DKK").await.0,
        StatusCode::NOT_FOUND
    );
    app.stop().await;
}

#[tokio::test]
async fn returns_are_reversals_and_a_week_long_clock_move_stays_in_time_order() {
    let mut app = TestApp::start_with(|c| {
        c.statement.cutoff = "17:30".into();
        c.statement.time_zone = "Europe/Copenhagen".into();
        c.payments.return_probability = 1.0;
        c.payments.max_return_days = 1;
    })
    .await;
    app.pay("d-1", "DEPOSIT", "100.00", "DKK").await;
    // Friday after the Copenhagen cutoff: the deposit lands on the next business day's statement.
    app.set_clock("2030-01-11T16:45:00Z").await;
    app.pay("d-2", "DEPOSIT", "50.00", "DKK").await;
    app.advance(json!({ "days": 7 })).await;
    let all = statements(&app, "").await;
    let dates: Vec<&str> = all
        .iter()
        .map(|s| s["businessDate"].as_str().unwrap())
        .collect();
    assert_eq!(
        dates,
        [
            "2030-01-07",
            "2030-01-08",
            "2030-01-09",
            "2030-01-10",
            "2030-01-11",
            "2030-01-14",
            "2030-01-15",
            "2030-01-16",
            "2030-01-17",
            "2030-01-18"
        ]
    );
    let entries = |date: &str| -> Vec<(String, i64)> {
        all.iter().find(|s| s["businessDate"] == date).unwrap()["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    e["kind"].as_str().unwrap().to_string(),
                    e["amountMinor"].as_i64().unwrap(),
                )
            })
            .collect()
    };
    assert_eq!(entries("2030-01-07"), [("DEPOSIT".to_string(), 10_000)]);
    assert_eq!(
        entries("2030-01-08"),
        [("DEPOSIT_RETURN".to_string(), -10_000)],
        "reversed a business day on"
    );
    assert_eq!(
        entries("2030-01-14"),
        [("DEPOSIT".to_string(), 5_000)],
        "after Friday's cutoff"
    );
    for pair in all.windows(2) {
        assert_eq!(
            pair[1]["openingBalanceMinor"],
            pair[0]["closingBalanceMinor"]
        );
    }
    assert_eq!(
        all.last().unwrap()["closingBalance"],
        "0.00",
        "everything was returned"
    );
    let stamps: Vec<String> = app
        .all_events()
        .await
        .iter()
        .map(|e| e["occurredAt"].as_str().unwrap().to_string())
        .collect();
    assert!(stamps.is_sorted(), "{stamps:#?}");
    app.stop().await;
}

#[tokio::test]
async fn late_statements_wait_across_a_restart_and_every_break_is_listed() {
    let mut app = TestApp::start_with(|c| {
        c.statement.breaks = BreakPolicy {
            seed: 4,
            alter_probability: 1.0,
            phantom_probability: 1.0,
            late_probability: 1.0,
            max_late_minutes: 20,
            ..BreakPolicy::default()
        };
    })
    .await;
    app.pay("d-1", "DEPOSIT", "100.00", "DKK").await;
    app.set_clock("2030-01-07T17:30:00Z").await;
    assert!(statements(&app, "").await.is_empty(), "late");
    let (_, breaks) = app.get("/test/statements/breaks").await;
    let record = &breaks[0];
    assert_eq!(record["issued"], false);
    let kinds: Vec<&str> = record["breaks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["AMOUNT_CHANGED", "PHANTOM_ENTRY", "LATE_STATEMENT"]);
    app.restart().await;
    app.advance(json!({ "minutes": 20 })).await;
    let issued = statements(&app, "").await;
    assert_eq!(issued.len(), 1);
    let s = &issued[0];
    assert_eq!(s["issuedAt"], record["issuedAt"]);
    let entries = s["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2, "the real one, altered, and a phantom");
    let listed: i64 = entries
        .iter()
        .map(|e| e["amountMinor"].as_i64().unwrap())
        .sum();
    assert_eq!(
        s["closingBalanceMinor"].as_i64().unwrap(),
        listed,
        "consistent with what it lists"
    );
    assert_ne!(listed, 2 * 10_000, "but not with the bookings");
    assert_eq!(
        app.events("BankStatementIssued").await.len(),
        1,
        "issued once"
    );

    // The next day opens at the canonical balance, so the break shows there too.
    app.set_clock("2030-01-08T17:51:00Z").await; // past the latest a late statement can come
    let (status, tuesday) = app.get("/api/statements/BS:2030-01-08:DKK").await;
    assert_eq!(status, StatusCode::OK, "{tuesday}");
    assert_eq!(
        tuesday["openingBalanceMinor"], 10_000,
        "the bookings, not Monday's broken closing"
    );
    assert_ne!(tuesday["openingBalanceMinor"], s["closingBalanceMinor"]);
    app.stop().await;
}

#[tokio::test]
async fn statements_can_be_turned_off_and_a_reset_starts_them_again() {
    let mut off = TestApp::start_with(|c| c.statement.enabled = false).await;
    off.pay("d-1", "DEPOSIT", "1.00", "DKK").await;
    off.advance(json!({ "days": 2 })).await;
    assert!(statements(&off, "").await.is_empty());
    off.stop().await;

    let mut app = TestApp::start().await;
    app.pay("d-1", "DEPOSIT", "1.00", "DKK").await;
    app.set_clock("2030-01-07T18:00:00Z").await;
    assert_eq!(statements(&app, "").await.len(), 1);
    app.call(axum::http::Method::POST, "/test/reset", None)
        .await;
    assert!(statements(&app, "").await.is_empty());
    app.pay("d-1", "DEPOSIT", "1.00", "DKK").await;
    app.set_clock("2030-01-07T18:00:00Z").await;
    assert_eq!(
        statements(&app, "").await.len(),
        1,
        "the same day again after a reset"
    );
    app.stop().await;
}

/// SR-33: a completed or returned payment carries the value date of its booking, the date of the statement entry
/// that lists it, on the event and on `GET /api/payments`.
#[tokio::test]
async fn completed_and_returned_payments_carry_their_bookings_value_dates() {
    let mut app = TestApp::start().await;
    app.set_clock("2030-01-08T17:31:00Z").await; // Tuesday, past the 17:30 cutoff.
    let deposit = app.pay("d-1", "DEPOSIT", "100.00", "DKK").await;
    assert_eq!(deposit["valueDate"], Value::Null, "nothing booked yet");
    let id = deposit["paymentId"].as_str().unwrap();
    let (status, body) = app
        .post(
            &format!("/test/payments/{id}/outcome"),
            json!({ "outcome": "RETURNED", "returnAfterSeconds": 86_400 }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    app.set_clock("2030-01-11T18:00:00Z").await;

    let entry_date = |date: &str, kind: &str| {
        let date = date.to_string();
        let kind = kind.to_string();
        let app = &app;
        async move {
            let day = statements(app, &format!("?date={date}")).await;
            let entries: Vec<Value> = day
                .iter()
                .flat_map(|s| s["entries"].as_array().unwrap().clone())
                .filter(|e| e["kind"] == kind.as_str())
                .collect();
            assert_eq!(entries.len(), 1, "{kind} on {date}'s statement: {day:?}");
            entries[0]["valueDate"].clone()
        }
    };
    let completed = &app.events("PaymentCompleted").await[0]["payload"];
    assert_eq!(
        completed["valueDate"], "2030-01-09",
        "completed after Tuesday's cutoff"
    );
    assert_eq!(
        completed["valueDate"],
        entry_date("2030-01-09", "DEPOSIT").await
    );
    let returned = &app.events("PaymentReturned").await[0]["payload"];
    assert_eq!(
        returned["valueDate"], "2030-01-10",
        "returned after Wednesday's cutoff"
    );
    assert_eq!(
        returned["valueDate"],
        entry_date("2030-01-10", "DEPOSIT_RETURN").await
    );
    let (_, payment) = app.get(&format!("/api/payments/{id}")).await;
    assert_eq!(payment["valueDate"], "2030-01-10", "the latest booking's");
    let (_, listed) = app.get("/api/payments").await;
    assert_eq!(listed[0]["valueDate"], "2030-01-10");
    app.stop().await;
}

/// Payments completed before payments carried value dates are dated at the next startup, on the current calendar.
#[tokio::test]
async fn payments_stored_before_they_carried_value_dates_are_dated_at_startup() {
    let mut app = TestApp::start().await;
    app.set_clock("2030-01-08T17:31:00Z").await;
    let deposit = app.pay("d-1", "DEPOSIT", "100.00", "DKK").await;
    app.set_clock("2030-01-08T23:00:00Z").await;
    app.db()
        .execute("UPDATE payments SET value_date = NULL", [])
        .unwrap();
    app.restart().await;
    let id = deposit["paymentId"].as_str().unwrap();
    let (_, payment) = app.get(&format!("/api/payments/{id}")).await;
    assert_eq!(
        (&payment["status"], &payment["valueDate"]),
        (&json!("COMPLETED"), &json!("2030-01-09"))
    );
    app.stop().await;
}
