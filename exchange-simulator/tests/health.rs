//! The HTTP skeleton: `/health` reports the process and database as ready.

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use exchange_simulator::api::{AppState, router};
use exchange_simulator::persistence;
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn health_reports_ok_with_schema_version() {
    let mut db = rusqlite::Connection::open_in_memory().unwrap();
    persistence::configure(&db, Duration::from_secs(1)).unwrap();
    persistence::migrate(&mut db).unwrap();

    let response = router(AppState::new(db))
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(
        body,
        serde_json::json!({ "status": "ok", "schemaVersion": 1 })
    );
}
