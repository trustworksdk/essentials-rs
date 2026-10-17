//! The persistence-mode gauge. It is process-wide, so it gets a test binary of its own: other tests starting services
//! in other modes at the same time would overwrite it.

mod common;

use axum::body::Body;
use axum::http::Request;
use common::TestApp;
use exchange_simulator::config::PersistenceMode;
use tower::ServiceExt;

#[tokio::test]
async fn metrics_report_the_persistence_mode() {
    for (mode, name) in [
        (PersistenceMode::Fast, "fast"),
        (PersistenceMode::Memory, "memory"),
        (PersistenceMode::Durable, "durable"),
    ] {
        let mut app = TestApp::start_with(|c| c.database.mode = mode).await;
        let response = app
            .router()
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap();
        for other in ["durable", "fast", "memory"] {
            let expected = if other == name { 1 } else { 0 };
            assert!(
                text.contains(&format!(
                    r#"exchange_persistence_mode{{mode="{other}"}} {expected}"#
                )),
                "{name}: {other} should be {expected} in {text}"
            );
        }
        app.stop().await;
    }
}
