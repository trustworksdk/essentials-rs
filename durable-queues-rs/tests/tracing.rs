//! Phase 4: spans per message and per storage operation, structured fields only, and payloads never logged.

mod common;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{TestDatabase, eventually};
use durable_queues_rs::{
    ConsumerConfig, HandlerError, Message, MessageType, QueueName, QueuedMessage, RedeliveryPolicy,
};
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

const SECRET: &str = "SECRET-4111-1111";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Card {
    number: String,
    fail: bool,
}

impl MessageType for Card {
    const TYPE_NAME: &'static str = "com.acme.Card";
}

type Fields = HashMap<String, String>;

/// Records every span (name and fields, including later `record`s) and every event.
#[derive(Clone, Default)]
struct Capture {
    spans: Arc<Mutex<HashMap<u64, (String, Fields)>>>,
    events: Arc<Mutex<Vec<Fields>>>,
}

struct FieldVisitor<'a>(&'a mut Fields);

impl Visit for FieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _: Context<'_, S>) {
        let mut fields = Fields::new();
        attrs.record(&mut FieldVisitor(&mut fields));
        self.spans
            .lock()
            .unwrap()
            .insert(id.into_u64(), (attrs.metadata().name().to_owned(), fields));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _: Context<'_, S>) {
        if let Some((_, fields)) = self.spans.lock().unwrap().get_mut(&id.into_u64()) {
            values.record(&mut FieldVisitor(fields));
        }
    }

    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields::new();
        event.record(&mut FieldVisitor(&mut fields));
        self.events.lock().unwrap().push(fields);
    }
}

impl Capture {
    fn spans_named(&self, name: &str) -> Vec<Fields> {
        let spans = self.spans.lock().unwrap();
        spans
            .values()
            .filter(|(n, _)| n == name)
            .map(|(_, f)| f.clone())
            .collect()
    }

    fn everything(&self) -> String {
        let spans = self.spans.lock().unwrap();
        let events = self.events.lock().unwrap();
        format!("{:?}{:?}", spans.values().collect::<Vec<_>>(), *events)
    }
}

#[tokio::test] // current-thread runtime: every task runs under the thread-local subscriber below
async fn deliveries_and_storage_operations_are_traced_without_payloads() {
    let capture = Capture::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let db = TestDatabase::start().await;
    let queues = db.durable_queues(db.pool.clone()).await;
    let queue = QueueName::from("Cards");

    let ok = queues
        .queue_message(
            &queue,
            &Message::new(Card {
                number: SECRET.into(),
                fail: false,
            }),
        )
        .await;
    let ok = ok.unwrap();
    let failing = queues
        .queue_message(
            &queue,
            &Message::new(Card {
                number: SECRET.into(),
                fail: true,
            }),
        )
        .await;
    let failing = failing.unwrap();
    // A payload serde rejects with an error that quotes it: `invalid type: string "SECRET…", expected a boolean`.
    sqlx::query(
        "INSERT INTO durable_queues (id, queue_name, message_payload, message_payload_type, added_ts, next_delivery_ts,
                                     delivery_mode, meta_data)
         VALUES ('undecodable', 'Cards', jsonb_build_object('number', 'x', 'fail', $1::text), 'com.acme.Card',
                 now(), now(), 'NORMAL', '{}')",
    )
    .bind(SECRET)
    .execute(&db.pool)
    .await
    .unwrap();

    let handler = |message: QueuedMessage<Card>| async move {
        match message.message.payload.fail {
            true => Err(HandlerError::transient("card declined")),
            false => Ok(()),
        }
    };
    let policy = RedeliveryPolicy::fixed_backoff(Duration::from_secs(3600), 3);
    let consumer = queues
        .consume_from_queue(ConsumerConfig::new(queue, policy), handler)
        .unwrap();
    eventually("all three deliveries to be settled", || async {
        db.count("SELECT count(*) FROM durable_queues WHERE NOT is_being_delivered AND total_attempts = 1").await == 2
            && db.count("SELECT count(*) FROM durable_queues").await == 2
    })
    .await;
    consumer.stop().await;

    let deliveries = capture.spans_named("deliver_message");
    let by_id = |id: &str| {
        deliveries
            .iter()
            .find(|f| f["queue_entry_id"] == id)
            .cloned()
            .unwrap()
    };
    let delivered = by_id(ok.as_str());
    assert_eq!(delivered["queue_name"], "Cards");
    assert_eq!(
        (
            delivered["attempt"].as_str(),
            delivered["redelivery_attempts"].as_str()
        ),
        ("1", "0")
    );
    assert_eq!(delivered["outcome"], "acknowledged");
    assert_eq!(by_id(failing.as_str())["outcome"], "retry");
    assert_eq!(by_id("undecodable")["outcome"], "dead_lettered_undecodable");

    assert_eq!(capture.spans_named("durable_queue_consumer").len(), 1);
    let claims = capture.spans_named("durable_queues.claim");
    assert!(
        claims
            .iter()
            .all(|f| f["queue_name"] == "Cards" && f["table"] == "durable_queues")
    );
    assert!(
        claims.iter().any(|f| f["reset_stuck"] == "true"),
        "the first poll resets stuck messages"
    );
    for (operation, id) in [
        ("durable_queues.acknowledge", ok.as_str()),
        ("durable_queues.retry", failing.as_str()),
        ("durable_queues.mark_as_dead_letter_direct", "undecodable"),
    ] {
        assert!(
            capture
                .spans_named(operation)
                .iter()
                .any(|f| f["queue_entry_id"] == id),
            "{operation}"
        );
    }
    let inserts = capture.spans_named("durable_queues.insert");
    assert_eq!(inserts.len(), 2);
    assert!(inserts.iter().all(|f| f["count"] == "1"));

    let stored_error: String = sqlx::query_scalar(
        "SELECT last_delivery_error FROM durable_queues WHERE id = 'undecodable'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(
        stored_error.contains(SECRET),
        "the stored reason keeps serde's full message: {stored_error}"
    );
    let logged = capture.everything();
    assert!(
        logged.contains("cannot decode message"),
        "the decode failure is logged"
    );
    assert!(
        !logged.contains(SECRET),
        "a payload value reached the logs: {logged}"
    );
}
