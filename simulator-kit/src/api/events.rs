//! `GET /api/events`, the event-log read both simulators serve (SR-34): `after`, `limit`, and `eventType`, repeated or
//! comma-separated, with the highest sequence the read looked at in the `Event-Log-Scanned-Through` header, so a
//! filtered reader continues from there instead of scanning a stretch without matches again.

use axum::Json;
use axum::http::HeaderName;
use axum::response::{IntoResponse, Response};

use super::ApiError;
use crate::events::EventEnvelope;
use crate::outbox::EventPage;

/// The response header carrying [`EventPage::scanned_through`].
pub const SCANNED_THROUGH: HeaderName = HeaderName::from_static("event-log-scanned-through");

/// A parsed `GET /api/events` query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRead {
    /// Events with a sequence greater than this; default 0.
    pub after: u64,
    /// 1 to 1000; default 100.
    pub limit: u32,
    /// The event types to return, `None` for every type.
    pub event_types: Option<Vec<String>>,
}

impl EventRead {
    /// Parses the raw query string. Every `eventType` value must be one of `known`; other parameters are refused.
    pub fn parse(query: Option<&str>, known: &[&str]) -> Result<Self, ApiError> {
        let mut read = Self {
            after: 0,
            limit: 100,
            event_types: None,
        };
        for (name, value) in form_urlencoded::parse(query.unwrap_or("").as_bytes()) {
            match name.as_ref() {
                "after" => {
                    read.after = value.parse().map_err(|_| {
                        ApiError::bad_request("after must be a non-negative integer")
                    })?;
                }
                "limit" => {
                    read.limit = value
                        .parse()
                        .ok()
                        .filter(|limit| (1..=1000).contains(limit))
                        .ok_or_else(|| ApiError::bad_request("limit must be between 1 and 1000"))?;
                }
                "eventType" => {
                    let types = read.event_types.get_or_insert_with(Vec::new);
                    for event_type in value.split(',').map(str::trim) {
                        if !known.contains(&event_type) {
                            return Err(ApiError::bad_request(format!(
                                "unknown event type {event_type:?}"
                            )));
                        }
                        if !types.iter().any(|t| t == event_type) {
                            types.push(event_type.to_string());
                        }
                    }
                }
                other => {
                    return Err(ApiError::bad_request(format!(
                        "unknown query parameter {other:?}"
                    )));
                }
            }
        }
        Ok(read)
    }
}

/// The events as JSON, with the scanned-through header.
pub fn response(page: EventPage) -> Response {
    let scanned = page.scanned_through.to_string();
    let events: Vec<EventEnvelope> = page.events;
    ([(SCANNED_THROUGH, scanned)], Json(events)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KNOWN: &[&str] = &["A", "B", "C"];

    fn types(query: &str) -> Option<Vec<String>> {
        EventRead::parse(Some(query), KNOWN).unwrap().event_types
    }

    #[test]
    fn event_types_are_repeated_or_comma_separated() {
        assert_eq!(types(""), None);
        assert_eq!(types("eventType=A"), Some(vec!["A".to_string()]));
        let both = Some(vec!["A".to_string(), "B".to_string()]);
        assert_eq!(types("eventType=A&eventType=B"), both);
        assert_eq!(types("eventType=A,B"), both);
        assert_eq!(
            types("eventType=A%2CB&eventType=A"),
            both,
            "duplicates once"
        );
        assert_eq!(
            EventRead::parse(Some("after=7&limit=1000"), KNOWN).unwrap(),
            EventRead {
                after: 7,
                limit: 1000,
                event_types: None
            }
        );
    }

    #[test]
    fn unknown_types_parameters_and_bad_numbers_are_refused() {
        for query in [
            "eventType=D",
            "eventType=A,D",
            "eventType=",
            "event_type=A",
            "limit=0",
            "limit=1001",
            "after=-1",
        ] {
            assert!(EventRead::parse(Some(query), KNOWN).is_err(), "{query}");
        }
    }
}
