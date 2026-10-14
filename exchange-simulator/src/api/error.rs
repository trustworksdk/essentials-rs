use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::engine::{BookError, DispatchError, ExchangeError};

/// An error response: an HTTP status and a body `{"error": "<code>", "message": "<text>"}`.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    pub fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(%error, "request failed");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            error.to_string(),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({ "error": self.code, "message": self.message })),
        )
            .into_response()
    }
}

impl From<DispatchError> for ApiError {
    fn from(error: DispatchError) -> Self {
        match error {
            DispatchError::Overloaded => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "overloaded",
                error.to_string(),
            ),
            DispatchError::Stopped => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                error.to_string(),
            ),
            DispatchError::Exchange(error) => error.into(),
        }
    }
}

impl From<ExchangeError> for ApiError {
    fn from(error: ExchangeError) -> Self {
        use ExchangeError as E;
        let (status, code) = match &error {
            E::UnknownInstrument(_) => (StatusCode::UNPROCESSABLE_ENTITY, "unknown_instrument"),
            E::UnknownParticipant(_) => (StatusCode::UNPROCESSABLE_ENTITY, "unknown_participant"),
            E::UnknownOrder(_) => (StatusCode::NOT_FOUND, "not_found"),
            E::OrderNotActive { .. } => (StatusCode::CONFLICT, "order_not_active"),
            E::DuplicateInstrument(_) | E::DuplicateListing { .. } | E::DuplicateParticipant(_) => {
                (StatusCode::CONFLICT, "already_exists")
            }
            E::ClientOrderIdConflict { .. } => (StatusCode::CONFLICT, "client_order_id_conflict"),
            E::InvalidClientOrderId
            | E::InvalidParticipant
            | E::InvalidWebhook(_)
            | E::Instrument(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            E::Book(BookError::Invalid(_)) => (StatusCode::BAD_REQUEST, "invalid_order"),
            E::Book(_) | E::Storage(_) => return Self::internal(error),
        };
        Self::new(status, code, error.to_string())
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            rejection.body_text(),
        )
    }
}

impl From<QueryRejection> for ApiError {
    fn from(rejection: QueryRejection) -> Self {
        Self::bad_request(rejection.body_text())
    }
}

impl From<PathRejection> for ApiError {
    fn from(rejection: PathRejection) -> Self {
        Self::bad_request(rejection.body_text())
    }
}
