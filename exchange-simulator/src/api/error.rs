use axum::http::StatusCode;

pub use simulator_kit::api::ApiError;

use crate::clock::ClockError;
use crate::engine::{BookError, DispatchError, ExchangeError};

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
            E::UnknownOrder(_) | E::UnknownClientOrderId { .. } | E::UnknownSubscription(_) => {
                (StatusCode::NOT_FOUND, "not_found")
            }
            E::ManagedByConfiguration { .. } => (StatusCode::CONFLICT, "managed_by_configuration"),
            E::OrderNotActive { .. } => (StatusCode::CONFLICT, "order_not_active"),
            E::DuplicateInstrument(_) | E::DuplicateListing { .. } | E::DuplicateParticipant(_) => {
                (StatusCode::CONFLICT, "already_exists")
            }
            E::ClientOrderIdConflict { .. } => (StatusCode::CONFLICT, "client_order_id_conflict"),
            E::InvalidClientOrderId
            | E::InvalidParticipant(_)
            | E::InvalidWebhook(_)
            | E::Instrument(_)
            | E::PriceSource(_)
            | E::Liquidity(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            E::Book(BookError::Invalid(_)) | E::InvalidTimeInForce(_) => {
                (StatusCode::BAD_REQUEST, "invalid_order")
            }
            E::ClockNotVirtual => (StatusCode::CONFLICT, "clock_not_virtual"),
            E::ExternalOutcomesDisabled => (StatusCode::CONFLICT, "external_outcomes_disabled"),
            E::UnknownExecution(_) => (StatusCode::NOT_FOUND, "not_found"),
            E::ExecutionBusted(_) => (StatusCode::CONFLICT, "execution_busted"),
            E::InvalidCorrection(_) | E::InvalidCorporateAction(_) => {
                (StatusCode::BAD_REQUEST, "invalid_request")
            }
            E::DuplicateCorporateAction(_) => (StatusCode::CONFLICT, "already_exists"),
            E::InvalidReport(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            E::ReportConflict { .. } => (StatusCode::CONFLICT, "report_conflict"),
            E::Clock(ClockError::Speed(_)) => (StatusCode::BAD_REQUEST, "invalid_clock_speed"),
            E::Clock(_) => (StatusCode::CONFLICT, "clock_backwards"),
            E::Book(_)
            | E::Storage(_)
            | E::GroupCommit(_)
            | E::Settlement(_)
            | E::Clearing(_)
            | E::DeclaredInstrumentConflict { .. }
            | E::DeclaredInstrument { .. } => {
                return Self::internal(error);
            }
        };
        Self::new(status, code, error.to_string())
    }
}
