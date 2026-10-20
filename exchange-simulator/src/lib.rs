//! Lightweight stock exchange simulator: real price-time matching over integer ticks, SQLite as the authoritative
//! store, and durable trade webhooks. See `rust_exchange_simulator_plan.md` and
//! `milestone_1_domain_schema_events.md` for the design.

pub mod api;
pub mod app;
pub mod clearing;
pub mod config;
pub mod domain;
pub mod engine;
pub mod events;
pub mod ids;
pub mod liquidity;
pub mod market;
pub mod market_data;
pub mod metrics;
pub mod persistence;
pub mod pricing;
pub mod scenario;
pub mod sessions;
pub mod settlement;

/// Shared with the other simulators (see `simulator_kit`), re-exported under their old paths.
pub use simulator_kit::{clock, webhooks};
