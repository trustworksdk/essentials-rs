//! Lightweight stock exchange simulator: real price-time matching over integer ticks, SQLite as the authoritative
//! store, and durable trade webhooks. See `rust_exchange_simulator_plan.md` and
//! `milestone_1_domain_schema_events.md` for the design.

pub mod api;
pub mod app;
pub mod clock;
pub mod config;
pub mod domain;
pub mod engine;
pub mod events;
pub mod metrics;
pub mod persistence;
pub mod webhooks;
