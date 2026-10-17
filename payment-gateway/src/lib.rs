//! Payment gateway simulator (plan item 5.4b): deposits and withdrawals for a trading demo's accounts, with seeded
//! outcomes and delays on a controllable clock, signed webhooks, and the bank's end-of-day statements to reconcile
//! against. It plays an outside party only (D8): client balances belong to the demo's own domain. The clock, outbox,
//! webhook delivery and maintenance, and persistence come from `simulator-kit`, as in the exchange simulator.

pub mod api;
pub mod app;
pub mod config;
pub mod gateway;
pub mod iban;
pub mod metrics;
pub mod money;
pub mod payments;
pub mod statement;
pub mod store;
