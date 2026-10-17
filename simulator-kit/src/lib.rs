//! Parts the simulators share (decision D10): the controllable clock, seeded randomness and reproducible IDs, SQLite
//! plumbing and persistence modes, the canonical event log with its transactional outbox, and signed, fault-injectable
//! webhook delivery with its maintenance API. Each simulator keeps its own domain, schema migrations and event
//! payloads; `exchange-simulator` is the first user, the payment gateway the second.

pub mod api;
pub mod clock;
pub mod events;
pub mod ids;
pub mod matcher;
pub mod metrics;
pub mod outbox;
pub mod persistence;
pub mod rng;
pub mod webhooks;
