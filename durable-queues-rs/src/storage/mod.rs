//! Queue storage. The `QueueStorage` trait arrives with the operations in Phase 2; Phase 1 provides the PostgreSQL
//! table, its schema handling and row decoding.

pub(crate) mod postgres;
