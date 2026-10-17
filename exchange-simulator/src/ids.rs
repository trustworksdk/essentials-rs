//! Identifiers of the exchange's own kinds (orders, executions, settlement reports), derived by the shared
//! [`IdScheme`] (see `simulator_kit::ids`): the same seed and commands give the same IDs, run after run.

use uuid::Uuid;

pub use simulator_kit::ids::IdScheme;

/// The exchange's ID kinds on top of the outbox's (events, deliveries, subscriptions).
pub trait ExchangeIds {
    /// An order, by its exchange sequence.
    fn order(self, sequence: u64) -> Uuid;
    /// The `fill`-th execution (from 0) of the incoming order with this sequence.
    fn execution(self, taker_sequence: u64, fill: usize) -> Uuid;
    /// The `ordinal`-th external settlement report (from 1) for an execution.
    fn report(self, execution: Uuid, ordinal: u64) -> Uuid;
}

impl ExchangeIds for IdScheme {
    fn order(self, sequence: u64) -> Uuid {
        self.derive(b"order", &[&sequence.to_le_bytes()])
    }

    fn execution(self, taker_sequence: u64, fill: usize) -> Uuid {
        self.derive(
            b"execution",
            &[&taker_sequence.to_le_bytes(), &(fill as u64).to_le_bytes()],
        )
    }

    fn report(self, execution: Uuid, ordinal: u64) -> Uuid {
        self.derive(b"report", &[execution.as_bytes(), &ordinal.to_le_bytes()])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    const SEEDED: IdScheme = IdScheme::Deterministic { seed: 42 };

    #[test]
    fn deterministic_ids_are_stable_and_seed_dependent() {
        assert_eq!(SEEDED.order(7), SEEDED.order(7));
        assert_eq!(SEEDED.order(7).get_version_num(), 8);
        assert_ne!(
            SEEDED.order(7),
            IdScheme::Deterministic { seed: 43 }.order(7)
        );
        // Pinned (computed independently with Python's hashlib), so an accidental change of the derivation is caught:
        // it would change every ID ever issued.
        let pinned = IdScheme::Deterministic { seed: 0 }.order(1).to_string();
        assert_eq!(pinned, PINNED_ORDER_1, "the derivation changed");
    }

    const PINNED_ORDER_1: &str = "2d7981f9-ffca-8b88-b271-dcc5591136fa";

    #[test]
    fn kinds_and_keys_never_collide() {
        let subscription = SEEDED.subscription(1);
        let execution = SEEDED.execution(1, 0);
        let mut ids = HashSet::new();
        for n in 1..=200_u64 {
            for id in [
                SEEDED.order(n),
                SEEDED.event(n),
                SEEDED.subscription(n),
                SEEDED.execution(n, 0),
                SEEDED.execution(n, 1),
                SEEDED.delivery(subscription, n),
                SEEDED.report(execution, n),
            ] {
                assert!(ids.insert(id), "collision at {n}");
            }
        }
    }

    #[test]
    fn random_ids_are_unique_v7() {
        let a = IdScheme::Random.order(1);
        let b = IdScheme::Random.order(1);
        assert_ne!(a, b);
        assert_eq!(a.get_version_num(), 7);
    }
}
