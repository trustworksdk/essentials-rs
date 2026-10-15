//! Identifier scheme. `Random` gives time-ordered random UUIDv7s. `Deterministic` derives every ID from a seed and
//! the natural key of what it names, all of which follow from the order of commands (sequences, ordinals). The same
//! commands with the same seed therefore produce the same IDs, run after run and across restarts, with no counters
//! to store.

use sha2::{Digest, Sha256};
use uuid::Uuid;

/// How IDs are made (`ids:` in the configuration).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum IdScheme {
    /// UUIDv7: unique, time-ordered, different every run.
    #[default]
    Random,
    /// UUIDv8 from SHA-256 of (seed, kind, natural key): reproducible.
    Deterministic { seed: u64 },
}

impl IdScheme {
    /// An order, by its exchange sequence.
    pub fn order(self, sequence: u64) -> Uuid {
        self.derive(b"order", &[&sequence.to_le_bytes()])
    }

    /// The `fill`-th execution (from 0) of the incoming order with this sequence.
    pub fn execution(self, taker_sequence: u64, fill: usize) -> Uuid {
        self.derive(
            b"execution",
            &[&taker_sequence.to_le_bytes(), &(fill as u64).to_le_bytes()],
        )
    }

    /// A canonical event, by its sequence in the event log.
    pub fn event(self, sequence: u64) -> Uuid {
        self.derive(b"event", &[&sequence.to_le_bytes()])
    }

    /// The delivery of one event to one subscription.
    pub fn delivery(self, subscription: Uuid, event_sequence: u64) -> Uuid {
        self.derive(
            b"delivery",
            &[subscription.as_bytes(), &event_sequence.to_le_bytes()],
        )
    }

    /// The `ordinal`-th webhook subscription (from 1).
    pub fn subscription(self, ordinal: u64) -> Uuid {
        self.derive(b"subscription", &[&ordinal.to_le_bytes()])
    }

    /// The `ordinal`-th external settlement report (from 1) for an execution.
    pub fn report(self, execution: Uuid, ordinal: u64) -> Uuid {
        self.derive(b"report", &[execution.as_bytes(), &ordinal.to_le_bytes()])
    }

    fn derive(self, kind: &[u8], parts: &[&[u8]]) -> Uuid {
        match self {
            Self::Random => Uuid::now_v7(),
            Self::Deterministic { seed } => {
                let mut hash = Sha256::new();
                hash.update(b"exchange-simulator/ids/v1");
                hash.update(seed.to_le_bytes());
                // Length-prefixed, so (kind, parts) boundaries cannot be confused.
                for part in std::iter::once(kind).chain(parts.iter().copied()) {
                    hash.update((part.len() as u64).to_le_bytes());
                    hash.update(part);
                }
                let digest = hash.finalize();
                let mut bytes = [0_u8; 16];
                bytes.copy_from_slice(&digest[..16]);
                Uuid::new_v8(bytes)
            }
        }
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
