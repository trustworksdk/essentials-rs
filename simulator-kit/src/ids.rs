//! Identifier scheme. `Random` gives time-ordered random UUIDv7s. `Deterministic` derives every ID from a seed and
//! the natural key of what it names, all of which follow from the order of commands (sequences, ordinals). The same
//! commands with the same seed therefore produce the same IDs, run after run and across restarts, with no counters
//! to store. This module names the kinds the outbox uses (events, deliveries, subscriptions); a simulator derives its
//! own through [`IdScheme::derive`].

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

    /// The ID of a thing of `kind` with natural key `parts`. A simulator adds its own kinds through this, e.g. an
    /// extension trait on [`IdScheme`]; kinds must differ from `event`, `delivery` and `subscription`.
    pub fn derive(self, kind: &[u8], parts: &[&[u8]]) -> Uuid {
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
        assert_eq!(SEEDED.event(7), SEEDED.event(7));
        assert_eq!(SEEDED.event(7).get_version_num(), 8);
        assert_ne!(
            SEEDED.event(7),
            IdScheme::Deterministic { seed: 43 }.event(7)
        );
        // Pinned (computed independently with Python's hashlib), so an accidental change of the derivation is caught:
        // it would change every ID ever issued.
        let pinned = IdScheme::Deterministic { seed: 0 }.event(1).to_string();
        assert_eq!(pinned, PINNED_EVENT_1, "the derivation changed");
    }

    const PINNED_EVENT_1: &str = "fc1f5e61-eb82-8087-84c5-3adae2731994";

    #[test]
    fn kinds_and_keys_never_collide() {
        let subscription = SEEDED.subscription(1);
        let mut ids = HashSet::new();
        for n in 1..=200_u64 {
            for id in [
                SEEDED.event(n),
                SEEDED.subscription(n),
                SEEDED.delivery(subscription, n),
                SEEDED.derive(b"other", &[&n.to_le_bytes()]),
            ] {
                assert!(ids.insert(id), "collision at {n}");
            }
        }
    }

    #[test]
    fn random_ids_are_unique_v7() {
        let a = IdScheme::Random.event(1);
        let b = IdScheme::Random.event(1);
        assert_ne!(a, b);
        assert_eq!(a.get_version_num(), 7);
    }
}
