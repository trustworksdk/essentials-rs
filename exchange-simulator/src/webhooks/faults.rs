//! Delivery fault injection (plan item 3.3): a per-subscription policy that damages the *outgoing copy* of an event
//! (missing fields, explicit nulls, invalid values), sends it twice, delays it, or holds it back so later events
//! overtake it. The canonical event in the log is never touched.
//!
//! Every decision is drawn from a generator seeded by (policy seed, delivery ID, attempt), so with deterministic IDs
//! the same deliveries get the same faults on every run, restarts included.
//!
//! ```json
//! {
//!   "seed": 7,
//!   "missing":  { "probability": 0.15 },                          // one random field
//!   "nulls":    { "paths": ["payload.reason"] },                  // probability defaults to 1
//!   "invalid":  { "paths": ["payload.price", "payload.quantity"], "probability": 0.5 },
//!   "duplicateProbability": 0.02,
//!   "delay":    { "probability": 0.1, "ms": 500 },
//!   "outOfOrder": { "probability": 0.05, "holdMs": 1000 }
//! }
//! ```
//!
//! Paths are dot-separated, with array indexes as numbers: `payload.price`, `payload.fills.0.quantity`. Without
//! `paths`, a fault picks one field at random from the event's leaf fields.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::market::Rng;

/// The longest `delay.ms`: a delayed request holds its slot in the worker's batch, so it must stay well inside the
/// claim lease.
pub const MAX_DELAY_MS: u64 = 10_000;
/// The longest `outOfOrder.holdMs`.
pub const MAX_HOLD_MS: u64 = 3_600_000;

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FaultPolicy {
    #[serde(default)]
    pub seed: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing: Option<FieldFault>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nulls: Option<FieldFault>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalid: Option<FieldFault>,
    #[serde(default)]
    pub duplicate_probability: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay: Option<DelayFault>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub out_of_order: Option<HoldFault>,
}

/// Damage to fields: the listed `paths`, or one random leaf field when there are none.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FieldFault {
    #[serde(default = "always")]
    pub probability: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
}

fn always() -> f64 {
    1.0
}

/// Waits `ms` before sending.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DelayFault {
    #[serde(default = "always")]
    pub probability: f64,
    pub ms: u64,
}

/// Holds the delivery back for `hold_ms` without sending it or counting an attempt; later events go out meanwhile.
/// A delivery is held at most once.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HoldFault {
    #[serde(default = "always")]
    pub probability: f64,
    pub hold_ms: u64,
}

/// What a policy does to one attempt.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FaultPlan {
    /// Hold instead of sending.
    pub hold: Option<Duration>,
    /// The damaged body, when any field fault applied.
    pub body: Option<Vec<u8>>,
    pub duplicate: bool,
    pub delay: Option<Duration>,
    /// What was applied, e.g. `missing:payload.price`, `duplicate`, `delay:500ms`, `held:1000ms`.
    pub applied: Vec<String>,
}

#[derive(Clone, Copy)]
enum FieldDamage {
    Missing,
    Null,
    Invalid,
}

impl FieldDamage {
    fn label(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Null => "null",
            Self::Invalid => "invalid",
        }
    }
}

impl FaultPolicy {
    /// Checks probabilities, limits and paths; the error names the field.
    pub fn validate(&self) -> Result<(), String> {
        let mut probabilities = vec![("duplicateProbability", self.duplicate_probability)];
        for (name, fault) in [
            ("missing", &self.missing),
            ("nulls", &self.nulls),
            ("invalid", &self.invalid),
        ] {
            if let Some(fault) = fault {
                probabilities.push((name, fault.probability));
                if let Some(path) = fault.paths.iter().find(|p| parse_path(p).is_none()) {
                    return Err(format!("{name}.paths: invalid path {path:?}"));
                }
            }
        }
        if let Some(delay) = &self.delay {
            probabilities.push(("delay", delay.probability));
            if delay.ms > MAX_DELAY_MS {
                return Err(format!("delay.ms must be at most {MAX_DELAY_MS}"));
            }
        }
        if let Some(hold) = &self.out_of_order {
            probabilities.push(("outOfOrder", hold.probability));
            if hold.hold_ms == 0 || hold.hold_ms > MAX_HOLD_MS {
                return Err(format!("outOfOrder.holdMs must be 1 to {MAX_HOLD_MS}"));
            }
        }
        for (name, p) in probabilities {
            if !(0.0..=1.0).contains(&p) {
                return Err(format!(
                    "{name}: probability must be between 0 and 1, got {p}"
                ));
            }
        }
        Ok(())
    }

    /// Decides the faults for one attempt of a delivery. `held` says whether the delivery was held before; `event`
    /// is the canonical envelope. Every draw is made whether or not its fault is configured, so adding one fault does
    /// not change the decisions of the others.
    pub fn plan(&self, delivery_id: Uuid, attempt: u32, held: bool, event: &Value) -> FaultPlan {
        let mut rng = self.rng(delivery_id, attempt);
        let mut plan = FaultPlan::default();
        let hold = rng.unit();
        if let Some(fault) = &self.out_of_order
            && !held
            && hold < fault.probability
        {
            plan.hold = Some(Duration::from_millis(fault.hold_ms));
            plan.applied.push(format!("held:{}ms", fault.hold_ms));
            return plan;
        }
        let mut body = event.clone();
        let mut damaged = false;
        for (fault, damage) in [
            (&self.missing, FieldDamage::Missing),
            (&self.nulls, FieldDamage::Null),
            (&self.invalid, FieldDamage::Invalid),
        ] {
            let fires = rng.unit();
            let pick = rng.next_u64();
            let Some(fault) = fault else { continue };
            if fires >= fault.probability {
                continue;
            }
            let paths: Vec<Vec<String>> = if fault.paths.is_empty() {
                let leaves = leaf_paths(&body);
                if leaves.is_empty() {
                    continue;
                }
                vec![leaves[(pick % leaves.len() as u64) as usize].clone()]
            } else {
                fault.paths.iter().filter_map(|p| parse_path(p)).collect()
            };
            for path in paths {
                if damage_at(&mut body, &path, damage) {
                    damaged = true;
                    plan.applied
                        .push(format!("{}:{}", damage.label(), path.join(".")));
                }
            }
        }
        if damaged {
            plan.body = Some(serde_json::to_vec(&body).expect("values serialize"));
        }
        if rng.unit() < self.duplicate_probability {
            plan.duplicate = true;
            plan.applied.push("duplicate".into());
        }
        let delay = rng.unit();
        if let Some(fault) = &self.delay
            && delay < fault.probability
            && fault.ms > 0
        {
            plan.delay = Some(Duration::from_millis(fault.ms));
            plan.applied.push(format!("delay:{}ms", fault.ms));
        }
        plan
    }

    fn rng(&self, delivery_id: Uuid, attempt: u32) -> Rng {
        let digest = Sha256::new()
            .chain_update(b"exchange-simulator/faults/v1")
            .chain_update(self.seed.to_be_bytes())
            .chain_update(delivery_id.as_bytes())
            .chain_update(attempt.to_be_bytes())
            .finalize();
        Rng::new(u64::from_be_bytes(digest[..8].try_into().expect("8 bytes")))
    }
}

/// `a.b.0.c` → segments; `None` for an empty path or an empty segment.
fn parse_path(path: &str) -> Option<Vec<String>> {
    let segments: Vec<String> = path.split('.').map(str::to_string).collect();
    (!path.is_empty() && segments.iter().all(|s| !s.is_empty())).then_some(segments)
}

/// Every path to a leaf (a value that is not a non-empty object or array), in document order.
fn leaf_paths(value: &Value) -> Vec<Vec<String>> {
    fn walk(value: &Value, prefix: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
        match value {
            Value::Object(map) if !map.is_empty() => {
                for (key, child) in map {
                    prefix.push(key.clone());
                    walk(child, prefix, out);
                    prefix.pop();
                }
            }
            Value::Array(items) if !items.is_empty() => {
                for (i, child) in items.iter().enumerate() {
                    prefix.push(i.to_string());
                    walk(child, prefix, out);
                    prefix.pop();
                }
            }
            _ if !prefix.is_empty() => out.push(prefix.clone()),
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(value, &mut Vec::new(), &mut out);
    out
}

/// Applies `damage` at `path`; false when the path does not exist.
fn damage_at(root: &mut Value, path: &[String], damage: FieldDamage) -> bool {
    let (last, parents) = path.split_last().expect("paths are non-empty");
    let mut node = root;
    for segment in parents {
        node = match node {
            Value::Object(map) => match map.get_mut(segment) {
                Some(child) => child,
                None => return false,
            },
            Value::Array(items) => {
                match segment.parse::<usize>().ok().and_then(|i| items.get_mut(i)) {
                    Some(child) => child,
                    None => return false,
                }
            }
            _ => return false,
        };
    }
    let target = match node {
        Value::Object(map) => {
            if let FieldDamage::Missing = damage {
                return map.remove(last).is_some();
            }
            map.get_mut(last)
        }
        Value::Array(items) => {
            let Some(index) = last.parse::<usize>().ok().filter(|i| *i < items.len()) else {
                return false;
            };
            if let FieldDamage::Missing = damage {
                items.remove(index);
                return true;
            }
            items.get_mut(index)
        }
        _ => None,
    };
    let Some(target) = target else { return false };
    *target = match damage {
        FieldDamage::Missing => unreachable!("handled above"),
        FieldDamage::Null => Value::Null,
        FieldDamage::Invalid => invalid(target),
    };
    true
}

/// A value of the wrong type for what is there: strings become numbers, everything else becomes a string.
fn invalid(value: &Value) -> Value {
    match value {
        Value::String(_) => Value::from(-12345),
        Value::Number(_) => Value::from("not-a-number"),
        Value::Bool(_) => Value::from("not-a-boolean"),
        Value::Null => Value::from("not-null"),
        Value::Array(_) => Value::from("not-an-array"),
        Value::Object(_) => Value::from("not-an-object"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn event() -> Value {
        json!({
            "eventId": "e-1",
            "eventType": "TradeExecuted",
            "sequence": 7,
            "payload": { "price": "100.25", "quantity": 150, "fills": [ { "q": 1 }, { "q": 2 } ], "flag": true }
        })
    }

    fn policy(json: Value) -> FaultPolicy {
        let policy: FaultPolicy = serde_json::from_value(json).unwrap();
        policy.validate().unwrap();
        policy
    }

    fn body(plan: &FaultPlan) -> Value {
        serde_json::from_slice(plan.body.as_ref().expect("damaged")).unwrap()
    }

    #[test]
    fn listed_paths_are_removed_nulled_or_made_invalid() {
        let id = Uuid::nil();
        let plan = policy(json!({
            "missing": { "paths": ["payload.fills.0", "payload.nothing"] },
            "nulls": { "paths": ["eventType"] },
            "invalid": { "paths": ["payload.price", "payload.quantity", "payload.flag", "payload.fills.0.q"] }
        }))
        .plan(id, 1, false, &event());
        assert_eq!(
            body(&plan),
            json!({
                "eventId": "e-1", "eventType": null, "sequence": 7,
                "payload": { "price": -12345, "quantity": "not-a-number", "fills": [ { "q": "not-a-number" } ],
                             "flag": "not-a-boolean" }
            })
        );
        assert_eq!(
            plan.applied,
            [
                "missing:payload.fills.0",
                "null:eventType",
                "invalid:payload.price",
                "invalid:payload.quantity",
                "invalid:payload.flag",
                "invalid:payload.fills.0.q"
            ],
            "a path that does not exist is not reported"
        );
        assert!(plan.hold.is_none() && !plan.duplicate && plan.delay.is_none());
    }

    #[test]
    fn decisions_are_reproducible_per_delivery_and_attempt() {
        let policy = policy(json!({
            "seed": 3, "missing": { "probability": 0.5 }, "duplicateProbability": 0.5,
            "delay": { "probability": 0.5, "ms": 5 }
        }));
        let plans: Vec<FaultPlan> = (0..200u32)
            .map(|i| policy.plan(Uuid::from_u128(u128::from(i)), 1, false, &event()))
            .collect();
        let again: Vec<FaultPlan> = (0..200u32)
            .map(|i| policy.plan(Uuid::from_u128(u128::from(i)), 1, false, &event()))
            .collect();
        assert_eq!(plans, again);
        let retry = policy.plan(Uuid::from_u128(0), 2, false, &event());
        let other_attempts = (2..20).map(|a| policy.plan(Uuid::from_u128(0), a, false, &event()));
        assert!(
            std::iter::once(retry)
                .chain(other_attempts)
                .any(|p| p != plans[0]),
            "retries draw anew"
        );
        let share = |f: &dyn Fn(&FaultPlan) -> bool| plans.iter().filter(|p| f(p)).count();
        for (name, count) in [
            ("missing", share(&|p| p.body.is_some())),
            ("duplicate", share(&|p| p.duplicate)),
            (
                "delay",
                share(&|p| p.delay == Some(Duration::from_millis(5))),
            ),
        ] {
            assert!((70..130).contains(&count), "{name}: {count} of 200");
        }
        // A random field fault removes exactly one leaf field, and it is gone from the body sent.
        for plan in plans.iter().filter(|p| p.body.is_some()) {
            let missing: Vec<&str> = plan
                .applied
                .iter()
                .filter_map(|a| a.strip_prefix("missing:"))
                .collect();
            assert_eq!(missing.len(), 1, "{:?}", plan.applied);
            let pointer = format!("/{}", missing[0].replace('.', "/"));
            assert!(event().pointer(&pointer).is_some());
            assert!(body(plan).pointer(&pointer).is_none(), "{pointer}");
        }
        let mut reseeded = policy.clone();
        reseeded.seed = 4;
        assert_ne!(
            plans,
            (0..200u32)
                .map(|i| reseeded.plan(Uuid::from_u128(u128::from(i)), 1, false, &event()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_hold_sends_nothing_and_happens_once() {
        let policy = policy(json!({ "outOfOrder": { "holdMs": 250 }, "duplicateProbability": 1 }));
        let first = policy.plan(Uuid::nil(), 1, false, &event());
        assert_eq!(first.hold, Some(Duration::from_millis(250)));
        assert_eq!(first.applied, ["held:250ms"]);
        assert!(!first.duplicate && first.body.is_none());
        let released = policy.plan(Uuid::nil(), 1, true, &event());
        assert_eq!(released.hold, None);
        assert_eq!(released.applied, ["duplicate"]);
    }

    #[test]
    fn adding_a_fault_does_not_change_the_others() {
        let base = policy(json!({ "duplicateProbability": 0.5 }));
        let more = policy(json!({ "duplicateProbability": 0.5, "nulls": { "probability": 0.5 } }));
        for i in 0..100u32 {
            let id = Uuid::from_u128(u128::from(i));
            assert_eq!(
                base.plan(id, 1, false, &event()).duplicate,
                more.plan(id, 1, false, &event()).duplicate
            );
        }
    }

    #[test]
    fn invalid_policies_name_the_field() {
        let error = |json: Value| {
            serde_json::from_value::<FaultPolicy>(json)
                .map_err(|e| e.to_string())
                .and_then(|p| p.validate())
                .unwrap_err()
        };
        assert!(error(json!({ "duplicateProbability": 2 })).contains("duplicateProbability"));
        assert!(error(json!({ "nulls": { "probability": -1 } })).contains("nulls"));
        assert!(error(json!({ "missing": { "paths": ["a..b"] } })).contains("missing.paths"));
        assert!(error(json!({ "delay": { "ms": 60000 } })).contains("delay.ms"));
        assert!(error(json!({ "outOfOrder": { "holdMs": 0 } })).contains("outOfOrder.holdMs"));
        assert!(error(json!({ "duplicates": 1 })).contains("duplicates"));
    }
}
