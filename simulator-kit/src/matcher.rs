//! "Contains" matching for scenario expectations: every field the expectation names must be present in the actual
//! value with an equal value; fields it leaves out are ignored. Arrays must have the same length and match element
//! by element.

use serde_json::Value;

/// Describes every way `actual` fails to contain `expected`; empty when it matches.
pub fn mismatches(expected: &Value, actual: &Value) -> Vec<String> {
    let mut out = Vec::new();
    compare(expected, actual, "", &mut out);
    out
}

pub fn contains(expected: &Value, actual: &Value) -> bool {
    mismatches(expected, actual).is_empty()
}

fn compare(expected: &Value, actual: &Value, path: &str, out: &mut Vec<String>) {
    let at = if path.is_empty() {
        "value".to_string()
    } else {
        path.to_string()
    };
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            for (key, value) in expected {
                let child = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                match actual.get(key) {
                    Some(found) => compare(value, found, &child, out),
                    None => out.push(format!("{child}: expected {value}, but it is missing")),
                }
            }
        }
        (Value::Array(expected), Value::Array(actual)) => {
            if expected.len() != actual.len() {
                out.push(format!(
                    "{at}: expected {} elements, got {}: {}",
                    expected.len(),
                    actual.len(),
                    Value::Array(actual.clone())
                ));
                return;
            }
            for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
                compare(e, a, &format!("{path}[{i}]"), out);
            }
        }
        (Value::Number(e), Value::Number(a)) if e.as_f64() == a.as_f64() => {}
        (e, a) if e == a => {}
        (e, a) => out.push(format!("{at}: expected {e}, got {a}")),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn named_fields_must_match_and_others_are_ignored() {
        let actual = json!({ "status": "FILLED", "remainingQuantity": 0, "extra": [1] });
        assert!(contains(&json!({ "status": "FILLED" }), &actual));
        assert!(contains(&json!({}), &actual));
        assert_eq!(
            mismatches(&json!({ "status": "OPEN", "price": "1.00" }), &actual),
            // In key order (serde_json maps are sorted), whatever the order in the scenario file.
            [
                "price: expected \"1.00\", but it is missing",
                "status: expected \"OPEN\", got \"FILLED\""
            ]
        );
    }

    #[test]
    fn nested_objects_and_arrays_report_their_path() {
        let actual = json!({ "asks": [{ "priceTicks": 10025, "quantity": 50 }], "bids": [] });
        assert!(contains(
            &json!({ "asks": [{ "quantity": 50 }], "bids": [] }),
            &actual
        ));
        assert_eq!(
            mismatches(&json!({ "asks": [{ "quantity": 40 }] }), &actual),
            ["asks[0].quantity: expected 40, got 50"]
        );
        assert_eq!(
            mismatches(&json!({ "bids": [{}] }), &actual),
            ["bids: expected 1 elements, got 0: []"]
        );
        assert!(
            contains(&json!({ "n": 1.0 }), &json!({ "n": 1 })),
            "numbers compare by value"
        );
        assert!(!contains(&json!({ "n": null }), &json!({ "n": 0 })));
    }
}
