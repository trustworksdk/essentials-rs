//! An OpenAPI 3.0 rendition of a simulator's OpenAPI 3.1 document, for client generators whose 3.1 support is
//! incomplete (openapi-generator's Java clients, for one). The 3.1 document stays the source of truth: the rendition is
//! derived from it, committed next to it (`openapi-3.0.json`) and shipped with it (`api-specs/`), and each crate's
//! contract tests fail when it is out of date ([`check_rendition`]).
//!
//! Only what the simulators' documents use is converted; anything else 3.1-only panics, so a new construct is
//! noticed rather than passed through.

use std::path::Path;

use serde_json::{Map, Value, json};

/// The environment variable that makes [`check_rendition`] rewrite an out-of-date rendition instead of failing.
pub const UPDATE_VARIABLE: &str = "UPDATE_OPENAPI_30";

/// Converts an OpenAPI 3.1 document to OpenAPI 3.0.3.
///
/// - `type: [T, "null"]`, `enum: [..., null]`, `const: null` and a `{type: "null"}` branch of `oneOf`/`anyOf`
///   become `nullable: true`. A branch left alone is inlined, or kept as `allOf: [$ref]` when it is a reference.
/// - `const: x` becomes `enum: [x]`; an untyped enum of strings gets `type: string`.
/// - `$ref` with sibling keywords becomes `allOf: [$ref]` with the siblings.
/// - `unevaluatedProperties: false` becomes `additionalProperties: false` where there is no `allOf` (3.0 cannot say
///   it across `allOf`, so it is dropped there).
/// - Numeric `exclusiveMinimum`/`exclusiveMaximum` become `minimum`/`maximum` with the boolean form.
/// - `webhooks`, `info.summary` and `info.license.identifier` are dropped; the event schemas stay in `components`.
pub fn to_openapi_30(document: &Value) -> Value {
    let mut document = convert(document.clone(), "");
    assert_no_null_type(&document, "");
    let root = document
        .as_object_mut()
        .expect("an OpenAPI document is an object");
    root.insert("openapi".into(), json!("3.0.3"));
    root.remove("webhooks");
    if let Some(info) = root.get_mut("info").and_then(Value::as_object_mut) {
        info.remove("summary");
        if let Some(license) = info.get_mut("license").and_then(Value::as_object_mut) {
            license.remove("identifier");
        }
    }
    document
}

/// Checks that the rendition at `path` is the conversion of `yaml_31`; with [`UPDATE_VARIABLE`] set, rewrites it
/// instead.
pub fn check_rendition(yaml_31: &str, path: &Path) {
    let document: Value =
        serde_saphyr::from_str(yaml_31).expect("the OpenAPI document is valid YAML");
    let expected = serde_json::to_string_pretty(&to_openapi_30(&document)).unwrap() + "\n";
    let current = std::fs::read_to_string(path).unwrap_or_default();
    if current == expected {
        return;
    }
    if std::env::var_os(UPDATE_VARIABLE).is_some() {
        std::fs::write(path, expected).unwrap();
        return;
    }
    panic!(
        "{} is not the OpenAPI 3.0 rendition of the current document; rerun the test with {UPDATE_VARIABLE}=1 to \
         rewrite it",
        path.display()
    );
}

fn convert(value: Value, path: &str) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .enumerate()
                .map(|(i, item)| convert(item, &format!("{path}/{i}")))
                .collect(),
        ),
        Value::Object(object) => {
            let object = object
                .into_iter()
                .map(|(key, value)| {
                    let inner = format!("{path}/{key}");
                    (key, convert(value, &inner))
                })
                .collect();
            Value::Object(convert_schema(object, path))
        }
        other => other,
    }
}

/// Rewrites one object's 3.1-only keywords. Every object is treated as a possible schema; the keywords looked at do
/// not occur elsewhere in an OpenAPI document with these value types.
fn convert_schema(mut o: Map<String, Value>, path: &str) -> Map<String, Value> {
    if let Some(Value::Array(types)) = o.get("type") {
        let rest: Vec<&Value> = types.iter().filter(|t| *t != "null").collect();
        let [only] = rest[..] else {
            panic!("{path}: OpenAPI 3.0 has no type list: {types:?}");
        };
        let (only, nullable) = (only.clone(), rest.len() < types.len());
        o.insert("type".into(), only);
        if nullable {
            o.insert("nullable".into(), json!(true));
        }
    }
    if let Some(constant) = o.remove("const") {
        if constant.is_null() {
            o.insert("nullable".into(), json!(true));
        } else {
            o.insert("enum".into(), json!([constant]));
        }
    }
    if let Some(Value::Array(values)) = o.get_mut("enum")
        && values.iter().any(Value::is_null)
    {
        values.retain(|v| !v.is_null());
        o.insert("nullable".into(), json!(true));
    }
    if let Some(Value::Array(values)) = o.get("enum")
        && !o.contains_key("type")
        && values.iter().all(Value::is_string)
    {
        o.insert("type".into(), json!("string"));
    }
    for key in ["oneOf", "anyOf"] {
        let Some(Value::Array(branches)) = o.get(key) else {
            continue;
        };
        let null = json!({ "type": "null" });
        if !branches.contains(&null) {
            continue;
        }
        let mut rest: Vec<Value> = branches.iter().filter(|b| **b != null).cloned().collect();
        o.remove(key);
        o.insert("nullable".into(), json!(true));
        if rest.len() > 1 {
            o.insert(key.into(), Value::Array(rest));
        } else if let Some(Value::Object(only)) = rest.pop() {
            if only.contains_key("$ref") {
                o.insert("allOf".into(), json!([only]));
            } else {
                for (k, v) in only {
                    o.entry(k).or_insert(v);
                }
            }
        }
    }
    if o.contains_key("$ref") && o.len() > 1 {
        let reference = o.remove("$ref").unwrap();
        o.insert("allOf".into(), json!([{ "$ref": reference }]));
    }
    if let Some(unevaluated) = o.remove("unevaluatedProperties")
        && unevaluated == json!(false)
        && !o.contains_key("allOf")
    {
        o.entry("additionalProperties").or_insert(json!(false));
    }
    for (exclusive, bound) in [
        ("exclusiveMinimum", "minimum"),
        ("exclusiveMaximum", "maximum"),
    ] {
        if let Some(limit) = o.get(exclusive).filter(|v| v.is_number()).cloned() {
            o.insert(bound.into(), limit);
            o.insert(exclusive.into(), json!(true));
        }
    }
    o
}

/// Panics on a `type: "null"` left after conversion: one that was not a branch of a nullable `oneOf`/`anyOf`.
fn assert_no_null_type(value: &Value, path: &str) {
    match value {
        Value::Object(object) => {
            assert!(
                object.get("type") != Some(&json!("null")),
                "{path}: OpenAPI 3.0 has no null type outside a nullable branch"
            );
            for (key, inner) in object {
                assert_no_null_type(inner, &format!("{path}/{key}"));
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                assert_no_null_type(item, &format!("{path}/{i}"));
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(value: Value) -> Value {
        convert(value, "")
    }

    #[test]
    fn nullable_forms_become_nullable() {
        assert_eq!(
            schema(json!({ "type": ["integer", "null"], "minimum": 1 })),
            json!({ "type": "integer", "nullable": true, "minimum": 1 })
        );
        assert_eq!(
            schema(json!({ "enum": ["on", "off", null] })),
            json!({ "enum": ["on", "off"], "nullable": true, "type": "string" })
        );
        assert_eq!(
            schema(json!({ "const": null })),
            json!({ "nullable": true })
        );
        assert_eq!(
            schema(json!({ "description": "d", "oneOf": [{ "type": "null" }, { "$ref": "#/x" }] })),
            json!({ "description": "d", "nullable": true, "allOf": [{ "$ref": "#/x" }] })
        );
        assert_eq!(
            schema(
                json!({ "oneOf": [{ "type": "object", "required": ["a"] }, { "type": "null" }] })
            ),
            json!({ "type": "object", "required": ["a"], "nullable": true })
        );
        assert_eq!(
            schema(json!({ "anyOf": [{ "$ref": "#/a" }, { "$ref": "#/b" }, { "type": "null" }] })),
            json!({ "anyOf": [{ "$ref": "#/a" }, { "$ref": "#/b" }], "nullable": true })
        );
    }

    #[test]
    fn other_31_keywords_get_their_30_form() {
        assert_eq!(
            schema(json!({ "const": "exchange" })),
            json!({ "enum": ["exchange"], "type": "string" })
        );
        assert_eq!(
            schema(json!({ "$ref": "#/a", "description": "d" })),
            json!({ "allOf": [{ "$ref": "#/a" }], "description": "d" })
        );
        assert_eq!(
            schema(json!({ "type": "object", "unevaluatedProperties": false })),
            json!({ "type": "object", "additionalProperties": false })
        );
        assert_eq!(
            schema(json!({ "allOf": [{ "$ref": "#/a" }], "unevaluatedProperties": false })),
            json!({ "allOf": [{ "$ref": "#/a" }] })
        );
        assert_eq!(
            schema(json!({ "type": "number", "exclusiveMinimum": 0 })),
            json!({ "type": "number", "minimum": 0, "exclusiveMinimum": true })
        );
        assert_eq!(
            schema(json!({ "properties": { "a": { "type": ["string", "null"] } } })),
            json!({ "properties": { "a": { "type": "string", "nullable": true } } }),
            "nested schemas too"
        );
    }

    #[test]
    fn the_document_loses_its_31_only_parts() {
        let document = json!({
            "openapi": "3.1.0",
            "info": { "title": "t", "version": "1", "summary": "s",
                      "license": { "name": "Apache-2.0", "identifier": "Apache-2.0" } },
            "paths": {},
            "webhooks": { "E": {} }
        });
        assert_eq!(
            to_openapi_30(&document),
            json!({
                "openapi": "3.0.3",
                "info": { "title": "t", "version": "1", "license": { "name": "Apache-2.0" } },
                "paths": {}
            })
        );
    }

    #[test]
    #[should_panic(expected = "no null type outside a nullable branch")]
    fn a_null_type_outside_a_nullable_branch_panics() {
        to_openapi_30(
            &json!({ "openapi": "3.1.0", "info": {}, "paths": {}, "x": { "type": "null" } }),
        );
    }

    #[test]
    #[should_panic(expected = "no type list")]
    fn a_type_list_without_a_3_0_form_panics() {
        schema(json!({ "type": ["string", "integer"] }));
    }
}
