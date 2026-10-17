//! Contract checks of a service against its OpenAPI 3.1 document, for the simulators' test suites (feature
//! `contract`). A [`Spec`] validates each request and response a test makes, and each event a service emits,
//! against the document's schemas, and records which operations were exercised. [`routes_in_source`] lists the
//! routes a service mounts, so a test can check the document has exactly those.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

use jsonschema::{Draft, Validator};
use serde_json::{Value, json};

/// An operation: (upper-case HTTP method, path template as in the document).
pub type Operation = (String, String);

/// A loaded OpenAPI document.
pub struct Spec {
    doc: Value,
    validators: Mutex<HashMap<String, Validator>>,
    covered: Mutex<BTreeSet<Operation>>,
}

const METHODS: [&str; 5] = ["get", "post", "put", "patch", "delete"];

impl Spec {
    /// Parses a YAML document. Panics on invalid YAML: the document is part of the code under test.
    pub fn from_yaml(text: &str) -> Self {
        let doc: Value = serde_saphyr::from_str(text).expect("the OpenAPI document is valid YAML");
        assert_eq!(doc["openapi"], "3.1.0", "an OpenAPI 3.1 document");
        Self {
            doc,
            validators: Mutex::default(),
            covered: Mutex::default(),
        }
    }

    pub fn document(&self) -> &Value {
        &self.doc
    }

    /// Every operation the document describes.
    pub fn operations(&self) -> BTreeSet<Operation> {
        let mut operations = BTreeSet::new();
        for (path, item) in self.doc["paths"].as_object().expect("paths") {
            for method in METHODS {
                if item.get(method).is_some() {
                    operations.insert((method.to_uppercase(), path.clone()));
                }
            }
        }
        operations
    }

    /// The operations no check has exercised yet.
    pub fn uncovered(&self) -> BTreeSet<Operation> {
        let covered = self
            .covered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.operations()
            .into_iter()
            .filter(|op| !covered.contains(op))
            .collect()
    }

    /// The template a concrete path (query string allowed) matches: the one with the most literal segments.
    pub fn template(&self, method: &str, uri: &str) -> String {
        let path = uri.split('?').next().unwrap_or(uri);
        let segments: Vec<&str> = path.split('/').collect();
        let mut best: Option<(usize, String)> = None;
        for (template, item) in self.doc["paths"].as_object().expect("paths") {
            if item.get(method.to_lowercase()).is_none() {
                continue;
            }
            let parts: Vec<&str> = template.split('/').collect();
            if parts.len() != segments.len() {
                continue;
            }
            let mut literal = 0;
            let matches = parts.iter().zip(&segments).all(|(t, s)| {
                if t.starts_with('{') && t.ends_with('}') {
                    !s.is_empty()
                } else {
                    literal += 1;
                    t == s
                }
            });
            if matches && best.as_ref().is_none_or(|(n, _)| literal > *n) {
                best = Some((literal, template.clone()));
            }
        }
        best.unwrap_or_else(|| panic!("{method} {path} is not in the OpenAPI document"))
            .1
    }

    fn operation(&self, method: &str, template: &str) -> &Value {
        &self.doc["paths"][template][method.to_lowercase()]
    }

    /// Follows a local `$ref` (`#/components/...`) to what it names; other values come back as they are.
    fn resolve<'a>(&'a self, value: &'a Value) -> &'a Value {
        match value.get("$ref").and_then(Value::as_str) {
            Some(pointer) => {
                let target = self
                    .doc
                    .pointer(pointer.trim_start_matches('#'))
                    .unwrap_or_else(|| panic!("dangling $ref {pointer}"));
                self.resolve(target)
            }
            None => value,
        }
    }

    /// Validates a request body against the operation's request schema.
    pub fn check_request(&self, method: &str, uri: &str, body: &Value) {
        let template = self.template(method, uri);
        let request = self.resolve(&self.operation(method, &template)["requestBody"]);
        assert!(
            !request.is_null(),
            "{method} {template}: the document describes no request body, but one was sent: {body}"
        );
        let schema = &request["content"]["application/json"]["schema"];
        if schema.is_null() {
            return; // a non-JSON request (e.g. a YAML scenario)
        }
        self.validate(schema, body, &format!("{method} {template} request"));
    }

    /// Validates a response: the operation must describe its status, and the body must match that response's schema
    /// (`application/json`; `text/plain` and `application/yaml` bodies are strings; no content means no body). The
    /// operation counts as exercised.
    pub fn check_response(&self, method: &str, uri: &str, status: u16, body: &Value) {
        let template = self.template(method, uri);
        let operation = self.operation(method, &template);
        let response = operation["responses"]
            .get(status.to_string())
            .map(|r| self.resolve(r))
            .unwrap_or_else(|| {
                panic!(
                    "{method} {template} answered {status}, which the document does not describe: {body}"
                )
            });
        let context = format!("{method} {template} {status}");
        match response.get("content").and_then(Value::as_object) {
            None => assert!(
                body.is_null(),
                "{context}: the document describes no body, but got {body}"
            ),
            Some(content) => {
                let (kind, media) = content.iter().next().expect("one media type");
                let instance = if kind == "application/json" {
                    body.clone()
                } else {
                    // The harness gives non-JSON bodies as strings.
                    json!(
                        body.as_str()
                            .map(str::to_string)
                            .unwrap_or_else(|| body.to_string())
                    )
                };
                self.validate(&media["schema"], &instance, &context);
            }
        }
        self.covered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((method.to_uppercase(), template));
    }

    /// The event types the document's `webhooks` section describes.
    pub fn event_types(&self) -> BTreeSet<String> {
        self.doc["webhooks"]
            .as_object()
            .map(|w| w.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Validates an event envelope against the schema the `webhooks` section gives its `eventType`.
    pub fn check_event(&self, event: &Value) {
        let event_type = event["eventType"].as_str().expect("an eventType");
        let hook = &self.doc["webhooks"][event_type]["post"];
        assert!(
            !hook.is_null(),
            "event type {event_type} is not in the webhooks section"
        );
        let body = self.resolve(&hook["requestBody"]);
        self.validate(
            &body["content"]["application/json"]["schema"],
            event,
            &format!("{event_type} event"),
        );
    }

    /// Validates `instance` against `schema` (a schema object of the document, `$ref`s resolved against it).
    pub fn validate(&self, schema: &Value, instance: &Value, context: &str) {
        let key = schema.to_string();
        let mut validators = self
            .validators
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let validator = validators.entry(key).or_insert_with(|| {
            // The whole document is the root, so `#/components/...` resolves; the schema to check sits in `$defs`.
            let mut root = self.doc.clone();
            root["$defs"] = json!({ "check": schema });
            root["$ref"] = json!("#/$defs/check");
            jsonschema::options()
                .with_draft(Draft::Draft202012)
                .should_validate_formats(true)
                .build(&root)
                .unwrap_or_else(|e| panic!("{context}: the schema does not compile: {e}"))
        });
        let errors: Vec<String> = validator
            .iter_errors(instance)
            .map(|e| format!("{} at {}", e, e.instance_path()))
            .collect();
        // Release the cache before failing, so one failed check does not poison the next.
        drop(validators);
        assert!(
            errors.is_empty(),
            "{context} does not match the OpenAPI document:\n  {}\n{}",
            errors.join("\n  "),
            serde_json::to_string_pretty(instance).unwrap_or_default()
        );
    }

    /// The parts of two documents that must agree, as JSON: the given paths and components (`"schemas/Clock"`,
    /// `"responses/NotFound"`, ...). Missing entries come back as `null`.
    pub fn sections(&self, paths: &[&str], components: &[&str]) -> BTreeMap<String, Value> {
        let mut out = BTreeMap::new();
        for path in paths {
            out.insert(format!("paths {path}"), self.doc["paths"][*path].clone());
        }
        for component in components {
            let pointer = format!("/components/{component}");
            out.insert(
                format!("components {component}"),
                self.doc.pointer(&pointer).cloned().unwrap_or(Value::Null),
            );
        }
        out
    }
}

/// `operations` with every path parameter written `{}`: parameter names do not reach the wire, so routes and the
/// document may name them differently.
pub fn without_parameter_names(operations: &BTreeSet<Operation>) -> BTreeSet<Operation> {
    let parameter = regex::Regex::new(r"\{[^}]*\}").expect("a valid pattern");
    operations
        .iter()
        .map(|(method, path)| {
            (
                method.clone(),
                parameter.replace_all(path, "{}").into_owned(),
            )
        })
        .collect()
}

/// The routes mounted in Rust sources: every `.route("<path>", <method>(..).<method>(..))`, as (method, path).
pub fn routes_in_source(sources: &[&str]) -> BTreeSet<Operation> {
    let start = regex::Regex::new(r#"\.route\(\s*"([^"]+)""#).expect("a valid pattern");
    let method = regex::Regex::new(r"\b(get|post|put|patch|delete)\(").expect("a valid pattern");
    let mut routes = BTreeSet::new();
    for source in sources {
        for found in start.captures_iter(source) {
            let path = found[1].to_string();
            // The method chain runs to the `.route(` call's closing parenthesis.
            let rest = &source[found.get(0).expect("a match").end()..];
            let mut depth = 1;
            let mut end = rest.len();
            for (i, c) in rest.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = i;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            for m in method.captures_iter(&rest[..end]) {
                routes.insert((m[1].to_uppercase(), path.clone()));
            }
        }
    }
    routes
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOC: &str = r##"
openapi: 3.1.0
info: { title: t, version: "1" }
paths:
  /things/{id}:
    get:
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema: { $ref: "#/components/schemas/Thing" }
        "404": { description: none }
  /things/special:
    get:
      responses:
        "200": { description: ok }
components:
  schemas:
    Thing:
      type: object
      additionalProperties: false
      required: [id, at]
      properties:
        id: { type: string }
        at: { type: string, format: date-time }
"##;

    #[test]
    fn responses_are_matched_to_templates_and_validated() {
        let spec = Spec::from_yaml(DOC);
        assert_eq!(spec.template("GET", "/things/42?x=1"), "/things/{id}");
        assert_eq!(
            spec.template("GET", "/things/special"),
            "/things/special",
            "literals win"
        );
        spec.check_response(
            "GET",
            "/things/1",
            200,
            &json!({ "id": "1", "at": "2030-01-07T09:00:00Z" }),
        );
        spec.check_response("GET", "/things/2", 404, &Value::Null);
        assert_eq!(spec.uncovered().len(), 1);
        for bad in [
            json!({ "id": "1" }),
            json!({ "id": "1", "at": "yesterday" }),
            json!({ "id": "1", "at": "2030-01-07T09:00:00Z", "extra": true }),
        ] {
            let failed =
                std::panic::catch_unwind(|| spec.check_response("GET", "/things/1", 200, &bad));
            let message = failed.expect_err("refused");
            let message = message
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_default();
            assert!(
                message.contains("does not match the OpenAPI document"),
                "{bad}: {message}"
            );
        }
        let undocumented =
            std::panic::catch_unwind(|| spec.check_response("GET", "/things/1", 500, &Value::Null));
        assert!(undocumented.is_err());
    }

    #[test]
    fn routes_are_read_from_source() {
        let source = r#"
            Router::new()
                .route("/a", post(register::<B>).get(list::<B>))
                .route(
                    "/a/{id}",
                    get(detail).patch(update).delete(delete),
                )
                .route("/b", axum::routing::put(set).delete(clear))
        "#;
        let routes: Vec<_> = routes_in_source(&[source]).into_iter().collect();
        let expected: Vec<Operation> = [
            ("DELETE", "/a/{id}"),
            ("DELETE", "/b"),
            ("GET", "/a"),
            ("GET", "/a/{id}"),
            ("PATCH", "/a/{id}"),
            ("POST", "/a"),
            ("PUT", "/b"),
        ]
        .into_iter()
        .map(|(m, p)| (m.to_string(), p.to_string()))
        .collect();
        assert_eq!(routes, expected);
    }
}
