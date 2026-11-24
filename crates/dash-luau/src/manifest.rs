//! Canonical Definition manifests, schema AST, JSON Schema conversion, and Luau type generation.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionCategory {
    Connector,
    Mock,
    DataProvider,
    ActionProvider,
    Adapter,
    WidgetProvider,
    Library,
    Automation,
}

impl DefinitionCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Connector => "connector",
            Self::Mock => "mock",
            Self::DataProvider => "data_provider",
            Self::ActionProvider => "action_provider",
            Self::Adapter => "adapter",
            Self::WidgetProvider => "widget_provider",
            Self::Library => "library",
            Self::Automation => "automation",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaSpec {
    String,
    Number,
    Integer,
    Boolean,
    Any,
    Enum {
        values: Vec<String>,
    },
    List {
        item: Box<SchemaSpec>,
    },
    Optional {
        inner: Box<SchemaSpec>,
    },
    Map {
        value: Box<SchemaSpec>,
    },
    Object {
        fields: BTreeMap<String, SchemaSpec>,
    },
    Semantic {
        semantic_type: String,
        #[serde(default)]
        shape: Option<Box<SchemaSpec>>,
    },
}

impl SchemaSpec {
    /// Extract semantic type tag if this schema (or list item) carries one.
    pub fn semantic_type_name(&self) -> Option<&str> {
        match self {
            Self::Semantic { semantic_type, .. } => Some(semantic_type.as_str()),
            Self::List { item } => item.semantic_type_name(),
            Self::Optional { inner } => inner.semantic_type_name(),
            _ => None,
        }
    }

    /// Visit every semantic reference (including nested ones) as `(name, declared shape)`.
    pub fn visit_semantics<'a>(&'a self, f: &mut dyn FnMut(&'a str, Option<&'a SchemaSpec>)) {
        match self {
            Self::List { item } => item.visit_semantics(f),
            Self::Optional { inner } => inner.visit_semantics(f),
            Self::Map { value } => value.visit_semantics(f),
            Self::Object { fields } => {
                for v in fields.values() {
                    v.visit_semantics(f);
                }
            }
            Self::Semantic {
                semantic_type,
                shape,
            } => {
                f(semantic_type.as_str(), shape.as_deref());
                if let Some(s) = shape {
                    s.visit_semantics(f);
                }
            }
            _ => {}
        }
    }

    /// Fill shapeless semantic references from `registry` (semantic id -> structural schema).
    /// Records each semantic id that was resolved into `used`. Recursive semantic definitions are
    /// left shapeless at the point of recursion.
    pub fn resolve_semantic_shapes(
        &mut self,
        registry: &BTreeMap<String, SchemaSpec>,
        used: &mut std::collections::BTreeSet<String>,
    ) {
        self.resolve_semantic_shapes_inner(registry, used, &mut Vec::new());
    }

    fn resolve_semantic_shapes_inner(
        &mut self,
        registry: &BTreeMap<String, SchemaSpec>,
        used: &mut std::collections::BTreeSet<String>,
        stack: &mut Vec<String>,
    ) {
        match self {
            Self::List { item } => item.resolve_semantic_shapes_inner(registry, used, stack),
            Self::Optional { inner } => inner.resolve_semantic_shapes_inner(registry, used, stack),
            Self::Map { value } => value.resolve_semantic_shapes_inner(registry, used, stack),
            Self::Object { fields } => {
                for v in fields.values_mut() {
                    v.resolve_semantic_shapes_inner(registry, used, stack);
                }
            }
            Self::Semantic {
                semantic_type,
                shape,
            } => {
                if stack.iter().any(|s| s == semantic_type) {
                    return;
                }
                stack.push(semantic_type.clone());
                match shape {
                    Some(s) => s.resolve_semantic_shapes_inner(registry, used, stack),
                    None => {
                        if let Some(reg) = registry.get(semantic_type.as_str()) {
                            used.insert(semantic_type.clone());
                            let mut filled = reg.clone();
                            filled.resolve_semantic_shapes_inner(registry, used, stack);
                            *shape = Some(Box::new(filled));
                        }
                    }
                }
                stack.pop();
            }
            _ => {}
        }
    }

    pub fn to_json_schema(&self) -> serde_json::Value {
        match self {
            Self::String => serde_json::json!({ "type": "string" }),
            Self::Number => serde_json::json!({ "type": "number" }),
            Self::Integer => serde_json::json!({ "type": "integer" }),
            Self::Boolean => serde_json::json!({ "type": "boolean" }),
            Self::Any => serde_json::json!({}),
            Self::Enum { values } => serde_json::json!({
                "type": "string",
                "enum": values
            }),
            Self::List { item } => serde_json::json!({
                "type": "array",
                "items": item.to_json_schema()
            }),
            // Standard JSON Schema has no `nullable`; express optionality as a union with null.
            Self::Optional { inner } => serde_json::json!({
                "anyOf": [inner.to_json_schema(), { "type": "null" }]
            }),
            Self::Map { value } => serde_json::json!({
                "type": "object",
                "additionalProperties": value.to_json_schema()
            }),
            Self::Object { fields } => {
                let mut props = serde_json::Map::new();
                let mut req = Vec::new();
                for (k, v) in fields {
                    props.insert(k.clone(), v.to_json_schema());
                    if !matches!(v, SchemaSpec::Optional { .. }) {
                        req.push(serde_json::Value::String(k.clone()));
                    }
                }
                serde_json::json!({
                    "type": "object",
                    "properties": props,
                    "required": req
                })
            }
            Self::Semantic {
                semantic_type,
                shape,
            } => {
                // A shapeless semantic reference carries no structural constraint of its own
                // (the runtime validator does not restrict it to objects either).
                let mut base = shape
                    .as_ref()
                    .map(|s| s.to_json_schema())
                    .unwrap_or_else(|| serde_json::json!({}));
                if let Some(obj) = base.as_object_mut() {
                    obj.insert(
                        "x-semantic-type".into(),
                        serde_json::Value::String(semantic_type.clone()),
                    );
                }
                base
            }
        }
    }

    /// Luau has a single table type, so an empty list returned by a handler (`return {}`)
    /// crosses the VM boundary as an empty JSON object. Rewrite such values to `[]` wherever this
    /// schema expects a list, so empty results satisfy their own contract (and stay lists for
    /// deltas and consumers). Call before [`Self::validate_value`] on handler outputs.
    pub fn normalize_empty_lists(&self, val: &mut serde_json::Value) {
        match self {
            Self::List { item } => {
                if val.as_object().is_some_and(|o| o.is_empty()) {
                    *val = serde_json::Value::Array(Vec::new());
                }
                if let Some(arr) = val.as_array_mut() {
                    for elem in arr {
                        item.normalize_empty_lists(elem);
                    }
                }
            }
            Self::Optional { inner } => inner.normalize_empty_lists(val),
            Self::Map { value } => {
                if let Some(obj) = val.as_object_mut() {
                    for v in obj.values_mut() {
                        value.normalize_empty_lists(v);
                    }
                }
            }
            Self::Object { fields } => {
                if let Some(obj) = val.as_object_mut() {
                    for (k, field_schema) in fields {
                        if let Some(v) = obj.get_mut(k) {
                            field_schema.normalize_empty_lists(v);
                        }
                    }
                }
            }
            Self::Semantic {
                shape: Some(inner), ..
            } => inner.normalize_empty_lists(val),
            _ => {}
        }
    }

    pub fn validate_value(&self, val: &serde_json::Value) -> Result<(), String> {
        self.validate_at("$", val)
    }

    fn validate_at(&self, path: &str, val: &serde_json::Value) -> Result<(), String> {
        match self {
            Self::Any => Ok(()),
            Self::String => {
                if val.is_string() {
                    Ok(())
                } else {
                    Err(format!("{path}: expected string, got {val}"))
                }
            }
            Self::Number => {
                if val.is_number() {
                    Ok(())
                } else {
                    Err(format!("{path}: expected number, got {val}"))
                }
            }
            Self::Integer => {
                if val.is_i64() || val.is_u64() {
                    Ok(())
                } else {
                    Err(format!("{path}: expected integer, got {val}"))
                }
            }
            Self::Boolean => {
                if val.is_boolean() {
                    Ok(())
                } else {
                    Err(format!("{path}: expected boolean, got {val}"))
                }
            }
            Self::Enum { values } => {
                let Some(s) = val.as_str() else {
                    return Err(format!("{path}: expected enum string, got {val}"));
                };
                if values.iter().any(|v| v == s) {
                    Ok(())
                } else {
                    Err(format!("{path}: '{s}' not in enum {values:?}"))
                }
            }
            Self::Optional { inner } => {
                if val.is_null() {
                    Ok(())
                } else {
                    inner.validate_at(path, val)
                }
            }
            Self::List { item } => {
                let Some(arr) = val.as_array() else {
                    return Err(format!("{path}: expected array, got {val}"));
                };
                for (idx, elem) in arr.iter().enumerate() {
                    item.validate_at(&format!("{path}[{idx}]"), elem)?;
                }
                Ok(())
            }
            Self::Map { value } => {
                let Some(obj) = val.as_object() else {
                    return Err(format!("{path}: expected object map, got {val}"));
                };
                for (k, v) in obj {
                    value.validate_at(&format!("{path}.{k}"), v)?;
                }
                Ok(())
            }
            Self::Object { fields } => {
                let Some(obj) = val.as_object() else {
                    return Err(format!("{path}: expected object, got {val}"));
                };
                for (k, field_schema) in fields {
                    match obj.get(k) {
                        None => {
                            if !matches!(field_schema, SchemaSpec::Optional { .. }) {
                                return Err(format!("{path}.{k}: missing required field"));
                            }
                        }
                        Some(v) => field_schema.validate_at(&format!("{path}.{k}"), v)?,
                    }
                }
                Ok(())
            }
            Self::Semantic { shape, .. } => {
                if let Some(inner) = shape {
                    inner.validate_at(path, val)
                } else if val.is_object() || val.is_array() || val.is_string() || val.is_number() {
                    Ok(())
                } else {
                    Err(format!("{path}: invalid value for semantic type: {val}"))
                }
            }
        }
    }

    /// Generate a Luau `--!strict` type expression for Package Studio autocomplete.
    pub fn to_luau_type(&self) -> String {
        match self {
            Self::String | Self::Enum { .. } => "string".into(),
            Self::Number | Self::Integer => "number".into(),
            Self::Boolean => "boolean".into(),
            Self::Any => "any".into(),
            Self::Optional { inner } => format!("({})?", inner.to_luau_type()),
            Self::List { item } => format!("{{ {} }}", item.to_luau_type()),
            Self::Map { value } => format!("{{ [string]: {} }}", value.to_luau_type()),
            Self::Object { fields } => {
                if fields.is_empty() {
                    return "{ [string]: any }".into();
                }
                let parts: Vec<String> = fields
                    .iter()
                    .map(|(k, v)| format!("{k}: {}", v.to_luau_type()))
                    .collect();
                format!("{{ {} }}", parts.join(", "))
            }
            Self::Semantic { shape, .. } => shape
                .as_ref()
                .map(|s| s.to_luau_type())
                .unwrap_or_else(|| "{ [string]: any }".into()),
        }
    }

    /// Check whether `producer` satisfies `consumer`.
    pub fn is_compatible(producer: &SchemaSpec, consumer: &SchemaSpec) -> Result<(), String> {
        match (producer, consumer) {
            (_, SchemaSpec::Any) => Ok(()),
            (SchemaSpec::Optional { inner: p }, SchemaSpec::Optional { inner: c }) => {
                Self::is_compatible(p, c)
            }
            (p, SchemaSpec::Optional { inner: c }) => Self::is_compatible(p, c),
            (
                SchemaSpec::List { item: p },
                SchemaSpec::List { item: c },
            ) => Self::is_compatible(p, c),
            (
                SchemaSpec::Semantic { semantic_type: p_sem, shape: p_shape },
                SchemaSpec::Semantic { semantic_type: c_sem, shape: c_shape },
            ) => {
                if p_sem == c_sem {
                    // Same semantic name must also agree structurally when both sides carry shapes.
                    match (p_shape, c_shape) {
                        (Some(p), Some(c)) => Self::is_compatible(p, c).map_err(|e| {
                            format!("semantic type '{p_sem}' structural mismatch: {e}")
                        }),
                        _ => Ok(()),
                    }
                } else {
                    Err(format!(
                        "semantic type mismatch: producer '{p_sem}' != consumer '{c_sem}'"
                    ))
                }
            }
            (SchemaSpec::Semantic { shape: Some(p_shape), .. }, c) => {
                Self::is_compatible(p_shape, c)
            }
            (SchemaSpec::Semantic { semantic_type, shape: None }, SchemaSpec::Object { .. }) => {
                Err(format!(
                    "semantic type '{semantic_type}' has no structural schema; cannot verify it satisfies the consumer object"
                ))
            }
            (
                SchemaSpec::Object { fields: p_fields },
                SchemaSpec::Object { fields: c_fields },
            ) => {
                for (k, c_ty) in c_fields {
                    match p_fields.get(k) {
                        None if matches!(c_ty, SchemaSpec::Optional { .. }) => {}
                        None => {
                            return Err(format!("missing required field '{k}' on producer output"))
                        }
                        Some(p_ty) => Self::is_compatible(p_ty, c_ty)?,
                    }
                }
                Ok(())
            }
            (p, c) if p == c => Ok(()),
            (SchemaSpec::Integer, SchemaSpec::Number) => Ok(()),
            (SchemaSpec::Enum { .. }, SchemaSpec::String) => Ok(()),
            (p, c) => Err(format!("incompatible schemas: producer {p:?} vs consumer {c:?}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "ref_kind", rename_all = "snake_case")]
pub enum InputRefSpec {
    Connector {
        definition: Option<String>,
        #[serde(default)]
        required_capabilities: Vec<String>,
    },
    Data {
        definition: Option<String>,
        #[serde(default)]
        expected_output: Option<SchemaSpec>,
    },
    Action {
        definition: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UpdateModes {
    #[serde(default = "default_true")]
    pub on_demand: bool,
    #[serde(default)]
    pub subscribe: bool,
    #[serde(default)]
    pub poll_interval_secs: Option<u64>,
    #[serde(default)]
    pub push: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorOperationSpec {
    pub description: String,
    pub required_capabilities: Vec<String>,
    pub args: SchemaSpec,
    pub result: SchemaSpec,
    /// Side-effect classification: `"read"` (no external side effects) or `"write"`.
    /// When omitted the host infers `read` only if the operation requires at least one
    /// capability and every required capability ends in `.read`; otherwise `write`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Declared to perform no side effects (equivalent to `mode = "read"`; the declaration
    /// parser keeps both in sync). Only read operations may run inside an action's Dry run or
    /// `reconcile` handler (§9.1).
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl ConnectorOperationSpec {
    /// Whether the operation may cause external side effects (deny by default, §20).
    pub fn has_side_effects(&self) -> bool {
        if self.read_only {
            return false;
        }
        match self.mode.as_deref() {
            Some("read") => false,
            Some(_) => true,
            None => {
                self.required_capabilities.is_empty()
                    || !self
                        .required_capabilities
                        .iter()
                        .all(|c| c.ends_with(".read"))
            }
        }
    }

    /// Whether the operation is a read (no side effects): the single classification used by
    /// data providers, Dry runs and reconcile handlers.
    pub fn is_read_only(&self) -> bool {
        !self.has_side_effects()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventDeclSpec {
    pub semantic_type: String,
    pub schema_version: String,
    #[serde(default)]
    pub durable: bool,
    #[serde(default)]
    pub payload: Option<SchemaSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerDeclSpec {
    pub kind: String, // cron | event | signal
    #[serde(default)]
    pub schedule: Option<String>,
    #[serde(default)]
    pub event_type: Option<String>,
    #[serde(default)]
    pub source_input: Option<String>,
    #[serde(default)]
    pub catch_up: Option<String>, // skip | catch_up
    /// Signal triggers: query parameters for the watched `source_input` data provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    /// Signal triggers: State key to watch instead of a Query result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
    /// Signal triggers: optional read-only Luau predicate function in the runtime module.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    /// Bounded retry attempts before dead-lettering (engine default: 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionManifest {
    pub id: String,
    pub category: DefinitionCategory,
    pub identity_version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub runtime: String,
    #[serde(default)]
    pub handler: String,
    #[serde(default)]
    pub dry_run_handler: Option<String>,
    #[serde(default)]
    pub reconcile_handler: Option<String>,
    #[serde(default)]
    pub request_handler: Option<String>,
    /// For `Mock`: target connector definition ID (`for = "..."`).
    #[serde(default)]
    pub target_connector: Option<String>,
    #[serde(default)]
    pub fixtures: BTreeMap<String, String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub optional_capabilities: Vec<String>,
    #[serde(default)]
    pub credentials: Vec<String>,
    #[serde(default)]
    pub operations: BTreeMap<String, ConnectorOperationSpec>,
    #[serde(default)]
    pub inputs: BTreeMap<String, InputRefSpec>,
    #[serde(default)]
    pub config: Option<SchemaSpec>,
    #[serde(default)]
    pub query: Option<SchemaSpec>,
    #[serde(default)]
    pub args: Option<SchemaSpec>,
    #[serde(default)]
    pub output: Option<SchemaSpec>,
    #[serde(default)]
    pub result: Option<SchemaSpec>,
    #[serde(default)]
    pub emits: Vec<EventDeclSpec>,
    #[serde(default)]
    pub invalidates: Vec<String>,
    #[serde(default)]
    pub dashboard_state_reads: Vec<String>,
    #[serde(default)]
    pub accepts_requests: Vec<String>,
    /// Payload schemas for accepted widget Requests (`requests = { name = schema }`), validated by the
    /// host before dispatch (Appendix A.3).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub request_schemas: BTreeMap<String, SchemaSpec>,
    #[serde(default)]
    pub widget_props: Option<SchemaSpec>,
    #[serde(default)]
    pub widget_asset: Option<String>,
    #[serde(default)]
    pub updates: UpdateModes,
    #[serde(default)]
    pub triggers: Vec<TriggerDeclSpec>,
    #[serde(default)]
    pub libraries: Vec<String>,
    #[serde(default)]
    pub generated_luau_types: String,
}

impl DefinitionManifest {
    /// Every schema declared by this manifest (contract fields, input expectations, operation
    /// schemas and event payloads), in a deterministic order.
    pub fn schemas(&self) -> Vec<&SchemaSpec> {
        let mut out: Vec<&SchemaSpec> = Vec::new();
        for s in [
            &self.config,
            &self.query,
            &self.args,
            &self.output,
            &self.result,
            &self.widget_props,
        ]
        .into_iter()
        .flatten()
        {
            out.push(s);
        }
        for spec in self.inputs.values() {
            if let InputRefSpec::Data {
                expected_output: Some(s),
                ..
            } = spec
            {
                out.push(s);
            }
        }
        for op in self.operations.values() {
            out.push(&op.args);
            out.push(&op.result);
        }
        for e in &self.emits {
            if let Some(p) = &e.payload {
                out.push(p);
            }
        }
        out
    }

    /// Mutable counterpart of [`DefinitionManifest::schemas`].
    pub fn schemas_mut(&mut self) -> Vec<&mut SchemaSpec> {
        let mut out: Vec<&mut SchemaSpec> = Vec::new();
        for s in [
            &mut self.config,
            &mut self.query,
            &mut self.args,
            &mut self.output,
            &mut self.result,
            &mut self.widget_props,
        ]
        .into_iter()
        .flatten()
        {
            out.push(s);
        }
        for spec in self.inputs.values_mut() {
            if let InputRefSpec::Data {
                expected_output: Some(s),
                ..
            } = spec
            {
                out.push(s);
            }
        }
        for op in self.operations.values_mut() {
            out.push(&mut op.args);
            out.push(&mut op.result);
        }
        for e in &mut self.emits {
            if let Some(p) = &mut e.payload {
                out.push(p);
            }
        }
        out
    }

    /// Deterministic canonical JSON representation of the manifest.
    pub fn canonical_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn generate_luau_types(&mut self) {
        let mut out = String::new();
        out.push_str("-- Auto-generated Luau type declarations for ");
        out.push_str(&self.id);
        out.push('\n');
        if let Some(ref q) = self.query {
            out.push_str(&format!("export type QueryParams = {}\n", q.to_luau_type()));
        }
        if let Some(ref a) = self.args {
            out.push_str(&format!("export type ActionArgs = {}\n", a.to_luau_type()));
        }
        if let Some(ref o) = self.output {
            out.push_str(&format!("export type OutputData = {}\n", o.to_luau_type()));
        }
        if let Some(ref r) = self.result {
            out.push_str(&format!(
                "export type ActionResult = {}\n",
                r.to_luau_type()
            ));
        }
        self.generated_luau_types = out;
    }
}

/// Compute content digest (`identity_version`) across resolved source files in deterministic order.
pub fn compute_sources_digest(files: &BTreeMap<String, Vec<u8>>) -> String {
    let mut hasher = Sha256::new();
    for (rel_path, content) in files {
        hasher.update(rel_path.as_bytes());
        hasher.update(b"\0");
        hasher.update((content.len() as u64).to_le_bytes());
        hasher.update(content);
        hasher.update(b"\0");
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

/// Build the Workspace semantic type registry (§4.2: a semantic contract is a named, versioned
/// type *paired with a structural schema*). Each semantic id maps to exactly one structural
/// schema taken from the Definitions that declare it with a shape. Two different shapes for the
/// same id are an error, as is a semantic id used anywhere without any declared shape.
///
/// `manifests` yields `(definition id, manifest)` pairs.
pub fn build_semantic_registry<'a>(
    manifests: impl IntoIterator<Item = (&'a str, &'a DefinitionManifest)>,
) -> Result<BTreeMap<String, SchemaSpec>, String> {
    let mut registry: BTreeMap<String, (SchemaSpec, String)> = BTreeMap::new();
    let mut used_by: BTreeMap<String, String> = BTreeMap::new();
    let mut conflict: Option<String> = None;
    for (def_id, m) in manifests {
        for schema in m.schemas() {
            schema.visit_semantics(&mut |name, shape| {
                used_by
                    .entry(name.to_string())
                    .or_insert_with(|| def_id.to_string());
                let Some(shape) = shape else { return };
                match registry.get(name) {
                    Some((existing, owner)) if existing != shape => {
                        if conflict.is_none() {
                            conflict = Some(format!(
                                "semantic type '{name}' has conflicting structural schemas in '{owner}' and '{def_id}'"
                            ));
                        }
                    }
                    Some(_) => {}
                    None => {
                        registry.insert(name.to_string(), (shape.clone(), def_id.to_string()));
                    }
                }
            });
        }
    }
    if let Some(c) = conflict {
        return Err(c);
    }
    for (name, def_id) in &used_by {
        if !registry.contains_key(name) {
            return Err(format!(
                "semantic type '{name}' (used by '{def_id}') has no structural schema; declare it once with dash.semantic(\"{name}\", <schema>)"
            ));
        }
    }
    Ok(registry.into_iter().map(|(k, (v, _))| (k, v)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_luau_tables_become_lists_where_the_schema_declares_lists() {
        let schema = SchemaSpec::List {
            item: Box::new(SchemaSpec::Object {
                fields: [(
                    "tags".to_string(),
                    SchemaSpec::List {
                        item: Box::new(SchemaSpec::String),
                    },
                )]
                .into_iter()
                .collect(),
            }),
        };
        let mut top = serde_json::json!({});
        schema.normalize_empty_lists(&mut top);
        assert_eq!(top, serde_json::json!([]));
        assert!(schema.validate_value(&top).is_ok());

        let mut nested = serde_json::json!([{ "tags": {} }, { "tags": ["a"] }]);
        schema.normalize_empty_lists(&mut nested);
        assert_eq!(
            nested,
            serde_json::json!([{ "tags": [] }, { "tags": ["a"] }])
        );

        // Maps and objects keep `{}`; non-empty objects are not reinterpreted as lists.
        let map = SchemaSpec::Map {
            value: Box::new(SchemaSpec::Any),
        };
        let mut m = serde_json::json!({});
        map.normalize_empty_lists(&mut m);
        assert_eq!(m, serde_json::json!({}));
        let mut not_list = serde_json::json!({ "a": 1 });
        schema.normalize_empty_lists(&mut not_list);
        assert!(schema.validate_value(&not_list).is_err());
    }

    fn obj(fields: &[(&str, SchemaSpec)]) -> SchemaSpec {
        SchemaSpec::Object {
            fields: fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    fn sem(name: &str, shape: Option<SchemaSpec>) -> SchemaSpec {
        SchemaSpec::Semantic {
            semantic_type: name.into(),
            shape: shape.map(Box::new),
        }
    }

    #[test]
    fn json_schema_uses_standard_keywords() {
        let opt = SchemaSpec::Optional {
            inner: Box::new(SchemaSpec::String),
        };
        assert_eq!(
            opt.to_json_schema(),
            serde_json::json!({ "anyOf": [{ "type": "string" }, { "type": "null" }] })
        );
        assert_eq!(
            sem("dev.pr@1", None).to_json_schema(),
            serde_json::json!({ "x-semantic-type": "dev.pr@1" })
        );
    }

    #[test]
    fn semantic_compatibility_compares_shapes() {
        let a = sem("dev.pr@1", Some(obj(&[("id", SchemaSpec::String)])));
        let b = sem("dev.pr@1", Some(obj(&[("id", SchemaSpec::Integer)])));
        assert!(SchemaSpec::is_compatible(&a, &a).is_ok());
        assert!(SchemaSpec::is_compatible(&a, &b).is_err());
        assert!(SchemaSpec::is_compatible(&sem("dev.pr@1", None), &obj(&[])).is_err());
        assert!(SchemaSpec::is_compatible(&sem("dev.pr@1", None), &sem("dev.pr@1", None)).is_ok());
        assert!(SchemaSpec::is_compatible(&sem("dev.pr@1", None), &sem("dev.pr@2", None)).is_err());
    }

    fn manifest_with_output(id: &str, output: SchemaSpec) -> DefinitionManifest {
        DefinitionManifest {
            id: id.into(),
            category: DefinitionCategory::DataProvider,
            identity_version: String::new(),
            description: String::new(),
            runtime: String::new(),
            handler: String::new(),
            dry_run_handler: None,
            reconcile_handler: None,
            request_handler: None,
            target_connector: None,
            fixtures: BTreeMap::new(),
            capabilities: vec![],
            required_capabilities: vec![],
            optional_capabilities: vec![],
            credentials: vec![],
            operations: BTreeMap::new(),
            inputs: BTreeMap::new(),
            config: None,
            query: None,
            args: None,
            output: Some(output),
            result: None,
            emits: vec![],
            invalidates: vec![],
            dashboard_state_reads: vec![],
            accepts_requests: vec![],
            request_schemas: BTreeMap::new(),
            widget_props: None,
            widget_asset: None,
            updates: UpdateModes::default(),
            triggers: vec![],
            libraries: vec![],
            generated_luau_types: String::new(),
        }
    }

    #[test]
    fn registry_pairs_each_semantic_type_with_one_schema() {
        let shape = obj(&[("id", SchemaSpec::String)]);
        let producer = manifest_with_output("a.p", sem("dev.pr@1", Some(shape.clone())));
        let mut user = manifest_with_output(
            "a.u",
            SchemaSpec::List {
                item: Box::new(sem("dev.pr@1", None)),
            },
        );
        let reg = build_semantic_registry([("a.p", &producer), ("a.u", &user)]).unwrap();
        assert_eq!(reg["dev.pr@1"], shape);

        let mut used = std::collections::BTreeSet::new();
        for s in user.schemas_mut() {
            s.resolve_semantic_shapes(&reg, &mut used);
        }
        assert!(used.contains("dev.pr@1"));
        assert!(user
            .output
            .as_ref()
            .unwrap()
            .validate_value(&serde_json::json!([{ "id": 5 }]))
            .is_err());

        let other = manifest_with_output(
            "a.o",
            sem("dev.pr@1", Some(obj(&[("id", SchemaSpec::Integer)]))),
        );
        assert!(build_semantic_registry([("a.p", &producer), ("a.o", &other)]).is_err());
        let orphan = manifest_with_output("a.x", sem("dev.none@1", None));
        assert!(build_semantic_registry([("a.x", &orphan)]).is_err());
    }

    #[test]
    fn empty_luau_tables_become_lists_where_the_schema_expects_one() {
        let schema = obj(&[
            (
                "rows",
                SchemaSpec::List {
                    item: Box::new(obj(&[(
                        "tags",
                        SchemaSpec::List {
                            item: Box::new(SchemaSpec::String),
                        },
                    )])),
                },
            ),
            (
                "meta",
                SchemaSpec::Map {
                    value: Box::new(SchemaSpec::Any),
                },
            ),
        ]);
        let mut v = serde_json::json!({ "rows": {}, "meta": {} });
        assert!(schema.validate_value(&v).is_err());
        schema.normalize_empty_lists(&mut v);
        assert_eq!(v, serde_json::json!({ "rows": [], "meta": {} }));
        let mut nested = serde_json::json!({ "rows": [{ "tags": {} }], "meta": {} });
        schema.normalize_empty_lists(&mut nested);
        assert_eq!(nested["rows"][0]["tags"], serde_json::json!([]));
        schema.validate_value(&nested).unwrap();
        // A non-empty object where a list is expected is still a contract violation.
        let mut bad = serde_json::json!({ "rows": { "a": 1 }, "meta": {} });
        schema.normalize_empty_lists(&mut bad);
        assert!(schema.validate_value(&bad).is_err());
    }
}
