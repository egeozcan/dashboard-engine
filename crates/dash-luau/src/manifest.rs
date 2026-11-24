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
            Self::Optional { inner } => {
                let mut s = inner.to_json_schema();
                if let Some(obj) = s.as_object_mut() {
                    obj.insert("nullable".into(), serde_json::Value::Bool(true));
                }
                s
            }
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
                let mut base = shape
                    .as_ref()
                    .map(|s| s.to_json_schema())
                    .unwrap_or_else(|| serde_json::json!({ "type": "object" }));
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
                SchemaSpec::Semantic { semantic_type: p_sem, .. },
                SchemaSpec::Semantic { semantic_type: c_sem, .. },
            ) => {
                if p_sem == c_sem {
                    Ok(())
                } else {
                    Err(format!(
                        "semantic type mismatch: producer '{p_sem}' != consumer '{c_sem}'"
                    ))
                }
            }
            (SchemaSpec::Semantic { shape: Some(p_shape), .. }, c) => {
                Self::is_compatible(p_shape, c)
            }
            (SchemaSpec::Semantic { shape: None, .. }, SchemaSpec::Object { .. }) => Ok(()),
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
    /// Declared to perform no side effects. Only `read_only` operations may run inside an action's
    /// Dry run or `reconcile` handler (§9.1); undeclared operations are treated as mutating.
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_only: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
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
            out.push_str(&format!("export type ActionResult = {}\n", r.to_luau_type()));
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
