//! Restricted deterministic Luau declaration evaluator.
//!
//! Executes `definition.luau` inside a sandboxed Luau VM with NO `os`, `io`, `debug`,
//! `package`, or non-deterministic `math.random` / `time` functions. Rejects non-deterministic
//! declarations and invalid contracts, producing a deterministic `DefinitionManifest`.

use crate::manifest::{
    compute_sources_digest, ConnectorOperationSpec, DefinitionCategory, DefinitionManifest,
    EventDeclSpec, InputRefSpec, SchemaSpec, TriggerDeclSpec, UpdateModes,
};
use mlua::{Function, Lua, LuaOptions, StdLib, Table, Value, VmState};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeclarationError {
    #[error("luau error during declaration: {0}")]
    Luau(#[from] mlua::Error),
    #[error("invalid contract for definition: {0}")]
    InvalidContract(String),
    #[error("forbidden non-deterministic API used during declaration: {0}")]
    NonDeterministic(String),
}

/// Evaluates `definition.luau` source and resolved accompanying files into a deterministic manifest.
pub fn evaluate_declaration(
    definition_source: &str,
    resolved_sources: &BTreeMap<String, Vec<u8>>,
) -> Result<DefinitionManifest, DeclarationError> {
    let libs = StdLib::TABLE | StdLib::STRING | StdLib::UTF8 | StdLib::BIT | StdLib::MATH;
    let lua = Lua::new_with(libs, LuaOptions::default())?;
    lua.set_memory_limit(8 * 1024 * 1024)?; // 8 MiB max for declaration phase

    let steps = Arc::new(AtomicU64::new(0));
    let steps_ref = Arc::clone(&steps);
    lua.set_interrupt(move |_| {
        if steps_ref.fetch_add(1, Ordering::Relaxed) > 100_000 {
            return Err(mlua::Error::runtime(
                "declaration phase instruction budget exceeded",
            ));
        }
        Ok(VmState::Continue)
    });

    // Replace math.random / math.randomseed with explicit determinism rejection
    {
        let math_tbl: Table = lua.globals().get("math")?;
        let forbid_rand = lua.create_function(|_, _: mlua::MultiValue| -> mlua::Result<()> {
            Err(mlua::Error::runtime(
                "NON_DETERMINISTIC: math.random is forbidden during declaration phase",
            ))
        })?;
        math_tbl.set("random", forbid_rand.clone())?;
        math_tbl.set("randomseed", forbid_rand)?;
    }

    let dash_mod = build_dash_declaration_sdk(&lua)?;
    let require_fn = lua.create_function(move |_, name: String| -> mlua::Result<Table> {
        if name == "dash" {
            Ok(dash_mod.clone())
        } else {
            Err(mlua::Error::runtime(format!(
                "declaration phase may only require(\"dash\"), got \"{name}\""
            )))
        }
    })?;
    lua.globals().set("require", require_fn)?;
    lua.sandbox(true)?;

    let result: Value = lua
        .load(definition_source)
        .set_name("definition.luau")
        .eval()
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("NON_DETERMINISTIC") {
                DeclarationError::NonDeterministic(msg)
            } else {
                DeclarationError::Luau(e)
            }
        })?;

    let tbl = match result {
        Value::Table(t) => t,
        _ => {
            return Err(DeclarationError::InvalidContract(
                "definition.luau must return a table from dash.<category>(...)".into(),
            ))
        }
    };

    let mut manifest = parse_declaration_table(&tbl)?;
    manifest.identity_version = compute_sources_digest(resolved_sources);
    manifest.generate_luau_types();
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn build_dash_declaration_sdk(lua: &Lua) -> mlua::Result<Table> {
    let dash = lua.create_table()?;

    // Category constructors
    let categories = [
        ("connector", "connector"),
        ("mock", "mock"),
        ("data_provider", "data_provider"),
        ("action_provider", "action_provider"),
        ("adapter", "adapter"),
        ("widget_provider", "widget_provider"),
        ("library", "library"),
        ("automation", "automation"),
    ];
    for (fn_name, cat_name) in categories {
        let cat = cat_name.to_string();
        let func = lua.create_function(move |lua, spec: Table| {
            spec.set("__dash_category", cat.clone())?;
            let _ = lua;
            Ok(spec)
        })?;
        dash.set(fn_name, func)?;
    }

    // Schema primitives
    for prim in ["string", "number", "integer", "boolean", "any"] {
        let k = prim.to_string();
        dash.set(
            prim,
            lua.create_function(move |lua, ()| {
                let t = lua.create_table()?;
                t.set("kind", k.clone())?;
                Ok(t)
            })?,
        )?;
    }

    dash.set(
        "enum",
        lua.create_function(|lua, values: Vec<String>| {
            let t = lua.create_table()?;
            t.set("kind", "enum")?;
            t.set("values", values)?;
            Ok(t)
        })?,
    )?;

    dash.set(
        "list",
        lua.create_function(|lua, item: Value| {
            let t = lua.create_table()?;
            t.set("kind", "list")?;
            match item {
                Value::String(s) => {
                    let sem = lua.create_table()?;
                    sem.set("kind", "semantic")?;
                    sem.set("semantic_type", s.to_str()?.to_string())?;
                    t.set("item", sem)?;
                }
                Value::Table(tbl) => t.set("item", tbl)?,
                _ => {
                    return Err(mlua::Error::runtime(
                        "dash.list expects a semantic type string or schema table",
                    ))
                }
            }
            Ok(t)
        })?,
    )?;

    dash.set(
        "optional",
        lua.create_function(|lua, inner: Value| {
            let t = lua.create_table()?;
            t.set("kind", "optional")?;
            match inner {
                Value::String(s) => {
                    let sem = lua.create_table()?;
                    sem.set("kind", "semantic")?;
                    sem.set("semantic_type", s.to_str()?.to_string())?;
                    t.set("inner", sem)?;
                }
                Value::Table(tbl) => t.set("inner", tbl)?,
                _ => {
                    return Err(mlua::Error::runtime(
                        "dash.optional expects a semantic type string or schema table",
                    ))
                }
            }
            Ok(t)
        })?,
    )?;

    dash.set(
        "map",
        lua.create_function(|lua, val_schema: Table| {
            let t = lua.create_table()?;
            t.set("kind", "map")?;
            t.set("value", val_schema)?;
            Ok(t)
        })?,
    )?;

    dash.set(
        "object",
        lua.create_function(|lua, fields: Table| {
            let t = lua.create_table()?;
            t.set("kind", "object")?;
            t.set("fields", fields)?;
            Ok(t)
        })?,
    )?;

    dash.set(
        "semantic",
        lua.create_function(|lua, (sem_type, shape): (String, Option<Table>)| {
            let t = lua.create_table()?;
            t.set("kind", "semantic")?;
            t.set("semantic_type", sem_type)?;
            if let Some(s) = shape {
                t.set("shape", s)?;
            }
            Ok(t)
        })?,
    )?;

    // Input refs
    dash.set(
        "connector_ref",
        lua.create_function(|lua, (def_id, opts): (Option<String>, Option<Table>)| {
            let t = lua.create_table()?;
            t.set("ref_kind", "connector")?;
            if let Some(d) = def_id {
                t.set("definition", d)?;
            }
            if let Some(o) = opts {
                if let Ok(caps) = o.get::<Value>("required_capabilities") {
                    t.set("required_capabilities", caps)?;
                }
            }
            Ok(t)
        })?,
    )?;

    dash.set(
        "data_ref",
        lua.create_function(|lua, (def_id, opts): (Option<String>, Option<Table>)| {
            let t = lua.create_table()?;
            t.set("ref_kind", "data")?;
            if let Some(d) = def_id {
                t.set("definition", d)?;
            }
            if let Some(o) = opts {
                if let Ok(exp) = o.get::<Value>("expected_output") {
                    t.set("expected_output", exp)?;
                }
            }
            Ok(t)
        })?,
    )?;

    dash.set(
        "action_ref",
        lua.create_function(|lua, def_id: Option<String>| {
            let t = lua.create_table()?;
            t.set("ref_kind", "action")?;
            if let Some(d) = def_id {
                t.set("definition", d)?;
            }
            Ok(t)
        })?,
    )?;

    // Provide dummy assert/test symbols so shared helpers won't crash if inspected, though tests live in tests.luau
    let noop: Function = lua.create_function(|_, _: mlua::MultiValue| Ok(()))?;
    dash.set("noop", noop)?;

    Ok(dash)
}

fn parse_declaration_table(tbl: &Table) -> Result<DefinitionManifest, DeclarationError> {
    let cat_str: String = tbl.get("__dash_category").map_err(|_| {
        DeclarationError::InvalidContract(
            "definition must be constructed using dash.<category>(...)".into(),
        )
    })?;
    let category = match cat_str.as_str() {
        "connector" => DefinitionCategory::Connector,
        "mock" => DefinitionCategory::Mock,
        "data_provider" => DefinitionCategory::DataProvider,
        "action_provider" => DefinitionCategory::ActionProvider,
        "adapter" => DefinitionCategory::Adapter,
        "widget_provider" => DefinitionCategory::WidgetProvider,
        "library" => DefinitionCategory::Library,
        "automation" => DefinitionCategory::Automation,
        other => {
            return Err(DeclarationError::InvalidContract(format!(
                "unknown category '{other}'"
            )))
        }
    };

    let id: String = tbl
        .get("id")
        .map_err(|_| DeclarationError::InvalidContract("missing required field 'id'".into()))?;

    let description: String = tbl.get("description").unwrap_or_default();
    let runtime: String = tbl.get("runtime").unwrap_or_else(|_| "runtime.luau".into());
    let handler: String = tbl.get("handler").unwrap_or_else(|_| match category {
        DefinitionCategory::DataProvider => "query".into(),
        DefinitionCategory::ActionProvider => "execute".into(),
        DefinitionCategory::Adapter => "render".into(),
        DefinitionCategory::Connector | DefinitionCategory::Mock => "call".into(),
        DefinitionCategory::Automation => "run".into(),
        DefinitionCategory::Library | DefinitionCategory::WidgetProvider => String::new(),
    });
    let dry_run_handler: Option<String> = tbl.get("dry_run").ok();
    let reconcile_handler: Option<String> = tbl.get("reconcile").ok();
    let request_handler: Option<String> = tbl.get("on_request").ok();
    let target_connector: Option<String> = tbl.get("for").ok();

    let fixtures = parse_string_map(tbl, "fixtures")?;
    let capabilities = parse_string_vec(tbl, "capabilities")?;
    let required_capabilities = parse_string_vec(tbl, "required_capabilities")?;
    let optional_capabilities = parse_string_vec(tbl, "optional_capabilities")?;
    let credentials = parse_string_vec(tbl, "credentials")?;
    let invalidates = parse_string_vec(tbl, "invalidates")?;
    let dashboard_state_reads = parse_string_vec(tbl, "dashboard_state_reads")?;
    let accepts_requests = parse_string_vec(tbl, "accepts_requests")?;
    let libraries = parse_string_vec(tbl, "libraries")?;
    let widget_asset: Option<String> = tbl.get("asset").ok();

    let operations = parse_operations(tbl)?;
    let inputs = parse_inputs(tbl)?;
    let config = parse_opt_schema(tbl, "config")?;
    let query = parse_opt_schema(tbl, "query")?;
    let args = parse_opt_schema(tbl, "args")?;
    let output = parse_opt_schema(tbl, "output")?;
    let result = parse_opt_schema(tbl, "result")?;
    let widget_props = parse_opt_schema(tbl, "props")?;
    let emits = parse_emits(tbl)?;
    let updates = parse_updates(tbl)?;
    let triggers = parse_triggers(tbl)?;

    Ok(DefinitionManifest {
        id,
        category,
        identity_version: String::new(),
        description,
        runtime,
        handler,
        dry_run_handler,
        reconcile_handler,
        request_handler,
        target_connector,
        fixtures,
        capabilities,
        required_capabilities,
        optional_capabilities,
        credentials,
        operations,
        inputs,
        config,
        query,
        args,
        output,
        result,
        emits,
        invalidates,
        dashboard_state_reads,
        accepts_requests,
        widget_props,
        widget_asset,
        updates,
        triggers,
        libraries,
        generated_luau_types: String::new(),
    })
}

fn validate_manifest(m: &DefinitionManifest) -> Result<(), DeclarationError> {
    if !m.id.contains('.') || m.id.trim().is_empty() {
        return Err(DeclarationError::InvalidContract(format!(
            "definition id '{}' must have format 'package.definition'",
            m.id
        )));
    }
    match m.category {
        DefinitionCategory::Mock => {
            if m.target_connector.as_deref().unwrap_or("").is_empty() {
                return Err(DeclarationError::InvalidContract(
                    "mock definition requires 'for = \"<connector_id>\"'".into(),
                ));
            }
        }
        DefinitionCategory::DataProvider => {
            for (input_name, spec) in &m.inputs {
                if matches!(spec, InputRefSpec::Action { .. }) {
                    return Err(DeclarationError::InvalidContract(format!(
                        "forbidden binding (§2.1): data_provider '{}' cannot bind action input '{input_name}'",
                        m.id
                    )));
                }
            }
        }
        DefinitionCategory::Adapter => {
            for (input_name, spec) in &m.inputs {
                if matches!(spec, InputRefSpec::Connector { .. }) {
                    return Err(DeclarationError::InvalidContract(format!(
                        "forbidden binding (§2.1): adapter '{}' cannot bind connector input '{input_name}'",
                        m.id
                    )));
                }
            }
        }
        DefinitionCategory::WidgetProvider => {
            if m.widget_asset.as_deref().unwrap_or("").is_empty() {
                return Err(DeclarationError::InvalidContract(format!(
                    "widget_provider '{}' requires 'asset' HTML entry path",
                    m.id
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_string_vec(tbl: &Table, key: &str) -> Result<Vec<String>, DeclarationError> {
    match tbl.get::<Value>(key)? {
        Value::Nil => Ok(Vec::new()),
        Value::Table(t) => {
            let mut out = Vec::new();
            for pair in t.pairs::<Value, String>() {
                let (_, v) = pair?;
                out.push(v);
            }
            Ok(out)
        }
        _ => Err(DeclarationError::InvalidContract(format!(
            "field '{key}' must be an array of strings"
        ))),
    }
}

fn parse_string_map(tbl: &Table, key: &str) -> Result<BTreeMap<String, String>, DeclarationError> {
    match tbl.get::<Value>(key)? {
        Value::Nil => Ok(BTreeMap::new()),
        Value::Table(t) => {
            let mut out = BTreeMap::new();
            for pair in t.pairs::<String, String>() {
                let (k, v) = pair?;
                out.insert(k, v);
            }
            Ok(out)
        }
        _ => Err(DeclarationError::InvalidContract(format!(
            "field '{key}' must be a map of strings"
        ))),
    }
}

fn parse_opt_schema(tbl: &Table, key: &str) -> Result<Option<SchemaSpec>, DeclarationError> {
    match tbl.get::<Value>(key)? {
        Value::Nil => Ok(None),
        v => Ok(Some(parse_schema_value(v)?)),
    }
}

fn parse_schema_value(val: Value) -> Result<SchemaSpec, DeclarationError> {
    match val {
        Value::String(s) => Ok(SchemaSpec::Semantic {
            semantic_type: s.to_str()?.to_string(),
            shape: None,
        }),
        Value::Table(t) => {
            let kind: String = t.get("kind").unwrap_or_else(|_| "object".into());
            match kind.as_str() {
                "string" => Ok(SchemaSpec::String),
                "number" => Ok(SchemaSpec::Number),
                "integer" => Ok(SchemaSpec::Integer),
                "boolean" => Ok(SchemaSpec::Boolean),
                "any" => Ok(SchemaSpec::Any),
                "enum" => {
                    let values = parse_string_vec(&t, "values")?;
                    Ok(SchemaSpec::Enum { values })
                }
                "list" => {
                    let item = parse_schema_value(t.get::<Value>("item")?)?;
                    Ok(SchemaSpec::List {
                        item: Box::new(item),
                    })
                }
                "optional" => {
                    let inner = parse_schema_value(t.get::<Value>("inner")?)?;
                    Ok(SchemaSpec::Optional {
                        inner: Box::new(inner),
                    })
                }
                "map" => {
                    let value = parse_schema_value(t.get::<Value>("value")?)?;
                    Ok(SchemaSpec::Map {
                        value: Box::new(value),
                    })
                }
                "semantic" => {
                    let semantic_type: String = t.get("semantic_type")?;
                    let shape = match t.get::<Value>("shape")? {
                        Value::Nil => None,
                        s => Some(Box::new(parse_schema_value(s)?)),
                    };
                    Ok(SchemaSpec::Semantic {
                        semantic_type,
                        shape,
                    })
                }
                "object" => {
                    let fields_val: Value = t.get("fields")?;
                    let fields_tbl = match fields_val {
                        Value::Table(ft) => ft,
                        _ => t.clone(),
                    };
                    let mut fields = BTreeMap::new();
                    for pair in fields_tbl.pairs::<String, Value>() {
                        let (k, v) = pair?;
                        if k == "kind" {
                            continue;
                        }
                        fields.insert(k, parse_schema_value(v)?);
                    }
                    Ok(SchemaSpec::Object { fields })
                }
                other => Err(DeclarationError::InvalidContract(format!(
                    "unknown schema kind '{other}'"
                ))),
            }
        }
        _ => Err(DeclarationError::InvalidContract(
            "expected schema table or semantic string".into(),
        )),
    }
}

fn parse_inputs(tbl: &Table) -> Result<BTreeMap<String, InputRefSpec>, DeclarationError> {
    let mut out = BTreeMap::new();
    let Value::Table(inputs) = tbl.get::<Value>("inputs")? else {
        return Ok(out);
    };
    for pair in inputs.pairs::<String, Table>() {
        let (name, spec_tbl) = pair?;
        let ref_kind: String = spec_tbl.get("ref_kind").map_err(|_| {
            DeclarationError::InvalidContract(format!(
                "input '{name}' must use dash.connector_ref / dash.data_ref / dash.action_ref"
            ))
        })?;
        let definition: Option<String> = spec_tbl.get("definition").ok();
        let spec = match ref_kind.as_str() {
            "connector" => InputRefSpec::Connector {
                definition,
                required_capabilities: parse_string_vec(&spec_tbl, "required_capabilities")?,
            },
            "data" => InputRefSpec::Data {
                definition,
                expected_output: parse_opt_schema(&spec_tbl, "expected_output")?,
            },
            "action" => InputRefSpec::Action { definition },
            other => {
                return Err(DeclarationError::InvalidContract(format!(
                    "unknown input ref_kind '{other}'"
                )))
            }
        };
        out.insert(name, spec);
    }
    Ok(out)
}

fn parse_operations(
    tbl: &Table,
) -> Result<BTreeMap<String, ConnectorOperationSpec>, DeclarationError> {
    let mut out = BTreeMap::new();
    let Value::Table(ops) = tbl.get::<Value>("operations")? else {
        return Ok(out);
    };
    for pair in ops.pairs::<String, Table>() {
        let (op_name, op_tbl) = pair?;
        let description: String = op_tbl.get("description").unwrap_or_default();
        let required_capabilities = parse_string_vec(&op_tbl, "required_capabilities")?;
        let args = parse_opt_schema(&op_tbl, "args")?.unwrap_or(SchemaSpec::Any);
        let result = parse_opt_schema(&op_tbl, "result")?.unwrap_or(SchemaSpec::Any);
        out.insert(
            op_name,
            ConnectorOperationSpec {
                description,
                required_capabilities,
                args,
                result,
            },
        );
    }
    Ok(out)
}

fn parse_emits(tbl: &Table) -> Result<Vec<EventDeclSpec>, DeclarationError> {
    let mut out = Vec::new();
    let Value::Table(emits) = tbl.get::<Value>("emits")? else {
        return Ok(out);
    };
    for pair in emits.pairs::<Value, Value>() {
        let (_, val) = pair?;
        match val {
            Value::String(s) => out.push(EventDeclSpec {
                semantic_type: s.to_str()?.to_string(),
                schema_version: "1".into(),
                durable: true,
                payload: None,
            }),
            Value::Table(t) => out.push(EventDeclSpec {
                semantic_type: t.get("semantic_type")?,
                schema_version: t.get("schema_version").unwrap_or_else(|_| "1".into()),
                durable: t.get("durable").unwrap_or(true),
                payload: parse_opt_schema(&t, "payload")?,
            }),
            _ => {
                return Err(DeclarationError::InvalidContract(
                    "emits entries must be event type string or table".into(),
                ))
            }
        }
    }
    Ok(out)
}

fn parse_updates(tbl: &Table) -> Result<UpdateModes, DeclarationError> {
    let Value::Table(u) = tbl.get::<Value>("updates")? else {
        return Ok(UpdateModes::default());
    };
    Ok(UpdateModes {
        on_demand: u.get("on_demand").unwrap_or(true),
        subscribe: u.get("subscribe").unwrap_or(false),
        poll_interval_secs: u.get("poll_interval_secs").ok(),
        push: u.get("push").unwrap_or(false),
    })
}

fn parse_triggers(tbl: &Table) -> Result<Vec<TriggerDeclSpec>, DeclarationError> {
    let mut out = Vec::new();
    let Value::Table(trigs) = tbl.get::<Value>("triggers")? else {
        return Ok(out);
    };
    for pair in trigs.pairs::<Value, Table>() {
        let (_, t) = pair?;
        out.push(TriggerDeclSpec {
            kind: t.get("kind")?,
            schedule: t.get("schedule").ok(),
            event_type: t.get("event_type").ok(),
            source_input: t.get("source_input").ok(),
            catch_up: t.get("catch_up").ok(),
        });
    }
    Ok(out)
}
