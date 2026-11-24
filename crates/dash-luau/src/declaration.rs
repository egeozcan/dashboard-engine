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
///
/// Determinism (§4.3) is enforced twice: APIs that expose heap addresses, GC state or randomness
/// raise `NON_DETERMINISTIC`, and as a backstop the declaration is evaluated in two fresh VMs whose
/// canonical manifests must be byte-identical.
pub fn evaluate_declaration(
    definition_source: &str,
    resolved_sources: &BTreeMap<String, Vec<u8>>,
) -> Result<DefinitionManifest, DeclarationError> {
    let first = evaluate_declaration_once(definition_source)?;
    let second = evaluate_declaration_once(definition_source)?;
    let a = first
        .canonical_json()
        .map_err(|e| DeclarationError::InvalidContract(format!("manifest serialization: {e}")))?;
    let b = second
        .canonical_json()
        .map_err(|e| DeclarationError::InvalidContract(format!("manifest serialization: {e}")))?;
    if a != b {
        return Err(DeclarationError::NonDeterministic(format!(
            "declaration of '{}' produced different manifests on repeated evaluation",
            first.id
        )));
    }
    let mut manifest = first;
    manifest.identity_version = compute_sources_digest(resolved_sources);
    manifest.generate_luau_types();
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn forbid_fn(lua: &Lua, what: &'static str) -> mlua::Result<Function> {
    lua.create_function(move |_, _: mlua::MultiValue| -> mlua::Result<()> {
        Err(mlua::Error::runtime(format!(
            "NON_DETERMINISTIC: {what} is forbidden during declaration phase"
        )))
    })
}

/// True when converting `v` to a string would expose a heap address (non-deterministic).
fn is_address_valued(v: &Value) -> bool {
    match v {
        Value::Table(t) => match t.metatable() {
            Some(mt) => !matches!(mt.raw_get::<Value>("__tostring"), Ok(Value::Function(_))),
            None => true,
        },
        Value::Function(_) | Value::Thread(_) | Value::UserData(_) | Value::LightUserData(_) => {
            true
        }
        _ => false,
    }
}

fn evaluate_declaration_once(
    definition_source: &str,
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

        // GC introspection exposes allocator state.
        let globals = lua.globals();
        globals.set("collectgarbage", forbid_fn(&lua, "collectgarbage")?)?;
        globals.set("gcinfo", forbid_fn(&lua, "gcinfo")?)?;

        // tostring()/string.format("%s") of tables, functions, threads and userdata expose heap
        // addresses; reject them instead of letting an address leak into the manifest.
        let orig_tostring: Function = globals.get("tostring")?;
        globals.set(
            "tostring",
            lua.create_function(move |_, v: Value| -> mlua::Result<Value> {
                if is_address_valued(&v) {
                    return Err(mlua::Error::runtime(format!(
                        "NON_DETERMINISTIC: tostring() of a {} exposes a heap address",
                        v.type_name()
                    )));
                }
                orig_tostring.call::<Value>(v)
            })?,
        )?;
        let string_tbl: Table = globals.get("string")?;
        let orig_format: Function = string_tbl.get("format")?;
        string_tbl.set(
            "format",
            lua.create_function(move |_, args: mlua::MultiValue| -> mlua::Result<Value> {
                if args.iter().skip(1).any(is_address_valued) {
                    return Err(mlua::Error::runtime(
                        "NON_DETERMINISTIC: string.format of a table/function/thread/userdata exposes a heap address",
                    ));
                }
                orig_format.call::<Value>(args)
            })?,
        )?;
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

    parse_declaration_table(&tbl)
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
                reject_unknown_ref_options(&o, "dash.connector_ref", &["required_capabilities"])?;
                t.set(
                    "required_capabilities",
                    o.get::<Value>("required_capabilities")?,
                )?;
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
                reject_unknown_ref_options(&o, "dash.data_ref", &["expected_output"])?;
                t.set("expected_output", o.get::<Value>("expected_output")?)?;
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

    check_known_keys(tbl, allowed_declaration_keys(category), "declaration")?;

    let id: String = get_opt_string(tbl, "id")?
        .ok_or_else(|| DeclarationError::InvalidContract("missing required field 'id'".into()))?;

    let description: String = get_opt_string(tbl, "description")?.unwrap_or_default();
    let runtime: String = get_opt_string(tbl, "runtime")?.unwrap_or_else(|| "runtime.luau".into());
    let handler: String = get_opt_string(tbl, "handler")?.unwrap_or_else(|| match category {
        DefinitionCategory::DataProvider => "query".into(),
        DefinitionCategory::ActionProvider => "execute".into(),
        DefinitionCategory::Adapter => "render".into(),
        DefinitionCategory::Connector | DefinitionCategory::Mock => "call".into(),
        DefinitionCategory::Automation => "run".into(),
        DefinitionCategory::Library | DefinitionCategory::WidgetProvider => String::new(),
    });
    let dry_run_handler: Option<String> = get_opt_string(tbl, "dry_run")?;
    let reconcile_handler: Option<String> = get_opt_string(tbl, "reconcile")?;
    let request_handler: Option<String> = get_opt_string(tbl, "on_request")?;
    let target_connector: Option<String> = get_opt_string(tbl, "for")?;

    let fixtures = parse_string_map(tbl, "fixtures")?;
    let capabilities = parse_string_vec(tbl, "capabilities")?;
    let required_capabilities = parse_string_vec(tbl, "required_capabilities")?;
    let optional_capabilities = parse_string_vec(tbl, "optional_capabilities")?;
    let credentials = parse_string_vec(tbl, "credentials")?;
    let invalidates = parse_string_vec(tbl, "invalidates")?;
    let dashboard_state_reads = parse_string_vec(tbl, "dashboard_state_reads")?;
    let mut accepts_requests = parse_string_vec(tbl, "accepts_requests")?;
    let request_schemas = parse_request_schemas(tbl)?;
    for name in request_schemas.keys() {
        if !accepts_requests.iter().any(|r| r == name) {
            accepts_requests.push(name.clone());
        }
    }
    let libraries = parse_string_vec(tbl, "libraries")?;
    let widget_asset: Option<String> = get_opt_string(tbl, "asset")?;

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
        request_schemas,
        widget_props,
        widget_asset,
        updates,
        triggers,
        libraries,
        generated_luau_types: String::new(),
    })
}

/// `^[a-z][a-z0-9_]{0,63}$` — the shape of a Package name or a Definition name.
pub fn is_valid_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    s.len() <= 64 && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// A Definition ID is exactly `package.definition`, both parts valid identifiers (§4.1).
pub fn is_valid_definition_id(s: &str) -> bool {
    match s.split_once('.') {
        Some((pkg, def)) => is_valid_identifier(pkg) && is_valid_identifier(def),
        None => false,
    }
}

/// A semantic type name is a dotted lowercase name with a version suffix, e.g. `dev.pull_request@1`.
pub fn is_valid_semantic_type_name(s: &str) -> bool {
    let Some((name, ver)) = s.rsplit_once('@') else {
        return false;
    };
    if ver.is_empty() || !ver.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }
    if name.is_empty() || name.len() > 128 {
        return false;
    }
    name.split('.').all(|seg| {
        let mut cs = seg.chars();
        matches!(cs.next(), Some(c) if c.is_ascii_lowercase())
            && cs.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    })
}

/// A path declared inside a Definition (runtime, fixture, asset) must be a plain relative path
/// that stays inside the Definition directory: no absolute paths, no `..`, no backslashes.
pub fn is_safe_relative_path(p: &str) -> bool {
    if p.is_empty() || p.len() > 512 || p.starts_with('/') || p.contains('\\') || p.contains('\0') {
        return false;
    }
    // Reject Windows drive prefixes like `C:`.
    if p.len() >= 2 && p.as_bytes()[1] == b':' {
        return false;
    }
    p.split('/')
        .all(|seg| !seg.is_empty() && seg != "." && seg != "..")
}

fn validate_manifest(m: &DefinitionManifest) -> Result<(), DeclarationError> {
    if !is_valid_definition_id(&m.id) {
        return Err(DeclarationError::InvalidContract(format!(
            "definition id '{}' must have format 'package.definition' (each part matching [a-z][a-z0-9_]*)",
            m.id
        )));
    }
    if !m.runtime.is_empty() && !is_safe_relative_path(&m.runtime) {
        return Err(DeclarationError::InvalidContract(format!(
            "definition '{}': runtime path '{}' must be relative and stay inside the Definition directory",
            m.id, m.runtime
        )));
    }
    for (name, path) in &m.fixtures {
        if !is_safe_relative_path(path) {
            return Err(DeclarationError::InvalidContract(format!(
                "definition '{}': fixture '{name}' path '{path}' must be relative and stay inside the Definition directory",
                m.id
            )));
        }
    }
    if let Some(asset) = &m.widget_asset {
        if !asset.is_empty() && !is_safe_relative_path(asset) {
            return Err(DeclarationError::InvalidContract(format!(
                "definition '{}': asset path '{asset}' must be relative and stay inside the Definition directory",
                m.id
            )));
        }
    }
    for lib in &m.libraries {
        if !is_valid_definition_id(lib) {
            return Err(DeclarationError::InvalidContract(format!(
                "definition '{}': library '{lib}' is not a valid Definition ID",
                m.id
            )));
        }
    }
    for (input_name, spec) in &m.inputs {
        let def = match spec {
            InputRefSpec::Connector { definition, .. }
            | InputRefSpec::Data { definition, .. }
            | InputRefSpec::Action { definition } => definition,
        };
        if let Some(d) = def {
            if !is_valid_definition_id(d) {
                return Err(DeclarationError::InvalidContract(format!(
                    "definition '{}': input '{input_name}' references invalid Definition ID '{d}'",
                    m.id
                )));
            }
        }
    }
    for t in &m.triggers {
        if !matches!(t.kind.as_str(), "cron" | "event" | "signal") {
            return Err(DeclarationError::InvalidContract(format!(
                "definition '{}': unknown trigger kind '{}' (expected cron | event | signal)",
                m.id, t.kind
            )));
        }
    }
    // Every operation's required capabilities must come from the declared capability vocabulary.
    for (op_name, op) in &m.operations {
        for cap in &op.required_capabilities {
            if !m.capabilities.iter().any(|c| c == cap) {
                return Err(DeclarationError::InvalidContract(format!(
                    "definition '{}': operation '{op_name}' requires capability '{cap}' that is not in the declared capabilities",
                    m.id
                )));
            }
        }
    }
    match m.category {
        DefinitionCategory::Mock => match m.target_connector.as_deref() {
            Some(t) if is_valid_definition_id(t) => {}
            Some(t) if !t.is_empty() => {
                return Err(DeclarationError::InvalidContract(format!(
                    "mock '{}': target '{t}' is not a valid connector Definition ID",
                    m.id
                )))
            }
            _ => {
                return Err(DeclarationError::InvalidContract(
                    "mock definition requires 'for = \"<connector_id>\"'".into(),
                ))
            }
        },
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
        DefinitionCategory::Automation => {
            for (idx, t) in m.triggers.iter().enumerate() {
                validate_trigger_shape(m, idx, t)?;
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

/// Structural trigger checks (§7.3); cron expression syntax is checked at graph validation.
fn validate_trigger_shape(
    m: &DefinitionManifest,
    idx: usize,
    t: &TriggerDeclSpec,
) -> Result<(), DeclarationError> {
    let bad = |msg: String| {
        Err(DeclarationError::InvalidContract(format!(
            "automation '{}' trigger #{idx}: {msg}",
            m.id
        )))
    };
    let missing = |v: &Option<String>| v.as_deref().map(str::trim).unwrap_or("").is_empty();
    match t.kind.as_str() {
        "event" if missing(&t.event_type) => {
            return bad("event trigger requires 'event_type'".into())
        }
        "cron" if missing(&t.schedule) => return bad("cron trigger requires 'schedule'".into()),
        "signal" if missing(&t.source_input) && missing(&t.state_key) => {
            return bad("signal trigger requires 'source_input' or 'state_key'".into())
        }
        "event" | "cron" | "signal" => {}
        other => {
            return bad(format!(
                "unknown trigger kind '{other}' (expected event | cron | signal)"
            ))
        }
    }
    if let Some(cu) = t.catch_up.as_deref() {
        if cu != "skip" && cu != "catch_up" {
            return bad(format!(
                "unknown catch_up policy '{cu}' (expected skip | catch_up)"
            ));
        }
    }
    if let Some(slot) = t.source_input.as_deref() {
        if !m.inputs.contains_key(slot) {
            return bad(format!("source_input '{slot}' is not a declared input"));
        }
    }
    if let Some(n) = t.max_attempts {
        if !(1..=10).contains(&n) {
            return bad(format!("max_attempts must be between 1 and 10, got {n}"));
        }
    }
    Ok(())
}

/// Top-level declaration keys accepted per category. Unknown keys (typically typos such as
/// `ouput`) are rejected rather than silently dropping part of the contract (§4.3, §17 #2).
fn allowed_declaration_keys(category: DefinitionCategory) -> &'static [&'static str] {
    use DefinitionCategory as C;
    match category {
        C::Connector => &[
            "id",
            "description",
            "runtime",
            "handler",
            "libraries",
            "config",
            "credentials",
            "capabilities",
            "operations",
            "emits",
        ],
        C::Mock => &[
            "id",
            "description",
            "runtime",
            "handler",
            "libraries",
            "config",
            "for",
            "fixtures",
            "credentials",
            "capabilities",
            "operations",
            "emits",
        ],
        C::DataProvider => &[
            "id",
            "description",
            "runtime",
            "handler",
            "libraries",
            "config",
            "inputs",
            "query",
            "output",
            "updates",
            "emits",
            "required_capabilities",
            "optional_capabilities",
        ],
        C::ActionProvider => &[
            "id",
            "description",
            "runtime",
            "handler",
            "libraries",
            "config",
            "inputs",
            "args",
            "result",
            "dry_run",
            "reconcile",
            "emits",
            "invalidates",
            "required_capabilities",
            "optional_capabilities",
        ],
        C::Adapter => &[
            "id",
            "description",
            "runtime",
            "handler",
            "libraries",
            "config",
            "inputs",
            "output",
            "dashboard_state_reads",
            "accepts_requests",
            "requests",
            "on_request",
        ],
        C::WidgetProvider => &[
            "id",
            "description",
            "asset",
            "props",
            "accepts_requests",
            "requests",
        ],
        C::Library => &["id", "description", "runtime", "libraries"],
        C::Automation => &[
            "id",
            "description",
            "runtime",
            "handler",
            "libraries",
            "config",
            "inputs",
            "triggers",
            "emits",
            "required_capabilities",
            "optional_capabilities",
        ],
    }
}

/// Options accepted by a `dash.*_ref(definition, opts)` constructor: anything else (a typo such
/// as `required_capabilites`) is an error rather than a silently dropped contract.
fn reject_unknown_ref_options(opts: &Table, ctor: &str, allowed: &[&str]) -> mlua::Result<()> {
    for pair in opts.pairs::<Value, Value>() {
        let (k, _) = pair?;
        let key = match &k {
            Value::String(s) => s.to_str()?.to_string(),
            other => {
                return Err(mlua::Error::runtime(format!(
                    "{ctor}: option keys must be strings, got {}",
                    other.type_name()
                )))
            }
        };
        if !allowed.contains(&key.as_str()) {
            return Err(mlua::Error::runtime(format!(
                "{ctor}: unknown option '{key}' (allowed: {})",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

fn check_known_keys(tbl: &Table, allowed: &[&str], ctx: &str) -> Result<(), DeclarationError> {
    for pair in tbl.pairs::<Value, Value>() {
        let (k, _) = pair?;
        let key = match &k {
            Value::String(s) => s.to_str()?.to_string(),
            other => {
                return Err(DeclarationError::InvalidContract(format!(
                    "{ctx}: keys must be strings, got {}",
                    other.type_name()
                )))
            }
        };
        if key == "__dash_category" {
            continue;
        }
        if !allowed.contains(&key.as_str()) {
            return Err(DeclarationError::InvalidContract(format!(
                "{ctx}: unknown field '{key}' (allowed: {})",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

/// Typed optional getter: absent -> None; present with the wrong type -> InvalidContract.
fn get_opt_string(tbl: &Table, key: &str) -> Result<Option<String>, DeclarationError> {
    match tbl.get::<Value>(key)? {
        Value::Nil => Ok(None),
        Value::String(s) => Ok(Some(s.to_str()?.to_string())),
        other => Err(DeclarationError::InvalidContract(format!(
            "field '{key}' must be a string, got {}",
            other.type_name()
        ))),
    }
}

fn get_opt_bool(tbl: &Table, key: &str) -> Result<Option<bool>, DeclarationError> {
    match tbl.get::<Value>(key)? {
        Value::Nil => Ok(None),
        Value::Boolean(b) => Ok(Some(b)),
        other => Err(DeclarationError::InvalidContract(format!(
            "field '{key}' must be a boolean, got {}",
            other.type_name()
        ))),
    }
}

fn get_opt_u64(tbl: &Table, key: &str) -> Result<Option<u64>, DeclarationError> {
    match tbl.get::<Value>(key)? {
        Value::Nil => Ok(None),
        Value::Integer(i) if i >= 0 => Ok(Some(i as u64)),
        Value::Number(n) if n >= 0.0 && n.fract() == 0.0 && n <= u64::MAX as f64 => {
            Ok(Some(n as u64))
        }
        other => Err(DeclarationError::InvalidContract(format!(
            "field '{key}' must be a non-negative integer, got {}",
            other.type_name()
        ))),
    }
}

fn parse_string_vec(tbl: &Table, key: &str) -> Result<Vec<String>, DeclarationError> {
    let bad =
        || DeclarationError::InvalidContract(format!("field '{key}' must be an array of strings"));
    match tbl.get::<Value>(key)? {
        Value::Nil => Ok(Vec::new()),
        Value::Table(t) => {
            let mut out = Vec::new();
            for v in t.clone().sequence_values::<Value>() {
                match v? {
                    Value::String(s) => out.push(s.to_str()?.to_string()),
                    _ => return Err(bad()),
                }
            }
            // A map-shaped table (non-sequence keys) is not an array.
            if t.pairs::<Value, Value>().count() != out.len() {
                return Err(bad());
            }
            Ok(out)
        }
        _ => Err(bad()),
    }
}

fn parse_string_map(tbl: &Table, key: &str) -> Result<BTreeMap<String, String>, DeclarationError> {
    let bad =
        || DeclarationError::InvalidContract(format!("field '{key}' must be a map of strings"));
    match tbl.get::<Value>(key)? {
        Value::Nil => Ok(BTreeMap::new()),
        Value::Table(t) => {
            let mut out = BTreeMap::new();
            for pair in t.pairs::<Value, Value>() {
                match pair? {
                    (Value::String(k), Value::String(v)) => {
                        out.insert(k.to_str()?.to_string(), v.to_str()?.to_string());
                    }
                    _ => return Err(bad()),
                }
            }
            Ok(out)
        }
        _ => Err(bad()),
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
            semantic_type: checked_semantic_name(s.to_str()?.to_string())?,
            shape: None,
        }),
        Value::Table(t) => {
            let kind: String = get_opt_string(&t, "kind")?.unwrap_or_else(|| "object".into());
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
                    let semantic_type = checked_semantic_name(
                        get_opt_string(&t, "semantic_type")?.ok_or_else(|| {
                            DeclarationError::InvalidContract(
                                "semantic schema requires 'semantic_type'".into(),
                            )
                        })?,
                    )?;
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

fn checked_semantic_name(name: String) -> Result<String, DeclarationError> {
    if is_valid_semantic_type_name(&name) {
        Ok(name)
    } else {
        Err(DeclarationError::InvalidContract(format!(
            "semantic type '{name}' must be a versioned name like 'dev.pull_request@1'"
        )))
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
        // Hand-built ref tables get the same allowlist as the `dash.*_ref` constructors, so a
        // misspelled option can never silently drop a capability or output contract.
        let ctx = format!("input '{name}'");
        match ref_kind.as_str() {
            "connector" => check_known_keys(
                &spec_tbl,
                &["ref_kind", "definition", "required_capabilities"],
                &ctx,
            )?,
            "data" => check_known_keys(
                &spec_tbl,
                &["ref_kind", "definition", "expected_output"],
                &ctx,
            )?,
            "action" => check_known_keys(&spec_tbl, &["ref_kind", "definition"], &ctx)?,
            _ => {}
        }
        let definition: Option<String> = get_opt_string(&spec_tbl, "definition")?;
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
        check_known_keys(
            &op_tbl,
            &[
                "description",
                "required_capabilities",
                "args",
                "result",
                "mode",
                "read_only",
            ],
            &format!("operation '{op_name}'"),
        )?;
        let description: String = get_opt_string(&op_tbl, "description")?.unwrap_or_default();
        let required_capabilities = parse_string_vec(&op_tbl, "required_capabilities")?;
        let args = parse_opt_schema(&op_tbl, "args")?.unwrap_or(SchemaSpec::Any);
        let result = parse_opt_schema(&op_tbl, "result")?.unwrap_or(SchemaSpec::Any);
        let mode = match op_tbl.get::<Value>("mode")? {
            Value::Nil => None,
            Value::String(s) => {
                let m = s.to_str()?.to_string();
                if m != "read" && m != "write" {
                    return Err(DeclarationError::InvalidContract(format!(
                        "operation '{op_name}' mode must be \"read\" or \"write\", got \"{m}\""
                    )));
                }
                Some(m)
            }
            _ => {
                return Err(DeclarationError::InvalidContract(format!(
                    "operation '{op_name}' mode must be a string"
                )))
            }
        };
        let declared_read_only = match op_tbl.get::<Value>("read_only")? {
            Value::Nil => None,
            Value::Boolean(b) => Some(b),
            _ => {
                return Err(DeclarationError::InvalidContract(format!(
                    "operation '{op_name}': read_only must be a boolean"
                )))
            }
        };
        // `read_only = true` and `mode = "read"` are two spellings of one classification.
        let read_only = match (declared_read_only, mode.as_deref()) {
            (Some(true), Some("write")) | (Some(false), Some("read")) => {
                return Err(DeclarationError::InvalidContract(format!(
                    "operation '{op_name}': read_only contradicts mode \"{}\"",
                    mode.as_deref().unwrap_or_default()
                )))
            }
            (Some(b), _) => b,
            (None, m) => m == Some("read"),
        };
        // An explicit `read_only = false` declares a mutating operation (no inference).
        let mode = match (declared_read_only, mode) {
            (Some(false), None) => Some("write".to_string()),
            (Some(true), None) => Some("read".to_string()),
            (_, m) => m,
        };
        out.insert(
            op_name,
            ConnectorOperationSpec {
                description,
                required_capabilities,
                args,
                result,
                mode,
                read_only,
            },
        );
    }
    Ok(out)
}

/// `requests = { merge_requested = dash.object { ... } }` on Adapters and widget providers.
fn parse_request_schemas(tbl: &Table) -> Result<BTreeMap<String, SchemaSpec>, DeclarationError> {
    let mut out = BTreeMap::new();
    let Value::Table(reqs) = tbl.get::<Value>("requests")? else {
        return Ok(out);
    };
    for pair in reqs.pairs::<String, Value>() {
        let (name, schema_val) = pair?;
        out.insert(name, parse_schema_value(schema_val)?);
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
            Value::Table(t) => {
                check_known_keys(
                    &t,
                    &["semantic_type", "schema_version", "durable", "payload"],
                    "emits entry",
                )?;
                out.push(EventDeclSpec {
                    semantic_type: get_opt_string(&t, "semantic_type")?.ok_or_else(|| {
                        DeclarationError::InvalidContract(
                            "emits entry requires 'semantic_type'".into(),
                        )
                    })?,
                    schema_version: get_opt_string(&t, "schema_version")?
                        .unwrap_or_else(|| "1".into()),
                    durable: get_opt_bool(&t, "durable")?.unwrap_or(true),
                    payload: parse_opt_schema(&t, "payload")?,
                })
            }
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
    check_known_keys(
        &u,
        &["on_demand", "subscribe", "poll_interval_secs", "push"],
        "updates",
    )?;
    Ok(UpdateModes {
        on_demand: get_opt_bool(&u, "on_demand")?.unwrap_or(true),
        subscribe: get_opt_bool(&u, "subscribe")?.unwrap_or(false),
        poll_interval_secs: get_opt_u64(&u, "poll_interval_secs")?,
        push: get_opt_bool(&u, "push")?.unwrap_or(false),
    })
}

fn parse_triggers(tbl: &Table) -> Result<Vec<TriggerDeclSpec>, DeclarationError> {
    let mut out = Vec::new();
    let Value::Table(trigs) = tbl.get::<Value>("triggers")? else {
        return Ok(out);
    };
    for pair in trigs.pairs::<Value, Table>() {
        let (_, t) = pair?;
        check_known_keys(
            &t,
            &[
                "kind",
                "schedule",
                "event_type",
                "source_input",
                "catch_up",
                "params",
                "state_key",
                "predicate",
                "max_attempts",
            ],
            "trigger",
        )?;
        out.push(TriggerDeclSpec {
            kind: get_opt_string(&t, "kind")?.ok_or_else(|| {
                DeclarationError::InvalidContract("trigger requires 'kind'".into())
            })?,
            schedule: get_opt_string(&t, "schedule")?,
            event_type: get_opt_string(&t, "event_type")?,
            source_input: get_opt_string(&t, "source_input")?,
            catch_up: get_opt_string(&t, "catch_up")?,
            params: match t.get::<Value>("params")? {
                Value::Nil => None,
                v => Some(serde_json::to_value(&v).map_err(|e| {
                    DeclarationError::InvalidContract(format!("trigger params: {e}"))
                })?),
            },
            state_key: get_opt_string(&t, "state_key")?,
            predicate: get_opt_string(&t, "predicate")?,
            max_attempts: get_opt_u64(&t, "max_attempts")?
                .map(|n| {
                    u32::try_from(n).map_err(|_| {
                        DeclarationError::InvalidContract(format!(
                            "trigger max_attempts {n} is out of range"
                        ))
                    })
                })
                .transpose()?,
        });
    }
    Ok(out)
}

/// Load-time check of a Definition's runtime module: the source must compile as Luau, and every
/// handler the manifest declares (`handler`, `dry_run`, `reconcile`, `on_request`) must be named
/// in it. The module is compiled but never executed here; operational code only ever runs in
/// runtime VMs (§4.3).
pub fn check_runtime_source(
    manifest: &DefinitionManifest,
    runtime_source: &str,
) -> Result<(), DeclarationError> {
    let lua = Lua::new_with(StdLib::NONE, LuaOptions::default())?;
    lua.load(runtime_source)
        .set_name(format!("{}/runtime.luau", manifest.id))
        .into_function()
        .map_err(|e| {
            DeclarationError::InvalidContract(format!(
                "runtime module of '{}' does not compile: {e}",
                manifest.id
            ))
        })?;
    let mut handlers: Vec<(&str, &str)> = Vec::new();
    if !manifest.handler.is_empty() {
        handlers.push(("handler", manifest.handler.as_str()));
    }
    for (field, h) in [
        ("dry_run", &manifest.dry_run_handler),
        ("reconcile", &manifest.reconcile_handler),
        ("on_request", &manifest.request_handler),
    ] {
        if let Some(h) = h {
            handlers.push((field, h.as_str()));
        }
    }
    for (field, h) in handlers {
        if !is_valid_lua_identifier(h) {
            return Err(DeclarationError::InvalidContract(format!(
                "definition '{}': {field} '{h}' is not a valid Luau function name",
                manifest.id
            )));
        }
        if !source_mentions_identifier(runtime_source, h) {
            return Err(DeclarationError::InvalidContract(format!(
                "definition '{}': declared {field} '{h}' is not defined in its runtime module",
                manifest.id
            )));
        }
    }
    Ok(())
}

fn is_valid_lua_identifier(s: &str) -> bool {
    let mut cs = s.chars();
    matches!(cs.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && cs.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Whether `ident` occurs in `src` as a whole identifier token (outside `--` line comments).
fn source_mentions_identifier(src: &str, ident: &str) -> bool {
    src.lines().any(|line| {
        let code = match line.find("--") {
            Some(i) => &line[..i],
            None => line,
        };
        let bytes = code.as_bytes();
        let mut start = 0;
        while let Some(pos) = code[start..].find(ident) {
            let i = start + pos;
            let j = i + ident.len();
            let before_ok =
                i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            let after_ok =
                j >= bytes.len() || !(bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_');
            if before_ok && after_ok {
                return true;
            }
            start = j;
        }
        false
    })
}

/// Statically collect the module names a Luau source passes to `require("...")` / `require "..."`.
pub fn scan_required_modules(src: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = src.as_bytes();
    let mut start = 0;
    while let Some(pos) = src[start..].find("require") {
        let i = start + pos;
        let mut j = i + "require".len();
        start = j;
        if i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_') {
            continue;
        }
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j < bytes.len() && bytes[j] == b'(' {
            j += 1;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
        }
        if j < bytes.len() && (bytes[j] == b'"' || bytes[j] == b'\'') {
            let quote = bytes[j];
            if let Some(end) = src[j + 1..].find(quote as char) {
                out.push(src[j + 1..j + 1 + end].to_string());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(src: &str) -> Result<DefinitionManifest, DeclarationError> {
        evaluate_declaration(src, &BTreeMap::new())
    }

    fn dp(body: &str) -> String {
        format!(
            "local dash = require(\"dash\")\nreturn dash.data_provider {{ id = \"p.d\", {body} }}"
        )
    }

    /// R3:decl-ref-options-typos-dropped: a misspelled ref option must fail the declaration
    /// instead of silently dropping the capability / expected-output contract.
    #[test]
    fn misspelled_ref_options_are_rejected() {
        for (src, needle) in [
            (
                dp(
                    r#"inputs = { gh = dash.connector_ref("g.c", { required_capabilites = { "prs.merge" } }) }"#,
                ),
                "required_capabilites",
            ),
            (
                dp(
                    r#"inputs = { up = dash.data_ref("g.p", { expected_ouput = dash.list("g.pr@1") }) }"#,
                ),
                "expected_ouput",
            ),
            // Hand-built ref tables are held to the same allowlist.
            (
                dp(
                    r#"inputs = { gh = { ref_kind = "connector", definition = "g.c", required_capabilites = { "x" } } }"#,
                ),
                "required_capabilites",
            ),
            (
                dp(
                    r#"inputs = { up = { ref_kind = "data", definition = "g.p", expected_ouput = {} } }"#,
                ),
                "expected_ouput",
            ),
            (
                dp(
                    r#"inputs = { act = { ref_kind = "action", definition = "g.a", required_capabilities = {} } }"#,
                ),
                "required_capabilities",
            ),
        ] {
            let err = eval(&src).expect_err(&src);
            assert!(err.to_string().contains(needle), "{src}: {err}");
        }

        // Correctly spelled options still produce the contracts.
        let m = eval(&dp(r#"inputs = {
                gh = dash.connector_ref("g.c", { required_capabilities = { "prs.merge" } }),
                up = dash.data_ref("g.p", { expected_output = dash.list("g.pr@1") }),
            }"#))
        .unwrap();
        match &m.inputs["gh"] {
            InputRefSpec::Connector {
                required_capabilities,
                ..
            } => assert_eq!(required_capabilities, &vec!["prs.merge".to_string()]),
            other => panic!("{other:?}"),
        }
        match &m.inputs["up"] {
            InputRefSpec::Data {
                expected_output, ..
            } => assert!(expected_output.is_some()),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn address_and_gc_dependent_values_are_non_deterministic() {
        for pre in [
            "local d = tostring({})",
            "local d = tostring(function() end)",
            "local d = string.format(\"%s\", {})",
            "local d = (\"%s\"):format({})",
            "local d = collectgarbage(\"count\")",
            "local d = gcinfo()",
            "local d = math.random()",
        ] {
            let src = format!(
                "local dash = require(\"dash\")\n{pre}\nreturn dash.data_provider {{ id = \"p.d\", description = tostring(d) }}"
            );
            assert!(
                matches!(eval(&src), Err(DeclarationError::NonDeterministic(_))),
                "{pre} must be rejected as non-deterministic"
            );
        }
        // Scalars still stringify deterministically.
        let ok = eval(&dp(
            "description = tostring(42) .. string.format(\"%d\", 7)",
        ))
        .unwrap();
        assert_eq!(ok.description, "427");
    }

    #[test]
    fn unknown_or_mistyped_fields_are_rejected() {
        for body in [
            "ouptut = dash.string()",
            "quer = 5",
            "dry_run = \"x\"",
            "handler = {}",
            "runtime = 42",
            "description = true",
            "updates = { subscribe = \"yes\" }",
            "updates = { poll_interval_secs = -1 }",
            "updates = { polll = true }",
        ] {
            assert!(
                matches!(eval(&dp(body)), Err(DeclarationError::InvalidContract(_))),
                "{body} must be rejected"
            );
        }
        let conn = r#"local dash = require("dash")
return dash.connector { id = "p.c", capabilities = { "a" }, operations = { op = { required_capabilities = { "a" }, descripton = "typo" } } }"#;
        assert!(matches!(
            eval(conn),
            Err(DeclarationError::InvalidContract(_))
        ));
        let ok = eval(&dp(
            "updates = { on_demand = true, poll_interval_secs = 60 }",
        ))
        .unwrap();
        assert_eq!(ok.updates.poll_interval_secs, Some(60));
    }

    #[test]
    fn ids_paths_and_semantic_names_are_validated() {
        for id in [".", "a.", ".b", "x.y.z", "A.b", "a-b.c", "nodot"] {
            let src =
                format!("local dash = require(\"dash\")\nreturn dash.library {{ id = \"{id}\" }}");
            assert!(eval(&src).is_err(), "id {id:?} must be rejected");
        }
        for body in [
            "runtime = \"/etc/passwd\"",
            "runtime = \"../../x.luau\"",
            "runtime = \"a\\\\b.luau\"",
            "output = dash.list(\"whatever\")",
            "output = dash.semantic(\"Dev.PR@x\")",
            "libraries = { \"../evil\" }",
        ] {
            assert!(eval(&dp(body)).is_err(), "{body} must be rejected");
        }
        let mock_fixture = r#"local dash = require("dash")
return dash.mock { id = "p.m", ["for"] = "p.c", fixtures = { x = "../secret.json" } }"#;
        assert!(eval(mock_fixture).is_err());
        assert!(eval(&dp("output = dash.list(\"dev.pull_request@1\")")).is_ok());
    }

    #[test]
    fn operation_capabilities_must_be_declared() {
        let bad = r#"local dash = require("dash")
return dash.connector { id = "p.c", capabilities = { "read" }, operations = { merge = { required_capabilities = { "merge" } } } }"#;
        assert!(matches!(
            eval(bad),
            Err(DeclarationError::InvalidContract(_))
        ));
    }

    #[test]
    fn runtime_source_check_requires_compiling_module_with_handlers() {
        let m = eval(
            "local dash = require(\"dash\")\nreturn dash.action_provider { id = \"p.a\", dry_run = \"preview\" }",
        )
        .unwrap();
        assert!(check_runtime_source(
            &m,
            "local R = {}; function R.execute() end; function R.preview() end; return R"
        )
        .is_ok());
        assert!(
            check_runtime_source(&m, "local R = {}; function R.execute() end; return R").is_err()
        );
        assert!(check_runtime_source(&m, "-- execute preview\nreturn {}").is_err());
        assert!(check_runtime_source(&m, "local R = {; function R.execute() end").is_err());
    }

    #[test]
    fn require_scanner_finds_module_names() {
        let src = "local a = require(\"eng.utils\")\nlocal b = require 'eng.more'\nlocal c = myrequire(\"no\")\nlocal d = require( \"x.y\" )";
        assert_eq!(
            scan_required_modules(src),
            vec!["eng.utils", "eng.more", "x.y"]
        );
    }
}
