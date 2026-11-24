//! Engine-level checks for the Luau runtime workstream: `unknown` after possible side effects,
//! contract-checked `ctx:emit` with declared versions, per-Instance budgets from `[resources]`,
//! VM trace spans, and library declaration validation.

use dash_core::model::WorkspaceResources;
use dash_core::{
    validate_configured_graph, ConfiguredGraph, InstanceConfig, LoadedDefinition, Principal,
    RuntimeEngine,
};
use dash_luau::{evaluate_declaration, DefinitionCategory, ResourceBudget, ResourceOverrides};
use dash_store::SqliteStore;
use serde_json::json;
use std::collections::BTreeMap;

fn load_def(pkg: &str, decl: &str, runtime: &str) -> LoadedDefinition {
    let mut sources = BTreeMap::new();
    sources.insert("definition.luau".into(), decl.as_bytes().to_vec());
    sources.insert("runtime.luau".into(), runtime.as_bytes().to_vec());
    LoadedDefinition {
        package_name: pkg.into(),
        manifest: evaluate_declaration(decl, &sources).unwrap(),
        definition_source: decl.into(),
        runtime_source: runtime.into(),
        tests_source: None,
        fixtures: BTreeMap::new(),
        widget_assets: BTreeMap::new(),
    }
}

fn inst(
    id: &str,
    category: DefinitionCategory,
    definition: &str,
    inputs: &[(&str, &str)],
    capabilities: &[&str],
) -> InstanceConfig {
    InstanceConfig {
        id: id.into(),
        category,
        definition: definition.into(),
        inputs: inputs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        config: json!({}),
        secrets: BTreeMap::new(),
        capabilities: capabilities.iter().map(|s| s.to_string()).collect(),
        run_as: None,
        enabled: None,
    }
}

const CONNECTOR_DECL: &str = r#"
local dash = require("dash")
return dash.connector {
    id = "t.conn",
    capabilities = { "prs.read", "prs.merge" },
    operations = {
        get_pr = { required_capabilities = { "prs.read" } },
        merge_pr = { required_capabilities = { "prs.merge" } },
    },
}
"#;
const CONNECTOR_RT: &str = r#"
local R = {}
function R.call(ctx)
    if ctx.operation == "get_pr" then return { number = ctx.args.number, state = "open" } end
    if ctx.operation == "merge_pr" then return { merged = true } end
    error("unknown op")
end
return R
"#;

fn action_def(id: &str, body: &str) -> LoadedDefinition {
    let decl = format!(
        r#"
local dash = require("dash")
return dash.action_provider {{
    id = "{id}",
    handler = "execute",
    inputs = {{ gh = dash.connector_ref("t.conn", {{ required_capabilities = {{ "prs.read", "prs.merge" }} }}) }},
    emits = {{ {{ semantic_type = "t.merged", schema_version = "2", durable = true }} }},
}}
"#
    );
    let rt = format!("local R = {{}}\nfunction R.execute(ctx)\n{body}\nend\nreturn R");
    load_def("t", &decl, &rt)
}

fn graph() -> ConfiguredGraph {
    let mut g = ConfiguredGraph {
        generation_id: 1,
        ..Default::default()
    };
    g.definitions
        .insert("t.conn".into(), load_def("t", CONNECTOR_DECL, CONNECTOR_RT));
    g.instances.insert(
        "conn".into(),
        inst(
            "conn",
            DefinitionCategory::Connector,
            "t.conn",
            &[],
            &["prs.read", "prs.merge"],
        ),
    );
    let actions = [
        (
            "t.after_merge",
            r#"    ctx:await(ctx.inputs.gh:call("merge_pr", { number = 1 }))
    error("crashed while building the result")"#,
        ),
        (
            "t.after_read",
            r#"    ctx:await(ctx.inputs.gh:call("get_pr", { number = 1 }))
    error("PR is not mergeable")"#,
        ),
        ("t.before_calls", r#"    error("bad input")"#),
        (
            "t.emit_ok",
            r#"    ctx:await(ctx:emit("t.merged", { number = 1 }))
    return { ok = true }"#,
        ),
        (
            "t.emit_spoof",
            r#"    ctx:await(ctx:emit("github.pr_merged", { number = 1 }))
    return { ok = true }"#,
        ),
        (
            "t.slow_after_merge",
            r#"    ctx:await(ctx.inputs.gh:call("merge_pr", { number = 1 }))
    while true do end"#,
        ),
    ];
    for (def_id, body) in actions {
        g.definitions
            .insert(def_id.into(), action_def(def_id, body));
        let inst_id = def_id.trim_start_matches("t.");
        g.instances.insert(
            inst_id.into(),
            inst(
                inst_id,
                DefinitionCategory::ActionProvider,
                def_id,
                &[("gh", "conn")],
                &[],
            ),
        );
    }
    g
}

#[tokio::test]
async fn failures_after_mutating_calls_are_unknown_not_failed() {
    let mut g = graph();
    // Per-Instance budget from `[resources.instances.<id>]`: keep the looping action short.
    g.workspace.resources.instances.insert(
        "slow_after_merge".into(),
        ResourceOverrides {
            timeout_ms: Some(150),
            ..Default::default()
        },
    );
    let engine = RuntimeEngine::new(
        g,
        SqliteStore::open_in_memory().unwrap(),
        ResourceBudget::default(),
    )
    .unwrap();
    let admin = Principal::administrator();
    assert_eq!(
        engine
            .pool
            .effective_budget("slow_after_merge")
            .wall_timeout,
        std::time::Duration::from_millis(150)
    );

    let rec = engine
        .actions
        .invoke("after_merge", json!({}), &admin, None, None)
        .await
        .unwrap();
    assert_eq!(rec.status, "unknown", "{rec:?}");
    assert!(rec.reason.as_deref().unwrap().contains("UNKNOWN_OUTCOME"));

    let rec = engine
        .actions
        .invoke("after_read", json!({}), &admin, None, None)
        .await
        .unwrap();
    assert_eq!(
        rec.status, "failed",
        "read-only call is not a side effect: {rec:?}"
    );

    let rec = engine
        .actions
        .invoke("before_calls", json!({}), &admin, None, None)
        .await
        .unwrap();
    assert_eq!(rec.status, "failed", "{rec:?}");

    let t0 = std::time::Instant::now();
    let rec = engine
        .actions
        .invoke("slow_after_merge", json!({}), &admin, None, None)
        .await
        .unwrap();
    assert_eq!(rec.status, "unknown", "{rec:?}");
    assert!(t0.elapsed() < std::time::Duration::from_secs(3));

    // §15.1 VM spans are recorded for executions.
    let spans = engine.telemetry.recent_spans(200);
    let vm_span = spans
        .iter()
        .find(|s| s.name == "luau_vm:after_merge:execute")
        .expect("luau_vm span recorded");
    assert!(vm_span.attributes.get("queue_ms").is_some());
    assert!(vm_span.attributes.get("used_memory_bytes").is_some());
}

#[tokio::test]
async fn action_emit_is_contract_checked_and_versioned() {
    let engine = RuntimeEngine::new(
        graph(),
        SqliteStore::open_in_memory().unwrap(),
        ResourceBudget::default(),
    )
    .unwrap();
    let admin = Principal::administrator();
    let rec = engine
        .actions
        .invoke("emit_ok", json!({}), &admin, None, None)
        .await
        .unwrap();
    assert_eq!(rec.status, "succeeded", "{rec:?}");
    let events = engine.events.replay_since("emit_ok", 0).unwrap();
    let ev = events
        .iter()
        .find(|e| e.semantic_type == "t.merged")
        .expect("declared Event emitted");
    assert_eq!(ev.schema_version, "2");

    let rec = engine
        .actions
        .invoke("emit_spoof", json!({}), &admin, None, None)
        .await
        .unwrap();
    assert_eq!(rec.status, "failed", "{rec:?}");
    assert!(rec.reason.as_deref().unwrap().contains("not declared"));
    let events = engine.events.replay_since("emit_spoof", 0).unwrap();
    assert!(events.iter().all(|e| e.semantic_type != "github.pr_merged"));
}

#[test]
fn workspace_resources_merge_defaults_and_preview_mock_ids() {
    let toml_src = r#"
memory_limit_mib = 16
timeout_ms = 2000

[instances.history]
timeout_ms = 9000
"#;
    let res: WorkspaceResources = toml::from_str(toml_src).unwrap();
    let h = res.overrides_for("history").unwrap();
    assert_eq!(h.timeout_ms, Some(9000));
    assert_eq!(h.memory_limit_mib, Some(16));
    assert_eq!(
        res.overrides_for("preview_mock:history")
            .unwrap()
            .timeout_ms,
        Some(9000)
    );
    assert_eq!(res.overrides_for("other").unwrap().timeout_ms, Some(2000));
    assert!(WorkspaceResources::default().overrides_for("x").is_none());
}

#[test]
fn undeclared_or_non_library_libraries_fail_validation() {
    let mut g = graph();
    let decl = r#"
local dash = require("dash")
return dash.data_provider { id = "t.uses_libs", libraries = { "t.missing", "t.conn" } }
"#;
    g.definitions.insert(
        "t.uses_libs".into(),
        load_def(
            "t",
            decl,
            "local R = {}\nfunction R.query(ctx) return {} end\nreturn R",
        ),
    );
    let report = validate_configured_graph(&g, &SqliteStore::open_in_memory().unwrap(), false);
    let issues: Vec<_> = report
        .issues
        .iter()
        .filter(|i| i.code == "UNRESOLVED_LIBRARY")
        .collect();
    assert_eq!(issues.len(), 2, "{:?}", report.issues);
    assert!(!report.valid);
}
