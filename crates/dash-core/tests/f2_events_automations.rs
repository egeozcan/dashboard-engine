//! Event retention and sequences, automation outcomes, unscoped Event triggers, automation
//! Dashboard State and the automation circuit breaker (§7.2, §7.3, §8, §15.2).

use dash_core::authority::check_draft_activation_authority;
use dash_core::state::{StateKeyDeclaration, StateMigrationPolicy, StateValueType};
use dash_core::{
    CapabilityCeiling, ConfiguredGraph, DashboardDoc, FailureInjectionRule, InstanceConfig,
    LoadedDefinition, Principal, PrincipalKind, RuntimeEngine, ServicePrincipalRequested,
};
use dash_luau::{evaluate_declaration, DefinitionCategory, ResourceBudget};
use dash_store::SqliteStore;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

fn load_def(decl: &str, runtime: &str) -> LoadedDefinition {
    let mut sources = BTreeMap::new();
    sources.insert("definition.luau".into(), decl.as_bytes().to_vec());
    sources.insert("runtime.luau".into(), runtime.as_bytes().to_vec());
    let manifest = evaluate_declaration(decl, &sources).unwrap();
    LoadedDefinition {
        package_name: "test".into(),
        manifest,
        definition_source: decl.into(),
        runtime_source: runtime.into(),
        tests_source: None,
        fixtures: BTreeMap::new(),
        widget_assets: BTreeMap::new(),
    }
}

fn instance(
    id: &str,
    category: DefinitionCategory,
    definition: &str,
    inputs: &[(&str, &str)],
    config: serde_json::Value,
    run_as: Option<&str>,
) -> InstanceConfig {
    InstanceConfig {
        id: id.into(),
        category,
        definition: definition.into(),
        inputs: inputs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        config,
        secrets: BTreeMap::new(),
        capabilities: vec![],
        run_as: run_as.map(|s| s.to_string()),
        enabled: None,
    }
}

const SOURCE_ACT: &str = r#"
local dash = require("dash")
return dash.action_provider { id = "test.source_act", handler = "execute" }
"#;
const SINK_ACT: &str = r#"
local dash = require("dash")
return dash.action_provider { id = "test.sink", handler = "execute" }
"#;
const SINK_RT: &str = r#"
local R = {}
function R.execute(ctx) return { n = ctx.args.n } end
return R
"#;
const FAILING_ACT: &str = r#"
local dash = require("dash")
return dash.action_provider { id = "test.failing", handler = "execute" }
"#;
const FAILING_RT: &str = r#"
local R = {}
function R.execute(ctx) error("upstream rejected") end
return R
"#;
const WILD_DP: &str = r#"
local dash = require("dash")
return dash.data_provider {
    id = "test.wild_dp",
    handler = "query",
    emits = { { semantic_type = "test.wild", durable = true } },
}
"#;
const PLAIN_DP: &str = r#"
local dash = require("dash")
return dash.data_provider { id = "test.plain_dp", handler = "query" }
"#;
const DP_RT: &str = r#"
local R = {}
function R.query(ctx) return { ok = true } end
return R
"#;
/// Scoped trigger on `src`; invokes `sink` with the Event's `n`.
const SCOPED_AUTO: &str = r#"
local dash = require("dash")
return dash.automation {
    id = "test.scoped_auto",
    handler = "run",
    inputs = {
        src = dash.action_ref("test.source_act"),
        sink = dash.action_ref("test.sink"),
    },
    triggers = {
        { kind = "event", event_type = "test.happened", source_input = "src", max_attempts = 3 },
    },
}
"#;
const SINK_AUTO_RT: &str = r#"
local R = {}
function R.run(ctx) return ctx:await(ctx.inputs.sink:invoke({ n = ctx.event.payload.n })) end
return R
"#;
/// Scoped trigger on `src`; invokes the always-failing action.
const FAILING_AUTO: &str = r#"
local dash = require("dash")
return dash.automation {
    id = "test.failing_auto",
    handler = "run",
    inputs = {
        src = dash.action_ref("test.source_act"),
        sink = dash.action_ref("test.failing"),
    },
    triggers = {
        { kind = "event", event_type = "test.fail_me", source_input = "src", max_attempts = 3 },
    },
}
"#;
/// Scoped trigger on `src`; invokes `sink` (whose outcome a test makes `unknown`).
const UNKNOWN_AUTO: &str = r#"
local dash = require("dash")
return dash.automation {
    id = "test.unknown_auto",
    handler = "run",
    inputs = {
        src = dash.action_ref("test.source_act"),
        sink = dash.action_ref("test.sink"),
    },
    triggers = {
        { kind = "event", event_type = "test.unknown_me", source_input = "src", max_attempts = 3 },
    },
}
"#;
/// Unscoped trigger: consumes `test.wild` from every declared emitter.
const WILD_AUTO: &str = r#"
local dash = require("dash")
return dash.automation {
    id = "test.wild_auto",
    handler = "run",
    inputs = { sink = dash.action_ref("test.sink") },
    triggers = { { kind = "event", event_type = "test.wild", max_attempts = 1 } },
}
"#;
const DASH_WRITER: &str = r#"
local dash = require("dash")
return dash.automation { id = "test.dash_writer", handler = "run" }
"#;
const DASH_WRITER_RT: &str = r#"
local R = {}
function R.run(ctx)
    ctx:await(ctx.dashboard_state:set(ctx.event.key, ctx.event.value))
    return { ok = true }
end
return R
"#;
const LOOPING_AUTO: &str = r#"
local dash = require("dash")
return dash.automation { id = "test.looping", handler = "run" }
"#;
const LOOPING_RT: &str = r#"
local R = {}
function R.run(ctx)
    while true do end
end
return R
"#;

fn sp_ceiling() -> CapabilityCeiling {
    CapabilityCeiling {
        allowed_actions: BTreeSet::from(["sink".into(), "failing".into()]),
        ..Default::default()
    }
}

fn test_graph() -> ConfiguredGraph {
    let mut g = ConfiguredGraph {
        generation_id: 1,
        ..Default::default()
    };
    for (id, decl, rt) in [
        ("test.source_act", SOURCE_ACT, SINK_RT),
        ("test.sink", SINK_ACT, SINK_RT),
        ("test.failing", FAILING_ACT, FAILING_RT),
        ("test.wild_dp", WILD_DP, DP_RT),
        ("test.plain_dp", PLAIN_DP, DP_RT),
        ("test.scoped_auto", SCOPED_AUTO, SINK_AUTO_RT),
        ("test.failing_auto", FAILING_AUTO, SINK_AUTO_RT),
        ("test.unknown_auto", UNKNOWN_AUTO, SINK_AUTO_RT),
        ("test.wild_auto", WILD_AUTO, SINK_AUTO_RT),
        ("test.dash_writer", DASH_WRITER, DASH_WRITER_RT),
        ("test.looping", LOOPING_AUTO, LOOPING_RT),
    ] {
        g.definitions.insert(id.into(), load_def(decl, rt));
    }
    use DefinitionCategory::*;
    for inst in [
        instance(
            "src",
            ActionProvider,
            "test.source_act",
            &[],
            json!({}),
            None,
        ),
        instance("sink", ActionProvider, "test.sink", &[], json!({}), None),
        instance(
            "failing",
            ActionProvider,
            "test.failing",
            &[],
            json!({}),
            None,
        ),
        instance(
            "wild_src",
            DataProvider,
            "test.wild_dp",
            &[],
            json!({}),
            None,
        ),
        instance("plain", DataProvider, "test.plain_dp", &[], json!({}), None),
        instance(
            "auto",
            Automation,
            "test.scoped_auto",
            &[("src", "src"), ("sink", "sink")],
            json!({}),
            Some("auto_sp"),
        ),
        instance(
            "failing_auto",
            Automation,
            "test.failing_auto",
            &[("src", "src"), ("sink", "failing")],
            json!({}),
            Some("auto_sp"),
        ),
        instance(
            "unknown_auto",
            Automation,
            "test.unknown_auto",
            &[("src", "src"), ("sink", "sink")],
            json!({}),
            Some("auto_sp"),
        ),
        instance(
            "wild_auto",
            Automation,
            "test.wild_auto",
            &[("sink", "sink")],
            json!({}),
            Some("auto_sp"),
        ),
        instance(
            "dash_writer",
            Automation,
            "test.dash_writer",
            &[],
            json!({"dashboard": "main"}),
            Some("auto_sp"),
        ),
        instance(
            "looping",
            Automation,
            "test.looping",
            &[],
            json!({}),
            Some("auto_sp"),
        ),
    ] {
        g.instances.insert(inst.id.clone(), inst);
    }
    g.dashboards.insert(
        "main".into(),
        DashboardDoc {
            id: "main".into(),
            title: "Main".into(),
            layout: "grid".into(),
            adapters: vec![],
            initial_state: BTreeMap::new(),
            overrides: BTreeMap::new(),
            state_schema: BTreeMap::from([(
                "repo".to_string(),
                StateKeyDeclaration {
                    schema_version: 2,
                    value_type: StateValueType::String,
                    nullable: false,
                    max_bytes: Some(64),
                    migration: StateMigrationPolicy::Reject,
                },
            )]),
        },
    );
    g.service_principals.insert(
        "auto_sp".into(),
        ServicePrincipalRequested {
            id: "auto_sp".into(),
            description: "test automations".into(),
            ceiling: sp_ceiling(),
        },
    );
    g
}

fn engine_with(g: ConfiguredGraph, store: &SqliteStore, budget: ResourceBudget) -> RuntimeEngine {
    let engine = RuntimeEngine::new(g, store.clone(), budget).unwrap();
    engine
        .automations
        .set_retry_backoff(Duration::from_millis(1));
    engine
}

fn engine_on(store: &SqliteStore) -> RuntimeEngine {
    engine_with(test_graph(), store, ResourceBudget::default())
}

fn granted_store() -> SqliteStore {
    let store = SqliteStore::open_in_memory().unwrap();
    store
        .set_service_principal_grant(
            "auto_sp",
            serde_json::to_value(sp_ceiling()).unwrap(),
            "admin",
        )
        .unwrap();
    store
}

fn executions_of(store: &SqliteStore, action: &str) -> Vec<dash_store::ActionExecutionRecord> {
    store
        .list_action_executions(1000)
        .unwrap()
        .into_iter()
        .filter(|r| r.action_instance == action)
        .collect()
}

fn future_cutoff() -> String {
    (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339()
}

/// EVT-RET-1 / SRV-1: hourly retention of a fully consumed source must not let its sequence
/// restart below the consumer checkpoint after a restart.
#[tokio::test]
async fn retention_of_a_consumed_source_never_restarts_its_sequence_below_the_checkpoint() {
    let store = granted_store();
    {
        let engine = engine_on(&store);
        for n in 1..=3 {
            engine
                .events
                .emit("src", "test.happened", "1", "tr", true, json!({ "n": n }))
                .unwrap();
        }
        let report = engine.automations.drain_durable_events().await;
        assert_eq!(report.succeeded, 3);
        assert_eq!(engine.events.get_checkpoint("auto", "src"), 3);
        // The hourly pass (EventBus::prune_retained -> prune_events_before).
        engine.events.log().prune_before(&future_cutoff()).unwrap();
        let kept: Vec<i64> = engine
            .events
            .read_since("src", 0, 100)
            .unwrap()
            .iter()
            .map(|e| e.sequence)
            .collect();
        assert_eq!(kept, vec![3], "only the newest Event of the source is kept");
    }
    // Restart: a fresh EventBus has no in-memory sequence for the source.
    let engine = engine_on(&store);
    let ev = engine
        .events
        .emit("src", "test.happened", "1", "tr", true, json!({ "n": 4 }))
        .unwrap();
    assert!(
        ev.sequence > engine.events.get_checkpoint("auto", "src"),
        "sequence {} must be above the checkpoint",
        ev.sequence
    );
    let report = engine.automations.drain_durable_events().await;
    assert_eq!(report.succeeded, 1, "{report:?}");
    assert_eq!(executions_of(&store, "sink").len(), 4);
}

/// Defence in depth for EVT-RET-1: even a log emptied by an older pruner (no head row) never
/// re-issues sequences at or below a consumer checkpoint, durable or ephemeral.
#[tokio::test]
async fn sequences_are_seeded_above_consumer_checkpoints_when_the_log_is_empty() {
    let store = granted_store();
    {
        let engine = engine_on(&store);
        for n in 1..=5 {
            engine
                .events
                .emit("src", "test.happened", "1", "tr", true, json!({ "n": n }))
                .unwrap();
        }
        engine.automations.drain_durable_events().await;
    }
    store
        .with_conn_sync(|c| {
            c.execute("DELETE FROM events", [])?;
            Ok(())
        })
        .unwrap();
    let engine = engine_on(&store);
    let live = engine
        .events
        .emit("src", "test.happened", "1", "tr", false, json!({ "n": 0 }))
        .unwrap();
    assert_eq!(live.sequence, 6, "ephemeral base comes from the high water");
    let ev = engine
        .events
        .emit("src", "test.happened", "1", "tr", true, json!({ "n": 6 }))
        .unwrap();
    assert_eq!(ev.sequence, 7);
    let report = engine.automations.drain_durable_events().await;
    assert_eq!(report.succeeded, 1, "{report:?}");
}

/// AUTO-FAIL-1: a run whose action failed is not recorded as succeeded: it is retried, then
/// recorded failed and dead-lettered.
#[tokio::test]
async fn a_failed_action_fails_the_run_retries_and_dead_letters() {
    let store = granted_store();
    let engine = engine_on(&store);
    let ev = engine
        .events
        .emit("src", "test.fail_me", "1", "tr", true, json!({ "n": 1 }))
        .unwrap();
    let report = engine.automations.drain_durable_events().await;
    assert_eq!(report.failed, 1, "{report:?}");
    let runs = engine
        .automations
        .list_executions(Some("failing_auto"), 10)
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, "failed");
    assert_eq!(runs[0].attempts, 3);
    assert!(
        runs[0]
            .error
            .as_deref()
            .unwrap_or("")
            .contains("ended failed"),
        "{:?}",
        runs[0].error
    );
    assert!(!runs[0].action_executions.is_empty());
    let dlq = engine.automations.list_dead_letters(false, 10).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].consumer_id, "failing_auto");
    assert_eq!(dlq[0].event_id, ev.event_id);
    // A later replay does not short-circuit on a bogus "succeeded" record.
    let replay = engine
        .automations
        .replay_events("src", 0, None)
        .await
        .unwrap();
    assert!(replay.iter().all(|r| r.is_err()), "{replay:?}");
}

/// AUTO-FAIL-1: an action whose outcome is unknown fails the run at once (no blind retries),
/// is dead-lettered, and a redrive after manual resolution completes the run.
#[tokio::test]
async fn an_unknown_action_outcome_is_dead_lettered_and_redriven_after_resolution() {
    let store = granted_store();
    let engine = engine_on(&store);
    engine.telemetry.inject_failure(FailureInjectionRule {
        instance_id: "sink".into(),
        mode: "unknown".into(),
        message: "connection lost after request".into(),
        remaining: Some(1),
    });
    engine
        .events
        .emit("src", "test.unknown_me", "1", "tr", true, json!({ "n": 9 }))
        .unwrap();
    let report = engine.automations.drain_durable_events().await;
    assert_eq!(report.failed, 1, "{report:?}");
    let run = &engine
        .automations
        .list_executions(Some("unknown_auto"), 10)
        .unwrap()[0];
    assert_eq!(run.status, "failed");
    assert_eq!(run.attempts, 1, "an unknown outcome is not retried blindly");
    let sink_runs = executions_of(&store, "sink");
    assert_eq!(sink_runs.len(), 1);
    assert_eq!(sink_runs[0].status, "unknown");
    let dlq = engine.automations.list_dead_letters(false, 10).unwrap();
    assert_eq!(dlq.len(), 1);

    let admin = Principal::administrator();
    engine
        .actions
        .resolve_execution(&sink_runs[0].execution_id, "succeeded", "verified", &admin)
        .await
        .unwrap();
    let res = engine
        .automations
        .redrive_dead_letter(&dlq[0].dlq_id, &admin)
        .await
        .unwrap();
    assert_eq!(res["execution_id"], json!(sink_runs[0].execution_id));
    assert_eq!(
        executions_of(&store, "sink").len(),
        1,
        "no duplicate side effect"
    );
    assert!(engine
        .automations
        .list_dead_letters(false, 10)
        .unwrap()
        .is_empty());
}

/// AUTH-EVT-SRC-1: an unscoped Event trigger consumes only Instances whose contract declares
/// the Event type.
#[tokio::test]
async fn unscoped_triggers_ignore_events_from_instances_that_do_not_declare_the_type() {
    let store = granted_store();
    let engine = engine_on(&store);
    engine.automations.register_consumers();
    engine
        .events
        .emit("plain", "test.wild", "1", "tr", true, json!({ "n": 1 }))
        .unwrap();
    engine
        .events
        .emit("wild_src", "test.wild", "1", "tr", true, json!({ "n": 2 }))
        .unwrap();
    let report = engine.automations.drain_durable_events().await;
    assert_eq!(report.succeeded, 1, "{report:?}");
    let runs = executions_of(&store, "sink");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].args["n"], 2);
    // Live delivery applies the same rule.
    let spoof = engine
        .events
        .emit("plain", "test.wild", "1", "tr", false, json!({ "n": 3 }))
        .unwrap();
    assert!(engine.automations.dispatch_event(&spoof).await.is_empty());
}

/// AUTH-EVT-SRC-1: a restricted editor cannot activate a new emitter that feeds an unscoped
/// trigger of a Service-principal automation whose ceiling exceeds its own.
#[test]
fn a_new_emitter_of_an_unscoped_trigger_needs_service_principal_approval() {
    let store = granted_store();
    let active = test_graph();
    let mut candidate = active.clone();
    candidate.instances.insert(
        "evil".into(),
        instance(
            "evil",
            DefinitionCategory::DataProvider,
            "test.wild_dp",
            &[],
            json!({}),
            None,
        ),
    );
    let agent = Principal {
        id: "agent_1".into(),
        kind: PrincipalKind::Agent,
        ceiling: CapabilityCeiling {
            can_author_drafts: true,
            can_activate_drafts: true,
            ..Default::default()
        },
    };
    let issues =
        check_draft_activation_authority(&store, &active, &candidate, std::slice::from_ref(&agent));
    assert!(
        issues
            .iter()
            .any(|i| i.code == "PENDING_SERVICE_PRINCIPAL_BINDING" && i.target == "wild_auto"),
        "{issues:?}"
    );
    // A new Instance that does not emit the type is not a change to the automation.
    let mut harmless = active.clone();
    harmless.instances.insert(
        "other".into(),
        instance(
            "other",
            DefinitionCategory::DataProvider,
            "test.plain_dp",
            &[],
            json!({}),
            None,
        ),
    );
    assert!(check_draft_activation_authority(&store, &active, &harmless, &[agent]).is_empty());
}

/// T2 residual: an unscoped trigger's consumer is registered for every declared emitter at
/// activation, so an Event a source first emits after activation survives a crash and a new
/// Generation before the first drain.
#[tokio::test]
async fn unscoped_consumers_are_registered_before_a_source_first_emits() {
    let store = granted_store();
    let gen1 = store.record_generation("rev1", "digest1").unwrap();
    {
        let mut g = test_graph();
        g.generation_id = gen1;
        let engine = engine_with(g, &store, ResourceBudget::default());
        engine.automations.register_consumers();
        assert_eq!(
            engine
                .events
                .find_checkpoint("wild_auto", "wild_src")
                .unwrap(),
            Some(0)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
        engine
            .events
            .emit("wild_src", "test.wild", "1", "tr", true, json!({ "n": 1 }))
            .unwrap();
        // Crash before the consumer drained.
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    let gen2 = store.record_generation("rev2", "digest2").unwrap();
    let mut g = test_graph();
    g.generation_id = gen2;
    let engine = engine_with(g, &store, ResourceBudget::default());
    engine.automations.register_consumers();
    let report = engine.automations.drain_durable_events().await;
    assert_eq!(report.succeeded, 1, "{report:?}");
    assert_eq!(executions_of(&store, "sink").len(), 1);
}

/// SC-2: automation Dashboard State writes go through the Dashboard's declared state schema.
#[tokio::test]
async fn automation_dashboard_state_writes_follow_the_declared_schema() {
    let store = granted_store();
    let engine = engine_on(&store);
    let run = |key: &str, value: serde_json::Value| {
        let automations = engine.automations.clone();
        let payload = json!({ "key": key, "value": value });
        async move { automations.trigger_automation("dash_writer", payload).await }
    };
    // Declared key, right type: stamped with the declared schema version.
    run("repo", json!("acme/platform")).await.unwrap();
    let rec = store
        .get_state("dashboard", "main", "repo")
        .unwrap()
        .unwrap();
    assert_eq!(rec.value, json!("acme/platform"));
    assert_eq!(rec.schema_version, 2);
    // Wrong type, too large, and undeclared keys are rejected.
    let err = run("repo", json!({ "x": 1 })).await.unwrap_err();
    assert!(err.contains("repo"), "{err}");
    let err = run("repo", json!("x".repeat(200))).await.unwrap_err();
    assert!(err.contains("bytes") || err.contains("limit"), "{err}");
    let err = run("undeclared", json!("v")).await.unwrap_err();
    assert!(err.contains("not declared"), "{err}");
    let rec = store
        .get_state("dashboard", "main", "repo")
        .unwrap()
        .unwrap();
    assert_eq!(rec.value, json!("acme/platform"));
    assert!(store
        .get_state("dashboard", "main", "undeclared")
        .unwrap()
        .is_none());
    assert!(store
        .list_audits(50)
        .unwrap()
        .iter()
        .any(|a| a.operation == "dashboard_state.set"
            && a.outcome == "denied"
            && a.principal_id == "auto_sp"));
}

/// SC-9: repeated automation timeouts trip the Instance's circuit breaker.
#[tokio::test]
async fn repeated_automation_timeouts_trip_the_circuit_breaker() {
    let store = granted_store();
    let budget = ResourceBudget {
        wall_timeout: Duration::from_millis(30),
        ..ResourceBudget::default()
    };
    let engine = engine_with(test_graph(), &store, budget);
    for _ in 0..5 {
        let err = engine
            .automations
            .trigger_automation("looping", json!({}))
            .await
            .unwrap_err();
        assert!(err.contains("timed out"), "{err}");
    }
    let started = std::time::Instant::now();
    let err = engine
        .automations
        .trigger_automation("looping", json!({}))
        .await
        .unwrap_err();
    assert!(err.contains("circuit_open"), "{err}");
    assert!(started.elapsed() < Duration::from_millis(30));
    let runs = engine
        .automations
        .list_executions(Some("looping"), 10)
        .unwrap();
    assert_eq!(runs.len(), 6);
    assert!(runs[0]
        .error
        .as_deref()
        .unwrap_or("")
        .contains("circuit_open"));
}

/// F2: whether an attempt is retried is decided by the host (open breaker, action outcome not
/// known yet), never by matching tokens in error text that upstream systems or scripts control.
#[tokio::test]
async fn retries_do_not_depend_on_error_text() {
    // An upstream failure whose message happens to contain the non-retryable codes.
    let store = granted_store();
    let mut g = test_graph();
    g.definitions.insert(
        "test.failing".into(),
        load_def(
            FAILING_ACT,
            r#"local R = {}
function R.execute(ctx) error("remote API said circuit_open / action_outcome_pending") end
return R"#,
        ),
    );
    let engine = engine_with(g, &store, ResourceBudget::default());
    engine
        .events
        .emit("src", "test.fail_me", "1", "tr", true, json!({ "n": 1 }))
        .unwrap();
    engine.automations.drain_durable_events().await;
    let runs = engine
        .automations
        .list_executions(Some("failing_auto"), 10)
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, "failed");
    assert_eq!(runs[0].attempts, 3, "{:?}", runs[0].error);

    // A handler cannot skip its retries by raising a forged code either.
    let store = granted_store();
    let mut g = test_graph();
    g.definitions.insert(
        "test.failing_auto".into(),
        load_def(
            FAILING_AUTO,
            r#"local R = {}
function R.run(ctx) error("action_outcome_pending: forged") end
return R"#,
        ),
    );
    let engine = engine_with(g, &store, ResourceBudget::default());
    engine
        .events
        .emit("src", "test.fail_me", "1", "tr", true, json!({ "n": 2 }))
        .unwrap();
    engine.automations.drain_durable_events().await;
    let runs = engine
        .automations
        .list_executions(Some("failing_auto"), 10)
        .unwrap();
    assert_eq!(runs[0].attempts, 3, "{:?}", runs[0].error);
}
