//! Durable Event delivery, replay safety, retries/DLQ, cron and Signal triggers, automation
//! execution records, automation State access and history sampling (§7.2–§7.4).

use chrono::{DateTime, Utc};
use dash_core::{
    validate_configured_graph, CapabilityCeiling, ConfiguredGraph, DashboardDoc,
    FailureInjectionRule, HistorySeriesConfig, InstanceConfig, LoadedDefinition, Principal,
    PrincipalKind, RuntimeEngine, ServicePrincipalRequested,
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
function R.execute(ctx) return { n = ctx.args.n, key = ctx.idempotency_key } end
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
const FLAG_DP: &str = r#"
local dash = require("dash")
return dash.data_provider { id = "test.flag", handler = "query" }
"#;
const FLAG_RT: &str = r#"
local R = {}
function R.query(ctx) return { value = ctx:await(ctx.state:get("flag")) or 0 } end
return R
"#;
const EVENT_AUTO: &str = r#"
local dash = require("dash")
return dash.automation {
    id = "test.event_auto",
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
const EVENT_AUTO_RT: &str = r#"
local R = {}
function R.run(ctx)
    local budget = ctx:await(ctx.state:get("fail_budget")) or 0
    if budget > 0 then
        ctx:await(ctx.state:set("fail_budget", budget - 1))
        error("transient failure")
    end
    return ctx:await(ctx.inputs.sink:invoke({ n = ctx.event.payload.n }))
end
return R
"#;
const CRON_SKIP: &str = r#"
local dash = require("dash")
return dash.automation {
    id = "test.cron_skip",
    handler = "run",
    triggers = { { kind = "cron", schedule = "0 * * * *" } },
}
"#;
const CRON_CATCH: &str = r#"
local dash = require("dash")
return dash.automation {
    id = "test.cron_catch",
    handler = "run",
    triggers = { { kind = "cron", schedule = "0 * * * *", catch_up = "catch_up" } },
}
"#;
const CRON_RT: &str = r#"
local R = {}
function R.run(ctx)
    return { scheduled_for = ctx.event.trigger.scheduled_for, missed = ctx.event.trigger.missed_windows }
end
return R
"#;
const SIGNAL_AUTO: &str = r#"
local dash = require("dash")
return dash.automation {
    id = "test.signal_auto",
    handler = "run",
    inputs = { data = dash.data_ref("test.flag") },
    triggers = {
        { kind = "signal", source_input = "data", params = {}, predicate = "should_run" },
    },
}
"#;
const SIGNAL_RT: &str = r#"
local R = {}
function R.should_run(ctx) return ctx.event.payload.value.value >= 2 end
function R.run(ctx) return { seen = ctx.event.payload.value.value } end
return R
"#;
const STATE_SIGNAL_AUTO: &str = r#"
local dash = require("dash")
return dash.automation {
    id = "test.state_signal_auto",
    handler = "run",
    triggers = { { kind = "signal", state_key = "mode" } },
}
"#;
const STATE_SIGNAL_RT: &str = r#"
local R = {}
function R.run(ctx) return { mode = ctx.event.payload.value } end
return R
"#;
const DASH_WRITER: &str = r#"
local dash = require("dash")
return dash.automation { id = "test.dash_writer", handler = "run" }
"#;
const DASH_WRITER_RT: &str = r#"
local R = {}
function R.run(ctx)
    ctx:await(ctx.dashboard_state:set("note", "hi"))
    return { note = ctx:await(ctx.dashboard_state:get("note")) }
end
return R
"#;
const EMITTER_AUTO: &str = r#"
local dash = require("dash")
return dash.automation { id = "test.emitter_auto", handler = "run" }
"#;
const EMITTER_RT: &str = r#"
local R = {}
function R.run(ctx)
    ctx:await(ctx:emit("test.happened", { n = 1 }, true))
    return { emitted = true }
end
return R
"#;

fn test_graph() -> ConfiguredGraph {
    let mut g = ConfiguredGraph {
        generation_id: 1,
        ..Default::default()
    };
    for (id, decl, rt) in [
        ("test.source_act", SOURCE_ACT, SINK_RT),
        ("test.sink", SINK_ACT, SINK_RT),
        ("test.failing", FAILING_ACT, FAILING_RT),
        ("test.flag", FLAG_DP, FLAG_RT),
        ("test.event_auto", EVENT_AUTO, EVENT_AUTO_RT),
        ("test.cron_skip", CRON_SKIP, CRON_RT),
        ("test.cron_catch", CRON_CATCH, CRON_RT),
        ("test.signal_auto", SIGNAL_AUTO, SIGNAL_RT),
        ("test.state_signal_auto", STATE_SIGNAL_AUTO, STATE_SIGNAL_RT),
        ("test.dash_writer", DASH_WRITER, DASH_WRITER_RT),
        ("test.emitter_auto", EMITTER_AUTO, EMITTER_RT),
    ] {
        g.definitions.insert(id.into(), load_def(decl, rt));
    }
    use DefinitionCategory::*;
    for inst in [
        instance("src", ActionProvider, "test.source_act", &[], json!({}), None),
        instance("sink", ActionProvider, "test.sink", &[], json!({}), None),
        instance("failing", ActionProvider, "test.failing", &[], json!({}), None),
        instance("flag", DataProvider, "test.flag", &[], json!({}), None),
        instance(
            "auto",
            Automation,
            "test.event_auto",
            &[("src", "src"), ("sink", "sink")],
            json!({}),
            Some("auto_sp"),
        ),
        instance("cron_skip", Automation, "test.cron_skip", &[], json!({}), Some("auto_sp")),
        instance("cron_catch", Automation, "test.cron_catch", &[], json!({}), Some("auto_sp")),
        instance(
            "sig_auto",
            Automation,
            "test.signal_auto",
            &[("data", "flag")],
            json!({}),
            Some("auto_sp"),
        ),
        instance(
            "state_sig",
            Automation,
            "test.state_signal_auto",
            &[],
            json!({"dashboard": "main"}),
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
        instance("unbound_writer", Automation, "test.dash_writer", &[], json!({}), Some("auto_sp")),
        instance("emitter", Automation, "test.emitter_auto", &[], json!({}), Some("auto_sp")),
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

fn sp_ceiling() -> CapabilityCeiling {
    CapabilityCeiling {
        allowed_actions: BTreeSet::from(["sink".into()]),
        allowed_providers: BTreeSet::from(["flag".into()]),
        ..Default::default()
    }
}

fn engine_on(store: &SqliteStore) -> RuntimeEngine {
    let engine = RuntimeEngine::new(test_graph(), store.clone(), ResourceBudget::default()).unwrap();
    engine.automations.set_retry_backoff(Duration::from_millis(1));
    engine
}

fn granted_store() -> SqliteStore {
    let store = SqliteStore::open_in_memory().unwrap();
    store
        .set_service_principal_grant("auto_sp", serde_json::to_value(sp_ceiling()).unwrap(), "admin")
        .unwrap();
    store
}

fn sink_executions(store: &SqliteStore) -> Vec<dash_store::ActionExecutionRecord> {
    store
        .list_action_executions(1000)
        .unwrap()
        .into_iter()
        .filter(|r| r.action_instance == "sink")
        .collect()
}

fn agent() -> Principal {
    Principal {
        id: "agent_1".into(),
        kind: PrincipalKind::Agent,
        ceiling: CapabilityCeiling::default(),
    }
}

#[tokio::test]
async fn durable_events_survive_restart_and_are_delivered_once_from_the_log() {
    let store = granted_store();
    {
        let engine = engine_on(&store);
        engine
            .events
            .emit("src", "test.happened", "1", "tr_restart", true, json!({"n": 1}))
            .unwrap();
        // The process stops before any consumer ran.
    }
    let engine = engine_on(&store);
    let first = engine.automations.drain_durable_events().await;
    assert_eq!(first.succeeded, 1);
    let second = engine.automations.drain_durable_events().await;
    assert_eq!(second.succeeded, 0, "checkpoint must prevent redelivery");
    assert_eq!(sink_executions(&store).len(), 1);
    assert_eq!(engine.events.get_checkpoint("auto", "src"), 1);
}

#[tokio::test]
async fn live_delivery_out_of_order_never_skips_earlier_events() {
    let store = granted_store();
    let engine = engine_on(&store);
    let e1 = engine
        .events
        .emit("src", "test.happened", "1", "tr_1", true, json!({"n": 1}))
        .unwrap();
    let e2 = engine
        .events
        .emit("src", "test.happened", "1", "tr_2", true, json!({"n": 2}))
        .unwrap();
    // e2 reaches the automation first (e.g. broadcast race): it must not mark e1 as consumed.
    let live = engine.automations.dispatch_event(&e2).await;
    assert!(live[0].is_ok());
    let report = engine.automations.drain_durable_events().await;
    assert_eq!(report.succeeded, 2, "e1 delivered, e2 recognised as already done");
    let runs = sink_executions(&store);
    assert_eq!(runs.len(), 2, "one run per event, no duplicate for e2");
    assert_eq!(engine.events.get_checkpoint("auto", "src"), e2.sequence);
    assert!(e1.sequence < e2.sequence);
}

#[tokio::test]
async fn replay_never_repeats_side_effects_without_authorized_reexecution() {
    let store = granted_store();
    let engine = engine_on(&store);
    let ev = engine
        .events
        .emit("src", "test.happened", "1", "tr_replay", true, json!({"n": 7}))
        .unwrap();
    engine.automations.drain_durable_events().await;
    let runs = sink_executions(&store);
    assert_eq!(runs.len(), 1);
    // The script passed no idempotency key: the engine derived a deterministic one.
    let key = runs[0].idempotency_key.clone().unwrap();
    assert_eq!(key, format!("auto:auto:event:{}:1", ev.event_id));

    // Plain replay (and replayed dispatch) returns the recorded result without re-running.
    let replayed = engine.automations.replay_events("src", 0, None).await.unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].as_ref().unwrap()["execution_id"], json!(runs[0].execution_id));
    let replay_env = engine.events.replay_since("src", 0).unwrap();
    engine.automations.dispatch_event(&replay_env[0]).await;
    assert_eq!(sink_executions(&store).len(), 1);

    // Re-execution needs explicit, authorized intent.
    assert!(engine
        .automations
        .replay_events("src", 0, Some(&agent()))
        .await
        .is_err());
    let again = engine
        .automations
        .replay_events("src", 0, Some(&Principal::administrator()))
        .await
        .unwrap();
    assert!(again[0].is_ok());
    assert_eq!(sink_executions(&store).len(), 2);
    let audits = store.list_audits(50).unwrap();
    assert!(audits.iter().any(|a| a.operation == "automation.reexecute"));
}

#[tokio::test]
async fn bounded_retry_then_dead_letter_and_authorized_redrive() {
    let store = granted_store();
    let engine = engine_on(&store);

    // Two transient failures: third attempt succeeds.
    engine
        .state
        .set("instance", "auto", "fail_budget", 1, 1, json!(2), None)
        .unwrap();
    let ev1 = engine
        .events
        .emit("src", "test.happened", "1", "tr_a", true, json!({"n": 1}))
        .unwrap();
    let r = engine.automations.drain_durable_events().await;
    assert_eq!(r.succeeded, 1);
    let rec = &engine.automations.list_executions(Some("auto"), 10).unwrap()[0];
    assert_eq!(rec.attempts, 3);
    assert_eq!(rec.status, "succeeded");
    assert_eq!(rec.event_id.as_deref(), Some(ev1.event_id.as_str()));

    // Persistent failure: dead-lettered after max_attempts, checkpoint still advances.
    engine
        .state
        .set("instance", "auto", "fail_budget", 1, 1, json!(10), None)
        .unwrap();
    let ev2 = engine
        .events
        .emit("src", "test.happened", "1", "tr_b", true, json!({"n": 2}))
        .unwrap();
    let r = engine.automations.drain_durable_events().await;
    assert_eq!(r.failed, 1);
    assert_eq!(engine.events.get_checkpoint("auto", "src"), ev2.sequence);
    let dlq = engine.automations.list_dead_letters(false, 10).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].event_id, ev2.event_id);
    assert_eq!(dlq[0].attempts, 3);

    // Redrive: denied for an Agent without authority, succeeds for the Administrator.
    engine
        .state
        .set("instance", "auto", "fail_budget", 1, 1, json!(0), None)
        .unwrap();
    assert!(engine
        .automations
        .redrive_dead_letter(&dlq[0].dlq_id, &agent())
        .await
        .is_err());
    let res = engine
        .automations
        .redrive_dead_letter(&dlq[0].dlq_id, &Principal::administrator())
        .await
        .unwrap();
    assert_eq!(res["action_instance"], "sink");
    assert!(engine.automations.list_dead_letters(false, 10).unwrap().is_empty());
    assert_eq!(sink_executions(&store).len(), 2);
}

#[tokio::test]
async fn missing_grant_fails_fast_and_is_dead_lettered() {
    let store = SqliteStore::open_in_memory().unwrap();
    let engine = engine_on(&store);
    let ev = engine
        .events
        .emit("src", "test.happened", "1", "tr", true, json!({"n": 1}))
        .unwrap();
    let res = engine.automations.dispatch_event(&ev).await;
    assert!(res[0].as_ref().unwrap_err().contains("no Administrator grant"));
    let dlq = engine.automations.list_dead_letters(false, 10).unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].attempts, 1);
}

#[tokio::test]
async fn execution_records_continue_the_event_trace() {
    let store = granted_store();
    let engine = engine_on(&store);
    let ev = engine
        .events
        .emit("src", "test.happened", "1", "tr_trace_42", true, json!({"n": 3}))
        .unwrap();
    engine.automations.drain_durable_events().await;
    let rec = &engine.automations.list_executions(Some("auto"), 10).unwrap()[0];
    assert_eq!(rec.trace_id, "tr_trace_42");
    assert_eq!(rec.trigger_kind, "event");
    assert_eq!(rec.trigger_ref, format!("event:{}", ev.event_id));
    assert_eq!(rec.run_as.as_deref(), Some("auto_sp"));
    assert_eq!(rec.action_executions.len(), 1);
    assert_eq!(rec.action_executions[0], sink_executions(&store)[0].execution_id);
    assert!(engine
        .telemetry
        .recent_spans(50)
        .iter()
        .any(|s| s.name == "automation:auto" && s.trace_id == "tr_trace_42"));
}

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

#[tokio::test]
async fn cron_triggers_apply_catch_up_policies_across_restarts() {
    let store = granted_store();
    let engine = engine_on(&store);
    let fired_for = |out: &[(String, Result<serde_json::Value, String>)], id: &str| {
        out.iter()
            .filter(|(a, _)| a == id)
            .map(|(_, r)| r.clone().unwrap())
            .collect::<Vec<_>>()
    };

    // First sight registers the trigger without firing.
    let out = engine.automations.run_due_schedules(at("2026-01-01T07:30:00Z")).await;
    assert!(out.is_empty());

    // "Restart" three hours later, 30 minutes after the last window: skip vs catch_up.
    let engine = engine_on(&store);
    let out = engine.automations.run_due_schedules(at("2026-01-01T10:30:00Z")).await;
    assert!(fired_for(&out, "cron_skip").is_empty(), "skip policy drops missed windows");
    let catch = fired_for(&out, "cron_catch");
    assert_eq!(catch.len(), 1, "catch_up fires once for all missed windows");
    assert_eq!(catch[0]["missed"], 3);
    assert_eq!(catch[0]["scheduled_for"], "2026-01-01T10:00:00+00:00");

    // An on-time window fires under the default policy, exactly once.
    let out = engine.automations.run_due_schedules(at("2026-01-01T11:00:20Z")).await;
    assert_eq!(fired_for(&out, "cron_skip").len(), 1);
    assert_eq!(fired_for(&out, "cron_catch").len(), 1);
    let out = engine.automations.run_due_schedules(at("2026-01-01T11:00:40Z")).await;
    assert!(out.is_empty());
    let recs = engine.automations.list_executions(Some("cron_skip"), 10).unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].trigger_ref, "cron:0:2026-01-01T11:00:00+00:00");
}

#[tokio::test]
async fn signal_triggers_evaluate_predicates_on_fresh_changes() {
    let store = granted_store();
    let engine = engine_on(&store);
    let sp = engine.automations.resolve_service_principal("auto_sp").unwrap();
    let sig_runs = |out: &[(String, Result<serde_json::Value, String>)]| {
        out.iter().filter(|(a, _)| a == "sig_auto").count()
    };

    // Baseline observation does not fire (default catch-up: skip).
    assert_eq!(sig_runs(&engine.automations.run_due_signal_triggers().await), 0);
    // Change to 1: predicate (value >= 2) rejects.
    engine.state.set("instance", "flag", "flag", 1, 1, json!(1), None).unwrap();
    engine.queries.query("flag", json!({}), &sp, true).await.unwrap();
    assert_eq!(sig_runs(&engine.automations.run_due_signal_triggers().await), 0);
    // Change to 2: predicate holds, automation runs once.
    engine.state.set("instance", "flag", "flag", 1, 1, json!(2), None).unwrap();
    engine.queries.query("flag", json!({}), &sp, true).await.unwrap();
    let out = engine.automations.run_due_signal_triggers().await;
    let run = out.iter().find(|(a, _)| a == "sig_auto").unwrap();
    assert_eq!(run.1.as_ref().unwrap()["seen"], 2);
    // No change: nothing fires again.
    assert_eq!(sig_runs(&engine.automations.run_due_signal_triggers().await), 0);

    // State Signal trigger on the bound dashboard.
    engine.state.set("dashboard", "main", "mode", 1, 1, json!("calm"), None).unwrap();
    let out = engine.automations.run_due_signal_triggers().await;
    let st = out.iter().find(|(a, _)| a == "state_sig").unwrap();
    assert_eq!(st.1.as_ref().unwrap()["mode"], "calm");
}

#[tokio::test]
async fn automation_state_uses_one_owner_and_publishes_signals_and_cannot_emit() {
    let store = granted_store();
    let engine = engine_on(&store);
    let mut state_rx = engine.signals.subscribe_state();
    let out = engine
        .automations
        .trigger_automation("dash_writer", json!({}))
        .await
        .unwrap();
    assert_eq!(out["note"], "hi");
    let rec = store.get_state("dashboard", "main", "note").unwrap().unwrap();
    assert_eq!(rec.value, json!("hi"));
    let sig = state_rx.try_recv().unwrap();
    assert_eq!((sig.scope.as_str(), sig.owner.as_str()), ("dashboard", "main"));

    let unbound = engine
        .automations
        .trigger_automation("unbound_writer", json!({}))
        .await
        .unwrap_err();
    assert!(unbound.contains("no bound dashboard"), "{unbound}");

    let emit = engine
        .automations
        .trigger_automation("emitter", json!({}))
        .await
        .unwrap_err();
    assert!(emit.contains("do not emit Events"), "{emit}");
    assert!(engine.events.replay_since("emitter", 0).unwrap().is_empty());
}

#[tokio::test]
async fn failed_and_resolved_actions_emit_completion_events() {
    let store = granted_store();
    let engine = engine_on(&store);
    let admin = Principal::administrator();
    let rec = engine
        .actions
        .invoke("failing", json!({}), &admin, None, None)
        .await
        .unwrap();
    assert_eq!(rec.status, "failed");
    let evs = engine.events.replay_since("failing", 0).unwrap();
    let done = evs
        .iter()
        .find(|e| e.semantic_type == "dash.action_completed")
        .expect("failed execution must emit a completion Event");
    assert_eq!(done.payload["status"], "failed");

    engine.telemetry.inject_failure(FailureInjectionRule {
        instance_id: "sink".into(),
        mode: "unknown".into(),
        message: "lost".into(),
        remaining: Some(1),
    });
    let unk = engine
        .actions
        .invoke("sink", json!({"n": 1}), &admin, None, None)
        .await
        .unwrap();
    assert_eq!(unk.status, "unknown");
    engine
        .actions
        .resolve_execution(&unk.execution_id, "succeeded", "verified upstream", &admin)
        .await
        .unwrap();
    let evs = engine.events.replay_since("sink", 0).unwrap();
    let resolved = evs
        .iter()
        .find(|e| e.payload["execution_id"] == json!(unk.execution_id))
        .expect("manual resolution must emit a completion Event");
    assert_eq!(resolved.payload["status"], "succeeded");
    assert_eq!(resolved.payload["resolution"], "manual");
}

#[tokio::test]
async fn history_sampling_skips_stale_values() {
    let store = granted_store();
    let engine = engine_on(&store);
    let series = HistorySeriesConfig {
        series_id: "flag_hist".into(),
        instance_id: "flag".into(),
        params: json!({}),
        aggregator: "count".into(),
        value_path: None,
        cadence_secs: 60,
        retention_days: 30,
        service_principal: "auto_sp".into(),
    };
    engine.history.sync_series_configs(std::slice::from_ref(&series)).unwrap();
    assert_eq!(engine.history.sample_series(&series, None).await.unwrap(), 1.0);
    engine.telemetry.inject_failure(FailureInjectionRule {
        instance_id: "flag".into(),
        mode: "error".into(),
        message: "provider down".into(),
        remaining: Some(1),
    });
    let err = engine.history.sample_series(&series, None).await.unwrap_err();
    assert!(err.contains("Stale"), "{err}");
    assert_eq!(engine.history.query_samples("flag_hist", None, 100).unwrap().len(), 1);
}

#[test]
fn bad_triggers_are_rejected_at_declaration_and_graph_validation() {
    let mut sources = BTreeMap::new();
    let unknown_kind = r#"
local dash = require("dash")
return dash.automation { id = "test.bad", handler = "run", triggers = { { kind = "webhook" } } }
"#;
    sources.insert("definition.luau".into(), unknown_kind.as_bytes().to_vec());
    let err = evaluate_declaration(unknown_kind, &sources).unwrap_err().to_string();
    assert!(err.contains("unknown trigger kind"), "{err}");

    let bad_policy = r#"
local dash = require("dash")
return dash.automation { id = "test.bad", handler = "run", triggers = { { kind = "cron", schedule = "* * * * *", catch_up = "always" } } }
"#;
    sources.insert("definition.luau".into(), bad_policy.as_bytes().to_vec());
    assert!(evaluate_declaration(bad_policy, &sources).is_err());

    let mut g = test_graph();
    g.definitions.insert(
        "test.bad_cron".into(),
        load_def(
            r#"
local dash = require("dash")
return dash.automation { id = "test.bad_cron", handler = "run", triggers = { { kind = "cron", schedule = "61 * * * *" } } }
"#,
            CRON_RT,
        ),
    );
    g.instances.insert(
        "bad_cron".into(),
        instance("bad_cron", DefinitionCategory::Automation, "test.bad_cron", &[], json!({}), Some("auto_sp")),
    );
    g.workspace.history.push(HistorySeriesConfig {
        series_id: "ghost".into(),
        instance_id: "flag".into(),
        params: json!({}),
        aggregator: "count".into(),
        value_path: None,
        cadence_secs: 60,
        retention_days: 30,
        service_principal: "nobody".into(),
    });
    let store = SqliteStore::open_in_memory().unwrap();
    let report = validate_configured_graph(&g, &store, false);
    assert!(!report.valid);
    assert!(report
        .issues
        .iter()
        .any(|i| i.code == "INVALID_TRIGGER" && i.target == "bad_cron" && i.message.contains("invalid cron")));
    assert!(report
        .issues
        .iter()
        .any(|i| i.code == "HISTORY_UNKNOWN_SERVICE_PRINCIPAL"));
}
