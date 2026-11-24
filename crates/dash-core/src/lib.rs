//! Core dashboard engine: contracts, authority, query graph, signals, events, actions, automations.

pub mod actions;
pub mod authority;
pub mod automations;
pub mod events;
pub mod history;
pub mod http_broker;
pub mod model;
pub mod query_engine;
pub mod signal;
pub mod state;
pub mod telemetry;

pub use actions::{ActionManager, DryRunReport, WidgetRequestEnvelope};
pub use authority::{
    check_action_authority, check_connector_operation_authority, check_provider_access,
    validate_configured_graph, AuthError, Principal, PrincipalKind, ValidationIssue,
    ValidationReport,
};
pub use authority::{
    audit_denial, audited, check_connector_operation_read_only, check_draft_activation_authority,
    draft_editor_principals, resolve_recorded_principal, DRAFT_EDITOR_AUDIT_OPS,
};
pub use http_broker::{load_network_ceiling, EffectiveNetworkPolicy};
pub use automations::AutomationEngine;
pub use events::{EventBus, EventEnvelope};
pub use history::HistoryManager;
pub use http_broker::{HttpBroker, HttpBrokerError};
pub use model::{
    CapabilityCeiling, ConfiguredGraph, DashboardAdapterPlacement, DashboardDoc,
    HistorySeriesConfig, InstanceConfig, LayoutOverride, LoadedDefinition, NetworkPolicy,
    ServicePrincipalRequested, WorkspaceConfig,
};
pub use query_engine::QueryEngine;
pub use signal::{
    apply_delta_envelope, compute_keyed_delta, DeltaOp, EnvelopeKind, QueryEnvelope, QueryStatus,
    SignalHub, StateSignalPayload,
};
pub use state::StateManager;
pub use telemetry::{
    ExecutionGraphEdge, ExecutionGraphNode, ExecutionGraphSnapshot, FailureInjectionRule,
    RuntimeMetricsSnapshot, SanitizedLogEntry, Telemetry,
};

use arc_swap::ArcSwap;
use dash_luau::{ResourceBudget, VmPool};
use dash_store::SqliteStore;
use std::sync::Arc;

/// Unified runtime engine bundle hosting reactive query execution, actions, automations, and history.
#[derive(Clone)]
pub struct RuntimeEngine {
    pub graph: Arc<ArcSwap<ConfiguredGraph>>,
    pub store: SqliteStore,
    pub pool: VmPool,
    pub http: HttpBroker,
    pub signals: SignalHub,
    pub events: EventBus,
    pub state: StateManager,
    pub telemetry: Telemetry,
    pub queries: QueryEngine,
    pub actions: ActionManager,
    pub automations: AutomationEngine,
    pub history: HistoryManager,
}

impl RuntimeEngine {
    pub fn new(
        initial_graph: ConfiguredGraph,
        store: SqliteStore,
        budget: ResourceBudget,
    ) -> Result<Self, HttpBrokerError> {
        let graph = Arc::new(ArcSwap::from_pointee(initial_graph));
        let pool = VmPool::new(budget);
        let http = HttpBroker::new(store.clone())?;
        let signals = SignalHub::new(256);
        let events = EventBus::new(store.clone(), 512);
        let state = StateManager::new(store.clone(), signals.clone());
        let telemetry = Telemetry::new(store.clone());
        // Per-Instance Luau budgets from the live graph's `[resources]` (§2.1), and VM-level
        // trace spans for startup, memory, interrupts, queue and execution time (§15.1).
        {
            let g = Arc::clone(&graph);
            pool.set_budget_resolver(Arc::new(move |inst_id: &str| {
                g.load().workspace.resources.overrides_for(inst_id)
            }));
            let tel = telemetry.clone();
            pool.set_execution_observer(Arc::new(move |s: &dash_luau::ExecutionStats| {
                tel.record_span(
                    &s.trace_id,
                    None,
                    &format!("luau_vm:{}:{}", s.instance_id, s.handler),
                    "system",
                    serde_json::to_value(s).unwrap_or_default(),
                    if s.outcome == "ok" { "ok" } else { "error" },
                    (s.queue_ms + s.exec_ms) as i64,
                );
            }));
        }
        let queries = QueryEngine::new(
            Arc::clone(&graph),
            pool.clone(),
            store.clone(),
            http.clone(),
            signals.clone(),
            events.clone(),
            state.clone(),
            telemetry.clone(),
        );
        let actions = ActionManager::new(
            Arc::clone(&graph),
            pool.clone(),
            store.clone(),
            queries.clone(),
            events.clone(),
            telemetry.clone(),
        );
        let automations = AutomationEngine::new(
            Arc::clone(&graph),
            pool.clone(),
            store.clone(),
            actions.clone(),
            queries.clone(),
            events.clone(),
            telemetry.clone(),
        );
        let history = HistoryManager::new(store.clone(), queries.clone());

        Ok(Self {
            graph,
            store,
            pool,
            http,
            signals,
            events,
            state,
            telemetry,
            queries,
            actions,
            automations,
            history,
        })
    }

    pub fn metrics_snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            active_vms: self.pool.active_vm_count(),
            reclaimed_vms: self.pool.total_reclaimed(),
            cache_entries: self.queries.cache_len(),
            query_cache_hits: self.telemetry.query_hits.load(std::sync::atomic::Ordering::Relaxed),
            query_cache_misses: self.telemetry.query_misses.load(std::sync::atomic::Ordering::Relaxed),
            active_subscribers: self.signals.total_subscribers(),
            pending_events: 0,
            action_executions_total: self
                .telemetry
                .actions_total
                .load(std::sync::atomic::Ordering::Relaxed),
            sqlite_bytes: self.store.db_size_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Json, Router};
    use dash_luau::{evaluate_declaration, DefinitionCategory};
    use dash_store::ActionExecutionRecord;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    fn load_def(pkg: &str, decl: &str, runtime: &str) -> LoadedDefinition {
        let mut sources = BTreeMap::new();
        sources.insert("definition.luau".into(), decl.as_bytes().to_vec());
        sources.insert("runtime.luau".into(), runtime.as_bytes().to_vec());
        let manifest = evaluate_declaration(decl, &sources).unwrap();
        LoadedDefinition {
            package_name: pkg.into(),
            manifest,
            definition_source: decl.into(),
            runtime_source: runtime.into(),
            tests_source: None,
            fixtures: BTreeMap::new(),
            widget_assets: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn test_query_dedup_partitioning_dynamic_deps_deltas_and_stale() {
        let mut graph = ConfiguredGraph::default();
        graph.generation_id = 1;

        let upstream_a = load_def(
            "test",
            r#"
local dash = require("dash")
return dash.data_provider { id = "test.upstream_a", handler = "query" }
"#,
            r#"
local R = {}
function R.query(ctx) return { source = "a", v = 1 } end
return R
"#,
        );
        let upstream_b = load_def(
            "test",
            r#"
local dash = require("dash")
return dash.data_provider { id = "test.upstream_b", handler = "query" }
"#,
            r#"
local R = {}
function R.query(ctx) return { source = "b", v = 2 } end
return R
"#,
        );
        let dynamic_router = load_def(
            "test",
            r#"
local dash = require("dash")
return dash.data_provider {
    id = "test.router",
    handler = "query",
    inputs = {
        a = dash.data_ref("test.upstream_a"),
        b = dash.data_ref("test.upstream_b"),
    },
}
"#,
            r#"
local R = {}
function R.query(ctx)
    if ctx.query.branch == "a" then
        return ctx:await(ctx.inputs.a:query({}))
    else
        return ctx:await(ctx.inputs.b:query({}))
    end
end
return R
"#,
        );
        let slow_counter = load_def(
            "test",
            r#"
local dash = require("dash")
return dash.data_provider { id = "test.slow_once", handler = "query" }
"#,
            r#"
local calls = 0
local R = {}
function R.query(ctx)
    calls = calls + 1
    return { calls = calls, repo = ctx.query.repo }
end
return R
"#,
        );

        graph.definitions.insert("test.upstream_a".into(), upstream_a);
        graph.definitions.insert("test.upstream_b".into(), upstream_b);
        graph.definitions.insert("test.router".into(), dynamic_router);
        graph.definitions.insert("test.slow_once".into(), slow_counter);

        graph.instances.insert(
            "inst_a".into(),
            InstanceConfig {
                id: "inst_a".into(),
                category: DefinitionCategory::DataProvider,
                definition: "test.upstream_a".into(),
                inputs: BTreeMap::new(),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        );
        graph.instances.insert(
            "inst_b".into(),
            InstanceConfig {
                id: "inst_b".into(),
                category: DefinitionCategory::DataProvider,
                definition: "test.upstream_b".into(),
                inputs: BTreeMap::new(),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        );
        graph.instances.insert(
            "inst_router".into(),
            InstanceConfig {
                id: "inst_router".into(),
                category: DefinitionCategory::DataProvider,
                definition: "test.router".into(),
                inputs: BTreeMap::from([
                    ("a".into(), "inst_a".into()),
                    ("b".into(), "inst_b".into()),
                ]),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        );
        graph.instances.insert(
            "inst_dedup".into(),
            InstanceConfig {
                id: "inst_dedup".into(),
                category: DefinitionCategory::DataProvider,
                definition: "test.slow_once".into(),
                inputs: BTreeMap::new(),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        );

        let store = SqliteStore::open_in_memory().unwrap();
        let engine = RuntimeEngine::new(graph, store, ResourceBudget::default()).unwrap();
        let admin = Principal::administrator();

        // 1. 100 identical concurrent authorized queries produce 1 provider computation (§15.3)
        let mut futs = Vec::new();
        for _ in 0..100 {
            let q = engine.queries.clone();
            let p = admin.clone();
            futs.push(tokio::spawn(async move {
                q.query("inst_dedup", json!({"repo": "acme/a"}), &p, false)
                    .await
                    .unwrap()
            }));
        }
        let results = futures_util::future::join_all(futs).await;
        for r in results {
            let env = r.unwrap();
            assert_eq!(env.value.unwrap()["calls"], 1);
        }

        // 2. Different parameter sets remain independent
        let other = engine
            .queries
            .query("inst_dedup", json!({"repo": "acme/b"}), &admin, false)
            .await
            .unwrap();
        assert_eq!(other.value.unwrap()["repo"], "acme/b");

        // 3. Cross-authorization cache partitioning: agent gets separate cache key
        let agent = Principal {
            id: "agt_1".into(),
            kind: PrincipalKind::Agent,
            ceiling: CapabilityCeiling::read_only_agent(),
        };
        let k_admin = engine
            .queries
            .compute_query_key("inst_dedup", "d1", &json!({"repo": "acme/a"}), &admin);
        let k_agent = engine
            .queries
            .compute_query_key("inst_dedup", "d1", &json!({"repo": "acme/a"}), &agent);
        assert_ne!(k_admin, k_agent);

        // 4. Dynamic dependency cleanup: route a -> route b removes edge to inst_a!
        let r_a = engine
            .queries
            .query("inst_router", json!({"branch": "a"}), &admin, false)
            .await
            .unwrap();
        assert_eq!(r_a.value.unwrap()["source"], "a");
        let router_def_digest = engine.graph.load().definitions["test.router"]
            .manifest
            .identity_version
            .clone();
        let r_a_qk = engine.queries.compute_query_key(
            "inst_router",
            &router_def_digest,
            &json!({"branch": "a"}),
            &admin,
        );
        let a_digest = engine.graph.load().definitions["test.upstream_a"]
            .manifest
            .identity_version
            .clone();
        let b_digest = engine.graph.load().definitions["test.upstream_b"]
            .manifest
            .identity_version
            .clone();
        let qk_a = engine
            .queries
            .compute_query_key("inst_a", &a_digest, &json!({}), &admin);
        let qk_b = engine
            .queries
            .compute_query_key("inst_b", &b_digest, &json!({}), &admin);
        assert!(engine.queries.reverse_subscribers(&qk_a).contains(&r_a_qk));

        // 5. Keyed delta + gap resync
        let push_def = load_def(
            "test",
            r#"local dash = require("dash"); return dash.data_provider { id = "test.list" }"#,
            r#"local R = {}; function R.query(ctx) return { { id = "1", state = "open" } } end; return R"#,
        );
        let mut g2 = (*engine.graph.load_full()).clone();
        g2.definitions.insert("test.list".into(), push_def);
        g2.instances.insert(
            "inst_list".into(),
            InstanceConfig {
                id: "inst_list".into(),
                category: DefinitionCategory::DataProvider,
                definition: "test.list".into(),
                inputs: BTreeMap::new(),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        );
        engine.graph.store(Arc::new(g2));

        let init = engine
            .queries
            .query("inst_list", json!({}), &admin, false)
            .await
            .unwrap();
        let mut stream = engine.signals.subscribe_stream(&init.query_key);
        let mut local_ver = init.version;
        let mut local_rows = init.value.clone().unwrap().as_array().unwrap().clone();

        engine
            .queries
            .push_query_value(
                "inst_list",
                &json!({}),
                &admin,
                json!([{"id": "1", "state": "merged"}, {"id": "2", "state": "open"}]),
            )
            .unwrap();

        let delta_env = stream.recv().await.unwrap();
        assert_eq!(delta_env.kind, EnvelopeKind::Delta);
        apply_delta_envelope(&mut local_ver, &mut local_rows, &delta_env, "id").unwrap();
        assert_eq!(local_rows.len(), 2);

        // Simulate sequence gap -> triggers resync error
        let mut bad_ver = 0;
        let gap_err = apply_delta_envelope(&mut bad_ver, &mut local_rows, &delta_env, "id").unwrap_err();
        assert!(gap_err.contains("snapshot resync required"));

        // 6. Failure retains stale data!
        engine.telemetry.inject_failure(FailureInjectionRule {
            instance_id: "inst_list".into(),
            mode: "error".into(),
            message: "upstream 503".into(),
            remaining: Some(1),
        });
        let stale = engine
            .queries
            .query("inst_list", json!({}), &admin, true)
            .await
            .unwrap();
        assert_eq!(stale.status, QueryStatus::Stale);
        assert_eq!(stale.value.unwrap().as_array().unwrap().len(), 2);
        let _ = qk_b;
    }

    #[tokio::test]
    async fn test_http_broker_ssrf_and_secret_redaction() {
        // Local mock server capturing Authorization header
        let app = Router::new().route(
            "/repos",
            get(|headers: axum::http::HeaderMap| async move {
                let auth = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                Json(json!({ "ok": true, "auth_seen": auth }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let store = SqliteStore::open_in_memory().unwrap();
        let secret_pat = "ghp_secret_abc123456";
        store
            .set_secret("github-work", "GitHub PAT", secret_pat, &["gh_conn".into()])
            .unwrap();
        let broker = HttpBroker::new(store.clone()).unwrap();

        let conn_inst = InstanceConfig {
            id: "gh_conn".into(),
            category: DefinitionCategory::Connector,
            definition: "github.connector".into(),
            inputs: BTreeMap::new(),
            config: json!({}),
            secrets: BTreeMap::from([("auth".into(), "secret://github-work".into())]),
            capabilities: vec!["pull_requests.read".into()],
            run_as: None,
            enabled: None,
        };
        let mut graph = ConfiguredGraph::default();
        // Network policy is deny-by-default: the Workspace requests hosts and the Administrator
        // ceiling (SQLite, outside Git) must cover them.
        graph.workspace.network.allowed_hosts = vec!["127.0.0.1".into()];
        graph.workspace.network.allowed_methods = vec!["GET".into()];
        store
            .set_network_policy_ceiling(
                json!({
                    "allowed_hosts": ["127.0.0.1"],
                    "allowed_methods": ["GET"],
                    "allow_loopback_for_tests": true
                }),
                "admin",
            )
            .unwrap();
        // Default policy blocks loopback SSRF!
        let admin = Principal::administrator();
        let blocked = broker
            .execute(
                &admin,
                &conn_inst,
                None,
                &graph,
                dash_luau::HttpHostRequest {
                    method: "GET".into(),
                    url: format!("http://{addr}/repos"),
                    headers: BTreeMap::new(),
                    body: None,
                    credential: Some("auth".into()),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(blocked, HttpBrokerError::SsrfBlocked(_)));

        // Enable test loopback opt-in -> broker injects secret and redacts any echoed secret in response!
        graph.workspace.network.allow_loopback_for_tests = true;
        let resp = broker
            .execute(
                &admin,
                &conn_inst,
                None,
                &graph,
                dash_luau::HttpHostRequest {
                    method: "GET".into(),
                    url: format!("http://{addr}/repos"),
                    headers: BTreeMap::new(),
                    body: None,
                    credential: Some("auth".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body["ok"], true);
        // Secret echoed back by upstream must be redacted before reaching Luau!
        assert!(!resp.body["auth_seen"].as_str().unwrap().contains(secret_pat));
        assert!(resp.body["auth_seen"].as_str().unwrap().contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn test_actions_nested_permissions_unknown_reconcile_and_automation_replay() {
        let gh_conn = load_def(
            "github",
            r#"
local dash = require("dash")
return dash.connector {
    id = "github.connector",
    capabilities = { "pull_requests.read", "pull_requests.merge" },
    operations = {
        get_pr = { required_capabilities = { "pull_requests.read" } },
        merge_pr = { required_capabilities = { "pull_requests.merge" } },
    },
}
"#,
            r#"
local state_merged = false
local R = {}
function R.call(ctx)
    if ctx.operation == "get_pr" then
        return { number = ctx.args.number, merged = state_merged }
    elseif ctx.operation == "merge_pr" then
        state_merged = true
        return { merged = true, sha = "abc123" }
    end
end
return R
"#,
        );
        let merge_act = load_def(
            "github",
            r#"
local dash = require("dash")
return dash.action_provider {
    id = "github.merge_pr",
    handler = "execute",
    dry_run = "dry_run",
    reconcile = "reconcile",
    inputs = {
        github = dash.connector_ref("github.connector", {
            required_capabilities = { "pull_requests.merge" },
        }),
    },
    emits = { "github.pr_merged" },
}
"#,
            r#"
local R = {}
function R.dry_run(ctx)
    return { feasible = true, effects = { will_merge = ctx.args.number } }
end
function R.execute(ctx)
    local res = ctx:await(ctx.inputs.github:call("merge_pr", { number = ctx.args.number }))
    ctx:await(ctx:emit("github.pr_merged", { number = ctx.args.number, key = "DEV-123" }, true))
    return res
end
function R.reconcile(ctx)
    local pr = ctx:await(ctx.inputs.github:call("get_pr", { number = ctx.args.number }))
    if pr.merged then
        return { status = "succeeded", pr = pr }
    end
    return { status = "unknown" }
end
return R
"#,
        );
        let transition_act = load_def(
            "jira",
            r#"
local dash = require("dash")
return dash.action_provider {
    id = "jira.transition_issue",
    handler = "execute",
}
"#,
            r#"
local count = 0
local R = {}
function R.execute(ctx)
    count = count + 1
    return { key = ctx.args.key, status = "Done", executions = count }
end
return R
"#,
        );
        let complete_auto = load_def(
            "engineering",
            r#"
local dash = require("dash")
return dash.automation {
    id = "engineering.complete_work",
    handler = "run",
    inputs = {
        merge = dash.action_ref("github.merge_pr"),
        transition = dash.action_ref("jira.transition_issue"),
    },
    triggers = {
        { kind = "event", event_type = "github.pr_merged", source_input = "merge" },
    },
}
"#,
            r#"
local R = {}
function R.run(ctx)
    local key = ctx.event.payload.key
    return ctx:await(ctx.inputs.transition:invoke({ key = key }, "jira-done-" .. ctx.event.event_id))
end
return R
"#,
        );

        let mut graph = ConfiguredGraph::default();
        graph.generation_id = 1;
        graph.definitions.insert("github.connector".into(), gh_conn);
        graph.definitions.insert("github.merge_pr".into(), merge_act);
        graph.definitions.insert("jira.transition_issue".into(), transition_act);
        graph.definitions.insert("engineering.complete_work".into(), complete_auto);

        graph.instances.insert(
            "github_reader".into(),
            InstanceConfig {
                id: "github_reader".into(),
                category: DefinitionCategory::Connector,
                definition: "github.connector".into(),
                inputs: BTreeMap::new(),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec!["pull_requests.read".into()],
                run_as: None,
                enabled: None,
            },
        );
        graph.instances.insert(
            "github_maintainer".into(),
            InstanceConfig {
                id: "github_maintainer".into(),
                category: DefinitionCategory::Connector,
                definition: "github.connector".into(),
                inputs: BTreeMap::new(),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec!["pull_requests.read".into(), "pull_requests.merge".into()],
                run_as: None,
                enabled: None,
            },
        );
        graph.instances.insert(
            "merge_with_reader".into(),
            InstanceConfig {
                id: "merge_with_reader".into(),
                category: DefinitionCategory::ActionProvider,
                definition: "github.merge_pr".into(),
                inputs: BTreeMap::from([("github".into(), "github_reader".into())]),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        );
        graph.instances.insert(
            "merge_pr".into(),
            InstanceConfig {
                id: "merge_pr".into(),
                category: DefinitionCategory::ActionProvider,
                definition: "github.merge_pr".into(),
                inputs: BTreeMap::from([("github".into(), "github_maintainer".into())]),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        );
        graph.instances.insert(
            "transition_issue".into(),
            InstanceConfig {
                id: "transition_issue".into(),
                category: DefinitionCategory::ActionProvider,
                definition: "jira.transition_issue".into(),
                inputs: BTreeMap::new(),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        );
        graph.service_principals.insert(
            "work_completion".into(),
            ServicePrincipalRequested {
                id: "work_completion".into(),
                description: "Transition Jira on PR merge".into(),
                ceiling: CapabilityCeiling {
                    allowed_actions: BTreeSet::from(["transition_issue".into()]),
                    ..Default::default()
                },
            },
        );
        graph.instances.insert(
            "complete_work".into(),
            InstanceConfig {
                id: "complete_work".into(),
                category: DefinitionCategory::Automation,
                definition: "engineering.complete_work".into(),
                inputs: BTreeMap::from([
                    ("merge".into(), "merge_pr".into()),
                    ("transition".into(), "transition_issue".into()),
                ]),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: Some("work_completion".into()),
                enabled: None,
            },
        );

        let store = SqliteStore::open_in_memory().unwrap();
        let engine = RuntimeEngine::new(graph, store.clone(), ResourceBudget::default()).unwrap();
        let admin = Principal::administrator();

        // 1. Reader-bound merge action MUST fail capability check
        let reader_err = engine
            .actions
            .invoke("merge_with_reader", json!({"number": 42}), &admin, None, None)
            .await
            .unwrap_err();
        assert!(reader_err.contains("lacks required capability"));

        // 2. Dry run succeeds without side effects
        let dry = engine
            .actions
            .dry_run("merge_pr", json!({"number": 42}), &admin)
            .await
            .unwrap();
        assert!(dry.feasible);
        assert!(dry.advisory);

        // 3. Unknown outcome + manual resolution
        engine.telemetry.inject_failure(FailureInjectionRule {
            instance_id: "merge_pr".into(),
            mode: "unknown".into(),
            message: "upstream_connection_lost_after_request".into(),
            remaining: Some(1),
        });
        let unk = engine
            .actions
            .invoke("merge_pr", json!({"number": 42}), &admin, None, None)
            .await
            .unwrap();
        // Reconcile ran (pr wasn't merged yet in connector VM), so still unknown
        assert_eq!(unk.status, "unknown");
        assert!(!unk.retry_safe);

        let resolved = engine
            .actions
            .resolve_execution(&unk.execution_id, "failed", "verified PR not merged", &admin)
            .await
            .unwrap();
        assert_eq!(resolved.status, "failed");

        // 4. Real merge succeeds and emits durable event
        let ok = engine
            .actions
            .invoke("merge_pr", json!({"number": 42}), &admin, None, None)
            .await
            .unwrap();
        assert_eq!(ok.status, "succeeded");

        // 5. Automation BEFORE admin grant is denied!
        let replay_evts = engine.events.replay_since("merge_pr", 0).unwrap();
        let merged_evt = replay_evts
            .iter()
            .find(|e| e.semantic_type == "github.pr_merged")
            .expect("expected github.pr_merged event")
            .clone();
        assert!(replay_evts
            .iter()
            .any(|e| e.semantic_type == "dash.action_completed"));
        let pre_grant = engine.automations.dispatch_event(&merged_evt).await;
        assert!(pre_grant[0].as_ref().unwrap_err().contains("no Administrator grant"));

        // 6. Grant Service principal ceiling in SQLite -> dispatch succeeds, and replay does NOT duplicate!
        store
            .set_service_principal_grant(
                "work_completion",
                serde_json::to_value(CapabilityCeiling {
                    allowed_actions: BTreeSet::from(["transition_issue".into()]),
                    ..Default::default()
                })
                .unwrap(),
                "admin",
            )
            .unwrap();

        let r1 = engine.automations.dispatch_event(&merged_evt).await;
        let exec1: ActionExecutionRecord =
            serde_json::from_value(r1[0].as_ref().unwrap().clone()).unwrap();
        assert_eq!(exec1.result.as_ref().unwrap()["executions"], 1);

        // Replay the same durable event -> idempotency key returns identical execution without re-running!
        let r2 = engine.automations.dispatch_event(&merged_evt).await;
        let exec2: ActionExecutionRecord =
            serde_json::from_value(r2[0].as_ref().unwrap().clone()).unwrap();
        assert_eq!(exec1.execution_id, exec2.execution_id);
        assert_eq!(exec2.result.as_ref().unwrap()["executions"], 1);

        // 7. Unknown outcome after actual external merge -> reconcile automatically resolves to `succeeded`!
        engine.telemetry.inject_failure(FailureInjectionRule {
            instance_id: "merge_pr".into(),
            mode: "unknown".into(),
            message: "lost_response_after_merge".into(),
            remaining: Some(1),
        });
        let rec_ok = engine
            .actions
            .invoke("merge_pr", json!({"number": 42}), &admin, None, None)
            .await
            .unwrap();
        assert_eq!(rec_ok.status, "succeeded");
        assert!(rec_ok.reconcile_attempts >= 1);
    }

    #[tokio::test]
    async fn test_dynamic_dependency_edge_cleanup_and_history_series() {
        let mut graph = ConfiguredGraph::default();
        graph.generation_id = 1;

        let switch_def = load_def(
            "test",
            r#"local dash = require("dash"); return dash.data_provider { id = "test.switch" }"#,
            r#"
local R = {}
function R.query(ctx)
    return ctx.state:get("target") or "a"
end
return R
"#,
        );
        let a_def = load_def(
            "test",
            r#"local dash = require("dash"); return dash.data_provider { id = "test.a" }"#,
            r#"local R = {}; function R.query(ctx) return { 1, 2 } end; return R"#,
        );
        let b_def = load_def(
            "test",
            r#"local dash = require("dash"); return dash.data_provider { id = "test.b" }"#,
            r#"local R = {}; function R.query(ctx) return { 10, 20, 30 } end; return R"#,
        );
        let router_def = load_def(
            "test",
            r#"
local dash = require("dash")
return dash.data_provider {
    id = "test.dyn_router",
    inputs = {
        sw = dash.data_ref("test.switch"),
        a = dash.data_ref("test.a"),
        b = dash.data_ref("test.b"),
    },
}
"#,
            r#"
local R = {}
function R.query(ctx)
    local which = ctx:await(ctx.inputs.sw:query({}))
    if which == "a" then
        return ctx:await(ctx.inputs.a:query({}))
    else
        return ctx:await(ctx.inputs.b:query({}))
    end
end
return R
"#,
        );

        graph.definitions.insert("test.switch".into(), switch_def);
        graph.definitions.insert("test.a".into(), a_def);
        graph.definitions.insert("test.b".into(), b_def);
        graph.definitions.insert("test.dyn_router".into(), router_def);

        for (id, def_id, inputs) in [
            ("inst_sw", "test.switch", BTreeMap::new()),
            ("inst_a", "test.a", BTreeMap::new()),
            ("inst_b", "test.b", BTreeMap::new()),
            (
                "inst_router",
                "test.dyn_router",
                BTreeMap::from([
                    ("sw".into(), "inst_sw".into()),
                    ("a".into(), "inst_a".into()),
                    ("b".into(), "inst_b".into()),
                ]),
            ),
        ] {
            graph.instances.insert(
                id.into(),
                InstanceConfig {
                    id: id.into(),
                    category: DefinitionCategory::DataProvider,
                    definition: def_id.into(),
                    inputs,
                    config: json!({}),
                    secrets: BTreeMap::new(),
                    capabilities: vec![],
                    run_as: None,
                    enabled: None,
                },
            );
        }
        graph.instances.insert(
            "trend_history".into(),
            InstanceConfig {
                id: "trend_history".into(),
                category: DefinitionCategory::DataProvider,
                definition: "dash.history_series".into(),
                inputs: BTreeMap::new(),
                config: json!({ "series_id": "open_prs_count" }),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        );

        let store = SqliteStore::open_in_memory().unwrap();
        let engine = RuntimeEngine::new(graph, store.clone(), ResourceBudget::default()).unwrap();
        let admin = Principal::administrator();

        let router_dig = engine.graph.load().definitions["test.dyn_router"]
            .manifest
            .identity_version
            .clone();
        let a_dig = engine.graph.load().definitions["test.a"]
            .manifest
            .identity_version
            .clone();
        let b_dig = engine.graph.load().definitions["test.b"]
            .manifest
            .identity_version
            .clone();

        let router_qk = engine
            .queries
            .compute_query_key("inst_router", &router_dig, &json!({}), &admin);
        let qk_a = engine
            .queries
            .compute_query_key("inst_a", &a_dig, &json!({}), &admin);
        let qk_b = engine
            .queries
            .compute_query_key("inst_b", &b_dig, &json!({}), &admin);

        let res1 = engine
            .queries
            .query("inst_router", json!({}), &admin, false)
            .await
            .unwrap();
        assert_eq!(res1.value.unwrap().as_array().unwrap().len(), 2);
        assert!(engine.queries.reverse_subscribers(&qk_a).contains(&router_qk));
        assert!(!engine.queries.reverse_subscribers(&qk_b).contains(&router_qk));

        // Flip instance state of inst_sw -> "b" and re-run SAME router query_key
        store
            .set_state("instance", "inst_sw", "target", 1, 1, json!("b"), None)
            .unwrap();
        let _ = engine
            .queries
            .query("inst_sw", json!({}), &admin, true)
            .await
            .unwrap();
        let res2 = engine
            .queries
            .query("inst_router", json!({}), &admin, true)
            .await
            .unwrap();
        assert_eq!(res2.value.unwrap().as_array().unwrap().len(), 3);
        // Old edge to inst_a MUST be unsubscribed (no leaked subscription!)
        assert!(!engine.queries.reverse_subscribers(&qk_a).contains(&router_qk));
        assert!(engine.queries.reverse_subscribers(&qk_b).contains(&router_qk));

        // History sampling under Service principal
        let series_cfg = HistorySeriesConfig {
            series_id: "open_prs_count".into(),
            instance_id: "inst_b".into(),
            params: json!({}),
            aggregator: "count".into(),
            value_path: None,
            cadence_secs: 3600,
            retention_days: 30,
            service_principal: "pr_history_sampler".into(),
        };
        engine.history.sync_series_configs(&[series_cfg.clone()]).unwrap();
        // Without Admin grant -> denied
        assert!(engine
            .history
            .sample_series(&series_cfg, None)
            .await
            .is_err());

        // Grant Service principal read access to inst_b -> succeeds and queryable via dash.history_series!
        store
            .set_service_principal_grant(
                "pr_history_sampler",
                serde_json::to_value(CapabilityCeiling {
                    allowed_providers: BTreeSet::from(["inst_b".into()]),
                    ..Default::default()
                })
                .unwrap(),
                "admin",
            )
            .unwrap();
        let sampled = engine.history.sample_series(&series_cfg, None).await.unwrap();
        assert_eq!(sampled, 3.0);

        let hist_env = engine
            .queries
            .query("trend_history", json!({}), &admin, false)
            .await
            .unwrap();
        let samples = hist_env.value.unwrap();
        assert_eq!(samples.as_array().unwrap().len(), 1);
        assert_eq!(samples[0]["value_num"], 3.0);
    }
}
