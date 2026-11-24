//! Isolated Preview runtime, Definition test execution, and declarative YAML Scenario test runner (§4.5, §10.3).

use crate::git_cli::GitError;
use crate::workspace::load_workspace_graph_async as load_workspace_graph;
use async_trait::async_trait;
use dash_core::{
    validate_configured_graph, ConfiguredGraph, FailureInjectionRule, LoadedDefinition, Principal,
    RuntimeEngine, ValidationIssue, ValidationReport, WidgetRequestEnvelope,
};
use dash_luau::{
    run_definition_tests, CompiledInstanceBundle, DefinitionCategory, DefinitionTestReport,
    ExecutionContextConfig, HostBridge, HttpHostRequest, HttpHostResponse, InputRefSpec,
    ResourceBudget, VmPool,
};
use dash_store::{ActionExecutionRecord, SqliteStore};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioSuiteFile {
    pub name: String,
    #[serde(default)]
    pub scenarios: Vec<ScenarioSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioSpec {
    pub name: String,
    #[serde(default)]
    pub steps: Vec<ScenarioStep>,
}

/// One Scenario step. Exactly one operation key must be set; `assert` may accompany it.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ScenarioStep {
    #[serde(default)]
    pub query: Option<QueryStep>,
    #[serde(default)]
    pub dry_run: Option<DryRunStep>,
    #[serde(default)]
    pub invoke_action: Option<InvokeActionStep>,
    #[serde(default)]
    pub send_request: Option<SendRequestStep>,
    #[serde(default)]
    pub set_dashboard_state: Option<SetDashboardStateStep>,
    #[serde(default)]
    pub render_dashboard: Option<RenderDashboardStep>,
    #[serde(default)]
    pub process_events: Option<ProcessEventsStep>,
    #[serde(default)]
    pub emit_event: Option<EmitEventStep>,
    #[serde(default)]
    pub push_update: Option<PushUpdateStep>,
    #[serde(default)]
    pub inject_failure: Option<InjectFailureStep>,
    #[serde(default)]
    pub clear_failure: Option<ClearFailureStep>,
    #[serde(default)]
    pub advance_time: Option<AdvanceTimeStep>,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryStep {
    pub instance: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub force_refresh: Option<bool>,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DryRunStep {
    pub action: String,
    #[serde(default)]
    pub args: serde_json::Value,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvokeActionStep {
    pub action: String,
    #[serde(default)]
    pub args: serde_json::Value,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendRequestStep {
    pub dashboard: String,
    pub adapter: String,
    pub widget_id: String,
    pub request: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetDashboardStateStep {
    pub dashboard: String,
    pub key: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderDashboardStep {
    pub dashboard: String,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessEventsStep {
    pub source: String,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

/// Inject a simulated Event into the Preview's isolated Event store (§4.5, §10.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmitEventStep {
    /// Source Instance id the Event is attributed to.
    pub source: String,
    /// Semantic Event type (e.g. `github.pr_merged`).
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default = "default_true")]
    pub durable: bool,
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
}

/// Push a value for a cached Query (push / keyed-delta update path, §6.2, §10.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushUpdateStep {
    pub instance: String,
    #[serde(default)]
    pub params: serde_json::Value,
    pub value: serde_json::Value,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

fn default_true() -> bool {
    true
}

fn default_schema_version() -> String {
    "1".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InjectFailureStep {
    pub instance: String,
    pub mode: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub remaining: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClearFailureStep {
    pub instance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvanceTimeStep {
    pub seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StepAssertion {
    #[serde(default)]
    pub length: Option<usize>,
    #[serde(default)]
    pub contains: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub feasible: Option<bool>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub transitions: Option<usize>,
    #[serde(default)]
    pub contains_text: Option<String>,
    /// Structural Widget-node assertions on a rendered Dashboard.
    #[serde(default)]
    pub widget: Option<Vec<WidgetAssertion>>,
    /// Expected envelope kind of a pushed update (`snapshot` | `delta`).
    #[serde(default)]
    pub envelope_kind: Option<String>,
}

/// Assert that a rendered Widget node with `widget_id` exists and matches the given fields.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct WidgetAssertion {
    pub widget_id: String,
    #[serde(default)]
    pub widget_type: Option<String>,
    /// JSON subset that the node's `props` must contain.
    #[serde(default)]
    pub props: Option<serde_json::Value>,
    /// Effective visibility after Dashboard layout overrides (defaults to visible).
    #[serde(default)]
    pub visible: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioResult {
    pub suite: String,
    pub scenario: String,
    pub passed: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreviewTestReport {
    pub validation: ValidationReport,
    pub definition_tests: Vec<DefinitionTestReport>,
    pub scenario_results: Vec<ScenarioResult>,
    pub preview_renders: BTreeMap<String, serde_json::Value>,
    /// Dashboards that failed to render in Preview (dashboard id -> error). Any entry fails Preview.
    #[serde(default)]
    pub preview_render_errors: BTreeMap<String, String>,
    pub all_passed: bool,
    /// Git tree id of the exact draft content this report was computed for. Activation only
    /// trusts a cached report whose `tested_tree` equals the draft's current content.
    #[serde(default)]
    pub tested_tree: Option<String>,
}

/// Maximum nesting of Mock-backed input resolution inside one Definition test (cycle guard).
const MAX_TEST_INPUT_DEPTH: usize = 16;

/// An Event captured by [`MockTestBridge`] instead of being delivered (Definition tests never
/// touch an Event log).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapturedTestEvent {
    pub source_definition: String,
    pub event_type: String,
    pub schema_version: String,
    pub durable: bool,
    pub payload: serde_json::Value,
}

struct MockTestShared {
    graph: Arc<ConfiguredGraph>,
    pool: VmPool,
    mock_kv: Mutex<HashMap<String, serde_json::Value>>,
    state: Mutex<HashMap<(String, String, String), serde_json::Value>>,
    events: Mutex<Vec<CapturedTestEvent>>,
}

/// HostBridge for Definition tests (§4.5 "run against Mocks").
///
/// Every input is resolved from the Definition's *contract* (its declared input refs), never from
/// live Instances:
/// - **connector inputs** (`ctx.inputs.x:call(op, args)`) run the Mock declared `for` the
///   referenced connector (`ConfiguredGraph::find_mock_for`), serving its fixture files and
///   keeping Mock state in memory. The operation must be declared by the connector and its
///   required capabilities must be covered by the input's declared `required_capabilities`.
/// - **data inputs** (`:query(params)`) run the referenced data provider, whose own inputs are
///   again resolved against Mocks; its output is checked against its output contract.
/// - **action inputs** (`:invoke(args)`) run the referenced action's handler against Mocks and
///   return an execution-record-shaped value.
///
/// `ctx.http` is unavailable (Definition tests never reach real external APIs); emitted Events
/// are captured, and Instance State lives in memory. All per-test state (Mock state, State,
/// captured Events and nested VMs) is reset before each test case.
pub struct MockTestBridge {
    shared: Arc<MockTestShared>,
    caller: Arc<LoadedDefinition>,
    depth: usize,
}

impl MockTestBridge {
    /// Bridge for testing `definition_id` within `graph`.
    pub fn new(graph: Arc<ConfiguredGraph>, definition_id: &str) -> Result<Self, String> {
        let caller = graph
            .definitions
            .get(definition_id)
            .cloned()
            .ok_or_else(|| format!("definition '{definition_id}' not found"))?;
        Ok(Self {
            shared: Arc::new(MockTestShared {
                graph,
                pool: VmPool::new(ResourceBudget::default()),
                mock_kv: Mutex::new(HashMap::new()),
                state: Mutex::new(HashMap::new()),
                events: Mutex::new(Vec::new()),
            }),
            caller: Arc::new(caller),
            depth: 0,
        })
    }

    /// Events emitted (by the tested Definition or any Mock-backed input) during the current test.
    pub fn captured_events(&self) -> Vec<CapturedTestEvent> {
        self.shared.events.lock().unwrap().clone()
    }

    fn input_ref(&self, input_name: &str) -> Result<&InputRefSpec, String> {
        self.caller.manifest.inputs.get(input_name).ok_or_else(|| {
            format!(
                "input '{input_name}' is not declared by '{}'",
                self.caller.manifest.id
            )
        })
    }

    async fn run_nested(
        &self,
        target: &LoadedDefinition,
        category: DefinitionCategory,
        default_handler: &str,
        payload: serde_json::Value,
        operation: Option<String>,
        idempotency_key: Option<String>,
    ) -> Result<serde_json::Value, String> {
        if self.depth >= MAX_TEST_INPUT_DEPTH {
            return Err(format!(
                "Definition test input nesting exceeded {MAX_TEST_INPUT_DEPTH} levels at '{}' (cycle?)",
                target.manifest.id
            ));
        }
        let child = Arc::new(MockTestBridge {
            shared: Arc::clone(&self.shared),
            caller: Arc::new(target.clone()),
            depth: self.depth + 1,
        });
        let instance_id = format!("test:{}", target.manifest.id);
        let bundle = CompiledInstanceBundle {
            instance_id: instance_id.clone(),
            manifest: target.manifest.clone(),
            runtime_source: target.runtime_source.clone(),
            config_digest: "definition-test".into(),
            resolved_libraries: self.shared.graph.resolved_libraries(),
        };
        let handler = if target.manifest.handler.is_empty() {
            default_handler.to_string()
        } else {
            target.manifest.handler.clone()
        };
        let cfg = ExecutionContextConfig {
            category,
            instance_id,
            input_names: target.manifest.inputs.keys().cloned().collect(),
            config: serde_json::json!({}),
            payload,
            operation,
            idempotency_key,
            allowed_emits: vec![],
            now_iso: "2026-11-24T12:00:00Z".into(),
            trace_id: format!("test_{}", uuid::Uuid::now_v7().simple()),
            bridge: child,
        };
        self.shared
            .pool
            .execute_handler(&bundle, &handler, cfg, None)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait]
impl HostBridge for MockTestBridge {
    async fn query_input(
        &self,
        input_name: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let InputRefSpec::Data { definition, .. } = self.input_ref(input_name)? else {
            return Err(format!("input '{input_name}' is not a data input; use :call or :invoke"));
        };
        let def_id = definition.as_deref().ok_or_else(|| {
            format!("data input '{input_name}' names no Definition; nothing to resolve in a Definition test")
        })?;
        let target = self
            .shared
            .graph
            .definitions
            .get(def_id)
            .ok_or_else(|| format!("data input '{input_name}' references unknown Definition '{def_id}'"))?
            .clone();
        if target.manifest.category != DefinitionCategory::DataProvider {
            return Err(format!("data input '{input_name}' references non-data-provider '{def_id}'"));
        }
        if let Some(ref q) = target.manifest.query {
            q.validate_value(&params)
                .map_err(|e| format!("invalid query parameters for '{def_id}': {e}"))?;
        }
        let val = self
            .run_nested(&target, DefinitionCategory::DataProvider, "query", params, None, None)
            .await?;
        if let Some(ref out) = target.manifest.output {
            out.validate_value(&val)
                .map_err(|e| format!("output contract violation on '{def_id}': {e}"))?;
        }
        Ok(val)
    }

    async fn call_input(
        &self,
        input_name: &str,
        operation: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let InputRefSpec::Connector {
            definition,
            required_capabilities,
        } = self.input_ref(input_name)?
        else {
            return Err(format!("input '{input_name}' is not a connector input"));
        };
        let conn_id = definition.as_deref().ok_or_else(|| {
            format!("connector input '{input_name}' names no connector Definition; cannot pick a Mock")
        })?;
        if let Some(conn) = self.shared.graph.definitions.get(conn_id) {
            let spec = conn.manifest.operations.get(operation).ok_or_else(|| {
                format!("connector '{conn_id}' declares no operation '{operation}'")
            })?;
            if !required_capabilities.is_empty() {
                if let Some(missing) = spec
                    .required_capabilities
                    .iter()
                    .find(|c| !required_capabilities.contains(c))
                {
                    return Err(format!(
                        "operation '{operation}' needs capability '{missing}', which input '{input_name}' does not declare"
                    ));
                }
            }
            if let Err(e) = spec.args.validate_value(&args) {
                return Err(format!("invalid args for '{conn_id}.{operation}': {e}"));
            }
        }
        let mock = self
            .shared
            .graph
            .find_mock_for(conn_id)
            .cloned()
            .ok_or_else(|| format!("no Mock declared for connector '{conn_id}' (§10.3)"))?;
        self.run_nested(
            &mock,
            DefinitionCategory::Mock,
            "call",
            args,
            Some(operation.to_string()),
            None,
        )
        .await
    }

    async fn invoke_action(
        &self,
        input_name: &str,
        args: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<serde_json::Value, String> {
        let InputRefSpec::Action { definition } = self.input_ref(input_name)? else {
            return Err(format!("input '{input_name}' is not an action input"));
        };
        let def_id = definition
            .as_deref()
            .ok_or_else(|| format!("action input '{input_name}' names no Definition"))?;
        let target = self
            .shared
            .graph
            .definitions
            .get(def_id)
            .ok_or_else(|| format!("action input '{input_name}' references unknown Definition '{def_id}'"))?
            .clone();
        if let Some(ref a) = target.manifest.args {
            a.validate_value(&args)
                .map_err(|e| format!("invalid action arguments for '{def_id}': {e}"))?;
        }
        let started = chrono::Utc::now().to_rfc3339();
        let res = self
            .run_nested(
                &target,
                DefinitionCategory::ActionProvider,
                "execute",
                args.clone(),
                None,
                idempotency_key.clone(),
            )
            .await;
        let (status, result, reason) = match res {
            Ok(v) => ("succeeded", Some(v), None),
            Err(e) => ("failed", None, Some(e)),
        };
        let rec = ActionExecutionRecord {
            execution_id: format!("test_act_{}", uuid::Uuid::now_v7().simple()),
            action_instance: format!("test:{def_id}"),
            parent_execution_id: None,
            principal_id: "definition-test".into(),
            principal_kind: "service".into(),
            status: status.into(),
            idempotency_key,
            args,
            result,
            reason,
            retry_safe: false,
            reconcile_attempts: 0,
            trace_id: String::new(),
            started_at: started.clone(),
            finished_at: Some(started),
            duration_ms: Some(0),
        };
        serde_json::to_value(rec).map_err(|e| e.to_string())
    }

    async fn http_request(&self, req: HttpHostRequest) -> Result<HttpHostResponse, String> {
        Err(format!(
            "Definition tests run against Mocks; ctx.http is unavailable ({} {})",
            req.method, req.url
        ))
    }

    async fn emit_event(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        durable: bool,
    ) -> Result<(), String> {
        self.emit_event_versioned(event_type, "1", payload, durable).await
    }

    async fn emit_event_versioned(
        &self,
        event_type: &str,
        schema_version: &str,
        payload: serde_json::Value,
        durable: bool,
    ) -> Result<(), String> {
        self.shared.events.lock().unwrap().push(CapturedTestEvent {
            source_definition: self.caller.manifest.id.clone(),
            event_type: event_type.to_string(),
            schema_version: schema_version.to_string(),
            durable,
            payload,
        });
        Ok(())
    }

    async fn begin_test_case(&self, _test_name: &str) {
        self.shared.mock_kv.lock().unwrap().clear();
        self.shared.state.lock().unwrap().clear();
        self.shared.events.lock().unwrap().clear();
        // Drop nested VMs so Mock/provider module state cannot leak between test cases.
        self.shared.pool.sync_generation(&[]);
    }

    async fn state_get(&self, scope: &str, key: &str) -> Result<serde_json::Value, String> {
        Ok(self
            .shared
            .state
            .lock()
            .unwrap()
            .get(&(scope.to_string(), self.caller.manifest.id.clone(), key.to_string()))
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }

    async fn state_set(
        &self,
        scope: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), String> {
        self.shared.state.lock().unwrap().insert(
            (scope.to_string(), self.caller.manifest.id.clone(), key.to_string()),
            value,
        );
        Ok(())
    }

    async fn load_fixture(&self, fixture_path: &str) -> Result<serde_json::Value, String> {
        self.caller
            .fixtures
            .get(fixture_path)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "fixture '{fixture_path}' not found in '{}'",
                    self.caller.manifest.id
                )
            })
    }

    async fn mock_state_get(&self, key: &str) -> Result<serde_json::Value, String> {
        let k = format!("{}:{key}", self.caller.manifest.id);
        Ok(self
            .shared
            .mock_kv
            .lock()
            .unwrap()
            .get(&k)
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }

    async fn mock_state_set(&self, key: &str, value: serde_json::Value) -> Result<(), String> {
        let k = format!("{}:{key}", self.caller.manifest.id);
        self.shared.mock_kv.lock().unwrap().insert(k, value);
        Ok(())
    }
}

/// Run one Definition's `tests.luau` against Mocks (§4.5). Errors if it has no tests.
pub async fn run_definition_tests_against_mocks(
    graph: Arc<ConfiguredGraph>,
    definition_id: &str,
) -> Result<DefinitionTestReport, String> {
    let def = graph
        .definitions
        .get(definition_id)
        .ok_or_else(|| format!("definition '{definition_id}' not found"))?
        .clone();
    let tests_src = def
        .tests_source
        .as_deref()
        .ok_or_else(|| format!("definition '{definition_id}' has no tests.luau"))?;
    let bridge = Arc::new(MockTestBridge::new(Arc::clone(&graph), definition_id)?);
    run_definition_tests(
        &def.manifest,
        &def.runtime_source,
        tests_src,
        &graph.resolved_libraries(),
        bridge,
        serde_json::json!({}),
    )
    .await
}

/// Run Definition tests for specified Definition IDs (or all Definitions with tests.luau if `None`).
pub async fn run_workspace_definition_tests(
    workspace_dir: &Path,
    data_dir: &Path,
    target_definitions: Option<&[String]>,
) -> Result<Vec<DefinitionTestReport>, GitError> {
    let graph = Arc::new(load_workspace_graph(workspace_dir, data_dir, "preview", 1, true).await?);
    let mut reports = Vec::new();

    for (def_id, def) in &graph.definitions {
        if let Some(targets) = target_definitions {
            if !targets.iter().any(|t| t == def_id) {
                continue;
            }
        }
        if def.tests_source.is_none() {
            continue;
        }
        let report = run_definition_tests_against_mocks(Arc::clone(&graph), def_id)
            .await
            .map_err(GitError::Workspace)?;
        reports.push(report);
    }
    Ok(reports)
}

/// Contract checks on Instance configuration that complement `validate_configured_graph`
/// (§12.2 step 3 "schema"): every Instance `config` must satisfy its Definition's declared
/// config schema, and every secret slot must be a credential the Definition declares and be
/// written as a `secret://<name>` reference.
pub fn validate_instance_contracts(graph: &ConfiguredGraph) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    for (inst_id, inst) in &graph.instances {
        let Some(def) = graph.definitions.get(&inst.definition) else {
            continue; // unknown Definitions are reported by validate_configured_graph
        };
        if let Some(schema) = &def.manifest.config {
            let cfg = if inst.config.is_null() {
                serde_json::json!({})
            } else {
                inst.config.clone()
            };
            if let Err(e) = schema.validate_value(&cfg) {
                issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "CONFIG_SCHEMA".into(),
                    target: inst_id.clone(),
                    message: format!(
                        "config of '{inst_id}' does not match the config schema of '{}': {e}",
                        inst.definition
                    ),
                });
            }
        }
        for (slot, reference) in &inst.secrets {
            if !def.manifest.credentials.iter().any(|c| c == slot) {
                issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "UNDECLARED_SECRET_SLOT".into(),
                    target: inst_id.clone(),
                    message: format!(
                        "secret slot '{slot}' is not a credential declared by '{}' (declared: {:?})",
                        inst.definition, def.manifest.credentials
                    ),
                });
            }
            let valid_ref = reference
                .strip_prefix("secret://")
                .map(|n| !n.trim().is_empty())
                .unwrap_or(false);
            if !valid_ref {
                issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "INVALID_SECRET_REF".into(),
                    target: inst_id.clone(),
                    message: format!(
                        "secret slot '{slot}' must reference a central secret as 'secret://<name>', got '{reference}'"
                    ),
                });
            }
        }
    }
    issues
}

/// `validate_configured_graph` plus [`validate_instance_contracts`].
pub fn validate_graph_with_contracts(
    graph: &ConfiguredGraph,
    store: &SqliteStore,
    is_preview: bool,
) -> ValidationReport {
    let mut report = validate_configured_graph(graph, store, is_preview);
    let extra = validate_instance_contracts(graph);
    if extra.iter().any(|i| i.severity == "error") {
        report.valid = false;
    }
    report.issues.extend(extra);
    report
}

/// Execute full isolated Preview: validates Mocks, runs Definition tests and all Scenario tests,
/// and renders Dashboards using Mocks (never touching production state or real external APIs).
pub async fn run_preview_and_scenarios(
    workspace_dir: &Path,
    data_dir: &Path,
    changed_definitions: Option<&[String]>,
) -> Result<PreviewTestReport, GitError> {
    let graph = load_workspace_graph(workspace_dir, data_dir, "preview", 1, true).await?;
    let preview_store = SqliteStore::open_in_memory()
        .map_err(|e| GitError::Workspace(e.to_string()))?;

    let validation = validate_graph_with_contracts(&graph, &preview_store, true);
    let definition_tests =
        run_workspace_definition_tests(workspace_dir, data_dir, changed_definitions).await?;

    let mut scenario_results = Vec::new();
    let mut preview_renders = BTreeMap::new();
    let mut preview_render_errors = BTreeMap::new();

    if validation.valid {
        // Also render each dashboard once in isolated Preview
        let render_engine = RuntimeEngine::new(
            graph.clone(),
            SqliteStore::open_in_memory().map_err(|e| GitError::Workspace(e.to_string()))?,
            ResourceBudget::default(),
        )
        .map_err(|e| GitError::Workspace(e.to_string()))?;
        let admin = Principal::administrator();
        for dash_id in graph.dashboards.keys() {
            match render_engine.queries.render_dashboard(dash_id, &admin).await {
                Ok(rendered) => {
                    preview_renders.insert(dash_id.clone(), rendered);
                }
                Err(e) => {
                    preview_render_errors.insert(dash_id.clone(), e);
                }
            }
        }

        let scenarios_dir = workspace_dir.join("scenarios");
        if scenarios_dir.exists() {
            let mut files: Vec<PathBuf> = std::fs::read_dir(&scenarios_dir)?
                .filter_map(|e| e.ok().map(|x| x.path()))
                .filter(|p| {
                    p.extension()
                        .and_then(|s| s.to_str())
                        .map(|e| e == "yaml" || e == "yml")
                        .unwrap_or(false)
                })
                .collect();
            files.sort();

            for file in files {
                let raw = std::fs::read_to_string(&file)?;
                let yaml_val: serde_json::Value = serde_yaml_ng::from_str(&raw)
                    .map_err(|e| GitError::Workspace(format!("{}: {e}", file.display())))?;
                let suite: ScenarioSuiteFile = serde_json::from_value(yaml_val)
                    .map_err(|e| GitError::Workspace(format!("{}: {e}", file.display())))?;

                for scenario in suite.scenarios {
                    // Fresh isolated Preview RuntimeEngine per scenario
                    let iso_store = SqliteStore::open_in_memory()
                        .map_err(|e| GitError::Workspace(e.to_string()))?;
                    let iso_engine =
                        RuntimeEngine::new(graph.clone(), iso_store, ResourceBudget::default())
                            .map_err(|e| GitError::Workspace(e.to_string()))?;

                    let res = execute_scenario(&iso_engine, &scenario.steps).await;
                    scenario_results.push(ScenarioResult {
                        suite: suite.name.clone(),
                        scenario: scenario.name,
                        passed: res.is_ok(),
                        error: res.err(),
                    });
                }
            }
        }
    }

    let defs_ok = definition_tests.iter().all(|r| r.failed == 0);
    let scens_ok = scenario_results.iter().all(|s| s.passed);
    let renders_ok = preview_render_errors.is_empty();
    let all_passed = validation.valid && defs_ok && scens_ok && renders_ok;

    Ok(PreviewTestReport {
        validation,
        definition_tests,
        scenario_results,
        preview_renders,
        preview_render_errors,
        all_passed,
        tested_tree: None,
    })
}

/// Assertion fields a step kind evaluates. Setting any other field is an error, so a typo or a
/// misplaced key can never make a Scenario pass silently.
fn check_assertion_fields(
    idx: usize,
    kind: &str,
    a: &StepAssertion,
    supported: &[&str],
) -> Result<(), String> {
    let set: Vec<&str> = [
        ("length", a.length.is_some()),
        ("contains", a.contains.is_some()),
        ("feasible", a.feasible.is_some()),
        ("status", a.status.is_some()),
        ("transitions", a.transitions.is_some()),
        ("contains_text", a.contains_text.is_some()),
        ("widget", a.widget.is_some()),
        ("envelope_kind", a.envelope_kind.is_some()),
    ]
    .into_iter()
    .filter(|(_, is_set)| *is_set)
    .map(|(name, _)| name)
    .collect();
    let unsupported: Vec<&str> = set
        .into_iter()
        .filter(|f| !supported.contains(f))
        .collect();
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "step {idx}: assertion field(s) {unsupported:?} are not supported by '{kind}' steps (supported: {supported:?})"
        ))
    }
}

/// Resolve the effective assertion of a step: an inline (operation-level) or step-level
/// `assert`, but not both.
fn step_assertion<'a>(
    idx: usize,
    inline: Option<&'a StepAssertion>,
    step_level: Option<&'a StepAssertion>,
) -> Result<Option<&'a StepAssertion>, String> {
    match (inline, step_level) {
        (Some(_), Some(_)) => Err(format!(
            "step {idx}: 'assert' given both inside the operation and on the step; use one"
        )),
        (a, b) => Ok(a.or(b)),
    }
}

fn operation_keys(step: &ScenarioStep) -> Vec<&'static str> {
    [
        ("query", step.query.is_some()),
        ("dry_run", step.dry_run.is_some()),
        ("invoke_action", step.invoke_action.is_some()),
        ("send_request", step.send_request.is_some()),
        ("set_dashboard_state", step.set_dashboard_state.is_some()),
        ("render_dashboard", step.render_dashboard.is_some()),
        ("process_events", step.process_events.is_some()),
        ("emit_event", step.emit_event.is_some()),
        ("push_update", step.push_update.is_some()),
        ("inject_failure", step.inject_failure.is_some()),
        ("clear_failure", step.clear_failure.is_some()),
        ("advance_time", step.advance_time.is_some()),
    ]
    .into_iter()
    .filter(|(_, set)| *set)
    .map(|(k, _)| k)
    .collect()
}

/// An automation result counts as a transition only when it is an action execution that
/// succeeded (a `{ skipped = true }` result or a failed execution does not).
fn is_succeeded_execution(v: &serde_json::Value) -> bool {
    v.get("execution_id").and_then(|x| x.as_str()).is_some()
        && v.get("status").and_then(|x| x.as_str()) == Some("succeeded")
}

fn find_widget_node<'a>(node: &'a serde_json::Value, widget_id: &str) -> Option<&'a serde_json::Value> {
    match node {
        serde_json::Value::Object(obj) => {
            if obj.get("widget_id").and_then(|v| v.as_str()) == Some(widget_id) {
                return Some(node);
            }
            obj.values().find_map(|v| find_widget_node(v, widget_id))
        }
        serde_json::Value::Array(arr) => arr.iter().find_map(|v| find_widget_node(v, widget_id)),
        _ => None,
    }
}

fn check_widget_assertion(
    idx: usize,
    rendered: &serde_json::Value,
    wa: &WidgetAssertion,
) -> Result<(), String> {
    let adapters = rendered
        .get("adapters")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let node = find_widget_node(&adapters, &wa.widget_id)
        .ok_or_else(|| format!("step {idx}: no rendered Widget node with widget_id '{}'", wa.widget_id))?;
    if let Some(ref exp_type) = wa.widget_type {
        let actual = node
            .get("widget_type")
            .or_else(|| node.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if actual != exp_type {
            return Err(format!(
                "step {idx}: widget '{}' has widget_type '{actual}', expected '{exp_type}'",
                wa.widget_id
            ));
        }
    }
    if let Some(ref exp_props) = wa.props {
        let props = node.get("props").cloned().unwrap_or(serde_json::Value::Null);
        if !json_matches_subset(&props, exp_props) {
            return Err(format!(
                "step {idx}: widget '{}' props {props} do not contain {exp_props}",
                wa.widget_id
            ));
        }
    }
    if let Some(exp_visible) = wa.visible {
        let visible = node
            .get("layout_override")
            .and_then(|o| o.get("visible"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if visible != exp_visible {
            return Err(format!(
                "step {idx}: widget '{}' visible={visible}, expected {exp_visible}",
                wa.widget_id
            ));
        }
    }
    Ok(())
}

async fn execute_scenario(engine: &RuntimeEngine, steps: &[ScenarioStep]) -> Result<(), String> {
    let admin = Principal::administrator();
    for (idx, step) in steps.iter().enumerate() {
        let ops = operation_keys(step);
        if ops.len() > 1 {
            return Err(format!(
                "step {idx}: exactly one operation per step is allowed, found {ops:?}"
            ));
        }
        let step_assert = step.assert.as_ref();
        if let Some(q) = &step.query {
            let assert = step_assertion(idx, q.assert.as_ref(), step_assert)?;
            if let Some(a) = assert {
                check_assertion_fields(idx, "query", a, &["status", "length", "contains"])?;
            }
            let env = engine
                .queries
                .query(
                    &q.instance,
                    q.params.clone(),
                    &admin,
                    q.force_refresh.unwrap_or(false),
                )
                .await?;
            if let Some(a) = assert {
                if let Some(ref exp_status) = a.status {
                    let actual = serde_json::to_value(env.status)
                        .unwrap()
                        .as_str()
                        .unwrap_or("")
                        .to_string();
                    if &actual != exp_status {
                        return Err(format!(
                            "step {idx}: expected query status '{exp_status}', got '{actual}'"
                        ));
                    }
                }
                let val = env.value.unwrap_or(serde_json::Value::Null);
                if let Some(exp_len) = a.length {
                    let actual_len = val.as_array().map(|v| v.len()).ok_or_else(|| {
                        format!("step {idx}: expected array for 'length' assertion, got {val}")
                    })?;
                    if actual_len != exp_len {
                        return Err(format!(
                            "step {idx}: expected length {exp_len}, got {actual_len} ({val})"
                        ));
                    }
                }
                if let Some(ref expected_items) = a.contains {
                    let arr = val.as_array().ok_or_else(|| {
                        format!("step {idx}: expected array for 'contains' assertion")
                    })?;
                    for needle in expected_items {
                        let found = arr.iter().any(|row| json_matches_subset(row, needle));
                        if !found {
                            return Err(format!(
                                "step {idx}: array did not contain subset {needle} (actual: {val})"
                            ));
                        }
                    }
                }
            }
        } else if let Some(d) = &step.dry_run {
            let assert = step_assertion(idx, d.assert.as_ref(), step_assert)?;
            if let Some(a) = assert {
                check_assertion_fields(idx, "dry_run", a, &["feasible"])?;
            }
            let report = engine
                .actions
                .dry_run(&d.action, d.args.clone(), &admin)
                .await?;
            if let Some(a) = assert {
                if let Some(exp_f) = a.feasible {
                    if report.feasible != exp_f {
                        return Err(format!(
                            "step {idx}: expected feasible={exp_f}, got {}",
                            report.feasible
                        ));
                    }
                }
            }
        } else if let Some(act) = &step.invoke_action {
            let assert = step_assertion(idx, act.assert.as_ref(), step_assert)?;
            if let Some(a) = assert {
                check_assertion_fields(idx, "invoke_action", a, &["status"])?;
            }
            let rec = engine
                .actions
                .invoke(
                    &act.action,
                    act.args.clone(),
                    &admin,
                    act.idempotency_key.clone(),
                    None,
                )
                .await?;
            if let Some(a) = assert {
                if let Some(ref exp_s) = a.status {
                    if &rec.status != exp_s {
                        return Err(format!(
                            "step {idx}: expected action status '{exp_s}', got '{}' (reason: {:?})",
                            rec.status, rec.reason
                        ));
                    }
                }
            }
        } else if let Some(req) = &step.send_request {
            let assert = step_assertion(idx, req.assert.as_ref(), step_assert)?;
            if let Some(a) = assert {
                check_assertion_fields(idx, "send_request", a, &["status"])?;
            }
            let rec = engine
                .actions
                .dispatch_widget_request_outcome(
                    WidgetRequestEnvelope {
                        protocol: "dash.widget/1".into(),
                        dashboard_id: req.dashboard.clone(),
                        adapter_instance: req.adapter.clone(),
                        widget_id: req.widget_id.clone(),
                        generation: engine.graph.load().generation_id,
                        sequence: (idx + 1) as u64,
                        kind: "request".into(),
                        request: req.request.clone(),
                        payload: req.payload.clone(),
                    },
                    &admin,
                )
                .await?;
            if let Some(a) = assert {
                if let Some(ref exp_s) = a.status {
                    if rec.status() != exp_s {
                        return Err(format!(
                            "step {idx}: expected request status '{exp_s}', got '{}'",
                            rec.status()
                        ));
                    }
                }
            }
        } else if let Some(st) = &step.set_dashboard_state {
            if step_assert.is_some() {
                return Err(format!("step {idx}: 'set_dashboard_state' steps take no assertions"));
            }
            engine
                .state
                .set(
                    "dashboard",
                    &st.dashboard,
                    &st.key,
                    1,
                    engine.graph.load().generation_id,
                    st.value.clone(),
                    None,
                )
                .map_err(|e| e.to_string())?;
        } else if let Some(rd) = &step.render_dashboard {
            let assert = step_assertion(idx, rd.assert.as_ref(), step_assert)?;
            if let Some(a) = assert {
                check_assertion_fields(idx, "render_dashboard", a, &["contains_text", "widget"])?;
            }
            let rendered = engine
                .queries
                .render_dashboard(&rd.dashboard, &admin)
                .await?;
            if let Some(a) = assert {
                if let Some(ref txt) = a.contains_text {
                    let s = serde_json::to_string(&rendered).unwrap_or_default();
                    if !s.contains(txt) {
                        return Err(format!(
                            "step {idx}: rendered dashboard missing text '{txt}'"
                        ));
                    }
                }
                if let Some(ref widgets) = a.widget {
                    for wa in widgets {
                        check_widget_assertion(idx, &rendered, wa)?;
                    }
                }
            }
        } else if let Some(pe) = &step.process_events {
            let assert = step_assertion(idx, pe.assert.as_ref(), step_assert)?;
            if let Some(a) = assert {
                check_assertion_fields(idx, "process_events", a, &["transitions"])?;
            }
            let events = engine.events.replay_since(&pe.source, 0)?;
            let mut transitions = 0;
            for ev in &events {
                let res = engine.automations.dispatch_event(ev).await;
                for item in res {
                    match item {
                        Ok(v) => {
                            if is_succeeded_execution(&v) {
                                transitions += 1;
                            }
                        }
                        Err(e) => return Err(format!("step {idx}: automation failed: {e}")),
                    }
                }
            }
            if let Some(a) = assert {
                if let Some(exp_t) = a.transitions {
                    if transitions != exp_t {
                        return Err(format!(
                            "step {idx}: expected {exp_t} automation transitions (succeeded action executions), got {transitions}"
                        ));
                    }
                }
            }
        } else if let Some(em) = &step.emit_event {
            if step_assert.is_some() {
                return Err(format!("step {idx}: 'emit_event' steps take no assertions; follow with process_events"));
            }
            if !engine.graph.load().instances.contains_key(&em.source) {
                return Err(format!(
                    "step {idx}: emit_event source '{}' is not an Instance of the Preview graph",
                    em.source
                ));
            }
            engine.events.emit(
                &em.source,
                &em.event_type,
                &em.schema_version,
                &format!("scenario_step_{idx}"),
                em.durable,
                em.payload.clone(),
            )?;
        } else if let Some(pu) = &step.push_update {
            let assert = step_assertion(idx, pu.assert.as_ref(), step_assert)?;
            if let Some(a) = assert {
                check_assertion_fields(idx, "push_update", a, &["status", "envelope_kind"])?;
            }
            let env = engine.queries.push_query_value(
                &pu.instance,
                &pu.params,
                &admin,
                pu.value.clone(),
            )?;
            if let Some(a) = assert {
                if let Some(ref exp_status) = a.status {
                    let actual = serde_json::to_value(env.status)
                        .ok()
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_default();
                    if &actual != exp_status {
                        return Err(format!(
                            "step {idx}: expected pushed envelope status '{exp_status}', got '{actual}'"
                        ));
                    }
                }
                if let Some(ref exp_kind) = a.envelope_kind {
                    let actual = serde_json::to_value(env.kind)
                        .ok()
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_default();
                    if &actual != exp_kind {
                        return Err(format!(
                            "step {idx}: expected pushed envelope kind '{exp_kind}', got '{actual}'"
                        ));
                    }
                }
            }
        } else if let Some(inj) = &step.inject_failure {
            if step_assert.is_some() {
                return Err(format!("step {idx}: 'inject_failure' steps take no assertions"));
            }
            engine.telemetry.inject_failure(FailureInjectionRule {
                instance_id: inj.instance.clone(),
                mode: inj.mode.clone(),
                message: inj.message.clone(),
                remaining: inj.remaining,
            });
        } else if let Some(clr) = &step.clear_failure {
            if step_assert.is_some() {
                return Err(format!("step {idx}: 'clear_failure' steps take no assertions"));
            }
            engine.telemetry.clear_failure(&clr.instance);
        } else if let Some(adv) = &step.advance_time {
            // Preview has no virtual clock yet (time simulation is an open Preview-fidelity
            // detail, §19.9). Refuse rather than pretend time moved, so time-dependent
            // Scenarios can never pass falsely.
            return Err(format!(
                "step {idx}: advance_time ({}s) is not supported: Preview has no virtual clock yet",
                adv.seconds
            ));
        } else {
            return Err(format!("step {idx}: empty/unknown scenario step"));
        }
    }
    Ok(())
}

fn json_matches_subset(haystack: &serde_json::Value, needle: &serde_json::Value) -> bool {
    match (haystack, needle) {
        (serde_json::Value::Object(h), serde_json::Value::Object(n)) => n
            .iter()
            .all(|(k, nv)| h.get(k).map(|hv| json_matches_subset(hv, nv)).unwrap_or(false)),
        (a, b) => a == b,
    }
}

#[cfg(test)]
mod mock_test_bridge_tests {
    use super::*;
    use tempfile::TempDir;

    fn fixture_ws() -> (TempDir, PathBuf, PathBuf) {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        let src = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/engineering-command-center");
        crate::packages::copy_tree_excluding_git(&src, &ws).unwrap();
        (tmp, ws, data)
    }

    fn report_for<'a>(reports: &'a [DefinitionTestReport], id: &str) -> &'a DefinitionTestReport {
        reports
            .iter()
            .find(|r| r.definition_id == id)
            .unwrap_or_else(|| panic!("no report for {id}: {reports:?}"))
    }

    #[tokio::test]
    async fn definition_tests_reach_mocks_through_connector_and_data_inputs() {
        let (_tmp, ws, data) = fixture_ws();
        let reports = run_workspace_definition_tests(&ws, &data, None).await.unwrap();
        for r in &reports {
            assert_eq!(r.failed, 0, "definition test failed: {r:?}");
        }
        // connector input -> github.mock fixtures (and Mock state within one test)
        // 3 Mock-backed cases + 6 self-contained reconcile/dry_run/execute cases
        assert_eq!(report_for(&reports, "github.merge_pr").passed, 9);
        assert_eq!(report_for(&reports, "github.pull_requests").passed, 2);
        // data inputs -> providers -> Mocks
        assert_eq!(report_for(&reports, "engineering.linked_prs").passed, 2);
        // adapter -> data provider -> providers -> Mocks
        assert_eq!(report_for(&reports, "engineering.pr_table_adapter").passed, 2);
    }

    #[tokio::test]
    async fn mock_bridge_captures_contract_events_with_declared_version() {
        let (_tmp, ws, data) = fixture_ws();
        let graph = Arc::new(load_workspace_graph(&ws, &data, "preview", 1, true).await.unwrap());
        let def = graph.definitions.get("github.merge_pr").unwrap().clone();
        let bridge = Arc::new(MockTestBridge::new(Arc::clone(&graph), "github.merge_pr").unwrap());
        let tests = r#"
local dash = require("dash")
local Runtime = require("runtime")
dash.test("execute emits", function(make_ctx)
    Runtime.execute(make_ctx({ args = { repository = "acme/platform", number = 43 } }))
end)
"#;
        let report = run_definition_tests(
            &def.manifest,
            &def.runtime_source,
            tests,
            &graph.resolved_libraries(),
            bridge.clone(),
            serde_json::json!({}),
        )
        .await
        .unwrap();
        assert_eq!(report.failed, 0, "{report:?}");
        let events = bridge.captured_events();
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0].event_type, "github.pr_merged");
        assert_eq!(events[0].schema_version, "1");
        assert!(events[0].durable);
        assert_eq!(events[0].payload["number"], 43);
    }

    #[tokio::test]
    async fn mock_bridge_rejects_missing_mock_undeclared_ops_and_http() {
        let (_tmp, ws, data) = fixture_ws();
        let mut graph = load_workspace_graph(&ws, &data, "preview", 1, true).await.unwrap();
        // Remove the github Mock: connector inputs can no longer be resolved.
        graph.definitions.remove("github.mock");
        let graph = Arc::new(graph);
        let bridge = MockTestBridge::new(Arc::clone(&graph), "github.pull_requests").unwrap();
        let err = bridge
            .call_input("github", "list_prs", serde_json::json!({ "repository": "acme/platform" }))
            .await
            .unwrap_err();
        assert!(err.contains("no Mock declared"), "{err}");
        let err = bridge
            .call_input("github", "delete_repo", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(err.contains("declares no operation"), "{err}");
        // pull_requests only declares pull_requests.read on its github input
        let err = bridge
            .call_input(
                "github",
                "merge_pr",
                serde_json::json!({ "repository": "acme/platform", "number": 42 }),
            )
            .await
            .unwrap_err();
        assert!(err.contains("does not declare"), "{err}");
        let err = bridge.query_input("nope", serde_json::json!({})).await.unwrap_err();
        assert!(err.contains("not declared"), "{err}");
        let err = bridge
            .http_request(HttpHostRequest {
                method: "GET".into(),
                url: "https://example.invalid".into(),
                headers: Default::default(),
                body: None,
                credential: None,
            })
            .await
            .unwrap_err();
        assert!(err.contains("ctx.http is unavailable"), "{err}");
    }

    #[tokio::test]
    async fn tests_file_without_tests_fails_activation_gate() {
        let (_tmp, ws, data) = fixture_ws();
        let tests_path = ws.join("packages/jira/issues_by_keys/tests.luau");
        std::fs::write(&tests_path, "local dash = require(\"dash\")\n").unwrap();
        let targets = vec!["jira.issues_by_keys".to_string()];
        let reports = run_workspace_definition_tests(&ws, &data, Some(&targets))
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].failed, 1);
        assert_eq!(reports[0].cases[0].name, dash_luau::NO_TESTS_CASE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_ws(dst: &Path) {
        let src = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/engineering-command-center");
        crate::packages::copy_tree_excluding_git(&src, dst).unwrap();
    }

    fn parse_steps(yaml: &str) -> Result<Vec<ScenarioStep>, String> {
        let v: serde_json::Value = serde_yaml_ng::from_str(yaml).map_err(|e| e.to_string())?;
        serde_json::from_value(v).map_err(|e| e.to_string())
    }

    async fn preview_engine(ws: &Path, data: &Path) -> RuntimeEngine {
        let graph = load_workspace_graph(ws, data, "preview", 1, true).await.unwrap();
        RuntimeEngine::new(
            graph,
            SqliteStore::open_in_memory().unwrap(),
            ResourceBudget::default(),
        )
        .unwrap()
    }

    #[test]
    fn misspelled_assertion_key_is_rejected_at_parse_time() {
        let err = parse_steps(
            "- query: { instance: linked_prs }\n  assert: { lenght: 3 }\n",
        )
        .unwrap_err();
        assert!(err.contains("lenght"), "{err}");
        let err = parse_steps("- quer: { instance: linked_prs }\n").unwrap_err();
        assert!(err.contains("quer"), "{err}");
    }

    #[tokio::test]
    async fn unsupported_assertion_multi_op_and_advance_time_fail() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        fixture_ws(&ws);
        let engine = preview_engine(&ws, &tmp.path().join("data")).await;

        // `contains_text` is a render assertion: on a query step it must fail, not pass
        let steps = parse_steps(
            "- query: { instance: linked_prs, params: { repository: acme/platform, state: open } }\n  assert: { contains_text: anything }\n",
        )
        .unwrap();
        let err = execute_scenario(&engine, &steps).await.unwrap_err();
        assert!(err.contains("not supported"), "{err}");

        let steps = parse_steps(
            "- query: { instance: linked_prs }\n  render_dashboard: { dashboard: engineering }\n",
        )
        .unwrap();
        let err = execute_scenario(&engine, &steps).await.unwrap_err();
        assert!(err.contains("exactly one operation"), "{err}");

        let steps = parse_steps("- advance_time: { seconds: 60 }\n").unwrap();
        let err = execute_scenario(&engine, &steps).await.unwrap_err();
        assert!(err.contains("not supported"), "{err}");
    }

    #[tokio::test]
    async fn widget_assertions_check_structure() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        fixture_ws(&ws);
        let engine = preview_engine(&ws, &tmp.path().join("data")).await;
        let ok = parse_steps(
            "- render_dashboard: { dashboard: engineering }\n  assert:\n    widget:\n      - widget_id: pr_table_adapter_pr_table\n        widget_type: dash.table\n        props: { client_sort_state_key: pr_table_sort }\n        visible: true\n",
        )
        .unwrap();
        execute_scenario(&engine, &ok).await.unwrap();

        let wrong_type = parse_steps(
            "- render_dashboard: { dashboard: engineering }\n  assert:\n    widget:\n      - widget_id: pr_table_adapter_pr_table\n        widget_type: dash.chart\n",
        )
        .unwrap();
        assert!(execute_scenario(&engine, &wrong_type).await.unwrap_err().contains("widget_type"));

        let missing = parse_steps(
            "- render_dashboard: { dashboard: engineering }\n  assert:\n    widget:\n      - widget_id: does_not_exist\n",
        )
        .unwrap();
        assert!(execute_scenario(&engine, &missing).await.unwrap_err().contains("no rendered Widget"));
    }

    #[tokio::test]
    async fn injected_event_drives_automation_and_skips_are_not_transitions() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        fixture_ws(&ws);
        let engine = preview_engine(&ws, &tmp.path().join("data")).await;
        let g = engine.graph.load_full();
        // Find the automation's Event trigger and the source Instance bound to it
        let (auto_inst, trigger) = g
            .instances
            .values()
            .filter(|i| i.category == dash_luau::DefinitionCategory::Automation)
            .find_map(|i| {
                let def = g.definitions.get(&i.definition)?;
                let t = def.manifest.triggers.iter().find(|t| t.kind == "event")?.clone();
                Some((i.clone(), t))
            })
            .expect("fixture automation with an event trigger");
        let source = trigger
            .source_input
            .as_deref()
            .and_then(|slot| auto_inst.inputs.get(slot).cloned())
            .unwrap();
        let event_type = trigger.event_type.clone().unwrap();

        // An Event without a linked issue: the automation skips -> 0 transitions
        let yaml = format!(
            "- emit_event: {{ source: {source}, type: {event_type}, payload: {{ repository: acme/platform, number: 44 }} }}\n- process_events: {{ source: {source} }}\n  assert: {{ transitions: 0 }}\n"
        );
        execute_scenario(&engine, &parse_steps(&yaml).unwrap()).await.unwrap();

        let yaml = format!(
            "- emit_event: {{ source: {source}, type: {event_type}, payload: {{ repository: acme/platform, number: 45 }} }}\n- process_events: {{ source: {source} }}\n  assert: {{ transitions: 1 }}\n"
        );
        let err = execute_scenario(&engine, &parse_steps(&yaml).unwrap()).await.unwrap_err();
        assert!(err.contains("got 0"), "{err}");
    }

    #[tokio::test]
    async fn push_update_step_reaches_the_query_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        fixture_ws(&ws);
        let engine = preview_engine(&ws, &tmp.path().join("data")).await;
        let steps = parse_steps(
            "- query: { instance: open_prs, params: { repository: acme/platform, state: open } }\n- push_update: { instance: open_prs, params: { repository: acme/platform, state: open }, value: [ { number: 1 } ] }\n  assert: { status: fresh }\n- query: { instance: open_prs, params: { repository: acme/platform, state: open } }\n  assert: { length: 1 }\n",
        )
        .unwrap();
        execute_scenario(&engine, &steps).await.unwrap();
    }

    #[tokio::test]
    async fn render_failure_fails_preview() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        fixture_ws(&ws);
        // Break the table Adapter at render time (valid declaration, failing runtime). Every
        // declared handler must still exist in the module, or loading fails first.
        let rt = ws.join("packages/engineering/pr_table_adapter/runtime.luau");
        std::fs::write(
            &rt,
            "local R = {}\nfunction R.render(ctx) error(\"adapter exploded\") end\nfunction R.on_request(ctx) return nil end\nreturn R\n",
        )
        .unwrap();
        let report = run_preview_and_scenarios(&ws, &tmp.path().join("data"), Some(&[]))
            .await
            .unwrap();
        assert!(report.preview_render_errors.contains_key("engineering"), "{report:?}");
        assert!(!report.all_passed);
    }

    #[test]
    fn instance_contract_checks_flag_bad_secret_slots() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        fixture_ws(&ws);
        let mut graph = crate::workspace::load_workspace_graph(&ws, &tmp.path().join("data"), "x", 1, true).unwrap();
        assert!(validate_instance_contracts(&graph).is_empty(), "{:?}", validate_instance_contracts(&graph));
        let (id, inst) = graph
            .instances
            .iter_mut()
            .find(|(_, i)| !i.secrets.is_empty())
            .unwrap();
        let id = id.clone();
        inst.secrets.insert("not_declared".into(), "plaintext-token".into());
        let issues = validate_instance_contracts(&graph);
        assert!(issues.iter().any(|i| i.code == "UNDECLARED_SECRET_SLOT" && i.target == id));
        assert!(issues.iter().any(|i| i.code == "INVALID_SECRET_REF" && i.target == id));
    }
}
