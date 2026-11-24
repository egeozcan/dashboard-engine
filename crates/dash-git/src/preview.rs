//! Isolated Preview runtime, Definition test execution, and declarative YAML Scenario test runner (§4.5, §10.3).

use crate::git_cli::GitError;
use crate::workspace::load_workspace_graph;
use dash_core::{
    validate_configured_graph, FailureInjectionRule, Principal, RuntimeEngine, ValidationReport,
    WidgetRequestEnvelope,
};
use dash_luau::{run_definition_tests, DefinitionTestReport, NullHostBridge, ResourceBudget};
use dash_store::SqliteStore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioSuiteFile {
    pub name: String,
    #[serde(default)]
    pub scenarios: Vec<ScenarioSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioSpec {
    pub name: String,
    #[serde(default)]
    pub steps: Vec<ScenarioStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
    pub inject_failure: Option<InjectFailureStep>,
    #[serde(default)]
    pub clear_failure: Option<ClearFailureStep>,
    #[serde(default)]
    pub advance_time: Option<AdvanceTimeStep>,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct DryRunStep {
    pub action: String,
    #[serde(default)]
    pub args: serde_json::Value,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct SetDashboardStateStep {
    pub dashboard: String,
    pub key: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderDashboardStep {
    pub dashboard: String,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessEventsStep {
    pub source: String,
    #[serde(default)]
    pub assert: Option<StepAssertion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InjectFailureStep {
    pub instance: String,
    pub mode: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub remaining: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClearFailureStep {
    pub instance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvanceTimeStep {
    pub seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
    pub all_passed: bool,
}

/// Run Definition tests for specified Definition IDs (or all Definitions with tests.luau if `None`).
pub async fn run_workspace_definition_tests(
    workspace_dir: &Path,
    data_dir: &Path,
    target_definitions: Option<&[String]>,
) -> Result<Vec<DefinitionTestReport>, GitError> {
    let graph = load_workspace_graph(workspace_dir, data_dir, "preview", 1, true)?;
    let libs = graph.resolved_libraries();
    let bridge = Arc::new(NullHostBridge);
    let mut reports = Vec::new();

    for (def_id, def) in &graph.definitions {
        if let Some(targets) = target_definitions {
            if !targets.iter().any(|t| t == def_id) {
                continue;
            }
        }
        let Some(ref tests_src) = def.tests_source else {
            continue;
        };
        let report = run_definition_tests(
            &def.manifest,
            &def.runtime_source,
            tests_src,
            &libs,
            bridge.clone(),
            serde_json::json!({}),
        )
        .await
        .map_err(GitError::Workspace)?;
        reports.push(report);
    }
    Ok(reports)
}

/// Execute full isolated Preview: validates Mocks, runs Definition tests and all Scenario tests,
/// and renders Dashboards using Mocks (never touching production state or real external APIs).
pub async fn run_preview_and_scenarios(
    workspace_dir: &Path,
    data_dir: &Path,
    changed_definitions: Option<&[String]>,
) -> Result<PreviewTestReport, GitError> {
    let graph = load_workspace_graph(workspace_dir, data_dir, "preview", 1, true)?;
    let preview_store = SqliteStore::open_in_memory()
        .map_err(|e| GitError::Workspace(e.to_string()))?;

    let validation = validate_configured_graph(&graph, &preview_store, true);
    let definition_tests =
        run_workspace_definition_tests(workspace_dir, data_dir, changed_definitions).await?;

    let mut scenario_results = Vec::new();
    let mut preview_renders = BTreeMap::new();

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
            if let Ok(rendered) = render_engine.queries.render_dashboard(dash_id, &admin).await {
                preview_renders.insert(dash_id.clone(), rendered);
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
    let all_passed = validation.valid && defs_ok && scens_ok;

    Ok(PreviewTestReport {
        validation,
        definition_tests,
        scenario_results,
        preview_renders,
        all_passed,
    })
}

async fn execute_scenario(engine: &RuntimeEngine, steps: &[ScenarioStep]) -> Result<(), String> {
    let admin = Principal::administrator();
    for (idx, step) in steps.iter().enumerate() {
        let step_assert = step.assert.as_ref();
        if let Some(q) = &step.query {
            let assert = q.assert.as_ref().or(step_assert);
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
                    let actual_len = val.as_array().map(|v| v.len()).unwrap_or(0);
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
            let assert = d.assert.as_ref().or(step_assert);
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
            let assert = act.assert.as_ref().or(step_assert);
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
            let assert = req.assert.as_ref().or(step_assert);
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
            let assert = rd.assert.as_ref().or(step_assert);
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
            }
        } else if let Some(pe) = &step.process_events {
            let assert = pe.assert.as_ref().or(step_assert);
            let events = engine.events.replay_since(&pe.source, 0)?;
            let mut ok_count = 0;
            for ev in &events {
                let res = engine.automations.dispatch_event(ev).await;
                for item in res {
                    match item {
                        Ok(_) => ok_count += 1,
                        Err(e) => return Err(format!("step {idx}: automation failed: {e}")),
                    }
                }
            }
            if let Some(a) = assert {
                if let Some(exp_t) = a.transitions {
                    if ok_count != exp_t {
                        return Err(format!(
                            "step {idx}: expected {exp_t} automation transitions, got {ok_count}"
                        ));
                    }
                }
            }
        } else if let Some(inj) = &step.inject_failure {
            engine.telemetry.inject_failure(FailureInjectionRule {
                instance_id: inj.instance.clone(),
                mode: inj.mode.clone(),
                message: inj.message.clone(),
                remaining: inj.remaining,
            });
        } else if let Some(clr) = &step.clear_failure {
            engine.telemetry.clear_failure(&clr.instance);
        } else if step.advance_time.is_some() {
            // Virtual clock hook
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
