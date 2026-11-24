//! Luau Definition test runner (`tests.luau`, §4.5).
//!
//! The runner executes tests in a sandbox built exactly like an Instance VM (same stdlib subset,
//! memory limit and controlled `require`). Inputs are served by the supplied `HostBridge`; for
//! activation and the `definition_test` tool that bridge resolves every input to Mocks (see
//! `dash_git::preview::MockTestBridge`).
//!
//! Budgets: the instruction budget and wall-clock deadline (from `ResourceBudget`) apply
//! separately to loading `runtime.luau`, loading `tests.luau`, and to each test case, so one
//! expensive test can neither starve later tests nor make results depend on test order. A
//! `tests.luau` that registers no tests is reported as a failure.

use crate::host_api::{build_dash_runtime_sdk, ExecutionContextConfig, HostBridge, LuaContext};
use crate::manifest::DefinitionManifest;
use crate::pool::{new_sandbox_lua, with_execution_deadline, ModuleLoader, ResourceBudget};
use mlua::{Function, Table, Value, VmState};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefinitionTestCaseResult {
    pub name: String,
    pub passed: bool,
    pub error: Option<String>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefinitionTestReport {
    pub definition_id: String,
    pub passed: usize,
    pub failed: usize,
    pub cases: Vec<DefinitionTestCaseResult>,
}

/// Name of the synthetic failing case reported when `tests.luau` registers no tests.
pub const NO_TESTS_CASE: &str = "tests.luau registers at least one test";

/// Per-phase budget shared with the interrupt callback: reset before every phase/test.
struct PhaseBudget {
    base: Instant,
    ticks: AtomicU64,
    /// Deadline as milliseconds since `base`.
    deadline_ms: AtomicU64,
}

impl PhaseBudget {
    fn reset(&self, timeout: Duration) -> Instant {
        self.ticks.store(0, Ordering::Relaxed);
        let deadline = Instant::now() + timeout;
        self.deadline_ms.store(
            deadline.duration_since(self.base).as_millis() as u64,
            Ordering::Relaxed,
        );
        deadline
    }
}

/// Run a Definition's `tests.luau` inside a sandboxed VM against the provided HostBridge, using
/// the default `ResourceBudget`.
pub async fn run_definition_tests(
    manifest: &DefinitionManifest,
    runtime_source: &str,
    tests_source: &str,
    resolved_libraries: &BTreeMap<String, String>,
    bridge: Arc<dyn HostBridge>,
    config: serde_json::Value,
) -> Result<DefinitionTestReport, String> {
    run_definition_tests_with_budget(
        manifest,
        runtime_source,
        tests_source,
        resolved_libraries,
        bridge,
        config,
        &ResourceBudget::default(),
    )
    .await
}

/// Run a Definition's `tests.luau` with explicit resource limits. `memory_limit_bytes` bounds the
/// test VM; `max_interrupts` and `wall_timeout` bound each phase and each test case.
pub async fn run_definition_tests_with_budget(
    manifest: &DefinitionManifest,
    runtime_source: &str,
    tests_source: &str,
    resolved_libraries: &BTreeMap<String, String>,
    bridge: Arc<dyn HostBridge>,
    config: serde_json::Value,
    budget: &ResourceBudget,
) -> Result<DefinitionTestReport, String> {
    let manifest_clone = manifest.clone();
    let runtime_src = runtime_source.to_string();
    let tests_src = tests_source.to_string();
    let libs_map = resolved_libraries.clone();
    let budget = budget.clone();
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || {
        handle.block_on(async move {
            let lua = new_sandbox_lua(budget.memory_limit_bytes).map_err(|e| e.to_string())?;

            let phase = Arc::new(PhaseBudget {
                base: Instant::now(),
                ticks: AtomicU64::new(0),
                deadline_ms: AtomicU64::new(0),
            });
            phase.reset(budget.wall_timeout);
            let phase_ref = Arc::clone(&phase);
            let max_interrupts = budget.max_interrupts;
            lua.set_interrupt(move |_| {
                if phase_ref.ticks.fetch_add(1, Ordering::Relaxed) > max_interrupts {
                    return Err(mlua::Error::runtime(
                        "definition test instruction budget exceeded",
                    ));
                }
                let now_ms = phase_ref.base.elapsed().as_millis() as u64;
                if now_ms > phase_ref.deadline_ms.load(Ordering::Relaxed) {
                    return Err(mlua::Error::runtime("definition test timed out"));
                }
                Ok(VmState::Continue)
            });

            let dash = build_dash_runtime_sdk(&lua).map_err(|e| e.to_string())?;
            let registered_tests: Arc<std::sync::Mutex<Vec<(String, mlua::RegistryKey)>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            let reg_ref = Arc::clone(&registered_tests);
            dash.set(
                "test",
                lua.create_function(move |lua, (name, fn_val): (String, Function)| {
                    let key = lua.create_registry_value(fn_val)?;
                    reg_ref.lock().unwrap().push((name, key));
                    Ok(())
                })
                .map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;

            dash.set(
                "assert",
                lua.create_function(|_, (cond, msg): (bool, Option<String>)| {
                    if !cond {
                        Err(mlua::Error::runtime(
                            msg.unwrap_or_else(|| "assertion failed".into()),
                        ))
                    } else {
                        Ok(())
                    }
                })
                .map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;

            dash.set(
                "assert_eq",
                lua.create_function(|lua, (a, b, msg): (Value, Value, Option<String>)| {
                    let ja: serde_json::Value = mlua::LuaSerdeExt::from_value(lua, a)?;
                    let jb: serde_json::Value = mlua::LuaSerdeExt::from_value(lua, b)?;
                    if ja != jb {
                        Err(mlua::Error::runtime(format!(
                            "{}: expected {jb}, got {ja}",
                            msg.unwrap_or_else(|| "assert_eq failed".into())
                        )))
                    } else {
                        Ok(())
                    }
                })
                .map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;

            // Same controlled loader as Instance VMs: only declared, resolved libraries.
            let loader = ModuleLoader::new(libs_map, &manifest_clone.libraries);
            loader.install(&lua, dash).map_err(|e| e.to_string())?;
            lua.sandbox(true).map_err(|e| e.to_string())?;

            phase.reset(budget.wall_timeout);
            let runtime_val: Value = lua
                .load(&runtime_src)
                .set_name(format!("{}/runtime.luau", manifest_clone.id))
                .eval()
                .map_err(|e| format!("failed to load runtime.luau: {e}"))?;
            let rk = lua
                .create_registry_value(runtime_val.clone())
                .map_err(|e| e.to_string())?;
            loader.preload("runtime", rk);
            let rk2 = lua
                .create_registry_value(runtime_val)
                .map_err(|e| e.to_string())?;
            loader.preload("./runtime", rk2);

            phase.reset(budget.wall_timeout);
            lua.load(&tests_src)
                .set_name(format!("{}/tests.luau", manifest_clone.id))
                .exec()
                .map_err(|e| format!("failed to load tests.luau: {e}"))?;

            let tests: Vec<(String, mlua::RegistryKey)> =
                registered_tests.lock().unwrap().drain(..).collect();
            let mut cases = Vec::with_capacity(tests.len().max(1));
            let mut passed = 0;
            let mut failed = 0;

            if tests.is_empty() {
                failed += 1;
                cases.push(DefinitionTestCaseResult {
                    name: NO_TESTS_CASE.into(),
                    passed: false,
                    error: Some("tests.luau registered no tests via dash.test(...)".into()),
                    duration_ms: 0,
                });
            }

            let input_names: Vec<String> = manifest_clone.inputs.keys().cloned().collect();
            let emit_decls = manifest_clone.emits.clone();

            for (name, key) in tests {
                bridge.begin_test_case(&name).await;
                let start = Instant::now();
                let test_fn: Function = lua.registry_value(&key).map_err(|e| e.to_string())?;
                let ctx_builder = {
                    let cfg_base = ExecutionContextConfig {
                        category: manifest_clone.category,
                        instance_id: format!("test:{}", manifest_clone.id),
                        input_names: input_names.clone(),
                        config: config.clone(),
                        payload: serde_json::json!({}),
                        operation: None,
                        idempotency_key: None,
                        allowed_emits: vec![],
                        now_iso: "2026-11-24T12:00:00Z".into(),
                        trace_id: format!("test_{}", uuid::Uuid::now_v7().simple()),
                        bridge: Arc::clone(&bridge),
                    };
                    let decls = emit_decls.clone();
                    lua.create_function(move |lua, overrides: Option<Table>| {
                        let mut c = cfg_base.clone();
                        if let Some(ov) = overrides {
                            for field in ["query", "args", "event", "request"] {
                                if let Ok(v) = ov.get::<Value>(field) {
                                    if !v.is_nil() {
                                        c.payload = mlua::LuaSerdeExt::from_value(lua, v)?;
                                    }
                                }
                            }
                            if let Ok(cfgv) = ov.get::<Value>("config") {
                                if !cfgv.is_nil() {
                                    c.config = mlua::LuaSerdeExt::from_value(lua, cfgv)?;
                                }
                            }
                            if let Ok(op) = ov.get::<String>("operation") {
                                c.operation = Some(op);
                            }
                            if let Ok(k) = ov.get::<String>("idempotency_key") {
                                c.idempotency_key = Some(k);
                            }
                        }
                        lua.create_userdata(LuaContext::with_emit_declarations(c, decls.clone()))
                    })
                    .map_err(|e| e.to_string())?
                };
                // Fresh instruction budget and deadline for every test case. The interrupt
                // enforces them while Luau runs; `timeout_at` bounds time spent in host awaits.
                let deadline = phase.reset(budget.wall_timeout);
                let outcome = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    with_execution_deadline(deadline, test_fn.call_async::<()>(ctx_builder)),
                )
                .await;
                let duration_ms = start.elapsed().as_millis() as u64;
                match outcome {
                    Ok(Ok(())) => {
                        passed += 1;
                        cases.push(DefinitionTestCaseResult {
                            name,
                            passed: true,
                            error: None,
                            duration_ms,
                        });
                    }
                    Ok(Err(e)) => {
                        failed += 1;
                        cases.push(DefinitionTestCaseResult {
                            name,
                            passed: false,
                            error: Some(e.to_string()),
                            duration_ms,
                        });
                    }
                    Err(_) => {
                        failed += 1;
                        cases.push(DefinitionTestCaseResult {
                            name,
                            passed: false,
                            error: Some("test timed out".into()),
                            duration_ms,
                        });
                    }
                }
            }

            Ok(DefinitionTestReport {
                definition_id: manifest_clone.id,
                passed,
                failed,
                cases,
            })
        })
    })
    .await
    .map_err(|e| e.to_string())?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::declaration::evaluate_declaration;
    use crate::pool::tests::ProbeBridge;

    fn manifest(decl: &str) -> DefinitionManifest {
        let mut sources = BTreeMap::new();
        sources.insert("definition.luau".into(), decl.as_bytes().to_vec());
        evaluate_declaration(decl, &sources).unwrap()
    }

    const DECL: &str = r#"local dash = require("dash"); return dash.data_provider { id = "t.tested", libraries = { "pkg.ok" } }"#;
    const RT: &str = "local R = {}\nfunction R.query(ctx) return 1 end\nreturn R";

    #[tokio::test]
    async fn instruction_budget_is_reset_for_each_test() {
        // Each test burns well over half the per-test budget; with a shared counter the
        // later tests would fail.
        let tests = r#"
local dash = require("dash")
local function burn() local x = 0; for i = 1, 60000 do x = x + i end; return x end
dash.test("one", function() burn() end)
dash.test("two", function() burn() end)
dash.test("three", function() burn() end)
"#;
        let budget = ResourceBudget {
            max_interrupts: 100_000,
            ..ResourceBudget::default()
        };
        // Sanity: one test alone really uses a large share of the budget.
        let probe = ResourceBudget {
            max_interrupts: 5_000,
            ..ResourceBudget::default()
        };
        let m = manifest(DECL);
        let single = r#"
local dash = require("dash")
dash.test("one", function() local x = 0; for i = 1, 60000 do x = x + i end end)
"#;
        let r = run_definition_tests_with_budget(
            &m,
            RT,
            single,
            &BTreeMap::new(),
            Arc::new(ProbeBridge::default()),
            serde_json::json!({}),
            &probe,
        )
        .await
        .unwrap();
        assert_eq!(r.failed, 1, "probe budget should be exceeded: {r:?}");

        let r = run_definition_tests_with_budget(
            &m,
            RT,
            tests,
            &BTreeMap::new(),
            Arc::new(ProbeBridge::default()),
            serde_json::json!({}),
            &budget,
        )
        .await
        .unwrap();
        assert_eq!((r.passed, r.failed), (3, 0), "{r:?}");
    }

    #[tokio::test]
    async fn cpu_bound_test_is_stopped_by_wall_clock_and_others_still_run() {
        let tests = r#"
local dash = require("dash")
dash.test("spins", function() while true do end end)
dash.test("fine", function() dash.assert(true) end)
"#;
        let budget = ResourceBudget {
            max_interrupts: u64::MAX / 2,
            wall_timeout: std::time::Duration::from_millis(150),
            ..ResourceBudget::default()
        };
        let t0 = Instant::now();
        let r = run_definition_tests_with_budget(
            &manifest(DECL),
            RT,
            tests,
            &BTreeMap::new(),
            Arc::new(ProbeBridge::default()),
            serde_json::json!({}),
            &budget,
        )
        .await
        .unwrap();
        assert!(t0.elapsed() < std::time::Duration::from_secs(3));
        assert_eq!((r.passed, r.failed), (1, 1), "{r:?}");
        assert!(
            r.cases[0]
                .error
                .as_deref()
                .unwrap_or("")
                .contains("timed out"),
            "{r:?}"
        );
    }

    #[tokio::test]
    async fn host_await_is_bounded_per_test() {
        let decl = r#"local dash = require("dash"); return dash.data_provider { id = "t.awaits", inputs = { up = dash.data_ref("t.up") } }"#;
        let rt = "local R = {}\nfunction R.query(ctx) return ctx:await(ctx.inputs.up:query({})) end\nreturn R";
        let tests = r#"
local dash = require("dash")
local Runtime = require("runtime")
dash.test("slow input", function(make_ctx) Runtime.query(make_ctx({})) end)
"#;
        let budget = ResourceBudget {
            wall_timeout: std::time::Duration::from_millis(100),
            ..ResourceBudget::default()
        };
        let bridge = Arc::new(ProbeBridge {
            sleep: std::time::Duration::from_secs(5),
            ..Default::default()
        });
        let t0 = Instant::now();
        let r = run_definition_tests_with_budget(
            &manifest(decl),
            rt,
            tests,
            &BTreeMap::new(),
            bridge,
            serde_json::json!({}),
            &budget,
        )
        .await
        .unwrap();
        assert!(t0.elapsed() < std::time::Duration::from_secs(2));
        assert_eq!(r.failed, 1);
        // Either the host-await deadline or the interrupt (on resumption) reports the timeout.
        assert!(
            r.cases[0]
                .error
                .as_deref()
                .unwrap_or("")
                .contains("timed out"),
            "{r:?}"
        );
    }

    #[tokio::test]
    async fn zero_tests_is_a_failure_and_require_is_restricted() {
        let r = run_definition_tests(
            &manifest(DECL),
            RT,
            "local dash = require(\"dash\")",
            &BTreeMap::new(),
            Arc::new(ProbeBridge::default()),
            serde_json::json!({}),
        )
        .await
        .unwrap();
        assert_eq!((r.passed, r.failed), (0, 1));
        assert_eq!(r.cases[0].name, NO_TESTS_CASE);

        let mut libs = BTreeMap::new();
        libs.insert("pkg.ok".to_string(), "return { v = 1 }".to_string());
        libs.insert("pkg.other".to_string(), "return { v = 2 }".to_string());
        let tests = r#"
local dash = require("dash")
dash.test("declared", function() dash.assert_eq(require("pkg.ok").v, 1) end)
dash.test("undeclared", function() require("pkg.other") end)
"#;
        let r = run_definition_tests(
            &manifest(DECL),
            RT,
            tests,
            &libs,
            Arc::new(ProbeBridge::default()),
            serde_json::json!({}),
        )
        .await
        .unwrap();
        assert_eq!((r.passed, r.failed), (1, 1), "{r:?}");
        assert!(r.cases[1]
            .error
            .as_deref()
            .unwrap()
            .contains("SANDBOX_DENIED"));
    }
}
