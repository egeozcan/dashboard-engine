//! Luau Definition test runner (`tests.luau`) executed against Mock bindings (§4.5).

use crate::host_api::{build_dash_runtime_sdk, ExecutionContextConfig, HostBridge, LuaContext};
use crate::manifest::DefinitionManifest;
use mlua::{Function, Lua, LuaOptions, StdLib, Table, Value, VmState};
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

/// Run a Definition's `tests.luau` inside a sandboxed VM against the provided HostBridge.
pub async fn run_definition_tests(
    manifest: &DefinitionManifest,
    runtime_source: &str,
    tests_source: &str,
    resolved_libraries: &BTreeMap<String, String>,
    bridge: Arc<dyn HostBridge>,
    config: serde_json::Value,
) -> Result<DefinitionTestReport, String> {
    let manifest_clone = manifest.clone();
    let runtime_src = runtime_source.to_string();
    let tests_src = tests_source.to_string();
    let libs_map = resolved_libraries.clone();
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || {
        handle.block_on(async move {
            let libs = StdLib::COROUTINE
                | StdLib::TABLE
                | StdLib::STRING
                | StdLib::UTF8
                | StdLib::BIT
                | StdLib::MATH
                | StdLib::BUFFER
                | StdLib::VECTOR;
            let lua = Lua::new_with(libs, LuaOptions::default()).map_err(|e| e.to_string())?;
            lua.set_memory_limit(32 * 1024 * 1024)
                .map_err(|e| e.to_string())?;

            let ticks = Arc::new(AtomicU64::new(0));
            let ticks_ref = Arc::clone(&ticks);
            lua.set_interrupt(move |_| {
                if ticks_ref.fetch_add(1, Ordering::Relaxed) > 2_000_000 {
                    return Err(mlua::Error::runtime("definition test instruction budget exceeded"));
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

            let runtime_val: Value = lua
                .load(&runtime_src)
                .set_name(format!("{}/runtime.luau", manifest_clone.id))
                .eval()
                .map_err(|e| format!("failed to load runtime.luau: {e}"))?;
            let runtime_key = lua
                .create_registry_value(runtime_val)
                .map_err(|e| e.to_string())?;

            let require_fn = lua
                .create_function(move |lua, name: String| -> mlua::Result<Value> {
                    if name == "dash" {
                        return Ok(Value::Table(dash.clone()));
                    }
                    if name == "runtime" || name == "./runtime" {
                        return lua.registry_value(&runtime_key);
                    }
                    if let Some(src) = libs_map.get(&name) {
                        return lua.load(src).set_name(format!("lib/{name}.luau")).eval();
                    }
                    Err(mlua::Error::runtime(format!(
                        "test module '{name}' is not a resolved package library"
                    )))
                })
                .map_err(|e| e.to_string())?;
            lua.globals()
                .set("require", require_fn)
                .map_err(|e| e.to_string())?;
            lua.sandbox(true).map_err(|e| e.to_string())?;

            lua.load(&tests_src)
                .set_name(format!("{}/tests.luau", manifest_clone.id))
                .exec()
                .map_err(|e| format!("failed to load tests.luau: {e}"))?;

            let tests: Vec<(String, mlua::RegistryKey)> =
                registered_tests.lock().unwrap().drain(..).collect();
            let mut cases = Vec::with_capacity(tests.len());
            let mut passed = 0;
            let mut failed = 0;

            let input_names: Vec<String> = manifest_clone.inputs.keys().cloned().collect();
            let allowed_emits: Vec<String> = manifest_clone
                .emits
                .iter()
                .map(|e| e.semantic_type.clone())
                .collect();

            for (name, key) in tests {
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
                        allowed_emits: allowed_emits.clone(),
                        now_iso: "2026-11-24T12:00:00Z".into(),
                        trace_id: format!("test_{}", uuid::Uuid::now_v7().simple()),
                        bridge: Arc::clone(&bridge),
                    };
                    lua.create_function(move |lua, overrides: Option<Table>| {
                        let mut c = cfg_base.clone();
                        if let Some(ov) = overrides {
                            if let Ok(q) = ov.get::<Value>("query") {
                                if !q.is_nil() {
                                    c.payload = mlua::LuaSerdeExt::from_value(lua, q)?;
                                }
                            }
                            if let Ok(a) = ov.get::<Value>("args") {
                                if !a.is_nil() {
                                    c.payload = mlua::LuaSerdeExt::from_value(lua, a)?;
                                }
                            }
                            if let Ok(op) = ov.get::<String>("operation") {
                                c.operation = Some(op);
                            }
                            if let Ok(ev) = ov.get::<Value>("event") {
                                if !ev.is_nil() {
                                    c.payload = mlua::LuaSerdeExt::from_value(lua, ev)?;
                                }
                            }
                        }
                        lua.create_userdata(LuaContext { cfg: c })
                    })
                    .map_err(|e| e.to_string())?
                };
                let outcome = tokio::time::timeout(
                    Duration::from_secs(5),
                    test_fn.call_async::<()>(ctx_builder),
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
