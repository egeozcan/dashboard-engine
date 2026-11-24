//! Sandboxed Luau declaration evaluator, coroutine runtime, VM pool, and quotas.

pub mod declaration;
pub mod host_api;
pub mod manifest;
pub mod pool;
pub mod test_runner;

pub use declaration::{evaluate_declaration, DeclarationError};
pub use host_api::{
    ExecutionContextConfig, FutureHandle, HostBridge, HttpHostRequest, HttpHostResponse,
    NullHostBridge,
};
pub use manifest::{
    compute_sources_digest, ConnectorOperationSpec, DefinitionCategory, DefinitionManifest,
    EventDeclSpec, InputRefSpec, SchemaSpec, TriggerDeclSpec, UpdateModes,
};
pub use pool::{CompiledInstanceBundle, ResourceBudget, RuntimeError, VmPool};
pub use test_runner::{run_definition_tests, DefinitionTestCaseResult, DefinitionTestReport};

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn test_declaration_deterministic_manifest_and_rejection() {
        let decl = r#"
local dash = require("dash")
return dash.data_provider {
    id = "engineering.linked_prs",
    runtime = "runtime.luau",
    handler = "query",
    inputs = {
        prs = dash.data_ref("github.pull_requests"),
        issues = dash.data_ref("jira.issues_by_keys"),
    },
    query = dash.object {
        repository = dash.string(),
        state = dash.enum { "open", "closed", "all" },
    },
    output = dash.list("engineering.linked_pr@1"),
    updates = {
        on_demand = true,
        subscribe = true,
    },
}
"#;
        let mut sources = BTreeMap::new();
        sources.insert("definition.luau".into(), decl.as_bytes().to_vec());
        sources.insert("runtime.luau".into(), b"return {}".to_vec());

        let m1 = evaluate_declaration(decl, &sources).unwrap();
        let m2 = evaluate_declaration(decl, &sources).unwrap();
        assert_eq!(m1.canonical_json().unwrap(), m2.canonical_json().unwrap());
        assert_eq!(m1.identity_version, m2.identity_version);
        assert_eq!(m1.category, DefinitionCategory::DataProvider);
        assert!(m1.generated_luau_types.contains("export type QueryParams"));

        // Non-deterministic declaration must be rejected
        let bad_nondet = r#"
local dash = require("dash")
local x = math.random()
return dash.data_provider { id = "bad.rand" }
"#;
        assert!(matches!(
            evaluate_declaration(bad_nondet, &sources),
            Err(DeclarationError::NonDeterministic(_))
        ));

        // Forbidden data_provider -> action binding must be rejected (§2.1)
        let forbidden = r#"
local dash = require("dash")
return dash.data_provider {
    id = "bad.provider",
    inputs = { merge = dash.action_ref("github.merge_pr") },
}
"#;
        assert!(matches!(
            evaluate_declaration(forbidden, &sources),
            Err(DeclarationError::InvalidContract(_))
        ));

        // Forbidden adapter -> connector binding must be rejected (§2.1)
        let forbidden_adapter = r#"
local dash = require("dash")
return dash.adapter {
    id = "bad.adapter",
    inputs = { gh = dash.connector_ref("github.connector") },
}
"#;
        assert!(matches!(
            evaluate_declaration(forbidden_adapter, &sources),
            Err(DeclarationError::InvalidContract(_))
        ));
    }

    struct MockAsyncBridge;

    #[async_trait]
    impl HostBridge for MockAsyncBridge {
        async fn query_input(
            &self,
            input_name: &str,
            params: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            tokio::time::sleep(Duration::from_millis(15)).await;
            if input_name == "prs" {
                Ok(json!([
                    { "number": 42, "title": "DEV-123 Improve caching", "repository": params["repository"] }
                ]))
            } else if input_name == "issues" {
                Ok(json!([
                    { "key": "DEV-123", "status": "In Review" }
                ]))
            } else {
                Err(format!("unknown input {input_name}"))
            }
        }
        async fn call_input(
            &self,
            _input_name: &str,
            _operation: &str,
            _args: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            Ok(json!({}))
        }
        async fn invoke_action(
            &self,
            _input_name: &str,
            _args: serde_json::Value,
            _idempotency_key: Option<String>,
        ) -> Result<serde_json::Value, String> {
            Ok(json!({}))
        }
        async fn http_request(&self, _req: HttpHostRequest) -> Result<HttpHostResponse, String> {
            Err("blocked".into())
        }
        async fn emit_event(
            &self,
            _event_type: &str,
            _payload: serde_json::Value,
            _durable: bool,
        ) -> Result<(), String> {
            Ok(())
        }
        async fn state_get(&self, _scope: &str, _key: &str) -> Result<serde_json::Value, String> {
            Ok(json!(null))
        }
        async fn state_set(
            &self,
            _scope: &str,
            _key: &str,
            _value: serde_json::Value,
        ) -> Result<(), String> {
            Ok(())
        }
        async fn load_fixture(&self, _fixture_path: &str) -> Result<serde_json::Value, String> {
            Ok(json!([]))
        }
    }

    #[tokio::test]
    async fn test_async_coroutine_bridge_libraries_and_quotas() {
        let decl = r#"
local dash = require("dash")
return dash.data_provider {
    id = "engineering.linked_prs",
    inputs = {
        prs = dash.data_ref("github.pull_requests"),
        issues = dash.data_ref("jira.issues_by_keys"),
    },
}
"#;
        let runtime_src = r#"
local helpers = require("engineering.helpers")
local Runtime = {}
function Runtime.query(ctx)
    local prs = ctx:await(ctx.inputs.prs:query({ repository = ctx.query.repository }))
    local issues = ctx:await(ctx.inputs.issues:query({ keys = { "DEV-123" } }))
    return {
        joined = helpers.join_count(prs, issues),
        first_pr = prs[1],
        first_issue = issues[1],
    }
end
return Runtime
"#;
        let mut sources = BTreeMap::new();
        sources.insert("definition.luau".into(), decl.as_bytes().to_vec());
        sources.insert("runtime.luau".into(), runtime_src.as_bytes().to_vec());
        let manifest = evaluate_declaration(decl, &sources).unwrap();

        let mut libs = BTreeMap::new();
        libs.insert(
            "engineering.helpers".into(),
            "local M = {}\nfunction M.join_count(a, b) return #a + #b end\nreturn M".into(),
        );

        let bundle = CompiledInstanceBundle {
            instance_id: "linked_prs".into(),
            manifest: manifest.clone(),
            runtime_source: runtime_src.into(),
            config_digest: "cfg1".into(),
            resolved_libraries: libs,
        };

        let pool = VmPool::new(ResourceBudget {
            memory_limit_bytes: 4 * 1024 * 1024,
            wall_timeout: Duration::from_millis(120),
            max_interrupts: 500_000,
            idle_ttl: Duration::from_millis(50),
        });

        let bridge = Arc::new(MockAsyncBridge);
        let res = pool
            .execute_handler(
                &bundle,
                "query",
                ExecutionContextConfig {
                    category: DefinitionCategory::DataProvider,
                    instance_id: "linked_prs".into(),
                    input_names: vec!["prs".into(), "issues".into()],
                    config: json!({}),
                    payload: json!({ "repository": "acme/platform" }),
                    operation: None,
                    idempotency_key: None,
                    allowed_emits: vec![],
                    now_iso: "2026-11-24T12:00:00Z".into(),
                    trace_id: "tr_1".into(),
                    bridge: bridge.clone(),
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(res["joined"], 2);
        assert_eq!(res["first_pr"]["number"], 42);
        assert_eq!(res["first_issue"]["key"], "DEV-123");

        // Verify Generation carry-over preserves VM UID when unchanged
        let uid_before = pool.vm_uid("linked_prs").unwrap();
        pool.sync_generation(&[bundle.clone()]);
        assert_eq!(pool.vm_uid("linked_prs").unwrap(), uid_before);

        // Verify sandbox denies io / os / unallowed require
        let sandbox_bundle = CompiledInstanceBundle {
            instance_id: "evil_sandbox".into(),
            manifest: manifest.clone(),
            runtime_source: r#"
local R = {}
function R.query(ctx)
    if io ~= nil or os ~= nil or debug ~= nil then
        error("OS/IO/DEBUG leaked!")
    end
    require("evil.host_fs")
    return {}
end
return R
"#
            .into(),
            config_digest: "c1".into(),
            resolved_libraries: BTreeMap::new(),
        };
        let err = pool
            .execute_handler(
                &sandbox_bundle,
                "query",
                ExecutionContextConfig {
                    category: DefinitionCategory::DataProvider,
                    instance_id: "evil_sandbox".into(),
                    input_names: vec![],
                    config: json!({}),
                    payload: json!({}),
                    operation: None,
                    idempotency_key: None,
                    allowed_emits: vec![],
                    now_iso: "2026-11-24T12:00:00Z".into(),
                    trace_id: "tr_2".into(),
                    bridge: bridge.clone(),
                },
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("SANDBOX_DENIED"));

        // Verify infinite loop is interrupted while async Tokio tasks remain responsive (< 10ms)
        let loop_bundle = CompiledInstanceBundle {
            instance_id: "infinite_looper".into(),
            manifest: manifest.clone(),
            runtime_source: "local R = {}\nfunction R.query(ctx) while true do end end\nreturn R".into(),
            config_digest: "c1".into(),
            resolved_libraries: BTreeMap::new(),
        };
        let pool_clone = pool.clone();
        let bridge_clone = bridge.clone();
        let loop_task = tokio::spawn(async move {
            pool_clone
                .execute_handler(
                    &loop_bundle,
                    "query",
                    ExecutionContextConfig {
                        category: DefinitionCategory::DataProvider,
                        instance_id: "infinite_looper".into(),
                        input_names: vec![],
                        config: json!({}),
                        payload: json!({}),
                        operation: None,
                        idempotency_key: None,
                        allowed_emits: vec![],
                        now_iso: "2026-11-24T12:00:00Z".into(),
                        trace_id: "tr_loop".into(),
                        bridge: bridge_clone,
                    },
                    None,
                )
                .await
        });
        // Concurrent Tokio ping must not stall behind the CPU-spinning Luau loop
        let ping_start = Instant::now();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(ping_start.elapsed() < Duration::from_millis(40));

        let loop_res = loop_task.await.unwrap();
        assert!(matches!(loop_res, Err(RuntimeError::Timeout(_))));

        // Verify memory exhaustion triggers MemoryExceeded
        let oom_bundle = CompiledInstanceBundle {
            instance_id: "oom_inst".into(),
            manifest,
            runtime_source: r#"
local R = {}
function R.query(ctx)
    local t = {}
    local s = string.rep("A", 1024 * 64)
    for i = 1, 200 do
        t[i] = s .. tostring(i)
    end
    return #t
end
return R
"#
            .into(),
            config_digest: "c1".into(),
            resolved_libraries: BTreeMap::new(),
        };
        let oom_res = pool
            .execute_handler(
                &oom_bundle,
                "query",
                ExecutionContextConfig {
                    category: DefinitionCategory::DataProvider,
                    instance_id: "oom_inst".into(),
                    input_names: vec![],
                    config: json!({}),
                    payload: json!({}),
                    operation: None,
                    idempotency_key: None,
                    allowed_emits: vec![],
                    now_iso: "2026-11-24T12:00:00Z".into(),
                    trace_id: "tr_oom".into(),
                    bridge,
                },
                None,
            )
            .await;
        assert!(matches!(oom_res, Err(RuntimeError::MemoryExceeded(_))));

        // Verify idle VM reclamation
        tokio::time::sleep(Duration::from_millis(60)).await;
        let reclaimed = pool.reclaim_idle(Some(Duration::from_millis(50)));
        assert!(reclaimed >= 1);
    }
}
