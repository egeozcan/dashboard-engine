//! Action execution lifecycle (§9), Dry run, nested calls, `unknown` reconcile,
//! manual resolution, invalidation, and widget Request dispatch.

use crate::authority::{
    audited, check_action_authority, check_connector_operation_read_only, Principal,
};
use crate::events::EventBus;
use crate::model::{ConfiguredGraph, InstanceConfig, LoadedDefinition};
use crate::query_engine::QueryEngine;
use crate::telemetry::Telemetry;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::Utc;
use dash_luau::{
    CompiledInstanceBundle, DefinitionCategory, ExecutionContextConfig, HostBridge,
    HttpHostRequest, HttpHostResponse, VmPool,
};
use dash_store::{ActionExecutionRecord, SqliteStore};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunReport {
    pub action_instance: String,
    pub feasible: bool,
    pub advisory: bool,
    pub warnings: Vec<String>,
    pub effects: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetRequestEnvelope {
    pub protocol: String, // "dash.widget/1"
    pub dashboard_id: String,
    pub adapter_instance: String,
    pub widget_id: String,
    pub generation: i64,
    pub sequence: u64,
    pub kind: String, // "request"
    pub request: String,
    pub payload: serde_json::Value,
}

#[derive(Clone)]
pub struct ActionManager {
    graph: Arc<ArcSwap<ConfiguredGraph>>,
    pool: VmPool,
    store: SqliteStore,
    queries: QueryEngine,
    events: EventBus,
    telemetry: Telemetry,
}

impl ActionManager {
    pub fn new(
        graph: Arc<ArcSwap<ConfiguredGraph>>,
        pool: VmPool,
        store: SqliteStore,
        queries: QueryEngine,
        events: EventBus,
        telemetry: Telemetry,
    ) -> Self {
        Self {
            graph,
            pool,
            store,
            queries,
            events,
            telemetry,
        }
    }

    pub fn dry_run<'a>(
        &'a self,
        action_instance: &'a str,
        args: serde_json::Value,
        principal: &'a Principal,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<DryRunReport, String>> + Send + 'a>>
    {
        Box::pin(async move {
            let g = self.graph.load_full();
            let inst = g
                .instances
                .get(action_instance)
                .ok_or_else(|| format!("action instance '{action_instance}' not found"))?
                .clone();
            if inst.category != DefinitionCategory::ActionProvider {
                return Err(format!("instance '{action_instance}' is not an action_provider"));
            }
            audited(&self.store, principal, "action.dry_run", check_action_authority(principal, &inst, &g, &self.store))
                .map_err(|e| e.to_string())?;

            let def = g
                .definitions
                .get(&inst.definition)
                .ok_or_else(|| format!("definition '{}' missing", inst.definition))?
                .clone();
            if let Some(ref args_schema) = def.manifest.args {
                args_schema
                    .validate_value(&args)
                    .map_err(|e| format!("invalid action arguments: {e}"))?;
            }

            if let Some(ref dry_handler) = def.manifest.dry_run_handler {
                let trace_id = format!("dry_{}", Uuid::now_v7().simple());
                let bridge = Arc::new(ActionHostBridge {
                    manager: self.clone(),
                    graph: (*g).clone(),
                    caller_inst: inst.clone(),
                    caller_def: def.clone(),
                    principal: principal.clone(),
                    trace_id: trace_id.clone(),
                    parent_execution_id: None,
                    is_read_only: true,
                    step_counter: Arc::new(std::sync::atomic::AtomicI64::new(0)),
                });
                let bundle = CompiledInstanceBundle {
                    instance_id: format!("dryrun:{}", inst.id),
                    manifest: def.manifest.clone(),
                    runtime_source: def.runtime_source.clone(),
                    config_digest: inst.config_digest(),
                    resolved_libraries: g.resolved_libraries(),
                };
                let val = self
                    .pool
                    .execute_handler(
                        &bundle,
                        dry_handler,
                        ExecutionContextConfig {
                            category: DefinitionCategory::ActionProvider,
                            instance_id: inst.id.clone(),
                            input_names: inst.inputs.keys().cloned().collect(),
                            config: inst.config.clone(),
                            payload: args,
                            operation: None,
                            idempotency_key: None,
                            allowed_emits: vec![],
                            now_iso: Utc::now().to_rfc3339(),
                            trace_id,
                            bridge,
                        },
                        None,
                    )
                    .await
                    .map_err(|e| self.store.redactor().redact_str(&e.to_string()))?;

                let feasible = val
                    .get("feasible")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                let warnings = val
                    .get("warnings")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                let effects = val.get("effects").cloned().unwrap_or(val);
                return Ok(DryRunReport {
                    action_instance: action_instance.to_string(),
                    feasible,
                    advisory: true,
                    warnings,
                    effects,
                });
            }

            Ok(DryRunReport {
                action_instance: action_instance.to_string(),
                feasible: true,
                advisory: true,
                warnings: vec![],
                effects: serde_json::json!({
                    "action": action_instance,
                    "args": self.store.redactor().redact_json(&args),
                    "note": "advisory estimate; no side effects performed",
                }),
            })
        })
    }

    pub fn invoke<'a>(
        &'a self,
        action_instance: &'a str,
        args: serde_json::Value,
        principal: &'a Principal,
        idempotency_key: Option<String>,
        parent_execution_id: Option<String>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ActionExecutionRecord, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let g = self.graph.load_full();
            let inst = g
                .instances
                .get(action_instance)
                .ok_or_else(|| format!("action instance '{action_instance}' not found"))?
                .clone();
            if inst.category != DefinitionCategory::ActionProvider {
                return Err(format!("instance '{action_instance}' is not an action_provider"));
            }

            // 1. Validate authority (auditing denials!)
            if let Err(auth_err) = check_action_authority(principal, &inst, &g, &self.store) {
                let _ = self.store.append_audit(
                    &principal.id,
                    principal.kind.as_str(),
                    "action.invoke",
                    action_instance,
                    "denied",
                    serde_json::json!({ "error": auth_err.to_string() }),
                );
                return Err(auth_err.to_string());
            }

            let def = g
                .definitions
                .get(&inst.definition)
                .ok_or_else(|| format!("definition '{}' missing", inst.definition))?
                .clone();

            // 2. Validate typed arguments
            if let Some(ref args_schema) = def.manifest.args {
                args_schema
                    .validate_value(&args)
                    .map_err(|e| format!("invalid action arguments: {e}"))?;
            }

            // 3. Idempotency deduplication (§9.2)
            if let Some(ref ikey) = idempotency_key {
                if let Ok(Some(existing)) = self
                    .store
                    .lookup_action_idempotency(action_instance, ikey)
                {
                    return Ok(existing);
                }
            }

            // 4. Create durable `queued` record before any side effects
            let execution_id = format!("act_{}", Uuid::now_v7().simple());
            let trace_id = format!("tr_{}", Uuid::now_v7().simple());
            let started_at = Utc::now().to_rfc3339();
            let started_instant = Instant::now();
            self.telemetry.actions_total.fetch_add(1, Ordering::Relaxed);

            let rec = ActionExecutionRecord {
                execution_id: execution_id.clone(),
                action_instance: action_instance.to_string(),
                parent_execution_id: parent_execution_id.clone(),
                principal_id: principal.id.clone(),
                principal_kind: principal.kind.as_str().into(),
                status: "queued".into(),
                idempotency_key: idempotency_key.clone(),
                args: args.clone(),
                result: None,
                reason: None,
                retry_safe: false,
                reconcile_attempts: 0,
                trace_id: trace_id.clone(),
                started_at: started_at.clone(),
                finished_at: None,
                duration_ms: None,
            };
            self.store
                .insert_action_execution(&rec)
                .map_err(|e| e.to_string())?;

            let _ = self.store.update_action_execution(
                &execution_id,
                "running",
                None,
                None,
                false,
                0,
                None,
                None,
            );

            // Check failure injection first (for tests / Scenarios)
            let run_res = if let Some(rule) = self.telemetry.take_injected_failure(action_instance) {
                if rule.mode == "unknown" {
                    Ok(serde_json::json!({
                        "__unknown": true,
                        "reason": rule.message
                    }))
                } else {
                    Err(format!("injected failure ({}): {}", rule.mode, rule.message))
                }
            } else {
                let bridge = Arc::new(ActionHostBridge {
                    manager: self.clone(),
                    graph: (*g).clone(),
                    caller_inst: inst.clone(),
                    caller_def: def.clone(),
                    principal: principal.clone(),
                    trace_id: trace_id.clone(),
                    parent_execution_id: Some(execution_id.clone()),
                    is_read_only: false,
                    step_counter: Arc::new(std::sync::atomic::AtomicI64::new(0)),
                });
                let bundle = CompiledInstanceBundle {
                    instance_id: inst.id.clone(),
                    manifest: def.manifest.clone(),
                    runtime_source: def.runtime_source.clone(),
                    config_digest: inst.config_digest(),
                    resolved_libraries: g.resolved_libraries(),
                };
                let handler = if def.manifest.handler.is_empty() {
                    "execute"
                } else {
                    def.manifest.handler.as_str()
                };
                let allowed_emits: Vec<String> = def
                    .manifest
                    .emits
                    .iter()
                    .map(|e| e.semantic_type.clone())
                    .collect();

                self.pool
                    .execute_handler(
                        &bundle,
                        handler,
                        ExecutionContextConfig {
                            category: DefinitionCategory::ActionProvider,
                            instance_id: inst.id.clone(),
                            input_names: inst.inputs.keys().cloned().collect(),
                            config: inst.config.clone(),
                            payload: args.clone(),
                            operation: None,
                            idempotency_key: idempotency_key.clone(),
                            allowed_emits,
                            now_iso: Utc::now().to_rfc3339(),
                            trace_id: trace_id.clone(),
                            bridge,
                        },
                        None,
                    )
                    .await
                    .map_err(|e| self.store.redactor().redact_str(&e.to_string()))
            };

            let duration_ms = started_instant.elapsed().as_millis() as i64;
            let finished_at = Utc::now().to_rfc3339();

            let (final_status, final_res, final_reason) = match run_res {
                Ok(val) => {
                    if val.get("__unknown").and_then(|v| v.as_bool()) == Some(true) {
                        let reason = val
                            .get("reason")
                            .and_then(|v| v.as_str())
                            .unwrap_or("upstream_connection_lost_after_request")
                            .to_string();
                        ("unknown".to_string(), None, Some(reason))
                    } else {
                        ("succeeded".to_string(), Some(val), None)
                    }
                }
                Err(err_msg) => {
                    if err_msg.contains("UNKNOWN_OUTCOME") || err_msg.contains("timed out") {
                        ("unknown".to_string(), None, Some(err_msg))
                    } else {
                        ("failed".to_string(), None, Some(err_msg))
                    }
                }
            };

            self.store
                .update_action_execution(
                    &execution_id,
                    &final_status,
                    final_res.clone(),
                    final_reason.as_deref(),
                    false,
                    0,
                    Some(&finished_at),
                    Some(duration_ms),
                )
                .map_err(|e| e.to_string())?;

            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "action.invoke",
                action_instance,
                &final_status,
                serde_json::json!({
                    "execution_id": execution_id,
                    "idempotency_key": idempotency_key,
                    "reason": final_reason,
                }),
            );

            self.telemetry.record_span(
                &trace_id,
                None,
                &format!("action:{action_instance}"),
                &principal.id,
                serde_json::json!({
                    "execution_id": execution_id,
                    "status": final_status,
                }),
                if final_status == "succeeded" { "ok" } else { "error" },
                duration_ms,
            );

            // Automatic completion event & query invalidation (§9.1 step 6)
            if final_status == "succeeded" {
                let _ = self.events.emit(
                    action_instance,
                    "dash.action_completed",
                    "1",
                    &trace_id,
                    true,
                    serde_json::json!({
                        "execution_id": execution_id,
                        "action_instance": action_instance,
                        "status": "succeeded",
                    }),
                );

                let to_invalidate = resolve_invalidations(&inst, &def, &g);
                if !to_invalidate.is_empty() {
                    self.queries.invalidate_instances(&to_invalidate).await;
                }
            } else if final_status == "unknown" && def.manifest.reconcile_handler.is_some() {
                // Attempt immediate read-only reconcile pass
                let _ = self.reconcile_execution(&execution_id, principal).await;
            }

            self.store
                .get_action_execution(&execution_id)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "action execution missing after update".into())
        })
    }

    /// Run an action's read-only `reconcile` handler under the original Principal (§9.1).
    pub async fn reconcile_execution(
        &self,
        execution_id: &str,
        principal: &Principal,
    ) -> Result<ActionExecutionRecord, String> {
        let rec = self
            .store
            .get_action_execution(execution_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("execution '{execution_id}' not found"))?;
        if rec.status != "unknown" {
            return Ok(rec);
        }

        let g = self.graph.load_full();
        let inst = g
            .instances
            .get(&rec.action_instance)
            .ok_or_else(|| format!("action instance '{}' not found", rec.action_instance))?
            .clone();
        let def = g
            .definitions
            .get(&inst.definition)
            .ok_or_else(|| format!("definition '{}' missing", inst.definition))?
            .clone();

        let Some(ref reconcile_handler) = def.manifest.reconcile_handler else {
            return Ok(rec);
        };

        audited(&self.store, principal, "action.reconcile", check_action_authority(principal, &inst, &g, &self.store))
            .map_err(|e| e.to_string())?;

        let attempts = rec.reconcile_attempts + 1;
        let bridge = Arc::new(ActionHostBridge {
            manager: self.clone(),
            graph: (*g).clone(),
            caller_inst: inst.clone(),
            caller_def: def.clone(),
            principal: principal.clone(),
            trace_id: rec.trace_id.clone(),
            parent_execution_id: Some(rec.execution_id.clone()),
            is_read_only: true,
            step_counter: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        });
        let bundle = CompiledInstanceBundle {
            instance_id: format!("reconcile:{}", inst.id),
            manifest: def.manifest.clone(),
            runtime_source: def.runtime_source.clone(),
            config_digest: inst.config_digest(),
            resolved_libraries: g.resolved_libraries(),
        };

        let val = self
            .pool
            .execute_handler(
                &bundle,
                reconcile_handler,
                ExecutionContextConfig {
                    category: DefinitionCategory::ActionProvider,
                    instance_id: inst.id.clone(),
                    input_names: inst.inputs.keys().cloned().collect(),
                    config: inst.config.clone(),
                    payload: rec.args.clone(),
                    operation: Some("reconcile".into()),
                    idempotency_key: rec.idempotency_key.clone(),
                    allowed_emits: vec![],
                    now_iso: Utc::now().to_rfc3339(),
                    trace_id: rec.trace_id.clone(),
                    bridge,
                },
                None,
            )
            .await
            .map_err(|e| e.to_string());

        let (next_status, next_res, next_reason): (String, Option<serde_json::Value>, Option<String>) = match val {
            Ok(v) => match v.get("status").and_then(|s| s.as_str()) {
                Some("succeeded") => ("succeeded".into(), Some(v), Some("reconciled_succeeded".into())),
                Some("failed") => ("failed".into(), Some(v), Some("reconciled_failed".into())),
                _ => ("unknown".into(), rec.result.clone(), rec.reason.clone()),
            },
            Err(e) => ("unknown".into(), rec.result.clone(), Some(format!("reconcile error: {e}"))),
        };

        let finished_at = Utc::now().to_rfc3339();
        self.store
            .update_action_execution(
                execution_id,
                &next_status,
                next_res,
                next_reason.as_deref(),
                false,
                attempts,
                Some(&finished_at),
                rec.duration_ms,
            )
            .map_err(|e| e.to_string())?;

        if next_status == "succeeded" {
            let to_invalidate = resolve_invalidations(&inst, &def, &g);
            if !to_invalidate.is_empty() {
                self.queries.invalidate_instances(&to_invalidate).await;
            }
        }

        self.store
            .get_action_execution(execution_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "execution disappeared".into())
    }

    /// Explicitly resolve an `unknown` action execution with audit logging (§9.1).
    pub async fn resolve_execution(
        &self,
        execution_id: &str,
        resolved_status: &str,
        reason: &str,
        principal: &Principal,
    ) -> Result<ActionExecutionRecord, String> {
        if !principal.ceiling.can_resolve_executions && !principal.ceiling.unrestricted {
            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "action.resolve",
                execution_id,
                "denied",
                serde_json::json!({ "reason": "principal lacks can_resolve_executions" }),
            );
            return Err("principal lacks authority to resolve unknown executions".into());
        }
        if resolved_status != "succeeded" && resolved_status != "failed" && resolved_status != "cancelled" {
            return Err("resolved_status must be 'succeeded', 'failed', or 'cancelled'".into());
        }

        let rec = self
            .store
            .get_action_execution(execution_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("execution '{execution_id}' not found"))?;
        if rec.status != "unknown" {
            return Err(format!("execution '{execution_id}' is '{}', not 'unknown'", rec.status));
        }

        let finished_at = Utc::now().to_rfc3339();
        self.store
            .update_action_execution(
                execution_id,
                resolved_status,
                rec.result.clone(),
                Some(reason),
                false,
                rec.reconcile_attempts,
                Some(&finished_at),
                rec.duration_ms,
            )
            .map_err(|e| e.to_string())?;

        self.store
            .append_audit(
                &principal.id,
                principal.kind.as_str(),
                "action.resolve",
                execution_id,
                resolved_status,
                serde_json::json!({
                    "action_instance": rec.action_instance,
                    "reason": reason,
                }),
            )
            .map_err(|e| e.to_string())?;

        if resolved_status == "succeeded" {
            let g = self.graph.load_full();
            if let (Some(inst), Some(def)) = (
                g.instances.get(&rec.action_instance),
                g.instances
                    .get(&rec.action_instance)
                    .and_then(|i| g.definitions.get(&i.definition)),
            ) {
                let to_invalidate = resolve_invalidations(inst, def, &g);
                if !to_invalidate.is_empty() {
                    self.queries.invalidate_instances(&to_invalidate).await;
                }
            }
        }

        self.store
            .get_action_execution(execution_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "execution disappeared".into())
    }

    /// Graceful drain on shutdown: incomplete in-flight actions become `unknown` (§15.2).
    pub fn mark_inflight_unknown_on_shutdown(&self) -> usize {
        self.store
            .mark_inflight_actions_unknown("graceful_shutdown_incomplete")
            .unwrap_or(0)
    }

    /// Validate and dispatch a browser Widget Request (`dash.widget/1`, Appendix A.3).
    pub async fn dispatch_widget_request(
        &self,
        req: WidgetRequestEnvelope,
        principal: &Principal,
    ) -> Result<ActionExecutionRecord, String> {
        let g = self.graph.load_full();
        if req.generation != g.generation_id {
            return Err(format!(
                "stale widget request generation {} (active generation is {})",
                req.generation, g.generation_id
            ));
        }

        let dash = g
            .dashboards
            .get(&req.dashboard_id)
            .ok_or_else(|| format!("dashboard '{}' not found", req.dashboard_id))?;

        if !dash
            .adapters
            .iter()
            .any(|p| p.adapter_instance == req.adapter_instance)
        {
            return Err(format!(
                "adapter '{}' is not placed on dashboard '{}'",
                req.adapter_instance, req.dashboard_id
            ));
        }
        if !req.widget_id.starts_with(&format!("{}_", req.adapter_instance)) {
            return Err(format!(
                "widget_id '{}' is not owned by adapter '{}'",
                req.widget_id, req.adapter_instance
            ));
        }

        let adapter_inst = g
            .instances
            .get(&req.adapter_instance)
            .ok_or_else(|| format!("adapter instance '{}' missing", req.adapter_instance))?
            .clone();
        let adapter_def = g
            .definitions
            .get(&adapter_inst.definition)
            .ok_or_else(|| format!("adapter definition '{}' missing", adapter_inst.definition))?
            .clone();

        if !adapter_def.manifest.accepts_requests.is_empty()
            && !adapter_def
                .manifest
                .accepts_requests
                .iter()
                .any(|r| r == &req.request)
        {
            return Err(format!(
                "adapter '{}' does not accept request '{}'",
                adapter_inst.id, req.request
            ));
        }

        // If Adapter declares `on_request`, invoke it in Luau with ActionHostBridge so it translates the Request
        // into authorized action call(s); otherwise map directly by bound input slot matching `req.request` or `action`.
        if let Some(ref on_req) = adapter_def.manifest.request_handler {
            let trace_id = format!("req_{}", Uuid::now_v7().simple());
            let bridge = Arc::new(ActionHostBridge {
                manager: self.clone(),
                graph: (*g).clone(),
                caller_inst: adapter_inst.clone(),
                caller_def: adapter_def.clone(),
                principal: principal.clone(),
                trace_id: trace_id.clone(),
                parent_execution_id: None,
                is_read_only: false,
                step_counter: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            });
            let bundle = CompiledInstanceBundle {
                instance_id: adapter_inst.id.clone(),
                manifest: adapter_def.manifest.clone(),
                runtime_source: adapter_def.runtime_source.clone(),
                config_digest: adapter_inst.config_digest(),
                resolved_libraries: g.resolved_libraries(),
            };
            let val = self
                .pool
                .execute_handler(
                    &bundle,
                    on_req,
                    ExecutionContextConfig {
                        category: DefinitionCategory::Adapter,
                        instance_id: adapter_inst.id.clone(),
                        input_names: adapter_inst.inputs.keys().cloned().collect(),
                        config: adapter_inst.config.clone(),
                        payload: serde_json::json!({
                            "widget_id": req.widget_id,
                            "request": req.request,
                            "payload": req.payload,
                        }),
                        operation: None,
                        idempotency_key: None,
                        allowed_emits: vec![],
                        now_iso: Utc::now().to_rfc3339(),
                        trace_id,
                        bridge,
                    },
                    None,
                )
                .await
                .map_err(|e| self.store.redactor().redact_str(&e.to_string()))?;

            if let Some(exec_id) = val.get("execution_id").and_then(|v| v.as_str()) {
                if let Ok(Some(rec)) = self.store.get_action_execution(exec_id) {
                    return Ok(rec);
                }
            }
            if let Some(action_slot) = val.get("action").and_then(|v| v.as_str()) {
                let target_action = adapter_inst
                    .inputs
                    .get(action_slot)
                    .map(|s| s.as_str())
                    .unwrap_or(action_slot);
                let args = val.get("args").cloned().unwrap_or(req.payload);
                let ikey = val
                    .get("idempotency_key")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                return self.invoke(target_action, args, principal, ikey, None).await;
            }
            if val.get("refresh").and_then(|v| v.as_bool()) == Some(true)
                || val.get("invalidate").is_some()
            {
                let mut targets: Vec<String> = Vec::new();
                if let Some(arr) = val.get("invalidate").and_then(|v| v.as_array()) {
                    for item in arr {
                        if let Some(s) = item.as_str() {
                            let resolved = adapter_inst
                                .inputs
                                .get(s)
                                .cloned()
                                .unwrap_or_else(|| s.to_string());
                            targets.push(resolved);
                        }
                    }
                } else {
                    for target_id in adapter_inst.inputs.values() {
                        if g.instances
                            .get(target_id)
                            .map(|i| i.category == DefinitionCategory::DataProvider)
                            .unwrap_or(false)
                        {
                            targets.push(target_id.clone());
                        }
                    }
                }
                if !targets.is_empty() {
                    self.queries.invalidate_instances(&targets).await;
                }
                let now = Utc::now().to_rfc3339();
                return Ok(ActionExecutionRecord {
                    execution_id: format!("req_{}", Uuid::now_v7().simple()),
                    action_instance: adapter_inst.id,
                    parent_execution_id: None,
                    principal_id: principal.id.clone(),
                    principal_kind: principal.kind.as_str().into(),
                    status: "succeeded".into(),
                    idempotency_key: None,
                    args: req.payload,
                    result: Some(serde_json::json!({ "refreshed": targets })),
                    reason: None,
                    retry_safe: true,
                    reconcile_attempts: 0,
                    trace_id: format!("req_{}", Uuid::now_v7().simple()),
                    started_at: now.clone(),
                    finished_at: Some(now),
                    duration_ms: Some(0),
                });
            }
            return Err("adapter on_request handler did not invoke or specify an action".into());
        }

        let target_action = adapter_inst
            .inputs
            .get(&req.request)
            .or_else(|| adapter_inst.inputs.get("action"))
            .ok_or_else(|| {
                format!(
                    "adapter '{}' has no action binding for request '{}'",
                    adapter_inst.id, req.request
                )
            })?
            .clone();

        self.invoke(&target_action, req.payload, principal, None, None)
            .await
    }
}

fn resolve_invalidations(
    inst: &InstanceConfig,
    def: &LoadedDefinition,
    graph: &ConfiguredGraph,
) -> Vec<String> {
    let mut out = Vec::new();
    for target in &def.manifest.invalidates {
        if graph.instances.contains_key(target) {
            out.push(target.clone());
        } else {
            // Also invalidate any data provider referencing the same connector or matching definition
            for (id, candidate) in &graph.instances {
                if candidate.definition == *target || id == target {
                    out.push(id.clone());
                } else if let Some(bound) = inst.inputs.get(target) {
                    if candidate.inputs.values().any(|v| v == bound) {
                        out.push(id.clone());
                    }
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

struct ActionHostBridge {
    manager: ActionManager,
    graph: ConfiguredGraph,
    caller_inst: InstanceConfig,
    caller_def: LoadedDefinition,
    principal: Principal,
    trace_id: String,
    parent_execution_id: Option<String>,
    is_read_only: bool,
    step_counter: Arc<std::sync::atomic::AtomicI64>,
}

#[async_trait]
impl HostBridge for ActionHostBridge {
    async fn query_input(
        &self,
        input_name: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let target_id = self
            .caller_inst
            .inputs
            .get(input_name)
            .ok_or_else(|| format!("input '{input_name}' not bound on '{}'", self.caller_inst.id))?;
        let env = self
            .manager
            .queries
            .query(target_id, params, &self.principal, false)
            .await?;
        Ok(env.value.unwrap_or(serde_json::Value::Null))
    }

    async fn call_input(
        &self,
        input_name: &str,
        operation: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let target_id = self
            .caller_inst
            .inputs
            .get(input_name)
            .ok_or_else(|| format!("input '{input_name}' not bound on '{}'", self.caller_inst.id))?;
        if self.is_read_only {
            // Dry run / reconcile may only execute operations the connector declares side-effect
            // free (`mode = "read"`, or read-only capabilities), never a name heuristic (§9, §20).
            let target_inst = self
                .graph
                .instances
                .get(target_id)
                .ok_or_else(|| format!("target instance '{target_id}' not found"))?;
            audited(
                &self.manager.store,
                &self.principal,
                "connector.invoke",
                check_connector_operation_read_only(&self.principal, target_inst, operation, &self.graph),
            )
            .map_err(|e| format!("dry_run/reconcile is read-only: {e}"))?;
        }
        self.manager
            .queries
            .invoke_connector(target_id, operation, args, &self.principal, &self.trace_id)
            .await
    }

    async fn invoke_action(
        &self,
        input_name: &str,
        args: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<serde_json::Value, String> {
        if self.is_read_only {
            return Err("nested action invocation is forbidden during dry_run/reconcile".into());
        }
        let target_id = self
            .caller_inst
            .inputs
            .get(input_name)
            .ok_or_else(|| format!("action input '{input_name}' not bound on '{}'", self.caller_inst.id))?;
        let child_rec = self
            .manager
            .invoke(
                target_id,
                args,
                &self.principal,
                idempotency_key,
                self.parent_execution_id.clone(),
            )
            .await?;
        if let Some(ref parent_id) = self.parent_execution_id {
            let order = self.step_counter.fetch_add(1, Ordering::Relaxed) + 1;
            let _ = self
                .manager
                .store
                .link_action_step(parent_id, &child_rec.execution_id, order);
        }
        serde_json::to_value(child_rec).map_err(|e| e.to_string())
    }

    async fn http_request(&self, _req: HttpHostRequest) -> Result<HttpHostResponse, String> {
        Err("actions use bound connector inputs, not raw ctx.http".into())
    }

    async fn emit_event(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        durable: bool,
    ) -> Result<(), String> {
        if self.is_read_only {
            return Ok(());
        }
        self.manager
            .events
            .emit(
                &self.caller_inst.id,
                event_type,
                "1",
                &self.trace_id,
                durable,
                payload,
            )
            .map(|_| ())
    }

    async fn state_get(&self, _scope: &str, key: &str) -> Result<serde_json::Value, String> {
        let rec = self
            .manager
            .store
            .get_state("instance", &self.caller_inst.id, key)
            .map_err(|e| e.to_string())?;
        Ok(rec.map(|r| r.value).unwrap_or(serde_json::Value::Null))
    }

    async fn state_set(
        &self,
        _scope: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), String> {
        if self.is_read_only {
            return Ok(());
        }
        self.manager
            .store
            .set_state(
                "instance",
                &self.caller_inst.id,
                key,
                1,
                self.graph.generation_id,
                value,
                None,
            )
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn load_fixture(&self, fixture_path: &str) -> Result<serde_json::Value, String> {
        self.caller_def
            .fixtures
            .get(fixture_path)
            .cloned()
            .ok_or_else(|| format!("fixture '{fixture_path}' missing"))
    }
}
