//! Automation engine: cron schedules, Event triggers (including durable replay),
//! Signal predicates, strictly executed under Administrator-granted Service principals (§7.3).

use crate::actions::ActionManager;
use crate::authority::{Principal, PrincipalKind};
use crate::events::{EventBus, EventEnvelope};
use crate::model::{CapabilityCeiling, ConfiguredGraph, InstanceConfig, LoadedDefinition};
use crate::query_engine::QueryEngine;
use crate::telemetry::Telemetry;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::Utc;
use dash_luau::{
    CompiledInstanceBundle, DefinitionCategory, ExecutionContextConfig, HostBridge,
    HttpHostRequest, HttpHostResponse, VmPool,
};
use dash_store::SqliteStore;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct AutomationEngine {
    graph: Arc<ArcSwap<ConfiguredGraph>>,
    pool: VmPool,
    store: SqliteStore,
    actions: ActionManager,
    queries: QueryEngine,
    events: EventBus,
    telemetry: Telemetry,
}

impl AutomationEngine {
    pub fn new(
        graph: Arc<ArcSwap<ConfiguredGraph>>,
        pool: VmPool,
        store: SqliteStore,
        actions: ActionManager,
        queries: QueryEngine,
        events: EventBus,
        telemetry: Telemetry,
    ) -> Self {
        Self {
            graph,
            pool,
            store,
            actions,
            queries,
            events,
            telemetry,
        }
    }

    /// Resolve a Service principal's Administrator-granted authority (never Workspace YAML alone!).
    pub fn resolve_service_principal(&self, principal_id: &str) -> Result<Principal, String> {
        let g = self.graph.load_full();
        let grant_val = if g.is_preview {
            // In isolated Preview, allow declared requested ceiling with Preview Mocks
            g.service_principals
                .get(principal_id)
                .map(|sp| serde_json::to_value(&sp.ceiling).unwrap_or_default())
        } else {
            self.store
                .get_service_principal_grant(principal_id)
                .map_err(|e| e.to_string())?
        };
        let Some(val) = grant_val else {
            return Err(format!(
                "service principal '{principal_id}' has no Administrator grant"
            ));
        };
        let ceiling: CapabilityCeiling =
            serde_json::from_value(val).map_err(|e| e.to_string())?;
        Ok(Principal {
            id: principal_id.to_string(),
            kind: PrincipalKind::ServicePrincipal,
            ceiling,
        })
    }

    /// Deliver an Event to every matching Automation instance and update durable consumer checkpoints.
    pub async fn dispatch_event(&self, event: &EventEnvelope) -> Vec<Result<serde_json::Value, String>> {
        let g = self.graph.load_full();
        let mut results = Vec::new();

        for (auto_id, inst) in &g.instances {
            if inst.category != DefinitionCategory::Automation || inst.enabled == Some(false) {
                continue;
            }
            let Some(def) = g.definitions.get(&inst.definition) else {
                continue;
            };
            let matches = def.manifest.triggers.iter().any(|t| {
                t.kind == "event"
                    && t.event_type.as_deref() == Some(event.semantic_type.as_str())
                    && match t.source_input.as_deref() {
                        Some(slot) => inst.inputs.get(slot).map(|s| s.as_str()) == Some(event.source_instance.as_str()),
                        None => true,
                    }
            });
            if !matches {
                continue;
            }

            if event.durable {
                let ckpt = self.events.get_checkpoint(auto_id, &event.source_instance);
                if event.sequence <= ckpt && !event.is_replay {
                    continue;
                }
            }

            let res = self.run_automation(auto_id, inst, def, &g, serde_json::to_value(event).unwrap_or_default()).await;
            match &res {
                Ok(_) => {
                    if event.durable {
                        let _ = self.events.ack_checkpoint(auto_id, &event.source_instance, event.sequence);
                    }
                }
                Err(err) => {
                    if event.durable {
                        let _ = self.events.send_to_dead_letter(auto_id, &event.event_id, 1, err);
                    }
                }
            }
            results.push(res);
        }
        results
    }

    /// Manually or cron-trigger one automation instance under its configured `run_as` Service principal.
    pub async fn trigger_automation(
        &self,
        automation_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let g = self.graph.load_full();
        let inst = g
            .instances
            .get(automation_id)
            .ok_or_else(|| format!("automation instance '{automation_id}' not found"))?
            .clone();
        if inst.category != DefinitionCategory::Automation {
            return Err(format!("instance '{automation_id}' is not an automation"));
        }
        let def = g
            .definitions
            .get(&inst.definition)
            .ok_or_else(|| format!("definition '{}' missing", inst.definition))?
            .clone();
        self.run_automation(automation_id, &inst, &def, &g, payload)
            .await
    }

    /// Validate a cron schedule expression (standard 5/6/7 field via `cron` crate).
    pub fn validate_cron_schedule(expr: &str) -> Result<(), String> {
        let normalized = if expr.split_whitespace().count() == 5 {
            format!("0 {expr}")
        } else {
            expr.to_string()
        };
        cron::Schedule::from_str(&normalized)
            .map(|_| ())
            .map_err(|e| format!("invalid cron expression '{expr}': {e}"))
    }

    async fn run_automation(
        &self,
        auto_id: &str,
        inst: &InstanceConfig,
        def: &LoadedDefinition,
        graph: &ConfiguredGraph,
        event_payload: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let sp_id = inst
            .run_as
            .as_deref()
            .ok_or_else(|| format!("automation '{auto_id}' missing 'run_as' Service principal"))?;
        let principal = self.resolve_service_principal(sp_id)?;
        let trace_id = format!("auto_{}", Uuid::now_v7().simple());

        let bridge = Arc::new(AutomationHostBridge {
            actions: self.actions.clone(),
            queries: self.queries.clone(),
            store: self.store.clone(),
            graph: graph.clone(),
            caller_inst: inst.clone(),
            principal: principal.clone(),
        });

        let bundle = CompiledInstanceBundle {
            instance_id: inst.id.clone(),
            manifest: def.manifest.clone(),
            runtime_source: def.runtime_source.clone(),
            config_digest: inst.config_digest(),
            resolved_libraries: graph.resolved_libraries(),
        };
        let handler = if def.manifest.handler.is_empty() {
            "run"
        } else {
            def.manifest.handler.as_str()
        };

        let out = self
            .pool
            .execute_handler(
                &bundle,
                handler,
                ExecutionContextConfig {
                    category: DefinitionCategory::Automation,
                    instance_id: inst.id.clone(),
                    input_names: inst.inputs.keys().cloned().collect(),
                    config: inst.config.clone(),
                    payload: event_payload,
                    operation: None,
                    idempotency_key: None,
                    allowed_emits: vec![],
                    now_iso: Utc::now().to_rfc3339(),
                    trace_id: trace_id.clone(),
                    bridge,
                },
                None,
            )
            .await
            .map_err(|e| self.store.redactor().redact_str(&e.to_string()));

        self.telemetry.log(
            if out.is_ok() { "info" } else { "error" },
            "automation",
            &principal.id,
            &trace_id,
            &format!("automation '{auto_id}' completed: {:?}", out.is_ok()),
        );
        out
    }
}

struct AutomationHostBridge {
    actions: ActionManager,
    queries: QueryEngine,
    store: SqliteStore,
    graph: ConfiguredGraph,
    caller_inst: InstanceConfig,
    principal: Principal,
}

#[async_trait]
impl HostBridge for AutomationHostBridge {
    async fn query_input(
        &self,
        input_name: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let target_id = self
            .caller_inst
            .inputs
            .get(input_name)
            .ok_or_else(|| format!("input '{input_name}' not bound"))?;
        let env = self
            .queries
            .query(target_id, params, &self.principal, false)
            .await?;
        Ok(env.value.unwrap_or(serde_json::Value::Null))
    }

    async fn call_input(
        &self,
        _input_name: &str,
        _operation: &str,
        _args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        Err("automations invoke actions or query data providers, not raw connectors".into())
    }

    async fn invoke_action(
        &self,
        input_name: &str,
        args: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<serde_json::Value, String> {
        let target_action = self
            .caller_inst
            .inputs
            .get(input_name)
            .ok_or_else(|| format!("action input '{input_name}' not bound"))?;
        let rec = self
            .actions
            .invoke(target_action, args, &self.principal, idempotency_key, None)
            .await?;
        serde_json::to_value(rec).map_err(|e| e.to_string())
    }

    async fn http_request(&self, _req: HttpHostRequest) -> Result<HttpHostResponse, String> {
        Err("automations cannot make raw HTTP requests".into())
    }

    async fn emit_event(
        &self,
        _event_type: &str,
        _payload: serde_json::Value,
        _durable: bool,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn state_get(&self, scope: &str, key: &str) -> Result<serde_json::Value, String> {
        let owner = if scope == "dashboard" {
            self.graph
                .dashboards
                .keys()
                .next()
                .cloned()
                .unwrap_or_else(|| "default".into())
        } else {
            self.caller_inst.id.clone()
        };
        let rec = self
            .store
            .get_state(scope, &owner, key)
            .map_err(|e| e.to_string())?;
        Ok(rec.map(|r| r.value).unwrap_or(serde_json::Value::Null))
    }

    async fn state_set(
        &self,
        scope: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), String> {
        self.store
            .set_state(
                scope,
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

    async fn load_fixture(&self, _fixture_path: &str) -> Result<serde_json::Value, String> {
        Err("fixtures unavailable in automations".into())
    }
}
