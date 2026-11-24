//! Shared `AppServices` layer powering HTTP API, WebSocket, MCP, and CLI identically (§11.2, §14.1).

use chrono::{DateTime, Utc};
use dash_core::{
    validate_configured_graph, CapabilityCeiling, ConfiguredGraph, DashboardDoc, DryRunReport,
    ExecutionGraphSnapshot, HistorySeriesConfig, InstanceConfig, Principal,
    QueryEnvelope, RuntimeEngine, RuntimeMetricsSnapshot, SanitizedLogEntry, ValidationReport,
    WidgetRequestEnvelope,
};
use dash_git::{
    read_packages_toml, write_packages_toml, ActivationResult, DraftManager, GitCommitInfo,
    GitError, GitStatusReport, PackageDependencySpec, PackagesLock, PreviewTestReport,
};
use dash_luau::{run_definition_tests, DefinitionCategory, DefinitionTestReport, NullHostBridge};
use dash_store::{
    ActionExecutionRecord, AgentPrincipalRecord, AuditRecord, DraftRecord, GitSyncRecord,
    SecretMetadata, SqliteStore, TraceSpanRecord,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AppServices {
    pub drafts: DraftManager,
    pub trusted_proxies: Vec<IpAddr>,
    pub allow_insecure_cookie_on_loopback: bool,
    pub admin_cli_token_path: PathBuf,
    last_history_sample: Arc<Mutex<HashMap<String, i64>>>,
    last_poll_run: Arc<Mutex<HashMap<String, i64>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub id: String,
    pub package: String,
    pub category: String,
    pub description: String,
    pub identity_version: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicePrincipalStatus {
    pub id: String,
    pub description: String,
    pub requested_ceiling: CapabilityCeiling,
    pub granted_ceiling: Option<CapabilityCeiling>,
    pub granted_by: Option<String>,
    pub granted_at: Option<String>,
}

impl AppServices {
    pub fn new(drafts: DraftManager, trusted_proxies: Vec<IpAddr>) -> Self {
        let admin_cli_token_path = drafts.data_dir().join("admin.token");
        Self {
            drafts,
            trusted_proxies,
            allow_insecure_cookie_on_loopback: true,
            admin_cli_token_path,
            last_history_sample: Arc::new(Mutex::new(HashMap::new())),
            last_poll_run: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn store(&self) -> &SqliteStore {
        self.drafts.store()
    }

    pub fn engine(&self) -> &RuntimeEngine {
        self.drafts.engine()
    }

    pub fn graph(&self) -> Arc<ConfiguredGraph> {
        self.engine().graph.load_full()
    }

    /// Spawn background maintenance loops: idle VM reclamation, durable Event automation consumer,
    /// provider polling, and History sampling under Administrator-granted Service principals.
    pub fn spawn_background_workers(&self) -> tokio::task::JoinHandle<()> {
        let services = self.clone();
        let mut event_rx = services.engine().events.subscribe();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        services.engine().pool.reclaim_idle(None);
                        services.run_due_history_samples().await;
                        services.run_due_polls().await;
                    }
                    Ok(ev) = event_rx.recv() => {
                        if !ev.is_replay {
                            let _ = services.engine().automations.dispatch_event(&ev).await;
                        }
                    }
                }
            }
        })
    }

    async fn run_due_history_samples(&self) {
        let g = self.graph();
        let now_ts = Utc::now().timestamp();
        for series in &g.workspace.history {
            let due = {
                let mut guard = self.last_history_sample.lock().await;
                let last = guard.get(&series.series_id).copied().unwrap_or(0);
                if now_ts - last >= series.cadence_secs.max(1) {
                    guard.insert(series.series_id.clone(), now_ts);
                    true
                } else {
                    false
                }
            };
            if due {
                let _ = self.engine().history.sample_series(series, None).await;
            }
        }
    }

    async fn run_due_polls(&self) {
        let g = self.graph();
        let now_ts = Utc::now().timestamp();
        for (inst_id, inst) in &g.instances {
            if inst.category != DefinitionCategory::DataProvider {
                continue;
            }
            let Some(def) = g.definitions.get(&inst.definition) else {
                continue;
            };
            let Some(interval) = def.manifest.updates.poll_interval_secs else {
                continue;
            };
            let due = {
                let mut guard = self.last_poll_run.lock().await;
                let last = guard.get(inst_id).copied().unwrap_or(0);
                if now_ts - last >= interval.max(5) as i64 {
                    guard.insert(inst_id.clone(), now_ts);
                    true
                } else {
                    false
                }
            };
            if due {
                // Poll runs under matching Service principal if granted (e.g. pr_history_sampler), never implicit system!
                if let Some(sp_id) = inst.run_as.as_deref() {
                    if let Ok(sp) = self.engine().automations.resolve_service_principal(sp_id) {
                        let _ = self
                            .engine()
                            .queries
                            .query(inst_id, serde_json::json!({}), &sp, true)
                            .await;
                    }
                }
            }
        }
    }

    // ---- Discover ----

    pub fn catalog_search(&self, query: Option<&str>) -> Vec<CatalogEntry> {
        let g = self.graph();
        let needle = query.unwrap_or("").to_ascii_lowercase();
        let mut entries: Vec<CatalogEntry> = g
            .definitions
            .values()
            .filter(|d| {
                needle.is_empty()
                    || d.manifest.id.to_ascii_lowercase().contains(&needle)
                    || d.manifest.description.to_ascii_lowercase().contains(&needle)
                    || d.manifest.category.as_str().contains(&needle)
            })
            .map(|d| CatalogEntry {
                id: d.manifest.id.clone(),
                package: d.package_name.clone(),
                category: d.manifest.category.as_str().into(),
                description: d.manifest.description.clone(),
                identity_version: d.manifest.identity_version.clone(),
                capabilities: d.manifest.capabilities.clone(),
            })
            .collect();
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        entries
    }

    pub fn contract_get(&self, definition_id: &str) -> Result<serde_json::Value, String> {
        let g = self.graph();
        let def = g
            .definitions
            .get(definition_id)
            .ok_or_else(|| format!("definition '{definition_id}' not found"))?;
        let mut val = serde_json::to_value(&def.manifest).map_err(|e| e.to_string())?;
        if let Some(obj) = val.as_object_mut() {
            if let Some(ref q) = def.manifest.query {
                obj.insert("query_json_schema".into(), q.to_json_schema());
            }
            if let Some(ref a) = def.manifest.args {
                obj.insert("args_json_schema".into(), a.to_json_schema());
            }
            if let Some(ref o) = def.manifest.output {
                obj.insert("output_json_schema".into(), o.to_json_schema());
            }
            if let Some(ref r) = def.manifest.result {
                obj.insert("result_json_schema".into(), r.to_json_schema());
            }
        }
        Ok(val)
    }

    pub fn graph_inspect(&self) -> serde_json::Value {
        let g = self.graph();
        let validation = validate_configured_graph(&g, self.store(), false);
        serde_json::json!({
            "revision_commit": g.revision_commit,
            "generation_id": g.generation_id,
            "graph_digest": g.graph_digest(),
            "instances": g.instances,
            "dashboards": g.dashboards,
            "service_principals": self.service_principal_list(),
            "validation": validation,
        })
    }

    pub fn capability_explain(&self, target: &str, principal: &Principal) -> serde_json::Value {
        let g = self.graph();
        let inst = g.instances.get(target);
        let action_allowed = inst
            .map(|i| {
                dash_core::check_action_authority(principal, i, &g, self.store())
                    .map(|_| true)
                    .map_err(|e| e.to_string())
            })
            .unwrap_or(Err(format!("instance '{target}' not found")));

        serde_json::json!({
            "principal": {
                "id": principal.id,
                "kind": principal.kind,
                "ceiling": principal.ceiling,
            },
            "target": target,
            "instance": inst,
            "action_authority": match action_allowed {
                Ok(_) => serde_json::json!({ "allowed": true }),
                Err(reason) => serde_json::json!({ "allowed": false, "reason": reason }),
            },
        })
    }

    // ---- Drafts & Definitions & Instances & Dashboards ----

    pub async fn draft_begin(&self, principal: &Principal) -> Result<DraftRecord, String> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            return Err("principal lacks can_author_drafts".into());
        }
        self.drafts
            .draft_begin(principal)
            .await
            .map_err(|e| e.to_string())
    }

    pub fn draft_status(&self, draft_id: &str) -> Result<DraftRecord, String> {
        self.drafts.draft_status(draft_id).map_err(|e| e.to_string())
    }

    pub fn draft_list(&self) -> Result<Vec<DraftRecord>, String> {
        self.drafts.draft_list().map_err(|e| e.to_string())
    }

    pub async fn draft_validate(&self, draft_id: &str) -> Result<ValidationReport, String> {
        self.drafts
            .draft_validate(draft_id)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn draft_preview(&self, draft_id: &str) -> Result<PreviewTestReport, String> {
        self.drafts
            .draft_preview_and_test(draft_id)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn draft_test(&self, draft_id: &str) -> Result<PreviewTestReport, String> {
        self.draft_preview(draft_id).await
    }

    pub async fn draft_rebase(&self, draft_id: &str) -> Result<DraftRecord, String> {
        self.drafts
            .draft_rebase(draft_id)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn draft_activate(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<ActivationResult, String> {
        self.drafts
            .draft_activate(draft_id, principal)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn draft_abort(&self, draft_id: &str) -> Result<(), String> {
        self.drafts
            .draft_abort(draft_id)
            .await
            .map_err(|e| e.to_string())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn definition_upsert_in_draft(
        &self,
        draft_id: &str,
        package: &str,
        def_name: &str,
        definition_luau: &str,
        runtime_luau: Option<&str>,
        tests_luau: Option<&str>,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            return Err("principal lacks can_author_drafts".into());
        }
        self.drafts
            .draft_write_definition(
                draft_id,
                package,
                def_name,
                definition_luau,
                runtime_luau,
                tests_luau,
            )
            .map_err(|e| e.to_string())
    }

    pub async fn definition_test(
        &self,
        definition_id: &str,
        draft_id: Option<&str>,
    ) -> Result<DefinitionTestReport, String> {
        let (ws_dir, is_preview) = match draft_id {
            Some(id) => (PathBuf::from(self.draft_status(id)?.worktree_path), true),
            None => (self.drafts.workspace_dir().to_path_buf(), true),
        };
        let graph = dash_git::load_workspace_graph(
            &ws_dir,
            self.drafts.data_dir(),
            "test",
            0,
            is_preview,
        )
        .map_err(|e| e.to_string())?;
        let def = graph
            .definitions
            .get(definition_id)
            .ok_or_else(|| format!("definition '{definition_id}' not found"))?;
        let tests_src = def
            .tests_source
            .as_deref()
            .ok_or_else(|| format!("definition '{definition_id}' has no tests.luau"))?;
        run_definition_tests(
            &def.manifest,
            &def.runtime_source,
            tests_src,
            &graph.resolved_libraries(),
            Arc::new(NullHostBridge),
            serde_json::json!({}),
        )
        .await
    }

    pub fn instance_upsert_in_draft(
        &self,
        draft_id: &str,
        inst: &InstanceConfig,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            return Err("principal lacks can_author_drafts".into());
        }
        self.drafts
            .draft_upsert_instance(draft_id, inst)
            .map_err(|e| e.to_string())
    }

    pub fn instance_delete_in_draft(
        &self,
        draft_id: &str,
        instance_id: &str,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            return Err("principal lacks can_author_drafts".into());
        }
        self.drafts
            .draft_delete_instance(draft_id, instance_id)
            .map_err(|e| e.to_string())
    }

    pub async fn dashboard_get(
        &self,
        dashboard_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        let rendered = self
            .engine()
            .queries
            .render_dashboard(dashboard_id, principal)
            .await?;
        let g = self.graph();
        let doc = g.dashboards.get(dashboard_id).cloned();
        Ok(serde_json::json!({
            "document": doc,
            "rendered": rendered,
        }))
    }

    pub fn dashboard_update_in_draft(
        &self,
        draft_id: &str,
        doc: &DashboardDoc,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            return Err("principal lacks can_author_drafts".into());
        }
        self.drafts
            .draft_update_dashboard(draft_id, doc)
            .map_err(|e| e.to_string())
    }

    pub async fn dashboard_preview(
        &self,
        draft_id: &str,
        dashboard_id: &str,
    ) -> Result<serde_json::Value, String> {
        let report = self.draft_preview(draft_id).await?;
        Ok(report
            .preview_renders
            .get(dashboard_id)
            .cloned()
            .unwrap_or_else(|| serde_json::json!({ "error": "dashboard not rendered in preview", "report": report })))
    }

    // ---- Runtime ----

    pub async fn provider_query(
        &self,
        instance_id: &str,
        params: serde_json::Value,
        principal: &Principal,
        force_refresh: bool,
    ) -> Result<QueryEnvelope, String> {
        self.engine()
            .queries
            .query(instance_id, params, principal, force_refresh)
            .await
    }

    pub async fn action_dry_run(
        &self,
        action_instance: &str,
        args: serde_json::Value,
        principal: &Principal,
    ) -> Result<DryRunReport, String> {
        self.engine()
            .actions
            .dry_run(action_instance, args, principal)
            .await
    }

    pub async fn action_invoke(
        &self,
        action_instance: &str,
        args: serde_json::Value,
        idempotency_key: Option<String>,
        principal: &Principal,
    ) -> Result<ActionExecutionRecord, String> {
        self.engine()
            .actions
            .invoke(action_instance, args, principal, idempotency_key, None)
            .await
    }

    pub async fn dispatch_widget_request(
        &self,
        req: WidgetRequestEnvelope,
        principal: &Principal,
    ) -> Result<ActionExecutionRecord, String> {
        self.engine()
            .actions
            .dispatch_widget_request(req, principal)
            .await
    }

    pub fn execution_inspect(&self, execution_id: Option<&str>) -> Result<serde_json::Value, String> {
        if let Some(id) = execution_id {
            let rec = self
                .store()
                .get_action_execution(id)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("execution '{id}' not found"))?;
            Ok(serde_json::to_value(rec).unwrap())
        } else {
            let list = self
                .store()
                .list_action_executions(100)
                .map_err(|e| e.to_string())?;
            Ok(serde_json::to_value(list).unwrap())
        }
    }

    pub async fn execution_resolve(
        &self,
        execution_id: &str,
        status: &str,
        reason: &str,
        principal: &Principal,
    ) -> Result<ActionExecutionRecord, String> {
        self.engine()
            .actions
            .resolve_execution(execution_id, status, reason, principal)
            .await
    }

    // ---- Diagnostics ----

    pub fn logs_query(&self, limit: usize) -> Vec<SanitizedLogEntry> {
        self.engine().telemetry.recent_logs(limit)
    }

    pub fn graph_trace(&self, limit: usize) -> serde_json::Value {
        let exec_graph: ExecutionGraphSnapshot = self.engine().queries.execution_graph_snapshot();
        let spans: Vec<TraceSpanRecord> = self.engine().telemetry.recent_spans(limit);
        serde_json::json!({
            "execution_graph": exec_graph,
            "spans": spans,
        })
    }

    pub fn metrics_query(&self) -> RuntimeMetricsSnapshot {
        self.engine().metrics_snapshot()
    }

    pub fn audit_query(&self, limit: usize) -> Result<Vec<AuditRecord>, String> {
        self.store().list_audits(limit).map_err(|e| e.to_string())
    }

    // ---- Git ----

    pub async fn git_status(&self) -> Result<GitStatusReport, String> {
        self.drafts.git_status().await.map_err(|e| e.to_string())
    }

    pub async fn git_history(&self, limit: usize) -> Result<Vec<GitCommitInfo>, String> {
        self.drafts
            .git_history(limit)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn git_sync(&self, principal: &Principal) -> Result<GitSyncRecord, String> {
        if !principal.ceiling.can_git_sync && !principal.ceiling.unrestricted {
            return Err("principal lacks can_git_sync".into());
        }
        self.drafts
            .git_sync(principal)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn dependency_update(
        &self,
        draft_id: &str,
        package_name: &str,
        spec: PackageDependencySpec,
        principal: &Principal,
    ) -> Result<(DraftRecord, PackagesLock), String> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            return Err("principal lacks can_author_drafts".into());
        }
        self.drafts
            .draft_update_dependency(draft_id, package_name, spec)
            .await
            .map_err(|e| e.to_string())
    }

    // ---- Secrets (never returns plaintext!) ----

    pub fn secret_list(&self, principal: &Principal) -> Result<Vec<SecretMetadata>, String> {
        let all = self.store().list_secrets().map_err(|e| e.to_string())?;
        Ok(all
            .into_iter()
            .filter(|m| principal.ceiling.allows_secret(&m.name))
            .collect())
    }

    pub fn secret_set(
        &self,
        name: &str,
        description: &str,
        plaintext: &str,
        allowed_bindings: &[String],
        principal: &Principal,
    ) -> Result<SecretMetadata, String> {
        if !principal.ceiling.unrestricted {
            return Err("only the Administrator may set secret values".into());
        }
        let meta = self
            .store()
            .set_secret(name, description, plaintext, allowed_bindings)
            .map_err(|e| e.to_string())?;
        let _ = self.store().append_audit(
            &principal.id,
            principal.kind.as_str(),
            "secret.set",
            name,
            "ok",
            serde_json::json!({ "key_version": meta.key_version }),
        );
        Ok(meta)
    }

    /// Bind an existing secret into a connector instance inside a Draft (requires secret ceiling!).
    pub fn secret_bind(
        &self,
        draft_id: &str,
        instance_id: &str,
        slot: &str,
        secret_name: &str,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            return Err("principal lacks can_author_drafts".into());
        }
        if !principal.ceiling.allows_secret(secret_name) {
            let _ = self.store().append_audit(
                &principal.id,
                principal.kind.as_str(),
                "secret.bind",
                secret_name,
                "denied",
                serde_json::json!({ "instance_id": instance_id }),
            );
            return Err(format!(
                "principal ceiling does not permit binding secret '{secret_name}'"
            ));
        }
        let secrets = self.store().list_secrets().map_err(|e| e.to_string())?;
        let meta = secrets
            .into_iter()
            .find(|m| m.name == secret_name)
            .ok_or_else(|| format!("secret '{secret_name}' not found or access denied"))?;
        if !meta.allowed_bindings.is_empty()
            && !meta
                .allowed_bindings
                .iter()
                .any(|b| b == instance_id || b == "*")
        {
            return Err(format!(
                "secret '{secret_name}' policy restricts allowed bindings"
            ));
        }

        let draft = self.draft_status(draft_id)?;
        let g = dash_git::load_workspace_graph(
            std::path::Path::new(&draft.worktree_path),
            self.drafts.data_dir(),
            &draft.base_revision,
            0,
            false,
        )
        .map_err(|e| e.to_string())?;
        let mut inst = g
            .instances
            .get(instance_id)
            .cloned()
            .ok_or_else(|| format!("instance '{instance_id}' not found in draft"))?;
        inst.secrets
            .insert(slot.to_string(), format!("secret://{secret_name}"));
        let updated = self
            .drafts
            .draft_upsert_instance(draft_id, &inst)
            .map_err(|e| e.to_string())?;
        let _ = self.store().append_audit(
            &principal.id,
            principal.kind.as_str(),
            "secret.bind",
            secret_name,
            "ok",
            serde_json::json!({ "instance_id": instance_id, "slot": slot }),
        );
        Ok(updated)
    }

    // ---- Principals & Security Admin ----

    pub fn service_principal_list(&self) -> Vec<ServicePrincipalStatus> {
        let g = self.graph();
        let grants: BTreeMap<String, (serde_json::Value, String, String)> = self
            .store()
            .list_service_principal_grants()
            .unwrap_or_default()
            .into_iter()
            .map(|(id, c, by, at)| (id, (c, by, at)))
            .collect();

        let mut all_ids: std::collections::BTreeSet<String> =
            g.service_principals.keys().cloned().collect();
        all_ids.extend(grants.keys().cloned());

        all_ids
            .into_iter()
            .map(|id| {
                let req = g.service_principals.get(&id);
                let gr = grants.get(&id);
                ServicePrincipalStatus {
                    id: id.clone(),
                    description: req.map(|r| r.description.clone()).unwrap_or_default(),
                    requested_ceiling: req.map(|r| r.ceiling.clone()).unwrap_or_default(),
                    granted_ceiling: gr
                        .and_then(|(v, _, _)| serde_json::from_value(v.clone()).ok()),
                    granted_by: gr.map(|(_, by, _)| by.clone()),
                    granted_at: gr.map(|(_, _, at)| at.clone()),
                }
            })
            .collect()
    }

    pub fn service_principal_grant(
        &self,
        principal_id: &str,
        ceiling: CapabilityCeiling,
        actor: &Principal,
    ) -> Result<(), String> {
        if !actor.ceiling.unrestricted {
            return Err("only the Administrator can grant Service principal ceilings".into());
        }
        self.store()
            .set_service_principal_grant(
                principal_id,
                serde_json::to_value(&ceiling).unwrap(),
                &actor.id,
            )
            .map_err(|e| e.to_string())?;
        let _ = self.store().append_audit(
            &actor.id,
            actor.kind.as_str(),
            "service_principal.grant",
            principal_id,
            "ok",
            serde_json::to_value(&ceiling).unwrap(),
        );
        Ok(())
    }

    pub fn agent_create(
        &self,
        name: &str,
        ceiling: CapabilityCeiling,
        expires_at: Option<DateTime<Utc>>,
        actor: &Principal,
    ) -> Result<(AgentPrincipalRecord, String), String> {
        if !actor.ceiling.unrestricted {
            return Err("only the Administrator can issue Agent MCP tokens".into());
        }
        let mut safe_ceiling = ceiling;
        // Agents NEVER become unrestricted Administrators!
        safe_ceiling.unrestricted = false;
        let plaintext = format!("dsh_agt_{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
        let rec = self
            .store()
            .create_agent_principal(
                name,
                &plaintext,
                serde_json::to_value(&safe_ceiling).unwrap(),
                expires_at,
            )
            .map_err(|e| e.to_string())?;
        let _ = self.store().append_audit(
            &actor.id,
            actor.kind.as_str(),
            "agent.create",
            &rec.agent_id,
            "ok",
            serde_json::json!({ "name": name }),
        );
        Ok((rec, plaintext))
    }

    pub fn agent_list(&self) -> Result<Vec<AgentPrincipalRecord>, String> {
        self.store()
            .list_agent_principals()
            .map_err(|e| e.to_string())
    }

    pub fn agent_revoke(&self, agent_id: &str, actor: &Principal) -> Result<bool, String> {
        if !actor.ceiling.unrestricted {
            return Err("only the Administrator can revoke Agent tokens".into());
        }
        let ok = self
            .store()
            .revoke_agent_principal(agent_id)
            .map_err(|e| e.to_string())?;
        let _ = self.store().append_audit(
            &actor.id,
            actor.kind.as_str(),
            "agent.revoke",
            agent_id,
            if ok { "ok" } else { "not_found" },
            serde_json::json!({}),
        );
        Ok(ok)
    }

    /// Helpers for reading/writing `packages.toml` when inspecting workspace configuration.
    pub fn packages_toml(&self) -> Result<dash_git::PackagesToml, String> {
        read_packages_toml(self.drafts.workspace_dir()).map_err(|e| e.to_string())
    }

    pub fn save_packages_toml(&self, cfg: &dash_git::PackagesToml) -> Result<(), String> {
        write_packages_toml(self.drafts.workspace_dir(), cfg).map_err(|e| e.to_string())
    }

    pub fn history_series_list(&self) -> Vec<HistorySeriesConfig> {
        self.graph().workspace.history.clone()
    }

    pub fn workspace_doctor(&self) -> Result<serde_json::Value, GitError> {
        let g = self.graph();
        let val = validate_configured_graph(&g, self.store(), false);
        Ok(serde_json::json!({
            "sqlite_ok": true,
            "sqlite_bytes": self.store().db_size_bytes(),
            "active_revision": g.revision_commit,
            "generation_id": g.generation_id,
            "definitions": g.definitions.len(),
            "instances": g.instances.len(),
            "validation": val,
            "service_principals": self.service_principal_list(),
        }))
    }
}

// ---- Read-only helpers for the Web UI (Dashboard Designer, Dependency Graph Editor,
// ---- Package Studio and live Dashboard Signals, §10.2) ----

/// Sources of one Definition as stored in the Workspace (or a Draft worktree), for Package Studio.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefinitionSources {
    pub definition_id: String,
    pub package: String,
    pub category: String,
    /// Directory under `packages/` holding the Package when the Definition belongs to a local
    /// Workspace Package; pass it as `package` when saving the Definition into a Draft.
    pub package_dir: Option<String>,
    /// Definition directory relative to `package_dir`; pass it as `name` when saving.
    pub definition_dir: Option<String>,
    /// False for built-ins and locked external Git Packages, which are not edited in place.
    pub editable: bool,
    pub draft_id: Option<String>,
    pub definition_luau: String,
    pub runtime_file: String,
    pub runtime_luau: String,
    pub tests_luau: Option<String>,
    pub generated_luau_types: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DraftDiffFile {
    pub path: String,
    /// `added` | `modified` | `deleted` | `renamed` (relative to the Draft's base Revision).
    pub status: String,
    pub patch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DraftDiff {
    pub draft_id: String,
    pub base_revision: String,
    pub files: Vec<DraftDiffFile>,
    /// True when the combined patch text exceeded the response budget and was cut.
    pub truncated: bool,
}

const MAX_DRAFT_DIFF_BYTES: usize = 1024 * 1024;

/// Find the Definition directory inside `workspace_dir/packages` whose `definition.luau` has the
/// given content. Returns `(package_dir, definition_dir)` relative to `packages/`.
pub(crate) fn locate_definition_dir(
    workspace_dir: &std::path::Path,
    definition_source: &str,
) -> Option<(String, String)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
        if dir.join("definition.luau").is_file() {
            out.push(dir.to_path_buf());
            return;
        }
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        let mut children: Vec<PathBuf> = rd
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name() != ".git")
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();
        children.sort();
        for c in children {
            walk(&c, out);
        }
    }
    let pkgs = workspace_dir.join("packages");
    let mut pkg_dirs: Vec<PathBuf> = std::fs::read_dir(&pkgs)
        .ok()?
        .filter_map(|e| e.ok().map(|x| x.path()))
        .filter(|p| p.is_dir())
        .collect();
    pkg_dirs.sort();
    for pkg_dir in pkg_dirs {
        let mut defs = Vec::new();
        walk(&pkg_dir, &mut defs);
        for d in defs {
            let Ok(src) = std::fs::read_to_string(d.join("definition.luau")) else {
                continue;
            };
            if src == definition_source {
                let pkg_name = pkg_dir.file_name()?.to_string_lossy().to_string();
                let rel = d
                    .strip_prefix(&pkg_dir)
                    .ok()?
                    .to_string_lossy()
                    .replace('\\', "/");
                if rel.is_empty() {
                    return None;
                }
                return Some((pkg_name, rel));
            }
        }
    }
    None
}

/// Per-Definition contract summary the Graph Editor needs to show required capabilities and
/// input expectations inline (§10.2.2).
pub(crate) fn definition_contract_summaries(g: &ConfiguredGraph) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for (id, def) in &g.definitions {
        out.insert(
            id.clone(),
            serde_json::json!({
                "package": def.package_name,
                "category": def.manifest.category.as_str(),
                "description": def.manifest.description,
                "capabilities": def.manifest.capabilities,
                "required_capabilities": def.manifest.required_capabilities,
                "optional_capabilities": def.manifest.optional_capabilities,
                "inputs": def.manifest.inputs,
                "accepts_requests": def.manifest.accepts_requests,
            }),
        );
    }
    serde_json::Value::Object(out)
}

/// Add `asset_url` to every rendered Widget node whose widget provider ships a custom asset, so
/// the browser loads the provider's declared entry point rather than guessing (§10.1).
pub(crate) fn annotate_widget_asset_urls(g: &ConfiguredGraph, rendered: &mut serde_json::Value) {
    fn walk(g: &ConfiguredGraph, node: &mut serde_json::Value) {
        let Some(obj) = node.as_object_mut() else {
            return;
        };
        if let Some(wt) = obj.get("widget_type").and_then(|v| v.as_str()).map(str::to_string) {
            if let Some(def) = g.definitions.get(&wt) {
                let safe_id = wt
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
                if def.manifest.category == DefinitionCategory::WidgetProvider && safe_id {
                    if let Some(asset) = def.manifest.widget_asset.as_deref() {
                        if def.widget_assets.contains_key(asset) {
                            obj.insert(
                                "asset_url".into(),
                                serde_json::Value::String(format!("/widgets/{wt}/{asset}")),
                            );
                        }
                    }
                }
            }
        }
        if let Some(children) = obj.get_mut("children").and_then(|v| v.as_array_mut()) {
            for c in children.iter_mut() {
                walk(g, c);
            }
        }
    }
    if let Some(adapters) = rendered.get_mut("adapters").and_then(|v| v.as_array_mut()) {
        for a in adapters.iter_mut() {
            if let Some(tree) = a.get_mut("tree") {
                walk(g, tree);
            }
        }
    }
}

/// Every Instance a Dashboard's Adapters (transitively) read from in the configured graph.
pub(crate) fn dashboard_upstream_instances(
    g: &ConfiguredGraph,
    dashboard_id: &str,
) -> std::collections::BTreeSet<String> {
    let mut seen = std::collections::BTreeSet::new();
    let Some(dash) = g.dashboards.get(dashboard_id) else {
        return seen;
    };
    let mut stack: Vec<String> = dash
        .adapters
        .iter()
        .map(|p| p.adapter_instance.clone())
        .collect();
    while let Some(id) = stack.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        if let Some(inst) = g.instances.get(&id) {
            stack.extend(inst.inputs.values().cloned());
        }
    }
    seen
}

impl AppServices {
    /// Resolve a widget Request through its Adapter without side effects and Dry-run the
    /// resolved action (no execution record is created).
    pub async fn widget_request_dry_run(
        &self,
        req: WidgetRequestEnvelope,
        principal: &Principal,
    ) -> Result<dash_core::WidgetRequestDryRun, String> {
        self.engine()
            .actions
            .dry_run_widget_request(req, principal)
            .await
    }

    /// `dashboard_get` plus custom widget asset URLs on rendered nodes.
    pub async fn dashboard_get_for_ui(
        &self,
        dashboard_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        let mut data = self.dashboard_get(dashboard_id, principal).await?;
        let g = self.graph();
        if let Some(rendered) = data.get_mut("rendered") {
            annotate_widget_asset_urls(&g, rendered);
        }
        Ok(data)
    }

    /// Instances whose Query Signals can change what a Dashboard renders.
    pub fn dashboard_upstream_instances(
        &self,
        dashboard_id: &str,
    ) -> std::collections::BTreeSet<String> {
        dashboard_upstream_instances(&self.graph(), dashboard_id)
    }

    fn load_draft_graph(&self, draft_id: &str) -> Result<(DraftRecord, ConfiguredGraph), String> {
        let draft = self.draft_status(draft_id)?;
        if draft.status == "aborted" || draft.status == "activated" {
            return Err(format!("draft '{draft_id}' is {}", draft.status));
        }
        let g = dash_git::load_workspace_graph(
            std::path::Path::new(&draft.worktree_path),
            self.drafts.data_dir(),
            &draft.base_revision,
            0,
            false,
        )
        .map_err(|e| e.to_string())?;
        Ok((draft, g))
    }

    /// The configured graph as it exists in a Draft worktree, with its validation report, so
    /// editors show and build on cumulative Draft edits rather than the active Generation.
    pub fn draft_graph_inspect(&self, draft_id: &str) -> Result<serde_json::Value, String> {
        let (draft, g) = self.load_draft_graph(draft_id)?;
        let validation = validate_configured_graph(&g, self.store(), false);
        Ok(serde_json::json!({
            "draft_id": draft.draft_id,
            "base_revision": draft.base_revision,
            "revision_commit": draft.base_revision,
            "generation_id": self.graph().generation_id,
            "graph_digest": g.graph_digest(),
            "instances": g.instances,
            "dashboards": g.dashboards,
            "definitions": definition_contract_summaries(&g),
            "service_principals": self.service_principal_list(),
            "validation": validation,
        }))
    }

    /// Contract summaries of every Definition in the active Generation.
    pub fn definition_summaries(&self) -> serde_json::Value {
        definition_contract_summaries(&self.graph())
    }

    /// Read a Definition's Luau sources from the active Workspace or from a Draft worktree.
    pub fn definition_sources(
        &self,
        definition_id: &str,
        draft_id: Option<&str>,
    ) -> Result<DefinitionSources, String> {
        let (ws_dir, def) = match draft_id {
            Some(id) => {
                let (draft, g) = self.load_draft_graph(id)?;
                let def = g
                    .definitions
                    .get(definition_id)
                    .cloned()
                    .ok_or_else(|| format!("definition '{definition_id}' not found in draft"))?;
                (PathBuf::from(draft.worktree_path), def)
            }
            None => {
                let g = self.graph();
                let def = g
                    .definitions
                    .get(definition_id)
                    .cloned()
                    .ok_or_else(|| format!("definition '{definition_id}' not found"))?;
                (self.drafts.workspace_dir().to_path_buf(), def)
            }
        };
        let located = locate_definition_dir(&ws_dir, &def.definition_source);
        let runtime_file = if def.manifest.runtime.is_empty() {
            "runtime.luau".to_string()
        } else {
            def.manifest.runtime.clone()
        };
        Ok(DefinitionSources {
            definition_id: definition_id.to_string(),
            package: def.package_name.clone(),
            category: def.manifest.category.as_str().to_string(),
            editable: located.is_some(),
            package_dir: located.as_ref().map(|(p, _)| p.clone()),
            definition_dir: located.map(|(_, d)| d),
            draft_id: draft_id.map(str::to_string),
            definition_luau: def.definition_source.clone(),
            runtime_file,
            runtime_luau: def.runtime_source.clone(),
            tests_luau: def.tests_source.clone(),
            generated_luau_types: def.manifest.generated_luau_types.clone(),
        })
    }

    /// Unified diff of a Draft worktree against its base Revision (tracked and new files).
    pub async fn draft_diff(&self, draft_id: &str) -> Result<DraftDiff, String> {
        use dash_git::git_cli::{run_git, run_git_allow_exit_1};
        let draft = self.draft_status(draft_id)?;
        if draft.status == "aborted" || draft.status == "activated" {
            return Err(format!("draft '{draft_id}' is {}", draft.status));
        }
        let wt = PathBuf::from(&draft.worktree_path);
        let base = draft.base_revision.clone();
        let mut files: Vec<DraftDiffFile> = Vec::new();
        let mut budget = MAX_DRAFT_DIFF_BYTES;
        let mut truncated = false;
        let mut push_patch = |files: &mut Vec<DraftDiffFile>, path: String, status: String, patch: String| {
            let patch = if patch.len() > budget {
                truncated = true;
                let mut cut = budget;
                while cut > 0 && !patch.is_char_boundary(cut) {
                    cut -= 1;
                }
                patch[..cut].to_string()
            } else {
                patch
            };
            budget -= patch.len();
            files.push(DraftDiffFile { path, status, patch });
        };

        let name_status = run_git(&wt, &["diff", "--name-status", "--no-renames", &base, "--"])
            .await
            .map_err(|e| e.to_string())?;
        for line in name_status.lines().filter(|l| !l.trim().is_empty()) {
            let mut parts = line.splitn(2, '\t');
            let code = parts.next().unwrap_or("").trim();
            let Some(path) = parts.next().map(|p| p.trim().to_string()) else {
                continue;
            };
            let status = match code.chars().next() {
                Some('A') => "added",
                Some('D') => "deleted",
                Some('R') => "renamed",
                _ => "modified",
            };
            let patch = run_git(&wt, &["diff", "--no-color", "--no-renames", &base, "--", &path])
                .await
                .map_err(|e| e.to_string())?;
            push_patch(&mut files, path, status.into(), patch);
        }

        let untracked = run_git(&wt, &["ls-files", "--others", "--exclude-standard"])
            .await
            .map_err(|e| e.to_string())?;
        for path in untracked.lines().map(str::trim).filter(|l| !l.is_empty()) {
            let (_code, patch) = run_git_allow_exit_1(
                &wt,
                &["diff", "--no-color", "--no-index", "--", "/dev/null", path],
            )
            .await
            .map_err(|e| e.to_string())?;
            push_patch(&mut files, path.to_string(), "added".into(), patch);
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(DraftDiff {
            draft_id: draft.draft_id,
            base_revision: base,
            files,
            truncated,
        })
    }
}
