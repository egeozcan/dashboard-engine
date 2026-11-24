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
                        services.engine().queries.prune_idle();
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
