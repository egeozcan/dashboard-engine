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
    /// Host allowlist, cookie policy, drain flag, MCP sessions and retention policy.
    pub server: crate::context::ServerContext,
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
            server: crate::context::ServerContext::default(),
            admin_cli_token_path,
            last_history_sample: Arc::new(Mutex::new(HashMap::new())),
            last_poll_run: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Replace the server context (Host allowlist, cookie mode, retention policy).
    pub fn with_server_context(mut self, ctx: crate::context::ServerContext) -> Self {
        self.server = ctx;
        self
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
        // The sender is dropped immediately: the worker then simply never observes a shutdown.
        let (_tx, rx) = tokio::sync::watch::channel(false);
        self.spawn_background_workers_until(rx)
    }

    /// Like [`Self::spawn_background_workers`], but the loop stops picking up new work once
    /// `shutdown` becomes true; the in-progress iteration (e.g. an automation-initiated action)
    /// completes first, so the caller can drain it within its configured limit (§15.2).
    pub fn spawn_background_workers_until(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let services = self.clone();
        let mut event_rx = services.engine().events.subscribe();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            let mut tick: u64 = 0;
            let mut shutdown_open = true;
            loop {
                if *shutdown.borrow() {
                    break;
                }
                tokio::select! {
                    changed = shutdown.changed(), if shutdown_open => {
                        match changed {
                            Err(_) => shutdown_open = false,
                            Ok(()) if *shutdown.borrow() => break,
                            Ok(()) => {}
                        }
                    }
                    _ = ticker.tick() => {
                        services.engine().pool.reclaim_idle(None);
                        services.run_due_history_samples().await;
                        services.run_due_polls().await;
                        // Every minute: sample runtime metrics; every 5 minutes (and at start):
                        // enforce retention and size budgets off the async runtime (§13).
                        if tick.is_multiple_of(12) {
                            services.sample_runtime_metrics().await;
                        }
                        if tick.is_multiple_of(60) {
                            let _ = services.enforce_retention().await;
                        }
                        tick = tick.wrapping_add(1);
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

    /// Apply the configured retention policy on a blocking thread.
    pub async fn enforce_retention(&self) -> Result<dash_store::RetentionReport, String> {
        let store = self.store().clone();
        let policy = self.server.retention.clone();
        let report = tokio::task::spawn_blocking(move || store.enforce_retention(&policy))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        if report.over_budget {
            tracing::warn!(
                "SQLite footprint {} bytes exceeds budget {} bytes after retention",
                report.db_size_bytes_after,
                report.db_size_budget_bytes
            );
        }
        Ok(report)
    }

    /// Persist a sample of the runtime metrics into `runtime_metrics` (bounded by retention).
    async fn sample_runtime_metrics(&self) {
        let snap = self.metrics_query();
        let store = self.store().clone();
        let _ = tokio::task::spawn_blocking(move || {
            let samples: [(&str, f64); 8] = [
                ("active_vms", snap.active_vms as f64),
                ("cache_entries", snap.cache_entries as f64),
                ("active_subscribers", snap.active_subscribers as f64),
                ("event_lag", snap.event_lag as f64),
                ("sqlite_bytes", snap.sqlite_bytes as f64),
                ("sqlite_wal_bytes", snap.sqlite_wal_bytes as f64),
                ("trace_write_queue_depth", snap.trace_write_queue_depth as f64),
                (
                    "refresh_latency_p95_ms",
                    snap.refresh_latency_p95_ms.unwrap_or(0) as f64,
                ),
            ];
            for (name, v) in samples {
                let _ = store.record_runtime_metric(name, v, serde_json::json!({}));
            }
        })
        .await;
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
        contract_json(def)
    }
}

/// Generated manifest of one Definition plus its JSON Schemas (§4.3), shared by API, MCP and CLI.
pub fn contract_json(def: &dash_core::LoadedDefinition) -> Result<serde_json::Value, String> {
    {
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
}

impl AppServices {

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

    /// Graph inspection as seen by `principal`: secret references the caller's ceiling does not
    /// permit are masked so secret names do not leak (§5.2, §11.1).
    pub fn graph_inspect_for(&self, principal: &Principal) -> serde_json::Value {
        let mut val = self.graph_inspect();
        if principal.ceiling.unrestricted {
            return val;
        }
        if let Some(instances) = val.get_mut("instances").and_then(|v| v.as_object_mut()) {
            for inst in instances.values_mut() {
                if let Some(secrets) = inst.get_mut("secrets").and_then(|v| v.as_object_mut()) {
                    for v in secrets.values_mut() {
                        let name = v
                            .as_str()
                            .map(|s| s.trim_start_matches("secret://").to_string())
                            .unwrap_or_default();
                        if !principal.ceiling.allows_secret(&name) {
                            *v = serde_json::Value::String("secret://[hidden]".into());
                        }
                    }
                }
            }
        }
        val
    }

    /// Explain effective authority on one Instance, per category (§5.1 item 4, §11.2):
    /// data providers use provider access, connectors report every operation, actions use the
    /// full action authority check. Also reports the Instance-granted capabilities and their
    /// intersection with the Principal's allowed capabilities.
    pub fn capability_explain(&self, target: &str, principal: &Principal) -> serde_json::Value {
        let g = self.graph();
        let inst = g.instances.get(target);
        let verdict = |r: Result<(), dash_core::AuthError>| match r {
            Ok(()) => serde_json::json!({ "allowed": true }),
            Err(e) => serde_json::json!({ "allowed": false, "reason": e.to_string() }),
        };
        let mut out = serde_json::json!({
            "principal": {
                "id": principal.id,
                "kind": principal.kind,
                "ceiling": principal.ceiling,
            },
            "target": target,
            "instance": inst,
        });
        let Some(i) = inst else {
            out["action_authority"] = serde_json::json!({
                "allowed": false,
                "reason": format!("instance '{target}' not found"),
            });
            return out;
        };
        out["category"] = serde_json::json!(i.category.as_str());
        let effective: Vec<&String> = i
            .capabilities
            .iter()
            .filter(|c| principal.ceiling.allows_capability(c))
            .collect();
        out["instance_capabilities"] = serde_json::json!(i.capabilities);
        out["effective_capabilities"] = serde_json::json!(effective);
        match i.category {
            DefinitionCategory::DataProvider => {
                out["provider_access"] = verdict(dash_core::check_provider_access(principal, i));
            }
            DefinitionCategory::Connector => {
                let mut ops = serde_json::Map::new();
                if let Some(def) = g.definitions.get(&i.definition) {
                    for op in def.manifest.operations.keys() {
                        ops.insert(
                            op.clone(),
                            verdict(dash_core::check_connector_operation_authority(
                                principal, i, op, &g,
                            )),
                        );
                    }
                }
                out["connector_operations"] = serde_json::Value::Object(ops);
            }
            _ => {}
        }
        out["action_authority"] = if i.category == DefinitionCategory::ActionProvider {
            verdict(dash_core::check_action_authority(principal, i, &g, self.store()))
        } else {
            serde_json::json!({
                "allowed": false,
                "reason": format!("instance '{target}' is a {} and cannot be invoked as an action", i.category.as_str()),
            })
        };
        out
    }

    /// Authorize `principal` to operate on a Draft (§2.1, §11.3): it needs `can_author_drafts`,
    /// and a non-Administrator may only touch Drafts it created.
    pub fn authorize_draft(&self, draft_id: &str, principal: &Principal) -> Result<DraftRecord, String> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            self.audit_denial(principal, "draft.access", draft_id, "principal lacks can_author_drafts");
            return Err("principal lacks can_author_drafts".into());
        }
        let draft = self.draft_status(draft_id)?;
        if !principal.ceiling.unrestricted && draft.created_by != principal.id {
            self.audit_denial(principal, "draft.access", draft_id, "draft belongs to another principal");
            return Err(format!("draft '{draft_id}' not found or not owned by this principal"));
        }
        Ok(draft)
    }

    fn audit_denial(&self, principal: &Principal, operation: &str, target: &str, reason: &str) {
        let _ = self.store().append_audit(
            &principal.id,
            principal.kind.as_str(),
            operation,
            target,
            "denied",
            serde_json::json!({ "reason": reason }),
        );
    }

    /// Drafts visible to `principal`: all for the Administrator, own Drafts otherwise.
    pub fn draft_list_for(&self, principal: &Principal) -> Result<Vec<DraftRecord>, String> {
        let all = self.draft_list()?;
        if principal.ceiling.unrestricted {
            return Ok(all);
        }
        if !principal.ceiling.can_author_drafts {
            return Ok(vec![]);
        }
        Ok(all.into_iter().filter(|d| d.created_by == principal.id).collect())
    }

    pub fn draft_status_as(&self, draft_id: &str, principal: &Principal) -> Result<DraftRecord, String> {
        self.authorize_draft(draft_id, principal)
    }

    pub async fn draft_validate_as(&self, draft_id: &str, principal: &Principal) -> Result<ValidationReport, String> {
        self.authorize_draft(draft_id, principal)?;
        self.draft_validate(draft_id).await
    }

    pub async fn draft_preview_as(&self, draft_id: &str, principal: &Principal) -> Result<PreviewTestReport, String> {
        self.authorize_draft(draft_id, principal)?;
        self.draft_preview(draft_id).await
    }

    /// Test-focused report: validation, Definition tests and Scenario results (no Preview renders).
    pub async fn draft_test_as(&self, draft_id: &str, principal: &Principal) -> Result<PreviewTestReport, String> {
        self.authorize_draft(draft_id, principal)?;
        let mut report = self.draft_test(draft_id).await?;
        report.preview_renders.clear();
        Ok(report)
    }

    pub async fn draft_rebase_as(&self, draft_id: &str, principal: &Principal) -> Result<DraftRecord, String> {
        self.authorize_draft(draft_id, principal)?;
        self.draft_rebase(draft_id).await
    }

    pub async fn draft_activate_as(&self, draft_id: &str, principal: &Principal) -> Result<ActivationResult, String> {
        self.authorize_draft(draft_id, principal)?;
        self.draft_activate(draft_id, principal).await
    }

    pub async fn draft_abort_as(&self, draft_id: &str, principal: &Principal) -> Result<(), String> {
        self.authorize_draft(draft_id, principal)?;
        self.draft_abort(draft_id).await
    }

    /// `definition_create`: fails when the Definition directory already exists in the Draft.
    #[allow(clippy::too_many_arguments)]
    pub fn definition_create_in_draft(
        &self,
        draft_id: &str,
        package: &str,
        def_name: &str,
        definition_luau: &str,
        runtime_luau: Option<&str>,
        tests_luau: Option<&str>,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        let draft = self.authorize_draft(draft_id, principal)?;
        if draft_definition_exists(&draft, package, def_name) {
            return Err(format!(
                "definition '{package}/{def_name}' already exists in draft; use definition_edit"
            ));
        }
        self.definition_upsert_in_draft(draft_id, package, def_name, definition_luau, runtime_luau, tests_luau, principal)
    }

    /// `definition_edit`: fails when the Definition does not exist in the Draft.
    #[allow(clippy::too_many_arguments)]
    pub fn definition_edit_in_draft(
        &self,
        draft_id: &str,
        package: &str,
        def_name: &str,
        definition_luau: &str,
        runtime_luau: Option<&str>,
        tests_luau: Option<&str>,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        let draft = self.authorize_draft(draft_id, principal)?;
        if !draft_definition_exists(&draft, package, def_name) {
            return Err(format!(
                "definition '{package}/{def_name}' does not exist in draft; use definition_create"
            ));
        }
        self.definition_upsert_in_draft(draft_id, package, def_name, definition_luau, runtime_luau, tests_luau, principal)
    }

    /// `instance_create`: fails when an Instance with this id already exists in the Draft.
    pub fn instance_create_in_draft(&self, draft_id: &str, inst: &InstanceConfig, principal: &Principal) -> Result<DraftRecord, String> {
        let draft = self.authorize_draft(draft_id, principal)?;
        if draft_instance_exists(&draft, &inst.id)? {
            return Err(format!("instance '{}' already exists in draft; use instance_configure", inst.id));
        }
        self.instance_upsert_in_draft(draft_id, inst, principal)
    }

    /// `instance_configure`: fails when the Instance does not exist in the Draft.
    pub fn instance_configure_in_draft(&self, draft_id: &str, inst: &InstanceConfig, principal: &Principal) -> Result<DraftRecord, String> {
        let draft = self.authorize_draft(draft_id, principal)?;
        if !draft_instance_exists(&draft, &inst.id)? {
            return Err(format!("instance '{}' does not exist in draft; use instance_create", inst.id));
        }
        self.instance_upsert_in_draft(draft_id, inst, principal)
    }

    /// Administrator-only hot reload of the bound Workspace via the Draft protocol (`dash reload`).
    pub async fn workspace_reload(&self, principal: &Principal) -> Result<ActivationResult, String> {
        if !principal.ceiling.unrestricted {
            return Err("only the Administrator may reload the Workspace".into());
        }
        self.drafts
            .reload_workspace(principal)
            .await
            .map_err(|e| e.to_string())
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
        if self.server.is_shutting_down() {
            return Err("daemon is shutting down; not admitting new actions".into());
        }
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
        if self.server.is_shutting_down() {
            return Err("daemon is shutting down; not admitting new actions".into());
        }
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

    /// Execution records visible to `principal` (§5.1.4, §11.2): the Administrator sees all;
    /// other Principals see executions they initiated or whose action Instance their ceiling allows.
    pub fn execution_inspect_for(
        &self,
        execution_id: Option<&str>,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        let visible = |r: &ActionExecutionRecord| {
            principal.ceiling.unrestricted
                || r.principal_id == principal.id
                || principal.ceiling.allows_action(&r.action_instance)
        };
        if let Some(id) = execution_id {
            let rec = self
                .store()
                .get_action_execution(id)
                .map_err(|e| e.to_string())?
                .filter(|r| visible(r))
                .ok_or_else(|| format!("execution '{id}' not found"))?;
            Ok(serde_json::to_value(rec).unwrap())
        } else {
            let scan = if principal.ceiling.unrestricted { 100 } else { 1000 };
            let list: Vec<ActionExecutionRecord> = self
                .store()
                .list_action_executions(scan)
                .map_err(|e| e.to_string())?
                .into_iter()
                .filter(|r| visible(r))
                .take(100)
                .collect();
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

    /// Audit trail visible to `principal`: everything for the Administrator, only the
    /// Principal's own attributed records otherwise.
    pub fn audit_query_for(&self, limit: usize, principal: &Principal) -> Result<Vec<AuditRecord>, String> {
        if principal.ceiling.unrestricted {
            return self.audit_query(limit);
        }
        self.store()
            .list_audits_for_principal(&principal.id, limit)
            .map_err(|e| e.to_string())
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

    /// `dash doctor` (§14.1): real store, vault, journal, Git sync, credential and budget checks.
    /// Read-only: it never repairs journals or records Generations.
    pub fn workspace_doctor(&self) -> Result<serde_json::Value, GitError> {
        let g = self.graph();
        let val = validate_configured_graph(&g, self.store(), false);
        let mut checks: Vec<serde_json::Value> = self
            .store()
            .health_checks()
            .into_iter()
            .map(|c| serde_json::json!({ "name": c.name, "ok": c.ok, "detail": c.detail }))
            .collect();
        let sqlite_ok = checks
            .iter()
            .filter(|c| c["name"].as_str().is_some_and(|n| n.starts_with("sqlite_")))
            .all(|c| c["ok"] == true);
        let journals = self
            .store()
            .list_incomplete_activation_journals()
            .map(|j| j.len())
            .map_err(|e| e.to_string());
        checks.push(match journals {
            Ok(0) => serde_json::json!({ "name": "activation_journal", "ok": true, "detail": "no incomplete activations" }),
            Ok(n) => serde_json::json!({ "name": "activation_journal", "ok": false, "detail": format!("{n} incomplete activation journal entr(y/ies); restart the daemon to reconcile") }),
            Err(e) => serde_json::json!({ "name": "activation_journal", "ok": false, "detail": e }),
        });
        let sync = self.store().get_git_sync_state().ok().flatten();
        checks.push(serde_json::json!({
            "name": "git_sync",
            "ok": sync.as_ref().map(|s| s.sync_status != "unsynced").unwrap_or(true),
            "detail": sync.as_ref().map(|s| format!("{}{}", s.sync_status, s.last_error.as_ref().map(|e| format!(": {e}")).unwrap_or_default())).unwrap_or_else(|| "no sync state recorded".into()),
        }));
        let budget = self.server.retention.db_size_budget_bytes;
        let size = self.store().db_size_bytes();
        checks.push(serde_json::json!({
            "name": "sqlite_size_budget",
            "ok": size <= budget,
            "detail": format!("{size} of {budget} bytes"),
        }));
        checks.push(serde_json::json!({
            "name": "graph_validation",
            "ok": val.valid,
            "detail": format!("{} issue(s)", val.issues.len()),
        }));
        let ok = checks.iter().all(|c| c["ok"] == true);
        Ok(serde_json::json!({
            "ok": ok,
            "checks": checks,
            "table_rows": self.store().table_row_counts().unwrap_or_default().into_iter().collect::<BTreeMap<_, _>>(),
            "retention_policy": self.server.retention,
            "git_sync_state": sync,
            "sqlite_ok": sqlite_ok,
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

fn draft_definition_exists(draft: &DraftRecord, package: &str, def_name: &str) -> bool {
    std::path::Path::new(&draft.worktree_path)
        .join("packages")
        .join(package)
        .join(def_name)
        .join("definition.luau")
        .exists()
}

/// Whether an Instance id is declared in any of the Draft's `instances/*.yaml` files.
fn draft_instance_exists(draft: &DraftRecord, instance_id: &str) -> Result<bool, String> {
    let dir = std::path::Path::new(&draft.worktree_path).join("instances");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Ok(false);
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let is_yaml = path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e == "yaml" || e == "yml");
        if !is_yaml || path.file_stem().and_then(|s| s.to_str()) == Some("service_principals") {
            continue;
        }
        let raw = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let doc: serde_yaml_ng::Value = serde_yaml_ng::from_str(&raw)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if let serde_yaml_ng::Value::Mapping(top) = doc {
            for (_, section) in top {
                if let serde_yaml_ng::Value::Mapping(m) = section {
                    if m.contains_key(serde_yaml_ng::Value::String(instance_id.to_string())) {
                        return Ok(true);
                    }
                }
            }
        }
    }
    Ok(false)
}
