//! Shared `AppServices` layer powering HTTP API, WebSocket, MCP, and CLI identically (§11.2, §14.1).

use chrono::{DateTime, Utc};
use dash_core::{
    validate_configured_graph, CapabilityCeiling, ConfiguredGraph, DashboardDoc, DryRunReport,
    ExecutionGraphSnapshot, HistorySeriesConfig, InstanceConfig, NetworkPolicy, Principal,
    QueryEnvelope, RuntimeEngine, RuntimeMetricsSnapshot, SanitizedLogEntry, ValidationReport,
    WidgetRequestEnvelope, WidgetRequestOutcome,
};
use dash_git::{
    read_packages_toml, write_packages_toml, ActivationResult, DraftManager, GitCommitInfo,
    GitError, GitStatusReport, PackageDependencySpec, PackagesLock, PreviewTestReport,
};
use dash_luau::{DefinitionCategory, DefinitionTestReport};
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

/// Background worker tick (seconds); also the minimum polling interval.
const BACKGROUND_TICK_SECS: u64 = 5;
/// How many recent spans/logs are scanned when filtering diagnostics for a non-Administrator.
const DIAGNOSTICS_SCAN_LIMIT: usize = 2000;
/// How often durable Event retention runs.
const EVENT_RETENTION_INTERVAL_SECS: i64 = 3600;
const EVENT_RETENTION_KEY: &str = "__dash.event_retention";

/// Aborts a spawned task when dropped (ties the consumer task to the worker handle).
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
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

    /// Graceful-shutdown step (§15.2): once HTTP and WebSocket readers are gone, cancel every
    /// running query and Adapter render so read work stops holding VM capacity while admitted
    /// actions drain. Reads have no side effects, so cancelling them is always safe. Returns the
    /// number of computations signalled.
    pub fn cancel_inflight_reads_for_shutdown(&self) -> usize {
        let n = self.engine().queries.cancel_all_in_flight();
        if n > 0 {
            tracing::info!("shutdown: cancelled {n} in-flight read computation(s)");
        }
        n
    }

    /// Audit a denial (§14.3 "Audit all capability denials") and return its message.
    fn deny(&self, principal: &Principal, operation: &str, target: &str, msg: &str) -> String {
        dash_core::audit_denial(self.store(), principal, operation, target, msg);
        msg.to_string()
    }

    fn require_draft_author(
        &self,
        principal: &Principal,
        operation: &str,
        target: &str,
    ) -> Result<(), String> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            return Err(self.deny(
                principal,
                operation,
                target,
                "principal lacks can_author_drafts",
            ));
        }
        Ok(())
    }

    fn require_administrator(
        &self,
        principal: &Principal,
        operation: &str,
        target: &str,
        msg: &str,
    ) -> Result<(), String> {
        if !principal.ceiling.unrestricted {
            return Err(self.deny(principal, operation, target, msg));
        }
        Ok(())
    }

    /// Attribute a Draft change to its Principal *before* applying it (fail closed). Activation
    /// checks every recorded editor's ceiling (secret bindings, Service principal consumers).
    fn record_draft_edit(
        &self,
        draft_id: &str,
        principal: &Principal,
        change: &str,
    ) -> Result<(), String> {
        self.store()
            .append_audit(
                &principal.id,
                principal.kind.as_str(),
                "draft.edit",
                draft_id,
                "ok",
                serde_json::json!({ "change": change }),
            )
            .map(|_| ())
            .map_err(|e| format!("failed to record draft attribution: {e}"))
    }

    /// The same checks `secret_bind` applies, for any path that binds a secret reference: the
    /// Principal's ceiling must allow the secret, the secret must exist, and its
    /// `allowed_bindings` policy must admit the Instance (§5.2). Denials are audited.
    fn check_secret_binding(
        &self,
        principal: &Principal,
        instance_id: &str,
        secret_name: &str,
    ) -> Result<SecretMetadata, String> {
        if !principal.ceiling.allows_secret(secret_name) {
            let _ = self.store().append_audit(
                &principal.id,
                principal.kind.as_str(),
                "secret.bind",
                secret_name,
                "denied",
                serde_json::json!({ "instance_id": instance_id, "reason": "ceiling" }),
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
            let _ = self.store().append_audit(
                &principal.id,
                principal.kind.as_str(),
                "secret.bind",
                secret_name,
                "denied",
                serde_json::json!({ "instance_id": instance_id, "reason": "allowed_bindings" }),
            );
            return Err(format!(
                "secret '{secret_name}' policy restricts allowed bindings"
            ));
        }
        Ok(meta)
    }

    /// An Instance as it stands in the Draft, read with the caller's draft ownership checked.
    fn draft_instance(
        &self,
        draft_id: &str,
        instance_id: &str,
        principal: &Principal,
    ) -> Result<Option<InstanceConfig>, String> {
        let draft = self.draft_status_as(draft_id, principal)?;
        let g = dash_git::load_workspace_graph(
            std::path::Path::new(&draft.worktree_path),
            self.drafts.data_dir(),
            &draft.base_revision,
            0,
            false,
        )
        .map_err(|e| e.to_string())?;
        Ok(g.instances.get(instance_id).cloned())
    }

    /// Spawn background workers (§7.2, §7.3, §7.4, ADR-0002), all under explicit Service
    /// principals:
    /// - a durable Event consumer task that drains the Event log from each automation's
    ///   checkpoint at startup, on every live Event or broadcast lag, and periodically;
    /// - an automation trigger task that fires due cron and Signal triggers and runs those
    ///   automations (with their actions and retry backoff) off the maintenance ticker, so a
    ///   slow or lock-contended automation never stalls polling, sampling or reconciles;
    /// - a maintenance ticker for idle VM reclamation, history sampling, scheduled polling,
    ///   reconcile retries and durable Event retention.
    ///
    /// Aborting the returned handle also stops the consumer and trigger tasks.
    pub fn spawn_background_workers(&self) -> tokio::task::JoinHandle<()> {
        // The sender is dropped immediately: the workers then simply never observe a shutdown.
        let (_tx, rx) = tokio::sync::watch::channel(false);
        self.spawn_background_workers_until(rx)
    }

    /// Like [`Self::spawn_background_workers`], but both loops stop picking up new work once
    /// `shutdown` becomes true; the in-progress iteration (e.g. an automation-initiated action)
    /// completes first, so the caller can drain it within its configured limit (§15.2).
    pub fn spawn_background_workers_until(
        &self,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let consumer = self.clone();
        let mut event_rx = consumer.engine().events.subscribe();
        let mut consumer_shutdown = shutdown.clone();
        let consumer_task = tokio::spawn(async move {
            // Startup catch-up: redeliver durable Events appended before a crash/restart.
            consumer.drain_durable_events().await;
            let mut tick = tokio::time::interval(Duration::from_secs(BACKGROUND_TICK_SECS));
            let mut shutdown_open = true;
            loop {
                if *consumer_shutdown.borrow() {
                    break;
                }
                tokio::select! {
                    changed = consumer_shutdown.changed(), if shutdown_open => {
                        match changed {
                            Err(_) => shutdown_open = false,
                            Ok(()) if *consumer_shutdown.borrow() => break,
                            Ok(()) => {}
                        }
                        continue;
                    }
                    _ = tick.tick() => {}
                    msg = event_rx.recv() => match msg {
                        Ok(ev) => {
                            // Ephemeral Events have no log: deliver them live (best effort).
                            if !ev.durable && !ev.is_replay {
                                let _ = consumer.engine().automations.dispatch_event(&ev).await;
                                continue;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            consumer.engine().telemetry.log(
                                "warn",
                                "events",
                                "",
                                "",
                                &format!("durable consumer lagged by {n} live Events; catching up from the log"),
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                }
                consumer.drain_durable_events().await;
            }
        });

        let triggers = self.clone();
        let mut trigger_shutdown = shutdown.clone();
        let trigger_task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(BACKGROUND_TICK_SECS));
            let mut shutdown_open = true;
            loop {
                if *trigger_shutdown.borrow() {
                    break;
                }
                tokio::select! {
                    changed = trigger_shutdown.changed(), if shutdown_open => {
                        match changed {
                            Err(_) => shutdown_open = false,
                            Ok(()) if *trigger_shutdown.borrow() => break,
                            Ok(()) => {}
                        }
                    }
                    _ = tick.tick() => triggers.run_due_automation_triggers().await,
                }
            }
        });

        let services = self.clone();
        let mut shutdown = shutdown;
        tokio::spawn(async move {
            let mut consumer = AbortOnDrop(consumer_task);
            let mut trigger_runs = AbortOnDrop(trigger_task);
            let mut ticker = tokio::time::interval(Duration::from_secs(BACKGROUND_TICK_SECS));
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
                        services.engine().queries.prune_idle();
                        // Host retry of `reconcile` with backoff for `unknown` executions (§9.1)
                        services.engine().actions.run_due_reconciles(16).await;
                        services.run_due_history_samples().await;
                        services.run_due_polls().await;
                        services.run_due_event_retention().await;
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
                }
            }
            // Shutdown: let the consumer and trigger tasks finish their in-progress delivery
            // (they observe the same signal); aborting the returned handle still aborts them
            // through AbortOnDrop.
            let _ = (&mut consumer.0).await;
            let _ = (&mut trigger_runs.0).await;
        })
    }

    /// One durable-consumer pass over the Event log (also used at startup).
    pub async fn drain_durable_events(&self) -> dash_core::automations::DrainReport {
        let report = self.engine().automations.drain_durable_events().await;
        if report.failed > 0 {
            self.engine().telemetry.log(
                "error",
                "events",
                "",
                "",
                &format!(
                    "{} durable Event deliveries failed and were dead-lettered",
                    report.failed
                ),
            );
        }
        report
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
                (
                    "trace_write_queue_depth",
                    snap.trace_write_queue_depth as f64,
                ),
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
                if let Err(e) = self.engine().history.sample_series(series, None).await {
                    self.engine().telemetry.log(
                        "warn",
                        "history",
                        &series.service_principal,
                        "",
                        &format!("history series '{}' not sampled: {e}", series.series_id),
                    );
                }
            }
        }
    }

    /// Fire due cron and Signal triggers and run their automations (the trigger task's body).
    pub async fn run_due_automation_triggers(&self) {
        let automations = &self.engine().automations;
        let fired = automations.run_due_schedules(Utc::now()).await;
        let signalled = automations.run_due_signal_triggers().await;
        for (auto_id, res) in fired.into_iter().chain(signalled) {
            if let Err(e) = res {
                self.engine().telemetry.log(
                    "error",
                    "automation",
                    "",
                    "",
                    &format!("automation '{auto_id}' trigger run failed: {e}"),
                );
            }
        }
    }

    async fn run_due_event_retention(&self) {
        let now_ts = Utc::now().timestamp();
        {
            let mut guard = self.last_poll_run.lock().await;
            let last = guard.get(EVENT_RETENTION_KEY).copied().unwrap_or(0);
            if now_ts - last < EVENT_RETENTION_INTERVAL_SECS {
                return;
            }
            guard.insert(EVENT_RETENTION_KEY.to_string(), now_ts);
        }
        if let Err(e) = self
            .engine()
            .events
            .prune_retained(dash_core::events::DEFAULT_EVENT_RETENTION_DAYS)
        {
            self.engine().telemetry.log(
                "warn",
                "events",
                "",
                "",
                &format!("event retention failed: {e}"),
            );
        }
    }

    /// Scheduled refresh for data providers that declare `poll_interval_secs` (§6.2, §7.2,
    /// ADR-0002), deduplicated per active query and run once per interval:
    /// - every cached query of the Instance that has live subscribers, or that a subscribed
    ///   query depends on, refreshed under that query's own Principal (so the subscribers'
    ///   cache partition receives the Signal);
    /// - background keep-warm queries: the parameter sets in the Instance's `config.keep_warm`
    ///   under its `run_as` Service principal, and the parameterized queries of history series on
    ///   the Instance under each series' Service principal.
    ///
    /// When a refreshed value changes, active downstream dependents are refreshed too.
    /// Returns the number of queries refreshed.
    pub async fn run_due_polls(&self) -> usize {
        self.run_polls_at(Utc::now().timestamp()).await
    }

    /// [`Self::run_due_polls`] evaluated at an explicit Unix time (deterministic tests).
    pub async fn run_polls_at(&self, now_ts: i64) -> usize {
        let g = self.graph();
        let mut refreshed = 0;
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
                if now_ts - last >= interval.max(BACKGROUND_TICK_SECS) as i64 {
                    guard.insert(inst_id.clone(), now_ts);
                    true
                } else {
                    false
                }
            };
            if due {
                refreshed += self.poll_instance(&g, inst_id, inst, def).await;
            }
        }
        refreshed
    }

    async fn poll_instance(
        &self,
        g: &ConfiguredGraph,
        inst_id: &str,
        inst: &InstanceConfig,
        def: &dash_core::LoadedDefinition,
    ) -> usize {
        let queries = &self.engine().queries;
        let mut targets: BTreeMap<String, (serde_json::Value, Principal)> = BTreeMap::new();

        for (qk, entry) in queries.cached_entries_for_instance(inst_id) {
            if self.query_is_active(&qk) {
                targets.insert(qk, (entry.params, entry.principal));
            }
        }

        let mut keep_warm: Vec<(String, serde_json::Value)> = Vec::new();
        if let Some(sp) = inst.run_as.as_deref() {
            if let Some(sets) = inst.config.get("keep_warm").and_then(|v| v.as_array()) {
                keep_warm.extend(sets.iter().map(|p| (sp.to_string(), p.clone())));
            }
        }
        for series in g
            .workspace
            .history
            .iter()
            .filter(|h| h.instance_id == inst_id)
        {
            keep_warm.push((series.service_principal.clone(), series.params.clone()));
        }
        for (sp_id, params) in keep_warm {
            match self.engine().automations.resolve_service_principal(&sp_id) {
                Ok(sp) => {
                    let qk = queries.compute_query_key(
                        inst_id,
                        &def.manifest.identity_version,
                        &params,
                        &sp,
                    );
                    targets.entry(qk).or_insert((params, sp));
                }
                Err(e) => self.engine().telemetry.log(
                    "warn",
                    "polling",
                    &sp_id,
                    "",
                    &format!("keep-warm poll of '{inst_id}' skipped: {e}"),
                ),
            }
        }

        let mut refreshed = 0;
        for (qk, (params, principal)) in targets {
            let before = queries.cached_entry(&qk).map(|e| e.value);
            match queries.query(inst_id, params, &principal, true).await {
                Ok(env) => {
                    refreshed += 1;
                    if env.status == dash_core::QueryStatus::Fresh && before != env.value {
                        refreshed += self.refresh_active_dependents(&qk).await;
                    }
                }
                Err(e) => self.engine().telemetry.log(
                    "warn",
                    "polling",
                    &principal.id,
                    "",
                    &format!("scheduled poll of '{inst_id}' failed: {e}"),
                ),
            }
        }
        refreshed
    }

    /// A query is active when it has live Signal subscribers or a (transitive) dependent has.
    fn query_is_active(&self, query_key: &str) -> bool {
        let queries = &self.engine().queries;
        let signals = &self.engine().signals;
        let mut stack = vec![query_key.to_string()];
        let mut seen = std::collections::BTreeSet::new();
        while let Some(qk) = stack.pop() {
            if !seen.insert(qk.clone()) {
                continue;
            }
            if signals.subscriber_count(&qk) > 0 {
                return true;
            }
            stack.extend(queries.reverse_subscribers(&qk));
        }
        false
    }

    async fn refresh_active_dependents(&self, query_key: &str) -> usize {
        let queries = &self.engine().queries;
        let mut refreshed = 0;
        let mut frontier: Vec<String> =
            queries.reverse_subscribers(query_key).into_iter().collect();
        let mut seen = std::collections::BTreeSet::new();
        while let Some(dep) = frontier.pop() {
            if !seen.insert(dep.clone()) || !self.query_is_active(&dep) {
                continue;
            }
            if let Some(entry) = queries.cached_entry(&dep) {
                if queries
                    .query(
                        &entry.instance_id,
                        entry.params.clone(),
                        &entry.principal,
                        true,
                    )
                    .await
                    .is_ok()
                {
                    refreshed += 1;
                }
            }
            frontier.extend(queries.reverse_subscribers(&dep));
        }
        refreshed
    }

    // ---- Automations: execution records, dead letters, replay (§7.2, §7.3) ----

    pub fn automation_executions(
        &self,
        automation_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<dash_store::AutomationExecutionRecord>, String> {
        self.engine()
            .automations
            .list_executions(automation_id, limit)
    }

    pub fn dead_letters(
        &self,
        include_resolved: bool,
        limit: usize,
        principal: &Principal,
    ) -> Result<Vec<dash_store::DeadLetterRecord>, String> {
        if !principal.ceiling.unrestricted && !principal.ceiling.can_resolve_executions {
            return Err("principal lacks authority to inspect dead letters".into());
        }
        self.engine()
            .automations
            .list_dead_letters(include_resolved, limit)
    }

    pub async fn dead_letter_redrive(
        &self,
        dlq_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        self.engine()
            .automations
            .redrive_dead_letter(dlq_id, principal)
            .await
    }

    pub async fn events_replay(
        &self,
        source_instance: &str,
        after_sequence: i64,
        reexecute: bool,
        principal: &Principal,
    ) -> Result<Vec<Result<serde_json::Value, String>>, String> {
        if !principal.ceiling.unrestricted && !principal.ceiling.can_resolve_executions {
            return Err("principal lacks authority to replay Events".into());
        }
        self.engine()
            .automations
            .replay_events(
                source_instance,
                after_sequence,
                reexecute.then_some(principal),
            )
            .await
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
                    || d.manifest
                        .description
                        .to_ascii_lowercase()
                        .contains(&needle)
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
        mask_graph_view_for(principal, &mut val);
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
            "instance": inst.map(|i| {
                // Secret names outside the caller's ceiling stay hidden (§5.2, §11.1).
                let mut one = serde_json::json!({ target: i });
                mask_instance_secrets(principal, &mut one);
                one[target].take()
            }),
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
            verdict(dash_core::check_action_authority(
                principal,
                i,
                &g,
                self.store(),
            ))
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
    pub fn authorize_draft(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            self.audit_denial(
                principal,
                "draft.access",
                draft_id,
                "principal lacks can_author_drafts",
            );
            return Err("principal lacks can_author_drafts".into());
        }
        let draft = self.draft_status(draft_id)?;
        if !principal.ceiling.unrestricted && draft.created_by != principal.id {
            self.audit_denial(
                principal,
                "draft.access",
                draft_id,
                "draft belongs to another principal",
            );
            return Err(format!(
                "draft '{draft_id}' not found or not owned by this principal"
            ));
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

    // Draft status/list/validate/preview/test/rebase/abort `_as` variants are defined below and
    // enforce ownership inside DraftManager.

    pub async fn draft_activate_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<ActivationResult, String> {
        self.authorize_draft(draft_id, principal)?;
        self.draft_activate(draft_id, principal).await
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
        self.definition_upsert_in_draft(
            draft_id,
            package,
            def_name,
            definition_luau,
            runtime_luau,
            tests_luau,
            principal,
        )
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
        self.definition_upsert_in_draft(
            draft_id,
            package,
            def_name,
            definition_luau,
            runtime_luau,
            tests_luau,
            principal,
        )
    }

    /// `instance_create`: fails when an Instance with this id already exists in the Draft.
    pub fn instance_create_in_draft(
        &self,
        draft_id: &str,
        inst: &InstanceConfig,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        let draft = self.authorize_draft(draft_id, principal)?;
        if draft_instance_exists(&draft, &inst.id)? {
            return Err(format!(
                "instance '{}' already exists in draft; use instance_configure",
                inst.id
            ));
        }
        self.instance_upsert_in_draft(draft_id, inst, principal)
    }

    /// `instance_configure`: fails when the Instance does not exist in the Draft.
    pub fn instance_configure_in_draft(
        &self,
        draft_id: &str,
        inst: &InstanceConfig,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        let draft = self.authorize_draft(draft_id, principal)?;
        if !draft_instance_exists(&draft, &inst.id)? {
            return Err(format!(
                "instance '{}' does not exist in draft; use instance_create",
                inst.id
            ));
        }
        self.instance_upsert_in_draft(draft_id, inst, principal)
    }

    /// Administrator-only hot reload of the bound Workspace via the Draft protocol (`dash reload`).
    pub async fn workspace_reload(
        &self,
        principal: &Principal,
    ) -> Result<ActivationResult, String> {
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
        self.draft_begin_at(principal, None).await
    }

    /// Begin a Draft from a specified base Revision (`None` = the active Revision, §12.2 step 1).
    pub async fn draft_begin_at(
        &self,
        principal: &Principal,
        base_revision: Option<&str>,
    ) -> Result<DraftRecord, String> {
        self.require_draft_author(principal, "draft.begin", "draft")?;
        let rec = self
            .drafts
            .draft_begin_at(principal, base_revision)
            .await
            .map_err(|e| e.to_string())?;
        let _ = self.store().append_audit(
            &principal.id,
            principal.kind.as_str(),
            "draft.begin",
            &rec.draft_id,
            "ok",
            serde_json::json!({ "base_revision": rec.base_revision }),
        );
        Ok(rec)
    }

    // The draft methods without a Principal act as the Administrator and exist for trusted
    // in-process callers (CLI). Network-facing handlers use the `_as` variants, which enforce
    // draft ownership and `can_author_drafts` (§5.1 deny by default, §20 attribution).

    pub fn draft_status(&self, draft_id: &str) -> Result<DraftRecord, String> {
        self.draft_status_as(draft_id, &Principal::administrator())
    }

    pub fn draft_status_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        self.drafts
            .draft_status_as(draft_id, principal)
            .map_err(|e| e.to_string())
    }

    pub fn draft_list(&self) -> Result<Vec<DraftRecord>, String> {
        self.drafts.draft_list().map_err(|e| e.to_string())
    }

    pub fn draft_list_for(&self, principal: &Principal) -> Result<Vec<DraftRecord>, String> {
        self.drafts
            .draft_list_for(principal)
            .map_err(|e| e.to_string())
    }

    pub async fn draft_validate(&self, draft_id: &str) -> Result<ValidationReport, String> {
        self.draft_validate_as(draft_id, &Principal::administrator())
            .await
    }

    pub async fn draft_validate_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<ValidationReport, String> {
        self.drafts
            .draft_validate_as(draft_id, principal)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn draft_preview(&self, draft_id: &str) -> Result<PreviewTestReport, String> {
        self.draft_preview_as(draft_id, &Principal::administrator())
            .await
    }

    pub async fn draft_preview_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<PreviewTestReport, String> {
        self.drafts
            .draft_preview_and_test_as(draft_id, principal)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn draft_test(&self, draft_id: &str) -> Result<PreviewTestReport, String> {
        self.draft_test_as(draft_id, &Principal::administrator())
            .await
    }

    /// Test-focused report: validation, Definition tests and Scenario results (no Preview renders).
    pub async fn draft_test_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<PreviewTestReport, String> {
        let mut report = self.draft_preview_as(draft_id, principal).await?;
        report.preview_renders.clear();
        Ok(report)
    }

    pub async fn draft_rebase(&self, draft_id: &str) -> Result<DraftRecord, String> {
        self.draft_rebase_as(draft_id, &Principal::administrator())
            .await
    }

    pub async fn draft_rebase_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        self.drafts
            .draft_rebase_as(draft_id, principal)
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
        self.draft_abort_as(draft_id, &Principal::administrator())
            .await
    }

    pub async fn draft_abort_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<(), String> {
        self.drafts
            .draft_abort_as(draft_id, principal)
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
        self.require_draft_author(principal, "draft.edit", draft_id)?;
        self.record_draft_edit(
            draft_id,
            principal,
            &format!("definition:{package}.{def_name}"),
        )?;
        self.drafts
            .draft_write_definition_as(
                draft_id,
                package,
                def_name,
                definition_luau,
                runtime_luau,
                tests_luau,
                principal,
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
        let graph = dash_git::workspace::load_workspace_graph_async(
            &ws_dir,
            self.drafts.data_dir(),
            "test",
            0,
            is_preview,
        )
        .await
        .map_err(|e| e.to_string())?;
        // §4.5: Definition tests run against Mocks, not a null bridge.
        dash_git::run_definition_tests_against_mocks(Arc::new(graph), definition_id).await
    }

    /// Upsert an Instance in a Draft. Any new or changed secret reference goes through the same
    /// checks as `secret_bind` (ceiling, existence, `allowed_bindings`) and is audited as
    /// `secret.bind`; Agents cannot bind ungranted secrets by writing the `secrets` map (§5.2).
    pub fn instance_upsert_in_draft(
        &self,
        draft_id: &str,
        inst: &InstanceConfig,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        self.require_draft_author(principal, "draft.edit", draft_id)?;
        let mut newly_bound: Vec<(String, String)> = Vec::new();
        if !inst.secrets.is_empty() {
            let existing = self.draft_instance(draft_id, &inst.id, principal)?;
            for slot in inst.secrets.keys() {
                let unchanged = existing.as_ref().and_then(|e| e.resolved_secret_name(slot))
                    == inst.resolved_secret_name(slot);
                if unchanged {
                    continue;
                }
                let Some(secret_name) = inst.resolved_secret_name(slot) else {
                    continue;
                };
                self.check_secret_binding(principal, &inst.id, &secret_name)?;
                newly_bound.push((slot.clone(), secret_name));
            }
        }
        self.record_draft_edit(draft_id, principal, &format!("instance.upsert:{}", inst.id))?;
        let rec = self
            .drafts
            .draft_upsert_instance_as(draft_id, inst, principal)
            .map_err(|e| e.to_string())?;
        for (slot, secret_name) in newly_bound {
            let _ = self.store().append_audit(
                &principal.id,
                principal.kind.as_str(),
                "secret.bind",
                &secret_name,
                "ok",
                serde_json::json!({ "instance_id": inst.id, "slot": slot, "draft_id": draft_id }),
            );
        }
        Ok(rec)
    }

    pub fn instance_delete_in_draft(
        &self,
        draft_id: &str,
        instance_id: &str,
        principal: &Principal,
    ) -> Result<DraftRecord, String> {
        self.require_draft_author(principal, "draft.edit", draft_id)?;
        self.record_draft_edit(
            draft_id,
            principal,
            &format!("instance.delete:{instance_id}"),
        )?;
        self.drafts
            .draft_delete_instance_as(draft_id, instance_id, principal)
            .map_err(|e| e.to_string())
    }

    pub async fn dashboard_get(
        &self,
        dashboard_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        self.dashboard_get_in(self.graph(), dashboard_id, principal)
            .await
    }

    /// Render and document from one Generation snapshot: a concurrent activation never mixes
    /// the old render with the new document (no visible partial state, §12.2 step 7).
    async fn dashboard_get_in(
        &self,
        g: Arc<ConfiguredGraph>,
        dashboard_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        let rendered = self
            .engine()
            .queries
            .render_dashboard_in(Arc::clone(&g), dashboard_id, principal)
            .await?;
        // Widget Requests are accepted only for widget_ids of the current render (Appendix A.3)
        self.engine().actions.record_rendered_dashboard(&rendered);
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
        self.require_draft_author(principal, "draft.edit", draft_id)?;
        self.record_draft_edit(draft_id, principal, &format!("dashboard:{}", doc.id))?;
        self.drafts
            .draft_update_dashboard_as(draft_id, doc, principal)
            .map_err(|e| e.to_string())
    }

    pub async fn dashboard_preview(
        &self,
        draft_id: &str,
        dashboard_id: &str,
    ) -> Result<serde_json::Value, String> {
        self.dashboard_preview_as(draft_id, dashboard_id, &Principal::administrator())
            .await
    }

    pub async fn dashboard_preview_as(
        &self,
        draft_id: &str,
        dashboard_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        let report = self.draft_preview_as(draft_id, principal).await?;
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

    /// Widget Request dispatch returning a real action execution, or a refresh that is not one.
    pub async fn dispatch_widget_request_outcome(
        &self,
        req: WidgetRequestEnvelope,
        principal: &Principal,
    ) -> Result<WidgetRequestOutcome, String> {
        if self.server.is_shutting_down() {
            return Err("daemon is shutting down; not admitting new actions".into());
        }
        self.engine()
            .actions
            .dispatch_widget_request_outcome(req, principal)
            .await
    }

    pub fn execution_inspect(
        &self,
        execution_id: Option<&str>,
    ) -> Result<serde_json::Value, String> {
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
            let scan = if principal.ceiling.unrestricted {
                100
            } else {
                1000
            };
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

    pub async fn execution_cancel(
        &self,
        execution_id: &str,
        reason: &str,
        principal: &Principal,
    ) -> Result<ActionExecutionRecord, String> {
        self.engine()
            .actions
            .cancel_execution(execution_id, reason, principal)
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

    /// Trace ids attributable to `principal` among the given spans: a trace belongs to the
    /// Principal when any of its spans names it (span principal or the request root span's
    /// `principal_id` attribute).
    fn traces_owned_by(
        spans: &[TraceSpanRecord],
        principal: &Principal,
    ) -> std::collections::HashSet<String> {
        spans
            .iter()
            .filter(|s| {
                s.principal_id == principal.id
                    || s.attributes.get("principal_id").and_then(|v| v.as_str())
                        == Some(principal.id.as_str())
            })
            .map(|s| s.trace_id.clone())
            .collect()
    }

    /// Trace inspection as seen by `principal` (§11.1, §15.1): the Administrator sees every
    /// span; any other Principal only the spans of its own traces, so it never sees another
    /// Principal's activity or client addresses.
    pub fn graph_trace_for(&self, limit: usize, principal: &Principal) -> serde_json::Value {
        if principal.ceiling.unrestricted {
            return self.graph_trace(limit);
        }
        let exec_graph: ExecutionGraphSnapshot = self.engine().queries.execution_graph_snapshot();
        let all = self.engine().telemetry.recent_spans(DIAGNOSTICS_SCAN_LIMIT);
        let owned = Self::traces_owned_by(&all, principal);
        let spans: Vec<TraceSpanRecord> = all
            .into_iter()
            .filter(|s| owned.contains(&s.trace_id))
            .take(limit)
            .collect();
        serde_json::json!({
            "execution_graph": exec_graph,
            "spans": spans,
        })
    }

    /// Logs visible to `principal`: everything for the Administrator, otherwise only entries
    /// attributed to the Principal or belonging to one of its traces.
    pub fn logs_query_for(&self, limit: usize, principal: &Principal) -> Vec<SanitizedLogEntry> {
        if principal.ceiling.unrestricted {
            return self.logs_query(limit);
        }
        let telemetry = &self.engine().telemetry;
        let owned =
            Self::traces_owned_by(&telemetry.recent_spans(DIAGNOSTICS_SCAN_LIMIT), principal);
        telemetry
            .recent_logs(DIAGNOSTICS_SCAN_LIMIT)
            .into_iter()
            .filter(|l| l.principal_id == principal.id || owned.contains(&l.trace_id))
            .take(limit)
            .collect()
    }

    pub fn metrics_query(&self) -> RuntimeMetricsSnapshot {
        self.engine().metrics_snapshot()
    }

    pub fn audit_query(&self, limit: usize) -> Result<Vec<AuditRecord>, String> {
        self.store().list_audits(limit).map_err(|e| e.to_string())
    }

    /// Audit trail visible to `principal`: everything for the Administrator, only the
    /// Principal's own attributed records otherwise.
    pub fn audit_query_for(
        &self,
        limit: usize,
        principal: &Principal,
    ) -> Result<Vec<AuditRecord>, String> {
        if principal.ceiling.unrestricted {
            return self.audit_query(limit);
        }
        self.store()
            .list_audits_for_principal(&principal.id, limit)
            .map_err(|e| e.to_string())
    }

    // ---- Git ----

    /// Workspace Git status. Credentials embedded in the remote URL (e.g. a push PAT in
    /// `https://user:token@host/...`) are never returned: they are not vault secrets, so the
    /// redactor does not know them (§5.2, §14.3).
    pub async fn git_status(&self) -> Result<GitStatusReport, String> {
        self.drafts
            .git_status()
            .await
            .map(redact_git_status)
            .map_err(|e| redact_url_credentials_in_text(&e.to_string()))
    }

    pub async fn git_history(&self, limit: usize) -> Result<Vec<GitCommitInfo>, String> {
        self.drafts
            .git_history(limit)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn git_sync(&self, principal: &Principal) -> Result<GitSyncRecord, String> {
        if !principal.ceiling.can_git_sync && !principal.ceiling.unrestricted {
            return Err(self.deny(
                principal,
                "git.sync",
                "workspace",
                "principal lacks can_git_sync",
            ));
        }
        self.drafts
            .git_sync(principal)
            .await
            .map(redact_git_sync)
            .map_err(|e| redact_url_credentials_in_text(&e.to_string()))
    }

    pub async fn dependency_update(
        &self,
        draft_id: &str,
        package_name: &str,
        spec: PackageDependencySpec,
        principal: &Principal,
    ) -> Result<(DraftRecord, PackagesLock), String> {
        self.require_draft_author(principal, "package.update", package_name)?;
        // Only unrestricted (Administrator) principals may point dependencies at local paths.
        dash_git::packages::validate_dependency_spec(
            package_name,
            &spec,
            principal.ceiling.unrestricted,
        )
        .map_err(|e| e.to_string())?;
        self.record_draft_edit(draft_id, principal, &format!("dependency:{package_name}"))?;
        let requested = serde_json::to_value(&spec).unwrap_or_default();
        let result = self
            .drafts
            .draft_update_dependency_as(draft_id, package_name, spec, principal)
            .await
            .map_err(|e| e.to_string());
        // §14.3: audit Package installation/updates, including the pinned lock entry.
        let (outcome, detail) = match &result {
            Ok((_, lock)) => (
                "ok",
                serde_json::json!({
                    "draft_id": draft_id,
                    "requested": requested,
                    "locked": lock.packages.iter().find(|p| p.name == package_name),
                }),
            ),
            Err(e) => (
                "error",
                serde_json::json!({ "draft_id": draft_id, "requested": requested, "error": e }),
            ),
        };
        let _ = self.store().append_audit(
            &principal.id,
            principal.kind.as_str(),
            "package.update",
            package_name,
            outcome,
            detail,
        );
        result
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
        self.secret_set_with_policy(
            name,
            description,
            plaintext,
            allowed_bindings,
            None,
            None,
            principal,
        )
    }

    /// Set a secret plus its Administrator policy: destination hosts the credential may be
    /// attached to and how the broker presents it (`bearer`, `basic`, `key`, `header:<Name>`,
    /// `query:<param>`). `None` keeps the current policy value.
    #[allow(clippy::too_many_arguments)]
    pub fn secret_set_with_policy(
        &self,
        name: &str,
        description: &str,
        plaintext: &str,
        allowed_bindings: &[String],
        allowed_hosts: Option<&[String]>,
        credential_scheme: Option<&str>,
        principal: &Principal,
    ) -> Result<SecretMetadata, String> {
        self.secret_update(
            name,
            plaintext,
            dash_store::SecretPolicyUpdate {
                description: Some(description),
                allowed_bindings: Some(allowed_bindings),
                allowed_hosts,
                credential_scheme,
            },
            principal,
        )
    }

    /// Set or rotate a secret value, changing only the policy fields `update` names. A plain
    /// rotation (every field `None`) keeps the description, `allowed_bindings`, destination
    /// hosts and credential scheme, so it never silently widens the binding policy (§5.2).
    pub fn secret_update(
        &self,
        name: &str,
        plaintext: &str,
        update: dash_store::SecretPolicyUpdate<'_>,
        principal: &Principal,
    ) -> Result<SecretMetadata, String> {
        self.require_administrator(
            principal,
            "secret.set",
            name,
            "only the Administrator may set secret values",
        )?;
        let meta = self
            .store()
            .set_secret_value(name, plaintext, update)
            .map_err(|e| e.to_string())?;
        let _ = self.store().append_audit(
            &principal.id,
            principal.kind.as_str(),
            "secret.set",
            name,
            "ok",
            serde_json::json!({
                "key_version": meta.key_version,
                "allowed_bindings": meta.allowed_bindings,
                "allowed_hosts": meta.allowed_hosts,
                "credential_scheme": meta.credential_scheme,
            }),
        );
        Ok(meta)
    }

    // ---- Network ceiling (Administrator-only, outside the Git Workspace) ----

    /// Requested Workspace `[network]` policy, the Administrator ceiling, and whether the
    /// request is fully covered (§5.2, §11.3).
    pub fn network_policy_status(&self) -> serde_json::Value {
        let g = self.graph();
        let ceiling = self.store().get_network_policy_ceiling().ok().flatten();
        let parsed: Option<NetworkPolicy> = ceiling
            .as_ref()
            .and_then(|(v, _, _)| serde_json::from_value(v.clone()).ok());
        let requested = &g.workspace.network;
        let pending_hosts: Vec<&String> = requested
            .allowed_hosts
            .iter()
            .filter(|h| parsed.as_ref().map(|c| !c.allows_host(h)).unwrap_or(true))
            .collect();
        serde_json::json!({
            "requested": requested,
            "granted": parsed,
            "granted_by": ceiling.as_ref().map(|(_, by, _)| by),
            "granted_at": ceiling.as_ref().map(|(_, _, at)| at),
            "pending_hosts": pending_hosts,
        })
    }

    /// Set the Administrator network ceiling (audited as `network_policy.grant`).
    pub fn network_policy_grant(
        &self,
        policy: NetworkPolicy,
        actor: &Principal,
    ) -> Result<(), String> {
        self.require_administrator(
            actor,
            "network_policy.grant",
            "network",
            "only the Administrator can grant the network ceiling",
        )?;
        let value = serde_json::to_value(&policy).map_err(|e| e.to_string())?;
        self.store()
            .set_network_policy_ceiling(value.clone(), &actor.id)
            .map_err(|e| e.to_string())?;
        let _ = self.store().append_audit(
            &actor.id,
            actor.kind.as_str(),
            "network_policy.grant",
            "network",
            "ok",
            value,
        );
        Ok(())
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
        self.require_draft_author(principal, "draft.edit", draft_id)?;
        self.check_secret_binding(principal, instance_id, secret_name)?;

        let mut inst = self
            .draft_instance(draft_id, instance_id, principal)?
            .ok_or_else(|| format!("instance '{instance_id}' not found in draft"))?;
        inst.secrets
            .insert(slot.to_string(), format!("secret://{secret_name}"));
        self.record_draft_edit(
            draft_id,
            principal,
            &format!("secret.bind:{instance_id}.{slot}"),
        )?;
        let updated = self
            .drafts
            .draft_upsert_instance_as(draft_id, &inst, principal)
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
        self.require_administrator(
            actor,
            "service_principal.grant",
            principal_id,
            "only the Administrator can grant Service principal ceilings",
        )?;
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
        self.require_administrator(
            actor,
            "agent.create",
            name,
            "only the Administrator can issue Agent MCP tokens",
        )?;
        let mut safe_ceiling = ceiling;
        // Agents NEVER become unrestricted Administrators!
        safe_ceiling.unrestricted = false;
        let plaintext = format!(
            "dsh_agt_{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
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

    /// Agent inventory (every Agent and its ceiling) is Administrator data (§5.1 deny by
    /// default): other Principals are refused.
    pub fn agent_list_for(&self, actor: &Principal) -> Result<Vec<AgentPrincipalRecord>, String> {
        self.require_administrator(
            actor,
            "agent.list",
            "agents",
            "only the Administrator can list Agent principals",
        )?;
        self.agent_list()
    }

    /// Service principal requests and grants, Administrator only (§5.1, §7.4).
    pub fn service_principal_list_for(
        &self,
        actor: &Principal,
    ) -> Result<Vec<ServicePrincipalStatus>, String> {
        self.require_administrator(
            actor,
            "service_principal.list",
            "service_principals",
            "only the Administrator can list Service principal grants",
        )?;
        Ok(self.service_principal_list())
    }

    pub fn agent_revoke(&self, agent_id: &str, actor: &Principal) -> Result<bool, String> {
        self.require_administrator(
            actor,
            "agent.revoke",
            agent_id,
            "only the Administrator can revoke Agent tokens",
        )?;
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
        let key_protection = self.store().vault().key_protection().clone();
        let warnings: Vec<String> = key_protection.warning().into_iter().collect();
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
        let sync = self
            .store()
            .get_git_sync_state()
            .ok()
            .flatten()
            .map(redact_git_sync);
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
            "vault_key_protection": key_protection,
            "network_policy": self.network_policy_status(),
            "warnings": warnings,
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
            "startup_warnings": self.drafts.startup_warnings(),
        }))
    }
}

/// Mask, in a serialized `instances` map, every `secret://<name>` reference the Principal's
/// ceiling does not permit, so secret names and existence do not leak (§5.2, §11.1).
pub fn mask_instance_secrets(principal: &Principal, instances: &mut serde_json::Value) {
    if principal.ceiling.unrestricted {
        return;
    }
    let Some(instances) = instances.as_object_mut() else {
        return;
    };
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

/// Per-Principal view of a graph document (`graph_inspect`, Draft graph): masks secret
/// references outside the ceiling and, for non-Administrators, drops who granted a Service
/// principal ceiling and when (Administrator audit data).
fn mask_graph_view_for(principal: &Principal, val: &mut serde_json::Value) {
    if principal.ceiling.unrestricted {
        return;
    }
    if let Some(instances) = val.get_mut("instances") {
        mask_instance_secrets(principal, instances);
    }
    if let Some(sps) = val
        .get_mut("service_principals")
        .and_then(|v| v.as_array_mut())
    {
        for sp in sps.iter_mut().filter_map(|v| v.as_object_mut()) {
            sp.remove("granted_by");
            sp.remove("granted_at");
        }
    }
}

/// Strip userinfo credentials from a Git remote URL before it is shown anywhere
/// (`https://x-access-token:ghp_…@github.com/o/r.git` -> `https://github.com/o/r.git`,
/// `git@github.com:o/r.git` -> `github.com:o/r.git`). Local paths are returned unchanged.
pub fn redact_remote_url(raw: &str) -> String {
    if raw.contains("://") {
        if let Ok(mut u) = url::Url::parse(raw) {
            if u.username().is_empty() && u.password().is_none() {
                return raw.to_string();
            }
            if u.set_password(None).is_ok() && u.set_username("").is_ok() {
                return u.to_string();
            }
        }
        return redact_url_credentials_in_text(raw);
    }
    // scp-like syntax `[user@]host:path` (no scheme).
    if let Some((user_host, path)) = raw.split_once(':') {
        if !user_host.contains('/') {
            if let Some((_, host)) = user_host.rsplit_once('@') {
                return format!("{host}:{path}");
            }
        }
    }
    raw.to_string()
}

/// Remove `user[:password]@` from every `scheme://` URL embedded in free text (e.g. a Git error
/// message that echoes the remote URL).
pub fn redact_url_credentials_in_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(idx) = rest.find("://") {
        let (head, tail) = rest.split_at(idx + 3);
        out.push_str(head);
        let end = tail
            .find(|c: char| c.is_whitespace() || matches!(c, '/' | '?' | '#' | '\'' | '"' | '>'))
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        match authority.rfind('@') {
            Some(at) => out.push_str(&authority[at + 1..]),
            None => out.push_str(authority),
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

fn redact_git_status(mut s: GitStatusReport) -> GitStatusReport {
    s.remote_url = s.remote_url.as_deref().map(redact_remote_url);
    s.last_error = s.last_error.as_deref().map(redact_url_credentials_in_text);
    s
}

fn redact_git_sync(mut s: GitSyncRecord) -> GitSyncRecord {
    s.remote_url = s.remote_url.as_deref().map(redact_remote_url);
    s.last_error = s.last_error.as_deref().map(redact_url_credentials_in_text);
    s
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
        let doc: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&raw).map_err(|e| format!("{}: {e}", path.display()))?;
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
        if let Some(wt) = obj
            .get("widget_type")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        {
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
        let g = self.graph();
        let mut data = self
            .dashboard_get_in(Arc::clone(&g), dashboard_id, principal)
            .await?;
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

    fn load_draft_graph(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<(DraftRecord, ConfiguredGraph), String> {
        // Draft ownership is enforced like every other draft read (§5.1, §11.3).
        let draft = self.draft_status_as(draft_id, principal)?;
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
    pub fn draft_graph_inspect(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        let (draft, g) = self.load_draft_graph(draft_id, principal)?;
        let validation = validate_configured_graph(&g, self.store(), false);
        let mut val = serde_json::json!({
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
        });
        // A Draft is a full copy of the Workspace: apply the same per-Principal secret-name
        // masking as `graph_inspect_for` (§5.2).
        mask_graph_view_for(principal, &mut val);
        Ok(val)
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
        principal: &Principal,
    ) -> Result<DefinitionSources, String> {
        let (ws_dir, def) = match draft_id {
            Some(id) => {
                let (draft, g) = self.load_draft_graph(id, principal)?;
                let def =
                    g.definitions.get(definition_id).cloned().ok_or_else(|| {
                        format!("definition '{definition_id}' not found in draft")
                    })?;
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
    pub async fn draft_diff(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<DraftDiff, String> {
        use dash_git::git_cli::{run_git, run_git_allow_exit_1};
        let draft = self.draft_status_as(draft_id, principal)?;
        if draft.status == "aborted" || draft.status == "activated" {
            return Err(format!("draft '{draft_id}' is {}", draft.status));
        }
        let wt = PathBuf::from(&draft.worktree_path);
        let base = draft.base_revision.clone();
        let mut files: Vec<DraftDiffFile> = Vec::new();
        let mut budget = MAX_DRAFT_DIFF_BYTES;
        let mut truncated = false;
        let mut push_patch =
            |files: &mut Vec<DraftDiffFile>, path: String, status: String, patch: String| {
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
                files.push(DraftDiffFile {
                    path,
                    status,
                    patch,
                });
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
            let patch = run_git(
                &wt,
                &["diff", "--no-color", "--no-renames", &base, "--", &path],
            )
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
