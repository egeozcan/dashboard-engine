//! Action execution lifecycle (§9), Dry run, nested calls, `unknown` reconcile,
//! manual resolution, cancellation, invalidation, and widget Request dispatch.
//!
//! # Outcome classification (`unknown` vs `failed`, §9.1, Appendix A.4)
//!
//! The host never classifies an outcome from free-form error text. It uses:
//!
//! * the typed [`RuntimeError`] of the handler run (`Timeout`, `Cancelled`, ...);
//! * host-tracked side-effect state of the run: which connector operations not declared
//!   `read_only` were started or completed, and whether nested actions were invoked;
//! * the stable [`UNKNOWN_OUTCOME_CODE`] carried by host errors whose request may have reached
//!   the upstream (HTTP transport failure after connect, lost response body, connector interrupted
//!   mid-flight), or raised explicitly by a connector (`error("UNKNOWN_OUTCOME: ...")`, e.g. on a
//!   5xx gateway reply to a write);
//! * an explicit `unknown` result from the action handler: `return dash.unknown_outcome(reason)`
//!   (equivalent to returning `{ __unknown = true, reason = ... }`).
//!
//! A run whose outcome cannot be proven becomes `unknown` (never retried automatically). `failed`
//! records carry `retry_safe = true` only when no external effect can have happened; only such
//! records release their idempotency key for a fresh attempt.
//!
//! # Resolving `unknown`
//!
//! An `unknown` execution whose Definition declares `reconcile` is reconciled once inline, then by
//! the host with exponential backoff and jitter ([`ActionManager::run_due_reconciles`]) under the
//! *original* Principal rebuilt from the record, up to [`RECONCILE_MAX_ATTEMPTS`]. After that, or
//! without a `reconcile` handler, it stays `unknown` until `execution_resolve`. All status changes
//! are conditional transitions, so reconcile, resolution, cancellation and crash recovery can never
//! overwrite each other's outcome.

use crate::authority::{
    audited, check_action_authority, check_connector_operation_authority,
    check_connector_operation_read_only, Principal, PrincipalKind,
};
use crate::events::EventBus;
use crate::model::{CapabilityCeiling, ConfiguredGraph, InstanceConfig, LoadedDefinition};
use crate::query_engine::QueryEngine;
use crate::telemetry::Telemetry;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use dash_luau::{
    CompiledInstanceBundle, DefinitionCategory, ExecutionContextConfig, HostBridge,
    HttpHostRequest, HttpHostResponse, InputRefSpec, RuntimeError, VmPool,
};
use dash_store::{
    ActionExecutionRecord, ActionTransition, IdempotencyClaim, InflightRecovery, SqliteStore,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Stable code marking an error whose external effect may have happened (§9.1 `unknown`).
pub const UNKNOWN_OUTCOME_CODE: &str = "UNKNOWN_OUTCOME";
/// The only accepted widget Request protocol (Appendix A.3).
pub const WIDGET_PROTOCOL: &str = "dash.widget/1";
/// Host-driven reconcile attempts (including the inline one) before an execution is left for
/// manual resolution.
pub const RECONCILE_MAX_ATTEMPTS: i64 = 30;
const RECONCILE_BASE_DELAY_SECS: i64 = 5;
const RECONCILE_MAX_DELAY_SECS: i64 = 30 * 60;
const UNSCHEDULED_UNKNOWN_SCAN_LIMIT: usize = 1000;

/// Whether a host/connector error carries the [`UNKNOWN_OUTCOME_CODE`].
pub fn is_unknown_outcome_error(msg: &str) -> bool {
    msg.contains(UNKNOWN_OUTCOME_CODE)
}

/// Delay before the next reconcile attempt after `attempts_done` attempts: exponential from 5 s,
/// capped at 30 min, plus up to 25 % random jitter.
pub fn reconcile_backoff(attempts_done: i64) -> Duration {
    let shift = (attempts_done - 1).clamp(0, 16) as u32;
    let base = RECONCILE_BASE_DELAY_SECS
        .saturating_mul(1_i64 << shift)
        .min(RECONCILE_MAX_DELAY_SECS);
    let jitter_range = (base / 4).max(1) as u128;
    let jitter = (Uuid::new_v4().as_u128() % jitter_range) as i64;
    Duration::from_secs((base + jitter) as u64)
}

fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunReport {
    pub action_instance: String,
    pub feasible: bool,
    pub advisory: bool,
    pub warnings: Vec<String>,
    pub effects: serde_json::Value,
    /// Preconditions the handler reports (e.g. "PR must be open with head sha X"), §9.1 step 2.
    #[serde(default)]
    pub preconditions: Vec<serde_json::Value>,
    /// `false` when the action declares no `dry_run` handler: nothing was evaluated, so `feasible`
    /// is `false` rather than an unfounded positive claim.
    #[serde(default = "default_true")]
    pub supported: bool,
    #[serde(default)]
    pub trace_id: Option<String>,
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

/// Result of dispatching a widget Request: either a real, durable action execution, or a refresh
/// (query invalidation) that is *not* an action execution (§9, §20 stable identity).
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WidgetRequestOutcome {
    /// Serialized as the execution record plus `"outcome": "action"`.
    Action(ActionExecutionRecord),
    Refreshed {
        status: String,
        dashboard_id: String,
        adapter_instance: String,
        widget_id: String,
        request: String,
        refreshed: Vec<String>,
        trace_id: String,
    },
}

impl WidgetRequestOutcome {
    pub fn status(&self) -> &str {
        match self {
            Self::Action(rec) => &rec.status,
            Self::Refreshed { status, .. } => status,
        }
    }

    /// Legacy record shape for callers of [`ActionManager::dispatch_widget_request`]. A refresh is
    /// not persisted as an execution; its `execution_id` is prefixed `req_` and is not inspectable.
    pub fn into_legacy_record(
        self,
        principal: &Principal,
        payload: serde_json::Value,
    ) -> ActionExecutionRecord {
        match self {
            Self::Action(rec) => rec,
            Self::Refreshed {
                status,
                adapter_instance,
                refreshed,
                trace_id,
                ..
            } => {
                let now = Utc::now().to_rfc3339();
                ActionExecutionRecord {
                    execution_id: format!("req_{}", Uuid::now_v7().simple()),
                    action_instance: adapter_instance,
                    parent_execution_id: None,
                    principal_id: principal.id.clone(),
                    principal_kind: principal.kind.as_str().into(),
                    status,
                    idempotency_key: None,
                    args: payload,
                    result: Some(serde_json::json!({ "refreshed": refreshed })),
                    reason: None,
                    retry_safe: true,
                    reconcile_attempts: 0,
                    trace_id,
                    started_at: now.clone(),
                    finished_at: Some(now),
                    duration_ms: Some(0),
                }
            }
        }
    }
}

/// Trace/composition context of a nested invocation (§5.1.5, §9.2, §15.1).
#[derive(Debug, Clone, Default)]
pub struct InvocationContext {
    pub parent_execution_id: Option<String>,
    /// Reuse this trace (children share their parent's trace).
    pub trace_id: Option<String>,
    /// Span of the caller (parent execution or widget Request) this execution's span hangs under.
    pub parent_span_id: Option<String>,
}

#[derive(Debug, Clone)]
struct RenderedWidgets {
    generation: i64,
    /// widget_id -> widget_type
    widgets: HashMap<String, String>,
}

/// How a widget Request resolved in its Dry run (`dry_run_widget_request`). Remembered per
/// Principal and Request so the real dispatch cannot diverge from what the UI showed: the UI skips
/// the confirmation for a refresh, and confirms exactly one action call otherwise (R7:SEC-1).
#[derive(Debug, Clone, PartialEq, Eq)]
enum DryRunResolution {
    /// Dispatched without confirmation: the dispatch may not run any action or mutating
    /// connector operation.
    Refresh,
    /// Confirmed as this action call; the dispatch may run exactly this action with these args.
    Action {
        action_instance: String,
        args_digest: String,
    },
}

impl DryRunResolution {
    fn describe(&self) -> String {
        match self {
            Self::Refresh => "a refresh (dispatched without confirmation)".into(),
            Self::Action {
                action_instance, ..
            } => format!("action '{action_instance}' with the confirmed arguments"),
        }
    }
}

#[derive(Debug, Clone)]
struct WidgetDryRunBinding {
    resolution: DryRunResolution,
    at: Instant,
}

/// How long a widget Request's Dry-run resolution constrains its dispatch.
const WIDGET_DRY_RUN_BINDING_TTL: Duration = Duration::from_secs(15 * 60);
/// Bound on remembered Dry-run resolutions (§20: no unbounded caches); oldest evicted first.
const MAX_WIDGET_DRY_RUN_BINDINGS: usize = 4096;

/// Built-in widget types whose host-rendered UI sends Requests; the Request name is the one the
/// Adapter configured in the widget's props (`request`, `row_action.request`) or its default.
const BUILTIN_REQUEST_WIDGETS: &[&str] = &["dash.button", "dash.form", "dash.table"];

/// Built-in widget types that never send Requests (they only render, or change Dashboard State).
const BUILTIN_SILENT_WIDGETS: &[&str] = &[
    "dash.chart",
    "dash.container",
    "dash.counter",
    "dash.filter",
    "dash.grid",
    "dash.map",
    "dash.tabs",
    "dash.text",
];

#[derive(Clone)]
pub struct ActionManager {
    graph: Arc<ArcSwap<ConfiguredGraph>>,
    pool: VmPool,
    store: SqliteStore,
    queries: QueryEngine,
    events: EventBus,
    telemetry: Telemetry,
    /// Cancellation flags of executions admitted by this process (§9.1 step 4).
    cancel_flags: Arc<DashMap<String, Arc<AtomicBool>>>,
    /// widget_ids produced by each Adapter's last render, per Dashboard (bounded by graph size).
    rendered_widgets: Arc<DashMap<String, RenderedWidgets>>,
    /// Single-flight guard for reconcile attempts in this process.
    reconcile_inflight: Arc<DashMap<String, ()>>,
    /// Dry-run resolution of recent widget Requests, keyed by [`widget_dry_run_key`].
    widget_dry_runs: Arc<DashMap<String, WidgetDryRunBinding>>,
}

/// Host-observed side effects of one handler run.
#[derive(Default)]
struct SideEffectTracker {
    writes_started: AtomicU64,
    writes_succeeded: AtomicU64,
    /// Mutating connector calls that ended with a definitive (non-`UNKNOWN_OUTCOME`) error.
    writes_rejected: AtomicU64,
    nested_started: AtomicU64,
    /// Nested actions that returned a record not proven effect-free (not `failed`/`cancelled`).
    nested_effective: AtomicU64,
    /// Side effects a read-only (Dry run / reconcile) bridge refused. A widget-Request Dry run
    /// that tried one is never reported as resolved, whatever the handler returned afterwards.
    read_only_refusals: AtomicU64,
    uncertain: Mutex<Option<String>>,
    children: Mutex<Vec<String>>,
}

impl SideEffectTracker {
    fn side_effects_started(&self) -> bool {
        self.writes_started.load(Ordering::SeqCst) > 0
            || self.nested_started.load(Ordering::SeqCst) > 0
    }

    fn uncertain(&self) -> Option<String> {
        self.uncertain.lock().expect("tracker mutex").clone()
    }

    /// A side effect may have happened: a mutating call succeeded or was still in flight when
    /// the run ended, or a nested action ran that did not end `failed`/`cancelled` (§9.1, §15.2).
    fn effect_possible(&self) -> bool {
        let started = self.writes_started.load(Ordering::SeqCst);
        let settled = self.writes_succeeded.load(Ordering::SeqCst)
            + self.writes_rejected.load(Ordering::SeqCst);
        self.writes_succeeded.load(Ordering::SeqCst) > 0
            || started > settled
            || self.nested_effective.load(Ordering::SeqCst) > 0
    }

    /// No external effect can have happened: no write completed, no nested action ran, nothing uncertain.
    fn proven_no_effect(&self) -> bool {
        self.writes_succeeded.load(Ordering::SeqCst) == 0
            && self.nested_started.load(Ordering::SeqCst) == 0
            && self.uncertain().is_none()
    }

    fn child_executions(&self) -> Vec<String> {
        self.children.lock().expect("tracker mutex").clone()
    }
}

enum RunError {
    Runtime(RuntimeError),
    Injected(String),
}

struct Classified {
    status: &'static str,
    result: Option<serde_json::Value>,
    reason: Option<String>,
    retry_safe: bool,
}

struct AdmittedExecution {
    graph: Arc<ConfiguredGraph>,
    inst: InstanceConfig,
    def: LoadedDefinition,
    principal: Principal,
    args: serde_json::Value,
    idempotency_key: Option<String>,
    execution_id: String,
    trace_id: String,
    span_id: String,
    ctx: InvocationContext,
    cancel_flag: Arc<AtomicBool>,
}

struct InflightGuard {
    map: Arc<DashMap<String, ()>>,
    key: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.map.remove(&self.key);
    }
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
            cancel_flags: Arc::new(DashMap::new()),
            rendered_widgets: Arc::new(DashMap::new()),
            reconcile_inflight: Arc::new(DashMap::new()),
            widget_dry_runs: Arc::new(DashMap::new()),
        }
    }

    /// Remember how a widget Request resolved in its Dry run (`None` forgets: an unresolved Dry
    /// run is always confirmed, so it does not constrain the dispatch).
    fn remember_widget_dry_run(&self, key: String, resolution: Option<DryRunResolution>) {
        let Some(resolution) = resolution else {
            self.widget_dry_runs.remove(&key);
            return;
        };
        if self.widget_dry_runs.len() >= MAX_WIDGET_DRY_RUN_BINDINGS {
            self.widget_dry_runs
                .retain(|_, b| b.at.elapsed() < WIDGET_DRY_RUN_BINDING_TTL);
        }
        while self.widget_dry_runs.len() >= MAX_WIDGET_DRY_RUN_BINDINGS {
            let oldest = self
                .widget_dry_runs
                .iter()
                .min_by_key(|e| e.value().at)
                .map(|e| e.key().clone());
            match oldest {
                Some(k) => {
                    self.widget_dry_runs.remove(&k);
                }
                None => break,
            }
        }
        self.widget_dry_runs.insert(
            key,
            WidgetDryRunBinding {
                resolution,
                at: Instant::now(),
            },
        );
    }

    /// The unexpired Dry-run resolution a dispatch of `key` must match, if any.
    fn widget_dry_run_constraint(&self, key: &str) -> Option<DryRunResolution> {
        let binding = self.widget_dry_runs.get(key)?.clone();
        if binding.at.elapsed() >= WIDGET_DRY_RUN_BINDING_TTL {
            self.widget_dry_runs.remove(key);
            return None;
        }
        Some(binding.resolution)
    }

    /// Refuse a dispatch whose resolution diverges from its Dry run (audited).
    fn widget_dispatch_diverged(
        &self,
        req: &WidgetRequestEnvelope,
        principal: &Principal,
        expected: &DryRunResolution,
        got: &str,
    ) -> String {
        let reason = format!(
            "widget Request '{}' from '{}' resolved to {got} at dispatch, but its Dry run resolved to {}; \
             refusing to run what the user did not confirm (run the Dry run again)",
            req.request,
            req.widget_id,
            expected.describe()
        );
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "widget_request.dispatch",
            &req.adapter_instance,
            "denied",
            serde_json::json!({
                "dashboard_id": req.dashboard_id,
                "widget_id": req.widget_id,
                "request": req.request,
                "reason": reason,
            }),
        );
        reason
    }

    #[allow(clippy::too_many_arguments)]
    fn bridge(
        &self,
        graph: &Arc<ConfiguredGraph>,
        inst: &InstanceConfig,
        def: &LoadedDefinition,
        principal: &Principal,
        trace_id: &str,
        is_read_only: bool,
        tracker: Arc<SideEffectTracker>,
    ) -> ActionHostBridge {
        ActionHostBridge {
            manager: self.clone(),
            graph: Arc::clone(graph),
            caller_inst: inst.clone(),
            caller_def: def.clone(),
            principal: principal.clone(),
            trace_id: trace_id.to_string(),
            span_id: None,
            parent_execution_id: None,
            dashboard_id: None,
            is_read_only,
            dispatch_constraint: None,
            cancel_flag: None,
            step_counter: Arc::new(AtomicI64::new(0)),
            tracker,
        }
    }

    pub fn dry_run<'a>(
        &'a self,
        action_instance: &'a str,
        args: serde_json::Value,
        principal: &'a Principal,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<DryRunReport, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let g = self.graph.load_full();
            let inst = g
                .instances
                .get(action_instance)
                .ok_or_else(|| format!("action instance '{action_instance}' not found"))?
                .clone();
            if inst.category != DefinitionCategory::ActionProvider {
                return Err(format!(
                    "instance '{action_instance}' is not an action_provider"
                ));
            }
            audited(
                &self.store,
                principal,
                "action.dry_run",
                check_action_authority(principal, &inst, &g, &self.store),
            )
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

            let trace_id = format!("dry_{}", Uuid::now_v7().simple());
            let started = Instant::now();

            let Some(ref dry_handler) = def.manifest.dry_run_handler else {
                let report = DryRunReport {
                    action_instance: action_instance.to_string(),
                    feasible: false,
                    advisory: true,
                    warnings: vec![format!(
                        "action '{action_instance}' declares no dry_run handler; feasibility was not evaluated"
                    )],
                    effects: serde_json::json!({
                        "action": action_instance,
                        "args": self.store.redactor().redact_json(&args),
                        "note": "no dry_run handler; nothing was evaluated and no side effects were performed",
                    }),
                    preconditions: vec![],
                    supported: false,
                    trace_id: Some(trace_id.clone()),
                };
                self.record_dry_run_span(
                    &trace_id,
                    action_instance,
                    principal,
                    &report,
                    "ok",
                    started,
                );
                return Ok(report);
            };

            let bridge = Arc::new(self.bridge(
                &g,
                &inst,
                &def,
                principal,
                &trace_id,
                true,
                Arc::new(SideEffectTracker::default()),
            ));
            let bundle = CompiledInstanceBundle {
                instance_id: format!("dryrun:{}", inst.id),
                manifest: def.manifest.clone(),
                runtime_source: def.runtime_source.clone(),
                config_digest: inst.config_digest(),
                resolved_libraries: g.resolved_libraries(),
            };
            let run = self
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
                        trace_id: trace_id.clone(),
                        bridge,
                    },
                    None,
                )
                .await
                .map_err(|e| self.store.redactor().redact_str(&e.to_string()));
            let val = match run {
                Ok(v) => v,
                Err(e) => {
                    self.telemetry.record_span(
                        &trace_id,
                        None,
                        &format!("action_dry_run:{action_instance}"),
                        &principal.id,
                        serde_json::json!({ "error": e }),
                        "error",
                        started.elapsed().as_millis() as i64,
                    );
                    return Err(e);
                }
            };

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
            let preconditions = match val.get("preconditions") {
                Some(serde_json::Value::Array(arr)) => arr.clone(),
                Some(serde_json::Value::Null) | None => vec![],
                Some(other) => vec![other.clone()],
            };
            let effects = val.get("effects").cloned().unwrap_or(val);
            let report = DryRunReport {
                action_instance: action_instance.to_string(),
                feasible,
                advisory: true,
                warnings,
                effects,
                preconditions,
                supported: true,
                trace_id: Some(trace_id.clone()),
            };
            self.record_dry_run_span(
                &trace_id,
                action_instance,
                principal,
                &report,
                "ok",
                started,
            );
            Ok(report)
        })
    }

    fn record_dry_run_span(
        &self,
        trace_id: &str,
        action_instance: &str,
        principal: &Principal,
        report: &DryRunReport,
        status: &str,
        started: Instant,
    ) {
        self.telemetry.record_span(
            trace_id,
            None,
            &format!("action_dry_run:{action_instance}"),
            &principal.id,
            serde_json::json!({
                "feasible": report.feasible,
                "supported": report.supported,
                "warnings": report.warnings.len(),
                "preconditions": report.preconditions.len(),
            }),
            status,
            started.elapsed().as_millis() as i64,
        );
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
        self.invoke_with_context(
            action_instance,
            args,
            principal,
            idempotency_key,
            InvocationContext {
                parent_execution_id,
                ..Default::default()
            },
        )
    }

    /// Invoke an action (§9.1). The run is detached from the caller's future once its durable
    /// record exists, so a dropped caller (e.g. a closed HTTP request) cannot orphan a `running`
    /// record; the caller still receives the final record when it awaits completion.
    pub fn invoke_with_context<'a>(
        &'a self,
        action_instance: &'a str,
        args: serde_json::Value,
        principal: &'a Principal,
        idempotency_key: Option<String>,
        ctx: InvocationContext,
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
                return Err(format!(
                    "instance '{action_instance}' is not an action_provider"
                ));
            }

            // 1. Validate authority (auditing denials, including nested ones, §9.2)
            if let Err(auth_err) = check_action_authority(principal, &inst, &g, &self.store) {
                let _ = self.store.append_audit(
                    &principal.id,
                    principal.kind.as_str(),
                    "action.invoke",
                    action_instance,
                    "denied",
                    serde_json::json!({
                        "error": auth_err.to_string(),
                        "parent_execution_id": ctx.parent_execution_id,
                        "trace_id": ctx.trace_id,
                    }),
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

            // 3. Durable `queued` record, created together with the atomic idempotency claim (§9.2)
            let execution_id = format!("act_{}", Uuid::now_v7().simple());
            let trace_id = ctx
                .trace_id
                .clone()
                .unwrap_or_else(|| format!("tr_{}", Uuid::now_v7().simple()));
            let span_id = format!("spn_{}", Uuid::now_v7().simple());
            let rec = ActionExecutionRecord {
                execution_id: execution_id.clone(),
                action_instance: action_instance.to_string(),
                parent_execution_id: ctx.parent_execution_id.clone(),
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
                started_at: Utc::now().to_rfc3339(),
                finished_at: None,
                duration_ms: None,
            };
            match self
                .store
                .claim_idempotency_and_insert(&rec)
                .map_err(|e| e.to_string())?
            {
                IdempotencyClaim::New => {}
                IdempotencyClaim::Existing(existing) => return Ok(existing),
                IdempotencyClaim::ArgsMismatch(existing) => {
                    let msg = format!(
                        "idempotency key '{}' was already used for execution '{}' of '{}' with different arguments",
                        idempotency_key.as_deref().unwrap_or_default(),
                        existing.execution_id,
                        action_instance
                    );
                    let _ = self.store.append_audit(
                        &principal.id,
                        principal.kind.as_str(),
                        "action.invoke",
                        action_instance,
                        "rejected",
                        serde_json::json!({ "error": msg, "existing_execution_id": existing.execution_id }),
                    );
                    return Err(msg);
                }
            }
            self.telemetry.actions_total.fetch_add(1, Ordering::Relaxed);

            let cancel_flag = Arc::new(AtomicBool::new(false));
            self.cancel_flags
                .insert(execution_id.clone(), Arc::clone(&cancel_flag));

            let job = AdmittedExecution {
                graph: Arc::clone(&g),
                inst,
                def,
                principal: principal.clone(),
                args,
                idempotency_key,
                execution_id: execution_id.clone(),
                trace_id,
                span_id,
                ctx,
                cancel_flag,
            };
            let this = self.clone();
            let handle = tokio::spawn(async move { this.run_admitted(job).await });
            match handle.await {
                Ok(res) => res,
                Err(join_err) => {
                    // The host task died mid-run: the outcome cannot be proven.
                    self.cancel_flags.remove(&execution_id);
                    let reason = format!("host task aborted mid-execution: {join_err}");
                    let _ = self.store.transition_action_execution(
                        &execution_id,
                        &["queued", "running"],
                        ActionTransition {
                            status: "unknown",
                            result: None,
                            reason: Some(&reason),
                            retry_safe: false,
                            reconcile_attempts: 0,
                            finished_at: Some(&Utc::now().to_rfc3339()),
                            duration_ms: None,
                        },
                    );
                    self.store
                        .get_action_execution(&execution_id)
                        .map_err(|e| e.to_string())?
                        .ok_or_else(|| reason.clone())
                }
            }
        })
    }

    async fn run_admitted(&self, job: AdmittedExecution) -> Result<ActionExecutionRecord, String> {
        let AdmittedExecution {
            graph: g,
            inst,
            def,
            principal,
            args,
            idempotency_key,
            execution_id,
            trace_id,
            span_id,
            ctx,
            cancel_flag,
        } = job;
        let started_instant = Instant::now();

        let fetch = |id: &str| -> Result<ActionExecutionRecord, String> {
            self.store
                .get_action_execution(id)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("action execution '{id}' missing"))
        };

        // queued -> running (conditional: a cancel may have won the race)
        let started = self
            .store
            .transition_action_execution(
                &execution_id,
                &["queued"],
                ActionTransition {
                    status: "running",
                    result: None,
                    reason: None,
                    retry_safe: false,
                    reconcile_attempts: 0,
                    finished_at: None,
                    duration_ms: None,
                },
            )
            .map_err(|e| e.to_string())?;
        if !started {
            self.cancel_flags.remove(&execution_id);
            return fetch(&execution_id);
        }

        let tracker = Arc::new(SideEffectTracker::default());
        let run_res: Result<serde_json::Value, RunError> = if cancel_flag.load(Ordering::SeqCst) {
            Err(RunError::Runtime(RuntimeError::Cancelled))
        } else if let Some(rule) = self.telemetry.take_injected_failure(&inst.id) {
            // Failure injection (tests / Scenarios)
            if rule.mode == "unknown" {
                Ok(serde_json::json!({ "__unknown": true, "reason": rule.message }))
            } else {
                Err(RunError::Injected(format!(
                    "injected failure ({}): {}",
                    rule.mode, rule.message
                )))
            }
        } else {
            let mut bridge = self.bridge(
                &g,
                &inst,
                &def,
                &principal,
                &trace_id,
                false,
                Arc::clone(&tracker),
            );
            bridge.parent_execution_id = Some(execution_id.clone());
            bridge.span_id = Some(span_id.clone());
            bridge.cancel_flag = Some(Arc::clone(&cancel_flag));
            let bridge = Arc::new(bridge);
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
                    Some(Arc::clone(&cancel_flag)),
                )
                .await
                .map_err(RunError::Runtime)
        };

        let classified = self.classify(&def, run_res, &tracker);
        let duration_ms = started_instant.elapsed().as_millis() as i64;
        let finished_at = Utc::now().to_rfc3339();

        let applied = self.store.transition_action_execution(
            &execution_id,
            &["running"],
            ActionTransition {
                status: classified.status,
                result: classified.result.clone(),
                reason: classified.reason.as_deref(),
                retry_safe: classified.retry_safe,
                reconcile_attempts: 0,
                finished_at: Some(&finished_at),
                duration_ms: Some(duration_ms),
            },
        );
        self.cancel_flags.remove(&execution_id);
        if !applied.map_err(|e| e.to_string())? {
            // Another actor (e.g. shutdown recovery) already moved the record; keep its outcome.
            return fetch(&execution_id);
        }

        let children = self.children_summary(&execution_id);
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "action.invoke",
            &inst.id,
            classified.status,
            serde_json::json!({
                "execution_id": execution_id,
                "idempotency_key": idempotency_key,
                "reason": classified.reason,
                "retry_safe": classified.retry_safe,
                "parent_execution_id": ctx.parent_execution_id,
                "children": children,
            }),
        );

        self.telemetry.record_span_with_id(
            &span_id,
            &trace_id,
            ctx.parent_span_id.as_deref(),
            &format!("action:{}", inst.id),
            &principal.id,
            serde_json::json!({
                "execution_id": execution_id,
                "status": classified.status,
                "parent_execution_id": ctx.parent_execution_id,
                "children": children,
            }),
            if classified.status == "succeeded" {
                "ok"
            } else {
                "error"
            },
            duration_ms,
        );

        let rec = fetch(&execution_id)?;
        match classified.status {
            "succeeded" | "failed" => self.finalize_terminal(&rec, &g, "invoke", &principal).await,
            "unknown" if def.manifest.reconcile_handler.is_some() => {
                // Immediate read-only reconcile attempt under the (original) invoking Principal;
                // further attempts are scheduled with backoff.
                return match self
                    .reconcile_with(&execution_id, Some(principal.clone()))
                    .await
                {
                    Ok(r) => Ok(r),
                    Err(_) => fetch(&execution_id),
                };
            }
            _ => {}
        }
        Ok(rec)
    }

    fn classify(
        &self,
        def: &LoadedDefinition,
        run_res: Result<serde_json::Value, RunError>,
        tracker: &SideEffectTracker,
    ) -> Classified {
        let unknown = |reason: String| Classified {
            status: "unknown",
            result: None,
            reason: Some(reason),
            retry_safe: false,
        };
        match run_res {
            Ok(mut val) => {
                if val.get("__unknown").and_then(|v| v.as_bool()) == Some(true) {
                    let reason = val
                        .get("reason")
                        .and_then(|v| v.as_str())
                        .unwrap_or("upstream_connection_lost_after_request")
                        .to_string();
                    return unknown(reason);
                }
                if let Some(ref schema) = def.manifest.result {
                    schema.normalize_empty_lists(&mut val);
                    if let Err(e) = schema.validate_value(&val) {
                        let reason = format!("invalid_result_contract: {e}");
                        // The side effect may already have happened: never claim `failed` then.
                        return if tracker.side_effects_started() {
                            unknown(reason)
                        } else {
                            Classified {
                                status: "failed",
                                result: None,
                                reason: Some(reason),
                                retry_safe: true,
                            }
                        };
                    }
                }
                Classified {
                    status: "succeeded",
                    result: Some(val),
                    reason: None,
                    retry_safe: false,
                }
            }
            Err(RunError::Injected(msg)) => Classified {
                status: "failed",
                result: None,
                reason: Some(msg),
                retry_safe: true,
            },
            Err(RunError::Runtime(RuntimeError::Cancelled)) => {
                if tracker.side_effects_started() {
                    unknown("cancelled_after_side_effect_started".into())
                } else {
                    Classified {
                        status: "cancelled",
                        result: None,
                        reason: Some("cancelled_by_request".into()),
                        retry_safe: true,
                    }
                }
            }
            Err(RunError::Runtime(RuntimeError::Timeout(d))) => {
                if tracker.side_effects_started() {
                    unknown(format!(
                        "deadline_exceeded_after_side_effect_started ({d:?})"
                    ))
                } else {
                    Classified {
                        status: "failed",
                        result: None,
                        reason: Some(format!("deadline_exceeded ({d:?})")),
                        retry_safe: true,
                    }
                }
            }
            Err(RunError::Runtime(e)) => {
                let msg = self.store.redactor().redact_str(&e.to_string());
                if let Some(u) = tracker.uncertain() {
                    unknown(u)
                } else if is_unknown_outcome_error(&msg) {
                    unknown(msg)
                } else if tracker.effect_possible() && !matches!(e, RuntimeError::QueueFull(_)) {
                    // Failed after a mutating call that succeeded (or never returned): the
                    // outcome cannot be proven, so it is `unknown`, never `failed` (§9.1, §15.2).
                    unknown(format!(
                        "{UNKNOWN_OUTCOME_CODE}: failed after a mutating external call; outcome cannot be proven: {msg}"
                    ))
                } else {
                    Classified {
                        status: "failed",
                        result: None,
                        reason: Some(msg),
                        retry_safe: tracker.proven_no_effect(),
                    }
                }
            }
        }
    }

    fn children_summary(&self, execution_id: &str) -> serde_json::Value {
        let children = self
            .store
            .list_action_step_children(execution_id)
            .unwrap_or_default();
        if children.is_empty() {
            return serde_json::Value::Null;
        }
        let mut by_status: BTreeMap<String, usize> = BTreeMap::new();
        for (_, status) in &children {
            *by_status.entry(status.clone()).or_default() += 1;
        }
        serde_json::json!({
            "total": children.len(),
            "by_status": by_status,
            "execution_ids": children.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        })
    }

    /// Post-terminal work shared by invoke, reconcile and manual resolution (§9.1 step 6).
    async fn finalize_terminal(
        &self,
        rec: &ActionExecutionRecord,
        g: &ConfiguredGraph,
        source: &str,
        actor: &Principal,
    ) {
        // §7.2: action executions emit durable completion Events automatically, for every
        // terminal outcome, including ones that became known later through `reconcile` or
        // audited manual resolution.
        let terminal = matches!(rec.status.as_str(), "succeeded" | "failed")
            || (source == "manual_resolution" && rec.status == "cancelled");
        if !terminal {
            return;
        }
        let resolution = match source {
            "reconcile" => Some("reconciled"),
            "manual_resolution" => Some("manual"),
            _ => None,
        };
        let _ = self.events.emit(
            &rec.action_instance,
            "dash.action_completed",
            "1",
            &rec.trace_id,
            true,
            serde_json::json!({
                "execution_id": rec.execution_id,
                "action_instance": rec.action_instance,
                "status": rec.status,
                "reason": rec.reason,
                "resolution": resolution,
                "resolved_by": source,
                "resolved_by_principal": actor.id,
            }),
        );
        if rec.status != "succeeded" {
            return;
        }
        if let Some(inst) = g.instances.get(&rec.action_instance) {
            if let Some(def) = g.definitions.get(&inst.definition) {
                let to_invalidate = resolve_invalidations(inst, def, g);
                if !to_invalidate.is_empty() {
                    self.queries.invalidate_instances(&to_invalidate).await;
                }
            }
        }
    }

    /// Rebuild the Principal that initiated `rec` (§9.1: reconcile runs under the original authority).
    fn principal_for_record(
        &self,
        rec: &ActionExecutionRecord,
        g: &ConfiguredGraph,
    ) -> Result<Principal, String> {
        match rec.principal_kind.as_str() {
            "administrator" => {
                let mut p = Principal::administrator();
                p.id = rec.principal_id.clone();
                Ok(p)
            }
            "service_principal" => {
                let grant = if g.is_preview {
                    g.service_principals
                        .get(&rec.principal_id)
                        .and_then(|sp| serde_json::to_value(&sp.ceiling).ok())
                } else {
                    self.store
                        .get_service_principal_grant(&rec.principal_id)
                        .map_err(|e| e.to_string())?
                };
                let grant = grant.ok_or_else(|| {
                    format!(
                        "service principal '{}' no longer has an Administrator grant",
                        rec.principal_id
                    )
                })?;
                let ceiling: CapabilityCeiling =
                    serde_json::from_value(grant).map_err(|e| e.to_string())?;
                Ok(Principal {
                    id: rec.principal_id.clone(),
                    kind: PrincipalKind::ServicePrincipal,
                    ceiling,
                })
            }
            "agent" => {
                let agent = self
                    .store
                    .list_agent_principals()
                    .map_err(|e| e.to_string())?
                    .into_iter()
                    .find(|a| a.agent_id == rec.principal_id)
                    .ok_or_else(|| format!("agent '{}' no longer exists", rec.principal_id))?;
                if agent.revoked_at.is_some() {
                    return Err(format!("agent '{}' has been revoked", rec.principal_id));
                }
                if let Some(ref exp) = agent.expires_at {
                    if let Ok(dt) = DateTime::parse_from_rfc3339(exp) {
                        if dt.with_timezone(&Utc) <= Utc::now() {
                            return Err(format!("agent '{}' token has expired", rec.principal_id));
                        }
                    }
                }
                let ceiling: CapabilityCeiling =
                    serde_json::from_value(agent.ceiling).unwrap_or_default();
                Ok(Principal {
                    id: rec.principal_id.clone(),
                    kind: PrincipalKind::Agent,
                    ceiling,
                })
            }
            other => Err(format!(
                "unknown principal kind '{other}' on execution record"
            )),
        }
    }

    /// Run an action's read-only `reconcile` handler (§9.1). Kept for compatibility: `principal`
    /// must be the execution's original Principal; reconcile never runs under another authority.
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
        if rec.principal_id != principal.id || rec.principal_kind != principal.kind.as_str() {
            return Err(format!(
                "reconcile of '{execution_id}' runs under its original Principal '{}' ({}), not '{}'",
                rec.principal_id, rec.principal_kind, principal.id
            ));
        }
        self.reconcile_with(execution_id, None).await
    }

    /// Run one reconcile attempt under the original Principal rebuilt from the execution record.
    pub async fn reconcile_execution_as_original(
        &self,
        execution_id: &str,
    ) -> Result<ActionExecutionRecord, String> {
        self.reconcile_with(execution_id, None).await
    }

    async fn reconcile_with(
        &self,
        execution_id: &str,
        original: Option<Principal>,
    ) -> Result<ActionExecutionRecord, String> {
        let fetch = || -> Result<ActionExecutionRecord, String> {
            self.store
                .get_action_execution(execution_id)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("execution '{execution_id}' not found"))
        };
        if self
            .reconcile_inflight
            .insert(execution_id.to_string(), ())
            .is_some()
        {
            return fetch();
        }
        let _guard = InflightGuard {
            map: Arc::clone(&self.reconcile_inflight),
            key: execution_id.to_string(),
        };

        let rec = fetch()?;
        if rec.status != "unknown" {
            let _ = self.store.clear_action_reconcile(execution_id);
            return Ok(rec);
        }

        let g = self.graph.load_full();
        let Some(inst) = g.instances.get(&rec.action_instance).cloned() else {
            let _ = self.store.clear_action_reconcile(execution_id);
            let _ = self.store.append_audit(
                &rec.principal_id,
                &rec.principal_kind,
                "action.reconcile",
                execution_id,
                "skipped",
                serde_json::json!({ "reason": "action instance is not in the active Generation" }),
            );
            return Ok(rec);
        };
        let Some(def) = g.definitions.get(&inst.definition).cloned() else {
            let _ = self.store.clear_action_reconcile(execution_id);
            return Ok(rec);
        };
        let Some(ref reconcile_handler) = def.manifest.reconcile_handler else {
            let _ = self.store.clear_action_reconcile(execution_id);
            return Ok(rec);
        };

        let principal = match original {
            Some(p) => p,
            None => match self.principal_for_record(&rec, &g) {
                Ok(p) => p,
                Err(e) => {
                    let _ = self.store.clear_action_reconcile(execution_id);
                    let _ = self.store.append_audit(
                        &rec.principal_id,
                        &rec.principal_kind,
                        "action.reconcile",
                        execution_id,
                        "denied",
                        serde_json::json!({ "error": e, "note": "left unknown for execution_resolve" }),
                    );
                    return Err(e);
                }
            },
        };
        if let Err(e) = check_action_authority(&principal, &inst, &g, &self.store) {
            let _ = self.store.clear_action_reconcile(execution_id);
            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "action.reconcile",
                execution_id,
                "denied",
                serde_json::json!({ "error": e.to_string() }),
            );
            return Err(e.to_string());
        }

        let attempts = rec.reconcile_attempts + 1;
        let started = Instant::now();
        let bridge = Arc::new(self.bridge(
            &g,
            &inst,
            &def,
            &principal,
            &rec.trace_id,
            true,
            Arc::new(SideEffectTracker::default()),
        ));
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
            .map_err(|e| self.store.redactor().redact_str(&e.to_string()));

        let (next_status, attempt_error): (&str, Option<String>) = match &val {
            Ok(v) => match v.get("status").and_then(|s| s.as_str()) {
                Some("succeeded") => ("succeeded", None),
                Some("failed") => ("failed", None),
                _ => ("unknown", None),
            },
            Err(e) => ("unknown", Some(e.clone())),
        };
        let terminal = next_status != "unknown";
        let now = Utc::now().to_rfc3339();
        let reason = match next_status {
            "succeeded" => Some("reconciled_succeeded".to_string()),
            "failed" => Some("reconciled_failed".to_string()),
            _ => rec.reason.clone(),
        };
        let applied = self
            .store
            .transition_action_execution(
                execution_id,
                &["unknown"],
                ActionTransition {
                    status: next_status,
                    result: if terminal {
                        val.as_ref().ok().cloned()
                    } else {
                        rec.result.clone()
                    },
                    reason: reason.as_deref(),
                    // A reconciled `failed` is verified not to have happened upstream.
                    retry_safe: next_status == "failed",
                    reconcile_attempts: attempts,
                    finished_at: if terminal {
                        Some(&now)
                    } else {
                        rec.finished_at.as_deref()
                    },
                    duration_ms: rec.duration_ms,
                },
            )
            .map_err(|e| e.to_string())?;
        if !applied {
            // Resolved or cancelled concurrently: that outcome stands.
            let _ = self.store.clear_action_reconcile(execution_id);
            return fetch();
        }

        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "action.reconcile",
            execution_id,
            next_status,
            serde_json::json!({
                "action_instance": inst.id,
                "attempt": attempts,
                "error": attempt_error,
            }),
        );
        self.telemetry.record_span(
            &rec.trace_id,
            None,
            &format!("action_reconcile:{}", inst.id),
            &principal.id,
            serde_json::json!({
                "execution_id": execution_id,
                "attempt": attempts,
                "outcome": next_status,
                "error": attempt_error,
            }),
            if attempt_error.is_some() {
                "error"
            } else {
                "ok"
            },
            started.elapsed().as_millis() as i64,
        );

        let updated = fetch()?;
        if terminal {
            let _ = self.store.clear_action_reconcile(execution_id);
            self.finalize_terminal(&updated, &g, "reconcile", &principal)
                .await;
        } else if attempts >= RECONCILE_MAX_ATTEMPTS {
            let _ = self.store.clear_action_reconcile(execution_id);
            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "action.reconcile",
                execution_id,
                "exhausted",
                serde_json::json!({
                    "attempts": attempts,
                    "note": "reconcile stays inconclusive; resolve explicitly with execution_resolve",
                }),
            );
        } else {
            let next = Utc::now()
                + chrono::Duration::from_std(reconcile_backoff(attempts))
                    .unwrap_or_else(|_| chrono::Duration::seconds(RECONCILE_MAX_DELAY_SECS));
            let _ = self
                .store
                .schedule_action_reconcile(execution_id, &ts(next));
        }
        Ok(updated)
    }

    /// Run every reconcile attempt that is due now (host retry with backoff, §9.1). Returns the
    /// number of attempts made. Call periodically from a background worker.
    pub async fn run_due_reconciles(&self, limit: usize) -> usize {
        let due = self
            .store
            .list_due_action_reconciles(&ts(Utc::now()), limit)
            .unwrap_or_default();
        let mut n = 0;
        for id in due {
            let _ = self.reconcile_execution_as_original(&id).await;
            n += 1;
        }
        n
    }

    /// Spawn a background loop calling [`ActionManager::run_due_reconciles`] every `interval`.
    pub fn spawn_reconcile_worker(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                this.run_due_reconciles(16).await;
            }
        })
    }

    /// Schedule an immediate reconcile for every `unknown` execution that has a `reconcile` handler
    /// but no pending schedule (e.g. records left by a crash or an older process).
    pub fn schedule_pending_reconciles(&self) -> usize {
        let g = self.graph.load_full();
        let now = ts(Utc::now());
        let mut n = 0;
        for rec in self
            .store
            .list_unscheduled_unknown_executions(UNSCHEDULED_UNKNOWN_SCAN_LIMIT)
            .unwrap_or_default()
        {
            let has_reconcile = g
                .instances
                .get(&rec.action_instance)
                .and_then(|i| g.definitions.get(&i.definition))
                .map(|d| d.manifest.reconcile_handler.is_some())
                .unwrap_or(false);
            if has_reconcile
                && self
                    .store
                    .schedule_action_reconcile(&rec.execution_id, &now)
                    .is_ok()
            {
                n += 1;
            }
        }
        n
    }

    fn recover_inflight(&self, reason: &str) -> InflightRecovery {
        let recovery = self
            .store
            .recover_inflight_actions(reason)
            .unwrap_or_default();
        for id in recovery
            .marked_unknown
            .iter()
            .chain(recovery.marked_not_started.iter())
        {
            if let Ok(Some(rec)) = self.store.get_action_execution(id) {
                let _ = self.store.append_audit(
                    &rec.principal_id,
                    &rec.principal_kind,
                    "action.recover",
                    id,
                    &rec.status,
                    serde_json::json!({ "action_instance": rec.action_instance, "reason": reason }),
                );
            }
        }
        self.schedule_pending_reconciles();
        recovery
    }

    /// Start-up crash recovery (§15.2): an Installation is single-host, so any `queued`/`running`
    /// record found before this process admits work was orphaned by a crash, SIGKILL or OOM.
    /// `running` records become `unknown` (and are queued for reconcile); `queued` records never
    /// started and become `failed` with `retry_safe = true`. Call once at start-up, before serving.
    pub fn recover_after_restart(&self) -> InflightRecovery {
        self.recover_inflight("process_restart_incomplete")
    }

    /// Graceful drain on shutdown: incomplete in-flight actions become `unknown` (§15.2) and are
    /// reconciled after the next start.
    pub fn mark_inflight_unknown_on_shutdown(&self) -> usize {
        let r = self.recover_inflight("graceful_shutdown_incomplete");
        r.marked_unknown.len() + r.marked_not_started.len()
    }

    /// Cancel a `queued` or `running` execution (§9.1 step 4). A `queued` execution becomes
    /// `cancelled` immediately. A `running` one is signalled: it ends `cancelled` if no side effect
    /// had started, otherwise `unknown`. Allowed for the initiating Principal, or a Principal that
    /// may resolve executions, in both cases intersected with authority over the action.
    pub async fn cancel_execution(
        &self,
        execution_id: &str,
        reason: &str,
        principal: &Principal,
    ) -> Result<ActionExecutionRecord, String> {
        let rec = self
            .store
            .get_action_execution(execution_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("execution '{execution_id}' not found"))?;
        let deny = |why: String| -> Result<ActionExecutionRecord, String> {
            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "action.cancel",
                execution_id,
                "denied",
                serde_json::json!({ "error": why }),
            );
            Err(why)
        };
        let is_initiator =
            rec.principal_id == principal.id && rec.principal_kind == principal.kind.as_str();
        if !is_initiator
            && !principal.ceiling.can_resolve_executions
            && !principal.ceiling.unrestricted
        {
            return deny(
                "only the initiating Principal or one allowed to resolve executions may cancel"
                    .into(),
            );
        }
        if let Err(e) = self.check_authority_over_execution(principal, &rec) {
            return deny(e);
        }
        let reason = if reason.trim().is_empty() {
            "cancelled_by_request"
        } else {
            reason
        };
        match rec.status.as_str() {
            "queued" => {
                if let Some(flag) = self.cancel_flags.get(execution_id) {
                    flag.store(true, Ordering::SeqCst);
                }
                let now = Utc::now().to_rfc3339();
                let applied = self
                    .store
                    .transition_action_execution(
                        execution_id,
                        &["queued"],
                        ActionTransition {
                            status: "cancelled",
                            result: None,
                            reason: Some(reason),
                            retry_safe: true,
                            reconcile_attempts: rec.reconcile_attempts,
                            finished_at: Some(&now),
                            duration_ms: Some(0),
                        },
                    )
                    .map_err(|e| e.to_string())?;
                let _ = self.store.append_audit(
                    &principal.id,
                    principal.kind.as_str(),
                    "action.cancel",
                    execution_id,
                    if applied { "cancelled" } else { "too_late" },
                    serde_json::json!({ "action_instance": rec.action_instance, "reason": reason }),
                );
            }
            "running" => {
                let Some(flag) = self.cancel_flags.get(execution_id).map(|f| Arc::clone(&f)) else {
                    return Err(format!(
                        "execution '{execution_id}' is running but not owned by this process"
                    ));
                };
                flag.store(true, Ordering::SeqCst);
                let _ = self.store.append_audit(
                    &principal.id,
                    principal.kind.as_str(),
                    "action.cancel",
                    execution_id,
                    "requested",
                    serde_json::json!({ "action_instance": rec.action_instance, "reason": reason }),
                );
            }
            other => {
                return Err(format!(
                    "execution '{execution_id}' is '{other}'; only queued or running executions can be cancelled"
                ))
            }
        }
        self.store
            .get_action_execution(execution_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "execution disappeared".into())
    }

    /// Effective authority over the action an execution belongs to (§5.1.4 intersection).
    fn check_authority_over_execution(
        &self,
        principal: &Principal,
        rec: &ActionExecutionRecord,
    ) -> Result<(), String> {
        let g = self.graph.load_full();
        match g.instances.get(&rec.action_instance) {
            Some(inst) => {
                check_action_authority(principal, inst, &g, &self.store).map_err(|e| e.to_string())
            }
            None if principal.ceiling.allows_action(&rec.action_instance) => Ok(()),
            None => Err(format!(
                "principal ceiling does not grant authority over action '{}'",
                rec.action_instance
            )),
        }
    }

    /// Explicitly resolve an `unknown` action execution with audit logging (§9.1).
    pub async fn resolve_execution(
        &self,
        execution_id: &str,
        resolved_status: &str,
        reason: &str,
        principal: &Principal,
    ) -> Result<ActionExecutionRecord, String> {
        let deny = |why: String| -> Result<ActionExecutionRecord, String> {
            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "action.resolve",
                execution_id,
                "denied",
                serde_json::json!({ "reason": why }),
            );
            Err(why)
        };
        if !principal.ceiling.can_resolve_executions && !principal.ceiling.unrestricted {
            return deny("principal lacks authority to resolve unknown executions".into());
        }
        if resolved_status != "succeeded"
            && resolved_status != "failed"
            && resolved_status != "cancelled"
        {
            return Err("resolved_status must be 'succeeded', 'failed', or 'cancelled'".into());
        }
        if reason.trim().is_empty() {
            return Err(
                "a non-empty resolution reason is required (manual resolution is audited)".into(),
            );
        }

        let rec = self
            .store
            .get_action_execution(execution_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("execution '{execution_id}' not found"))?;
        if let Err(e) = self.check_authority_over_execution(principal, &rec) {
            return deny(e);
        }
        if rec.status != "unknown" {
            return Err(format!(
                "execution '{execution_id}' is '{}', not 'unknown'",
                rec.status
            ));
        }

        let finished_at = Utc::now().to_rfc3339();
        let applied = self
            .store
            .transition_action_execution(
                execution_id,
                &["unknown"],
                ActionTransition {
                    status: resolved_status,
                    result: rec.result.clone(),
                    reason: Some(reason),
                    retry_safe: resolved_status != "succeeded",
                    reconcile_attempts: rec.reconcile_attempts,
                    finished_at: Some(&finished_at),
                    duration_ms: rec.duration_ms,
                },
            )
            .map_err(|e| e.to_string())?;
        if !applied {
            let now = self
                .store
                .get_action_execution(execution_id)
                .ok()
                .flatten()
                .map(|r| r.status)
                .unwrap_or_default();
            return Err(format!(
                "execution '{execution_id}' changed concurrently (now '{now}'); resolution not applied"
            ));
        }
        let _ = self.store.clear_action_reconcile(execution_id);

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
                    "previous_status": rec.status,
                    "previous_reason": rec.reason,
                    "reconcile_attempts": rec.reconcile_attempts,
                }),
            )
            .map_err(|e| e.to_string())?;

        let updated = self
            .store
            .get_action_execution(execution_id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "execution disappeared".to_string())?;
        let g = self.graph.load_full();
        self.finalize_terminal(&updated, &g, "manual_resolution", principal)
            .await;
        Ok(updated)
    }

    /// Record the widget_ids of a rendered Dashboard (output of `QueryEngine::render_dashboard`) so
    /// widget Requests can be checked against what is currently bound (Appendix A.3).
    pub fn record_rendered_dashboard(&self, rendered: &serde_json::Value) {
        let (Some(dashboard_id), Some(generation)) = (
            rendered.get("dashboard_id").and_then(|v| v.as_str()),
            rendered.get("generation").and_then(|v| v.as_i64()),
        ) else {
            return;
        };
        for a in rendered
            .get("adapters")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            let Some(adapter) = a.get("adapter_instance").and_then(|v| v.as_str()) else {
                continue;
            };
            let mut widgets = HashMap::new();
            if let Some(tree) = a.get("tree") {
                collect_widget_ids(tree, &mut widgets);
            }
            self.rendered_widgets.insert(
                rendered_key(dashboard_id, adapter),
                RenderedWidgets {
                    generation,
                    widgets,
                },
            );
        }
    }

    fn bound_widget_type(&self, req: &WidgetRequestEnvelope) -> Option<Option<String>> {
        let entry = self
            .rendered_widgets
            .get(&rendered_key(&req.dashboard_id, &req.adapter_instance))?;
        if entry.generation != req.generation {
            return None;
        }
        entry
            .widgets
            .get(&req.widget_id)
            .map(|t| if t.is_empty() { None } else { Some(t.clone()) })
    }

    /// Ensure `req.widget_id` is produced by the Adapter's current render; re-render once on a miss.
    async fn ensure_widget_bound(
        &self,
        req: &WidgetRequestEnvelope,
        principal: &Principal,
    ) -> Result<Option<String>, String> {
        if let Some(t) = self.bound_widget_type(req) {
            return Ok(t);
        }
        let rendered = self
            .queries
            .render_dashboard(&req.dashboard_id, principal)
            .await
            .map_err(|e| {
                format!(
                    "cannot verify that widget_id '{}' is currently bound: {e}",
                    req.widget_id
                )
            })?;
        self.record_rendered_dashboard(&rendered);
        self.bound_widget_type(req).ok_or_else(|| {
            format!(
                "widget_id '{}' is not currently bound to adapter '{}' on dashboard '{}' in generation {}",
                req.widget_id, req.adapter_instance, req.dashboard_id, req.generation
            )
        })
    }

    /// Validate and dispatch a browser Widget Request (`dash.widget/1`, Appendix A.3).
    /// Compatibility wrapper over [`ActionManager::dispatch_widget_request_outcome`].
    pub async fn dispatch_widget_request(
        &self,
        req: WidgetRequestEnvelope,
        principal: &Principal,
    ) -> Result<ActionExecutionRecord, String> {
        let payload = req.payload.clone();
        let outcome = self.dispatch_widget_request_outcome(req, principal).await?;
        Ok(outcome.into_legacy_record(principal, payload))
    }

    /// Validate and dispatch a browser Widget Request (`dash.widget/1`, Appendix A.3).
    ///
    /// The host checks protocol/kind, the active Generation, the Adapter placement, that the
    /// widget_id is produced by the Adapter's current render, that the Request is declared
    /// (deny by default) and its payload matches the declared schema. Actions and invalidations
    /// resolve only through the Adapter's own bound inputs.
    /// Host-side validation shared by dispatch and Dry run of a widget Request (Appendix A.3):
    /// protocol/kind, active Generation, Adapter placement, widget_id bound by the Adapter's
    /// current render, declared Request (deny by default) and payload schemas.
    async fn validate_widget_request_binding(
        &self,
        req: &WidgetRequestEnvelope,
        principal: &Principal,
    ) -> Result<(Arc<ConfiguredGraph>, InstanceConfig, LoadedDefinition), String> {
        if req.protocol != WIDGET_PROTOCOL {
            return Err(format!(
                "unsupported widget protocol '{}' (expected '{WIDGET_PROTOCOL}')",
                req.protocol
            ));
        }
        if req.kind != "request" {
            return Err(format!(
                "unsupported widget message kind '{}' (expected 'request')",
                req.kind
            ));
        }
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

        let adapter_inst = g
            .instances
            .get(&req.adapter_instance)
            .ok_or_else(|| format!("adapter instance '{}' missing", req.adapter_instance))?
            .clone();
        if adapter_inst.category != DefinitionCategory::Adapter {
            return Err(format!("instance '{}' is not an adapter", adapter_inst.id));
        }
        let adapter_def = g
            .definitions
            .get(&adapter_inst.definition)
            .ok_or_else(|| format!("adapter definition '{}' missing", adapter_inst.definition))?
            .clone();

        // Deny by default: only declared Requests are accepted.
        if !adapter_def
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
        if let Some(schema) = adapter_def.manifest.request_schemas.get(&req.request) {
            schema
                .validate_value(&req.payload)
                .map_err(|e| format!("invalid payload for request '{}': {e}", req.request))?;
        }

        let widget_type = self.ensure_widget_bound(req, principal).await?;
        widget_may_send_request(&g, widget_type.as_deref(), req)?;
        if let Some(wdef) = widget_type.as_ref().and_then(|t| g.definitions.get(t)) {
            if let Some(schema) = wdef.manifest.request_schemas.get(&req.request) {
                schema.validate_value(&req.payload).map_err(|e| {
                    format!(
                        "invalid payload for request '{}' of widget '{}': {e}",
                        req.request, wdef.manifest.id
                    )
                })?;
            }
        }

        Ok((g, adapter_inst, adapter_def))
    }

    pub async fn dispatch_widget_request_outcome(
        &self,
        req: WidgetRequestEnvelope,
        principal: &Principal,
    ) -> Result<WidgetRequestOutcome, String> {
        let (g, adapter_inst, adapter_def) = self
            .validate_widget_request_binding(&req, principal)
            .await?;

        let trace_id = format!("tr_{}", Uuid::now_v7().simple());
        let span_id = format!("spn_{}", Uuid::now_v7().simple());
        let started = Instant::now();
        let derived_key = widget_request_idempotency_key(&req);
        let res = self
            .dispatch_validated(
                &req,
                principal,
                &g,
                &adapter_inst,
                &adapter_def,
                &trace_id,
                &span_id,
                derived_key,
            )
            .await;
        self.telemetry.record_span_with_id(
            &span_id,
            &trace_id,
            None,
            &format!("widget_request:{}", adapter_inst.id),
            &principal.id,
            serde_json::json!({
                "dashboard_id": req.dashboard_id,
                "widget_id": req.widget_id,
                "request": req.request,
                "generation": req.generation,
                "sequence": req.sequence,
                "status": res.as_ref().map(|o| o.status().to_string()).unwrap_or_else(|e| format!("rejected: {e}")),
            }),
            if res.is_ok() { "ok" } else { "error" },
            started.elapsed().as_millis() as i64,
        );
        res
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_validated(
        &self,
        req: &WidgetRequestEnvelope,
        principal: &Principal,
        g: &Arc<ConfiguredGraph>,
        adapter_inst: &InstanceConfig,
        adapter_def: &LoadedDefinition,
        trace_id: &str,
        span_id: &str,
        derived_key: String,
    ) -> Result<WidgetRequestOutcome, String> {
        let child_ctx = || InvocationContext {
            parent_execution_id: None,
            trace_id: Some(trace_id.to_string()),
            parent_span_id: Some(span_id.to_string()),
        };

        let Some(ref on_req) = adapter_def.manifest.request_handler else {
            // No on_request: map the Request name (or the conventional `action` slot) to a bound action.
            let slot = if adapter_inst.inputs.contains_key(&req.request) {
                req.request.clone()
            } else {
                "action".to_string()
            };
            let target =
                resolve_bound_action(adapter_inst, adapter_def, g, &slot).map_err(|_| {
                    format!(
                        "adapter '{}' has no action binding for request '{}'",
                        adapter_inst.id, req.request
                    )
                })?;
            let rec = self
                .invoke_with_context(
                    &target,
                    req.payload.clone(),
                    principal,
                    Some(derived_key),
                    child_ctx(),
                )
                .await?;
            return Ok(WidgetRequestOutcome::Action(rec));
        };

        // The Adapter translates the Request into an authorized action call or a refresh (Luau).
        // When this Request was Dry-run, the dispatch must resolve the same way (R7:SEC-1).
        let constraint = self.widget_dry_run_constraint(&widget_dry_run_key(principal, req));
        let tracker = Arc::new(SideEffectTracker::default());
        let mut bridge = self.bridge(
            g,
            adapter_inst,
            adapter_def,
            principal,
            trace_id,
            false,
            Arc::clone(&tracker),
        );
        bridge.dashboard_id = Some(req.dashboard_id.clone());
        bridge.span_id = Some(span_id.to_string());
        bridge.dispatch_constraint = constraint.clone();
        let bridge = Arc::new(bridge);
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
                        "dashboard_id": req.dashboard_id,
                        "widget_id": req.widget_id,
                        "request": req.request,
                        "payload": req.payload,
                        "generation": req.generation,
                        "sequence": req.sequence,
                    }),
                    operation: None,
                    idempotency_key: None,
                    allowed_emits: vec![],
                    now_iso: Utc::now().to_rfc3339(),
                    trace_id: trace_id.to_string(),
                    bridge,
                },
                None,
            )
            .await
            .map_err(|e| self.store.redactor().redact_str(&e.to_string()))?;

        if let Some(exec_id) = val.get("execution_id").and_then(|v| v.as_str()) {
            // Only an execution this Request itself created may be reported as its result.
            if !tracker.child_executions().iter().any(|c| c == exec_id) {
                return Err(format!(
                    "adapter on_request returned execution '{exec_id}', which this Request did not create"
                ));
            }
            let rec = self
                .store
                .get_action_execution(exec_id)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("execution '{exec_id}' not found"))?;
            return Ok(WidgetRequestOutcome::Action(rec));
        }
        if let Some(action_slot) = val.get("action").and_then(|v| v.as_str()) {
            let target = resolve_bound_action(adapter_inst, adapter_def, g, action_slot)?;
            let args = val
                .get("args")
                .cloned()
                .unwrap_or_else(|| req.payload.clone());
            if let Some(expected) = &constraint {
                let got = DryRunResolution::Action {
                    action_instance: target.clone(),
                    args_digest: args_digest(&args),
                };
                if &got != expected {
                    return Err(self.widget_dispatch_diverged(
                        req,
                        principal,
                        expected,
                        &format!("action '{target}'"),
                    ));
                }
            }
            let ikey = val
                .get("idempotency_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or(derived_key);
            let rec = self
                .invoke_with_context(&target, args, principal, Some(ikey), child_ctx())
                .await?;
            return Ok(WidgetRequestOutcome::Action(rec));
        }
        if val.get("refresh").and_then(|v| v.as_bool()) == Some(true)
            || val.get("invalidate").is_some()
        {
            let mut targets: Vec<String> = Vec::new();
            if let Some(inv) = val.get("invalidate") {
                let arr = inv.as_array().ok_or_else(|| {
                    "on_request 'invalidate' must be a list of bound input slots".to_string()
                })?;
                for item in arr {
                    let slot = item.as_str().ok_or_else(|| {
                        "on_request 'invalidate' entries must be strings".to_string()
                    })?;
                    targets.push(resolve_bound_data_provider(
                        adapter_inst,
                        adapter_def,
                        g,
                        slot,
                    )?);
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
            targets.sort();
            targets.dedup();
            if !targets.is_empty() {
                self.queries.invalidate_instances(&targets).await;
            }
            return Ok(WidgetRequestOutcome::Refreshed {
                status: "succeeded".into(),
                dashboard_id: req.dashboard_id.clone(),
                adapter_instance: adapter_inst.id.clone(),
                widget_id: req.widget_id.clone(),
                request: req.request.clone(),
                refreshed: targets,
                trace_id: trace_id.to_string(),
            });
        }
        Err("adapter on_request handler did not invoke or specify an action".into())
    }
}

fn rendered_key(dashboard_id: &str, adapter: &str) -> String {
    format!("{dashboard_id}\u{1f}{adapter}")
}

fn collect_widget_ids(node: &serde_json::Value, out: &mut HashMap<String, String>) {
    let Some(obj) = node.as_object() else {
        return;
    };
    if let Some(id) = obj.get("widget_id").and_then(|v| v.as_str()) {
        let wtype = obj
            .get("widget_type")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        out.insert(id.to_string(), wtype);
    }
    if let Some(children) = obj.get("children").and_then(|v| v.as_array()) {
        for c in children {
            collect_widget_ids(c, out);
        }
    }
}

/// Resolve an Adapter input slot to a bound action Instance; raw Instance ids are never accepted.
fn resolve_bound_action(
    adapter_inst: &InstanceConfig,
    adapter_def: &LoadedDefinition,
    g: &ConfiguredGraph,
    slot: &str,
) -> Result<String, String> {
    let target = adapter_inst.inputs.get(slot).ok_or_else(|| {
        format!(
            "adapter '{}' has no bound input '{slot}'; Requests may only invoke the Adapter's bound actions",
            adapter_inst.id
        )
    })?;
    if let Some(spec) = adapter_def.manifest.inputs.get(slot) {
        if !matches!(spec, InputRefSpec::Action { .. }) {
            return Err(format!(
                "adapter input '{slot}' is not declared as an action_ref"
            ));
        }
    }
    let inst = g
        .instances
        .get(target)
        .ok_or_else(|| format!("bound action instance '{target}' not found"))?;
    if inst.category != DefinitionCategory::ActionProvider {
        return Err(format!(
            "adapter input '{slot}' is bound to {} '{target}', not an action",
            inst.category.as_str()
        ));
    }
    Ok(target.clone())
}

/// Resolve an Adapter input slot to a bound data-provider Instance (for Request-driven refresh).
fn resolve_bound_data_provider(
    adapter_inst: &InstanceConfig,
    adapter_def: &LoadedDefinition,
    g: &ConfiguredGraph,
    slot: &str,
) -> Result<String, String> {
    let target = adapter_inst.inputs.get(slot).ok_or_else(|| {
        format!(
            "adapter '{}' has no bound input '{slot}'; Requests may only refresh the Adapter's bound data providers",
            adapter_inst.id
        )
    })?;
    if let Some(spec) = adapter_def.manifest.inputs.get(slot) {
        if !matches!(spec, InputRefSpec::Data { .. }) {
            return Err(format!(
                "adapter input '{slot}' is not declared as a data_ref"
            ));
        }
    }
    let inst = g
        .instances
        .get(target)
        .ok_or_else(|| format!("bound data provider '{target}' not found"))?;
    if inst.category != DefinitionCategory::DataProvider {
        return Err(format!(
            "adapter input '{slot}' is bound to {} '{target}', not a data provider",
            inst.category.as_str()
        ));
    }
    Ok(target.clone())
}

fn canonical_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<&String, String> =
                map.iter().map(|(k, v)| (k, canonical_json(v))).collect();
            let parts: Vec<String> = sorted
                .into_iter()
                .map(|(k, v)| format!("{}:{v}", serde_json::Value::String(k.clone())))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        serde_json::Value::Array(arr) => {
            format!(
                "[{}]",
                arr.iter().map(canonical_json).collect::<Vec<_>>().join(",")
            )
        }
        other => other.to_string(),
    }
}

/// Default idempotency key for a widget Request: a retransmitted Request (same Generation,
/// widget_id, sequence, name and payload) maps to the same action execution.
fn widget_request_idempotency_key(req: &WidgetRequestEnvelope) -> String {
    let mut h = Sha256::new();
    h.update(canonical_json(&req.payload).as_bytes());
    let digest = hex::encode(&h.finalize()[..8]);
    format!(
        "widget:{}:{}:{}:{}:{}:{}:{digest}",
        req.dashboard_id,
        req.adapter_instance,
        req.widget_id,
        req.generation,
        req.sequence,
        req.request
    )
}

/// Key of a widget Request's remembered Dry-run resolution: the Principal and everything the
/// Adapter sees except the per-message `sequence` (the UI numbers Dry run and dispatch apart).
fn widget_dry_run_key(principal: &Principal, req: &WidgetRequestEnvelope) -> String {
    let mut h = Sha256::new();
    for part in [
        principal.id.as_str(),
        req.dashboard_id.as_str(),
        req.adapter_instance.as_str(),
        req.widget_id.as_str(),
        &req.generation.to_string(),
        req.request.as_str(),
        &canonical_json(&req.payload),
    ] {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part.as_bytes());
    }
    hex::encode(h.finalize())
}

/// Digest of an action call's arguments (canonical JSON), to compare Dry run and dispatch.
fn args_digest(args: &serde_json::Value) -> String {
    hex::encode(Sha256::digest(canonical_json(args).as_bytes()))
}

/// Whether the bound widget may send `req.request` (§3 "Requests it can send", §10.1). Deny by
/// default: a custom widget provider must list the Request in `accepts_requests`/`requests`; the
/// built-in widgets that send Requests are fixed host code sending the names the Adapter
/// configured; every other widget type sends none.
fn widget_may_send_request(
    g: &ConfiguredGraph,
    widget_type: Option<&str>,
    req: &WidgetRequestEnvelope,
) -> Result<(), String> {
    let Some(wt) = widget_type else {
        return Err(format!(
            "widget '{}' has no widget_type, so it cannot send Requests",
            req.widget_id
        ));
    };
    let def = g.definitions.get(wt);
    let builtin = def.is_none_or(|d| {
        d.package_name == "dash" && d.manifest.widget_asset.as_deref() == Some("builtin")
    });
    if !builtin {
        let def = def.expect("non-builtin widget has a Definition");
        if def.manifest.category != DefinitionCategory::WidgetProvider {
            return Err(format!(
                "widget '{}' has type '{wt}', which is not a widget provider",
                req.widget_id
            ));
        }
        if !def
            .manifest
            .accepts_requests
            .iter()
            .any(|r| r == &req.request)
        {
            return Err(format!(
                "widget provider '{wt}' (widget '{}') does not declare request '{}' in accepts_requests/requests",
                req.widget_id, req.request
            ));
        }
        return Ok(());
    }
    if BUILTIN_REQUEST_WIDGETS.contains(&wt) {
        return Ok(());
    }
    if BUILTIN_SILENT_WIDGETS.contains(&wt) || def.is_some() {
        return Err(format!(
            "built-in widget type '{wt}' (widget '{}') does not send Requests",
            req.widget_id
        ));
    }
    Err(format!(
        "widget '{}' has unknown widget type '{wt}'; it cannot send Requests",
        req.widget_id
    ))
}

/// Whether `operation` of connector Instance `conn` is a read (`read_only = true`, `mode = "read"`,
/// or read-only capabilities) per its Definition; undeclared operations are mutating.
fn connector_operation_is_read_only(
    g: &ConfiguredGraph,
    conn: &InstanceConfig,
    operation: &str,
) -> bool {
    g.definitions
        .get(&conn.definition)
        .and_then(|d| d.manifest.operations.get(operation))
        .map(|op| op.is_read_only())
        .unwrap_or(false)
}

/// Result of resolving a widget Request through its Adapter binding without side effects,
/// followed by the resolved action's optional Dry run (§9.1 step 2, Appendix A.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetRequestDryRun {
    pub protocol: String, // "dash.widget/1"
    pub dashboard_id: String,
    pub adapter_instance: String,
    pub widget_id: String,
    pub request: String,
    /// `action`: the Request resolves to an action Instance (see `action_instance`/`args`).
    /// `refresh`: the Request only invalidates/refreshes queries; nothing to confirm.
    /// `unresolved`: the Adapter could not be evaluated read-only (for example it invokes the
    /// action imperatively); `dry_run_error` explains why.
    pub resolution: String,
    #[serde(default)]
    pub action_instance: Option<String>,
    #[serde(default)]
    pub args: Option<serde_json::Value>,
    #[serde(default)]
    pub dry_run: Option<DryRunReport>,
    #[serde(default)]
    pub dry_run_error: Option<String>,
    /// Whether the UI should ask the Administrator to confirm before dispatching the Request.
    pub requires_confirmation: bool,
}

impl ActionManager {
    /// Resolve a widget Request through the Adapter's `on_request` (evaluated read-only: nested
    /// action invocation and mutating connector operations are refused) or its direct action
    /// binding, then run the resolved action's Dry run. Performs no side effects and creates no
    /// execution record. The Request is validated exactly like a dispatch.
    pub async fn dry_run_widget_request(
        &self,
        req: WidgetRequestEnvelope,
        principal: &Principal,
    ) -> Result<WidgetRequestDryRun, String> {
        let (g, adapter_inst, adapter_def) = self
            .validate_widget_request_binding(&req, principal)
            .await?;

        let mut out = WidgetRequestDryRun {
            protocol: WIDGET_PROTOCOL.into(),
            dashboard_id: req.dashboard_id.clone(),
            adapter_instance: req.adapter_instance.clone(),
            widget_id: req.widget_id.clone(),
            request: req.request.clone(),
            resolution: "unresolved".into(),
            action_instance: None,
            args: None,
            dry_run: None,
            dry_run_error: None,
            requires_confirmation: true,
        };

        let binding_key = widget_dry_run_key(principal, &req);
        let (target_action, args) = if let Some(ref on_req) = adapter_def.manifest.request_handler {
            // Same trace id shape as a dispatch: `on_request` must not be able to tell the two
            // apart (R7:SEC-1). Whatever it does, the dispatch is bound to this resolution.
            let trace_id = format!("tr_{}", Uuid::now_v7().simple());
            let tracker = Arc::new(SideEffectTracker::default());
            let mut bridge = self.bridge(
                &g,
                &adapter_inst,
                &adapter_def,
                principal,
                &trace_id,
                true,
                Arc::clone(&tracker),
            );
            bridge.dashboard_id = Some(req.dashboard_id.clone());
            let bridge = Arc::new(bridge);
            let bundle = CompiledInstanceBundle {
                instance_id: format!("dryrun:{}", adapter_inst.id),
                manifest: adapter_def.manifest.clone(),
                runtime_source: adapter_def.runtime_source.clone(),
                config_digest: adapter_inst.config_digest(),
                resolved_libraries: g.resolved_libraries(),
            };
            let evaluated = self
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
                            "dashboard_id": req.dashboard_id,
                            "widget_id": req.widget_id,
                            "request": req.request,
                            "payload": req.payload,
                            "generation": req.generation,
                            "sequence": req.sequence,
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
                .map_err(|e| self.store.redactor().redact_str(&e.to_string()));
            let val = match evaluated {
                Ok(v) => v,
                Err(e) => {
                    self.remember_widget_dry_run(binding_key, None);
                    out.dry_run_error = Some(format!(
                        "adapter on_request could not be evaluated without side effects: {e}"
                    ));
                    return Ok(out);
                }
            };
            if tracker.read_only_refusals.load(Ordering::SeqCst) > 0 {
                // A side effect was attempted (and refused) while evaluating read-only: whatever
                // the handler returned after that does not describe the dispatch.
                self.remember_widget_dry_run(binding_key, None);
                out.dry_run_error = Some(
                    "adapter on_request attempted a side effect (nested action or mutating connector operation) during its read-only evaluation; the Request cannot be resolved without running it"
                        .into(),
                );
                return Ok(out);
            }
            if let Some(action_slot) = val.get("action").and_then(|v| v.as_str()) {
                // Same resolution as dispatch: only the Adapter's own bound action inputs.
                let target =
                    match resolve_bound_action(&adapter_inst, &adapter_def, &g, action_slot) {
                        Ok(t) => t,
                        Err(e) => {
                            self.remember_widget_dry_run(binding_key, None);
                            out.dry_run_error = Some(e);
                            return Ok(out);
                        }
                    };
                let args = val.get("args").cloned().unwrap_or(req.payload.clone());
                self.remember_widget_dry_run(
                    binding_key,
                    Some(DryRunResolution::Action {
                        action_instance: target.clone(),
                        args_digest: args_digest(&args),
                    }),
                );
                (target, args)
            } else if val.get("refresh").and_then(|v| v.as_bool()) == Some(true)
                || val.get("invalidate").is_some()
            {
                self.remember_widget_dry_run(binding_key, Some(DryRunResolution::Refresh));
                out.resolution = "refresh".into();
                out.requires_confirmation = false;
                return Ok(out);
            } else {
                self.remember_widget_dry_run(binding_key, None);
                out.dry_run_error =
                    Some("adapter on_request handler did not specify an action".into());
                return Ok(out);
            }
        } else {
            let slot = if adapter_inst.inputs.contains_key(&req.request) {
                req.request.clone()
            } else {
                "action".to_string()
            };
            let target =
                resolve_bound_action(&adapter_inst, &adapter_def, &g, &slot).map_err(|_| {
                    format!(
                        "adapter '{}' has no action binding for request '{}'",
                        adapter_inst.id, req.request
                    )
                })?;
            (target, req.payload.clone())
        };

        out.resolution = "action".into();
        out.action_instance = Some(target_action.clone());
        out.args = Some(self.store.redactor().redact_json(&args));
        match self.dry_run(&target_action, args, principal).await {
            Ok(rep) => out.dry_run = Some(rep),
            Err(e) => out.dry_run_error = Some(e),
        }
        Ok(out)
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

/// `invalidates` entries of action Instances that match no Instance, Definition or input slot
/// (they silently invalidate nothing). Returns `(action_instance, entry)` pairs.
pub fn unresolved_invalidations(graph: &ConfiguredGraph) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (id, inst) in &graph.instances {
        if inst.category != DefinitionCategory::ActionProvider {
            continue;
        }
        let Some(def) = graph.definitions.get(&inst.definition) else {
            continue;
        };
        for target in &def.manifest.invalidates {
            let matched = graph.instances.contains_key(target)
                || graph.instances.values().any(|c| c.definition == *target)
                || inst.inputs.contains_key(target);
            if !matched {
                out.push((id.clone(), target.clone()));
            }
        }
    }
    out
}

struct ActionHostBridge {
    manager: ActionManager,
    graph: Arc<ConfiguredGraph>,
    caller_inst: InstanceConfig,
    caller_def: LoadedDefinition,
    principal: Principal,
    trace_id: String,
    /// Span of the running execution / Request; nested executions hang under it.
    span_id: Option<String>,
    parent_execution_id: Option<String>,
    /// Dashboard of the widget Request being handled (Adapters read Dashboard State only).
    dashboard_id: Option<String>,
    is_read_only: bool,
    /// Set while dispatching a widget Request whose Dry run resolved (refresh or one confirmed
    /// action call): the Adapter's evaluation may then start no nested action and no mutating
    /// connector operation itself, because its Dry run did neither (R7:SEC-1).
    dispatch_constraint: Option<DryRunResolution>,
    cancel_flag: Option<Arc<AtomicBool>>,
    step_counter: Arc<AtomicI64>,
    tracker: Arc<SideEffectTracker>,
}

impl ActionHostBridge {
    fn cancelled(&self) -> bool {
        self.cancel_flag
            .as_ref()
            .map(|f| f.load(Ordering::SeqCst))
            .unwrap_or(false)
    }

    fn is_adapter(&self) -> bool {
        self.caller_def.manifest.category == DefinitionCategory::Adapter
            || self.caller_inst.category == DefinitionCategory::Adapter
    }
}

#[async_trait]
impl HostBridge for ActionHostBridge {
    async fn query_input(
        &self,
        input_name: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let target_id = self.caller_inst.inputs.get(input_name).ok_or_else(|| {
            format!(
                "input '{input_name}' not bound on '{}'",
                self.caller_inst.id
            )
        })?;
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
        let target_id = self.caller_inst.inputs.get(input_name).ok_or_else(|| {
            format!(
                "input '{input_name}' not bound on '{}'",
                self.caller_inst.id
            )
        })?;
        let target = self
            .graph
            .instances
            .get(target_id)
            .ok_or_else(|| format!("target instance '{target_id}' not found"))?;
        if target.category != DefinitionCategory::Connector {
            return Err(format!(
                "call_input expects a connector; input '{input_name}' is bound to {} '{target_id}' (nested actions use :invoke)",
                target.category.as_str()
            ));
        }
        let read_only_op = connector_operation_is_read_only(&self.graph, target, operation);
        if self.is_read_only {
            // Dry run / reconcile may only run operations the connector declares side-effect
            // free (§9, §20); denials are audited.
            audited(
                &self.manager.store,
                &self.principal,
                "connector.invoke",
                check_connector_operation_read_only(&self.principal, target, operation, &self.graph),
            )
            .map_err(|e| {
                self.tracker
                    .read_only_refusals
                    .fetch_add(1, Ordering::SeqCst);
                format!(
                    "dry_run/reconcile is read-only; connector operation '{operation}' on '{target_id}' is not declared read_only: {e}"
                )
            })?;
        }
        if let (Some(expected), false) = (&self.dispatch_constraint, read_only_op) {
            return Err(format!(
                "connector operation '{operation}' on '{target_id}' was not part of this widget Request's Dry run, which resolved to {}; run the Dry run again",
                expected.describe()
            ));
        }
        if self.cancelled() {
            return Err(format!(
                "execution cancelled before connector operation '{operation}' started"
            ));
        }
        // Authority is checked here (again inside invoke_connector) so a denial is provably pre-send.
        audited(
            &self.manager.store,
            &self.principal,
            "connector.invoke",
            check_connector_operation_authority(&self.principal, target, operation, &self.graph),
        )
        .map_err(|e| e.to_string())?;
        if !read_only_op {
            self.tracker.writes_started.fetch_add(1, Ordering::SeqCst);
        }
        let res = self
            .manager
            .queries
            .invoke_connector(target_id, operation, args, &self.principal, &self.trace_id)
            .await;
        if !read_only_op {
            match &res {
                Ok(_) => {
                    self.tracker.writes_succeeded.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) if is_unknown_outcome_error(e) => {
                    let mut guard = self.tracker.uncertain.lock().expect("tracker mutex");
                    if guard.is_none() {
                        *guard = Some(format!(
                            "upstream_outcome_unknown: connector '{target_id}' operation '{operation}': {e}"
                        ));
                    }
                }
                Err(_) => {
                    self.tracker.writes_rejected.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        res
    }

    async fn invoke_action(
        &self,
        input_name: &str,
        args: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<serde_json::Value, String> {
        if self.is_read_only {
            self.tracker
                .read_only_refusals
                .fetch_add(1, Ordering::SeqCst);
            return Err("nested action invocation is forbidden during dry_run/reconcile".into());
        }
        if let Some(expected) = &self.dispatch_constraint {
            return Err(format!(
                "nested action invocation through '{input_name}' was not part of this widget Request's Dry run, which resolved to {}; run the Dry run again",
                expected.describe()
            ));
        }
        let target_id = self.caller_inst.inputs.get(input_name).ok_or_else(|| {
            format!(
                "action input '{input_name}' not bound on '{}'",
                self.caller_inst.id
            )
        })?;
        let target = self
            .graph
            .instances
            .get(target_id)
            .ok_or_else(|| format!("target instance '{target_id}' not found"))?;
        if target.category != DefinitionCategory::ActionProvider {
            return Err(format!(
                "invoke expects an action; input '{input_name}' is bound to {} '{target_id}'",
                target.category.as_str()
            ));
        }
        if self.cancelled() {
            return Err("execution cancelled before nested action started".into());
        }
        self.tracker.nested_started.fetch_add(1, Ordering::SeqCst);
        let child_rec = self
            .manager
            .invoke_with_context(
                target_id,
                args,
                &self.principal,
                idempotency_key,
                InvocationContext {
                    parent_execution_id: self.parent_execution_id.clone(),
                    trace_id: Some(self.trace_id.clone()),
                    parent_span_id: self.span_id.clone(),
                },
            )
            .await?;
        if child_rec.status != "failed" && child_rec.status != "cancelled" {
            self.tracker.nested_effective.fetch_add(1, Ordering::SeqCst);
        }
        self.tracker
            .children
            .lock()
            .expect("tracker mutex")
            .push(child_rec.execution_id.clone());
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
        if self.is_adapter() {
            return Err("adapters never emit Events; send a Request instead (§7.2)".into());
        }
        if self.is_read_only {
            return Ok(());
        }
        // Schema version and durability come from the executing Definition's declaration.
        self.manager
            .events
            .emit_declared(
                &self.caller_inst.id,
                &self.caller_def.manifest.emits,
                event_type,
                &self.trace_id,
                durable,
                payload,
            )
            .map(|_| ())
    }

    async fn emit_event_versioned(
        &self,
        event_type: &str,
        schema_version: &str,
        payload: serde_json::Value,
        durable: bool,
    ) -> Result<(), String> {
        if self.is_adapter() {
            return Err("adapters never emit Events; send a Request instead (§7.2)".into());
        }
        if self.is_read_only {
            return Err("dry_run/reconcile is read-only and cannot emit Events".into());
        }
        self.manager
            .events
            .emit(
                &self.caller_inst.id,
                event_type,
                schema_version,
                &self.trace_id,
                durable,
                payload,
            )
            .map(|_| ())
    }

    async fn state_get(&self, scope: &str, key: &str) -> Result<serde_json::Value, String> {
        let rec = if self.is_adapter() {
            // ADR-0001: Adapters hold no State of their own; they read declared Dashboard State only.
            if scope != "dashboard" {
                return Err(
                    "adapters hold no state of their own (ADR-0001); use ctx.dashboard_state"
                        .into(),
                );
            }
            let dashboard_id = self
                .dashboard_id
                .as_deref()
                .ok_or_else(|| "no Dashboard in context for dashboard_state".to_string())?;
            if !self
                .caller_def
                .manifest
                .dashboard_state_reads
                .iter()
                .any(|k| k == key)
            {
                return Err(format!(
                    "adapter '{}' does not declare dashboard_state_reads entry '{key}'",
                    self.caller_inst.id
                ));
            }
            self.manager
                .store
                .get_state("dashboard", dashboard_id, key)
                .map_err(|e| e.to_string())?
        } else {
            if scope != "instance" {
                return Err(format!(
                    "scope '{scope}' state is not readable from {}",
                    self.caller_def.manifest.category.as_str()
                ));
            }
            self.manager
                .store
                .get_state("instance", &self.caller_inst.id, key)
                .map_err(|e| e.to_string())?
        };
        Ok(rec.map(|r| r.value).unwrap_or(serde_json::Value::Null))
    }

    async fn state_set(
        &self,
        scope: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), String> {
        if self.is_adapter() {
            return Err("adapters hold no writable state (ADR-0001)".into());
        }
        if scope != "instance" {
            return Err(format!(
                "scope '{scope}' state is not writable from actions"
            ));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DashboardAdapterPlacement, DashboardDoc};
    use crate::RuntimeEngine;
    use dash_luau::{evaluate_declaration, ResourceBudget};
    use serde_json::json;
    use std::collections::BTreeSet;

    fn load_def(pkg: &str, decl: &str, runtime: &str) -> LoadedDefinition {
        let mut sources = BTreeMap::new();
        sources.insert("definition.luau".into(), decl.as_bytes().to_vec());
        sources.insert("runtime.luau".into(), runtime.as_bytes().to_vec());
        let manifest = evaluate_declaration(decl, &sources).unwrap();
        LoadedDefinition {
            package_name: pkg.into(),
            manifest,
            definition_source: decl.into(),
            runtime_source: runtime.into(),
            tests_source: None,
            fixtures: BTreeMap::new(),
            widget_assets: BTreeMap::new(),
        }
    }

    fn instance(
        id: &str,
        category: DefinitionCategory,
        definition: &str,
        inputs: &[(&str, &str)],
    ) -> InstanceConfig {
        InstanceConfig {
            id: id.into(),
            category,
            definition: definition.into(),
            inputs: inputs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            config: json!({}),
            secrets: BTreeMap::new(),
            capabilities: vec![],
            run_as: None,
            enabled: None,
        }
    }

    const CONN_DECL: &str = r#"
local dash = require("dash")
return dash.connector {
    id = "t.conn",
    operations = {
        get_item = { read_only = true },
        write_item = {},
        approve = {},
        reject_write = {},
        lost_write = {},
    },
}
"#;
    const CONN_RT: &str = r#"
local done = {}
local writes = 0
local R = {}
function R.call(ctx)
    local op = ctx.operation
    if op == "get_item" then
        return { id = ctx.args.id, done = done[ctx.args.id] == true, writes = writes }
    elseif op == "write_item" then
        writes = writes + 1
        done[ctx.args.id] = true
        return { ok = true, writes = writes }
    elseif op == "approve" then
        writes = writes + 1
        return { approved = true }
    elseif op == "reject_write" then
        error("upstream rejected the write with HTTP 422")
    elseif op == "lost_write" then
        writes = writes + 1
        done[ctx.args.id] = true
        error("UNKNOWN_OUTCOME: upstream returned HTTP 502 after the write")
    end
    error("unknown op " .. tostring(op))
end
return R
"#;
    const WRITE_DECL: &str = r#"
local dash = require("dash")
return dash.action_provider {
    id = "t.write",
    handler = "execute",
    dry_run = "dry_run",
    reconcile = "reconcile",
    inputs = { c = dash.connector_ref("t.conn") },
    args = dash.object {
        id = dash.string(),
        op = dash.optional(dash.string()),
        dry_op = dash.optional(dash.string()),
        reconcile_op = dash.optional(dash.string()),
    },
    result = dash.object { ok = dash.boolean() },
    invalidates = { "t.data" },
}
"#;
    const WRITE_RT: &str = r#"
local dash = require("dash")
local runs = 0
local R = {}
function R.execute(ctx)
    runs = runs + 1
    local op = ctx.args.op or "write_item"
    if op == "unknown_sentinel" then
        return dash.unknown_outcome("explicit_unknown")
    elseif op == "bad_result_after_write" then
        ctx:await(ctx.inputs.c:call("write_item", { id = ctx.args.id }))
        return { wrong = true, runs = runs }
    elseif op == "bad_result_no_write" then
        return { wrong = true }
    elseif op == "spin" then
        local n = 0
        for i = 1, 4000 do
            n = n + #string.rep("x", 400000)
        end
        return { ok = true, n = n }
    elseif op == "spin_forever" then
        local n = 0
        while true do
            n = n + #string.rep("x", 100000)
        end
    elseif op == "plain_error" then
        error("handler validation failed before any write")
    end
    local res = ctx:await(ctx.inputs.c:call(op, { id = ctx.args.id }))
    return { ok = true, runs = runs, writes = res.writes }
end
function R.dry_run(ctx)
    if ctx.args.dry_op then
        ctx:await(ctx.inputs.c:call(ctx.args.dry_op, { id = ctx.args.id }))
    end
    local it = ctx:await(ctx.inputs.c:call("get_item", { id = ctx.args.id }))
    return {
        feasible = not it.done,
        warnings = {},
        preconditions = { { check = "not_done", id = ctx.args.id } },
        effects = { will_write = ctx.args.id },
    }
end
function R.reconcile(ctx)
    local it = ctx:await(ctx.inputs.c:call(ctx.args.reconcile_op or "get_item", { id = ctx.args.id }))
    if it.done then
        return { status = "succeeded", ok = true }
    end
    return { status = "unknown" }
end
return R
"#;
    const NESTED_DECL: &str = r#"
local dash = require("dash")
return dash.action_provider {
    id = "t.nested",
    handler = "execute",
    dry_run = "dry_run",
    inputs = { child = dash.action_ref("t.write") },
}
"#;
    const NESTED_RT: &str = r#"
local R = {}
function R.execute(ctx)
    if ctx.args.mode == "call_action" then
        return ctx:await(ctx.inputs.child:call("execute", { id = "x" }))
    end
    return ctx:await(ctx.inputs.child:invoke({ id = ctx.args.id }))
end
function R.dry_run(ctx)
    return ctx:await(ctx.inputs.child:call("execute", { id = "x" }))
end
return R
"#;
    const NODRY_DECL: &str = r#"
local dash = require("dash")
return dash.action_provider { id = "t.nodry", handler = "execute" }
"#;
    const NODRY_RT: &str = r#"
local R = {}
function R.execute(ctx) return { ok = true } end
return R
"#;
    const BADGE_DECL: &str = r#"
local dash = require("dash")
return dash.widget_provider {
    id = "t.badge",
    asset = "assets/index.html",
    props = dash.any(),
    accepts_requests = { "go" },
    requests = { refresh = dash.any() },
}
"#;
    const MUTE_DECL: &str = r#"
local dash = require("dash")
return dash.widget_provider { id = "t.mute", asset = "assets/index.html", props = dash.any() }
"#;
    const DATA_DECL: &str = r#"
local dash = require("dash")
return dash.data_provider { id = "t.data", handler = "query" }
"#;
    const DATA_RT: &str = r#"
local R = {}
function R.query(ctx) return { 1, 2, 3 } end
return R
"#;
    const ADAPTER_DECL: &str = r#"
local dash = require("dash")
return dash.adapter {
    id = "t.adapter",
    handler = "render",
    on_request = "on_request",
    inputs = {
        act = dash.action_ref("t.write"),
        data = dash.data_ref("t.data"),
    },
    dashboard_state_reads = { "sel" },
    requests = {
        go = dash.object { id = dash.string() },
        raw = dash.any(),
        refresh = dash.any(),
        inv_raw = dash.any(),
        exec_steal = dash.any(),
        state = dash.any(),
        undeclared_state = dash.any(),
        probe = dash.any(),
        flip = dash.any(),
        flip_args = dash.any(),
        probe_ro = dash.any(),
        probe_write = dash.any(),
    },
}
"#;
    const ADAPTER_RT: &str = r#"
local R = {}
function R.render(ctx)
    return {
        key = "root",
        widget_type = "dash.container",
        children = {
            { key = "btn", widget_type = "dash.button", props = {} },
            { key = "badge", widget_type = "t.badge", props = {} },
            { key = "mute", widget_type = "t.mute", props = {} },
            { key = "label", widget_type = "dash.text", props = {} },
            { key = "stray", widget_type = "t.not_a_widget", props = {} },
        },
    }
end
function R.on_request(ctx)
    local r = ctx.request.request
    local p = ctx.request.payload or {}
    if r == "go" then
        return { action = "act", args = { id = p.id } }
    elseif r == "raw" then
        return { action = "other_write", args = { id = "raw" } }
    elseif r == "refresh" then
        return { refresh = true, invalidate = { "data" } }
    elseif r == "inv_raw" then
        return { invalidate = { "data_inst" } }
    elseif r == "exec_steal" then
        return { execution_id = p.execution_id }
    elseif r == "state" then
        local sel = ctx:await(ctx.dashboard_state:get("sel"))
        return { action = "act", args = { id = sel, op = "get_item" } }
    elseif r == "undeclared_state" then
        local v = ctx:await(ctx.dashboard_state:get("secret_key"))
        return { action = "act", args = { id = tostring(v) } }
    elseif r == "probe" then
        -- The R7:SEC-1 exploit: look harmless in the Dry run, act in the dispatch.
        if string.sub(ctx.trace_id, 1, 7) == "reqdry_" then
            return { refresh = true }
        end
        return { action = "act", args = { id = "probe" } }
    elseif r == "flip" then
        if ctx:await(ctx.dashboard_state:get("sel")) == "safe" then
            return { refresh = true }
        end
        return { action = "act", args = { id = "flip" } }
    elseif r == "flip_args" then
        return { action = "act", args = { id = tostring(ctx:await(ctx.dashboard_state:get("sel"))) } }
    elseif r == "probe_ro" then
        -- Probe for the read-only Dry run bridge, then pretend to be a refresh.
        local ok = pcall(function()
            return ctx:await(ctx.inputs.act:invoke({ id = "probe_ro" }))
        end)
        return { refresh = true, probed = ok }
    elseif r == "probe_write" then
        if ctx:await(ctx.dashboard_state:get("sel")) == "safe" then
            return { refresh = true }
        end
        return ctx:await(ctx.inputs.act:invoke({ id = "probe_write" }))
    end
    error("unsupported")
end
return R
"#;

    fn build_graph() -> ConfiguredGraph {
        let mut g = ConfiguredGraph {
            generation_id: 7,
            ..Default::default()
        };
        for (pkg, decl, rt) in [
            ("t", CONN_DECL, CONN_RT),
            ("t", WRITE_DECL, WRITE_RT),
            ("t", NESTED_DECL, NESTED_RT),
            ("t", NODRY_DECL, NODRY_RT),
            ("t", DATA_DECL, DATA_RT),
            ("t", ADAPTER_DECL, ADAPTER_RT),
            ("t", BADGE_DECL, ""),
            ("t", MUTE_DECL, ""),
        ] {
            let d = load_def(pkg, decl, rt);
            g.definitions.insert(d.manifest.id.clone(), d);
        }
        for inst in [
            instance("conn", DefinitionCategory::Connector, "t.conn", &[]),
            instance(
                "write",
                DefinitionCategory::ActionProvider,
                "t.write",
                &[("c", "conn")],
            ),
            instance(
                "other_write",
                DefinitionCategory::ActionProvider,
                "t.write",
                &[("c", "conn")],
            ),
            instance(
                "nested",
                DefinitionCategory::ActionProvider,
                "t.nested",
                &[("child", "write")],
            ),
            instance("nodry", DefinitionCategory::ActionProvider, "t.nodry", &[]),
            instance("data_inst", DefinitionCategory::DataProvider, "t.data", &[]),
            instance(
                "adapter",
                DefinitionCategory::Adapter,
                "t.adapter",
                &[("act", "write"), ("data", "data_inst")],
            ),
        ] {
            g.instances.insert(inst.id.clone(), inst);
        }
        g.dashboards.insert(
            "main".into(),
            DashboardDoc {
                id: "main".into(),
                title: "Main".into(),
                layout: "grid".into(),
                adapters: vec![DashboardAdapterPlacement {
                    adapter_instance: "adapter".into(),
                    area: "a".into(),
                    query_params: json!({}),
                }],
                initial_state: BTreeMap::new(),
                overrides: BTreeMap::new(),
                state_schema: BTreeMap::new(),
            },
        );
        g
    }

    fn engine() -> RuntimeEngine {
        let budget = ResourceBudget {
            wall_timeout: Duration::from_secs(20),
            ..ResourceBudget::default()
        };
        RuntimeEngine::new(
            build_graph(),
            SqliteStore::open_in_memory().unwrap(),
            budget,
        )
        .unwrap()
    }

    fn req(
        request: &str,
        widget_id: &str,
        sequence: u64,
        payload: serde_json::Value,
    ) -> WidgetRequestEnvelope {
        WidgetRequestEnvelope {
            protocol: WIDGET_PROTOCOL.into(),
            dashboard_id: "main".into(),
            adapter_instance: "adapter".into(),
            widget_id: widget_id.into(),
            generation: 7,
            sequence,
            kind: "request".into(),
            request: request.into(),
            payload,
        }
    }

    fn audits(e: &RuntimeEngine, op: &str) -> Vec<dash_store::AuditRecord> {
        e.store
            .list_audits(500)
            .unwrap()
            .into_iter()
            .filter(|a| a.operation == op)
            .collect()
    }

    async fn connector_writes(e: &RuntimeEngine) -> i64 {
        let admin = Principal::administrator();
        e.queries
            .invoke_connector(
                "conn",
                "get_item",
                json!({"id": "probe"}),
                &admin,
                "tr_probe",
            )
            .await
            .unwrap()["writes"]
            .as_i64()
            .unwrap()
    }

    // act-1: an action-bound input can never be driven through `:call` as if it were a connector.
    #[tokio::test]
    async fn call_on_action_bound_input_is_rejected_in_execute_and_dry_run() {
        let e = engine();
        let admin = Principal::administrator();
        let rec = e
            .actions
            .invoke("nested", json!({"mode": "call_action"}), &admin, None, None)
            .await
            .unwrap();
        assert_eq!(rec.status, "failed");
        assert!(rec
            .reason
            .unwrap()
            .contains("call_input expects a connector"));
        let dry = e
            .actions
            .dry_run("nested", json!({}), &admin)
            .await
            .unwrap_err();
        assert!(dry.contains("call_input expects a connector"), "{dry}");
        // The child action never ran: no execution of `write` exists and no event was emitted.
        let execs = e.store.list_action_executions(50).unwrap();
        assert!(execs.iter().all(|r| r.action_instance != "write"));
        assert!(e.events.replay_since("write", 0).unwrap().is_empty());
        // And the QueryEngine itself refuses to run a non-connector Instance as a connector.
        let direct = e
            .queries
            .invoke_connector("write", "execute", json!({"id": "x"}), &admin, "tr")
            .await
            .unwrap_err();
        assert!(direct.contains("not a connector"), "{direct}");
    }

    // act-2: Dry run and reconcile may only run operations declared read_only.
    #[tokio::test]
    async fn dry_run_and_reconcile_cannot_run_undeclared_or_mutating_operations() {
        let e = engine();
        let admin = Principal::administrator();
        let before = connector_writes(&e).await;
        let err = e
            .actions
            .dry_run("write", json!({"id": "a", "dry_op": "approve"}), &admin)
            .await
            .unwrap_err();
        assert!(err.contains("not declared read_only"), "{err}");
        assert_eq!(
            connector_writes(&e).await,
            before,
            "dry run performed no write"
        );

        // reconcile with a mutating op stays unknown and performs no write
        let rec = e
            .actions
            .invoke(
                "write",
                json!({"id": "r", "op": "unknown_sentinel", "reconcile_op": "approve"}),
                &admin,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(rec.status, "unknown");
        assert_eq!(
            connector_writes(&e).await,
            before,
            "reconcile performed no write"
        );
        let rec_audit = audits(&e, "action.reconcile");
        assert!(rec_audit.iter().any(|a| a.detail["error"]
            .as_str()
            .unwrap_or("")
            .contains("not declared read_only")));

        // A read_only operation is allowed and reports preconditions (act-17)
        let ok = e
            .actions
            .dry_run("write", json!({"id": "fresh"}), &admin)
            .await
            .unwrap();
        assert!(ok.supported && ok.feasible);
        assert_eq!(ok.preconditions.len(), 1);
        let tid = ok.trace_id.clone().unwrap();
        assert!(e
            .telemetry
            .recent_spans(200)
            .iter()
            .any(|s| s.trace_id == tid && s.name == "action_dry_run:write"));
    }

    // act-m1: no dry_run handler -> no unfounded feasibility claim.
    #[tokio::test]
    async fn dry_run_without_handler_is_reported_as_unsupported() {
        let e = engine();
        let rep = e
            .actions
            .dry_run("nodry", json!({}), &Principal::administrator())
            .await
            .unwrap();
        assert!(!rep.supported);
        assert!(!rep.feasible);
        assert!(rep.warnings[0].contains("declares no dry_run handler"));
    }

    // act-3: unknown vs failed classification is structural, not substring-based on arbitrary text.
    #[tokio::test]
    async fn unknown_versus_failed_classification() {
        let e = engine();
        let admin = Principal::administrator();

        let lost = e
            .actions
            .invoke(
                "write",
                json!({"id": "l1", "op": "lost_write"}),
                &admin,
                None,
                None,
            )
            .await
            .unwrap();
        // Classified unknown, then the inline reconcile found the write landed.
        assert_eq!(lost.status, "succeeded");
        assert!(lost.reconcile_attempts >= 1);
        assert_eq!(lost.reason.as_deref(), Some("reconciled_succeeded"));
        let first = audits(&e, "action.invoke")
            .into_iter()
            .find(|a| a.detail["execution_id"] == lost.execution_id.as_str())
            .unwrap();
        assert_eq!(first.outcome, "unknown");
        assert!(first.detail["reason"]
            .as_str()
            .unwrap()
            .contains("upstream_outcome_unknown"));

        let rejected = e
            .actions
            .invoke(
                "write",
                json!({"id": "r1", "op": "reject_write"}),
                &admin,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(rejected.status, "failed");
        assert!(
            rejected.retry_safe,
            "a definitive rejection had no external effect"
        );

        let explicit = e
            .actions
            .invoke(
                "other_write",
                json!({"id": "never", "op": "unknown_sentinel"}),
                &admin,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(explicit.status, "unknown");
        assert_eq!(explicit.reason.as_deref(), Some("explicit_unknown"));
        assert!(!explicit.retry_safe);

        let plain = e
            .actions
            .invoke(
                "write",
                json!({"id": "p", "op": "plain_error"}),
                &admin,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(plain.status, "failed");
        assert!(plain.retry_safe);
    }

    // act-3: the HTTP broker tells "never sent" from "may have reached the upstream".
    #[tokio::test]
    async fn http_broker_marks_possibly_sent_requests_as_unknown_outcome() {
        use crate::http_broker::{HttpBroker, HttpBrokerError};
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                drop(sock); // request received, connection closed without a response
            }
        });
        let refused = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };

        let store = SqliteStore::open_in_memory().unwrap();
        // Outbound HTTP is deny-by-default: grant an Administrator ceiling covering the test.
        store
            .set_network_policy_ceiling(
                json!({
                    "allowed_hosts": ["127.0.0.1"],
                    "allowed_methods": ["GET", "PUT"],
                    "allow_loopback_for_tests": true
                }),
                "admin",
            )
            .unwrap();
        let broker = HttpBroker::new(store).unwrap();
        let mut graph = ConfiguredGraph::default();
        graph.workspace.network.allowed_hosts = vec!["127.0.0.1".into()];
        graph.workspace.network.allowed_methods = vec!["GET".into(), "PUT".into()];
        graph.workspace.network.allow_loopback_for_tests = true;
        let conn = instance("conn", DefinitionCategory::Connector, "t.conn", &[]);
        let admin = Principal::administrator();
        let mk = |url: String| HttpHostRequest {
            method: "PUT".into(),
            url,
            headers: BTreeMap::new(),
            body: Some(json!({"merge": true})),
            credential: None,
        };

        let lost = broker
            .execute(
                &admin,
                &conn,
                None,
                &graph,
                mk(format!("http://{addr}/merge")),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(lost, HttpBrokerError::OutcomeUnknown(_)),
            "{lost:?}"
        );
        assert!(is_unknown_outcome_error(&lost.to_string()));

        // A safe method has no side effect: a lost response is a plain transport failure.
        let mut get = mk(format!("http://{addr}/pulls"));
        get.method = "GET".into();
        get.body = None;
        let read_err = broker
            .execute(&admin, &conn, None, &graph, get)
            .await
            .unwrap_err();
        assert!(
            matches!(read_err, HttpBrokerError::Transport(_)),
            "{read_err:?}"
        );

        let not_sent = broker
            .execute(
                &admin,
                &conn,
                None,
                &graph,
                mk(format!("http://{refused}/merge")),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(not_sent, HttpBrokerError::Transport(_)),
            "{not_sent:?}"
        );
        assert!(!is_unknown_outcome_error(&not_sent.to_string()));
    }

    // act-4 / act-14: reconcile is retried by the host with backoff and finalizes like invoke.
    #[tokio::test]
    async fn reconcile_is_scheduled_with_backoff_and_finalizes_on_success() {
        let e = engine();
        let admin = Principal::administrator();
        let rec = e
            .actions
            .invoke(
                "write",
                json!({"id": "b1", "op": "unknown_sentinel"}),
                &admin,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(rec.status, "unknown");
        assert_eq!(rec.reconcile_attempts, 1, "one inline attempt");
        let next = e
            .store
            .get_action_reconcile_schedule(&rec.execution_id)
            .unwrap()
            .expect("next attempt scheduled");
        assert!(
            next > ts(Utc::now()),
            "next attempt is in the future (backoff)"
        );
        assert_eq!(e.actions.run_due_reconciles(10).await, 0, "nothing due yet");

        // The external effect lands later (e.g. a slow upstream); make the attempt due.
        e.queries
            .invoke_connector("conn", "write_item", json!({"id": "b1"}), &admin, "tr_ext")
            .await
            .unwrap();
        e.store
            .schedule_action_reconcile(
                &rec.execution_id,
                &ts(Utc::now() - chrono::Duration::seconds(1)),
            )
            .unwrap();
        assert_eq!(e.actions.run_due_reconciles(10).await, 1);
        let done = e
            .store
            .get_action_execution(&rec.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(done.status, "succeeded");
        assert_eq!(done.reconcile_attempts, 2);
        assert!(e
            .store
            .get_action_reconcile_schedule(&rec.execution_id)
            .unwrap()
            .is_none());

        let completed: Vec<_> = e
            .events
            .replay_since("write", 0)
            .unwrap()
            .into_iter()
            .filter(|ev| ev.semantic_type == "dash.action_completed")
            .collect();
        assert!(completed
            .iter()
            .any(|ev| ev.payload["execution_id"] == rec.execution_id.as_str()
                && ev.payload["resolved_by"] == "reconcile"));
        let rec_audits = audits(&e, "action.reconcile");
        assert!(
            rec_audits
                .iter()
                .filter(|a| a.target == rec.execution_id)
                .count()
                >= 2
        );
        assert!(e
            .telemetry
            .recent_spans(500)
            .iter()
            .any(|s| s.name == "action_reconcile:write" && s.trace_id == rec.trace_id));

        // Backoff grows and is capped.
        assert!(reconcile_backoff(1) <= reconcile_backoff(4));
        assert!(
            reconcile_backoff(1000)
                <= Duration::from_secs((RECONCILE_MAX_DELAY_SECS * 5 / 4 + 1) as u64)
        );
    }

    // act-4: reconcile never runs under a Principal other than the original one.
    #[tokio::test]
    async fn reconcile_runs_only_under_the_original_principal() {
        let e = engine();
        let admin = Principal::administrator();
        let rec = e
            .actions
            .invoke(
                "write",
                json!({"id": "o1", "op": "unknown_sentinel"}),
                &admin,
                None,
                None,
            )
            .await
            .unwrap();
        let agent = Principal {
            id: "agt_x".into(),
            kind: PrincipalKind::Agent,
            ceiling: CapabilityCeiling::administrator(),
        };
        let err = e
            .actions
            .reconcile_execution(&rec.execution_id, &agent)
            .await
            .unwrap_err();
        assert!(err.contains("original Principal"), "{err}");
        // A record whose Agent principal no longer exists is left unknown, not run as someone else.
        let mut orphan = rec.clone();
        orphan.execution_id = "act_agent_gone".into();
        orphan.principal_id = "agt_gone".into();
        orphan.principal_kind = "agent".into();
        orphan.reconcile_attempts = 0;
        e.store.claim_idempotency_and_insert(&orphan).unwrap();
        let err = e
            .actions
            .reconcile_execution_as_original("act_agent_gone")
            .await
            .unwrap_err();
        assert!(err.contains("no longer exists"), "{err}");
        let still = e
            .store
            .get_action_execution("act_agent_gone")
            .unwrap()
            .unwrap();
        assert_eq!(still.status, "unknown");
        assert_eq!(
            still.reconcile_attempts, 0,
            "nothing ran under another authority"
        );
    }

    // act-5: concurrent invocations with one idempotency key run the side effect once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn idempotency_claim_is_atomic_and_checks_arguments() {
        let e = engine();
        let admin = Principal::administrator();
        let before = connector_writes(&e).await;
        let mut handles = Vec::new();
        for _ in 0..8 {
            let a = e.actions.clone();
            let p = admin.clone();
            handles.push(tokio::spawn(async move {
                a.invoke("write", json!({"id": "k1"}), &p, Some("key-1".into()), None)
                    .await
            }));
        }
        let mut ids = BTreeSet::new();
        for h in handles {
            ids.insert(h.await.unwrap().unwrap().execution_id);
        }
        assert_eq!(
            ids.len(),
            1,
            "all concurrent callers got the same execution"
        );
        assert_eq!(
            connector_writes(&e).await,
            before + 1,
            "the side effect ran exactly once"
        );
        let ra = e
            .store
            .lookup_action_idempotency("write", "key-1")
            .unwrap()
            .unwrap();
        let n = e
            .store
            .list_action_executions(100)
            .unwrap()
            .into_iter()
            .filter(|r| r.idempotency_key.as_deref() == Some("key-1"))
            .count();
        assert_eq!(n, 1, "exactly one execution record for the key");

        let mismatch = e
            .actions
            .invoke(
                "write",
                json!({"id": "other"}),
                &admin,
                Some("key-1".into()),
                None,
            )
            .await
            .unwrap_err();
        assert!(mismatch.contains("different arguments"), "{mismatch}");

        // A retry-safe failure releases its key for a fresh attempt; a success never does.
        let f1 = e
            .actions
            .invoke(
                "write",
                json!({"id": "k2", "op": "reject_write"}),
                &admin,
                Some("key-2".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(f1.status, "failed");
        assert!(f1.retry_safe);
        let f2 = e
            .actions
            .invoke(
                "write",
                json!({"id": "k2", "op": "reject_write"}),
                &admin,
                Some("key-2".into()),
                None,
            )
            .await
            .unwrap();
        assert_ne!(f1.execution_id, f2.execution_id);
        let again = e
            .actions
            .invoke(
                "write",
                json!({"id": "k1"}),
                &admin,
                Some("key-1".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(again.execution_id, ra.execution_id);

        // An unknown outcome pins its key: no silent repeat of an uncertain external action.
        let u1 = e
            .actions
            .invoke(
                "other_write",
                json!({"id": "k3", "op": "unknown_sentinel"}),
                &admin,
                Some("key-3".into()),
                None,
            )
            .await
            .unwrap();
        let u2 = e
            .actions
            .invoke(
                "other_write",
                json!({"id": "k3", "op": "unknown_sentinel"}),
                &admin,
                Some("key-3".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(u1.execution_id, u2.execution_id);
    }

    // act-6: crash recovery of queued/running records, and dropped callers cannot orphan a run.
    #[tokio::test]
    async fn restart_recovery_and_dropped_callers() {
        let e = engine();
        let admin = Principal::administrator();
        let mk = |id: &str, status: &str| ActionExecutionRecord {
            execution_id: id.into(),
            action_instance: "write".into(),
            parent_execution_id: None,
            principal_id: "admin".into(),
            principal_kind: "administrator".into(),
            status: status.into(),
            idempotency_key: None,
            args: json!({"id": "crash"}),
            result: None,
            reason: None,
            retry_safe: false,
            reconcile_attempts: 0,
            trace_id: "tr_crash".into(),
            started_at: Utc::now().to_rfc3339(),
            finished_at: None,
            duration_ms: None,
        };
        e.store
            .claim_idempotency_and_insert(&mk("act_running", "running"))
            .unwrap();
        e.store
            .claim_idempotency_and_insert(&mk("act_queued", "queued"))
            .unwrap();
        let rec = e.actions.recover_after_restart();
        assert_eq!(rec.marked_unknown, vec!["act_running".to_string()]);
        assert_eq!(rec.marked_not_started, vec!["act_queued".to_string()]);
        let r = e
            .store
            .get_action_execution("act_running")
            .unwrap()
            .unwrap();
        assert_eq!(r.status, "unknown");
        assert!(e
            .store
            .get_action_reconcile_schedule("act_running")
            .unwrap()
            .is_some());
        let q = e.store.get_action_execution("act_queued").unwrap().unwrap();
        assert_eq!(q.status, "failed");
        assert!(q.retry_safe);
        assert!(audits(&e, "action.recover").len() >= 2);
        // The recovered record can be resolved explicitly.
        let resolved = e
            .actions
            .resolve_execution("act_running", "failed", "verified upstream", &admin)
            .await
            .unwrap();
        assert_eq!(resolved.status, "failed");

        // A caller that gives up (dropped future) does not leave the execution running.
        let fut = e.actions.invoke(
            "write",
            json!({"id": "spin", "op": "spin"}),
            &admin,
            None,
            None,
        );
        let _ = tokio::time::timeout(Duration::from_millis(5), fut).await;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let recs = e.store.list_action_executions(50).unwrap();
            let spin = recs
                .iter()
                .find(|r| r.args["op"] == "spin")
                .expect("record exists");
            if spin.status != "running" && spin.status != "queued" {
                assert_eq!(spin.status, "succeeded");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "execution orphaned in '{}'",
                spin.status
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // act-9: cancellation of a running execution with no side effects yet -> cancelled.
    #[tokio::test]
    async fn running_execution_can_be_cancelled() {
        let e = engine();
        let admin = Principal::administrator();
        let actions = e.actions.clone();
        let a2 = admin.clone();
        let h = tokio::spawn(async move {
            actions
                .invoke(
                    "write",
                    json!({"id": "c", "op": "spin_forever"}),
                    &a2,
                    None,
                    None,
                )
                .await
        });
        let exec_id = loop {
            let recs = e.store.list_action_executions(10).unwrap();
            if let Some(r) = recs
                .iter()
                .find(|r| r.args["op"] == "spin_forever" && r.status == "running")
            {
                break r.execution_id.clone();
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        // An unrelated Agent without resolve rights may not cancel it.
        let agent = Principal {
            id: "agt".into(),
            kind: PrincipalKind::Agent,
            ceiling: CapabilityCeiling::read_only_agent(),
        };
        assert!(e
            .actions
            .cancel_execution(&exec_id, "nope", &agent)
            .await
            .is_err());
        e.actions
            .cancel_execution(&exec_id, "operator abort", &admin)
            .await
            .unwrap();
        let fin = h.await.unwrap().unwrap();
        assert_eq!(fin.status, "cancelled");
        assert!(fin.retry_safe);
        assert!(audits(&e, "action.cancel")
            .iter()
            .any(|a| a.outcome == "requested"));
        // Terminal executions cannot be cancelled.
        assert!(e
            .actions
            .cancel_execution(&exec_id, "late", &admin)
            .await
            .is_err());
    }

    // act-10: nested executions share the parent's trace and hang under its span.
    #[tokio::test]
    async fn nested_actions_share_trace_and_link_spans() {
        let e = engine();
        let admin = Principal::administrator();
        let parent = e
            .actions
            .invoke("nested", json!({"id": "n1"}), &admin, None, None)
            .await
            .unwrap();
        assert_eq!(parent.status, "succeeded");
        let child = e
            .store
            .list_action_executions(20)
            .unwrap()
            .into_iter()
            .find(|r| r.parent_execution_id.as_deref() == Some(parent.execution_id.as_str()))
            .expect("child record");
        assert_eq!(child.trace_id, parent.trace_id);
        let spans = e.telemetry.recent_spans(200);
        let pspan = spans.iter().find(|s| s.name == "action:nested").unwrap();
        let cspan = spans
            .iter()
            .find(|s| s.name == "action:write" && s.trace_id == parent.trace_id)
            .unwrap();
        assert_eq!(
            cspan.parent_span_id.as_deref(),
            Some(pspan.span_id.as_str())
        );
        assert_eq!(pspan.attributes["children"]["total"], 1);
    }

    // act-11 / auth-17 / act-12 / act-14: resolution is conditional, authorized and finalized.
    #[tokio::test]
    async fn resolution_is_conditional_authorized_and_finalized() {
        let e = engine();
        let admin = Principal::administrator();
        let rec = e
            .actions
            .invoke(
                "other_write",
                json!({"id": "z", "op": "unknown_sentinel"}),
                &admin,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(rec.status, "unknown");

        assert!(e
            .actions
            .resolve_execution(&rec.execution_id, "succeeded", "   ", &admin)
            .await
            .unwrap_err()
            .contains("non-empty"));

        let agent = Principal {
            id: "agt_resolver".into(),
            kind: PrincipalKind::Agent,
            ceiling: CapabilityCeiling {
                can_resolve_executions: true,
                ..CapabilityCeiling::default()
            },
        };
        let denied = e
            .actions
            .resolve_execution(&rec.execution_id, "succeeded", "looked fine", &agent)
            .await
            .unwrap_err();
        assert!(denied.contains("does not grant action"), "{denied}");
        assert!(audits(&e, "action.resolve")
            .iter()
            .any(|a| a.outcome == "denied" && a.principal_id == "agt_resolver"));

        let ok = e
            .actions
            .resolve_execution(&rec.execution_id, "succeeded", "verified upstream", &admin)
            .await
            .unwrap();
        assert_eq!(ok.status, "succeeded");
        assert!(e
            .store
            .get_action_reconcile_schedule(&rec.execution_id)
            .unwrap()
            .is_none());
        let audit = audits(&e, "action.resolve")
            .into_iter()
            .find(|a| a.outcome == "succeeded")
            .unwrap();
        assert_eq!(audit.detail["previous_status"], "unknown");
        assert!(e
            .events
            .replay_since("other_write", 0)
            .unwrap()
            .iter()
            .any(|ev| ev.semantic_type == "dash.action_completed"
                && ev.payload["resolved_by"] == "manual_resolution"));

        // A late reconcile can no longer overwrite the audited resolution.
        let after = e
            .actions
            .reconcile_execution_as_original(&rec.execution_id)
            .await
            .unwrap();
        assert_eq!(after.status, "succeeded");
        assert!(e
            .actions
            .resolve_execution(&rec.execution_id, "failed", "second opinion", &admin)
            .await
            .is_err());
        // Store-level guard: a transition from the wrong state applies to no row.
        let applied = e
            .store
            .transition_action_execution(
                &rec.execution_id,
                &["unknown"],
                ActionTransition {
                    status: "failed",
                    result: None,
                    reason: Some("race"),
                    retry_safe: true,
                    reconcile_attempts: 9,
                    finished_at: None,
                    duration_ms: None,
                },
            )
            .unwrap();
        assert!(!applied);
    }

    // act-18: the result contract is validated; a violation after a write is `unknown`.
    #[tokio::test]
    async fn result_contract_violations() {
        let e = engine();
        let admin = Principal::administrator();
        let after_write = e
            .actions
            .invoke(
                "other_write",
                json!({"id": "rc1", "op": "bad_result_after_write"}),
                &admin,
                None,
                None,
            )
            .await
            .unwrap();
        let audit = audits(&e, "action.invoke")
            .into_iter()
            .find(|a| a.detail["execution_id"] == after_write.execution_id.as_str())
            .unwrap();
        assert_eq!(audit.outcome, "unknown");
        assert!(audit.detail["reason"]
            .as_str()
            .unwrap()
            .starts_with("invalid_result_contract"));

        let no_write = e
            .actions
            .invoke(
                "write",
                json!({"id": "rc2", "op": "bad_result_no_write"}),
                &admin,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(no_write.status, "failed");
        assert!(no_write
            .reason
            .unwrap()
            .starts_with("invalid_result_contract"));
    }

    // act-7 / widget-3: Requests resolve only through the Adapter's own bound inputs.
    #[tokio::test]
    async fn widget_requests_cannot_reach_unbound_instances() {
        let e = engine();
        let admin = Principal::administrator();
        let raw = e
            .actions
            .dispatch_widget_request_outcome(req("raw", "adapter_btn", 1, json!({})), &admin)
            .await
            .unwrap_err();
        assert!(raw.contains("no bound input 'other_write'"), "{raw}");
        assert!(e
            .store
            .list_action_executions(20)
            .unwrap()
            .iter()
            .all(|r| r.action_instance != "other_write"));

        let inv = e
            .actions
            .dispatch_widget_request_outcome(req("inv_raw", "adapter_btn", 2, json!({})), &admin)
            .await
            .unwrap_err();
        assert!(inv.contains("no bound input 'data_inst'"), "{inv}");

        let other = e
            .actions
            .invoke("other_write", json!({"id": "victim"}), &admin, None, None)
            .await
            .unwrap();
        let steal = e
            .actions
            .dispatch_widget_request_outcome(
                req(
                    "exec_steal",
                    "adapter_btn",
                    3,
                    json!({"execution_id": other.execution_id}),
                ),
                &admin,
            )
            .await
            .unwrap_err();
        assert!(steal.contains("did not create"), "{steal}");
    }

    // act-8 / widget-4 / act-20: protocol, binding, declaration and schema checks; refresh outcome.
    #[tokio::test]
    async fn widget_request_validation_and_outcomes() {
        let e = engine();
        let admin = Principal::administrator();
        let a = &e.actions;

        let mut bad = req("go", "adapter_btn", 1, json!({"id": "w1"}));
        bad.protocol = "dash.widget/0".into();
        assert!(a
            .dispatch_widget_request_outcome(bad, &admin)
            .await
            .unwrap_err()
            .contains("protocol"));
        let mut bad = req("go", "adapter_btn", 1, json!({"id": "w1"}));
        bad.kind = "event".into();
        assert!(a
            .dispatch_widget_request_outcome(bad, &admin)
            .await
            .unwrap_err()
            .contains("kind"));
        let mut stale = req("go", "adapter_btn", 1, json!({"id": "w1"}));
        stale.generation = 6;
        assert!(a
            .dispatch_widget_request_outcome(stale, &admin)
            .await
            .unwrap_err()
            .contains("stale"));

        // A made-up widget_id with the Adapter's prefix is not currently bound.
        let ghost = a
            .dispatch_widget_request_outcome(
                req("go", "adapter_ghost", 1, json!({"id": "w1"})),
                &admin,
            )
            .await
            .unwrap_err();
        assert!(ghost.contains("not currently bound"), "{ghost}");

        let undeclared = a
            .dispatch_widget_request_outcome(req("explode", "adapter_btn", 1, json!({})), &admin)
            .await
            .unwrap_err();
        assert!(
            undeclared.contains("does not accept request"),
            "{undeclared}"
        );

        let schema = a
            .dispatch_widget_request_outcome(req("go", "adapter_btn", 1, json!({"id": 5})), &admin)
            .await
            .unwrap_err();
        assert!(schema.contains("invalid payload"), "{schema}");

        let ok = a
            .dispatch_widget_request_outcome(
                req("go", "adapter_btn", 1, json!({"id": "w1"})),
                &admin,
            )
            .await
            .unwrap();
        let WidgetRequestOutcome::Action(rec) = ok else {
            panic!("expected action outcome")
        };
        assert_eq!(rec.status, "succeeded");
        assert_eq!(rec.action_instance, "write");
        // A retransmission of the same Request does not run the action twice.
        let WidgetRequestOutcome::Action(rec2) = a
            .dispatch_widget_request_outcome(
                req("go", "adapter_btn", 1, json!({"id": "w1"})),
                &admin,
            )
            .await
            .unwrap()
        else {
            panic!("expected action outcome")
        };
        assert_eq!(rec.execution_id, rec2.execution_id);
        // The child execution is traced under the Request span.
        let spans = e.telemetry.recent_spans(200);
        let req_span = spans
            .iter()
            .find(|s| s.name == "widget_request:adapter" && s.trace_id == rec.trace_id)
            .unwrap();
        let act_span = spans
            .iter()
            .find(|s| s.name == "action:write" && s.trace_id == rec.trace_id)
            .unwrap();
        assert_eq!(
            act_span.parent_span_id.as_deref(),
            Some(req_span.span_id.as_str())
        );

        // A refresh is reported as a refresh, not as a fabricated execution record.
        let refreshed = a
            .dispatch_widget_request_outcome(req("refresh", "adapter_btn", 2, json!({})), &admin)
            .await
            .unwrap();
        let v = serde_json::to_value(&refreshed).unwrap();
        assert_eq!(v["outcome"], "refreshed");
        assert_eq!(v["refreshed"], json!(["data_inst"]));
        assert!(v.get("execution_id").is_none());
        let action_json = serde_json::to_value(WidgetRequestOutcome::Action(rec.clone())).unwrap();
        assert_eq!(action_json["outcome"], "action");
        assert_eq!(action_json["execution_id"], rec.execution_id.as_str());
    }

    // state-3: on_request reads Dashboard State (declared keys only), never Instance State.
    #[tokio::test]
    async fn adapter_on_request_reads_dashboard_state() {
        let e = engine();
        let admin = Principal::administrator();
        e.store
            .set_state(
                "dashboard",
                "main",
                "sel",
                1,
                7,
                json!("from-dashboard"),
                None,
            )
            .unwrap();
        e.store
            .set_state(
                "instance",
                "adapter",
                "sel",
                1,
                7,
                json!("from-instance"),
                None,
            )
            .unwrap();
        let WidgetRequestOutcome::Action(rec) = e
            .actions
            .dispatch_widget_request_outcome(req("state", "adapter_btn", 1, json!({})), &admin)
            .await
            .unwrap()
        else {
            panic!("expected action outcome")
        };
        assert_eq!(rec.args["id"], "from-dashboard");

        let err = e
            .actions
            .dispatch_widget_request_outcome(
                req("undeclared_state", "adapter_btn", 2, json!({})),
                &admin,
            )
            .await
            .unwrap_err();
        assert!(err.contains("dashboard_state_reads"), "{err}");
    }

    // R7:SEC-2: a widget may only send the Requests its type declares (deny by default).
    #[tokio::test]
    async fn widgets_may_only_send_the_requests_they_declare() {
        let e = engine();
        let admin = Principal::administrator();
        let a = &e.actions;
        // A custom widget sends what it declares (`accepts_requests` and `requests` keys).
        let WidgetRequestOutcome::Action(rec) = a
            .dispatch_widget_request_outcome(
                req("go", "adapter_badge", 1, json!({"id": "b1"})),
                &admin,
            )
            .await
            .unwrap()
        else {
            panic!("expected action outcome")
        };
        assert_eq!(rec.status, "succeeded");
        a.dispatch_widget_request_outcome(req("refresh", "adapter_badge", 2, json!({})), &admin)
            .await
            .unwrap();
        // ... but nothing else the Adapter accepts.
        let err = a
            .dispatch_widget_request_outcome(req("state", "adapter_badge", 3, json!({})), &admin)
            .await
            .unwrap_err();
        assert!(err.contains("does not declare request 'state'"), "{err}");
        // A custom widget declaring nothing sends nothing, Dry run included.
        let err = a
            .dispatch_widget_request_outcome(
                req("go", "adapter_mute", 1, json!({"id": "m1"})),
                &admin,
            )
            .await
            .unwrap_err();
        assert!(err.contains("t.mute"), "{err}");
        let err = a
            .dry_run_widget_request(req("go", "adapter_mute", 1, json!({"id": "m1"})), &admin)
            .await
            .unwrap_err();
        assert!(err.contains("does not declare"), "{err}");
        // Built-in widgets that only render, and unknown widget types, send no Requests.
        for (wid, needle) in [
            ("adapter_label", "does not send Requests"),
            ("adapter_root", "does not send Requests"),
            ("adapter_stray", "unknown widget type"),
        ] {
            let err = a
                .dispatch_widget_request_outcome(req("refresh", wid, 1, json!({})), &admin)
                .await
                .unwrap_err();
            assert!(err.contains(needle), "{wid}: {err}");
        }
        assert!(e
            .store
            .list_action_executions(20)
            .unwrap()
            .iter()
            .all(|r| r.args["id"] != "m1"));
    }

    // R7:SEC-1: on_request cannot tell its Dry run from the dispatch by the trace id.
    #[tokio::test]
    async fn widget_request_dry_run_is_indistinguishable_by_trace_id() {
        let e = engine();
        let admin = Principal::administrator();
        let dry = e
            .actions
            .dry_run_widget_request(req("probe", "adapter_btn", 1, json!({})), &admin)
            .await
            .unwrap();
        assert_eq!(dry.resolution, "action", "{dry:?}");
        assert!(dry.requires_confirmation);
        assert_eq!(dry.action_instance.as_deref(), Some("write"));
    }

    // R7:SEC-1: a dispatch is bound to its Dry run's resolution. A Request shown as a refresh
    // (no confirmation) cannot run an action, and a confirmed action cannot change its target or
    // arguments; with no Dry run on record the dispatch is unconstrained.
    #[tokio::test]
    async fn widget_request_dispatch_cannot_diverge_from_its_dry_run() {
        let e = engine();
        let admin = Principal::administrator();
        let a = &e.actions;
        let set_sel = |v: &str| {
            e.store
                .set_state("dashboard", "main", "sel", 1, 7, json!(v), None)
                .unwrap();
        };
        let ran = |id: &str| {
            e.store
                .list_action_executions(50)
                .unwrap()
                .iter()
                .any(|r| r.args["id"] == id)
        };

        set_sel("safe");
        let dry = a
            .dry_run_widget_request(req("flip", "adapter_btn", 10, json!({})), &admin)
            .await
            .unwrap();
        assert_eq!(dry.resolution, "refresh");
        assert!(!dry.requires_confirmation);
        set_sel("evil");
        let err = a
            .dispatch_widget_request_outcome(req("flip", "adapter_btn", 11, json!({})), &admin)
            .await
            .unwrap_err();
        assert!(err.contains("Dry run resolved to a refresh"), "{err}");
        assert!(!ran("flip"), "the unconfirmed action must not run");
        assert!(audits(&e, "widget_request.dispatch")
            .iter()
            .any(|r| r.outcome == "denied"));

        // Imperative invocation is refused as well when the Dry run showed a refresh.
        set_sel("safe");
        let dry = a
            .dry_run_widget_request(req("probe_write", "adapter_btn", 12, json!({})), &admin)
            .await
            .unwrap();
        assert!(!dry.requires_confirmation);
        set_sel("evil");
        let err = a
            .dispatch_widget_request_outcome(
                req("probe_write", "adapter_btn", 13, json!({})),
                &admin,
            )
            .await
            .unwrap_err();
        assert!(
            err.contains("was not part of this widget Request's Dry run"),
            "{err}"
        );
        assert!(!ran("probe_write"));

        // A confirmed action call must keep its target and arguments.
        set_sel("one");
        let dry = a
            .dry_run_widget_request(req("flip_args", "adapter_btn", 14, json!({})), &admin)
            .await
            .unwrap();
        assert_eq!(dry.args, Some(json!({"id": "one"})));
        set_sel("two");
        let err = a
            .dispatch_widget_request_outcome(req("flip_args", "adapter_btn", 15, json!({})), &admin)
            .await
            .unwrap_err();
        assert!(
            err.contains("resolved to action 'write' at dispatch"),
            "{err}"
        );
        assert!(!ran("two"));
        // After a fresh Dry run (and confirmation) the same Request dispatches.
        a.dry_run_widget_request(req("flip_args", "adapter_btn", 16, json!({})), &admin)
            .await
            .unwrap();
        let WidgetRequestOutcome::Action(rec) = a
            .dispatch_widget_request_outcome(req("flip_args", "adapter_btn", 17, json!({})), &admin)
            .await
            .unwrap()
        else {
            panic!("expected action outcome")
        };
        assert_eq!(rec.args["id"], "two");

        // The binding is per Principal: another Principal's dispatch is not constrained by it.
        set_sel("safe");
        a.dry_run_widget_request(req("flip", "adapter_btn", 18, json!({})), &admin)
            .await
            .unwrap();
        set_sel("evil");
        let mut other = Principal::administrator();
        other.id = "admin2".into();
        assert!(a
            .dispatch_widget_request_outcome(req("flip", "adapter_btn", 19, json!({})), &other)
            .await
            .is_ok());
    }

    // R7:SEC-1: probing the read-only Dry run bridge taints the Dry run (always confirmed).
    #[tokio::test]
    async fn widget_request_dry_run_that_probes_for_read_only_requires_confirmation() {
        let e = engine();
        let admin = Principal::administrator();
        let dry = e
            .actions
            .dry_run_widget_request(req("probe_ro", "adapter_btn", 1, json!({})), &admin)
            .await
            .unwrap();
        assert_eq!(dry.resolution, "unresolved", "{dry:?}");
        assert!(dry.requires_confirmation);
        assert!(dry
            .dry_run_error
            .as_deref()
            .unwrap()
            .contains("attempted a side effect"));
    }

    // act-21: invalidates may name a Definition; unmatched entries are reported.
    #[test]
    fn invalidations_resolve_definitions_and_report_unmatched_entries() {
        let mut g = build_graph();
        let inst = g.instances["write"].clone();
        let def = g.definitions["t.write"].clone();
        assert_eq!(
            resolve_invalidations(&inst, &def, &g),
            vec!["data_inst".to_string()]
        );
        assert!(unresolved_invalidations(&g).is_empty());
        g.instances.remove("data_inst");
        let missing = unresolved_invalidations(&g);
        assert!(missing.contains(&("write".to_string(), "t.data".to_string())));
    }

    // Reference fixture: merge_pr declares a Definition (not Instance names) in `invalidates`,
    // the GitHub connector marks its reads read_only, and its Definition tests pass.
    #[tokio::test]
    async fn reference_merge_pr_fixture_contracts_and_tests() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/engineering-command-center/packages/github");
        let read = |p: &str| std::fs::read_to_string(root.join(p)).unwrap();
        let merge = load_def(
            "github",
            &read("merge_pr/definition.luau"),
            &read("merge_pr/runtime.luau"),
        );
        assert_eq!(
            merge.manifest.invalidates,
            vec!["github.pull_requests".to_string()]
        );
        let conn = load_def(
            "github",
            &read("connector/definition.luau"),
            &read("connector/runtime.luau"),
        );
        assert!(conn.manifest.operations["get_pr"].read_only);
        assert!(conn.manifest.operations["list_prs"].read_only);
        assert!(!conn.manifest.operations["merge_pr"].read_only);
        // The Definition tests mix Mock-backed cases (make_ctx, §4.5) with self-contained ones;
        // all of them run against the github Mock in dash-git's preview tests
        // (`definition_tests_reach_mocks_through_connector_and_data_inputs`). Here, without a
        // Mock bridge, the self-contained cases must still pass.
        let report = dash_luau::run_definition_tests(
            &merge.manifest,
            &merge.runtime_source,
            &read("merge_pr/tests.luau"),
            &BTreeMap::new(),
            Arc::new(dash_luau::NullHostBridge),
            json!({}),
        )
        .await
        .unwrap();
        let self_contained: Vec<_> = report
            .cases
            .iter()
            .filter(|c| !c.error.as_deref().unwrap_or("").contains("not bound"))
            .collect();
        assert!(
            self_contained.iter().all(|c| c.passed),
            "{:?}",
            report.cases
        );
        assert_eq!(self_contained.len(), 5, "{:?}", report.cases);
    }
}
