//! Automation engine (§7.3): Event triggers with durable at-least-once delivery from the Event
//! log, cron schedules with explicit catch-up policies, Signal predicate triggers, bounded
//! retries with dead-lettering, replay safety and execution records. Every run executes under the
//! automation Instance's Administrator-granted Service principal (`run_as`, ADR-0002).
//!
//! Documented defaults (spec §19 leaves these open):
//! - **Event delivery** is driven by [`AutomationEngine::drain_durable_events`], which reads each
//!   trigger source's log in sequence order from the automation's consumer checkpoint. A consumer
//!   (automation, source) seen for the first time starts at the last Event emitted before the
//!   current Generation was activated (catch-up policy `skip` for history).
//! - **Retries**: up to `max_attempts` (default 3) with exponential backoff, then dead-letter.
//!   A missing Service principal grant fails fast (no retry) and is dead-lettered for redrive.
//! - **Replay safety**: a run that succeeded for a trigger occurrence (`event:<event_id>`,
//!   `cron:<i>:<time>`, …) is never repeated automatically; its recorded result is returned.
//!   Re-execution needs an explicit request by an authorized Principal.
//! - **Idempotency**: action invocations without a script-supplied key get a deterministic key
//!   `auto:<automation>:<trigger_ref>:<call ordinal>`, stable across retries and redelivery.
//! - **Cron** (`skip` default): fires only for a scheduled time noticed within 60 s; older missed
//!   windows are skipped. `catch_up` fires once for all missed windows.
//! - **Signal triggers** fire at most once per observed change of a Query result or State value
//!   (intermediate values may coalesce, §7.1); the first observation only records a baseline
//!   unless `catch_up = "catch_up"`.
//! - **Concurrency**: one run at a time per automation Instance (preserves per-source order).

use crate::actions::ActionManager;
use crate::authority::{Principal, PrincipalKind, ValidationIssue};
use crate::events::{EventBus, EventEnvelope};
use crate::model::{CapabilityCeiling, ConfiguredGraph, InstanceConfig, LoadedDefinition};
use crate::query_engine::QueryEngine;
use crate::signal::QueryStatus;
use crate::telemetry::Telemetry;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dash_luau::{
    CompiledInstanceBundle, DefinitionCategory, ExecutionContextConfig, HostBridge,
    HttpHostRequest, HttpHostResponse, TriggerDeclSpec, VmPool,
};
use dash_store::{AutomationExecutionRecord, DeadLetterRecord, SqliteStore, EVENT_READ_BUDGET};
use dashmap::DashMap;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Default bounded delivery attempts before an Event is dead-lettered.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Default catch-up policy for missed schedules and first Signal observations (§7.3).
pub const DEFAULT_CATCH_UP: &str = "skip";
/// A cron time noticed within this many seconds of being due counts as on time.
pub const CRON_ON_TIME_GRACE_SECS: i64 = 60;
/// Upper bound on scheduled times inspected per cron trigger and tick.
const MAX_CRON_WINDOWS: usize = 10_000;

/// Outcome summary of one durable-consumer pass.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DrainReport {
    pub succeeded: usize,
    pub failed: usize,
    /// Events read but not matching any trigger of the consumer.
    pub skipped: usize,
}

/// What caused one automation run.
enum RunTrigger<'a> {
    Event(&'a EventEnvelope),
    Cron {
        index: usize,
        schedule: String,
        scheduled_for: DateTime<Utc>,
        missed: usize,
        catch_up: String,
    },
    Signal {
        index: usize,
        observed_at: String,
        fingerprint: String,
        value: serde_json::Value,
    },
    Manual(serde_json::Value),
}

impl RunTrigger<'_> {
    fn kind(&self) -> &'static str {
        match self {
            RunTrigger::Event(_) => "event",
            RunTrigger::Cron { .. } => "cron",
            RunTrigger::Signal { .. } => "signal",
            RunTrigger::Manual(_) => "manual",
        }
    }

    fn trigger_ref(&self, execution_id: &str) -> String {
        match self {
            RunTrigger::Event(ev) => format!("event:{}", ev.event_id),
            RunTrigger::Cron {
                index,
                scheduled_for,
                ..
            } => format!("cron:{index}:{}", scheduled_for.to_rfc3339()),
            RunTrigger::Signal {
                index,
                observed_at,
                fingerprint,
                ..
            } => format!("signal:{index}:{observed_at}:{fingerprint}"),
            RunTrigger::Manual(_) => format!("manual:{execution_id}"),
        }
    }

    fn event(&self) -> Option<&EventEnvelope> {
        match self {
            RunTrigger::Event(ev) => Some(ev),
            _ => None,
        }
    }

    /// The value exposed to Luau as `ctx.event`.
    fn payload(&self) -> serde_json::Value {
        match self {
            RunTrigger::Event(ev) => serde_json::to_value(ev).unwrap_or_default(),
            RunTrigger::Cron {
                index,
                schedule,
                scheduled_for,
                missed,
                catch_up,
            } => serde_json::json!({
                "trigger": {
                    "kind": "cron",
                    "index": index,
                    "schedule": schedule,
                    "scheduled_for": scheduled_for.to_rfc3339(),
                    "missed_windows": missed,
                    "catch_up": catch_up,
                },
                "payload": {},
            }),
            RunTrigger::Signal {
                index,
                observed_at,
                fingerprint,
                value,
            } => serde_json::json!({
                "trigger": {
                    "kind": "signal",
                    "index": index,
                    "observed_at": observed_at,
                    "fingerprint": fingerprint,
                },
                "payload": { "value": value },
            }),
            RunTrigger::Manual(p) => p.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AutomationEngine {
    graph: Arc<ArcSwap<ConfiguredGraph>>,
    pool: VmPool,
    store: SqliteStore,
    actions: ActionManager,
    queries: QueryEngine,
    events: EventBus,
    telemetry: Telemetry,
    run_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    retry_backoff_ms: Arc<AtomicU64>,
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
            run_locks: Arc::new(DashMap::new()),
            retry_backoff_ms: Arc::new(AtomicU64::new(200)),
        }
    }

    /// Base delay of the exponential retry backoff (default 200 ms).
    pub fn set_retry_backoff(&self, base: Duration) {
        self.retry_backoff_ms
            .store(base.as_millis() as u64, Ordering::Relaxed);
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
        let ceiling: CapabilityCeiling = serde_json::from_value(val).map_err(|e| e.to_string())?;
        Ok(Principal {
            id: principal_id.to_string(),
            kind: PrincipalKind::ServicePrincipal,
            ceiling,
        })
    }

    fn automations(g: &ConfiguredGraph) -> Vec<(&String, &InstanceConfig, &LoadedDefinition)> {
        g.instances
            .iter()
            .filter(|(_, inst)| {
                inst.category == DefinitionCategory::Automation && inst.enabled != Some(false)
            })
            .filter_map(|(id, inst)| g.definitions.get(&inst.definition).map(|d| (id, inst, d)))
            .collect()
    }

    fn event_trigger_matches(
        t: &TriggerDeclSpec,
        inst: &InstanceConfig,
        ev: &EventEnvelope,
    ) -> bool {
        t.kind == "event"
            && t.event_type.as_deref() == Some(ev.semantic_type.as_str())
            && match t.source_input.as_deref() {
                Some(slot) => {
                    inst.inputs.get(slot).map(|s| s.as_str()) == Some(ev.source_instance.as_str())
                }
                None => true,
            }
    }

    /// Bounded attempts for an Event: the largest `max_attempts` among matching triggers.
    fn max_attempts_for_event(
        def: &LoadedDefinition,
        inst: &InstanceConfig,
        ev: &EventEnvelope,
    ) -> Option<u32> {
        let matching: Vec<&TriggerDeclSpec> = def
            .manifest
            .triggers
            .iter()
            .filter(|t| Self::event_trigger_matches(t, inst, ev))
            .collect();
        if matching.is_empty() {
            return None;
        }
        Some(
            matching
                .iter()
                .map(|t| t.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1))
                .max()
                .unwrap_or(DEFAULT_MAX_ATTEMPTS),
        )
    }

    /// Deliver one Event to every matching Automation instance (live/explicit delivery).
    ///
    /// Durable, non-replay Events at or below the consumer checkpoint were already consumed by
    /// [`Self::drain_durable_events`] and are skipped. Replayed Events whose run already succeeded
    /// return the recorded result without re-running (§7.2). Checkpoints are only advanced by the
    /// ordered log consumer, so delivering one Event here never causes earlier ones to be skipped.
    pub async fn dispatch_event(
        &self,
        event: &EventEnvelope,
    ) -> Vec<Result<serde_json::Value, String>> {
        let g = self.graph.load_full();
        let mut results = Vec::new();
        for (auto_id, inst, def) in Self::automations(&g) {
            let Some(max_attempts) = Self::max_attempts_for_event(def, inst, event) else {
                continue;
            };
            if event.durable && !event.is_replay {
                if let Ok(Some(ckpt)) = self.events.find_checkpoint(auto_id, &event.source_instance)
                {
                    if event.sequence <= ckpt {
                        continue;
                    }
                }
            }
            results.push(
                self.deliver(
                    auto_id,
                    inst,
                    def,
                    &g,
                    RunTrigger::Event(event),
                    false,
                    max_attempts,
                )
                .await,
            );
        }
        results
    }

    /// Durable at-least-once consumer pass (§7.2): for every enabled automation and each source
    /// Instance of its Event triggers, read the durable log after the consumer checkpoint in
    /// sequence order, deliver matching Events (with bounded retry, then dead-letter) and advance
    /// the checkpoint. Run at startup, periodically and whenever the live broadcast signals new
    /// Events or reports lag.
    /// Source Instances an automation's Event triggers consume (`all_sources` is filled lazily
    /// from the log for triggers without a `source_input`).
    fn event_sources(
        &self,
        inst: &InstanceConfig,
        def: &LoadedDefinition,
        all_sources: &mut Option<Vec<String>>,
    ) -> BTreeSet<String> {
        let mut sources = BTreeSet::new();
        for t in def.manifest.triggers.iter().filter(|t| t.kind == "event") {
            match t.source_input.as_deref() {
                Some(slot) => {
                    if let Some(src) = inst.inputs.get(slot) {
                        sources.insert(src.clone());
                    }
                }
                None => {
                    if all_sources.is_none() {
                        *all_sources = Some(self.events.log().sources().unwrap_or_default());
                    }
                    sources.extend(all_sources.clone().unwrap_or_default());
                }
            }
        }
        sources
    }

    /// Register the durable consumer of every active automation that has none yet, at the
    /// current Generation's activation point. Called when a Generation becomes active (startup
    /// and activation), *before* Events can be emitted under it: a consumer first registered by
    /// a later drain would start after that later Generation's activation and silently skip
    /// Events emitted (but not yet delivered) before a restart (§7.2 at-least-once). Returns the
    /// number of consumers registered.
    pub fn register_consumers(&self) -> usize {
        let g = self.graph.load_full();
        let mut all_sources: Option<Vec<String>> = None;
        let mut registered = 0;
        for (auto_id, inst, def) in Self::automations(&g) {
            for src in self.event_sources(inst, def, &mut all_sources) {
                if let Ok(None) = self.events.find_checkpoint(auto_id, &src) {
                    let start = self.consumer_start_sequence(&g, &src);
                    match self.events.ack_checkpoint(auto_id, &src, start) {
                        Ok(()) => registered += 1,
                        Err(e) => self.log_error(
                            auto_id,
                            &format!("cannot register consumer for '{src}': {e}"),
                        ),
                    }
                }
            }
        }
        registered
    }

    pub async fn drain_durable_events(&self) -> DrainReport {
        let g = self.graph.load_full();
        let mut report = DrainReport::default();
        let mut all_sources: Option<Vec<String>> = None;
        for (auto_id, inst, def) in Self::automations(&g) {
            if !def.manifest.triggers.iter().any(|t| t.kind == "event") {
                continue;
            }
            let sources = self.event_sources(inst, def, &mut all_sources);
            for src in sources {
                let mut ckpt = match self.events.find_checkpoint(auto_id, &src) {
                    Ok(Some(c)) => c,
                    Ok(None) => {
                        let start = self.consumer_start_sequence(&g, &src);
                        if let Err(e) = self.events.ack_checkpoint(auto_id, &src, start) {
                            self.log_error(
                                auto_id,
                                &format!("cannot register consumer for '{src}': {e}"),
                            );
                            continue;
                        }
                        start
                    }
                    Err(e) => {
                        self.log_error(
                            auto_id,
                            &format!("cannot read checkpoint for '{src}': {e}"),
                        );
                        continue;
                    }
                };
                loop {
                    let batch = match self.events.read_since(&src, ckpt, EVENT_READ_BUDGET) {
                        Ok(b) => b,
                        Err(e) => {
                            self.log_error(auto_id, &format!("cannot read events of '{src}': {e}"));
                            break;
                        }
                    };
                    let full = batch.len() >= EVENT_READ_BUDGET;
                    for ev in &batch {
                        match Self::max_attempts_for_event(def, inst, ev) {
                            Some(max_attempts) => {
                                match self
                                    .deliver(
                                        auto_id,
                                        inst,
                                        def,
                                        &g,
                                        RunTrigger::Event(ev),
                                        false,
                                        max_attempts,
                                    )
                                    .await
                                {
                                    Ok(_) => report.succeeded += 1,
                                    // Dead-lettered inside `deliver`; redrive from the DLQ.
                                    Err(_) => report.failed += 1,
                                }
                            }
                            None => report.skipped += 1,
                        }
                        if let Err(e) = self.events.ack_checkpoint(auto_id, &src, ev.sequence) {
                            self.log_error(auto_id, &format!("checkpoint ack failed: {e}"));
                        }
                        ckpt = ev.sequence;
                    }
                    if !full {
                        break;
                    }
                }
            }
        }
        report
    }

    /// Where a newly registered consumer starts: after the last Event emitted before the current
    /// Generation was activated (history is skipped, §7.3 default). Without a recorded Generation
    /// (Preview, unit tests) it starts at the beginning of the log.
    fn consumer_start_sequence(&self, g: &ConfiguredGraph, source: &str) -> i64 {
        match self.store.get_generation_activated_at(g.generation_id) {
            Ok(Some(activated_at)) => self
                .events
                .log()
                .max_sequence_before(source, &activated_at)
                .unwrap_or(0),
            _ => 0,
        }
    }

    /// Replay durable Events of `source_instance` after `after_sequence` to matching automations.
    ///
    /// Without `reexecute_as`, runs that already succeeded are not repeated (their recorded result
    /// is returned). With `reexecute_as`, an Administrator explicitly requests re-execution: runs
    /// are repeated with fresh idempotency keys, and the request is audited (§7.2).
    pub async fn replay_events(
        &self,
        source_instance: &str,
        after_sequence: i64,
        reexecute_as: Option<&Principal>,
    ) -> Result<Vec<Result<serde_json::Value, String>>, String> {
        if let Some(p) = reexecute_as {
            if p.kind != PrincipalKind::Administrator && !p.ceiling.unrestricted {
                let _ = self.store.append_audit(
                    &p.id,
                    p.kind.as_str(),
                    "automation.reexecute",
                    source_instance,
                    "denied",
                    serde_json::json!({ "after_sequence": after_sequence }),
                );
                return Err("only the Administrator may re-execute replayed Events".into());
            }
            let _ = self.store.append_audit(
                &p.id,
                p.kind.as_str(),
                "automation.reexecute",
                source_instance,
                "allowed",
                serde_json::json!({ "after_sequence": after_sequence }),
            );
        }
        let g = self.graph.load_full();
        let events = self.events.replay_since(source_instance, after_sequence)?;
        let mut results = Vec::new();
        for ev in &events {
            for (auto_id, inst, def) in Self::automations(&g) {
                let Some(max_attempts) = Self::max_attempts_for_event(def, inst, ev) else {
                    continue;
                };
                results.push(
                    self.deliver(
                        auto_id,
                        inst,
                        def,
                        &g,
                        RunTrigger::Event(ev),
                        reexecute_as.is_some(),
                        max_attempts,
                    )
                    .await,
                );
            }
        }
        Ok(results)
    }

    pub fn list_dead_letters(
        &self,
        include_resolved: bool,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, String> {
        self.events.list_dead_letters(include_resolved, limit)
    }

    /// Redeliver a dead-lettered Event to its consumer automation with the original `event_id`
    /// (so derived idempotency keys still apply), marking the dead letter resolved on success.
    /// Requires the Administrator or a Principal allowed to resolve executions.
    pub async fn redrive_dead_letter(
        &self,
        dlq_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        if !principal.ceiling.unrestricted && !principal.ceiling.can_resolve_executions {
            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "automation.redrive",
                dlq_id,
                "denied",
                serde_json::json!({}),
            );
            return Err("principal lacks authority to redrive dead letters".into());
        }
        let dlq = self
            .events
            .get_dead_letter(dlq_id)?
            .ok_or_else(|| format!("dead letter '{dlq_id}' not found"))?;
        if dlq.resolved {
            return Err(format!("dead letter '{dlq_id}' is already resolved"));
        }
        let event = self
            .events
            .get_event(&dlq.event_id)?
            .ok_or_else(|| format!("event '{}' is no longer retained", dlq.event_id))?;
        let g = self.graph.load_full();
        let inst = g
            .instances
            .get(&dlq.consumer_id)
            .filter(|i| i.category == DefinitionCategory::Automation)
            .ok_or_else(|| format!("consumer automation '{}' not found", dlq.consumer_id))?;
        let def = g
            .definitions
            .get(&inst.definition)
            .ok_or_else(|| format!("definition '{}' missing", inst.definition))?;
        let max_attempts =
            Self::max_attempts_for_event(def, inst, &event).unwrap_or(DEFAULT_MAX_ATTEMPTS);
        let res = self
            .deliver(
                &dlq.consumer_id,
                inst,
                def,
                &g,
                RunTrigger::Event(&event),
                false,
                max_attempts,
            )
            .await;
        let outcome = if res.is_ok() { "succeeded" } else { "failed" };
        if res.is_ok() {
            self.events.resolve_dead_letter(dlq_id)?;
        }
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "automation.redrive",
            dlq_id,
            outcome,
            serde_json::json!({ "event_id": dlq.event_id, "automation": dlq.consumer_id }),
        );
        res
    }

    pub fn list_executions(
        &self,
        automation_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AutomationExecutionRecord>, String> {
        self.store
            .list_automation_executions(automation_id, limit)
            .map_err(|e| e.to_string())
    }

    /// Manually trigger one automation instance under its configured `run_as` Service principal.
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
        self.deliver(
            automation_id,
            &inst,
            &def,
            &g,
            RunTrigger::Manual(payload),
            false,
            1,
        )
        .await
    }

    /// Validate a cron schedule expression (standard 5/6/7 field via `cron` crate).
    pub fn validate_cron_schedule(expr: &str) -> Result<(), String> {
        parse_cron(expr).map(|_| ())
    }

    /// Fire due cron triggers (§7.3). Last fire times are persisted, so restarts apply the
    /// trigger's catch-up policy: `skip` (default) runs only an on-time window, `catch_up` runs
    /// once for all missed windows. A trigger seen for the first time starts from `now`.
    pub async fn run_due_schedules(
        &self,
        now: DateTime<Utc>,
    ) -> Vec<(String, Result<serde_json::Value, String>)> {
        let g = self.graph.load_full();
        let mut out = Vec::new();
        for (auto_id, inst, def) in Self::automations(&g) {
            for (index, t) in def.manifest.triggers.iter().enumerate() {
                if t.kind != "cron" {
                    continue;
                }
                let Some(expr) = t.schedule.as_deref() else {
                    continue;
                };
                let schedule = match parse_cron(expr) {
                    Ok(s) => s,
                    Err(e) => {
                        self.log_error(auto_id, &e);
                        continue;
                    }
                };
                let key = format!("cron:{index}");
                let state = self
                    .store
                    .get_automation_trigger_state(auto_id, &key)
                    .ok()
                    .flatten();
                let last = state
                    .as_ref()
                    .and_then(|s| s.last_fired_at.as_deref())
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|d| d.with_timezone(&Utc));
                let Some(last) = last else {
                    let _ = self.store.set_automation_trigger_state(
                        auto_id,
                        &key,
                        Some(&now.to_rfc3339()),
                        None,
                    );
                    continue;
                };
                let due: Vec<DateTime<Utc>> = schedule
                    .after(&last)
                    .take_while(|t| *t <= now)
                    .take(MAX_CRON_WINDOWS)
                    .collect();
                let Some(latest) = due.last().copied() else {
                    continue;
                };
                let policy = t
                    .catch_up
                    .as_deref()
                    .unwrap_or(DEFAULT_CATCH_UP)
                    .to_string();
                let on_time = (now - latest).num_seconds() <= CRON_ON_TIME_GRACE_SECS;
                let fire = policy == "catch_up" || on_time;
                if fire {
                    let missed = if policy == "catch_up" {
                        due.len()
                    } else {
                        due.len() - 1
                    };
                    let res = self
                        .deliver(
                            auto_id,
                            inst,
                            def,
                            &g,
                            RunTrigger::Cron {
                                index,
                                schedule: expr.to_string(),
                                scheduled_for: latest,
                                missed,
                                catch_up: policy.clone(),
                            },
                            false,
                            t.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1),
                        )
                        .await;
                    out.push((auto_id.clone(), res));
                } else {
                    self.telemetry.log(
                        "info",
                        "automation",
                        inst.run_as.as_deref().unwrap_or(""),
                        "",
                        &format!(
                            "automation '{auto_id}' skipped {} missed cron window(s) (catch_up = skip)",
                            due.len()
                        ),
                    );
                }
                let _ = self.store.set_automation_trigger_state(
                    auto_id,
                    &key,
                    Some(&latest.to_rfc3339()),
                    None,
                );
            }
        }
        out
    }

    /// Evaluate Signal triggers (§7.3): when the watched Query result (fresh only) or State value
    /// changes, evaluate the optional read-only Luau predicate and run the automation if it holds.
    pub async fn run_due_signal_triggers(
        &self,
    ) -> Vec<(String, Result<serde_json::Value, String>)> {
        let g = self.graph.load_full();
        let mut out = Vec::new();
        for (auto_id, inst, def) in Self::automations(&g) {
            for (index, t) in def.manifest.triggers.iter().enumerate() {
                if t.kind != "signal" {
                    continue;
                }
                let observed = match self.observe_signal(auto_id, inst, t).await {
                    Ok(Some(o)) => o,
                    Ok(None) => continue,
                    Err(e) => {
                        self.log_error(auto_id, &format!("signal trigger #{index}: {e}"));
                        continue;
                    }
                };
                let (fingerprint, value) = observed;
                let key = format!("signal:{index}");
                let prev = self
                    .store
                    .get_automation_trigger_state(auto_id, &key)
                    .ok()
                    .flatten()
                    .and_then(|s| s.last_fingerprint);
                let changed = match prev.as_deref() {
                    Some(p) => p != fingerprint,
                    None => t.catch_up.as_deref() == Some("catch_up"),
                };
                if prev.as_deref() == Some(fingerprint.as_str()) {
                    continue;
                }
                let observed_at = Utc::now().to_rfc3339();
                // Record the observation first: Signal triggers fire at most once per change.
                let _ = self.store.set_automation_trigger_state(
                    auto_id,
                    &key,
                    Some(&observed_at),
                    Some(&fingerprint),
                );
                if !changed {
                    continue;
                }
                let trigger = RunTrigger::Signal {
                    index,
                    observed_at,
                    fingerprint,
                    value,
                };
                if let Some(pred) = t.predicate.as_deref() {
                    match self
                        .evaluate_predicate(auto_id, inst, def, &g, pred, &trigger)
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => {
                            self.log_error(
                                auto_id,
                                &format!("signal predicate '{pred}' failed: {e}"),
                            );
                            continue;
                        }
                    }
                }
                let res = self
                    .deliver(
                        auto_id,
                        inst,
                        def,
                        &g,
                        trigger,
                        false,
                        t.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1),
                    )
                    .await;
                out.push((auto_id.clone(), res));
            }
        }
        out
    }

    /// Current (fingerprint, value) of a Signal trigger's source, or `None` when the Query result
    /// is not fresh (stale data never fires a trigger).
    async fn observe_signal(
        &self,
        auto_id: &str,
        inst: &InstanceConfig,
        t: &TriggerDeclSpec,
    ) -> Result<Option<(String, serde_json::Value)>, String> {
        if let Some(state_key) = t.state_key.as_deref() {
            let (scope, owner) = match t.source_input.as_deref() {
                Some(slot) => (
                    "instance",
                    inst.inputs
                        .get(slot)
                        .cloned()
                        .ok_or_else(|| format!("source_input '{slot}' not bound"))?,
                ),
                None => ("dashboard", bound_dashboard(inst)?),
            };
            let rec = self
                .queries
                .state_manager()
                .get(scope, &owner, state_key)
                .map_err(|e| e.to_string())?;
            return Ok(Some(match rec {
                Some(r) => (format!("v{}:{}", r.version, r.updated_at), r.value),
                None => ("absent".to_string(), serde_json::Value::Null),
            }));
        }
        let slot = t
            .source_input
            .as_deref()
            .ok_or_else(|| "signal trigger needs 'source_input' or 'state_key'".to_string())?;
        let target = inst
            .inputs
            .get(slot)
            .ok_or_else(|| format!("source_input '{slot}' not bound"))?;
        let sp_id = inst
            .run_as
            .as_deref()
            .ok_or_else(|| format!("automation '{auto_id}' missing 'run_as' Service principal"))?;
        let principal = self.resolve_service_principal(sp_id)?;
        let params = t.params.clone().unwrap_or_else(|| serde_json::json!({}));
        let env = self
            .queries
            .query(target, params, &principal, false)
            .await?;
        if env.status != QueryStatus::Fresh {
            return Ok(None);
        }
        let value = env.value.unwrap_or(serde_json::Value::Null);
        Ok(Some((fingerprint_json(&value), value)))
    }

    async fn evaluate_predicate(
        &self,
        auto_id: &str,
        inst: &InstanceConfig,
        def: &LoadedDefinition,
        g: &ConfiguredGraph,
        predicate: &str,
        trigger: &RunTrigger<'_>,
    ) -> Result<bool, String> {
        let sp_id = inst
            .run_as
            .as_deref()
            .ok_or_else(|| format!("automation '{auto_id}' missing 'run_as' Service principal"))?;
        let principal = self.resolve_service_principal(sp_id)?;
        let trace_id = format!("auto_{}", Uuid::now_v7().simple());
        let bridge = self.bridge(inst, g, &principal, String::new(), None, true);
        let out = self
            .execute(
                inst,
                def,
                g,
                predicate,
                trigger.payload(),
                &trace_id,
                bridge,
            )
            .await?;
        Ok(match out {
            serde_json::Value::Bool(b) => b,
            serde_json::Value::Null => false,
            _ => true,
        })
    }

    fn run_lock(&self, auto_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.run_locks
            .entry(auto_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn log_error(&self, auto_id: &str, msg: &str) {
        self.telemetry.log(
            "error",
            "automation",
            auto_id,
            "",
            &format!("automation '{auto_id}': {msg}"),
        );
    }

    /// Run one automation for one trigger occurrence: replay guard, execution record, bounded
    /// retries, trace span and dead-lettering of durable Events.
    #[allow(clippy::too_many_arguments)]
    async fn deliver(
        &self,
        auto_id: &str,
        inst: &InstanceConfig,
        def: &LoadedDefinition,
        g: &ConfiguredGraph,
        trigger: RunTrigger<'_>,
        reexecute: bool,
        max_attempts: u32,
    ) -> Result<serde_json::Value, String> {
        let lock = self.run_lock(auto_id);
        let _guard = lock.lock().await;

        let execution_id = format!("aex_{}", Uuid::now_v7().simple());
        let trigger_ref = trigger.trigger_ref(&execution_id);
        if !reexecute {
            match self
                .store
                .find_succeeded_automation_execution(auto_id, &trigger_ref)
            {
                Ok(Some(prev)) => return Ok(prev.result.unwrap_or(serde_json::Value::Null)),
                Ok(None) => {}
                Err(e) => return Err(format!("cannot read automation execution records: {e}")),
            }
        }

        let event = trigger.event();
        let trace_id = event
            .map(|e| e.trace_id.clone())
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| format!("auto_{}", Uuid::now_v7().simple()));
        let started = Instant::now();
        let mut record = AutomationExecutionRecord {
            execution_id: execution_id.clone(),
            automation_id: auto_id.to_string(),
            trigger_kind: trigger.kind().to_string(),
            trigger_ref: trigger_ref.clone(),
            event_id: event.map(|e| e.event_id.clone()),
            run_as: inst.run_as.clone(),
            status: "running".into(),
            attempts: 0,
            error: None,
            result: None,
            action_executions: vec![],
            trace_id: trace_id.clone(),
            started_at: Utc::now().to_rfc3339(),
            finished_at: None,
        };
        self.store
            .insert_automation_execution(&record)
            .map_err(|e| format!("cannot record automation execution: {e}"))?;

        let run_key = if reexecute {
            format!("auto:{auto_id}:{trigger_ref}:reexec:{execution_id}")
        } else {
            format!("auto:{auto_id}:{trigger_ref}")
        };
        let action_ids = Arc::new(Mutex::new(Vec::new()));
        let payload = trigger.payload();

        let principal = inst
            .run_as
            .as_deref()
            .ok_or_else(|| format!("automation '{auto_id}' missing 'run_as' Service principal"))
            .and_then(|sp| self.resolve_service_principal(sp));
        let mut attempts: u32 = 0;
        let outcome = match principal {
            // A missing grant is a configuration problem: fail fast, dead-letter for redrive.
            Err(e) => Err(e),
            Ok(principal) => {
                let mut last = Err("automation did not run".to_string());
                while attempts < max_attempts.max(1) {
                    attempts += 1;
                    let bridge = self.bridge(
                        inst,
                        g,
                        &principal,
                        run_key.clone(),
                        reexecute.then(|| execution_id.clone()),
                        false,
                    );
                    let ids = Arc::clone(&bridge.action_ids);
                    let handler = if def.manifest.handler.is_empty() {
                        "run"
                    } else {
                        def.manifest.handler.as_str()
                    };
                    last = self
                        .execute(inst, def, g, handler, payload.clone(), &trace_id, bridge)
                        .await;
                    action_ids
                        .lock()
                        .unwrap()
                        .extend(ids.lock().unwrap().drain(..));
                    match &last {
                        Ok(_) => break,
                        Err(e) if attempts < max_attempts => {
                            self.telemetry.log(
                                "warn",
                                "automation",
                                &principal.id,
                                &trace_id,
                                &format!("automation '{auto_id}' attempt {attempts} failed, retrying: {e}"),
                            );
                            let base = self.retry_backoff_ms.load(Ordering::Relaxed);
                            let delay = base.saturating_mul(1u64 << (attempts - 1).min(10));
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                        }
                        Err(_) => {}
                    }
                }
                last
            }
        };

        let mut seen = BTreeSet::new();
        let ids: Vec<String> = action_ids
            .lock()
            .unwrap()
            .iter()
            .filter(|id| seen.insert((*id).clone()))
            .cloned()
            .collect();
        record.attempts = attempts as i64;
        record.action_executions = ids;
        record.status = if outcome.is_ok() {
            "succeeded"
        } else {
            "failed"
        }
        .into();
        record.error = outcome.as_ref().err().cloned();
        let finished_at = Utc::now().to_rfc3339();
        if let Err(e) = self.store.finish_automation_execution(
            &execution_id,
            &record.status,
            record.attempts,
            record.error.as_deref(),
            outcome.as_ref().ok(),
            &record.action_executions,
            &finished_at,
        ) {
            self.log_error(auto_id, &format!("cannot finish execution record: {e}"));
        }

        let principal_id = inst.run_as.clone().unwrap_or_default();
        self.telemetry.record_span(
            &trace_id,
            None,
            &format!("automation:{auto_id}"),
            &principal_id,
            serde_json::json!({
                "execution_id": execution_id,
                "trigger_kind": record.trigger_kind,
                "trigger_ref": trigger_ref,
                "event_id": record.event_id,
                "attempts": record.attempts,
                "reexecute": reexecute,
                "action_executions": record.action_executions,
                "error": record.error,
            }),
            if outcome.is_ok() { "ok" } else { "error" },
            started.elapsed().as_millis() as i64,
        );
        self.telemetry.log(
            if outcome.is_ok() { "info" } else { "error" },
            "automation",
            &principal_id,
            &trace_id,
            &format!(
                "automation '{auto_id}' ({}) {} after {} attempt(s)",
                trigger_ref, record.status, record.attempts
            ),
        );

        if let (Err(err), Some(ev)) = (&outcome, event) {
            if ev.durable {
                if let Err(e) = self.events.send_to_dead_letter(
                    auto_id,
                    &ev.event_id,
                    record.attempts.max(1),
                    err,
                ) {
                    self.log_error(
                        auto_id,
                        &format!("cannot dead-letter '{}': {e}", ev.event_id),
                    );
                }
            }
        }
        outcome
    }

    fn bridge(
        &self,
        inst: &InstanceConfig,
        g: &ConfiguredGraph,
        principal: &Principal,
        run_key: String,
        reexecution_id: Option<String>,
        read_only: bool,
    ) -> Arc<AutomationHostBridge> {
        Arc::new(AutomationHostBridge {
            actions: self.actions.clone(),
            queries: self.queries.clone(),
            graph: g.clone(),
            caller_inst: inst.clone(),
            principal: principal.clone(),
            run_key,
            reexecution_id,
            call_ordinal: AtomicU32::new(0),
            action_ids: Arc::new(Mutex::new(Vec::new())),
            read_only,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute(
        &self,
        inst: &InstanceConfig,
        def: &LoadedDefinition,
        graph: &ConfiguredGraph,
        handler: &str,
        payload: serde_json::Value,
        trace_id: &str,
        bridge: Arc<AutomationHostBridge>,
    ) -> Result<serde_json::Value, String> {
        let bundle = CompiledInstanceBundle {
            instance_id: inst.id.clone(),
            manifest: def.manifest.clone(),
            runtime_source: def.runtime_source.clone(),
            config_digest: inst.config_digest(),
            resolved_libraries: graph.resolved_libraries(),
        };
        self.pool
            .execute_handler(
                &bundle,
                handler,
                ExecutionContextConfig {
                    category: DefinitionCategory::Automation,
                    instance_id: inst.id.clone(),
                    input_names: inst.inputs.keys().cloned().collect(),
                    config: inst.config.clone(),
                    payload,
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
            .map_err(|e| self.store.redactor().redact_str(&e.to_string()))
    }
}

fn parse_cron(expr: &str) -> Result<cron::Schedule, String> {
    let normalized = if expr.split_whitespace().count() == 5 {
        format!("0 {expr}")
    } else {
        expr.to_string()
    };
    cron::Schedule::from_str(&normalized)
        .map_err(|e| format!("invalid cron expression '{expr}': {e}"))
}

/// The Dashboard an automation's Dashboard State refers to: its Instance config must bind one
/// explicitly (`config.dashboard`), which is reviewed like any other configuration change.
fn bound_dashboard(inst: &InstanceConfig) -> Result<String, String> {
    inst.config
        .get("dashboard")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            format!(
                "automation '{}' has no bound dashboard; set config.dashboard to use Dashboard State",
                inst.id
            )
        })
}

fn canonical_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<&String, &serde_json::Value> = map.iter().collect();
            let parts: Vec<String> = sorted
                .into_iter()
                .map(|(k, v)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap_or_default(),
                        canonical_json(v)
                    )
                })
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

fn fingerprint_json(v: &serde_json::Value) -> String {
    let mut h = Sha256::new();
    h.update(canonical_json(v).as_bytes());
    hex::encode(&h.finalize()[..12])
}

/// Graph validation for background work (§7.3, §7.4, ADR-0002), called from
/// `validate_configured_graph`: automation triggers (kinds, cron syntax, bound sources, catch-up
/// policy, Dashboard binding), history series Service principals, and polling providers that
/// have no background (keep-warm) query.
pub fn validate_background_work(
    graph: &ConfiguredGraph,
    store: &SqliteStore,
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let mut push = |severity: &str, code: &str, target: &str, message: String| {
        issues.push(ValidationIssue {
            severity: severity.into(),
            code: code.into(),
            target: target.into(),
            message,
        })
    };
    for (inst_id, inst) in &graph.instances {
        let Some(def) = graph.definitions.get(&inst.definition) else {
            continue;
        };
        match inst.category {
            DefinitionCategory::Automation => {
                if let Some(d) = inst.config.get("dashboard") {
                    match d.as_str() {
                        Some(id) if graph.dashboards.contains_key(id) => {}
                        _ => push(
                            "error",
                            "AUTOMATION_UNKNOWN_DASHBOARD",
                            inst_id,
                            format!("config.dashboard {d} does not name a configured dashboard"),
                        ),
                    }
                }
                for (idx, t) in def.manifest.triggers.iter().enumerate() {
                    let mut bad = |msg: String| {
                        push(
                            "error",
                            "INVALID_TRIGGER",
                            inst_id,
                            format!("trigger #{idx}: {msg}"),
                        )
                    };
                    match t.kind.as_str() {
                        "event" => {
                            if t.event_type.as_deref().unwrap_or("").is_empty() {
                                bad("event trigger requires 'event_type'".into());
                            }
                        }
                        "cron" => match t.schedule.as_deref() {
                            None | Some("") => bad("cron trigger requires 'schedule'".into()),
                            Some(expr) => {
                                if let Err(e) = parse_cron(expr) {
                                    bad(e);
                                }
                            }
                        },
                        "signal" => {
                            if t.state_key.is_none() && t.source_input.is_none() {
                                bad("signal trigger requires 'source_input' or 'state_key'".into());
                            }
                            if t.state_key.is_some() && t.source_input.is_none() {
                                if let Err(e) = bound_dashboard(inst) {
                                    bad(e);
                                }
                            }
                            if t.state_key.is_none() {
                                if let Some(target) = t
                                    .source_input
                                    .as_deref()
                                    .and_then(|s| inst.inputs.get(s))
                                    .and_then(|id| graph.instances.get(id))
                                {
                                    if target.category != DefinitionCategory::DataProvider {
                                        bad(format!(
                                            "signal source '{}' must be a data_provider",
                                            target.id
                                        ));
                                    }
                                }
                            }
                        }
                        other => bad(format!(
                            "unknown trigger kind '{other}' (expected event | cron | signal)"
                        )),
                    }
                    if let Some(cu) = t.catch_up.as_deref() {
                        if cu != "skip" && cu != "catch_up" {
                            bad(format!(
                                "unknown catch_up policy '{cu}' (expected skip | catch_up)"
                            ));
                        }
                    }
                    if let Some(slot) = t.source_input.as_deref() {
                        if !inst.inputs.contains_key(slot) {
                            bad(format!(
                                "source_input '{slot}' is not bound on this instance"
                            ));
                        }
                    }
                }
            }
            DefinitionCategory::DataProvider => {
                if def.manifest.updates.poll_interval_secs.is_none() {
                    continue;
                }
                let has_history = graph
                    .workspace
                    .history
                    .iter()
                    .any(|h| &h.instance_id == inst_id);
                let keep_warm = inst.config.get("keep_warm");
                match inst.run_as.as_deref() {
                    Some(sp) if !graph.service_principals.contains_key(sp) => push(
                        "error",
                        "UNKNOWN_SERVICE_PRINCIPAL",
                        inst_id,
                        format!("keep-warm polling references undeclared service principal '{sp}'"),
                    ),
                    Some(_) if !keep_warm.map(|k| k.is_array()).unwrap_or(false) => push(
                        "warning",
                        "KEEP_WARM_PARAMS_MISSING",
                        inst_id,
                        "run_as is set but config.keep_warm lists no query parameter sets to poll".into(),
                    ),
                    None if !has_history => push(
                        "warning",
                        "POLL_WITHOUT_BACKGROUND_QUERY",
                        inst_id,
                        "provider declares poll_interval_secs but has no run_as + config.keep_warm or history series; it is polled only while a query is subscribed".into(),
                    ),
                    _ => {}
                }
            }
            _ => {}
        }
    }
    issues.extend(crate::history::validate_history_series(graph, store));
    issues
}

struct AutomationHostBridge {
    actions: ActionManager,
    queries: QueryEngine,
    graph: ConfiguredGraph,
    caller_inst: InstanceConfig,
    principal: Principal,
    /// Base of deterministic default idempotency keys for this run.
    run_key: String,
    /// Set for an explicitly authorized re-execution: script-supplied keys are suffixed so the
    /// action really runs again.
    reexecution_id: Option<String>,
    call_ordinal: AtomicU32,
    action_ids: Arc<Mutex<Vec<String>>>,
    /// Predicates are read-only: no actions, no State writes.
    read_only: bool,
}

impl AutomationHostBridge {
    /// One owner resolution shared by reads and writes (§8).
    fn state_target(&self, scope: &str) -> Result<(String, String), String> {
        match scope {
            "instance" => Ok(("instance".into(), self.caller_inst.id.clone())),
            "dashboard" => {
                let dash = bound_dashboard(&self.caller_inst)?;
                if !self.graph.dashboards.contains_key(&dash) {
                    return Err(format!("bound dashboard '{dash}' not found"));
                }
                Ok(("dashboard".into(), dash))
            }
            other => Err(format!("automations cannot access '{other}' State")),
        }
    }
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
        if self.read_only {
            return Err("automation predicates are read-only and cannot invoke actions".into());
        }
        let target_action = self
            .caller_inst
            .inputs
            .get(input_name)
            .ok_or_else(|| format!("action input '{input_name}' not bound"))?;
        let ordinal = self.call_ordinal.fetch_add(1, Ordering::Relaxed) + 1;
        let key = match (idempotency_key, &self.reexecution_id) {
            (Some(k), None) => k,
            (Some(k), Some(reexec)) => format!("{k}:reexec:{reexec}"),
            (None, _) => format!("{}:{ordinal}", self.run_key),
        };
        let rec = self
            .actions
            .invoke(target_action, args, &self.principal, Some(key), None)
            .await?;
        self.action_ids
            .lock()
            .unwrap()
            .push(rec.execution_id.clone());
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
        Err("automations do not emit Events; invoke an action instead (§7.2)".into())
    }

    async fn state_get(&self, scope: &str, key: &str) -> Result<serde_json::Value, String> {
        let (scope, owner) = self.state_target(scope)?;
        let rec = self
            .queries
            .state_manager()
            .get(&scope, &owner, key)
            .map_err(|e| e.to_string())?;
        Ok(rec.map(|r| r.value).unwrap_or(serde_json::Value::Null))
    }

    async fn state_set(
        &self,
        scope: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), String> {
        if self.read_only {
            return Err("automation predicates are read-only and cannot write State".into());
        }
        let (scope, owner) = self.state_target(scope)?;
        if scope == "instance" {
            // Declared Instance State schema version, type and size limit (§8).
            return self
                .queries
                .state_manager()
                .set_instance_state(&self.graph, &owner, key, value)
                .map(|_| ())
                .map_err(|e| e.to_string());
        }
        // Write through the StateManager so a State Signal is published (§8).
        self.queries
            .state_manager()
            .set(
                &scope,
                &owner,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cron_parsing_accepts_five_field_expressions() {
        assert!(AutomationEngine::validate_cron_schedule("*/5 * * * *").is_ok());
        assert!(AutomationEngine::validate_cron_schedule("0 0 9 * * Mon-Fri").is_ok());
        assert!(AutomationEngine::validate_cron_schedule("not a cron").is_err());
    }

    #[test]
    fn fingerprints_ignore_key_order() {
        assert_eq!(
            fingerprint_json(&json!({"a": 1, "b": [1, 2]})),
            fingerprint_json(&json!({"b": [1, 2], "a": 1}))
        );
        assert_ne!(
            fingerprint_json(&json!([1, 2])),
            fingerprint_json(&json!([2, 1]))
        );
    }

    #[test]
    fn trigger_refs_are_stable_per_occurrence() {
        let ev = EventEnvelope {
            event_id: "evt_1".into(),
            source_instance: "merge_pr".into(),
            sequence: 1,
            emitted_at: Utc::now().to_rfc3339(),
            semantic_type: "github.pr_merged".into(),
            schema_version: "1".into(),
            trace_id: "tr".into(),
            durable: true,
            payload: json!({}),
            is_replay: false,
        };
        assert_eq!(RunTrigger::Event(&ev).trigger_ref("aex_a"), "event:evt_1");
        assert_eq!(RunTrigger::Event(&ev).trigger_ref("aex_b"), "event:evt_1");
        let manual = RunTrigger::Manual(json!({}));
        assert_ne!(manual.trigger_ref("aex_a"), manual.trigger_ref("aex_b"));
        let at = DateTime::parse_from_rfc3339("2026-01-01T09:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let cron = RunTrigger::Cron {
            index: 0,
            schedule: "0 9 * * *".into(),
            scheduled_for: at,
            missed: 0,
            catch_up: "skip".into(),
        };
        assert_eq!(cron.trigger_ref("x"), "cron:0:2026-01-01T09:00:00+00:00");
        assert_eq!(cron.payload()["trigger"]["kind"], "cron");
    }
}
