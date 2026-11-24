//! Sandboxed Luau VM pool, quota enforcement, controlled library loading, and Generation carry-over.
//!
//! Resource control (§3.2, §15.3, ADR-0003):
//! - **Wall-clock deadline** covers the whole invocation, including time spent suspended in
//!   `ctx:await`/`ctx:await_all` on host operations. Nested executions started from inside an
//!   invocation (e.g. a provider querying another provider) inherit the caller's remaining deadline.
//! - **Concurrency**: one coroutine per Instance VM at a time; waiting requests queue on the VM
//!   lock asynchronously (no blocking thread is held while queued). The queue per Instance is bounded
//!   (`max_pending_per_instance`) and the wait is bounded (`queue_timeout`); top-level executions are
//!   additionally bounded by a pool-wide semaphore (`max_concurrent_executions`).
//! - **Budgets** default to the pool's `ResourceBudget`, may be overridden per Instance via a budget
//!   resolver (`ResourceOverrides`), and are always clamped to the pool's daemon-wide ceiling.

use crate::host_api::{build_dash_runtime_sdk, ExecutionContextConfig, LuaContext};
use crate::manifest::DefinitionManifest;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use mlua::{Function, Lua, LuaOptions, LuaSerdeExt, StdLib, Table, Value, VmState};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("luau runtime error: {0}")]
    Luau(#[from] mlua::Error),
    #[error("execution timed out after {0:?}")]
    Timeout(Duration),
    #[error("execution cancelled")]
    Cancelled,
    #[error("memory budget ({0} bytes) exceeded")]
    MemoryExceeded(usize),
    #[error("definition '{0}' handler '{1}' not found")]
    HandlerNotFound(String, String),
    #[error("blocking execution join error: {0}")]
    Join(#[from] tokio::task::JoinError),
    /// Admission rejected: the Instance queue is full or the queue wait exceeded its bound.
    /// No Luau code ran and no host operation was issued.
    #[error("execution queue full: {0}")]
    QueueFull(String),
}

#[derive(Debug, Clone)]
pub struct ResourceBudget {
    pub memory_limit_bytes: usize,
    /// Wall-clock deadline for one invocation, including host awaits.
    pub wall_timeout: Duration,
    pub max_interrupts: u64,
    pub idle_ttl: Duration,
    /// Pool-wide bound on concurrently running top-level executions.
    pub max_concurrent_executions: usize,
    /// Bound on executions queued behind the one running in a single Instance VM.
    pub max_pending_per_instance: usize,
    /// Maximum time an execution may wait for admission (semaphore + Instance VM lock).
    pub queue_timeout: Duration,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            memory_limit_bytes: 32 * 1024 * 1024, // 32 MiB per active Instance VM
            wall_timeout: Duration::from_secs(5),
            max_interrupts: 2_000_000,
            idle_ttl: Duration::from_secs(300),
            max_concurrent_executions: 64,
            max_pending_per_instance: 256,
            queue_timeout: Duration::from_secs(10),
        }
    }
}

impl ResourceBudget {
    /// Default daemon-wide ceiling that per-Instance overrides are clamped to.
    pub fn default_ceiling() -> Self {
        Self {
            memory_limit_bytes: 256 * 1024 * 1024,
            wall_timeout: Duration::from_secs(60),
            max_interrupts: 50_000_000,
            idle_ttl: Duration::from_secs(24 * 3600),
            max_concurrent_executions: 1024,
            max_pending_per_instance: 4096,
            queue_timeout: Duration::from_secs(120),
        }
    }

    /// Apply per-Instance overrides on top of `self`, clamping every value to `ceiling`.
    pub fn with_overrides(&self, ov: &ResourceOverrides, ceiling: &ResourceBudget) -> Self {
        let mut b = self.clone();
        if let Some(mib) = ov.memory_limit_mib {
            b.memory_limit_bytes = (mib as usize).saturating_mul(1024 * 1024);
        }
        if let Some(ms) = ov.timeout_ms {
            b.wall_timeout = Duration::from_millis(ms);
        }
        if let Some(n) = ov.max_interrupts {
            b.max_interrupts = n;
        }
        if let Some(s) = ov.idle_ttl_secs {
            b.idle_ttl = Duration::from_secs(s);
        }
        if let Some(n) = ov.max_pending {
            b.max_pending_per_instance = n;
        }
        if let Some(ms) = ov.queue_timeout_ms {
            b.queue_timeout = Duration::from_millis(ms);
        }
        b.clamp_to(ceiling)
    }

    fn clamp_to(mut self, c: &ResourceBudget) -> Self {
        self.memory_limit_bytes = self
            .memory_limit_bytes
            .clamp(1024 * 1024, c.memory_limit_bytes.max(1024 * 1024));
        self.wall_timeout = self.wall_timeout.clamp(
            Duration::from_millis(1),
            c.wall_timeout.max(Duration::from_millis(1)),
        );
        self.max_interrupts = self.max_interrupts.clamp(1, c.max_interrupts.max(1));
        self.idle_ttl = self.idle_ttl.min(c.idle_ttl);
        self.max_concurrent_executions = self
            .max_concurrent_executions
            .clamp(1, c.max_concurrent_executions.max(1));
        self.max_pending_per_instance = self
            .max_pending_per_instance
            .min(c.max_pending_per_instance);
        self.queue_timeout = self.queue_timeout.min(c.queue_timeout);
        self
    }

    fn fingerprint(&self) -> String {
        format!(
            "{}:{}:{}",
            self.memory_limit_bytes,
            self.wall_timeout.as_millis(),
            self.max_interrupts
        )
    }
}

/// Optional per-Instance (or Workspace-wide) resource budget overrides (§2.1 "resource budgets
/// attach to Instances"). Every value is clamped to the pool's daemon-wide ceiling.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_mib: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_interrupts: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_ttl_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pending: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_timeout_ms: Option<u64>,
}

impl ResourceOverrides {
    /// Field-wise merge: values in `self` win over `base`.
    pub fn merged_over(&self, base: &ResourceOverrides) -> ResourceOverrides {
        ResourceOverrides {
            memory_limit_mib: self.memory_limit_mib.or(base.memory_limit_mib),
            timeout_ms: self.timeout_ms.or(base.timeout_ms),
            max_interrupts: self.max_interrupts.or(base.max_interrupts),
            idle_ttl_secs: self.idle_ttl_secs.or(base.idle_ttl_secs),
            max_pending: self.max_pending.or(base.max_pending),
            queue_timeout_ms: self.queue_timeout_ms.or(base.queue_timeout_ms),
        }
    }
}

/// Resolves per-Instance budget overrides at execution time (e.g. from the live Configured graph).
pub type BudgetResolver = Arc<dyn Fn(&str) -> Option<ResourceOverrides> + Send + Sync>;

/// Per-invocation VM telemetry (§15.1: VM startup, memory, interruption, queue and execution time).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub instance_id: String,
    pub definition_id: String,
    pub handler: String,
    pub trace_id: String,
    pub vm_uid: String,
    /// True when this invocation had to boot a fresh VM.
    pub vm_started: bool,
    pub vm_startup_ms: u64,
    pub queue_ms: u64,
    pub exec_ms: u64,
    pub interrupts: u64,
    pub used_memory_bytes: usize,
    pub memory_limit_bytes: usize,
    /// ok | error | timeout | cancelled | memory_exceeded | queue_full
    pub outcome: String,
}

/// Callback receiving the stats of every invocation (wired to the trace store by the engine).
pub type ExecutionObserver = Arc<dyn Fn(&ExecutionStats) + Send + Sync>;

/// Pool-level counters for metrics views.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolMetrics {
    pub active_vms: usize,
    pub queued_or_running: usize,
    pub total_executions: u64,
    pub total_vm_starts: u64,
    pub total_timeouts: u64,
    pub total_memory_exceeded: u64,
    pub total_queue_rejections: u64,
    pub total_reclaimed: u64,
}

tokio::task_local! {
    /// Deadline of the invocation currently driving this task; nested executions inherit it.
    static EXECUTION_DEADLINE: Instant;
}

/// Remaining deadline of the enclosing Luau invocation, if called from inside one.
pub fn current_execution_deadline() -> Option<Instant> {
    EXECUTION_DEADLINE.try_with(|d| *d).ok()
}

/// Run `fut` with `deadline` as the inherited execution deadline (used by the test runner so
/// Mock-backed nested executions share the per-test deadline).
pub(crate) async fn with_execution_deadline<F: std::future::Future>(
    deadline: Instant,
    fut: F,
) -> F::Output {
    EXECUTION_DEADLINE.scope(deadline, fut).await
}

/// Source bundle needed to boot an Instance's Luau VM.
#[derive(Debug, Clone)]
pub struct CompiledInstanceBundle {
    pub instance_id: String,
    pub manifest: DefinitionManifest,
    pub runtime_source: String,
    pub config_digest: String,
    /// Resolved package libraries allowed in `require(lib_name)`. Definition code may only
    /// require the libraries its manifest declares in `libraries`.
    pub resolved_libraries: BTreeMap<String, String>,
}

struct PooledVm {
    vm_uid: String,
    definition_digest: String,
    config_digest: String,
    budget_fingerprint: String,
    idle_ttl: Duration,
    last_used: Instant,
    /// Executions queued on or running in this VM.
    in_flight: Arc<AtomicUsize>,
    /// Protected behind an async mutex so only one coroutine executes in a given VM at a time,
    /// while multiple Instances execute fully concurrently across blocking pool threads.
    lua: Arc<tokio::sync::Mutex<Lua>>,
}

struct VmHandle {
    vm_uid: String,
    lua: Arc<tokio::sync::Mutex<Lua>>,
    in_flight: Arc<AtomicUsize>,
    started: bool,
    startup: Duration,
}

#[derive(Default)]
struct PoolCounters {
    executions: AtomicU64,
    vm_starts: AtomicU64,
    timeouts: AtomicU64,
    memory_exceeded: AtomicU64,
    queue_rejections: AtomicU64,
}

/// Replacement VMs built for a pending Generation but not yet installed in the live pool.
pub struct PreparedVms {
    vms: Vec<(String, PooledVm)>,
}

impl PreparedVms {
    pub fn instance_ids(&self) -> Vec<String> {
        self.vms.iter().map(|(id, _)| id.clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.vms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vms.is_empty()
    }
}

#[derive(Clone)]
pub struct VmPool {
    vms: Arc<DashMap<String, PooledVm>>,
    budget: ResourceBudget,
    ceiling: ResourceBudget,
    total_reclaimed: Arc<AtomicU64>,
    permits: Arc<tokio::sync::Semaphore>,
    resolver: Arc<RwLock<Option<BudgetResolver>>>,
    observer: Arc<RwLock<Option<ExecutionObserver>>>,
    counters: Arc<PoolCounters>,
}

struct InFlightGuard(Arc<AtomicUsize>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl VmPool {
    pub fn new(budget: ResourceBudget) -> Self {
        let ceiling = ResourceBudget::default_ceiling();
        Self::with_ceiling(budget, ceiling)
    }

    /// Create a pool whose per-Instance overrides are clamped to `ceiling`. The default budget is
    /// clamped as well, so the ceiling is always authoritative.
    pub fn with_ceiling(budget: ResourceBudget, ceiling: ResourceBudget) -> Self {
        let budget = budget.clamp_to(&ceiling);
        Self {
            vms: Arc::new(DashMap::new()),
            permits: Arc::new(tokio::sync::Semaphore::new(
                budget.max_concurrent_executions,
            )),
            budget,
            ceiling,
            total_reclaimed: Arc::new(AtomicU64::new(0)),
            resolver: Arc::new(RwLock::new(None)),
            observer: Arc::new(RwLock::new(None)),
            counters: Arc::new(PoolCounters::default()),
        }
    }

    /// Install a per-Instance budget resolver (shared by all clones of this pool).
    pub fn set_budget_resolver(&self, resolver: BudgetResolver) {
        *self.resolver.write().unwrap() = Some(resolver);
    }

    /// Install an observer receiving `ExecutionStats` for every invocation.
    pub fn set_execution_observer(&self, observer: ExecutionObserver) {
        *self.observer.write().unwrap() = Some(observer);
    }

    /// Effective budget for an Instance: pool default + resolver overrides, clamped to the ceiling.
    pub fn effective_budget(&self, instance_id: &str) -> ResourceBudget {
        let resolver = self.resolver.read().unwrap().clone();
        match resolver.and_then(|r| r(instance_id)) {
            Some(ov) => self.budget.with_overrides(&ov, &self.ceiling),
            None => self.budget.clone(),
        }
    }

    /// Resource budget applied to every execution in this pool.
    pub fn budget(&self) -> &ResourceBudget {
        &self.budget
    }

    pub fn active_vm_count(&self) -> usize {
        self.vms.len()
    }

    pub fn total_reclaimed(&self) -> u64 {
        self.total_reclaimed.load(Ordering::Relaxed)
    }

    pub fn metrics(&self) -> PoolMetrics {
        PoolMetrics {
            active_vms: self.vms.len(),
            queued_or_running: self
                .vms
                .iter()
                .map(|e| e.value().in_flight.load(Ordering::Acquire))
                .sum(),
            total_executions: self.counters.executions.load(Ordering::Relaxed),
            total_vm_starts: self.counters.vm_starts.load(Ordering::Relaxed),
            total_timeouts: self.counters.timeouts.load(Ordering::Relaxed),
            total_memory_exceeded: self.counters.memory_exceeded.load(Ordering::Relaxed),
            total_queue_rejections: self.counters.queue_rejections.load(Ordering::Relaxed),
            total_reclaimed: self.total_reclaimed(),
        }
    }

    /// Returns the live VM UID for an instance (used in tests to prove Generation carry-over!).
    pub fn vm_uid(&self, instance_id: &str) -> Option<String> {
        self.vms.get(instance_id).map(|v| v.vm_uid.clone())
    }

    /// Synchronize pool on Generation activation (§12.2 Generation carry-over).
    /// Unchanged instances (same `definition_digest` + `config_digest`) keep their live VM!
    pub fn sync_generation(&self, desired: &[CompiledInstanceBundle]) -> Vec<String> {
        let mut desired_map: HashMap<&str, &CompiledInstanceBundle> = HashMap::new();
        for b in desired {
            desired_map.insert(b.instance_id.as_str(), b);
        }

        let mut dropped = Vec::new();
        let existing_keys: Vec<String> = self.vms.iter().map(|e| e.key().clone()).collect();
        for inst_id in existing_keys {
            match desired_map.get(inst_id.as_str()) {
                None => {
                    self.vms.remove(&inst_id);
                    dropped.push(inst_id);
                }
                Some(bundle) => {
                    let unchanged = self
                        .vms
                        .get(&inst_id)
                        .map(|v| {
                            v.definition_digest == bundle.manifest.identity_version
                                && v.config_digest == bundle.config_digest
                        })
                        .unwrap_or(false);
                    if !unchanged {
                        self.vms.remove(&inst_id);
                        dropped.push(inst_id);
                    }
                }
            }
        }
        dropped
    }

    /// Reclaim VMs idle longer than their `idle_ttl` (or a custom override). VMs with queued or
    /// running executions are never reclaimed.
    pub fn reclaim_idle(&self, ttl_override: Option<Duration>) -> usize {
        let now = Instant::now();
        let to_remove: Vec<(String, String)> = self
            .vms
            .iter()
            .filter(|e| {
                let ttl = ttl_override.unwrap_or(e.value().idle_ttl);
                e.value().in_flight.load(Ordering::Acquire) == 0
                    && now.duration_since(e.value().last_used) >= ttl
            })
            .map(|e| (e.key().clone(), e.value().vm_uid.clone()))
            .collect();
        let mut count = 0;
        for (k, uid) in to_remove {
            // Re-check atomically: the VM may have been picked up (or replaced) meanwhile.
            if self
                .vms
                .remove_if(&k, |_, v| {
                    v.vm_uid == uid && v.in_flight.load(Ordering::Acquire) == 0
                })
                .is_some()
            {
                count += 1;
                self.total_reclaimed.fetch_add(1, Ordering::Relaxed);
            }
        }
        count
    }

    /// Evict a single instance VM (e.g., on unrecoverable runtime fault or circuit open).
    pub fn evict(&self, instance_id: &str) {
        self.vms.remove(instance_id);
    }

    /// Prepare replacement VMs for a pending Generation without installing them (§12.2 step 5):
    /// each VM is built under this pool's `ResourceBudget` and its `runtime.luau` module is
    /// evaluated (wall-time, interrupt and memory limits enforced). The first failing Instance
    /// is returned so activation can be refused before anything is committed.
    pub async fn prepare_replacements(
        &self,
        bundles: &[CompiledInstanceBundle],
    ) -> Result<PreparedVms, (String, RuntimeError)> {
        let mut vms = Vec::with_capacity(bundles.len());
        for bundle in bundles {
            let inst_id = bundle.instance_id.clone();
            // Per-Instance budget (`[resources]` overrides clamped to the ceiling), as at runtime.
            let budget = self.effective_budget(&inst_id);
            let lua = build_sandboxed_vm(bundle, &budget).map_err(|e| (inst_id.clone(), e))?;
            let fingerprint = budget.fingerprint();
            let idle_ttl = budget.idle_ttl;
            let src = bundle.runtime_source.clone();
            let def_id = bundle.manifest.id.clone();
            let lua = tokio::task::spawn_blocking(move || -> Result<Lua, RuntimeError> {
                let started = Instant::now();
                let ticks = Arc::new(AtomicU64::new(0));
                let ticks_ref = Arc::clone(&ticks);
                let wall_timeout = budget.wall_timeout;
                let max_interrupts = budget.max_interrupts;
                lua.set_interrupt(move |_| {
                    let count = ticks_ref.fetch_add(1, Ordering::Relaxed);
                    if started.elapsed() > wall_timeout || count > max_interrupts {
                        return Err(mlua::Error::runtime("DASH_TIMEOUT"));
                    }
                    Ok(VmState::Continue)
                });
                let evaluated = lua
                    .load(&src)
                    .set_name(format!("{def_id}/runtime.luau"))
                    .eval::<Value>();
                lua.set_interrupt(|_| Ok(VmState::Continue));
                match evaluated {
                    Ok(Value::Table(t)) => {
                        lua.globals().set("__dash_module", Value::Table(t))?;
                    }
                    Ok(_) => {
                        return Err(RuntimeError::Luau(mlua::Error::runtime(
                            "runtime.luau must return a module table",
                        )))
                    }
                    Err(e) => {
                        return Err(if e.to_string().contains("DASH_TIMEOUT") {
                            RuntimeError::Timeout(wall_timeout)
                        } else if matches!(e, mlua::Error::MemoryError(_)) {
                            RuntimeError::MemoryExceeded(budget.memory_limit_bytes)
                        } else {
                            RuntimeError::Luau(e)
                        })
                    }
                }
                Ok(lua)
            })
            .await
            .map_err(|e| (inst_id.clone(), RuntimeError::Join(e)))?
            .map_err(|e| (inst_id.clone(), e))?;
            self.counters.vm_starts.fetch_add(1, Ordering::Relaxed);
            vms.push((
                inst_id,
                PooledVm {
                    vm_uid: format!("vm_{}", Uuid::now_v7().simple()),
                    definition_digest: bundle.manifest.identity_version.clone(),
                    config_digest: bundle.config_digest.clone(),
                    budget_fingerprint: fingerprint,
                    idle_ttl,
                    last_used: Instant::now(),
                    in_flight: Arc::new(AtomicUsize::new(0)),
                    lua: Arc::new(tokio::sync::Mutex::new(lua)),
                },
            ));
        }
        Ok(PreparedVms { vms })
    }

    /// Install VMs prepared by [`VmPool::prepare_replacements`], replacing any live VM of the
    /// same Instance. Called right after the atomic Generation switch.
    pub fn install_prepared(&self, prepared: PreparedVms) -> Vec<String> {
        let mut installed = Vec::with_capacity(prepared.vms.len());
        for (inst_id, vm) in prepared.vms {
            self.vms.insert(inst_id.clone(), vm);
            installed.push(inst_id);
        }
        installed
    }

    /// Evict only if the live VM is still the one identified by `vm_uid`.
    fn evict_vm(&self, instance_id: &str, vm_uid: &str) {
        self.vms.remove_if(instance_id, |_, v| v.vm_uid == vm_uid);
    }

    /// Atomically fetch the Instance VM or boot a new one (no check-then-insert race).
    fn get_or_spawn_vm(
        &self,
        bundle: &CompiledInstanceBundle,
        budget: &ResourceBudget,
    ) -> Result<VmHandle, RuntimeError> {
        let fp = budget.fingerprint();
        let spawn = |pool: &VmPool| -> Result<(PooledVm, VmHandle), RuntimeError> {
            let t0 = Instant::now();
            let lua = build_sandboxed_vm(bundle, budget)?;
            let startup = t0.elapsed();
            pool.counters.vm_starts.fetch_add(1, Ordering::Relaxed);
            let vm_arc = Arc::new(tokio::sync::Mutex::new(lua));
            let in_flight = Arc::new(AtomicUsize::new(0));
            let uid = format!("vm_{}", Uuid::now_v7().simple());
            Ok((
                PooledVm {
                    vm_uid: uid.clone(),
                    definition_digest: bundle.manifest.identity_version.clone(),
                    config_digest: bundle.config_digest.clone(),
                    budget_fingerprint: fp.clone(),
                    idle_ttl: budget.idle_ttl,
                    last_used: Instant::now(),
                    in_flight: Arc::clone(&in_flight),
                    lua: Arc::clone(&vm_arc),
                },
                VmHandle {
                    vm_uid: uid,
                    lua: vm_arc,
                    in_flight,
                    started: true,
                    startup,
                },
            ))
        };

        match self.vms.entry(bundle.instance_id.clone()) {
            Entry::Occupied(mut o) => {
                let v = o.get_mut();
                if v.definition_digest == bundle.manifest.identity_version
                    && v.config_digest == bundle.config_digest
                    && v.budget_fingerprint == fp
                {
                    v.last_used = Instant::now();
                    return Ok(VmHandle {
                        vm_uid: v.vm_uid.clone(),
                        lua: Arc::clone(&v.lua),
                        in_flight: Arc::clone(&v.in_flight),
                        started: false,
                        startup: Duration::ZERO,
                    });
                }
                let (pooled, handle) = spawn(self)?;
                o.insert(pooled);
                Ok(handle)
            }
            Entry::Vacant(vac) => {
                let (pooled, handle) = spawn(self)?;
                vac.insert(pooled);
                Ok(handle)
            }
        }
    }

    /// Execute a handler function (`module[handler_name](ctx)`) inside the Instance's sandboxed VM
    /// on a Tokio blocking pool thread so CPU-bound/looping Luau NEVER blocks the async event loop.
    pub async fn execute_handler(
        &self,
        bundle: &CompiledInstanceBundle,
        handler_name: &str,
        ctx_cfg: ExecutionContextConfig,
        cancel_flag: Option<Arc<AtomicBool>>,
    ) -> Result<serde_json::Value, RuntimeError> {
        self.execute_handler_with_stats(bundle, handler_name, ctx_cfg, cancel_flag)
            .await
            .0
    }

    /// Like [`VmPool::execute_handler`], also returning the invocation's `ExecutionStats`.
    pub async fn execute_handler_with_stats(
        &self,
        bundle: &CompiledInstanceBundle,
        handler_name: &str,
        ctx_cfg: ExecutionContextConfig,
        cancel_flag: Option<Arc<AtomicBool>>,
    ) -> (Result<serde_json::Value, RuntimeError>, ExecutionStats) {
        let mut stats = ExecutionStats {
            instance_id: bundle.instance_id.clone(),
            definition_id: bundle.manifest.id.clone(),
            handler: handler_name.to_string(),
            trace_id: ctx_cfg.trace_id.clone(),
            vm_uid: String::new(),
            vm_started: false,
            vm_startup_ms: 0,
            queue_ms: 0,
            exec_ms: 0,
            interrupts: 0,
            used_memory_bytes: 0,
            memory_limit_bytes: 0,
            outcome: "ok".into(),
        };
        self.counters.executions.fetch_add(1, Ordering::Relaxed);
        let res = self
            .execute_inner(bundle, handler_name, ctx_cfg, cancel_flag, &mut stats)
            .await;
        stats.outcome = match &res {
            Ok(_) => "ok",
            Err(RuntimeError::Timeout(_)) => "timeout",
            Err(RuntimeError::Cancelled) => "cancelled",
            Err(RuntimeError::MemoryExceeded(_)) => "memory_exceeded",
            Err(RuntimeError::QueueFull(_)) => "queue_full",
            Err(_) => "error",
        }
        .to_string();
        match &res {
            Err(RuntimeError::Timeout(_)) => {
                self.counters.timeouts.fetch_add(1, Ordering::Relaxed);
            }
            Err(RuntimeError::MemoryExceeded(_)) => {
                self.counters
                    .memory_exceeded
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(RuntimeError::QueueFull(_)) => {
                self.counters
                    .queue_rejections
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        let observer = self.observer.read().unwrap().clone();
        if let Some(obs) = observer {
            obs(&stats);
        }
        (res, stats)
    }

    async fn execute_inner(
        &self,
        bundle: &CompiledInstanceBundle,
        handler_name: &str,
        ctx_cfg: ExecutionContextConfig,
        cancel_flag: Option<Arc<AtomicBool>>,
        stats: &mut ExecutionStats,
    ) -> Result<serde_json::Value, RuntimeError> {
        let inherited_deadline = current_execution_deadline();
        let budget = self.effective_budget(&bundle.instance_id);
        stats.memory_limit_bytes = budget.memory_limit_bytes;
        let queued_at = Instant::now();

        let vm = self.get_or_spawn_vm(bundle, &budget)?;
        stats.vm_uid = vm.vm_uid.clone();
        stats.vm_started = vm.started;
        stats.vm_startup_ms = vm.startup.as_millis() as u64;

        // Bounded per-Instance queue: one running + `max_pending_per_instance` waiting.
        let prev = vm.in_flight.fetch_add(1, Ordering::AcqRel);
        let _in_flight = InFlightGuard(Arc::clone(&vm.in_flight));
        if prev > budget.max_pending_per_instance {
            return Err(RuntimeError::QueueFull(format!(
                "instance '{}' already has {prev} queued/running executions (limit {})",
                bundle.instance_id,
                budget.max_pending_per_instance + 1
            )));
        }

        let mut queue_deadline = queued_at + budget.queue_timeout;
        if let Some(d) = inherited_deadline {
            queue_deadline = queue_deadline.min(d);
        }
        let queue_deadline_tokio = tokio::time::Instant::from_std(queue_deadline);

        // Pool-wide admission for top-level executions. Nested executions ride on their caller's
        // permit, so a parent waiting on a child can never starve the child of a permit.
        let _permit = if inherited_deadline.is_none() {
            match tokio::time::timeout_at(
                queue_deadline_tokio,
                Arc::clone(&self.permits).acquire_owned(),
            )
            .await
            {
                Ok(Ok(p)) => Some(p),
                Ok(Err(_)) => {
                    return Err(RuntimeError::QueueFull("execution semaphore closed".into()))
                }
                Err(_) => {
                    return Err(RuntimeError::QueueFull(format!(
                        "no execution slot within {:?} ({} concurrent executions)",
                        budget.queue_timeout, budget.max_concurrent_executions
                    )))
                }
            }
        } else {
            None
        };

        // Wait for the Instance VM asynchronously: no blocking-pool thread is held while queued.
        let guard =
            match tokio::time::timeout_at(queue_deadline_tokio, Arc::clone(&vm.lua).lock_owned())
                .await
            {
                Ok(g) => g,
                Err(_) => {
                    if inherited_deadline.is_some_and(|d| Instant::now() >= d) {
                        return Err(RuntimeError::Timeout(budget.wall_timeout));
                    }
                    return Err(RuntimeError::QueueFull(format!(
                        "instance '{}' VM busy for longer than {:?}",
                        bundle.instance_id, budget.queue_timeout
                    )));
                }
            };
        stats.queue_ms = queued_at.elapsed().as_millis() as u64;

        let started = Instant::now();
        let mut deadline = started + budget.wall_timeout;
        if let Some(d) = inherited_deadline {
            deadline = deadline.min(d);
        }
        let reported_timeout = budget.wall_timeout;
        let max_interrupts = budget.max_interrupts;
        let memory_limit = budget.memory_limit_bytes;
        let handler = handler_name.to_string();
        let def_id = bundle.manifest.id.clone();
        let runtime_source = bundle.runtime_source.clone();
        let emit_decls = bundle.manifest.emits.clone();
        let handle = tokio::runtime::Handle::current();
        let ticks = Arc::new(AtomicU64::new(0));
        let ticks_out = Arc::clone(&ticks);
        let used_memory = Arc::new(AtomicUsize::new(0));
        let used_memory_out = Arc::clone(&used_memory);

        let res = tokio::task::spawn_blocking(move || {
            handle.block_on(async move {
                let lua = guard;
                let ticks_ref = Arc::clone(&ticks);
                let timed_out = Arc::new(AtomicBool::new(false));
                let timed_out_ref = Arc::clone(&timed_out);
                let cancelled = cancel_flag.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
                let cancelled_ref = Arc::clone(&cancelled);

                lua.set_interrupt(move |_| {
                    if cancelled_ref.load(Ordering::Relaxed) {
                        return Err(mlua::Error::runtime("DASH_CANCELLED"));
                    }
                    let count = ticks_ref.fetch_add(1, Ordering::Relaxed);
                    if Instant::now() > deadline || count > max_interrupts {
                        timed_out_ref.store(true, Ordering::Relaxed);
                        return Err(mlua::Error::runtime("DASH_TIMEOUT"));
                    }
                    Ok(VmState::Continue)
                });

                let outcome = run_module_handler(
                    &lua,
                    &runtime_source,
                    &def_id,
                    &handler,
                    ctx_cfg,
                    emit_decls,
                    deadline,
                    &cancelled,
                )
                .await;

                // Clear interrupt after execution
                lua.set_interrupt(|_| Ok(VmState::Continue));
                used_memory.store(lua.used_memory(), Ordering::Relaxed);

                match outcome {
                    HandlerOutcome::Value(val) => {
                        let json_val: serde_json::Value = lua.from_value(val)?;
                        Ok(json_val)
                    }
                    HandlerOutcome::DeadlineElapsed => Err(RuntimeError::Timeout(reported_timeout)),
                    HandlerOutcome::Cancelled => Err(RuntimeError::Cancelled),
                    HandlerOutcome::Failed(err) => Err(err),
                    HandlerOutcome::Lua(e) => {
                        if timed_out.load(Ordering::Relaxed)
                            || e.to_string().contains("DASH_TIMEOUT")
                        {
                            Err(RuntimeError::Timeout(reported_timeout))
                        } else if e.to_string().contains("DASH_CANCELLED") {
                            Err(RuntimeError::Cancelled)
                        } else if is_memory_error(&e) {
                            Err(RuntimeError::MemoryExceeded(memory_limit))
                        } else {
                            Err(RuntimeError::Luau(e))
                        }
                    }
                }
            })
        })
        .await;
        stats.exec_ms = started.elapsed().as_millis() as u64;
        stats.interrupts = ticks_out.load(Ordering::Relaxed);
        stats.used_memory_bytes = used_memory_out.load(Ordering::Relaxed);
        let res = match res {
            Ok(r) => r,
            Err(join) => Err(RuntimeError::Join(join)),
        };

        // Evict the VM that failed (and only that VM) so clean state is guaranteed next time. A
        // timed-out/cancelled coroutine may have been abandoned mid-execution.
        if matches!(
            res,
            Err(RuntimeError::Timeout(_))
                | Err(RuntimeError::MemoryExceeded(_))
                | Err(RuntimeError::Cancelled)
                | Err(RuntimeError::Join(_))
        ) {
            self.evict_vm(&bundle.instance_id, &vm.vm_uid);
        } else if let Some(mut entry) = self.vms.get_mut(&bundle.instance_id) {
            if entry.vm_uid == vm.vm_uid {
                entry.last_used = Instant::now();
            }
        }

        res
    }
}

enum HandlerOutcome {
    Value(Value),
    Lua(mlua::Error),
    Failed(RuntimeError),
    DeadlineElapsed,
    Cancelled,
}

#[allow(clippy::too_many_arguments)]
async fn run_module_handler(
    lua: &Lua,
    runtime_source: &str,
    def_id: &str,
    handler: &str,
    ctx_cfg: ExecutionContextConfig,
    emit_decls: Vec<crate::manifest::EventDeclSpec>,
    deadline: Instant,
    cancelled: &Arc<AtomicBool>,
) -> HandlerOutcome {
    // Load or retrieve cached module table
    let mod_val: Value = match lua.globals().get::<Value>("__dash_module") {
        Ok(Value::Table(t)) => Value::Table(t),
        Ok(_) => {
            let evaluated: Value = match lua
                .load(runtime_source)
                .set_name(format!("{def_id}/runtime.luau"))
                .eval()
            {
                Ok(v) => v,
                Err(e) => return HandlerOutcome::Lua(e),
            };
            if let Err(e) = lua.globals().set("__dash_module", evaluated.clone()) {
                return HandlerOutcome::Lua(e);
            }
            evaluated
        }
        Err(e) => return HandlerOutcome::Lua(e),
    };

    let mod_tbl = match mod_val {
        Value::Table(t) => t,
        _ => {
            return HandlerOutcome::Failed(RuntimeError::Luau(mlua::Error::runtime(
                "runtime.luau must return a module table",
            )))
        }
    };

    let func: Function = match mod_tbl.get(handler) {
        Ok(f) => f,
        Err(_) => {
            return HandlerOutcome::Failed(RuntimeError::HandlerNotFound(
                def_id.to_string(),
                handler.to_string(),
            ))
        }
    };

    let ctx_ud = match lua.create_userdata(LuaContext::with_emit_declarations(ctx_cfg, emit_decls))
    {
        Ok(u) => u,
        Err(e) => return HandlerOutcome::Lua(e),
    };

    // The deadline bounds the whole invocation, including time suspended on host awaits; nested
    // executions issued by host operations inherit it via the task-local.
    let call = EXECUTION_DEADLINE.scope(deadline, func.call_async::<Value>(ctx_ud));
    let cancel_watch = async {
        loop {
            if cancelled.load(Ordering::Relaxed) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::select! {
        r = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), call) => match r {
            Ok(Ok(v)) => HandlerOutcome::Value(v),
            Ok(Err(e)) => HandlerOutcome::Lua(e),
            Err(_) => HandlerOutcome::DeadlineElapsed,
        },
        _ = cancel_watch => HandlerOutcome::Cancelled,
    }
}

fn is_memory_error(e: &mlua::Error) -> bool {
    match e {
        mlua::Error::MemoryError(_) => true,
        mlua::Error::CallbackError { cause, .. } => is_memory_error(cause),
        mlua::Error::WithContext { cause, .. } => is_memory_error(cause),
        _ => false,
    }
}

/// Create a bare sandbox-ready Luau state with the ADR-0003 stdlib subset and a memory limit.
/// Shared by Instance VMs and the Definition test runner so the two sandboxes cannot drift.
pub(crate) fn new_sandbox_lua(memory_limit_bytes: usize) -> Result<Lua, mlua::Error> {
    // Exclude OS, IO, DEBUG, PACKAGE, JIT, FFI; expose only safe deterministic + coroutine stdlibs
    let libs = StdLib::COROUTINE
        | StdLib::TABLE
        | StdLib::STRING
        | StdLib::UTF8
        | StdLib::BIT
        | StdLib::MATH
        | StdLib::BUFFER
        | StdLib::VECTOR;
    let lua = Lua::new_with(libs, LuaOptions::default())?;
    lua.set_memory_limit(memory_limit_bytes)?;
    Ok(lua)
}

/// Controlled module loader behind `require(...)` (§3.2 no unrestricted global module loading,
/// §4.4 library import restricted to resolved packages).
///
/// Rules:
/// - `require("dash")` returns the runtime SDK.
/// - Pre-registered modules (e.g. `runtime` inside Definition tests) are always available.
/// - Any other name must be a resolved package library. Definition code may only require
///   libraries listed in its manifest `libraries`; while a library module is being loaded, it
///   may require further resolved libraries (its own dependencies). A library that requires
///   lazily (inside a function called later from Definition code) is checked against the
///   Definition's declared list, i.e. the check fails closed.
pub(crate) struct ModuleLoader {
    libs: BTreeMap<String, String>,
    declared: BTreeSet<String>,
    loaded: std::sync::Mutex<HashMap<String, mlua::RegistryKey>>,
    loading: std::sync::Mutex<Vec<String>>,
}

impl ModuleLoader {
    pub(crate) fn new(libs: BTreeMap<String, String>, declared: &[String]) -> Arc<Self> {
        Arc::new(Self {
            libs,
            declared: declared.iter().cloned().collect(),
            loaded: std::sync::Mutex::new(HashMap::new()),
            loading: std::sync::Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn preload(&self, name: &str, key: mlua::RegistryKey) {
        self.loaded
            .lock()
            .unwrap()
            .insert(format!("preload:{name}"), key);
    }

    pub(crate) fn install(self: &Arc<Self>, lua: &Lua, dash_sdk: Table) -> mlua::Result<()> {
        let loader = Arc::clone(self);
        let require_fn = lua.create_function(move |lua, name: String| -> mlua::Result<Value> {
            if name == "dash" {
                return Ok(Value::Table(dash_sdk.clone()));
            }
            if let Some(key) = loader.loaded.lock().unwrap().get(&format!("preload:{name}")) {
                return lua.registry_value(key);
            }
            let Some(src) = loader.libs.get(&name) else {
                return Err(mlua::Error::runtime(format!(
                    "SANDBOX_DENIED: module '{name}' is not a resolved package library"
                )));
            };
            let inside_library_load = !loader.loading.lock().unwrap().is_empty();
            if !inside_library_load && !loader.declared.contains(&name) {
                return Err(mlua::Error::runtime(format!(
                    "SANDBOX_DENIED: library '{name}' is not declared in this Definition's `libraries`"
                )));
            }
            if let Some(key) = loader.loaded.lock().unwrap().get(&name) {
                return lua.registry_value(key);
            }
            {
                let mut loading = loader.loading.lock().unwrap();
                if loading.contains(&name) {
                    return Err(mlua::Error::runtime(format!(
                        "SANDBOX_DENIED: cyclic library require of '{name}'"
                    )));
                }
                loading.push(name.clone());
            }
            let src = src.clone();
            let evaluated: mlua::Result<Value> =
                lua.load(&src).set_name(format!("lib/{name}.luau")).eval();
            loader.loading.lock().unwrap().retain(|n| n != &name);
            let evaluated = evaluated?;
            let reg_key = lua.create_registry_value(evaluated.clone())?;
            loader.loaded.lock().unwrap().insert(name, reg_key);
            Ok(evaluated)
        })?;
        lua.globals().set("require", require_fn)
    }
}

fn build_sandboxed_vm(
    bundle: &CompiledInstanceBundle,
    budget: &ResourceBudget,
) -> Result<Lua, RuntimeError> {
    let lua = new_sandbox_lua(budget.memory_limit_bytes)?;
    let dash_sdk = build_dash_runtime_sdk(&lua)?;
    let loader = ModuleLoader::new(
        bundle.resolved_libraries.clone(),
        &bundle.manifest.libraries,
    );
    loader.install(&lua, dash_sdk)?;
    lua.sandbox(true)?;
    Ok(lua)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::declaration::evaluate_declaration;
    use crate::host_api::{HostBridge, HttpHostRequest, HttpHostResponse};
    use crate::manifest::DefinitionCategory;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Mutex;

    /// Bridge whose inputs sleep, recording deadlines seen and Events emitted.
    #[derive(Default)]
    pub(crate) struct ProbeBridge {
        pub sleep: Duration,
        pub seen_deadlines: Mutex<Vec<Option<Instant>>>,
        pub emitted: Mutex<Vec<(String, String, bool, serde_json::Value)>>,
        pub state: Mutex<HashMap<String, serde_json::Value>>,
    }

    #[async_trait]
    impl HostBridge for ProbeBridge {
        async fn query_input(
            &self,
            _n: &str,
            _p: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            self.seen_deadlines
                .lock()
                .unwrap()
                .push(current_execution_deadline());
            tokio::time::sleep(self.sleep).await;
            Ok(json!([1, 2, 3]))
        }
        async fn call_input(
            &self,
            _n: &str,
            _o: &str,
            _a: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            tokio::time::sleep(self.sleep).await;
            Ok(json!({ "ok": true }))
        }
        async fn invoke_action(
            &self,
            _n: &str,
            _a: serde_json::Value,
            _k: Option<String>,
        ) -> Result<serde_json::Value, String> {
            Ok(json!({}))
        }
        async fn http_request(&self, _r: HttpHostRequest) -> Result<HttpHostResponse, String> {
            Err("blocked".into())
        }
        async fn emit_event(&self, t: &str, p: serde_json::Value, d: bool) -> Result<(), String> {
            self.emitted
                .lock()
                .unwrap()
                .push((t.into(), "?".into(), d, p));
            Ok(())
        }
        async fn emit_event_versioned(
            &self,
            t: &str,
            v: &str,
            p: serde_json::Value,
            d: bool,
        ) -> Result<(), String> {
            self.emitted
                .lock()
                .unwrap()
                .push((t.into(), v.into(), d, p));
            Ok(())
        }
        async fn state_get(&self, _s: &str, k: &str) -> Result<serde_json::Value, String> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .get(k)
                .cloned()
                .unwrap_or(serde_json::Value::Null))
        }
        async fn state_set(&self, _s: &str, k: &str, v: serde_json::Value) -> Result<(), String> {
            self.state.lock().unwrap().insert(k.into(), v);
            Ok(())
        }
        async fn load_fixture(&self, _p: &str) -> Result<serde_json::Value, String> {
            Err("none".into())
        }
    }

    pub(crate) fn bundle(id: &str, decl: &str, runtime: &str) -> CompiledInstanceBundle {
        let mut sources = BTreeMap::new();
        sources.insert("definition.luau".into(), decl.as_bytes().to_vec());
        sources.insert("runtime.luau".into(), runtime.as_bytes().to_vec());
        CompiledInstanceBundle {
            instance_id: id.into(),
            manifest: evaluate_declaration(decl, &sources).unwrap(),
            runtime_source: runtime.into(),
            config_digest: "cfg".into(),
            resolved_libraries: BTreeMap::new(),
        }
    }

    pub(crate) fn cfg(
        category: DefinitionCategory,
        id: &str,
        inputs: &[&str],
        bridge: Arc<dyn HostBridge>,
    ) -> ExecutionContextConfig {
        ExecutionContextConfig {
            category,
            instance_id: id.into(),
            input_names: inputs.iter().map(|s| s.to_string()).collect(),
            config: json!({}),
            payload: json!({}),
            operation: None,
            idempotency_key: None,
            allowed_emits: vec![],
            now_iso: "2026-11-24T12:00:00Z".into(),
            trace_id: "tr".into(),
            bridge,
        }
    }

    const DP_DECL: &str = r#"
local dash = require("dash")
return dash.data_provider { id = "t.p", inputs = { up = dash.data_ref("t.up") } }
"#;
    const AWAIT_RT: &str = r#"
local R = {}
function R.query(ctx) return ctx:await(ctx.inputs.up:query({})) end
return R
"#;

    fn budget(timeout_ms: u64) -> ResourceBudget {
        ResourceBudget {
            wall_timeout: Duration::from_millis(timeout_ms),
            ..ResourceBudget::default()
        }
    }

    fn probe(sleep_ms: u64) -> Arc<ProbeBridge> {
        Arc::new(ProbeBridge {
            sleep: Duration::from_millis(sleep_ms),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn deadline_covers_host_awaits_and_evicts_vm() {
        let pool = VmPool::new(budget(150));
        let bridge = probe(5_000);
        let b = bundle("slow", DP_DECL, AWAIT_RT);
        let t0 = Instant::now();
        let res = pool
            .execute_handler(
                &b,
                "query",
                cfg(
                    DefinitionCategory::DataProvider,
                    "slow",
                    &["up"],
                    bridge.clone(),
                ),
                None,
            )
            .await;
        assert!(matches!(res, Err(RuntimeError::Timeout(_))), "{res:?}");
        assert!(
            t0.elapsed() < Duration::from_secs(2),
            "deadline not enforced during await"
        );
        assert!(
            pool.vm_uid("slow").is_none(),
            "timed-out VM must be evicted"
        );
        // The host operation saw the invocation's deadline (nested executions inherit it).
        let seen = bridge.seen_deadlines.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        let d = seen[0].expect("deadline visible to host operations");
        assert!(d <= t0 + Duration::from_millis(400));
        assert_eq!(pool.metrics().total_timeouts, 1);
    }

    #[tokio::test]
    async fn cancellation_is_observed_while_awaiting() {
        let pool = VmPool::new(budget(5_000));
        let b = bundle("cancel_me", DP_DECL, AWAIT_RT);
        let flag = Arc::new(AtomicBool::new(false));
        let f2 = Arc::clone(&flag);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            f2.store(true, Ordering::Relaxed);
        });
        let t0 = Instant::now();
        let res = pool
            .execute_handler(
                &b,
                "query",
                cfg(
                    DefinitionCategory::DataProvider,
                    "cancel_me",
                    &["up"],
                    probe(5_000),
                ),
                Some(flag),
            )
            .await;
        assert!(matches!(res, Err(RuntimeError::Cancelled)), "{res:?}");
        assert!(t0.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn per_instance_queue_is_bounded() {
        let pool = VmPool::new(ResourceBudget {
            max_pending_per_instance: 0,
            ..budget(2_000)
        });
        let bridge = probe(300);
        let b = bundle("busy", DP_DECL, AWAIT_RT);
        let (p2, b2, br2) = (pool.clone(), b.clone(), bridge.clone());
        let first = tokio::spawn(async move {
            p2.execute_handler(
                &b2,
                "query",
                cfg(DefinitionCategory::DataProvider, "busy", &["up"], br2),
                None,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let second = pool
            .execute_handler(
                &b,
                "query",
                cfg(DefinitionCategory::DataProvider, "busy", &["up"], bridge),
                None,
            )
            .await;
        assert!(
            matches!(second, Err(RuntimeError::QueueFull(_))),
            "{second:?}"
        );
        assert!(first.await.unwrap().is_ok());
        assert_eq!(pool.metrics().total_queue_rejections, 1);
    }

    #[tokio::test]
    async fn global_concurrency_limit_rejects_after_queue_timeout() {
        let pool = VmPool::new(ResourceBudget {
            max_concurrent_executions: 1,
            queue_timeout: Duration::from_millis(80),
            ..budget(2_000)
        });
        let bridge = probe(400);
        let a = bundle("inst_a", DP_DECL, AWAIT_RT);
        let b = bundle("inst_b", DP_DECL, AWAIT_RT);
        let (p2, br2) = (pool.clone(), bridge.clone());
        let first = tokio::spawn(async move {
            p2.execute_handler(
                &a,
                "query",
                cfg(DefinitionCategory::DataProvider, "inst_a", &["up"], br2),
                None,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let second = pool
            .execute_handler(
                &b,
                "query",
                cfg(
                    DefinitionCategory::DataProvider,
                    "inst_b",
                    &["up"],
                    bridge.clone(),
                ),
                None,
            )
            .await;
        assert!(
            matches!(second, Err(RuntimeError::QueueFull(_))),
            "{second:?}"
        );
        assert!(first.await.unwrap().is_ok());
        // The slot is free again once the first execution finished.
        let third = pool
            .execute_handler(
                &b,
                "query",
                cfg(DefinitionCategory::DataProvider, "inst_b", &["up"], bridge),
                None,
            )
            .await;
        assert!(third.is_ok(), "{third:?}");
    }

    #[tokio::test]
    async fn concurrent_first_requests_share_one_vm_and_reclaim_skips_busy_vms() {
        let pool = VmPool::new(budget(3_000));
        let bridge = probe(150);
        let b = bundle("shared", DP_DECL, AWAIT_RT);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let (p, b, br) = (pool.clone(), b.clone(), bridge.clone());
            handles.push(tokio::spawn(async move {
                p.execute_handler(
                    &b,
                    "query",
                    cfg(DefinitionCategory::DataProvider, "shared", &["up"], br),
                    None,
                )
                .await
            }));
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
        // While executions are queued/running the VM must never be reclaimed.
        assert_eq!(pool.reclaim_idle(Some(Duration::ZERO)), 0);
        for h in handles {
            assert!(h.await.unwrap().is_ok());
        }
        assert_eq!(
            pool.metrics().total_vm_starts,
            1,
            "exactly one VM for one Instance"
        );
        assert_eq!(pool.reclaim_idle(Some(Duration::ZERO)), 1);
    }

    #[tokio::test]
    async fn eviction_only_removes_the_failed_vm() {
        let pool = VmPool::new(budget(1_000));
        let b = bundle("evict_me", DP_DECL, AWAIT_RT);
        pool.execute_handler(
            &b,
            "query",
            cfg(
                DefinitionCategory::DataProvider,
                "evict_me",
                &["up"],
                probe(0),
            ),
            None,
        )
        .await
        .unwrap();
        let uid = pool.vm_uid("evict_me").unwrap();
        pool.evict_vm("evict_me", "vm_someone_else");
        assert_eq!(pool.vm_uid("evict_me").as_deref(), Some(uid.as_str()));
        pool.evict_vm("evict_me", &uid);
        assert!(pool.vm_uid("evict_me").is_none());
    }

    #[tokio::test]
    async fn per_instance_overrides_apply_and_are_clamped_to_ceiling() {
        let ceiling = ResourceBudget {
            wall_timeout: Duration::from_millis(400),
            ..ResourceBudget::default_ceiling()
        };
        let pool = VmPool::with_ceiling(budget(3_000), ceiling);
        // The default budget is clamped to the ceiling as well.
        assert_eq!(
            pool.effective_budget("anything").wall_timeout,
            Duration::from_millis(400)
        );
        pool.set_budget_resolver(Arc::new(|id: &str| match id {
            "tight" => Some(ResourceOverrides {
                timeout_ms: Some(60),
                ..Default::default()
            }),
            "greedy" => Some(ResourceOverrides {
                timeout_ms: Some(3_600_000),
                memory_limit_mib: Some(1 << 20),
                ..Default::default()
            }),
            _ => None,
        }));
        assert_eq!(
            pool.effective_budget("tight").wall_timeout,
            Duration::from_millis(60)
        );
        let greedy = pool.effective_budget("greedy");
        assert_eq!(greedy.wall_timeout, Duration::from_millis(400));
        assert_eq!(
            greedy.memory_limit_bytes,
            ResourceBudget::default_ceiling().memory_limit_bytes
        );

        let bridge = probe(200);
        let tight = bundle("tight", DP_DECL, AWAIT_RT);
        let res = pool
            .execute_handler(
                &tight,
                "query",
                cfg(
                    DefinitionCategory::DataProvider,
                    "tight",
                    &["up"],
                    bridge.clone(),
                ),
                None,
            )
            .await;
        assert!(matches!(res, Err(RuntimeError::Timeout(_))), "{res:?}");
        let normal = bundle("normal", DP_DECL, AWAIT_RT);
        let res = pool
            .execute_handler(
                &normal,
                "query",
                cfg(DefinitionCategory::DataProvider, "normal", &["up"], bridge),
                None,
            )
            .await;
        assert!(res.is_ok(), "{res:?}");
    }

    #[tokio::test]
    async fn execution_stats_are_reported_to_observer() {
        let pool = VmPool::new(budget(2_000));
        let seen: Arc<Mutex<Vec<ExecutionStats>>> = Arc::default();
        let s2 = Arc::clone(&seen);
        pool.set_execution_observer(Arc::new(move |s: &ExecutionStats| {
            s2.lock().unwrap().push(s.clone())
        }));
        let bridge = probe(20);
        let b = bundle("observed", DP_DECL, AWAIT_RT);
        for _ in 0..2 {
            pool.execute_handler(
                &b,
                "query",
                cfg(
                    DefinitionCategory::DataProvider,
                    "observed",
                    &["up"],
                    bridge.clone(),
                ),
                None,
            )
            .await
            .unwrap();
        }
        let stats = seen.lock().unwrap().clone();
        assert_eq!(stats.len(), 2);
        assert!(stats[0].vm_started && !stats[1].vm_started);
        assert_eq!(stats[0].vm_uid, stats[1].vm_uid);
        assert!(stats[0].exec_ms >= 20);
        assert!(stats[0].used_memory_bytes > 0);
        assert_eq!(stats[0].memory_limit_bytes, 32 * 1024 * 1024);
        assert_eq!(stats[0].outcome, "ok");
        assert_eq!(stats[0].trace_id, "tr");
    }

    #[tokio::test]
    async fn require_is_limited_to_declared_libraries() {
        let pool = VmPool::new(budget(2_000));
        let mut libs = BTreeMap::new();
        libs.insert(
            "pkg.a".to_string(),
            "local b = require(\"pkg.b\")\nreturn { v = function() return b.v + 1 end }"
                .to_string(),
        );
        libs.insert("pkg.b".to_string(), "return { v = 41 }".to_string());
        libs.insert("pkg.secret".to_string(), "return { v = 0 }".to_string());
        let decl = r#"
local dash = require("dash")
return dash.data_provider { id = "t.libs", libraries = { "pkg.a" } }
"#;
        let ok_rt = "local a = require(\"pkg.a\")\nlocal R = {}\nfunction R.query(ctx) return a.v() end\nreturn R";
        let mut b = bundle("libs_ok", decl, ok_rt);
        b.resolved_libraries = libs.clone();
        let v = pool
            .execute_handler(
                &b,
                "query",
                cfg(DefinitionCategory::DataProvider, "libs_ok", &[], probe(0)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            v,
            json!(42),
            "transitive library loaded by a declared library"
        );

        for (id, rt) in [
            ("libs_undeclared", "local R = {}\nfunction R.query(ctx) return require(\"pkg.secret\").v end\nreturn R"),
            ("libs_transitive_direct", "local R = {}\nfunction R.query(ctx) return require(\"pkg.b\").v end\nreturn R"),
        ] {
            let mut b = bundle(id, decl, rt);
            b.resolved_libraries = libs.clone();
            let err = pool
                .execute_handler(&b, "query", cfg(DefinitionCategory::DataProvider, id, &[], probe(0)), None)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("not declared"), "{id}: {err}");
        }
    }
}
