//! Reactive Query Engine: singleflight deduplication, partitioned caching, dynamic dependency
//! tracking and propagation, cycle isolation, stale retention, retry/backoff, circuit breakers,
//! cancellation and deadlines, keyed deltas, cached Adapter evaluation, and Preview mocks.
//!
//! Execution model (§6.1, §6.3, ADR-0001, ADR-0003):
//! - Every computation of a query identity (`query_key`) or an Adapter render runs as one detached
//!   *flight*. Callers, including the one that started the flight, only wait on its result, so a
//!   dropped caller (client disconnect, deadline) can never orphan the singleflight entry.
//! - Waiters are reference-counted. When the last waiter goes away, the flight is cancelled after
//!   a grace period, and the cancellation reaches the Luau VM through its interrupt flag.
//! - Nested `ctx:await` calls on dependency queries and connectors are bounded by the parent
//!   execution's deadline and cancellation.
//! - Dependency edges are recorded from every execution (successful or failed). When a result
//!   changes, its dynamic dependents are marked stale and recomputed if they have subscribers.

use crate::authority::{
    audited, check_connector_operation_authority, check_connector_operation_read_only,
    check_provider_access, Principal,
};
use crate::events::EventBus;
use crate::http_broker::HttpBroker;
use crate::model::{
    ConfiguredGraph, DashboardAdapterPlacement, DashboardDoc, InstanceConfig, LoadedDefinition,
};
use crate::signal::{
    compute_keyed_delta_inferred, QueryEnvelope, QueryStatus, RetryInfo, SignalHub,
};
use crate::state::StateManager;
use crate::telemetry::{ExecutionGraphEdge, ExecutionGraphNode, ExecutionGraphSnapshot, Telemetry};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dash_luau::{
    CompiledInstanceBundle, DefinitionCategory, ExecutionContextConfig, HostBridge,
    HttpHostRequest, HttpHostResponse, RuntimeError, VmPool,
};
use dash_store::SqliteStore;
use dashmap::DashMap;
use lru::LruCache;
use rand::Rng;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use uuid::Uuid;

/// Bounded LRU capacity for data-provider query results (§20: no unbounded caches).
const QUERY_CACHE_CAPACITY: usize = 2048;
/// Bounded LRU capacity for cached Adapter renders.
const RENDER_CACHE_CAPACITY: usize = 256;
/// Consecutive failures before an Instance's circuit breaker opens.
pub(crate) const BREAKER_THRESHOLD: u32 = 5;
/// First open window; doubles per further failure up to `2^BREAKER_MAX_EXP` times this.
const BREAKER_BASE_BACKOFF: Duration = Duration::from_secs(1);
const BREAKER_MAX_EXP: u32 = 4;
/// Background retry schedule for failing queries with active subscribers (§15.2).
const RETRY_BASE_DELAY: Duration = Duration::from_secs(1);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(60);
/// Relative jitter (±) applied to breaker windows and retry delays.
const BACKOFF_JITTER: f64 = 0.2;
/// Coalescing window for recomputing dependents after an upstream change.
const RECOMPUTE_DEBOUNCE: Duration = Duration::from_millis(15);
/// Grace period before a computation without remaining consumers is cancelled (§6.1).
const DEFAULT_CANCEL_GRACE: Duration = Duration::from_secs(2);
/// Query budget of the built-in `dash.history_series` provider (§7.4).
const HISTORY_DEFAULT_LIMIT: usize = 500;
const HISTORY_MAX_LIMIT: usize = 2000;
const HISTORY_BUILTIN_DIGEST: &str = "builtin:dash.history_series";

#[derive(Debug, Clone)]
pub struct CachedQueryEntry {
    pub instance_id: String,
    pub definition_digest: String,
    pub auth_fingerprint: String,
    pub params: serde_json::Value,
    pub principal: Principal,
    pub generation_id: i64,
    pub version: u64,
    pub status: QueryStatus,
    pub value: serde_json::Value,
    pub schema: Option<String>,
    /// When this entry was computed (drives the cache TTL).
    pub updated_at: DateTime<Utc>,
    /// Last successful update of the underlying data. Older than `updated_at` when the value was
    /// derived from stale upstream results (§6.1 "last successful update timestamp").
    pub source_updated_at: DateTime<Utc>,
    /// Logical start time of the computation that produced this entry; later-started
    /// computations win, so an older in-flight run never overwrites a newer value.
    pub start_tick: u64,
    /// Needs recomputation (failed, invalidated, or an upstream changed).
    pub stale: bool,
    pub last_error: Option<String>,
    pub hits: u64,
    pub misses: u64,
}

#[derive(Debug, Clone, Default)]
struct CircuitState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
    /// Half-open: one probe execution has been admitted after the open window elapsed.
    probing: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Admission {
    Closed,
    /// Half-open trial execution; closes the breaker on success, reopens it on failure.
    Probe,
    /// Rejected without executing and without counting as a failure.
    Rejected {
        until: Option<Instant>,
    },
}

/// Cooperative cancellation shared between the engine and a Luau execution (§6.3, ADR-0003).
/// The flag is what the VM interrupt hook checks; the notifier wakes async waiters.
#[derive(Clone, Default)]
pub(crate) struct CancelSignal {
    flag: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl CancelSignal {
    fn new() -> Self {
        Self::default()
    }

    fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    fn vm_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.flag)
    }

    async fn cancelled(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

/// Cancels its signal when dropped, so dropping the awaiting future stops the VM execution.
struct CancelOnDrop(CancelSignal);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Bounds a wait by an optional deadline and an optional cancellation signal.
#[derive(Clone, Default)]
struct WaitCtx {
    deadline: Option<Instant>,
    cancel: Option<CancelSignal>,
}

async fn wait_bounded<T>(fut: impl Future<Output = T>, wait: &WaitCtx) -> Result<T, String> {
    let deadline = async {
        match wait.deadline {
            Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
            None => std::future::pending::<()>().await,
        }
    };
    let cancelled = async {
        match &wait.cancel {
            Some(c) => c.cancelled().await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        biased;
        r = fut => Ok(r),
        _ = cancelled => Err("execution cancelled".to_string()),
        _ = deadline => Err("deadline exceeded while awaiting a dependency".to_string()),
    }
}

/// Result of verifying a provider delta: done, or a resync the caller performs or schedules.
enum DeltaStep {
    Done(Box<crate::signal::ProviderDeltaOutcome>),
    NeedsResync(String),
}

/// One running computation shared by all of its waiters.
struct Flight<X> {
    id: u64,
    instance_id: String,
    tx: broadcast::Sender<Result<X, String>>,
    waiters: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    cancel: CancelSignal,
}

type FlightMap<X> = Arc<DashMap<String, Flight<X>>>;

/// Reference to a flight held by one waiting caller.
struct WaiterGuard {
    waiters: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    cancel: CancelSignal,
    grace: Duration,
}

impl WaiterGuard {
    fn new<X>(flight: &Flight<X>, grace: Duration) -> Self {
        flight.waiters.fetch_add(1, Ordering::SeqCst);
        Self {
            waiters: Arc::clone(&flight.waiters),
            done: Arc::clone(&flight.done),
            cancel: flight.cancel.clone(),
            grace,
        }
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if self.waiters.fetch_sub(1, Ordering::SeqCst) != 1 || self.done.load(Ordering::SeqCst) {
            return;
        }
        // Last consumer left before the result arrived: cancel after the grace period unless
        // somebody joined again in the meantime.
        let waiters = Arc::clone(&self.waiters);
        let done = Arc::clone(&self.done);
        let cancel = self.cancel.clone();
        let grace = self.grace;
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    tokio::time::sleep(grace).await;
                    if waiters.load(Ordering::SeqCst) == 0 && !done.load(Ordering::SeqCst) {
                        cancel.cancel();
                    }
                });
            }
            Err(_) => cancel.cancel(),
        }
    }
}

/// Removes the flight's map entry when the computation ends, including on panic, so waiters
/// observe `Closed` instead of hanging and later callers start a new computation.
struct FlightCompletion<X> {
    map: FlightMap<X>,
    key: String,
    id: u64,
    done: Arc<AtomicBool>,
}

impl<X> Drop for FlightCompletion<X> {
    fn drop(&mut self) {
        self.done.store(true, Ordering::SeqCst);
        let id = self.id;
        self.map.remove_if(&self.key, |_, f| f.id == id);
    }
}

/// Everything a query flight needs, owned so the flight can run detached.
struct QueryJob {
    graph: Arc<ConfiguredGraph>,
    inst: InstanceConfig,
    def: LoadedDefinition,
    params: serde_json::Value,
    principal: Principal,
    query_key: String,
    call_stack: Vec<String>,
}

/// Status of one upstream result observed by a dependent execution through `ctx:await`.
#[derive(Debug, Clone)]
struct UpstreamObservation {
    query_key: String,
    status: QueryStatus,
    updated_at: DateTime<Utc>,
    /// Cache version of the upstream at observation time; `None` for uncached built-ins.
    cached_version: Option<u64>,
}

struct ProviderRun {
    value: serde_json::Value,
    deps: BTreeSet<String>,
    upstreams: Vec<UpstreamObservation>,
}

struct ProviderFailure {
    message: String,
    cancelled: bool,
    deps: BTreeSet<String>,
}

struct CommitOutcome {
    snapshot: QueryEnvelope,
    delta: Option<QueryEnvelope>,
    written: bool,
    changed: bool,
}

/// Result of evaluating one Adapter placement (before widget_id annotation).
#[derive(Debug, Clone)]
struct RenderOutcome {
    tree: serde_json::Value,
    status: QueryStatus,
    updated_at: DateTime<Utc>,
    version: u64,
    render_key: String,
}

#[derive(Debug, Clone)]
struct CachedRender {
    dashboard_id: String,
    adapter_instance: String,
    query_params: serde_json::Value,
    principal: Principal,
    generation_id: i64,
    outcome: RenderOutcome,
    computed_at: DateTime<Utc>,
    start_tick: u64,
    stale: bool,
    /// Dashboard State keys read during the render and the record version seen.
    state_reads: BTreeMap<String, Option<i64>>,
    upstreams: Vec<(String, Option<u64>)>,
}

struct RenderJob {
    graph: Arc<ConfiguredGraph>,
    dashboard: DashboardDoc,
    inst: InstanceConfig,
    def: LoadedDefinition,
    query_params: serde_json::Value,
    principal: Principal,
    render_key: String,
}

#[derive(Clone)]
pub struct QueryEngine {
    graph: Arc<ArcSwap<ConfiguredGraph>>,
    pool: VmPool,
    store: SqliteStore,
    http: HttpBroker,
    signals: SignalHub,
    events: EventBus,
    state: StateManager,
    telemetry: Telemetry,
    cache: Arc<Mutex<LruCache<String, CachedQueryEntry>>>,
    in_flight: FlightMap<QueryEnvelope>,
    forward_deps: Arc<DashMap<String, BTreeSet<String>>>,
    reverse_deps: Arc<DashMap<String, BTreeSet<String>>>,
    circuit_breakers: Arc<DashMap<String, CircuitState>>,
    mock_kv: Arc<DashMap<String, serde_json::Value>>,
    cache_ttl: Duration,
    cancel_grace: Duration,
    /// Logical clock ordering computation starts and invalidations.
    ticks: Arc<AtomicU64>,
    /// Engine-wide monotonic snapshot version source: versions never go backwards for a
    /// query identity, even after LRU eviction (Appendix A.2).
    versions: Arc<AtomicU64>,
    instance_invalidated: Arc<DashMap<String, u64>>,
    key_invalidated: Arc<DashMap<String, u64>>,
    retry_attempts: Arc<DashMap<String, u32>>,
    retry_pending: Arc<DashMap<String, DateTime<Utc>>>,
    recompute_pending: Arc<DashMap<String, ()>>,
    render_cache: Arc<Mutex<LruCache<String, CachedRender>>>,
    render_in_flight: FlightMap<RenderOutcome>,
    /// widget_id -> owning Adapter Instance of the latest render per dashboard and Generation.
    rendered_widgets: Arc<RenderedWidgets>,
    /// Active provider `subscribe` hooks (§6.1), one per subscribed query identity shared by all
    /// its consumers; released when the query has no subscribers left.
    provider_subscriptions: Arc<DashMap<String, CancelSignal>>,
}

/// dashboard_id -> (Generation, widget_id -> Adapter Instance).
type RenderedWidgets = DashMap<String, (i64, HashMap<String, String>)>;

impl QueryEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        graph: Arc<ArcSwap<ConfiguredGraph>>,
        pool: VmPool,
        store: SqliteStore,
        http: HttpBroker,
        signals: SignalHub,
        events: EventBus,
        state: StateManager,
        telemetry: Telemetry,
    ) -> Self {
        Self {
            graph,
            pool,
            store,
            http,
            signals,
            events,
            state,
            telemetry,
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(QUERY_CACHE_CAPACITY).unwrap(),
            ))),
            in_flight: Arc::new(DashMap::new()),
            forward_deps: Arc::new(DashMap::new()),
            reverse_deps: Arc::new(DashMap::new()),
            circuit_breakers: Arc::new(DashMap::new()),
            mock_kv: Arc::new(DashMap::new()),
            cache_ttl: Duration::from_secs(30),
            cancel_grace: DEFAULT_CANCEL_GRACE,
            ticks: Arc::new(AtomicU64::new(0)),
            versions: Arc::new(AtomicU64::new(0)),
            instance_invalidated: Arc::new(DashMap::new()),
            key_invalidated: Arc::new(DashMap::new()),
            retry_attempts: Arc::new(DashMap::new()),
            retry_pending: Arc::new(DashMap::new()),
            recompute_pending: Arc::new(DashMap::new()),
            render_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(RENDER_CACHE_CAPACITY).unwrap(),
            ))),
            render_in_flight: Arc::new(DashMap::new()),
            rendered_widgets: Arc::new(DashMap::new()),
            provider_subscriptions: Arc::new(DashMap::new()),
        }
    }

    pub fn graph(&self) -> Arc<ConfiguredGraph> {
        self.graph.load_full()
    }

    pub fn pool(&self) -> &VmPool {
        &self.pool
    }

    pub fn signals(&self) -> &SignalHub {
        &self.signals
    }

    /// Host-side Mock state access (Scenario/test hook): read a Mock Definition's state key.
    pub fn mock_state_get(&self, mock_definition_id: &str, key: &str) -> serde_json::Value {
        self.mock_kv
            .get(&format!("{mock_definition_id}:{key}"))
            .map(|v| v.clone())
            .unwrap_or(serde_json::Value::Null)
    }

    /// Host-side Mock state mutation (Scenario/test hook), e.g. simulating an external change
    /// without exposing an undeclared connector operation.
    pub fn mock_state_set(&self, mock_definition_id: &str, key: &str, value: serde_json::Value) {
        self.mock_kv
            .insert(format!("{mock_definition_id}:{key}"), value);
    }

    pub fn cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Mediated State access shared with background workers (automations write State through it
    /// so State Signals are published).
    pub fn state_manager(&self) -> &StateManager {
        &self.state
    }

    /// Snapshot of cached query entries for one Instance (used by scheduled polling, §6.2).
    pub fn cached_entries_for_instance(
        &self,
        instance_id: &str,
    ) -> Vec<(String, CachedQueryEntry)> {
        let cache = self.cache.lock().unwrap();
        cache
            .iter()
            .filter(|(_, e)| e.instance_id == instance_id)
            .map(|(k, e)| (k.clone(), e.clone()))
            .collect()
    }

    /// Peek one cached entry without promoting it in the LRU.
    pub fn cached_entry(&self, query_key: &str) -> Option<CachedQueryEntry> {
        self.cache.lock().unwrap().peek(query_key).cloned()
    }

    /// Canonical, collision-resistant query key partitioned by authorization fingerprint (§6.1, A.1).
    pub fn compute_query_key(
        &self,
        instance_id: &str,
        def_digest: &str,
        params: &serde_json::Value,
        principal: &Principal,
    ) -> String {
        let canonical_params = canonicalize_json(params);
        let mut hasher = Sha256::new();
        hasher.update(instance_id.as_bytes());
        hasher.update(b"\0");
        hasher.update(def_digest.as_bytes());
        hasher.update(b"\0");
        hasher.update(canonical_params.as_bytes());
        hasher.update(b"\0");
        hasher.update(principal.authorization_fingerprint().as_bytes());
        hex::encode(&hasher.finalize()[..16])
    }

    /// Query key of `inst` in `graph`, including the built-in history provider.
    fn query_key_for(
        &self,
        graph: &ConfiguredGraph,
        inst: &InstanceConfig,
        params: &serde_json::Value,
        principal: &Principal,
    ) -> Option<String> {
        if inst.definition == "dash.history_series" {
            return Some(self.compute_query_key(
                &inst.id,
                HISTORY_BUILTIN_DIGEST,
                params,
                principal,
            ));
        }
        graph.definitions.get(&inst.definition).map(|def| {
            self.compute_query_key(&inst.id, &def.manifest.identity_version, params, principal)
        })
    }

    /// Whether the Instance's circuit breaker currently rejects executions.
    pub fn circuit_open(&self, instance_id: &str) -> bool {
        self.breaker_open_until(instance_id).is_some()
    }

    /// Cancel every running query and Adapter computation (e.g. on daemon shutdown).
    /// Returns the number of computations signalled.
    pub fn cancel_all_in_flight(&self) -> usize {
        let mut n = 0;
        for f in self.in_flight.iter() {
            f.cancel.cancel();
            n += 1;
        }
        for f in self.render_in_flight.iter() {
            f.cancel.cancel();
            n += 1;
        }
        let subs: Vec<String> = self
            .provider_subscriptions
            .iter()
            .map(|e| e.key().clone())
            .collect();
        for k in subs {
            if let Some((_, c)) = self.provider_subscriptions.remove(&k) {
                c.cancel();
                n += 1;
            }
        }
        n
    }

    /// Resolve the `query_key` that `query(instance_id, params, principal)` uses, after the same
    /// existence, category and access checks, without executing anything. Lets consumers
    /// subscribe to a query's Signal *before* its first computation (no lost updates) and stay
    /// subscribed when that computation fails.
    pub fn resolve_query_key(
        &self,
        instance_id: &str,
        params: &serde_json::Value,
        principal: &Principal,
    ) -> Result<String, String> {
        let g = self.graph.load_full();
        let inst = g
            .instances
            .get(instance_id)
            .ok_or_else(|| format!("data provider instance '{instance_id}' not found"))?;
        if inst.category != DefinitionCategory::DataProvider
            && inst.category != DefinitionCategory::Adapter
        {
            return Err(format!(
                "instance '{instance_id}' is '{}', not queryable",
                inst.category.as_str()
            ));
        }
        check_provider_access(principal, inst).map_err(|e| e.to_string())?;
        self.query_key_for(&g, inst, params, principal)
            .ok_or_else(|| format!("definition '{}' missing", inst.definition))
    }

    /// Execute or return cached query result (with singleflight dedup and stale-on-error fallback).
    pub async fn query(
        &self,
        instance_id: &str,
        params: serde_json::Value,
        principal: &Principal,
        force_refresh: bool,
    ) -> Result<QueryEnvelope, String> {
        self.query_with_trace(
            instance_id,
            params,
            principal,
            force_refresh,
            Vec::new(),
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_with_trace<'a>(
        &'a self,
        instance_id: &'a str,
        params: serde_json::Value,
        principal: &'a Principal,
        force_refresh: bool,
        call_stack: Vec<String>,
        parent_query_key: Option<String>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<QueryEnvelope, String>> + Send + 'a>,
    > {
        Box::pin(self.query_inner(
            instance_id,
            params,
            principal,
            force_refresh,
            call_stack,
            parent_query_key,
            WaitCtx::default(),
            None,
        ))
    }

    /// `pinned` is the Generation snapshot of a calling computation: dependency queries resolve
    /// against the caller's graph, never against a Generation activated mid-computation, so no
    /// result mixes two Generations (§12.2 "no partial graph visible during activation").
    #[allow(clippy::too_many_arguments)]
    async fn query_inner(
        &self,
        instance_id: &str,
        params: serde_json::Value,
        principal: &Principal,
        force_refresh: bool,
        mut call_stack: Vec<String>,
        parent_query_key: Option<String>,
        wait: WaitCtx,
        pinned: Option<Arc<ConfiguredGraph>>,
    ) -> Result<QueryEnvelope, String> {
        let live_generation = self.graph.load().generation_id;
        let g = pinned.unwrap_or_else(|| self.graph.load_full());
        let inst = g
            .instances
            .get(instance_id)
            .ok_or_else(|| format!("data provider instance '{instance_id}' not found"))?
            .clone();

        if inst.category != DefinitionCategory::DataProvider
            && inst.category != DefinitionCategory::Adapter
        {
            return Err(format!(
                "instance '{instance_id}' is '{}', not queryable",
                inst.category.as_str()
            ));
        }

        audited(
            &self.store,
            principal,
            "provider.query",
            check_provider_access(principal, &inst),
        )
        .map_err(|e| e.to_string())?;

        // Built-in history_series provider (§7.4)
        if inst.definition == "dash.history_series" {
            return self.query_history_series(&g, &inst, &params, principal);
        }

        let def = g
            .definitions
            .get(&inst.definition)
            .ok_or_else(|| format!("definition '{}' missing", inst.definition))?
            .clone();

        // Validate query parameter schema at boundary
        if let Some(ref q_schema) = def.manifest.query {
            q_schema
                .validate_value(&params)
                .map_err(|e| format!("invalid query parameters for '{instance_id}': {e}"))?;
        }

        let query_key = self.compute_query_key(
            instance_id,
            &def.manifest.identity_version,
            &params,
            principal,
        );

        // Dynamic cycle detection (§6.1) — isolate failure to affected query, with a diagnostic trace
        if call_stack.contains(&query_key) {
            return Err(self.report_cycle(
                instance_id,
                &query_key,
                &call_stack,
                parent_query_key.as_deref(),
                principal,
            ));
        }
        call_stack.push(query_key.clone());

        // A subscribed query of a provider with a `subscribe` hook starts that hook once for all
        // of its consumers (§6.1 "identical subscribed queries share underlying subscriptions").
        if def.manifest.updates.subscribe && self.signals.subscriber_count(&query_key) > 0 {
            self.ensure_provider_subscription(&g, &inst, &def, &params, principal, &query_key);
        }

        // Fast cache hit path (bounded LRU + TTL + generation match)
        if !force_refresh {
            if let Some(hit) = self.cache_hit(instance_id, &query_key, g.generation_id) {
                return Ok(hit);
            }
        }

        // Singleflight: join the running computation unless it started before the latest
        // invalidation of this Instance/query (then it may carry pre-mutation data).
        let not_before = self.invalidation_tick(instance_id, &query_key);
        let job = Arc::new(QueryJob {
            graph: Arc::clone(&g),
            inst,
            def,
            params,
            principal: principal.clone(),
            query_key: query_key.clone(),
            call_stack,
        });
        // A computation pinned to a superseded Generation never shares a flight with the live
        // Generation (an unchanged Definition keeps its query key across Generations).
        let flight_key = if g.generation_id == live_generation {
            query_key.clone()
        } else {
            format!("{query_key}@generation:{}", g.generation_id)
        };
        let engine = self.clone();
        self.join_or_lead(
            &self.in_flight,
            &flight_key,
            instance_id,
            not_before,
            &wait,
            move |tick, cancel| {
                let engine = engine.clone();
                let job = Arc::clone(&job);
                // A dependency query issued from inside a running execution stays nested
                // although its flight runs detached: it rides on the caller's pool permit (no
                // permit starvation). The flight is shared, though, so it runs under the
                // provider's own wall timeout, not the creating caller's remaining deadline: a
                // nested caller with little time left must not time out the top-level queries
                // and subscription recomputes that join the flight, nor charge its deadline to
                // the provider's circuit breaker. Each waiter stays bounded by its own WaitCtx.
                let own_deadline = dash_luau::current_execution_deadline().map(|_| {
                    Instant::now() + engine.pool.effective_budget(&job.inst.id).wall_timeout
                });
                async move {
                    let leader = Box::pin(engine.run_query_leader(job, tick, cancel));
                    match own_deadline {
                        Some(deadline) => {
                            dash_luau::with_execution_deadline(deadline, leader).await
                        }
                        None => leader.await,
                    }
                }
            },
        )
        .await
    }

    /// Join the flight for `key` (if it started after `not_before`) or start a new detached one,
    /// then wait for its result within `wait`'s deadline/cancellation.
    async fn join_or_lead<X, F, Fut>(
        &self,
        map: &FlightMap<X>,
        key: &str,
        instance_id: &str,
        not_before: u64,
        wait: &WaitCtx,
        mut lead: F,
    ) -> Result<X, String>
    where
        X: Clone + Send + 'static,
        F: FnMut(u64, CancelSignal) -> Fut,
        Fut: Future<Output = Result<X, String>> + Send + 'static,
    {
        for _ in 0..3 {
            let mut to_spawn = None;
            let (mut rx, guard) = match map.entry(key.to_string()) {
                dashmap::mapref::entry::Entry::Occupied(occ) if occ.get().id > not_before => {
                    let f = occ.get();
                    (f.tx.subscribe(), WaiterGuard::new(f, self.cancel_grace))
                }
                entry => {
                    let id = self.next_tick();
                    let (tx, rx) = broadcast::channel(1);
                    let flight = Flight {
                        id,
                        instance_id: instance_id.to_string(),
                        tx: tx.clone(),
                        waiters: Arc::new(AtomicUsize::new(0)),
                        done: Arc::new(AtomicBool::new(false)),
                        cancel: CancelSignal::new(),
                    };
                    let guard = WaiterGuard::new(&flight, self.cancel_grace);
                    let fut = lead(id, flight.cancel.clone());
                    let completion = FlightCompletion {
                        map: Arc::clone(map),
                        key: key.to_string(),
                        id,
                        done: Arc::clone(&flight.done),
                    };
                    match entry {
                        dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                            occ.insert(flight);
                        }
                        dashmap::mapref::entry::Entry::Vacant(vac) => {
                            vac.insert(flight);
                        }
                    }
                    to_spawn = Some((fut, completion, tx));
                    (rx, guard)
                }
            };
            if let Some((fut, completion, tx)) = to_spawn {
                tokio::spawn(async move {
                    let res = fut.await;
                    drop(completion);
                    let _ = tx.send(res);
                });
            }
            let outcome = wait_bounded(rx.recv(), wait).await;
            drop(guard);
            match outcome {
                Err(wait_err) => return Err(wait_err),
                Ok(Ok(res)) => return res,
                // The computation ended without a result (panic/abort): start over.
                Ok(Err(_)) => continue,
            }
        }
        Err(format!(
            "computation for '{instance_id}' aborted repeatedly"
        ))
    }

    fn next_tick(&self) -> u64 {
        self.ticks.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn next_version(&self) -> u64 {
        self.versions.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Latest invalidation affecting `key`: computations started before it are not joined.
    fn invalidation_tick(&self, instance_id: &str, key: &str) -> u64 {
        let k = self.key_invalidated.get(key).map(|t| *t).unwrap_or(0);
        self.instance_invalidation_tick(instance_id).max(k)
    }

    fn instance_invalidation_tick(&self, instance_id: &str) -> u64 {
        self.instance_invalidated
            .get(instance_id)
            .map(|t| *t)
            .unwrap_or(0)
    }

    fn cache_hit(
        &self,
        instance_id: &str,
        query_key: &str,
        generation_id: i64,
    ) -> Option<QueryEnvelope> {
        let mut cache = self.cache.lock().unwrap();
        let entry = cache.get_mut(query_key)?;
        let age = Utc::now()
            .signed_duration_since(entry.updated_at)
            .to_std()
            .unwrap_or(Duration::MAX);
        if entry.stale || entry.generation_id != generation_id || age >= self.cache_ttl {
            return None;
        }
        entry.hits += 1;
        self.telemetry.query_hits.fetch_add(1, Ordering::Relaxed);
        let mut env = QueryEnvelope::snapshot(
            instance_id,
            query_key,
            generation_id,
            entry.version,
            entry.status,
            entry.schema.clone(),
            entry.value.clone(),
            None,
        );
        env.updated_at = entry.source_updated_at.to_rfc3339();
        Some(env)
    }

    fn cached_version(&self, query_key: &str) -> Option<u64> {
        self.cache
            .lock()
            .unwrap()
            .peek(query_key)
            .map(|e| e.version)
    }

    /// Built-in history provider: serves only the Instance's configured series, under the
    /// caller's authority over the series' source provider, within the query budget (§7.4).
    fn query_history_series(
        &self,
        g: &ConfiguredGraph,
        inst: &InstanceConfig,
        params: &serde_json::Value,
        principal: &Principal,
    ) -> Result<QueryEnvelope, String> {
        // Configured series: `series_id` (one) and/or `series_ids` (several, e.g. one sampled
        // series per repository); a query can only ever reach these.
        let mut configured: Vec<&str> = Vec::new();
        if let Some(v) = inst.config.get("series_id") {
            configured.push(v.as_str().ok_or_else(|| {
                format!(
                    "history_series Instance '{}': 'series_id' must be a string",
                    inst.id
                )
            })?);
        }
        if let Some(v) = inst.config.get("series_ids") {
            let arr = v.as_array().ok_or_else(|| {
                format!(
                    "history_series Instance '{}': 'series_ids' must be a list",
                    inst.id
                )
            })?;
            for item in arr {
                let id = item.as_str().ok_or_else(|| {
                    format!(
                        "history_series Instance '{}': 'series_ids' must hold strings",
                        inst.id
                    )
                })?;
                if !configured.contains(&id) {
                    configured.push(id);
                }
            }
        }
        if configured.is_empty() {
            return Err(format!(
                "history_series Instance '{}' has no configured 'series_id'",
                inst.id
            ));
        }
        let empty = serde_json::Map::new();
        let obj = match params {
            serde_json::Value::Null => &empty,
            serde_json::Value::Object(m) => m,
            _ => return Err("history_series query parameters must be an object".into()),
        };
        let read = crate::history::parse_history_read_options(obj, Utc::now())?;
        let serves = || {
            format!(
                "history_series Instance '{}' serves series {}",
                inst.id,
                configured
                    .iter()
                    .map(|c| format!("'{c}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let series_id: Option<&str> = match (obj.get("series_id"), obj.get("params")) {
            (Some(_), Some(_)) => {
                return Err("history_series query takes 'series_id' or 'params', not both".into())
            }
            (Some(requested), None) => {
                let requested = requested
                    .as_str()
                    .ok_or_else(|| "history_series 'series_id' must be a string".to_string())?;
                if !configured.contains(&requested) {
                    return Err(format!("{}, not '{requested}'", serves()));
                }
                Some(requested)
            }
            // Select the configured series whose sampled query parameters include `params`
            // (e.g. `{repository = "acme/payments"}`); none sampled means no history: `[]`.
            (None, Some(selector)) => {
                let selector = selector
                    .as_object()
                    .ok_or_else(|| "history_series 'params' must be an object".to_string())?;
                let matching: Vec<&str> = configured
                    .iter()
                    .copied()
                    .filter(|id| {
                        g.workspace
                            .history
                            .iter()
                            .find(|h| h.series_id == *id)
                            .and_then(|h| h.params.as_object())
                            .is_some_and(|sampled| {
                                selector.iter().all(|(k, v)| sampled.get(k) == Some(v))
                            })
                    })
                    .collect();
                if matching.len() > 1 {
                    return Err(format!(
                        "history_series 'params' matches several series ({}); be more specific",
                        matching.join(", ")
                    ));
                }
                matching.first().copied()
            }
            (None, None) if configured.len() == 1 => Some(configured[0]),
            (None, None) => {
                return Err(format!(
                    "{}; select one with 'series_id' or 'params'",
                    serves()
                ))
            }
        };
        let limit = read
            .limit
            .unwrap_or(HISTORY_DEFAULT_LIMIT)
            .min(HISTORY_MAX_LIMIT);

        let val = match series_id {
            None => serde_json::Value::Array(Vec::new()),
            Some(series_id) => {
                // Reading a series exposes aggregates of its source provider: require authority
                // over it.
                let source = g
                    .workspace
                    .history
                    .iter()
                    .find(|h| h.series_id == series_id);
                match source.and_then(|cfg| g.instances.get(&cfg.instance_id)) {
                    Some(src_inst) => audited(
                        &self.store,
                        principal,
                        "provider.query",
                        check_provider_access(principal, src_inst),
                    )
                    .map_err(|e| e.to_string())?,
                    None if principal.ceiling.unrestricted => {}
                    None => {
                        return Err(format!(
                            "history series '{series_id}' has no declared source provider in this Workspace; access denied"
                        ))
                    }
                }
                let (since, until) = (read.since.as_deref(), read.until.as_deref());
                match read.bucket {
                    // Downsampled: one aggregate per time bucket.
                    Some((bucket_secs, aggregate)) => serde_json::to_value(
                        self.store
                            .query_history_buckets(
                                series_id,
                                since,
                                until,
                                bucket_secs,
                                aggregate,
                                limit,
                            )
                            .map_err(|e| e.to_string())?,
                    ),
                    None => serde_json::to_value(
                        self.store
                            .query_history_samples_window(series_id, since, until, limit)
                            .map_err(|e| e.to_string())?,
                    ),
                }
                .map_err(|e| e.to_string())?
            }
        };
        let qk = self.compute_query_key(&inst.id, HISTORY_BUILTIN_DIGEST, params, principal);
        Ok(QueryEnvelope::snapshot(
            &inst.id,
            &qk,
            g.generation_id,
            self.next_version(),
            QueryStatus::Fresh,
            Some("dash.history_series@1".into()),
            val,
            None,
        ))
    }

    fn describe_key(&self, key: &str) -> String {
        if let Some(f) = self.in_flight.get(key) {
            return f.instance_id.clone();
        }
        if let Some(f) = self.render_in_flight.get(key) {
            return format!("{} (render)", f.instance_id);
        }
        if let Some(e) = self.cache.lock().unwrap().peek(key) {
            return e.instance_id.clone();
        }
        key.to_string()
    }

    fn report_cycle(
        &self,
        instance_id: &str,
        query_key: &str,
        call_stack: &[String],
        parent_query_key: Option<&str>,
        principal: &Principal,
    ) -> String {
        let mut path: Vec<String> = call_stack.iter().map(|k| self.describe_key(k)).collect();
        path.push(instance_id.to_string());
        let msg = format!(
            "dynamic dependency cycle detected involving '{instance_id}' (query_key={query_key}): {}",
            path.join(" -> ")
        );
        let trace_id = format!("tr_{}", Uuid::now_v7().simple());
        self.telemetry
            .log("error", "query_engine", &principal.id, &trace_id, &msg);
        self.telemetry.record_span(
            &trace_id,
            None,
            &format!("query:{instance_id}"),
            &principal.id,
            serde_json::json!({
                "query_key": query_key,
                "parent_query_key": parent_query_key,
                "cycle_path": path,
                "call_stack": call_stack,
                "error": "dependency_cycle",
            }),
            "error",
            0,
        );
        msg
    }

    async fn run_query_leader(
        self,
        job: Arc<QueryJob>,
        start_tick: u64,
        cancel: CancelSignal,
    ) -> Result<QueryEnvelope, String> {
        let instance_id = job.inst.id.clone();
        let trace_id = format!("tr_{}", Uuid::now_v7().simple());
        let started = Instant::now();

        let admission = self.breaker_admit(&instance_id);
        if let Admission::Rejected { until } = admission {
            return self.serve_breaker_rejection(&job, until, &trace_id);
        }
        self.telemetry.query_misses.fetch_add(1, Ordering::Relaxed);

        // First computation of a subscribed query: subscribers see `loading` (§6.1).
        if !self.cache.lock().unwrap().contains(&job.query_key) {
            let _ = self.signals.publish_query(QueryEnvelope::loading(
                &instance_id,
                &job.query_key,
                job.graph.generation_id,
                job.def
                    .manifest
                    .output
                    .as_ref()
                    .and_then(|s| s.semantic_type_name().map(|n| n.to_string())),
            ));
        }

        let run = if let Some(rule) = self.telemetry.take_injected_failure(&instance_id) {
            Err(ProviderFailure {
                message: format!("injected failure ({}): {}", rule.mode, rule.message),
                cancelled: false,
                deps: BTreeSet::new(),
            })
        } else {
            self.execute_provider_vm(&job, &trace_id, &cancel).await
        };
        let duration_ms = started.elapsed().as_millis() as i64;

        match run {
            Ok(run) => Ok(self.finish_query_success(&job, run, start_tick, &trace_id, duration_ms)),
            Err(f) if f.cancelled => {
                if admission == Admission::Probe {
                    self.breaker_release_probe(&instance_id);
                }
                self.telemetry.record_span(
                    &trace_id,
                    None,
                    &format!("query:{instance_id}"),
                    &job.principal.id,
                    serde_json::json!({ "query_key": job.query_key, "status": "cancelled" }),
                    "cancelled",
                    duration_ms,
                );
                Err(format!("query '{instance_id}' cancelled: {}", f.message))
            }
            Err(f) => self.finish_query_failure(&job, f, &trace_id, duration_ms),
        }
    }

    fn finish_query_success(
        &self,
        job: &Arc<QueryJob>,
        run: ProviderRun,
        start_tick: u64,
        trace_id: &str,
        duration_ms: i64,
    ) -> QueryEnvelope {
        let instance_id = job.inst.id.as_str();
        self.breaker_success(instance_id);
        self.retry_attempts.remove(&job.query_key);
        self.reconcile_dynamic_edges(&job.query_key, run.deps);

        let (status, source_updated_at) = derive_status(&run.upstreams);
        let commit = self.commit_query_value(
            &job.graph,
            instance_id,
            &job.def,
            &job.params,
            &job.principal,
            &job.query_key,
            run.value,
            status,
            source_updated_at,
            start_tick,
            &run.upstreams,
            true,
        );
        self.publish_commit(&commit);
        if commit.changed {
            // Ancestors on the call stack are reading this very value right now.
            self.propagate_change(&job.query_key, &job.call_stack);
        }

        self.telemetry.record_span(
            trace_id,
            None,
            &format!("query:{instance_id}"),
            &job.principal.id,
            serde_json::json!({
                "query_key": job.query_key,
                "status": status_str(status),
                "committed": commit.written,
                "changed": commit.changed,
            }),
            "ok",
            duration_ms,
        );
        commit.snapshot
    }

    fn finish_query_failure(
        &self,
        job: &Arc<QueryJob>,
        failure: ProviderFailure,
        trace_id: &str,
        duration_ms: i64,
    ) -> Result<QueryEnvelope, String> {
        let instance_id = job.inst.id.as_str();
        let err_msg = failure.message;
        // Edges observed up to the error still describe this run (§6.1).
        self.reconcile_dynamic_edges(&job.query_key, failure.deps);
        if self.breaker_failure(instance_id) {
            self.pool.evict(instance_id);
        }
        self.telemetry.log(
            "error",
            "query_engine",
            &job.principal.id,
            trace_id,
            &format!("query '{instance_id}' failed: {err_msg}"),
        );
        self.telemetry.record_span(
            trace_id,
            None,
            &format!("query:{instance_id}"),
            &job.principal.id,
            serde_json::json!({ "query_key": job.query_key, "error": err_msg }),
            "error",
            duration_ms,
        );

        let retry = if self.signals.subscriber_count(&job.query_key) > 0 {
            let until = self.breaker_open_until(instance_id);
            self.schedule_retry(job, true, until)
        } else {
            None
        };

        // Stale data retention (§6.1, §15.2): return last good value marked `stale`, with a new
        // version so subscribers observe the transition (Appendix A.2).
        let stale_env = {
            let mut cache = self.cache.lock().unwrap();
            if let Some(entry) = cache.get_mut(&job.query_key) {
                let was_fresh = entry.status != QueryStatus::Stale;
                entry.status = QueryStatus::Stale;
                entry.stale = true;
                entry.last_error = Some(err_msg.clone());
                entry.version = self.next_version();
                let mut env = QueryEnvelope::snapshot(
                    instance_id,
                    &job.query_key,
                    entry.generation_id,
                    entry.version,
                    QueryStatus::Stale,
                    entry.schema.clone(),
                    entry.value.clone(),
                    Some(err_msg.clone()),
                );
                env.updated_at = entry.source_updated_at.to_rfc3339();
                env.retry = retry.clone();
                Some((env, was_fresh))
            } else {
                None
            }
        };
        match stale_env {
            Some((env, was_fresh)) => {
                self.signals.publish_query(env.clone());
                if was_fresh {
                    // Dependents computed over the fresh value now sit on stale data.
                    self.propagate_change(&job.query_key, &job.call_stack);
                }
                Ok(env)
            }
            None => {
                // No last good value: subscribers get an `error` Signal (§6.1).
                let mut env = QueryEnvelope::error(
                    instance_id,
                    &job.query_key,
                    job.graph.generation_id,
                    job.def
                        .manifest
                        .output
                        .as_ref()
                        .and_then(|s| s.semantic_type_name().map(|n| n.to_string())),
                    err_msg.clone(),
                );
                env.retry = retry;
                let _ = self.signals.publish_query(env);
                Err(err_msg)
            }
        }
    }

    /// Open breaker: serve the last value as stale without executing or counting a failure.
    fn serve_breaker_rejection(
        &self,
        job: &Arc<QueryJob>,
        until: Option<Instant>,
        trace_id: &str,
    ) -> Result<QueryEnvelope, String> {
        let instance_id = job.inst.id.as_str();
        let msg = format!("circuit breaker open for '{instance_id}'");
        let scheduled = if self.signals.subscriber_count(&job.query_key) > 0 {
            self.schedule_retry(job, false, until)
        } else {
            None
        };
        let retry = scheduled.unwrap_or_else(|| RetryInfo {
            attempt: self
                .retry_attempts
                .get(&job.query_key)
                .map(|a| *a)
                .unwrap_or(0),
            next_retry_at: until.map(instant_to_utc).map(|t| t.to_rfc3339()),
            circuit_open: true,
        });
        self.telemetry.record_span(
            trace_id,
            None,
            &format!("query:{instance_id}"),
            &job.principal.id,
            serde_json::json!({ "query_key": job.query_key, "status": "circuit_open" }),
            "rejected",
            0,
        );
        let cache = self.cache.lock().unwrap();
        match cache.peek(&job.query_key) {
            Some(entry) => {
                let mut env = QueryEnvelope::snapshot(
                    instance_id,
                    &job.query_key,
                    entry.generation_id,
                    entry.version,
                    QueryStatus::Stale,
                    entry.schema.clone(),
                    entry.value.clone(),
                    Some(entry.last_error.clone().unwrap_or_else(|| msg.clone())),
                );
                env.updated_at = entry.source_updated_at.to_rfc3339();
                env.retry = Some(retry);
                Ok(env)
            }
            None => Err(msg),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_query_value(
        &self,
        g: &ConfiguredGraph,
        instance_id: &str,
        def: &LoadedDefinition,
        params: &serde_json::Value,
        principal: &Principal,
        query_key: &str,
        value: serde_json::Value,
        status: QueryStatus,
        source_updated_at: DateTime<Utc>,
        start_tick: u64,
        observed: &[UpstreamObservation],
        count_miss: bool,
    ) -> CommitOutcome {
        let schema_str = def
            .manifest
            .output
            .as_ref()
            .and_then(|s| s.semantic_type_name().map(|n| n.to_string()));

        let mut cache = self.cache.lock().unwrap();
        let prev = cache.peek(query_key).cloned();
        // Outdated when a later-started run already committed, the Instance was invalidated
        // (e.g. by an action) after this run started, or an upstream it read has changed since.
        let outdated = prev
            .as_ref()
            .map(|p| p.start_tick > start_tick)
            .unwrap_or(false)
            || self.instance_invalidation_tick(instance_id) > start_tick
            || upstreams_moved(&cache, observed);
        let version = self.next_version();
        let mut snap = QueryEnvelope::snapshot(
            instance_id,
            query_key,
            g.generation_id,
            version,
            if outdated { QueryStatus::Stale } else { status },
            schema_str.clone(),
            value.clone(),
            None,
        );
        snap.updated_at = source_updated_at.to_rfc3339();
        if outdated {
            // Started before a newer computation or an invalidation: hand the result to this
            // run's own waiters, but never cache or publish it as current.
            return CommitOutcome {
                snapshot: snap,
                delta: None,
                written: false,
                changed: false,
            };
        }

        let changed = prev
            .as_ref()
            .map(|p| p.value != value || p.status != status)
            .unwrap_or(true);
        let evicted = cache.push(
            query_key.to_string(),
            CachedQueryEntry {
                instance_id: instance_id.to_string(),
                definition_digest: def.manifest.identity_version.clone(),
                auth_fingerprint: principal.authorization_fingerprint(),
                params: params.clone(),
                principal: principal.clone(),
                generation_id: g.generation_id,
                version,
                status,
                value: value.clone(),
                schema: schema_str.clone(),
                updated_at: Utc::now(),
                source_updated_at,
                start_tick,
                stale: false,
                last_error: None,
                hits: prev.as_ref().map(|p| p.hits).unwrap_or(0),
                misses: prev.as_ref().map(|p| p.misses).unwrap_or(0) + u64::from(count_miss),
            },
        );
        drop(cache);
        if let Some((evicted_key, _)) = evicted {
            if evicted_key != query_key {
                self.on_node_evicted(&evicted_key);
            }
        }
        self.key_invalidated
            .remove_if(query_key, |_, t| *t < start_tick);

        // Order-preserving keyed delta against the version subscribers hold (the previous
        // entry's version); the row key is inferred (preferring `id`) and carried as `row_key`.
        let delta = match (prev.as_ref(), value.as_array()) {
            (Some(p), Some(new_arr)) => p.value.as_array().and_then(|old_arr| {
                compute_keyed_delta_inferred(old_arr, new_arr, "id")
                    .filter(|(_, ops)| !ops.is_empty())
                    .map(|(field, ops)| {
                        let mut d = QueryEnvelope::delta(
                            instance_id,
                            query_key,
                            g.generation_id,
                            p.version,
                            version,
                            schema_str.clone(),
                            ops,
                        )
                        .with_row_key(field);
                        d.status = status;
                        d
                    })
            }),
            _ => None,
        };
        CommitOutcome {
            snapshot: snap,
            delta,
            written: true,
            changed,
        }
    }

    fn publish_commit(&self, commit: &CommitOutcome) {
        if !commit.written {
            return;
        }
        if let Some(d) = &commit.delta {
            let _ = self.signals.publish_query(d.clone());
        }
        let _ = self.signals.publish_query(commit.snapshot.clone());
    }

    async fn execute_provider_vm(
        &self,
        job: &QueryJob,
        trace_id: &str,
        cancel: &CancelSignal,
    ) -> Result<ProviderRun, ProviderFailure> {
        let inst = &job.inst;
        let def = &job.def;
        let recorded_deps = Arc::new(Mutex::new(BTreeSet::new()));
        let recorded_upstreams = Arc::new(Mutex::new(Vec::new()));
        let bridge = Arc::new(EngineHostBridge {
            engine: self.clone(),
            graph: Arc::clone(&job.graph),
            caller_inst: inst.clone(),
            caller_def: def.clone(),
            principal: job.principal.clone(),
            trace_id: trace_id.to_string(),
            parent_query_key: Some(job.query_key.clone()),
            call_stack: job.call_stack.clone(),
            recorded_deps: Arc::clone(&recorded_deps),
            recorded_upstreams: Arc::clone(&recorded_upstreams),
            recorded_state: Arc::new(Mutex::new(BTreeMap::new())),
            dashboard_id: None,
            operation: None,
            deadline: self.host_await_deadline(&inst.id),
            cancel: cancel.clone(),
            outcome_unknown: Arc::new(AtomicBool::new(false)),
            batches: BatchMap::default(),
        });

        let bundle = CompiledInstanceBundle {
            instance_id: inst.id.clone(),
            manifest: def.manifest.clone(),
            runtime_source: def.runtime_source.clone(),
            config_digest: inst.config_digest(),
            resolved_libraries: job.graph.resolved_libraries(),
        };

        let handler = if def.manifest.handler.is_empty() {
            "query"
        } else {
            def.manifest.handler.as_str()
        };

        let allowed_emits: Vec<String> = def
            .manifest
            .emits
            .iter()
            .map(|e| e.semantic_type.clone())
            .collect();

        let res = self
            .pool
            .execute_handler(
                &bundle,
                handler,
                ExecutionContextConfig {
                    category: def.manifest.category,
                    instance_id: inst.id.clone(),
                    input_names: inst.inputs.keys().cloned().collect(),
                    config: inst.config.clone(),
                    payload: job.params.clone(),
                    operation: None,
                    idempotency_key: None,
                    allowed_emits,
                    now_iso: Utc::now().to_rfc3339(),
                    trace_id: trace_id.to_string(),
                    bridge,
                },
                Some(cancel.vm_flag()),
            )
            .await;

        let deps = recorded_deps.lock().unwrap().clone();
        let upstreams = recorded_upstreams.lock().unwrap().clone();
        let mut val = match res {
            Ok(v) => v,
            Err(e) => {
                return Err(ProviderFailure {
                    cancelled: cancel.is_cancelled() || matches!(e, RuntimeError::Cancelled),
                    message: self.store.redactor().redact_str(&e.to_string()),
                    deps,
                })
            }
        };

        // Validate output contract at boundary. When a projection was pushed to the provider
        // (`pushdown = { projection = true }`), its rows carry only the projected fields: they
        // are checked against the contract restricted to those fields.
        if let Some(ref out_schema) = def.manifest.output {
            let pushed_fields: Option<Vec<String>> = job
                .params
                .get(PUSHDOWN_PARAM)
                .and_then(|p| p.get("fields"))
                .and_then(|f| serde_json::from_value(f.clone()).ok());
            let projected;
            let out_schema = match &pushed_fields {
                Some(fields) => {
                    projected = out_schema.projected(fields);
                    &projected
                }
                None => out_schema,
            };
            out_schema.normalize_empty_lists(&mut val);
            if let Err(e) = out_schema.validate_value(&val) {
                return Err(ProviderFailure {
                    message: format!("output contract violation on '{}': {e}", inst.id),
                    cancelled: false,
                    deps,
                });
            }
        }

        Ok(ProviderRun {
            value: val,
            deps,
            upstreams,
        })
    }

    /// Directly execute a connector operation (or its Mock in Preview mode).
    pub async fn invoke_connector(
        &self,
        connector_id: &str,
        operation: &str,
        args: serde_json::Value,
        principal: &Principal,
        trace_id: &str,
    ) -> Result<serde_json::Value, String> {
        self.invoke_connector_inner(connector_id, operation, args, principal, trace_id, None)
            .await
    }

    /// Connector invocation; `cancel` is set when the call comes from a read context, so dropping
    /// the call (deadline, cancelled parent) also stops the connector's VM execution.
    async fn invoke_connector_inner(
        &self,
        connector_id: &str,
        operation: &str,
        args: serde_json::Value,
        principal: &Principal,
        trace_id: &str,
        cancel: Option<CancelSignal>,
    ) -> Result<serde_json::Value, String> {
        let g = self.graph.load_full();
        let conn_inst = g
            .instances
            .get(connector_id)
            .ok_or_else(|| format!("connector instance '{connector_id}' not found"))?
            .clone();
        // Only connector Instances run here; an action bound as an input must go through
        // ActionManager::invoke (own execution record, authority and audit, §9.2).
        if conn_inst.category != DefinitionCategory::Connector {
            return Err(format!(
                "instance '{connector_id}' is a {}, not a connector; connector operations cannot target it",
                conn_inst.category.as_str()
            ));
        }

        audited(
            &self.store,
            principal,
            "connector.invoke",
            check_connector_operation_authority(principal, &conn_inst, operation, &g),
        )
        .map_err(|e| e.to_string())?;

        // The operation contract is the real connector Definition's, also when a Mock stands in
        // for it (§4.1: a Mock offers the same operations as its target). Arguments are checked
        // before anything runs, so a rejection here is provably pre-send (§4.2).
        let op_spec = g
            .definitions
            .get(&conn_inst.definition)
            .and_then(|d| d.manifest.operations.get(operation))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "undeclared operation '{operation}' on connector '{}'",
                    conn_inst.definition
                )
            })?;
        let mut args = args;
        op_spec.args.normalize_empty_lists(&mut args);
        op_spec.args.validate_value(&args).map_err(|e| {
            format!("invalid args for connector '{connector_id}' operation '{operation}': {e}")
        })?;

        if let Some(rule) = self.telemetry.take_injected_failure(connector_id) {
            return Err(format!(
                "injected failure ({}): {}",
                rule.mode, rule.message
            ));
        }

        // In Preview mode, substitute matching Mock Definition (§10.3)
        let (eff_def, eff_cat) = if g.routes_to_mocks() {
            let mock_def = g.find_mock_for(&conn_inst.definition).ok_or_else(|| {
                format!(
                    "preview requires a Mock definition for connector '{}'",
                    conn_inst.definition
                )
            })?;
            (mock_def.clone(), DefinitionCategory::Mock)
        } else {
            let real_def = g.definitions.get(&conn_inst.definition).ok_or_else(|| {
                format!("connector definition '{}' missing", conn_inst.definition)
            })?;
            (real_def.clone(), DefinitionCategory::Connector)
        };

        let bundle = CompiledInstanceBundle {
            instance_id: if g.routes_to_mocks() {
                format!("preview_mock:{}", conn_inst.id)
            } else {
                conn_inst.id.clone()
            },
            manifest: eff_def.manifest.clone(),
            runtime_source: eff_def.runtime_source.clone(),
            config_digest: conn_inst.config_digest(),
            resolved_libraries: g.resolved_libraries(),
        };

        // Per-Instance circuit breaker for runtime faults (ADR-0003).
        let breaker_key = bundle.instance_id.clone();
        let admission = self.breaker_admit(&breaker_key);
        if let Admission::Rejected { .. } = admission {
            return Err(format!(
                "circuit breaker open for connector '{connector_id}'"
            ));
        }

        // A signal handed in by a read context is owned by this call: dropping the call cancels
        // the VM execution. Direct invocations (e.g. from actions) are never cancelled on drop,
        // so an in-progress side effect is not interrupted mid-way.
        let _cancel_on_drop = cancel.clone().map(CancelOnDrop);
        let signal = cancel.unwrap_or_default();
        let outcome_unknown = Arc::new(AtomicBool::new(false));
        let bridge = Arc::new(EngineHostBridge {
            engine: self.clone(),
            graph: Arc::clone(&g),
            caller_inst: conn_inst.clone(),
            caller_def: eff_def.clone(),
            principal: principal.clone(),
            trace_id: trace_id.to_string(),
            parent_query_key: None,
            call_stack: Vec::new(),
            recorded_deps: Arc::new(Mutex::new(BTreeSet::new())),
            recorded_upstreams: Arc::new(Mutex::new(Vec::new())),
            recorded_state: Arc::new(Mutex::new(BTreeMap::new())),
            dashboard_id: None,
            operation: Some(operation.to_string()),
            deadline: self.host_await_deadline(&bundle.instance_id),
            cancel: signal.clone(),
            outcome_unknown: Arc::clone(&outcome_unknown),
            batches: BatchMap::default(),
        });

        let handler = if eff_def.manifest.handler.is_empty() {
            "call"
        } else {
            eff_def.manifest.handler.as_str()
        };
        let allowed_emits: Vec<String> = eff_def
            .manifest
            .emits
            .iter()
            .map(|e| e.semantic_type.clone())
            .collect();

        let res = self
            .pool
            .execute_handler(
                &bundle,
                handler,
                ExecutionContextConfig {
                    category: eff_cat,
                    instance_id: conn_inst.id.clone(),
                    input_names: Vec::new(),
                    config: conn_inst.config.clone(),
                    payload: args,
                    operation: Some(operation.to_string()),
                    idempotency_key: None,
                    allowed_emits,
                    now_iso: Utc::now().to_rfc3339(),
                    trace_id: trace_id.to_string(),
                    bridge,
                },
                Some(signal.vm_flag()),
            )
            .await;
        // Untrusted connector output is checked against the operation contract at the boundary
        // (§4.2), for Mocks too; an invalid result counts as a fault for the breaker (§15.2).
        let res = res.map(|mut v| {
            op_spec.result.normalize_empty_lists(&mut v);
            v
        });
        let contract_violation = res
            .as_ref()
            .ok()
            .and_then(|v| op_spec.result.validate_value(v).err());
        if contract_violation.is_some() {
            if self.breaker_failure(&breaker_key) {
                self.pool.evict(&breaker_key);
            }
        } else {
            self.record_runtime_outcome(&breaker_key, admission, &res, false);
        }
        let possibly_sent = outcome_unknown.load(Ordering::SeqCst);
        let val = res.map_err(|e| match e {
            // The connector handler was interrupted mid-flight: any request it issued may have
            // been processed upstream (§9.1 `unknown`).
            RuntimeError::Timeout(_) | RuntimeError::Cancelled => format!(
                "{}: connector '{}' operation '{operation}' did not complete: {e}",
                crate::actions::UNKNOWN_OUTCOME_CODE,
                conn_inst.id
            ),
            other => {
                let msg = self.store.redactor().redact_str(&other.to_string());
                // A write of this execution ended without a known outcome; whatever the handler
                // raised afterwards, the effect may have happened (§9.1, Appendix A.4).
                if possibly_sent && !crate::actions::is_unknown_outcome_error(&msg) {
                    format!(
                        "{}: connector '{}' operation '{operation}' failed after a request whose outcome is unknown: {msg}",
                        crate::actions::UNKNOWN_OUTCOME_CODE,
                        conn_inst.id
                    )
                } else {
                    msg
                }
            }
        })?;

        // A side-effecting operation already ran, so its malformed result leaves the outcome
        // unknown rather than proving the write did not happen.
        if let Some(e) = contract_violation {
            let msg = format!(
                "output contract violation on connector '{connector_id}' operation '{operation}': {e}"
            );
            return Err(if op_spec.has_side_effects() {
                format!("{}: {msg}", crate::actions::UNKNOWN_OUTCOME_CODE)
            } else {
                msg
            });
        }
        Ok(val)
    }

    /// Fallback deadline for host awaits of an execution of `vm_instance_id` started now: the
    /// Instance's effective wall budget (Workspace `[resources]` overrides included), never later
    /// than the enclosing execution's deadline. While the VM runs, its own deadline applies.
    fn host_await_deadline(&self, vm_instance_id: &str) -> Instant {
        let own = Instant::now() + self.pool.effective_budget(vm_instance_id).wall_timeout;
        dash_luau::current_execution_deadline().map_or(own, |d| d.min(own))
    }

    /// Update the breaker of a non-query execution. Timeouts and memory violations always count;
    /// script errors count only when `count_script_errors` (business errors of connectors don't).
    pub(crate) fn record_runtime_outcome(
        &self,
        breaker_key: &str,
        admission: Admission,
        res: &Result<serde_json::Value, RuntimeError>,
        count_script_errors: bool,
    ) {
        match res {
            Ok(_) => self.breaker_success(breaker_key),
            Err(RuntimeError::Timeout(_)) | Err(RuntimeError::MemoryExceeded(_)) => {
                if self.breaker_failure(breaker_key) {
                    self.pool.evict(breaker_key);
                }
            }
            Err(RuntimeError::Luau(_)) | Err(RuntimeError::HandlerNotFound(..))
                if count_script_errors =>
            {
                if self.breaker_failure(breaker_key) {
                    self.pool.evict(breaker_key);
                }
            }
            Err(_) => {
                if admission == Admission::Probe {
                    self.breaker_release_probe(breaker_key);
                }
            }
        }
    }

    /// [`QueryEngine::record_runtime_outcome`] for an execution started at `started` under the
    /// caller's `inherited` deadline (`None` at top level). A timeout that the caller's deadline
    /// caused (it fell before the callee's own wall timeout and has passed) is the caller's
    /// budget running out, not a fault of the callee: it does not count against its breaker.
    pub(crate) fn record_runtime_outcome_within(
        &self,
        breaker_key: &str,
        admission: Admission,
        res: &Result<serde_json::Value, RuntimeError>,
        inherited: Option<Instant>,
        started: Instant,
    ) {
        if let (Err(RuntimeError::Timeout(_)), Some(deadline)) = (res, inherited) {
            let own = started + self.pool.effective_budget(breaker_key).wall_timeout;
            if deadline < own && Instant::now() >= deadline {
                if admission == Admission::Probe {
                    self.breaker_release_probe(breaker_key);
                }
                return;
            }
        }
        self.record_runtime_outcome(breaker_key, admission, res, false);
    }

    /// Push a direct update (or simulated push) for an instance's cached queries and compute keyed delta.
    pub fn push_query_value(
        &self,
        instance_id: &str,
        params: &serde_json::Value,
        principal: &Principal,
        new_value: serde_json::Value,
    ) -> Result<QueryEnvelope, String> {
        let g = self.graph.load_full();
        let inst = g
            .instances
            .get(instance_id)
            .ok_or_else(|| format!("instance '{instance_id}' not found"))?;
        let def = g
            .definitions
            .get(&inst.definition)
            .ok_or_else(|| format!("definition '{}' missing", inst.definition))?;
        let query_key = self.compute_query_key(
            instance_id,
            &def.manifest.identity_version,
            params,
            principal,
        );

        let start_tick = self.next_tick();
        let commit = self.commit_query_value(
            &g,
            instance_id,
            def,
            params,
            principal,
            &query_key,
            new_value,
            QueryStatus::Fresh,
            Utc::now(),
            start_tick,
            &[],
            false,
        );
        self.publish_commit(&commit);
        if commit.changed {
            self.propagate_change(&query_key, &[]);
        }
        Ok(commit.snapshot)
    }

    /// Apply a provider-emitted keyed delta (§6.2) to a cached query.
    ///
    /// The provider's monotonic source-local `source_sequence` is verified (duplicates are
    /// ignored); the delta is applied to the cached value, the version is bumped and the delta is
    /// published with its `base_version` (the materialized value becomes the resync snapshot).
    /// On a sequence gap, or when no cached snapshot exists to apply onto, the query is
    /// resynchronized from a fresh snapshot instead. Requires the Definition to declare
    /// `updates.push`.
    pub async fn apply_provider_delta(
        &self,
        instance_id: &str,
        params: &serde_json::Value,
        principal: &Principal,
        source_sequence: u64,
        ops: Vec<crate::signal::DeltaOp>,
    ) -> Result<crate::signal::ProviderDeltaOutcome, String> {
        match self.provider_delta_step(instance_id, params, principal, source_sequence, ops)? {
            DeltaStep::Done(outcome) => Ok(*outcome),
            DeltaStep::NeedsResync(reason) => {
                let snapshot = self
                    .query(instance_id, params.clone(), principal, true)
                    .await?;
                Ok(crate::signal::ProviderDeltaOutcome::Resynced { reason, snapshot })
            }
        }
    }

    /// Verify and apply a provider delta; returns the resync reason instead of resynchronizing,
    /// so a caller running inside the provider's own VM (ctx:push_delta) can schedule the resync
    /// rather than wait on its own Instance.
    fn provider_delta_step(
        &self,
        instance_id: &str,
        params: &serde_json::Value,
        principal: &Principal,
        source_sequence: u64,
        ops: Vec<crate::signal::DeltaOp>,
    ) -> Result<DeltaStep, String> {
        use crate::signal::{ProviderDeltaOutcome, SequenceCheck};
        let g = self.graph.load_full();
        let inst = g
            .instances
            .get(instance_id)
            .ok_or_else(|| format!("instance '{instance_id}' not found"))?;
        let def = g
            .definitions
            .get(&inst.definition)
            .ok_or_else(|| format!("definition '{}' missing", inst.definition))?;
        if !def.manifest.updates.push {
            return Err(format!(
                "definition '{}' does not declare push updates",
                def.manifest.id
            ));
        }
        let query_key = self.resolve_query_key(instance_id, params, principal)?;
        let seqs = self.signals.source_sequences();

        let resync_reason = match seqs.check_and_advance(&query_key, source_sequence) {
            SequenceCheck::Duplicate { last } => {
                return Ok(DeltaStep::Done(Box::new(ProviderDeltaOutcome::Duplicate {
                    last_sequence: last,
                })))
            }
            SequenceCheck::Gap { expected, got } => Some(format!(
                "source sequence gap: expected {expected}, got {got}"
            )),
            SequenceCheck::Baseline | SequenceCheck::InOrder => None,
        };

        let applied = if resync_reason.is_none() {
            let mut cache = self.cache.lock().unwrap();
            match cache.get_mut(&query_key) {
                Some(entry) if entry.value.is_array() && !entry.stale => {
                    let mut rows = entry.value.as_array().cloned().unwrap_or_default();
                    let key_field = crate::signal::infer_row_key_field(&rows, &[], "id")
                        .unwrap_or_else(|| "id".to_string());
                    crate::signal::apply_delta_ops(&mut rows, &ops, &key_field);
                    let base = entry.version;
                    // Versions come from the engine-wide monotonic counter (Appendix A.2).
                    entry.version = self.next_version();
                    entry.value = serde_json::Value::Array(rows);
                    entry.updated_at = Utc::now();
                    entry.source_updated_at = entry.updated_at;
                    entry.status = QueryStatus::Fresh;
                    entry.last_error = None;
                    let delta = QueryEnvelope::delta(
                        instance_id,
                        &query_key,
                        entry.generation_id,
                        base,
                        entry.version,
                        entry.schema.clone(),
                        ops,
                    )
                    .with_row_key(key_field)
                    .with_source_sequence(source_sequence);
                    let materialized = QueryEnvelope::snapshot(
                        instance_id,
                        &query_key,
                        entry.generation_id,
                        entry.version,
                        QueryStatus::Fresh,
                        entry.schema.clone(),
                        entry.value.clone(),
                        None,
                    )
                    .with_updated_at(entry.updated_at);
                    Some((delta, materialized))
                }
                _ => None,
            }
        } else {
            None
        };

        if let Some((delta, materialized)) = applied {
            let _ = self
                .signals
                .publish_delta_with_snapshot(delta.clone(), materialized);
            // Dynamic dependents of this query observe the change like any other commit.
            self.propagate_change(&query_key, &[]);
            return Ok(DeltaStep::Done(Box::new(ProviderDeltaOutcome::Applied(
                delta,
            ))));
        }

        // Resynchronize from a snapshot; the next delta establishes the new sequence baseline.
        let reason = resync_reason
            .unwrap_or_else(|| "no cached snapshot to apply the delta onto".to_string());
        seqs.reset(&query_key, None);
        Ok(DeltaStep::NeedsResync(reason))
    }

    /// Start the provider's `subscribe(params)` hook for a subscribed query unless it is already
    /// active (§6.1). The hook runs as one bounded execution of the provider Instance with the
    /// query's parameters; it may push snapshots or keyed deltas (ctx:push_snapshot /
    /// ctx:push_delta) for the query. The subscription is released, and a running hook
    /// cancelled, once the query has no subscribers left ([`Self::prune_idle`]); a later
    /// subscriber starts it again. A failed hook is logged and released so the next subscriber
    /// retries it; a Definition without a `subscribe` function keeps the (hook-less)
    /// subscription so it is not looked up again while subscribed.
    fn ensure_provider_subscription(
        &self,
        g: &Arc<ConfiguredGraph>,
        inst: &InstanceConfig,
        def: &LoadedDefinition,
        params: &serde_json::Value,
        principal: &Principal,
        query_key: &str,
    ) {
        let cancel = CancelSignal::new();
        match self.provider_subscriptions.entry(query_key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(_) => return,
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(cancel.clone());
            }
        }
        let engine = self.clone();
        let graph = Arc::clone(g);
        let inst = inst.clone();
        let def = def.clone();
        let params = params.clone();
        let principal = principal.clone();
        let query_key = query_key.to_string();
        tokio::spawn(async move {
            let trace_id = format!("tr_{}", Uuid::now_v7().simple());
            let bridge = Arc::new(EngineHostBridge {
                engine: engine.clone(),
                graph: Arc::clone(&graph),
                caller_inst: inst.clone(),
                caller_def: def.clone(),
                principal: principal.clone(),
                trace_id: trace_id.clone(),
                parent_query_key: Some(query_key.clone()),
                call_stack: vec![query_key.clone()],
                recorded_deps: Arc::new(Mutex::new(BTreeSet::new())),
                recorded_upstreams: Arc::new(Mutex::new(Vec::new())),
                recorded_state: Arc::new(Mutex::new(BTreeMap::new())),
                dashboard_id: None,
                operation: None,
                deadline: engine.host_await_deadline(&inst.id),
                cancel: cancel.clone(),
                outcome_unknown: Arc::new(AtomicBool::new(false)),
                batches: BatchMap::default(),
            });
            let bundle = CompiledInstanceBundle {
                instance_id: inst.id.clone(),
                manifest: def.manifest.clone(),
                runtime_source: def.runtime_source.clone(),
                config_digest: inst.config_digest(),
                resolved_libraries: graph.resolved_libraries(),
            };
            let started = Instant::now();
            // The hook may wait on upstream events for most of its budget: it runs in its own
            // fresh VM, outside the provider's per-Instance admission and VM lock, so the
            // provider's queries never queue behind it.
            let res = engine
                .pool
                .execute_handler_isolated(
                    &bundle,
                    "subscribe",
                    ExecutionContextConfig {
                        category: DefinitionCategory::DataProvider,
                        instance_id: inst.id.clone(),
                        input_names: inst.inputs.keys().cloned().collect(),
                        config: inst.config.clone(),
                        payload: params,
                        operation: None,
                        idempotency_key: None,
                        allowed_emits: def
                            .manifest
                            .emits
                            .iter()
                            .map(|e| e.semantic_type.clone())
                            .collect(),
                        now_iso: Utc::now().to_rfc3339(),
                        trace_id: trace_id.clone(),
                        bridge,
                    },
                    Some(cancel.vm_flag()),
                )
                .await;
            let status = match &res {
                Ok(_) => "ok",
                // `updates.subscribe` without a hook: consumers share the subscription and
                // updates arrive through polling, push or on-demand refresh (§6.2).
                Err(RuntimeError::HandlerNotFound(..)) => "no_hook",
                Err(_) if cancel.is_cancelled() => "cancelled",
                Err(_) => "error",
            };
            if let Err(e) = &res {
                if status == "error" {
                    let msg = engine.store.redactor().redact_str(&e.to_string());
                    engine.telemetry.log(
                        "warn",
                        "query_engine",
                        &principal.id,
                        &trace_id,
                        &format!("subscribe hook of '{}' failed: {msg}", inst.id),
                    );
                    // Release so the next subscriber retries (only our own registration).
                    engine
                        .provider_subscriptions
                        .remove_if(&query_key, |_, c| Arc::ptr_eq(&c.flag, &cancel.flag));
                }
            }
            engine.telemetry.record_span(
                &trace_id,
                None,
                &format!("subscribe:{}", inst.id),
                &principal.id,
                serde_json::json!({ "query_key": query_key, "hook": status != "no_hook" }),
                if status == "no_hook" { "ok" } else { status },
                started.elapsed().as_millis() as i64,
            );
        });
    }

    /// Whether the provider `subscribe` hook of `query_key` is active.
    pub fn provider_subscription_active(&self, query_key: &str) -> bool {
        self.provider_subscriptions.contains_key(query_key)
    }

    /// Release idle query state (§6.1, §20 "no unbounded queues or caches"): expire cached
    /// entries nobody subscribes to once they are older than the idle expiry (20 x cache TTL),
    /// release Signal channels past their idle grace period, and prune dependency edges and
    /// circuit breakers that no longer refer to a live query or Instance. Returns the number of
    /// expired cache entries. Called periodically by the server's maintenance loop.
    pub fn prune_idle(&self) -> usize {
        self.signals.sweep_idle();
        // Release provider subscriptions nobody consumes any more (reference count zero, §6.1).
        let unsubscribed: Vec<String> = self
            .provider_subscriptions
            .iter()
            .filter(|e| !self.signals.is_query_subscribed(e.key()))
            .map(|e| e.key().clone())
            .collect();
        for k in unsubscribed {
            if let Some((_, c)) = self.provider_subscriptions.remove(&k) {
                c.cancel();
            }
        }
        let g = self.graph.load_full();
        let expiry = self.cache_ttl * 20;
        let now = Utc::now();
        let (expired, live_keys) = {
            let mut cache = self.cache.lock().unwrap();
            let idle: Vec<String> = cache
                .iter()
                .filter(|(k, e)| {
                    let age = now
                        .signed_duration_since(e.updated_at)
                        .to_std()
                        .unwrap_or(Duration::MAX);
                    age >= expiry && !self.signals.is_query_subscribed(k)
                })
                .map(|(k, _)| k.clone())
                .collect();
            for k in &idle {
                cache.pop(k);
            }
            let live: HashSet<String> = cache.iter().map(|(k, _)| k.clone()).collect();
            (idle.len(), live)
        };
        let is_live = |k: &str| live_keys.contains(k) || self.signals.is_query_subscribed(k);
        self.forward_deps.retain(|k, _| is_live(k));
        self.reverse_deps.retain(|_, parents| {
            parents.retain(|p| is_live(p));
            !parents.is_empty()
        });
        self.circuit_breakers
            .retain(|inst, _| g.instances.contains_key(inst));
        expired
    }

    /// Mark cached queries for specified instances (and downstream dependents) as stale & refresh active ones.
    pub async fn invalidate_instances(&self, instance_ids: &[String]) -> usize {
        let g = self.graph.load_full();
        let seed: BTreeSet<String> = instance_ids.iter().cloned().collect();
        let affected = g.downstream_dependents(&seed);

        // Computations that started before this point must not be joined or cached (§9).
        let tick = self.next_tick();
        for inst_id in &affected {
            self.instance_invalidated.insert(inst_id.clone(), tick);
        }

        let mut marked = Vec::new();
        let mut to_refresh: Vec<(String, serde_json::Value, Principal)> = Vec::new();
        {
            let mut cache = self.cache.lock().unwrap();
            for (k, entry) in cache.iter_mut() {
                if affected.contains(&entry.instance_id) {
                    entry.stale = true;
                    entry.status = QueryStatus::Stale;
                    marked.push(k.clone());
                    to_refresh.push((
                        entry.instance_id.clone(),
                        entry.params.clone(),
                        entry.principal.clone(),
                    ));
                }
            }
        }
        {
            let mut renders = self.render_cache.lock().unwrap();
            for (k, entry) in renders.iter_mut() {
                if affected.contains(&entry.adapter_instance) {
                    entry.stale = true;
                    marked.push(k.clone());
                }
            }
        }
        // Dynamic dependents (recorded edges) are invalidated as well.
        for k in &marked {
            self.propagate_change(k, &[]);
        }

        let refreshed = to_refresh.len();
        for (inst_id, params, principal) in to_refresh {
            let _ = self.query(&inst_id, params, &principal, true).await;
        }
        refreshed
    }

    /// Mark downstream queries stale during Generation carry-over without dropping unaffected caches (§12.2).
    pub fn mark_generation_stale(&self, affected_instances: &BTreeSet<String>, new_gen_id: i64) {
        let tick = self.next_tick();
        for inst_id in affected_instances {
            self.instance_invalidated.insert(inst_id.clone(), tick);
            // A rebuilt Instance starts with a closed breaker (ADR-0003).
            self.circuit_breakers.remove(inst_id);
            self.circuit_breakers
                .remove(&format!("preview_mock:{inst_id}"));
        }
        {
            let mut cache = self.cache.lock().unwrap();
            for (_, entry) in cache.iter_mut() {
                if affected_instances.contains(&entry.instance_id) {
                    entry.stale = true;
                    entry.status = QueryStatus::Stale;
                } else {
                    // Unaffected instances carry over cleanly into the new Generation!
                    entry.generation_id = new_gen_id;
                }
            }
        }
        let mut renders = self.render_cache.lock().unwrap();
        for (_, entry) in renders.iter_mut() {
            if affected_instances.contains(&entry.adapter_instance) {
                entry.stale = true;
            } else {
                entry.generation_id = new_gen_id;
            }
        }
    }

    pub fn execution_graph_snapshot(&self) -> ExecutionGraphSnapshot {
        let g = self.graph.load_full();
        let poll_interval = |inst_id: &str| {
            g.instances
                .get(inst_id)
                .and_then(|i| g.definitions.get(&i.definition))
                .and_then(|d| d.manifest.updates.poll_interval_secs)
        };
        let mut nodes = Vec::new();
        {
            let cache = self.cache.lock().unwrap();
            for (qk, entry) in cache.iter() {
                nodes.push(ExecutionGraphNode {
                    query_key: qk.clone(),
                    instance_id: entry.instance_id.clone(),
                    status: status_str(entry.status).into(),
                    version: entry.version,
                    subscribers: self.signals.subscriber_count(qk),
                    cache_hits: entry.hits,
                    cache_misses: entry.misses,
                    kind: "query".into(),
                    poll_interval_secs: poll_interval(&entry.instance_id),
                });
            }
        }
        {
            let renders = self.render_cache.lock().unwrap();
            for (rk, entry) in renders.iter() {
                nodes.push(ExecutionGraphNode {
                    query_key: rk.clone(),
                    instance_id: entry.adapter_instance.clone(),
                    status: status_str(entry.outcome.status).into(),
                    version: entry.outcome.version,
                    subscribers: self.signals.subscriber_count(rk),
                    cache_hits: 0,
                    cache_misses: 0,
                    kind: "adapter_render".into(),
                    poll_interval_secs: None,
                });
            }
        }

        let mut edges = Vec::new();
        for entry in self.forward_deps.iter() {
            let from = entry.key().clone();
            for to in entry.value() {
                edges.push(ExecutionGraphEdge {
                    from_query_key: from.clone(),
                    to_query_key: to.clone(),
                });
            }
        }

        ExecutionGraphSnapshot {
            generation_id: g.generation_id,
            nodes,
            edges,
        }
    }

    /// Inspect current recorded upstream dependencies for a `query_key` (used to verify cleanup).
    pub fn recorded_dependencies(&self, query_key: &str) -> BTreeSet<String> {
        self.forward_deps
            .get(query_key)
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    /// Inspect reverse subscribers for an upstream `query_key`.
    pub fn reverse_subscribers(&self, upstream_query_key: &str) -> BTreeSet<String> {
        self.reverse_deps
            .get(upstream_query_key)
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    fn reconcile_dynamic_edges(&self, parent_qk: &str, new_deps: BTreeSet<String>) {
        let old_deps = self
            .forward_deps
            .insert(parent_qk.to_string(), new_deps.clone())
            .unwrap_or_default();
        // Unsubscribe removed edges so dynamic dependency changes never leak subscriptions (§17 AC #6)
        for removed in old_deps.difference(&new_deps) {
            if let Some(mut rev) = self.reverse_deps.get_mut(removed) {
                rev.remove(parent_qk);
            }
            self.reverse_deps.remove_if(removed, |_, s| s.is_empty());
        }
        for added in new_deps.difference(&old_deps) {
            self.reverse_deps
                .entry(added.clone())
                .or_default()
                .insert(parent_qk.to_string());
        }
        if new_deps.is_empty() {
            self.forward_deps.remove_if(parent_qk, |_, s| s.is_empty());
        }
    }

    /// Drop bookkeeping of a node that left its cache, unless it is still subscribed.
    fn on_node_evicted(&self, key: &str) {
        if self.signals.subscriber_count(key) > 0 {
            return;
        }
        self.reconcile_dynamic_edges(key, BTreeSet::new());
        self.key_invalidated.remove(key);
        self.retry_attempts.remove(key);
    }

    /// A result changed: mark its (transitive) dynamic dependents stale and recompute the ones
    /// with active subscribers (§6.1, §6.2: push, poll and on-demand share one interface).
    ///
    /// `reading_now` lists the computations currently reading the changed value (the call stack
    /// of the run that produced it); they observe the new value already, so they are neither
    /// invalidated nor rescheduled.
    fn propagate_change(&self, changed_key: &str, reading_now: &[String]) {
        let tick = self.next_tick();
        let mut visited: HashSet<String> = HashSet::from([changed_key.to_string()]);
        let mut frontier = vec![changed_key.to_string()];
        while let Some(k) = frontier.pop() {
            let dependents: Vec<String> = self
                .reverse_deps
                .get(&k)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default();
            for dep in dependents {
                if !visited.insert(dep.clone()) {
                    continue;
                }
                self.mark_node_stale(&dep);
                if !reading_now.contains(&dep) {
                    self.key_invalidated.insert(dep.clone(), tick);
                    if self.signals.subscriber_count(&dep) > 0 {
                        self.schedule_recompute(&dep);
                    }
                }
                frontier.push(dep);
            }
        }
    }

    fn mark_node_stale(&self, key: &str) {
        if let Some(e) = self.cache.lock().unwrap().peek_mut(key) {
            e.stale = true;
            return;
        }
        if let Some(r) = self.render_cache.lock().unwrap().peek_mut(key) {
            r.stale = true;
        }
    }

    fn schedule_recompute(&self, key: &str) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        if self.recompute_pending.insert(key.to_string(), ()).is_some() {
            return;
        }
        let engine = self.clone();
        let key = key.to_string();
        handle.spawn(async move {
            tokio::time::sleep(RECOMPUTE_DEBOUNCE).await;
            engine.recompute_pending.remove(&key);
            engine.recompute_node(&key).await;
        });
    }

    async fn recompute_node(&self, key: &str) {
        let query_spec = self
            .cache
            .lock()
            .unwrap()
            .peek(key)
            .map(|e| (e.instance_id.clone(), e.params.clone(), e.principal.clone()));
        if let Some((inst_id, params, principal)) = query_spec {
            let _ = self.query(&inst_id, params, &principal, true).await;
            return;
        }
        let render_spec = self.render_cache.lock().unwrap().peek(key).map(|r| {
            (
                r.dashboard_id.clone(),
                r.adapter_instance.clone(),
                r.query_params.clone(),
                r.principal.clone(),
            )
        });
        if let Some((dashboard_id, adapter, params, principal)) = render_spec {
            let g = self.graph.load_full();
            let Some(dash) = g.dashboards.get(&dashboard_id).cloned() else {
                return;
            };
            if let Some(placement) = dash
                .adapters
                .iter()
                .find(|p| p.adapter_instance == adapter && p.query_params == params)
                .cloned()
            {
                let _ = self
                    .render_placement(&g, &dash, &placement, &principal, true)
                    .await;
            }
        }
    }

    /// Schedule a background retry (exponential backoff with jitter, not before an open breaker
    /// closes) for a failing query with active subscribers (§15.2). Deduplicated per query_key.
    fn schedule_retry(
        &self,
        job: &Arc<QueryJob>,
        bump_attempt: bool,
        not_before: Option<Instant>,
    ) -> Option<RetryInfo> {
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let attempt = if bump_attempt {
            let mut a = self
                .retry_attempts
                .entry(job.query_key.clone())
                .or_insert(0);
            *a += 1;
            *a
        } else {
            self.retry_attempts
                .get(&job.query_key)
                .map(|a| *a)
                .unwrap_or(1)
        };
        let circuit_open = not_before.is_some();
        if let Some(at) = self.retry_pending.get(&job.query_key) {
            return Some(RetryInfo {
                attempt,
                next_retry_at: Some(at.to_rfc3339()),
                circuit_open,
            });
        }
        let mut delay = retry_delay(attempt);
        if let Some(nb) = not_before {
            let until_open = nb.saturating_duration_since(Instant::now());
            if until_open > delay {
                delay = until_open + Duration::from_millis(10);
            }
        }
        let next_at = Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default();
        if self
            .retry_pending
            .insert(job.query_key.clone(), next_at)
            .is_none()
        {
            let engine = self.clone();
            let job = Arc::clone(job);
            handle.spawn(async move {
                tokio::time::sleep(delay).await;
                engine.retry_pending.remove(&job.query_key);
                if engine.signals.subscriber_count(&job.query_key) > 0 {
                    let _ = engine
                        .query(&job.inst.id, job.params.clone(), &job.principal, true)
                        .await;
                }
            });
        }
        Some(RetryInfo {
            attempt,
            next_retry_at: Some(next_at.to_rfc3339()),
            circuit_open,
        })
    }

    pub(crate) fn breaker_admit(&self, key: &str) -> Admission {
        let Some(mut st) = self.circuit_breakers.get_mut(key) else {
            return Admission::Closed;
        };
        match st.open_until {
            None => Admission::Closed,
            Some(until) if Instant::now() < until => Admission::Rejected { until: Some(until) },
            Some(_) if st.probing => Admission::Rejected { until: None },
            Some(_) => {
                st.probing = true;
                Admission::Probe
            }
        }
    }

    /// Count one execution failure; returns true when this failure (re)opens the breaker.
    fn breaker_failure(&self, key: &str) -> bool {
        let mut st = self.circuit_breakers.entry(key.to_string()).or_default();
        st.consecutive_failures += 1;
        st.probing = false;
        if st.consecutive_failures >= BREAKER_THRESHOLD {
            let exp = (st.consecutive_failures - BREAKER_THRESHOLD).min(BREAKER_MAX_EXP);
            let window = jittered(BREAKER_BASE_BACKOFF.saturating_mul(1u32 << exp));
            st.open_until = Some(Instant::now() + window);
            true
        } else {
            false
        }
    }

    fn breaker_success(&self, key: &str) {
        self.circuit_breakers.remove(key);
    }

    fn breaker_release_probe(&self, key: &str) {
        if let Some(mut st) = self.circuit_breakers.get_mut(key) {
            st.probing = false;
        }
    }

    fn breaker_open_until(&self, key: &str) -> Option<Instant> {
        self.circuit_breakers
            .get(key)
            .and_then(|st| st.open_until)
            .filter(|until| *until > Instant::now())
    }

    /// Render a single Dashboard by evaluating all its placed Adapter instances and merging overrides.
    pub async fn render_dashboard(
        &self,
        dashboard_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        self.render_dashboard_in(self.graph.load_full(), dashboard_id, principal)
            .await
    }

    /// Render `dashboard_id` entirely from the Generation snapshot `g`. Callers that combine
    /// the render with other Generation data (the Dashboard document, widget asset URLs) pass
    /// the same snapshot so a concurrent activation can never produce a mixed response (§17.13).
    pub async fn render_dashboard_in(
        &self,
        g: Arc<ConfiguredGraph>,
        dashboard_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        let dash = g
            .dashboards
            .get(dashboard_id)
            .ok_or_else(|| format!("dashboard '{dashboard_id}' not found"))?
            .clone();

        self.state
            .seed_dashboard_defaults(&dash, g.generation_id)
            .map_err(|e| e.to_string())?;

        let mut owners: HashMap<String, String> = HashMap::new();
        let mut diagnostics = Vec::new();
        let mut adapter_trees = Vec::new();
        for placement in &dash.adapters {
            let out = self
                .render_placement(&g, &dash, placement, principal, false)
                .await?;
            // Derive stable `widget_id`s, apply Dashboard layout overrides (§8) and check every
            // Widget node against its widget provider's contract (§4.2, §10.1).
            let first_diag = diagnostics.len();
            let tree = annotate_widget_ids(
                &g,
                out.tree.clone(),
                &placement.adapter_instance,
                &dash.overrides,
                &mut owners,
                &mut diagnostics,
            )?;
            adapter_trees.push(serde_json::json!({
                "adapter_instance": placement.adapter_instance,
                "area": placement.area,
                "tree": tree,
                "status": out.status,
                "updated_at": out.updated_at.to_rfc3339(),
                "signal_key": out.render_key,
                "diagnostics": diagnostics[first_diag..],
            }));
        }
        for d in diagnostics {
            self.telemetry
                .log("warn", "query_engine", &principal.id, "", &d);
        }
        self.rendered_widgets
            .insert(dashboard_id.to_string(), (g.generation_id, owners));

        let dash_state_recs = self
            .state
            .list_dashboard_state(dashboard_id)
            .map_err(|e| e.to_string())?;
        let mut dash_state = BTreeMap::new();
        // Per-key record versions so Clients can write with `expected_version` (CAS, §8).
        let mut dash_state_versions = BTreeMap::new();
        for r in dash_state_recs {
            dash_state_versions.insert(r.state_key.clone(), r.version);
            dash_state.insert(r.state_key, r.value);
        }

        Ok(serde_json::json!({
            "dashboard_id": dash.id,
            "title": dash.title,
            "layout": dash.layout,
            "generation": g.generation_id,
            "dashboard_state": dash_state,
            "dashboard_state_versions": dash_state_versions,
            "overrides": dash.overrides,
            "adapters": adapter_trees,
        }))
    }

    /// Adapter Instance that owns `widget_id` in the current render of `dashboard_id`, rendering
    /// the dashboard first if this Generation has not rendered it yet (Appendix A.3: Requests for
    /// widget_ids that are not currently bound are rejected).
    pub async fn widget_owner(
        &self,
        dashboard_id: &str,
        widget_id: &str,
        principal: &Principal,
    ) -> Result<Option<String>, String> {
        let generation = self.graph.load().generation_id;
        let lookup = |engine: &Self| {
            engine
                .rendered_widgets
                .get(dashboard_id)
                .filter(|e| e.0 == generation)
                .and_then(|e| e.1.get(widget_id).cloned())
        };
        if let Some(owner) = lookup(self) {
            return Ok(Some(owner));
        }
        self.render_dashboard(dashboard_id, principal).await?;
        Ok(lookup(self))
    }

    fn render_key(
        &self,
        g: &ConfiguredGraph,
        dashboard_id: &str,
        inst: &InstanceConfig,
        def: &LoadedDefinition,
        query_params: &serde_json::Value,
        principal: &Principal,
    ) -> String {
        let mut hasher = Sha256::new();
        for part in [
            "render",
            dashboard_id,
            &inst.id,
            &def.manifest.identity_version,
            &inst.config_digest(),
            &canonicalize_json(query_params),
            if g.is_preview { "preview" } else { "live" },
            &principal.authorization_fingerprint(),
        ] {
            hasher.update(part.as_bytes());
            hasher.update(b"\0");
        }
        format!("render:{}", hex::encode(&hasher.finalize()[..16]))
    }

    /// Render keys (the `signal_key`s of `render_dashboard`) of `dashboard_id`'s Adapter
    /// placements for `principal` in the active Generation, without rendering or checking
    /// access. Lets a live Dashboard subscription register as a Signal consumer of what its
    /// renders read (via [`Self::recorded_dependencies`]) even when a render failed.
    pub fn dashboard_render_keys(&self, dashboard_id: &str, principal: &Principal) -> Vec<String> {
        let g = self.graph.load_full();
        let Some(dash) = g.dashboards.get(dashboard_id) else {
            return Vec::new();
        };
        dash.adapters
            .iter()
            .filter_map(|placement| {
                let inst = g.instances.get(&placement.adapter_instance)?;
                let def = g.definitions.get(&inst.definition)?;
                Some(self.render_key(&g, &dash.id, inst, def, &placement.query_params, principal))
            })
            .collect()
    }

    /// Evaluate one Adapter placement, sharing, caching and deduplicating the output like a
    /// Query result (ADR-0001). The cache entry stays valid while the Dashboard State it read
    /// and the upstream query versions it observed are unchanged.
    async fn render_placement(
        &self,
        g: &Arc<ConfiguredGraph>,
        dash: &DashboardDoc,
        placement: &DashboardAdapterPlacement,
        principal: &Principal,
        force: bool,
    ) -> Result<RenderOutcome, String> {
        let inst = g
            .instances
            .get(&placement.adapter_instance)
            .ok_or_else(|| {
                format!(
                    "adapter instance '{}' not found",
                    placement.adapter_instance
                )
            })?
            .clone();
        let def = g
            .definitions
            .get(&inst.definition)
            .ok_or_else(|| format!("adapter definition '{}' missing", inst.definition))?
            .clone();

        audited(
            &self.store,
            principal,
            "provider.query",
            check_provider_access(principal, &inst),
        )
        .map_err(|e| e.to_string())?;

        let render_key =
            self.render_key(g, &dash.id, &inst, &def, &placement.query_params, principal);
        if !force {
            if let Some(hit) = self.render_cache_hit(&render_key, g.generation_id) {
                return Ok(hit);
            }
        }

        let not_before = self.invalidation_tick(&inst.id, &render_key);
        let instance_id = inst.id.clone();
        let job = Arc::new(RenderJob {
            graph: Arc::clone(g),
            dashboard: dash.clone(),
            inst,
            def,
            query_params: placement.query_params.clone(),
            principal: principal.clone(),
            render_key: render_key.clone(),
        });
        let engine = self.clone();
        self.join_or_lead(
            &self.render_in_flight,
            &render_key,
            &instance_id,
            not_before,
            &WaitCtx::default(),
            move |tick, cancel| {
                let engine = engine.clone();
                let job = Arc::clone(&job);
                async move { engine.run_render_leader(job, tick, cancel).await }
            },
        )
        .await
    }

    fn render_cache_hit(&self, render_key: &str, generation_id: i64) -> Option<RenderOutcome> {
        let entry = self.render_cache.lock().unwrap().get(render_key).cloned()?;
        let age = Utc::now()
            .signed_duration_since(entry.computed_at)
            .to_std()
            .unwrap_or(Duration::MAX);
        if entry.stale || entry.generation_id != generation_id || age >= self.cache_ttl {
            return None;
        }
        for (key, seen_version) in &entry.state_reads {
            let current = self
                .state
                .get("dashboard", &entry.dashboard_id, key)
                .ok()?
                .map(|r| r.version);
            if current != *seen_version {
                return None;
            }
        }
        let cache = self.cache.lock().unwrap();
        for (qk, seen_version) in &entry.upstreams {
            if let Some(v) = seen_version {
                match cache.peek(qk) {
                    Some(e) if e.version == *v && !e.stale => {}
                    _ => return None,
                }
            }
        }
        Some(entry.outcome)
    }

    async fn run_render_leader(
        self,
        job: Arc<RenderJob>,
        start_tick: u64,
        cancel: CancelSignal,
    ) -> Result<RenderOutcome, String> {
        let adapter_id = job.inst.id.clone();
        let admission = self.breaker_admit(&adapter_id);
        if let Admission::Rejected { .. } = admission {
            let cached = self
                .render_cache
                .lock()
                .unwrap()
                .peek(&job.render_key)
                .map(|r| r.outcome.clone());
            return match cached {
                Some(mut out) => {
                    out.status = QueryStatus::Stale;
                    Ok(out)
                }
                None => Err(format!("circuit breaker open for adapter '{adapter_id}'")),
            };
        }

        let trace_id = format!("tr_{}", Uuid::now_v7().simple());
        let started = Instant::now();
        let mut payload = job.query_params.clone();
        if let Some(obj) = payload.as_object_mut() {
            obj.insert(
                "dashboard_id".into(),
                serde_json::Value::String(job.dashboard.id.clone()),
            );
        }

        let recorded_deps = Arc::new(Mutex::new(BTreeSet::new()));
        let recorded_upstreams = Arc::new(Mutex::new(Vec::new()));
        let recorded_state = Arc::new(Mutex::new(BTreeMap::new()));
        let bridge = Arc::new(EngineHostBridge {
            engine: self.clone(),
            graph: Arc::clone(&job.graph),
            caller_inst: job.inst.clone(),
            caller_def: job.def.clone(),
            principal: job.principal.clone(),
            trace_id: trace_id.clone(),
            parent_query_key: Some(job.render_key.clone()),
            call_stack: vec![job.render_key.clone()],
            recorded_deps: Arc::clone(&recorded_deps),
            recorded_upstreams: Arc::clone(&recorded_upstreams),
            recorded_state: Arc::clone(&recorded_state),
            dashboard_id: Some(job.dashboard.id.clone()),
            operation: None,
            deadline: self.host_await_deadline(&job.inst.id),
            cancel: cancel.clone(),
            outcome_unknown: Arc::new(AtomicBool::new(false)),
            batches: BatchMap::default(),
        });
        let bundle = CompiledInstanceBundle {
            instance_id: job.inst.id.clone(),
            manifest: job.def.manifest.clone(),
            runtime_source: job.def.runtime_source.clone(),
            config_digest: job.inst.config_digest(),
            resolved_libraries: job.graph.resolved_libraries(),
        };
        let handler = if job.def.manifest.handler.is_empty() {
            "render"
        } else {
            job.def.manifest.handler.as_str()
        };

        let res = self
            .pool
            .execute_handler(
                &bundle,
                handler,
                ExecutionContextConfig {
                    category: DefinitionCategory::Adapter,
                    instance_id: job.inst.id.clone(),
                    input_names: job.inst.inputs.keys().cloned().collect(),
                    config: job.inst.config.clone(),
                    payload,
                    operation: None,
                    idempotency_key: None,
                    allowed_emits: vec![],
                    now_iso: Utc::now().to_rfc3339(),
                    trace_id: trace_id.clone(),
                    bridge,
                },
                Some(cancel.vm_flag()),
            )
            .await;
        let duration_ms = started.elapsed().as_millis() as i64;
        let deps = recorded_deps.lock().unwrap().clone();
        let upstreams = recorded_upstreams.lock().unwrap().clone();
        let state_reads = recorded_state.lock().unwrap().clone();

        if cancel.is_cancelled() || matches!(res, Err(RuntimeError::Cancelled)) {
            if admission == Admission::Probe {
                self.breaker_release_probe(&adapter_id);
            }
            return Err(format!("adapter '{adapter_id}' render cancelled"));
        }
        self.reconcile_dynamic_edges(&job.render_key, deps);
        self.record_runtime_outcome(&adapter_id, admission, &res, true);

        let tree = match res {
            Ok(tree) => tree,
            Err(e) => {
                let msg = self.store.redactor().redact_str(&e.to_string());
                self.telemetry.record_span(
                    &trace_id,
                    None,
                    &format!("render:{adapter_id}"),
                    &job.principal.id,
                    serde_json::json!({ "dashboard_id": job.dashboard.id, "error": msg }),
                    "error",
                    duration_ms,
                );
                return Err(msg);
            }
        };

        let (status, updated_at) = derive_status(&upstreams);
        let outcome = RenderOutcome {
            tree,
            status,
            updated_at,
            version: self.next_version(),
            render_key: job.render_key.clone(),
        };
        let upstream_moved = upstreams_moved(&self.cache.lock().unwrap(), &upstreams);
        let written = {
            let mut renders = self.render_cache.lock().unwrap();
            let outdated = renders
                .peek(&job.render_key)
                .map(|p| p.start_tick > start_tick)
                .unwrap_or(false)
                || self.instance_invalidation_tick(&adapter_id) > start_tick
                || upstream_moved;
            if outdated {
                None
            } else {
                Some(
                    renders.push(
                        job.render_key.clone(),
                        CachedRender {
                            dashboard_id: job.dashboard.id.clone(),
                            adapter_instance: adapter_id.clone(),
                            query_params: job.query_params.clone(),
                            principal: job.principal.clone(),
                            generation_id: job.graph.generation_id,
                            outcome: outcome.clone(),
                            computed_at: Utc::now(),
                            start_tick,
                            stale: false,
                            state_reads,
                            upstreams: upstreams
                                .iter()
                                .map(|u| (u.query_key.clone(), u.cached_version))
                                .collect(),
                        },
                    ),
                )
            }
        };
        let was_written = written.is_some();
        if let Some((evicted_key, _)) = written.flatten() {
            if evicted_key != job.render_key {
                self.on_node_evicted(&evicted_key);
            }
        }
        if was_written {
            self.key_invalidated
                .remove_if(&job.render_key, |_, t| *t < start_tick);
            // Rendered output is a Signal like any Query result (ADR-0001).
            if self.signals.subscriber_count(&job.render_key) > 0 {
                let mut owners = HashMap::new();
                let mut diags = Vec::new();
                if let Ok(annotated) = annotate_widget_ids(
                    &job.graph,
                    outcome.tree.clone(),
                    &adapter_id,
                    &job.dashboard.overrides,
                    &mut owners,
                    &mut diags,
                ) {
                    let mut env = QueryEnvelope::snapshot(
                        &adapter_id,
                        &job.render_key,
                        job.graph.generation_id,
                        outcome.version,
                        status,
                        Some("dash.widget_tree@1".into()),
                        annotated,
                        None,
                    );
                    env.updated_at = updated_at.to_rfc3339();
                    self.signals.publish_query(env);
                }
            }
        }

        self.telemetry.record_span(
            &trace_id,
            None,
            &format!("render:{adapter_id}"),
            &job.principal.id,
            serde_json::json!({
                "dashboard_id": job.dashboard.id,
                "render_key": job.render_key,
                "status": status_str(status),
            }),
            "ok",
            duration_ms,
        );
        Ok(outcome)
    }
}

/// Whether any cached upstream observed by a run has a different version now.
fn upstreams_moved(
    cache: &LruCache<String, CachedQueryEntry>,
    observed: &[UpstreamObservation],
) -> bool {
    observed.iter().any(|u| match u.cached_version {
        Some(seen) => cache
            .peek(&u.query_key)
            .map(|e| e.version != seen)
            .unwrap_or(false),
        None => false,
    })
}

fn status_str(status: QueryStatus) -> &'static str {
    match status {
        QueryStatus::Fresh => "fresh",
        QueryStatus::Stale => "stale",
        QueryStatus::Loading => "loading",
        QueryStatus::Error => "error",
    }
}

/// A dependent's status is the worst of its upstreams: computed over any non-fresh upstream it is
/// `stale`, and its last successful update is the oldest such upstream's (§6.1, §15.2).
fn derive_status(upstreams: &[UpstreamObservation]) -> (QueryStatus, DateTime<Utc>) {
    let now = Utc::now();
    let oldest_degraded = upstreams
        .iter()
        .filter(|u| u.status != QueryStatus::Fresh)
        .map(|u| u.updated_at)
        .min();
    match oldest_degraded {
        None => (QueryStatus::Fresh, now),
        Some(at) => (QueryStatus::Stale, at.min(now)),
    }
}

fn jittered(d: Duration) -> Duration {
    let factor = rand::thread_rng().gen_range((1.0 - BACKOFF_JITTER)..=(1.0 + BACKOFF_JITTER));
    d.mul_f64(factor)
}

fn retry_delay(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(1).min(16);
    jittered(
        RETRY_BASE_DELAY
            .saturating_mul(1u32 << exp)
            .min(RETRY_MAX_DELAY),
    )
}

fn instant_to_utc(at: Instant) -> DateTime<Utc> {
    let ahead = at.saturating_duration_since(Instant::now());
    Utc::now() + chrono::Duration::from_std(ahead).unwrap_or_default()
}

/// Assign stable `widget_id = "{adapter_instance}_{node key}"` to every Widget node (§8), apply
/// Dashboard layout overrides, and register each id in `owners` (shared across the Adapters of
/// one dashboard). Adapter-supplied `widget_id`/`layout_override` fields are ignored, keyless
/// non-root nodes get a positional fallback key (reported in `diagnostics`), and any duplicate
/// widget_id is a render error.
///
/// Adapter output is untrusted (§4.2): every node naming a `widget_type` is checked against the
/// Generation (`graph`): the type must be a widget provider and `props` must satisfy its declared
/// `props` schema. A violating node is replaced by a `dash.text` error node carrying
/// `render_error` (same `widget_id`), and a `WIDGET_CONTRACT_VIOLATION` diagnostic is recorded,
/// so the rest of the tree still renders and invalid props never reach a widget.
fn annotate_widget_ids(
    graph: &ConfiguredGraph,
    node: serde_json::Value,
    adapter_instance: &str,
    overrides: &BTreeMap<String, crate::model::LayoutOverride>,
    owners: &mut HashMap<String, String>,
    diagnostics: &mut Vec<String>,
) -> Result<serde_json::Value, String> {
    annotate_node(
        graph,
        node,
        adapter_instance,
        overrides,
        owners,
        diagnostics,
        None,
    )
}

/// Check one Widget node's `widget_type` and `props` (normalizing empty-list props in place).
/// Built-in `dash.*` primitives are checked when the Generation registers them (Workspace graphs
/// always do); bare graphs without the built-in catalog accept them unchecked.
fn check_widget_contract(
    graph: &ConfiguredGraph,
    obj: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let Some(widget_type) = obj.get("widget_type") else {
        return Ok(());
    };
    let Some(widget_type) = widget_type.as_str().map(str::to_string) else {
        return Err(format!("widget_type must be a string, got {widget_type}"));
    };
    if widget_type.starts_with("dash.") && !graph.definitions.contains_key(&widget_type) {
        return Ok(());
    }
    if let Some(schema) = graph
        .definitions
        .get(&widget_type)
        .and_then(|d| d.manifest.widget_props.as_ref())
    {
        if let Some(p) = obj.get_mut("props") {
            schema.normalize_empty_lists(p);
        }
    }
    let empty = serde_json::Value::Object(serde_json::Map::new());
    let props = obj.get("props").unwrap_or(&empty);
    graph.check_widget_node(&widget_type, props)
}

fn annotate_node(
    graph: &ConfiguredGraph,
    mut node: serde_json::Value,
    adapter_instance: &str,
    overrides: &BTreeMap<String, crate::model::LayoutOverride>,
    owners: &mut HashMap<String, String>,
    diagnostics: &mut Vec<String>,
    position: Option<(&str, usize)>,
) -> Result<serde_json::Value, String> {
    let Some(obj) = node.as_object_mut() else {
        return Ok(node);
    };
    let explicit = match obj.get("key") {
        Some(serde_json::Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        _ => None,
    };
    let node_key = match (explicit, position) {
        (Some(k), _) => k,
        (None, None) => "root".to_string(),
        (None, Some((parent, idx))) => {
            let k = format!("{parent}/{idx}");
            diagnostics.push(format!(
                "widget node '{k}' of adapter '{adapter_instance}' has no key; widget_id falls back to its position and will not survive reordering"
            ));
            k
        }
    };
    let widget_id = format!("{adapter_instance}_{node_key}");
    if let Some(prev_owner) = owners.get(&widget_id) {
        return Err(format!(
            "DUPLICATE_WIDGET_KEY: widget_id '{widget_id}' (node key '{node_key}' of adapter '{adapter_instance}') is already used by a node of adapter '{prev_owner}'"
        ));
    }
    owners.insert(widget_id.clone(), adapter_instance.to_string());
    obj.insert(
        "widget_id".into(),
        serde_json::Value::String(widget_id.clone()),
    );
    obj.remove("layout_override");
    if let Some(ov) = overrides.get(&widget_id) {
        obj.insert(
            "layout_override".into(),
            serde_json::to_value(ov).unwrap_or(serde_json::Value::Null),
        );
    }

    if let Err(e) = check_widget_contract(graph, obj) {
        let msg = format!(
            "WIDGET_CONTRACT_VIOLATION: widget '{widget_id}' of adapter '{adapter_instance}': {e}"
        );
        diagnostics.push(msg.clone());
        let mut err_node = serde_json::json!({
            "key": node_key,
            "widget_id": widget_id,
            "widget_type": "dash.text",
            "props": { "title": "Widget unavailable", "text": msg },
            "render_error": msg,
        });
        if let Some(ov) = obj.get("layout_override") {
            err_node["layout_override"] = ov.clone();
        }
        return Ok(err_node);
    }

    if let Some(children) = obj.get_mut("children").and_then(|v| v.as_array_mut()) {
        let taken = std::mem::take(children);
        let mut annotated = Vec::with_capacity(taken.len());
        for (idx, child) in taken.into_iter().enumerate() {
            annotated.push(annotate_node(
                graph,
                child,
                adapter_instance,
                overrides,
                owners,
                diagnostics,
                Some((&node_key, idx)),
            )?);
        }
        *children = annotated;
    }
    Ok(node)
}

fn canonicalize_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<String, serde_json::Value> = map
                .iter()
                .map(|(k, val)| (k.clone(), val.clone()))
                .collect();
            let mut entries = Vec::with_capacity(sorted.len());
            for (k, val) in sorted {
                entries.push(format!(
                    "{}:{}",
                    serde_json::to_string(&k).unwrap_or_default(),
                    canonicalize_json(&val)
                ));
            }
            format!("{{{}}}", entries.join(","))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(canonicalize_json).collect();
            format!("[{}]", items.join(","))
        }
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Reserved dependency-query parameter carrying query options (§6.1 pushdown):
/// `{ filter = { field = value }, sort = { { field = "f", desc = true } } | { "f" },
/// page = { offset = 0, limit = 50 }, fields = { "f", ... } }`. Options the target provider
/// declares (`pushdown = { ... }`) are passed to it under this key; the host applies the rest
/// to the provider's result, so a consumer gets the same rows either way.
pub const PUSHDOWN_PARAM: &str = "_pushdown";

/// Parsed query options of one dependency query.
#[derive(Debug, Clone, Default, PartialEq)]
struct QueryOptions {
    filter: Option<serde_json::Map<String, serde_json::Value>>,
    sort: Option<Vec<(String, bool)>>,
    page: Option<(usize, Option<usize>)>,
    fields: Option<Vec<String>>,
}

impl QueryOptions {
    fn is_empty(&self) -> bool {
        self.filter.is_none() && self.sort.is_none() && self.page.is_none() && self.fields.is_none()
    }

    fn parse(v: &serde_json::Value) -> Result<Self, String> {
        let bad = |m: &str| format!("invalid {PUSHDOWN_PARAM}: {m}");
        let Some(obj) = v.as_object() else {
            return Err(bad("expected a table"));
        };
        let mut out = QueryOptions::default();
        for (k, val) in obj {
            match k.as_str() {
                "filter" => {
                    let f = val
                        .as_object()
                        .ok_or_else(|| bad("filter must map fields to values"))?;
                    if !f.is_empty() {
                        out.filter = Some(f.clone());
                    }
                }
                "sort" => {
                    let items = val.as_array().ok_or_else(|| bad("sort must be a list"))?;
                    let mut keys = Vec::new();
                    for item in items {
                        match item {
                            serde_json::Value::String(f) => keys.push((f.clone(), false)),
                            serde_json::Value::Object(o) => {
                                let f = o
                                    .get("field")
                                    .and_then(|x| x.as_str())
                                    .ok_or_else(|| bad("sort entries need a 'field'"))?;
                                let desc = o.get("desc").and_then(|x| x.as_bool()).unwrap_or(false);
                                keys.push((f.to_string(), desc));
                            }
                            _ => return Err(bad("sort entries must be names or { field, desc }")),
                        }
                    }
                    if !keys.is_empty() {
                        out.sort = Some(keys);
                    }
                }
                "page" => {
                    let p = val
                        .as_object()
                        .ok_or_else(|| bad("page must be { offset, limit }"))?;
                    let num = |name: &str| -> Result<Option<usize>, String> {
                        match p.get(name) {
                            None | Some(serde_json::Value::Null) => Ok(None),
                            Some(x) => x.as_u64().map(|n| Some(n as usize)).ok_or_else(|| {
                                bad(&format!("page.{name} must be a non-negative integer"))
                            }),
                        }
                    };
                    out.page = Some((num("offset")?.unwrap_or(0), num("limit")?));
                }
                "fields" => {
                    let items = val
                        .as_array()
                        .ok_or_else(|| bad("fields must be a list of names"))?;
                    let names = items
                        .iter()
                        .map(|x| x.as_str().map(str::to_string))
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| bad("fields must be a list of names"))?;
                    out.fields = Some(names);
                }
                other => return Err(bad(&format!("unknown option '{other}'"))),
            }
        }
        Ok(out)
    }

    fn to_json(&self) -> serde_json::Value {
        let mut o = serde_json::Map::new();
        if let Some(f) = &self.filter {
            o.insert("filter".into(), serde_json::Value::Object(f.clone()));
        }
        if let Some(s) = &self.sort {
            o.insert(
                "sort".into(),
                serde_json::Value::Array(
                    s.iter()
                        .map(|(f, d)| serde_json::json!({ "field": f, "desc": d }))
                        .collect(),
                ),
            );
        }
        if let Some((offset, limit)) = self.page {
            o.insert(
                "page".into(),
                serde_json::json!({ "offset": offset, "limit": limit }),
            );
        }
        if let Some(f) = &self.fields {
            o.insert("fields".into(), serde_json::json!(f));
        }
        serde_json::Value::Object(o)
    }

    /// Split into the options the provider evaluates (declared in `spec`) and the ones the host
    /// applies. Options compose in the fixed order filter, sort, page, projection: once a stage
    /// stays on the host, every later stage does too, so pushing never changes the result.
    fn split(&self, spec: &dash_luau::manifest::PushdownSpec) -> (QueryOptions, QueryOptions) {
        let mut pushed = QueryOptions::default();
        let mut host = QueryOptions::default();
        let mut chain = true;
        if let Some(f) = &self.filter {
            chain = f.keys().all(|k| spec.filter.iter().any(|d| d == k));
            if chain {
                pushed.filter = Some(f.clone());
            } else {
                host.filter = Some(f.clone());
            }
        }
        if let Some(s) = &self.sort {
            chain = chain && s.iter().all(|(k, _)| spec.sort.iter().any(|d| d == k));
            if chain {
                pushed.sort = Some(s.clone());
            } else {
                host.sort = Some(s.clone());
            }
        }
        if let Some(p) = self.page {
            chain = chain && spec.page;
            if chain {
                pushed.page = Some(p);
            } else {
                host.page = Some(p);
            }
        }
        if let Some(f) = &self.fields {
            chain = chain && spec.projection;
            if chain {
                pushed.fields = Some(f.clone());
            } else {
                host.fields = Some(f.clone());
            }
        }
        (pushed, host)
    }

    /// Apply the options on the host, in order filter, sort, page, projection.
    fn apply(&self, value: serde_json::Value) -> Result<serde_json::Value, String> {
        if self.is_empty() {
            return Ok(value);
        }
        let serde_json::Value::Array(mut rows) = value else {
            return Err(format!(
                "{PUSHDOWN_PARAM} options require a list result from the provider"
            ));
        };
        if let Some(f) = &self.filter {
            rows.retain(|r| f.iter().all(|(k, v)| r.get(k) == Some(v)));
        }
        if let Some(keys) = &self.sort {
            rows.sort_by(|a, b| {
                for (k, desc) in keys {
                    let ord = cmp_json(
                        a.get(k).unwrap_or(&serde_json::Value::Null),
                        b.get(k).unwrap_or(&serde_json::Value::Null),
                    );
                    let ord = if *desc { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        if let Some((offset, limit)) = self.page {
            let end = limit.map_or(rows.len(), |l| offset.saturating_add(l).min(rows.len()));
            rows = if offset >= rows.len() {
                Vec::new()
            } else {
                rows[offset..end].to_vec()
            };
        }
        if let Some(fields) = &self.fields {
            for r in rows.iter_mut() {
                if let Some(o) = r.as_object_mut() {
                    o.retain(|k, _| fields.iter().any(|f| f == k));
                }
            }
        }
        Ok(serde_json::Value::Array(rows))
    }
}

/// Total order over JSON scalars used by host-side sorting: null < bool < number < string <
/// other; values of one kind compare naturally.
fn cmp_json(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value as V;
    fn rank(v: &V) -> u8 {
        match v {
            V::Null => 0,
            V::Bool(_) => 1,
            V::Number(_) => 2,
            V::String(_) => 3,
            _ => 4,
        }
    }
    match (a, b) {
        (V::Bool(x), V::Bool(y)) => x.cmp(y),
        (V::Number(x), V::Number(y)) => x
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&y.as_f64().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal),
        (V::String(x), V::String(y)) => x.cmp(y),
        _ => rank(a).cmp(&rank(b)),
    }
}

/// Comparable identity of a batch key / row key value.
fn batch_key_repr(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

type BatchWaiter = (
    Vec<serde_json::Value>,
    tokio::sync::oneshot::Sender<Result<serde_json::Value, String>>,
);

/// Dependency queries of one execution waiting to be merged into one batched query (§6.1).
#[derive(Default)]
struct BatchSlot {
    keys: Vec<serde_json::Value>,
    seen: HashSet<String>,
    waiters: Vec<BatchWaiter>,
}

type BatchMap = Arc<Mutex<HashMap<String, BatchSlot>>>;

/// Removes a batch group if its leader is dropped before dispatching it, so followers observe a
/// closed channel instead of waiting forever.
struct BatchLeaderGuard {
    map: BatchMap,
    group: String,
    armed: bool,
}

impl Drop for BatchLeaderGuard {
    fn drop(&mut self) {
        if self.armed {
            self.map.lock().unwrap().remove(&self.group);
        }
    }
}

pub(crate) struct EngineHostBridge {
    pub engine: QueryEngine,
    pub graph: Arc<ConfiguredGraph>,
    pub caller_inst: InstanceConfig,
    pub caller_def: LoadedDefinition,
    pub principal: Principal,
    pub trace_id: String,
    pub parent_query_key: Option<String>,
    pub call_stack: Vec<String>,
    pub recorded_deps: Arc<Mutex<BTreeSet<String>>>,
    recorded_upstreams: Arc<Mutex<Vec<UpstreamObservation>>>,
    /// Dashboard State keys read (with record version) during an Adapter render.
    pub recorded_state: Arc<Mutex<BTreeMap<String, Option<i64>>>>,
    /// Dashboard being rendered; Dashboard State reads resolve against it only.
    pub dashboard_id: Option<String>,
    pub operation: Option<String>,
    /// Fallback deadline of this execution (the Instance's effective budget, §2.1 `[resources]`)
    /// for host awaits polled outside the VM's own deadline scope. Inside a Luau invocation the
    /// VM's deadline (task-local, set after admission) bounds every nested `ctx:await` (§6.3).
    pub deadline: Instant,
    cancel: CancelSignal,
    /// Set when a possibly side-effecting HTTP request of this execution ended without a known
    /// outcome (abandoned in flight, or the broker reported `UNKNOWN_OUTCOME`), so the connector
    /// invocation reports `unknown` even if the handler swallowed the error (§9.1).
    outcome_unknown: Arc<AtomicBool>,
    /// Dependency queries of this execution waiting to be merged by batch-capable providers.
    batches: BatchMap,
}

impl EngineHostBridge {
    fn wait_ctx(&self) -> WaitCtx {
        WaitCtx {
            deadline: Some(dash_luau::current_execution_deadline().unwrap_or(self.deadline)),
            cancel: Some(self.cancel.clone()),
        }
    }
}

/// Safe HTTP methods have no upstream side effect, so abandoning one never makes an outcome
/// uncertain.
fn is_safe_http_method(method_upper: &str) -> bool {
    matches!(method_upper, "GET" | "HEAD" | "OPTIONS")
}

impl EngineHostBridge {
    /// Run one dependency query against `target_id` under this execution's Generation,
    /// deadline and cancellation, recording the dependency edge (also when it fails).
    async fn query_dependency(
        &self,
        target_id: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let result = self
            .engine
            .query_inner(
                target_id,
                params.clone(),
                &self.principal,
                false,
                self.call_stack.clone(),
                self.parent_query_key.clone(),
                self.wait_ctx(),
                Some(Arc::clone(&self.graph)),
            )
            .await;

        match result {
            Ok(env) => {
                self.recorded_deps
                    .lock()
                    .unwrap()
                    .insert(env.query_key.clone());
                let cached_version = self
                    .engine
                    .cached_version(&env.query_key)
                    .map(|_| env.version);
                let updated_at = DateTime::parse_from_rfc3339(&env.updated_at)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());
                self.recorded_upstreams
                    .lock()
                    .unwrap()
                    .push(UpstreamObservation {
                        query_key: env.query_key.clone(),
                        status: env.status,
                        updated_at,
                        cached_version,
                    });
                Ok(env.value.unwrap_or(serde_json::Value::Null))
            }
            Err(e) => {
                // A failed dependency is still a dependency of this run (§6.1).
                if let Some(target_inst) = self.graph.instances.get(target_id) {
                    if let Some(child_qk) = self.engine.query_key_for(
                        &self.graph,
                        target_inst,
                        &params,
                        &self.principal,
                    ) {
                        self.recorded_deps.lock().unwrap().insert(child_qk);
                    }
                }
                Err(e)
            }
        }
    }

    /// §6.1 batching: concurrent dependency queries of this execution (e.g. `ctx:await_all`)
    /// to a batch-capable provider that differ only in its list parameter are merged into one
    /// query over the union of the lists; each caller receives the rows whose `key_field` is
    /// one of its own keys.
    async fn query_batched(
        &self,
        target_id: &str,
        spec: &dash_luau::manifest::BatchSpec,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let Some(my_keys) = params.get(&spec.param).and_then(|v| v.as_array()).cloned() else {
            return self.query_dependency(target_id, params).await;
        };
        let mut base = params.clone();
        if let Some(o) = base.as_object_mut() {
            o.remove(&spec.param);
        }
        let group = format!("{target_id}\u{1f}{}", canonicalize_json(&base));
        let fits = |n: usize| spec.max_keys.is_none_or(|m| n <= m);
        let (tx, rx) = tokio::sync::oneshot::channel();
        // `None`: the batch would exceed `max_keys`, so this query runs on its own.
        let role: Option<bool> = {
            let mut map = self.batches.lock().unwrap();
            match map.get_mut(&group) {
                Some(slot) => {
                    let new: Vec<&serde_json::Value> = my_keys
                        .iter()
                        .filter(|k| !slot.seen.contains(&batch_key_repr(k)))
                        .collect();
                    if fits(slot.keys.len() + new.len()) {
                        for k in new {
                            slot.seen.insert(batch_key_repr(k));
                            slot.keys.push(k.clone());
                        }
                        slot.waiters.push((my_keys.clone(), tx));
                        Some(false)
                    } else {
                        None
                    }
                }
                None => {
                    let mut slot = BatchSlot::default();
                    for k in &my_keys {
                        if slot.seen.insert(batch_key_repr(k)) {
                            slot.keys.push(k.clone());
                        }
                    }
                    if fits(slot.keys.len()) {
                        slot.waiters.push((my_keys.clone(), tx));
                        map.insert(group.clone(), slot);
                        Some(true)
                    } else {
                        None
                    }
                }
            }
        };
        let Some(leader) = role else {
            return self.query_dependency(target_id, params).await;
        };
        if leader {
            let mut guard = BatchLeaderGuard {
                map: Arc::clone(&self.batches),
                group: group.clone(),
                armed: true,
            };
            // One scheduler turn: sibling awaits of this execution join the batch meanwhile.
            tokio::task::yield_now().await;
            let slot = self.batches.lock().unwrap().remove(&group);
            guard.armed = false;
            if let Some(slot) = slot {
                let mut merged = base;
                if let Some(o) = merged.as_object_mut() {
                    o.insert(spec.param.clone(), serde_json::Value::Array(slot.keys));
                }
                let res = self.query_dependency(target_id, merged).await;
                for (keys, tx) in slot.waiters {
                    let wanted: HashSet<String> = keys.iter().map(batch_key_repr).collect();
                    let part = res.clone().and_then(|v| match v {
                        serde_json::Value::Array(rows) => Ok(serde_json::Value::Array(
                            rows.into_iter()
                                .filter(|r| {
                                    r.get(&spec.key_field)
                                        .is_some_and(|k| wanted.contains(&batch_key_repr(k)))
                                })
                                .collect(),
                        )),
                        _ => Err(format!(
                            "batch-capable provider '{target_id}' must return a list"
                        )),
                    });
                    let _ = tx.send(part);
                }
            }
        }
        match wait_bounded(rx, &self.wait_ctx()).await? {
            Ok(r) => r,
            Err(_) => Err("batched dependency query was abandoned".into()),
        }
    }
}

#[async_trait]
impl HostBridge for EngineHostBridge {
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
        let mut params = params;
        let options = match params
            .as_object_mut()
            .and_then(|o| o.remove(PUSHDOWN_PARAM))
        {
            Some(v) => QueryOptions::parse(&v)?,
            None => QueryOptions::default(),
        };
        let caps = self
            .graph
            .instances
            .get(target_id)
            .and_then(|i| self.graph.definitions.get(&i.definition))
            .map(|d| d.manifest.query_capabilities.clone())
            .unwrap_or_default();
        // Provider-declared options go to the source; the rest are applied here (§6.1).
        let (pushed, host) = options.split(&caps.pushdown);
        if !pushed.is_empty() {
            if let Some(o) = params.as_object_mut() {
                o.insert(PUSHDOWN_PARAM.into(), pushed.to_json());
            }
        }
        let value = match &caps.batch {
            Some(b) if options.is_empty() => self.query_batched(target_id, b, params).await?,
            _ => self.query_dependency(target_id, params).await?,
        };
        host.apply(value)
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
        let target_inst = self
            .graph
            .instances
            .get(target_id)
            .ok_or_else(|| format!("target instance '{target_id}' not found"))?;
        if target_inst.category != DefinitionCategory::Connector {
            return Err(format!(
                "call_input expects connector, got '{}'",
                target_inst.category.as_str()
            ));
        }
        // Reads have no side effects (§20): data providers may only call read operations.
        audited(
            &self.engine.store,
            &self.principal,
            "connector.invoke",
            check_connector_operation_read_only(
                &self.principal,
                target_inst,
                operation,
                &self.graph,
            ),
        )
        .map_err(|e| e.to_string())?;
        // Read-context connector call: bounded by this execution's deadline, and stopped when
        // this execution is cancelled.
        let call = self.engine.invoke_connector_inner(
            target_id,
            operation,
            args,
            &self.principal,
            &self.trace_id,
            Some(CancelSignal::new()),
        );
        wait_bounded(call, &self.wait_ctx()).await?
    }

    async fn invoke_action(
        &self,
        _input_name: &str,
        _args: serde_json::Value,
        _idempotency_key: Option<String>,
    ) -> Result<serde_json::Value, String> {
        Err("action invocation from read/query context is forbidden (§2.1)".into())
    }

    async fn http_request(&self, req: HttpHostRequest) -> Result<HttpHostResponse, String> {
        let method = req.method.to_ascii_uppercase();
        let destination = url::Url::parse(&req.url)
            .map(|u| {
                format!(
                    "{}://{}:{}{}",
                    u.scheme(),
                    u.host_str().unwrap_or(""),
                    u.port_or_known_default().unwrap_or(0),
                    u.path()
                )
            })
            .unwrap_or_else(|_| "<invalid url>".into());
        let call = self.engine.http.execute_traced(
            &self.principal,
            &self.caller_inst,
            self.operation.as_deref(),
            &self.graph,
            req,
            Some(&self.trace_id),
        );
        match wait_bounded(call, &self.wait_ctx()).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => {
                let msg = e.to_string();
                if crate::actions::is_unknown_outcome_error(&msg) {
                    self.outcome_unknown.store(true, Ordering::SeqCst);
                }
                Err(msg)
            }
            // The broker future was dropped (deadline or cancellation): it writes no audit of its
            // own, and a non-safe request may already have reached the upstream (§9.1).
            Err(wait_err) => {
                let unsafe_method = !is_safe_http_method(&method);
                if unsafe_method {
                    self.outcome_unknown.store(true, Ordering::SeqCst);
                }
                let _ = self.engine.store.append_audit(
                    &self.principal.id,
                    self.principal.kind.as_str(),
                    "http.request",
                    &self.caller_inst.id,
                    if unsafe_method { "unknown" } else { "error" },
                    serde_json::json!({
                        "operation": self.operation,
                        "method": method,
                        "destination": destination,
                        "error": format!("abandoned in flight: {wait_err}"),
                    }),
                );
                Err(if unsafe_method {
                    format!(
                        "{}: {method} request abandoned in flight ({wait_err}); it may have reached the upstream",
                        crate::actions::UNKNOWN_OUTCOME_CODE
                    )
                } else {
                    wait_err
                })
            }
        }
    }

    async fn emit_event(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        durable: bool,
    ) -> Result<(), String> {
        self.engine
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
        self.engine
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
        if scope == "dashboard" {
            // Dashboard State is read from the dashboard being rendered only (§8, ADR-0001).
            let dashboard_id = self.dashboard_id.as_deref().ok_or_else(|| {
                "dashboard_state is only readable while rendering a Dashboard".to_string()
            })?;
            // Deny by default (§4.1, §20): an Adapter reads only the keys it declares, also
            // when it declares none, exactly as in Request handling (actions.rs).
            let declared = &self.caller_def.manifest.dashboard_state_reads;
            if self.caller_def.manifest.category == DefinitionCategory::Adapter
                && !declared.iter().any(|k| k == key)
            {
                return Err(format!(
                    "adapter '{}' did not declare Dashboard State key '{key}' in dashboard_state_reads",
                    self.caller_inst.id
                ));
            }
            let rec = self
                .engine
                .state
                .get("dashboard", dashboard_id, key)
                .map_err(|e| e.to_string())?;
            self.recorded_state
                .lock()
                .unwrap()
                .insert(key.to_string(), rec.as_ref().map(|r| r.version));
            return Ok(rec.map(|r| r.value).unwrap_or(serde_json::Value::Null));
        }
        let rec = self
            .engine
            .state
            .get(scope, &self.caller_inst.id, key)
            .map_err(|e| e.to_string())?;
        Ok(rec.map(|r| r.value).unwrap_or(serde_json::Value::Null))
    }

    async fn state_set(
        &self,
        scope: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), String> {
        if scope == "dashboard" && self.caller_def.manifest.category == DefinitionCategory::Adapter
        {
            return Err("adapters hold no writable state (ADR-0001)".into());
        }
        let owner = self.caller_inst.id.clone();
        if scope == "instance" {
            // Declared schema version, type and size limit of the Definition (§8).
            return self
                .engine
                .state
                .set_instance_state(&self.graph, &owner, key, value)
                .map(|_| ())
                .map_err(|e| e.to_string());
        }
        self.engine
            .state
            .set(scope, &owner, key, 1, self.graph.generation_id, value, None)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn load_fixture(&self, fixture_path: &str) -> Result<serde_json::Value, String> {
        self.caller_def
            .fixtures
            .get(fixture_path)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "fixture '{fixture_path}' not found in mock '{}'",
                    self.caller_def.manifest.id
                )
            })
    }

    async fn mock_state_get(&self, key: &str) -> Result<serde_json::Value, String> {
        let namespaced = format!("{}:{key}", self.caller_def.manifest.id);
        Ok(self
            .engine
            .mock_kv
            .get(&namespaced)
            .map(|v| v.clone())
            .unwrap_or(serde_json::Value::Null))
    }

    async fn mock_state_set(&self, key: &str, value: serde_json::Value) -> Result<(), String> {
        let namespaced = format!("{}:{key}", self.caller_def.manifest.id);
        self.engine.mock_kv.insert(namespaced, value);
        Ok(())
    }

    async fn push_snapshot(
        &self,
        params: serde_json::Value,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.check_push_allowed(&params)?;
        let mut value = value;
        if let Some(out_schema) = &self.caller_def.manifest.output {
            out_schema.normalize_empty_lists(&mut value);
            out_schema.validate_value(&value).map_err(|e| {
                format!(
                    "output contract violation on pushed snapshot of '{}': {e}",
                    self.caller_inst.id
                )
            })?;
        }
        let env =
            self.engine
                .push_query_value(&self.caller_inst.id, &params, &self.principal, value)?;
        // A pushed snapshot is a new baseline for provider sequences.
        self.engine
            .signals
            .source_sequences()
            .reset(&env.query_key, None);
        Ok(serde_json::json!({ "outcome": "applied", "version": env.version }))
    }

    async fn push_delta(
        &self,
        params: serde_json::Value,
        source_sequence: u64,
        ops: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        use crate::signal::ProviderDeltaOutcome;
        self.check_push_allowed(&params)?;
        let ops: Vec<crate::signal::DeltaOp> =
            serde_json::from_value(ops).map_err(|e| format!("ctx:push_delta: invalid ops: {e}"))?;
        let step = self.engine.provider_delta_step(
            &self.caller_inst.id,
            &params,
            &self.principal,
            source_sequence,
            ops,
        )?;
        let step = match step {
            DeltaStep::Done(outcome) => Ok(*outcome),
            DeltaStep::NeedsResync(reason) => Err(reason),
        };
        Ok(match step {
            Ok(ProviderDeltaOutcome::Applied(env)) => serde_json::json!({
                "outcome": "applied",
                "version": env.version,
                "base_version": env.base_version,
            }),
            Ok(ProviderDeltaOutcome::Duplicate { last_sequence }) => {
                serde_json::json!({ "outcome": "duplicate", "last_sequence": last_sequence })
            }
            Ok(ProviderDeltaOutcome::Resynced { reason, .. }) | Err(reason) => {
                // The provider's own VM is busy running this hook: resynchronize from a fresh
                // snapshot once it is free instead of waiting on it here.
                let engine = self.engine.clone();
                let instance_id = self.caller_inst.id.clone();
                let principal = self.principal.clone();
                tokio::spawn(async move {
                    let _ = engine.query(&instance_id, params, &principal, true).await;
                });
                serde_json::json!({ "outcome": "resync_scheduled", "reason": reason })
            }
        })
    }
}

impl EngineHostBridge {
    /// Pushes are scoped to the calling data provider's own queries and require the Definition
    /// to declare `updates.push` (§6.1).
    fn check_push_allowed(&self, params: &serde_json::Value) -> Result<(), String> {
        if self.caller_def.manifest.category != DefinitionCategory::DataProvider {
            return Err("only data providers push query updates (§6.1)".into());
        }
        if !self.caller_def.manifest.updates.push {
            return Err(format!(
                "definition '{}' does not declare push updates (updates.push)",
                self.caller_def.manifest.id
            ));
        }
        if let Some(q) = &self.caller_def.manifest.query {
            q.validate_value(params).map_err(|e| {
                format!(
                    "invalid query parameters for push on '{}': {e}",
                    self.caller_inst.id
                )
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CapabilityCeiling, HistorySeriesConfig};
    use crate::{FailureInjectionRule, PrincipalKind, RuntimeEngine};
    use dash_luau::{evaluate_declaration, ResourceBudget};
    use serde_json::json;

    fn load_def(decl: &str, runtime: &str) -> LoadedDefinition {
        let mut sources = BTreeMap::new();
        sources.insert("definition.luau".to_string(), decl.as_bytes().to_vec());
        sources.insert("runtime.luau".to_string(), runtime.as_bytes().to_vec());
        let manifest = evaluate_declaration(decl, &sources).unwrap();
        LoadedDefinition {
            package_name: "test".into(),
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
        config: serde_json::Value,
    ) -> InstanceConfig {
        InstanceConfig {
            id: id.into(),
            category,
            definition: definition.into(),
            inputs: inputs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            config,
            secrets: BTreeMap::new(),
            capabilities: vec![],
            run_as: None,
            enabled: None,
        }
    }

    fn provider(
        graph: &mut ConfiguredGraph,
        inst_id: &str,
        def_id: &str,
        inputs: &[(&str, &str)],
        runtime: &str,
    ) {
        let refs: Vec<String> = inputs
            .iter()
            .map(|(name, _)| format!("{name} = dash.data_ref(\"test.any\")"))
            .collect();
        let decl = format!(
            "local dash = require(\"dash\")\nreturn dash.data_provider {{ id = \"{def_id}\", inputs = {{ {} }} }}",
            refs.join(", ")
        );
        graph
            .definitions
            .insert(def_id.into(), load_def(&decl, runtime));
        graph.instances.insert(
            inst_id.into(),
            instance(
                inst_id,
                DefinitionCategory::DataProvider,
                def_id,
                inputs,
                json!({}),
            ),
        );
    }

    fn engine_for(graph: ConfiguredGraph, budget: ResourceBudget) -> (RuntimeEngine, QueryEngine) {
        let store = SqliteStore::open_in_memory().unwrap();
        let engine = RuntimeEngine::new(graph, store, budget).unwrap();
        let q = engine.queries.clone();
        (engine, q)
    }

    fn key_of(q: &QueryEngine, inst_id: &str, params: &serde_json::Value, p: &Principal) -> String {
        let g = q.graph();
        let inst = g.instances[inst_id].clone();
        q.query_key_for(&g, &inst, params, p).unwrap()
    }

    const LIST_RT: &str = r#"local R = {}; function R.query(ctx) return { { id = "1", state = "open" } } end; return R"#;
    const COUNT_RT: &str = r#"
local R = {}
function R.query(ctx)
    local rows = ctx:await(ctx.inputs.list:query({}))
    return { count = #rows }
end
return R
"#;

    fn list_and_count_graph() -> ConfiguredGraph {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        provider(&mut g, "inst_list", "test.list", &[], LIST_RT);
        provider(
            &mut g,
            "inst_count",
            "test.count",
            &[("list", "inst_list")],
            COUNT_RT,
        );
        g
    }

    async fn next_snapshot(
        rx: &mut broadcast::Receiver<QueryEnvelope>,
        within: Duration,
    ) -> Option<QueryEnvelope> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(env)) if env.kind == crate::signal::EnvelopeKind::Snapshot => {
                    return Some(env)
                }
                Ok(Ok(_)) => continue,
                _ => return None,
            }
        }
    }

    // ---- query-1: singleflight survives a dropped leader caller ----

    #[tokio::test]
    async fn singleflight_survives_dropped_leader_caller() {
        let (_e, q) = engine_for(ConfiguredGraph::default(), ResourceBudget::default());
        let map: FlightMap<u32> = Arc::new(DashMap::new());
        let runs = Arc::new(AtomicUsize::new(0));
        let lead = |runs: Arc<AtomicUsize>| {
            move |_tick: u64, _cancel: CancelSignal| {
                let runs = Arc::clone(&runs);
                async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    runs.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, String>(7)
                }
            }
        };

        let (q1, m1, r1) = (q.clone(), Arc::clone(&map), Arc::clone(&runs));
        let first = tokio::spawn(async move {
            q1.join_or_lead(&m1, "k", "inst", 0, &WaitCtx::default(), lead(r1))
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        first.abort();

        let second = tokio::time::timeout(
            Duration::from_secs(3),
            q.join_or_lead(
                &map,
                "k",
                "inst",
                0,
                &WaitCtx::default(),
                lead(Arc::clone(&runs)),
            ),
        )
        .await
        .expect("follow-up query must not hang after the leader caller was dropped");
        assert_eq!(second.unwrap(), 7);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "follower joined the running flight"
        );
        assert!(map.is_empty(), "flight entry removed after completion");
    }

    #[tokio::test]
    async fn shared_flight_created_by_a_nested_caller_runs_under_its_own_deadline() {
        const BUSY_RT: &str = r#"
local R = {}
function R.query(ctx)
    local x = 0
    for i = 1, 4000000 do x = (x + i) % 1000003 end
    return { x = x }
end
return R
"#;
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        provider(&mut g, "busy", "test.busy", &[], BUSY_RT);
        let budget = ResourceBudget {
            max_interrupts: 50_000_000,
            wall_timeout: Duration::from_secs(20),
            ..ResourceBudget::default()
        };
        let (_e, q) = engine_for(g, budget);
        let admin = Principal::administrator();
        // Calibrate one run (other params: its cached value is not reused below).
        let t0 = Instant::now();
        let expected = q
            .query("busy", json!({ "calibrate": true }), &admin, true)
            .await
            .unwrap()
            .value;
        let short = (t0.elapsed() / 5).max(Duration::from_millis(1));

        // A nested caller (e.g. a parent provider) with far less time left than one run creates
        // the flight; a top-level query joins it.
        let (q1, a1) = (q.clone(), admin.clone());
        let nested = tokio::spawn(async move {
            dash_luau::with_execution_deadline(
                Instant::now() + short,
                q1.query("busy", json!({}), &a1, false),
            )
            .await
        });
        tokio::time::sleep(short / 4).await;
        let top = q.query("busy", json!({}), &admin, false).await.unwrap();
        assert_eq!(top.status, QueryStatus::Fresh, "{top:?}");
        assert_eq!(
            top.value, expected,
            "the joining top-level query gets the value"
        );
        let _ = nested.await;
        assert_eq!(
            q.breaker_admit("busy"),
            Admission::Closed,
            "the caller's deadline is not charged to the provider"
        );
    }

    #[tokio::test]
    async fn flight_without_waiters_is_cancelled_after_grace() {
        let (_e, mut q) = engine_for(ConfiguredGraph::default(), ResourceBudget::default());
        q.cancel_grace = Duration::from_millis(30);
        let map: FlightMap<u32> = Arc::new(DashMap::new());
        let observed_cancel = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&observed_cancel);
        let (q1, m1) = (q.clone(), Arc::clone(&map));
        let caller = tokio::spawn(async move {
            q1.join_or_lead(
                &m1,
                "k",
                "inst",
                0,
                &WaitCtx::default(),
                move |_t, cancel: CancelSignal| {
                    let seen = Arc::clone(&seen);
                    async move {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(10)) => Ok(1u32),
                            _ = cancel.cancelled() => {
                                seen.store(true, Ordering::SeqCst);
                                Err("cancelled".to_string())
                            }
                        }
                    }
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        caller.abort();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            observed_cancel.load(Ordering::SeqCst),
            "unused computation cancelled after grace"
        );
        assert!(map.is_empty());
    }

    // ---- query-9: invalidated flights are not joined, outdated results not cached ----

    #[tokio::test]
    async fn flight_started_before_invalidation_is_not_joined() {
        let (_e, q) = engine_for(ConfiguredGraph::default(), ResourceBudget::default());
        let map: FlightMap<u32> = Arc::new(DashMap::new());
        let (q1, m1) = (q.clone(), Arc::clone(&map));
        let old = tokio::spawn(async move {
            q1.join_or_lead(&m1, "k", "inst", 0, &WaitCtx::default(), |_t, _c| async {
                tokio::time::sleep(Duration::from_millis(150)).await;
                Ok::<u32, String>(1)
            })
            .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let invalidated_at = q.next_tick();
        let fresh = q
            .join_or_lead(
                &map,
                "k",
                "inst",
                invalidated_at,
                &WaitCtx::default(),
                |_t, _c| async { Ok::<u32, String>(2) },
            )
            .await
            .unwrap();
        assert_eq!(
            fresh, 2,
            "post-invalidation caller starts a new computation"
        );
        assert_eq!(old.await.unwrap().unwrap(), 1);
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn outdated_leader_result_is_not_cached_as_fresh() {
        let (_e, q) = engine_for(list_and_count_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        let first = q
            .query("inst_list", json!({}), &admin, false)
            .await
            .unwrap();
        let qk = first.query_key.clone();

        // A run that started before an invalidation finishes afterwards.
        let old_start = q.next_tick();
        q.invalidate_instances(&["inst_list".to_string()]).await;
        let current = q.cache.lock().unwrap().peek(&qk).cloned().unwrap();

        let g = q.graph();
        let def = g.definitions["test.list"].clone();
        let commit = q.commit_query_value(
            &g,
            "inst_list",
            &def,
            &json!({}),
            &admin,
            &qk,
            json!([{"id": "old"}]),
            QueryStatus::Fresh,
            Utc::now(),
            old_start,
            &[],
            true,
        );
        assert!(!commit.written);
        assert_eq!(commit.snapshot.status, QueryStatus::Stale);
        let after = q.cache.lock().unwrap().peek(&qk).cloned().unwrap();
        assert_eq!(
            after.value, current.value,
            "pre-invalidation data must not overwrite the cache"
        );
        assert_eq!(after.version, current.version);
    }

    // ---- query-2 / luau-m1: breaker rejections, half-open probe, jitter ----

    #[tokio::test]
    async fn open_breaker_rejections_do_not_extend_window_and_probe_closes_it() {
        let (e, q) = engine_for(list_and_count_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        q.query("inst_list", json!({}), &admin, false)
            .await
            .unwrap();
        e.telemetry.inject_failure(FailureInjectionRule {
            instance_id: "inst_list".into(),
            mode: "error".into(),
            message: "upstream 503".into(),
            remaining: Some(BREAKER_THRESHOLD as usize),
        });
        for _ in 0..BREAKER_THRESHOLD {
            let env = q.query("inst_list", json!({}), &admin, true).await.unwrap();
            assert_eq!(env.status, QueryStatus::Stale);
        }
        assert!(q.circuit_open("inst_list"));
        let opened_until = q
            .circuit_breakers
            .get("inst_list")
            .unwrap()
            .open_until
            .unwrap();

        for _ in 0..10 {
            let env = q.query("inst_list", json!({}), &admin, true).await.unwrap();
            assert_eq!(env.status, QueryStatus::Stale);
            assert!(env.retry.as_ref().unwrap().circuit_open);
        }
        {
            let st = q.circuit_breakers.get("inst_list").unwrap();
            assert_eq!(
                st.open_until,
                Some(opened_until),
                "rejections must not push the window"
            );
            assert_eq!(
                st.consecutive_failures, BREAKER_THRESHOLD,
                "rejections are not failures"
            );
        }

        // Window elapsed: exactly one probe runs; success closes the breaker.
        q.circuit_breakers.get_mut("inst_list").unwrap().open_until =
            Some(Instant::now() - Duration::from_millis(1));
        let probe = q.query("inst_list", json!({}), &admin, true).await.unwrap();
        assert_eq!(probe.status, QueryStatus::Fresh);
        assert!(!q.circuit_open("inst_list"));
        assert!(q.circuit_breakers.get("inst_list").is_none());
    }

    #[tokio::test]
    async fn half_open_admits_one_probe_and_failed_probe_reopens_longer() {
        let (_e, q) = engine_for(ConfiguredGraph::default(), ResourceBudget::default());
        for _ in 0..BREAKER_THRESHOLD {
            q.breaker_failure("x");
        }
        assert!(matches!(
            q.breaker_admit("x"),
            Admission::Rejected { until: Some(_) }
        ));
        q.circuit_breakers.get_mut("x").unwrap().open_until = Some(Instant::now());
        assert_eq!(q.breaker_admit("x"), Admission::Probe);
        assert_eq!(q.breaker_admit("x"), Admission::Rejected { until: None });
        assert!(q.breaker_failure("x"), "failed probe reopens the breaker");
        let window = q
            .breaker_open_until("x")
            .unwrap()
            .saturating_duration_since(Instant::now());
        // Second opening doubles the base window (2s ± 20%).
        assert!(window > Duration::from_millis(1500), "window {window:?}");
        q.breaker_success("x");
        assert_eq!(q.breaker_admit("x"), Admission::Closed);
    }

    #[test]
    fn backoff_has_bounded_jitter() {
        for _ in 0..200 {
            let d = jittered(Duration::from_secs(1));
            assert!(d >= Duration::from_millis(800) && d <= Duration::from_millis(1200));
        }
        assert!(retry_delay(1) <= Duration::from_millis(1200));
        assert!(retry_delay(30) <= RETRY_MAX_DELAY.mul_f64(1.0 + BACKOFF_JITTER));
        assert!(retry_delay(3) >= Duration::from_millis(3200));
    }

    // ---- query-3: dynamic dependency propagation ----

    #[tokio::test]
    async fn upstream_push_recomputes_subscribed_dependent() {
        let (_e, q) = engine_for(list_and_count_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        let count = q
            .query("inst_count", json!({}), &admin, false)
            .await
            .unwrap();
        assert_eq!(count.value.as_ref().unwrap()["count"], 1);
        let mut rx = q.signals().subscribe_stream(&count.query_key);

        q.push_query_value(
            "inst_list",
            &json!({}),
            &admin,
            json!([{"id": "1"}, {"id": "2"}, {"id": "3"}]),
        )
        .unwrap();
        let next = next_snapshot(&mut rx, Duration::from_secs(3))
            .await
            .expect("dependent must publish a new Signal after its upstream changed");
        assert_eq!(next.value.unwrap()["count"], 3);
        assert!(next.version > count.version);
    }

    #[tokio::test]
    async fn unsubscribed_dependent_is_marked_stale_on_upstream_change() {
        let (_e, q) = engine_for(list_and_count_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        let count = q
            .query("inst_count", json!({}), &admin, false)
            .await
            .unwrap();
        q.push_query_value(
            "inst_list",
            &json!({}),
            &admin,
            json!([{"id": "1"}, {"id": "2"}]),
        )
        .unwrap();
        // Not subscribed: no background recompute, but the next read must not serve the old value.
        let again = q
            .query("inst_count", json!({}), &admin, false)
            .await
            .unwrap();
        assert_eq!(again.value.unwrap()["count"], 2);
        assert!(again.version > count.version);
    }

    #[tokio::test]
    async fn dynamic_switch_stops_propagation_from_previous_upstream() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        provider(
            &mut g,
            "inst_sw",
            "test.sw",
            &[],
            r#"local R = {}; function R.query(ctx) return ctx.state:get("target") or "a" end; return R"#,
        );
        provider(
            &mut g,
            "inst_a",
            "test.a",
            &[],
            r#"local R = {}; function R.query(ctx) return { 1, 2 } end; return R"#,
        );
        provider(
            &mut g,
            "inst_b",
            "test.b",
            &[],
            r#"local R = {}; function R.query(ctx) return { 10, 20, 30 } end; return R"#,
        );
        provider(
            &mut g,
            "inst_router",
            "test.router",
            &[("sw", "inst_sw"), ("a", "inst_a"), ("b", "inst_b")],
            r#"
local R = {}
function R.query(ctx)
    local which = ctx:await(ctx.inputs.sw:query({}))
    if which == "a" then return ctx:await(ctx.inputs.a:query({})) end
    return ctx:await(ctx.inputs.b:query({}))
end
return R
"#,
        );
        let (e, q) = engine_for(g, ResourceBudget::default());
        let admin = Principal::administrator();
        let r = q
            .query("inst_router", json!({}), &admin, false)
            .await
            .unwrap();
        assert_eq!(r.value.as_ref().unwrap().as_array().unwrap().len(), 2);
        let mut rx = q.signals().subscribe_stream(&r.query_key);

        // Switch upstream: the changed switch value propagates to the subscribed router.
        e.store
            .set_state("instance", "inst_sw", "target", 1, 1, json!("b"), None)
            .unwrap();
        q.query("inst_sw", json!({}), &admin, true).await.unwrap();
        let switched = next_snapshot(&mut rx, Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(switched.value.unwrap().as_array().unwrap().len(), 3);

        let qk_a = key_of(&q, "inst_a", &json!({}), &admin);
        assert!(!q.reverse_subscribers(&qk_a).contains(&r.query_key));
        // Updates of the old upstream no longer trigger the router.
        q.push_query_value("inst_a", &json!({}), &admin, json!([1, 2, 3, 4]))
            .unwrap();
        assert!(next_snapshot(&mut rx, Duration::from_millis(300))
            .await
            .is_none());
    }

    #[tokio::test]
    async fn upstream_change_observed_during_dependent_run_does_not_invalidate_it() {
        let (_e, q) = engine_for(list_and_count_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        let first = q
            .query("inst_count", json!({}), &admin, false)
            .await
            .unwrap();
        // The upstream must recompute inside the dependent's run (e.g. its TTL lapsed) and
        // returns a changed value there.
        let qk_list = key_of(&q, "inst_list", &json!({}), &admin);
        {
            let mut cache = q.cache.lock().unwrap();
            let e = cache.peek_mut(&qk_list).unwrap();
            e.stale = true;
            e.value = json!([]);
        }
        let second = q
            .query("inst_count", json!({}), &admin, true)
            .await
            .unwrap();
        assert_eq!(
            second.status,
            QueryStatus::Fresh,
            "dependent read the new upstream value"
        );
        let hit = q
            .query("inst_count", json!({}), &admin, false)
            .await
            .unwrap();
        assert_eq!(hit.version, second.version, "result was cached");
        assert!(second.version > first.version);
    }

    #[tokio::test]
    async fn dependent_result_is_not_cached_when_its_upstream_moved_meanwhile() {
        let (_e, q) = engine_for(list_and_count_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        let list = q
            .query("inst_list", json!({}), &admin, false)
            .await
            .unwrap();
        let observed = vec![UpstreamObservation {
            query_key: list.query_key.clone(),
            status: QueryStatus::Fresh,
            updated_at: Utc::now(),
            cached_version: Some(list.version),
        }];
        let start = q.next_tick();
        q.push_query_value("inst_list", &json!({}), &admin, json!([]))
            .unwrap();
        let g = q.graph();
        let def = g.definitions["test.count"].clone();
        let qk_count = key_of(&q, "inst_count", &json!({}), &admin);
        let commit = q.commit_query_value(
            &g,
            "inst_count",
            &def,
            &json!({}),
            &admin,
            &qk_count,
            json!({"count": 1}),
            QueryStatus::Fresh,
            Utc::now(),
            start,
            &observed,
            true,
        );
        assert!(
            !commit.written,
            "a value computed over a superseded upstream is not cached"
        );
    }

    // ---- query-8: staleness flows through ctx:await ----

    #[tokio::test]
    async fn dependent_over_stale_upstream_reports_stale_with_last_success_time() {
        let (e, q) = engine_for(list_and_count_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        let list = q
            .query("inst_list", json!({}), &admin, false)
            .await
            .unwrap();
        e.telemetry.inject_failure(FailureInjectionRule {
            instance_id: "inst_list".into(),
            mode: "error".into(),
            message: "down".into(),
            remaining: Some(2),
        });
        let stale_list = q.query("inst_list", json!({}), &admin, true).await.unwrap();
        assert_eq!(stale_list.status, QueryStatus::Stale);
        assert_eq!(
            stale_list.updated_at, list.updated_at,
            "stale value keeps its last success time"
        );

        let count = q
            .query("inst_count", json!({}), &admin, false)
            .await
            .unwrap();
        assert_eq!(count.status, QueryStatus::Stale);
        assert_eq!(count.updated_at, list.updated_at);
        // Cache hits keep the derived status.
        let hit = q
            .query("inst_count", json!({}), &admin, false)
            .await
            .unwrap();
        assert_eq!(hit.status, QueryStatus::Stale);
    }

    // ---- query-10 / query-m3: monotonic versions ----

    #[tokio::test]
    async fn versions_stay_monotonic_across_stale_transition_and_eviction() {
        let (e, q) = engine_for(list_and_count_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        let v1 = q
            .query("inst_list", json!({}), &admin, false)
            .await
            .unwrap();
        e.telemetry.inject_failure(FailureInjectionRule {
            instance_id: "inst_list".into(),
            mode: "error".into(),
            message: "down".into(),
            remaining: Some(1),
        });
        let stale = q.query("inst_list", json!({}), &admin, true).await.unwrap();
        assert!(
            stale.version > v1.version,
            "fresh -> stale transition gets a new version"
        );
        q.cache.lock().unwrap().pop(&v1.query_key); // simulate LRU eviction
        let again = q
            .query("inst_list", json!({}), &admin, false)
            .await
            .unwrap();
        assert!(
            again.version > stale.version,
            "no version reset after eviction"
        );
    }

    #[test]
    fn signal_hub_drops_out_of_order_envelopes() {
        let hub = SignalHub::new(32);
        let mut rx = hub.subscribe_stream("k");
        let watch = hub.subscribe_watch("k");
        let snap = |v: u64| {
            QueryEnvelope::snapshot("i", "k", 1, v, QueryStatus::Fresh, None, json!(v), None)
        };
        hub.publish_query(snap(5));
        hub.publish_query(snap(3));
        hub.publish_query(QueryEnvelope::delta("i", "k", 1, 4, 5, None, vec![]));
        assert_eq!(watch.borrow().as_ref().unwrap().version, 5);
        assert_eq!(rx.try_recv().unwrap().version, 5);
        assert!(
            rx.try_recv().is_err(),
            "older snapshot and stale delta were dropped"
        );
        assert_eq!(hub.subscriber_count("k"), 2);
        assert_eq!(hub.subscriber_count("other"), 0);
    }

    // ---- query-12: background retry with metadata ----

    #[tokio::test]
    async fn failing_subscribed_query_is_retried_in_background() {
        let (e, q) = engine_for(list_and_count_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        let first = q
            .query("inst_list", json!({}), &admin, false)
            .await
            .unwrap();
        let mut rx = q.signals().subscribe_stream(&first.query_key);
        e.telemetry.inject_failure(FailureInjectionRule {
            instance_id: "inst_list".into(),
            mode: "error".into(),
            message: "flaky".into(),
            remaining: Some(1),
        });
        let stale = q.query("inst_list", json!({}), &admin, true).await.unwrap();
        let retry = stale
            .retry
            .clone()
            .expect("retry metadata on stale envelope");
        assert_eq!(retry.attempt, 1);
        assert!(retry.next_retry_at.is_some());
        assert!(!retry.circuit_open);

        let mut recovered = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while tokio::time::Instant::now() < deadline {
            match next_snapshot(&mut rx, Duration::from_secs(4)).await {
                Some(env) if env.status == QueryStatus::Fresh => {
                    recovered = Some(env);
                    break;
                }
                Some(_) => continue,
                None => break,
            }
        }
        let recovered = recovered.expect("background retry publishes a fresh Signal");
        assert!(recovered.retry.is_none());
        assert!(q.retry_attempts.get(&first.query_key).is_none());
    }

    // ---- query-18: cycle diagnostics, failed-run edges, per-node subscribers ----

    #[tokio::test]
    async fn dynamic_cycle_fails_with_diagnostic_path_and_records_edges() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        let rt = |input: &str| {
            format!(
                "local R = {{}}; function R.query(ctx) return ctx:await(ctx.inputs.{input}:query({{}})) end; return R"
            )
        };
        provider(&mut g, "inst_x", "test.x", &[("y", "inst_y")], &rt("y"));
        provider(&mut g, "inst_y", "test.y", &[("x", "inst_x")], &rt("x"));
        let (e, q) = engine_for(g, ResourceBudget::default());
        let admin = Principal::administrator();
        let err = q
            .query("inst_x", json!({}), &admin, false)
            .await
            .unwrap_err();
        assert!(err.contains("inst_x -> inst_y -> inst_x"), "{err}");
        let spans = e.telemetry.recent_spans(50);
        assert!(spans
            .iter()
            .any(|s| s.attributes.get("cycle_path").is_some() && s.status == "error"));
        let qk_x = key_of(&q, "inst_x", &json!({}), &admin);
        let qk_y = key_of(&q, "inst_y", &json!({}), &admin);
        assert!(
            q.recorded_dependencies(&qk_x).contains(&qk_y),
            "failed run's edges are recorded"
        );
    }

    #[tokio::test]
    async fn execution_graph_reports_per_node_subscribers() {
        let (_e, q) = engine_for(list_and_count_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        let count = q
            .query("inst_count", json!({}), &admin, false)
            .await
            .unwrap();
        let _rx = q.signals().subscribe_stream(&count.query_key);
        let snap = q.execution_graph_snapshot();
        let node = |k: &str| {
            snap.nodes
                .iter()
                .find(|n| n.query_key == k)
                .unwrap()
                .clone()
        };
        assert_eq!(node(&count.query_key).subscribers, 1);
        let qk_list = key_of(&q, "inst_list", &json!({}), &admin);
        assert_eq!(node(&qk_list).subscribers, 0);
        assert!(snap
            .edges
            .iter()
            .any(|e| e.from_query_key == count.query_key && e.to_query_key == qk_list));
    }

    // ---- query-14 / luau-5: deadlines and cancellation ----

    #[tokio::test]
    async fn bounded_waits_honor_deadline_and_cancellation() {
        let wait = WaitCtx {
            deadline: Some(Instant::now() + Duration::from_millis(30)),
            cancel: None,
        };
        let err = wait_bounded(tokio::time::sleep(Duration::from_secs(5)), &wait)
            .await
            .unwrap_err();
        assert!(err.contains("deadline"));

        let cancel = CancelSignal::new();
        let c2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            c2.cancel();
        });
        let wait = WaitCtx {
            deadline: None,
            cancel: Some(cancel),
        };
        let err = wait_bounded(tokio::time::sleep(Duration::from_secs(5)), &wait)
            .await
            .unwrap_err();
        assert!(err.contains("cancelled"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoned_query_cancels_its_vm_before_the_timeout() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        provider(
            &mut g,
            "inst_spin",
            "test.spin",
            &[],
            "local R = {}; function R.query(ctx) while true do end end; return R",
        );
        let budget = ResourceBudget {
            wall_timeout: Duration::from_secs(30),
            max_interrupts: u64::MAX,
            ..Default::default()
        };
        let (e, mut q) = engine_for(g, budget);
        q.cancel_grace = Duration::from_millis(30);
        let q1 = q.clone();
        let started = Instant::now();
        let caller = tokio::spawn(async move {
            q1.query("inst_spin", json!({}), &Principal::administrator(), false)
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        caller.abort();
        while !q.in_flight.is_empty() {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "VM was not cancelled"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(e
            .telemetry
            .recent_spans(20)
            .iter()
            .any(|s| s.name == "query:inst_spin" && s.status == "cancelled"));
        assert!(
            q.circuit_breakers.get("inst_spin").is_none(),
            "cancellation is not a failure"
        );
    }

    // ---- hist-1: history series authority and budget ----

    #[tokio::test]
    async fn history_series_serves_only_configured_series_under_source_authority() {
        let mut g = list_and_count_graph();
        g.instances.insert(
            "hist".into(),
            instance(
                "hist",
                DefinitionCategory::DataProvider,
                "dash.history_series",
                &[],
                json!({"series_id": "s_list"}),
            ),
        );
        let cfg = HistorySeriesConfig {
            series_id: "s_list".into(),
            instance_id: "inst_list".into(),
            params: json!({}),
            aggregator: "count".into(),
            value_path: None,
            cadence_secs: 3600,
            retention_days: 30,
            service_principal: "sampler".into(),
        };
        g.workspace.history = vec![cfg.clone()];
        let (e, q) = engine_for(g, ResourceBudget::default());
        e.history.sync_series_configs(&[cfg]).unwrap();
        e.store
            .append_history_sample("s_list", &Utc::now().to_rfc3339(), 4.0, None)
            .unwrap();

        let agent = |providers: &[&str]| Principal {
            id: "agt".into(),
            kind: PrincipalKind::Agent,
            ceiling: CapabilityCeiling {
                allowed_providers: providers.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            },
        };
        let denied = q.query("hist", json!({}), &agent(&["hist"]), false).await;
        assert!(
            denied.is_err(),
            "reading a series requires authority over its source provider"
        );
        let ok = q
            .query(
                "hist",
                json!({"series_id": "s_list", "limit": 1_000_000}),
                &agent(&["hist", "inst_list"]),
                false,
            )
            .await
            .unwrap();
        assert_eq!(ok.value.unwrap().as_array().unwrap().len(), 1);

        let admin = Principal::administrator();
        let other = q
            .query("hist", json!({"series_id": "someone_elses"}), &admin, false)
            .await;
        assert!(other.unwrap_err().contains("serves series 's_list'"));
        let unknown = q.query("hist", json!({"bogus": 1}), &admin, false).await;
        assert!(unknown.is_err());

        // Windowed, downsampled read: one bucket aggregating both recent samples; samples
        // outside the window are excluded.
        e.store
            .append_history_sample("s_list", &Utc::now().to_rfc3339(), 6.0, None)
            .unwrap();
        e.store
            .append_history_sample(
                "s_list",
                &(Utc::now() - chrono::Duration::days(3)).to_rfc3339(),
                100.0,
                None,
            )
            .unwrap();
        let buckets = q
            .query(
                "hist",
                json!({"window": "1d", "bucket_secs": "1d", "aggregate": "max"}),
                &admin,
                false,
            )
            .await
            .unwrap()
            .value
            .unwrap();
        let buckets = buckets.as_array().unwrap();
        let total: i64 = buckets.iter().map(|b| b["samples"].as_i64().unwrap()).sum();
        assert_eq!(total, 2, "{buckets:?}");
        assert!(buckets
            .iter()
            .all(|b| b["value_num"].as_f64().unwrap() <= 6.0));
        let bad = q
            .query(
                "hist",
                json!({"aggregate": "median", "bucket_secs": 60}),
                &admin,
                false,
            )
            .await
            .unwrap_err();
        assert!(bad.contains("aggregate"), "{bad}");
    }

    /// One history Instance serving a series per sampled repository; readers select by the
    /// sampled query parameters (§16 behaviour 4: trend follows the selected repository).
    #[tokio::test]
    async fn history_series_selects_configured_series_by_sampled_params() {
        let mut g = list_and_count_graph();
        g.instances.insert(
            "hist".into(),
            instance(
                "hist",
                DefinitionCategory::DataProvider,
                "dash.history_series",
                &[],
                json!({"series_ids": ["s_a", "s_b"]}),
            ),
        );
        let series = |id: &str, repo: &str| HistorySeriesConfig {
            series_id: id.into(),
            instance_id: "inst_list".into(),
            params: json!({"repository": repo, "state": "open"}),
            aggregator: "count".into(),
            value_path: None,
            cadence_secs: 3600,
            retention_days: 30,
            service_principal: "sampler".into(),
        };
        let cfgs = vec![
            series("s_a", "acme/a"),
            series("s_b", "acme/b"),
            series("s_c", "acme/c"),
        ];
        g.workspace.history = cfgs.clone();
        let (e, q) = engine_for(g, ResourceBudget::default());
        e.history.sync_series_configs(&cfgs).unwrap();
        let now = Utc::now().to_rfc3339();
        e.store
            .append_history_sample("s_a", &now, 1.0, None)
            .unwrap();
        e.store
            .append_history_sample("s_b", &now, 2.0, None)
            .unwrap();
        e.store
            .append_history_sample("s_c", &now, 3.0, None)
            .unwrap();
        let admin = Principal::administrator();

        let b = q
            .query(
                "hist",
                json!({"params": {"repository": "acme/b"}}),
                &admin,
                false,
            )
            .await
            .unwrap()
            .value
            .unwrap();
        let b = b.as_array().unwrap();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0]["series_id"], "s_b");
        assert_eq!(b[0]["value_num"], 2.0);

        // A series sampled in the Workspace but not configured on this Instance is unreachable.
        let c = q
            .query(
                "hist",
                json!({"params": {"repository": "acme/c"}}),
                &admin,
                false,
            )
            .await
            .unwrap();
        assert_eq!(c.value, Some(json!([])));
        assert!(q
            .query("hist", json!({"series_id": "s_c"}), &admin, false)
            .await
            .unwrap_err()
            .contains("serves series 's_a', 's_b'"));
        // Ambiguous or missing selectors are rejected rather than guessed.
        assert!(q
            .query("hist", json!({"params": {"state": "open"}}), &admin, false)
            .await
            .unwrap_err()
            .contains("matches several series"));
        assert!(q
            .query("hist", json!({}), &admin, false)
            .await
            .unwrap_err()
            .contains("select one"));
    }

    // ---- state-7 / widget-16: widget_id derivation ----

    fn ids(tree: &serde_json::Value, out: &mut Vec<String>) {
        if let Some(id) = tree.get("widget_id").and_then(|v| v.as_str()) {
            out.push(id.to_string());
        }
        if let Some(children) = tree.get("children").and_then(|v| v.as_array()) {
            for c in children {
                ids(c, out);
            }
        }
    }

    #[test]
    fn widget_ids_are_derived_not_adapter_supplied() {
        let mut owners = HashMap::new();
        let mut diags = Vec::new();
        let overrides = BTreeMap::new();
        let tree = json!({
            "key": "root",
            "widget_id": "other_adapter_table",
            "layout_override": {"w": 99},
            "children": [{ "key": "t", "widget_id": "spoofed" }, {}, {}],
        });
        let out = annotate_widget_ids(
            &ConfiguredGraph::default(),
            tree,
            "a",
            &overrides,
            &mut owners,
            &mut diags,
        )
        .unwrap();
        let mut got = Vec::new();
        ids(&out, &mut got);
        assert_eq!(got, vec!["a_root", "a_t", "a_root/1", "a_root/2"]);
        assert!(
            out.get("layout_override").is_none(),
            "adapter cannot inject layout overrides"
        );
        assert_eq!(diags.len(), 2, "keyless nodes are reported");
        assert!(owners.values().all(|o| o == "a"));
    }

    #[test]
    fn duplicate_widget_keys_are_render_errors() {
        let overrides = BTreeMap::new();
        let dup = json!({ "key": "root", "children": [{ "key": "x" }, { "key": "x" }] });
        let err = annotate_widget_ids(
            &ConfiguredGraph::default(),
            dup,
            "a",
            &overrides,
            &mut HashMap::new(),
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(err.starts_with("DUPLICATE_WIDGET_KEY"));

        // Ambiguous `_` joins across Adapters of one dashboard are detected, not merged.
        let mut owners = HashMap::new();
        annotate_widget_ids(
            &ConfiguredGraph::default(),
            json!({ "key": "b_c" }),
            "a",
            &overrides,
            &mut owners,
            &mut Vec::new(),
        )
        .unwrap();
        let err = annotate_widget_ids(
            &ConfiguredGraph::default(),
            json!({ "key": "c" }),
            "a_b",
            &overrides,
            &mut owners,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(err.contains("already used by a node of adapter 'a'"));
    }

    #[test]
    fn widget_ids_survive_reordering() {
        let overrides = BTreeMap::new();
        let a = json!({ "key": "root", "children": [{ "key": "x" }, { "key": "y" }] });
        let b = json!({ "key": "root", "children": [{ "key": "y" }, { "key": "x" }] });
        let ta = annotate_widget_ids(
            &ConfiguredGraph::default(),
            a,
            "ad",
            &overrides,
            &mut HashMap::new(),
            &mut Vec::new(),
        )
        .unwrap();
        let tb = annotate_widget_ids(
            &ConfiguredGraph::default(),
            b,
            "ad",
            &overrides,
            &mut HashMap::new(),
            &mut Vec::new(),
        )
        .unwrap();
        assert_eq!(
            ta["children"][0]["widget_id"],
            tb["children"][1]["widget_id"]
        );
        assert_eq!(
            ta["children"][1]["widget_id"],
            tb["children"][0]["widget_id"]
        );
    }

    // ---- state-1 / state-8 / query-m1: Adapter renders ----

    fn dashboards_graph(adapter_reads: &str, render_key_read: &str) -> ConfiguredGraph {
        let mut g = list_and_count_graph();
        let decl = format!(
            r#"
local dash = require("dash")
return dash.adapter {{
    id = "test.adapter",
    handler = "render",
    inputs = {{ list = dash.data_ref("test.list") }},
    dashboard_state_reads = {{ {adapter_reads} }},
}}
"#
        );
        let rt = format!(
            r#"
local calls = 0
local R = {{}}
function R.render(ctx)
    calls = calls + 1
    local repo = ctx:await(ctx.dashboard_state:get("{render_key_read}"))
    local rows = ctx:await(ctx.inputs.list:query({{}}))
    return {{ key = "root", widget_type = "dash.text", props = {{ repo = repo, calls = calls, rows = #rows }} }}
end
return R
"#
        );
        g.definitions
            .insert("test.adapter".into(), load_def(&decl, &rt));
        g.instances.insert(
            "adp".into(),
            instance(
                "adp",
                DefinitionCategory::Adapter,
                "test.adapter",
                &[("list", "inst_list")],
                json!({}),
            ),
        );
        for (id, repo) in [("alpha", "acme/alpha"), ("engineering", "acme/eng")] {
            g.dashboards.insert(
                id.into(),
                DashboardDoc {
                    id: id.into(),
                    title: id.into(),
                    layout: "grid".into(),
                    adapters: vec![DashboardAdapterPlacement {
                        adapter_instance: "adp".into(),
                        area: "main".into(),
                        query_params: json!({}),
                    }],
                    initial_state: BTreeMap::from([(
                        "selected_repository".to_string(),
                        json!(repo),
                    )]),
                    overrides: BTreeMap::new(),
                    state_schema: BTreeMap::new(),
                },
            );
        }
        g
    }

    fn props(rendered: &serde_json::Value) -> serde_json::Value {
        rendered["adapters"][0]["tree"]["props"].clone()
    }

    #[tokio::test]
    async fn adapter_reads_dashboard_state_of_the_rendered_dashboard() {
        let (_e, q) = engine_for(
            dashboards_graph("\"selected_repository\"", "selected_repository"),
            ResourceBudget::default(),
        );
        let admin = Principal::administrator();
        let eng = q.render_dashboard("engineering", &admin).await.unwrap();
        let alpha = q.render_dashboard("alpha", &admin).await.unwrap();
        assert_eq!(props(&eng)["repo"], "acme/eng");
        assert_eq!(props(&alpha)["repo"], "acme/alpha");
    }

    #[tokio::test]
    async fn adapter_cannot_read_undeclared_dashboard_state() {
        let (_e, q) = engine_for(
            dashboards_graph("\"selected_repository\"", "admin_only_key"),
            ResourceBudget::default(),
        );
        let err = q
            .render_dashboard("alpha", &Principal::administrator())
            .await
            .unwrap_err();
        assert!(err.contains("did not declare"), "{err}");
    }

    #[tokio::test]
    async fn adapter_render_is_cached_and_invalidated_by_state_and_upstream_changes() {
        let (e, q) = engine_for(
            dashboards_graph("\"selected_repository\"", "selected_repository"),
            ResourceBudget::default(),
        );
        let admin = Principal::administrator();
        let calls = |v: &serde_json::Value| props(v)["calls"].as_i64().unwrap();

        // Concurrent viewers share one evaluation.
        let renders =
            futures_util::future::join_all((0..10).map(|_| q.render_dashboard("alpha", &admin)))
                .await;
        for r in &renders {
            assert_eq!(calls(r.as_ref().unwrap()), 1);
        }
        let cached = q.render_dashboard("alpha", &admin).await.unwrap();
        assert_eq!(calls(&cached), 1, "second render served from cache");

        e.state
            .set(
                "dashboard",
                "alpha",
                "selected_repository",
                1,
                1,
                json!("acme/new"),
                None,
            )
            .unwrap();
        let after_state = q.render_dashboard("alpha", &admin).await.unwrap();
        assert_eq!(calls(&after_state), 2);
        assert_eq!(props(&after_state)["repo"], "acme/new");

        q.push_query_value(
            "inst_list",
            &json!({}),
            &admin,
            json!([{"id": "1"}, {"id": "2"}]),
        )
        .unwrap();
        let after_push = q.render_dashboard("alpha", &admin).await.unwrap();
        assert_eq!(calls(&after_push), 3);
        assert_eq!(props(&after_push)["rows"], 2);
        assert_eq!(after_push["adapters"][0]["status"], "fresh");

        // The render node and its dynamic edge to the input query are in the execution graph.
        let snap = q.execution_graph_snapshot();
        let render_key = after_push["adapters"][0]["signal_key"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(snap
            .nodes
            .iter()
            .any(|n| n.query_key == render_key && n.kind == "adapter_render"));
        assert!(snap.edges.iter().any(|e| e.from_query_key == render_key));
    }

    #[tokio::test]
    async fn widget_owner_resolves_only_currently_bound_widgets() {
        let (_e, q) = engine_for(
            dashboards_graph("\"selected_repository\"", "selected_repository"),
            ResourceBudget::default(),
        );
        let admin = Principal::administrator();
        // Not rendered yet: resolving renders the dashboard first.
        assert_eq!(
            q.widget_owner("alpha", "adp_root", &admin)
                .await
                .unwrap()
                .as_deref(),
            Some("adp")
        );
        assert_eq!(
            q.widget_owner("alpha", "adp_table", &admin).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn repeatedly_timing_out_adapter_trips_breaker_reset_by_generation() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        g.definitions.insert(
            "test.spin_adapter".into(),
            load_def(
                r#"local dash = require("dash"); return dash.adapter { id = "test.spin_adapter", handler = "render" }"#,
                "local R = {}; function R.render(ctx) while true do end end; return R",
            ),
        );
        g.instances.insert(
            "spin".into(),
            instance(
                "spin",
                DefinitionCategory::Adapter,
                "test.spin_adapter",
                &[],
                json!({}),
            ),
        );
        g.dashboards.insert(
            "d".into(),
            DashboardDoc {
                id: "d".into(),
                title: "d".into(),
                layout: "grid".into(),
                adapters: vec![DashboardAdapterPlacement {
                    adapter_instance: "spin".into(),
                    area: "main".into(),
                    query_params: json!({}),
                }],
                initial_state: BTreeMap::new(),
                overrides: BTreeMap::new(),
                state_schema: BTreeMap::new(),
            },
        );
        let budget = ResourceBudget {
            wall_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let (_e, q) = engine_for(g, budget);
        let admin = Principal::administrator();
        for _ in 0..BREAKER_THRESHOLD {
            let err = q.render_dashboard("d", &admin).await.unwrap_err();
            assert!(err.contains("timed out"), "{err}");
        }
        let err = q.render_dashboard("d", &admin).await.unwrap_err();
        assert!(
            err.contains("circuit breaker open for adapter 'spin'"),
            "{err}"
        );
        q.mark_generation_stale(&BTreeSet::from(["spin".to_string()]), 2);
        assert!(
            !q.circuit_open("spin"),
            "rebuilt Instance starts with a closed breaker"
        );
    }

    // ---- F1b regression tests ----

    /// Loopback HTTP server: every request is answered after `delay` (or never, when `None`).
    async fn slow_http_server(delay: Option<Duration>) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    let _ = sock.read(&mut buf).await;
                    match delay {
                        Some(d) => {
                            tokio::time::sleep(d).await;
                            let body = r#"{"ok":true}"#;
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = sock.write_all(resp.as_bytes()).await;
                        }
                        None => {
                            tokio::time::sleep(Duration::from_secs(60)).await;
                        }
                    }
                });
            }
        });
        addr
    }

    const HTTP_CONN_DECL: &str = r#"
local dash = require("dash")
return dash.connector {
    id = "t.http",
    operations = {
        fetch = { read_only = true },
        merge = {},
        merge_swallowing = {},
    },
}
"#;
    const HTTP_CONN_RT: &str = r#"
local R = {}
function R.call(ctx)
    local method = (ctx.operation == "fetch") and "GET" or "POST"
    local req = { method = method, url = ctx.config.url, body = { merge = true } }
    if ctx.operation == "merge_swallowing" then
        local ok = pcall(function() return ctx:await(ctx.http:request(req)) end)
        error("upstream failed")
    end
    local r = ctx:await(ctx.http:request(req))
    return { status = r.status }
end
return R
"#;

    /// Graph with an HTTP connector (`conn`), a data provider and an action using it, with
    /// loopback egress allowed; the store gets a matching Administrator ceiling.
    fn http_graph(url: &str, resources: crate::model::WorkspaceResources) -> ConfiguredGraph {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        g.workspace.network.allowed_hosts = vec!["127.0.0.1".into()];
        g.workspace.network.allowed_methods = vec!["GET".into(), "POST".into()];
        g.workspace.network.allow_loopback_for_tests = true;
        g.workspace.resources = resources;
        g.definitions
            .insert("t.http".into(), load_def(HTTP_CONN_DECL, HTTP_CONN_RT));
        g.instances.insert(
            "conn".into(),
            instance(
                "conn",
                DefinitionCategory::Connector,
                "t.http",
                &[],
                json!({ "url": url }),
            ),
        );
        g.definitions.insert(
            "t.reader".into(),
            load_def(
                r#"local dash = require("dash")
return dash.data_provider { id = "t.reader", inputs = { c = dash.connector_ref("t.http") } }"#,
                r#"local R = {}
function R.query(ctx) return ctx:await(ctx.inputs.c:call("fetch", {})) end
return R"#,
            ),
        );
        g.instances.insert(
            "reader".into(),
            instance(
                "reader",
                DefinitionCategory::DataProvider,
                "t.reader",
                &[("c", "conn")],
                json!({}),
            ),
        );
        g.definitions.insert(
            "t.merge".into(),
            load_def(
                r#"local dash = require("dash")
return dash.action_provider { id = "t.merge", inputs = { c = dash.connector_ref("t.http") } }"#,
                r#"local R = {}
function R.execute(ctx) return ctx:await(ctx.inputs.c:call(ctx.args.op, {})) end
return R"#,
            ),
        );
        g.instances.insert(
            "merge".into(),
            instance(
                "merge",
                DefinitionCategory::ActionProvider,
                "t.merge",
                &[("c", "conn")],
                json!({}),
            ),
        );
        g
    }

    fn http_engine(g: ConfiguredGraph, budget: ResourceBudget) -> RuntimeEngine {
        let store = SqliteStore::open_in_memory().unwrap();
        store
            .set_network_policy_ceiling(
                json!({
                    "allowed_hosts": ["127.0.0.1"],
                    "allowed_methods": ["GET", "POST"],
                    "allow_loopback_for_tests": true
                }),
                "admin",
            )
            .unwrap();
        RuntimeEngine::new(g, store, budget).unwrap()
    }

    fn timeout_override(ms: u64) -> crate::model::WorkspaceResources {
        crate::model::WorkspaceResources {
            defaults: dash_luau::ResourceOverrides {
                timeout_ms: Some(ms),
                ..Default::default()
            },
            instances: BTreeMap::new(),
        }
    }

    // engine-bridge-deadline-ignores-instance-budget / ACT-DEADLINE-1: host awaits are bounded
    // by the Instance's effective budget (`[resources]`), not the pool's base budget.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_awaits_honor_raised_resource_timeouts() {
        let addr = slow_http_server(Some(Duration::from_millis(700))).await;
        let base = ResourceBudget {
            wall_timeout: Duration::from_millis(300),
            ..Default::default()
        };
        let e = http_engine(
            http_graph(&format!("http://{addr}/x"), timeout_override(5000)),
            base,
        );
        let admin = Principal::administrator();
        // Nested connector call of a data provider (query_input/call_input/http_request awaits).
        let env = e
            .queries
            .query("reader", json!({}), &admin, false)
            .await
            .unwrap();
        assert_eq!(env.value.unwrap()["status"], 200);
        // The failure scenario: a slow merge with raised timeouts succeeds instead of being
        // cut at the base budget and recorded as a retry-safe `failed`.
        let rec = e
            .actions
            .invoke("merge", json!({ "op": "merge" }), &admin, None, None)
            .await
            .unwrap();
        assert_eq!(rec.status, "succeeded", "{:?}", rec.reason);
    }

    // ACT-DEADLINE-1: with raised `[resources]` timeouts, a write still in flight when the base
    // budget elapses is no longer abandoned early as a plain (retry-safe) failure; when it is cut
    // at the effective deadline the outcome is `unknown`, also when the connector swallows the
    // host error and raises its own.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abandoned_in_flight_write_is_unknown_outcome() {
        let addr = slow_http_server(None).await;
        let base = ResourceBudget {
            wall_timeout: Duration::from_millis(200),
            ..Default::default()
        };
        let e = http_engine(
            http_graph(&format!("http://{addr}/merge"), timeout_override(1000)),
            base,
        );
        let admin = Principal::administrator();
        for op in ["merge", "merge_swallowing"] {
            let started = Instant::now();
            let err = e
                .queries
                .invoke_connector("conn", op, json!({}), &admin, "tr_t")
                .await
                .unwrap_err();
            assert!(
                started.elapsed() >= Duration::from_millis(900),
                "{op}: the effective 1000ms budget applies, not the 200ms base"
            );
            assert!(
                crate::actions::is_unknown_outcome_error(&err),
                "{op}: {err}"
            );
            let rec = e
                .actions
                .invoke("merge", json!({ "op": op }), &admin, None, None)
                .await
                .unwrap();
            assert_eq!(rec.status, "unknown", "{op}: {:?}", rec.reason);
            assert!(!rec.retry_safe, "{op}: the key must not be released");
        }
    }

    // ACT-DEADLINE-1: an HTTP await abandoned by the bridge (deadline or cancellation) reports a
    // non-safe request as UNKNOWN_OUTCOME, flags the execution, and audits the request.
    #[tokio::test]
    async fn bridge_abandoning_a_write_reports_unknown_outcome_and_audits_it() {
        let addr = slow_http_server(None).await;
        let e = http_engine(
            http_graph(&format!("http://{addr}/merge"), Default::default()),
            ResourceBudget::default(),
        );
        let g = e.queries.graph();
        let mk = |deadline: Instant| EngineHostBridge {
            engine: e.queries.clone(),
            graph: Arc::clone(&g),
            caller_inst: g.instances["conn"].clone(),
            caller_def: g.definitions["t.http"].clone(),
            principal: Principal::administrator(),
            trace_id: "tr_b".into(),
            parent_query_key: None,
            call_stack: Vec::new(),
            recorded_deps: Arc::new(Mutex::new(BTreeSet::new())),
            recorded_upstreams: Arc::new(Mutex::new(Vec::new())),
            recorded_state: Arc::new(Mutex::new(BTreeMap::new())),
            dashboard_id: None,
            operation: Some("merge".into()),
            deadline,
            cancel: CancelSignal::new(),
            outcome_unknown: Arc::new(AtomicBool::new(false)),
            batches: BatchMap::default(),
        };
        let req = |method: &str| HttpHostRequest {
            method: method.into(),
            url: format!("http://{addr}/merge"),
            headers: BTreeMap::new(),
            body: None,
            credential: None,
            confine_to: None,
        };
        let post = mk(Instant::now() + Duration::from_millis(150));
        let err = post.http_request(req("POST")).await.unwrap_err();
        assert!(crate::actions::is_unknown_outcome_error(&err), "{err}");
        assert!(post.outcome_unknown.load(Ordering::SeqCst));
        let get = mk(Instant::now() + Duration::from_millis(150));
        let err = get.http_request(req("GET")).await.unwrap_err();
        assert!(!crate::actions::is_unknown_outcome_error(&err), "{err}");
        assert!(!get.outcome_unknown.load(Ordering::SeqCst));
        let audits = e.store.list_audits(200).unwrap();
        let outcomes: Vec<&str> = audits
            .iter()
            .filter(|a| a.operation == "http.request" && a.target == "conn")
            .map(|a| a.outcome.as_str())
            .collect();
        assert!(outcomes.contains(&"unknown"), "{outcomes:?}");
        assert!(outcomes.contains(&"error"), "{outcomes:?}");
    }

    const TYPED_CONN_DECL: &str = r#"
local dash = require("dash")
return dash.connector {
    id = "t.typed",
    operations = {
        get = {
            read_only = true,
            args = dash.object { id = dash.string(), bad = dash.optional(dash.boolean()) },
            result = dash.object { id = dash.string(), n = dash.integer() },
        },
        write = {
            args = dash.object { bad = dash.optional(dash.boolean()) },
            result = dash.object { ok = dash.boolean() },
        },
        calls = { read_only = true },
    },
}
"#;
    const TYPED_CONN_RT: &str = r#"
local calls = 0
local R = {}
function R.call(ctx)
    if ctx.operation == "calls" then return { calls = calls } end
    calls = calls + 1
    if ctx.operation == "get" then
        if ctx.args.bad then return { items = {} } end
        return { id = ctx.args.id, n = 1 }
    end
    if ctx.args.bad then return { ok = "yes" } end
    return { ok = true }
end
return R
"#;

    fn typed_graph() -> ConfiguredGraph {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        g.definitions
            .insert("t.typed".into(), load_def(TYPED_CONN_DECL, TYPED_CONN_RT));
        g.instances.insert(
            "typed".into(),
            instance(
                "typed",
                DefinitionCategory::Connector,
                "t.typed",
                &[],
                json!({}),
            ),
        );
        g
    }

    // SC-3: connector operation args and results are validated at the boundary.
    #[tokio::test]
    async fn connector_operation_args_and_results_are_validated() {
        let (e, q) = engine_for(typed_graph(), ResourceBudget::default());
        let admin = Principal::administrator();
        let call = |op: &'static str, args: serde_json::Value| {
            let q = q.clone();
            let admin = admin.clone();
            async move { q.invoke_connector("typed", op, args, &admin, "tr").await }
        };
        let err = call("get", json!({ "id": 5 })).await.unwrap_err();
        assert!(err.contains("invalid args"), "{err}");
        assert_eq!(
            call("calls", json!({})).await.unwrap()["calls"],
            0,
            "invalid args are rejected before the handler runs"
        );
        assert_eq!(call("get", json!({ "id": "a" })).await.unwrap()["n"], 1);

        let err = call("get", json!({ "id": "a", "bad": true }))
            .await
            .unwrap_err();
        assert!(err.contains("output contract violation"), "{err}");
        assert!(!crate::actions::is_unknown_outcome_error(&err), "{err}");

        // A malformed result of a write cannot prove the write did not happen.
        let err = call("write", json!({ "bad": true })).await.unwrap_err();
        assert!(err.contains("output contract violation"), "{err}");
        assert!(crate::actions::is_unknown_outcome_error(&err), "{err}");
        drop(e);
    }

    // SC-3: a Mock standing in for a connector is held to the target's operation contract.
    #[tokio::test]
    async fn mock_results_are_validated_against_the_target_contract() {
        let mut g = typed_graph();
        g.is_preview = true;
        g.definitions.insert(
            "t.typed_mock".into(),
            load_def(
                r#"local dash = require("dash")
return dash.mock { id = "t.typed_mock", ["for"] = "t.typed", operations = { get = {} } }"#,
                r#"local R = {}
function R.call(ctx)
    if ctx.args.id == "drift" then return { items = {} } end
    return { id = ctx.args.id, n = 7 }
end
return R"#,
            ),
        );
        let (_e, q) = engine_for(g, ResourceBudget::default());
        let admin = Principal::administrator();
        let ok = q
            .invoke_connector("typed", "get", json!({ "id": "a" }), &admin, "tr")
            .await
            .unwrap();
        assert_eq!(ok["n"], 7);
        let err = q
            .invoke_connector("typed", "get", json!({ "id": "drift" }), &admin, "tr")
            .await
            .unwrap_err();
        assert!(err.contains("output contract violation"), "{err}");
    }

    fn widget_graph() -> ConfiguredGraph {
        let mut g = list_and_count_graph();
        g.definitions.insert(
            "t.badge".into(),
            load_def(
                r#"local dash = require("dash")
return dash.widget_provider { id = "t.badge", asset = "index.html", props = dash.object { count = dash.integer() } }"#,
                "",
            ),
        );
        g
    }

    // SC-4: Adapter output is checked against widget provider contracts during render.
    #[test]
    fn invalid_widget_nodes_are_replaced_with_error_nodes() {
        let g = widget_graph();
        let tree = json!({
            "key": "root",
            "widget_type": "dash.container",
            "children": [
                { "key": "ok", "widget_type": "t.badge", "props": { "count": 3 } },
                { "key": "bad", "widget_type": "t.badge", "props": { "count": "x" } },
                { "key": "notwidget", "widget_type": "test.list", "props": {} },
                { "key": "unknown", "widget_type": "t.missing" },
            ]
        });
        let mut diags = Vec::new();
        let out = annotate_widget_ids(
            &g,
            tree,
            "adp",
            &BTreeMap::new(),
            &mut HashMap::new(),
            &mut diags,
        )
        .unwrap();
        let kids = out["children"].as_array().unwrap();
        assert_eq!(kids[0]["widget_type"], "t.badge");
        assert_eq!(kids[0]["props"]["count"], 3);
        for (i, needle) in [
            (1, "props"),
            (2, "not a widget provider"),
            (3, "unknown widget type"),
        ] {
            assert_eq!(kids[i]["widget_type"], "dash.text", "{:?}", kids[i]);
            assert!(kids[i].get("props").unwrap()["text"]
                .as_str()
                .unwrap()
                .contains(needle));
            assert!(kids[i]["render_error"]
                .as_str()
                .unwrap()
                .starts_with("WIDGET_CONTRACT_VIOLATION"));
            assert!(kids[i]["widget_id"].as_str().unwrap().starts_with("adp_"));
        }
        assert_eq!(
            diags
                .iter()
                .filter(|d| d.starts_with("WIDGET_CONTRACT_VIOLATION"))
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn render_reports_widget_contract_violations() {
        let mut g = widget_graph();
        g.definitions.insert(
            "test.badges".into(),
            load_def(
                r#"local dash = require("dash")
return dash.adapter { id = "test.badges", inputs = { list = dash.data_ref("test.list") } }"#,
                r#"local R = {}
function R.render(ctx)
    local rows = ctx:await(ctx.inputs.list:query({}))
    return { key = "badge", widget_type = "t.badge", props = { count = "many" } }
end
return R"#,
            ),
        );
        g.instances.insert(
            "badges".into(),
            instance(
                "badges",
                DefinitionCategory::Adapter,
                "test.badges",
                &[("list", "inst_list")],
                json!({}),
            ),
        );
        g.dashboards.insert(
            "d".into(),
            DashboardDoc {
                id: "d".into(),
                title: "d".into(),
                layout: "grid".into(),
                adapters: vec![DashboardAdapterPlacement {
                    adapter_instance: "badges".into(),
                    area: "main".into(),
                    query_params: json!({}),
                }],
                initial_state: BTreeMap::new(),
                overrides: BTreeMap::new(),
                state_schema: BTreeMap::new(),
            },
        );
        let (_e, q) = engine_for(g, ResourceBudget::default());
        let out = q
            .render_dashboard("d", &Principal::administrator())
            .await
            .unwrap();
        let tree = &out["adapters"][0]["tree"];
        assert_eq!(tree["widget_type"], "dash.text");
        assert_eq!(tree["widget_id"], "badges_badge");
        assert!(out["adapters"][0]["diagnostics"][0]
            .as_str()
            .unwrap()
            .contains("WIDGET_CONTRACT_VIOLATION"));
    }

    // SC-8: an Adapter that declares no dashboard_state_reads reads no Dashboard State.
    #[tokio::test]
    async fn adapter_without_declared_reads_cannot_read_dashboard_state() {
        let (_e, q) = engine_for(
            dashboards_graph("", "selected_repository"),
            ResourceBudget::default(),
        );
        let err = q
            .render_dashboard("alpha", &Principal::administrator())
            .await
            .unwrap_err();
        assert!(err.contains("did not declare"), "{err}");
    }

    // SC-12: shutdown cancellation stops running reads well before their wall timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_all_in_flight_stops_running_queries() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        provider(
            &mut g,
            "inst_spin",
            "test.spin",
            &[],
            "local R = {}; function R.query(ctx) while true do end end; return R",
        );
        let budget = ResourceBudget {
            wall_timeout: Duration::from_secs(30),
            max_interrupts: u64::MAX,
            ..Default::default()
        };
        let (_e, q) = engine_for(g, budget);
        let q1 = q.clone();
        let started = Instant::now();
        let caller = tokio::spawn(async move {
            q1.query("inst_spin", json!({}), &Principal::administrator(), false)
                .await
        });
        while q.in_flight.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(q.cancel_all_in_flight(), 1);
        let res = tokio::time::timeout(Duration::from_secs(10), caller)
            .await
            .expect("query must stop after cancellation")
            .unwrap();
        assert!(res.is_err());
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    const PUSH_DECL: &str = r#"
local dash = require("dash")
return dash.data_provider { id = "test.pushy", updates = { subscribe = true, push = true } }
"#;
    const PUSH_RT: &str = r#"
local R = {}
function R.query(ctx) return { { id = "1", state = "open" } } end
function R.subscribe(ctx)
    local a = ctx:push_delta({ sequence = 1, ops = { { op = "update", key = "1", value = { id = "1", state = "merged" } } } })
    local b = ctx:push_delta({ sequence = 1, ops = { { op = "delete", key = "1" } } })
    local c = ctx:push_delta({ sequence = 2, ops = { { op = "insert", key = "2", value = { id = "2", state = "open" } } } })
    if a.outcome ~= "applied" or b.outcome ~= "duplicate" or c.outcome ~= "applied" then
        error("unexpected outcomes " .. a.outcome .. "/" .. b.outcome .. "/" .. c.outcome)
    end
    return true
end
return R
"#;

    // F1b: a long-running subscribe hook does not hold the provider's VM or admission, so the
    // provider's queries do not queue behind it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn long_running_subscribe_hook_does_not_block_the_providers_queries() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        g.definitions.insert(
            "test.waiting".into(),
            load_def(
                r#"local dash = require("dash")
return dash.data_provider { id = "test.waiting", updates = { subscribe = true } }"#,
                r#"local R = {}
function R.query(ctx) return { { id = "1" } } end
function R.subscribe(ctx) while true do end end
return R"#,
            ),
        );
        g.instances.insert(
            "waiting".into(),
            instance(
                "waiting",
                DefinitionCategory::DataProvider,
                "test.waiting",
                &[],
                json!({}),
            ),
        );
        let budget = ResourceBudget {
            wall_timeout: Duration::from_secs(3),
            max_interrupts: u64::MAX / 2,
            queue_timeout: Duration::from_secs(10),
            ..ResourceBudget::default()
        };
        let (e, q) = engine_for(g, budget);
        let admin = Principal::administrator();
        let first = q.query("waiting", json!({}), &admin, false).await.unwrap();
        let _rx = e.signals.subscribe_stream(&first.query_key);
        q.query("waiting", json!({}), &admin, false).await.unwrap();
        assert!(q.provider_subscription_active(&first.query_key));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let t0 = Instant::now();
        let again = q.query("waiting", json!({}), &admin, true).await.unwrap();
        assert_eq!(again.status, QueryStatus::Fresh);
        assert!(
            t0.elapsed() < Duration::from_millis(1500),
            "the query queued behind the subscribe hook ({:?})",
            t0.elapsed()
        );
    }

    // SC-6: provider subscribe hooks run for subscribed queries and push sequence-checked keyed
    // deltas through ctx:push_delta.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_hook_pushes_keyed_deltas() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        g.definitions
            .insert("test.pushy".into(), load_def(PUSH_DECL, PUSH_RT));
        g.instances.insert(
            "pushy".into(),
            instance(
                "pushy",
                DefinitionCategory::DataProvider,
                "test.pushy",
                &[],
                json!({}),
            ),
        );
        let (e, q) = engine_for(g, ResourceBudget::default());
        let admin = Principal::administrator();
        let first = q.query("pushy", json!({}), &admin, false).await.unwrap();
        assert!(
            !q.provider_subscription_active(&first.query_key),
            "no subscribers: no subscription"
        );
        let mut rx = e.signals.subscribe_stream(&first.query_key);
        q.query("pushy", json!({}), &admin, false).await.unwrap();
        assert!(q.provider_subscription_active(&first.query_key));

        let mut deltas = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while deltas.len() < 2 {
            let env = tokio::time::timeout_at(deadline, rx.recv())
                .await
                .expect("pushed deltas")
                .unwrap();
            if env.kind == crate::signal::EnvelopeKind::Delta {
                deltas.push(env);
            }
        }
        assert_eq!(deltas[0].source_sequence, Some(1));
        assert_eq!(deltas[1].source_sequence, Some(2));
        let now = q.query("pushy", json!({}), &admin, false).await.unwrap();
        assert_eq!(
            now.value.unwrap(),
            json!([{ "id": "1", "state": "merged" }, { "id": "2", "state": "open" }])
        );
        // Shared: further queries do not start the hook again; exactly one run is recorded.
        let runs = || {
            e.telemetry
                .recent_spans(50)
                .iter()
                .filter(|s| s.name == "subscribe:pushy")
                .count()
        };
        for _ in 0..100 {
            if runs() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        q.query("pushy", json!({}), &admin, false).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(runs(), 1);

        // Reference count zero releases the subscription.
        drop(rx);
        let mut released = false;
        for _ in 0..100 {
            q.prune_idle();
            if !q.provider_subscription_active(&first.query_key) {
                released = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(released, "subscription released once unsubscribed");
    }

    // SC-6: `updates.subscribe` without a `subscribe` function keeps one shared, hook-less
    // subscription per query (looked up once, not retried on every query).
    #[tokio::test]
    async fn subscribe_mode_without_hook_is_shared_without_retries() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        g.definitions.insert(
            "test.subonly".into(),
            load_def(
                r#"local dash = require("dash")
return dash.data_provider { id = "test.subonly", updates = { subscribe = true } }"#,
                r#"local R = {}; function R.query(ctx) return {} end; return R"#,
            ),
        );
        g.instances.insert(
            "subonly".into(),
            instance(
                "subonly",
                DefinitionCategory::DataProvider,
                "test.subonly",
                &[],
                json!({}),
            ),
        );
        let (e, q) = engine_for(g, ResourceBudget::default());
        let admin = Principal::administrator();
        let qk = q.resolve_query_key("subonly", &json!({}), &admin).unwrap();
        let _rx = e.signals.subscribe_stream(&qk);
        for _ in 0..3 {
            q.query("subonly", json!({}), &admin, true).await.unwrap();
        }
        let spans = || {
            e.telemetry
                .recent_spans(50)
                .into_iter()
                .filter(|s| s.name == "subscribe:subonly")
                .collect::<Vec<_>>()
        };
        for _ in 0..100 {
            if !spans().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let spans = spans();
        assert_eq!(spans.len(), 1, "looked up once");
        assert_eq!(spans[0].status, "ok");
        assert!(q.provider_subscription_active(&qk));
    }

    // SC-6: pushing requires `updates.push` and is limited to data providers.
    #[tokio::test]
    async fn push_requires_declared_push_updates() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        provider(
            &mut g,
            "nopush",
            "test.nopush",
            &[],
            r#"local R = {}
function R.query(ctx)
    ctx:push_snapshot({ value = {} })
    return {}
end
return R"#,
        );
        let (_e, q) = engine_for(g, ResourceBudget::default());
        let err = q
            .query("nopush", json!({}), &Principal::administrator(), false)
            .await
            .unwrap_err();
        assert!(err.contains("does not declare push updates"), "{err}");
    }

    // SC-13: dependency-query options are pushed to providers that declare them and applied by
    // the host otherwise, with identical results.
    #[tokio::test]
    async fn pushdown_options_are_pushed_when_declared_and_applied_by_host_otherwise() {
        let rows_rt = |pushdown_aware: bool| {
            format!(
                r#"
local ROWS = {{
    {{ id = "a", state = "open", n = 3, title = "A" }},
    {{ id = "b", state = "closed", n = 1, title = "B" }},
    {{ id = "c", state = "open", n = 2, title = "C" }},
    {{ id = "d", state = "open", n = 5, title = "D" }},
}}
local R = {{}}
function R.query(ctx)
    local pd = ctx.query._pushdown
    if {pushdown_aware} and pd ~= nil then
        local out = {{}}
        for _, r in ipairs(ROWS) do
            if pd.filter == nil or r.state == pd.filter.state then table.insert(out, r) end
        end
        table.sort(out, function(x, y) return x.n > y.n end)
        local res = {{}}
        for i = pd.page.offset + 1, math.min(#out, pd.page.offset + pd.page.limit) do
            table.insert(res, {{ id = out[i].id, n = out[i].n, pushed = true }})
        end
        return res
    end
    return ROWS
end
return R
"#
            )
        };
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        for (def_id, inst_id, decl_extra, aware) in [
            (
                "test.smart",
                "smart",
                r#", pushdown = { filter = { "state" }, sort = { "n" }, page = true, projection = true }"#,
                true,
            ),
            ("test.plain", "plain", "", false),
        ] {
            g.definitions.insert(
                def_id.into(),
                load_def(
                    &format!(
                        "local dash = require(\"dash\")\nreturn dash.data_provider {{ id = \"{def_id}\"{decl_extra} }}"
                    ),
                    &rows_rt(aware),
                ),
            );
            g.instances.insert(
                inst_id.into(),
                instance(
                    inst_id,
                    DefinitionCategory::DataProvider,
                    def_id,
                    &[],
                    json!({}),
                ),
            );
        }
        let consumer_rt = r#"local R = {}
function R.query(ctx)
    local opts = { filter = { state = "open" }, sort = { { field = "n", desc = true } }, page = { offset = 1, limit = 2 }, fields = { "id", "n" } }
    local a = ctx:await(ctx.inputs.smart:query({ _pushdown = opts }))
    local b = ctx:await(ctx.inputs.plain:query({ _pushdown = opts }))
    return { smart = a, plain = b }
end
return R"#;
        provider(
            &mut g,
            "consumer",
            "test.consumer",
            &[("smart", "smart"), ("plain", "plain")],
            consumer_rt,
        );
        let (_e, q) = engine_for(g, ResourceBudget::default());
        let out = q
            .query("consumer", json!({}), &Principal::administrator(), false)
            .await
            .unwrap()
            .value
            .unwrap();
        // The declaring provider evaluated every stage itself (rows carry `pushed`).
        assert_eq!(
            out["smart"],
            json!([{ "id": "a", "n": 3, "pushed": true }, { "id": "c", "n": 2, "pushed": true }])
        );
        // The plain provider returned everything; the host filtered, sorted, paged, projected.
        assert_eq!(
            out["plain"],
            json!([{ "id": "a", "n": 3 }, { "id": "c", "n": 2 }])
        );
    }

    // F1b: a provider declaring `projection = true` and a record output contract with
    // required fields may honour a pushed projection without failing its own contract; fields
    // it does return are still checked.
    #[tokio::test]
    async fn pushed_projection_is_checked_against_the_projected_contract() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        g.definitions.insert(
            "test.typed".into(),
            load_def(
                r#"local dash = require("dash")
return dash.data_provider { id = "test.typed", pushdown = { projection = true },
  output = dash.list(dash.object { id = dash.string(), n = dash.number(), title = dash.string() }) }"#,
                r#"local ROWS = { { id = "a", n = 1, title = "A" }, { id = "b", n = 2, title = "B" } }
local R = {}
function R.query(ctx)
    local pd = ctx.query._pushdown
    if pd == nil or pd.fields == nil then return ROWS end
    local out = {}
    for _, r in ipairs(ROWS) do
        local row = {}
        for _, f in ipairs(pd.fields) do row[f] = r[f] end
        if ctx.query.corrupt then row.n = "not a number" end
        table.insert(out, row)
    end
    return out
end
return R"#,
            ),
        );
        g.instances.insert(
            "typed".into(),
            instance(
                "typed",
                DefinitionCategory::DataProvider,
                "test.typed",
                &[],
                json!({}),
            ),
        );
        provider(
            &mut g,
            "consumer",
            "test.consumer",
            &[("typed", "typed")],
            r#"local R = {}
function R.query(ctx)
    return ctx:await(ctx.inputs.typed:query({ corrupt = ctx.query.corrupt, _pushdown = { fields = ctx.query.fields } }))
end
return R"#,
        );
        let (_e, q) = engine_for(g, ResourceBudget::default());
        let admin = Principal::administrator();
        let out = q
            .query("consumer", json!({ "fields": ["id"] }), &admin, false)
            .await
            .unwrap();
        assert_eq!(out.status, QueryStatus::Fresh, "{out:?}");
        assert_eq!(out.value.unwrap(), json!([{ "id": "a" }, { "id": "b" }]));
        let bad = q
            .query(
                "consumer",
                json!({ "fields": ["id", "n"], "corrupt": true }),
                &admin,
                false,
            )
            .await;
        let msg = match bad {
            Ok(env) => env.error.unwrap_or_default(),
            Err(e) => e,
        };
        assert!(msg.contains("output contract violation"), "{msg}");
    }

    #[test]
    fn pushdown_split_keeps_stage_order() {
        let opts = QueryOptions::parse(&json!({
            "filter": { "state": "open" },
            "sort": ["n"],
            "page": { "offset": 0, "limit": 1 },
        }))
        .unwrap();
        // Page is declared but filter is not: filtering stays on the host, so paging must too.
        let spec = dash_luau::manifest::PushdownSpec {
            page: true,
            sort: vec!["n".into()],
            ..Default::default()
        };
        let (pushed, host) = opts.split(&spec);
        assert!(pushed.is_empty());
        assert_eq!(host, opts);
        assert!(QueryOptions::parse(&json!({ "bogus": 1 })).is_err());
    }

    // SC-13: concurrent dependency queries to a batch-capable provider are merged into one.
    #[tokio::test]
    async fn concurrent_dependency_queries_are_batched() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        g.definitions.insert(
            "test.issues".into(),
            load_def(
                r#"local dash = require("dash")
return dash.data_provider {
    id = "test.issues",
    query = dash.object { keys = dash.list(dash.string()) },
    batch = { param = "keys", key_field = "key" },
}"#,
                r#"local runs = 0
local R = {}
function R.query(ctx)
    runs = runs + 1
    local out = {}
    for _, k in ipairs(ctx.query.keys) do
        table.insert(out, { key = k, runs = runs, batch = #ctx.query.keys })
    end
    return out
end
return R"#,
            ),
        );
        g.instances.insert(
            "issues".into(),
            instance(
                "issues",
                DefinitionCategory::DataProvider,
                "test.issues",
                &[],
                json!({}),
            ),
        );
        provider(
            &mut g,
            "linked",
            "test.linked",
            &[("issues", "issues")],
            r#"local R = {}
function R.query(ctx)
    local res = ctx:await_all({
        ctx.inputs.issues:query({ keys = { "A", "B" } }),
        ctx.inputs.issues:query({ keys = { "B", "C" } }),
        ctx.inputs.issues:query({ keys = { "D" } }),
    })
    return res
end
return R"#,
        );
        let (_e, q) = engine_for(g, ResourceBudget::default());
        let out = q
            .query("linked", json!({}), &Principal::administrator(), false)
            .await
            .unwrap()
            .value
            .unwrap();
        let keys = |i: usize| -> Vec<String> {
            out[i]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["key"].as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(keys(0), vec!["A", "B"]);
        assert_eq!(keys(1), vec!["B", "C"]);
        assert_eq!(keys(2), vec!["D"]);
        for i in 0..3 {
            for r in out[i].as_array().unwrap() {
                assert_eq!(r["runs"], 1, "one provider run served every caller");
                assert_eq!(r["batch"], 4, "merged union of keys A, B, C, D");
            }
        }
    }

    // SC-7: Instance State writes follow the Definition's declaration.
    #[tokio::test]
    async fn instance_state_writes_follow_declarations() {
        let mut g = ConfiguredGraph {
            generation_id: 1,
            ..Default::default()
        };
        g.definitions.insert(
            "test.cursor".into(),
            load_def(
                r#"local dash = require("dash")
return dash.data_provider {
    id = "test.cursor",
    state = { cursor = { schema_version = 2, type = "object", max_bytes = 64, migration = "reset" } },
}"#,
                r#"local R = {}
function R.query(ctx)
    if ctx.query.mode == "ok" then ctx:await(ctx.state:set("cursor", { page = 1 })) end
    if ctx.query.mode == "type" then ctx:await(ctx.state:set("cursor", "2026-11-01")) end
    if ctx.query.mode == "undeclared" then ctx:await(ctx.state:set("other", 1)) end
    if ctx.query.mode == "big" then ctx:await(ctx.state:set("cursor", { pad = string.rep("x", 100) })) end
    return {}
end
return R"#,
            ),
        );
        g.instances.insert(
            "cursor".into(),
            instance(
                "cursor",
                DefinitionCategory::DataProvider,
                "test.cursor",
                &[],
                json!({}),
            ),
        );
        let (e, q) = engine_for(g, ResourceBudget::default());
        let admin = Principal::administrator();
        q.query("cursor", json!({ "mode": "ok" }), &admin, false)
            .await
            .unwrap();
        let rec = e
            .state
            .get("instance", "cursor", "cursor")
            .unwrap()
            .unwrap();
        assert_eq!(rec.schema_version, 2, "declared schema version is stamped");
        for (mode, needle) in [
            ("type", "expects type 'object'"),
            ("undeclared", "not declared"),
            ("big", "limit is 64"),
        ] {
            let err = q
                .query("cursor", json!({ "mode": mode }), &admin, false)
                .await
                .unwrap_err();
            assert!(err.contains(needle), "{mode}: {err}");
        }
    }
}
