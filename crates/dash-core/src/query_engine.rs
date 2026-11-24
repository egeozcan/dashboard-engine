//! Reactive Query Engine: singleflight deduplication, partitioned caching, dynamic dependency
//! tracking, cycle isolation, stale retention, circuit breakers, keyed deltas, and Preview mocks.

use crate::authority::{
    check_connector_operation_authority, check_provider_access, Principal,
};
use crate::events::EventBus;
use crate::http_broker::HttpBroker;
use crate::model::{ConfiguredGraph, InstanceConfig, LoadedDefinition};
use crate::signal::{QueryEnvelope, QueryStatus, SignalHub};
use crate::state::StateManager;
use crate::telemetry::{
    ExecutionGraphEdge, ExecutionGraphNode, ExecutionGraphSnapshot, Telemetry,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dash_luau::{
    CompiledInstanceBundle, DefinitionCategory, ExecutionContextConfig, HostBridge,
    HttpHostRequest, HttpHostResponse, VmPool,
};
use dash_store::SqliteStore;
use dashmap::DashMap;
use lru::LruCache;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use uuid::Uuid;

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
    pub updated_at: DateTime<Utc>,
    pub stale: bool,
    pub last_error: Option<String>,
    pub hits: u64,
    pub misses: u64,
}

#[derive(Debug, Clone)]
struct CircuitState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
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
    in_flight: Arc<DashMap<String, broadcast::Sender<Result<QueryEnvelope, String>>>>,
    forward_deps: Arc<DashMap<String, BTreeSet<String>>>,
    reverse_deps: Arc<DashMap<String, BTreeSet<String>>>,
    circuit_breakers: Arc<DashMap<String, CircuitState>>,
    mock_kv: Arc<DashMap<String, serde_json::Value>>,
    cache_ttl: Duration,
}

impl QueryEngine {
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
            cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(2048).unwrap()))),
            in_flight: Arc::new(DashMap::new()),
            forward_deps: Arc::new(DashMap::new()),
            reverse_deps: Arc::new(DashMap::new()),
            circuit_breakers: Arc::new(DashMap::new()),
            mock_kv: Arc::new(DashMap::new()),
            cache_ttl: Duration::from_secs(30),
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

    pub fn cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
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
        let digest = if inst.definition == "dash.history_series" {
            "builtin:dash.history_series".to_string()
        } else {
            g.definitions
                .get(&inst.definition)
                .ok_or_else(|| format!("definition '{}' missing", inst.definition))?
                .manifest
                .identity_version
                .clone()
        };
        Ok(self.compute_query_key(instance_id, &digest, params, principal))
    }

    /// Execute or return cached query result (with singleflight dedup and stale-on-error fallback).
    pub async fn query(
        &self,
        instance_id: &str,
        params: serde_json::Value,
        principal: &Principal,
        force_refresh: bool,
    ) -> Result<QueryEnvelope, String> {
        self.query_with_trace(instance_id, params, principal, force_refresh, Vec::new(), None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_with_trace<'a>(
        &'a self,
        instance_id: &'a str,
        params: serde_json::Value,
        principal: &'a Principal,
        force_refresh: bool,
        mut call_stack: Vec<String>,
        parent_query_key: Option<String>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<QueryEnvelope, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let g = self.graph.load_full();
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

            check_provider_access(principal, &inst).map_err(|e| e.to_string())?;

            // Built-in history_series provider (§7.4)
            if inst.definition == "dash.history_series" {
                let series_id = params
                    .get("series_id")
                    .and_then(|v| v.as_str())
                    .or_else(|| inst.config.get("series_id").and_then(|v| v.as_str()))
                    .ok_or_else(|| "history_series query requires 'series_id'".to_string())?;
                let since = params.get("since").and_then(|v| v.as_str());
                let limit = params
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(500) as usize;
                let samples = self
                    .store
                    .query_history_samples(series_id, since, limit)
                    .map_err(|e| e.to_string())?;
                let val = serde_json::to_value(samples).map_err(|e| e.to_string())?;
                let qk = self.compute_query_key(instance_id, "builtin:dash.history_series", &params, principal);
                return Ok(QueryEnvelope::snapshot(
                    instance_id,
                    &qk,
                    g.generation_id,
                    1,
                    QueryStatus::Fresh,
                    Some("dash.history_series@1".into()),
                    val,
                    None,
                ));
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

            // Dynamic cycle detection (§6.1) — isolate failure to affected query
            if call_stack.contains(&query_key) {
                return Err(format!(
                    "dynamic dependency cycle detected involving '{instance_id}' (query_key={query_key})"
                ));
            }
            call_stack.push(query_key.clone());

            // Record dynamic edge if invoked as upstream dependency
            if let Some(ref parent_qk) = parent_query_key {
                self.record_dynamic_edge(parent_qk, &query_key);
            }

            // Fast cache hit path (bounded LRU + TTL + generation match)
            if !force_refresh {
                let maybe_hit = {
                    let mut cache = self.cache.lock().unwrap();
                    if let Some(entry) = cache.get_mut(&query_key) {
                        let age = Utc::now()
                            .signed_duration_since(entry.updated_at)
                            .to_std()
                            .unwrap_or(Duration::MAX);
                        if !entry.stale
                            && entry.generation_id == g.generation_id
                            && age < self.cache_ttl
                        {
                            entry.hits += 1;
                            self.telemetry.query_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Some(QueryEnvelope::snapshot(
                                instance_id,
                                &query_key,
                                g.generation_id,
                                entry.version,
                                QueryStatus::Fresh,
                                entry.schema.clone(),
                                entry.value.clone(),
                                None,
                            )
                            .with_updated_at(entry.updated_at))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                };
                if let Some(hit) = maybe_hit {
                    return Ok(hit);
                }
            }

            // Singleflight deduplication: attach to in-flight computation if one already exists!
            if let Some(tx) = self.in_flight.get(&query_key) {
                let mut rx = tx.subscribe();
                drop(tx);
                match rx.recv().await {
                    Ok(res) => return res,
                    Err(_) => {
                        // Leader dropped unexpectedly; fall through and compute
                    }
                }
            }

            let (tx, _rx) = broadcast::channel(16);
            // Insert singleflight channel atomically
            let leader = match self.in_flight.entry(query_key.clone()) {
                dashmap::mapref::entry::Entry::Occupied(occ) => {
                    let mut rx = occ.get().subscribe();
                    drop(occ);
                    match rx.recv().await {
                        Ok(res) => return res,
                        Err(_) => false,
                    }
                }
                dashmap::mapref::entry::Entry::Vacant(vac) => {
                    vac.insert(tx.clone());
                    true
                }
            };
            if !leader {
                // Retry once cleanly
                return self
                    .query_with_trace(instance_id, params, principal, force_refresh, call_stack, None)
                    .await;
            }

            self.telemetry.query_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let trace_id = format!("tr_{}", Uuid::now_v7().simple());
            let started = Instant::now();

            // First computation of a subscribed query: subscribers see `loading` (§6.1).
            if !self.cache.lock().unwrap().contains(&query_key) {
                self.signals.publish_query(QueryEnvelope::loading(
                    instance_id,
                    &query_key,
                    g.generation_id,
                    def.manifest
                        .output
                        .as_ref()
                        .and_then(|s| s.semantic_type_name().map(|n| n.to_string())),
                ));
            }

            // Check circuit breaker before executing provider
            let cb_tripped = self
                .circuit_breakers
                .get(instance_id)
                .and_then(|c| c.open_until)
                .map(|until| Instant::now() < until)
                .unwrap_or(false);

            let exec_result = if cb_tripped {
                Err(format!("circuit breaker open for '{instance_id}'"))
            } else if let Some(rule) = self.telemetry.take_injected_failure(instance_id) {
                Err(format!("injected failure ({}): {}", rule.mode, rule.message))
            } else {
                self.execute_provider_vm(
                    &g,
                    &inst,
                    &def,
                    &params,
                    principal,
                    &query_key,
                    &trace_id,
                    &call_stack,
                )
                .await
            };

            let duration_ms = started.elapsed().as_millis() as i64;
            let final_res = match exec_result {
                Ok((val, recorded_deps)) => {
                    self.circuit_breakers.remove(instance_id);
                    self.reconcile_dynamic_edges(&query_key, recorded_deps);

                    let schema_str = def
                        .manifest
                        .output
                        .as_ref()
                        .and_then(|s| s.semantic_type_name().map(|n| n.to_string()));

                    let success_at = Utc::now();
                    let (version, prev_val) = {
                        let mut cache = self.cache.lock().unwrap();
                        let prev = cache.get(&query_key).cloned();
                        let next_ver = prev.as_ref().map(|p| p.version + 1).unwrap_or(1);
                        let prev_v = prev.as_ref().map(|p| p.value.clone());
                        cache.put(
                            query_key.clone(),
                            CachedQueryEntry {
                                instance_id: instance_id.to_string(),
                                definition_digest: def.manifest.identity_version.clone(),
                                auth_fingerprint: principal.authorization_fingerprint(),
                                params: params.clone(),
                                principal: principal.clone(),
                                generation_id: g.generation_id,
                                version: next_ver,
                                status: QueryStatus::Fresh,
                                value: val.clone(),
                                schema: schema_str.clone(),
                                updated_at: success_at,
                                stale: false,
                                last_error: None,
                                hits: prev.as_ref().map(|p| p.hits).unwrap_or(0),
                                misses: prev.as_ref().map(|p| p.misses + 1).unwrap_or(1),
                            },
                        );
                        (next_ver, prev_v)
                    };

                    let snap = QueryEnvelope::snapshot(
                        instance_id,
                        &query_key,
                        g.generation_id,
                        version,
                        QueryStatus::Fresh,
                        schema_str.clone(),
                        val.clone(),
                        None,
                    )
                    .with_updated_at(success_at);

                    // Emit an order-preserving keyed delta (when computable) plus the latest snapshot
                    self.signals.publish_snapshot_with_delta(
                        prev_val.as_ref().map(|v| (version - 1, v)),
                        snap.clone(),
                        "id",
                    );

                    self.telemetry.record_span(
                        &trace_id,
                        None,
                        &format!("query:{instance_id}"),
                        &principal.id,
                        serde_json::json!({
                            "query_key": query_key,
                            "status": "fresh",
                        }),
                        "ok",
                        duration_ms,
                    );

                    Ok(snap)
                }
                Err(err_msg) => {
                    self.record_failure(instance_id);
                    self.telemetry.log(
                        "error",
                        "query_engine",
                        &principal.id,
                        &trace_id,
                        &format!("query '{instance_id}' failed: {err_msg}"),
                    );
                    self.telemetry.record_span(
                        &trace_id,
                        None,
                        &format!("query:{instance_id}"),
                        &principal.id,
                        serde_json::json!({ "query_key": query_key, "error": err_msg }),
                        "error",
                        duration_ms,
                    );

                    // Stale data retention (§6.1, §15.2): return last good value marked `stale`
                    let stale_env = {
                        let mut cache = self.cache.lock().unwrap();
                        if let Some(entry) = cache.get_mut(&query_key) {
                            entry.status = QueryStatus::Stale;
                            entry.stale = true;
                            entry.last_error = Some(err_msg.clone());
                            Some(QueryEnvelope::snapshot(
                                instance_id,
                                &query_key,
                                entry.generation_id,
                                entry.version,
                                QueryStatus::Stale,
                                entry.schema.clone(),
                                entry.value.clone(),
                                Some(err_msg.clone()),
                            )
                            .with_updated_at(entry.updated_at))
                        } else {
                            None
                        }
                    };
                    if let Some(env) = stale_env {
                        self.signals.publish_query(env.clone());
                        Ok(env)
                    } else {
                        // No last good value: subscribers get an `error` Signal.
                        self.signals.publish_query(QueryEnvelope::error(
                            instance_id,
                            &query_key,
                            g.generation_id,
                            def.manifest
                                .output
                                .as_ref()
                                .and_then(|s| s.semantic_type_name().map(|n| n.to_string())),
                            err_msg.clone(),
                        ));
                        Err(err_msg)
                    }
                }
            };

            self.in_flight.remove(&query_key);
            let _ = tx.send(final_res.clone());
            final_res
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_provider_vm(
        &self,
        graph: &ConfiguredGraph,
        inst: &InstanceConfig,
        def: &LoadedDefinition,
        params: &serde_json::Value,
        principal: &Principal,
        query_key: &str,
        trace_id: &str,
        call_stack: &[String],
    ) -> Result<(serde_json::Value, BTreeSet<String>), String> {
        let recorded_deps = Arc::new(Mutex::new(BTreeSet::new()));
        let bridge = Arc::new(EngineHostBridge {
            engine: self.clone(),
            graph: graph.clone(),
            caller_inst: inst.clone(),
            caller_def: def.clone(),
            principal: principal.clone(),
            trace_id: trace_id.to_string(),
            parent_query_key: Some(query_key.to_string()),
            call_stack: call_stack.to_vec(),
            recorded_deps: Arc::clone(&recorded_deps),
            operation: None,
        });

        let bundle = CompiledInstanceBundle {
            instance_id: inst.id.clone(),
            manifest: def.manifest.clone(),
            runtime_source: def.runtime_source.clone(),
            config_digest: inst.config_digest(),
            resolved_libraries: graph.resolved_libraries(),
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

        let val = self
            .pool
            .execute_handler(
                &bundle,
                handler,
                ExecutionContextConfig {
                    category: def.manifest.category,
                    instance_id: inst.id.clone(),
                    input_names: inst.inputs.keys().cloned().collect(),
                    config: inst.config.clone(),
                    payload: params.clone(),
                    operation: None,
                    idempotency_key: None,
                    allowed_emits,
                    now_iso: Utc::now().to_rfc3339(),
                    trace_id: trace_id.to_string(),
                    bridge,
                },
                None,
            )
            .await
            .map_err(|e| self.store.redactor().redact_str(&e.to_string()))?;

        // Validate output contract at boundary
        if let Some(ref out_schema) = def.manifest.output {
            out_schema
                .validate_value(&val)
                .map_err(|e| format!("output contract violation on '{}': {e}", inst.id))?;
        }

        let deps = recorded_deps.lock().unwrap().clone();
        Ok((val, deps))
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
        let g = self.graph.load_full();
        let conn_inst = g
            .instances
            .get(connector_id)
            .ok_or_else(|| format!("connector instance '{connector_id}' not found"))?
            .clone();

        check_connector_operation_authority(principal, &conn_inst, operation, &g)
            .map_err(|e| e.to_string())?;

        if let Some(rule) = self.telemetry.take_injected_failure(connector_id) {
            return Err(format!("injected failure ({}): {}", rule.mode, rule.message));
        }

        // In Preview mode, substitute matching Mock Definition (§10.3)
        let (eff_def, eff_cat) = if g.is_preview {
            let mock_def = g.find_mock_for(&conn_inst.definition).ok_or_else(|| {
                format!(
                    "preview requires a Mock definition for connector '{}'",
                    conn_inst.definition
                )
            })?;
            (mock_def.clone(), DefinitionCategory::Mock)
        } else {
            let real_def = g
                .definitions
                .get(&conn_inst.definition)
                .ok_or_else(|| format!("connector definition '{}' missing", conn_inst.definition))?;
            (real_def.clone(), DefinitionCategory::Connector)
        };

        let bridge = Arc::new(EngineHostBridge {
            engine: self.clone(),
            graph: (*g).clone(),
            caller_inst: conn_inst.clone(),
            caller_def: eff_def.clone(),
            principal: principal.clone(),
            trace_id: trace_id.to_string(),
            parent_query_key: None,
            call_stack: Vec::new(),
            recorded_deps: Arc::new(Mutex::new(BTreeSet::new())),
            operation: Some(operation.to_string()),
        });

        let bundle = CompiledInstanceBundle {
            instance_id: if g.is_preview {
                format!("preview_mock:{}", conn_inst.id)
            } else {
                conn_inst.id.clone()
            },
            manifest: eff_def.manifest.clone(),
            runtime_source: eff_def.runtime_source.clone(),
            config_digest: conn_inst.config_digest(),
            resolved_libraries: g.resolved_libraries(),
        };

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

        self.pool
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
                None,
            )
            .await
            .map_err(|e| self.store.redactor().redact_str(&e.to_string()))
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
        let query_key =
            self.compute_query_key(instance_id, &def.manifest.identity_version, params, principal);
        let schema_str = def
            .manifest
            .output
            .as_ref()
            .and_then(|s| s.semantic_type_name().map(|n| n.to_string()));

        let (version, old_val) = {
            let mut cache = self.cache.lock().unwrap();
            let prev = cache.get(&query_key).cloned();
            let next_ver = prev.as_ref().map(|p| p.version + 1).unwrap_or(1);
            let old_v = prev.as_ref().map(|p| p.value.clone());
            cache.put(
                query_key.clone(),
                CachedQueryEntry {
                    instance_id: instance_id.to_string(),
                    definition_digest: def.manifest.identity_version.clone(),
                    auth_fingerprint: principal.authorization_fingerprint(),
                    params: params.clone(),
                    principal: principal.clone(),
                    generation_id: g.generation_id,
                    version: next_ver,
                    status: QueryStatus::Fresh,
                    value: new_value.clone(),
                    schema: schema_str.clone(),
                    updated_at: Utc::now(),
                    stale: false,
                    last_error: None,
                    hits: prev.as_ref().map(|p| p.hits).unwrap_or(0),
                    misses: prev.as_ref().map(|p| p.misses).unwrap_or(0),
                },
            );
            (next_ver, old_v)
        };

        let snap = QueryEnvelope::snapshot(
            instance_id,
            &query_key,
            g.generation_id,
            version,
            QueryStatus::Fresh,
            schema_str,
            new_value,
            None,
        );
        self.signals
            .publish_snapshot_with_delta(old_val.as_ref().map(|v| (version - 1, v)), snap.clone(), "id");
        Ok(snap)
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
                return Ok(ProviderDeltaOutcome::Duplicate {
                    last_sequence: last,
                })
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
                    entry.version += 1;
                    entry.value = serde_json::Value::Array(rows);
                    entry.updated_at = Utc::now();
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
            self.signals
                .publish_delta_with_snapshot(delta.clone(), materialized);
            return Ok(ProviderDeltaOutcome::Applied(delta));
        }

        // Resynchronize from a snapshot; the next delta establishes the new sequence baseline.
        let reason = resync_reason
            .unwrap_or_else(|| "no cached snapshot to apply the delta onto".to_string());
        seqs.reset(&query_key, None);
        let snapshot = self
            .query(instance_id, params.clone(), principal, true)
            .await?;
        Ok(ProviderDeltaOutcome::Resynced { reason, snapshot })
    }

    /// Release idle query state (§6.1, §20 "no unbounded queues or caches"): expire cached
    /// entries nobody subscribes to once they are older than the idle expiry (20 x cache TTL),
    /// release Signal channels past their idle grace period, and prune dependency edges and
    /// circuit breakers that no longer refer to a live query or Instance. Returns the number of
    /// expired cache entries. Called periodically by the server's maintenance loop.
    pub fn prune_idle(&self) -> usize {
        self.signals.sweep_idle();
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

        let mut to_refresh: Vec<(String, serde_json::Value, Principal)> = Vec::new();
        {
            let mut cache = self.cache.lock().unwrap();
            let keys: Vec<String> = cache.iter().map(|(k, _)| k.clone()).collect();
            for k in keys {
                if let Some(entry) = cache.get_mut(&k) {
                    if affected.contains(&entry.instance_id) {
                        entry.stale = true;
                        entry.status = QueryStatus::Stale;
                        to_refresh.push((
                            entry.instance_id.clone(),
                            entry.params.clone(),
                            entry.principal.clone(),
                        ));
                    }
                }
            }
        }

        let refreshed = to_refresh.len();
        for (inst_id, params, principal) in to_refresh {
            let _ = self.query(&inst_id, params, &principal, true).await;
        }
        refreshed
    }

    /// Mark downstream queries stale during Generation carry-over without dropping unaffected caches (§12.2).
    pub fn mark_generation_stale(&self, affected_instances: &BTreeSet<String>, new_gen_id: i64) {
        let mut cache = self.cache.lock().unwrap();
        let keys: Vec<String> = cache.iter().map(|(k, _)| k.clone()).collect();
        for k in keys {
            if let Some(entry) = cache.get_mut(&k) {
                if affected_instances.contains(&entry.instance_id) {
                    entry.stale = true;
                    entry.status = QueryStatus::Stale;
                } else {
                    // Unaffected instances carry over cleanly into the new Generation!
                    entry.generation_id = new_gen_id;
                }
            }
        }
    }

    pub fn execution_graph_snapshot(&self) -> ExecutionGraphSnapshot {
        let g = self.graph.load_full();
        let cache = self.cache.lock().unwrap();
        let mut nodes = Vec::new();
        for (qk, entry) in cache.iter() {
            nodes.push(ExecutionGraphNode {
                query_key: qk.clone(),
                instance_id: entry.instance_id.clone(),
                status: match entry.status {
                    QueryStatus::Fresh => "fresh".into(),
                    QueryStatus::Stale => "stale".into(),
                    QueryStatus::Loading => "loading".into(),
                    QueryStatus::Error => "error".into(),
                },
                version: entry.version,
                subscribers: self.signals.total_subscribers(),
                cache_hits: entry.hits,
                cache_misses: entry.misses,
            });
        }
        drop(cache);

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

    fn record_dynamic_edge(&self, parent_qk: &str, child_qk: &str) {
        self.forward_deps
            .entry(parent_qk.to_string())
            .or_default()
            .insert(child_qk.to_string());
        self.reverse_deps
            .entry(child_qk.to_string())
            .or_default()
            .insert(parent_qk.to_string());
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
        }
        for added in new_deps.difference(&old_deps) {
            self.reverse_deps
                .entry(added.clone())
                .or_default()
                .insert(parent_qk.to_string());
        }
    }

    fn record_failure(&self, instance_id: &str) {
        let mut entry = self
            .circuit_breakers
            .entry(instance_id.to_string())
            .or_insert(CircuitState {
                consecutive_failures: 0,
                open_until: None,
            });
        entry.consecutive_failures += 1;
        if entry.consecutive_failures >= 5 {
            let backoff_secs = 2_u64.pow((entry.consecutive_failures - 5).min(4));
            entry.open_until = Some(Instant::now() + Duration::from_secs(backoff_secs));
        }
    }

    /// Render a single Dashboard by evaluating all its placed Adapter instances and merging overrides.
    pub async fn render_dashboard(
        &self,
        dashboard_id: &str,
        principal: &Principal,
    ) -> Result<serde_json::Value, String> {
        let g = self.graph.load_full();
        let dash = g
            .dashboards
            .get(dashboard_id)
            .ok_or_else(|| format!("dashboard '{dashboard_id}' not found"))?
            .clone();

        self.state
            .seed_dashboard_defaults(&dash, g.generation_id)
            .map_err(|e| e.to_string())?;

        let mut adapter_trees = Vec::new();
        for placement in &dash.adapters {
            let inst = g
                .instances
                .get(&placement.adapter_instance)
                .ok_or_else(|| {
                    format!("adapter instance '{}' not found", placement.adapter_instance)
                })?
                .clone();
            let def = g
                .definitions
                .get(&inst.definition)
                .ok_or_else(|| format!("adapter definition '{}' missing", inst.definition))?
                .clone();

            check_provider_access(principal, &inst).map_err(|e| e.to_string())?;

            let trace_id = format!("tr_{}", Uuid::now_v7().simple());
            let mut payload = placement.query_params.clone();
            if let Some(obj) = payload.as_object_mut() {
                obj.insert(
                    "dashboard_id".into(),
                    serde_json::Value::String(dashboard_id.to_string()),
                );
            }

            let bridge = Arc::new(EngineHostBridge {
                engine: self.clone(),
                graph: (*g).clone(),
                caller_inst: inst.clone(),
                caller_def: def.clone(),
                principal: principal.clone(),
                trace_id: trace_id.clone(),
                parent_query_key: None,
                call_stack: Vec::new(),
                recorded_deps: Arc::new(Mutex::new(BTreeSet::new())),
                operation: None,
            });
            let bundle = CompiledInstanceBundle {
                instance_id: inst.id.clone(),
                manifest: def.manifest.clone(),
                runtime_source: def.runtime_source.clone(),
                config_digest: inst.config_digest(),
                resolved_libraries: g.resolved_libraries(),
            };
            let handler = if def.manifest.handler.is_empty() {
                "render"
            } else {
                def.manifest.handler.as_str()
            };

            let raw_tree = self
                .pool
                .execute_handler(
                    &bundle,
                    handler,
                    ExecutionContextConfig {
                        category: DefinitionCategory::Adapter,
                        instance_id: inst.id.clone(),
                        input_names: inst.inputs.keys().cloned().collect(),
                        config: inst.config.clone(),
                        payload,
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

            // Derive stable `widget_id`s and apply Dashboard layout overrides (§8)
            let tree = annotate_widget_ids(
                raw_tree,
                &inst.id,
                &dash.overrides,
                &mut HashSet::new(),
            );
            adapter_trees.push(serde_json::json!({
                "adapter_instance": inst.id,
                "area": placement.area,
                "tree": tree,
            }));
        }

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
}

fn annotate_widget_ids(
    mut node: serde_json::Value,
    adapter_instance: &str,
    overrides: &BTreeMap<String, crate::model::LayoutOverride>,
    seen: &mut HashSet<String>,
) -> serde_json::Value {
    if let Some(obj) = node.as_object_mut() {
        let node_key = obj
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or("root")
            .to_string();
        let widget_id = obj
            .get("widget_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{adapter_instance}_{node_key}"));
        seen.insert(widget_id.clone());
        obj.insert("widget_id".into(), serde_json::Value::String(widget_id.clone()));

        if let Some(ov) = overrides.get(&widget_id) {
            obj.insert(
                "layout_override".into(),
                serde_json::to_value(ov).unwrap_or(serde_json::Value::Null),
            );
        }

        if let Some(children) = obj.get_mut("children").and_then(|v| v.as_array_mut()) {
            let updated: Vec<serde_json::Value> = children
                .drain(..)
                .map(|c| annotate_widget_ids(c, adapter_instance, overrides, seen))
                .collect();
            *children = updated;
        }
    }
    node
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

pub(crate) struct EngineHostBridge {
    pub engine: QueryEngine,
    pub graph: ConfiguredGraph,
    pub caller_inst: InstanceConfig,
    pub caller_def: LoadedDefinition,
    pub principal: Principal,
    pub trace_id: String,
    pub parent_query_key: Option<String>,
    pub call_stack: Vec<String>,
    pub recorded_deps: Arc<Mutex<BTreeSet<String>>>,
    pub operation: Option<String>,
}

#[async_trait]
impl HostBridge for EngineHostBridge {
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
            .engine
            .query_with_trace(
                target_id,
                params.clone(),
                &self.principal,
                false,
                self.call_stack.clone(),
                self.parent_query_key.clone(),
            )
            .await?;

        if let Some(target_inst) = self.graph.instances.get(target_id) {
            if let Some(target_def) = self.graph.definitions.get(&target_inst.definition) {
                let child_qk = self.engine.compute_query_key(
                    target_id,
                    &target_def.manifest.identity_version,
                    &params,
                    &self.principal,
                );
                self.recorded_deps.lock().unwrap().insert(child_qk);
            }
        }

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
        let target_inst = self
            .graph
            .instances
            .get(target_id)
            .ok_or_else(|| format!("target instance '{target_id}' not found"))?;
        if target_inst.category != DefinitionCategory::Connector {
            return Err(format!("call_input expects connector, got '{}'", target_inst.category.as_str()));
        }
        self.engine
            .invoke_connector(target_id, operation, args, &self.principal, &self.trace_id)
            .await
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
        self.engine
            .http
            .execute(
                &self.principal,
                &self.caller_inst,
                self.operation.as_deref(),
                &self.graph,
                req,
            )
            .await
            .map_err(|e| e.to_string())
    }

    async fn emit_event(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        durable: bool,
    ) -> Result<(), String> {
        self.engine
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

    async fn state_get(&self, scope: &str, key: &str) -> Result<serde_json::Value, String> {
        let owner = match scope {
            "dashboard" => {
                // Dashboard ID from query/payload or "engineering" fallback
                key.split_once(':')
                    .map(|(d, _)| d.to_string())
                    .unwrap_or_else(|| {
                        self.graph
                            .dashboards
                            .keys()
                            .next()
                            .cloned()
                            .unwrap_or_else(|| "default".into())
                    })
            }
            _ => self.caller_inst.id.clone(),
        };
        let actual_key = if scope == "dashboard" {
            key.split_once(':').map(|(_, k)| k).unwrap_or(key)
        } else {
            key
        };
        let rec = self
            .engine
            .state
            .get(scope, &owner, actual_key)
            .map_err(|e| e.to_string())?;
        Ok(rec.map(|r| r.value).unwrap_or(serde_json::Value::Null))
    }

    async fn state_set(
        &self,
        scope: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), String> {
        if scope == "dashboard" && self.caller_def.manifest.category == DefinitionCategory::Adapter {
            return Err("adapters hold no writable state (ADR-0001)".into());
        }
        let owner = self.caller_inst.id.clone();
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
}
