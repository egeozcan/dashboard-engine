//! Bounded sanitized logs, trace spans, metrics, execution graph snapshot, and test failure injection.

use chrono::Utc;
use dash_store::{Redactor, SqliteStore, TraceSpanRecord};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uuid::Uuid;

const MAX_RECENT_LOGS: usize = 500;
const MAX_RECENT_SPANS: usize = 500;
/// Bounded queue between request paths and the dedicated trace-span SQLite writer thread
/// (§3.2: no blocking SQLite writes on Tokio event-loop threads; §20: no unbounded queues).
const SPAN_WRITE_QUEUE: usize = 4096;
/// Bounded window of provider refresh latencies used for p50/p95 metrics.
const MAX_REFRESH_SAMPLES: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SanitizedLogEntry {
    pub timestamp: String,
    pub level: String,
    pub target: String,
    pub principal_id: String,
    pub trace_id: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionGraphNode {
    pub query_key: String,
    pub instance_id: String,
    pub status: String,
    pub version: u64,
    pub subscribers: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    /// `query` for data-provider results, `adapter_render` for cached Adapter evaluations.
    #[serde(default = "default_node_kind")]
    pub kind: String,
    /// Declared polling interval of the node's provider, if it is polled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval_secs: Option<u64>,
}

fn default_node_kind() -> String {
    "query".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionGraphEdge {
    pub from_query_key: String,
    pub to_query_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionGraphSnapshot {
    pub generation_id: i64,
    pub nodes: Vec<ExecutionGraphNode>,
    pub edges: Vec<ExecutionGraphEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeMetricsSnapshot {
    pub active_vms: usize,
    pub reclaimed_vms: u64,
    pub cache_entries: usize,
    pub query_cache_hits: u64,
    pub query_cache_misses: u64,
    pub active_subscribers: usize,
    pub pending_events: usize,
    pub action_executions_total: u64,
    /// Total SQLite footprint (main db + WAL + SHM).
    pub sqlite_bytes: u64,
    /// WAL file size alone.
    #[serde(default)]
    pub sqlite_wal_bytes: u64,
    /// Durable Events not yet processed across all consumer checkpoints (event lag).
    #[serde(default)]
    pub event_lag: u64,
    /// Provider refresh latency percentiles over the most recent refreshes.
    #[serde(default)]
    pub refresh_latency_p50_ms: Option<i64>,
    #[serde(default)]
    pub refresh_latency_p95_ms: Option<i64>,
    /// Trace spans pending in the bounded write queue (queue depth) and dropped because it was full.
    #[serde(default)]
    pub trace_write_queue_depth: u64,
    #[serde(default)]
    pub trace_spans_dropped: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureInjectionRule {
    pub instance_id: String,
    pub mode: String, // error | timeout | unknown
    pub message: String,
    pub remaining: Option<usize>,
}

/// Request-scoped trace context (§15.1 "Browser/agent request and authenticated Principal").
/// The HTTP/MCP layer opens a root span and runs the request inside [`REQUEST_TRACE`]; spans
/// recorded without a parent while the request runs (provider queries, actions, ...) are linked
/// under that root span so the UI and MCP see one end-to-end trace.
#[derive(Clone, Debug)]
pub struct RequestTraceContext {
    pub trace_id: String,
    pub root_span_id: String,
    principal: Arc<Mutex<Option<(String, String)>>>,
}

impl RequestTraceContext {
    pub fn new(trace_id: String, root_span_id: String) -> Self {
        Self {
            trace_id,
            root_span_id,
            principal: Arc::new(Mutex::new(None)),
        }
    }

    /// `(principal_id, principal_kind)` noted by the authentication layer, if any.
    pub fn principal(&self) -> Option<(String, String)> {
        self.principal.lock().ok().and_then(|g| g.clone())
    }
}

tokio::task_local! {
    pub static REQUEST_TRACE: RequestTraceContext;
}

/// Current request trace context, when called inside a request scope.
pub fn current_request_trace() -> Option<RequestTraceContext> {
    REQUEST_TRACE.try_with(|c| c.clone()).ok()
}

/// Record the authenticated Principal on the current request trace (no-op outside a request).
pub fn note_request_principal(principal_id: &str, principal_kind: &str) {
    let _ = REQUEST_TRACE.try_with(|c| {
        if let Ok(mut g) = c.principal.lock() {
            *g = Some((principal_id.to_string(), principal_kind.to_string()));
        }
    });
}

/// An open span; call [`SpanGuard::finish`] to record it with its measured duration.
/// Child spans link to it through [`SpanGuard::span_id`] (§15.1 stable span IDs).
pub struct SpanGuard {
    telemetry: Telemetry,
    pub trace_id: String,
    pub span_id: String,
    parent_span_id: Option<String>,
    name: String,
    principal_id: String,
    started_at: chrono::DateTime<Utc>,
    started: Instant,
}

impl SpanGuard {
    pub fn finish(self, status: &str, attributes: serde_json::Value) -> String {
        let duration_ms = self.started.elapsed().as_millis() as i64;
        self.telemetry.push_span(TraceSpanRecord {
            span_id: self.span_id.clone(),
            trace_id: self.trace_id,
            parent_span_id: self.parent_span_id,
            name: self.name,
            principal_id: self.principal_id,
            attributes: self.telemetry.redactor.redact_json(&attributes),
            status: status.to_string(),
            started_at: self.started_at.to_rfc3339(),
            duration_ms,
        });
        self.span_id
    }
}

#[derive(Clone)]
pub struct Telemetry {
    store: SqliteStore,
    redactor: Redactor,
    logs: Arc<Mutex<VecDeque<SanitizedLogEntry>>>,
    spans: Arc<Mutex<VecDeque<TraceSpanRecord>>>,
    injected_failures: Arc<DashMap<String, FailureInjectionRule>>,
    span_tx: std::sync::mpsc::SyncSender<TraceSpanRecord>,
    span_queue_depth: Arc<AtomicU64>,
    spans_dropped: Arc<AtomicU64>,
    refresh_latencies: Arc<Mutex<VecDeque<i64>>>,
    pub query_hits: Arc<AtomicU64>,
    pub query_misses: Arc<AtomicU64>,
    pub actions_total: Arc<AtomicU64>,
}

impl Telemetry {
    pub fn new(store: SqliteStore) -> Self {
        let redactor = store.redactor().clone();
        let (span_tx, span_rx) = std::sync::mpsc::sync_channel::<TraceSpanRecord>(SPAN_WRITE_QUEUE);
        let span_queue_depth = Arc::new(AtomicU64::new(0));
        {
            // Dedicated writer thread: exits when every Telemetry clone (sender) is dropped.
            let writer_store = store.clone();
            let depth = Arc::clone(&span_queue_depth);
            let _ = std::thread::Builder::new()
                .name("dash-trace-writer".into())
                .stack_size(256 * 1024)
                .spawn(move || {
                    while let Ok(span) = span_rx.recv() {
                        let _ = writer_store.record_trace_span(&span);
                        depth.fetch_sub(1, Ordering::Relaxed);
                    }
                });
        }
        Self {
            store,
            redactor,
            logs: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_RECENT_LOGS))),
            spans: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_RECENT_SPANS))),
            injected_failures: Arc::new(DashMap::new()),
            span_tx,
            span_queue_depth,
            spans_dropped: Arc::new(AtomicU64::new(0)),
            refresh_latencies: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_REFRESH_SAMPLES))),
            query_hits: Arc::new(AtomicU64::new(0)),
            query_misses: Arc::new(AtomicU64::new(0)),
            actions_total: Arc::new(AtomicU64::new(0)),
        }
    }

    /// New random trace id for a request-scoped trace.
    pub fn new_trace_id() -> String {
        format!("tr_{}", Uuid::now_v7().simple())
    }

    /// Open a span now; it is recorded when finished.
    pub fn start_span(
        &self,
        trace_id: &str,
        parent_span_id: Option<&str>,
        name: &str,
        principal_id: &str,
    ) -> SpanGuard {
        SpanGuard {
            telemetry: self.clone(),
            trace_id: trace_id.to_string(),
            span_id: format!("spn_{}", Uuid::now_v7().simple()),
            parent_span_id: parent_span_id.map(|s| s.to_string()),
            name: name.to_string(),
            principal_id: principal_id.to_string(),
            started_at: Utc::now(),
            started: Instant::now(),
        }
    }

    fn push_span(&self, mut span: TraceSpanRecord) {
        if span.parent_span_id.is_none() {
            if let Some(ctx) = current_request_trace() {
                if ctx.root_span_id != span.span_id {
                    // Link root-level work under the request span, keeping the original id.
                    if ctx.trace_id != span.trace_id {
                        if let Some(obj) = span.attributes.as_object_mut() {
                            obj.insert(
                                "origin_trace_id".into(),
                                serde_json::Value::String(span.trace_id.clone()),
                            );
                        }
                        span.trace_id = ctx.trace_id.clone();
                    }
                    span.parent_span_id = Some(ctx.root_span_id.clone());
                }
            }
        }
        if span.name.starts_with("query:") {
            let mut lat = self
                .refresh_latencies
                .lock()
                .expect("latency mutex poisoned");
            if lat.len() >= MAX_REFRESH_SAMPLES {
                lat.pop_front();
            }
            lat.push_back(span.duration_ms.max(0));
        }
        {
            let mut guard = self.spans.lock().expect("spans mutex poisoned");
            if guard.len() >= MAX_RECENT_SPANS {
                guard.pop_front();
            }
            guard.push_back(span.clone());
        }
        self.span_queue_depth.fetch_add(1, Ordering::Relaxed);
        if self.span_tx.try_send(span).is_err() {
            // Queue full (or writer gone): keep the in-memory copy, count the dropped durable write.
            self.span_queue_depth.fetch_sub(1, Ordering::Relaxed);
            self.spans_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `(p50, p95)` provider refresh latency in milliseconds over the recent window.
    pub fn refresh_latency_percentiles(&self) -> (Option<i64>, Option<i64>) {
        let lat = self
            .refresh_latencies
            .lock()
            .expect("latency mutex poisoned");
        if lat.is_empty() {
            return (None, None);
        }
        let mut v: Vec<i64> = lat.iter().copied().collect();
        v.sort_unstable();
        let pick = |q: f64| v[((v.len() as f64 - 1.0) * q).round() as usize];
        (Some(pick(0.50)), Some(pick(0.95)))
    }

    pub fn trace_write_queue_depth(&self) -> u64 {
        self.span_queue_depth.load(Ordering::Relaxed)
    }

    pub fn trace_spans_dropped(&self) -> u64 {
        self.spans_dropped.load(Ordering::Relaxed)
    }

    /// Wait (bounded) until queued trace spans have been written to SQLite. Used on shutdown and in tests.
    pub fn flush_spans(&self, timeout: std::time::Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while self.trace_write_queue_depth() > 0 {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        true
    }

    pub fn log(
        &self,
        level: &str,
        target: &str,
        principal_id: &str,
        trace_id: &str,
        message: &str,
    ) {
        let entry = SanitizedLogEntry {
            timestamp: Utc::now().to_rfc3339(),
            level: level.to_string(),
            target: target.to_string(),
            principal_id: principal_id.to_string(),
            trace_id: trace_id.to_string(),
            message: self.redactor.redact_str(message),
        };
        let mut guard = self.logs.lock().expect("logs mutex poisoned");
        if guard.len() >= MAX_RECENT_LOGS {
            guard.pop_front();
        }
        guard.push_back(entry);
    }

    pub fn recent_logs(&self, limit: usize) -> Vec<SanitizedLogEntry> {
        let guard = self.logs.lock().expect("logs mutex poisoned");
        guard.iter().rev().take(limit).cloned().collect()
    }

    pub fn record_span(
        &self,
        trace_id: &str,
        parent_span_id: Option<&str>,
        name: &str,
        principal_id: &str,
        attributes: serde_json::Value,
        status: &str,
        duration_ms: i64,
    ) -> String {
        let span_id = format!("spn_{}", Uuid::now_v7().simple());
        let span = TraceSpanRecord {
            span_id: span_id.clone(),
            trace_id: trace_id.to_string(),
            parent_span_id: parent_span_id.map(|s| s.to_string()),
            name: name.to_string(),
            principal_id: principal_id.to_string(),
            attributes: self.redactor.redact_json(&attributes),
            status: status.to_string(),
            started_at: Utc::now().to_rfc3339(),
            duration_ms,
        };
        self.push_span(span);
        span_id
    }

    /// Like [`Telemetry::record_span`], but with a caller-chosen `span_id` so nested work (e.g. child
    /// action executions) can name this span as its parent before it is recorded (§5.1.5, §15.1).
    #[allow(clippy::too_many_arguments)]
    pub fn record_span_with_id(
        &self,
        span_id: &str,
        trace_id: &str,
        parent_span_id: Option<&str>,
        name: &str,
        principal_id: &str,
        attributes: serde_json::Value,
        status: &str,
        duration_ms: i64,
    ) {
        let span = TraceSpanRecord {
            span_id: span_id.to_string(),
            trace_id: trace_id.to_string(),
            parent_span_id: parent_span_id.map(|s| s.to_string()),
            name: name.to_string(),
            principal_id: principal_id.to_string(),
            attributes: self.redactor.redact_json(&attributes),
            status: status.to_string(),
            started_at: Utc::now().to_rfc3339(),
            duration_ms,
        };
        {
            let mut guard = self.spans.lock().expect("spans mutex poisoned");
            if guard.len() >= MAX_RECENT_SPANS {
                guard.pop_front();
            }
            guard.push_back(span.clone());
        }
        let _ = self.store.record_trace_span(&span);
    }

    pub fn recent_spans(&self, limit: usize) -> Vec<TraceSpanRecord> {
        let guard = self.spans.lock().expect("spans mutex poisoned");
        if !guard.is_empty() {
            return guard.iter().rev().take(limit).cloned().collect();
        }
        self.store.list_trace_spans(limit).unwrap_or_default()
    }

    pub fn inject_failure(&self, rule: FailureInjectionRule) {
        self.injected_failures
            .insert(rule.instance_id.clone(), rule);
    }

    pub fn clear_failure(&self, instance_id: &str) {
        self.injected_failures.remove(instance_id);
    }

    pub fn take_injected_failure(&self, instance_id: &str) -> Option<FailureInjectionRule> {
        let mut remove = false;
        let out = if let Some(mut entry) = self.injected_failures.get_mut(instance_id) {
            let cloned = entry.value().clone();
            if let Some(ref mut rem) = entry.value_mut().remaining {
                if *rem <= 1 {
                    remove = true;
                } else {
                    *rem -= 1;
                }
            }
            Some(cloned)
        } else {
            None
        };
        if remove {
            self.injected_failures.remove(instance_id);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn spans_are_written_off_thread_with_parent_links_and_latency_metrics() {
        let store = SqliteStore::open_in_memory().unwrap();
        let t = Telemetry::new(store.clone());
        let trace = Telemetry::new_trace_id();
        let root = t.start_span(&trace, None, "http:POST /mcp", "agt_1");
        let root_id = root.span_id.clone();
        t.record_span(
            &trace,
            Some(&root_id),
            "query:open_prs",
            "agt_1",
            json!({}),
            "ok",
            12,
        );
        t.record_span(
            &trace,
            Some(&root_id),
            "query:open_prs",
            "agt_1",
            json!({}),
            "ok",
            40,
        );
        root.finish("ok", json!({"principal_kind": "agent"}));
        assert!(t.flush_spans(std::time::Duration::from_secs(5)));
        let stored = store.list_trace_spans(10).unwrap();
        assert_eq!(stored.len(), 3);
        assert_eq!(
            stored
                .iter()
                .filter(|s| s.parent_span_id.as_deref() == Some(root_id.as_str()))
                .count(),
            2
        );
        let (p50, p95) = t.refresh_latency_percentiles();
        assert!(p50.is_some() && p95.unwrap() >= p50.unwrap());
        assert_eq!(t.trace_spans_dropped(), 0);
    }

    #[tokio::test]
    async fn request_scope_links_parentless_spans_under_request_root() {
        let store = SqliteStore::open_in_memory().unwrap();
        let t = Telemetry::new(store.clone());
        let trace = Telemetry::new_trace_id();
        let root = t.start_span(&trace, None, "http:POST /api/queries/open_prs", "pending");
        let ctx = RequestTraceContext::new(trace.clone(), root.span_id.clone());
        let t2 = t.clone();
        REQUEST_TRACE
            .scope(ctx.clone(), async move {
                note_request_principal("agt_9", "agent");
                t2.record_span(
                    "tr_engine_local",
                    None,
                    "query:open_prs",
                    "agt_9",
                    json!({}),
                    "ok",
                    3,
                );
            })
            .await;
        assert_eq!(ctx.principal(), Some(("agt_9".into(), "agent".into())));
        let root_id = root.finish("ok", json!({}));
        let spans = t.recent_spans(10);
        let child = spans.iter().find(|s| s.name == "query:open_prs").unwrap();
        assert_eq!(child.parent_span_id.as_deref(), Some(root_id.as_str()));
        assert_eq!(child.trace_id, trace);
        assert_eq!(child.attributes["origin_trace_id"], "tr_engine_local");
        // Outside a request scope nothing is rewritten.
        t.record_span("tr_bg", None, "query:bg", "svc", json!({}), "ok", 1);
        let bg = t.recent_spans(1).remove(0);
        assert_eq!(bg.trace_id, "tr_bg");
        assert!(bg.parent_span_id.is_none());
    }
}
