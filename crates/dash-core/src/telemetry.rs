//! Bounded sanitized logs, trace spans, metrics, execution graph snapshot, and test failure injection.

use chrono::Utc;
use dash_store::{Redactor, SqliteStore, TraceSpanRecord};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const MAX_RECENT_LOGS: usize = 500;
const MAX_RECENT_SPANS: usize = 500;

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
    pub sqlite_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureInjectionRule {
    pub instance_id: String,
    pub mode: String, // error | timeout | unknown
    pub message: String,
    pub remaining: Option<usize>,
}

#[derive(Clone)]
pub struct Telemetry {
    store: SqliteStore,
    redactor: Redactor,
    logs: Arc<Mutex<VecDeque<SanitizedLogEntry>>>,
    spans: Arc<Mutex<VecDeque<TraceSpanRecord>>>,
    injected_failures: Arc<DashMap<String, FailureInjectionRule>>,
    pub query_hits: Arc<AtomicU64>,
    pub query_misses: Arc<AtomicU64>,
    pub actions_total: Arc<AtomicU64>,
}

impl Telemetry {
    pub fn new(store: SqliteStore) -> Self {
        let redactor = store.redactor().clone();
        Self {
            store,
            redactor,
            logs: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_RECENT_LOGS))),
            spans: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_RECENT_SPANS))),
            injected_failures: Arc::new(DashMap::new()),
            query_hits: Arc::new(AtomicU64::new(0)),
            query_misses: Arc::new(AtomicU64::new(0)),
            actions_total: Arc::new(AtomicU64::new(0)),
        }
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
        {
            let mut guard = self.spans.lock().expect("spans mutex poisoned");
            if guard.len() >= MAX_RECENT_SPANS {
                guard.pop_front();
            }
            guard.push_back(span.clone());
        }
        let _ = self.store.record_trace_span(&span);
        span_id
    }

    pub fn recent_spans(&self, limit: usize) -> Vec<TraceSpanRecord> {
        let guard = self.spans.lock().expect("spans mutex poisoned");
        if !guard.is_empty() {
            return guard.iter().rev().take(limit).cloned().collect();
        }
        self.store.list_trace_spans(limit).unwrap_or_default()
    }

    pub fn inject_failure(&self, rule: FailureInjectionRule) {
        self.injected_failures.insert(rule.instance_id.clone(), rule);
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
