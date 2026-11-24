//! Signals, QueryResult envelopes (`dash.query/1`), keyed delta operations, and coalescing SignalHub.

use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryStatus {
    Fresh,
    Stale,
    Loading,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeKind {
    Snapshot,
    Delta,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DeltaOp {
    Insert {
        key: String,
        value: serde_json::Value,
    },
    Update {
        key: String,
        value: serde_json::Value,
    },
    Delete {
        key: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEnvelope {
    pub protocol: String, // "dash.query/1"
    pub instance: String,
    pub query_key: String,
    pub generation: i64,
    pub version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_version: Option<u64>,
    pub status: QueryStatus,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub kind: EnvelopeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operations: Option<Vec<DeltaOp>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Retry/backoff metadata attached to `stale` envelopes of failing queries (§6.1, §15.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryInfo>,
}

/// Retry/backoff metadata for a failing query: the last failed attempt, when the engine retries
/// next (scheduled only while the query has active subscribers) and whether the Instance's
/// circuit breaker is open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryInfo {
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<String>,
    #[serde(default)]
    pub circuit_open: bool,
}

impl QueryEnvelope {
    pub fn snapshot(
        instance: &str,
        query_key: &str,
        generation: i64,
        version: u64,
        status: QueryStatus,
        schema: Option<String>,
        value: serde_json::Value,
        error: Option<String>,
    ) -> Self {
        Self {
            protocol: "dash.query/1".into(),
            instance: instance.to_string(),
            query_key: query_key.to_string(),
            generation,
            version,
            base_version: None,
            status,
            updated_at: Utc::now().to_rfc3339(),
            schema,
            kind: EnvelopeKind::Snapshot,
            value: Some(value),
            operations: None,
            error,
            retry: None,
        }
    }

    pub fn delta(
        instance: &str,
        query_key: &str,
        generation: i64,
        base_version: u64,
        version: u64,
        schema: Option<String>,
        operations: Vec<DeltaOp>,
    ) -> Self {
        Self {
            protocol: "dash.query/1".into(),
            instance: instance.to_string(),
            query_key: query_key.to_string(),
            generation,
            version,
            base_version: Some(base_version),
            status: QueryStatus::Fresh,
            updated_at: Utc::now().to_rfc3339(),
            schema,
            kind: EnvelopeKind::Delta,
            value: None,
            operations: Some(operations),
            error: None,
            retry: None,
        }
    }
}

/// Extract stable row key from a JSON object row.
pub fn extract_row_key(row: &serde_json::Value, key_field: &str) -> Option<String> {
    if let Some(k) = row.get(key_field) {
        return Some(match k {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            other => other.to_string(),
        });
    }
    if let Some(id) = row.get("id").or_else(|| row.get("key")).or_else(|| row.get("number")) {
        return Some(match id {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            other => other.to_string(),
        });
    }
    None
}

/// Compute keyed delta operations between two JSON array snapshots.
pub fn compute_keyed_delta(
    old_rows: &[serde_json::Value],
    new_rows: &[serde_json::Value],
    key_field: &str,
) -> Option<Vec<DeltaOp>> {
    let mut old_map = BTreeMap::new();
    for row in old_rows {
        let key = extract_row_key(row, key_field)?;
        old_map.insert(key, row);
    }
    let mut new_map = BTreeMap::new();
    for row in new_rows {
        let key = extract_row_key(row, key_field)?;
        new_map.insert(key, row);
    }

    let mut ops = Vec::new();
    for (k, v) in &new_map {
        match old_map.get(k) {
            None => ops.push(DeltaOp::Insert {
                key: k.clone(),
                value: (*v).clone(),
            }),
            Some(old_v) if *old_v != *v => ops.push(DeltaOp::Update {
                key: k.clone(),
                value: (*v).clone(),
            }),
            _ => {}
        }
    }
    for k in old_map.keys() {
        if !new_map.contains_key(k) {
            ops.push(DeltaOp::Delete { key: k.clone() });
        }
    }
    Some(ops)
}

/// Apply keyed delta operations onto a client/subscriber local snapshot, checking `base_version`.
pub fn apply_delta_envelope(
    local_version: &mut u64,
    local_rows: &mut Vec<serde_json::Value>,
    envelope: &QueryEnvelope,
    key_field: &str,
) -> Result<(), String> {
    match envelope.kind {
        EnvelopeKind::Snapshot => {
            let val = envelope
                .value
                .clone()
                .ok_or_else(|| "snapshot envelope missing value".to_string())?;
            *local_rows = val.as_array().cloned().unwrap_or_else(|| vec![val]);
            *local_version = envelope.version;
            Ok(())
        }
        EnvelopeKind::Delta => {
            let base = envelope
                .base_version
                .ok_or_else(|| "delta envelope missing base_version".to_string())?;
            if base != *local_version {
                return Err(format!(
                    "sequence gap: subscriber at version {}, delta requires base_version {}; snapshot resync required",
                    *local_version, base
                ));
            }
            let ops = envelope.operations.as_deref().unwrap_or(&[]);
            for op in ops {
                match op {
                    DeltaOp::Insert { key, value } => {
                        local_rows.retain(|r| extract_row_key(r, key_field).as_deref() != Some(key));
                        local_rows.push(value.clone());
                    }
                    DeltaOp::Update { key, value } => {
                        if let Some(slot) = local_rows
                            .iter_mut()
                            .find(|r| extract_row_key(r, key_field).as_deref() == Some(key))
                        {
                            *slot = value.clone();
                        } else {
                            local_rows.push(value.clone());
                        }
                    }
                    DeltaOp::Delete { key } => {
                        local_rows.retain(|r| extract_row_key(r, key_field).as_deref() != Some(key));
                    }
                }
            }
            *local_version = envelope.version;
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSignalPayload {
    pub scope: String,
    pub owner: String,
    pub state_key: String,
    pub version: i64,
    pub value: serde_json::Value,
    pub updated_at: String,
}

/// Manages reactive Signal subscriptions for Queries (coalescing watch + bounded delta stream)
/// and State updates.
#[derive(Clone)]
pub struct SignalHub {
    /// Latest-value watch channels coalesce intermediate updates automatically under slow consumers.
    query_watches: Arc<DashMap<String, watch::Sender<Option<QueryEnvelope>>>>,
    /// Bounded broadcast channels deliver keyed deltas without unbounded memory growth.
    query_streams: Arc<DashMap<String, broadcast::Sender<QueryEnvelope>>>,
    /// Dashboard / Instance state change broadcast.
    state_tx: broadcast::Sender<StateSignalPayload>,
    /// Bounded queue capacity for delta streams.
    channel_capacity: usize,
}

impl SignalHub {
    pub fn new(channel_capacity: usize) -> Self {
        let (state_tx, _) = broadcast::channel(channel_capacity.max(64));
        Self {
            query_watches: Arc::new(DashMap::new()),
            query_streams: Arc::new(DashMap::new()),
            state_tx,
            channel_capacity: channel_capacity.max(32),
        }
    }

    pub fn publish_query(&self, envelope: QueryEnvelope) {
        let key = envelope.query_key.clone();
        // The watch entry stays locked for the whole publish, so concurrent publishers of one
        // query_key are serialized. Envelopes older than the latest published snapshot are
        // dropped so subscribers never see versions go backwards (Appendix A.2).
        let watch_entry = self
            .query_watches
            .entry(key.clone())
            .or_insert_with(|| watch::channel(None).0);
        let latest = watch_entry.borrow().as_ref().map(|prev| prev.version);
        if let Some(latest) = latest {
            let out_of_order = match envelope.kind {
                EnvelopeKind::Snapshot => envelope.version < latest,
                EnvelopeKind::Delta => envelope.version <= latest,
            };
            if out_of_order {
                return;
            }
        }
        // Watch always stores latest snapshot/envelope for fast coalescing consumers
        if envelope.kind == EnvelopeKind::Snapshot {
            watch_entry.send_replace(Some(envelope.clone()));
        }
        let stream = self
            .query_streams
            .entry(key)
            .or_insert_with(|| broadcast::channel(self.channel_capacity).0);
        let _ = stream.send(envelope);
        drop(stream);
        drop(watch_entry);
    }

    /// Number of live subscribers (stream and watch receivers) for one `query_key`.
    pub fn subscriber_count(&self, query_key: &str) -> usize {
        let streams = self
            .query_streams
            .get(query_key)
            .map(|s| s.receiver_count())
            .unwrap_or(0);
        let watches = self
            .query_watches
            .get(query_key)
            .map(|w| w.receiver_count())
            .unwrap_or(0);
        streams + watches
    }

    /// Subscribe to coalesced latest-value Signals (never drops the latest value under slow consumers).
    pub fn subscribe_watch(&self, query_key: &str) -> watch::Receiver<Option<QueryEnvelope>> {
        let entry = self
            .query_watches
            .entry(query_key.to_string())
            .or_insert_with(|| watch::channel(None).0);
        entry.subscribe()
    }

    /// Subscribe to ordered delta/snapshot stream (bounded; lagged receivers see `Lagged` and resync).
    pub fn subscribe_stream(&self, query_key: &str) -> broadcast::Receiver<QueryEnvelope> {
        let entry = self
            .query_streams
            .entry(query_key.to_string())
            .or_insert_with(|| broadcast::channel(self.channel_capacity).0);
        entry.subscribe()
    }

    pub fn publish_state(&self, payload: StateSignalPayload) {
        let _ = self.state_tx.send(payload);
    }

    pub fn subscribe_state(&self) -> broadcast::Receiver<StateSignalPayload> {
        self.state_tx.subscribe()
    }

    pub fn total_subscribers(&self) -> usize {
        self.query_streams
            .iter()
            .map(|e| e.value().receiver_count())
            .sum::<usize>()
            + self.state_tx.receiver_count()
    }
}
