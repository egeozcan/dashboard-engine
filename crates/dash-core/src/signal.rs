//! Signals, QueryResult envelopes (`dash.query/1`), keyed delta operations, and coalescing SignalHub.
//!
//! Delivery model (§6.2, §7.1):
//! - Every query channel holds a latest-value `watch` (the materialized snapshot, used for
//!   resynchronization) and a bounded `broadcast` stream of snapshots and keyed deltas.
//! - Consumers are reference-counted by their receivers. A channel without receivers is kept for
//!   an idle grace period (so a quick re-subscribe can resync from the latest value) and then
//!   removed; publishing to a query nobody subscribes to allocates nothing.
//! - The hub verifies delta bases: a delta whose `base_version` does not match the last version
//!   published on that channel is rejected (the snapshot that accompanies every engine-computed
//!   delta then reconciles consumers).
//! - Provider-emitted deltas carry a monotonic source-local sequence number which is checked by a
//!   [`SourceSequenceTracker`]; gaps require resynchronization from a snapshot.

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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
        /// Final position of the inserted row in the resulting snapshot. Engine-computed deltas
        /// always set it so applying a delta reproduces the snapshot's row order exactly; when
        /// absent (e.g. some provider deltas) the row is appended.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        index: Option<usize>,
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
    /// Time of the last *successful* update of the value (not the time this envelope was built).
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
    /// Row field the delta's `key`s were taken from (deltas only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_key: Option<String>,
    /// Provider source-local sequence number for provider-emitted deltas.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_sequence: Option<u64>,
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
    #[allow(clippy::too_many_arguments)]
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
            row_key: None,
            source_sequence: None,
        }
    }

    /// A value-less status Signal (`loading`, or `error` when no last good value exists).
    pub fn status_only(
        instance: &str,
        query_key: &str,
        generation: i64,
        status: QueryStatus,
        schema: Option<String>,
        error: Option<String>,
    ) -> Self {
        let mut env = Self::snapshot(
            instance,
            query_key,
            generation,
            0,
            status,
            schema,
            serde_json::Value::Null,
            error,
        );
        // There is no successful update yet; keep the timestamp as "now" for diagnostics.
        env.updated_at = Utc::now().to_rfc3339();
        env
    }

    pub fn loading(instance: &str, query_key: &str, generation: i64, schema: Option<String>) -> Self {
        Self::status_only(instance, query_key, generation, QueryStatus::Loading, schema, None)
    }

    pub fn error(
        instance: &str,
        query_key: &str,
        generation: i64,
        schema: Option<String>,
        error: String,
    ) -> Self {
        Self::status_only(
            instance,
            query_key,
            generation,
            QueryStatus::Error,
            schema,
            Some(error),
        )
    }

    /// Stamp the last successful update time (cache hits and stale-on-failure envelopes must
    /// report when the value was produced, not when it was read).
    pub fn with_updated_at(mut self, updated_at: DateTime<Utc>) -> Self {
        self.updated_at = updated_at.to_rfc3339();
        self
    }

    pub fn with_row_key(mut self, row_key: impl Into<String>) -> Self {
        self.row_key = Some(row_key.into());
        self
    }

    pub fn with_source_sequence(mut self, seq: u64) -> Self {
        self.source_sequence = Some(seq);
        self
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
            row_key: None,
            source_sequence: None,
        }
    }
}

fn scalar_key(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
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

/// Strict key extraction: only `key_field`, only scalar (string/number) values.
pub fn strict_row_key(row: &serde_json::Value, key_field: &str) -> Option<String> {
    row.get(key_field).and_then(scalar_key)
}

/// Choose one row-key field used consistently for every row of both snapshots.
///
/// Candidates are `preferred` then `id`, `key`, `number`. A candidate qualifies only if every row
/// is an object carrying a scalar value for it and keys are unique within each snapshot.
pub fn infer_row_key_field(
    old_rows: &[serde_json::Value],
    new_rows: &[serde_json::Value],
    preferred: &str,
) -> Option<String> {
    let mut candidates = vec![preferred];
    for c in ["id", "key", "number"] {
        if !candidates.contains(&c) {
            candidates.push(c);
        }
    }
    'next: for field in candidates {
        for rows in [old_rows, new_rows] {
            let mut seen = HashSet::new();
            for row in rows {
                let Some(k) = strict_row_key(row, field) else {
                    continue 'next;
                };
                if !seen.insert(k) {
                    continue 'next;
                }
            }
        }
        return Some(field.to_string());
    }
    None
}

/// Compute order-preserving keyed delta operations keyed strictly by `key_field`.
///
/// Returns `None` (the caller must fall back to a snapshot) when a row lacks the key, keys are
/// duplicated, or the relative order of retained rows changed (moves are not expressible).
/// Operations are emitted as deletes, then in-place updates, then inserts in ascending final
/// `index`, so [`apply_delta_ops`] reproduces `new_rows` exactly.
pub fn compute_keyed_delta(
    old_rows: &[serde_json::Value],
    new_rows: &[serde_json::Value],
    key_field: &str,
) -> Option<Vec<DeltaOp>> {
    let mut old_map = BTreeMap::new();
    let mut old_order = Vec::with_capacity(old_rows.len());
    for row in old_rows {
        let key = strict_row_key(row, key_field)?;
        if old_map.insert(key.clone(), row).is_some() {
            return None;
        }
        old_order.push(key);
    }
    let mut new_map = BTreeMap::new();
    let mut new_order = Vec::with_capacity(new_rows.len());
    for row in new_rows {
        let key = strict_row_key(row, key_field)?;
        if new_map.insert(key.clone(), row).is_some() {
            return None;
        }
        new_order.push(key);
    }

    // Retained rows must keep their relative order.
    let retained_old: Vec<&String> = old_order.iter().filter(|k| new_map.contains_key(*k)).collect();
    let retained_new: Vec<&String> = new_order.iter().filter(|k| old_map.contains_key(*k)).collect();
    if retained_old != retained_new {
        return None;
    }

    let mut ops = Vec::new();
    for k in &old_order {
        if !new_map.contains_key(k) {
            ops.push(DeltaOp::Delete { key: k.clone() });
        }
    }
    for k in &new_order {
        if let Some(old_v) = old_map.get(k) {
            let new_v = new_map[k];
            if *old_v != new_v {
                ops.push(DeltaOp::Update {
                    key: k.clone(),
                    value: new_v.clone(),
                });
            }
        }
    }
    for (idx, k) in new_order.iter().enumerate() {
        if !old_map.contains_key(k) {
            ops.push(DeltaOp::Insert {
                key: k.clone(),
                value: new_map[k].clone(),
                index: Some(idx),
            });
        }
    }
    Some(ops)
}

/// Like [`compute_keyed_delta`] but infers a consistent row-key field (see
/// [`infer_row_key_field`]) and returns it with the operations.
pub fn compute_keyed_delta_inferred(
    old_rows: &[serde_json::Value],
    new_rows: &[serde_json::Value],
    preferred_key: &str,
) -> Option<(String, Vec<DeltaOp>)> {
    let field = infer_row_key_field(old_rows, new_rows, preferred_key)?;
    let ops = compute_keyed_delta(old_rows, new_rows, &field)?;
    Some((field, ops))
}

fn row_key_of(row: &serde_json::Value, key_field: &str, strict: bool) -> Option<String> {
    if strict {
        strict_row_key(row, key_field)
    } else {
        extract_row_key(row, key_field)
    }
}

fn apply_ops(
    local_rows: &mut Vec<serde_json::Value>,
    ops: &[DeltaOp],
    key_field: &str,
    strict: bool,
) {
    for op in ops {
        match op {
            DeltaOp::Insert { key, value, index } => {
                local_rows.retain(|r| row_key_of(r, key_field, strict).as_deref() != Some(key));
                match index {
                    Some(i) => {
                        let at = (*i).min(local_rows.len());
                        local_rows.insert(at, value.clone());
                    }
                    None => local_rows.push(value.clone()),
                }
            }
            DeltaOp::Update { key, value } => {
                if let Some(slot) = local_rows
                    .iter_mut()
                    .find(|r| row_key_of(r, key_field, strict).as_deref() == Some(key))
                {
                    *slot = value.clone();
                } else {
                    local_rows.push(value.clone());
                }
            }
            DeltaOp::Delete { key } => {
                local_rows.retain(|r| row_key_of(r, key_field, strict).as_deref() != Some(key));
            }
        }
    }
}

/// Apply keyed delta operations onto rows keyed strictly by `key_field`.
pub fn apply_delta_ops(local_rows: &mut Vec<serde_json::Value>, ops: &[DeltaOp], key_field: &str) {
    apply_ops(local_rows, ops, key_field, true);
}

/// Apply keyed delta operations onto a client/subscriber local snapshot, checking `base_version`.
///
/// When the envelope names its `row_key`, that field is used (strictly); otherwise `key_field`
/// with the legacy `id`/`key`/`number` fallback.
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
            match envelope.row_key.as_deref() {
                Some(rk) => apply_ops(local_rows, ops, rk, true),
                None => apply_ops(local_rows, ops, key_field, false),
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

/// Result of checking a provider's source-local sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceCheck {
    /// No baseline was known; this sequence number establishes it.
    Baseline,
    /// Exactly `last + 1`.
    InOrder,
    /// Already applied (`seq <= last`); ignore.
    Duplicate { last: u64 },
    /// A sequence number was skipped; resynchronize from a snapshot.
    Gap { expected: u64, got: u64 },
}

/// Tracks monotonic source-local sequence numbers of provider-emitted deltas per query key.
///
/// The baseline is established by a provider snapshot that carries its sequence number, or (when
/// none is known, e.g. right after an engine refetch) by the first delta received.
#[derive(Clone, Default)]
pub struct SourceSequenceTracker {
    last: Arc<DashMap<String, u64>>,
}

impl SourceSequenceTracker {
    pub fn check_and_advance(&self, query_key: &str, seq: u64) -> SequenceCheck {
        let mut entry = match self.last.entry(query_key.to_string()) {
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(seq);
                return SequenceCheck::Baseline;
            }
            dashmap::mapref::entry::Entry::Occupied(o) => o,
        };
        let last = *entry.get();
        if seq <= last {
            SequenceCheck::Duplicate { last }
        } else if seq == last + 1 {
            *entry.get_mut() = seq;
            SequenceCheck::InOrder
        } else {
            SequenceCheck::Gap {
                expected: last + 1,
                got: seq,
            }
        }
    }

    /// Reset after a resync; `baseline` is the sequence number the new snapshot reflects, if known.
    pub fn reset(&self, query_key: &str, baseline: Option<u64>) {
        match baseline {
            Some(b) => {
                self.last.insert(query_key.to_string(), b);
            }
            None => {
                self.last.remove(query_key);
            }
        }
    }

    pub fn last(&self, query_key: &str) -> Option<u64> {
        self.last.get(query_key).map(|v| *v)
    }

    pub fn len(&self) -> usize {
        self.last.len()
    }

    pub fn is_empty(&self) -> bool {
        self.last.is_empty()
    }

    fn retain_keys(&self, keep: impl Fn(&str) -> bool) {
        self.last.retain(|k, _| keep(k));
    }
}

/// Outcome of a provider-emitted delta (see `QueryEngine::apply_provider_delta`).
#[derive(Debug, Clone)]
pub enum ProviderDeltaOutcome {
    /// The delta was verified, applied to the cached value and published.
    Applied(QueryEnvelope),
    /// The delta's sequence number was already applied; nothing changed.
    Duplicate { last_sequence: u64 },
    /// A gap (or missing base) was detected and the query was resynchronized from a snapshot.
    Resynced { reason: String, snapshot: QueryEnvelope },
}

/// What happened to an envelope handed to [`SignalHub::publish_query`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    /// Nobody subscribes to this query; nothing was retained.
    NoSubscribers,
    Published,
    /// The delta's `base_version` did not match the channel's last published version.
    DeltaRejected { expected_base: u64, got_base: Option<u64> },
    /// The snapshot's version is older than the channel's latest published version.
    Superseded { latest: u64 },
}

struct QueryChannel {
    /// Latest materialized snapshot (coalesced; resync source).
    watch: watch::Sender<Option<QueryEnvelope>>,
    /// Bounded ordered stream of snapshots and deltas.
    stream: broadcast::Sender<QueryEnvelope>,
    /// Last version published on this channel (for delta base verification).
    last_version: Option<u64>,
    /// When the channel last had zero receivers.
    idle_since: Option<Instant>,
}

impl QueryChannel {
    fn new(capacity: usize) -> Self {
        Self {
            watch: watch::channel(None).0,
            stream: broadcast::channel(capacity).0,
            last_version: None,
            idle_since: None,
        }
    }

    fn receivers(&self) -> usize {
        self.stream.receiver_count() + self.watch.receiver_count()
    }
}

/// Default grace period before an unsubscribed query channel is released (§6.1).
pub const DEFAULT_QUERY_IDLE_GRACE: Duration = Duration::from_secs(30);

/// Manages reactive Signal subscriptions for Queries (coalescing watch + bounded delta stream)
/// and State updates.
#[derive(Clone)]
pub struct SignalHub {
    queries: Arc<DashMap<String, QueryChannel>>,
    /// Dashboard / Instance state change broadcast. Delivery to sockets is filtered per Principal
    /// and subscription by the server (Instance State never leaves the host).
    state_tx: broadcast::Sender<StateSignalPayload>,
    /// Bounded queue capacity for delta streams.
    channel_capacity: usize,
    idle_grace: Duration,
    last_sweep: Arc<Mutex<Instant>>,
    source_sequences: SourceSequenceTracker,
}

impl SignalHub {
    pub fn new(channel_capacity: usize) -> Self {
        Self::with_idle_grace(channel_capacity, DEFAULT_QUERY_IDLE_GRACE)
    }

    pub fn with_idle_grace(channel_capacity: usize, idle_grace: Duration) -> Self {
        let (state_tx, _) = broadcast::channel(channel_capacity.max(64));
        Self {
            queries: Arc::new(DashMap::new()),
            state_tx,
            channel_capacity: channel_capacity.max(32),
            idle_grace,
            last_sweep: Arc::new(Mutex::new(Instant::now())),
            source_sequences: SourceSequenceTracker::default(),
        }
    }

    pub fn channel_capacity(&self) -> usize {
        self.channel_capacity
    }

    pub fn source_sequences(&self) -> &SourceSequenceTracker {
        &self.source_sequences
    }

    /// Publish a snapshot or delta to the query's subscribers.
    ///
    /// Snapshots replace the channel's latest value. Deltas are verified against the last
    /// published version and rejected on mismatch.
    pub fn publish_query(&self, envelope: QueryEnvelope) -> PublishOutcome {
        self.maybe_sweep();
        let Some(mut chan) = self.queries.get_mut(&envelope.query_key) else {
            return PublishOutcome::NoSubscribers;
        };
        match envelope.kind {
            EnvelopeKind::Snapshot => {
                // Snapshots older than the latest published version are dropped so subscribers
                // never see versions go backwards (Appendix A.2). The channel entry stays locked
                // for the whole publish, which serializes concurrent publishers of one key.
                // Status-only envelopes (`loading`/`error`, version 0) carry no value and are
                // always delivered.
                if let (Some(last), true) = (chan.last_version, envelope.version > 0) {
                    if envelope.version < last {
                        return PublishOutcome::Superseded { latest: last };
                    }
                }
                if envelope.version > 0 {
                    chan.last_version = Some(envelope.version);
                }
                chan.watch.send_replace(Some(envelope.clone()));
            }
            EnvelopeKind::Delta => {
                if let Some(expected) = chan.last_version {
                    if envelope.base_version != Some(expected) {
                        return PublishOutcome::DeltaRejected {
                            expected_base: expected,
                            got_base: envelope.base_version,
                        };
                    }
                }
                chan.last_version = Some(envelope.version);
            }
        }
        let _ = chan.stream.send(envelope);
        PublishOutcome::Published
    }

    /// Publish a delta together with the snapshot it materializes to: the snapshot becomes the
    /// channel's latest value (resync source) while only the delta is streamed.
    pub fn publish_delta_with_snapshot(
        &self,
        delta: QueryEnvelope,
        materialized: QueryEnvelope,
    ) -> PublishOutcome {
        self.maybe_sweep();
        let Some(mut chan) = self.queries.get_mut(&delta.query_key) else {
            return PublishOutcome::NoSubscribers;
        };
        if let Some(expected) = chan.last_version {
            if delta.base_version != Some(expected) {
                return PublishOutcome::DeltaRejected {
                    expected_base: expected,
                    got_base: delta.base_version,
                };
            }
        }
        chan.last_version = Some(delta.version);
        chan.watch.send_replace(Some(materialized));
        let _ = chan.stream.send(delta);
        PublishOutcome::Published
    }

    /// Publish a new snapshot, preceded by an order-preserving keyed delta from the previous
    /// value when one can be computed (`previous` = (version, value)).
    pub fn publish_snapshot_with_delta(
        &self,
        previous: Option<(u64, &serde_json::Value)>,
        snapshot: QueryEnvelope,
        preferred_row_key: &str,
    ) -> PublishOutcome {
        if !self.queries.contains_key(&snapshot.query_key) {
            return PublishOutcome::NoSubscribers;
        }
        if let (Some((prev_version, prev_value)), Some(new_value)) = (previous, snapshot.value.as_ref())
        {
            if let (Some(old_arr), Some(new_arr)) = (prev_value.as_array(), new_value.as_array()) {
                if let Some((field, ops)) =
                    compute_keyed_delta_inferred(old_arr, new_arr, preferred_row_key)
                {
                    if !ops.is_empty() && snapshot.version > prev_version {
                        let delta = QueryEnvelope::delta(
                            &snapshot.instance,
                            &snapshot.query_key,
                            snapshot.generation,
                            prev_version,
                            snapshot.version,
                            snapshot.schema.clone(),
                            ops,
                        )
                        .with_row_key(field);
                        let _ = self.publish_query(delta);
                    }
                }
            }
        }
        self.publish_query(snapshot)
    }

    /// Latest materialized snapshot of a query, if the channel holds one.
    pub fn latest_snapshot(&self, query_key: &str) -> Option<QueryEnvelope> {
        self.queries
            .get(query_key)
            .and_then(|c| c.watch.borrow().clone())
    }

    fn channel_entry(
        &self,
        query_key: &str,
    ) -> dashmap::mapref::one::RefMut<'_, String, QueryChannel> {
        let mut entry = self
            .queries
            .entry(query_key.to_string())
            .or_insert_with(|| QueryChannel::new(self.channel_capacity));
        entry.idle_since = None;
        entry
    }

    /// Subscribe to coalesced latest-value Signals (never drops the latest value under slow consumers).
    pub fn subscribe_watch(&self, query_key: &str) -> watch::Receiver<Option<QueryEnvelope>> {
        self.maybe_sweep();
        self.channel_entry(query_key).watch.subscribe()
    }

    /// Subscribe to the ordered delta/snapshot stream. The stream is bounded: a receiver that
    /// falls behind gets `RecvError::Lagged` and must resynchronize from [`Self::latest_snapshot`]
    /// (or a fresh query) before applying further deltas.
    pub fn subscribe_stream(&self, query_key: &str) -> broadcast::Receiver<QueryEnvelope> {
        self.maybe_sweep();
        self.channel_entry(query_key).stream.subscribe()
    }

    /// Number of live consumers (stream + watch receivers) of one query.
    pub fn subscriber_count(&self, query_key: &str) -> usize {
        self.queries.get(query_key).map(|c| c.receivers()).unwrap_or(0)
    }

    pub fn is_query_subscribed(&self, query_key: &str) -> bool {
        self.subscriber_count(query_key) > 0
    }

    /// Number of query channels currently retained (subscribed or within their idle grace).
    pub fn query_channel_count(&self) -> usize {
        self.queries.len()
    }

    /// Release query channels that have had no consumers for longer than the idle grace period.
    /// Returns the released query keys.
    pub fn sweep_idle(&self) -> Vec<String> {
        let now = Instant::now();
        let mut removed = Vec::new();
        self.queries.retain(|k, chan| {
            if chan.receivers() > 0 {
                chan.idle_since = None;
                return true;
            }
            match chan.idle_since {
                None => {
                    chan.idle_since = Some(now);
                    if self.idle_grace.is_zero() {
                        removed.push(k.clone());
                        return false;
                    }
                    true
                }
                Some(since) if now.duration_since(since) >= self.idle_grace => {
                    removed.push(k.clone());
                    false
                }
                Some(_) => true,
            }
        });
        if !removed.is_empty() {
            let removed_set: HashSet<&str> = removed.iter().map(String::as_str).collect();
            self.source_sequences
                .retain_keys(|k| !removed_set.contains(k) || self.queries.contains_key(k));
        }
        removed
    }

    fn maybe_sweep(&self) {
        let due = {
            let Ok(mut last) = self.last_sweep.try_lock() else {
                return;
            };
            if last.elapsed() >= Duration::from_secs(1) {
                *last = Instant::now();
                true
            } else {
                false
            }
        };
        if due {
            self.sweep_idle();
        }
    }

    pub fn publish_state(&self, payload: StateSignalPayload) {
        let _ = self.state_tx.send(payload);
    }

    pub fn subscribe_state(&self) -> broadcast::Receiver<StateSignalPayload> {
        self.state_tx.subscribe()
    }

    /// Live subscribers (stream + watch receivers) of one query key (§6.2 active queries).
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

    pub fn total_subscribers(&self) -> usize {
        self.queries
            .iter()
            .map(|e| e.value().receivers())
            .sum::<usize>()
            + self.state_tx.receiver_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snap(qk: &str, version: u64, value: serde_json::Value) -> QueryEnvelope {
        QueryEnvelope::snapshot("inst", qk, 1, version, QueryStatus::Fresh, None, value, None)
    }

    fn roundtrip(old: serde_json::Value, new: serde_json::Value) -> Option<Vec<serde_json::Value>> {
        let old_rows = old.as_array().unwrap().clone();
        let new_rows = new.as_array().unwrap();
        let (field, ops) = compute_keyed_delta_inferred(&old_rows, new_rows, "id")?;
        let mut rows = old_rows;
        apply_delta_ops(&mut rows, &ops, &field);
        Some(rows)
    }

    #[test]
    fn keyed_delta_preserves_snapshot_order() {
        let old = json!([{"id": "a", "v": 1}, {"id": "b", "v": 1}, {"id": "c", "v": 1}]);
        let new = json!([
            {"id": "x", "v": 0},
            {"id": "a", "v": 2},
            {"id": "y", "v": 0},
            {"id": "c", "v": 1},
            {"id": "z", "v": 0}
        ]);
        let rows = roundtrip(old, new.clone()).unwrap();
        assert_eq!(serde_json::Value::Array(rows), new);
    }

    #[test]
    fn keyed_delta_falls_back_to_snapshot_on_duplicates_or_moves() {
        let dup = json!([{"id": "a"}, {"id": "a"}]);
        assert!(compute_keyed_delta(dup.as_array().unwrap(), &[], "id").is_none());
        let old = json!([{"id": "a"}, {"id": "b"}]);
        let moved = json!([{"id": "b"}, {"id": "a"}]);
        assert!(compute_keyed_delta(old.as_array().unwrap(), moved.as_array().unwrap(), "id").is_none());
    }

    #[test]
    fn keyed_delta_uses_one_consistent_key_field() {
        // Rows have no `id`; `number` is used for every row and recorded on the envelope.
        let old = json!([{"number": 1, "s": "open"}, {"number": 2, "s": "open"}]);
        let new = json!([{"number": 1, "s": "merged"}, {"number": 2, "s": "open"}]);
        let (field, ops) =
            compute_keyed_delta_inferred(old.as_array().unwrap(), new.as_array().unwrap(), "id").unwrap();
        assert_eq!(field, "number");
        assert_eq!(ops.len(), 1);
        // Strict key field: a row missing it means no delta.
        let mixed = json!([{"id": "a"}, {"number": 3}]);
        assert!(compute_keyed_delta(mixed.as_array().unwrap(), &[], "id").is_none());
    }

    #[test]
    fn hub_drops_publishes_without_subscribers_and_releases_idle_channels() {
        let hub = SignalHub::with_idle_grace(32, Duration::ZERO);
        assert_eq!(hub.publish_query(snap("q1", 1, json!([]))), PublishOutcome::NoSubscribers);
        assert_eq!(hub.query_channel_count(), 0);

        let rx = hub.subscribe_stream("q1");
        assert_eq!(hub.subscriber_count("q1"), 1);
        assert_eq!(hub.publish_query(snap("q1", 1, json!([]))), PublishOutcome::Published);
        assert!(hub.sweep_idle().is_empty());
        drop(rx);
        assert_eq!(hub.subscriber_count("q1"), 0);
        assert_eq!(hub.sweep_idle(), vec!["q1".to_string()]);
        assert_eq!(hub.query_channel_count(), 0);
    }

    #[test]
    fn hub_keeps_idle_channel_during_grace_period() {
        let hub = SignalHub::with_idle_grace(32, Duration::from_secs(3600));
        drop(hub.subscribe_stream("q1"));
        assert!(hub.sweep_idle().is_empty());
        assert!(hub.sweep_idle().is_empty());
        assert_eq!(hub.query_channel_count(), 1);
    }

    #[test]
    fn hub_verifies_delta_bases_and_keeps_latest_snapshot() {
        let hub = SignalHub::new(32);
        let mut rx = hub.subscribe_stream("q1");
        hub.publish_query(snap("q1", 3, json!([{"id": "a"}])));
        let bad = QueryEnvelope::delta("inst", "q1", 1, 1, 2, None, vec![]);
        assert_eq!(
            hub.publish_query(bad),
            PublishOutcome::DeltaRejected { expected_base: 3, got_base: Some(1) }
        );
        let good = QueryEnvelope::delta("inst", "q1", 1, 3, 4, None, vec![]);
        assert_eq!(hub.publish_query(good), PublishOutcome::Published);
        assert_eq!(rx.try_recv().unwrap().version, 3);
        assert_eq!(rx.try_recv().unwrap().kind, EnvelopeKind::Delta);
        assert!(rx.try_recv().is_err());
        assert_eq!(hub.latest_snapshot("q1").unwrap().version, 3);
    }

    #[test]
    fn publish_snapshot_with_delta_emits_verified_delta_then_snapshot() {
        let hub = SignalHub::new(32);
        let mut rx = hub.subscribe_stream("q1");
        let v1 = json!([{"id": "a", "s": 1}]);
        hub.publish_query(snap("q1", 1, v1.clone()));
        let v2 = json!([{"id": "a", "s": 2}, {"id": "b", "s": 1}]);
        hub.publish_snapshot_with_delta(Some((1, &v1)), snap("q1", 2, v2.clone()), "id");

        let mut ver = 0;
        let mut rows = vec![];
        for _ in 0..3 {
            let env = rx.try_recv().unwrap();
            apply_delta_envelope(&mut ver, &mut rows, &env, "id").unwrap();
        }
        assert_eq!(ver, 2);
        assert_eq!(serde_json::Value::Array(rows), v2);
    }

    #[test]
    fn slow_stream_receiver_lags_but_latest_snapshot_survives() {
        let hub = SignalHub::new(32);
        let mut rx = hub.subscribe_stream("q1");
        for v in 1..=100u64 {
            hub.publish_query(snap("q1", v, json!(v)));
        }
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
        assert_eq!(hub.latest_snapshot("q1").unwrap().version, 100);
    }

    #[test]
    fn source_sequence_tracker_detects_gaps_and_duplicates() {
        let t = SourceSequenceTracker::default();
        assert_eq!(t.check_and_advance("q", 5), SequenceCheck::Baseline);
        assert_eq!(t.check_and_advance("q", 6), SequenceCheck::InOrder);
        assert_eq!(t.check_and_advance("q", 6), SequenceCheck::Duplicate { last: 6 });
        assert_eq!(
            t.check_and_advance("q", 9),
            SequenceCheck::Gap { expected: 7, got: 9 }
        );
        t.reset("q", Some(9));
        assert_eq!(t.check_and_advance("q", 10), SequenceCheck::InOrder);
        t.reset("q", None);
        assert_eq!(t.last("q"), None);
    }

    #[test]
    fn envelope_updated_at_reports_last_success() {
        let ts = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z").unwrap().with_timezone(&Utc);
        let env = snap("q", 1, json!(1)).with_updated_at(ts);
        assert_eq!(env.updated_at, ts.to_rfc3339());
        let err = QueryEnvelope::error("inst", "q", 1, None, "down".into());
        assert_eq!(err.status, QueryStatus::Error);
        assert_eq!(err.value, Some(serde_json::Value::Null));
        assert_eq!(err.version, 0);
    }
}
