//! Discrete Event log, ephemeral/durable broadcast, consumer checkpoints, replay, and DLQ (§7.2).

use dash_store::{DurableEventRecord, SqliteStore};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: String,
    pub source_instance: String,
    pub sequence: i64,
    pub emitted_at: String,
    pub semantic_type: String,
    pub schema_version: String,
    pub trace_id: String,
    pub durable: bool,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub is_replay: bool,
}

impl From<DurableEventRecord> for EventEnvelope {
    fn from(r: DurableEventRecord) -> Self {
        Self {
            event_id: r.event_id,
            source_instance: r.source_instance,
            sequence: r.sequence,
            emitted_at: r.emitted_at,
            semantic_type: r.semantic_type,
            schema_version: r.schema_version,
            trace_id: r.trace_id,
            durable: r.durable,
            payload: r.payload,
            is_replay: false,
        }
    }
}

#[derive(Clone)]
pub struct EventBus {
    store: SqliteStore,
    tx: broadcast::Sender<EventEnvelope>,
    ephemeral_seq: Arc<Mutex<std::collections::HashMap<String, i64>>>,
    seen_replay_ids: Arc<Mutex<HashSet<String>>>,
}

impl EventBus {
    pub fn new(store: SqliteStore, capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(128));
        Self {
            store,
            tx,
            ephemeral_seq: Arc::new(Mutex::new(std::collections::HashMap::new())),
            seen_replay_ids: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn emit(
        &self,
        source_instance: &str,
        semantic_type: &str,
        schema_version: &str,
        trace_id: &str,
        durable: bool,
        payload: serde_json::Value,
    ) -> Result<EventEnvelope, String> {
        let env = if durable {
            let rec = self
                .store
                .append_event(
                    source_instance,
                    semantic_type,
                    schema_version,
                    trace_id,
                    true,
                    payload,
                )
                .map_err(|e| e.to_string())?;
            EventEnvelope::from(rec)
        } else {
            let mut seqs = self.ephemeral_seq.lock().unwrap();
            let seq = seqs.entry(source_instance.to_string()).or_insert(0);
            *seq += 1;
            EventEnvelope {
                event_id: format!("evt_{}", uuid::Uuid::now_v7().simple()),
                source_instance: source_instance.to_string(),
                sequence: *seq,
                emitted_at: chrono::Utc::now().to_rfc3339(),
                semantic_type: semantic_type.to_string(),
                schema_version: schema_version.to_string(),
                trace_id: trace_id.to_string(),
                durable: false,
                payload: self.store.redactor().redact_json(&payload),
                is_replay: false,
            }
        };
        let _ = self.tx.send(env.clone());
        Ok(env)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.tx.subscribe()
    }

    /// Replay durable events for `source_instance` since consumer checkpoint (or sequence 0).
    /// Note: downstream action invocation uses deterministic idempotency keys based on `event_id`
    /// so replaying events never duplicates side effects (§7.2).
    pub fn replay_since(
        &self,
        source_instance: &str,
        after_sequence: i64,
    ) -> Result<Vec<EventEnvelope>, String> {
        let records = self
            .store
            .list_events_since(source_instance, after_sequence)
            .map_err(|e| e.to_string())?;
        let mut out = Vec::with_capacity(records.len());
        let mut seen = self.seen_replay_ids.lock().unwrap();
        for r in records {
            seen.insert(r.event_id.clone());
            let mut env = EventEnvelope::from(r);
            env.is_replay = true;
            let _ = self.tx.send(env.clone());
            out.push(env);
        }
        Ok(out)
    }

    pub fn ack_checkpoint(
        &self,
        consumer_id: &str,
        source_instance: &str,
        sequence: i64,
    ) -> Result<(), String> {
        self.store
            .set_consumer_checkpoint(consumer_id, source_instance, sequence)
            .map_err(|e| e.to_string())
    }

    pub fn get_checkpoint(&self, consumer_id: &str, source_instance: &str) -> i64 {
        self.store
            .get_consumer_checkpoint(consumer_id, source_instance)
            .unwrap_or(0)
    }

    pub fn send_to_dead_letter(
        &self,
        consumer_id: &str,
        event_id: &str,
        attempts: i64,
        error: &str,
    ) -> Result<String, String> {
        self.store
            .record_dead_letter(consumer_id, event_id, attempts, error)
            .map_err(|e| e.to_string())
    }
}
