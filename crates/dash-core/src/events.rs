//! Discrete Event log, ephemeral/durable broadcast, consumer checkpoints, replay, and DLQ (§7.2).
//!
//! Producer semantics (§7.2 "ordering per source Instance"):
//! - Every Event of one source Instance — ephemeral or durable — takes its sequence from one
//!   monotonic per-source counter. The durable log therefore may contain gaps where ephemeral
//!   Events were delivered but not persisted.
//! - Sequence allocation, the durable append and the broadcast happen under one lock, so
//!   broadcast order equals sequence order.
//! - The broadcast channel is only a live wake-up/fan-out path. Durable consumers (automations)
//!   read the log in sequence order from their checkpoint, so a lagged receiver or a restart never
//!   loses a durable Event (at-least-once).
//!
//! The durable log sits behind the [`EventLog`] storage contract; SQLite is the first backend.

use dash_luau::EventDeclSpec;
use dash_store::{DeadLetterRecord, DurableEventRecord, Redactor, SqliteStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

/// Replaceable durable Event storage contract (§7.2): append, ordered reads, consumer
/// checkpoints, dead letters and retention.
pub trait EventLog: Send + Sync {
    /// Append with sequence `max(high water + 1, min_sequence)`, where the high water is
    /// [`EventLog::max_sequence`]: a backend must never re-issue a sequence at or below one it
    /// already issued or a consumer acknowledged, even after retention emptied the log.
    #[allow(clippy::too_many_arguments)]
    fn append(
        &self,
        source_instance: &str,
        semantic_type: &str,
        schema_version: &str,
        trace_id: &str,
        payload: serde_json::Value,
        min_sequence: i64,
    ) -> Result<DurableEventRecord, String>;
    /// Highest sequence the source is known to have reached (newest logged Event or furthest
    /// consumer checkpoint); seeds the per-source counter after a restart.
    fn max_sequence(&self, source_instance: &str) -> Result<i64, String>;
    fn max_sequence_before(&self, source_instance: &str, cutoff: &str) -> Result<i64, String>;
    fn list_since(
        &self,
        source_instance: &str,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<DurableEventRecord>, String>;
    fn get(&self, event_id: &str) -> Result<Option<DurableEventRecord>, String>;
    fn sources(&self) -> Result<Vec<String>, String>;
    fn checkpoint(&self, consumer_id: &str, source_instance: &str) -> Result<Option<i64>, String>;
    /// Monotonic: never moves a checkpoint backwards.
    fn set_checkpoint(
        &self,
        consumer_id: &str,
        source_instance: &str,
        sequence: i64,
    ) -> Result<(), String>;
    fn record_dead_letter(
        &self,
        consumer_id: &str,
        event_id: &str,
        attempts: i64,
        error: &str,
    ) -> Result<String, String>;
    fn list_dead_letters(
        &self,
        include_resolved: bool,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, String>;
    fn get_dead_letter(&self, dlq_id: &str) -> Result<Option<DeadLetterRecord>, String>;
    fn resolve_dead_letter(&self, dlq_id: &str) -> Result<bool, String>;
    /// Delete acknowledged Events older than `cutoff`, keeping each source's newest Event and every
    /// Event an unresolved dead letter references; returns how many were deleted.
    fn prune_before(&self, cutoff: &str) -> Result<usize, String>;
}

impl EventLog for SqliteStore {
    fn append(
        &self,
        source_instance: &str,
        semantic_type: &str,
        schema_version: &str,
        trace_id: &str,
        payload: serde_json::Value,
        min_sequence: i64,
    ) -> Result<DurableEventRecord, String> {
        self.append_event_min_sequence(
            source_instance,
            semantic_type,
            schema_version,
            trace_id,
            true,
            payload,
            min_sequence,
        )
        .map_err(|e| e.to_string())
    }
    fn max_sequence(&self, source_instance: &str) -> Result<i64, String> {
        self.event_sequence_high_water(source_instance)
            .map_err(|e| e.to_string())
    }
    fn max_sequence_before(&self, source_instance: &str, cutoff: &str) -> Result<i64, String> {
        self.max_event_sequence_before(source_instance, cutoff)
            .map_err(|e| e.to_string())
    }
    fn list_since(
        &self,
        source_instance: &str,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<DurableEventRecord>, String> {
        self.list_events_since_limited(source_instance, after_sequence, limit)
            .map_err(|e| e.to_string())
    }
    fn get(&self, event_id: &str) -> Result<Option<DurableEventRecord>, String> {
        self.get_event(event_id).map_err(|e| e.to_string())
    }
    fn sources(&self) -> Result<Vec<String>, String> {
        self.list_event_sources().map_err(|e| e.to_string())
    }
    fn checkpoint(&self, consumer_id: &str, source_instance: &str) -> Result<Option<i64>, String> {
        self.find_consumer_checkpoint(consumer_id, source_instance)
            .map_err(|e| e.to_string())
    }
    fn set_checkpoint(
        &self,
        consumer_id: &str,
        source_instance: &str,
        sequence: i64,
    ) -> Result<(), String> {
        self.set_consumer_checkpoint(consumer_id, source_instance, sequence)
            .map_err(|e| e.to_string())
    }
    fn record_dead_letter(
        &self,
        consumer_id: &str,
        event_id: &str,
        attempts: i64,
        error: &str,
    ) -> Result<String, String> {
        SqliteStore::record_dead_letter(self, consumer_id, event_id, attempts, error)
            .map_err(|e| e.to_string())
    }
    fn list_dead_letters(
        &self,
        include_resolved: bool,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, String> {
        SqliteStore::list_dead_letters(self, include_resolved, limit).map_err(|e| e.to_string())
    }
    fn get_dead_letter(&self, dlq_id: &str) -> Result<Option<DeadLetterRecord>, String> {
        SqliteStore::get_dead_letter(self, dlq_id).map_err(|e| e.to_string())
    }
    fn resolve_dead_letter(&self, dlq_id: &str) -> Result<bool, String> {
        SqliteStore::resolve_dead_letter(self, dlq_id).map_err(|e| e.to_string())
    }
    fn prune_before(&self, cutoff: &str) -> Result<usize, String> {
        self.prune_events_before(cutoff).map_err(|e| e.to_string())
    }
}

/// Default durable Event retention when nothing else is configured (§7.2 "retention").
pub const DEFAULT_EVENT_RETENTION_DAYS: i64 = 30;

#[derive(Clone)]
pub struct EventBus {
    log: Arc<dyn EventLog>,
    redactor: Redactor,
    tx: broadcast::Sender<EventEnvelope>,
    /// Last sequence allocated per source Instance (shared by ephemeral and durable Events).
    /// Held across allocation, append and broadcast so per-source order is total.
    sequences: Arc<Mutex<HashMap<String, i64>>>,
}

impl EventBus {
    pub fn new(store: SqliteStore, capacity: usize) -> Self {
        let redactor = store.redactor().clone();
        Self::with_log(Arc::new(store), redactor, capacity)
    }

    /// Build an EventBus over any [`EventLog`] backend.
    pub fn with_log(log: Arc<dyn EventLog>, redactor: Redactor, capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(128));
        Self {
            log,
            redactor,
            tx,
            sequences: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn log(&self) -> &Arc<dyn EventLog> {
        &self.log
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
        let mut seqs = self.sequences.lock().unwrap_or_else(|p| p.into_inner());
        let last = seqs.get(source_instance).copied();
        let env = if durable {
            let rec = self.log.append(
                source_instance,
                semantic_type,
                schema_version,
                trace_id,
                payload,
                last.map(|s| s + 1).unwrap_or(1),
            )?;
            EventEnvelope::from(rec)
        } else {
            let base = match last {
                Some(s) => s,
                None => self.log.max_sequence(source_instance)?,
            };
            EventEnvelope {
                event_id: format!("evt_{}", uuid::Uuid::now_v7().simple()),
                source_instance: source_instance.to_string(),
                sequence: base + 1,
                emitted_at: chrono::Utc::now().to_rfc3339(),
                semantic_type: semantic_type.to_string(),
                schema_version: schema_version.to_string(),
                trace_id: trace_id.to_string(),
                durable: false,
                payload: self.redactor.redact_json(&payload),
                is_replay: false,
            }
        };
        seqs.insert(source_instance.to_string(), env.sequence);
        // Broadcast while still holding the sequence lock: live order == sequence order.
        let _ = self.tx.send(env.clone());
        drop(seqs);
        Ok(env)
    }

    /// Emit an Event declared in a Definition contract: `schema_version` comes from the matching
    /// [`EventDeclSpec`], and a contract that declares `durable = true` makes the Event durable even
    /// when the script did not ask for it (the script may only upgrade to durable).
    pub fn emit_declared(
        &self,
        source_instance: &str,
        declared: &[EventDeclSpec],
        semantic_type: &str,
        trace_id: &str,
        durable_requested: bool,
        payload: serde_json::Value,
    ) -> Result<EventEnvelope, String> {
        let decl = declared.iter().find(|d| d.semantic_type == semantic_type);
        let schema_version = decl
            .map(|d| d.schema_version.trim())
            .filter(|v| !v.is_empty())
            .unwrap_or("1");
        let durable = durable_requested || decl.map(|d| d.durable).unwrap_or(false);
        self.emit(
            source_instance,
            semantic_type,
            schema_version,
            trace_id,
            durable,
            payload,
        )
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.tx.subscribe()
    }

    /// Read durable events for `source_instance` after `after_sequence`, marked `is_replay`.
    ///
    /// Replay is a read: it does not by itself re-run anything. Consumers that receive replayed
    /// Events (see `AutomationEngine::dispatch_event`) consult their execution records and do not
    /// repeat a run that already succeeded for the same `event_id`; re-execution requires an
    /// explicit, authorized request (`AutomationEngine::replay_events` with `reexecute_as`) (§7.2).
    pub fn replay_since(
        &self,
        source_instance: &str,
        after_sequence: i64,
    ) -> Result<Vec<EventEnvelope>, String> {
        let records = self
            .log
            .list_since(source_instance, after_sequence, usize::MAX)?;
        Ok(records
            .into_iter()
            .map(|r| {
                let mut env = EventEnvelope::from(r);
                env.is_replay = true;
                env
            })
            .collect())
    }

    /// Ordered, bounded read for durable consumers (not marked as replay).
    pub fn read_since(
        &self,
        source_instance: &str,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<EventEnvelope>, String> {
        Ok(self
            .log
            .list_since(source_instance, after_sequence, limit)?
            .into_iter()
            .map(EventEnvelope::from)
            .collect())
    }

    pub fn get_event(&self, event_id: &str) -> Result<Option<EventEnvelope>, String> {
        Ok(self.log.get(event_id)?.map(EventEnvelope::from))
    }

    pub fn ack_checkpoint(
        &self,
        consumer_id: &str,
        source_instance: &str,
        sequence: i64,
    ) -> Result<(), String> {
        self.log
            .set_checkpoint(consumer_id, source_instance, sequence)
    }

    pub fn get_checkpoint(&self, consumer_id: &str, source_instance: &str) -> i64 {
        self.log
            .checkpoint(consumer_id, source_instance)
            .ok()
            .flatten()
            .unwrap_or(0)
    }

    /// Checkpoint, or `None` if this consumer was never registered for the source.
    pub fn find_checkpoint(
        &self,
        consumer_id: &str,
        source_instance: &str,
    ) -> Result<Option<i64>, String> {
        self.log.checkpoint(consumer_id, source_instance)
    }

    pub fn send_to_dead_letter(
        &self,
        consumer_id: &str,
        event_id: &str,
        attempts: i64,
        error: &str,
    ) -> Result<String, String> {
        self.log
            .record_dead_letter(consumer_id, event_id, attempts, error)
    }

    pub fn list_dead_letters(
        &self,
        include_resolved: bool,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, String> {
        self.log.list_dead_letters(include_resolved, limit)
    }

    pub fn get_dead_letter(&self, dlq_id: &str) -> Result<Option<DeadLetterRecord>, String> {
        self.log.get_dead_letter(dlq_id)
    }

    pub fn resolve_dead_letter(&self, dlq_id: &str) -> Result<bool, String> {
        self.log.resolve_dead_letter(dlq_id)
    }

    /// Apply durable Event retention: delete acknowledged Events older than `retention_days`.
    pub fn prune_retained(&self, retention_days: i64) -> Result<usize, String> {
        let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days.max(1));
        self.log.prune_before(&cutoff.to_rfc3339())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ephemeral_and_durable_events_share_one_sequence_space() {
        let store = SqliteStore::open_in_memory().unwrap();
        let bus = EventBus::new(store.clone(), 16);
        let e1 = bus.emit("src", "a", "1", "tr", false, json!({})).unwrap();
        let e2 = bus.emit("src", "a", "1", "tr", true, json!({})).unwrap();
        let e3 = bus.emit("src", "a", "1", "tr", false, json!({})).unwrap();
        let e4 = bus.emit("src", "a", "1", "tr", true, json!({})).unwrap();
        assert_eq!(
            vec![e1.sequence, e2.sequence, e3.sequence, e4.sequence],
            vec![1, 2, 3, 4]
        );
        // Only durable Events are persisted; the log has a gap where ephemeral #3 was.
        let durable: Vec<i64> = bus
            .replay_since("src", 0)
            .unwrap()
            .iter()
            .map(|e| e.sequence)
            .collect();
        assert_eq!(durable, vec![2, 4]);

        // A fresh bus (restart) seeds ephemeral sequences from the durable log.
        let bus2 = EventBus::new(store, 16);
        assert_eq!(
            bus2.emit("src", "a", "1", "tr", false, json!({}))
                .unwrap()
                .sequence,
            5
        );
    }

    #[test]
    fn broadcast_order_matches_sequence_order_under_concurrency() {
        let store = SqliteStore::open_in_memory().unwrap();
        let bus = EventBus::new(store, 4096);
        let mut rx = bus.subscribe();
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let b = bus.clone();
                std::thread::spawn(move || {
                    for _ in 0..25 {
                        b.emit("src", "a", "1", "tr", true, json!({})).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let mut last = 0;
        for _ in 0..200 {
            let ev = rx.try_recv().unwrap();
            assert!(
                ev.sequence > last,
                "broadcast out of order: {} after {last}",
                ev.sequence
            );
            last = ev.sequence;
        }
    }

    #[test]
    fn declared_contract_supplies_schema_version_and_durability() {
        let store = SqliteStore::open_in_memory().unwrap();
        let bus = EventBus::new(store, 16);
        let decls = vec![EventDeclSpec {
            semantic_type: "github.pr_merged".into(),
            schema_version: "3".into(),
            durable: true,
            payload: None,
        }];
        let ev = bus
            .emit_declared("src", &decls, "github.pr_merged", "tr", false, json!({}))
            .unwrap();
        assert_eq!(ev.schema_version, "3");
        assert!(ev.durable);
        let other = bus
            .emit_declared("src", &decls, "other.event", "tr", false, json!({}))
            .unwrap();
        assert_eq!(other.schema_version, "1");
        assert!(!other.durable);
    }

    #[test]
    fn replay_does_not_broadcast_or_move_checkpoints_backwards() {
        let store = SqliteStore::open_in_memory().unwrap();
        let bus = EventBus::new(store, 16);
        bus.emit("src", "a", "1", "tr", true, json!({})).unwrap();
        bus.emit("src", "a", "1", "tr", true, json!({})).unwrap();
        let mut rx = bus.subscribe();
        let replayed = bus.replay_since("src", 0).unwrap();
        assert_eq!(replayed.len(), 2);
        assert!(replayed.iter().all(|e| e.is_replay));
        assert!(rx.try_recv().is_err(), "replay must not re-broadcast");
        bus.ack_checkpoint("c", "src", 2).unwrap();
        bus.ack_checkpoint("c", "src", 1).unwrap();
        assert_eq!(bus.get_checkpoint("c", "src"), 2);
    }
}
