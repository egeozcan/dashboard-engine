//! Retention, compaction and size budgets (§13 "Retention, compaction and size budgets are
//! mandatory", §20 "No unbounded queues or caches"), plus read-only diagnostics used by `dash doctor`.

use crate::{AuditRecord, SqliteStore, StoreError};
use chrono::{DateTime, Duration, Utc};
use rusqlite::params;
use serde::{Deserialize, Serialize};

/// Per-table retention windows and row caps. Defaults are deliberately conservative for a
/// single-host Installation; they are a documented safe default, not a tuned product decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Trace spans older than this are deleted.
    pub trace_span_days: i64,
    /// At most this many trace spans are kept (newest first).
    pub trace_span_max_rows: i64,
    /// Audit records older than this are deleted (audit is security evidence: long window).
    pub audit_days: i64,
    pub audit_max_rows: i64,
    /// Events older than this are deleted once every consumer checkpoint has passed them.
    /// The newest Event of each source is always kept so per-source sequences never restart.
    pub event_days: i64,
    /// Terminal (succeeded/failed/cancelled) action executions older than this are deleted.
    /// `unknown`, `queued` and `running` executions are never pruned.
    pub action_execution_days: i64,
    /// Resolved dead letters older than this are deleted.
    pub dead_letter_days: i64,
    /// Sampled runtime metrics older than this are deleted.
    pub runtime_metric_days: i64,
    pub runtime_metric_max_rows: i64,
    /// Total SQLite footprint (db + WAL + SHM) budget. When exceeded, an aggressive pass trims
    /// diagnostics tables to `budget_trim_rows` and the WAL is truncated.
    pub db_size_budget_bytes: u64,
    pub budget_trim_rows: i64,
    /// Truncate the WAL when it grows beyond this size.
    pub wal_checkpoint_bytes: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            trace_span_days: 7,
            trace_span_max_rows: 200_000,
            audit_days: 365,
            audit_max_rows: 500_000,
            event_days: 30,
            action_execution_days: 90,
            dead_letter_days: 30,
            runtime_metric_days: 7,
            runtime_metric_max_rows: 100_000,
            db_size_budget_bytes: 1024 * 1024 * 1024,
            budget_trim_rows: 10_000,
            wal_checkpoint_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetentionReport {
    pub trace_spans_deleted: usize,
    pub audit_records_deleted: usize,
    pub events_deleted: usize,
    pub action_executions_deleted: usize,
    pub action_idempotency_deleted: usize,
    pub dead_letters_deleted: usize,
    pub sessions_deleted: usize,
    pub runtime_metrics_deleted: usize,
    pub wal_checkpointed: bool,
    pub db_size_bytes_before: u64,
    pub db_size_bytes_after: u64,
    pub db_size_budget_bytes: u64,
    pub over_budget: bool,
}

/// One `dash doctor` check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreCheck {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

fn cutoff(days: i64) -> String {
    (Utc::now() - Duration::days(days.max(0))).to_rfc3339()
}

fn trim_to_rows(
    conn: &rusqlite::Connection,
    table: &str,
    order_col: &str,
    max_rows: i64,
) -> Result<usize, StoreError> {
    if max_rows < 0 {
        return Ok(0);
    }
    // Table/column names are compile-time constants from this module, never user input.
    let sql = format!(
        "DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} ORDER BY {order_col} DESC LIMIT -1 OFFSET ?1)"
    );
    Ok(conn.execute(&sql, params![max_rows])?)
}

/// The one durable Event retention rule (§7.2, §13), shared by [`SqliteStore::enforce_retention`]
/// and [`SqliteStore::prune_events_before`] so the two passes cannot drift apart. An Event
/// emitted before `cutoff` is deleted only when
/// - it is not the newest Event of its source: the head row is always kept, so the per-source
///   sequence never restarts below a consumer checkpoint after a restart;
/// - every registered consumer of its source has acknowledged it;
/// - no unresolved dead letter references it, so the dead letter can still be redriven.
///
/// Finished automation execution records older than `cutoff` whose Event is gone (or that had no
/// Event) can no longer guard a replay and are dropped with the same retention.
/// Returns the number of deleted Events.
pub(crate) fn prune_retained_events(
    conn: &rusqlite::Connection,
    cutoff: DateTime<Utc>,
) -> Result<usize, StoreError> {
    let is_old = |at: &str| {
        DateTime::parse_from_rfc3339(at)
            .map(|d| d.with_timezone(&Utc) < cutoff)
            .unwrap_or(false)
    };
    let candidates: Vec<(String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT e.event_id, e.emitted_at FROM events e
             WHERE e.sequence < (SELECT MAX(e2.sequence) FROM events e2
                                  WHERE e2.source_instance = e.source_instance)
               AND e.sequence <= COALESCE(
                    (SELECT MIN(c.last_sequence) FROM event_consumer_checkpoints c
                      WHERE c.source_instance = e.source_instance),
                    e.sequence)
               AND NOT EXISTS (SELECT 1 FROM dead_letters d
                                WHERE d.event_id = e.event_id AND d.resolved = 0)",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        let mut v = Vec::new();
        for row in rows {
            v.push(row?);
        }
        v
    };
    let tx = conn.unchecked_transaction()?;
    let mut deleted = 0;
    for (event_id, at) in candidates {
        if is_old(&at) {
            deleted += tx.execute("DELETE FROM events WHERE event_id = ?1", params![event_id])?;
        }
    }
    let stale_runs: Vec<(String, String)> = {
        let mut stmt = tx.prepare(
            "SELECT a.execution_id, a.started_at FROM automation_executions a
             WHERE a.status != 'running'
               AND (a.event_id IS NULL
                    OR NOT EXISTS (SELECT 1 FROM events e WHERE e.event_id = a.event_id))",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        let mut v = Vec::new();
        for row in rows {
            v.push(row?);
        }
        v
    };
    for (execution_id, at) in stale_runs {
        if is_old(&at) {
            tx.execute(
                "DELETE FROM automation_executions WHERE execution_id = ?1",
                params![execution_id],
            )?;
        }
    }
    tx.commit()?;
    Ok(deleted)
}

impl SqliteStore {
    /// Apply the retention policy to every growing table and enforce the SQLite size budget.
    /// Safe to call repeatedly; intended for the daemon's maintenance loop (off the async runtime).
    pub fn enforce_retention(
        &self,
        policy: &RetentionPolicy,
    ) -> Result<RetentionReport, StoreError> {
        let mut report = RetentionReport {
            db_size_bytes_before: self.db_size_bytes(),
            db_size_budget_bytes: policy.db_size_budget_bytes,
            ..Default::default()
        };
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            report.trace_spans_deleted = conn.execute(
                "DELETE FROM trace_spans WHERE started_at < ?1",
                params![cutoff(policy.trace_span_days)],
            )?;
            report.trace_spans_deleted +=
                trim_to_rows(conn, "trace_spans", "started_at", policy.trace_span_max_rows)?;

            report.audit_records_deleted = conn.execute(
                "DELETE FROM audit_records WHERE timestamp < ?1",
                params![cutoff(policy.audit_days)],
            )?;
            report.audit_records_deleted +=
                trim_to_rows(conn, "audit_records", "timestamp", policy.audit_max_rows)?;

            report.events_deleted = prune_retained_events(
                conn,
                Utc::now() - Duration::days(policy.event_days.max(0)),
            )?;

            let exec_cutoff = cutoff(policy.action_execution_days);
            report.action_executions_deleted = conn.execute(
                "DELETE FROM action_executions
                 WHERE status IN ('succeeded', 'failed', 'cancelled')
                   AND COALESCE(finished_at, started_at) < ?1",
                params![exec_cutoff],
            )?;
            conn.execute(
                "DELETE FROM action_steps WHERE execution_id NOT IN (SELECT execution_id FROM action_executions)",
                [],
            )?;
            report.action_idempotency_deleted = conn.execute(
                "DELETE FROM action_idempotency
                 WHERE created_at < ?1
                   AND execution_id NOT IN (SELECT execution_id FROM action_executions)",
                params![exec_cutoff],
            )?;

            report.dead_letters_deleted = conn.execute(
                "DELETE FROM dead_letters WHERE resolved != 0 AND failed_at < ?1",
                params![cutoff(policy.dead_letter_days)],
            )?;

            report.sessions_deleted =
                conn.execute("DELETE FROM sessions WHERE expires_at < ?1", params![now])?;

            report.runtime_metrics_deleted = conn.execute(
                "DELETE FROM runtime_metrics WHERE recorded_at < ?1",
                params![cutoff(policy.runtime_metric_days)],
            )?;
            report.runtime_metrics_deleted += trim_to_rows(
                conn,
                "runtime_metrics",
                "recorded_at",
                policy.runtime_metric_max_rows,
            )?;
            Ok(())
        })?;

        if self.db_path().is_some() {
            let over = self.db_size_bytes() > policy.db_size_budget_bytes;
            if over {
                // Aggressive pass: diagnostics are the only data that may be sacrificed for budget.
                self.with_conn_sync(|conn| {
                    report.trace_spans_deleted +=
                        trim_to_rows(conn, "trace_spans", "started_at", policy.budget_trim_rows)?;
                    report.runtime_metrics_deleted += trim_to_rows(
                        conn,
                        "runtime_metrics",
                        "recorded_at",
                        policy.budget_trim_rows,
                    )?;
                    Ok(())
                })?;
            }
            if over || self.wal_size_bytes() > policy.wal_checkpoint_bytes {
                self.checkpoint_wal_truncate()?;
                report.wal_checkpointed = true;
            }
            report.db_size_bytes_after = self.db_size_bytes();
            report.over_budget = report.db_size_bytes_after > policy.db_size_budget_bytes;
        }
        Ok(report)
    }

    /// `PRAGMA wal_checkpoint(TRUNCATE)`: fold the WAL into the main file and truncate it.
    pub fn checkpoint_wal_truncate(&self) -> Result<(), StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
            Ok(())
        })
    }

    /// Sample one runtime metric into `runtime_metrics` (bounded by [`RetentionPolicy`]).
    pub fn record_runtime_metric(
        &self,
        name: &str,
        value: f64,
        labels: serde_json::Value,
    ) -> Result<(), StoreError> {
        let labels_json = serde_json::to_string(&labels)?;
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO runtime_metrics (recorded_at, name, value, labels_json) VALUES (?1, ?2, ?3, ?4)",
                params![now, name, value, labels_json],
            )?;
            Ok(())
        })
    }

    /// Most recent sampled metrics, newest first: `(recorded_at, name, value, labels)`.
    pub fn list_runtime_metrics(
        &self,
        limit: usize,
    ) -> Result<Vec<(String, String, f64, serde_json::Value)>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT recorded_at, name, value, labels_json FROM runtime_metrics
                 ORDER BY metric_id DESC LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![limit as i64], |r| {
                let raw: String = r.get(3)?;
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    /// Event lag: durable Events not yet processed, summed over every consumer checkpoint
    /// (latest sequence of the source minus the consumer's checkpoint).
    pub fn event_consumer_lag(&self) -> Result<u64, StoreError> {
        self.with_conn_sync(|conn| {
            let lag: i64 = conn.query_row(
                "SELECT COALESCE(SUM(MAX(0, COALESCE((SELECT MAX(e.sequence) FROM events e
                                                     WHERE e.source_instance = c.source_instance), 0)
                                          - c.last_sequence)), 0)
                 FROM event_consumer_checkpoints c",
                [],
                |r| r.get(0),
            )?;
            Ok(lag.max(0) as u64)
        })
    }

    /// Number of action executions still `queued` or `running` (graceful-shutdown drain, §15.2).
    pub fn count_inflight_action_executions(&self) -> Result<u64, StoreError> {
        self.with_conn_sync(|conn| {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM action_executions WHERE status IN ('queued', 'running')",
                [],
                |r| r.get(0),
            )?;
            Ok(n.max(0) as u64)
        })
    }

    /// Audit records attributed to one Principal, newest first.
    pub fn list_audits_for_principal(
        &self,
        principal_id: &str,
        limit: usize,
    ) -> Result<Vec<AuditRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT audit_id, timestamp, principal_id, principal_kind, operation, target, outcome, detail_json
                 FROM audit_records WHERE principal_id = ?1 ORDER BY timestamp DESC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![principal_id, limit as i64], |r| {
                let raw: String = r.get(7)?;
                Ok(AuditRecord {
                    audit_id: r.get(0)?,
                    timestamp: r.get(1)?,
                    principal_id: r.get(2)?,
                    principal_kind: r.get(3)?,
                    operation: r.get(4)?,
                    target: r.get(5)?,
                    outcome: r.get(6)?,
                    detail: serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    /// Row counts of the growing tables, for diagnostics.
    pub fn table_row_counts(&self) -> Result<Vec<(String, i64)>, StoreError> {
        const TABLES: &[&str] = &[
            "trace_spans",
            "audit_records",
            "events",
            "action_executions",
            "dead_letters",
            "sessions",
            "runtime_metrics",
            "history_samples",
        ];
        self.with_conn_sync(|conn| {
            let mut out = Vec::new();
            for t in TABLES {
                let n: i64 =
                    conn.query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| r.get(0))?;
                out.push(((*t).to_string(), n));
            }
            Ok(out)
        })
    }

    /// Read-only store health checks for `dash doctor` (§14.1): integrity, journal mode,
    /// vault key ability to decrypt every stored secret blob, and admin credential presence.
    pub fn health_checks(&self) -> Vec<StoreCheck> {
        let mut checks = Vec::new();
        let quick = self.with_conn_sync(|conn| {
            let v: String = conn.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
            Ok(v)
        });
        checks.push(match quick {
            Ok(v) => StoreCheck {
                name: "sqlite_quick_check".into(),
                ok: v.eq_ignore_ascii_case("ok"),
                detail: v,
            },
            Err(e) => StoreCheck {
                name: "sqlite_quick_check".into(),
                ok: false,
                detail: e.to_string(),
            },
        });

        let mode = self.with_conn_sync(|conn| {
            let v: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
            Ok(v)
        });
        let in_memory = self.db_path().is_none();
        checks.push(match mode {
            Ok(v) => StoreCheck {
                name: "sqlite_journal_mode".into(),
                ok: in_memory || v.eq_ignore_ascii_case("wal"),
                detail: v,
            },
            Err(e) => StoreCheck {
                name: "sqlite_journal_mode".into(),
                ok: false,
                detail: e.to_string(),
            },
        });

        let blobs = self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT name, key_version, nonce, ciphertext FROM encrypted_secret_blobs",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                    r.get::<_, Vec<u8>>(3)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        });
        checks.push(match blobs {
            Ok(rows) => {
                let failed: Vec<String> = rows
                    .iter()
                    .filter(|(name, kv, nonce, ct)| {
                        self.vault().decrypt(name, *kv, nonce, ct).is_err()
                    })
                    .map(|(name, _, _, _)| name.clone())
                    .collect();
                StoreCheck {
                    name: "vault_decrypts_all_secrets".into(),
                    ok: failed.is_empty(),
                    detail: if failed.is_empty() {
                        format!(
                            "{} secret blob(s) decrypt with the current master key",
                            rows.len()
                        )
                    } else {
                        // Names only, never plaintext.
                        format!("cannot decrypt: {}", failed.join(", "))
                    },
                }
            }
            Err(e) => StoreCheck {
                name: "vault_decrypts_all_secrets".into(),
                ok: false,
                detail: e.to_string(),
            },
        });

        checks.push(match self.get_admin_credentials() {
            Ok(Some((user, _))) => StoreCheck {
                name: "admin_credentials".into(),
                ok: true,
                detail: format!("configured for '{user}'"),
            },
            Ok(None) => StoreCheck {
                name: "admin_credentials".into(),
                ok: false,
                detail: "not initialized; run `dash init`".into(),
            },
            Err(e) => StoreCheck {
                name: "admin_credentials".into(),
                ok: false,
                detail: e.to_string(),
            },
        });
        checks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn old_ts(days: i64) -> String {
        (Utc::now() - Duration::days(days)).to_rfc3339()
    }

    #[test]
    fn retention_keeps_events_unresolved_dead_letters_reference() {
        let store = SqliteStore::open_in_memory().unwrap();
        let ids: Vec<String> = (0..4)
            .map(|_| {
                store
                    .append_event("merge_pr", "github.pr_merged", "1", "t", true, json!({}))
                    .unwrap()
                    .event_id
            })
            .collect();
        store
            .with_conn_sync(|c| {
                c.execute("UPDATE events SET emitted_at = ?1", params![old_ts(60)])?;
                Ok(())
            })
            .unwrap();
        // Every Event is acknowledged; #2 failed delivery and is dead-lettered (unresolved),
        // #1 was dead-lettered and later resolved.
        store
            .set_consumer_checkpoint("complete_work", "merge_pr", 4)
            .unwrap();
        let resolved = store
            .record_dead_letter("complete_work", &ids[0], 3, "jira 503")
            .unwrap();
        store.resolve_dead_letter(&resolved).unwrap();
        store
            .record_dead_letter("complete_work", &ids[1], 3, "jira 503")
            .unwrap();
        let report = store
            .enforce_retention(&RetentionPolicy::default())
            .unwrap();
        assert_eq!(
            report.events_deleted, 2,
            "#1 and #3 go; #2 (DLQ) and #4 (head) stay"
        );
        assert!(store.get_event(&ids[0]).unwrap().is_none());
        assert!(
            store.get_event(&ids[1]).unwrap().is_some(),
            "an unresolved dead letter must stay redrivable"
        );
        assert!(store.get_event(&ids[2]).unwrap().is_none());
        assert!(store.get_event(&ids[3]).unwrap().is_some());
    }

    #[test]
    fn retention_prunes_old_rows_but_keeps_unknown_actions_and_latest_event() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::open(dir.path()).unwrap();
        store
            .with_conn_sync(|c| {
                c.execute(
                    "INSERT INTO trace_spans VALUES ('s_old','t',NULL,'q','p','{}','ok',?1,1)",
                    params![old_ts(30)],
                )?;
                c.execute(
                    "INSERT INTO trace_spans VALUES ('s_new','t',NULL,'q','p','{}','ok',?1,1)",
                    params![Utc::now().to_rfc3339()],
                )?;
                c.execute(
                    "INSERT INTO sessions VALUES ('ses1','h1','c','admin',?1,?1)",
                    params![old_ts(2)],
                )?;
                for (id, status) in [("e_ok", "succeeded"), ("e_unknown", "unknown")] {
                    c.execute(
                        "INSERT INTO action_executions (execution_id, action_instance, principal_id, principal_kind,
                            status, args_json, trace_id, started_at, finished_at)
                         VALUES (?1, 'merge_pr', 'admin', 'administrator', ?2, '{}', 't', ?3, ?3)",
                        params![id, status, old_ts(200)],
                    )?;
                }
                Ok(())
            })
            .unwrap();
        // Three old events from one source, consumer has processed up to 2.
        for _ in 0..3 {
            store
                .append_event("merge_pr", "github.pr_merged", "1", "t", true, json!({}))
                .unwrap();
        }
        store
            .with_conn_sync(|c| {
                c.execute("UPDATE events SET emitted_at = ?1", params![old_ts(60)])?;
                Ok(())
            })
            .unwrap();
        store
            .set_consumer_checkpoint("complete_work", "merge_pr", 2)
            .unwrap();

        let report = store
            .enforce_retention(&RetentionPolicy::default())
            .unwrap();
        assert_eq!(report.trace_spans_deleted, 1);
        assert_eq!(report.sessions_deleted, 1);
        assert_eq!(report.action_executions_deleted, 1);
        assert_eq!(
            report.events_deleted, 2,
            "events 1,2 consumed; 3 is newest and kept"
        );
        assert!(store.get_action_execution("e_unknown").unwrap().is_some());
        assert!(store.get_action_execution("e_ok").unwrap().is_none());
        // Sequence continues after pruning (newest event per source kept).
        let next = store
            .append_event("merge_pr", "github.pr_merged", "1", "t", true, json!({}))
            .unwrap();
        assert_eq!(next.sequence, 4);
        assert_eq!(store.event_consumer_lag().unwrap(), 2);
    }

    #[test]
    fn retention_row_caps_and_size_budget() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::open(dir.path()).unwrap();
        for i in 0..20 {
            store
                .record_runtime_metric("active_vms", i as f64, json!({}))
                .unwrap();
        }
        let policy = RetentionPolicy {
            runtime_metric_max_rows: 5,
            db_size_budget_bytes: 1, // always over budget -> aggressive trim + WAL truncate
            budget_trim_rows: 3,
            ..Default::default()
        };
        let report = store.enforce_retention(&policy).unwrap();
        assert!(report.wal_checkpointed);
        assert_eq!(store.list_runtime_metrics(100).unwrap().len(), 3);
        assert_eq!(store.wal_size_bytes(), 0);
        assert!(report.db_size_bytes_before > 0);
    }

    #[test]
    fn health_checks_report_integrity_and_missing_admin() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::open(dir.path()).unwrap();
        store
            .set_secret("s1", "", "plaintext-value-1", &[])
            .unwrap();
        let checks = store.health_checks();
        let get = |n: &str| checks.iter().find(|c| c.name == n).unwrap().clone();
        assert!(get("sqlite_quick_check").ok);
        assert!(get("sqlite_journal_mode").ok);
        assert!(get("vault_decrypts_all_secrets").ok);
        assert!(!get("admin_credentials").ok);
        assert!(!get("vault_decrypts_all_secrets")
            .detail
            .contains("plaintext-value-1"));
    }

    #[test]
    fn audits_filtered_by_principal() {
        let store = SqliteStore::open_in_memory().unwrap();
        store
            .append_audit("agt_a", "agent", "op", "t", "ok", json!({}))
            .unwrap();
        store
            .append_audit("admin", "administrator", "op", "t", "ok", json!({}))
            .unwrap();
        let mine = store.list_audits_for_principal("agt_a", 10).unwrap();
        assert_eq!(mine.len(), 1);
        assert_eq!(mine[0].principal_id, "agt_a");
    }
}
