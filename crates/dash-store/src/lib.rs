//! SQLite storage engine, transactional migrations, secret vault, and redaction.

pub mod migrations;
pub mod retention;
pub mod vault;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use uuid::Uuid;

pub use retention::{RetentionPolicy, RetentionReport, StoreCheck};
pub use vault::{KeyProtection, Redactor, SecretVault, VaultError, VaultKeySource};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("vault error: {0}")]
    Vault(#[from] VaultError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("state CAS version conflict: expected {expected}, actual {actual}")]
    CasConflict { expected: i64, actual: i64 },
    #[error("state value too large: {size} bytes exceeds the {limit}-byte limit")]
    StateTooLarge { size: usize, limit: usize },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("blocking task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateRecord {
    pub scope: String,
    pub owner: String,
    pub state_key: String,
    pub schema_version: i64,
    pub version: i64,
    pub generation_id: i64,
    pub value: serde_json::Value,
    pub updated_at: String,
}

/// Default per-record size limit for persisted State values (encoded JSON bytes, §8).
///
/// Applies to every write that does not carry a declared limit. Chosen to comfortably fit
/// cursors, checkpoints and synchronized mappings while keeping a single record bounded.
pub const DEFAULT_STATE_MAX_BYTES: usize = 256 * 1024;

/// Absolute ceiling for any declared State size limit.
pub const HARD_STATE_MAX_BYTES: usize = 4 * 1024 * 1024;

/// One step of a transactional State migration (see [`SqliteStore::apply_state_migrations`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateMigrationStep {
    pub scope: String,
    pub owner: String,
    pub state_key: String,
    /// The record version the plan was computed against (CAS).
    pub expected_version: i64,
    pub new_schema_version: i64,
    pub generation_id: i64,
    /// New value, or `None` to remove the record.
    pub new_value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretMetadata {
    pub name: String,
    pub description: String,
    pub key_version: i64,
    pub allowed_bindings: Vec<String>,
    /// Destination hosts the credential may be attached to (`*.example.com` / exact / `*`).
    /// Empty = any host the effective network policy allows.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    /// How the broker presents the credential: `bearer` (default), `basic` (value is
    /// `user:password`), `header:<Name>` or `query:<param>`. Administrator-controlled.
    #[serde(default = "default_credential_scheme")]
    pub credential_scheme: String,
    pub created_at: String,
    pub updated_at: String,
}

pub fn default_credential_scheme() -> String {
    "bearer".into()
}

/// Validate a credential scheme string (`bearer`, `basic`, `header:<Name>`, `query:<param>`).
pub fn validate_credential_scheme(scheme: &str) -> Result<(), String> {
    let valid_token = |t: &str| {
        !t.is_empty()
            && t.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    };
    match scheme {
        "bearer" | "basic" => Ok(()),
        s => {
            if let Some(name) = s.strip_prefix("header:") {
                let lower = name.to_ascii_lowercase();
                if !valid_token(name)
                    || lower == "host"
                    || lower == "cookie"
                    || lower == "content-length"
                    || lower.starts_with("proxy-")
                {
                    return Err(format!("invalid credential header name '{name}'"));
                }
                Ok(())
            } else if let Some(param) = s.strip_prefix("query:") {
                if !valid_token(param) {
                    return Err(format!("invalid credential query parameter '{param}'"));
                }
                Ok(())
            } else {
                Err(format!(
                    "unknown credential scheme '{s}' (expected bearer, basic, header:<Name> or query:<param>)"
                ))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableEventRecord {
    pub event_id: String,
    pub source_instance: String,
    pub sequence: i64,
    pub emitted_at: String,
    pub semantic_type: String,
    pub schema_version: String,
    pub trace_id: String,
    pub durable: bool,
    pub payload: serde_json::Value,
}

/// A durable Event that a consumer could not process after bounded retries (§7.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    pub dlq_id: String,
    pub consumer_id: String,
    pub event_id: String,
    pub attempts: i64,
    pub last_error: String,
    pub failed_at: String,
    pub resolved: bool,
}

/// One Automation run: which trigger caused it, under which Service principal, its outcome and
/// the action executions it produced (§7.3 execution records).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationExecutionRecord {
    pub execution_id: String,
    pub automation_id: String,
    /// `event` | `cron` | `signal` | `manual`
    pub trigger_kind: String,
    /// Stable identity of the triggering occurrence, e.g. `event:<event_id>`,
    /// `cron:<index>:<scheduled_for>` or `signal:<index>:<fingerprint>`.
    pub trigger_ref: String,
    pub event_id: Option<String>,
    pub run_as: Option<String>,
    /// `running` | `succeeded` | `failed`
    pub status: String,
    pub attempts: i64,
    pub error: Option<String>,
    pub result: Option<serde_json::Value>,
    #[serde(default)]
    pub action_executions: Vec<String>,
    pub trace_id: String,
    pub started_at: String,
    pub finished_at: Option<String>,
}

/// Persisted per-trigger state for cron catch-up and Signal predicate change detection (§7.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationTriggerState {
    pub automation_id: String,
    pub trigger_key: String,
    pub last_fired_at: Option<String>,
    pub last_fingerprint: Option<String>,
    pub updated_at: String,
}

/// Upper bound on history samples returned by one query (§7.4 query budget).
pub const HISTORY_QUERY_BUDGET: usize = 10_000;
/// Upper bound on durable Events returned by one consumer log read.
pub const EVENT_READ_BUDGET: usize = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionExecutionRecord {
    pub execution_id: String,
    pub action_instance: String,
    pub parent_execution_id: Option<String>,
    pub principal_id: String,
    pub principal_kind: String,
    pub status: String,
    pub idempotency_key: Option<String>,
    pub args: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub reason: Option<String>,
    pub retry_safe: bool,
    pub reconcile_attempts: i64,
    pub trace_id: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_ms: Option<i64>,
}

/// Outcome of atomically claiming an idempotency key while inserting a new execution record (§9.2).
#[derive(Debug, Clone)]
pub enum IdempotencyClaim {
    /// The key was free (or superseded a retry-safe terminal failure); the new record was inserted.
    New,
    /// The key already names an execution with the same arguments; nothing was inserted.
    Existing(ActionExecutionRecord),
    /// The key already names an execution with *different* arguments; nothing was inserted.
    ArgsMismatch(ActionExecutionRecord),
}

/// New field values for a conditional execution status transition (§9.1).
#[derive(Debug, Clone)]
pub struct ActionTransition<'a> {
    pub status: &'a str,
    pub result: Option<serde_json::Value>,
    pub reason: Option<&'a str>,
    pub retry_safe: bool,
    pub reconcile_attempts: i64,
    pub finished_at: Option<&'a str>,
    pub duration_ms: Option<i64>,
}

/// Records swept by start-up / shutdown crash recovery of in-flight executions (§15.2).
#[derive(Debug, Clone, Default)]
pub struct InflightRecovery {
    /// `running` records whose outcome cannot be proven; now `unknown`.
    pub marked_unknown: Vec<String>,
    /// `queued` records whose handler never started; now `failed` with `retry_safe = true`.
    pub marked_not_started: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    pub audit_id: String,
    pub timestamp: String,
    pub principal_id: String,
    pub principal_kind: String,
    pub operation: String,
    pub target: String,
    pub outcome: String,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSpanRecord {
    pub span_id: String,
    pub trace_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub principal_id: String,
    pub attributes: serde_json::Value,
    pub status: String,
    pub started_at: String,
    pub duration_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationJournalRecord {
    pub journal_id: String,
    pub draft_id: String,
    pub base_revision: String,
    pub target_revision: Option<String>,
    pub generation_id: Option<i64>,
    pub phase: String,
    pub error_message: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DraftRecord {
    pub draft_id: String,
    pub base_revision: String,
    pub worktree_path: String,
    pub created_by: String,
    pub status: String,
    pub changes: Vec<String>,
    pub validation_report: Option<serde_json::Value>,
    pub test_report: Option<serde_json::Value>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentPrincipalRecord {
    pub agent_id: String,
    pub name: String,
    pub ceiling: serde_json::Value,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistorySeriesSpec {
    pub series_id: String,
    pub instance_id: String,
    pub params: serde_json::Value,
    pub aggregator: String,
    pub cadence_secs: i64,
    pub retention_days: i64,
    pub service_principal: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistorySampleRecord {
    pub sample_id: i64,
    pub series_id: String,
    pub sampled_at: String,
    pub value_num: f64,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitSyncRecord {
    pub active_revision: Option<String>,
    pub remote_name: String,
    pub remote_url: Option<String>,
    pub sync_status: String,
    pub last_error: Option<String>,
    pub updated_at: String,
}

#[derive(Clone)]
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
    db_path: Option<PathBuf>,
    vault: SecretVault,
}

impl SqliteStore {
    /// Open or create a persistent SQLite database in WAL mode inside `data_dir`.
    pub fn open(data_dir: &Path) -> Result<Self, StoreError> {
        std::fs::create_dir_all(data_dir)?;
        let db_path = data_dir.join("dash.db");
        let conn = Connection::open(&db_path)?;
        migrations::run_migrations(&conn)?;
        Self::ensure_action_ledger_schema(&conn)?;
        let vault = SecretVault::load_or_create(data_dir)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            db_path: Some(db_path),
            vault,
        };
        store.preload_redactor()?;
        Ok(store)
    }

    /// Create an isolated in-memory SQLite store (for Preview and unit tests).
    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        migrations::run_migrations(&conn)?;
        Self::ensure_action_ledger_schema(&conn)?;
        let vault = SecretVault::generate_ephemeral();
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            db_path: None,
            vault,
        })
    }

    pub fn vault(&self) -> &SecretVault {
        &self.vault
    }

    pub fn redactor(&self) -> &Redactor {
        self.vault.redactor()
    }

    pub fn db_path(&self) -> Option<&Path> {
        self.db_path.as_deref()
    }

    /// Total on-disk SQLite footprint: main database plus `-wal` and `-shm` side files (§15.1 "SQLite space").
    pub fn db_size_bytes(&self) -> u64 {
        let Some(p) = self.db_path.as_ref() else {
            return 0;
        };
        let main = std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        main + self.wal_size_bytes() + Self::side_file_len(p, "-shm")
    }

    /// Size of the write-ahead log file alone.
    pub fn wal_size_bytes(&self) -> u64 {
        self.db_path
            .as_ref()
            .map(|p| Self::side_file_len(p, "-wal"))
            .unwrap_or(0)
    }

    fn side_file_len(db: &Path, suffix: &str) -> u64 {
        let mut os = db.as_os_str().to_os_string();
        os.push(suffix);
        std::fs::metadata(PathBuf::from(os))
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Execute a blocking SQLite closure without blocking Tokio worker threads.
    pub async fn with_conn<F, R>(&self, f: F) -> Result<R, StoreError>
    where
        F: FnOnce(&Connection) -> Result<R, StoreError> + Send + 'static,
        R: Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().expect("sqlite mutex poisoned");
            f(&guard)
        })
        .await?
    }

    /// Synchronous access helper for setup/teardown and tests.
    pub fn with_conn_sync<F, R>(&self, f: F) -> Result<R, StoreError>
    where
        F: FnOnce(&Connection) -> Result<R, StoreError>,
    {
        let guard = self.conn.lock().expect("sqlite mutex poisoned");
        f(&guard)
    }

    fn preload_redactor(&self) -> Result<(), StoreError> {
        self.with_conn_sync(|conn| {
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
            for row in rows {
                let (name, key_ver, nonce, ct) = row?;
                if let Ok(plain) = self.vault.decrypt(&name, key_ver, &nonce, &ct) {
                    self.vault.redactor().register_secret(&plain);
                }
            }
            Ok(())
        })
    }

    pub fn backup_to(&self, dest_path: &Path) -> Result<(), StoreError> {
        if let Some(parent) = dest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        self.with_conn_sync(|conn| {
            let mut dst = Connection::open(dest_path)?;
            let backup = rusqlite::backup::Backup::new(conn, &mut dst)?;
            backup.run_to_completion(5, std::time::Duration::from_millis(10), None)?;
            Ok(())
        })
    }

    // ---- Secrets ----

    pub fn set_secret(
        &self,
        name: &str,
        description: &str,
        plaintext: &str,
        allowed_bindings: &[String],
    ) -> Result<SecretMetadata, StoreError> {
        self.set_secret_with_policy(name, description, plaintext, allowed_bindings, None, None)
    }

    /// Store (or rotate) a secret with its destination-host and credential-scheme policy.
    /// `None` keeps the existing policy value (or the default for a new secret).
    pub fn set_secret_with_policy(
        &self,
        name: &str,
        description: &str,
        plaintext: &str,
        allowed_bindings: &[String],
        allowed_hosts: Option<&[String]>,
        credential_scheme: Option<&str>,
    ) -> Result<SecretMetadata, StoreError> {
        if let Some(scheme) = credential_scheme {
            validate_credential_scheme(scheme).map_err(StoreError::InvalidInput)?;
        }
        let now = Utc::now().to_rfc3339();
        let allowed_json = serde_json::to_string(allowed_bindings)?;
        let (key_version, created_at, prev_hosts, prev_scheme) = self.with_conn_sync(|conn| {
            let existing: Option<(i64, String, String, String)> = conn
                .query_row(
                    "SELECT key_version, created_at, allowed_hosts_json, credential_scheme
                     FROM secret_metadata WHERE name = ?1",
                    params![name],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .optional()?;
            match existing {
                Some((v, c, h, sc)) => Ok((
                    v + 1,
                    c,
                    serde_json::from_str::<Vec<String>>(&h).unwrap_or_default(),
                    sc,
                )),
                None => Ok((1_i64, now.clone(), Vec::new(), default_credential_scheme())),
            }
        })?;
        let hosts: Vec<String> = allowed_hosts.map(|h| h.to_vec()).unwrap_or(prev_hosts);
        let scheme: String = credential_scheme.map(str::to_string).unwrap_or(prev_scheme);
        let hosts_json = serde_json::to_string(&hosts)?;
        let (nonce, ciphertext) = self.vault.encrypt(name, key_version, plaintext)?;
        self.with_conn_sync(|conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute(
                "INSERT INTO secret_metadata (name, description, key_version, allowed_bindings_json, created_at, updated_at, allowed_hosts_json, credential_scheme)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(name) DO UPDATE SET
                    description = excluded.description,
                    key_version = excluded.key_version,
                    allowed_bindings_json = excluded.allowed_bindings_json,
                    updated_at = excluded.updated_at,
                    allowed_hosts_json = excluded.allowed_hosts_json,
                    credential_scheme = excluded.credential_scheme",
                params![name, description, key_version, allowed_json, created_at, now, hosts_json, scheme],
            )?;
            tx.execute(
                "INSERT INTO encrypted_secret_blobs (name, key_version, nonce, ciphertext, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(name) DO UPDATE SET
                    key_version = excluded.key_version,
                    nonce = excluded.nonce,
                    ciphertext = excluded.ciphertext,
                    updated_at = excluded.updated_at",
                params![name, key_version, nonce, ciphertext, now],
            )?;
            tx.commit()?;
            Ok(())
        })?;
        Ok(SecretMetadata {
            name: name.to_string(),
            description: description.to_string(),
            key_version,
            allowed_bindings: allowed_bindings.to_vec(),
            allowed_hosts: hosts,
            credential_scheme: scheme,
            created_at,
            updated_at: now,
        })
    }

    pub fn list_secrets(&self) -> Result<Vec<SecretMetadata>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT name, description, key_version, allowed_bindings_json, created_at, updated_at,
                        allowed_hosts_json, credential_scheme
                 FROM secret_metadata ORDER BY name ASC",
            )?;
            let rows = stmt.query_map([], |r| {
                let allowed_raw: String = r.get(3)?;
                let allowed: Vec<String> = serde_json::from_str(&allowed_raw).unwrap_or_default();
                let hosts_raw: String = r.get(6)?;
                Ok(SecretMetadata {
                    name: r.get(0)?,
                    description: r.get(1)?,
                    key_version: r.get(2)?,
                    allowed_bindings: allowed,
                    allowed_hosts: serde_json::from_str(&hosts_raw).unwrap_or_default(),
                    credential_scheme: r.get(7)?,
                    created_at: r.get(4)?,
                    updated_at: r.get(5)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn resolve_secret_plaintext(&self, name: &str) -> Result<(SecretMetadata, String), StoreError> {
        self.with_conn_sync(|conn| {
            let row = conn
                .query_row(
                    "SELECT m.name, m.description, m.key_version, m.allowed_bindings_json, m.created_at, m.updated_at,
                            b.nonce, b.ciphertext, m.allowed_hosts_json, m.credential_scheme
                     FROM secret_metadata m
                     JOIN encrypted_secret_blobs b ON m.name = b.name
                     WHERE m.name = ?1",
                    params![name],
                    |r| {
                        let allowed_raw: String = r.get(3)?;
                        let allowed: Vec<String> =
                            serde_json::from_str(&allowed_raw).unwrap_or_default();
                        let hosts_raw: String = r.get(8)?;
                        Ok((
                            SecretMetadata {
                                name: r.get(0)?,
                                description: r.get(1)?,
                                key_version: r.get(2)?,
                                allowed_bindings: allowed,
                                allowed_hosts: serde_json::from_str(&hosts_raw).unwrap_or_default(),
                                credential_scheme: r.get(9)?,
                                created_at: r.get(4)?,
                                updated_at: r.get(5)?,
                            },
                            r.get::<_, Vec<u8>>(6)?,
                            r.get::<_, Vec<u8>>(7)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| StoreError::NotFound(format!("secret '{name}'")))?;
            let plaintext = self
                .vault
                .decrypt(&row.0.name, row.0.key_version, &row.1, &row.2)?;
            Ok((row.0, plaintext))
        })
    }

    // ---- State (CAS versioned) ----

    pub fn get_state(
        &self,
        scope: &str,
        owner: &str,
        state_key: &str,
    ) -> Result<Option<StateRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT scope, owner, state_key, schema_version, version, generation_id, encoded_value, updated_at
                 FROM state_values WHERE scope = ?1 AND owner = ?2 AND state_key = ?3",
                params![scope, owner, state_key],
                |r| {
                    let raw: String = r.get(6)?;
                    Ok(StateRecord {
                        scope: r.get(0)?,
                        owner: r.get(1)?,
                        state_key: r.get(2)?,
                        schema_version: r.get(3)?,
                        version: r.get(4)?,
                        generation_id: r.get(5)?,
                        value: serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
                        updated_at: r.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    pub fn list_state_for_owner(
        &self,
        scope: &str,
        owner: &str,
    ) -> Result<Vec<StateRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT scope, owner, state_key, schema_version, version, generation_id, encoded_value, updated_at
                 FROM state_values WHERE scope = ?1 AND owner = ?2 ORDER BY state_key ASC",
            )?;
            let rows = stmt.query_map(params![scope, owner], |r| {
                let raw: String = r.get(6)?;
                Ok(StateRecord {
                    scope: r.get(0)?,
                    owner: r.get(1)?,
                    state_key: r.get(2)?,
                    schema_version: r.get(3)?,
                    version: r.get(4)?,
                    generation_id: r.get(5)?,
                    value: serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
                    updated_at: r.get(7)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    /// List every State record of one scope (e.g. all Dashboard State, for migration planning).
    pub fn list_state_for_scope(&self, scope: &str) -> Result<Vec<StateRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT scope, owner, state_key, schema_version, version, generation_id, encoded_value, updated_at
                 FROM state_values WHERE scope = ?1 ORDER BY owner ASC, state_key ASC",
            )?;
            let rows = stmt.query_map(params![scope], |r| {
                let raw: String = r.get(6)?;
                Ok(StateRecord {
                    scope: r.get(0)?,
                    owner: r.get(1)?,
                    state_key: r.get(2)?,
                    schema_version: r.get(3)?,
                    version: r.get(4)?,
                    generation_id: r.get(5)?,
                    value: serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
                    updated_at: r.get(7)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    /// Compare-and-swap state update. Pass `expected_version: None` to upsert unconditionally.
    /// Values larger than [`DEFAULT_STATE_MAX_BYTES`] are rejected.
    #[allow(clippy::too_many_arguments)]
    pub fn set_state(
        &self,
        scope: &str,
        owner: &str,
        state_key: &str,
        schema_version: i64,
        generation_id: i64,
        value: serde_json::Value,
        expected_version: Option<i64>,
    ) -> Result<StateRecord, StoreError> {
        self.set_state_with_limit(
            scope,
            owner,
            state_key,
            schema_version,
            generation_id,
            value,
            expected_version,
            DEFAULT_STATE_MAX_BYTES,
        )
    }

    /// [`Self::set_state`] with an explicit per-record size limit (capped at
    /// [`HARD_STATE_MAX_BYTES`]).
    #[allow(clippy::too_many_arguments)]
    pub fn set_state_with_limit(
        &self,
        scope: &str,
        owner: &str,
        state_key: &str,
        schema_version: i64,
        generation_id: i64,
        value: serde_json::Value,
        expected_version: Option<i64>,
        max_bytes: usize,
    ) -> Result<StateRecord, StoreError> {
        let now = Utc::now().to_rfc3339();
        let encoded = serde_json::to_string(&value)?;
        let limit = max_bytes.min(HARD_STATE_MAX_BYTES);
        if encoded.len() > limit {
            return Err(StoreError::StateTooLarge {
                size: encoded.len(),
                limit,
            });
        }
        self.with_conn_sync(|conn| {
            let tx = conn.unchecked_transaction()?;
            let current: Option<i64> = tx
                .query_row(
                    "SELECT version FROM state_values WHERE scope = ?1 AND owner = ?2 AND state_key = ?3",
                    params![scope, owner, state_key],
                    |r| r.get(0),
                )
                .optional()?;

            let next_version = match (current, expected_version) {
                (None, Some(exp)) if exp != 0 => {
                    return Err(StoreError::CasConflict {
                        expected: exp,
                        actual: 0,
                    });
                }
                (Some(actual), Some(exp)) if actual != exp => {
                    return Err(StoreError::CasConflict {
                        expected: exp,
                        actual,
                    });
                }
                (Some(actual), _) => actual + 1,
                (None, _) => 1,
            };

            tx.execute(
                "INSERT INTO state_values (scope, owner, state_key, schema_version, version, generation_id, encoded_value, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(scope, owner, state_key) DO UPDATE SET
                    schema_version = excluded.schema_version,
                    version = excluded.version,
                    generation_id = excluded.generation_id,
                    encoded_value = excluded.encoded_value,
                    updated_at = excluded.updated_at",
                params![
                    scope,
                    owner,
                    state_key,
                    schema_version,
                    next_version,
                    generation_id,
                    encoded,
                    now
                ],
            )?;
            tx.commit()?;
            Ok(StateRecord {
                scope: scope.to_string(),
                owner: owner.to_string(),
                state_key: state_key.to_string(),
                schema_version,
                version: next_version,
                generation_id,
                value,
                updated_at: now,
            })
        })
    }

    /// Insert a State record only if none exists (atomic; never overwrites a concurrent write).
    /// Returns the inserted record, or `None` when a record was already present.
    pub fn insert_state_if_absent(
        &self,
        scope: &str,
        owner: &str,
        state_key: &str,
        schema_version: i64,
        generation_id: i64,
        value: serde_json::Value,
    ) -> Result<Option<StateRecord>, StoreError> {
        let now = Utc::now().to_rfc3339();
        let encoded = serde_json::to_string(&value)?;
        if encoded.len() > HARD_STATE_MAX_BYTES {
            return Err(StoreError::StateTooLarge {
                size: encoded.len(),
                limit: HARD_STATE_MAX_BYTES,
            });
        }
        self.with_conn_sync(|conn| {
            let inserted = conn.execute(
                "INSERT INTO state_values (scope, owner, state_key, schema_version, version, generation_id, encoded_value, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, ?7)
                 ON CONFLICT(scope, owner, state_key) DO NOTHING",
                params![scope, owner, state_key, schema_version, generation_id, encoded, now],
            )?;
            if inserted == 0 {
                return Ok(None);
            }
            Ok(Some(StateRecord {
                scope: scope.to_string(),
                owner: owner.to_string(),
                state_key: state_key.to_string(),
                schema_version,
                version: 1,
                generation_id,
                value,
                updated_at: now,
            }))
        })
    }

    /// Apply State migration steps in ONE SQLite transaction: every step is version-checked
    /// (CAS) and either all steps commit or none do, leaving live State untouched (§8).
    /// Returns the resulting records (`None` for removed records), in step order.
    pub fn apply_state_migrations(
        &self,
        steps: &[StateMigrationStep],
    ) -> Result<Vec<Option<StateRecord>>, StoreError> {
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            let tx = conn.unchecked_transaction()?;
            let mut out = Vec::with_capacity(steps.len());
            for step in steps {
                let current: Option<i64> = tx
                    .query_row(
                        "SELECT version FROM state_values WHERE scope = ?1 AND owner = ?2 AND state_key = ?3",
                        params![step.scope, step.owner, step.state_key],
                        |r| r.get(0),
                    )
                    .optional()?;
                let actual = current.unwrap_or(0);
                if actual != step.expected_version {
                    // Dropping `tx` without commit rolls back every earlier step.
                    return Err(StoreError::CasConflict {
                        expected: step.expected_version,
                        actual,
                    });
                }
                match &step.new_value {
                    Some(value) => {
                        let encoded = serde_json::to_string(value)?;
                        if encoded.len() > HARD_STATE_MAX_BYTES {
                            return Err(StoreError::StateTooLarge {
                                size: encoded.len(),
                                limit: HARD_STATE_MAX_BYTES,
                            });
                        }
                        let next_version = actual + 1;
                        tx.execute(
                            "INSERT INTO state_values (scope, owner, state_key, schema_version, version, generation_id, encoded_value, updated_at)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                             ON CONFLICT(scope, owner, state_key) DO UPDATE SET
                                schema_version = excluded.schema_version,
                                version = excluded.version,
                                generation_id = excluded.generation_id,
                                encoded_value = excluded.encoded_value,
                                updated_at = excluded.updated_at",
                            params![
                                step.scope,
                                step.owner,
                                step.state_key,
                                step.new_schema_version,
                                next_version,
                                step.generation_id,
                                encoded,
                                now
                            ],
                        )?;
                        out.push(Some(StateRecord {
                            scope: step.scope.clone(),
                            owner: step.owner.clone(),
                            state_key: step.state_key.clone(),
                            schema_version: step.new_schema_version,
                            version: next_version,
                            generation_id: step.generation_id,
                            value: value.clone(),
                            updated_at: now.clone(),
                        }));
                    }
                    None => {
                        tx.execute(
                            "DELETE FROM state_values WHERE scope = ?1 AND owner = ?2 AND state_key = ?3",
                            params![step.scope, step.owner, step.state_key],
                        )?;
                        out.push(None);
                    }
                }
            }
            tx.commit()?;
            Ok(out)
        })
    }

    // ---- Events, Checkpoints, DLQ ----

    pub fn append_event(
        &self,
        source_instance: &str,
        semantic_type: &str,
        schema_version: &str,
        trace_id: &str,
        durable: bool,
        payload: serde_json::Value,
    ) -> Result<DurableEventRecord, StoreError> {
        self.append_event_min_sequence(
            source_instance,
            semantic_type,
            schema_version,
            trace_id,
            durable,
            payload,
            1,
        )
    }

    /// Append an Event whose source-local sequence is `max(MAX(sequence) + 1, min_sequence)`.
    /// The EventBus uses `min_sequence` to keep ephemeral and durable Events of one source in a
    /// single monotonic sequence space (§7.2); the durable log may therefore contain gaps.
    #[allow(clippy::too_many_arguments)]
    pub fn append_event_min_sequence(
        &self,
        source_instance: &str,
        semantic_type: &str,
        schema_version: &str,
        trace_id: &str,
        durable: bool,
        payload: serde_json::Value,
        min_sequence: i64,
    ) -> Result<DurableEventRecord, StoreError> {
        let safe_payload = self.redactor().redact_json(&payload);
        let payload_json = serde_json::to_string(&safe_payload)?;
        let event_id = format!("evt_{}", Uuid::now_v7().simple());
        let emitted_at = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            let tx = conn.unchecked_transaction()?;
            let max_seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(sequence), 0) FROM events WHERE source_instance = ?1",
                params![source_instance],
                |r| r.get(0),
            )?;
            let next_seq = (max_seq + 1).max(min_sequence);
            tx.execute(
                "INSERT INTO events (event_id, source_instance, sequence, emitted_at, semantic_type, schema_version, trace_id, durable, payload_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    event_id,
                    source_instance,
                    next_seq,
                    emitted_at,
                    semantic_type,
                    schema_version,
                    trace_id,
                    if durable { 1_i64 } else { 0_i64 },
                    payload_json
                ],
            )?;
            tx.commit()?;
            Ok(DurableEventRecord {
                event_id,
                source_instance: source_instance.to_string(),
                sequence: next_seq,
                emitted_at,
                semantic_type: semantic_type.to_string(),
                schema_version: schema_version.to_string(),
                trace_id: trace_id.to_string(),
                durable,
                payload: safe_payload,
            })
        })
    }

    pub fn list_events_since(
        &self,
        source_instance: &str,
        after_sequence: i64,
    ) -> Result<Vec<DurableEventRecord>, StoreError> {
        self.list_events_since_limited(source_instance, after_sequence, usize::MAX)
    }

    /// Read at most `limit` durable Events of one source after `after_sequence`, in sequence order.
    pub fn list_events_since_limited(
        &self,
        source_instance: &str,
        after_sequence: i64,
        limit: usize,
    ) -> Result<Vec<DurableEventRecord>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT event_id, source_instance, sequence, emitted_at, semantic_type, schema_version, trace_id, durable, payload_json
                 FROM events WHERE source_instance = ?1 AND sequence > ?2 ORDER BY sequence ASC LIMIT ?3",
            )?;
            let rows = stmt.query_map(
                params![source_instance, after_sequence, limit],
                Self::map_event_row,
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    fn map_event_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<DurableEventRecord> {
        let raw: String = r.get(8)?;
        Ok(DurableEventRecord {
            event_id: r.get(0)?,
            source_instance: r.get(1)?,
            sequence: r.get(2)?,
            emitted_at: r.get(3)?,
            semantic_type: r.get(4)?,
            schema_version: r.get(5)?,
            trace_id: r.get(6)?,
            durable: r.get::<_, i64>(7)? != 0,
            payload: serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
        })
    }

    pub fn get_event(&self, event_id: &str) -> Result<Option<DurableEventRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT event_id, source_instance, sequence, emitted_at, semantic_type, schema_version, trace_id, durable, payload_json
                 FROM events WHERE event_id = ?1",
                params![event_id],
                Self::map_event_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// Highest durable sequence of one source (0 when it has no Events).
    pub fn max_event_sequence(&self, source_instance: &str) -> Result<i64, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT COALESCE(MAX(sequence), 0) FROM events WHERE source_instance = ?1",
                params![source_instance],
                |r| r.get(0),
            )
            .map_err(StoreError::from)
        })
    }

    /// Highest durable sequence of one source among Events emitted strictly before `cutoff`
    /// (RFC 3339). Used to start a newly registered consumer at the log head (§7.3 catch-up: skip).
    pub fn max_event_sequence_before(
        &self,
        source_instance: &str,
        cutoff: &str,
    ) -> Result<i64, StoreError> {
        let cutoff_dt = DateTime::parse_from_rfc3339(cutoff)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT sequence, emitted_at FROM events WHERE source_instance = ?1 ORDER BY sequence DESC",
            )?;
            let rows = stmt.query_map(params![source_instance], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (seq, at) = row?;
                let before = DateTime::parse_from_rfc3339(&at)
                    .map(|d| d.with_timezone(&Utc) < cutoff_dt)
                    .unwrap_or(true);
                if before {
                    return Ok(seq);
                }
            }
            Ok(0)
        })
    }

    /// Distinct source Instances that have durable Events in the log.
    pub fn list_event_sources(&self) -> Result<Vec<String>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt =
                conn.prepare("SELECT DISTINCT source_instance FROM events ORDER BY source_instance")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    /// Retention (§7.2): delete durable Events emitted before `cutoff` (RFC 3339) that every
    /// registered consumer of their source has acknowledged and that no unresolved dead letter
    /// still references, together with finished automation execution records older than
    /// `cutoff` whose Event is gone. Returns the number of deleted Events.
    pub fn prune_events_before(&self, cutoff: &str) -> Result<usize, StoreError> {
        let cutoff_dt = DateTime::parse_from_rfc3339(cutoff)
            .map(|d| d.with_timezone(&Utc))
            .map_err(|e| StoreError::NotFound(format!("invalid retention cutoff '{cutoff}': {e}")))?;
        self.with_conn_sync(|conn| {
            let candidates: Vec<(String, String)> = {
                let mut stmt = conn.prepare(
                    "SELECT e.event_id, e.emitted_at FROM events e
                     WHERE e.sequence <= COALESCE(
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
                let old = DateTime::parse_from_rfc3339(&at)
                    .map(|d| d.with_timezone(&Utc) < cutoff_dt)
                    .unwrap_or(false);
                if old {
                    deleted += tx.execute("DELETE FROM events WHERE event_id = ?1", params![event_id])?;
                }
            }
            // Automation execution records older than the cutoff whose Event is gone (or that had
            // no Event) can no longer guard a replay; drop them with the same retention.
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
                let old = DateTime::parse_from_rfc3339(&at)
                    .map(|d| d.with_timezone(&Utc) < cutoff_dt)
                    .unwrap_or(false);
                if old {
                    tx.execute(
                        "DELETE FROM automation_executions WHERE execution_id = ?1",
                        params![execution_id],
                    )?;
                }
            }
            tx.commit()?;
            Ok(deleted)
        })
    }

    pub fn get_consumer_checkpoint(
        &self,
        consumer_id: &str,
        source_instance: &str,
    ) -> Result<i64, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT last_sequence FROM event_consumer_checkpoints
                 WHERE consumer_id = ?1 AND source_instance = ?2",
                params![consumer_id, source_instance],
                |r| r.get(0),
            )
            .optional()
            .map(|v| v.unwrap_or(0))
            .map_err(StoreError::from)
        })
    }

    /// Consumer checkpoint, or `None` when the consumer was never registered for the source.
    pub fn find_consumer_checkpoint(
        &self,
        consumer_id: &str,
        source_instance: &str,
    ) -> Result<Option<i64>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT last_sequence FROM event_consumer_checkpoints
                 WHERE consumer_id = ?1 AND source_instance = ?2",
                params![consumer_id, source_instance],
                |r| r.get(0),
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// Advance a consumer checkpoint. Checkpoints never move backwards: acknowledging an older
    /// sequence (for example while replaying) keeps the higher value.
    pub fn set_consumer_checkpoint(
        &self,
        consumer_id: &str,
        source_instance: &str,
        last_sequence: i64,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO event_consumer_checkpoints (consumer_id, source_instance, last_sequence, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(consumer_id, source_instance) DO UPDATE SET
                    last_sequence = MAX(last_sequence, excluded.last_sequence),
                    updated_at = excluded.updated_at",
                params![consumer_id, source_instance, last_sequence, now],
            )?;
            Ok(())
        })
    }

    pub fn record_dead_letter(
        &self,
        consumer_id: &str,
        event_id: &str,
        attempts: i64,
        last_error: &str,
    ) -> Result<String, StoreError> {
        let dlq_id = format!("dlq_{}", Uuid::now_v7().simple());
        let failed_at = Utc::now().to_rfc3339();
        let safe_err = self.redactor().redact_str(last_error);
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO dead_letters (dlq_id, consumer_id, event_id, attempts, last_error, failed_at, resolved)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
                params![dlq_id, consumer_id, event_id, attempts, safe_err, failed_at],
            )?;
            Ok(dlq_id)
        })
    }

    fn map_dead_letter_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<DeadLetterRecord> {
        Ok(DeadLetterRecord {
            dlq_id: r.get(0)?,
            consumer_id: r.get(1)?,
            event_id: r.get(2)?,
            attempts: r.get(3)?,
            last_error: r.get(4)?,
            failed_at: r.get(5)?,
            resolved: r.get::<_, i64>(6)? != 0,
        })
    }

    pub fn list_dead_letters(
        &self,
        include_resolved: bool,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT dlq_id, consumer_id, event_id, attempts, last_error, failed_at, resolved
                 FROM dead_letters WHERE (?1 = 1 OR resolved = 0)
                 ORDER BY failed_at DESC LIMIT ?2",
            )?;
            let rows = stmt.query_map(
                params![if include_resolved { 1_i64 } else { 0_i64 }, limit],
                Self::map_dead_letter_row,
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn get_dead_letter(&self, dlq_id: &str) -> Result<Option<DeadLetterRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT dlq_id, consumer_id, event_id, attempts, last_error, failed_at, resolved
                 FROM dead_letters WHERE dlq_id = ?1",
                params![dlq_id],
                Self::map_dead_letter_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// Mark a dead letter resolved. Returns false when it does not exist or was already resolved.
    pub fn resolve_dead_letter(&self, dlq_id: &str) -> Result<bool, StoreError> {
        self.with_conn_sync(|conn| {
            let n = conn.execute(
                "UPDATE dead_letters SET resolved = 1 WHERE dlq_id = ?1 AND resolved = 0",
                params![dlq_id],
            )?;
            Ok(n > 0)
        })
    }

    // ---- Automation executions & trigger state (§7.3) ----

    pub fn insert_automation_execution(
        &self,
        rec: &AutomationExecutionRecord,
    ) -> Result<(), StoreError> {
        let result_json = rec
            .result
            .as_ref()
            .map(|v| serde_json::to_string(&self.redactor().redact_json(v)))
            .transpose()?;
        let actions_json = serde_json::to_string(&rec.action_executions)?;
        let safe_err = rec.error.as_deref().map(|e| self.redactor().redact_str(e));
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO automation_executions (execution_id, automation_id, trigger_kind, trigger_ref, event_id, run_as, status, attempts, error, result_json, action_executions_json, trace_id, started_at, finished_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    rec.execution_id,
                    rec.automation_id,
                    rec.trigger_kind,
                    rec.trigger_ref,
                    rec.event_id,
                    rec.run_as,
                    rec.status,
                    rec.attempts,
                    safe_err,
                    result_json,
                    actions_json,
                    rec.trace_id,
                    rec.started_at,
                    rec.finished_at
                ],
            )?;
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finish_automation_execution(
        &self,
        execution_id: &str,
        status: &str,
        attempts: i64,
        error: Option<&str>,
        result: Option<&serde_json::Value>,
        action_executions: &[String],
        finished_at: &str,
    ) -> Result<(), StoreError> {
        let result_json = result
            .map(|v| serde_json::to_string(&self.redactor().redact_json(v)))
            .transpose()?;
        let actions_json = serde_json::to_string(action_executions)?;
        let safe_err = error.map(|e| self.redactor().redact_str(e));
        self.with_conn_sync(|conn| {
            conn.execute(
                "UPDATE automation_executions SET status = ?2, attempts = ?3, error = ?4, result_json = ?5,
                    action_executions_json = ?6, finished_at = ?7
                 WHERE execution_id = ?1",
                params![execution_id, status, attempts, safe_err, result_json, actions_json, finished_at],
            )?;
            Ok(())
        })
    }

    fn map_automation_execution_row(
        r: &rusqlite::Row<'_>,
    ) -> rusqlite::Result<AutomationExecutionRecord> {
        let result_raw: Option<String> = r.get(9)?;
        let actions_raw: Option<String> = r.get(10)?;
        Ok(AutomationExecutionRecord {
            execution_id: r.get(0)?,
            automation_id: r.get(1)?,
            trigger_kind: r.get(2)?,
            trigger_ref: r.get(3)?,
            event_id: r.get(4)?,
            run_as: r.get(5)?,
            status: r.get(6)?,
            attempts: r.get(7)?,
            error: r.get(8)?,
            result: result_raw.and_then(|s| serde_json::from_str(&s).ok()),
            action_executions: actions_raw
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            trace_id: r.get(11)?,
            started_at: r.get(12)?,
            finished_at: r.get(13)?,
        })
    }

    const AUTOMATION_EXEC_COLUMNS: &'static str = "execution_id, automation_id, trigger_kind, trigger_ref, event_id, run_as, status, attempts, error, result_json, action_executions_json, trace_id, started_at, finished_at";

    /// Latest succeeded execution of `automation_id` for one trigger occurrence (replay guard).
    pub fn find_succeeded_automation_execution(
        &self,
        automation_id: &str,
        trigger_ref: &str,
    ) -> Result<Option<AutomationExecutionRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                &format!(
                    "SELECT {} FROM automation_executions
                     WHERE automation_id = ?1 AND trigger_ref = ?2 AND status = 'succeeded'
                     ORDER BY started_at DESC LIMIT 1",
                    Self::AUTOMATION_EXEC_COLUMNS
                ),
                params![automation_id, trigger_ref],
                Self::map_automation_execution_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    pub fn get_automation_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<AutomationExecutionRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                &format!(
                    "SELECT {} FROM automation_executions WHERE execution_id = ?1",
                    Self::AUTOMATION_EXEC_COLUMNS
                ),
                params![execution_id],
                Self::map_automation_execution_row,
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// Most recent automation executions, optionally for one automation Instance.
    pub fn list_automation_executions(
        &self,
        automation_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AutomationExecutionRecord>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {} FROM automation_executions
                 WHERE (?1 IS NULL OR automation_id = ?1)
                 ORDER BY started_at DESC LIMIT ?2",
                Self::AUTOMATION_EXEC_COLUMNS
            ))?;
            let rows = stmt.query_map(
                params![automation_id, limit],
                Self::map_automation_execution_row,
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn get_automation_trigger_state(
        &self,
        automation_id: &str,
        trigger_key: &str,
    ) -> Result<Option<AutomationTriggerState>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT automation_id, trigger_key, last_fired_at, last_fingerprint, updated_at
                 FROM automation_trigger_state WHERE automation_id = ?1 AND trigger_key = ?2",
                params![automation_id, trigger_key],
                |r| {
                    Ok(AutomationTriggerState {
                        automation_id: r.get(0)?,
                        trigger_key: r.get(1)?,
                        last_fired_at: r.get(2)?,
                        last_fingerprint: r.get(3)?,
                        updated_at: r.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    pub fn set_automation_trigger_state(
        &self,
        automation_id: &str,
        trigger_key: &str,
        last_fired_at: Option<&str>,
        last_fingerprint: Option<&str>,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO automation_trigger_state (automation_id, trigger_key, last_fired_at, last_fingerprint, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(automation_id, trigger_key) DO UPDATE SET
                    last_fired_at = excluded.last_fired_at,
                    last_fingerprint = excluded.last_fingerprint,
                    updated_at = excluded.updated_at",
                params![automation_id, trigger_key, last_fired_at, last_fingerprint, now],
            )?;
            Ok(())
        })
    }

    // ---- Action executions & idempotency ----

    pub fn lookup_action_idempotency(
        &self,
        action_instance: &str,
        idempotency_key: &str,
    ) -> Result<Option<ActionExecutionRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let exec_id: Option<String> = conn
                .query_row(
                    "SELECT execution_id FROM action_idempotency
                     WHERE idempotency_key = ?1 AND action_instance = ?2",
                    params![idempotency_key, action_instance],
                    |r| r.get(0),
                )
                .optional()?;
            match exec_id {
                Some(id) => Self::get_action_execution_conn(conn, &id),
                None => Ok(None),
            }
        })
    }

    #[deprecated(note = "does not claim the idempotency key atomically (a conflicting key is silently ignored); use claim_idempotency_and_insert")]
    pub fn insert_action_execution(
        &self,
        rec: &ActionExecutionRecord,
    ) -> Result<(), StoreError> {
        let safe_args = self.redactor().redact_json(&rec.args);
        let safe_res = rec.result.as_ref().map(|v| self.redactor().redact_json(v));
        let safe_reason = rec.reason.as_ref().map(|r| self.redactor().redact_str(r));
        let args_json = serde_json::to_string(&safe_args)?;
        let res_json = safe_res.as_ref().map(serde_json::to_string).transpose()?;
        self.with_conn_sync(|conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute(
                "INSERT INTO action_executions
                 (execution_id, action_instance, parent_execution_id, principal_id, principal_kind, status,
                  idempotency_key, args_json, result_json, reason, retry_safe, reconcile_attempts, trace_id,
                  started_at, finished_at, duration_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                params![
                    rec.execution_id,
                    rec.action_instance,
                    rec.parent_execution_id,
                    rec.principal_id,
                    rec.principal_kind,
                    rec.status,
                    rec.idempotency_key,
                    args_json,
                    res_json,
                    safe_reason,
                    if rec.retry_safe { 1_i64 } else { 0_i64 },
                    rec.reconcile_attempts,
                    rec.trace_id,
                    rec.started_at,
                    rec.finished_at,
                    rec.duration_ms
                ],
            )?;
            if let Some(ref key) = rec.idempotency_key {
                tx.execute(
                    "INSERT OR IGNORE INTO action_idempotency (idempotency_key, action_instance, execution_id, created_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![key, rec.action_instance, rec.execution_id, rec.started_at],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
    }

    #[deprecated(note = "unconditional update can overwrite a concurrent outcome; use transition_action_execution")]
    #[allow(clippy::too_many_arguments)]
    pub fn update_action_execution(
        &self,
        execution_id: &str,
        status: &str,
        result: Option<serde_json::Value>,
        reason: Option<&str>,
        retry_safe: bool,
        reconcile_attempts: i64,
        finished_at: Option<&str>,
        duration_ms: Option<i64>,
    ) -> Result<(), StoreError> {
        let safe_res = result.map(|v| self.redactor().redact_json(&v));
        let res_json = safe_res.as_ref().map(serde_json::to_string).transpose()?;
        let safe_reason = reason.map(|r| self.redactor().redact_str(r));
        self.with_conn_sync(|conn| {
            conn.execute(
                "UPDATE action_executions
                 SET status = ?1, result_json = ?2, reason = ?3, retry_safe = ?4,
                     reconcile_attempts = ?5, finished_at = ?6, duration_ms = ?7
                 WHERE execution_id = ?8",
                params![
                    status,
                    res_json,
                    safe_reason,
                    if retry_safe { 1_i64 } else { 0_i64 },
                    reconcile_attempts,
                    finished_at,
                    duration_ms,
                    execution_id
                ],
            )?;
            Ok(())
        })
    }

    pub fn link_action_step(
        &self,
        parent_execution_id: &str,
        child_execution_id: &str,
        step_order: i64,
    ) -> Result<(), StoreError> {
        let step_id = format!("step_{}", Uuid::now_v7().simple());
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO action_steps (step_id, execution_id, child_execution_id, step_order, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![step_id, parent_execution_id, child_execution_id, step_order, now],
            )?;
            Ok(())
        })
    }

    pub fn get_action_execution(&self, execution_id: &str) -> Result<Option<ActionExecutionRecord>, StoreError> {
        self.with_conn_sync(|conn| Self::get_action_execution_conn(conn, execution_id))
    }

    pub fn list_action_executions(&self, limit: usize) -> Result<Vec<ActionExecutionRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT execution_id, action_instance, parent_execution_id, principal_id, principal_kind,
                        status, idempotency_key, args_json, result_json, reason, retry_safe,
                        reconcile_attempts, trace_id, started_at, finished_at, duration_ms
                 FROM action_executions ORDER BY started_at DESC LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![limit as i64], Self::map_action_row)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    #[deprecated(note = "marks never-started queued records unknown too; use recover_inflight_actions")]
    pub fn mark_inflight_actions_unknown(&self, reason: &str) -> Result<usize, StoreError> {
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            let n = conn.execute(
                "UPDATE action_executions SET status = 'unknown', retry_safe = 0, reason = ?1, finished_at = ?2
                 WHERE status IN ('queued', 'running')",
                params![reason, now],
            )?;
            Ok(n)
        })
    }

    /// Idempotent, additive schema for the action ledger (reconcile scheduling). Safe to run on every open.
    fn ensure_action_ledger_schema(conn: &Connection) -> Result<(), StoreError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS action_reconcile_schedule (
                execution_id TEXT PRIMARY KEY,
                next_attempt_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (execution_id) REFERENCES action_executions(execution_id) ON DELETE CASCADE
             );
             CREATE INDEX IF NOT EXISTS idx_action_reconcile_due
                ON action_reconcile_schedule(next_attempt_at);",
        )?;
        Ok(())
    }

    /// Atomically claim `rec.idempotency_key` (if any) and insert `rec` in one transaction (§9.2).
    ///
    /// * No key, or a free key: the record is inserted and the key now names it (`New`).
    /// * A key naming an execution with equal (redacted) arguments: nothing is inserted (`Existing`),
    ///   except when that execution ended `failed`/`cancelled` with `retry_safe = true` (its outcome is
    ///   proven to have had no external effect): then the key is atomically repointed to the new record.
    /// * A key naming an execution with different arguments: nothing is inserted (`ArgsMismatch`).
    pub fn claim_idempotency_and_insert(
        &self,
        rec: &ActionExecutionRecord,
    ) -> Result<IdempotencyClaim, StoreError> {
        let safe_args = self.redactor().redact_json(&rec.args);
        let safe_res = rec.result.as_ref().map(|v| self.redactor().redact_json(v));
        let safe_reason = rec.reason.as_ref().map(|r| self.redactor().redact_str(r));
        let args_json = serde_json::to_string(&safe_args)?;
        let res_json = safe_res.as_ref().map(serde_json::to_string).transpose()?;
        self.with_conn_sync(|conn| {
            let tx = conn.unchecked_transaction()?;
            let insert_exec = |tx: &rusqlite::Transaction<'_>| -> Result<(), StoreError> {
                tx.execute(
                    "INSERT INTO action_executions
                     (execution_id, action_instance, parent_execution_id, principal_id, principal_kind, status,
                      idempotency_key, args_json, result_json, reason, retry_safe, reconcile_attempts, trace_id,
                      started_at, finished_at, duration_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                    params![
                        rec.execution_id,
                        rec.action_instance,
                        rec.parent_execution_id,
                        rec.principal_id,
                        rec.principal_kind,
                        rec.status,
                        rec.idempotency_key,
                        args_json,
                        res_json,
                        safe_reason,
                        if rec.retry_safe { 1_i64 } else { 0_i64 },
                        rec.reconcile_attempts,
                        rec.trace_id,
                        rec.started_at,
                        rec.finished_at,
                        rec.duration_ms
                    ],
                )?;
                Ok(())
            };

            let Some(ref key) = rec.idempotency_key else {
                insert_exec(&tx)?;
                tx.commit()?;
                return Ok(IdempotencyClaim::New);
            };

            // Plain INSERT (never OR IGNORE): the PRIMARY KEY is the claim. A constraint violation
            // means another invocation already owns the key.
            let claimed = tx.execute(
                "INSERT INTO action_idempotency (idempotency_key, action_instance, execution_id, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![key, rec.action_instance, rec.execution_id, rec.started_at],
            );
            match claimed {
                Ok(_) => {
                    insert_exec(&tx)?;
                    tx.commit()?;
                    return Ok(IdempotencyClaim::New);
                }
                Err(rusqlite::Error::SqliteFailure(e, _))
                    if e.code == rusqlite::ErrorCode::ConstraintViolation => {}
                Err(e) => return Err(e.into()),
            }

            let owner_id: String = tx.query_row(
                "SELECT execution_id FROM action_idempotency
                 WHERE idempotency_key = ?1 AND action_instance = ?2",
                params![key, rec.action_instance],
                |r| r.get(0),
            )?;
            let existing = Self::get_action_execution_conn(&tx, &owner_id)?;
            let supersede = match existing {
                None => true, // dangling claim: repoint it
                Some(ref ex) => {
                    if ex.args != safe_args {
                        return Ok(IdempotencyClaim::ArgsMismatch(ex.clone()));
                    }
                    ex.retry_safe && (ex.status == "failed" || ex.status == "cancelled")
                }
            };
            if !supersede {
                return Ok(IdempotencyClaim::Existing(existing.expect("checked above")));
            }
            insert_exec(&tx)?;
            let n = tx.execute(
                "UPDATE action_idempotency SET execution_id = ?1, created_at = ?2
                 WHERE idempotency_key = ?3 AND action_instance = ?4 AND execution_id = ?5",
                params![rec.execution_id, rec.started_at, key, rec.action_instance, owner_id],
            )?;
            if n != 1 {
                // Lost a race to another superseding attempt; roll back our insert.
                drop(tx);
                return match self.lookup_action_idempotency_conn(conn, &rec.action_instance, key)? {
                    Some(ex) => Ok(IdempotencyClaim::Existing(ex)),
                    None => Err(StoreError::NotFound(format!("idempotency key '{key}'"))),
                };
            }
            tx.commit()?;
            Ok(IdempotencyClaim::New)
        })
    }

    fn lookup_action_idempotency_conn(
        &self,
        conn: &Connection,
        action_instance: &str,
        idempotency_key: &str,
    ) -> Result<Option<ActionExecutionRecord>, StoreError> {
        let exec_id: Option<String> = conn
            .query_row(
                "SELECT execution_id FROM action_idempotency
                 WHERE idempotency_key = ?1 AND action_instance = ?2",
                params![idempotency_key, action_instance],
                |r| r.get(0),
            )
            .optional()?;
        match exec_id {
            Some(id) => Self::get_action_execution_conn(conn, &id),
            None => Ok(None),
        }
    }

    /// Conditionally transition an execution: the UPDATE applies only while the current status is one of
    /// `expected_statuses`. Returns `true` when exactly one row changed (§9.1; prevents reconcile, manual
    /// resolution and cancellation from overwriting each other).
    pub fn transition_action_execution(
        &self,
        execution_id: &str,
        expected_statuses: &[&str],
        next: ActionTransition<'_>,
    ) -> Result<bool, StoreError> {
        if expected_statuses.is_empty() {
            return Ok(false);
        }
        let safe_res = next.result.map(|v| self.redactor().redact_json(&v));
        let res_json = safe_res.as_ref().map(serde_json::to_string).transpose()?;
        let safe_reason = next.reason.map(|r| self.redactor().redact_str(r));
        self.with_conn_sync(|conn| {
            let placeholders: Vec<String> =
                (0..expected_statuses.len()).map(|i| format!("?{}", i + 9)).collect();
            let sql = format!(
                "UPDATE action_executions
                 SET status = ?1, result_json = ?2, reason = ?3, retry_safe = ?4,
                     reconcile_attempts = ?5, finished_at = ?6, duration_ms = ?7
                 WHERE execution_id = ?8 AND status IN ({})",
                placeholders.join(", ")
            );
            let mut values: Vec<rusqlite::types::Value> = vec![
                next.status.to_string().into(),
                res_json.into(),
                safe_reason.into(),
                (if next.retry_safe { 1_i64 } else { 0_i64 }).into(),
                next.reconcile_attempts.into(),
                next.finished_at.map(|s| s.to_string()).into(),
                next.duration_ms.into(),
                execution_id.to_string().into(),
            ];
            values.extend(expected_statuses.iter().map(|s| rusqlite::types::Value::from(s.to_string())));
            let n = conn.execute(&sql, rusqlite::params_from_iter(values))?;
            Ok(n == 1)
        })
    }

    /// Crash/shutdown recovery for in-flight executions (§15.2, §14): `running` records become `unknown`
    /// (the outcome cannot be proven), `queued` records — whose handler never started — become `failed`
    /// with `retry_safe = true`. Only call this when no executions are in flight in this process.
    pub fn recover_inflight_actions(&self, reason: &str) -> Result<InflightRecovery, StoreError> {
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            let tx = conn.unchecked_transaction()?;
            let collect = |status: &str| -> Result<Vec<String>, StoreError> {
                let mut stmt =
                    tx.prepare("SELECT execution_id FROM action_executions WHERE status = ?1")?;
                let rows = stmt.query_map(params![status], |r| r.get::<_, String>(0))?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(row?);
                }
                Ok(out)
            };
            let running = collect("running")?;
            let queued = collect("queued")?;
            tx.execute(
                "UPDATE action_executions SET status = 'unknown', retry_safe = 0, reason = ?1, finished_at = ?2
                 WHERE status = 'running'",
                params![reason, now],
            )?;
            tx.execute(
                "UPDATE action_executions SET status = 'failed', retry_safe = 1, reason = ?1, finished_at = ?2
                 WHERE status = 'queued'",
                params![format!("{reason}: handler never started"), now],
            )?;
            tx.commit()?;
            Ok(InflightRecovery {
                marked_unknown: running,
                marked_not_started: queued,
            })
        })
    }

    /// Schedule (or reschedule) the next host-driven `reconcile` attempt for an `unknown` execution.
    pub fn schedule_action_reconcile(
        &self,
        execution_id: &str,
        next_attempt_at: &str,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO action_reconcile_schedule (execution_id, next_attempt_at, updated_at)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(execution_id) DO UPDATE SET next_attempt_at = excluded.next_attempt_at,
                                                         updated_at = excluded.updated_at",
                params![execution_id, next_attempt_at, now],
            )?;
            Ok(())
        })
    }

    pub fn clear_action_reconcile(&self, execution_id: &str) -> Result<(), StoreError> {
        self.with_conn_sync(|conn| {
            conn.execute(
                "DELETE FROM action_reconcile_schedule WHERE execution_id = ?1",
                params![execution_id],
            )?;
            Ok(())
        })
    }

    pub fn get_action_reconcile_schedule(&self, execution_id: &str) -> Result<Option<String>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT next_attempt_at FROM action_reconcile_schedule WHERE execution_id = ?1",
                params![execution_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    /// `unknown` executions whose scheduled reconcile attempt is due at or before `now_rfc3339`.
    pub fn list_due_action_reconciles(
        &self,
        now_rfc3339: &str,
        limit: usize,
    ) -> Result<Vec<String>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT s.execution_id FROM action_reconcile_schedule s
                 JOIN action_executions e ON e.execution_id = s.execution_id
                 WHERE e.status = 'unknown' AND s.next_attempt_at <= ?1
                 ORDER BY s.next_attempt_at ASC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![now_rfc3339, limit as i64], |r| r.get::<_, String>(0))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    /// `unknown` executions that have no reconcile schedule row (e.g. left by an older process).
    pub fn list_unscheduled_unknown_executions(&self, limit: usize) -> Result<Vec<ActionExecutionRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT execution_id, action_instance, parent_execution_id, principal_id, principal_kind,
                        status, idempotency_key, args_json, result_json, reason, retry_safe,
                        reconcile_attempts, trace_id, started_at, finished_at, duration_ms
                 FROM action_executions e
                 WHERE e.status = 'unknown'
                   AND NOT EXISTS (SELECT 1 FROM action_reconcile_schedule s WHERE s.execution_id = e.execution_id)
                 ORDER BY started_at ASC LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![limit as i64], Self::map_action_row)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    /// Child executions of a composed action, in step order, with their current status (§9.2).
    pub fn list_action_step_children(
        &self,
        parent_execution_id: &str,
    ) -> Result<Vec<(String, String)>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT s.child_execution_id, e.status FROM action_steps s
                 JOIN action_executions e ON e.execution_id = s.child_execution_id
                 WHERE s.execution_id = ?1 ORDER BY s.step_order ASC",
            )?;
            let rows = stmt.query_map(params![parent_execution_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    fn get_action_execution_conn(
        conn: &Connection,
        execution_id: &str,
    ) -> Result<Option<ActionExecutionRecord>, StoreError> {
        conn.query_row(
            "SELECT execution_id, action_instance, parent_execution_id, principal_id, principal_kind,
                    status, idempotency_key, args_json, result_json, reason, retry_safe,
                    reconcile_attempts, trace_id, started_at, finished_at, duration_ms
             FROM action_executions WHERE execution_id = ?1",
            params![execution_id],
            Self::map_action_row,
        )
        .optional()
        .map_err(StoreError::from)
    }

    fn map_action_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ActionExecutionRecord> {
        let args_raw: String = r.get(7)?;
        let res_raw: Option<String> = r.get(8)?;
        Ok(ActionExecutionRecord {
            execution_id: r.get(0)?,
            action_instance: r.get(1)?,
            parent_execution_id: r.get(2)?,
            principal_id: r.get(3)?,
            principal_kind: r.get(4)?,
            status: r.get(5)?,
            idempotency_key: r.get(6)?,
            args: serde_json::from_str(&args_raw).unwrap_or(serde_json::Value::Null),
            result: res_raw.and_then(|s| serde_json::from_str(&s).ok()),
            reason: r.get(9)?,
            retry_safe: r.get::<_, i64>(10)? != 0,
            reconcile_attempts: r.get(11)?,
            trace_id: r.get(12)?,
            started_at: r.get(13)?,
            finished_at: r.get(14)?,
            duration_ms: r.get(15)?,
        })
    }

    // ---- Audit & Telemetry ----

    pub fn append_audit(
        &self,
        principal_id: &str,
        principal_kind: &str,
        operation: &str,
        target: &str,
        outcome: &str,
        detail: serde_json::Value,
    ) -> Result<String, StoreError> {
        let audit_id = format!("aud_{}", Uuid::now_v7().simple());
        let ts = Utc::now().to_rfc3339();
        let safe_detail = self.redactor().redact_json(&detail);
        let detail_json = serde_json::to_string(&safe_detail)?;
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO audit_records (audit_id, timestamp, principal_id, principal_kind, operation, target, outcome, detail_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![audit_id, ts, principal_id, principal_kind, operation, target, outcome, detail_json],
            )?;
            Ok(audit_id)
        })
    }

    /// Distinct `(principal_id, principal_kind)` pairs that recorded one of `operations` on
    /// `target` (e.g. every Principal that edited a Draft, for activation-time authority checks).
    pub fn list_audit_principals_for_target(
        &self,
        target: &str,
        operations: &[&str],
    ) -> Result<Vec<(String, String)>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT principal_id, principal_kind, operation FROM audit_records WHERE target = ?1",
            )?;
            let rows = stmt.query_map(params![target], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            let mut out: Vec<(String, String)> = Vec::new();
            for row in rows {
                let (id, kind, op) = row?;
                if operations.contains(&op.as_str())
                    && !out.iter().any(|(i, k)| i == &id && k == &kind)
                {
                    out.push((id, kind));
                }
            }
            Ok(out)
        })
    }

    pub fn list_audits(&self, limit: usize) -> Result<Vec<AuditRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT audit_id, timestamp, principal_id, principal_kind, operation, target, outcome, detail_json
                 FROM audit_records ORDER BY timestamp DESC LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![limit as i64], |r| {
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

    pub fn record_trace_span(&self, span: &TraceSpanRecord) -> Result<(), StoreError> {
        let safe_attrs = self.redactor().redact_json(&span.attributes);
        let attrs_json = serde_json::to_string(&safe_attrs)?;
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO trace_spans (span_id, trace_id, parent_span_id, name, principal_id, attributes_json, status, started_at, duration_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    span.span_id,
                    span.trace_id,
                    span.parent_span_id,
                    span.name,
                    span.principal_id,
                    attrs_json,
                    span.status,
                    span.started_at,
                    span.duration_ms
                ],
            )?;
            Ok(())
        })
    }

    pub fn list_trace_spans(&self, limit: usize) -> Result<Vec<TraceSpanRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT span_id, trace_id, parent_span_id, name, principal_id, attributes_json, status, started_at, duration_ms
                 FROM trace_spans ORDER BY started_at DESC LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![limit as i64], |r| {
                let raw: String = r.get(5)?;
                Ok(TraceSpanRecord {
                    span_id: r.get(0)?,
                    trace_id: r.get(1)?,
                    parent_span_id: r.get(2)?,
                    name: r.get(3)?,
                    principal_id: r.get(4)?,
                    attributes: serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
                    status: r.get(6)?,
                    started_at: r.get(7)?,
                    duration_ms: r.get(8)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    // ---- Principals, Agents, Service Principals, Admin ----

    pub fn set_service_principal_grant(
        &self,
        principal_id: &str,
        granted_ceiling: serde_json::Value,
        granted_by: &str,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        let json = serde_json::to_string(&granted_ceiling)?;
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO service_principal_grants (principal_id, granted_ceiling_json, granted_by, granted_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(principal_id) DO UPDATE SET
                    granted_ceiling_json = excluded.granted_ceiling_json,
                    granted_by = excluded.granted_by,
                    granted_at = excluded.granted_at",
                params![principal_id, json, granted_by, now],
            )?;
            Ok(())
        })
    }

    /// Set the Administrator network ceiling (stored outside the Git Workspace, §11.3).
    pub fn set_network_policy_ceiling(
        &self,
        policy: serde_json::Value,
        granted_by: &str,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        let json = serde_json::to_string(&policy)?;
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO network_policy_ceiling (id, policy_json, granted_by, granted_at)
                 VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET
                    policy_json = excluded.policy_json,
                    granted_by = excluded.granted_by,
                    granted_at = excluded.granted_at",
                params![json, granted_by, now],
            )?;
            Ok(())
        })
    }

    /// Returns `(policy, granted_by, granted_at)` when the Administrator has set a network ceiling.
    pub fn get_network_policy_ceiling(
        &self,
    ) -> Result<Option<(serde_json::Value, String, String)>, StoreError> {
        self.with_conn_sync(|conn| {
            let row: Option<(String, String, String)> = conn
                .query_row(
                    "SELECT policy_json, granted_by, granted_at FROM network_policy_ceiling WHERE id = 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            Ok(row.and_then(|(p, by, at)| serde_json::from_str(&p).ok().map(|v| (v, by, at))))
        })
    }

    pub fn get_service_principal_grant(
        &self,
        principal_id: &str,
    ) -> Result<Option<serde_json::Value>, StoreError> {
        self.with_conn_sync(|conn| {
            let raw: Option<String> = conn
                .query_row(
                    "SELECT granted_ceiling_json FROM service_principal_grants WHERE principal_id = ?1",
                    params![principal_id],
                    |r| r.get(0),
                )
                .optional()?;
            Ok(raw.and_then(|s| serde_json::from_str(&s).ok()))
        })
    }

    pub fn list_service_principal_grants(
        &self,
    ) -> Result<Vec<(String, serde_json::Value, String, String)>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT principal_id, granted_ceiling_json, granted_by, granted_at
                 FROM service_principal_grants ORDER BY principal_id ASC",
            )?;
            let rows = stmt.query_map([], |r| {
                let raw: String = r.get(1)?;
                Ok((
                    r.get(0)?,
                    serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
                    r.get(2)?,
                    r.get(3)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn create_agent_principal(
        &self,
        name: &str,
        token_plaintext: &str,
        ceiling: serde_json::Value,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<AgentPrincipalRecord, StoreError> {
        // Server-generated plaintext: safe to register for redaction (bounded token tier).
        self.redactor().register_token(token_plaintext);
        let agent_id = format!("agt_{}", Uuid::now_v7().simple());
        let token_hash = hash_token(token_plaintext);
        let ceiling_json = serde_json::to_string(&ceiling)?;
        let created_at = Utc::now().to_rfc3339();
        let exp = expires_at.map(|d| d.to_rfc3339());
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO agent_principals (agent_id, name, token_hash, ceiling_json, expires_at, revoked_at, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6)",
                params![agent_id, name, token_hash, ceiling_json, exp, created_at],
            )?;
            Ok(AgentPrincipalRecord {
                agent_id,
                name: name.to_string(),
                ceiling,
                expires_at: exp,
                revoked_at: None,
                created_at,
            })
        })
    }

    pub fn authenticate_agent_token(
        &self,
        token_plaintext: &str,
    ) -> Result<Option<AgentPrincipalRecord>, StoreError> {
        // NEVER register caller-supplied input before verification (redaction poisoning, §14.3).
        let token_hash = hash_token(token_plaintext);
        let verified = self.with_conn_sync(|conn| {
            let rec = conn
                .query_row(
                    "SELECT agent_id, name, ceiling_json, expires_at, revoked_at, created_at
                     FROM agent_principals WHERE token_hash = ?1",
                    params![token_hash],
                    |r| {
                        let raw: String = r.get(2)?;
                        Ok(AgentPrincipalRecord {
                            agent_id: r.get(0)?,
                            name: r.get(1)?,
                            ceiling: serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
                            expires_at: r.get(3)?,
                            revoked_at: r.get(4)?,
                            created_at: r.get(5)?,
                        })
                    },
                )
                .optional()?;
            if let Some(ref a) = rec {
                if a.revoked_at.is_some() {
                    return Ok(None);
                }
                if let Some(ref exp) = a.expires_at {
                    match DateTime::parse_from_rfc3339(exp) {
                        Ok(dt) if dt.with_timezone(&Utc) > Utc::now() => {}
                        // Expired or unparseable expiry: fail closed.
                        _ => return Ok(None),
                    }
                }
            }
            Ok(rec)
        })?;
        if verified.is_some() {
            // Verified, server-issued token (e.g. after a restart): register for redaction.
            self.redactor().register_token(token_plaintext);
        }
        Ok(verified)
    }

    pub fn revoke_agent_principal(&self, agent_id: &str) -> Result<bool, StoreError> {
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            let n = conn.execute(
                "UPDATE agent_principals SET revoked_at = ?1 WHERE agent_id = ?2 AND revoked_at IS NULL",
                params![now, agent_id],
            )?;
            Ok(n > 0)
        })
    }

    pub fn list_agent_principals(&self) -> Result<Vec<AgentPrincipalRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT agent_id, name, ceiling_json, expires_at, revoked_at, created_at
                 FROM agent_principals ORDER BY created_at DESC",
            )?;
            let rows = stmt.query_map([], |r| {
                let raw: String = r.get(2)?;
                Ok(AgentPrincipalRecord {
                    agent_id: r.get(0)?,
                    name: r.get(1)?,
                    ceiling: serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null),
                    expires_at: r.get(3)?,
                    revoked_at: r.get(4)?,
                    created_at: r.get(5)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    // ---- Admin Credentials & Sessions ----

    pub fn set_admin_credentials(&self, username: &str, password_hash: &str) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO admin_credentials (id, username, password_hash, updated_at)
                 VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET
                    username = excluded.username,
                    password_hash = excluded.password_hash,
                    updated_at = excluded.updated_at",
                params![username, password_hash, now],
            )?;
            Ok(())
        })
    }

    pub fn get_admin_credentials(&self) -> Result<Option<(String, String)>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT username, password_hash FROM admin_credentials WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    pub fn create_session(
        &self,
        token_plaintext: &str,
        csrf_token: &str,
        principal_id: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<String, StoreError> {
        // Server-generated session token: safe to register (bounded token tier).
        self.redactor().register_token(token_plaintext);
        let session_id = format!("ses_{}", Uuid::now_v7().simple());
        let token_hash = hash_token(token_plaintext);
        let created_at = Utc::now().to_rfc3339();
        let exp = expires_at.to_rfc3339();
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO sessions (session_id, token_hash, csrf_token, principal_id, expires_at, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![session_id, token_hash, csrf_token, principal_id, exp, created_at],
            )?;
            Ok(session_id)
        })
    }

    pub fn validate_session(
        &self,
        token_plaintext: &str,
    ) -> Result<Option<(String, String, String)>, StoreError> {
        // NEVER register the caller-supplied cookie before verification (redaction poisoning, §14.3).
        let token_hash = hash_token(token_plaintext);
        let verified = self.with_conn_sync(|conn| {
            let row: Option<(String, String, String, String)> = conn
                .query_row(
                    "SELECT session_id, csrf_token, principal_id, expires_at
                     FROM sessions WHERE token_hash = ?1",
                    params![token_hash],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .optional()?;
            if let Some((sid, csrf, pid, exp)) = row {
                if let Ok(dt) = DateTime::parse_from_rfc3339(&exp) {
                    if dt.with_timezone(&Utc) > Utc::now() {
                        return Ok(Some((sid, csrf, pid)));
                    }
                }
            }
            Ok(None)
        })?;
        if verified.is_some() {
            self.redactor().register_token(token_plaintext);
        }
        Ok(verified)
    }

    pub fn delete_session(&self, token_plaintext: &str) -> Result<(), StoreError> {
        let token_hash = hash_token(token_plaintext);
        self.with_conn_sync(|conn| {
            conn.execute(
                "DELETE FROM sessions WHERE token_hash = ?1",
                params![token_hash],
            )?;
            Ok(())
        })
    }

    // ---- History Series & Samples ----

    pub fn upsert_history_series(&self, spec: &HistorySeriesSpec) -> Result<(), StoreError> {
        let params_json = serde_json::to_string(&spec.params)?;
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO history_series (series_id, instance_id, params_json, aggregator, cadence_secs, retention_days, service_principal, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(series_id) DO UPDATE SET
                    cadence_secs = excluded.cadence_secs,
                    retention_days = excluded.retention_days,
                    service_principal = excluded.service_principal",
                params![
                    spec.series_id,
                    spec.instance_id,
                    params_json,
                    spec.aggregator,
                    spec.cadence_secs,
                    spec.retention_days,
                    spec.service_principal,
                    spec.created_at
                ],
            )?;
            Ok(())
        })
    }

    pub fn append_history_sample(
        &self,
        series_id: &str,
        sampled_at: &str,
        value_num: f64,
        metadata: Option<serde_json::Value>,
    ) -> Result<(), StoreError> {
        let meta_json = metadata.as_ref().map(serde_json::to_string).transpose()?;
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO history_samples (series_id, sampled_at, value_num, metadata_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![series_id, sampled_at, value_num, meta_json],
            )?;
            Ok(())
        })
    }

    pub fn compact_history_series(&self, series_id: &str, retention_days: i64) -> Result<usize, StoreError> {
        let cutoff = (Utc::now() - chrono::Duration::days(retention_days.max(1))).to_rfc3339();
        self.with_conn_sync(|conn| {
            let n = conn.execute(
                "DELETE FROM history_samples WHERE series_id = ?1 AND sampled_at < ?2",
                params![series_id, cutoff],
            )?;
            Ok(n)
        })
    }

    pub fn query_history_samples(
        &self,
        series_id: &str,
        since: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HistorySampleRecord>, StoreError> {
        // Query budget (§7.4): one read never returns more than HISTORY_QUERY_BUDGET samples.
        let limit = limit.clamp(1, HISTORY_QUERY_BUDGET);
        self.with_conn_sync(|conn| {
            let mut out = Vec::new();
            if let Some(since_ts) = since {
                let mut stmt = conn.prepare(
                    "SELECT sample_id, series_id, sampled_at, value_num, metadata_json
                     FROM history_samples
                     WHERE series_id = ?1 AND sampled_at >= ?2
                     ORDER BY sampled_at ASC LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![series_id, since_ts, limit as i64], |r| {
                    let meta_raw: Option<String> = r.get(4)?;
                    Ok(HistorySampleRecord {
                        sample_id: r.get(0)?,
                        series_id: r.get(1)?,
                        sampled_at: r.get(2)?,
                        value_num: r.get(3)?,
                        metadata: meta_raw.and_then(|s| serde_json::from_str(&s).ok()),
                    })
                })?;
                for row in rows {
                    out.push(row?);
                }
            } else {
                let mut stmt = conn.prepare(
                    "SELECT sample_id, series_id, sampled_at, value_num, metadata_json
                     FROM history_samples
                     WHERE sample_id IN (
                        SELECT sample_id FROM history_samples
                        WHERE series_id = ?1
                        ORDER BY sampled_at DESC, sample_id DESC LIMIT ?2)
                     ORDER BY sampled_at ASC, sample_id ASC",
                )?;
                let rows = stmt.query_map(params![series_id, limit as i64], |r| {
                    let meta_raw: Option<String> = r.get(4)?;
                    Ok(HistorySampleRecord {
                        sample_id: r.get(0)?,
                        series_id: r.get(1)?,
                        sampled_at: r.get(2)?,
                        value_num: r.get(3)?,
                        metadata: meta_raw.and_then(|s| serde_json::from_str(&s).ok()),
                    })
                })?;
                for row in rows {
                    out.push(row?);
                }
            }
            Ok(out)
        })
    }

    // ---- Revisions, Generations, Drafts, Activation Journal, Git Sync ----

    pub fn record_revision(
        &self,
        commit_hash: &str,
        parent_commit: Option<&str>,
        principal_id: &str,
        principal_kind: &str,
        summary: &str,
    ) -> Result<(), StoreError> {
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT OR IGNORE INTO revisions (commit_hash, parent_commit, principal_id, principal_kind, summary, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![commit_hash, parent_commit, principal_id, principal_kind, summary, now],
            )?;
            Ok(())
        })
    }

    pub fn record_generation(
        &self,
        revision_commit: &str,
        graph_digest: &str,
    ) -> Result<i64, StoreError> {
        let now = Utc::now().to_rfc3339();
        self.with_conn_sync(|conn| {
            let tx = conn.unchecked_transaction()?;
            tx.execute(
                "UPDATE generations SET status = 'drained' WHERE status = 'active'",
                [],
            )?;
            tx.execute(
                "INSERT INTO generations (revision_commit, activated_at, status, graph_digest)
                 VALUES (?1, ?2, 'active', ?3)",
                params![revision_commit, now, graph_digest],
            )?;
            let gen_id = tx.last_insert_rowid();
            tx.commit()?;
            Ok(gen_id)
        })
    }

    /// Activation timestamp (RFC 3339) of a recorded Generation.
    pub fn get_generation_activated_at(
        &self,
        generation_id: i64,
    ) -> Result<Option<String>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT activated_at FROM generations WHERE generation_id = ?1",
                params![generation_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    pub fn upsert_draft(&self, draft: &DraftRecord) -> Result<(), StoreError> {
        let changes_json = serde_json::to_string(&draft.changes)?;
        let val_json = draft
            .validation_report
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let test_json = draft
            .test_report
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO draft_transactions
                 (draft_id, base_revision, worktree_path, created_by, status, changes_json,
                  validation_report_json, test_report_json, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(draft_id) DO UPDATE SET
                    base_revision = excluded.base_revision,
                    worktree_path = excluded.worktree_path,
                    status = excluded.status,
                    changes_json = excluded.changes_json,
                    validation_report_json = excluded.validation_report_json,
                    test_report_json = excluded.test_report_json,
                    updated_at = excluded.updated_at",
                params![
                    draft.draft_id,
                    draft.base_revision,
                    draft.worktree_path,
                    draft.created_by,
                    draft.status,
                    changes_json,
                    val_json,
                    test_json,
                    draft.created_at,
                    draft.updated_at
                ],
            )?;
            Ok(())
        })
    }

    pub fn get_draft(&self, draft_id: &str) -> Result<Option<DraftRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT draft_id, base_revision, worktree_path, created_by, status, changes_json,
                        validation_report_json, test_report_json, created_at, updated_at
                 FROM draft_transactions WHERE draft_id = ?1",
                params![draft_id],
                |r| {
                    let ch_raw: String = r.get(5)?;
                    let val_raw: Option<String> = r.get(6)?;
                    let test_raw: Option<String> = r.get(7)?;
                    Ok(DraftRecord {
                        draft_id: r.get(0)?,
                        base_revision: r.get(1)?,
                        worktree_path: r.get(2)?,
                        created_by: r.get(3)?,
                        status: r.get(4)?,
                        changes: serde_json::from_str(&ch_raw).unwrap_or_default(),
                        validation_report: val_raw.and_then(|s| serde_json::from_str(&s).ok()),
                        test_report: test_raw.and_then(|s| serde_json::from_str(&s).ok()),
                        created_at: r.get(8)?,
                        updated_at: r.get(9)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
        })
    }

    pub fn list_drafts(&self) -> Result<Vec<DraftRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT draft_id, base_revision, worktree_path, created_by, status, changes_json,
                        validation_report_json, test_report_json, created_at, updated_at
                 FROM draft_transactions ORDER BY updated_at DESC",
            )?;
            let rows = stmt.query_map([], |r| {
                let ch_raw: String = r.get(5)?;
                let val_raw: Option<String> = r.get(6)?;
                let test_raw: Option<String> = r.get(7)?;
                Ok(DraftRecord {
                    draft_id: r.get(0)?,
                    base_revision: r.get(1)?,
                    worktree_path: r.get(2)?,
                    created_by: r.get(3)?,
                    status: r.get(4)?,
                    changes: serde_json::from_str(&ch_raw).unwrap_or_default(),
                    validation_report: val_raw.and_then(|s| serde_json::from_str(&s).ok()),
                    test_report: test_raw.and_then(|s| serde_json::from_str(&s).ok()),
                    created_at: r.get(8)?,
                    updated_at: r.get(9)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn upsert_activation_journal(&self, rec: &ActivationJournalRecord) -> Result<(), StoreError> {
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO activation_journal
                 (journal_id, draft_id, base_revision, target_revision, generation_id, phase, error_message, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(journal_id) DO UPDATE SET
                    target_revision = excluded.target_revision,
                    generation_id = excluded.generation_id,
                    phase = excluded.phase,
                    error_message = excluded.error_message,
                    updated_at = excluded.updated_at",
                params![
                    rec.journal_id,
                    rec.draft_id,
                    rec.base_revision,
                    rec.target_revision,
                    rec.generation_id,
                    rec.phase,
                    rec.error_message,
                    rec.updated_at
                ],
            )?;
            Ok(())
        })
    }

    pub fn list_incomplete_activation_journals(&self) -> Result<Vec<ActivationJournalRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            let mut stmt = conn.prepare(
                "SELECT journal_id, draft_id, base_revision, target_revision, generation_id, phase, error_message, updated_at
                 FROM activation_journal
                 WHERE phase IN ('started', 'committed')
                 ORDER BY updated_at ASC",
            )?;
            let rows = stmt.query_map([], |r| {
                Ok(ActivationJournalRecord {
                    journal_id: r.get(0)?,
                    draft_id: r.get(1)?,
                    base_revision: r.get(2)?,
                    target_revision: r.get(3)?,
                    generation_id: r.get(4)?,
                    phase: r.get(5)?,
                    error_message: r.get(6)?,
                    updated_at: r.get(7)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
    }

    pub fn set_git_sync_state(&self, rec: &GitSyncRecord) -> Result<(), StoreError> {
        self.with_conn_sync(|conn| {
            conn.execute(
                "INSERT INTO git_sync_state (id, active_revision, remote_name, remote_url, sync_status, last_error, updated_at)
                 VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(id) DO UPDATE SET
                    active_revision = excluded.active_revision,
                    remote_name = excluded.remote_name,
                    remote_url = excluded.remote_url,
                    sync_status = excluded.sync_status,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at",
                params![
                    rec.active_revision,
                    rec.remote_name,
                    rec.remote_url,
                    rec.sync_status,
                    rec.last_error,
                    rec.updated_at
                ],
            )?;
            Ok(())
        })
    }

    pub fn get_git_sync_state(&self) -> Result<Option<GitSyncRecord>, StoreError> {
        self.with_conn_sync(|conn| {
            conn.query_row(
                "SELECT active_revision, remote_name, remote_url, sync_status, last_error, updated_at
                 FROM git_sync_state WHERE id = 1",
                [],
                |r| {
                    Ok(GitSyncRecord {
                        active_revision: r.get(0)?,
                        remote_name: r.get(1)?,
                        remote_url: r.get(2)?,
                        sync_status: r.get(3)?,
                        last_error: r.get(4)?,
                        updated_at: r.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
        })
    }
}

pub fn hash_token(token_plaintext: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token_plaintext.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn test_wal_mode_and_idempotent_migrations() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::open(dir.path()).unwrap();
        let mode: String = store
            .with_conn_sync(|c| {
                c.query_row("PRAGMA journal_mode", [], |r| r.get(0))
                    .map_err(StoreError::from)
            })
            .unwrap();
        assert_eq!(mode.to_ascii_lowercase(), "wal");
        // Re-running migrations on the same database is idempotent.
        store
            .with_conn_sync(|c| migrations::run_migrations(c).map_err(StoreError::from))
            .unwrap();
        drop(store);
        let reopened = SqliteStore::open(dir.path()).unwrap();
        assert!(reopened.list_secrets().unwrap().is_empty());
    }

    fn exec(id: &str, status: &str, key: Option<&str>, args: serde_json::Value) -> ActionExecutionRecord {
        ActionExecutionRecord {
            execution_id: id.into(),
            action_instance: "merge_pr".into(),
            parent_execution_id: None,
            principal_id: "admin".into(),
            principal_kind: "administrator".into(),
            status: status.into(),
            idempotency_key: key.map(|k| k.to_string()),
            args,
            result: None,
            reason: None,
            retry_safe: false,
            reconcile_attempts: 0,
            trace_id: "tr".into(),
            started_at: Utc::now().to_rfc3339(),
            finished_at: None,
            duration_ms: None,
        }
    }

    #[test]
    fn test_action_ledger_claims_transitions_recovery_and_schedule() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::open(dir.path()).unwrap();
        let t = |status: &'static str, retry_safe: bool| ActionTransition {
            status,
            result: None,
            reason: Some("r"),
            retry_safe,
            reconcile_attempts: 0,
            finished_at: None,
            duration_ms: None,
        };

        // Atomic claim: the second insert with the same key inserts nothing.
        assert!(matches!(
            store.claim_idempotency_and_insert(&exec("a1", "queued", Some("k"), json!({"n": 1}))).unwrap(),
            IdempotencyClaim::New
        ));
        match store.claim_idempotency_and_insert(&exec("a2", "queued", Some("k"), json!({"n": 1}))).unwrap() {
            IdempotencyClaim::Existing(r) => assert_eq!(r.execution_id, "a1"),
            other => panic!("expected Existing, got {other:?}"),
        }
        assert!(store.get_action_execution("a2").unwrap().is_none());
        assert!(matches!(
            store.claim_idempotency_and_insert(&exec("a3", "queued", Some("k"), json!({"n": 2}))).unwrap(),
            IdempotencyClaim::ArgsMismatch(_)
        ));

        // Conditional transitions.
        assert!(store.transition_action_execution("a1", &["queued"], t("running", false)).unwrap());
        assert!(!store.transition_action_execution("a1", &["queued"], t("cancelled", true)).unwrap());
        assert!(store.transition_action_execution("a1", &["running"], t("failed", true)).unwrap());
        // A retry-safe failure is superseded atomically by the next claim.
        assert!(matches!(
            store.claim_idempotency_and_insert(&exec("a4", "queued", Some("k"), json!({"n": 1}))).unwrap(),
            IdempotencyClaim::New
        ));
        assert_eq!(store.lookup_action_idempotency("merge_pr", "k").unwrap().unwrap().execution_id, "a4");
        assert!(store.transition_action_execution("a4", &["queued"], t("running", false)).unwrap());
        assert!(store.transition_action_execution("a4", &["running"], t("unknown", false)).unwrap());
        // An unknown outcome is never superseded.
        match store.claim_idempotency_and_insert(&exec("a5", "queued", Some("k"), json!({"n": 1}))).unwrap() {
            IdempotencyClaim::Existing(r) => assert_eq!(r.execution_id, "a4"),
            other => panic!("expected Existing, got {other:?}"),
        }

        // Crash recovery + reconcile schedule.
        store.claim_idempotency_and_insert(&exec("r1", "running", None, json!({}))).unwrap();
        store.claim_idempotency_and_insert(&exec("q1", "queued", None, json!({}))).unwrap();
        let rec = store.recover_inflight_actions("restart").unwrap();
        assert_eq!(rec.marked_unknown, vec!["r1".to_string()]);
        assert_eq!(rec.marked_not_started, vec!["q1".to_string()]);
        assert_eq!(store.get_action_execution("r1").unwrap().unwrap().status, "unknown");
        let q = store.get_action_execution("q1").unwrap().unwrap();
        assert_eq!(q.status, "failed");
        assert!(q.retry_safe);

        let unscheduled: Vec<String> = store
            .list_unscheduled_unknown_executions(10)
            .unwrap()
            .into_iter()
            .map(|r| r.execution_id)
            .collect();
        assert_eq!(unscheduled, vec!["a4".to_string(), "r1".to_string()]);
        store.schedule_action_reconcile("r1", "2000-01-01T00:00:00.000Z").unwrap();
        store.schedule_action_reconcile("a4", "2999-01-01T00:00:00.000Z").unwrap();
        assert_eq!(
            store.list_due_action_reconciles("2026-01-01T00:00:00.000Z", 10).unwrap(),
            vec!["r1".to_string()]
        );
        store.clear_action_reconcile("r1").unwrap();
        assert!(store.list_due_action_reconciles("2026-01-01T00:00:00.000Z", 10).unwrap().is_empty());
        drop(store);
        // The additive ledger schema survives reopening.
        let reopened = SqliteStore::open(dir.path()).unwrap();
        assert_eq!(reopened.get_action_reconcile_schedule("a4").unwrap().as_deref(), Some("2999-01-01T00:00:00.000Z"));
    }

    #[test]
    fn test_secret_policy_network_ceiling_and_draft_editor_audits() {
        let store = SqliteStore::open_in_memory().unwrap();
        let hosts = vec!["api.github.com".to_string()];
        let meta = store
            .set_secret_with_policy("jira", "", "user:tok", &[], Some(&hosts), Some("basic"))
            .unwrap();
        assert_eq!(meta.credential_scheme, "basic");
        // Rotating through the legacy API keeps the Administrator's destination policy.
        let rotated = store.set_secret("jira", "", "user:tok2", &[]).unwrap();
        assert_eq!(rotated.key_version, 2);
        assert_eq!(rotated.allowed_hosts, hosts);
        assert_eq!(rotated.credential_scheme, "basic");
        let (resolved, plain) = store.resolve_secret_plaintext("jira").unwrap();
        assert_eq!(plain, "user:tok2");
        assert_eq!(resolved.allowed_hosts, hosts);
        assert!(store
            .set_secret_with_policy("x", "", "v", &[], None, Some("header:Host"))
            .is_err());
        assert!(store
            .set_secret_with_policy("x", "", "v", &[], None, Some("query:api_key"))
            .is_ok());

        assert!(store.get_network_policy_ceiling().unwrap().is_none());
        store
            .set_network_policy_ceiling(json!({"allowed_hosts": ["api.github.com"]}), "admin")
            .unwrap();
        let (policy, by, _) = store.get_network_policy_ceiling().unwrap().unwrap();
        assert_eq!(policy["allowed_hosts"][0], "api.github.com");
        assert_eq!(by, "admin");

        store
            .append_audit("agt_1", "agent", "draft.edit", "dft_1", "ok", json!({}))
            .unwrap();
        store
            .append_audit("agt_1", "agent", "draft.edit", "dft_1", "ok", json!({}))
            .unwrap();
        store
            .append_audit("admin", "administrator", "draft.begin", "dft_1", "ok", json!({}))
            .unwrap();
        store
            .append_audit("agt_2", "agent", "draft.edit", "dft_2", "ok", json!({}))
            .unwrap();
        let mut editors = store
            .list_audit_principals_for_target("dft_1", &["draft.edit", "draft.begin"])
            .unwrap();
        editors.sort();
        assert_eq!(
            editors,
            vec![
                ("admin".to_string(), "administrator".to_string()),
                ("agt_1".to_string(), "agent".to_string())
            ]
        );
    }

    #[test]
    fn test_state_cas_prevents_lost_updates() {
        let store = SqliteStore::open_in_memory().unwrap();
        let r1 = store
            .set_state("dashboard", "engineering", "selected_repo", 1, 1, json!("acme/a"), Some(0))
            .unwrap();
        assert_eq!(r1.version, 1);

        let r2 = store
            .set_state("dashboard", "engineering", "selected_repo", 1, 1, json!("acme/b"), Some(1))
            .unwrap();
        assert_eq!(r2.version, 2);

        let err = store
            .set_state("dashboard", "engineering", "selected_repo", 1, 1, json!("acme/stale"), Some(1))
            .unwrap_err();
        assert!(matches!(err, StoreError::CasConflict { expected: 1, actual: 2 }));
    }

    #[test]
    fn test_state_size_limit_and_insert_if_absent() {
        let store = SqliteStore::open_in_memory().unwrap();
        let big = json!("x".repeat(DEFAULT_STATE_MAX_BYTES + 1));
        let err = store
            .set_state("instance", "p", "k", 1, 1, big, None)
            .unwrap_err();
        assert!(matches!(err, StoreError::StateTooLarge { .. }));
        let err = store
            .set_state_with_limit("dashboard", "d", "k", 1, 1, json!("abcdef"), None, 4)
            .unwrap_err();
        assert!(matches!(err, StoreError::StateTooLarge { limit: 4, .. }));

        let seeded = store
            .insert_state_if_absent("dashboard", "d", "repo", 1, 1, json!("a"))
            .unwrap();
        assert_eq!(seeded.unwrap().version, 1);
        store
            .set_state("dashboard", "d", "repo", 1, 1, json!("client-write"), Some(1))
            .unwrap();
        // Seeding again never overwrites the concurrent write.
        assert!(store
            .insert_state_if_absent("dashboard", "d", "repo", 1, 1, json!("a"))
            .unwrap()
            .is_none());
        assert_eq!(
            store.get_state("dashboard", "d", "repo").unwrap().unwrap().value,
            json!("client-write")
        );
    }

    #[test]
    fn test_state_migrations_are_transactional() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.set_state("dashboard", "d", "a", 1, 1, json!(1), None).unwrap();
        store.set_state("dashboard", "d", "b", 1, 1, json!(2), None).unwrap();
        let step = |key: &str, expected: i64, v: Option<serde_json::Value>| StateMigrationStep {
            scope: "dashboard".into(),
            owner: "d".into(),
            state_key: key.into(),
            expected_version: expected,
            new_schema_version: 2,
            generation_id: 2,
            new_value: v,
        };
        // Second step has a stale expected version: nothing is applied.
        let err = store
            .apply_state_migrations(&[step("a", 1, Some(json!(10))), step("b", 7, None)])
            .unwrap_err();
        assert!(matches!(err, StoreError::CasConflict { expected: 7, actual: 1 }));
        let a = store.get_state("dashboard", "d", "a").unwrap().unwrap();
        assert_eq!((a.value, a.schema_version, a.version), (json!(1), 1, 1));

        let out = store
            .apply_state_migrations(&[step("a", 1, Some(json!(10))), step("b", 1, None)])
            .unwrap();
        assert_eq!(out[0].as_ref().unwrap().schema_version, 2);
        assert!(out[1].is_none());
        let a = store.get_state("dashboard", "d", "a").unwrap().unwrap();
        assert_eq!((a.value, a.schema_version, a.version), (json!(10), 2, 2));
        assert!(store.get_state("dashboard", "d", "b").unwrap().is_none());
        assert_eq!(store.list_state_for_scope("dashboard").unwrap().len(), 1);
    }

    #[test]
    fn test_secret_encryption_redaction_and_backup() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().join("data");
        let store = SqliteStore::open(&data_dir).unwrap();
        let secret_val = "ghp_super_secret_token_987654321";
        store
            .set_secret("github-work", "Work GitHub PAT", secret_val, &["github_reader".into()])
            .unwrap();

        let (meta, decrypted) = store.resolve_secret_plaintext("github-work").unwrap();
        assert_eq!(meta.key_version, 1);
        assert_eq!(decrypted, secret_val);

        let redacted = store
            .redactor()
            .redact_str(&format!("Authorization: Bearer {secret_val} failed"));
        assert!(!redacted.contains(secret_val));
        assert!(redacted.contains("[REDACTED]"));

        // Backup and restore preserve encrypted secret blobs
        let backup_path = dir.path().join("backup.db");
        store.backup_to(&backup_path).unwrap();
        assert!(backup_path.exists());
    }

    #[test]
    fn unverified_tokens_never_poison_redaction() {
        let store = SqliteStore::open_in_memory().unwrap();
        // Unauthenticated callers present arbitrary words as Bearer tokens / session cookies.
        assert!(store.authenticate_agent_token("merge_pr").unwrap().is_none());
        assert!(store.validate_session("open").unwrap().is_none());
        assert!(store.authenticate_agent_token("acme/platform").unwrap().is_none());
        assert_eq!(store.redactor().registered_len(), 0);
        store
            .append_audit("admin", "administrator", "action.invoke", "merge_pr", "ok", json!({"repo": "acme/platform", "state": "open"}))
            .unwrap();
        let audits = store.list_audits(1).unwrap();
        assert_eq!(audits[0].detail["repo"], "acme/platform");
        assert_eq!(audits[0].detail["state"], "open");

        // A real, server-issued token is registered once verified (e.g. after restart).
        let tok = "dsh_agt_0123456789abcdef0123456789abcdef";
        store.create_agent_principal("bot", tok, json!({}), None).unwrap();
        let fresh = store.redactor().clone();
        fresh.unregister_secret(tok);
        assert!(!fresh.is_registered(tok));
        assert!(store.authenticate_agent_token(tok).unwrap().is_some());
        assert!(store.redactor().is_registered(tok));
    }

    #[test]
    fn redactor_token_tier_is_bounded() {
        let r = Redactor::new();
        for i in 0..(vault::MAX_REDACTED_TOKENS + 10) {
            r.register_token(&format!("ses_{i:032}"));
        }
        assert_eq!(r.registered_len(), vault::MAX_REDACTED_TOKENS);
        // Oldest evicted first.
        assert!(!r.is_registered(&format!("ses_{:032}", 0)));
        assert!(r.is_registered(&format!("ses_{:032}", vault::MAX_REDACTED_TOKENS + 9)));
        // Short values are ignored entirely.
        r.register_token("open");
        assert!(!r.is_registered("open"));
    }

    #[test]
    fn expired_agent_token_rejected() {
        let store = SqliteStore::open_in_memory().unwrap();
        let tok = "dsh_agt_expired_0123456789abcdef0123456789";
        store
            .create_agent_principal("old", tok, json!({}), Some(Utc::now() - chrono::Duration::seconds(5)))
            .unwrap();
        assert!(store.authenticate_agent_token(tok).unwrap().is_none());
    }

    #[test]
    fn db_size_includes_wal() {
        let dir = TempDir::new().unwrap();
        let store = SqliteStore::open(dir.path()).unwrap();
        store.append_audit("a", "administrator", "op", "t", "ok", json!({})).unwrap();
        let main = std::fs::metadata(dir.path().join("dash.db")).unwrap().len();
        assert!(store.wal_size_bytes() > 0);
        assert!(store.db_size_bytes() > main);
    }

    #[test]
    fn test_events_checkpoints_dlq_and_activation_journal() {
        let store = SqliteStore::open_in_memory().unwrap();
        let e1 = store
            .append_event("merge_pr", "github.pr_merged", "1", "tr_1", true, json!({"pr": 42}))
            .unwrap();
        let e2 = store
            .append_event("merge_pr", "github.pr_merged", "1", "tr_2", true, json!({"pr": 43}))
            .unwrap();
        assert_eq!(e1.sequence, 1);
        assert_eq!(e2.sequence, 2);

        let replay = store.list_events_since("merge_pr", 1).unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].event_id, e2.event_id);

        store.set_consumer_checkpoint("complete_work", "merge_pr", 1).unwrap();
        assert_eq!(store.get_consumer_checkpoint("complete_work", "merge_pr").unwrap(), 1);

        let dlq_id = store.record_dead_letter("complete_work", &e2.event_id, 3, "jira 503").unwrap();
        assert!(dlq_id.starts_with("dlq_"));

        store
            .upsert_activation_journal(&ActivationJournalRecord {
                journal_id: "j1".into(),
                draft_id: "d1".into(),
                base_revision: "abc".into(),
                target_revision: Some("def".into()),
                generation_id: None,
                phase: "committed".into(),
                error_message: None,
                updated_at: Utc::now().to_rfc3339(),
            })
            .unwrap();
        let pending = store.list_incomplete_activation_journals().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].phase, "committed");
    }

    #[test]
    fn test_checkpoint_never_moves_backwards_and_min_sequence_append() {
        let store = SqliteStore::open_in_memory().unwrap();
        assert_eq!(store.find_consumer_checkpoint("c", "src").unwrap(), None);
        store.set_consumer_checkpoint("c", "src", 5).unwrap();
        store.set_consumer_checkpoint("c", "src", 2).unwrap();
        assert_eq!(store.get_consumer_checkpoint("c", "src").unwrap(), 5);
        assert_eq!(store.find_consumer_checkpoint("c", "src").unwrap(), Some(5));

        let e1 = store
            .append_event_min_sequence("src", "t", "1", "tr", true, json!({}), 7)
            .unwrap();
        assert_eq!(e1.sequence, 7);
        let e2 = store.append_event("src", "t", "1", "tr", true, json!({})).unwrap();
        assert_eq!(e2.sequence, 8);
        assert_eq!(store.max_event_sequence("src").unwrap(), 8);
        assert_eq!(store.get_event(&e1.event_id).unwrap().unwrap().sequence, 7);
        assert_eq!(store.list_events_since_limited("src", 0, 1).unwrap().len(), 1);
        assert_eq!(store.list_event_sources().unwrap(), vec!["src".to_string()]);
        let future = (Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
        assert_eq!(store.max_event_sequence_before("src", &future).unwrap(), 8);
        let past = (Utc::now() - chrono::Duration::days(1)).to_rfc3339();
        assert_eq!(store.max_event_sequence_before("src", &past).unwrap(), 0);
    }

    #[test]
    fn test_dead_letter_listing_resolution_and_event_retention() {
        let store = SqliteStore::open_in_memory().unwrap();
        let e1 = store.append_event("src", "t", "1", "tr", true, json!({"n": 1})).unwrap();
        let e2 = store.append_event("src", "t", "1", "tr", true, json!({"n": 2})).unwrap();
        let e3 = store.append_event("src", "t", "1", "tr", true, json!({"n": 3})).unwrap();
        // Consumer acked e1 and e2; e2 is dead-lettered and unresolved.
        store.set_consumer_checkpoint("auto", "src", e2.sequence).unwrap();
        let dlq = store.record_dead_letter("auto", &e2.event_id, 3, "boom").unwrap();
        assert_eq!(store.list_dead_letters(false, 10).unwrap().len(), 1);

        let old_run = |id: &str, event_id: Option<&str>| AutomationExecutionRecord {
            execution_id: id.into(),
            automation_id: "auto".into(),
            trigger_kind: "event".into(),
            trigger_ref: format!("event:{}", event_id.unwrap_or("none")),
            event_id: event_id.map(|s| s.to_string()),
            run_as: None,
            status: "succeeded".into(),
            attempts: 1,
            error: None,
            result: None,
            action_executions: vec![],
            trace_id: "tr".into(),
            started_at: (Utc::now() - chrono::Duration::days(2)).to_rfc3339(),
            finished_at: None,
        };
        store.insert_automation_execution(&old_run("aex_e1", Some(&e1.event_id))).unwrap();
        store.insert_automation_execution(&old_run("aex_e3", Some(&e3.event_id))).unwrap();

        let cutoff = (Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
        // Only e1: acked by every consumer and not referenced by an unresolved dead letter.
        assert_eq!(store.prune_events_before(&cutoff).unwrap(), 1);
        // The run record of the pruned Event goes too; the one guarding retained e3 stays.
        assert!(store.get_automation_execution("aex_e1").unwrap().is_none());
        assert!(store.get_automation_execution("aex_e3").unwrap().is_some());
        assert!(store.get_event(&e1.event_id).unwrap().is_none());
        assert!(store.get_event(&e2.event_id).unwrap().is_some());
        assert!(store.get_event(&e3.event_id).unwrap().is_some());

        assert!(store.resolve_dead_letter(&dlq).unwrap());
        assert!(!store.resolve_dead_letter(&dlq).unwrap());
        assert!(store.get_dead_letter(&dlq).unwrap().unwrap().resolved);
        assert!(store.list_dead_letters(false, 10).unwrap().is_empty());
        assert_eq!(store.list_dead_letters(true, 10).unwrap().len(), 1);
        assert_eq!(store.prune_events_before(&cutoff).unwrap(), 1);
        assert!(store.get_event(&e3.event_id).unwrap().is_some());
    }

    #[test]
    fn test_automation_execution_records_and_trigger_state() {
        let store = SqliteStore::open_in_memory().unwrap();
        let rec = AutomationExecutionRecord {
            execution_id: "aex_1".into(),
            automation_id: "complete_work".into(),
            trigger_kind: "event".into(),
            trigger_ref: "event:evt_1".into(),
            event_id: Some("evt_1".into()),
            run_as: Some("work_completion".into()),
            status: "running".into(),
            attempts: 0,
            error: None,
            result: None,
            action_executions: vec![],
            trace_id: "tr_1".into(),
            started_at: Utc::now().to_rfc3339(),
            finished_at: None,
        };
        store.insert_automation_execution(&rec).unwrap();
        assert!(store
            .find_succeeded_automation_execution("complete_work", "event:evt_1")
            .unwrap()
            .is_none());
        store
            .finish_automation_execution(
                "aex_1",
                "succeeded",
                1,
                None,
                Some(&json!({"ok": true})),
                &["act_1".to_string()],
                &Utc::now().to_rfc3339(),
            )
            .unwrap();
        let found = store
            .find_succeeded_automation_execution("complete_work", "event:evt_1")
            .unwrap()
            .unwrap();
        assert_eq!(found.result.unwrap()["ok"], true);
        assert_eq!(found.action_executions, vec!["act_1".to_string()]);
        assert_eq!(store.list_automation_executions(Some("complete_work"), 10).unwrap().len(), 1);
        assert_eq!(store.list_automation_executions(None, 10).unwrap().len(), 1);
        assert!(store.list_automation_executions(Some("other"), 10).unwrap().is_empty());

        assert!(store.get_automation_trigger_state("a", "cron:0").unwrap().is_none());
        store
            .set_automation_trigger_state("a", "cron:0", Some("2026-01-01T00:00:00+00:00"), None)
            .unwrap();
        store
            .set_automation_trigger_state("a", "cron:0", Some("2026-01-02T00:00:00+00:00"), Some("fp"))
            .unwrap();
        let st = store.get_automation_trigger_state("a", "cron:0").unwrap().unwrap();
        assert_eq!(st.last_fired_at.as_deref(), Some("2026-01-02T00:00:00+00:00"));
        assert_eq!(st.last_fingerprint.as_deref(), Some("fp"));
    }

    #[test]
    fn test_history_default_read_returns_most_recent_samples() {
        let store = SqliteStore::open_in_memory().unwrap();
        store
            .upsert_history_series(&HistorySeriesSpec {
                series_id: "s".into(),
                instance_id: "open_prs".into(),
                params: json!({}),
                aggregator: "count".into(),
                cadence_secs: 3600,
                retention_days: 30,
                service_principal: "sp".into(),
                created_at: Utc::now().to_rfc3339(),
            })
            .unwrap();
        let base = Utc::now() - chrono::Duration::hours(10);
        for i in 0..10 {
            let ts = (base + chrono::Duration::hours(i)).to_rfc3339();
            store.append_history_sample("s", &ts, i as f64, None).unwrap();
        }
        let recent = store.query_history_samples("s", None, 3).unwrap();
        let values: Vec<f64> = recent.iter().map(|r| r.value_num).collect();
        assert_eq!(values, vec![7.0, 8.0, 9.0], "newest samples, oldest first");
        // Query budget clamps absurd limits.
        assert_eq!(store.query_history_samples("s", None, usize::MAX).unwrap().len(), 10);
    }
}
