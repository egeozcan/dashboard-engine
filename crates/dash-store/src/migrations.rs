//! Transactional SQLite schema migrations for the §13 logical tables.
//!
//! Not every table is populated yet: `instances`, `contracts`, `dependency_locks`,
//! `service_principals` and `query_cache_index` are reserved schema (the active graph, manifests,
//! `packages.lock` and requested Service principal ceilings currently live in the Git Workspace
//! and the in-memory Generation). `runtime_metrics` is sampled by the daemon and pruned by
//! [`crate::retention`].

use rusqlite::Connection;

pub const LATEST_SCHEMA_VERSION: i64 = 1;

const MIGRATION_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS revisions (
    commit_hash TEXT PRIMARY KEY,
    parent_commit TEXT,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL,
    summary TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS generations (
    generation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    revision_commit TEXT NOT NULL,
    activated_at TEXT NOT NULL,
    status TEXT NOT NULL, -- active | drained | rolled_back
    graph_digest TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS draft_transactions (
    draft_id TEXT PRIMARY KEY,
    base_revision TEXT NOT NULL,
    worktree_path TEXT NOT NULL,
    created_by TEXT NOT NULL,
    status TEXT NOT NULL, -- open | validated | previewed | activated | aborted | stale
    changes_json TEXT NOT NULL,
    validation_report_json TEXT,
    test_report_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS activation_journal (
    journal_id TEXT PRIMARY KEY,
    draft_id TEXT NOT NULL,
    base_revision TEXT NOT NULL,
    target_revision TEXT,
    generation_id INTEGER,
    phase TEXT NOT NULL, -- started | committed | activated | pushed | failed | rolled_back
    error_message TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS instances (
    instance_id TEXT PRIMARY KEY,
    generation_id INTEGER NOT NULL,
    category TEXT NOT NULL,
    definition_id TEXT NOT NULL,
    definition_digest TEXT NOT NULL,
    config_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contracts (
    definition_id TEXT NOT NULL,
    definition_digest TEXT NOT NULL,
    category TEXT NOT NULL,
    manifest_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (definition_id, definition_digest)
);

CREATE TABLE IF NOT EXISTS dependency_locks (
    lock_id INTEGER PRIMARY KEY AUTOINCREMENT,
    revision_commit TEXT NOT NULL,
    lockfile_toml TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS service_principals (
    principal_id TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT '',
    requested_ceiling_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS service_principal_grants (
    principal_id TEXT PRIMARY KEY,
    granted_ceiling_json TEXT NOT NULL,
    granted_by TEXT NOT NULL,
    granted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_principals (
    agent_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    ceiling_json TEXT NOT NULL,
    expires_at TEXT,
    revoked_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS state_values (
    scope TEXT NOT NULL, -- client | dashboard | instance
    owner TEXT NOT NULL,
    state_key TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    version INTEGER NOT NULL DEFAULT 1,
    generation_id INTEGER NOT NULL DEFAULT 0,
    encoded_value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (scope, owner, state_key)
);

CREATE TABLE IF NOT EXISTS query_cache_index (
    query_key TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    definition_digest TEXT NOT NULL,
    auth_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE TABLE IF NOT EXISTS history_series (
    series_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    params_json TEXT NOT NULL,
    aggregator TEXT NOT NULL,
    cadence_secs INTEGER NOT NULL,
    retention_days INTEGER NOT NULL,
    service_principal TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS history_samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id TEXT NOT NULL,
    sampled_at TEXT NOT NULL,
    value_num REAL NOT NULL,
    metadata_json TEXT,
    FOREIGN KEY (series_id) REFERENCES history_series(series_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_history_samples_series_time
    ON history_samples(series_id, sampled_at);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    source_instance TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    emitted_at TEXT NOT NULL,
    semantic_type TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    durable INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL,
    UNIQUE (source_instance, sequence)
);
CREATE INDEX IF NOT EXISTS idx_events_source_seq ON events(source_instance, sequence);

CREATE TABLE IF NOT EXISTS event_consumer_checkpoints (
    consumer_id TEXT NOT NULL,
    source_instance TEXT NOT NULL,
    last_sequence INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (consumer_id, source_instance)
);

CREATE TABLE IF NOT EXISTS dead_letters (
    dlq_id TEXT PRIMARY KEY,
    consumer_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    last_error TEXT NOT NULL,
    failed_at TEXT NOT NULL,
    resolved INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS action_executions (
    execution_id TEXT PRIMARY KEY,
    action_instance TEXT NOT NULL,
    parent_execution_id TEXT,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL,
    status TEXT NOT NULL, -- queued | running | succeeded | failed | unknown | cancelled
    idempotency_key TEXT,
    args_json TEXT NOT NULL,
    result_json TEXT,
    reason TEXT,
    retry_safe INTEGER NOT NULL DEFAULT 0,
    reconcile_attempts INTEGER NOT NULL DEFAULT 0,
    trace_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    duration_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_action_exec_status ON action_executions(status, started_at);

CREATE TABLE IF NOT EXISTS action_steps (
    step_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    child_execution_id TEXT NOT NULL,
    step_order INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (execution_id) REFERENCES action_executions(execution_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS action_idempotency (
    idempotency_key TEXT NOT NULL,
    action_instance TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (idempotency_key, action_instance)
);

CREATE TABLE IF NOT EXISTS audit_records (
    audit_id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL,
    operation TEXT NOT NULL,
    target TEXT NOT NULL,
    outcome TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_records(timestamp);

CREATE TABLE IF NOT EXISTS trace_spans (
    span_id TEXT PRIMARY KEY,
    trace_id TEXT NOT NULL,
    parent_span_id TEXT,
    name TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    duration_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trace_spans_trace ON trace_spans(trace_id, started_at);

CREATE TABLE IF NOT EXISTS runtime_metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    name TEXT NOT NULL,
    value REAL NOT NULL,
    labels_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS secret_metadata (
    name TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT '',
    key_version INTEGER NOT NULL DEFAULT 1,
    allowed_bindings_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS encrypted_secret_blobs (
    name TEXT PRIMARY KEY,
    key_version INTEGER NOT NULL,
    nonce BLOB NOT NULL,
    ciphertext BLOB NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (name) REFERENCES secret_metadata(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS admin_credentials (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL UNIQUE,
    csrf_token TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS git_sync_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    active_revision TEXT,
    remote_name TEXT NOT NULL DEFAULT 'origin',
    remote_url TEXT,
    sync_status TEXT NOT NULL, -- synced | unsynced | no_remote
    last_error TEXT,
    updated_at TEXT NOT NULL
);
"#;

/// Idempotent event-log / automation schema extensions (§7.2, §7.3): automation execution
/// records (replay safety, traceability) and persisted trigger state (cron catch-up, Signal
/// predicates). Applied on every open with `IF NOT EXISTS`, so it is safe on existing databases.
const EVENT_LOG_EXTENSIONS: &str = r#"
CREATE TABLE IF NOT EXISTS automation_executions (
    execution_id TEXT PRIMARY KEY,
    automation_id TEXT NOT NULL,
    trigger_kind TEXT NOT NULL,
    trigger_ref TEXT NOT NULL,
    event_id TEXT,
    run_as TEXT,
    status TEXT NOT NULL, -- running | succeeded | failed
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    result_json TEXT,
    action_executions_json TEXT,
    trace_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_automation_exec_trigger
    ON automation_executions(automation_id, trigger_ref, status);
CREATE INDEX IF NOT EXISTS idx_automation_exec_started
    ON automation_executions(started_at);

CREATE TABLE IF NOT EXISTS automation_trigger_state (
    automation_id TEXT NOT NULL,
    trigger_key TEXT NOT NULL,
    last_fired_at TEXT,
    last_fingerprint TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (automation_id, trigger_key)
);

CREATE INDEX IF NOT EXISTS idx_events_emitted_at ON events(emitted_at);
CREATE INDEX IF NOT EXISTS idx_dead_letters_event ON dead_letters(event_id, resolved);
"#;

pub fn run_migrations(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;
         PRAGMA synchronous = NORMAL;",
    )?;

    let table_exists: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations')",
        [],
        |r| r.get(0),
    )?;
    let current: i64 = if table_exists {
        conn.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            [],
            |r| r.get(0),
        )?
    } else {
        0
    };

    if current < 1 {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(MIGRATION_V1)?;
        tx.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
            rusqlite::params![1_i64, chrono::Utc::now().to_rfc3339()],
        )?;
        tx.commit()?;
    }

    ensure_security_schema(conn)?;
    conn.execute_batch(EVENT_LOG_EXTENSIONS)?;
    Ok(())
}

/// Additive, idempotent security schema extensions (Administrator network ceiling and per-secret
/// destination/credential-scheme policy). Applied on every open, independent of the versioned
/// migrations, so it never conflicts with them and is safe to re-run.
fn ensure_security_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS network_policy_ceiling (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            policy_json TEXT NOT NULL,
            granted_by TEXT NOT NULL,
            granted_at TEXT NOT NULL
        );",
    )?;
    ensure_column(
        conn,
        "secret_metadata",
        "allowed_hosts_json",
        "TEXT NOT NULL DEFAULT '[]'",
    )?;
    ensure_column(
        conn,
        "secret_metadata",
        "credential_scheme",
        "TEXT NOT NULL DEFAULT 'bearer'",
    )?;
    Ok(())
}

fn ensure_column(conn: &Connection, table: &str, column: &str, decl: &str) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .filter_map(Result::ok)
        .any(|c| c == column);
    if !exists {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {decl};"))?;
    }
    Ok(())
}
