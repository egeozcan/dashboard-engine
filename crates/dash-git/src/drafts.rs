//! Transactional Draft manager: concurrent isolated Git worktrees, validate, preview,
//! explicit 3-way rebase, Generation carry-over, atomic swap, activation journal, and push sync (§12).
//!
//! Activation protocol (§12.2, ADR-0004; the concrete worktree-switching mechanics are a §19.10
//! design choice documented here):
//!
//! 1. The draft's content is fingerprinted as a Git tree (`git add -A && git write-tree` in the
//!    draft-private worktree index). Preview/test reports record the tree they tested, and
//!    activation only trusts a report whose tree equals the draft's current content.
//! 2. Live validation and the replacement Generation are built from the draft worktree, and
//!    replacement Instance VMs are prepared under the resource budget, all before any commit.
//! 3. The tested tree is committed with `git commit-tree` (no ref moves), then the Workspace
//!    `HEAD` is moved with a compare-and-swap `update-ref` and the index/working tree are
//!    fast-forwarded with `read-tree -m -u`. Files are never enumerated and deleted, so
//!    untracked and gitignored Workspace files survive, and uncommitted tracked modifications
//!    make activation fail instead of being overwritten (§12.3).
//! 4. Every phase is journalled (`started` with the target commit id before HEAD moves,
//!    `committed`, `activated`, `pushed`, or `failed`), so startup reconciliation can decide
//!    deterministically whether a crashed activation landed.

use crate::git_cli::{
    commit_trailer, create_worktree, ensure_git_repo, git_commit_message, git_commit_tree,
    git_diff_name_status, git_export_tree, git_head, git_history, git_index_tree, git_is_ancestor,
    git_push_commit, git_read_tree_fast_forward, git_read_tree_index_only, git_read_tree_reset,
    git_remote_tracking_commit, git_remote_url, git_remote_url_raw, git_resolve_commit,
    git_show_file, git_snapshot_worktree_tree, git_stage_all_tree, git_tracked_changes,
    git_tree_of, git_update_head, remove_worktree, run_git, run_git_bytes, sanitize_remote_url,
    GitCommitInfo, GitError, GitStatusReport,
};
use crate::packages::{
    fetch_locked_packages, read_packages_lock, read_packages_toml,
    resolve_package_update_with_policy, write_packages_lock, write_packages_toml,
    PackageDependencySpec, PackageFetchPolicy, PackagesLock, PackagesToml,
};
use crate::preview::{
    run_preview_and_scenarios, run_workspace_definition_tests, validate_graph_with_contracts,
    PreviewTestReport,
};
use crate::workspace::{
    delete_instance_in_workspace, load_workspace_graph_async as load_workspace_graph,
    upsert_instance_in_workspace, write_dashboard_in_workspace, write_definition_in_workspace,
};
use chrono::Utc;
use dash_core::{
    ConfiguredGraph, DashboardDoc, InstanceConfig, Principal, RuntimeEngine, ValidationIssue,
    ValidationReport,
};
use dash_luau::CompiledInstanceBundle;
use dash_store::{ActivationJournalRecord, DraftRecord, GitSyncRecord, SqliteStore};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationResult {
    pub revision_commit: String,
    pub generation_id: i64,
    pub carried_over_instances: Vec<String>,
    /// Instances that got a freshly prepared VM: changed, new, and downstream dependents.
    pub rebuilt_instances: Vec<String>,
    pub sync_status: String, // synced | unsynced | no_remote
    pub sync_error: Option<String>,
    /// Instances whose cached Query results were marked `stale` and are being recomputed.
    #[serde(default)]
    pub stale_instances: Vec<String>,
}

/// How the Workspace working tree relates to the activated content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivationMode {
    /// A normal draft: the Workspace must have no uncommitted tracked modifications and is
    /// fast-forwarded to the new commit.
    Draft,
    /// `dash reload`: the draft was built from the Workspace working tree itself, which must
    /// still equal the tested content; only HEAD and the index move.
    Reload,
}

const DRAFT_STATUSES_FINAL: [&str; 2] = ["activated", "aborted"];

struct BusyGuard {
    set: Arc<std::sync::Mutex<BTreeMap<String, String>>>,
    draft_id: String,
}

impl Drop for BusyGuard {
    fn drop(&mut self) {
        if let Ok(mut s) = self.set.lock() {
            s.remove(&self.draft_id);
        }
    }
}

/// A draft checked out for one mutation: holds the draft's busy slot until dropped, so an
/// activation, rebase or abort can never interleave with the file writes and the change-log
/// entry of the mutation (and vice versa). Derefs to the [`DraftRecord`] read after the slot
/// was taken.
struct MutationGuard {
    draft: DraftRecord,
    _busy: BusyGuard,
}

impl std::ops::Deref for MutationGuard {
    type Target = DraftRecord;
    fn deref(&self) -> &DraftRecord {
        &self.draft
    }
}

/// An activation whose Generation is live and whose draft is final, still to be pushed. The
/// push runs after the activation lock and the draft's busy slot are released (§12.2 step 8).
struct PendingPush {
    result: ActivationResult,
    journal: ActivationJournalRecord,
    draft_author: String,
    trace: SpanCtx,
}

/// Trace context of one Git transaction (§15.1: draft validation, migrations, activation and
/// sync spans). Root spans link under the current request span automatically.
#[derive(Clone)]
struct SpanCtx {
    trace_id: String,
    parent: Option<String>,
}

impl SpanCtx {
    fn child(&self, parent_span_id: &str) -> SpanCtx {
        SpanCtx {
            trace_id: self.trace_id.clone(),
            parent: Some(parent_span_id.to_string()),
        }
    }
}

/// Record `span` with status `ok`/`error` from `res` (the error message is added to `attrs`).
fn finish_span<T, E: std::fmt::Display>(
    span: dash_core::telemetry::SpanGuard,
    res: &Result<T, E>,
    mut attrs: serde_json::Value,
) {
    let status = match res {
        Ok(_) => "ok",
        Err(e) => {
            if let Some(obj) = attrs.as_object_mut() {
                obj.insert("error".into(), serde_json::Value::String(e.to_string()));
            }
            "error"
        }
    };
    span.finish(status, attrs);
}

#[derive(Clone)]
pub struct DraftManager {
    workspace_dir: PathBuf,
    data_dir: PathBuf,
    store: SqliteStore,
    engine: RuntimeEngine,
    use_mocks: bool,
    activate_lock: Arc<Mutex<()>>,
    /// Serializes pushes (never held together with `activate_lock`), so concurrent pushes of
    /// successive activations cannot race each other or overwrite each other's sync state.
    push_lock: Arc<Mutex<()>>,
    /// Upper bound of one push (see [`crate::git_cli::DEFAULT_PUSH_TIMEOUT`]).
    push_timeout: Arc<std::sync::Mutex<std::time::Duration>>,
    /// Serializes read-modify-write updates of DraftRecords (never held across `.await`).
    record_lock: Arc<std::sync::Mutex<()>>,
    /// Drafts with an activation, rebase or abort in progress (draft id -> operation).
    busy: Arc<std::sync::Mutex<BTreeMap<String, String>>>,
    startup_warnings: Arc<Vec<String>>,
    /// Test-only simulated process crash point inside activation (see [`CRASH_POINTS`]).
    crash_point: Arc<std::sync::Mutex<Option<String>>>,
}

/// Named points of the activation protocol at which [`DraftManager::set_crash_point_for_tests`]
/// can simulate a process crash, in protocol order.
#[doc(hidden)]
pub const CRASH_POINTS: &[&str] = &[
    "after_journal_started",
    "after_commit_object",
    "after_target_journalled",
    "after_head_moved",
    "after_committed",
    "after_generation_recorded",
    "after_swap",
    "before_push",
];

const SIMULATED_CRASH_PREFIX: &str = "simulated crash at ";

fn is_simulated_crash(e: &GitError) -> bool {
    matches!(e, GitError::Workspace(m) if m.starts_with(SIMULATED_CRASH_PREFIX))
}

impl DraftManager {
    pub async fn initialize(
        workspace_dir: PathBuf,
        data_dir: PathBuf,
        store: SqliteStore,
        budget: dash_luau::ResourceBudget,
    ) -> Result<Self, GitError> {
        Self::initialize_with_mocks(workspace_dir, data_dir, store, budget, false).await
    }

    pub async fn initialize_with_mocks(
        workspace_dir: PathBuf,
        data_dir: PathBuf,
        store: SqliteStore,
        budget: dash_luau::ResourceBudget,
        use_mocks: bool,
    ) -> Result<Self, GitError> {
        std::fs::create_dir_all(&workspace_dir)?;
        std::fs::create_dir_all(&data_dir)?;
        let workspace_dir = crate::git_cli::absolutize_path(&workspace_dir)?;
        let data_dir = crate::git_cli::absolutize_path(&data_dir)?;
        let mut warnings = Vec::new();
        if data_dir.starts_with(&workspace_dir) {
            warnings.push(format!(
                "data directory {} is inside the Workspace {}; keep it gitignored (or move it outside) so draft worktrees and SQLite files never appear as Workspace changes",
                data_dir.display(),
                workspace_dir.display()
            ));
        }

        let branch = "main";
        ensure_git_repo(&workspace_dir, branch).await?;

        // §4.4: the runtime never silently resolves or updates external dependencies. A missing
        // or stale packages.lock is surfaced; it is fixed through `dependency_update` in a draft.
        if let Err(msg) = check_lock_matches_manifest(&workspace_dir) {
            warnings.push(msg);
        }
        // Crash recovery on startup from activation_journal (§12.2, §15.2, ADR-0004)
        let landed = reconcile_activation_journals(&workspace_dir, &store, &mut warnings).await;
        let mut head = git_head(&workspace_dir).await?;

        // The active Generation is built exactly from its Revision: uncommitted external
        // modifications are never silently activated (§12.3). `--use-mocks` substitutes Mock
        // connectors but keeps live authority (SQLite grants).
        let mut graph =
            load_revision_graph(&workspace_dir, &data_dir, &head, use_mocks, &mut warnings).await?;

        // §8: the Generation that starts serving must have its Dashboard and declared Instance
        // State migrated, exactly as an activation would. An activation interrupted before its
        // migration ran is either completed below or, if its State can no longer migrate, rolled
        // back to its base.
        let mut rolled_back: Option<String> = None;
        let precheck = dash_core::StateManager::new(store.clone(), dash_core::SignalHub::new(64));
        if let Err(e) = precheck.check_graph_state(&graph) {
            let Some(l) = landed.iter().find(|l| l.target == head) else {
                return Err(GitError::Workspace(format!(
                    "Workspace HEAD {} declares Dashboard State that cannot be migrated from the stored State: {e}. Dash refuses to serve it unmigrated (§8). Move HEAD back to the previous Revision or fix the state_schema migration in a new commit, then restart.",
                    short(&head)
                )));
            };
            let msg = format!(
                "reconciled_on_startup: interrupted activation {} of {} rolled back to {}: its Dashboard State cannot be migrated ({e})",
                l.journal.journal_id,
                short(&l.target),
                short(&l.journal.base_revision)
            );
            git_update_head(
                &workspace_dir,
                &l.journal.base_revision,
                &l.target,
                "dash: roll back interrupted activation (State migration failed)",
            )
            .await?;
            repair_index(&workspace_dir, &l.target, &l.journal.base_revision, l.mode).await;
            let mut failed = l.journal.clone();
            failed.phase = "failed".into();
            failed.error_message = Some(msg.clone());
            failed.updated_at = Utc::now().to_rfc3339();
            let _ = store.upsert_activation_journal(&failed);
            warnings.push(msg);
            rolled_back = Some(l.journal.journal_id.clone());
            head = l.journal.base_revision.clone();
            graph = load_revision_graph(&workspace_dir, &data_dir, &head, use_mocks, &mut warnings)
                .await?;
            if let Err(e) = precheck.check_graph_state(&graph) {
                return Err(GitError::Workspace(format!(
                    "Revision {} declares Dashboard State that cannot be migrated from the stored State: {e}. Dash refuses to serve it unmigrated (§8).",
                    short(&head)
                )));
            }
        }
        let gen_id = store
            .record_generation(&head, &graph.graph_digest())
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        graph.generation_id = gen_id;
        let admin = Principal::administrator();
        let _ = store.record_revision(
            &head,
            None,
            &admin.id,
            admin.kind.as_str(),
            "Initial active revision",
        );

        // Sync status is always re-derived, never trusted from a previous run (§12.2 step 8).
        let sync = derive_sync_state(&workspace_dir, &store, &graph.workspace.git_branch).await;
        let _ = store.set_git_sync_state(&sync);

        for w in &warnings {
            tracing::warn!("{w}");
        }

        let engine = RuntimeEngine::new(graph.clone(), store.clone(), budget)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        let _ = engine.history.sync_series_configs(&graph.workspace.history);
        // Durable consumers start at this Generation's activation point (§7.2).
        engine.automations.register_consumers();
        // Migrate stored Dashboard and declared Instance State to the schema versions (one
        // transaction, audited), then seed missing defaults (§8). Only records whose
        // schema_version differs are touched, so this is a no-op when the previous run already
        // migrated them.
        let report = engine
            .state
            .apply_graph_state(&graph, gen_id, &admin)
            .map_err(|e| {
                GitError::Workspace(format!(
                    "Dashboard State migration for Revision {} failed at startup: {e}; Dash refuses to serve unmigrated State (§8)",
                    short(&head)
                ))
            })?;
        if report.applied > 0 {
            tracing::info!(
                "migrated {} Dashboard State record(s) to Revision {} at startup",
                report.applied,
                short(&head)
            );
        }
        // Only now, with the Generation's State migrated, are interrupted activations complete.
        for l in &landed {
            if rolled_back.as_deref() != Some(l.journal.journal_id.as_str()) {
                finalize_landed_activation(&workspace_dir, &store, l).await;
            }
        }

        Ok(Self {
            workspace_dir,
            data_dir,
            store,
            engine,
            use_mocks,
            activate_lock: Arc::new(Mutex::new(())),
            push_lock: Arc::new(Mutex::new(())),
            push_timeout: Arc::new(std::sync::Mutex::new(crate::git_cli::DEFAULT_PUSH_TIMEOUT)),
            record_lock: Arc::new(std::sync::Mutex::new(())),
            busy: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            startup_warnings: Arc::new(warnings),
            crash_point: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    pub fn workspace_dir(&self) -> &Path {
        &self.workspace_dir
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn engine(&self) -> &RuntimeEngine {
        &self.engine
    }

    pub fn store(&self) -> &SqliteStore {
        &self.store
    }

    /// Problems detected at startup (stale/missing packages.lock, uncommitted external
    /// Workspace modifications, journal divergence, ...).
    pub fn startup_warnings(&self) -> &[String] {
        &self.startup_warnings
    }

    /// The active Revision used for draft bases and the stale check (§12.3): Workspace HEAD,
    /// which only Dash activations move. `git_status` reports when HEAD diverges from the
    /// running Generation's Revision.
    pub async fn active_revision(&self) -> Result<String, GitError> {
        git_head(&self.workspace_dir).await
    }

    /// Revision of the Generation that is currently serving.
    pub fn active_generation_revision(&self) -> String {
        self.engine.graph.load().revision_commit.clone()
    }

    /// Test hook: make the next activation stop dead at `point` (one of [`CRASH_POINTS`]),
    /// returning an error without running any of the remaining steps or rollbacks, as if the
    /// process had been killed there. Startup reconciliation is then exercised by reopening
    /// the Workspace with a new manager (§12.2 activation journal).
    #[doc(hidden)]
    pub fn set_crash_point_for_tests(&self, point: Option<&str>) {
        if let Some(p) = point {
            assert!(CRASH_POINTS.contains(&p), "unknown crash point '{p}'");
        }
        *self
            .crash_point
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = point.map(str::to_string);
    }

    fn simulated_crash(&self, point: &str) -> Result<(), GitError> {
        let mut cp = self
            .crash_point
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if cp.as_deref() == Some(point) {
            *cp = None;
            return Err(GitError::Workspace(format!(
                "{SIMULATED_CRASH_PREFIX}{point}"
            )));
        }
        Ok(())
    }

    // ---- Authorization ----

    fn deny(&self, principal: &Principal, op: &str, draft_id: &str, reason: &str) -> GitError {
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            &format!("draft.{op}"),
            draft_id,
            "denied",
            serde_json::json!({ "reason": reason }),
        );
        GitError::Workspace(format!("access denied: {reason}"))
    }

    /// Draft access policy: only the draft's creator or an unrestricted principal (the
    /// Administrator) may read or operate on a draft; operations that change a draft
    /// additionally require `can_author_drafts`. Denials are audited.
    fn authorize(
        &self,
        draft: &DraftRecord,
        principal: &Principal,
        op: &str,
        needs_author: bool,
    ) -> Result<(), GitError> {
        let unrestricted = principal.ceiling.unrestricted;
        if needs_author && !principal.ceiling.can_author_drafts && !unrestricted {
            return Err(self.deny(
                principal,
                op,
                &draft.draft_id,
                "principal lacks can_author_drafts",
            ));
        }
        if !unrestricted && draft.created_by != principal.id {
            return Err(self.deny(
                principal,
                op,
                &draft.draft_id,
                &format!(
                    "draft '{}' belongs to '{}'; only its creator or the Administrator may {op} it",
                    draft.draft_id, draft.created_by
                ),
            ));
        }
        Ok(())
    }

    fn acquire_busy(&self, draft_id: &str, op: &str) -> Result<BusyGuard, GitError> {
        let mut s = self
            .busy
            .lock()
            .map_err(|_| GitError::Workspace("draft busy table poisoned".into()))?;
        if let Some(cur) = s.get(draft_id) {
            return Err(GitError::Workspace(format!(
                "draft '{draft_id}' is busy ({cur} in progress)"
            )));
        }
        s.insert(draft_id.to_string(), op.to_string());
        Ok(BusyGuard {
            set: Arc::clone(&self.busy),
            draft_id: draft_id.to_string(),
        })
    }

    /// Read-modify-write a DraftRecord against its latest stored version.
    fn update_record<F>(&self, draft_id: &str, f: F) -> Result<DraftRecord, GitError>
    where
        F: FnOnce(&mut DraftRecord) -> Result<(), GitError>,
    {
        let _g = self
            .record_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut rec = self.draft_status(draft_id)?;
        f(&mut rec)?;
        rec.updated_at = Utc::now().to_rfc3339();
        self.store
            .upsert_draft(&rec)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        Ok(rec)
    }

    // ---- Draft lifecycle ----

    /// Begin a new isolated Draft transaction against the current active Revision (§12.2, §12.3).
    pub async fn draft_begin(&self, principal: &Principal) -> Result<DraftRecord, GitError> {
        self.draft_begin_at(principal, None).await
    }

    /// Begin a Draft from a specified base Revision (§12.2 step 1); `None` uses the active
    /// Revision. Activation still requires the base to equal the active Revision (§12.3).
    pub async fn draft_begin_at(
        &self,
        principal: &Principal,
        base_revision: Option<&str>,
    ) -> Result<DraftRecord, GitError> {
        if !principal.ceiling.can_author_drafts && !principal.ceiling.unrestricted {
            return Err(self.deny(principal, "begin", "-", "principal lacks can_author_drafts"));
        }
        let base_rev = match base_revision.map(str::trim).filter(|s| !s.is_empty()) {
            Some(rev) => git_resolve_commit(&self.workspace_dir, rev)
                .await
                .map_err(|_| {
                    GitError::Workspace(format!(
                        "base revision '{rev}' is not a commit of the Workspace"
                    ))
                })?,
            None => self.active_revision().await?,
        };
        let draft_id = format!("dft_{}", Uuid::now_v7().simple());
        let wt_dir = self.data_dir.join("drafts").join(&draft_id);
        create_worktree(&self.workspace_dir, &wt_dir, &base_rev).await?;

        let now = Utc::now().to_rfc3339();
        let rec = DraftRecord {
            draft_id,
            base_revision: base_rev,
            worktree_path: wt_dir.to_string_lossy().to_string(),
            created_by: principal.id.clone(),
            status: "open".into(),
            changes: vec![],
            validation_report: None,
            test_report: None,
            created_at: now.clone(),
            updated_at: now,
        };
        self.store
            .upsert_draft(&rec)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "draft.begin",
            &rec.draft_id,
            "ok",
            serde_json::json!({ "base_revision": rec.base_revision }),
        );
        Ok(rec)
    }

    /// Unchecked lookup for trusted in-process callers; external callers use [`Self::draft_status_as`].
    pub fn draft_status(&self, draft_id: &str) -> Result<DraftRecord, GitError> {
        self.store
            .get_draft(draft_id)
            .map_err(|e| GitError::Workspace(e.to_string()))?
            .ok_or_else(|| GitError::Workspace(format!("draft '{draft_id}' not found")))
    }

    pub fn draft_status_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<DraftRecord, GitError> {
        let draft = self.draft_status(draft_id)?;
        self.authorize(&draft, principal, "read", false)?;
        Ok(draft)
    }

    pub fn draft_list(&self) -> Result<Vec<DraftRecord>, GitError> {
        self.store
            .list_drafts()
            .map_err(|e| GitError::Workspace(e.to_string()))
    }

    /// Drafts visible to `principal`: all for the Administrator, otherwise only its own.
    pub fn draft_list_for(&self, principal: &Principal) -> Result<Vec<DraftRecord>, GitError> {
        let all = self.draft_list()?;
        if principal.ceiling.unrestricted {
            return Ok(all);
        }
        Ok(all
            .into_iter()
            .filter(|d| d.created_by == principal.id)
            .collect())
    }

    /// Abort as the Administrator (trusted in-process callers).
    pub async fn draft_abort(&self, draft_id: &str) -> Result<(), GitError> {
        self.draft_abort_as(draft_id, &Principal::administrator())
            .await
    }

    /// Abort and discard an unactivated draft. Aborting an aborted draft is a no-op; an
    /// activated draft cannot be aborted.
    pub async fn draft_abort_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<(), GitError> {
        let draft = self.draft_status(draft_id)?;
        self.authorize(&draft, principal, "abort", true)?;
        match draft.status.as_str() {
            "aborted" => return Ok(()),
            "activated" => {
                return Err(GitError::Workspace(format!(
                    "draft '{draft_id}' is already activated and cannot be aborted"
                )))
            }
            _ => {}
        }
        let _busy = self.acquire_busy(draft_id, "abort")?;
        // Status may have changed while we were not holding the busy slot
        let fresh = self.draft_status(draft_id)?;
        if fresh.status == "activated" {
            return Err(GitError::Workspace(format!(
                "draft '{draft_id}' is already activated and cannot be aborted"
            )));
        }
        remove_worktree(&self.workspace_dir, Path::new(&fresh.worktree_path)).await;
        self.update_record(draft_id, |r| {
            if r.status == "activated" {
                return Err(GitError::Workspace(format!(
                    "draft '{draft_id}' is already activated and cannot be aborted"
                )));
            }
            r.status = "aborted".into();
            Ok(())
        })?;
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "draft.abort",
            draft_id,
            "aborted",
            serde_json::json!({ "created_by": draft.created_by }),
        );
        Ok(())
    }

    // ---- Draft Mutation Methods (strictly isolated to draft worktree) ----
    //
    // The un-suffixed variants act as the Administrator and exist for trusted in-process
    // callers (CLI, tests). Services and MCP use the `_as` variants.

    pub fn draft_write_definition(
        &self,
        draft_id: &str,
        package: &str,
        def_name: &str,
        definition_luau: &str,
        runtime_luau: Option<&str>,
        tests_luau: Option<&str>,
    ) -> Result<DraftRecord, GitError> {
        self.draft_write_definition_as(
            draft_id,
            package,
            def_name,
            definition_luau,
            runtime_luau,
            tests_luau,
            &Principal::administrator(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn draft_write_definition_as(
        &self,
        draft_id: &str,
        package: &str,
        def_name: &str,
        definition_luau: &str,
        runtime_luau: Option<&str>,
        tests_luau: Option<&str>,
        principal: &Principal,
    ) -> Result<DraftRecord, GitError> {
        let draft = self.begin_mutation(draft_id, principal, "edit")?;
        let wt = Path::new(&draft.worktree_path);
        write_definition_in_workspace(
            wt,
            package,
            def_name,
            definition_luau,
            runtime_luau,
            tests_luau,
        )?;
        self.record_draft_mutation(
            draft_id,
            principal,
            format!("definition:{package}.{def_name}"),
        )
    }

    pub fn draft_upsert_instance(
        &self,
        draft_id: &str,
        inst: &InstanceConfig,
    ) -> Result<DraftRecord, GitError> {
        self.draft_upsert_instance_as(draft_id, inst, &Principal::administrator())
    }

    pub fn draft_upsert_instance_as(
        &self,
        draft_id: &str,
        inst: &InstanceConfig,
        principal: &Principal,
    ) -> Result<DraftRecord, GitError> {
        let draft = self.begin_mutation(draft_id, principal, "edit")?;
        let wt = Path::new(&draft.worktree_path);
        upsert_instance_in_workspace(wt, inst)?;
        self.record_draft_mutation(draft_id, principal, format!("instance.upsert:{}", inst.id))
    }

    pub fn draft_delete_instance(
        &self,
        draft_id: &str,
        instance_id: &str,
    ) -> Result<DraftRecord, GitError> {
        self.draft_delete_instance_as(draft_id, instance_id, &Principal::administrator())
    }

    pub fn draft_delete_instance_as(
        &self,
        draft_id: &str,
        instance_id: &str,
        principal: &Principal,
    ) -> Result<DraftRecord, GitError> {
        let draft = self.begin_mutation(draft_id, principal, "edit")?;
        let wt = Path::new(&draft.worktree_path);
        delete_instance_in_workspace(wt, instance_id)?;
        self.record_draft_mutation(
            draft_id,
            principal,
            format!("instance.delete:{instance_id}"),
        )
    }

    pub fn draft_update_dashboard(
        &self,
        draft_id: &str,
        doc: &DashboardDoc,
    ) -> Result<DraftRecord, GitError> {
        self.draft_update_dashboard_as(draft_id, doc, &Principal::administrator())
    }

    pub fn draft_update_dashboard_as(
        &self,
        draft_id: &str,
        doc: &DashboardDoc,
        principal: &Principal,
    ) -> Result<DraftRecord, GitError> {
        let draft = self.begin_mutation(draft_id, principal, "edit")?;
        let wt = Path::new(&draft.worktree_path);
        write_dashboard_in_workspace(wt, doc)?;
        self.record_draft_mutation(draft_id, principal, format!("dashboard:{}", doc.id))
    }

    pub async fn draft_update_dependency(
        &self,
        draft_id: &str,
        pkg_name: &str,
        spec: PackageDependencySpec,
    ) -> Result<(DraftRecord, PackagesLock), GitError> {
        self.draft_update_dependency_as(draft_id, pkg_name, spec, &Principal::administrator())
            .await
    }

    /// Update a dependency pin and re-resolve the whole `packages.lock` inside the draft (§4.4).
    pub async fn draft_update_dependency_as(
        &self,
        draft_id: &str,
        pkg_name: &str,
        spec: PackageDependencySpec,
        principal: &Principal,
    ) -> Result<(DraftRecord, PackagesLock), GitError> {
        let draft = self.begin_mutation(draft_id, principal, "edit")?;
        let wt = Path::new(&draft.worktree_path);
        let mut cfg = read_packages_toml(wt)?;
        cfg.dependencies.insert(pkg_name.to_string(), spec);
        // Resolve the candidate requirement set in memory first: a failed resolution (unknown
        // ref, clone failure, conflict, cycle) leaves the draft's packages.toml and packages.lock
        // exactly as they were, so it cannot poison later updates.
        // The fetch is steered by `principal`: restricted Principals are held to the
        // Administrator network ceiling and get no ambient Git credentials (R7:SEC-5).
        let fetch_policy = PackageFetchPolicy::for_principal(principal, &self.store);
        let lock = resolve_package_update_with_policy(
            wt,
            &self.data_dir,
            &cfg,
            pkg_name,
            &fetch_policy,
        )
        .await?;
        // Persist both files, restoring the originals if either write fails.
        let originals: Vec<(PathBuf, Option<Vec<u8>>)> = ["packages.toml", "packages.lock"]
            .iter()
            .map(|f| {
                let p = wt.join(f);
                let bytes = std::fs::read(&p).ok();
                (p, bytes)
            })
            .collect();
        let persisted = write_packages_toml(wt, &cfg).and_then(|_| write_packages_lock(wt, &lock));
        if let Err(e) = persisted {
            for (path, bytes) in originals {
                let _ = match bytes {
                    Some(b) => std::fs::write(&path, b),
                    None => std::fs::remove_file(&path),
                };
            }
            return Err(e);
        }
        let rec =
            self.record_draft_mutation(draft_id, principal, format!("dependency:{pkg_name}"))?;
        Ok((rec, lock))
    }

    /// Validate as the Administrator (trusted in-process callers).
    pub async fn draft_validate(&self, draft_id: &str) -> Result<ValidationReport, GitError> {
        self.draft_validate_as(draft_id, &Principal::administrator())
            .await
    }

    /// Validate contracts, Instance config schemas, bindings, capabilities, cycles, the package
    /// lock, and the Definition tests of every changed Definition inside the Draft (§12.2 step 3).
    pub async fn draft_validate_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<ValidationReport, GitError> {
        let draft = self.draft_status(draft_id)?;
        self.authorize(&draft, principal, "validate", true)?;
        let span = self.start_span(&self.span_ctx(), "draft.validate", principal);
        let res = self.validate_inner(draft).await;
        let attrs = match &res {
            Ok(r) => json!({
                "draft_id": draft_id,
                "valid": r.valid,
                "issues": r.issues.len(),
            }),
            Err(_) => json!({ "draft_id": draft_id }),
        };
        finish_span(span, &res, attrs);
        res
    }

    async fn validate_inner(&self, draft: DraftRecord) -> Result<ValidationReport, GitError> {
        let draft_id = draft.draft_id.as_str();
        let changes_before = draft.changes.len();
        let wt = Path::new(&draft.worktree_path);
        let graph =
            load_workspace_graph(wt, &self.data_dir, &draft.base_revision, 0, false).await?;
        let mut report = validate_graph_with_contracts(&graph, &self.store, false);
        merge_issues(&mut report, lock_issues(wt));
        // Report grants the draft's editors lack (the activator is checked again on activation).
        let editors = dash_core::draft_editor_principals(&self.store, draft_id, &draft.created_by);
        let authz = dash_core::check_draft_activation_authority(
            &self.store,
            &self.engine.graph.load_full(),
            &graph,
            &editors,
        );
        if !authz.is_empty() {
            report.valid = false;
            report.issues.extend(authz);
        }

        let changed = self.changed_definitions(&draft).await?;
        if !changed.is_empty() {
            let tests = run_workspace_definition_tests(wt, &self.data_dir, Some(&changed)).await?;
            let failures: Vec<ValidationIssue> = tests
                .iter()
                .filter(|t| t.failed > 0)
                .map(|t| ValidationIssue {
                    severity: "error".into(),
                    code: "DEFINITION_TEST_FAILED".into(),
                    target: t.definition_id.clone(),
                    message: format!("{} Definition test(s) failed", t.failed),
                })
                .collect();
            merge_issues(&mut report, failures);
        }

        let report_json = serde_json::to_value(&report).unwrap_or_default();
        let valid = report.valid;
        self.update_record(draft_id, |r| {
            if DRAFT_STATUSES_FINAL.contains(&r.status.as_str())
                || r.changes.len() != changes_before
            {
                return Ok(()); // the draft moved on; this report no longer describes it
            }
            r.validation_report = Some(report_json);
            if valid && r.status == "open" {
                r.status = "validated".into();
            }
            Ok(())
        })?;
        Ok(report)
    }

    /// Preview as the Administrator (trusted in-process callers).
    pub async fn draft_preview_and_test(
        &self,
        draft_id: &str,
    ) -> Result<PreviewTestReport, GitError> {
        self.draft_preview_and_test_as(draft_id, &Principal::administrator())
            .await
    }

    /// Run isolated Preview + Definition tests + all Scenario tests against Mocks (§12.2 steps 3-4).
    pub async fn draft_preview_and_test_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<PreviewTestReport, GitError> {
        let draft = self.draft_status(draft_id)?;
        self.authorize(&draft, principal, "preview", true)?;
        if DRAFT_STATUSES_FINAL.contains(&draft.status.as_str()) {
            return Err(GitError::Workspace(format!(
                "draft '{draft_id}' is already '{}'",
                draft.status
            )));
        }
        self.preview_inner(&draft, principal, &self.span_ctx())
            .await
    }

    async fn preview_inner(
        &self,
        draft: &DraftRecord,
        principal: &Principal,
        trace: &SpanCtx,
    ) -> Result<PreviewTestReport, GitError> {
        let span = self.start_span(trace, "draft.preview", principal);
        let res = self.preview_body(draft).await;
        let attrs = match &res {
            Ok(r) => json!({
                "draft_id": draft.draft_id,
                "all_passed": r.all_passed,
                "tested_tree": r.tested_tree,
            }),
            Err(_) => json!({ "draft_id": draft.draft_id }),
        };
        finish_span(span, &res, attrs);
        res
    }

    async fn preview_body(&self, draft: &DraftRecord) -> Result<PreviewTestReport, GitError> {
        let draft_id = draft.draft_id.as_str();
        let wt = Path::new(&draft.worktree_path);
        let changes_before = draft.changes.len();
        let tree_before = git_stage_all_tree(wt).await?;

        let changed_defs = self.changed_definitions(draft).await?;
        let target_slice = if changed_defs.is_empty() {
            None
        } else {
            Some(changed_defs.as_slice())
        };
        let mut report = run_preview_and_scenarios(wt, &self.data_dir, target_slice).await?;
        let lock = lock_issues(wt);
        if !lock.is_empty() {
            merge_issues(&mut report.validation, lock);
            report.all_passed = false;
        }

        let tree_after = git_stage_all_tree(wt).await?;
        if tree_before == tree_after {
            report.tested_tree = Some(tree_before);
        } else {
            merge_issues(
                &mut report.validation,
                vec![ValidationIssue {
                    severity: "error".into(),
                    code: "DRAFT_CHANGED_DURING_PREVIEW".into(),
                    target: draft_id.to_string(),
                    message: "draft files changed while Preview was running; run Preview again"
                        .into(),
                }],
            );
            report.all_passed = false;
            report.tested_tree = None;
        }

        let val_json = serde_json::to_value(&report.validation).unwrap_or_default();
        let test_json = serde_json::to_value(&report).unwrap_or_default();
        let passed = report.all_passed;
        self.update_record(draft_id, |r| {
            if DRAFT_STATUSES_FINAL.contains(&r.status.as_str())
                || r.changes.len() != changes_before
            {
                return Ok(()); // a concurrent mutation superseded this report; keep it unapproved
            }
            r.validation_report = Some(val_json);
            r.test_report = Some(test_json);
            if passed {
                r.status = "previewed".into();
            }
            Ok(())
        })?;
        Ok(report)
    }

    /// Definitions whose identity differs between the draft and its base Revision.
    async fn changed_definitions(&self, draft: &DraftRecord) -> Result<Vec<String>, GitError> {
        let wt = Path::new(&draft.worktree_path);
        let base_dir = tempfile::tempdir()?;
        git_export_tree(&self.workspace_dir, &draft.base_revision, base_dir.path()).await?;
        let base_graph = load_workspace_graph(
            base_dir.path(),
            &self.data_dir,
            &draft.base_revision,
            0,
            true,
        )
        .await?;
        let draft_graph =
            load_workspace_graph(wt, &self.data_dir, &draft.base_revision, 0, true).await?;
        let mut changed = Vec::new();
        for (id, def) in &draft_graph.definitions {
            let unchanged = base_graph
                .definitions
                .get(id)
                .map(|b| b.manifest.identity_version == def.manifest.identity_version)
                .unwrap_or(false);
            if !unchanged {
                changed.push(id.clone());
            }
        }
        Ok(changed)
    }

    /// Rebase as the Administrator (trusted in-process callers).
    pub async fn draft_rebase(&self, draft_id: &str) -> Result<DraftRecord, GitError> {
        self.draft_rebase_as(draft_id, &Principal::administrator())
            .await
    }

    /// Explicitly rebase a stale Draft onto the current active Revision (§12.3).
    /// Draft changes (including deletions) are computed from the draft tree, merged with a
    /// deterministic 3-way merge (`git merge-file`) in temporary files, and applied to the
    /// worktree only when there is no conflict. A conflict leaves the draft untouched. A
    /// successful rebase clears all prior validation and test results.
    pub async fn draft_rebase_as(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<DraftRecord, GitError> {
        let ctx = self.span_ctx();
        let span = self.start_span(&ctx, "draft.rebase", principal);
        let res = self.rebase_inner(draft_id, principal).await;
        let attrs = match &res {
            Ok(r) => json!({ "draft_id": draft_id, "base_revision": r.base_revision }),
            Err(GitError::MergeConflict(files)) => {
                json!({ "draft_id": draft_id, "conflicts": files })
            }
            Err(_) => json!({ "draft_id": draft_id }),
        };
        finish_span(span, &res, attrs);
        res
    }

    async fn rebase_inner(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<DraftRecord, GitError> {
        // Holds the busy slot for the whole rebase: no mutation can write into the worktree
        // between the draft tree snapshot and the application of the merge plan.
        let draft = self.begin_mutation(draft_id, principal, "rebase")?;
        let active_rev = self.active_revision().await?;
        if draft.base_revision == active_rev {
            return Ok(draft.draft.clone());
        }

        let wt = Path::new(&draft.worktree_path);
        let base = draft.base_revision.clone();
        let draft_tree = git_stage_all_tree(wt).await?;
        let draft_changed: BTreeSet<String> = git_diff_name_status(wt, &base, &draft_tree)
            .await?
            .into_iter()
            .map(|(_, p)| p)
            .collect();
        let upstream = git_diff_name_status(&self.workspace_dir, &base, &active_rev).await?;

        let mut plan: Vec<(String, Option<Vec<u8>>)> = Vec::new();
        let mut conflicts = Vec::new();
        for (status, rel) in &upstream {
            let theirs = git_show_file(&self.workspace_dir, &active_rev, rel).await;
            if *status != 'D' && theirs.is_none() {
                // Never turn an unreadable upstream file into a silent delete / no-op (§12.3)
                return Err(GitError::Workspace(format!(
                    "rebase refused: upstream file '{rel}' (status {status}) could not be read from {}",
                    short(&active_rev)
                )));
            }
            if !draft_changed.contains(rel) {
                plan.push((rel.clone(), theirs));
                continue;
            }
            let ours = std::fs::read(wt.join(rel)).ok();
            if ours == theirs {
                continue; // both sides made the identical change
            }
            match (ours, theirs) {
                (Some(ours_b), Some(theirs_b)) => {
                    let base_b = git_show_file(&self.workspace_dir, &base, rel)
                        .await
                        .unwrap_or_default();
                    match merge_file_bytes(wt, &ours_b, &base_b, &theirs_b).await? {
                        Some(merged) => plan.push((rel.clone(), Some(merged))),
                        None => conflicts.push(rel.clone()),
                    }
                }
                // modify/delete or delete/modify: never pick a side silently
                _ => conflicts.push(rel.clone()),
            }
        }

        if !conflicts.is_empty() {
            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "draft.rebase",
                draft_id,
                "conflict",
                serde_json::json!({ "onto": active_rev, "conflicts": conflicts }),
            );
            return Err(GitError::MergeConflict(conflicts));
        }

        for (rel, content) in plan {
            let target = wt.join(&rel);
            match content {
                Some(bytes) => {
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(&target, bytes)?;
                }
                None => {
                    if target.exists() {
                        std::fs::remove_file(&target)?;
                    }
                }
            }
        }

        let old_base = base.clone();
        let new_base = active_rev.clone();
        let pid = principal.id.clone();
        let rec = self.update_record(draft_id, move |r| {
            r.base_revision = new_base.clone();
            r.status = "open".into();
            // Rebasing invalidates prior validation & test results (§12.3)
            r.validation_report = None;
            r.test_report = None;
            r.changes.push(format!(
                "rebase:{}..{} by {pid}",
                short(&old_base),
                short(&new_base)
            ));
            Ok(())
        })?;
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "draft.rebase",
            draft_id,
            "ok",
            serde_json::json!({ "from": base, "onto": active_rev }),
        );
        Ok(rec)
    }

    /// Activate a validated & tested Draft atomically (§12.2):
    /// rejects stale base revisions, prepares replacement Instances, creates one Git commit,
    /// swaps `ArcSwap<ConfiguredGraph>`, updates the activation journal, and attempts push.
    pub async fn draft_activate(
        &self,
        draft_id: &str,
        principal: &Principal,
    ) -> Result<ActivationResult, GitError> {
        if !principal.ceiling.can_activate_drafts && !principal.ceiling.unrestricted {
            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "draft.activate",
                draft_id,
                "denied",
                serde_json::json!({ "reason": "principal lacks can_activate_drafts" }),
            );
            return Err(GitError::Workspace(
                "principal lacks authority to activate drafts".into(),
            ));
        }
        let draft = self.draft_status(draft_id)?;
        self.authorize(&draft, principal, "activate", false)?;

        let ctx = self.span_ctx();
        let span = self.start_span(&ctx, "draft.activate", principal);
        let child = ctx.child(&span.span_id);
        let staged = {
            let _guard = self.activate_lock.lock().await;
            match self.acquire_busy(draft_id, "activate") {
                Ok(_busy) => match self.require_mutable_status(draft_id) {
                    Ok(draft) => {
                        self.activate_locked(draft, principal, ActivationMode::Draft, &child)
                            .await
                    }
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            }
        };
        finish_span(
            span,
            &staged,
            json!({ "draft_id": draft_id, "mode": "draft" }),
        );
        // The activation lock and the busy slot are released: a slow remote never blocks
        // other activations or reloads (§12.2 step 8).
        self.push_activation(staged?, principal).await
    }

    async fn activate_locked(
        &self,
        draft: DraftRecord,
        principal: &Principal,
        mode: ActivationMode,
        trace: &SpanCtx,
    ) -> Result<PendingPush, GitError> {
        let draft_id = draft.draft_id.clone();
        let active_rev = self.active_revision().await?;
        if draft.base_revision != active_rev {
            return Err(GitError::Workspace(format!(
                "stale draft base revision '{}'; active revision is '{}'. Call draft_rebase first (§12.3).",
                draft.base_revision, active_rev
            )));
        }
        let wt = PathBuf::from(&draft.worktree_path);

        // Trust a cached report only if it was computed for exactly the current draft content
        let current_tree = git_stage_all_tree(&wt).await?;

        // State migrations must be possible before anything is committed (§8). Checked before
        // Preview so a draft whose State cannot migrate is refused with that reason.
        let candidate_graph =
            load_workspace_graph(&wt, &self.data_dir, &active_rev, 0, false).await?;
        self.engine
            .state
            .check_graph_state(&candidate_graph)
            .map_err(|e| {
                GitError::Workspace(format!("{e}; activation refused, live State preserved"))
            })?;
        let cached: Option<PreviewTestReport> = draft
            .test_report
            .clone()
            .and_then(|v| serde_json::from_value(v).ok());
        let report = match cached {
            Some(r) if r.all_passed && r.tested_tree.as_deref() == Some(current_tree.as_str()) => r,
            _ => self.preview_inner(&draft, principal, trace).await?,
        };
        if !report.all_passed {
            return Err(GitError::Workspace(format!(
                "draft validation/tests failed; live Generation preserved untouched: {}",
                serde_json::to_string(&report).unwrap_or_default()
            )));
        }
        let tree = report
            .tested_tree
            .clone()
            .ok_or_else(|| GitError::Workspace("draft test report has no tested tree".into()))?;

        // Live validation (Service principal grants etc. against the real store) + lock check
        let mut live_val = validate_graph_with_contracts(&candidate_graph, &self.store, false);
        merge_issues(&mut live_val, lock_issues(&wt));
        if !live_val.valid {
            return Err(GitError::Workspace(format!(
                "live validation errors: {:?}",
                live_val.issues
            )));
        }

        // Principal-aware authority (§5.2, §11.3, ADR-0002): every editor of the draft and the
        // activating Principal must hold the secrets / Service principal ceilings the change reaches.
        let mut authz_principals =
            dash_core::draft_editor_principals(&self.store, &draft_id, &draft.created_by);
        authz_principals.push(principal.clone());
        let authz_issues = dash_core::check_draft_activation_authority(
            &self.store,
            &self.engine.graph.load_full(),
            &candidate_graph,
            &authz_principals,
        );
        if !authz_issues.is_empty() {
            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "draft.activate",
                &draft_id,
                "denied",
                serde_json::json!({ "issues": authz_issues }),
            );
            return Err(GitError::Workspace(format!(
                "activation denied (pending grant): {:?}",
                authz_issues
            )));
        }

        // Build the replacement live Generation off to the side (§12.2 step 5).
        // `--use-mocks` substitutes Mock connectors but keeps live authority (SQLite grants).
        let old_graph = self.engine.graph.load_full();
        let mut live_graph = candidate_graph;
        live_graph.substitute_mocks = self.use_mocks;
        if git_stage_all_tree(&wt).await? != tree {
            return Err(GitError::Workspace(
                "draft content changed during activation; run draft_preview again".into(),
            ));
        }
        let (carried_over, rebuilt, downstream_affected) =
            compute_generation_diff(&old_graph, &live_graph);
        let desired_bundles = instance_bundles(&live_graph);
        let rebuild_set: BTreeSet<&String> = rebuilt.iter().collect();
        // Prepare under the candidate Generation's `[resources]` (the live budget resolver still
        // reads the outgoing one), and also validate Instances whose only change is their budget.
        let pool = &self.engine.pool;
        let to_prepare: Vec<(CompiledInstanceBundle, dash_luau::ResourceBudget)> = desired_bundles
            .iter()
            .filter(|b| has_luau_runtime(b))
            .filter_map(|b| {
                let id = &b.instance_id;
                let new_budget = pool.budget_with_overrides(
                    live_graph.workspace.resources.overrides_for(id).as_ref(),
                );
                let old_budget = pool.budget_with_overrides(
                    old_graph.workspace.resources.overrides_for(id).as_ref(),
                );
                (rebuild_set.contains(id) || !new_budget.same_vm_limits(&old_budget))
                    .then(|| (b.clone(), new_budget))
            })
            .collect();
        let prepared = pool
            .prepare_replacements_with(&to_prepare)
            .await
            .map_err(|(inst, e)| {
                GitError::Workspace(format!(
                    "replacement Instance '{inst}' could not be prepared within resource limits: {e}; live Generation preserved untouched"
                ))
            })?;

        // Workspace precondition: never overwrite or delete external modifications (§12.3)
        self.check_workspace_precondition(&active_rev, &tree, mode)
            .await?;

        // Activation journal (`started`) + exactly one commit for the whole draft (§12.2 step 6)
        let journal_id = format!("jrn_{}", Uuid::now_v7().simple());
        let journal = |phase: &str, target: Option<&str>, gen: Option<i64>, err: Option<String>| {
            ActivationJournalRecord {
                journal_id: journal_id.clone(),
                draft_id: draft_id.clone(),
                base_revision: active_rev.clone(),
                target_revision: target.map(|s| s.to_string()),
                generation_id: gen,
                phase: phase.into(),
                error_message: err,
                updated_at: Utc::now().to_rfc3339(),
            }
        };
        self.store
            .upsert_activation_journal(&journal("started", None, None, None))
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        self.simulated_crash("after_journal_started")?;

        let (summary, message) = commit_message(&draft, principal);
        let base_tree = git_tree_of(&self.workspace_dir, &active_rev).await?;
        let new_commit = if tree == base_tree {
            active_rev.clone() // nothing to commit; the Generation is still rebuilt
        } else {
            let author_email = format!("{}@dash.local", draft.created_by);
            let committer_email = format!("{}@dash.local", principal.id);
            match git_commit_tree(
                &self.workspace_dir,
                &tree,
                &active_rev,
                &message,
                (&draft.created_by, &author_email),
                (&principal.id, &committer_email),
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    let _ = self.store.upsert_activation_journal(&journal(
                        "failed",
                        None,
                        None,
                        Some(format!("commit failed: {e}")),
                    ));
                    return Err(e);
                }
            }
        };
        self.simulated_crash("after_commit_object")?;
        // Record the target before HEAD moves so crash recovery can decide deterministically
        self.store
            .upsert_activation_journal(&journal("started", Some(&new_commit), None, None))
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        self.simulated_crash("after_target_journalled")?;
        if new_commit != active_rev {
            if let Err(e) = self.advance_workspace(&active_rev, &new_commit, mode).await {
                if is_simulated_crash(&e) {
                    return Err(e);
                }
                let _ = self.store.upsert_activation_journal(&journal(
                    "failed",
                    Some(&new_commit),
                    None,
                    Some(format!("advancing the Workspace failed: {e}")),
                ));
                return Err(e);
            }
        }
        if let Err(e) = self.store.upsert_activation_journal(&journal(
            "committed",
            Some(&new_commit),
            None,
            None,
        )) {
            self.rollback_workspace(&active_rev, &new_commit, mode)
                .await;
            let _ = self.store.upsert_activation_journal(&journal(
                "failed",
                Some(&new_commit),
                None,
                Some(format!("journal write failed: {e}")),
            ));
            return Err(GitError::Workspace(e.to_string()));
        }
        self.simulated_crash("after_committed")?;

        // Record the Generation only once the new graph is fully built, right before the swap
        live_graph.revision_commit = new_commit.clone();
        let new_gen_id = match self
            .store
            .record_generation(&new_commit, &live_graph.graph_digest())
        {
            Ok(id) => id,
            Err(e) => {
                // Restore the old Revision and mark the commit as not active (§12.2 step 7)
                self.rollback_workspace(&active_rev, &new_commit, mode)
                    .await;
                let _ = self.store.upsert_activation_journal(&journal(
                    "failed",
                    Some(&new_commit),
                    None,
                    Some(format!(
                        "recording the Generation failed: {e}; commit not active"
                    )),
                ));
                return Err(GitError::Workspace(e.to_string()));
            }
        };
        live_graph.generation_id = new_gen_id;
        self.simulated_crash("after_generation_recorded")?;

        // Migrate State transactionally and seed defaults before touching the live runtime; a
        // failed migration rolls back and leaves the live Generation and its State untouched (§8).
        let migrate_span = self.start_span(trace, "state.migrate", principal);
        let migrated = self
            .engine
            .state
            .apply_graph_state(&live_graph, new_gen_id, principal);
        let migrate_attrs = match &migrated {
            Ok(r) => json!({
                "generation_id": new_gen_id,
                "applied": r.applied,
                "seeded": r.seeded,
                "migrations": r.migrations,
            }),
            Err(e) => json!({ "generation_id": new_gen_id, "error": e.to_string() }),
        };
        finish_span(migrate_span, &migrated, migrate_attrs);
        if let Err(e) = migrated {
            self.rollback_workspace(&active_rev, &new_commit, mode)
                .await;
            let _ = self.store.upsert_activation_journal(&journal(
                "failed",
                Some(&new_commit),
                Some(new_gen_id),
                Some(format!("{e}; commit not active")),
            ));
            return Err(GitError::Workspace(format!(
                "{e}; live Generation preserved"
            )));
        }

        let _ = self.store.record_revision(
            &new_commit,
            Some(&active_rev),
            &principal.id,
            principal.kind.as_str(),
            &summary,
        );

        // Atomic Generation swap first; side effects keyed on the new Generation follow (§12.2 step 7)
        let swap_span = self.start_span(trace, "generation.swap", principal);
        let history_cfg = live_graph.workspace.history.clone();
        self.engine.graph.store(Arc::new(live_graph));
        let rebuilt_installed = self.engine.pool.install_prepared(prepared);
        self.engine.pool.sync_generation(&desired_bundles);
        self.engine
            .queries
            .mark_generation_stale(&downstream_affected, new_gen_id);
        // Dashboard State defaults were seeded by apply_graph_state above.
        let _ = self.engine.history.sync_series_configs(&history_cfg);
        self.engine.automations.register_consumers();
        swap_span.finish(
            "ok",
            json!({
                "generation_id": new_gen_id,
                "revision": new_commit,
                "rebuilt": rebuilt.len(),
                "carried_over": carried_over.len(),
            }),
        );
        self.simulated_crash("after_swap")?;
        // Mark the draft activated before the journal leaves `committed`: a crash in between is
        // then still reconciled at startup, and a draft whose content is live never stays open
        // (e.g. after a crash before the push).
        remove_worktree(&self.workspace_dir, &wt).await;
        let activated_changes = draft.changes.len();
        self.update_record(&draft_id, |r| {
            if r.changes.len() != activated_changes {
                // Mutations hold the busy slot, so this cannot happen; never hide it if it does.
                tracing::error!(
                    "draft '{}' recorded {} change(s) during its activation that are not in commit {}",
                    r.draft_id,
                    r.changes.len().saturating_sub(activated_changes),
                    short(&new_commit)
                );
            }
            r.status = "activated".into();
            Ok(())
        })?;
        if let Err(e) = self.store.upsert_activation_journal(&journal(
            "activated",
            Some(&new_commit),
            Some(new_gen_id),
            None,
        )) {
            tracing::warn!("activation journal write failed after swap: {e}");
        }
        tracing::debug!("installed {} prepared VMs", rebuilt_installed.len());

        // Affected downstream Query results stay `stale` until recomputed: recompute them now
        // so subscribers receive fresh values without waiting for the next poll.
        let stale_instances: Vec<String> = downstream_affected.iter().cloned().collect();
        if !stale_instances.is_empty() {
            let queries = self.engine.queries.clone();
            let ids = stale_instances.clone();
            tokio::spawn(async move {
                queries.invalidate_instances(&ids).await;
            });
        }

        let activated_journal = journal("activated", Some(&new_commit), Some(new_gen_id), None);
        Ok(PendingPush {
            result: ActivationResult {
                revision_commit: new_commit,
                generation_id: new_gen_id,
                carried_over_instances: carried_over,
                rebuilt_instances: rebuilt,
                sync_status: "unsynced".into(),
                sync_error: None,
                stale_instances,
            },
            journal: activated_journal,
            draft_author: draft.created_by.clone(),
            trace: trace.clone(),
        })
    }

    /// Push an activation that is already live and final, then audit it. Runs without the
    /// activation lock or the draft's busy slot; a push failure or timeout leaves the Revision
    /// active and `unsynced` (§12.2 step 8).
    async fn push_activation(
        &self,
        pending: PendingPush,
        principal: &Principal,
    ) -> Result<ActivationResult, GitError> {
        let PendingPush {
            mut result,
            journal,
            draft_author,
            trace,
        } = pending;
        self.simulated_crash("before_push")?;
        let (sync_status, sync_error) = self
            .attempt_push_traced(&result.revision_commit, principal, &trace)
            .await;
        if sync_status == "synced" {
            let mut pushed = journal.clone();
            pushed.phase = "pushed".into();
            pushed.updated_at = Utc::now().to_rfc3339();
            let _ = self.store.upsert_activation_journal(&pushed);
        }
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "draft.activate",
            &journal.draft_id,
            "activated",
            serde_json::json!({
                "revision": result.revision_commit,
                "generation": result.generation_id,
                "sync_status": sync_status,
                "author": draft_author,
            }),
        );
        result.sync_status = sync_status;
        result.sync_error = sync_error;
        Ok(result)
    }

    async fn check_workspace_precondition(
        &self,
        base: &str,
        tree: &str,
        mode: ActivationMode,
    ) -> Result<(), GitError> {
        match mode {
            ActivationMode::Draft => {
                let changes = git_tracked_changes(&self.workspace_dir).await?;
                if !changes.is_empty() {
                    return Err(GitError::Workspace(format!(
                        "Workspace has uncommitted external modifications {changes:?}; activation refused so they are not overwritten. Turn them into a draft proposal with `dash reload`, or commit/revert them and rebase the draft (§12.3)."
                    )));
                }
                let added: Vec<String> = git_diff_name_status(&self.workspace_dir, base, tree)
                    .await?
                    .into_iter()
                    .filter(|(st, _)| *st == 'A')
                    .map(|(_, p)| p)
                    .filter(|p| std::fs::symlink_metadata(self.workspace_dir.join(p)).is_ok())
                    .collect();
                if !added.is_empty() {
                    return Err(GitError::Workspace(format!(
                        "untracked Workspace files {added:?} would be overwritten by this draft; move them away or add them through a draft (§12.3)"
                    )));
                }
                Ok(())
            }
            ActivationMode::Reload => {
                let now = git_snapshot_worktree_tree(&self.workspace_dir).await?;
                if now != tree {
                    return Err(GitError::Workspace(
                        "Workspace files changed while the reload draft was being tested; run reload again".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    async fn advance_workspace(
        &self,
        base: &str,
        new_commit: &str,
        mode: ActivationMode,
    ) -> Result<(), GitError> {
        let ws = &self.workspace_dir;
        git_update_head(ws, new_commit, base, "dash: activate draft").await?;
        self.simulated_crash("after_head_moved")?;
        let res = match mode {
            ActivationMode::Draft => git_read_tree_fast_forward(ws, base, new_commit).await,
            ActivationMode::Reload => git_read_tree_index_only(ws, new_commit).await,
        };
        if let Err(e) = res {
            let _ =
                git_update_head(ws, base, new_commit, "dash: roll back failed activation").await;
            return Err(e);
        }
        Ok(())
    }

    /// Undo [`Self::advance_workspace`] (activation failed after the Workspace moved).
    async fn rollback_workspace(&self, base: &str, new_commit: &str, mode: ActivationMode) {
        if base == new_commit {
            return;
        }
        let ws = &self.workspace_dir;
        if let Err(e) =
            git_update_head(ws, base, new_commit, "dash: roll back failed activation").await
        {
            tracing::error!("rollback of HEAD to {base} failed: {e}");
            return;
        }
        let res = match mode {
            ActivationMode::Draft => git_read_tree_fast_forward(ws, new_commit, base).await,
            ActivationMode::Reload => git_read_tree_index_only(ws, base).await,
        };
        if let Err(e) = res {
            tracing::error!("rollback of the Workspace working tree to {base} failed: {e}");
        }
    }

    /// Turn external modifications of the Workspace working tree (edits, additions and
    /// deletions) into a draft proposal and run it through the full validation and activation
    /// pipeline (§12.3 "file watchers can create draft proposals").
    pub async fn reload_workspace(
        &self,
        principal: &Principal,
    ) -> Result<ActivationResult, GitError> {
        if !principal.ceiling.unrestricted
            && !(principal.ceiling.can_author_drafts && principal.ceiling.can_activate_drafts)
        {
            return Err(GitError::Workspace(
                "principal lacks authority to author and activate drafts".into(),
            ));
        }
        let ctx = self.span_ctx();
        let span = self.start_span(&ctx, "draft.activate", principal);
        let child = ctx.child(&span.span_id);
        let mut reload_draft_id = None;
        let staged = {
            let _guard = self.activate_lock.lock().await;
            let staged = async {
                let snapshot = git_snapshot_worktree_tree(&self.workspace_dir).await?;
                let draft = self.draft_begin(principal).await?;
                reload_draft_id = Some(draft.draft_id.clone());
                let _busy = self.acquire_busy(&draft.draft_id, "activate")?;
                let wt = PathBuf::from(&draft.worktree_path);
                // Mirror the Workspace exactly, deletions included
                run_git(&wt, &["read-tree", "--reset", "-u", &snapshot]).await?;
                self.record_draft_mutation(&draft.draft_id, principal, RELOAD_CHANGE.into())?;
                let draft = self.require_mutable_status(&draft.draft_id)?;
                self.activate_locked(draft, principal, ActivationMode::Reload, &child)
                    .await
            }
            .await;
            if let (Err(e), Some(id)) = (&staged, &reload_draft_id) {
                // A reload draft is never reused (each reload proposes a fresh one), so a failed
                // reload must not leave an open draft and a Git worktree behind. A simulated
                // crash leaves everything as a killed process would.
                if !is_simulated_crash(e) {
                    self.discard_failed_reload(id, principal, e).await;
                }
            }
            staged
        };
        finish_span(
            span,
            &staged,
            json!({ "draft_id": reload_draft_id, "mode": "reload" }),
        );
        self.push_activation(staged?, principal).await
    }

    /// Abort the draft of a failed `dash reload` and remove its worktree, unless its content
    /// went live after all.
    async fn discard_failed_reload(&self, draft_id: &str, principal: &Principal, err: &GitError) {
        let Ok(draft) = self.draft_status(draft_id) else {
            return;
        };
        if DRAFT_STATUSES_FINAL.contains(&draft.status.as_str()) {
            return;
        }
        remove_worktree(&self.workspace_dir, Path::new(&draft.worktree_path)).await;
        let aborted = self.update_record(draft_id, |r| {
            if r.status != "activated" {
                r.status = "aborted".into();
            }
            Ok(())
        });
        if let Err(e) = aborted {
            tracing::warn!("could not abort failed reload draft '{draft_id}': {e}");
            return;
        }
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "draft.abort",
            draft_id,
            "aborted",
            serde_json::json!({ "reason": "reload failed", "error": err.to_string() }),
        );
    }

    pub async fn git_status(&self) -> Result<GitStatusReport, GitError> {
        let head = git_head(&self.workspace_dir).await.ok();
        let porcelain = run_git(&self.workspace_dir, &["status", "--porcelain"]).await?;
        let changed_files: Vec<String> = porcelain
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|s| s.trim().to_string())
            .collect();
        let branch = run_git(&self.workspace_dir, &["rev-parse", "--abbrev-ref", "HEAD"])
            .await
            .unwrap_or_else(|_| "main".into());
        let remote_url = self.sanitized_origin_url().await;
        let push_branch = self.engine.graph.load().workspace.git_branch.clone();
        let sync = derive_sync_state(&self.workspace_dir, &self.store, &push_branch).await;
        let active_gen_rev = self.active_generation_revision();
        let head_diverged = head.as_deref() != Some(active_gen_rev.as_str());
        Ok(GitStatusReport {
            head_commit: head,
            branch,
            dirty: !changed_files.is_empty(),
            changed_files,
            remote_url,
            sync_status: sync.sync_status,
            last_error: sync.last_error,
            active_generation_revision: Some(active_gen_rev),
            head_diverged,
        })
    }

    pub async fn git_history(&self, limit: usize) -> Result<Vec<GitCommitInfo>, GitError> {
        git_history(&self.workspace_dir, limit).await
    }

    pub async fn git_sync(&self, principal: &Principal) -> Result<GitSyncRecord, GitError> {
        let head = self.active_revision().await?;
        let ctx = self.span_ctx();
        let span = self.start_span(&ctx, "git.sync", principal);
        let (sync_status, last_error) = self
            .attempt_push_traced(&head, principal, &ctx.child(&span.span_id))
            .await;
        span.finish(
            if sync_status == "unsynced" {
                "error"
            } else {
                "ok"
            },
            json!({ "revision": head, "sync_status": sync_status }),
        );
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "git.sync",
            &head,
            &sync_status,
            serde_json::json!({ "error": last_error }),
        );
        self.store
            .get_git_sync_state()
            .map_err(|e| GitError::Workspace(e.to_string()))?
            .ok_or_else(|| GitError::Workspace("git sync state missing".into()))
    }

    /// Test hook: bound every push by `timeout` instead of [`crate::git_cli::DEFAULT_PUSH_TIMEOUT`].
    #[doc(hidden)]
    pub fn set_push_timeout_for_tests(&self, timeout: std::time::Duration) {
        *self
            .push_timeout
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = timeout;
    }

    /// The `origin` URL without credentials. Removed credentials are registered with the
    /// redactor, so Git output that echoes them is masked too (§5.2, §12.1).
    async fn sanitized_origin_url(&self) -> Option<String> {
        let raw = git_remote_url_raw(&self.workspace_dir, "origin").await?;
        let clean = sanitize_remote_url(&raw);
        for secret in &clean.removed_secrets {
            self.store.redactor().register_secret(secret);
        }
        Some(clean.url)
    }

    async fn attempt_push_traced(
        &self,
        commit: &str,
        principal: &Principal,
        trace: &SpanCtx,
    ) -> (String, Option<String>) {
        let span = self.start_span(trace, "git.push", principal);
        let started = std::time::Instant::now();
        let (status, error) = self.attempt_push(commit).await;
        span.finish(
            if status == "unsynced" { "error" } else { "ok" },
            json!({
                "revision": commit,
                "sync_status": status,
                "error": error,
                "elapsed_ms": started.elapsed().as_millis() as u64,
            }),
        );
        (status, error)
    }

    /// Push `commit` (or the running Generation's Revision, when that already contains it) to
    /// `origin`. Pushes are serialized by `push_lock`, so successive activations whose pushes
    /// overlap never race: a later push of a descendant supersedes an earlier one.
    async fn attempt_push(&self, commit: &str) -> (String, Option<String>) {
        let _push = self.push_lock.lock().await;
        let remote_url = self.sanitized_origin_url().await;
        let live = self.active_generation_revision();
        let commit = if live != commit && git_is_ancestor(&self.workspace_dir, commit, &live).await
        {
            live
        } else {
            commit.to_string()
        };
        let commit = commit.as_str();
        let Some(ref url) = remote_url else {
            let rec = GitSyncRecord {
                active_revision: Some(commit.to_string()),
                remote_name: "origin".into(),
                remote_url: None,
                sync_status: "no_remote".into(),
                last_error: None,
                updated_at: Utc::now().to_rfc3339(),
            };
            let _ = self.store.set_git_sync_state(&rec);
            return ("no_remote".into(), None);
        };

        let branch = push_branch(&self.engine.graph.load().workspace.git_branch);
        let timeout = *self
            .push_timeout
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match git_push_commit(&self.workspace_dir, "origin", commit, &branch, timeout).await {
            Ok(()) => {
                let rec = GitSyncRecord {
                    active_revision: Some(commit.to_string()),
                    remote_name: "origin".into(),
                    remote_url: Some(url.clone()),
                    sync_status: "synced".into(),
                    last_error: None,
                    updated_at: Utc::now().to_rfc3339(),
                };
                let _ = self.store.set_git_sync_state(&rec);
                ("synced".into(), None)
            }
            Err(e) => {
                let msg = self.store.redactor().redact_str(&e.to_string());
                let rec = GitSyncRecord {
                    active_revision: Some(commit.to_string()),
                    remote_name: "origin".into(),
                    remote_url: Some(url.clone()),
                    sync_status: "unsynced".into(),
                    last_error: Some(msg.clone()),
                    updated_at: Utc::now().to_rfc3339(),
                };
                let _ = self.store.set_git_sync_state(&rec);
                ("unsynced".into(), Some(msg))
            }
        }
    }

    fn require_mutable_status(&self, draft_id: &str) -> Result<DraftRecord, GitError> {
        let draft = self.draft_status(draft_id)?;
        if DRAFT_STATUSES_FINAL.contains(&draft.status.as_str()) {
            return Err(GitError::Workspace(format!(
                "draft '{draft_id}' is already '{}'",
                draft.status
            )));
        }
        Ok(draft)
    }

    /// Check out a draft for one mutation: accessible to `principal` with `can_author_drafts`,
    /// not busy, and still mutable once its busy slot is held. The slot stays taken until the
    /// returned guard is dropped, i.e. across the file writes and the change-log entry, so an
    /// activation, rebase or abort cannot interleave with the mutation.
    fn begin_mutation(
        &self,
        draft_id: &str,
        principal: &Principal,
        op: &str,
    ) -> Result<MutationGuard, GitError> {
        let draft = self.draft_status(draft_id)?;
        self.authorize(&draft, principal, op, true)?;
        let busy = self.acquire_busy(draft_id, op)?;
        // Re-read under the slot: an activation may have finished just before we took it
        let draft = self.require_mutable_status(draft_id)?;
        Ok(MutationGuard { draft, _busy: busy })
    }

    /// Trace context for a new Git transaction: inside a request it joins the request's trace
    /// (root spans link under the request span), otherwise it starts a fresh trace.
    fn span_ctx(&self) -> SpanCtx {
        let trace_id = dash_core::telemetry::current_request_trace()
            .map(|c| c.trace_id)
            .unwrap_or_else(dash_core::telemetry::Telemetry::new_trace_id);
        SpanCtx {
            trace_id,
            parent: None,
        }
    }

    fn start_span(
        &self,
        ctx: &SpanCtx,
        name: &str,
        principal: &Principal,
    ) -> dash_core::telemetry::SpanGuard {
        self.engine
            .telemetry
            .start_span(&ctx.trace_id, ctx.parent.as_deref(), name, &principal.id)
    }

    fn record_draft_mutation(
        &self,
        draft_id: &str,
        principal: &Principal,
        change: String,
    ) -> Result<DraftRecord, GitError> {
        let entry = format!("{change} by {}", principal.id);
        let rec = self.update_record(draft_id, |r| {
            if DRAFT_STATUSES_FINAL.contains(&r.status.as_str()) {
                return Err(GitError::Workspace(format!(
                    "draft '{draft_id}' is already '{}'",
                    r.status
                )));
            }
            r.changes.push(entry.clone());
            r.status = "open".into();
            r.validation_report = None;
            r.test_report = None;
            Ok(())
        })?;
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "draft.change",
            draft_id,
            "ok",
            serde_json::json!({ "change": change }),
        );
        Ok(rec)
    }
}

fn short(rev: &str) -> &str {
    &rev[..rev.len().min(12)]
}

fn push_branch(configured: &str) -> String {
    if configured.trim().is_empty() {
        "main".into()
    } else {
        configured.to_string()
    }
}

fn merge_issues(report: &mut ValidationReport, issues: Vec<ValidationIssue>) {
    if issues.iter().any(|i| i.severity == "error") {
        report.valid = false;
    }
    report.issues.extend(issues);
}

/// Split a change-log entry `"<change> by <principal_id>"`.
fn split_change(entry: &str) -> (&str, Option<&str>) {
    match entry.rsplit_once(" by ") {
        Some((c, p)) if !p.contains(' ') => (c, Some(p)),
        _ => (entry, None),
    }
}

/// Commit subject + full message with Principal attribution trailers (§11.3, §20).
fn commit_message(draft: &DraftRecord, activator: &Principal) -> (String, String) {
    let mut kinds: Vec<&str> = Vec::new();
    let mut contributors: Vec<&str> = Vec::new();
    for entry in &draft.changes {
        let (change, who) = split_change(entry);
        if !kinds.contains(&change) {
            kinds.push(change);
        }
        if let Some(p) = who {
            if p != draft.created_by && !contributors.contains(&p) {
                contributors.push(p);
            }
        }
    }
    let subject = if kinds.is_empty() {
        format!("Activate draft {}", draft.draft_id)
    } else {
        format!("Activate draft {}: {}", draft.draft_id, kinds.join(", "))
    };
    let mut msg = subject.clone();
    if !draft.changes.is_empty() {
        msg.push_str("\n\nChanges:");
        for entry in &draft.changes {
            msg.push_str(&format!("\n- {entry}"));
        }
    }
    msg.push_str(&format!("\n\nDash-Draft: {}", draft.draft_id));
    msg.push_str(&format!("\nDash-Author: {}", draft.created_by));
    for c in contributors {
        msg.push_str(&format!("\nDash-Contributor: {c}"));
    }
    msg.push_str(&format!("\nDash-Principal: {}", activator.id));
    msg.push_str(&format!(
        "\nDash-Principal-Kind: {}",
        activator.kind.as_str()
    ));
    (subject, msg)
}

fn instance_bundles(graph: &ConfiguredGraph) -> Vec<CompiledInstanceBundle> {
    let libs = graph.resolved_libraries();
    graph
        .instances
        .values()
        .filter_map(|inst| {
            graph
                .definitions
                .get(&inst.definition)
                .map(|def| CompiledInstanceBundle {
                    instance_id: inst.id.clone(),
                    manifest: def.manifest.clone(),
                    runtime_source: def.runtime_source.clone(),
                    config_digest: inst.config_digest(),
                    resolved_libraries: libs.clone(),
                })
        })
        .collect()
}

fn has_luau_runtime(b: &CompiledInstanceBundle) -> bool {
    !b.runtime_source.trim().is_empty() && !b.manifest.identity_version.starts_with("builtin:")
}

/// 3-way merge of byte contents with `git merge-file -p`; `None` on conflict.
async fn merge_file_bytes(
    cwd: &Path,
    ours: &[u8],
    base: &[u8],
    theirs: &[u8],
) -> Result<Option<Vec<u8>>, GitError> {
    let tmp = tempfile::tempdir()?;
    let ours_p = tmp.path().join("ours");
    let base_p = tmp.path().join("base");
    let theirs_p = tmp.path().join("theirs");
    std::fs::write(&ours_p, ours)?;
    std::fs::write(&base_p, base)?;
    std::fs::write(&theirs_p, theirs)?;
    let (code, out) = run_git_bytes(
        cwd,
        &[
            "merge-file",
            "-p",
            &ours_p.to_string_lossy(),
            &base_p.to_string_lossy(),
            &theirs_p.to_string_lossy(),
        ],
    )
    .await?;
    match code {
        0 => Ok(Some(out)),
        c if c > 0 => Ok(None),
        _ => Err(GitError::Command {
            cmd: "git merge-file -p".into(),
            stderr: "merge-file failed".into(),
        }),
    }
}

/// Check that `packages.lock` pins exactly what `packages.toml` requests (§4.4): every
/// top-level dependency has a locked entry with the same git URL, requested ref and subdir,
/// and every locked entry is reachable from a top-level dependency (no orphans).
pub fn verify_lock_matches_manifest(
    manifest: &PackagesToml,
    lock: &PackagesLock,
) -> Result<(), String> {
    let by_name: BTreeMap<&str, &crate::packages::LockedPackage> =
        lock.packages.iter().map(|p| (p.name.as_str(), p)).collect();
    let mut problems = Vec::new();
    for (name, spec) in &manifest.dependencies {
        match by_name.get(name.as_str()) {
            None => problems.push(format!(
                "dependency '{name}' is not pinned in packages.lock"
            )),
            Some(l) => {
                if l.git != spec.git || l.requested_ref != spec.git_ref || l.subdir != spec.subdir {
                    problems.push(format!(
                        "dependency '{name}' changed in packages.toml ({} @ {} {:?}) but packages.lock pins ({} @ {} {:?})",
                        spec.git, spec.git_ref, spec.subdir, l.git, l.requested_ref, l.subdir
                    ));
                }
            }
        }
    }
    let mut reachable: BTreeSet<&str> = BTreeSet::new();
    let mut frontier: Vec<&str> = manifest.dependencies.keys().map(|s| s.as_str()).collect();
    while let Some(n) = frontier.pop() {
        if !reachable.insert(n) {
            continue;
        }
        if let Some(l) = by_name.get(n) {
            frontier.extend(l.dependencies.iter().map(|s| s.as_str()));
        }
    }
    for p in &lock.packages {
        if !reachable.contains(p.name.as_str()) {
            problems.push(format!(
                "packages.lock pins '{}' which packages.toml no longer requires",
                p.name
            ));
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("; "))
    }
}

/// Lock/manifest consistency of a Workspace directory, with an actionable message.
fn check_lock_matches_manifest(dir: &Path) -> Result<(), String> {
    let manifest = read_packages_toml(dir).map_err(|e| e.to_string())?;
    let lock_exists = dir.join("packages.lock").exists();
    if !manifest.dependencies.is_empty() && !lock_exists {
        return Err(
            "packages.toml declares external dependencies but packages.lock is missing; external Packages are not loaded. Run `dependency_update` in a draft to resolve and pin them (§4.4)".into(),
        );
    }
    let lock = read_packages_lock(dir).map_err(|e| e.to_string())?;
    verify_lock_matches_manifest(&manifest, &lock).map_err(|e| {
        format!("packages.lock does not match packages.toml: {e}. Run `dependency_update` in a draft to re-resolve the lock (§4.4)")
    })
}

fn lock_issues(dir: &Path) -> Vec<ValidationIssue> {
    match check_lock_matches_manifest(dir) {
        Ok(()) => vec![],
        Err(message) => vec![ValidationIssue {
            severity: "error".into(),
            code: "LOCK_MISMATCH".into(),
            target: "packages.lock".into(),
            message,
        }],
    }
}

/// Load the graph of `head` exactly: from the Workspace directly when it is clean, otherwise
/// from an export of the Revision (uncommitted changes are reported, never activated).
async fn load_revision_graph(
    workspace_dir: &Path,
    data_dir: &Path,
    head: &str,
    use_mocks: bool,
    warnings: &mut Vec<String>,
) -> Result<ConfiguredGraph, GitError> {
    let porcelain = run_git(workspace_dir, &["status", "--porcelain"]).await?;
    if porcelain.trim().is_empty() {
        materialize_locked_packages(workspace_dir, data_dir).await?;
        let mut graph = load_workspace_graph(workspace_dir, data_dir, head, 1, false).await?;
        graph.substitute_mocks = use_mocks;
        return Ok(graph);
    }
    let changed: Vec<&str> = porcelain.lines().map(|l| l.trim()).take(20).collect();
    warnings.push(format!(
        "Workspace has uncommitted external modifications {changed:?}; the active Generation is built from HEAD {}. Use `dash reload` to propose them as a draft (§12.3)",
        short(head)
    ));
    let export = tempfile::tempdir()?;
    git_export_tree(workspace_dir, head, export.path()).await?;
    materialize_locked_packages(export.path(), data_dir).await?;
    let mut graph = load_workspace_graph(export.path(), data_dir, head, 1, false).await?;
    graph.substitute_mocks = use_mocks;
    Ok(graph)
}

/// Materialize the Packages pinned by the `packages.lock` of the tree being loaded (the Revision
/// itself, never an uncommitted working-tree lock) at their locked commits. Entries that are
/// already cached and verified are skipped; missing or corrupt cache entries are re-fetched.
/// The lockfile is never modified (§4.4).
async fn materialize_locked_packages(revision_dir: &Path, data_dir: &Path) -> Result<(), GitError> {
    let lock = read_packages_lock(revision_dir)?;
    fetch_locked_packages(data_dir, &lock).await?;
    Ok(())
}

/// Derive the remote sync state from local facts only (no network): HEAD counts as synced
/// when the last known remote-tracking ref contains it, or when the last successful push
/// recorded exactly this HEAD. Anything else is `unsynced` (§12.2 step 8: never falsely
/// report remote success).
async fn derive_sync_state(
    ws: &Path,
    store: &SqliteStore,
    configured_branch: &str,
) -> GitSyncRecord {
    let head = git_head(ws).await.ok();
    let remote_url = git_remote_url(ws, "origin").await;
    let rec = store.get_git_sync_state().ok().flatten();
    let now = Utc::now().to_rfc3339();
    let Some(url) = remote_url else {
        return GitSyncRecord {
            active_revision: head,
            remote_name: "origin".into(),
            remote_url: None,
            sync_status: "no_remote".into(),
            last_error: None,
            updated_at: now,
        };
    };
    let branch = push_branch(configured_branch);
    let mut synced = false;
    if let Some(ref h) = head {
        if let Some(tracking) = git_remote_tracking_commit(ws, "origin", &branch).await {
            synced = git_is_ancestor(ws, h, &tracking).await;
        }
        if !synced {
            synced = rec
                .as_ref()
                .map(|r| {
                    r.sync_status == "synced"
                        && r.active_revision.as_deref() == Some(h.as_str())
                        && r.remote_url.as_deref().map(|u| sanitize_remote_url(u).url)
                            == Some(url.clone())
                })
                .unwrap_or(false);
        }
    }
    let last_error = if synced {
        None
    } else {
        rec.as_ref().and_then(|r| r.last_error.clone()).or_else(|| {
            Some(format!(
                "HEAD {} has not been confirmed pushed to origin/{branch}; run git_sync",
                head.as_deref().map(short).unwrap_or("?")
            ))
        })
    };
    GitSyncRecord {
        active_revision: head,
        remote_name: "origin".into(),
        remote_url: Some(url),
        sync_status: if synced { "synced" } else { "unsynced" }.into(),
        last_error,
        updated_at: now,
    }
}

/// An interrupted activation whose commit is HEAD. Its index/working-tree switch has been
/// finished; completing it (Revision record, draft final, journal `activated`) waits until the
/// Generation's State is migrated at startup (§8).
struct LandedActivation {
    journal: ActivationJournalRecord,
    target: String,
    mode: ActivationMode,
}

/// Mode an activation ran in, from its draft: `dash reload` drafts carry the
/// `reload:workspace_changes` change entry, which no other path records.
fn journal_mode(store: &SqliteStore, draft_id: &str) -> ActivationMode {
    let is_reload = store
        .get_draft(draft_id)
        .ok()
        .flatten()
        .is_some_and(|d| d.changes.iter().any(|c| c.starts_with(RELOAD_CHANGE)));
    if is_reload {
        ActivationMode::Reload
    } else {
        ActivationMode::Draft
    }
}

/// Change-log entry of a `dash reload` draft (see [`DraftManager::reload_workspace`]).
const RELOAD_CHANGE: &str = "reload:workspace_changes";

/// Complete an interrupted activation that landed: record the Revision, discard the draft's
/// worktree, mark the draft activated and the journal `activated` (sync is re-derived, so an
/// unconfirmed push shows `unsynced`).
async fn finalize_landed_activation(ws: &Path, store: &SqliteStore, l: &LandedActivation) {
    let j = &l.journal;
    let draft = store.get_draft(&j.draft_id).ok().flatten();
    let msg = git_commit_message(ws, &l.target).await.unwrap_or_default();
    let principal_id = commit_trailer(&msg, "Dash-Principal")
        .or_else(|| draft.as_ref().map(|d| d.created_by.clone()))
        .unwrap_or_else(|| Principal::administrator().id);
    let principal_kind =
        commit_trailer(&msg, "Dash-Principal-Kind").unwrap_or_else(|| "administrator".into());
    let _ = store.record_revision(
        &l.target,
        Some(&j.base_revision),
        &principal_id,
        &principal_kind,
        "Activation completed by crash recovery",
    );
    if let Some(mut d) = draft {
        remove_worktree(ws, Path::new(&d.worktree_path)).await;
        d.status = "activated".into();
        d.updated_at = Utc::now().to_rfc3339();
        let _ = store.upsert_draft(&d);
    }
    let mut repaired = j.clone();
    repaired.phase = "activated".into();
    repaired.updated_at = Utc::now().to_rfc3339();
    repaired.error_message = Some(format!(
        "reconciled_on_startup: commit {} is HEAD and was activated by crash recovery",
        short(&l.target)
    ));
    let _ = store.upsert_activation_journal(&repaired);
}

/// Deterministically reconcile activations interrupted by a crash (ADR-0004 §2):
/// - target commit is HEAD: the commit landed; finish the index/working-tree switch Dash was
///   performing and return it, so startup completes it once State is migrated (§8);
/// - HEAD is still the base: the activation never landed; undo any partial switch;
/// - anything else: external divergence; mark `failed` and leave the Workspace alone.
///
/// Index/working-tree repairs follow the activation's mode: a `dash reload` never touches the
/// Workspace working tree (it holds the user's uncommitted edits), only the index.
async fn reconcile_activation_journals(
    ws: &Path,
    store: &SqliteStore,
    warnings: &mut Vec<String>,
) -> Vec<LandedActivation> {
    let incomplete = store
        .list_incomplete_activation_journals()
        .unwrap_or_default();
    let mut landed_activations = Vec::new();
    for j in incomplete {
        let head = git_head(ws).await.ok();
        let mode = journal_mode(store, &j.draft_id);
        let mut repaired = j.clone();
        repaired.updated_at = Utc::now().to_rfc3339();
        let landed = match (&j.target_revision, &head) {
            (Some(t), Some(h)) => t == h,
            _ => false,
        };
        if landed {
            let target = j.target_revision.clone().unwrap_or_default();
            if target != j.base_revision {
                repair_index(ws, &j.base_revision, &target, mode).await;
            }
            landed_activations.push(LandedActivation {
                journal: j,
                target,
                mode,
            });
            continue;
        } else if head.as_deref() == Some(j.base_revision.as_str()) {
            if let Some(ref t) = j.target_revision {
                if *t != j.base_revision {
                    repair_index(ws, t, &j.base_revision, mode).await;
                }
            }
            repaired.phase = "rolled_back".into();
            repaired.error_message = Some(
                "reconciled_on_startup: HEAD is still the base Revision; the draft was not activated".into(),
            );
        } else {
            repaired.phase = "failed".into();
            let m = format!(
                "reconciled_on_startup: HEAD {} matches neither base {} nor target {:?} of interrupted activation {}; external Git divergence needs an explicit draft proposal",
                head.as_deref().map(short).unwrap_or("?"),
                short(&j.base_revision),
                j.target_revision.as_deref().map(short),
                j.journal_id
            );
            warnings.push(m.clone());
            repaired.error_message = Some(m);
        }
        let _ = store.upsert_activation_journal(&repaired);
    }
    landed_activations
}

/// Finish an interrupted index/working-tree switch from `from` to `to` (HEAD already at `to`).
/// Only acts when the index still holds exactly `from`'s tree, i.e. Dash's own switch was
/// interrupted; any other state is left for the operator. A `Draft` activation switches index
/// and working tree; a `Reload` activation only ever moves the index, because the working tree
/// holds the user's uncommitted edits that the reload proposed (§12.3).
async fn repair_index(ws: &Path, from: &str, to: &str, mode: ActivationMode) {
    let (Ok(idx), Ok(from_tree), Ok(to_tree)) = (
        git_index_tree(ws).await,
        git_tree_of(ws, from).await,
        git_tree_of(ws, to).await,
    ) else {
        return;
    };
    if idx == to_tree || idx != from_tree {
        return;
    }
    let res = match mode {
        ActivationMode::Draft => git_read_tree_reset(ws, to).await,
        ActivationMode::Reload => git_read_tree_index_only(ws, to).await,
    };
    if let Err(e) = res {
        tracing::error!("crash repair of the Workspace index to {to} failed: {e}");
    }
}

fn compute_generation_diff(
    old: &ConfiguredGraph,
    new: &ConfiguredGraph,
) -> (Vec<String>, Vec<String>, BTreeSet<String>) {
    let mut directly_changed = BTreeSet::new();
    for (id, new_inst) in &new.instances {
        let new_def_dig = new
            .definitions
            .get(&new_inst.definition)
            .map(|d| d.manifest.identity_version.as_str())
            .unwrap_or("");
        let unchanged = old.instances.get(id).map(|old_inst| {
            let old_def_dig = old
                .definitions
                .get(&old_inst.definition)
                .map(|d| d.manifest.identity_version.as_str())
                .unwrap_or("");
            old_inst.config_digest() == new_inst.config_digest() && old_def_dig == new_def_dig
        });
        if unchanged != Some(true) {
            directly_changed.insert(id.clone());
        }
    }

    let affected = new.downstream_dependents(&directly_changed);
    let mut carried_over = Vec::new();
    let mut rebuilt = Vec::new();
    for id in new.instances.keys() {
        if affected.contains(id) {
            rebuilt.push(id.clone());
        } else if old.instances.contains_key(id) {
            carried_over.push(id.clone());
        } else {
            rebuilt.push(id.clone());
        }
    }
    (carried_over, rebuilt, affected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packages::LockedPackage;

    fn locked(name: &str, git: &str, r: &str, deps: &[&str]) -> LockedPackage {
        LockedPackage {
            name: name.into(),
            git: git.into(),
            requested_ref: r.into(),
            commit: "c".into(),
            tree_digest: "sha256:x".into(),
            subdir: None,
            dependencies: deps.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn lock_verification_detects_missing_changed_and_orphan_entries() {
        let mut manifest = PackagesToml::default();
        manifest.dependencies.insert(
            "a".into(),
            PackageDependencySpec {
                git: "g/a".into(),
                git_ref: "main".into(),
                subdir: None,
            },
        );
        let ok = PackagesLock {
            version: 1,
            packages: vec![
                locked("a", "g/a", "main", &["b"]),
                locked("b", "g/b", "v1", &[]),
            ],
        };
        verify_lock_matches_manifest(&manifest, &ok).unwrap();

        let changed_ref = PackagesLock {
            version: 1,
            packages: vec![locked("a", "g/a", "v2", &[])],
        };
        assert!(verify_lock_matches_manifest(&manifest, &changed_ref)
            .unwrap_err()
            .contains("changed"));

        let orphan = PackagesLock {
            version: 1,
            packages: vec![
                locked("a", "g/a", "main", &[]),
                locked("z", "g/z", "main", &[]),
            ],
        };
        assert!(verify_lock_matches_manifest(&manifest, &orphan)
            .unwrap_err()
            .contains("'z'"));

        assert!(
            verify_lock_matches_manifest(&manifest, &PackagesLock::default())
                .unwrap_err()
                .contains("not pinned")
        );
    }

    #[test]
    fn commit_message_carries_author_contributors_and_activator() {
        let draft = DraftRecord {
            draft_id: "dft_1".into(),
            base_revision: "b".into(),
            worktree_path: "/x".into(),
            created_by: "agt_a".into(),
            status: "open".into(),
            changes: vec![
                "instance.upsert:x by agt_a".into(),
                "dashboard:ops by admin".into(),
            ],
            validation_report: None,
            test_report: None,
            created_at: String::new(),
            updated_at: String::new(),
        };
        let (subject, msg) = commit_message(&draft, &Principal::administrator());
        assert_eq!(
            subject,
            "Activate draft dft_1: instance.upsert:x, dashboard:ops"
        );
        assert_eq!(
            commit_trailer(&msg, "Dash-Author").as_deref(),
            Some("agt_a")
        );
        assert_eq!(
            commit_trailer(&msg, "Dash-Contributor").as_deref(),
            Some("admin")
        );
        assert_eq!(
            commit_trailer(&msg, "Dash-Principal").as_deref(),
            Some("admin")
        );
        assert_eq!(commit_trailer(&msg, "Dash-Draft").as_deref(), Some("dft_1"));
    }
}

#[cfg(test)]
#[path = "drafts_tx_tests.rs"]
mod tx_tests;
