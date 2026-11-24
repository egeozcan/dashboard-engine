//! Transactional Draft manager: concurrent isolated Git worktrees, validate, preview,
//! explicit 3-way rebase, Generation carry-over, atomic swap, activation journal, and push sync (§12).

use crate::git_cli::{
    create_worktree, ensure_git_repo, git_changed_files_between, git_commit_all, git_head,
    git_history, git_push, git_remote_url, git_show_file, remove_worktree, run_git, GitCommitInfo,
    GitError, GitStatusReport,
};
use crate::packages::{
    copy_tree_excluding_git, fetch_locked_packages, missing_locked_packages, read_packages_lock,
    read_packages_toml, resolve_and_lock_packages, update_package_lock, write_packages_toml,
    PackageDependencySpec, PackagesLock,
};
use crate::preview::{run_preview_and_scenarios, PreviewTestReport};
use crate::workspace::{
    delete_instance_in_workspace, load_workspace_graph_async as load_workspace_graph,
    upsert_instance_in_workspace, write_dashboard_in_workspace, write_definition_in_workspace,
};
use chrono::Utc;
use dash_core::{
    validate_configured_graph, ConfiguredGraph, DashboardDoc, InstanceConfig, Principal,
    RuntimeEngine, ValidationReport,
};
use dash_luau::CompiledInstanceBundle;
use dash_store::{ActivationJournalRecord, DraftRecord, GitSyncRecord, SqliteStore};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationResult {
    pub revision_commit: String,
    pub generation_id: i64,
    pub carried_over_instances: Vec<String>,
    pub rebuilt_instances: Vec<String>,
    pub sync_status: String, // synced | unsynced | no_remote
    pub sync_error: Option<String>,
}

#[derive(Clone)]
pub struct DraftManager {
    workspace_dir: PathBuf,
    data_dir: PathBuf,
    store: SqliteStore,
    engine: RuntimeEngine,
    use_mocks: bool,
    activate_lock: std::sync::Arc<Mutex<()>>,
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

        let branch = "main";
        let head = ensure_git_repo(&workspace_dir, branch).await?;
        let lock_path = workspace_dir.join("packages.lock");
        let pkg_toml = read_packages_toml(&workspace_dir)?;
        if !pkg_toml.dependencies.is_empty() && !lock_path.exists() {
            resolve_and_lock_packages(&workspace_dir, &data_dir).await?;
        }
        // A committed packages.lock on a fresh Installation: materialize the pinned commits.
        let lock = read_packages_lock(&workspace_dir)?;
        if !missing_locked_packages(&data_dir, &lock).is_empty() {
            fetch_locked_packages(&data_dir, &lock).await?;
        }

        // Crash recovery on startup from activation_journal (§12.2, §15.2)
        let incomplete = store
            .list_incomplete_activation_journals()
            .unwrap_or_default();
        for j in incomplete {
            let mut repaired = j.clone();
            repaired.phase = if j.target_revision.as_deref() == Some(head.as_str()) {
                "activated".into()
            } else {
                "rolled_back".into()
            };
            repaired.error_message = Some("reconciled_on_startup".into());
            repaired.updated_at = Utc::now().to_rfc3339();
            let _ = store.upsert_activation_journal(&repaired);
        }

        // `--use-mocks` substitutes Mock connectors but keeps live authority (SQLite grants).
        let mut graph = load_workspace_graph(&workspace_dir, &data_dir, &head, 1, false).await?;
        graph.substitute_mocks = use_mocks;
        let gen_id = store
            .record_generation(&head, &graph.graph_digest())
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        graph.generation_id = gen_id;
        let _ = store.record_revision(
            &head,
            None,
            "system_init",
            "administrator",
            "Initial active revision",
        );

        let remote_url = git_remote_url(&workspace_dir, "origin").await;
        if store.get_git_sync_state().ok().flatten().is_none() {
            let _ = store.set_git_sync_state(&GitSyncRecord {
                active_revision: Some(head.clone()),
                remote_name: "origin".into(),
                remote_url: remote_url.clone(),
                sync_status: if remote_url.is_some() {
                    "synced".into()
                } else {
                    "no_remote".into()
                },
                last_error: None,
                updated_at: Utc::now().to_rfc3339(),
            });
        }

        let engine = RuntimeEngine::new(graph.clone(), store.clone(), budget)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        let _ = engine
            .history
            .sync_series_configs(&graph.workspace.history);
        for dash in graph.dashboards.values() {
            let _ = engine.state.seed_dashboard_defaults(dash, gen_id);
        }

        Ok(Self {
            workspace_dir,
            data_dir,
            store,
            engine,
            use_mocks,
            activate_lock: std::sync::Arc::new(Mutex::new(())),
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

    pub async fn active_revision(&self) -> Result<String, GitError> {
        git_head(&self.workspace_dir).await
    }

    /// Begin a new isolated Draft transaction against the current active Revision (§12.2, §12.3).
    pub async fn draft_begin(&self, principal: &Principal) -> Result<DraftRecord, GitError> {
        let base_rev = self.active_revision().await?;
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
        Ok(rec)
    }

    pub fn draft_status(&self, draft_id: &str) -> Result<DraftRecord, GitError> {
        self.store
            .get_draft(draft_id)
            .map_err(|e| GitError::Workspace(e.to_string()))?
            .ok_or_else(|| GitError::Workspace(format!("draft '{draft_id}' not found")))
    }

    pub fn draft_list(&self) -> Result<Vec<DraftRecord>, GitError> {
        self.store
            .list_drafts()
            .map_err(|e| GitError::Workspace(e.to_string()))
    }

    pub async fn draft_abort(&self, draft_id: &str) -> Result<(), GitError> {
        let mut draft = self.draft_status(draft_id)?;
        remove_worktree(&self.workspace_dir, Path::new(&draft.worktree_path)).await;
        draft.status = "aborted".into();
        draft.updated_at = Utc::now().to_rfc3339();
        self.store
            .upsert_draft(&draft)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        Ok(())
    }

    // ---- Draft Mutation Methods (strictly isolated to draft worktree) ----

    pub fn draft_write_definition(
        &self,
        draft_id: &str,
        package: &str,
        def_name: &str,
        definition_luau: &str,
        runtime_luau: Option<&str>,
        tests_luau: Option<&str>,
    ) -> Result<DraftRecord, GitError> {
        let mut draft = self.require_mutable_draft(draft_id)?;
        let wt = Path::new(&draft.worktree_path);
        write_definition_in_workspace(
            wt,
            package,
            def_name,
            definition_luau,
            runtime_luau,
            tests_luau,
        )?;
        self.record_draft_mutation(&mut draft, format!("definition:{package}.{def_name}"))?;
        Ok(draft)
    }

    pub fn draft_upsert_instance(
        &self,
        draft_id: &str,
        inst: &InstanceConfig,
    ) -> Result<DraftRecord, GitError> {
        let mut draft = self.require_mutable_draft(draft_id)?;
        let wt = Path::new(&draft.worktree_path);
        upsert_instance_in_workspace(wt, inst)?;
        self.record_draft_mutation(&mut draft, format!("instance.upsert:{}", inst.id))?;
        Ok(draft)
    }

    pub fn draft_delete_instance(
        &self,
        draft_id: &str,
        instance_id: &str,
    ) -> Result<DraftRecord, GitError> {
        let mut draft = self.require_mutable_draft(draft_id)?;
        let wt = Path::new(&draft.worktree_path);
        delete_instance_in_workspace(wt, instance_id)?;
        self.record_draft_mutation(&mut draft, format!("instance.delete:{instance_id}"))?;
        Ok(draft)
    }

    pub fn draft_update_dashboard(
        &self,
        draft_id: &str,
        doc: &DashboardDoc,
    ) -> Result<DraftRecord, GitError> {
        let mut draft = self.require_mutable_draft(draft_id)?;
        let wt = Path::new(&draft.worktree_path);
        write_dashboard_in_workspace(wt, doc)?;
        self.record_draft_mutation(&mut draft, format!("dashboard:{}", doc.id))?;
        Ok(draft)
    }

    pub async fn draft_update_dependency(
        &self,
        draft_id: &str,
        pkg_name: &str,
        spec: PackageDependencySpec,
    ) -> Result<(DraftRecord, PackagesLock), GitError> {
        let mut draft = self.require_mutable_draft(draft_id)?;
        let wt = Path::new(&draft.worktree_path);
        let mut cfg = read_packages_toml(wt)?;
        cfg.dependencies.insert(pkg_name.to_string(), spec);
        write_packages_toml(wt, &cfg)?;
        let lock = update_package_lock(wt, &self.data_dir, pkg_name).await?;
        self.record_draft_mutation(&mut draft, format!("dependency:{pkg_name}"))?;
        Ok((draft, lock))
    }

    /// Validate contracts, bindings, capabilities, cycles, and Definition tests inside the Draft.
    pub async fn draft_validate(&self, draft_id: &str) -> Result<ValidationReport, GitError> {
        let mut draft = self.draft_status(draft_id)?;
        let wt = Path::new(&draft.worktree_path);
        let graph = load_workspace_graph(wt, &self.data_dir, &draft.base_revision, 0, false).await?;
        let mut report = validate_configured_graph(&graph, &self.store, false);
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

        draft.validation_report = Some(serde_json::to_value(&report).unwrap());
        if report.valid {
            draft.status = "validated".into();
        }
        draft.updated_at = Utc::now().to_rfc3339();
        self.store
            .upsert_draft(&draft)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        Ok(report)
    }

    /// Run isolated Preview + Definition tests + all Scenario tests against Mocks (§12.2 steps 3-4).
    pub async fn draft_preview_and_test(
        &self,
        draft_id: &str,
    ) -> Result<PreviewTestReport, GitError> {
        let mut draft = self.draft_status(draft_id)?;
        let wt = Path::new(&draft.worktree_path);

        // Identify changed definitions vs base
        let base_graph =
            load_workspace_graph(&self.workspace_dir, &self.data_dir, &draft.base_revision, 0, true).await?;
        let draft_graph = load_workspace_graph(wt, &self.data_dir, &draft.base_revision, 0, true).await?;
        let mut changed_defs = Vec::new();
        for (id, def) in &draft_graph.definitions {
            let unchanged = base_graph
                .definitions
                .get(id)
                .map(|b| b.manifest.identity_version == def.manifest.identity_version)
                .unwrap_or(false);
            if !unchanged {
                changed_defs.push(id.clone());
            }
        }
        let target_slice = if changed_defs.is_empty() {
            None
        } else {
            Some(changed_defs.as_slice())
        };

        let report = run_preview_and_scenarios(wt, &self.data_dir, target_slice).await?;
        draft.validation_report = Some(serde_json::to_value(&report.validation).unwrap());
        draft.test_report = Some(serde_json::to_value(&report).unwrap());
        if report.all_passed {
            draft.status = "previewed".into();
        }
        draft.updated_at = Utc::now().to_rfc3339();
        self.store
            .upsert_draft(&draft)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        Ok(report)
    }

    /// Explicitly rebase a stale Draft onto the current active Revision (§12.3).
    /// Performs deterministic 3-way merge (`git merge-file`), rejecting conflicts explicitly
    /// and clearing prior validation/test approvals!
    pub async fn draft_rebase(&self, draft_id: &str) -> Result<DraftRecord, GitError> {
        let mut draft = self.require_mutable_draft(draft_id)?;
        let active_rev = self.active_revision().await?;
        if draft.base_revision == active_rev {
            return Ok(draft);
        }

        let wt = Path::new(&draft.worktree_path);
        let upstream_changed =
            git_changed_files_between(&self.workspace_dir, &draft.base_revision, &active_rev).await?;
        let draft_files = list_relative_files(wt)?;
        let mut draft_changed = BTreeSet::new();
        for rel in &draft_files {
            let base_bytes = git_show_file(&self.workspace_dir, &draft.base_revision, rel).await;
            let cur_bytes = std::fs::read(wt.join(rel)).ok();
            if base_bytes != cur_bytes {
                draft_changed.insert(rel.clone());
            }
        }

        let mut conflicts = Vec::new();
        for rel in &upstream_changed {
            if !draft_changed.contains(rel) {
                // Fast-forward non-conflicting upstream file into draft worktree
                if let Some(bytes) = git_show_file(&self.workspace_dir, &active_rev, rel).await {
                    let target = wt.join(rel);
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(target, bytes)?;
                } else {
                    let _ = std::fs::remove_file(wt.join(rel));
                }
            } else {
                // Attempt line-level 3-way merge via `git merge-file`
                let base_b = git_show_file(&self.workspace_dir, &draft.base_revision, rel)
                    .await
                    .unwrap_or_default();
                let theirs_b = git_show_file(&self.workspace_dir, &active_rev, rel)
                    .await
                    .unwrap_or_default();
                let tmp = tempfile::tempdir()?;
                let cur_p = wt.join(rel);
                let base_p = tmp.path().join("base");
                let theirs_p = tmp.path().join("theirs");
                std::fs::write(&base_p, base_b)?;
                std::fs::write(&theirs_p, theirs_b)?;

                let (code, _) = crate::git_cli::run_git_allow_exit_1(
                    wt,
                    &[
                        "merge-file",
                        &cur_p.to_string_lossy(),
                        &base_p.to_string_lossy(),
                        &theirs_p.to_string_lossy(),
                    ],
                )
                .await?;
                if code != 0 {
                    conflicts.push(rel.clone());
                }
            }
        }

        if !conflicts.is_empty() {
            return Err(GitError::MergeConflict(conflicts));
        }

        draft.base_revision = active_rev;
        draft.status = "open".into();
        draft.validation_report = None; // Rebasing invalidates prior validation & test results (§12.3)!
        draft.test_report = None;
        draft.updated_at = Utc::now().to_rfc3339();
        self.store
            .upsert_draft(&draft)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        Ok(draft)
    }

    /// Activate a validated & tested Draft atomically (§12.2):
    /// Rejects stale base revisions, runs Generation carry-over, creates 1 Git commit,
    /// swaps `ArcSwap<ConfiguredGraph>`, updates activation journal, and attempts push.
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

        let _guard = self.activate_lock.lock().await;
        let mut draft = self.require_mutable_draft(draft_id)?;
        let active_rev = self.active_revision().await?;
        if draft.base_revision != active_rev {
            return Err(GitError::Workspace(format!(
                "stale draft base revision '{}'; active revision is '{}'. Call draft_rebase first (§12.3).",
                draft.base_revision, active_rev
            )));
        }

        // Validate & test if not already passed since last mutation
        if draft.validation_report.is_none() || draft.test_report.is_none() {
            let report = self.draft_preview_and_test(draft_id).await?;
            if !report.all_passed {
                return Err(GitError::Workspace(format!(
                    "draft validation/tests failed; live Generation preserved untouched: {}",
                    serde_json::to_string(&report).unwrap_or_default()
                )));
            }
            draft = self.draft_status(draft_id)?;
        } else {
            let parsed_test: PreviewTestReport =
                serde_json::from_value(draft.test_report.clone().unwrap())
                    .map_err(|e| GitError::Workspace(e.to_string()))?;
            if !parsed_test.all_passed {
                return Err(GitError::Workspace(
                    "draft tests failed; activation refused".into(),
                ));
            }
        }

        let wt = Path::new(&draft.worktree_path);
        // Also ensure live validation (checking Service principal grants etc. against real store)
        let candidate_graph = load_workspace_graph(wt, &self.data_dir, &active_rev, 0, false).await?;
        let live_val = validate_configured_graph(&candidate_graph, &self.store, false);
        if !live_val.valid {
            return Err(GitError::Workspace(format!(
                "live validation errors: {:?}",
                live_val.issues
            )));
        }

        // Principal-aware authority (§5.2, §11.3, ADR-0002): every editor of the draft and the
        // activating Principal must hold the secrets / Service principal ceilings the change reaches.
        let mut authz_principals =
            dash_core::draft_editor_principals(&self.store, draft_id, &draft.created_by);
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
                draft_id,
                "denied",
                serde_json::json!({ "issues": authz_issues }),
            );
            return Err(GitError::Workspace(format!(
                "activation denied (pending grant): {:?}",
                authz_issues
            )));
        }

        // Persist activation journal (`started`)
        let journal_id = format!("jrn_{}", Uuid::now_v7().simple());
        self.store
            .upsert_activation_journal(&ActivationJournalRecord {
                journal_id: journal_id.clone(),
                draft_id: draft_id.to_string(),
                base_revision: active_rev.clone(),
                target_revision: None,
                generation_id: None,
                phase: "started".into(),
                error_message: None,
                updated_at: Utc::now().to_rfc3339(),
            })
            .map_err(|e| GitError::Workspace(e.to_string()))?;

        // Compute Generation carry-over diff before mutating workspace
        let old_graph = self.engine.graph.load_full();
        let (carried_over, rebuilt, downstream_affected) =
            compute_generation_diff(&old_graph, &candidate_graph);

        // Sync draft files cleanly into workspace_dir and commit ONCE
        sync_worktree_to_workspace(wt, &self.workspace_dir)?;
        let summary = if draft.changes.is_empty() {
            format!("Activate draft {draft_id}")
        } else {
            format!("Activate draft {draft_id}: {}", draft.changes.join(", "))
        };
        let new_commit =
            git_commit_all(&self.workspace_dir, &summary, &principal.id, Some(draft_id)).await?;

        self.store
            .upsert_activation_journal(&ActivationJournalRecord {
                journal_id: journal_id.clone(),
                draft_id: draft_id.to_string(),
                base_revision: active_rev.clone(),
                target_revision: Some(new_commit.clone()),
                generation_id: None,
                phase: "committed".into(),
                error_message: None,
                updated_at: Utc::now().to_rfc3339(),
            })
            .map_err(|e| GitError::Workspace(e.to_string()))?;

        let _ = self.store.record_revision(
            &new_commit,
            Some(&active_rev),
            &principal.id,
            principal.kind.as_str(),
            &summary,
        );

        // Build final live Generation and atomically swap!
        let mut live_graph = load_workspace_graph(
            &self.workspace_dir,
            &self.data_dir,
            &new_commit,
            old_graph.generation_id + 1,
            false,
        )
        .await?;
        live_graph.substitute_mocks = self.use_mocks;
        let new_gen_id = self
            .store
            .record_generation(&new_commit, &live_graph.graph_digest())
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        live_graph.generation_id = new_gen_id;

        // Carry over unchanged VMs in VmPool, drop changed ones
        let desired_bundles: Vec<CompiledInstanceBundle> = live_graph
            .instances
            .values()
            .filter_map(|inst| {
                live_graph.definitions.get(&inst.definition).map(|def| CompiledInstanceBundle {
                    instance_id: inst.id.clone(),
                    manifest: def.manifest.clone(),
                    runtime_source: def.runtime_source.clone(),
                    config_digest: inst.config_digest(),
                    resolved_libraries: live_graph.resolved_libraries(),
                })
            })
            .collect();
        self.engine.pool.sync_generation(&desired_bundles);
        self.engine
            .queries
            .mark_generation_stale(&downstream_affected, new_gen_id);
        let _ = self
            .engine
            .history
            .sync_series_configs(&live_graph.workspace.history);
        for dash in live_graph.dashboards.values() {
            let _ = self.engine.state.seed_dashboard_defaults(dash, new_gen_id);
        }

        // Atomic Generation swap (no partial graph ever visible!)
        self.engine.graph.store(std::sync::Arc::new(live_graph));

        self.store
            .upsert_activation_journal(&ActivationJournalRecord {
                journal_id: journal_id.clone(),
                draft_id: draft_id.to_string(),
                base_revision: active_rev.clone(),
                target_revision: Some(new_commit.clone()),
                generation_id: Some(new_gen_id),
                phase: "activated".into(),
                error_message: None,
                updated_at: Utc::now().to_rfc3339(),
            })
            .map_err(|e| GitError::Workspace(e.to_string()))?;

        // Attempt push to configured remote; push failure leaves active/committed/unsynced (§12.2 step 8)
        let (sync_status, sync_error) = self.attempt_push(&new_commit).await;
        if sync_status == "synced" {
            let _ = self.store.upsert_activation_journal(&ActivationJournalRecord {
                journal_id,
                draft_id: draft_id.to_string(),
                base_revision: active_rev,
                target_revision: Some(new_commit.clone()),
                generation_id: Some(new_gen_id),
                phase: "pushed".into(),
                error_message: None,
                updated_at: Utc::now().to_rfc3339(),
            });
        }

        // Cleanup draft worktree & mark activated
        remove_worktree(&self.workspace_dir, wt).await;
        draft.status = "activated".into();
        draft.updated_at = Utc::now().to_rfc3339();
        self.store
            .upsert_draft(&draft)
            .map_err(|e| GitError::Workspace(e.to_string()))?;

        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "draft.activate",
            draft_id,
            "activated",
            serde_json::json!({
                "revision": new_commit,
                "generation": new_gen_id,
                "sync_status": sync_status,
            }),
        );

        Ok(ActivationResult {
            revision_commit: new_commit,
            generation_id: new_gen_id,
            carried_over_instances: carried_over,
            rebuilt_instances: rebuilt,
            sync_status,
            sync_error,
        })
    }

    /// Hot-reload direct workspace changes (e.g. from `dash reload`) through the full validation & activation pipeline.
    pub async fn reload_workspace(&self, principal: &Principal) -> Result<ActivationResult, GitError> {
        let draft = self.draft_begin(principal).await?;
        copy_tree_excluding_git(&self.workspace_dir, Path::new(&draft.worktree_path))?;
        self.draft_activate(&draft.draft_id, principal).await
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
        let remote_url = git_remote_url(&self.workspace_dir, "origin").await;
        let sync_rec = self.store.get_git_sync_state().ok().flatten();
        let (sync_status, last_error) = match sync_rec {
            Some(r) => (r.sync_status, r.last_error),
            None => (
                if remote_url.is_some() {
                    "synced".into()
                } else {
                    "no_remote".into()
                },
                None,
            ),
        };
        Ok(GitStatusReport {
            head_commit: head,
            branch,
            dirty: !changed_files.is_empty(),
            changed_files,
            remote_url,
            sync_status,
            last_error,
        })
    }

    pub async fn git_history(&self, limit: usize) -> Result<Vec<GitCommitInfo>, GitError> {
        git_history(&self.workspace_dir, limit).await
    }

    pub async fn git_sync(&self, principal: &Principal) -> Result<GitSyncRecord, GitError> {
        let head = self.active_revision().await?;
        let (sync_status, last_error) = self.attempt_push(&head).await;
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

    async fn attempt_push(&self, commit: &str) -> (String, Option<String>) {
        let remote_url = git_remote_url(&self.workspace_dir, "origin").await;
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

        let branch = self.engine.graph.load().workspace.git_branch.clone();
        match git_push(&self.workspace_dir, "origin", &branch).await {
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
                let msg = e.to_string();
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

    fn require_mutable_draft(&self, draft_id: &str) -> Result<DraftRecord, GitError> {
        let draft = self.draft_status(draft_id)?;
        if draft.status == "activated" || draft.status == "aborted" {
            return Err(GitError::Workspace(format!(
                "draft '{draft_id}' is already '{}'",
                draft.status
            )));
        }
        Ok(draft)
    }

    fn record_draft_mutation(&self, draft: &mut DraftRecord, change: String) -> Result<(), GitError> {
        draft.changes.push(change);
        draft.status = "open".into();
        draft.validation_report = None;
        draft.test_report = None;
        draft.updated_at = Utc::now().to_rfc3339();
        self.store
            .upsert_draft(draft)
            .map_err(|e| GitError::Workspace(e.to_string()))
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

fn list_relative_files(root: &Path) -> Result<BTreeSet<String>, GitError> {
    let mut out = BTreeSet::new();
    collect_rel_paths(root, root, &mut out)?;
    Ok(out)
}

fn collect_rel_paths(
    base: &Path,
    cur: &Path,
    out: &mut BTreeSet<String>,
) -> Result<(), GitError> {
    for entry in std::fs::read_dir(cur)? {
        let entry = entry?;
        if entry.file_name() == ".git" {
            continue;
        }
        let p = entry.path();
        if p.is_dir() {
            collect_rel_paths(base, &p, out)?;
        } else if p.is_file() {
            let rel = p
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            out.insert(rel);
        }
    }
    Ok(())
}

fn sync_worktree_to_workspace(src: &Path, dst: &Path) -> Result<(), GitError> {
    if !src.join(".git").exists() && !src.join("dashboard.toml").exists() {
        return Err(GitError::Workspace(
            "refusing to sync uninitialized draft worktree".into(),
        ));
    }
    let src_files = list_relative_files(src)?;
    let dst_files = list_relative_files(dst)?;
    for stale in dst_files.difference(&src_files) {
        let _ = std::fs::remove_file(dst.join(stale));
    }
    for rel in src_files {
        let target = dst.join(&rel);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(src.join(&rel), target)?;
    }
    Ok(())
}
