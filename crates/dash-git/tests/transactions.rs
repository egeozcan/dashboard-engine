//! Integration tests for draft ownership, transactional activation, crash reconciliation,
//! sync status, Workspace file safety, reload and rebase (§11.3, §12.2, §12.3, §15.2).

use dash_core::{CapabilityCeiling, Principal, PrincipalKind};
use dash_git::git_cli::{self, run_git};
use dash_git::{DraftManager, GitError};
use dash_luau::ResourceBudget;
use dash_store::{ActivationJournalRecord, SqliteStore};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn copy_fixture_ws(dst: &Path) {
    let src =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&src, dst).unwrap();
}

async fn setup(use_mocks: bool) -> (TempDir, PathBuf, PathBuf, DraftManager) {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_fixture_ws(&ws);
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize_with_mocks(
        ws.clone(),
        data.clone(),
        store,
        ResourceBudget::default(),
        use_mocks,
    )
    .await
    .unwrap();
    let ws = std::fs::canonicalize(ws).unwrap();
    let data = std::fs::canonicalize(data).unwrap();
    (tmp, ws, data, mgr)
}

async fn reopen(ws: &Path, data: &Path) -> DraftManager {
    let store = SqliteStore::open(data).unwrap();
    DraftManager::initialize(
        ws.to_path_buf(),
        data.to_path_buf(),
        store,
        ResourceBudget::default(),
    )
    .await
    .unwrap()
}

fn agent(id: &str, author: bool, activate: bool) -> Principal {
    Principal {
        id: id.into(),
        kind: PrincipalKind::Agent,
        ceiling: CapabilityCeiling {
            can_author_drafts: author,
            can_activate_drafts: activate,
            ..CapabilityCeiling::read_only_agent()
        },
    }
}

fn ops_dashboard(title: &str) -> dash_core::DashboardDoc {
    dash_core::DashboardDoc {
        id: "ops".into(),
        title: title.into(),
        layout: "grid".into(),
        adapters: vec![],
        initial_state: Default::default(),
        overrides: Default::default(),
        state_schema: Default::default(),
    }
}

#[tokio::test]
async fn draft_ownership_is_enforced_and_audited() {
    let (_tmp, _ws, _data, mgr) = setup(false).await;
    let admin = Principal::administrator();
    let a1 = agent("agt_one", true, true);
    let a2 = agent("agt_two", true, true);
    let reader = agent("agt_reader", false, false);

    let admin_draft = mgr.draft_begin(&admin).await.unwrap();
    let own = mgr.draft_begin(&a1).await.unwrap();
    assert!(mgr.draft_begin(&reader).await.is_err());

    // Another Agent cannot read, edit, preview, rebase, abort or activate someone else's draft
    let id = &admin_draft.draft_id;
    assert!(mgr.draft_status_as(id, &a1).is_err());
    assert!(mgr
        .draft_update_dashboard_as(id, &ops_dashboard("x"), &a1)
        .is_err());
    assert!(mgr.draft_validate_as(id, &a1).await.is_err());
    assert!(mgr.draft_preview_and_test_as(id, &a1).await.is_err());
    assert!(mgr.draft_rebase_as(id, &a1).await.is_err());
    assert!(mgr.draft_abort_as(id, &a1).await.is_err());
    assert!(mgr.draft_activate(id, &a1).await.is_err());
    assert!(mgr.draft_abort_as(&own.draft_id, &a2).await.is_err());
    // A principal without can_author_drafts cannot abort even its "own" draft id
    assert!(mgr.draft_abort_as(&own.draft_id, &reader).await.is_err());
    assert_eq!(mgr.draft_status(id).unwrap().status, "open");

    // The owner and the Administrator can
    mgr.draft_status_as(&own.draft_id, &a1).unwrap();
    mgr.draft_status_as(&own.draft_id, &admin).unwrap();
    assert_eq!(mgr.draft_list_for(&a1).unwrap().len(), 1);
    assert_eq!(mgr.draft_list_for(&admin).unwrap().len(), 2);

    let denials = mgr
        .store()
        .list_audits(100)
        .unwrap()
        .into_iter()
        .filter(|a| a.outcome == "denied" && a.principal_id == "agt_one")
        .count();
    assert!(denials >= 7, "denials audited: {denials}");
}

#[tokio::test]
async fn abort_is_idempotent_and_cannot_rewrite_an_activated_draft() {
    let (_tmp, _ws, _data, mgr) = setup(false).await;
    let admin = Principal::administrator();
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    mgr.draft_activate(&d.draft_id, &admin).await.unwrap();
    let err = mgr.draft_abort(&d.draft_id).await.unwrap_err().to_string();
    assert!(err.contains("already activated"), "{err}");
    assert_eq!(mgr.draft_status(&d.draft_id).unwrap().status, "activated");

    let d2 = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_abort(&d2.draft_id).await.unwrap();
    mgr.draft_abort(&d2.draft_id).await.unwrap();
    assert_eq!(mgr.draft_status(&d2.draft_id).unwrap().status, "aborted");
}

#[tokio::test]
async fn commit_attributes_author_and_activator() {
    let (_tmp, ws, _data, mgr) = setup(false).await;
    let admin = Principal::administrator();
    let author = agent("agt_author", true, false);
    let d = mgr.draft_begin(&author).await.unwrap();
    mgr.draft_update_dashboard_as(&d.draft_id, &ops_dashboard("Ops"), &author)
        .unwrap();
    // The author cannot activate (no can_activate_drafts); the Administrator activates it
    assert!(mgr.draft_activate(&d.draft_id, &author).await.is_err());
    let act = mgr.draft_activate(&d.draft_id, &admin).await.unwrap();

    let msg = git_cli::git_commit_message(&ws, &act.revision_commit)
        .await
        .unwrap();
    assert_eq!(
        git_cli::commit_trailer(&msg, "Dash-Author").as_deref(),
        Some("agt_author")
    );
    assert_eq!(
        git_cli::commit_trailer(&msg, "Dash-Principal").as_deref(),
        Some("admin")
    );
    assert_eq!(
        git_cli::commit_trailer(&msg, "Dash-Draft").as_deref(),
        Some(d.draft_id.as_str())
    );
    assert!(msg.contains("dashboard:ops by agt_author"), "{msg}");
    let hist = mgr.git_history(1).await.unwrap();
    assert_eq!(hist[0].author, "agt_author");
}

#[tokio::test]
async fn stale_passing_report_is_not_trusted_after_a_change() {
    let (_tmp, _ws, _data, mgr) = setup(true).await;
    let admin = Principal::administrator();
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    let report = mgr.draft_preview_and_test(&d.draft_id).await.unwrap();
    assert!(report.all_passed, "{report:?}");
    assert!(report.tested_tree.is_some());
    let passing = mgr.draft_status(&d.draft_id).unwrap();

    // A change lands after the Preview ran (e.g. concurrently), and a stale write-back
    // restores the old passing report on the record.
    let wt = PathBuf::from(&d.worktree_path);
    std::fs::write(
        wt.join("packages/engineering/linked_prs/runtime.luau"),
        "local R = {}\nfunction R.query(ctx) error(\"broken after preview\") end\nreturn R\n",
    )
    .unwrap();
    mgr.store().upsert_draft(&passing).unwrap();

    let head_before = mgr.active_revision().await.unwrap();
    let err = mgr
        .draft_activate(&d.draft_id, &admin)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("tests failed"), "{err}");
    assert_eq!(mgr.active_revision().await.unwrap(), head_before);
}

#[tokio::test]
async fn activation_keeps_untracked_and_ignored_files_and_refuses_dirty_workspace() {
    let (_tmp, ws, _data, mgr) = setup(false).await;
    let admin = Principal::administrator();

    // Ignored and untracked local files survive activation
    std::fs::write(ws.join(".gitignore"), "*.local\n").unwrap();
    run_git(&ws, &["add", ".gitignore"]).await.unwrap();
    run_git(&ws, &["commit", "-m", "ignore local files"])
        .await
        .unwrap();
    // Re-open so the active Generation is the new HEAD
    drop(mgr);
    let mgr = reopen(&ws, &std::fs::canonicalize(ws.join("../data")).unwrap()).await;
    std::fs::write(ws.join("secrets.local"), "do not delete").unwrap();
    std::fs::write(ws.join("NOTES.txt"), "untracked notes").unwrap();

    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    mgr.draft_activate(&d.draft_id, &admin).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(ws.join("secrets.local")).unwrap(),
        "do not delete"
    );
    assert_eq!(
        std::fs::read_to_string(ws.join("NOTES.txt")).unwrap(),
        "untracked notes"
    );
    assert!(ws.join("dashboards/ops.yaml").exists());
    assert!(git_cli::git_tracked_changes(&ws).await.unwrap().is_empty());

    // An uncommitted external edit blocks activation and is not overwritten
    let dash_file = ws.join("dashboards/engineering.yaml");
    let original = std::fs::read_to_string(&dash_file).unwrap();
    let edited = format!("{original}\n# external edit\n");
    std::fs::write(&dash_file, &edited).unwrap();
    let d2 = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d2.draft_id, &ops_dashboard("Ops 2"))
        .unwrap();
    let head_before = mgr.active_revision().await.unwrap();
    let err = mgr
        .draft_activate(&d2.draft_id, &admin)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("uncommitted external modifications"), "{err}");
    assert_eq!(std::fs::read_to_string(&dash_file).unwrap(), edited);
    assert_eq!(mgr.active_revision().await.unwrap(), head_before);

    // An untracked file at a path the draft adds blocks activation too
    std::fs::write(&dash_file, &original).unwrap();
    std::fs::write(ws.join("dashboards/new_one.yaml"), "local scratch").unwrap();
    let d3 = mgr.draft_begin(&admin).await.unwrap();
    let mut doc = ops_dashboard("New");
    doc.id = "new_one".into();
    mgr.draft_update_dashboard(&d3.draft_id, &doc).unwrap();
    let err = mgr
        .draft_activate(&d3.draft_id, &admin)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("untracked Workspace files"), "{err}");
    assert_eq!(
        std::fs::read_to_string(ws.join("dashboards/new_one.yaml")).unwrap(),
        "local scratch"
    );
}

#[tokio::test]
async fn reload_propagates_deletions_and_leaves_a_clean_workspace() {
    let (_tmp, ws, _data, mgr) = setup(false).await;
    let admin = Principal::administrator();
    // Add a second dashboard through a draft, then delete it externally and reload
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    mgr.draft_activate(&d.draft_id, &admin).await.unwrap();
    assert!(mgr.engine().graph.load().dashboards.contains_key("ops"));

    std::fs::remove_file(ws.join("dashboards/ops.yaml")).unwrap();
    let act = mgr.reload_workspace(&admin).await.unwrap();
    assert!(!mgr.engine().graph.load().dashboards.contains_key("ops"));
    assert!(!ws.join("dashboards/ops.yaml").exists());
    let in_head = git_cli::git_show_file(&ws, &act.revision_commit, "dashboards/ops.yaml").await;
    assert!(
        in_head.is_none(),
        "deleted file must not be in the reload commit"
    );
    let status = mgr.git_status().await.unwrap();
    assert!(!status.dirty, "{:?}", status.changed_files);
}

#[tokio::test]
async fn startup_loads_head_not_uncommitted_edits() {
    let (_tmp, ws, data, mgr) = setup(false).await;
    drop(mgr);
    // An external uncommitted deletion of a dashboard must not silently change the Generation
    std::fs::remove_file(ws.join("dashboards/engineering.yaml")).unwrap();
    let mgr = reopen(&ws, &data).await;
    assert!(mgr
        .engine()
        .graph
        .load()
        .dashboards
        .contains_key("engineering"));
    assert!(mgr
        .startup_warnings()
        .iter()
        .any(|w| w.contains("uncommitted external modifications")));
    // The edit is still there (never reverted) and can be proposed with reload
    assert!(!ws.join("dashboards/engineering.yaml").exists());
}

#[tokio::test]
async fn crash_after_commit_is_reconciled_as_activated() {
    let (_tmp, ws, data, mgr) = setup(false).await;
    let admin = Principal::administrator();
    let base = mgr.active_revision().await.unwrap();
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    // Simulate a crash right after HEAD moved: commit + advance by hand, journal still `started`
    let wt = PathBuf::from(&d.worktree_path);
    let tree = git_cli::git_stage_all_tree(&wt).await.unwrap();
    let msg = format!("Activate draft {}\n\nDash-Draft: {}\nDash-Principal: admin\nDash-Principal-Kind: administrator", d.draft_id, d.draft_id);
    let c = git_cli::git_commit_tree(&ws, &tree, &base, &msg, ("admin", "a@x"), ("admin", "a@x"))
        .await
        .unwrap();
    git_cli::git_update_head(&ws, &c, &base, "test")
        .await
        .unwrap();
    // (working tree switch never happened: index/working tree still at base)
    mgr.store()
        .upsert_activation_journal(&ActivationJournalRecord {
            journal_id: "jrn_crash".into(),
            draft_id: d.draft_id.clone(),
            base_revision: base.clone(),
            target_revision: Some(c.clone()),
            generation_id: None,
            phase: "started".into(),
            error_message: None,
            updated_at: chrono::Utc::now().to_rfc3339(),
        })
        .unwrap();
    drop(mgr);

    let mgr = reopen(&ws, &data).await;
    assert!(mgr
        .store()
        .list_incomplete_activation_journals()
        .unwrap()
        .is_empty());
    assert_eq!(mgr.draft_status(&d.draft_id).unwrap().status, "activated");
    assert_eq!(mgr.active_generation_revision(), c);
    assert!(mgr.engine().graph.load().dashboards.contains_key("ops"));
    // The interrupted working-tree switch was completed
    assert!(ws.join("dashboards/ops.yaml").exists());
    assert!(git_cli::git_tracked_changes(&ws).await.unwrap().is_empty());
}

#[tokio::test]
async fn crash_before_head_moved_is_rolled_back() {
    let (_tmp, ws, data, mgr) = setup(false).await;
    let admin = Principal::administrator();
    let base = mgr.active_revision().await.unwrap();
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.store()
        .upsert_activation_journal(&ActivationJournalRecord {
            journal_id: "jrn_early".into(),
            draft_id: d.draft_id.clone(),
            base_revision: base.clone(),
            target_revision: None,
            generation_id: None,
            phase: "started".into(),
            error_message: None,
            updated_at: chrono::Utc::now().to_rfc3339(),
        })
        .unwrap();
    drop(mgr);
    let mgr = reopen(&ws, &data).await;
    assert!(mgr
        .store()
        .list_incomplete_activation_journals()
        .unwrap()
        .is_empty());
    assert_eq!(mgr.active_revision().await.unwrap(), base);
    assert_eq!(mgr.draft_status(&d.draft_id).unwrap().status, "open");
}

#[tokio::test]
async fn sync_status_is_derived_and_never_falsely_synced() {
    let (tmp, ws, data, mgr) = setup(false).await;
    let admin = Principal::administrator();
    let bare = tmp.path().join("remote.git");
    std::fs::create_dir_all(&bare).unwrap();
    run_git(&bare, &["init", "--bare"]).await.unwrap();
    run_git(&ws, &["remote", "add", "origin", &bare.to_string_lossy()])
        .await
        .unwrap();
    let synced = mgr.git_sync(&admin).await.unwrap();
    assert_eq!(synced.sync_status, "synced");
    assert_eq!(mgr.git_status().await.unwrap().sync_status, "synced");

    // HEAD moves without a confirmed push (e.g. crash between swap and push)
    std::fs::write(ws.join("README.md"), "external commit").unwrap();
    run_git(&ws, &["add", "README.md"]).await.unwrap();
    run_git(&ws, &["commit", "-m", "external"]).await.unwrap();
    let st = mgr.git_status().await.unwrap();
    assert_eq!(st.sync_status, "unsynced");
    assert!(
        st.head_diverged,
        "HEAD moved away from the running Generation"
    );
    drop(mgr);
    let mgr = reopen(&ws, &data).await;
    assert_eq!(
        mgr.store()
            .get_git_sync_state()
            .unwrap()
            .unwrap()
            .sync_status,
        "unsynced"
    );
    assert!(!mgr.git_status().await.unwrap().head_diverged);
    assert_eq!(mgr.git_sync(&admin).await.unwrap().sync_status, "synced");
}

#[tokio::test]
async fn rebase_never_resurrects_draft_deletions_and_conflicts_leave_draft_untouched() {
    let (_tmp, ws, _data, mgr) = setup(false).await;
    let admin = Principal::administrator();
    // Base has dashboards/ops.yaml and dashboards/extra.yaml
    let seed = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&seed.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    let mut extra = ops_dashboard("Extra");
    extra.id = "extra".into();
    mgr.draft_update_dashboard(&seed.draft_id, &extra).unwrap();
    mgr.draft_activate(&seed.draft_id, &admin).await.unwrap();

    // Draft A deletes extra.yaml; draft B deletes ops.yaml
    let a = mgr.draft_begin(&admin).await.unwrap();
    std::fs::remove_file(Path::new(&a.worktree_path).join("dashboards/extra.yaml")).unwrap();
    let b = mgr.draft_begin(&admin).await.unwrap();
    std::fs::remove_file(Path::new(&b.worktree_path).join("dashboards/ops.yaml")).unwrap();

    // Upstream modifies ops.yaml and adds a new file
    let up = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&up.draft_id, &ops_dashboard("Ops v2"))
        .unwrap();
    let mut other = ops_dashboard("Other");
    other.id = "other".into();
    mgr.draft_update_dashboard(&up.draft_id, &other).unwrap();
    mgr.draft_activate(&up.draft_id, &admin).await.unwrap();

    // A: its deletion of extra.yaml survives the rebase, upstream changes come in
    let rebased = mgr.draft_rebase(&a.draft_id).await.unwrap();
    let wt_a = Path::new(&rebased.worktree_path);
    assert!(!wt_a.join("dashboards/extra.yaml").exists());
    assert!(wt_a.join("dashboards/other.yaml").exists());
    assert!(std::fs::read_to_string(wt_a.join("dashboards/ops.yaml"))
        .unwrap()
        .contains("Ops v2"));

    // B: delete/modify conflict on ops.yaml -> explicit conflict, worktree untouched
    let before_tree = git_cli::git_stage_all_tree(Path::new(&b.worktree_path))
        .await
        .unwrap();
    let err = mgr.draft_rebase(&b.draft_id).await.unwrap_err();
    assert!(
        matches!(err, GitError::MergeConflict(ref f) if f.contains(&"dashboards/ops.yaml".to_string()))
    );
    let after_tree = git_cli::git_stage_all_tree(Path::new(&b.worktree_path))
        .await
        .unwrap();
    assert_eq!(before_tree, after_tree);
    assert!(!Path::new(&b.worktree_path)
        .join("dashboards/other.yaml")
        .exists());
    let _ = ws;
}

#[tokio::test]
async fn text_conflict_writes_no_markers_into_the_draft() {
    let (_tmp, _ws, _data, mgr) = setup(false).await;
    let admin = Principal::administrator();
    let d1 = mgr.draft_begin(&admin).await.unwrap();
    let d2 = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d1.draft_id, &ops_dashboard("One"))
        .unwrap();
    mgr.draft_activate(&d1.draft_id, &admin).await.unwrap();
    mgr.draft_update_dashboard(&d2.draft_id, &ops_dashboard("Two"))
        .unwrap();
    let prior = mgr.draft_preview_and_test(&d2.draft_id).await.unwrap();
    let err = mgr.draft_rebase(&d2.draft_id).await.unwrap_err();
    assert!(matches!(err, GitError::MergeConflict(_)));
    let content =
        std::fs::read_to_string(Path::new(&d2.worktree_path).join("dashboards/ops.yaml")).unwrap();
    assert!(!content.contains("<<<<<<<"), "{content}");
    assert!(content.contains("Two"));
    // Record still describes the (unchanged) files
    let rec = mgr.draft_status(&d2.draft_id).unwrap();
    assert_eq!(rec.base_revision, d2.base_revision);
    let _ = prior;
}

#[tokio::test]
async fn draft_begin_accepts_a_specified_base_revision() {
    let (_tmp, ws, _data, mgr) = setup(false).await;
    let admin = Principal::administrator();
    let base = mgr.active_revision().await.unwrap();
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    mgr.draft_activate(&d.draft_id, &admin).await.unwrap();

    let old = mgr.draft_begin_at(&admin, Some(&base)).await.unwrap();
    assert_eq!(old.base_revision, base);
    assert!(!Path::new(&old.worktree_path)
        .join("dashboards/ops.yaml")
        .exists());
    assert!(mgr
        .draft_begin_at(&admin, Some("not-a-revision"))
        .await
        .is_err());
    let _ = ws;
}

#[tokio::test]
async fn startup_never_resolves_dependencies_implicitly() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    std::fs::create_dir_all(ws.join("instances")).unwrap();
    std::fs::write(ws.join("dashboard.toml"), "name = \"deps\"\n").unwrap();
    std::fs::write(
        ws.join("packages.toml"),
        "[dependencies.ext]\ngit = \"/nonexistent/ext.git\"\nref = \"main\"\n",
    )
    .unwrap();
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize(ws.clone(), data, store, ResourceBudget::default())
        .await
        .unwrap();
    assert!(
        !ws.join("packages.lock").exists(),
        "startup must not write packages.lock"
    );
    assert!(mgr
        .startup_warnings()
        .iter()
        .any(|w| w.contains("dependency_update")));

    // A draft with the stale lock fails validation with an actionable error
    let d = mgr.draft_begin(&Principal::administrator()).await.unwrap();
    let report = mgr.draft_validate(&d.draft_id).await.unwrap();
    assert!(!report.valid);
    assert!(report.issues.iter().any(|i| i.code == "LOCK_MISMATCH"));
}

#[tokio::test]
async fn replacement_instances_are_prepared_before_commit() {
    let (_tmp, _ws, _data, mgr) = setup(true).await;
    let admin = Principal::administrator();
    let head = mgr.active_revision().await.unwrap();
    let d = mgr.draft_begin(&admin).await.unwrap();
    // Valid declaration whose runtime module never finishes loading (exceeds the budget). The
    // declared handler must be defined in the module, or loading fails before VM preparation.
    mgr.draft_write_definition(
        &d.draft_id,
        "engineering",
        "spin",
        r#"local dash = require("dash"); return dash.data_provider { id = "engineering.spin", handler = "query" }"#,
        Some("local R = {}\nfunction R.query(ctx) return {} end\nlocal n = 0\nwhile true do n = n + 1 end\nreturn R\n"),
        None,
    )
    .unwrap();
    mgr.draft_upsert_instance(
        &d.draft_id,
        &dash_core::InstanceConfig {
            id: "spin_inst".into(),
            category: dash_luau::DefinitionCategory::DataProvider,
            definition: "engineering.spin".into(),
            inputs: Default::default(),
            config: serde_json::json!({}),
            secrets: Default::default(),
            capabilities: vec![],
            run_as: None,
            enabled: None,
        },
    )
    .unwrap();
    let err = mgr
        .draft_activate(&d.draft_id, &admin)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("could not be prepared"), "{err}");
    assert_eq!(
        mgr.active_revision().await.unwrap(),
        head,
        "nothing may be committed"
    );
    assert!(!mgr
        .engine()
        .graph
        .load()
        .instances
        .contains_key("spin_inst"));
}

#[tokio::test]
async fn downstream_dependents_get_fresh_vms_and_stale_recompute() {
    let (_tmp, _ws, _data, mgr) = setup(true).await;
    let admin = Principal::administrator();
    mgr.engine()
        .queries
        .render_dashboard("engineering", &admin)
        .await
        .unwrap();
    let table_vm = mgr.engine().pool.vm_uid("pr_table_adapter").unwrap();
    let trend_vm = mgr.engine().pool.vm_uid("pr_trend_adapter").unwrap();

    // Change linked_prs (upstream of pr_table_adapter only)
    let d = mgr.draft_begin(&admin).await.unwrap();
    let rt = Path::new(&d.worktree_path).join("packages/engineering/linked_prs/runtime.luau");
    let src = std::fs::read_to_string(&rt).unwrap();
    std::fs::write(&rt, format!("{src}\n-- revised\n")).unwrap();
    let act = mgr.draft_activate(&d.draft_id, &admin).await.unwrap();

    assert!(act.rebuilt_instances.contains(&"linked_prs".to_string()));
    assert!(act
        .rebuilt_instances
        .contains(&"pr_table_adapter".to_string()));
    assert!(act
        .stale_instances
        .contains(&"pr_table_adapter".to_string()));
    assert!(act
        .carried_over_instances
        .contains(&"pr_trend_adapter".to_string()));
    let new_table_vm = mgr.engine().pool.vm_uid("pr_table_adapter");
    assert!(
        new_table_vm.is_some(),
        "replacement VM prepared before the switch"
    );
    assert_ne!(new_table_vm.unwrap(), table_vm);
    assert_eq!(
        mgr.engine().pool.vm_uid("pr_trend_adapter").unwrap(),
        trend_vm
    );
}
