//! Regression tests for draft transactions: quoted Git paths in rebase, State migrations on
//! startup and crash recovery, push outside the activation lock, mutations holding the busy
//! slot, failed reload cleanup, reload-aware crash repair, credential-free remote URLs and Git
//! trace spans (§8, §12.2, §12.3, §15.1).

use super::*;
use crate::git_cli::{git_commit_tree, git_snapshot_worktree_tree};
use dash_core::state::{StateKeyDeclaration, StateMigrationPolicy, StateValueType};
use std::time::{Duration, Instant};
use tempfile::TempDir;

async fn setup() -> (TempDir, PathBuf, PathBuf, DraftManager) {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    crate::packages::copy_tree_excluding_git(&fixture, &ws).unwrap();
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize(
        ws.clone(),
        data.clone(),
        store,
        dash_luau::ResourceBudget::default(),
    )
    .await
    .unwrap();
    let ws = std::fs::canonicalize(ws).unwrap();
    let data = std::fs::canonicalize(data).unwrap();
    (tmp, ws, data, mgr)
}

async fn reopen(ws: &Path, data: &Path) -> Result<DraftManager, GitError> {
    let store = SqliteStore::open(data).unwrap();
    DraftManager::initialize(
        ws.to_path_buf(),
        data.to_path_buf(),
        store,
        dash_luau::ResourceBudget::default(),
    )
    .await
}

async fn commit_file(ws: &Path, rel: &str, content: &str) -> String {
    let p = ws.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(&p, content).unwrap();
    run_git(ws, &["add", "-A"]).await.unwrap();
    run_git(ws, &["commit", "-m", &format!("external: {rel}")])
        .await
        .unwrap();
    git_head(ws).await.unwrap()
}

fn ops_dashboard(title: &str) -> DashboardDoc {
    DashboardDoc {
        id: "ops".into(),
        title: title.into(),
        layout: "grid".into(),
        adapters: vec![],
        initial_state: Default::default(),
        overrides: Default::default(),
        state_schema: Default::default(),
    }
}

/// The fixture's `engineering` dashboard with `selected_repository` declared at schema v2.
fn engineering_v2(
    mgr: &DraftManager,
    default: serde_json::Value,
    migration: StateMigrationPolicy,
) -> DashboardDoc {
    let mut doc = mgr.engine().graph.load().dashboards["engineering"].clone();
    doc.initial_state
        .insert("selected_repository".into(), default);
    doc.state_schema.insert(
        "selected_repository".into(),
        StateKeyDeclaration {
            schema_version: 2,
            value_type: StateValueType::String,
            migration,
            ..Default::default()
        },
    );
    doc
}

fn selected_repository(mgr: &DraftManager) -> (serde_json::Value, i64) {
    let rec = mgr
        .engine()
        .state
        .get("dashboard", "engineering", "selected_repository")
        .unwrap()
        .unwrap();
    (rec.value, rec.schema_version)
}

// ---- R4:tx-1: C-quoted paths ----

#[tokio::test]
async fn rebase_applies_and_conflicts_on_non_ascii_file_names() {
    let (_tmp, ws, _data, mgr) = setup().await;
    let admin = Principal::administrator();
    let rel = "docs/übersicht.md";
    commit_file(&ws, rel, "v1\n").await;

    // Upstream-only change to a non-ASCII-named file is applied to the draft
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    commit_file(&ws, rel, "v2\n").await;
    mgr.draft_rebase(&d.draft_id).await.unwrap();
    let wt = PathBuf::from(&d.worktree_path);
    assert_eq!(std::fs::read_to_string(wt.join(rel)).unwrap(), "v2\n");

    // Both sides changed it differently: an explicit conflict, never a silent keep
    std::fs::write(wt.join(rel), "draft edit\n").unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops 2"))
        .unwrap();
    commit_file(&ws, rel, "v3\n").await;
    match mgr.draft_rebase(&d.draft_id).await {
        Err(GitError::MergeConflict(files)) => assert_eq!(files, vec![rel.to_string()]),
        other => panic!("expected a conflict on {rel}, got {other:?}"),
    }
    assert_eq!(
        std::fs::read_to_string(wt.join(rel)).unwrap(),
        "draft edit\n"
    );
}

// ---- R4:tx-2 / R8:SC-1: State migrations at startup ----

#[tokio::test]
async fn crash_before_state_migration_is_completed_with_the_migration_on_startup() {
    let (_tmp, ws, data, mgr) = setup().await;
    let admin = Principal::administrator();
    assert_eq!(selected_repository(&mgr).1, 1);
    let d = mgr.draft_begin(&admin).await.unwrap();
    let doc = engineering_v2(&mgr, json!("acme/payments"), StateMigrationPolicy::Reset);
    mgr.draft_update_dashboard(&d.draft_id, &doc).unwrap();
    mgr.set_crash_point_for_tests(Some("after_generation_recorded"));
    let err = mgr.draft_activate(&d.draft_id, &admin).await.unwrap_err();
    assert!(is_simulated_crash(&err), "{err}");
    let target = git_head(&ws).await.unwrap();
    drop(mgr);

    let mgr = reopen(&ws, &data).await.unwrap();
    assert_eq!(mgr.active_generation_revision(), target);
    assert_eq!(mgr.draft_status(&d.draft_id).unwrap().status, "activated");
    assert_eq!(
        selected_repository(&mgr),
        (json!("acme/payments"), 2),
        "the reset migration the draft was approved for must run before serving"
    );
    assert!(mgr
        .store()
        .list_incomplete_activation_journals()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn interrupted_activation_whose_state_cannot_migrate_is_rolled_back_on_startup() {
    let (_tmp, ws, data, mgr) = setup().await;
    let admin = Principal::administrator();
    let base = git_head(&ws).await.unwrap();
    let d = mgr.draft_begin(&admin).await.unwrap();
    let doc = engineering_v2(&mgr, json!("acme/payments"), StateMigrationPolicy::Keep);
    mgr.draft_update_dashboard(&d.draft_id, &doc).unwrap();
    mgr.set_crash_point_for_tests(Some("after_committed"));
    assert!(mgr.draft_activate(&d.draft_id, &admin).await.is_err());
    assert_ne!(git_head(&ws).await.unwrap(), base);
    // The stored value changes so that `keep` can no longer validate it at v2
    let gen = mgr.engine().graph.load().generation_id;
    mgr.engine()
        .state
        .set(
            "dashboard",
            "engineering",
            "selected_repository",
            1,
            gen,
            json!(7),
            None,
        )
        .unwrap();
    drop(mgr);

    let mgr = reopen(&ws, &data).await.unwrap();
    assert_eq!(git_head(&ws).await.unwrap(), base, "HEAD rolled back");
    assert_eq!(mgr.active_generation_revision(), base);
    assert!(git_tracked_changes(&ws).await.unwrap().is_empty());
    assert_eq!(selected_repository(&mgr), (json!(7), 1), "State untouched");
    assert_ne!(mgr.draft_status(&d.draft_id).unwrap().status, "activated");
    assert!(PathBuf::from(&d.worktree_path).exists(), "draft kept");
    assert!(mgr
        .startup_warnings()
        .iter()
        .any(|w| w.contains("rolled back") && w.contains("cannot be migrated")));
    assert!(mgr
        .store()
        .list_incomplete_activation_journals()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn external_head_with_unmigratable_state_refuses_to_start_and_migratable_one_migrates() {
    let (_tmp, ws, data, mgr) = setup().await;
    let reject = engineering_v2(&mgr, json!("x"), StateMigrationPolicy::Reject);
    let reset = engineering_v2(&mgr, json!("acme/web"), StateMigrationPolicy::Reset);
    drop(mgr);
    let base = git_head(&ws).await.unwrap();

    let yaml = serde_yaml_ng::to_string(&reject).unwrap();
    commit_file(&ws, "dashboards/engineering.yaml", &yaml).await;
    let err = reopen(&ws, &data)
        .await
        .err()
        .expect("must refuse to start");
    assert!(
        err.to_string().contains("refuses to serve it unmigrated"),
        "{err}"
    );

    run_git(&ws, &["reset", "--hard", &base]).await.unwrap();
    let yaml = serde_yaml_ng::to_string(&reset).unwrap();
    commit_file(&ws, "dashboards/engineering.yaml", &yaml).await;
    let mgr = reopen(&ws, &data).await.unwrap();
    assert_eq!(selected_repository(&mgr), (json!("acme/web"), 2));
}

// ---- R4:tx-3: push outside the activation lock, bounded ----

#[tokio::test]
async fn hung_push_is_bounded_and_does_not_hold_the_activation_lock() {
    let (tmp, ws, _data, mgr) = setup().await;
    let admin = Principal::administrator();
    let hang = tmp.path().join("hang.sh");
    std::fs::write(&hang, "#!/bin/sh\nsleep 30\n").unwrap();
    std::process::Command::new("chmod")
        .args(["+x", &hang.to_string_lossy()])
        .status()
        .unwrap();
    run_git(
        &ws,
        &[
            "remote",
            "add",
            "origin",
            "ssh://git@example.invalid/acme/ws.git",
        ],
    )
    .await
    .unwrap();
    run_git(&ws, &["config", "core.sshCommand", &hang.to_string_lossy()])
        .await
        .unwrap();
    mgr.set_push_timeout_for_tests(Duration::from_secs(4));

    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    let started = Instant::now();
    let m = mgr.clone();
    let id = d.draft_id.clone();
    let task =
        tokio::spawn(async move { m.draft_activate(&id, &Principal::administrator()).await });
    // Wait until the activation is live and final (only the push is left)
    loop {
        if mgr.draft_status(&d.draft_id).unwrap().status == "activated" {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "activation stuck"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!task.is_finished(), "push is expected to still be hanging");
    assert!(
        mgr.activate_lock.try_lock().is_ok(),
        "the activation lock must not be held while pushing"
    );
    assert!(mgr.acquire_busy(&d.draft_id, "probe").is_ok());

    let res = task.await.unwrap().unwrap();
    assert_eq!(res.sync_status, "unsynced");
    assert!(
        res.sync_error
            .as_deref()
            .unwrap_or("")
            .contains("timed out"),
        "{:?}",
        res.sync_error
    );
    assert!(started.elapsed() < Duration::from_secs(25));
    assert_eq!(mgr.active_generation_revision(), res.revision_commit);
}

#[tokio::test]
async fn activation_pushes_its_commit_to_the_configured_branch() {
    let (tmp, ws, _data, mgr) = setup().await;
    let admin = Principal::administrator();
    let bare = tmp.path().join("remote.git");
    std::fs::create_dir_all(&bare).unwrap();
    run_git(&bare, &["init", "--bare", "-q"]).await.unwrap();
    run_git(&ws, &["remote", "add", "origin", &bare.to_string_lossy()])
        .await
        .unwrap();
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    let res = mgr.draft_activate(&d.draft_id, &admin).await.unwrap();
    assert_eq!(res.sync_status, "synced", "{:?}", res.sync_error);
    let remote_head = run_git(&bare, &["rev-parse", "refs/heads/main"])
        .await
        .unwrap();
    assert_eq!(remote_head, res.revision_commit);
}

// ---- R4:tx-4: mutations hold the busy slot ----

#[tokio::test]
async fn mutation_holds_the_busy_slot_until_recorded() {
    let (_tmp, _ws, _data, mgr) = setup().await;
    let admin = Principal::administrator();
    let d = mgr.draft_begin(&admin).await.unwrap();
    let guard = mgr.begin_mutation(&d.draft_id, &admin, "edit").unwrap();
    let err = mgr
        .acquire_busy(&d.draft_id, "activate")
        .err()
        .expect("activation must not start during a mutation");
    assert!(err.to_string().contains("edit in progress"), "{err}");
    assert!(mgr
        .draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .is_err());
    drop(guard);
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    // The slot is released after the mutation; an activation can now proceed
    let busy = mgr.acquire_busy(&d.draft_id, "activate").unwrap();
    assert!(mgr
        .draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops 2"))
        .is_err());
    drop(busy);
}

// ---- R4:tx-5: failed reloads clean up ----

#[tokio::test]
async fn failed_reload_aborts_its_draft_and_removes_the_worktree() {
    let (_tmp, ws, data, mgr) = setup().await;
    let admin = Principal::administrator();
    std::fs::write(ws.join("dashboards/broken.yaml"), "id: [unclosed\n").unwrap();
    for _ in 0..3 {
        assert!(mgr.reload_workspace(&admin).await.is_err());
    }
    let drafts = mgr.draft_list().unwrap();
    assert_eq!(drafts.len(), 3);
    for d in &drafts {
        assert_eq!(d.status, "aborted", "{d:?}");
        assert!(!Path::new(&d.worktree_path).exists());
    }
    let wts = run_git(&ws, &["worktree", "list", "--porcelain"])
        .await
        .unwrap();
    assert!(
        !wts.contains(&data.join("drafts").to_string_lossy().to_string()),
        "{wts}"
    );
    // A later valid reload still works
    std::fs::remove_file(ws.join("dashboards/broken.yaml")).unwrap();
    std::fs::write(ws.join("docs.md"), "notes\n").unwrap();
    mgr.reload_workspace(&admin).await.unwrap();
}

// ---- R4:tx-6: reload-aware crash repair ----

#[tokio::test]
async fn crash_repair_of_an_interrupted_reload_rollback_keeps_working_tree_edits() {
    let (_tmp, ws, data, mgr) = setup().await;
    let admin = Principal::administrator();
    let base = git_head(&ws).await.unwrap();
    let base_tree = git_tree_of(&ws, &base).await.unwrap();
    // The user's uncommitted edits that `dash reload` proposed
    std::fs::write(ws.join("notes.md"), "new file\n").unwrap();
    let eng = ws.join("dashboards/engineering.yaml");
    let edited = format!("{}# user edit\n", std::fs::read_to_string(&eng).unwrap());
    std::fs::write(&eng, &edited).unwrap();

    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.record_draft_mutation(&d.draft_id, &admin, RELOAD_CHANGE.into())
        .unwrap();
    let snap = git_snapshot_worktree_tree(&ws).await.unwrap();
    let target = git_commit_tree(&ws, &snap, &base, "reload", ("a", "a@x"), ("a", "a@x"))
        .await
        .unwrap();
    // Advance (HEAD + index only), then a rollback that died after restoring HEAD
    git_update_head(&ws, &target, &base, "test").await.unwrap();
    git_read_tree_index_only(&ws, &target).await.unwrap();
    git_update_head(&ws, &base, &target, "test").await.unwrap();
    mgr.store()
        .upsert_activation_journal(&ActivationJournalRecord {
            journal_id: "jrn_reload".into(),
            draft_id: d.draft_id.clone(),
            base_revision: base.clone(),
            target_revision: Some(target.clone()),
            generation_id: None,
            phase: "committed".into(),
            error_message: None,
            updated_at: Utc::now().to_rfc3339(),
        })
        .unwrap();
    drop(mgr);

    let _mgr = reopen(&ws, &data).await.unwrap();
    assert_eq!(git_head(&ws).await.unwrap(), base);
    assert_eq!(git_index_tree(&ws).await.unwrap(), base_tree);
    assert_eq!(
        std::fs::read_to_string(ws.join("notes.md")).unwrap(),
        "new file\n"
    );
    assert_eq!(std::fs::read_to_string(&eng).unwrap(), edited);
}

// ---- R7:SEC-4: credentials never leave through remote URLs ----

#[tokio::test]
async fn git_status_and_sync_never_expose_remote_credentials() {
    let (_tmp, ws, _data, mgr) = setup().await;
    let admin = Principal::administrator();
    let token = "ghp_S3cretTokenValue123";
    run_git(
        &ws,
        &[
            "remote",
            "add",
            "origin",
            &format!("https://x-access-token:{token}@127.0.0.1:9/acme/ws.git"),
        ],
    )
    .await
    .unwrap();
    mgr.set_push_timeout_for_tests(Duration::from_secs(20));
    let st = mgr.git_status().await.unwrap();
    assert_eq!(
        st.remote_url.as_deref(),
        Some("https://127.0.0.1:9/acme/ws.git")
    );
    let rec = mgr.git_sync(&admin).await.unwrap();
    assert_eq!(rec.sync_status, "unsynced");
    let persisted = serde_json::to_string(&rec).unwrap();
    assert!(!persisted.contains(token), "{persisted}");
    let st = serde_json::to_string(&mgr.git_status().await.unwrap()).unwrap();
    assert!(!st.contains(token), "{st}");
}

// ---- R8:SC-11: Git draft trace spans ----

#[tokio::test]
async fn validation_activation_and_sync_record_linked_trace_spans() {
    let (_tmp, _ws, _data, mgr) = setup().await;
    let admin = Principal::administrator();
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d.draft_id, &ops_dashboard("Ops"))
        .unwrap();
    mgr.draft_validate(&d.draft_id).await.unwrap();
    mgr.draft_activate(&d.draft_id, &admin).await.unwrap();
    mgr.git_sync(&admin).await.unwrap();

    let spans = mgr.engine().telemetry.recent_spans(200);
    let find = |name: &str| {
        spans.iter().find(|s| s.name == name).unwrap_or_else(|| {
            panic!(
                "no '{name}' span in {:?}",
                spans.iter().map(|s| &s.name).collect::<Vec<_>>()
            )
        })
    };
    assert_eq!(find("draft.validate").status, "ok");
    let activate = find("draft.activate");
    for child in ["draft.preview", "state.migrate", "generation.swap"] {
        let s = find(child);
        assert_eq!(s.parent_span_id.as_deref(), Some(activate.span_id.as_str()));
        assert_eq!(s.trace_id, activate.trace_id);
    }
    let pushes: Vec<_> = spans.iter().filter(|s| s.name == "git.push").collect();
    assert!(pushes
        .iter()
        .any(|s| s.parent_span_id.as_deref() == Some(activate.span_id.as_str())));
    let sync = find("git.sync");
    assert!(pushes
        .iter()
        .any(|s| s.parent_span_id.as_deref() == Some(sync.span_id.as_str())));
    assert_eq!(
        find("state.migrate").attributes["generation_id"],
        json!(mgr.engine().graph.load().generation_id)
    );
}
