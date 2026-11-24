//! Workspace canonical serializer, Git package resolver, draft transactions, preview, and hot reload.

pub mod drafts;
pub mod git_cli;
pub mod packages;
pub mod preview;
pub mod workspace;

pub use drafts::{ActivationResult, DraftManager};
pub use git_cli::{GitCommitInfo, GitError, GitStatusReport};
pub use packages::{
    compute_tree_digest, read_packages_lock, read_packages_toml, resolve_and_lock_packages,
    write_packages_lock, write_packages_toml, LockedPackage, PackageDependencySpec, PackagesLock,
    PackagesToml,
};
pub use preview::{
    run_definition_tests_against_mocks, run_preview_and_scenarios, run_workspace_definition_tests,
    CapturedTestEvent, MockTestBridge, PreviewTestReport, ScenarioResult,
};
pub use workspace::{
    delete_instance_in_workspace, load_workspace_graph, upsert_instance_in_workspace,
    write_dashboard_in_workspace, write_definition_in_workspace,
};

#[cfg(test)]
mod tests {
    use super::*;
    use dash_core::Principal;
    use dash_luau::{DefinitionCategory, ResourceBudget};
    use dash_store::SqliteStore;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::path::Path;
    use tempfile::TempDir;

    fn copy_fixture_ws(dst: &Path) {
        let src = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/engineering-command-center");
        packages::copy_tree_excluding_git(&src, dst).unwrap();
    }

    #[tokio::test]
    async fn test_engineering_command_center_preview_and_scenarios() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        copy_fixture_ws(&ws);

        let report = run_preview_and_scenarios(&ws, &data, None).await.unwrap();
        assert!(
            report.validation.valid,
            "validation issues: {:?}",
            report.validation.issues
        );
        for d in &report.definition_tests {
            assert_eq!(d.failed, 0, "def test failed: {:?}", d);
        }
        for s in &report.scenario_results {
            assert!(s.passed, "scenario failed: {:?}", s);
        }
        assert!(report.all_passed);
        assert!(report.preview_renders.contains_key("engineering"));
    }

    #[tokio::test]
    async fn test_external_git_package_lockfile_and_offline_restart() {
        let tmp = TempDir::new().unwrap();
        // Create external Git package repo
        let ext_repo = tmp.path().join("ext_pkg");
        std::fs::create_dir_all(ext_repo.join("hello")).unwrap();
        std::fs::write(
            ext_repo.join("package.toml"),
            "name = \"ext\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();
        std::fs::write(
            ext_repo.join("hello/definition.luau"),
            r#"
local dash = require("dash")
return dash.data_provider { id = "ext.hello", handler = "query" }
"#,
        )
        .unwrap();
        std::fs::write(
            ext_repo.join("hello/runtime.luau"),
            r#"local R = {}; function R.query(ctx) return { greeting = "pinned_v1" } end; return R"#,
        )
        .unwrap();
        git_cli::ensure_git_repo(&ext_repo, "main").await.unwrap();
        let pinned_commit =
            git_cli::git_commit_all(&ext_repo, "v1 release", "author", None)
                .await
                .unwrap();

        // Create workspace depending on ext_pkg with ref = "main"
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        std::fs::create_dir_all(ws.join("instances")).unwrap();
        std::fs::write(ws.join("dashboard.toml"), "name = \"ext-test\"\n").unwrap();
        std::fs::write(
            ws.join("packages.toml"),
            format!(
                "[dependencies.ext]\ngit = \"{}\"\nref = \"main\"\n",
                ext_repo.display()
            ),
        )
        .unwrap();
        std::fs::write(
            ws.join("instances/data.yaml"),
            "data:\n  hello_inst:\n    definition: ext.hello\n",
        )
        .unwrap();

        let lock = resolve_and_lock_packages(&ws, &data).await.unwrap();
        assert_eq!(lock.packages.len(), 1);
        assert_eq!(lock.packages[0].commit, pinned_commit);

        // Delete remote repository completely and advance nothing -> offline restart loads exact pinned version!
        std::fs::remove_dir_all(&ext_repo).unwrap();
        let store = SqliteStore::open(&data).unwrap();
        let mgr = DraftManager::initialize(ws, data, store, ResourceBudget::default())
            .await
            .unwrap();
        let res = mgr
            .engine()
            .queries
            .query("hello_inst", json!({}), &Principal::administrator(), false)
            .await
            .unwrap();
        assert_eq!(res.value.unwrap()["greeting"], "pinned_v1");
    }

    #[tokio::test]
    async fn test_draft_concurrent_rebase_carry_over_and_unsynced_retry() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        copy_fixture_ws(&ws);

        // Configure a bare remote that will temporarily fail push (e.g. non-existent path first, then real bare repo)
        let bare_remote = tmp.path().join("remote.git");
        let store = SqliteStore::open(&data).unwrap();
        let mgr = DraftManager::initialize(ws.clone(), data, store, ResourceBudget::default())
            .await
            .unwrap();
        let admin = Principal::administrator();

        // Warm open_prs VM before edits so we can verify Generation carry-over!
        let _ = mgr
            .engine()
            .pool
            .execute_handler(
                &dash_luau::CompiledInstanceBundle {
                    instance_id: "pr_trend_adapter".into(),
                    manifest: mgr.engine().graph.load().definitions["engineering.pr_trend_adapter"]
                        .manifest
                        .clone(),
                    runtime_source: mgr.engine().graph.load().definitions
                        ["engineering.pr_trend_adapter"]
                        .runtime_source
                        .clone(),
                    config_digest: mgr.engine().graph.load().instances["pr_trend_adapter"]
                        .config_digest(),
                    resolved_libraries: BTreeMap::new(),
                },
                "render",
                dash_luau::ExecutionContextConfig {
                    category: DefinitionCategory::Adapter,
                    instance_id: "pr_trend_adapter".into(),
                    input_names: vec![],
                    config: json!({}),
                    payload: json!({}),
                    operation: None,
                    idempotency_key: None,
                    allowed_emits: vec![],
                    now_iso: "2026-11-24T12:00:00Z".into(),
                    trace_id: "warm".into(),
                    bridge: std::sync::Arc::new(dash_luau::NullHostBridge),
                },
                None,
            )
            .await;
        let vm_uid_before = mgr.engine().pool.vm_uid("pr_trend_adapter");

        // Open three concurrent drafts at base rev 1
        let d1 = mgr.draft_begin(&admin).await.unwrap();
        let d2 = mgr.draft_begin(&admin).await.unwrap();
        let d3 = mgr.draft_begin(&admin).await.unwrap();

        // Point origin to a broken remote URL first to verify push failure -> `unsynced`!
        git_cli::run_git(
            &ws,
            &["remote", "add", "origin", "/nonexistent/remote/path.git"],
        )
        .await
        .unwrap();

        // Draft 1 adds a new data provider instance
        mgr.draft_write_definition(
            &d1.draft_id,
            "engineering",
            "hello_metric",
            r#"local dash = require("dash"); return dash.data_provider { id = "engineering.hello_metric", handler = "query" }"#,
            Some(r#"local R = {}; function R.query(ctx) return { count = 99 } end; return R"#),
            None,
        )
        .unwrap();
        mgr.draft_upsert_instance(
            &d1.draft_id,
            &dash_core::InstanceConfig {
                id: "hello_metric".into(),
                category: DefinitionCategory::DataProvider,
                definition: "engineering.hello_metric".into(),
                inputs: BTreeMap::new(),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        )
        .unwrap();

        let act1 = mgr.draft_activate(&d1.draft_id, &admin).await.unwrap();
        assert_eq!(act1.sync_status, "unsynced");
        assert!(act1.carried_over_instances.contains(&"pr_trend_adapter".into()));
        // Unchanged instance keeps its exact live VM across hot reload!
        assert_eq!(mgr.engine().pool.vm_uid("pr_trend_adapter"), vm_uid_before);

        // Fix remote and retry git_sync -> transitions cleanly to `synced`!
        std::fs::create_dir_all(&bare_remote).unwrap();
        git_cli::run_git(&bare_remote, &["init", "--bare"])
            .await
            .unwrap();
        git_cli::run_git(
            &ws,
            &["remote", "set-url", "origin", &bare_remote.to_string_lossy()],
        )
        .await
        .unwrap();
        let sync_res = mgr.git_sync(&admin).await.unwrap();
        assert_eq!(sync_res.sync_status, "synced");

        // Draft 2 now has stale base_revision -> activation MUST fail until rebased!
        mgr.draft_upsert_instance(
            &d2.draft_id,
            &dash_core::InstanceConfig {
                id: "hello_metric_two".into(),
                category: DefinitionCategory::DataProvider,
                definition: "engineering.hello_metric".into(),
                inputs: BTreeMap::new(),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
        )
        .unwrap();
        let stale_err = mgr
            .draft_activate(&d2.draft_id, &admin)
            .await
            .unwrap_err()
            .to_string();
        assert!(stale_err.contains("stale draft base revision"));

        // Draft 2 touched the same EOF hunk in instances/data.yaml -> rebase MUST fail with explicit MergeConflict!
        let conflict_err = mgr.draft_rebase(&d2.draft_id).await.unwrap_err();
        assert!(matches!(conflict_err, GitError::MergeConflict(ref f) if f.contains(&"instances/data.yaml".into())));
        mgr.draft_abort(&d2.draft_id).await.unwrap();

        // Draft 3 (also started on old base rev 1) edits a non-conflicting dashboard file -> rebases cleanly!
        mgr.draft_update_dashboard(
            &d3.draft_id,
            &dash_core::DashboardDoc {
                id: "ops".into(),
                title: "Ops Dashboard".into(),
                layout: "grid".into(),
                adapters: vec![],
                initial_state: BTreeMap::new(),
                overrides: BTreeMap::new(),
            },
        )
        .unwrap();
        let stale_d3 = mgr
            .draft_activate(&d3.draft_id, &admin)
            .await
            .unwrap_err()
            .to_string();
        assert!(stale_d3.contains("stale draft base revision"));

        let rebased = mgr.draft_rebase(&d3.draft_id).await.unwrap();
        assert_eq!(rebased.base_revision, act1.revision_commit);
        assert!(rebased.validation_report.is_none());
        let act2 = mgr.draft_activate(&d3.draft_id, &admin).await.unwrap();
        assert_eq!(act2.sync_status, "synced");
        assert!(mgr.engine().graph.load().instances.contains_key("hello_metric"));
        assert!(mgr.engine().graph.load().dashboards.contains_key("ops"));
    }

    #[tokio::test]
    async fn test_relative_paths_draft_worktree_activation_preserves_workspace() {
        let cwd = std::env::current_dir().unwrap();
        let tmp = tempfile::Builder::new()
            .prefix(".dash-rel-test-")
            .tempdir_in(&cwd)
            .unwrap();
        let rel_tmp = tmp.path().strip_prefix(&cwd).unwrap().to_path_buf();
        let rel_ws = rel_tmp.join("ws");
        let rel_data = rel_tmp.join("data");
        assert!(!rel_ws.is_absolute() && !rel_data.is_absolute());

        copy_fixture_ws(&rel_ws);
        let store = SqliteStore::open(&rel_data).unwrap();
        let mgr = DraftManager::initialize_with_mocks(
            rel_ws,
            rel_data,
            store,
            ResourceBudget::default(),
            true,
        )
        .await
        .unwrap();
        let admin = Principal::administrator();

        let status0 = mgr.git_status().await.unwrap();
        assert!(!status0.dirty, "initial status dirty: {:?}", status0.changed_files);
        assert!(status0.changed_files.is_empty());

        // 1. Package Studio draft flow: adding custom_metric must keep existing packages & dashboards intact
        let d1 = mgr.draft_begin(&admin).await.unwrap();
        assert!(
            Path::new(&d1.worktree_path).join("dashboard.toml").exists(),
            "draft worktree missing dashboard.toml at {}",
            d1.worktree_path
        );
        let status_during_d1 = mgr.git_status().await.unwrap();
        assert!(
            !status_during_d1.dirty,
            "workspace dirty after draft_begin: {:?}",
            status_during_d1.changed_files
        );

        mgr.draft_write_definition(
            &d1.draft_id,
            "engineering",
            "custom_metric",
            r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.custom_metric",
    runtime = "runtime.luau",
    handler = "query",
}
"#,
            Some(
                r#"--!strict
local Runtime = {}
function Runtime.query(ctx)
    return { ok = true, instance = ctx.instance_id }
end
return Runtime
"#,
            ),
            Some(
                r#"local dash = require("dash")
local Runtime = require("runtime")
dash.test("smoke", function(make_ctx)
    local res = Runtime.query(make_ctx({}))
    dash.assert(res.ok == true, "expected ok")
end)
"#,
            ),
        )
        .unwrap();
        let preview = mgr.draft_preview_and_test(&d1.draft_id).await.unwrap();
        assert!(preview.all_passed, "preview failed: {:?}", preview);

        let _act1 = mgr.draft_activate(&d1.draft_id, &admin).await.unwrap();
        let g1 = mgr.engine().graph.load();
        assert!(g1.definitions.contains_key("engineering.custom_metric"));
        assert!(g1.definitions.contains_key("engineering.linked_prs"));
        assert!(g1.dashboards.contains_key("engineering"));
        let status_after_d1 = mgr.git_status().await.unwrap();
        assert!(
            !status_after_d1.dirty,
            "workspace dirty after d1 activate: {:?}",
            status_after_d1.changed_files
        );

        // 2. Dashboard Designer override flow: updating override in draft activates cleanly
        let d2 = mgr.draft_begin(&admin).await.unwrap();
        let mut dash_doc = g1.dashboards.get("engineering").unwrap().clone();
        dash_doc.overrides.insert(
            "pr_table_adapter_pr_table".into(),
            dash_core::LayoutOverride {
                x: None,
                y: None,
                w: Some(6),
                h: Some(6),
                visible: Some(true),
                props: BTreeMap::new(),
            },
        );
        mgr.draft_update_dashboard(&d2.draft_id, &dash_doc).unwrap();
        let _act2 = mgr.draft_activate(&d2.draft_id, &admin).await.unwrap();
        let g2 = mgr.engine().graph.load();
        assert_eq!(
            g2.dashboards["engineering"].overrides["pr_table_adapter_pr_table"].w,
            Some(6)
        );
        let status_after_d2 = mgr.git_status().await.unwrap();
        assert!(
            !status_after_d2.dirty,
            "workspace dirty after d2 activate: {:?}",
            status_after_d2.changed_files
        );
    }
}
