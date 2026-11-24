//! R3:pool-prepare-replacements-old-budget: activation prepares replacement VMs under the
//! candidate Generation's `[resources]`, including Instances whose only change is their budget.

use dash_core::Principal;
use dash_git::DraftManager;
use dash_luau::ResourceBudget;
use dash_store::SqliteStore;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

async fn setup() -> (TempDir, DraftManager) {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let src =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&src, &ws).unwrap();
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize_with_mocks(ws, data, store, ResourceBudget::default(), true)
        .await
        .unwrap();
    (tmp, mgr)
}

const HEAVY_DECL: &str = r#"local dash = require("dash"); return dash.data_provider { id = "engineering.heavy", handler = "query" }"#;
/// Allocates ~4 MiB while `runtime.luau` loads: fits the default 32 MiB, not a 1 MiB budget.
const HEAVY_RT: &str = "local blob = string.rep(\"x\", 4 * 1024 * 1024)\nlocal R = {}\nfunction R.query(ctx) return #blob end\nreturn R\n";

fn heavy_instance() -> dash_core::InstanceConfig {
    dash_core::InstanceConfig {
        id: "heavy_inst".into(),
        category: dash_luau::DefinitionCategory::DataProvider,
        definition: "engineering.heavy".into(),
        inputs: Default::default(),
        config: serde_json::json!({}),
        secrets: Default::default(),
        capabilities: vec![],
        run_as: None,
        enabled: None,
    }
}

fn tighten_budget(worktree: &str) {
    let path = PathBuf::from(worktree).join("dashboard.toml");
    let mut toml = std::fs::read_to_string(&path).unwrap();
    toml.push_str("\n[resources.instances.heavy_inst]\nmemory_limit_mib = 1\n");
    std::fs::write(&path, toml).unwrap();
}

#[tokio::test]
async fn new_instance_is_prepared_under_its_new_resource_limits() {
    let (_tmp, mgr) = setup().await;
    let admin = Principal::administrator();
    let head = mgr.active_revision().await.unwrap();
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_write_definition(
        &d.draft_id,
        "engineering",
        "heavy",
        HEAVY_DECL,
        Some(HEAVY_RT),
        None,
    )
    .unwrap();
    mgr.draft_upsert_instance(&d.draft_id, &heavy_instance())
        .unwrap();
    tighten_budget(&d.worktree_path);
    let err = mgr
        .draft_activate(&d.draft_id, &admin)
        .await
        .expect_err("the module does not fit the new 1 MiB limit")
        .to_string();
    assert!(err.contains("could not be prepared"), "{err}");
    assert!(err.contains("heavy_inst"), "{err}");
    assert_eq!(mgr.active_revision().await.unwrap(), head);
}

#[tokio::test]
async fn resource_only_change_is_validated_before_commit() {
    let (_tmp, mgr) = setup().await;
    let admin = Principal::administrator();
    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_write_definition(
        &d.draft_id,
        "engineering",
        "heavy",
        HEAVY_DECL,
        Some(HEAVY_RT),
        None,
    )
    .unwrap();
    mgr.draft_upsert_instance(&d.draft_id, &heavy_instance())
        .unwrap();
    mgr.draft_activate(&d.draft_id, &admin)
        .await
        .expect("fits the default budget");
    let head = mgr.active_revision().await.unwrap();

    // Second draft changes nothing but the Instance's `[resources]` override.
    let d2 = mgr.draft_begin(&admin).await.unwrap();
    tighten_budget(&d2.worktree_path);
    let err = mgr
        .draft_activate(&d2.draft_id, &admin)
        .await
        .expect_err("a budget the live module cannot fit must be refused before commit")
        .to_string();
    assert!(err.contains("could not be prepared"), "{err}");
    assert_eq!(mgr.active_revision().await.unwrap(), head);
}
