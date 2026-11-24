//! Draft activation runs Dashboard State migrations transactionally; a failing migration refuses
//! activation and preserves the live Generation and its State (§8).

use dash_core::state::{StateKeyDeclaration, StateMigrationPolicy, StateValueType};
use dash_core::Principal;
use dash_git::DraftManager;
use dash_luau::ResourceBudget;
use dash_store::SqliteStore;
use serde_json::json;
use std::path::Path;
use tempfile::TempDir;

#[tokio::test]
async fn activation_migrates_state_or_refuses_and_preserves_live_state() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&fixture, &ws).unwrap();
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize(ws, data, store, ResourceBudget::default())
        .await
        .unwrap();
    let admin = Principal::administrator();
    let state = mgr.engine().state.clone();
    let live_gen = mgr.engine().graph.load().generation_id;
    let rec = state
        .get("dashboard", "engineering", "selected_repository")
        .unwrap()
        .expect("initial_state seeded at startup");
    assert_eq!(rec.schema_version, 1);

    // Draft 1: the key becomes an integer at schema v2 with policy `keep`; the stored string can
    // not be kept, so activation is refused before anything is committed or swapped.
    let base = mgr.engine().graph.load().dashboards["engineering"].clone();
    let mut bad = base.clone();
    bad.initial_state
        .insert("selected_repository".into(), json!(7));
    bad.state_schema.insert(
        "selected_repository".into(),
        StateKeyDeclaration {
            schema_version: 2,
            value_type: StateValueType::Integer,
            ..Default::default()
        },
    );
    let d1 = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d1.draft_id, &bad).unwrap();
    let err = mgr
        .draft_activate(&d1.draft_id, &admin)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("state migration failed"), "{err}");
    assert_eq!(mgr.engine().graph.load().generation_id, live_gen);
    let still = state
        .get("dashboard", "engineering", "selected_repository")
        .unwrap()
        .unwrap();
    assert_eq!((still.value, still.schema_version), (rec.value.clone(), 1));
    mgr.draft_abort(&d1.draft_id).await.unwrap();

    // Draft 2: schema v2 with policy `reset` migrates the record to the new default.
    let mut good = base;
    good.initial_state
        .insert("selected_repository".into(), json!("acme/payments"));
    good.state_schema.insert(
        "selected_repository".into(),
        StateKeyDeclaration {
            schema_version: 2,
            value_type: StateValueType::String,
            migration: StateMigrationPolicy::Reset,
            ..Default::default()
        },
    );
    let d2 = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_update_dashboard(&d2.draft_id, &good).unwrap();
    mgr.draft_activate(&d2.draft_id, &admin).await.unwrap();
    assert!(mgr.engine().graph.load().generation_id > live_gen);
    let migrated = state
        .get("dashboard", "engineering", "selected_repository")
        .unwrap()
        .unwrap();
    assert_eq!(
        (migrated.value, migrated.schema_version),
        (json!("acme/payments"), 2)
    );
}
