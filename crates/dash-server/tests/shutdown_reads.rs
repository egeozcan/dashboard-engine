//! §15.2 graceful shutdown: once readers are gone, the daemon cancels in-flight read
//! computations so they stop holding VM capacity while admitted actions drain (SC-12).

use dash_core::{InstanceConfig, LoadedDefinition, Principal};
use dash_git::DraftManager;
use dash_luau::{evaluate_declaration, DefinitionCategory, ResourceBudget};
use dash_server::AppServices;
use dash_store::SqliteStore;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_cancels_in_flight_reads() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&fixture, &ws).unwrap();
    let store = SqliteStore::open(&data).unwrap();
    let budget = ResourceBudget {
        wall_timeout: Duration::from_secs(30),
        max_interrupts: u64::MAX,
        ..ResourceBudget::default()
    };
    let mgr = DraftManager::initialize_with_mocks(ws, data, store, budget, true)
        .await
        .unwrap();
    let services = AppServices::new(mgr, vec![]);

    // A runaway read computation in the live graph.
    let decl = r#"local dash = require("dash")
return dash.data_provider { id = "shutdown.spin" }"#;
    let runtime = "local R = {}; function R.query(ctx) while true do end end; return R";
    let mut sources = BTreeMap::new();
    sources.insert("definition.luau".to_string(), decl.as_bytes().to_vec());
    sources.insert("runtime.luau".to_string(), runtime.as_bytes().to_vec());
    let mut g = (*services.graph()).clone();
    g.definitions.insert(
        "shutdown.spin".into(),
        LoadedDefinition {
            package_name: "shutdown".into(),
            manifest: evaluate_declaration(decl, &sources).unwrap(),
            definition_source: decl.into(),
            runtime_source: runtime.into(),
            tests_source: None,
            fixtures: BTreeMap::new(),
            widget_assets: BTreeMap::new(),
        },
    );
    g.instances.insert(
        "spin".into(),
        serde_json::from_value::<InstanceConfig>(json!({
            "id": "spin",
            "category": DefinitionCategory::DataProvider,
            "definition": "shutdown.spin",
        }))
        .unwrap(),
    );
    services.engine().graph.store(Arc::new(g));

    let s2 = services.clone();
    let started = Instant::now();
    let read = tokio::spawn(async move {
        s2.engine()
            .queries
            .query("spin", json!({}), &Principal::administrator(), false)
            .await
    });
    // Let the computation start.
    let mut cancelled = 0;
    while cancelled == 0 && started.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancelled = services.cancel_inflight_reads_for_shutdown();
    }
    assert_eq!(cancelled, 1, "the running read is signalled");
    let res = tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .expect("cancelled read stops long before its 30s wall timeout")
        .unwrap();
    assert!(res.is_err());
}
