//! Automation trigger runs execute off the maintenance ticker (§7.3, §15.2): a slow cron-run
//! automation must not stall polling, history sampling, reconciles or metrics sampling.

use dash_core::{CapabilityCeiling, InstanceConfig, LoadedDefinition, ServicePrincipalRequested};
use dash_git::DraftManager;
use dash_luau::{evaluate_declaration, DefinitionCategory, ResourceBudget};
use dash_server::AppServices;
use dash_store::SqliteStore;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const SLOW_CRON: &str = r#"
local dash = require("dash")
return dash.automation {
    id = "test.slow_cron",
    handler = "run",
    triggers = { { kind = "cron", schedule = "* * * * * *", max_attempts = 1 } },
}
"#;
/// Runs until the wall timeout: a stand-in for a slow action or a contended run lock.
const SLOW_CRON_RT: &str = r#"
local R = {}
function R.run(ctx)
    while true do end
end
return R
"#;

async fn boot_preview(tmp: &TempDir) -> AppServices {
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let src =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&src, &ws).unwrap();
    let store = SqliteStore::open(&data).unwrap();
    // A large interrupt budget lets the looping handler run until the 5 s wall timeout.
    let budget = ResourceBudget {
        max_interrupts: 50_000_000,
        ..ResourceBudget::default()
    };
    let mgr = DraftManager::initialize(ws, data, store, budget)
        .await
        .unwrap();
    AppServices::new(mgr, vec![])
}

#[tokio::test]
async fn a_slow_cron_automation_does_not_stall_the_maintenance_ticker() {
    let tmp = TempDir::new().unwrap();
    let services = boot_preview(&tmp).await;

    let mut g = (*services.graph()).clone();
    g.is_preview = true;
    let mut sources = BTreeMap::new();
    sources.insert("definition.luau".into(), SLOW_CRON.as_bytes().to_vec());
    sources.insert("runtime.luau".into(), SLOW_CRON_RT.as_bytes().to_vec());
    g.definitions.insert(
        "test.slow_cron".into(),
        LoadedDefinition {
            package_name: "test".into(),
            manifest: evaluate_declaration(SLOW_CRON, &sources).unwrap(),
            definition_source: SLOW_CRON.into(),
            runtime_source: SLOW_CRON_RT.into(),
            tests_source: None,
            fixtures: BTreeMap::new(),
            widget_assets: BTreeMap::new(),
        },
    );
    g.instances.insert(
        "slow_cron".into(),
        InstanceConfig {
            id: "slow_cron".into(),
            category: DefinitionCategory::Automation,
            definition: "test.slow_cron".into(),
            inputs: BTreeMap::new(),
            config: serde_json::json!({}),
            secrets: BTreeMap::new(),
            capabilities: vec![],
            run_as: Some("slow_sp".into()),
            enabled: None,
        },
    );
    g.service_principals.insert(
        "slow_sp".into(),
        ServicePrincipalRequested {
            id: "slow_sp".into(),
            description: "slow cron automation".into(),
            ceiling: CapabilityCeiling::default(),
        },
    );
    services.engine().graph.store(Arc::new(g));
    // The cron trigger last fired two seconds ago, so it is due (on time) at the first tick.
    let last = (chrono::Utc::now() - chrono::Duration::seconds(2)).to_rfc3339();
    services
        .store()
        .set_automation_trigger_state("slow_cron", "cron:0", Some(&last), None)
        .unwrap();
    assert!(services
        .store()
        .list_runtime_metrics(10)
        .unwrap()
        .is_empty());

    let (stop, rx) = tokio::sync::watch::channel(false);
    let workers = services.spawn_background_workers_until(rx);

    // The first maintenance tick samples runtime metrics. It must not wait for the cron run,
    // which only ends at the 5 s wall timeout.
    let started = Instant::now();
    let mut sampled = false;
    while started.elapsed() < Duration::from_secs(3) {
        if !services
            .store()
            .list_runtime_metrics(10)
            .unwrap()
            .is_empty()
        {
            sampled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(sampled, "maintenance tick stalled behind an automation run");

    // The cron run itself still happens, on the trigger task.
    let deadline = Instant::now() + Duration::from_secs(15);
    let run = loop {
        let runs = services
            .automation_executions(Some("slow_cron"), 10)
            .unwrap();
        if let Some(r) = runs.into_iter().find(|r| r.status != "running") {
            break r;
        }
        assert!(Instant::now() < deadline, "cron trigger never ran");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(run.trigger_kind, "cron");
    assert_eq!(run.status, "failed");
    let _ = stop.send(true);
    workers.abort();
}
