//! Background work on the reference application (§6.2, §7.2, §7.3, §16 behaviour 7):
//! scheduled polling without a browser, polled updates reaching subscribers, and one Jira
//! transition per merged PR whether the merge came from the dashboard or from polling.

use dash_core::Principal;
use dash_git::DraftManager;
use dash_luau::ResourceBudget;
use dash_server::AppServices;
use dash_store::SqliteStore;
use serde_json::json;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;

async fn boot_preview(tmp: &TempDir) -> AppServices {
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let src =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&src, &ws).unwrap();
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize(ws, data, store, ResourceBudget::default())
        .await
        .unwrap();
    let services = AppServices::new(mgr, vec![]);
    // Preview substitutes the fixture Mocks for GitHub/Jira and uses declared Service principal
    // ceilings, so no external tokens are needed.
    let mut g = (*services.graph()).clone();
    g.is_preview = true;
    services.engine().graph.store(std::sync::Arc::new(g));
    services
}

fn merged_events(services: &AppServices, source: &str) -> Vec<dash_core::EventEnvelope> {
    services
        .engine()
        .events
        .replay_since(source, 0)
        .unwrap()
        .into_iter()
        .filter(|e| e.semantic_type == "github.pr_merged")
        .collect()
}

fn transition_runs(services: &AppServices) -> usize {
    services
        .store()
        .list_action_executions(1000)
        .unwrap()
        .into_iter()
        .filter(|r| r.action_instance == "transition_issue" && r.status == "succeeded")
        .count()
}

/// A merge made outside the dashboard: mutate the github Mock's state from the host (the Mock no
/// longer exposes an undeclared `simulate_external_merge` connector operation, §20).
async fn simulate_external_merge(services: &AppServices, number: i64) {
    let queries = &services.engine().queries;
    let mut prs = queries.mock_state_get("github.mock", "prs");
    let rows = prs
        .as_array_mut()
        .expect("github Mock state is loaded by the first poll");
    let mut found = false;
    for pr in rows.iter_mut() {
        if pr["repository"] == "acme/platform" && pr["number"] == number {
            pr["state"] = json!("merged");
            pr["merged"] = json!(true);
            found = true;
        }
    }
    assert!(found, "PR #{number} not in the github Mock fixture");
    queries.mock_state_set("github.mock", "prs", prs);
}

#[tokio::test]
async fn external_merge_is_detected_by_background_polling_without_foreground_queries() {
    let tmp = TempDir::new().unwrap();
    let services = boot_preview(&tmp).await;
    let t0 = chrono::Utc::now().timestamp();

    // No browser, no foreground query: the keep-warm poll (history series params under the
    // pr_history_sampler Service principal) is the only reader.
    assert!(services.run_polls_at(t0).await >= 1);
    assert!(merged_events(&services, "open_prs").is_empty());

    simulate_external_merge(&services, 42).await;
    // Within the poll interval nothing runs again...
    assert_eq!(services.run_polls_at(t0 + 10).await, 0);
    // ...after poll_interval_secs (60) the merge is detected.
    assert!(services.run_polls_at(t0 + 61).await >= 1);
    let evs = merged_events(&services, "open_prs");
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].payload["source"], "polling");
    assert_eq!(evs[0].payload["number"], 42);

    let report = services.drain_durable_events().await;
    assert!(report.succeeded >= 1, "{report:?}");
    assert_eq!(transition_runs(&services), 1);
}

#[tokio::test]
async fn dashboard_merge_and_polling_detection_transition_the_issue_once() {
    let tmp = TempDir::new().unwrap();
    let services = boot_preview(&tmp).await;
    let admin = Principal::administrator();
    services.run_polls_at(chrono::Utc::now().timestamp()).await;

    let exec = services
        .engine()
        .actions
        .invoke(
            "merge_pr",
            json!({"repository": "acme/platform", "number": 42, "issue_key": "DEV-123"}),
            &admin,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(exec.status, "succeeded");
    // The action emits one Event; the invalidation refresh of the polled query detects the same
    // merge and emits another (same Event type, §17 AC8).
    assert_eq!(merged_events(&services, "merge_pr").len(), 1);
    assert_eq!(merged_events(&services, "open_prs").len(), 1);

    services.drain_durable_events().await;
    assert_eq!(
        transition_runs(&services),
        1,
        "one logical merge -> one Jira transition"
    );
    let runs = services
        .automation_executions(Some("complete_work"), 10)
        .unwrap();
    assert_eq!(runs.len(), 2);
    assert!(runs.iter().all(|r| r.status == "succeeded"));
    // Both automation runs point at the same (idempotent) action execution.
    assert_eq!(runs[0].action_executions, runs[1].action_executions);
}

#[tokio::test]
async fn polling_refreshes_subscribed_queries_in_the_subscribers_partition() {
    let tmp = TempDir::new().unwrap();
    let services = boot_preview(&tmp).await;
    let admin = Principal::administrator();
    let params = json!({"repository": "acme/platform", "state": "open"});
    let env = services
        .provider_query("open_prs", params.clone(), &admin, false)
        .await
        .unwrap();
    let unsubscribed = services
        .provider_query(
            "open_prs",
            json!({"repository": "acme/payments", "state": "open"}),
            &admin,
            false,
        )
        .await
        .unwrap();
    let mut rx = services.engine().signals.subscribe_stream(&env.query_key);

    services.run_polls_at(chrono::Utc::now().timestamp()).await;
    let next = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("poll must publish to the subscriber's query key")
        .unwrap();
    assert_eq!(next.query_key, env.query_key);
    assert!(next.version > env.version);
    let cached = services
        .engine()
        .queries
        .cached_entry(&unsubscribed.query_key)
        .unwrap();
    assert_eq!(
        cached.version, unsubscribed.version,
        "inactive queries are not polled"
    );
}

#[tokio::test]
async fn background_worker_consumes_durable_events_without_manual_dispatch() {
    let tmp = TempDir::new().unwrap();
    let services = boot_preview(&tmp).await;
    let handle = services.spawn_background_workers();
    services
        .engine()
        .actions
        .invoke(
            "merge_pr",
            json!({"repository": "acme/platform", "number": 42, "issue_key": "DEV-123"}),
            &Principal::administrator(),
            None,
            None,
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while transition_runs(&services) == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    handle.abort();
    assert_eq!(transition_runs(&services), 1);
}
