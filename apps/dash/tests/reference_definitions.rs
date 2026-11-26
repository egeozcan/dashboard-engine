//! Reference application Definitions (§16) and their Definition tests (§4.5).
//!
//! - every fixture `tests.luau` passes against the Mocks and its behavioural cases are present;
//! - the tests have teeth: re-introducing a behaviour bug in a Definition's runtime makes the
//!   named behavioural case fail (so the activation gate is not vacuous);
//! - §16 behaviour 4: the trend Adapter's counter *and* 30-day chart follow the Dashboard's
//!   selected repository, end to end (history sampled under its Service principal from the Mocks,
//!   served by `pr_count_history`, rendered through `GET /api/dashboards/engineering`).

mod common_t4;

use common_t4::*;
use dash_core::{CapabilityCeiling, Principal};
use dash_luau::{DefinitionTestReport, ResourceBudget};
use serde_json::{json, Value};
use std::collections::BTreeSet;

async fn definition_reports(w: &Workspace) -> Vec<DefinitionTestReport> {
    dash_git::run_workspace_definition_tests(&w.ws, &w.data, None)
        .await
        .unwrap()
}

fn report<'a>(reports: &'a [DefinitionTestReport], id: &str) -> &'a DefinitionTestReport {
    reports
        .iter()
        .find(|r| r.definition_id == id)
        .unwrap_or_else(|| panic!("no Definition test report for {id}"))
}

fn case_passed(reports: &[DefinitionTestReport], id: &str, case: &str) -> bool {
    let r = report(reports, id);
    r.cases
        .iter()
        .find(|c| c.name == case)
        .unwrap_or_else(|| panic!("{id} has no test case '{case}': {:?}", r.cases))
        .passed
}

const BEHAVIOUR_CASES: &[(&str, &str)] = &[
    ("github.merge_pr", "dry_run reports an open PR as feasible"),
    (
        "github.merge_pr",
        "execute merges through the github Mock and reconcile observes the merge",
    ),
    (
        "github.merge_pr",
        "after a Mock merge, dry_run reports the PR infeasible with an explanation",
    ),
    (
        "github.merge_pr",
        "dry_run of a PR the Mock does not know fails instead of reporting feasible",
    ),
    (
        "github.pull_requests",
        "filters open PRs by repository via the github Mock",
    ),
    (
        "github.pull_requests",
        "the reader input cannot merge through the connector",
    ),
    (
        "github.pull_requests",
        "polling emits one durable github.pr_merged when an open PR turns merged",
    ),
    (
        "engineering.linked_prs",
        "joins Mock-backed PRs with Mock-backed Jira issues",
    ),
    (
        "engineering.linked_prs",
        "repository parameter selects the joined rows",
    ),
    (
        "engineering.pr_table_adapter",
        "on_request translates merge_requested to the merge action",
    ),
    (
        "engineering.pr_trend_adapter",
        "render counts Mock-backed open PRs and labels both widgets with the repository",
    ),
    (
        "engineering.pr_trend_adapter",
        "counter and trend chart both follow the selected repository",
    ),
    (
        "engineering.pr_trend_adapter",
        "a repository without sampled history renders an empty trend, not another repository's",
    ),
];

#[tokio::test]
async fn reference_definition_tests_pass_against_mocks() {
    let w = Workspace::ecc();
    let reports = definition_reports(&w).await;
    for r in &reports {
        assert_eq!(r.failed, 0, "Definition test failed: {r:?}");
    }
    for (id, case) in BEHAVIOUR_CASES {
        assert!(case_passed(&reports, id, case), "{id}: '{case}' failed");
    }
    // No reference test is a bare "handler exists" smoke check any more.
    let names: BTreeSet<_> = reports
        .iter()
        .flat_map(|r| r.cases.iter().map(|c| c.name.to_lowercase()))
        .collect();
    assert!(
        names
            .iter()
            .all(|n| !n.contains("exposes") && !n.contains("exports")),
        "{names:?}"
    );
}

/// Replace `from` with `to` in a fixture runtime of the temp Workspace (the mutation must apply).
fn mutate(w: &Workspace, rel: &str, from: &str, to: &str) {
    let p = w.ws.join("packages").join(rel);
    let src = std::fs::read_to_string(&p).unwrap();
    assert!(src.contains(from), "mutation anchor missing in {rel}");
    std::fs::write(&p, src.replacen(from, to, 1)).unwrap();
}

#[tokio::test]
async fn reference_definition_tests_catch_behaviour_regressions() {
    struct Mutation {
        file: &'static str,
        from: &'static str,
        to: &'static str,
        id: &'static str,
        catches: &'static str,
    }
    let mutations = [
        // Dry run claims every PR is mergeable.
        Mutation {
            file: "github/merge_pr/runtime.luau",
            from: "local feasible = (pr.state == \"open\")",
            to: "local feasible = true",
            id: "github.merge_pr",
            catches: "after a Mock merge, dry_run reports the PR infeasible with an explanation",
        },
        // Polling stops reporting external merges.
        Mutation {
            file: "github/pull_requests/runtime.luau",
            from: "if pr.state == \"merged\" then",
            to: "if false then",
            id: "github.pull_requests",
            catches: "polling emits one durable github.pr_merged when an open PR turns merged",
        },
        // Trend chart pinned to one hardcoded series again (the original ecc-20 bug).
        Mutation {
            file: "engineering/pr_trend_adapter/runtime.luau",
            from: "ctx.inputs.history:query({ params = selection })",
            to: "ctx.inputs.history:query({ params = { repository = \"acme/platform\", state = \"open\" } })",
            id: "engineering.pr_trend_adapter",
            catches: "counter and trend chart both follow the selected repository",
        },
        // Join ignores the requested repository.
        Mutation {
            file: "engineering/linked_prs/runtime.luau",
            from: "repository = ctx.query.repository,",
            to: "repository = \"acme/platform\",",
            id: "engineering.linked_prs",
            catches: "repository parameter selects the joined rows",
        },
    ];
    for m in mutations {
        let w = Workspace::ecc();
        mutate(&w, m.file, m.from, m.to);
        let reports = definition_reports(&w).await;
        assert!(
            !case_passed(&reports, m.id, m.catches),
            "mutating {} was not caught by '{}'",
            m.file,
            m.catches
        );
        assert!(report(&reports, m.id).failed >= 1);
    }
}

fn find_widget<'a>(node: &'a Value, widget_id: &str) -> Option<&'a Value> {
    if node.get("widget_id").and_then(|v| v.as_str()) == Some(widget_id) {
        return Some(node);
    }
    match node {
        Value::Array(items) => items.iter().find_map(|c| find_widget(c, widget_id)),
        Value::Object(map) => map.values().find_map(|c| find_widget(c, widget_id)),
        _ => None,
    }
}

async fn render_trend(client: &reqwest::Client, b: &Booted) -> (Value, Value) {
    let resp = client
        .get(b.url("/api/dashboards/engineering"))
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(status, 200, "{body}");
    let counter = find_widget(&body["rendered"], "pr_trend_adapter_open_count")
        .unwrap_or_else(|| panic!("no counter in {body}"))
        .clone();
    let chart = find_widget(&body["rendered"], "pr_trend_adapter_trend_chart")
        .unwrap_or_else(|| panic!("no trend chart in {body}"))
        .clone();
    (counter, chart)
}

#[tokio::test]
async fn trend_adapter_follows_the_selected_repository() {
    let w = Workspace::ecc();
    let b = boot(&w, true, ResourceBudget::default()).await;
    let s = &b.services;
    let admin = Principal::administrator();
    s.store()
        .set_service_principal_grant(
            "pr_history_sampler",
            serde_json::to_value(CapabilityCeiling {
                allowed_providers: BTreeSet::from(["open_prs".into()]),
                allowed_connectors: BTreeSet::from(["github_reader".into()]),
                allowed_capabilities: BTreeSet::from(["pull_requests.read".into()]),
                ..Default::default()
            })
            .unwrap(),
            "admin",
        )
        .unwrap();
    // Sample every configured series once, under its Service principal, from the github Mock.
    let series = s.history_series_list();
    let ids: BTreeSet<_> = series.iter().map(|c| c.series_id.as_str()).collect();
    assert_eq!(
        ids,
        BTreeSet::from(["open_prs_30d", "open_prs_30d_payments"])
    );
    for cfg in &series {
        let expected = if cfg.params["repository"] == "acme/payments" {
            1.0
        } else {
            3.0
        };
        let v = s.engine().history.sample_series(cfg, None).await.unwrap();
        assert_eq!(v, expected, "{}", cfg.series_id);
    }

    let client = reqwest::Client::new();
    let (counter, chart) = render_trend(&client, &b).await;
    assert_eq!(counter["props"]["value"], 3);
    assert_eq!(counter["props"]["label"], "Open PRs (acme/platform)");
    assert_eq!(
        chart["props"]["title"],
        "30-Day Open PR Count (acme/platform)"
    );
    let platform = chart["props"]["series"].as_array().unwrap();
    assert_eq!(platform.len(), 1);
    assert_eq!(platform[0]["series_id"], "open_prs_30d");
    assert_eq!(platform[0]["value_num"], 3.0);

    let set = client
        .post(b.url("/api/dashboards/engineering/state"))
        .bearer_auth(ADMIN_TOKEN)
        .json(&json!({ "key": "selected_repository", "value": "acme/payments" }))
        .send()
        .await
        .unwrap();
    assert_eq!(set.status(), 200, "{}", set.text().await.unwrap());

    let (counter, chart) = render_trend(&client, &b).await;
    assert_eq!(counter["props"]["value"], 1);
    assert_eq!(counter["props"]["label"], "Open PRs (acme/payments)");
    assert_eq!(
        chart["props"]["title"],
        "30-Day Open PR Count (acme/payments)"
    );
    let payments = chart["props"]["series"].as_array().unwrap();
    assert_eq!(payments.len(), 1, "{payments:?}");
    assert_eq!(payments[0]["series_id"], "open_prs_30d_payments");
    assert_eq!(payments[0]["value_num"], 1.0);

    // A repository nobody samples has no trend (never another repository's).
    s.engine()
        .state
        .set_dashboard_state(
            &s.graph(),
            &admin,
            "engineering",
            "selected_repository",
            json!("acme/unsampled"),
            None,
        )
        .unwrap();
    let (counter, chart) = render_trend(&client, &b).await;
    assert_eq!(counter["props"]["value"], 0);
    assert_eq!(chart["props"]["series"], json!([]));
}
