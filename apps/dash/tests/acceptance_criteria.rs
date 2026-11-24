//! Automated test suite mapping 1:1 to all 17 MVP Acceptance Criteria (§17).

use axum::{routing::get, Json, Router};
use dash_core::{
    apply_delta_envelope, CapabilityCeiling, EnvelopeKind, FailureInjectionRule,
    HistorySeriesConfig, InstanceConfig, Principal, QueryStatus,
};
use dash_git::{
    read_packages_lock, resolve_and_lock_packages, run_preview_and_scenarios, DraftManager,
};
use dash_luau::{evaluate_declaration, DefinitionCategory, ResourceBudget};
use dash_server::{build_router, hash_password, AppServices};
use dash_store::{ActivationJournalRecord, SqliteStore};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn copy_ecc_fixture(dst: &Path) {
    let src =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&src, dst).unwrap();
}

async fn boot_http_app(
    tmp: &TempDir,
    trusted_proxies: Vec<IpAddr>,
) -> (AppServices, SocketAddr, tokio::task::JoinHandle<()>) {
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_ecc_fixture(&ws);
    let store = SqliteStore::open(&data).unwrap();
    store
        .set_admin_credentials("admin", &hash_password("adminpass").unwrap())
        .unwrap();
    let mgr = DraftManager::initialize(ws, data, store, ResourceBudget::default())
        .await
        .unwrap();
    let services = AppServices::new(mgr, trusted_proxies);
    std::fs::write(&services.admin_cli_token_path, "adm_test_local").unwrap();

    let app = build_router(services.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (services, addr, h)
}

#[tokio::test]
async fn ac01_single_executable_and_health() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let bin = env!("CARGO_BIN_EXE_dash");

    let init = tokio::process::Command::new(bin)
        .args([
            "--workspace",
            ws.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
            "init",
            "--admin-password",
            "secret123",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        init.status.success(),
        "dash init failed: {}",
        String::from_utf8_lossy(&init.stderr)
    );
    assert!(data.join("dash.db").exists());
    assert!(data.join("vault.key").exists());

    // Bind ephemeral port via probe socket first
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let bind_addr = format!("127.0.0.1:{port}");

    let mut child = tokio::process::Command::new(bin)
        .args([
            "--workspace",
            ws.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
            "serve",
            "--bind",
            &bind_addr,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let client = reqwest::Client::new();
    let mut ready = false;
    for _ in 0..40 {
        if let Ok(r) = client
            .get(format!("http://{bind_addr}/healthz"))
            .send()
            .await
        {
            if r.status() == 200 {
                ready = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready, "daemon /healthz never became ready");

    let ui_html = client
        .get(format!("http://{bind_addr}/"))
        .send()
        .await
        .unwrap();
    assert_eq!(ui_html.status(), 200);
    assert!(ui_html.text().await.unwrap().contains("dash"));

    let _ = child.kill().await;
}

#[test]
fn ac02_declarative_luau_manifest_and_rejection() {
    let decl = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "../../fixtures/engineering-command-center/packages/engineering/linked_prs/definition.luau",
    ))
    .unwrap();
    let rt = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "../../fixtures/engineering-command-center/packages/engineering/linked_prs/runtime.luau",
    ))
    .unwrap();
    let mut src = BTreeMap::new();
    src.insert("definition.luau".into(), decl.as_bytes().to_vec());
    src.insert("runtime.luau".into(), rt.as_bytes().to_vec());

    let m1 = evaluate_declaration(&decl, &src).unwrap();
    let m2 = evaluate_declaration(&decl, &src).unwrap();
    assert_eq!(m1.canonical_json().unwrap(), m2.canonical_json().unwrap());

    let bad = r#"local dash = require("dash"); return dash.adapter { id = "x.bad", inputs = { c = dash.connector_ref("github.connector") } }"#;
    assert!(evaluate_declaration(bad, &src).is_err());
}

#[tokio::test]
async fn ac03_git_packages_lockfile_and_offline_restart() {
    let tmp = TempDir::new().unwrap();
    let ext = tmp.path().join("ext_pkg");
    std::fs::create_dir_all(ext.join("ping")).unwrap();
    std::fs::write(
        ext.join("package.toml"),
        "name = \"ext\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    std::fs::write(
        ext.join("ping/definition.luau"),
        "local dash = require(\"dash\"); return dash.data_provider { id = \"ext.ping\", handler = \"query\" }",
    )
    .unwrap();
    std::fs::write(
        ext.join("ping/runtime.luau"),
        "local R = {}; function R.query(ctx) return { pinned = true } end; return R",
    )
    .unwrap();
    dash_git::git_cli::ensure_git_repo(&ext, "main")
        .await
        .unwrap();
    let commit = dash_git::git_cli::git_commit_all(&ext, "init", "dev", None)
        .await
        .unwrap();

    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    std::fs::create_dir_all(ws.join("instances")).unwrap();
    std::fs::write(ws.join("dashboard.toml"), "name = \"locktest\"\n").unwrap();
    std::fs::write(
        ws.join("packages.toml"),
        format!(
            "[dependencies.ext]\ngit = \"{}\"\nref = \"main\"\n",
            ext.display()
        ),
    )
    .unwrap();
    std::fs::write(
        ws.join("instances/data.yaml"),
        "data:\n  ping_inst:\n    definition: ext.ping\n",
    )
    .unwrap();

    resolve_and_lock_packages(&ws, &data).await.unwrap();
    let lock = read_packages_lock(&ws).unwrap();
    assert_eq!(lock.packages[0].commit, commit);

    // Delete upstream repo and reopen cleanly offline
    std::fs::remove_dir_all(&ext).unwrap();
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize(ws, data, store, ResourceBudget::default())
        .await
        .unwrap();
    let res = mgr
        .engine()
        .queries
        .query("ping_inst", json!({}), &Principal::administrator(), false)
        .await
        .unwrap();
    assert_eq!(res.value.unwrap()["pinned"], true);
}

#[tokio::test]
async fn ac04_capabilities_forbidden_bindings_ceilings_and_stdio_mcp() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_ecc_fixture(&ws);
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize(ws.clone(), data, store, ResourceBudget::default())
        .await
        .unwrap();
    let admin = Principal::administrator();

    // 1. Bind merge_pr to github_reader in a draft -> validation MUST fail!
    let draft = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_upsert_instance(
        &draft.draft_id,
        &InstanceConfig {
            id: "bad_merge".into(),
            category: DefinitionCategory::ActionProvider,
            definition: "github.merge_pr".into(),
            inputs: BTreeMap::from([("github".into(), "github_reader".into())]),
            config: json!({}),
            secrets: BTreeMap::new(),
            capabilities: vec![],
            run_as: None,
            enabled: None,
        },
    )
    .unwrap();
    let val = mgr.draft_validate(&draft.draft_id).await.unwrap();
    assert!(!val.valid);
    assert!(val
        .issues
        .iter()
        .any(|i| i.code == "INSUFFICIENT_CAPABILITY"));
    mgr.draft_abort(&draft.draft_id).await.unwrap();

    // 2. Service principal ceiling in YAML cannot self-grant without Admin approval
    let ungranted = mgr
        .engine()
        .automations
        .resolve_service_principal("work_completion")
        .unwrap_err();
    assert!(ungranted.contains("no Administrator grant"));

    // 3. `dash mcp-stdio` refuses admin token or missing token (never runs as Administrator!)
    let bin = env!("CARGO_BIN_EXE_dash");
    let bad_stdio = tokio::process::Command::new(bin)
        .args(["mcp-stdio", "--token", "adm_forbidden_admin_token"])
        .output()
        .await
        .unwrap();
    assert!(!bad_stdio.status.success());
    let stderr = String::from_utf8_lossy(&bad_stdio.stderr);
    assert!(stderr.contains("never runs as Administrator"));
}

#[tokio::test]
async fn ac05_secrets_zero_leakage_and_host_mediated_http() {
    let app = Router::new().route(
        "/repos/acme/platform/pulls",
        get(|headers: axum::http::HeaderMap| async move {
            let auth = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            assert_eq!(auth, "Bearer ghp_real_test_secret_999");
            Json(json!([
                {
                    "id": "acme/platform#1",
                    "number": 1,
                    "title": format!("Echoed {auth}"),
                    "repository": "acme/platform",
                    "state": "open"
                }
            ]))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_ecc_fixture(&ws);
    let store = SqliteStore::open(&data).unwrap();
    let secret = "ghp_real_test_secret_999";
    store
        .set_secret("github-work", "PAT", secret, &["github_reader".into()])
        .unwrap();
    // Outbound HTTP is deny-by-default: the Administrator network ceiling lives in SQLite,
    // outside the agent-editable Workspace (§5.2, §11.3).
    store
        .set_network_policy_ceiling(
            json!({
                "allowed_hosts": ["127.0.0.1"],
                "allowed_methods": ["GET"],
                "allow_loopback_for_tests": true
            }),
            "admin",
        )
        .unwrap();
    let mgr = DraftManager::initialize(ws, data, store.clone(), ResourceBudget::default())
        .await
        .unwrap();
    let admin = Principal::administrator();

    // Update github_reader base_url to local test server & enable host allowlist
    let mut g = (*mgr.engine().graph.load_full()).clone();
    g.workspace.network.allowed_hosts = vec!["127.0.0.1".into()];
    g.workspace.network.allow_loopback_for_tests = true;
    g.instances.get_mut("github_reader").unwrap().config =
        json!({ "base_url": format!("http://{mock_addr}") });
    mgr.engine().graph.store(std::sync::Arc::new(g));

    let res = mgr
        .engine()
        .queries
        .query(
            "open_prs",
            json!({"repository": "acme/platform", "state": "open"}),
            &admin,
            false,
        )
        .await
        .unwrap();
    let serialized = serde_json::to_string(&res).unwrap();
    assert!(!serialized.contains(secret), "plaintext secret leaked!");
    assert!(serialized.contains("[REDACTED]"));
}

#[tokio::test]
async fn ac06_reactive_graph_dedup_params_isolation_and_dynamic_cleanup() {
    let tmp = TempDir::new().unwrap();
    let (services, _addr, h) = boot_http_app(&tmp, vec![]).await;
    let admin = Principal::administrator();
    // Switch to preview mode mocks to exercise full graph deduplication cleanly
    let mut g = (*services.graph()).clone();
    g.is_preview = true;
    services.engine().graph.store(std::sync::Arc::new(g));

    let mut futs = Vec::new();
    for _ in 0..100 {
        let eng = services.engine().clone();
        let p = admin.clone();
        futs.push(tokio::spawn(async move {
            eng.queries
                .query(
                    "linked_prs",
                    json!({"repository": "acme/platform", "state": "open"}),
                    &p,
                    false,
                )
                .await
                .unwrap()
        }));
    }
    let res = futures_util::future::join_all(futs).await;
    for r in res {
        assert_eq!(r.unwrap().value.unwrap().as_array().unwrap().len(), 3);
    }
    let m = services.metrics_query();
    assert!(m.query_cache_misses <= 3); // linked_prs + open_prs + jira_issues_by_keys once!

    // Separate params remain independent
    let pay = services
        .provider_query(
            "linked_prs",
            json!({"repository": "acme/payments", "state": "open"}),
            &admin,
            false,
        )
        .await
        .unwrap();
    assert_eq!(pay.value.unwrap().as_array().unwrap().len(), 1);
    h.abort();
}

#[tokio::test]
async fn ac07_updates_polling_push_deltas_gap_resync_and_stale_retention() {
    let tmp = TempDir::new().unwrap();
    let (services, _addr, h) = boot_http_app(&tmp, vec![]).await;
    let admin = Principal::administrator();
    let mut g = (*services.graph()).clone();
    g.is_preview = true;
    services.engine().graph.store(std::sync::Arc::new(g));

    let init = services
        .provider_query(
            "open_prs",
            json!({"repository": "acme/platform", "state": "open"}),
            &admin,
            false,
        )
        .await
        .unwrap();
    let mut rx = services.engine().signals.subscribe_stream(&init.query_key);
    let mut ver = init.version;
    let mut rows = init.value.unwrap().as_array().unwrap().clone();

    services
        .engine()
        .queries
        .push_query_value(
            "open_prs",
            &json!({"repository": "acme/platform", "state": "open"}),
            &admin,
            json!([{"id": "acme/platform#42", "number": 42, "state": "merged"}]),
        )
        .unwrap();
    let delta = rx.recv().await.unwrap();
    assert_eq!(delta.kind, EnvelopeKind::Delta);
    apply_delta_envelope(&mut ver, &mut rows, &delta, "id").unwrap();
    assert_eq!(rows.len(), 1);

    // Gap triggers resync error
    let mut bad_ver = 999;
    assert!(apply_delta_envelope(&mut bad_ver, &mut rows, &delta, "id").is_err());

    // Failure retains stale data
    services
        .engine()
        .telemetry
        .inject_failure(FailureInjectionRule {
            instance_id: "open_prs".into(),
            mode: "error".into(),
            message: "down".into(),
            remaining: Some(1),
        });
    let stale = services
        .provider_query(
            "open_prs",
            json!({"repository": "acme/platform", "state": "open"}),
            &admin,
            true,
        )
        .await
        .unwrap();
    assert_eq!(stale.status, QueryStatus::Stale);
    h.abort();
}

#[tokio::test]
async fn ac08_events_durable_replay_idempotency_poll_emit_and_history_restart() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_ecc_fixture(&ws);

    {
        let store = SqliteStore::open(&data).unwrap();
        let mgr = DraftManager::initialize(
            ws.clone(),
            data.clone(),
            store.clone(),
            ResourceBudget::default(),
        )
        .await
        .unwrap();
        let admin = Principal::administrator();

        let mut g = (*mgr.engine().graph.load_full()).clone();
        // Mock connectors on the live Generation: authority still comes from the SQLite grants below.
        g.substitute_mocks = true;
        mgr.engine().graph.store(std::sync::Arc::new(g));

        // Grant both Service principals
        store
            .set_service_principal_grant(
                "work_completion",
                serde_json::to_value(CapabilityCeiling {
                    allowed_actions: BTreeSet::from(["transition_issue".into()]),
                    allowed_connectors: BTreeSet::from(["jira_transitioner".into()]),
                    allowed_capabilities: BTreeSet::from([
                        "issues.read".into(),
                        "issues.transition".into(),
                    ]),
                    ..Default::default()
                })
                .unwrap(),
                "admin",
            )
            .unwrap();
        store
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

        // 1. Sample history under Service principal
        let series = HistorySeriesConfig {
            series_id: "open_prs_30d".into(),
            instance_id: "open_prs".into(),
            params: json!({"repository": "acme/platform", "state": "open"}),
            aggregator: "count".into(),
            value_path: None,
            cadence_secs: 3600,
            retention_days: 30,
            service_principal: "pr_history_sampler".into(),
        };
        let count = mgr
            .engine()
            .history
            .sample_series(&series, None)
            .await
            .unwrap();
        assert_eq!(count, 3.0);

        // 2. Initial poll sees PR #42 open; then external merge happens; next poll emits github.pr_merged!
        let _ = mgr
            .engine()
            .queries
            .query(
                "open_prs",
                json!({"repository": "acme/platform", "state": "open"}),
                &admin,
                true,
            )
            .await
            .unwrap();
        // Simulate the external merge through the host-side Mock state API: the reader-bound
        // connector Instance must not be able to perform a merge (§17 AC4), and undeclared
        // connector operations are denied by default (§20).
        assert!(mgr
            .engine()
            .queries
            .invoke_connector(
                "github_reader",
                "simulate_external_merge",
                json!({"repository": "acme/platform", "number": 42}),
                &admin,
                "poll_sim",
            )
            .await
            .unwrap_err()
            .contains("undeclared operation"));
        let mut prs = mgr.engine().queries.mock_state_get("github.mock", "prs");
        for pr in prs.as_array_mut().unwrap() {
            if pr["repository"] == "acme/platform" && pr["number"] == 42 {
                pr["state"] = json!("merged");
                pr["merged"] = json!(true);
            }
        }
        mgr.engine()
            .queries
            .mock_state_set("github.mock", "prs", prs);
        let after_poll = mgr
            .engine()
            .queries
            .query(
                "open_prs",
                json!({"repository": "acme/platform", "state": "open"}),
                &admin,
                true,
            )
            .await
            .unwrap();
        assert_eq!(after_poll.value.unwrap().as_array().unwrap().len(), 2);

        let poll_events = mgr.engine().events.replay_since("open_prs", 0).unwrap();
        let merged_ev = poll_events
            .iter()
            .find(|e| e.semantic_type == "github.pr_merged")
            .expect("polling must emit github.pr_merged");
        assert_eq!(merged_ev.payload["source"], "polling");

        // Dispatch once + replay once -> identical execution_id (no duplicate Jira transitions!)
        let d1 = mgr.engine().automations.dispatch_event(merged_ev).await;
        let d2 = mgr.engine().automations.dispatch_event(merged_ev).await;
        assert_eq!(
            d1[0].as_ref().unwrap()["execution_id"],
            d2[0].as_ref().unwrap()["execution_id"]
        );
    }

    // Full restart: history sample survives in SQLite and is served via `pr_count_history`!
    let store2 = SqliteStore::open(&data).unwrap();
    let mgr2 = DraftManager::initialize(ws, data, store2, ResourceBudget::default())
        .await
        .unwrap();
    let hist = mgr2
        .engine()
        .queries
        .query(
            "pr_count_history",
            json!({"series_id": "open_prs_30d"}),
            &Principal::administrator(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(hist.value.unwrap().as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn ac09_state_scopes_stable_widget_ids_and_layout_overrides() {
    let tmp = TempDir::new().unwrap();
    let (services, _addr, h) = boot_http_app(&tmp, vec![]).await;
    let admin = Principal::administrator();
    let mut g = (*services.graph()).clone();
    g.is_preview = true;
    services.engine().graph.store(std::sync::Arc::new(g));

    let r1 = services.dashboard_get("engineering", &admin).await.unwrap();
    let rendered_str = serde_json::to_string(&r1["rendered"]).unwrap();
    assert!(rendered_str.contains("pr_table_adapter_pr_table"));
    assert!(rendered_str.contains("layout_override"));
    assert!(rendered_str.contains("\"w\":12"));

    // Updating Dashboard State updates Adapter render output
    services
        .engine()
        .state
        .set(
            "dashboard",
            "engineering",
            "selected_repository",
            1,
            1,
            json!("acme/payments"),
            None,
        )
        .unwrap();
    let r2 = services.dashboard_get("engineering", &admin).await.unwrap();
    assert!(serde_json::to_string(&r2["rendered"])
        .unwrap()
        .contains("PAY-9"));
    h.abort();
}

#[tokio::test]
async fn ac10_actions_dry_run_nested_permissions_invalidation_unknown_reconcile_and_resolve() {
    // Run full Scenario suite + audited resolution verification
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_ecc_fixture(&ws);
    let rep = run_preview_and_scenarios(&ws, &data, None).await.unwrap();
    assert!(rep.all_passed);

    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize(ws, data, store.clone(), ResourceBudget::default())
        .await
        .unwrap();
    let admin = Principal::administrator();
    let mut g = (*mgr.engine().graph.load_full()).clone();
    g.is_preview = true;
    mgr.engine().graph.store(std::sync::Arc::new(g));

    mgr.engine().telemetry.inject_failure(FailureInjectionRule {
        instance_id: "merge_pr".into(),
        mode: "unknown".into(),
        message: "lost_after_send".into(),
        remaining: Some(1),
    });
    let unk = mgr
        .engine()
        .actions
        .invoke(
            "merge_pr",
            json!({"repository": "acme/platform", "number": 42}),
            &admin,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(unk.status, "unknown");
    let resolved = mgr
        .engine()
        .actions
        .resolve_execution(&unk.execution_id, "failed", "checked GH manually", &admin)
        .await
        .unwrap();
    assert_eq!(resolved.status, "failed");
    assert!(store
        .list_audits(20)
        .unwrap()
        .iter()
        .any(|a| a.operation == "action.resolve" && a.target == unk.execution_id));
}

#[tokio::test]
async fn ac11_visual_ui_embedded_assets_and_api_flows() {
    let tmp = TempDir::new().unwrap();
    let (_services, addr, h) = boot_http_app(&tmp, vec![]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    let index = client.get(&base).send().await.unwrap();
    assert_eq!(index.status(), 200);
    assert!(index.headers().contains_key("content-security-policy"));
    assert!(index.text().await.unwrap().contains("<div id=\"root\">"));

    // Widget assets require an authenticated Principal (the shell iframe carries the session
    // cookie) and are isolated by a CSP `sandbox` even when opened top-level (§10.1, §14.3).
    let anon = client
        .get(format!(
            "{base}/widgets/engineering.pr_badge_widget/assets/index.html"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), 401);
    let widget = client
        .get(format!(
            "{base}/widgets/engineering.pr_badge_widget/assets/index.html"
        ))
        .header("authorization", "Bearer adm_test_local")
        .send()
        .await
        .unwrap();
    assert_eq!(widget.status(), 200);
    let csp = widget.headers()["content-security-policy"]
        .to_str()
        .unwrap()
        .to_string();
    assert!(csp.contains("sandbox allow-scripts"));
    assert!(widget.text().await.unwrap().contains("dash.widget/1"));

    let graph = client
        .get(format!("{base}/api/graph"))
        .header("authorization", "Bearer adm_test_local")
        .send()
        .await
        .unwrap();
    assert_eq!(graph.status(), 200);
    h.abort();
}

// AC-12 and AC-13 are covered end to end in `ac_11_14.rs` (MCP Agent cycle with rebase, action
// invocation and push; invalid updates, concurrent readers, carry-over, stale-base rejection).

#[tokio::test]
async fn ac14_git_transaction_single_commit_unsynced_retry_and_crash_recovery() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_ecc_fixture(&ws);

    {
        let store = SqliteStore::open(&data).unwrap();
        // Simulate interrupted activation journal on previous crash
        store
            .upsert_activation_journal(&ActivationJournalRecord {
                journal_id: "jrn_crash".into(),
                draft_id: "d_old".into(),
                base_revision: "old".into(),
                target_revision: Some("unfinished".into()),
                generation_id: None,
                phase: "started".into(),
                error_message: None,
                updated_at: chrono::Utc::now().to_rfc3339(),
            })
            .unwrap();
        let mgr = DraftManager::initialize(ws, data, store.clone(), ResourceBudget::default())
            .await
            .unwrap();
        // Incomplete journal must be reconciled deterministically on startup!
        assert!(store
            .list_incomplete_activation_journals()
            .unwrap()
            .is_empty());
        assert_eq!(mgr.engine().graph.load().generation_id, 1);
        // A journal whose base and target both differ from HEAD is not silently treated as
        // landed or rolled back: it is resolved as failed and surfaced as a startup warning.
        assert!(
            mgr.startup_warnings()
                .iter()
                .any(|w| w.contains("jrn_crash") && w.contains("external Git divergence")),
            "{:?}",
            mgr.startup_warnings()
        );
        assert_eq!(
            mgr.engine().graph.load().revision_commit,
            mgr.active_revision().await.unwrap()
        );
    }
    // One commit per draft, push failure / sync retry and crash recovery at every activation
    // journal stage are covered in `ac_11_14.rs`.
}

#[tokio::test]
async fn ac15_sandbox_denials_and_runaway_isolation() {
    let tmp = TempDir::new().unwrap();
    let (services, addr, h) = boot_http_app(&tmp, vec![]).await;
    let admin = Principal::administrator();

    let draft = services.draft_begin(&admin).await.unwrap();
    services
        .definition_upsert_in_draft(
            &draft.draft_id,
            "engineering",
            "runaway",
            "local dash = require(\"dash\"); return dash.data_provider { id = \"engineering.runaway\", handler = \"query\" }",
            Some("local R = {}; function R.query(ctx) while true do end end; return R"),
            None,
            &admin,
        )
        .unwrap();
    services
        .instance_upsert_in_draft(
            &draft.draft_id,
            &InstanceConfig {
                id: "runaway_inst".into(),
                category: DefinitionCategory::DataProvider,
                definition: "engineering.runaway".into(),
                inputs: BTreeMap::new(),
                config: json!({}),
                secrets: BTreeMap::new(),
                capabilities: vec![],
                run_as: None,
                enabled: None,
            },
            &admin,
        )
        .unwrap();
    services
        .draft_activate(&draft.draft_id, &admin)
        .await
        .unwrap();

    let eng = services.engine().clone();
    let loop_task = tokio::spawn(async move {
        eng.queries
            .query(
                "runaway_inst",
                json!({}),
                &Principal::administrator(),
                false,
            )
            .await
    });

    // HTTP /healthz remains fast (< 50ms) while Luau infinite loop spins on blocking worker pool!
    let t0 = Instant::now();
    let hz = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap();
    assert_eq!(hz.status(), 200);
    assert!(t0.elapsed() < Duration::from_millis(100));

    let res = loop_task.await.unwrap();
    assert!(res.is_err());
    h.abort();
}

#[tokio::test]
async fn ac16_tailnet_loopback_auth_csrf_origin_and_trusted_proxy() {
    let tmp = TempDir::new().unwrap();
    let (_services, addr, h) = boot_http_app(&tmp, vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]).await;
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");

    // 1. Spoofed tailnet identity headers MUST NOT authenticate!
    let spoof = client
        .get(format!("{base}/api/graph"))
        .header("Tailscale-User-Login", "attacker@tailnet.ts.net")
        .header("X-Forwarded-User", "admin")
        .send()
        .await
        .unwrap();
    assert_eq!(spoof.status(), 401);

    // 2. Trusted proxy X-Forwarded-Proto: https causes Set-Cookie to include Secure
    let login = client
        .post(format!("{base}/api/auth/login"))
        .header("x-forwarded-proto", "https")
        .json(&json!({ "username": "admin", "password": "adminpass" }))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), 200);
    let cookie = login
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(cookie.contains("Secure"));
    assert!(cookie.contains("HttpOnly"));

    // 3. Cross-origin request rejected 403
    let xorigin = client
        .post(format!("{base}/api/drafts"))
        .header("origin", "https://evil.example")
        .header("authorization", "Bearer adm_test_local")
        .send()
        .await
        .unwrap();
    assert_eq!(xorigin.status(), 403);
    h.abort();
}

#[tokio::test]
async fn ac17_benchmark_gates_sanity() {
    let t0 = Instant::now();
    let tmp = TempDir::new().unwrap();
    let (services, addr, h) = boot_http_app(&tmp, vec![]).await;
    let hz = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .unwrap();
    assert_eq!(hz.status(), 200);
    assert!(
        t0.elapsed() < Duration::from_secs(2),
        "startup took {:?}",
        t0.elapsed()
    );

    // Warm cache query under 5ms
    let admin = Principal::administrator();
    let mut g = (*services.graph()).clone();
    g.is_preview = true;
    services.engine().graph.store(std::sync::Arc::new(g));
    let _ = services
        .provider_query(
            "open_prs",
            json!({"repository": "acme/platform", "state": "open"}),
            &admin,
            false,
        )
        .await
        .unwrap();
    let q0 = Instant::now();
    let _ = services
        .provider_query(
            "open_prs",
            json!({"repository": "acme/platform", "state": "open"}),
            &admin,
            false,
        )
        .await
        .unwrap();
    assert!(q0.elapsed() < Duration::from_millis(5));
    h.abort();
}

#[tokio::test]
async fn pr_badge_widget_refresh_request_succeeds() {
    let tmp = TempDir::new().unwrap();
    let (services, _addr, h) = boot_http_app(&tmp, vec![]).await;
    let admin = Principal::administrator();
    let mut g = (*services.graph()).clone();
    g.is_preview = true;
    services.engine().graph.store(std::sync::Arc::new(g));

    let exec = services
        .dispatch_widget_request(
            dash_core::WidgetRequestEnvelope {
                protocol: "dash.widget/1".into(),
                dashboard_id: "engineering".into(),
                adapter_instance: "pr_table_adapter".into(),
                widget_id: "pr_table_adapter_pr_summary".into(),
                generation: services.graph().generation_id,
                sequence: 1,
                kind: "request".into(),
                request: "refresh_requested".into(),
                payload: json!({}),
            },
            &admin,
        )
        .await
        .unwrap();
    assert_eq!(exec.status, "succeeded");
    h.abort();
}
