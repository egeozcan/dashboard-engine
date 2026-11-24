//! End-to-end checks that Agents cannot escalate through secret bindings, connector edits,
//! Service principal consumers or the network policy (§5.1.5, §5.2, §11.3, ADR-0002), and that
//! background work under a properly granted Service principal reaches credentialed connectors.

use axum::{routing::get, Json, Router};
use dash_core::{
    CapabilityCeiling, HistorySeriesConfig, InstanceConfig, NetworkPolicy, Principal, PrincipalKind,
};
use dash_git::DraftManager;
use dash_luau::{DefinitionCategory, ResourceBudget};
use dash_server::AppServices;
use dash_store::SqliteStore;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use tempfile::TempDir;

fn copy_ecc_fixture(dst: &Path) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&src, dst).unwrap();
}

async fn boot(tmp: &TempDir) -> AppServices {
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_ecc_fixture(&ws);
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize(ws, data, store, ResourceBudget::default())
        .await
        .unwrap();
    AppServices::new(mgr, vec![])
}

fn drafting_agent(services: &AppServices) -> Principal {
    let ceiling = CapabilityCeiling {
        allowed_providers: BTreeSet::from(["*".into()]),
        can_author_drafts: true,
        can_activate_drafts: true,
        ..Default::default()
    };
    let (rec, _token) = services
        .agent_create("drafting-bot", ceiling.clone(), None, &Principal::administrator())
        .unwrap();
    Principal {
        id: rec.agent_id,
        kind: PrincipalKind::Agent,
        ceiling,
    }
}

fn audits(services: &AppServices, op: &str, outcome: &str) -> usize {
    services
        .audit_query(500)
        .unwrap()
        .into_iter()
        .filter(|a| a.operation == op && a.outcome == outcome)
        .count()
}

#[tokio::test]
async fn agent_cannot_bind_or_redirect_administrator_secrets() {
    let tmp = TempDir::new().unwrap();
    let services = boot(&tmp).await;
    let admin = Principal::administrator();
    services
        .secret_set(
            "github-work",
            "PAT",
            "ghp_admin_only_secret_1",
            &["github_reader".into(), "github_maintainer".into(), "exfil".into()],
            &admin,
        )
        .unwrap();
    let agent = drafting_agent(&services);
    let draft = services.draft_begin(&agent).await.unwrap();
    let graph = services.graph();

    // 1. instance_create/instance_configure cannot bind an ungranted secret (same check as secret_bind).
    let mut exfil = graph.instances["github_reader"].clone();
    exfil.id = "exfil".into();
    let err = services
        .instance_upsert_in_draft(&draft.draft_id, &exfil, &agent)
        .unwrap_err();
    assert!(err.contains("does not permit binding secret 'github-work'"), "{err}");
    assert!(audits(&services, "secret.bind", "denied") >= 1);

    // 2. Redirecting the existing credentialed connector (secrets untouched) is a pending grant.
    let mut redirected = graph.instances["github_reader"].clone();
    redirected.config = json!({ "base_url": "https://attacker.example" });
    services
        .instance_upsert_in_draft(&draft.draft_id, &redirected, &agent)
        .unwrap();
    let report = services.draft_validate(&draft.draft_id).await.unwrap();
    assert!(!report.valid);
    assert!(report.issues.iter().any(|i| i.code == "PENDING_SECRET_GRANT"));
    let err = services.draft_activate(&draft.draft_id, &agent).await.unwrap_err();
    assert!(err.contains("PENDING_SECRET_GRANT"), "{err}");
    // Even the Administrator cannot activate a change an ungranted Agent authored.
    let err = services.draft_activate(&draft.draft_id, &admin).await.unwrap_err();
    assert!(err.contains("PENDING_SECRET_GRANT"), "{err}");
    assert_eq!(
        services.graph().instances["github_reader"].config["base_url"],
        "https://api.github.com",
        "live Generation untouched"
    );
    assert!(audits(&services, "draft.activate", "denied") >= 2);
    services.draft_abort(&draft.draft_id).await.unwrap();

    // 3. The Administrator may make the same change in their own draft.
    let admin_draft = services.draft_begin(&admin).await.unwrap();
    services
        .instance_upsert_in_draft(&admin_draft.draft_id, &redirected, &admin)
        .unwrap();
    services
        .draft_activate(&admin_draft.draft_id, &admin)
        .await
        .unwrap();
    assert_eq!(
        services.graph().instances["github_reader"].config["base_url"],
        "https://attacker.example"
    );
}

#[tokio::test]
async fn agent_cannot_borrow_a_granted_service_principal() {
    let tmp = TempDir::new().unwrap();
    let services = boot(&tmp).await;
    let admin = Principal::administrator();
    services
        .service_principal_grant(
            "pr_history_sampler",
            CapabilityCeiling {
                allowed_providers: BTreeSet::from(["open_prs".into()]),
                allowed_connectors: BTreeSet::from(["github_reader".into()]),
                allowed_capabilities: BTreeSet::from(["pull_requests.read".into()]),
                allowed_secrets: BTreeSet::from(["github-work".into()]),
                ..Default::default()
            },
            &admin,
        )
        .unwrap();
    let agent = drafting_agent(&services);
    let draft = services.draft_begin(&agent).await.unwrap();
    let borrowed = InstanceConfig {
        id: "borrowed_poll".into(),
        category: DefinitionCategory::DataProvider,
        definition: "github.pull_requests".into(),
        inputs: BTreeMap::from([("github".into(), "github_reader".into())]),
        config: json!({}),
        secrets: BTreeMap::new(),
        capabilities: vec![],
        run_as: Some("pr_history_sampler".into()),
        enabled: None,
    };
    services
        .instance_upsert_in_draft(&draft.draft_id, &borrowed, &agent)
        .unwrap();
    let err = services.draft_activate(&draft.draft_id, &agent).await.unwrap_err();
    assert!(err.contains("PENDING_SERVICE_PRINCIPAL_BINDING"), "{err}");
    assert!(!services.graph().instances.contains_key("borrowed_poll"));
}

#[tokio::test]
async fn use_mocks_substitutes_connectors_but_keeps_administrator_grants() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_ecc_fixture(&ws);
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize_with_mocks(ws, data, store, ResourceBudget::default(), true)
        .await
        .unwrap();
    let g = mgr.engine().graph.load_full();
    assert!(g.substitute_mocks && !g.is_preview);
    // The Workspace YAML request is not an effective ceiling on the live Installation (§11.3).
    let err = mgr
        .engine()
        .automations
        .resolve_service_principal("work_completion")
        .unwrap_err();
    assert!(err.contains("no Administrator grant"), "{err}");
    // Connectors still resolve to their Mocks.
    let env = mgr
        .engine()
        .queries
        .query("open_prs", json!({"repository": "acme/platform", "state": "open"}), &Principal::administrator(), true)
        .await
        .unwrap();
    assert!(!env.value.unwrap().as_array().unwrap().is_empty());
}

#[tokio::test]
async fn data_providers_cannot_call_mutating_connector_operations() {
    let tmp = TempDir::new().unwrap();
    let services = boot(&tmp).await;
    let admin = Principal::administrator();
    let decl = r#"local dash = require("dash")
return dash.data_provider { id = "github.sneaky_merge", handler = "query",
  inputs = { github = dash.connector_ref("github.connector", { required_capabilities = { "pull_requests.read" } }) } }"#;
    let runtime = r#"local R = {}
function R.query(ctx)
  return ctx:await(ctx.inputs.github:call("merge_pr", { repository = "acme/platform", number = 42 }))
end
return R"#;
    let mut sources = BTreeMap::new();
    sources.insert("definition.luau".to_string(), decl.as_bytes().to_vec());
    sources.insert("runtime.luau".to_string(), runtime.as_bytes().to_vec());
    let mut g = (*services.graph()).clone();
    g.substitute_mocks = true;
    g.definitions.insert(
        "github.sneaky_merge".into(),
        dash_core::LoadedDefinition {
            package_name: "github".into(),
            manifest: dash_luau::evaluate_declaration(decl, &sources).unwrap(),
            definition_source: decl.into(),
            runtime_source: runtime.into(),
            tests_source: None,
            fixtures: BTreeMap::new(),
            widget_assets: BTreeMap::new(),
        },
    );
    g.instances.insert(
        "sneaky".into(),
        InstanceConfig {
            id: "sneaky".into(),
            category: DefinitionCategory::DataProvider,
            definition: "github.sneaky_merge".into(),
            // Bound to the maintainer connector, which *does* grant pull_requests.merge.
            inputs: BTreeMap::from([("github".into(), "github_maintainer".into())]),
            config: json!({}),
            secrets: BTreeMap::new(),
            capabilities: vec![],
            run_as: None,
            enabled: None,
        },
    );
    services.engine().graph.store(std::sync::Arc::new(g));
    let res = services
        .provider_query("sneaky", json!({}), &admin, true)
        .await;
    let msg = match res {
        Err(e) => e,
        Ok(env) => serde_json::to_string(&env).unwrap(),
    };
    assert!(msg.contains("side effects"), "{msg}");
    assert!(audits(&services, "connector.invoke", "denied") >= 1);
}

#[tokio::test]
async fn network_ceiling_is_administrator_only_and_sp_reaches_credentialed_connector_live() {
    // Loopback stand-in for the GitHub API.
    let app = Router::new().route(
        "/repos/acme/platform/pulls",
        get(|headers: axum::http::HeaderMap| async move {
            let auth = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            assert_eq!(auth, "Bearer ghp_live_sampler_secret_7");
            Json(json!([
                { "id": "acme/platform#1", "number": 1, "title": "a", "repository": "acme/platform", "state": "open" },
                { "id": "acme/platform#2", "number": 2, "title": "b", "repository": "acme/platform", "state": "open" }
            ]))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let tmp = TempDir::new().unwrap();
    let services = boot(&tmp).await;
    let admin = Principal::administrator();
    let agent = drafting_agent(&services);

    // Only the Administrator sets the network ceiling; Agent attempts are denied and audited.
    let wide = NetworkPolicy {
        allowed_hosts: vec!["127.0.0.1".into()],
        allowed_methods: vec!["GET".into()],
        allow_loopback_for_tests: true,
    };
    assert!(services.network_policy_grant(wide.clone(), &agent).is_err());
    assert!(audits(&services, "network_policy.grant", "denied") == 1);
    assert!(!services.network_policy_status()["pending_hosts"].as_array().unwrap().is_empty());
    services.network_policy_grant(wide, &admin).unwrap();

    services
        .secret_set("github-work", "PAT", "ghp_live_sampler_secret_7", &["github_reader".into()], &admin)
        .unwrap();
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
    let mut sampler = CapabilityCeiling {
        allowed_providers: BTreeSet::from(["open_prs".into()]),
        allowed_connectors: BTreeSet::from(["github_reader".into()]),
        allowed_capabilities: BTreeSet::from(["pull_requests.read".into()]),
        ..Default::default()
    };

    // Live Generation (no Mocks): point github_reader at the loopback server and request it.
    let mut g = (*services.graph()).clone();
    assert!(!g.is_preview && !g.substitute_mocks);
    g.workspace.network.allowed_hosts = vec!["127.0.0.1".into()];
    g.workspace.network.allow_loopback_for_tests = true;
    g.instances.get_mut("github_reader").unwrap().config =
        json!({ "base_url": format!("http://{addr}") });
    services.engine().graph.store(std::sync::Arc::new(g));

    // Without the secret in its grant the Service principal is refused the credential...
    services
        .service_principal_grant("pr_history_sampler", sampler.clone(), &admin)
        .unwrap();
    let err = services.engine().history.sample_series(&series, None).await.unwrap_err();
    assert!(err.contains("does not permit binding this secret"), "{err}");

    // ...and with it, history sampling works against the credentialed connector.
    sampler.allowed_secrets.insert("github-work".into());
    services
        .service_principal_grant("pr_history_sampler", sampler, &admin)
        .unwrap();
    let count = services.engine().history.sample_series(&series, None).await.unwrap();
    assert_eq!(count, 2.0);
    assert!(audits(&services, "http.request", "ok") >= 1);
}
