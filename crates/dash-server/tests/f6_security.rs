//! Server-side authorization of diagnostics/inventory reads, credential-free Git remote URLs,
//! per-Principal secret masking on the Draft graph, policy-preserving secret rotation and
//! Origin scheme checks (§5.1, §5.2, §11.1, §14.3).

use axum::http::{HeaderMap, HeaderValue};
use dash_core::{CapabilityCeiling, Principal};
use dash_git::DraftManager;
use dash_luau::ResourceBudget;
use dash_server::auth::validate_origin;
use dash_server::services::{redact_remote_url, redact_url_credentials_in_text};
use dash_server::{build_router, hash_password, AppServices};
use dash_store::SqliteStore;
use serde_json::{json, Value};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use tempfile::TempDir;

const ADMIN_TOKEN: &str = "adm_f6_test_token";
const REMOTE_PAT: &str = "ghp_F6remotePushToken0123456789";

struct Server {
    _tmp: TempDir,
    services: AppServices,
    base: String,
    ws: std::path::PathBuf,
    http: reqwest::Client,
}

async fn boot() -> Server {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&fixture, &ws).unwrap();
    let store = SqliteStore::open(&data).unwrap();
    store
        .set_admin_credentials("admin", &hash_password("pass1234").unwrap())
        .unwrap();
    let mgr = DraftManager::initialize(ws.clone(), data, store, ResourceBudget::default())
        .await
        .unwrap();
    // No trusted proxies: the test client is a plain loopback peer.
    let services = AppServices::new(mgr, vec![]);
    std::fs::write(&services.admin_cli_token_path, ADMIN_TOKEN).unwrap();
    let app = build_router(services.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Server {
        _tmp: tmp,
        services,
        base: format!("http://{addr}"),
        ws,
        http: reqwest::Client::new(),
    }
}

impl Server {
    fn agent(&self, name: &str, ceiling: CapabilityCeiling) -> (String, String) {
        let (rec, token) = self
            .services
            .agent_create(name, ceiling, None, &Principal::administrator())
            .unwrap();
        (rec.agent_id, token)
    }

    async fn get(&self, token: &str, path: &str) -> (u16, Value) {
        let r = self
            .http
            .get(format!("{}{path}", self.base))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    async fn post(&self, token: &str, path: &str, body: Value) -> (u16, Value) {
        let r = self
            .http
            .post(format!("{}{path}", self.base))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }
}

// ---- R5:SRV-3 -------------------------------------------------------------------------------

#[test]
fn remote_url_credentials_are_redacted() {
    assert_eq!(
        redact_remote_url(&format!(
            "https://x-access-token:{REMOTE_PAT}@github.com/acme/ws.git"
        )),
        "https://github.com/acme/ws.git"
    );
    assert_eq!(
        redact_remote_url(&format!("https://{REMOTE_PAT}@github.com/acme/ws.git")),
        "https://github.com/acme/ws.git"
    );
    assert_eq!(
        redact_remote_url("git@github.com:acme/ws.git"),
        "github.com:acme/ws.git"
    );
    for unchanged in [
        "https://github.com/acme/ws.git",
        "/srv/git/ws.git",
        "../relative@odd/ws.git",
    ] {
        assert_eq!(redact_remote_url(unchanged), unchanged);
    }
    let msg =
        format!("fatal: unable to access 'https://bot:{REMOTE_PAT}@github.com/acme/ws.git/': 403");
    let redacted = redact_url_credentials_in_text(&msg);
    assert!(!redacted.contains(REMOTE_PAT), "{redacted}");
    assert!(redacted.contains("https://github.com/acme/ws.git/"));
}

#[tokio::test]
async fn agent_git_status_never_returns_remote_url_credentials() {
    let srv = boot().await;
    let st = std::process::Command::new("git")
        .args([
            "remote",
            "add",
            "origin",
            &format!("https://x-access-token:{REMOTE_PAT}@github.com/acme/dash-ws.git"),
        ])
        .current_dir(&srv.ws)
        .status()
        .unwrap();
    assert!(st.success());
    let (_, agent_tok) = srv.agent("ci-bot", CapabilityCeiling::default());
    for token in [agent_tok.as_str(), ADMIN_TOKEN] {
        let (status, body) = srv.get(token, "/api/git/status").await;
        assert_eq!(status, 200, "{body}");
        let text = body.to_string();
        assert!(
            !text.contains(REMOTE_PAT),
            "remote credential leaked: {text}"
        );
        assert_eq!(
            body["remote_url"].as_str(),
            Some("https://github.com/acme/dash-ws.git")
        );
    }
}

#[tokio::test]
async fn agent_and_service_principal_inventory_is_administrator_only() {
    let srv = boot().await;
    let (_, agent_tok) = srv.agent("ci-bot", CapabilityCeiling::default());
    srv.agent("other-bot", CapabilityCeiling::default());
    for path in ["/api/agents", "/api/service-principals"] {
        let (status, body) = srv.get(&agent_tok, path).await;
        assert_eq!(status, 403, "{path} must be refused to an Agent: {body}");
        assert!(!body.to_string().contains("other-bot"));
        let (status, _) = srv.get(ADMIN_TOKEN, path).await;
        assert_eq!(status, 200, "{path} stays available to the Administrator");
    }
}

#[tokio::test]
async fn agent_traces_and_logs_only_show_its_own_activity() {
    let srv = boot().await;
    let (agent_id, agent_tok) = srv.agent("ci-bot", CapabilityCeiling::default());
    let (other_id, other_tok) = srv.agent("other-bot", CapabilityCeiling::default());
    // Administrator, another Agent and this Agent each make traced requests.
    assert_eq!(srv.get(ADMIN_TOKEN, "/api/graph").await.0, 200);
    assert_eq!(srv.get(&other_tok, "/api/graph").await.0, 200);
    assert_eq!(srv.get(&agent_tok, "/api/graph").await.0, 200);
    let telemetry = &srv.services.engine().telemetry;
    telemetry.log("info", "test", "admin", "trc_admin_only", "admin activity");
    telemetry.log("info", "test", &agent_id, "trc_agent", "agent activity");

    let (status, traces) = srv
        .get(&agent_tok, "/api/diagnostics/traces?limit=500")
        .await;
    assert_eq!(status, 200);
    let spans = traces["spans"].as_array().unwrap();
    assert!(
        spans
            .iter()
            .any(|s| s["attributes"]["principal_id"] == json!(agent_id)),
        "the Agent sees its own request spans: {traces}"
    );
    for s in spans {
        let who = s["attributes"]["principal_id"]
            .as_str()
            .unwrap_or(s["principal_id"].as_str().unwrap_or(""));
        assert!(
            who != "admin" && who != other_id,
            "another Principal's span leaked to the Agent: {s}"
        );
    }
    // The Administrator still sees everything.
    let (_, admin_traces) = srv
        .get(ADMIN_TOKEN, "/api/diagnostics/traces?limit=500")
        .await;
    let admin_spans = admin_traces["spans"].as_array().unwrap();
    assert!(admin_spans
        .iter()
        .any(|s| s["attributes"]["principal_id"] == json!(other_id)));
    assert!(admin_spans
        .iter()
        .any(|s| s["attributes"]["principal_id"] == json!("admin")));

    let (_, logs) = srv.get(&agent_tok, "/api/diagnostics/logs?limit=500").await;
    let logs = logs.as_array().unwrap();
    assert!(logs.iter().any(|l| l["message"] == "agent activity"));
    assert!(
        logs.iter().all(|l| l["principal_id"] != "admin"),
        "{logs:?}"
    );
    let (_, admin_logs) = srv
        .get(ADMIN_TOKEN, "/api/diagnostics/logs?limit=500")
        .await;
    assert!(admin_logs
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["message"] == "admin activity"));

    // MCP shares the same service: the filtered variants back `graph_trace`/`logs_query`.
    let agent_principal = Principal {
        id: agent_id.clone(),
        kind: dash_core::PrincipalKind::Agent,
        ceiling: CapabilityCeiling::default(),
    };
    let mcp_view = srv.services.graph_trace_for(500, &agent_principal);
    assert!(!mcp_view.to_string().contains(&other_id));
}

// ---- R7:SEC-6 -------------------------------------------------------------------------------

#[tokio::test]
async fn draft_graph_masks_secret_names_outside_the_agent_ceiling() {
    let srv = boot().await;
    let ceiling = CapabilityCeiling {
        can_author_drafts: true,
        allowed_secrets: ["jira-work".to_string()].into_iter().collect(),
        ..Default::default()
    };
    let (_, agent_tok) = srv.agent("author-bot", ceiling);
    let (status, draft) = srv.post(&agent_tok, "/api/drafts", json!({})).await;
    assert_eq!(status, 200, "{draft}");
    let id = draft["draft_id"].as_str().unwrap();
    let (status, g) = srv
        .get(&agent_tok, &format!("/api/drafts/{id}/graph"))
        .await;
    assert_eq!(status, 200, "{g}");
    let text = g["instances"].to_string();
    assert!(
        !text.contains("github-work"),
        "out-of-ceiling secret name leaked through the Draft graph: {text}"
    );
    assert!(text.contains("secret://[hidden]"));
    assert!(
        text.contains("secret://jira-work"),
        "permitted secret refs stay visible"
    );
    // Same masking as the active graph.
    let (_, active) = srv.get(&agent_tok, "/api/graph").await;
    assert!(!active["instances"].to_string().contains("github-work"));
}

/// F6: capability_explain (MCP, any authenticated Principal) masks secret names outside the
/// caller's ceiling like the other instance views.
#[tokio::test]
async fn capability_explain_masks_secret_names_outside_the_ceiling() {
    let srv = boot().await;
    let g = srv.services.graph();
    let (target, slot) = g
        .instances
        .iter()
        .find_map(|(id, i)| {
            i.secrets
                .iter()
                .find(|(_, v)| v.contains("github-work"))
                .map(|(k, _)| (id.clone(), k.clone()))
        })
        .expect("an Instance bound to secret://github-work");
    let agent = dash_core::Principal {
        id: "agt_explain".into(),
        kind: dash_core::PrincipalKind::Agent,
        ceiling: CapabilityCeiling {
            allowed_secrets: ["jira-work".to_string()].into_iter().collect(),
            ..Default::default()
        },
    };
    let out = srv.services.capability_explain(&target, &agent);
    assert!(
        !out.to_string().contains("github-work"),
        "out-of-ceiling secret name leaked: {out}"
    );
    assert_eq!(out["instance"]["secrets"][&slot], "secret://[hidden]");
    let admin = srv
        .services
        .capability_explain(&target, &dash_core::Principal::administrator());
    assert!(admin["instance"]["secrets"][&slot]
        .as_str()
        .unwrap()
        .contains("github-work"));
}

// ---- R5:SRV-6 -------------------------------------------------------------------------------

#[tokio::test]
async fn rest_secret_rotation_keeps_binding_policy_and_description() {
    let srv = boot().await;
    let (status, _) = srv
        .post(
            ADMIN_TOKEN,
            "/api/secrets",
            json!({
                "name": "github-work",
                "description": "Work PAT",
                "plaintext": "ghp_v1",
                "allowed_bindings": ["github_reader"],
                "allowed_hosts": ["api.github.com"],
            }),
        )
        .await;
    assert_eq!(status, 200);
    // Plain value rotation: only name + plaintext.
    let (status, meta) = srv
        .post(
            ADMIN_TOKEN,
            "/api/secrets",
            json!({ "name": "github-work", "plaintext": "ghp_v2" }),
        )
        .await;
    assert_eq!(status, 200, "{meta}");
    assert_eq!(meta["key_version"], 2);
    assert_eq!(meta["allowed_bindings"], json!(["github_reader"]));
    assert_eq!(meta["allowed_hosts"], json!(["api.github.com"]));
    assert_eq!(meta["description"], "Work PAT");
    let (_, list) = srv.get(ADMIN_TOKEN, "/api/secrets").await;
    assert_eq!(list[0]["allowed_bindings"], json!(["github_reader"]));
    // An explicit empty list is the deliberate way to widen the policy.
    let (_, widened) = srv
        .post(
            ADMIN_TOKEN,
            "/api/secrets",
            json!({ "name": "github-work", "plaintext": "ghp_v3", "allowed_bindings": [] }),
        )
        .await;
    assert_eq!(widened["allowed_bindings"], json!([]));
}

// ---- T1: Origin scheme ----------------------------------------------------------------------

fn origin_headers(origin: &str, host: &str, extra: &[(&'static str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("origin", HeaderValue::from_str(origin).unwrap());
    h.insert("host", HeaderValue::from_str(host).unwrap());
    for (k, v) in extra {
        h.insert(*k, HeaderValue::from_str(v).unwrap());
    }
    h
}

#[test]
fn origin_scheme_must_match_the_effective_request_scheme() {
    let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let proxy = [loopback];
    // Plain-http daemon, direct peer: only an http Origin matches.
    let h = origin_headers("http://127.0.0.1:8080", "127.0.0.1:8080", &[]);
    assert!(validate_origin(&h, loopback, &[]).is_ok());
    let h = origin_headers("https://127.0.0.1:8080", "127.0.0.1:8080", &[]);
    assert!(validate_origin(&h, loopback, &[]).is_err());
    // A trusted proxy without X-Forwarded-Proto does not make an https Origin match.
    let h = origin_headers("https://dash.tail.ts.net", "dash.tail.ts.net", &[]);
    assert!(validate_origin(&h, loopback, &proxy).is_err());
    // ... it does once the trusted proxy says the client connection was https.
    let h = origin_headers(
        "https://dash.tail.ts.net",
        "dash.tail.ts.net",
        &[("x-forwarded-proto", "https")],
    );
    assert!(validate_origin(&h, loopback, &proxy).is_ok());
    // ... and then an http Origin no longer matches.
    let h = origin_headers(
        "http://dash.tail.ts.net",
        "dash.tail.ts.net",
        &[("x-forwarded-proto", "https")],
    );
    assert!(validate_origin(&h, loopback, &proxy).is_err());
    // An untrusted peer's X-Forwarded-Proto is ignored.
    let h = origin_headers(
        "https://127.0.0.1:8080",
        "127.0.0.1:8080",
        &[("x-forwarded-proto", "https")],
    );
    assert!(validate_origin(&h, loopback, &[]).is_err());
}
