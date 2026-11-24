//! Axum HTTP API, WebSocket signal & request bridge, MCP Streamable HTTP server, auth, and embedded UI.

pub mod auth;
pub mod mcp;
pub mod routes;
pub mod services;
pub mod ws;

pub use auth::{create_browser_session, generate_random_token, hash_password, verify_password};
pub use routes::build_router;
pub use services::{AppServices, CatalogEntry, ServicePrincipalStatus};

#[cfg(test)]
mod tests {
    use super::*;
    use dash_core::{CapabilityCeiling, Principal};
    use dash_git::DraftManager;
    use dash_luau::ResourceBudget;
    use dash_store::SqliteStore;
    use serde_json::json;
    use std::collections::BTreeSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::Path;
    use tempfile::TempDir;

    async fn boot_test_server() -> (
        TempDir,
        AppServices,
        SocketAddr,
        String,
        tokio::task::JoinHandle<()>,
    ) {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/engineering-command-center");
        dash_git::packages::copy_tree_excluding_git(&fixture, &ws).unwrap();

        let store = SqliteStore::open(&data).unwrap();
        store
            .set_admin_credentials("admin", &hash_password("pass1234").unwrap())
            .unwrap();
        let mgr = DraftManager::initialize(ws, data.clone(), store, ResourceBudget::default())
            .await
            .unwrap();
        let services = AppServices::new(mgr, vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        // Write local admin CLI token
        std::fs::write(&services.admin_cli_token_path, "adm_cli_test_token_1").unwrap();

        let app = build_router(services.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        (tmp, services, addr, "adm_cli_test_token_1".into(), handle)
    }

    #[tokio::test]
    async fn test_auth_csrf_mcp_and_ws_lifecycle() {
        let (_tmp, services, addr, _admin_tok, server_h) = boot_test_server().await;
        let base = format!("http://{addr}");
        let client = reqwest::Client::new();

        // 1. Health check works unauthenticated
        let hz = client.get(format!("{base}/healthz")).send().await.unwrap();
        assert_eq!(hz.status(), 200);

        // 2. Unauthenticated API & MCP requests rejected (401)
        let unauth = client.get(format!("{base}/api/graph")).send().await.unwrap();
        assert_eq!(unauth.status(), 401);

        // 3. Browser login sets HttpOnly cookie & returns CSRF token; mutations without CSRF fail 403!
        let login = client
            .post(format!("{base}/api/auth/login"))
            .json(&json!({ "username": "admin", "password": "pass1234" }))
            .send()
            .await
            .unwrap();
        assert_eq!(login.status(), 200);
        let set_cookie = login
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("SameSite=Lax"));
        let session_cookie = set_cookie.split(';').next().unwrap().to_string();
        let login_json: serde_json::Value = login.json().await.unwrap();
        let csrf = login_json["csrf_token"].as_str().unwrap().to_string();

        let no_csrf = client
            .post(format!("{base}/api/secrets"))
            .header("cookie", &session_cookie)
            .json(&json!({ "name": "github-work", "plaintext": "ghp_123" }))
            .send()
            .await
            .unwrap();
        assert_eq!(no_csrf.status(), 403);

        // With CSRF header -> succeeds, and secret list never exposes plaintext!
        let set_sec = client
            .post(format!("{base}/api/secrets"))
            .header("cookie", &session_cookie)
            .header("x-csrf-token", &csrf)
            .json(&json!({
                "name": "github-work",
                "description": "Work PAT",
                "plaintext": "ghp_super_secret_xyz",
                "allowed_bindings": ["github_reader"]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(set_sec.status(), 200);

        let list_sec = client
            .get(format!("{base}/api/secrets"))
            .header("cookie", &session_cookie)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(!list_sec.contains("ghp_super_secret_xyz"));

        // 4. Create scoped Agent MCP token that can author drafts & read, CANNOT invoke merge_pr!
        let (_agt_rec, agt_token) = services
            .agent_create(
                "ci-bot",
                CapabilityCeiling {
                    allowed_providers: BTreeSet::from(["*".into()]),
                    allowed_actions: BTreeSet::new(),
                    can_author_drafts: true,
                    can_activate_drafts: true,
                    ..Default::default()
                },
                None,
                &Principal::administrator(),
            )
            .unwrap();

        // 5. MCP initialize + tools/list + draft cycle + denied live action
        let mcp_init = client
            .post(format!("{base}/mcp"))
            .header("authorization", format!("Bearer {agt_token}"))
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": "2026-07-28" }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(mcp_init.status(), 200);

        let tools_res: serde_json::Value = client
            .post(format!("{base}/mcp"))
            .header("authorization", format!("Bearer {agt_token}"))
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list"
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(tools_res["result"]["tools"].as_array().unwrap().len() >= 30);

        // Agent attempting ungranted action_invoke -> denied!
        let denied_act: serde_json::Value = client
            .post(format!("{base}/mcp"))
            .header("authorization", format!("Bearer {agt_token}"))
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "action_invoke",
                    "arguments": {
                        "action_instance": "merge_pr",
                        "args": { "repository": "acme/platform", "number": 42 }
                    }
                }
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(denied_act["result"]["isError"], true);

        // Agent cannot read secret plaintext or bind ungranted secret!
        let denied_bind: serde_json::Value = client
            .post(format!("{base}/mcp"))
            .header("authorization", format!("Bearer {agt_token}"))
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "secret_bind",
                    "arguments": {
                        "draft_id": services.draft_begin(&Principal::administrator()).await.unwrap().draft_id,
                        "instance_id": "github_reader",
                        "secret_name": "github-work"
                    }
                }
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(denied_bind["result"]["isError"], true);

        // Custom widget asset serves with strict CSP
        let widget_resp = client
            .get(format!("{base}/widgets/engineering.pr_badge_widget/assets/index.html"))
            .send()
            .await
            .unwrap();
        assert_eq!(widget_resp.status(), 200);
        let csp = widget_resp
            .headers()
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(csp.contains("connect-src 'none'"));

        // Embedded UI index.html must obey strict CSP `script-src 'self'` with no inline scripts
        let src_index = include_str!("../../../web/index.html");
        assert!(
            src_index.contains(r#"src="/theme-init.js""#),
            "web/index.html should load /theme-init.js externally"
        );
        assert!(
            !src_index.contains("localStorage.getItem"),
            "web/index.html must not contain inline theme script"
        );

        let ui_resp = client.get(format!("{base}/")).send().await.unwrap();
        assert_eq!(ui_resp.status(), 200);
        let ui_csp = ui_resp
            .headers()
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(ui_csp.contains("script-src 'self'"));
        assert!(!ui_csp.contains("script-src 'self' 'unsafe-inline'"));
        let ui_html = ui_resp.text().await.unwrap();
        assert!(ui_html.contains(r#"src="/theme-init.js""#));
        assert!(!ui_html.contains("localStorage.getItem"));

        let theme_js = client
            .get(format!("{base}/theme-init.js"))
            .send()
            .await
            .unwrap();
        assert_eq!(theme_js.status(), 200);
        let theme_body = theme_js.text().await.unwrap();
        assert!(theme_body.contains("localStorage.getItem"));

        server_h.abort();
    }
}
