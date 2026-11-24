//! Axum HTTP API, WebSocket signal & request bridge, MCP Streamable HTTP server, auth, and embedded UI.

pub mod auth;
pub mod context;
pub mod mcp;
pub mod routes;
pub mod services;
pub mod ws;

pub use auth::{create_browser_session, generate_random_token, hash_password, verify_password};
pub use context::{HostPattern, SecureCookieMode, ServerContext};
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
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
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
        let unauth = client
            .get(format!("{base}/api/graph"))
            .send()
            .await
            .unwrap();
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

        // Custom widget asset requires authentication and serves with a sandboxing CSP
        let anon_widget = client
            .get(format!(
                "{base}/widgets/engineering.pr_badge_widget/assets/index.html"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(anon_widget.status(), 401);
        let widget_resp = client
            .get(format!(
                "{base}/widgets/engineering.pr_badge_widget/assets/index.html"
            ))
            .header("cookie", &session_cookie)
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
        assert!(csp.starts_with("sandbox allow-scripts"));
        assert!(csp.contains("frame-ancestors 'self'"));
        assert!(!csp.contains("script-src 'self'"));
        assert_eq!(widget_resp.headers()["x-content-type-options"], "nosniff");

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
        assert!(ui_csp.contains("frame-ancestors 'none'"));
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

    async fn login(client: &reqwest::Client, base: &str) -> (String, String) {
        let login = client
            .post(format!("{base}/api/auth/login"))
            .json(&json!({ "username": "admin", "password": "pass1234" }))
            .send()
            .await
            .unwrap();
        assert_eq!(login.status(), 200);
        let cookie = login.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string();
        let csrf = login.json::<serde_json::Value>().await.unwrap()["csrf_token"]
            .as_str()
            .unwrap()
            .to_string();
        (cookie, csrf)
    }

    fn agent(services: &AppServices, ceiling: CapabilityCeiling) -> String {
        services
            .agent_create("t", ceiling, None, &Principal::administrator())
            .unwrap()
            .1
    }

    #[tokio::test]
    async fn origin_and_host_checks_block_cross_port_and_dns_rebinding() {
        let (_tmp, _services, addr, admin_tok, server_h) = boot_test_server().await;
        let base = format!("http://{addr}");
        let client = reqwest::Client::new();
        let (cookie, csrf) = login(&client, &base).await;

        // Same-site page on another local port: Origin host matches but port differs -> 403.
        let cross_port = client
            .post(format!("{base}/api/drafts"))
            .header("cookie", &cookie)
            .header("x-csrf-token", &csrf)
            .header("origin", "http://127.0.0.1:3000")
            .send()
            .await
            .unwrap();
        assert_eq!(cross_port.status(), 403);
        // Exact same origin is accepted.
        let same = client
            .get(format!("{base}/api/graph"))
            .header("cookie", &cookie)
            .header("origin", format!("http://{addr}"))
            .send()
            .await
            .unwrap();
        assert_eq!(same.status(), 200);

        // DNS rebinding: Origin == Host == attacker name. Host allowlist rejects it everywhere,
        // including unauthenticated login, /healthz and /mcp.
        for (method, path) in [
            ("POST", "/api/auth/login"),
            ("GET", "/healthz"),
            ("POST", "/mcp"),
            ("GET", "/api/graph"),
        ] {
            let req = client
                .request(method.parse().unwrap(), format!("{base}{path}"))
                .header("host", format!("evil.example:{}", addr.port()))
                .header("origin", format!("http://evil.example:{}", addr.port()))
                .header("authorization", format!("Bearer {admin_tok}"))
                .json(&json!({ "username": "admin", "password": "pass1234" }));
            let r = req.send().await.unwrap();
            assert_eq!(
                r.status(),
                421,
                "{method} {path} must reject rebinding Host"
            );
        }

        // Login itself validates Origin.
        let xlogin = client
            .post(format!("{base}/api/auth/login"))
            .header("origin", "http://localhost:3000")
            .json(&json!({ "username": "admin", "password": "pass1234" }))
            .send()
            .await
            .unwrap();
        assert_eq!(xlogin.status(), 403);
        server_h.abort();
    }

    #[tokio::test]
    async fn unauthenticated_bearer_values_do_not_poison_redaction() {
        let (_tmp, services, addr, _admin_tok, server_h) = boot_test_server().await;
        let base = format!("http://{addr}");
        let client = reqwest::Client::new();
        for word in ["merge_pr", "acme/platform", "open"] {
            let r = client
                .get(format!("{base}/api/graph"))
                .header("authorization", format!("Bearer {word}"))
                .header("cookie", format!("dash_session={word}"))
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), 401);
        }
        services
            .store()
            .append_audit(
                "admin",
                "administrator",
                "action.invoke",
                "merge_pr",
                "ok",
                json!({ "repo": "acme/platform", "state": "open" }),
            )
            .unwrap();
        let a = services.audit_query(1).unwrap();
        assert_eq!(a[0].detail["repo"], "acme/platform");
        assert_eq!(a[0].detail["state"], "open");
        server_h.abort();
    }

    #[tokio::test]
    async fn admin_cli_token_only_from_direct_loopback_and_rate_limit_per_forwarded_client() {
        let (_tmp, _services, addr, admin_tok, server_h) = boot_test_server().await;
        let base = format!("http://{addr}");
        let client = reqwest::Client::new();
        let direct = client
            .get(format!("{base}/api/graph"))
            .header("authorization", format!("Bearer {admin_tok}"))
            .send()
            .await
            .unwrap();
        assert_eq!(direct.status(), 200);
        // Through a proxy hop (e.g. Tailscale Serve adds X-Forwarded-For): refused.
        let proxied = client
            .get(format!("{base}/api/graph"))
            .header("authorization", format!("Bearer {admin_tok}"))
            .header("x-forwarded-for", "100.64.0.7")
            .send()
            .await
            .unwrap();
        assert_eq!(proxied.status(), 401);

        // 127.0.0.1 is a trusted proxy here: limiter keys on the forwarded client address.
        let bad = |ip: &'static str| {
            let c = client.clone();
            let b = base.clone();
            async move {
                c.post(format!("{b}/api/auth/login"))
                    .header("x-forwarded-for", ip)
                    .json(&json!({ "username": "admin", "password": "wrong" }))
                    .send()
                    .await
                    .unwrap()
                    .status()
            }
        };
        for _ in 0..crate::auth::LOGIN_ATTEMPTS_PER_WINDOW {
            assert_eq!(bad("100.64.0.1").await, 401);
        }
        assert_eq!(bad("100.64.0.1").await, 429);
        // A different tailnet device is not locked out.
        assert_eq!(bad("100.64.0.2").await, 401);
        server_h.abort();
    }

    #[tokio::test]
    async fn mcp_protocol_notifications_batches_sessions_and_stdio_marker() {
        let (_tmp, services, addr, admin_tok, server_h) = boot_test_server().await;
        let base = format!("http://{addr}/mcp");
        let client = reqwest::Client::new();
        let tok = agent(
            &services,
            CapabilityCeiling {
                can_author_drafts: true,
                ..Default::default()
            },
        );

        let init = client
            .post(&base)
            .header("authorization", format!("Bearer {tok}"))
            .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": { "protocolVersion": "2025-03-26" } }))
            .send()
            .await
            .unwrap();
        assert_eq!(init.status(), 200);
        assert_eq!(init.headers()["mcp-protocol-version"], "2025-03-26");
        let sid = init.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_string();
        let init_body: serde_json::Value = init.json().await.unwrap();
        assert_eq!(
            init_body["result"]["capabilities"]["tools"]["listChanged"],
            false
        );

        // Notification -> 202, empty body.
        let note = client
            .post(&base)
            .header("authorization", format!("Bearer {tok}"))
            .header("mcp-session-id", &sid)
            .json(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))
            .send()
            .await
            .unwrap();
        assert_eq!(note.status(), 202);
        assert!(note.text().await.unwrap().is_empty());

        // Batch with a request + notification -> array with one response.
        let batch: serde_json::Value = client
            .post(&base)
            .header("authorization", format!("Bearer {tok}"))
            .header("mcp-session-id", &sid)
            .json(&json!([
                { "jsonrpc": "2.0", "id": 7, "method": "ping" },
                { "jsonrpc": "2.0", "method": "notifications/cancelled", "params": { "requestId": 3 } }
            ]))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(batch.as_array().unwrap().len(), 1);
        assert_eq!(batch[0]["id"], 7);

        // Malformed body -> JSON-RPC parse error.
        let parse = client
            .post(&base)
            .header("authorization", format!("Bearer {tok}"))
            .header("content-type", "application/json")
            .body("{not json")
            .send()
            .await
            .unwrap();
        assert_eq!(parse.status(), 400);
        assert_eq!(
            parse.json::<serde_json::Value>().await.unwrap()["error"]["code"],
            -32700
        );

        // Array results are wrapped so structuredContent is an object.
        let cat: serde_json::Value = client
            .post(&base)
            .header("authorization", format!("Bearer {tok}"))
            .json(&json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": { "name": "catalog_search", "arguments": {} } }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(cat["result"]["structuredContent"]["items"].is_array());

        // Session terminated via DELETE -> later use is 404.
        let del = client
            .delete(&base)
            .header("authorization", format!("Bearer {tok}"))
            .header("mcp-session-id", &sid)
            .send()
            .await
            .unwrap();
        assert_eq!(del.status(), 204);
        let after = client
            .post(&base)
            .header("authorization", format!("Bearer {tok}"))
            .header("mcp-session-id", &sid)
            .json(&json!({ "jsonrpc": "2.0", "id": 9, "method": "ping" }))
            .send()
            .await
            .unwrap();
        assert_eq!(after.status(), 404);

        // stdio transport marker with the real admin CLI token is refused server-side.
        let stdio_admin = client
            .post(&base)
            .header("authorization", format!("Bearer {admin_tok}"))
            .header("x-dash-transport", "stdio")
            .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": "ping" }))
            .send()
            .await
            .unwrap();
        assert_eq!(stdio_admin.status(), 403);
        let stdio_agent = client
            .post(&base)
            .header("authorization", format!("Bearer {tok}"))
            .header("x-dash-transport", "stdio")
            .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": "ping" }))
            .send()
            .await
            .unwrap();
        assert_eq!(stdio_agent.status(), 200);
        server_h.abort();
    }

    #[tokio::test]
    async fn agents_cannot_touch_foreign_drafts_or_read_foreign_executions_and_audits() {
        let (_tmp, services, addr, _admin_tok, server_h) = boot_test_server().await;
        let base = format!("http://{addr}");
        let client = reqwest::Client::new();
        let author = agent(
            &services,
            CapabilityCeiling {
                can_author_drafts: true,
                ..Default::default()
            },
        );
        let reader = agent(&services, CapabilityCeiling::default());
        let admin_draft = services
            .draft_begin(&Principal::administrator())
            .await
            .unwrap();

        // Read-only Agent cannot abort the Administrator's Draft (HTTP and MCP).
        let abort = client
            .delete(format!("{base}/api/drafts/{}", admin_draft.draft_id))
            .header("authorization", format!("Bearer {reader}"))
            .send()
            .await
            .unwrap();
        assert_eq!(abort.status(), 400);
        let mcp_abort: serde_json::Value = client
            .post(format!("{base}/mcp"))
            .header("authorization", format!("Bearer {author}"))
            .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": { "name": "draft_abort", "arguments": { "draft_id": admin_draft.draft_id } } }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(mcp_abort["result"]["isError"], true);
        assert_eq!(
            services.draft_status(&admin_draft.draft_id).unwrap().status,
            "open"
        );

        // definition_create on an existing Definition / instance_configure on a missing Instance fail.
        let own: serde_json::Value = client
            .post(format!("{base}/mcp"))
            .header("authorization", format!("Bearer {author}"))
            .json(&json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": { "name": "draft_begin", "arguments": {} } }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let own_id = own["result"]["structuredContent"]["draft_id"]
            .as_str()
            .unwrap()
            .to_string();
        let cfg_missing = services.instance_configure_in_draft(
            &own_id,
            &serde_json::from_value(
                json!({ "id": "nope_missing", "category": "data_provider", "definition": "x.y" }),
            )
            .unwrap(),
            &Principal::administrator(),
        );
        assert!(cfg_missing.unwrap_err().contains("does not exist"));
        let create_existing = services.instance_create_in_draft(
            &own_id,
            &serde_json::from_value(json!({ "id": "open_prs", "category": "data_provider", "definition": "github.pull_requests" })).unwrap(),
            &Principal::administrator(),
        );
        assert!(create_existing.unwrap_err().contains("already exists"));

        // Executions and audits are filtered for non-Administrators.
        services
            .store()
            .append_audit(
                "admin",
                "administrator",
                "secret.set",
                "github-work",
                "ok",
                json!({}),
            )
            .unwrap();
        let audits: serde_json::Value = client
            .get(format!("{base}/api/diagnostics/audits"))
            .header("authorization", format!("Bearer {reader}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(audits
            .as_array()
            .unwrap()
            .iter()
            .all(|a| a["principal_kind"] == "agent"));
        let execs: serde_json::Value = client
            .get(format!("{base}/api/executions"))
            .header("authorization", format!("Bearer {reader}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(execs.as_array().unwrap().is_empty());

        // Draft resources are listed only for the owner.
        let res: serde_json::Value = client
            .post(format!("{base}/mcp"))
            .header("authorization", format!("Bearer {author}"))
            .json(&json!({ "jsonrpc": "2.0", "id": 3, "method": "resources/list" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let uris: Vec<String> = res["result"]["resources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["uri"].as_str().unwrap().to_string())
            .collect();
        assert!(uris.contains(&format!("dash://drafts/{own_id}/preview-report")));
        assert!(!uris.iter().any(|u| u.contains(&admin_draft.draft_id)));
        assert!(uris.contains(&"dash://graph/dependencies".to_string()));
        assert!(uris.contains(&"dash://docs/glossary".to_string()));
        let deps: serde_json::Value = client
            .post(format!("{base}/mcp"))
            .header("authorization", format!("Bearer {author}"))
            .json(&json!({ "jsonrpc": "2.0", "id": 4, "method": "resources/read", "params": { "uri": "dash://graph/dependencies" } }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let text = deps["result"]["contents"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"kind\": \"input\""));
        server_h.abort();
    }

    #[tokio::test]
    async fn agent_token_expiry_defaults_and_capability_explain_by_category() {
        let (_tmp, services, addr, admin_tok, server_h) = boot_test_server().await;
        let base = format!("http://{addr}");
        let client = reqwest::Client::new();
        let created: serde_json::Value = client
            .post(format!("{base}/api/agents"))
            .header("authorization", format!("Bearer {admin_tok}"))
            .json(&json!({ "name": "short-lived", "expires_in_secs": 3600 }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(created["agent"]["expires_at"].is_string());
        let default_exp: serde_json::Value = client
            .post(format!("{base}/api/agents"))
            .header("authorization", format!("Bearer {admin_tok}"))
            .json(&json!({ "name": "default" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(default_exp["agent"]["expires_at"].is_string());
        let too_long = client
            .post(format!("{base}/api/agents"))
            .header("authorization", format!("Bearer {admin_tok}"))
            .json(&json!({ "name": "forever", "expires_in_secs": 10 * 365 * 24 * 3600_i64 }))
            .send()
            .await
            .unwrap();
        assert_eq!(too_long.status(), 400);

        let reader = Principal {
            id: "agt_r".into(),
            kind: dash_core::PrincipalKind::Agent,
            ceiling: CapabilityCeiling {
                allowed_providers: BTreeSet::from(["open_prs".into()]),
                ..Default::default()
            },
        };
        let explain = services.capability_explain("open_prs", &reader);
        assert_eq!(explain["provider_access"]["allowed"], true);
        assert_eq!(explain["category"], "data_provider");
        let conn = services.capability_explain("github_reader", &reader);
        assert!(conn["connector_operations"].is_object());
        server_h.abort();
    }

    #[tokio::test]
    async fn doctor_reports_real_checks_and_graph_masks_secret_names() {
        let (_tmp, services, _addr, _admin_tok, server_h) = boot_test_server().await;
        let doc = services.workspace_doctor().unwrap();
        let checks = doc["checks"].as_array().unwrap();
        for name in [
            "sqlite_quick_check",
            "sqlite_journal_mode",
            "vault_decrypts_all_secrets",
            "admin_credentials",
            "activation_journal",
            "sqlite_size_budget",
        ] {
            assert!(
                checks.iter().any(|c| c["name"] == name),
                "missing check {name}"
            );
        }
        assert_eq!(doc["sqlite_ok"], true);

        let agent_p = Principal {
            id: "agt_x".into(),
            kind: dash_core::PrincipalKind::Agent,
            ceiling: CapabilityCeiling::default(),
        };
        // Bind a secret reference into the active graph, then inspect as an Agent without
        // a secret grant and as one with it.
        let mut graph = (*services.graph()).clone();
        graph
            .instances
            .get_mut("github_reader")
            .unwrap()
            .secrets
            .insert("auth".into(), "secret://github-work".into());
        services.engine().graph.store(std::sync::Arc::new(graph));
        let g = services.graph_inspect_for(&agent_p);
        let text = g["instances"].to_string();
        assert!(
            !text.contains("github-work"),
            "secret names must be masked: {text}"
        );
        assert!(text.contains("secret://[hidden]"));
        let mut granted = agent_p.clone();
        granted.ceiling.allowed_secrets = BTreeSet::from(["github-work".into()]);
        assert!(services.graph_inspect_for(&granted)["instances"]
            .to_string()
            .contains("secret://github-work"));
        let report = services.enforce_retention().await.unwrap();
        assert!(!report.over_budget);
        server_h.abort();
    }
}
