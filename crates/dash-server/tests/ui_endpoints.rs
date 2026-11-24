//! Integration tests for the read-only endpoints backing the Web UI (§10.2) and the widget
//! Request Dry run (§9.1, Appendix A.3), plus the live `/ws` Dashboard subscription.

use dash_git::DraftManager;
use dash_luau::ResourceBudget;
use dash_server::{build_router, AppServices};
use dash_store::SqliteStore;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

const TOKEN: &str = "adm_cli_ui_endpoints_token";

struct TestServer {
    _tmp: TempDir,
    services: AppServices,
    base: String,
    addr: SocketAddr,
    client: reqwest::Client,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl TestServer {
    async fn boot() -> Self {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
        dash_git::packages::copy_tree_excluding_git(&fixture, &ws).unwrap();
        let store = SqliteStore::open(&data).unwrap();
        let mgr =
            DraftManager::initialize_with_mocks(ws, data, store, ResourceBudget::default(), true)
                .await
                .unwrap();
        let services = AppServices::new(mgr, vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        std::fs::write(&services.admin_cli_token_path, TOKEN).unwrap();
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
        Self {
            _tmp: tmp,
            services,
            base: format!("http://{addr}"),
            addr,
            client: reqwest::Client::new(),
            handle,
        }
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        let r = self
            .client
            .get(format!("{}{path}", self.base))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    async fn post(&self, path: &str, body: Value) -> (u16, Value) {
        let r = self
            .client
            .post(format!("{}{path}", self.base))
            .bearer_auth(TOKEN)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    fn generation(&self) -> i64 {
        self.services.graph().generation_id
    }
}

fn widget_request(generation: i64, request: &str, payload: Value) -> Value {
    json!({
        "protocol": "dash.widget/1",
        "dashboard_id": "engineering",
        "adapter_instance": "pr_table_adapter",
        "widget_id": "pr_table_adapter_pr_table",
        "generation": generation,
        "sequence": 1,
        "kind": "request",
        "request": request,
        "payload": payload,
    })
}

#[tokio::test]
async fn widget_request_dry_run_resolves_through_adapter_without_side_effects() {
    let srv = TestServer::boot().await;
    let gen_id = srv.generation();

    let (status, rep) = srv
        .post(
            "/api/widget-request/dry-run",
            widget_request(
                gen_id,
                "merge_requested",
                json!({ "repository": "acme/platform", "number": 42, "issue_key": "DEV-123" }),
            ),
        )
        .await;
    assert_eq!(status, 200, "{rep}");
    assert_eq!(rep["resolution"], "action");
    // The action Instance comes from the Adapter binding (`merge` input), not from the UI.
    assert_eq!(rep["action_instance"], "merge_pr");
    assert_eq!(rep["args"]["number"], 42);
    assert_eq!(rep["requires_confirmation"], true);
    assert_eq!(rep["dry_run"]["action_instance"], "merge_pr");
    assert_eq!(rep["dry_run"]["advisory"], true);

    // No execution record was created by the Dry run.
    let (_, execs) = srv.get("/api/executions").await;
    assert_eq!(execs.as_array().map(|a| a.len()), Some(0), "{execs}");

    // A refresh Request needs no confirmation.
    let (status, rep) = srv
        .post(
            "/api/widget-request/dry-run",
            widget_request(gen_id, "refresh_requested", json!({})),
        )
        .await;
    assert_eq!(status, 200, "{rep}");
    assert_eq!(rep["resolution"], "refresh");
    assert_eq!(rep["requires_confirmation"], false);

    // Stale Generation and undeclared Requests are rejected exactly like the real dispatch.
    let (status, _) = srv
        .post(
            "/api/widget-request/dry-run",
            widget_request(gen_id - 1, "merge_requested", json!({ "number": 42 })),
        )
        .await;
    assert_eq!(status, 400);
    let (status, _) = srv
        .post(
            "/api/widget-request/dry-run",
            widget_request(gen_id, "not_declared", json!({})),
        )
        .await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn definition_sources_are_served_for_workspace_packages_only() {
    let srv = TestServer::boot().await;
    let (status, src) = srv
        .get("/api/definitions/engineering.pr_table_adapter/sources")
        .await;
    assert_eq!(status, 200, "{src}");
    assert_eq!(src["editable"], true);
    assert_eq!(src["package_dir"], "engineering");
    assert_eq!(src["definition_dir"], "pr_table_adapter");
    assert_eq!(src["runtime_file"], "runtime.luau");
    assert!(src["definition_luau"].as_str().unwrap().contains("dash.adapter"));
    assert!(src["runtime_luau"].as_str().unwrap().contains("on_request"));
    assert!(src["tests_luau"].is_string());

    let (status, builtin) = srv.get("/api/definitions/dash.table/sources").await;
    assert_eq!(status, 200);
    assert_eq!(builtin["editable"], false);
    assert!(builtin["package_dir"].is_null());

    let (status, _) = srv.get("/api/definitions/nope.missing/sources").await;
    assert_eq!(status, 404);

    let (status, defs) = srv.get("/api/definitions").await;
    assert_eq!(status, 200);
    assert_eq!(
        defs["engineering.pr_table_adapter"]["inputs"]["merge"]["ref_kind"],
        "action"
    );
}

#[tokio::test]
async fn draft_graph_accumulates_edits_and_draft_diff_reports_them() {
    let srv = TestServer::boot().await;
    let (status, draft) = srv.post("/api/drafts", json!({})).await;
    assert_eq!(status, 200, "{draft}");
    let did = draft["draft_id"].as_str().unwrap().to_string();

    // First edit: rebind an input on pr_table_adapter inside the Draft.
    let (_, g0) = srv.get(&format!("/api/drafts/{did}/graph")).await;
    let mut adapter = g0["instances"]["pr_table_adapter"].clone();
    adapter["inputs"]["extra_prs"] = json!("open_prs");
    let (status, body) = srv
        .post(&format!("/api/drafts/{did}/instances"), adapter)
        .await;
    assert_eq!(status, 200, "{body}");

    // Second edit is built from the Draft graph, so the first edit survives.
    let (status, g1) = srv.get(&format!("/api/drafts/{did}/graph")).await;
    assert_eq!(status, 200, "{g1}");
    assert_eq!(g1["draft_id"], did.as_str());
    assert_eq!(g1["instances"]["pr_table_adapter"]["inputs"]["extra_prs"], "open_prs");
    let mut adapter = g1["instances"]["pr_table_adapter"].clone();
    adapter["inputs"]["more_prs"] = json!("open_prs");
    srv.post(&format!("/api/drafts/{did}/instances"), adapter)
        .await;
    let (_, g2) = srv.get(&format!("/api/drafts/{did}/graph")).await;
    let inputs = &g2["instances"]["pr_table_adapter"]["inputs"];
    assert_eq!(inputs["extra_prs"], "open_prs");
    assert_eq!(inputs["more_prs"], "open_prs");
    assert!(g2["validation"]["issues"].is_array());
    assert!(g2["definitions"]["engineering.pr_table_adapter"].is_object());

    // The active Generation is untouched by Draft edits.
    let (_, active) = srv.get("/api/graph").await;
    assert!(active["instances"]["pr_table_adapter"]["inputs"]["extra_prs"].is_null());

    // A new Definition appears as an added file in the Draft diff.
    let (status, _) = srv
        .post(
            &format!("/api/drafts/{did}/definitions"),
            json!({
                "package": "engineering",
                "name": "custom_metric",
                "definition_luau": "local dash = require(\"dash\")\nreturn dash.data_provider { id = \"engineering.custom_metric\", handler = \"query\" }\n",
                "runtime_luau": "local R = {}\nfunction R.query(ctx) return { ok = true } end\nreturn R\n",
            }),
        )
        .await;
    assert_eq!(status, 200);
    let (status, diff) = srv.get(&format!("/api/drafts/{did}/diff")).await;
    assert_eq!(status, 200, "{diff}");
    assert_eq!(diff["base_revision"], draft["base_revision"]);
    let files = diff["files"].as_array().unwrap();
    let find = |p: &str| files.iter().find(|f| f["path"] == p).cloned();
    let adapters_yaml = find("instances/adapters.yaml").expect("adapters.yaml in diff");
    assert_eq!(adapters_yaml["status"], "modified");
    assert!(adapters_yaml["patch"].as_str().unwrap().contains("+      more_prs: open_prs"));
    let new_def = find("packages/engineering/custom_metric/definition.luau")
        .expect("new definition in diff");
    assert_eq!(new_def["status"], "added");
    assert!(new_def["patch"].as_str().unwrap().contains("+return dash.data_provider"));

    // Sources can be read from the Draft worktree.
    let (status, src) = srv
        .get(&format!(
            "/api/definitions/engineering.custom_metric/sources?draft_id={did}"
        ))
        .await;
    assert_eq!(status, 200, "{src}");
    assert_eq!(src["definition_dir"], "custom_metric");
    assert_eq!(src["draft_id"], did.as_str());
}

#[tokio::test]
async fn rendered_custom_widgets_carry_declared_asset_url() {
    let srv = TestServer::boot().await;
    let (status, body) = srv.get("/api/dashboards/engineering").await;
    assert_eq!(status, 200, "{body}");
    let adapters = body["rendered"]["adapters"].as_array().unwrap();
    let table = adapters
        .iter()
        .find(|a| a["adapter_instance"] == "pr_table_adapter")
        .unwrap();
    let badge = table["tree"]["children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["widget_type"] == "engineering.pr_badge_widget")
        .unwrap();
    assert_eq!(
        badge["asset_url"],
        "/widgets/engineering.pr_badge_widget/assets/index.html"
    );
    // Built-in widgets have no asset URL.
    assert!(table["tree"]["asset_url"].is_null());

    // Assets are only published for widget provider Definitions.
    let r = srv
        .client
        .get(format!("{}/widgets/engineering.linked_prs/runtime.luau", srv.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 404);
}

#[tokio::test]
async fn ws_dashboard_subscription_pushes_state_changes_from_other_clients() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
    let srv = TestServer::boot().await;
    let mut req = format!("ws://{}/ws", srv.addr).into_client_request().unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    socket
        .send(Message::Text(
            json!({ "type": "subscribe_dashboard", "dashboard_id": "engineering" })
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

    async fn next_render(
        socket: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Value {
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(20), socket.next())
                .await
                .expect("timed out waiting for ws message")
                .expect("socket closed")
                .unwrap();
            if let Message::Text(t) = frame {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["type"] == "dashboard_render" {
                    return v;
                }
            }
        }
    }

    let first = next_render(&mut socket).await;
    assert_eq!(first["protocol"], "dash.render/1");
    assert_eq!(first["reason"], "subscribe");

    // Another Client (plain HTTP) changes Dashboard State.
    let (status, _) = srv
        .post(
            "/api/dashboards/engineering/state",
            json!({ "key": "selected_repository", "value": "acme/payments" }),
        )
        .await;
    assert_eq!(status, 200);
    let mut delivered = false;
    for _ in 0..6 {
        let msg = next_render(&mut socket).await;
        if msg["data"]["rendered"]["dashboard_state"]["selected_repository"] == "acme/payments" {
            delivered = true;
            break;
        }
    }
    assert!(delivered, "Dashboard State Signal was not pushed over /ws");
}
