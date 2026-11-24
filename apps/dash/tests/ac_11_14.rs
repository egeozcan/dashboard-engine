//! End-to-end acceptance coverage for §17 criteria 11–14 (visual UI backend, MCP Agent cycle,
//! transactional hot reload, Git transaction), driven through the real HTTP, MCP and
//! application-service paths of a fully initialized Engineering Command Center Workspace.
//!
//! The browser-side half of criterion 11 (drag-to-connect, Designer, Package Studio, execution
//! graph view, widget bridge) is covered by the Vitest suites in `web/src/**`; the tests here
//! check that the backend produces exactly the data those views rely on.

use axum::{
    extract::{Path as AxPath, State},
    routing::{get, post},
    Json, Router,
};
use dash_core::{CapabilityCeiling, DashboardDoc, InstanceConfig, Principal};
use dash_git::DraftManager;
use dash_luau::{DefinitionCategory, ResourceBudget};
use dash_server::{build_router, AppServices};
use dash_store::SqliteStore;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

const ADMIN_TOKEN: &str = "adm_t3_acceptance_token";

// ---------------------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------------------

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center")
}

fn copy_fixture(dst: &Path) {
    dash_git::packages::copy_tree_excluding_git(&fixture_dir(), dst).unwrap();
}

/// Run git synchronously and return trimmed stdout (panics on failure).
fn git(dir: &Path, args: &[&str]) -> String {
    let out = std::process::Command::new("git")
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?} in {} failed: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn commit_count(ws: &Path) -> usize {
    git(ws, &["rev-list", "--count", "HEAD"]).parse().unwrap()
}

/// The Workspace's tracked files exactly equal HEAD (index and working tree).
fn assert_workspace_matches_head(ws: &Path) {
    let porcelain = git(ws, &["status", "--porcelain", "--untracked-files=no"]);
    assert!(
        porcelain.is_empty(),
        "Workspace has tracked modifications: {porcelain}"
    );
    git(ws, &["diff", "--quiet", "HEAD"]);
}

/// A local bare repository used as the `origin` remote. When `reject` is set, a pre-receive
/// hook refuses pushes while the marker file exists.
struct BareRemote {
    dir: PathBuf,
    marker: PathBuf,
}

impl BareRemote {
    fn create(parent: &Path, reject: bool) -> Self {
        let dir = parent.join("remote.git");
        std::fs::create_dir_all(&dir).unwrap();
        git(&dir, &["init", "--bare", "-b", "main"]);
        let marker = parent.join("reject-pushes");
        if reject {
            let hook = dir.join("hooks/pre-receive");
            std::fs::write(
                &hook,
                format!(
                    "#!/bin/sh\nif [ -e '{}' ]; then echo 'remote rejects pushes' >&2; exit 1; fi\nexit 0\n",
                    marker.display()
                ),
            )
            .unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            std::fs::write(&marker, "1").unwrap();
        }
        Self { dir, marker }
    }

    fn allow_pushes(&self) {
        let _ = std::fs::remove_file(&self.marker);
    }

    fn main_commit(&self) -> Option<String> {
        let out = std::process::Command::new("git")
            .current_dir(&self.dir)
            .args(["rev-parse", "--verify", "refs/heads/main"])
            .output()
            .unwrap();
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

/// Initialize the Workspace repository by hand so a remote can be attached before Dash's first
/// startup (Dash then makes the initial commit itself).
fn attach_remote(ws: &Path, remote: &BareRemote) {
    git(ws, &["init", "-b", "main"]);
    git(
        ws,
        &["remote", "add", "origin", remote.dir.to_str().unwrap()],
    );
}

/// Local stand-in for the GitHub and Jira APIs the reference connectors call (§16), recording
/// every Jira transition it receives.
#[derive(Clone, Default)]
struct Upstream {
    transitions: Arc<Mutex<Vec<(String, Value)>>>,
}

async fn spawn_upstream() -> (Upstream, std::net::SocketAddr) {
    let upstream = Upstream::default();
    let prs: Value = serde_json::from_str(
        &std::fs::read_to_string(fixture_dir().join("packages/github/mock/fixtures/prs.json"))
            .unwrap(),
    )
    .unwrap();
    let issues: Value = serde_json::from_str(
        &std::fs::read_to_string(fixture_dir().join("packages/jira/mock/fixtures/issues.json"))
            .unwrap(),
    )
    .unwrap();
    let app = Router::new()
        .route(
            "/repos/{owner}/{repo}/pulls",
            get(move |AxPath((owner, repo)): AxPath<(String, String)>| {
                let prs = prs.clone();
                async move {
                    let full = format!("{owner}/{repo}");
                    let rows: Vec<Value> = prs
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|p| p["repository"] == full.as_str())
                        .cloned()
                        .collect();
                    Json(Value::Array(rows))
                }
            }),
        )
        .route(
            "/rest/api/3/search",
            get(move || {
                let issues = issues.clone();
                async move { Json(issues) }
            }),
        )
        .route(
            "/rest/api/3/issue/{key}/transitions",
            post(
                |State(up): State<Upstream>,
                 AxPath(key): AxPath<String>,
                 Json(body): Json<Value>| async move {
                    let status = body["status"].as_str().unwrap_or("Done").to_string();
                    up.transitions.lock().unwrap().push((key.clone(), body));
                    Json(json!({ "key": key, "status": status, "transitioned": true }))
                },
            ),
        )
        .with_state(upstream.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (upstream, addr)
}

/// Point the reference connectors at the local upstream and allow loopback HTTP in the
/// Workspace's requested network policy (the Administrator ceiling is set in SQLite).
fn point_connectors_at(ws: &Path, upstream: std::net::SocketAddr) {
    let base = format!("http://{upstream}");
    let connectors = std::fs::read_to_string(ws.join("instances/connectors.yaml")).unwrap();
    let connectors = connectors
        .replace("https://api.github.com", &base)
        .replace("https://jira.example.com", &base);
    std::fs::write(ws.join("instances/connectors.yaml"), connectors).unwrap();
    let toml = std::fs::read_to_string(ws.join("dashboard.toml")).unwrap();
    let toml = toml.replace(
        "allowed_hosts = [\"api.github.com\", \"jira.example.com\"]",
        "allowed_hosts = [\"api.github.com\", \"jira.example.com\", \"127.0.0.1\"]\nallow_loopback_for_tests = true",
    );
    std::fs::write(ws.join("dashboard.toml"), toml).unwrap();
}

fn grant_live_upstream(store: &SqliteStore) {
    store
        .set_secret(
            "github-work",
            "GitHub PAT",
            "ghp_t3_secret",
            &["github_reader".into(), "github_maintainer".into()],
        )
        .unwrap();
    store
        .set_secret(
            "jira-work",
            "Jira token",
            "jira_t3_secret",
            &["jira_reader".into(), "jira_transitioner".into()],
        )
        .unwrap();
    store
        .set_network_policy_ceiling(
            json!({
                "allowed_hosts": ["127.0.0.1"],
                "allowed_methods": ["GET", "POST", "PUT"],
                "allow_loopback_for_tests": true
            }),
            "admin",
        )
        .unwrap();
}

struct App {
    tmp: Option<TempDir>,
    ws: PathBuf,
    services: AppServices,
    base: String,
    client: reqwest::Client,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for App {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl App {
    /// Boot the daemon's router over an already prepared `tmp/ws` + `tmp/data`.
    async fn boot_prepared(tmp: TempDir, use_mocks: bool) -> Self {
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        let store = SqliteStore::open(&data).unwrap();
        let mgr = DraftManager::initialize_with_mocks(
            ws.clone(),
            data.clone(),
            store,
            ResourceBudget::default(),
            use_mocks,
        )
        .await
        .unwrap();
        let services = AppServices::new(mgr, vec![]);
        std::fs::write(&services.admin_cli_token_path, ADMIN_TOKEN).unwrap();
        let app = build_router(services.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let ws = std::fs::canonicalize(ws).unwrap();
        Self {
            tmp: Some(tmp),
            ws,
            services,
            base: format!("http://{addr}"),
            client: reqwest::Client::new(),
            handle,
        }
    }

    async fn boot_mocked() -> Self {
        let tmp = TempDir::new().unwrap();
        copy_fixture(&tmp.path().join("ws"));
        Self::boot_prepared(tmp, true).await
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        let r = self
            .client
            .get(format!("{}{path}", self.base))
            .bearer_auth(ADMIN_TOKEN)
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
            .bearer_auth(ADMIN_TOKEN)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    async fn post_ok(&self, path: &str, body: Value) -> Value {
        let (st, v) = self.post(path, body).await;
        assert_eq!(st, 200, "POST {path} failed: {v}");
        v
    }

    fn generation(&self) -> i64 {
        self.services.graph().generation_id
    }

    fn mgr(&self) -> &dash_git::DraftManager {
        &self.services.drafts
    }

    /// Stop serving and hand back the directories (the "process" exits; files stay).
    fn shutdown(mut self) -> TempDir {
        self.handle.abort();
        self.tmp.take().unwrap()
    }
}

/// A draft that was not activated stays editable (`open`, or `previewed` once Preview ran).
fn assert_still_open(status: &str) {
    assert!(
        matches!(status, "open" | "previewed"),
        "draft should still be open, is '{status}'"
    );
}

fn data_provider(id: &str, definition: &str) -> InstanceConfig {
    InstanceConfig {
        id: id.into(),
        category: DefinitionCategory::DataProvider,
        definition: definition.into(),
        inputs: BTreeMap::new(),
        config: json!({}),
        secrets: BTreeMap::new(),
        capabilities: vec![],
        run_as: None,
        enabled: None,
    }
}

fn adapter(id: &str, definition: &str, inputs: &[(&str, &str)]) -> InstanceConfig {
    InstanceConfig {
        category: DefinitionCategory::Adapter,
        inputs: inputs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..data_provider(id, definition)
    }
}

fn constant_provider_luau(id: &str) -> String {
    format!(
        "local dash = require(\"dash\"); return dash.data_provider {{ id = \"{id}\", handler = \"query\" }}"
    )
}

fn constant_runtime(value: i64) -> String {
    format!("local R = {{}}\nfunction R.query(ctx) return {{ value = {value} }} end\nreturn R\n")
}

/// Adapter rendering a `dash.counter` of open PRs from its `prs` input.
fn badge_definition(id: &str) -> String {
    format!(
        r#"local dash = require("dash")
return dash.adapter {{
    id = "{id}",
    description = "Open PR count badge",
    runtime = "runtime.luau",
    handler = "render",
    inputs = {{ prs = dash.data_ref("github.pull_requests") }},
}}
"#
    )
}

fn badge_runtime(label: &str) -> String {
    format!(
        r#"--!strict
local Runtime = {{}}
function Runtime.render(ctx)
    local prs = ctx:await(ctx.inputs.prs:query({{ repository = "acme/platform", state = "open" }}))
    return {{
        key = "badge",
        widget_type = "dash.counter",
        props = {{ label = "{label}", value = #prs, status = "healthy" }},
    }}
end
return Runtime
"#
    )
}

const BADGE_TESTS: &str = r#"local dash = require("dash")
local Runtime = require("runtime")

dash.test("badge counts Mock-backed open PRs", function(make_ctx)
    local tree = Runtime.render(make_ctx({}))
    dash.assert_eq(tree.widget_type, "dash.counter", "counter widget")
    dash.assert_eq(tree.props.value, 3, "three open acme/platform PRs in the GitHub Mock")
end)
"#;

fn engineering_doc(services: &AppServices) -> DashboardDoc {
    services.graph().dashboards["engineering"].clone()
}

fn find_widget<'a>(tree: &'a Value, widget_type: &str) -> Option<&'a Value> {
    if tree["widget_type"] == widget_type {
        return Some(tree);
    }
    tree["children"]
        .as_array()?
        .iter()
        .find_map(|c| find_widget(c, widget_type))
}

fn rendered_adapter<'a>(rendered: &'a Value, adapter_instance: &str) -> Option<&'a Value> {
    rendered["adapters"]
        .as_array()?
        .iter()
        .find(|a| a["adapter_instance"] == adapter_instance)
}

/// MCP Streamable-HTTP client for one Agent token.
struct Mcp {
    client: reqwest::Client,
    url: String,
    token: String,
    next_id: AtomicU64,
}

impl Mcp {
    fn new(base: &str, token: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: format!("{base}/mcp"),
            token: token.into(),
            next_id: AtomicU64::new(1),
        }
    }

    async fn rpc(&self, method: &str, params: Value) -> Value {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let r = self
            .client
            .post(&self.url)
            .bearer_auth(&self.token)
            .json(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200, "MCP {method} HTTP status");
        let v: Value = r.json().await.unwrap();
        assert!(v.get("error").is_none(), "MCP {method} failed: {v}");
        v["result"].clone()
    }

    async fn call(&self, tool: &str, args: Value) -> Result<Value, String> {
        let res = self
            .rpc("tools/call", json!({ "name": tool, "arguments": args }))
            .await;
        if res["isError"] == true {
            Err(res["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .to_string())
        } else {
            Ok(res["structuredContent"].clone())
        }
    }

    async fn tool(&self, tool: &str, args: Value) -> Value {
        match self.call(tool, args).await {
            Ok(v) => v,
            Err(e) => panic!("MCP tool {tool} failed: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// AC-11: visual UI — backend contract of the Graph Editor, Designer, Package Studio, traces
// ---------------------------------------------------------------------------------------------

/// Drag-to-connect in the Graph Editor upserts the target Instance with the new binding into a
/// Draft and reloads `/api/drafts/{id}/graph`; compatibility/capability errors must come back
/// on the offending Instance (and name the input or bound Instance so the UI can attach them
/// to the edge), and such a Draft can never be activated.
#[tokio::test]
async fn ac11_graph_editor_drafts_report_forbidden_bindings_and_capability_errors_inline() {
    let app = App::boot_mocked().await;
    let gen0 = app.generation();
    let head0 = git(&app.ws, &["rev-parse", "HEAD"]);

    let draft = app.post_ok("/api/drafts", json!({})).await;
    let id = draft["draft_id"].as_str().unwrap().to_string();
    let (st, graph) = app.get(&format!("/api/drafts/{id}/graph")).await;
    assert_eq!(st, 200);
    assert_eq!(graph["validation"]["valid"], true, "{graph}");

    let inst =
        |id: &str| -> Value { serde_json::to_value(&app.services.graph().instances[id]).unwrap() };

    // (1) data provider <- action: forbidden binding (§2.1)
    let mut linked = inst("linked_prs");
    linked["inputs"]["merge"] = json!("merge_pr");
    app.post_ok(&format!("/api/drafts/{id}/instances"), linked)
        .await;
    // (2) adapter <- connector: forbidden binding (§2.1)
    let mut trend = inst("pr_trend_adapter");
    trend["inputs"]["gh"] = json!("github_reader");
    app.post_ok(&format!("/api/drafts/{id}/instances"), trend)
        .await;
    // (3) action input bound to a connector lacking the required capability
    let mut merge = inst("merge_pr");
    merge["inputs"]["github"] = json!("github_reader");
    app.post_ok(&format!("/api/drafts/{id}/instances"), merge)
        .await;
    // (4) data input bound to an Instance of the wrong category
    let mut table = inst("pr_table_adapter");
    table["inputs"]["linked"] = json!("jira_reader");
    app.post_ok(&format!("/api/drafts/{id}/instances"), table)
        .await;

    let (st, graph) = app.get(&format!("/api/drafts/{id}/graph")).await;
    assert_eq!(st, 200);
    // The Draft graph shows the edited edges (the editor builds on it, not on the active graph).
    assert_eq!(
        graph["instances"]["linked_prs"]["inputs"]["merge"],
        "merge_pr"
    );
    assert_eq!(
        graph["instances"]["merge_pr"]["inputs"]["github"],
        "github_reader"
    );
    assert_eq!(graph["validation"]["valid"], false);
    let issues = graph["validation"]["issues"].as_array().unwrap().clone();
    let has = |code: &str, target: &str, needle: &str| {
        issues.iter().any(|i| {
            i["code"] == code
                && i["target"] == target
                && i["severity"] == "error"
                && i["message"].as_str().unwrap_or("").contains(needle)
        })
    };
    assert!(
        has(
            "FORBIDDEN_BINDING",
            "linked_prs",
            "cannot bind action 'merge_pr'"
        ),
        "{issues:#?}"
    );
    assert!(
        has(
            "FORBIDDEN_BINDING",
            "pr_trend_adapter",
            "cannot bind connector 'github_reader'"
        ),
        "{issues:#?}"
    );
    assert!(
        has(
            "INSUFFICIENT_CAPABILITY",
            "merge_pr",
            "input 'github' requires capability 'pull_requests.merge'"
        ),
        "{issues:#?}"
    );
    assert!(
        has(
            "INPUT_CATEGORY_MISMATCH",
            "pr_table_adapter",
            "input 'linked'"
        ),
        "{issues:#?}"
    );
    // Required capabilities are shown inline from the Definition summaries.
    let (_, defs) = app.get("/api/definitions").await;
    assert!(
        defs["github.merge_pr"]["inputs"]["github"]["required_capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!("pull_requests.merge")),
        "{}",
        defs["github.merge_pr"]
    );

    // The same report is what `validate` returns, and activation refuses the Draft.
    let (st, v) = app
        .post(&format!("/api/drafts/{id}/validate"), json!({}))
        .await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["valid"], false);
    let (st, err) = app
        .post(&format!("/api/drafts/{id}/activate"), json!({}))
        .await;
    assert_eq!(st, 400, "{err}");
    assert_eq!(app.generation(), gen0);
    assert_eq!(git(&app.ws, &["rev-parse", "HEAD"]), head0);
    let (_, live) = app.get("/api/graph").await;
    assert!(live["instances"]["linked_prs"]["inputs"]
        .get("merge")
        .is_none());

    // Removing the offending bindings (the editor's "Remove binding") clears the errors.
    for name in [
        "linked_prs",
        "pr_trend_adapter",
        "merge_pr",
        "pr_table_adapter",
    ] {
        app.post_ok(&format!("/api/drafts/{id}/instances"), inst(name))
            .await;
    }
    let (_, graph) = app.get(&format!("/api/drafts/{id}/graph")).await;
    assert_eq!(
        graph["validation"]["valid"], true,
        "{}",
        graph["validation"]
    );
}

/// Edit code in Package Studio, edit the Dashboard layout in the Designer and drag-connect a new
/// Instance, all in one Draft, then activate it: the live render reflects every edit, and the
/// read-only execution graph and runtime traces describe what actually ran.
#[tokio::test]
async fn ac11_ui_edits_code_layout_and_bindings_then_observes_execution_graph_and_traces() {
    let app = App::boot_mocked().await;
    let (st, before) = app.get("/api/dashboards/engineering").await;
    assert_eq!(st, 200, "{before}");

    let id = app.post_ok("/api/drafts", json!({})).await["draft_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Package Studio: open sources, edit runtime.luau, save to the Draft.
    let (st, src) = app
        .get("/api/definitions/engineering.pr_trend_adapter/sources")
        .await;
    assert_eq!(st, 200, "{src}");
    let runtime = src["runtime_luau"].as_str().unwrap();
    assert!(runtime.contains("\"Open PRs (\""), "{runtime}");
    let edited = runtime.replace("\"Open PRs (\"", "\"Open pull requests (\"");
    app.post_ok(
        &format!("/api/drafts/{id}/definitions"),
        json!({
            "package": "engineering",
            "name": "pr_trend_adapter",
            "definition_luau": src["definition_luau"],
            "runtime_luau": edited,
            "tests_luau": src["tests_luau"],
        }),
    )
    .await;
    let (_, draft_src) = app
        .get(&format!(
            "/api/definitions/engineering.pr_trend_adapter/sources?draft_id={id}"
        ))
        .await;
    assert_eq!(draft_src["runtime_luau"], json!(edited));
    let (_, diff) = app.get(&format!("/api/drafts/{id}/diff")).await;
    let files = diff["files"].as_array().unwrap();
    assert!(
        files.iter().any(
            |f| f["path"] == "packages/engineering/pr_trend_adapter/runtime.luau"
                && f["status"] == "modified"
                && f["patch"].as_str().unwrap().contains("+")
                && f["patch"]
                    .as_str()
                    .unwrap()
                    .contains("Open pull requests (")
        ),
        "{diff}"
    );

    // Graph Editor: create a new adapter Instance and drag-connect open_prs onto it.
    app.post_ok(
        &format!("/api/drafts/{id}/definitions"),
        json!({
            "package": "engineering",
            "name": "ui_badge",
            "definition_luau": badge_definition("engineering.ui_badge"),
            "runtime_luau": badge_runtime("UI badge"),
            "tests_luau": BADGE_TESTS,
        }),
    )
    .await;
    let mut badge = serde_json::to_value(adapter("ui_badge", "engineering.ui_badge", &[])).unwrap();
    app.post_ok(&format!("/api/drafts/{id}/instances"), badge.clone())
        .await;
    badge["inputs"]["prs"] = json!("open_prs");
    app.post_ok(&format!("/api/drafts/{id}/instances"), badge)
        .await;

    // Designer: place it and override a generated Widget node's layout, built on the Draft base.
    let (_, dgraph) = app.get(&format!("/api/drafts/{id}/graph")).await;
    let mut doc: DashboardDoc =
        serde_json::from_value(dgraph["dashboards"]["engineering"].clone()).unwrap();
    doc.adapters.push(
        serde_json::from_value(
            json!({ "adapter_instance": "ui_badge", "area": "side", "query_params": {} }),
        )
        .unwrap(),
    );
    let mut ov = serde_json::to_value(&doc.overrides).unwrap();
    ov["pr_trend_adapter_trend_chart"]["w"] = json!(6);
    ov["ui_badge_badge"] = json!({ "w": 3, "h": 2 });
    doc.overrides = serde_json::from_value(ov).unwrap();
    app.post_ok(
        &format!("/api/drafts/{id}/dashboards"),
        serde_json::to_value(&doc).unwrap(),
    )
    .await;
    let (_, dgraph) = app.get(&format!("/api/drafts/{id}/graph")).await;
    assert_eq!(
        dgraph["validation"]["valid"], true,
        "{}",
        dgraph["validation"]
    );

    let act = app
        .post_ok(&format!("/api/drafts/{id}/activate"), json!({}))
        .await;
    let gen = act["generation_id"].as_i64().unwrap();

    let (st, after) = app.get("/api/dashboards/engineering").await;
    assert_eq!(st, 200, "{after}");
    let rendered = &after["rendered"];
    assert_eq!(rendered["generation"], gen);
    // Code edit is live.
    let trend = rendered_adapter(rendered, "pr_trend_adapter").unwrap();
    let counter = find_widget(&trend["tree"], "dash.counter").unwrap();
    assert!(
        counter["props"]["label"]
            .as_str()
            .unwrap()
            .starts_with("Open pull requests ("),
        "{counter}"
    );
    // Layout overrides survive into the render, keyed by stable widget_id.
    let chart = find_widget(&trend["tree"], "dash.chart").unwrap();
    assert_eq!(chart["widget_id"], "pr_trend_adapter_trend_chart");
    assert_eq!(
        rendered["overrides"]["pr_trend_adapter_trend_chart"]["w"],
        6
    );
    // The drag-connected Instance renders from the bound data provider (GitHub Mock: 3 open).
    let ui_badge = rendered_adapter(rendered, "ui_badge").expect("new placement rendered");
    assert_eq!(ui_badge["tree"]["widget_id"], "ui_badge_badge");
    assert_eq!(ui_badge["tree"]["props"]["value"], 3);
    assert_eq!(after["document"]["overrides"]["ui_badge_badge"]["w"], 3);

    // Read-only execution graph: runtime query nodes with subscribers / cache stats, the
    // Adapter render nodes, and the dynamic edge ui_badge -> open_prs; spans carry trace ids.
    let (st, traces) = app.get("/api/diagnostics/traces").await;
    assert_eq!(st, 200);
    let eg = &traces["execution_graph"];
    assert_eq!(eg["generation_id"], gen);
    let nodes = eg["nodes"].as_array().unwrap();
    let open_prs_node = nodes
        .iter()
        .find(|n| n["instance_id"] == "open_prs" && n["kind"] == "query")
        .expect("open_prs query node");
    assert!(open_prs_node["cache_misses"].as_u64().unwrap() >= 1);
    assert_eq!(open_prs_node["poll_interval_secs"], 60);
    let badge_node = nodes
        .iter()
        .find(|n| n["instance_id"] == "ui_badge" && n["kind"] == "adapter_render")
        .expect("ui_badge render node");
    let edges = eg["edges"].as_array().unwrap();
    assert!(
        edges
            .iter()
            .any(|e| e["from_query_key"] == badge_node["query_key"]
                && e["to_query_key"] == open_prs_node["query_key"]),
        "dynamic dependency ui_badge -> open_prs missing: {edges:#?}"
    );
    let spans = traces["spans"].as_array().unwrap();
    assert!(!spans.is_empty());
    assert!(spans
        .iter()
        .all(|s| s["trace_id"].as_str().is_some_and(|t| !t.is_empty())));
    assert!(
        spans
            .iter()
            .any(|s| s["name"].as_str().unwrap_or("").contains("ui_badge")),
        "no span recorded for the new Adapter: {spans:#?}"
    );
}

// ---------------------------------------------------------------------------------------------
// AC-12 + §16 behaviour 9: autonomous Agent cycle over MCP
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac12_agent_adds_widget_via_mcp_tests_rebases_activates_pushes_and_invokes_action() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    copy_fixture(&ws);
    let (upstream, up_addr) = spawn_upstream().await;
    point_connectors_at(&ws, up_addr);
    let remote = BareRemote::create(tmp.path(), false);
    attach_remote(&ws, &remote);
    {
        let store = SqliteStore::open(&tmp.path().join("data")).unwrap();
        grant_live_upstream(&store);
    }
    let app = App::boot_prepared(tmp, false).await;
    let admin = Principal::administrator();
    app.services.git_sync(&admin).await.unwrap();
    let base_head = git(&app.ws, &["rev-parse", "HEAD"]);
    assert_eq!(remote.main_commit().as_deref(), Some(base_head.as_str()));
    let base_count = commit_count(&app.ws);

    let (agent, token) = app
        .services
        .agent_create(
            "widget-agent",
            CapabilityCeiling {
                allowed_providers: BTreeSet::from(["*".into()]),
                allowed_actions: BTreeSet::from(["transition_issue".into()]),
                // Rendering the Dashboard reads through the reader connectors as the Agent.
                allowed_connectors: BTreeSet::from([
                    "jira_transitioner".into(),
                    "jira_reader".into(),
                    "github_reader".into(),
                ]),
                allowed_capabilities: BTreeSet::from([
                    "issues.transition".into(),
                    "issues.read".into(),
                    "pull_requests.read".into(),
                ]),
                allowed_secrets: BTreeSet::from(["github-work".into(), "jira-work".into()]),
                can_author_drafts: true,
                can_activate_drafts: true,
                ..Default::default()
            },
            None,
            &admin,
        )
        .unwrap();
    let mcp = Mcp::new(&app.base, &token);

    // Discover
    let init = mcp
        .rpc("initialize", json!({ "protocolVersion": "2025-06-18" }))
        .await;
    assert_eq!(init["serverInfo"]["name"], "dash-mcp");
    let tools = mcp.rpc("tools/list", json!({})).await;
    let tool_names: BTreeSet<String> = tools["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    for t in [
        "draft_begin",
        "definition_create",
        "definition_edit",
        "definition_test",
        "instance_create",
        "dashboard_update",
        "draft_test",
        "draft_preview",
        "draft_rebase",
        "draft_activate",
        "action_invoke",
        "execution_inspect",
        "git_status",
    ] {
        assert!(tool_names.contains(t), "tools/list lacks {t}");
    }
    let resources = mcp.rpc("resources/list", json!({})).await;
    let uris: Vec<String> = resources["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["uri"].as_str().unwrap().to_string())
        .collect();
    assert!(uris.contains(&"dash://definitions/github.pull_requests".to_string()));
    assert!(uris.contains(&"dash://graph/dependencies".to_string()));
    let contract = mcp
        .rpc(
            "resources/read",
            json!({ "uri": "dash://definitions/github.pull_requests" }),
        )
        .await;
    let contract: Value =
        serde_json::from_str(contract["contents"][0]["text"].as_str().unwrap()).unwrap();
    assert!(
        contract.to_string().contains("dev.pull_request@1"),
        "contract lacks the semantic output type: {contract}"
    );
    let deps = mcp
        .rpc(
            "resources/read",
            json!({ "uri": "dash://graph/dependencies" }),
        )
        .await;
    let deps: Value = serde_json::from_str(deps["contents"][0]["text"].as_str().unwrap()).unwrap();
    assert!(deps["edges"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["from"] == "pr_trend_adapter" && e["to"] == "open_prs"));
    let cat = mcp
        .tool("catalog_search", json!({ "query": "pull_requests" }))
        .await;
    assert!(cat["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["id"] == "github.pull_requests"));

    // Draft: new adapter Definition (Luau), edit it, graph edit, Dashboard placement
    let draft = mcp.tool("draft_begin", json!({})).await;
    let draft_id = draft["draft_id"].as_str().unwrap().to_string();
    assert_eq!(draft["base_revision"], base_head.as_str());
    mcp.tool(
        "definition_create",
        json!({
            "draft_id": draft_id, "package": "engineering", "name": "open_pr_badge",
            "definition_luau": badge_definition("engineering.open_pr_badge"),
            "runtime_luau": badge_runtime("draft label"),
            "tests_luau": BADGE_TESTS,
        }),
    )
    .await;
    mcp.tool(
        "definition_edit",
        json!({
            "draft_id": draft_id, "package": "engineering", "name": "open_pr_badge",
            "definition_luau": badge_definition("engineering.open_pr_badge"),
            "runtime_luau": badge_runtime("Open PRs (agent)"),
            "tests_luau": BADGE_TESTS,
        }),
    )
    .await;
    mcp.tool(
        "instance_create",
        json!({ "draft_id": draft_id, "instance": adapter("open_pr_badge", "engineering.open_pr_badge", &[("prs", "open_prs")]) }),
    )
    .await;
    let dash_doc = mcp
        .tool("dashboard_get", json!({ "dashboard_id": "engineering" }))
        .await;
    let mut doc = dash_doc["document"].clone();
    doc["adapters"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "adapter_instance": "open_pr_badge", "area": "side", "query_params": {} }));
    mcp.tool(
        "dashboard_update",
        json!({ "draft_id": draft_id, "dashboard": doc }),
    )
    .await;

    // Definition tests and Scenario tests (against Mocks), then Preview
    let dt = mcp
        .tool(
            "definition_test",
            json!({ "draft_id": draft_id, "definition_id": "engineering.open_pr_badge" }),
        )
        .await;
    assert_eq!(dt["failed"], 0, "{dt}");
    assert_eq!(dt["passed"], 1, "{dt}");
    let tests = mcp
        .tool("draft_test", json!({ "draft_id": draft_id }))
        .await;
    assert_eq!(tests["all_passed"], true, "{tests}");
    assert!(!tests["scenario_results"].as_array().unwrap().is_empty());
    assert!(tests["definition_tests"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d["definition_id"] == "engineering.open_pr_badge" && d["passed"] == 1));
    let preview = mcp
        .tool("draft_preview", json!({ "draft_id": draft_id }))
        .await;
    assert_eq!(preview["all_passed"], true, "{preview}");
    let pv = mcp
        .tool(
            "dashboard_preview",
            json!({ "draft_id": draft_id, "dashboard_id": "engineering" }),
        )
        .await;
    let pv_badge = rendered_adapter(&pv, "open_pr_badge")
        .or_else(|| rendered_adapter(&pv["rendered"], "open_pr_badge"))
        .unwrap_or_else(|| panic!("Preview render lacks the new widget: {pv}"));
    assert_eq!(pv_badge["tree"]["props"]["label"], "Open PRs (agent)");
    // Preview never touched the live Generation or the Workspace.
    assert!(!app.services.graph().instances.contains_key("open_pr_badge"));
    assert_eq!(git(&app.ws, &["rev-parse", "HEAD"]), base_head);

    // Concurrent Administrator change makes the Agent's draft stale
    let other = app.services.draft_begin(&admin).await.unwrap();
    app.services
        .dashboard_update_in_draft(
            &other.draft_id,
            &serde_json::from_value(json!({
                "id": "ops", "title": "Ops", "layout": "grid", "adapters": [], "overrides": {}
            }))
            .unwrap(),
            &admin,
        )
        .unwrap();
    let other_act = app
        .services
        .draft_activate_as(&other.draft_id, &admin)
        .await
        .unwrap();
    assert_eq!(other_act.sync_status, "synced");

    let stale = mcp
        .call("draft_activate", json!({ "draft_id": draft_id }))
        .await
        .unwrap_err();
    assert!(stale.contains("stale draft base"), "{stale}");
    assert_eq!(
        git(&app.ws, &["rev-parse", "HEAD"]),
        other_act.revision_commit
    );
    let status = mcp
        .tool("draft_status", json!({ "draft_id": draft_id }))
        .await;
    assert_still_open(status["status"].as_str().unwrap());
    assert_eq!(status["base_revision"], base_head.as_str());

    let rebased = mcp
        .tool("draft_rebase", json!({ "draft_id": draft_id }))
        .await;
    assert_eq!(rebased["base_revision"], other_act.revision_commit.as_str());
    assert!(
        rebased["test_report"].is_null(),
        "rebase invalidates test results"
    );

    // Activate: zero-downtime swap, one commit, pushed to the bare remote
    let act = mcp
        .tool("draft_activate", json!({ "draft_id": draft_id }))
        .await;
    assert_eq!(act["sync_status"], "synced", "{act}");
    let new_head = git(&app.ws, &["rev-parse", "HEAD"]);
    assert_eq!(act["revision_commit"], new_head.as_str());
    assert_eq!(
        commit_count(&app.ws),
        base_count + 2,
        "one commit per draft"
    );
    assert_eq!(
        git(&app.ws, &["rev-parse", "HEAD^"]),
        other_act.revision_commit
    );
    assert_eq!(remote.main_commit().as_deref(), Some(new_head.as_str()));
    let changed = git(&app.ws, &["show", "--name-only", "--format=", "HEAD"]);
    for f in [
        "packages/engineering/open_pr_badge/definition.luau",
        "packages/engineering/open_pr_badge/runtime.luau",
        "packages/engineering/open_pr_badge/tests.luau",
        "dashboards/engineering.yaml",
    ] {
        assert!(
            changed.lines().any(|l| l == f),
            "{f} not in commit: {changed}"
        );
    }
    assert!(
        !changed.contains("dashboards/ops.yaml"),
        "rebase must not re-commit upstream files"
    );
    let msg = git(&app.ws, &["log", "-1", "--format=%B"]);
    assert!(msg.contains(&format!("Dash-Draft: {draft_id}")), "{msg}");
    assert!(
        msg.contains(&format!("Dash-Principal: {}", agent.agent_id)),
        "{msg}"
    );
    assert!(msg.contains("Dash-Principal-Kind: agent"), "{msg}");
    assert_workspace_matches_head(&app.ws);

    let gs = mcp.tool("git_status", json!({})).await;
    assert_eq!(gs["sync_status"], "synced");
    assert_eq!(gs["head_commit"], new_head.as_str());
    assert_eq!(gs["head_diverged"], false);
    let status = mcp
        .tool("draft_status", json!({ "draft_id": draft_id }))
        .await;
    assert_eq!(status["status"], "activated");

    // The live Dashboard now renders the widget from the real (local) GitHub API.
    let live = mcp
        .tool("dashboard_get", json!({ "dashboard_id": "engineering" }))
        .await;
    let badge = rendered_adapter(&live["rendered"], "open_pr_badge")
        .unwrap_or_else(|| panic!("live render lacks the widget: {live}"));
    assert_eq!(badge["tree"]["props"]["label"], "Open PRs (agent)");
    assert_eq!(badge["tree"]["props"]["value"], 3);
    assert_eq!(live["rendered"]["generation"], act["generation_id"]);

    // Live authorized action, outside any draft
    let dry = mcp
        .tool(
            "action_dry_run",
            json!({ "action_instance": "transition_issue", "args": { "key": "DEV-123" } }),
        )
        .await;
    assert_eq!(dry["feasible"], true, "{dry}");
    assert!(
        upstream.transitions.lock().unwrap().is_empty(),
        "Dry run must not call Jira"
    );
    let exec = mcp
        .tool(
            "action_invoke",
            json!({ "action_instance": "transition_issue", "args": { "key": "DEV-123", "status": "Done" }, "idempotency_key": "t3-dev-123" }),
        )
        .await;
    assert_eq!(exec["status"], "succeeded", "{exec}");
    assert_eq!(exec["principal_id"], agent.agent_id.as_str());
    {
        let calls = upstream.transitions.lock().unwrap();
        assert_eq!(calls.len(), 1, "the live Jira API received the transition");
        assert_eq!(calls[0].0, "DEV-123");
        assert_eq!(calls[0].1["status"], "Done");
    }
    // Not a configuration write: no draft, no commit.
    assert_eq!(git(&app.ws, &["rev-parse", "HEAD"]), new_head);
    let open: Vec<_> = app
        .services
        .draft_list()
        .unwrap()
        .into_iter()
        .filter(|d| d.status == "open")
        .collect();
    assert!(open.is_empty(), "{open:?}");
    // Not authorized: an action outside the Agent ceiling is refused and audited.
    let denied = mcp
        .call(
            "action_invoke",
            json!({ "action_instance": "merge_pr", "args": { "repository": "acme/platform", "number": 42 } }),
        )
        .await
        .unwrap_err();
    assert!(denied.contains("merge_pr"), "{denied}");
    assert!(app
        .services
        .audit_query(200)
        .unwrap()
        .iter()
        .any(|a| a.operation == "action.invoke"
            && a.target == "merge_pr"
            && a.principal_id == agent.agent_id
            && a.outcome == "denied"));

    // Inspect status
    let inspected = mcp
        .tool(
            "execution_inspect",
            json!({ "execution_id": exec["execution_id"] }),
        )
        .await;
    assert_eq!(inspected["status"], "succeeded");
    assert_eq!(inspected["action_instance"], "transition_issue");
    assert_eq!(inspected["principal_kind"], "agent");
    let history = mcp.tool("git_history", json!({ "limit": 5 })).await;
    assert!(history.to_string().contains(&new_head), "{history}");
    let audits = app.services.audit_query(200).unwrap();
    assert!(audits.iter().any(|a| a.operation == "draft.activate"
        && a.principal_id == agent.agent_id
        && a.outcome == "activated"));
}

// ---------------------------------------------------------------------------------------------
// AC-13: transactional hot reload
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac13_invalid_updates_leave_the_old_generation_serving() {
    let app = App::boot_mocked().await;
    let admin = Principal::administrator();
    let render = |app: &App| {
        let s = app.services.clone();
        async move {
            s.dashboard_get("engineering", &Principal::administrator())
                .await
        }
    };
    let before = render(&app).await.unwrap();
    let gen0 = app.generation();
    let head0 = git(&app.ws, &["rev-parse", "HEAD"]);
    let count0 = commit_count(&app.ws);
    let trend_vm = app
        .services
        .engine()
        .pool
        .vm_uid("pr_trend_adapter")
        .unwrap();
    let qk = app
        .services
        .engine()
        .queries
        .resolve_query_key(
            "open_prs",
            &json!({ "repository": "acme/platform", "state": "open" }),
            &admin,
        )
        .unwrap();
    let cached_version = app
        .services
        .engine()
        .queries
        .cached_entry(&qk)
        .expect("render warmed the open_prs cache")
        .version;

    let trend_src = app
        .services
        .definition_sources("engineering.pr_trend_adapter", None, &admin)
        .map(|s| serde_json::to_value(s).unwrap())
        .unwrap();
    let def_luau = trend_src["definition_luau"].as_str().unwrap().to_string();
    let tests_luau = trend_src["tests_luau"].as_str().unwrap().to_string();

    type Edit = Box<dyn Fn(&AppServices, &str)>;
    let cases: Vec<(&str, Edit, &str)> = vec![
        (
            "Luau syntax error",
            Box::new({
                let d = def_luau.clone();
                let t = tests_luau.clone();
                move |s, id| {
                    s.definition_upsert_in_draft(
                        id,
                        "engineering",
                        "pr_trend_adapter",
                        &d,
                        Some("local Runtime = {\nfunction Runtime.render(ctx) return nil end\n"),
                        Some(&t),
                        &Principal::administrator(),
                    )
                    .unwrap();
                }
            }),
            "does not compile: syntax error",
        ),
        (
            "failing Definition test",
            Box::new({
                let d = def_luau.clone();
                let rt = trend_src["runtime_luau"].as_str().unwrap().to_string();
                move |s, id| {
                    s.definition_upsert_in_draft(
                        id,
                        "engineering",
                        "pr_trend_adapter",
                        &d,
                        Some(&rt),
                        Some("local dash = require(\"dash\")\ndash.test(\"always fails\", function(make_ctx) dash.assert(false, \"boom\") end)\n"),
                        &Principal::administrator(),
                    )
                    .unwrap();
                }
            }),
            "always fails",
        ),
        (
            "binding to a missing Instance",
            Box::new(|s, id| {
                s.instance_upsert_in_draft(
                    id,
                    &adapter(
                        "ghost_badge",
                        "engineering.pr_trend_adapter",
                        &[("prs", "ghost"), ("history", "pr_count_history")],
                    ),
                    &Principal::administrator(),
                )
                .unwrap();
            }),
            "MISSING_INPUT_INSTANCE",
        ),
        (
            "runtime that cannot load within the budget",
            Box::new(|s, id| {
                s.definition_upsert_in_draft(
                    id,
                    "engineering",
                    "spin",
                    &constant_provider_luau("engineering.spin"),
                    Some("local R = {}\nfunction R.query(ctx) return {} end\nwhile true do end\nreturn R\n"),
                    None,
                    &Principal::administrator(),
                )
                .unwrap();
                s.instance_upsert_in_draft(
                    id,
                    &data_provider("spin", "engineering.spin"),
                    &Principal::administrator(),
                )
                .unwrap();
            }),
            "could not be prepared",
        ),
    ];

    for (name, edit, needle) in cases {
        let d = app.services.draft_begin(&admin).await.unwrap();
        edit(&app.services, &d.draft_id);
        let err = app
            .services
            .draft_activate_as(&d.draft_id, &admin)
            .await
            .expect_err(name);
        assert!(err.contains(needle), "{name}: {err}");
        assert_eq!(app.generation(), gen0, "{name}: Generation swapped");
        assert_eq!(
            git(&app.ws, &["rev-parse", "HEAD"]),
            head0,
            "{name}: committed"
        );
        assert_eq!(commit_count(&app.ws), count0, "{name}");
        assert_workspace_matches_head(&app.ws);
        // The old Generation keeps working: same VM, same cached query, renders still succeed.
        let after = render(&app)
            .await
            .unwrap_or_else(|e| panic!("{name}: render failed: {e}"));
        assert_eq!(after["rendered"]["generation"], gen0);
        assert_eq!(
            after["rendered"]["adapters"], before["rendered"]["adapters"],
            "{name}"
        );
        assert_eq!(
            app.services
                .engine()
                .pool
                .vm_uid("pr_trend_adapter")
                .unwrap(),
            trend_vm,
            "{name}"
        );
        assert_eq!(
            app.services
                .engine()
                .queries
                .cached_entry(&qk)
                .unwrap()
                .version,
            cached_version,
            "{name}"
        );
        assert_still_open(&app.services.draft_status(&d.draft_id).unwrap().status);
        app.services.draft_abort(&d.draft_id).await.unwrap();
    }
    let journals = app
        .services
        .store()
        .list_incomplete_activation_journals()
        .unwrap();
    assert!(journals.is_empty(), "{journals:?}");

    // And the system is not wedged: a valid update still activates.
    let d = app.services.draft_begin(&admin).await.unwrap();
    app.services
        .definition_upsert_in_draft(
            &d.draft_id,
            "engineering",
            "ok_metric",
            &constant_provider_luau("engineering.ok_metric"),
            Some(&constant_runtime(5)),
            None,
            &admin,
        )
        .unwrap();
    app.services
        .instance_upsert_in_draft(
            &d.draft_id,
            &data_provider("ok_metric", "engineering.ok_metric"),
            &admin,
        )
        .unwrap();
    let act = app
        .services
        .draft_activate_as(&d.draft_id, &admin)
        .await
        .unwrap();
    assert!(act.generation_id > gen0);
    let v = app
        .services
        .provider_query("ok_metric", json!({}), &admin, false)
        .await
        .unwrap();
    assert_eq!(v.value.as_ref().unwrap()["value"], 5);
}

/// Readers rendering the Dashboard while a draft that changes the Dashboard document, an Adapter's
/// code and the graph is activated must see either the whole old or the whole new Generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac13_concurrent_readers_never_observe_a_mixed_generation() {
    let app = App::boot_mocked().await;
    let admin = Principal::administrator();
    let gen0 = app.generation();
    let old_title = engineering_doc(&app.services).title.clone();

    let d = app.services.draft_begin(&admin).await.unwrap();
    let id = d.draft_id.clone();
    let src = app
        .services
        .definition_sources("engineering.pr_trend_adapter", None, &admin)
        .map(|s| serde_json::to_value(s).unwrap())
        .unwrap();
    app.services
        .definition_upsert_in_draft(
            &id,
            "engineering",
            "pr_trend_adapter",
            src["definition_luau"].as_str().unwrap(),
            Some(
                &src["runtime_luau"]
                    .as_str()
                    .unwrap()
                    .replace("\"Open PRs (\"", "\"Open PRs v2 (\""),
            ),
            src["tests_luau"].as_str(),
            &admin,
        )
        .unwrap();
    app.services
        .definition_upsert_in_draft(
            &id,
            "engineering",
            "v2_badge",
            &badge_definition("engineering.v2_badge"),
            Some(&badge_runtime("v2 badge")),
            Some(BADGE_TESTS),
            &admin,
        )
        .unwrap();
    app.services
        .instance_upsert_in_draft(
            &id,
            &adapter("v2_badge", "engineering.v2_badge", &[("prs", "open_prs")]),
            &admin,
        )
        .unwrap();
    let mut doc = engineering_doc(&app.services);
    doc.title = "Engineering v2".into();
    doc.adapters.push(
        serde_json::from_value(
            json!({ "adapter_instance": "v2_badge", "area": "side", "query_params": {} }),
        )
        .unwrap(),
    );
    app.services
        .dashboard_update_in_draft(&id, &doc, &admin)
        .unwrap();
    // Run Preview up front so activation itself is short and the readers overlap the swap.
    let rep = app.services.draft_preview(&id).await.unwrap();
    assert!(rep.all_passed, "{rep:?}");

    let stop = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::new();
    for _ in 0..4 {
        let s = app.services.clone();
        let stop = Arc::clone(&stop);
        let started = Arc::clone(&started);
        let old_title = old_title.clone();
        readers.push(tokio::spawn(async move {
            let mut seen: Vec<i64> = Vec::new();
            let mut n = 0usize;
            // Renders that start after `stop` was observed began after activation returned;
            // do a couple of those so every reader ends on the new Generation.
            let mut after_stop = 0;
            while after_stop < 2 {
                if stop.load(Ordering::SeqCst) {
                    after_stop += 1;
                }
                n += 1;
                let v = s
                    .dashboard_get_for_ui("engineering", &Principal::administrator())
                    .await
                    .unwrap_or_else(|e| panic!("render failed during activation: {e}"));
                let r = &v["rendered"];
                let gen = r["generation"].as_i64().unwrap();
                let title = r["title"].as_str().unwrap().to_string();
                let label = find_widget(
                    &rendered_adapter(r, "pr_trend_adapter").unwrap()["tree"],
                    "dash.counter",
                )
                .unwrap()["props"]["label"]
                    .as_str()
                    .unwrap()
                    .to_string();
                let has_badge = rendered_adapter(r, "v2_badge").is_some();
                let doc_title = v["document"]["title"].as_str().unwrap().to_string();
                let doc_has_badge = v["document"]["adapters"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|a| a["adapter_instance"] == "v2_badge");
                let new = title == "Engineering v2";
                assert!(
                    new || title == old_title,
                    "unexpected title {title}"
                );
                let consistent = has_badge == new
                    && label.starts_with("Open PRs v2 (") == new
                    && doc_title == title
                    && doc_has_badge == new
                    && (gen == gen0) == !new;
                assert!(
                    consistent,
                    "mixed Generation observed: gen={gen} title={title} label={label} badge={has_badge} doc_title={doc_title} doc_badge={doc_has_badge}"
                );
                if seen.last() != Some(&gen) {
                    seen.push(gen);
                }
                if n == 1 {
                    started.fetch_add(1, Ordering::SeqCst);
                }
                tokio::task::yield_now().await;
            }
            seen
        }));
    }
    // Every reader completes at least one render on the old Generation first.
    tokio::time::timeout(Duration::from_secs(20), async {
        while started.load(Ordering::SeqCst) < 4 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("readers never rendered");
    let act = app.services.draft_activate_as(&id, &admin).await.unwrap();
    stop.store(true, Ordering::SeqCst);
    for r in readers {
        let seen = r.await.unwrap();
        // Monotonic: once a reader saw the new Generation it never goes back.
        assert!(seen.windows(2).all(|w| w[0] < w[1]), "{seen:?}");
        assert_eq!(seen, vec![gen0, act.generation_id], "{seen:?}");
    }
}

/// Unchanged Instances keep their VM, cached query results (no recomputation), live
/// subscriptions and Instance State across an activation; changed Instances and downstream
/// dependents are rebuilt and their results marked stale until recomputed.
#[tokio::test]
async fn ac13_unchanged_instances_keep_vms_caches_subscriptions_and_instance_state() {
    let app = App::boot_mocked().await;
    let admin = Principal::administrator();
    let engine = app.services.engine().clone();
    engine
        .queries
        .render_dashboard("engineering", &admin)
        .await
        .unwrap();
    let params = json!({ "repository": "acme/platform", "state": "open" });
    let qk = engine
        .queries
        .resolve_query_key("open_prs", &params, &admin)
        .unwrap();
    let mut sub = engine.signals.subscribe_stream(&qk);
    let subs_before = engine.signals.subscriber_count(&qk);
    let entry_before = engine.queries.cached_entry(&qk).unwrap();
    let open_vm = engine.pool.vm_uid("open_prs").unwrap();
    let trend_vm = engine.pool.vm_uid("pr_trend_adapter").unwrap();
    let linked_vm = engine.pool.vm_uid("linked_prs").unwrap();
    let state_before = engine
        .state
        .get("instance", "open_prs", "seen_pr_states:acme/platform")
        .unwrap()
        .expect("open_prs wrote Instance State while querying");
    let starts_before = engine.pool.metrics().total_vm_starts;

    // Change only linked_prs (upstream of pr_table_adapter; unrelated to open_prs / trend).
    let d = app.services.draft_begin(&admin).await.unwrap();
    let src = app
        .services
        .definition_sources("engineering.linked_prs", None, &admin)
        .map(|s| serde_json::to_value(s).unwrap())
        .unwrap();
    app.services
        .definition_upsert_in_draft(
            &d.draft_id,
            "engineering",
            "linked_prs",
            src["definition_luau"].as_str().unwrap(),
            Some(&format!(
                "{}\n-- revised\n",
                src["runtime_luau"].as_str().unwrap()
            )),
            src["tests_luau"].as_str(),
            &admin,
        )
        .unwrap();
    let act = app
        .services
        .draft_activate_as(&d.draft_id, &admin)
        .await
        .unwrap();
    for id in ["open_prs", "pr_trend_adapter", "github_reader"] {
        assert!(
            act.carried_over_instances.contains(&id.to_string()),
            "{id}: {act:?}"
        );
    }
    for id in ["linked_prs", "pr_table_adapter"] {
        assert!(
            act.rebuilt_instances.contains(&id.to_string()),
            "{id}: {act:?}"
        );
        assert!(
            act.stale_instances.contains(&id.to_string()),
            "{id}: {act:?}"
        );
    }

    // VMs
    assert_eq!(engine.pool.vm_uid("open_prs").unwrap(), open_vm);
    assert_eq!(engine.pool.vm_uid("pr_trend_adapter").unwrap(), trend_vm);
    assert_ne!(engine.pool.vm_uid("linked_prs").unwrap(), linked_vm);
    // Cache: same entry, relabelled to the new Generation, served without recomputation.
    let entry_after = engine
        .queries
        .cached_entry(&qk)
        .expect("cache entry dropped");
    assert_eq!(entry_after.version, entry_before.version);
    assert_eq!(entry_after.value, entry_before.value);
    assert!(!entry_after.stale);
    assert_eq!(entry_after.generation_id, act.generation_id);
    let starts_mid = engine.pool.metrics().total_vm_starts;
    let env = engine
        .queries
        .query("open_prs", params.clone(), &admin, false)
        .await
        .unwrap();
    assert_eq!(
        env.version, entry_before.version,
        "served from carried-over cache"
    );
    assert_eq!(
        engine.queries.cached_entry(&qk).unwrap().hits,
        entry_after.hits + 1
    );
    assert_eq!(
        engine.pool.metrics().total_vm_starts,
        starts_mid,
        "no VM spawned for a cache hit"
    );
    // Only the rebuilt Instances got new VMs at activation.
    assert!(starts_mid - starts_before <= act.rebuilt_instances.len() as u64);
    // Subscription: still registered and still receives fresh values of the same query key.
    assert_eq!(engine.signals.subscriber_count(&qk), subs_before);
    engine
        .queries
        .query("open_prs", params.clone(), &admin, true)
        .await
        .unwrap();
    let pushed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match sub.recv().await {
                Ok(env) if env.version > entry_before.version => return env,
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(e) => panic!("subscription closed by activation: {e}"),
            }
        }
    })
    .await
    .expect("no update delivered to the pre-activation subscription");
    assert_eq!(pushed.query_key, qk);
    // Instance State carried over untouched.
    let state_after = engine
        .state
        .get("instance", "open_prs", "seen_pr_states:acme/platform")
        .unwrap()
        .unwrap();
    assert_eq!(state_after.value, state_before.value);
    // Downstream results of the changed Instance were marked stale, then recomputed.
    let table_render = engine
        .queries
        .render_dashboard("engineering", &admin)
        .await
        .unwrap();
    assert_eq!(table_render["generation"], act.generation_id);
}

#[tokio::test]
async fn ac13_stale_base_drafts_are_rejected_until_rebased() {
    let app = App::boot_mocked().await;
    let head0 = git(&app.ws, &["rev-parse", "HEAD"]);
    let begin = || async {
        app.post_ok("/api/drafts", json!({})).await["draft_id"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let (a, b, c) = (begin().await, begin().await, begin().await);
    // a and c both add a data provider (same Instance file); b adds an Adapter elsewhere.
    for (draft, name, value) in [(&a, "metric_a", 1), (&c, "metric_c", 3)] {
        app.post_ok(
            &format!("/api/drafts/{draft}/definitions"),
            json!({
                "package": "engineering", "name": name,
                "definition_luau": constant_provider_luau(&format!("engineering.{name}")),
                "runtime_luau": constant_runtime(value),
            }),
        )
        .await;
        app.post_ok(
            &format!("/api/drafts/{draft}/instances"),
            serde_json::to_value(data_provider(name, &format!("engineering.{name}"))).unwrap(),
        )
        .await;
    }
    app.post_ok(
        &format!("/api/drafts/{b}/definitions"),
        json!({
            "package": "engineering", "name": "b_badge",
            "definition_luau": badge_definition("engineering.b_badge"),
            "runtime_luau": badge_runtime("b badge"),
            "tests_luau": BADGE_TESTS,
        }),
    )
    .await;
    app.post_ok(
        &format!("/api/drafts/{b}/instances"),
        serde_json::to_value(adapter(
            "b_badge",
            "engineering.b_badge",
            &[("prs", "open_prs")],
        ))
        .unwrap(),
    )
    .await;
    // All pass Preview on their own base.
    for d in [&a, &b, &c] {
        let rep = app
            .post_ok(&format!("/api/drafts/{d}/preview"), json!({}))
            .await;
        assert_eq!(rep["all_passed"], true, "{rep}");
    }
    let act_a = app
        .post_ok(&format!("/api/drafts/{a}/activate"), json!({}))
        .await;
    let head_a = git(&app.ws, &["rev-parse", "HEAD"]);
    assert_eq!(act_a["revision_commit"], head_a.as_str());
    assert_ne!(head_a, head0);
    let gen_a = app.generation();

    // b and c still have the old base: rejected even though their cached reports passed.
    for d in [&b, &c, &b] {
        let (st, err) = app
            .post(&format!("/api/drafts/{d}/activate"), json!({}))
            .await;
        assert_eq!(st, 400, "{err}");
        assert!(
            err["error"].as_str().unwrap().contains("stale draft base"),
            "{err}"
        );
        assert_eq!(git(&app.ws, &["rev-parse", "HEAD"]), head_a);
        assert_eq!(app.generation(), gen_a);
        let g = app.services.graph();
        assert!(!g.instances.contains_key("b_badge") && !g.instances.contains_key("metric_c"));
        let (_, status) = app.get(&format!("/api/drafts/{d}")).await;
        assert_still_open(status["status"].as_str().unwrap());
        assert_eq!(status["base_revision"], head0.as_str());
    }

    // c conflicts textually with a: rebase refuses, leaves c untouched, and c stays rejected.
    let c_wt = PathBuf::from(app.services.draft_status(&c).unwrap().worktree_path);
    let c_data_before = std::fs::read_to_string(c_wt.join("instances/data.yaml")).unwrap();
    let (st, conflict) = app
        .post(&format!("/api/drafts/{c}/rebase"), json!({}))
        .await;
    assert_eq!(st, 409, "{conflict}");
    assert!(
        conflict["error"]
            .as_str()
            .unwrap()
            .contains("instances/data.yaml"),
        "{conflict}"
    );
    assert_eq!(
        std::fs::read_to_string(c_wt.join("instances/data.yaml")).unwrap(),
        c_data_before
    );
    assert_eq!(app.services.draft_status(&c).unwrap().base_revision, head0);
    let (st, _) = app
        .post(&format!("/api/drafts/{c}/activate"), json!({}))
        .await;
    assert_eq!(st, 400);
    assert_eq!(git(&app.ws, &["rev-parse", "HEAD"]), head_a);

    // b rebases cleanly, which invalidates its results, and then activates on top of a.
    let (st, rebased) = app
        .post(&format!("/api/drafts/{b}/rebase"), json!({}))
        .await;
    assert_eq!(st, 200, "{rebased}");
    assert_eq!(rebased["base_revision"], head_a.as_str());
    assert!(rebased["test_report"].is_null());
    let act_b = app
        .post_ok(&format!("/api/drafts/{b}/activate"), json!({}))
        .await;
    assert_eq!(git(&app.ws, &["rev-parse", "HEAD^"]), head_a);
    assert_eq!(
        act_b["revision_commit"],
        git(&app.ws, &["rev-parse", "HEAD"]).as_str()
    );
    let admin = Principal::administrator();
    let v = app
        .services
        .provider_query("metric_a", json!({}), &admin, false)
        .await
        .unwrap();
    assert_eq!(v.value.as_ref().unwrap()["value"], 1);
    assert!(app.services.graph().instances.contains_key("b_badge"));
    assert!(!app.services.graph().instances.contains_key("metric_c"));
}

// ---------------------------------------------------------------------------------------------
// AC-14: Git transaction
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ac14_exactly_one_commit_per_successful_draft_with_principal_attribution() {
    let app = App::boot_mocked().await;
    let admin = Principal::administrator();
    let count0 = commit_count(&app.ws);
    let head0 = git(&app.ws, &["rev-parse", "HEAD"]);

    // A draft with many edits across Definitions, Instances and Dashboards...
    let d = app.services.draft_begin(&admin).await.unwrap();
    for (name, v) in [("multi_a", 1), ("multi_b", 2)] {
        app.services
            .definition_upsert_in_draft(
                &d.draft_id,
                "engineering",
                name,
                &constant_provider_luau(&format!("engineering.{name}")),
                Some(&constant_runtime(v)),
                None,
                &admin,
            )
            .unwrap();
        app.services
            .instance_upsert_in_draft(
                &d.draft_id,
                &data_provider(name, &format!("engineering.{name}")),
                &admin,
            )
            .unwrap();
    }
    // ... including rewriting the same file twice.
    let mut doc = engineering_doc(&app.services);
    doc.title = "first".into();
    app.services
        .dashboard_update_in_draft(&d.draft_id, &doc, &admin)
        .unwrap();
    doc.title = "Engineering (renamed)".into();
    app.services
        .dashboard_update_in_draft(&d.draft_id, &doc, &admin)
        .unwrap();
    // A failed activation attempt in between commits nothing.
    app.services
        .instance_upsert_in_draft(
            &d.draft_id,
            &adapter("broken", "engineering.pr_trend_adapter", &[("prs", "nope")]),
            &admin,
        )
        .unwrap();
    assert!(app
        .services
        .draft_activate_as(&d.draft_id, &admin)
        .await
        .is_err());
    assert_eq!(commit_count(&app.ws), count0);
    app.services
        .instance_delete_in_draft(&d.draft_id, "broken", &admin)
        .unwrap();

    let act = app
        .services
        .draft_activate_as(&d.draft_id, &admin)
        .await
        .unwrap();
    assert_eq!(commit_count(&app.ws), count0 + 1);
    let head = git(&app.ws, &["rev-parse", "HEAD"]);
    assert_eq!(act.revision_commit, head);
    assert_eq!(git(&app.ws, &["rev-parse", "HEAD^"]), head0);
    let files = git(&app.ws, &["show", "--name-only", "--format=", "HEAD"]);
    for f in [
        "packages/engineering/multi_a/definition.luau",
        "packages/engineering/multi_a/runtime.luau",
        "packages/engineering/multi_b/definition.luau",
        "packages/engineering/multi_b/runtime.luau",
        "instances/data.yaml",
        "dashboards/engineering.yaml",
    ] {
        assert!(
            files.lines().any(|l| l == f),
            "{f} missing from the commit: {files}"
        );
    }
    assert!(!files.contains("broken"));
    assert!(git(&app.ws, &["show", "HEAD:dashboards/engineering.yaml"])
        .contains("Engineering (renamed)"));
    let msg = git(&app.ws, &["log", "-1", "--format=%B"]);
    assert!(
        msg.contains(&format!("Dash-Draft: {}", d.draft_id)),
        "{msg}"
    );
    assert!(msg.contains("Dash-Author: admin"), "{msg}");
    assert!(msg.contains("Dash-Principal: admin"), "{msg}");
    assert!(msg.contains("Dash-Principal-Kind: administrator"), "{msg}");
    assert_eq!(app.mgr().active_generation_revision(), head);
    assert_workspace_matches_head(&app.ws);

    // An Agent-authored draft activated by the Administrator records both.
    let (agent, _) = app
        .services
        .agent_create(
            "author-agent",
            CapabilityCeiling {
                allowed_providers: BTreeSet::from(["*".into()]),
                can_author_drafts: true,
                ..Default::default()
            },
            None,
            &admin,
        )
        .unwrap();
    let agent_p = Principal {
        id: agent.agent_id.clone(),
        kind: dash_core::PrincipalKind::Agent,
        ceiling: serde_json::from_value(agent.ceiling.clone()).unwrap(),
    };
    let d2 = app.services.draft_begin(&agent_p).await.unwrap();
    app.services
        .definition_upsert_in_draft(
            &d2.draft_id,
            "engineering",
            "agent_metric",
            &constant_provider_luau("engineering.agent_metric"),
            Some(&constant_runtime(9)),
            None,
            &agent_p,
        )
        .unwrap();
    assert!(
        app.services
            .draft_activate_as(&d2.draft_id, &agent_p)
            .await
            .is_err(),
        "agent without can_activate_drafts cannot activate"
    );
    app.services
        .draft_activate_as(&d2.draft_id, &admin)
        .await
        .unwrap();
    assert_eq!(commit_count(&app.ws), count0 + 2);
    let msg = git(&app.ws, &["log", "-1", "--format=%B"]);
    assert!(
        msg.contains(&format!("Dash-Author: {}", agent.agent_id)),
        "{msg}"
    );
    assert!(msg.contains("Dash-Principal: admin"), "{msg}");
    assert_eq!(
        git(&app.ws, &["log", "-1", "--format=%an|%cn"]),
        format!("{}|admin", agent.agent_id)
    );
}

#[tokio::test]
async fn ac14_push_failure_leaves_active_committed_unsynced_and_sync_retry_pushes() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    copy_fixture(&ws);
    let remote = BareRemote::create(tmp.path(), true);
    attach_remote(&ws, &remote);
    let app = App::boot_prepared(tmp, true).await;
    let admin = Principal::administrator();
    assert_eq!(
        app.services.git_status().await.unwrap().sync_status,
        "unsynced"
    );

    let d = app.services.draft_begin(&admin).await.unwrap();
    app.services
        .definition_upsert_in_draft(
            &d.draft_id,
            "engineering",
            "pushed_metric",
            &constant_provider_luau("engineering.pushed_metric"),
            Some(&constant_runtime(11)),
            None,
            &admin,
        )
        .unwrap();
    app.services
        .instance_upsert_in_draft(
            &d.draft_id,
            &data_provider("pushed_metric", "engineering.pushed_metric"),
            &admin,
        )
        .unwrap();
    let act = app
        .services
        .draft_activate_as(&d.draft_id, &admin)
        .await
        .expect("push failure must not fail the activation");
    // active
    assert_eq!(act.sync_status, "unsynced");
    assert!(
        act.sync_error
            .as_deref()
            .unwrap_or("")
            .contains("remote rejects pushes"),
        "{act:?}"
    );
    assert_eq!(app.generation(), act.generation_id);
    let v = app
        .services
        .provider_query("pushed_metric", json!({}), &admin, false)
        .await
        .unwrap();
    assert_eq!(v.value.as_ref().unwrap()["value"], 11);
    // committed
    let head = git(&app.ws, &["rev-parse", "HEAD"]);
    assert_eq!(act.revision_commit, head);
    assert_eq!(
        app.services.draft_status(&d.draft_id).unwrap().status,
        "activated"
    );
    // unsynced, never falsely reported
    assert_eq!(remote.main_commit(), None);
    let st = app.services.git_status().await.unwrap();
    assert_eq!(st.sync_status, "unsynced");
    assert!(st.last_error.is_some());
    // HTTP shows the same state
    let (_, http_status) = app.get("/api/git/status").await;
    assert_eq!(http_status["sync_status"], "unsynced");

    // Retry while the remote still refuses: still unsynced, still active.
    let retry = app.services.git_sync(&admin).await;
    match retry {
        Ok(rec) => assert_eq!(rec.sync_status, "unsynced"),
        Err(e) => assert!(e.contains("remote rejects pushes"), "{e}"),
    }
    assert_eq!(remote.main_commit(), None);

    // Restart: still active/committed/unsynced (re-derived, not trusted).
    let tmp = app.shutdown();
    let store = SqliteStore::open(&tmp.path().join("data")).unwrap();
    let mgr = DraftManager::initialize_with_mocks(
        tmp.path().join("ws"),
        tmp.path().join("data"),
        store,
        ResourceBudget::default(),
        true,
    )
    .await
    .unwrap();
    assert_eq!(mgr.active_generation_revision(), head);
    assert_eq!(mgr.git_status().await.unwrap().sync_status, "unsynced");
    assert!(mgr
        .engine()
        .graph
        .load()
        .instances
        .contains_key("pushed_metric"));

    // The remote recovers: sync retry pushes exactly the active Revision.
    remote.allow_pushes();
    let synced = mgr.git_sync(&admin).await.unwrap();
    assert_eq!(synced.sync_status, "synced");
    assert_eq!(remote.main_commit().as_deref(), Some(head.as_str()));
    let st = mgr.git_status().await.unwrap();
    assert_eq!(st.sync_status, "synced");
    assert_eq!(st.head_commit.as_deref(), Some(head.as_str()));
    assert_workspace_matches_head(mgr.workspace_dir());
    drop(tmp);
}

// ---- crash recovery at every activation journal stage ----

struct CrashOutcome {
    landed: bool,
}

/// Simulate a process crash at `point` during activation of a draft that adds a Definition, an
/// Instance and a Dashboard edit, restart on the same directories, and check that startup
/// reconciliation restores one coherent state: Workspace files == HEAD == the running
/// Generation's Revision, the journal is resolved, the draft status matches, and sync status is
/// re-derived. Rolled-back drafts remain usable and activate normally afterwards.
async fn crash_and_recover(point: &str, expect_landed: bool) -> CrashOutcome {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_fixture(&ws);
    let remote = BareRemote::create(tmp.path(), false);
    attach_remote(&ws, &remote);
    let admin = Principal::administrator();
    let open = |ws: PathBuf, data: PathBuf| async move {
        let store = SqliteStore::open(&data).unwrap();
        DraftManager::initialize_with_mocks(ws, data, store, ResourceBudget::default(), true)
            .await
            .unwrap()
    };

    let mgr = open(ws.clone(), data.clone()).await;
    let ws = mgr.workspace_dir().to_path_buf();
    assert_eq!(mgr.git_sync(&admin).await.unwrap().sync_status, "synced");
    let base = git(&ws, &["rev-parse", "HEAD"]);
    let base_count = commit_count(&ws);
    let gen_before = mgr.engine().graph.load().generation_id;

    let d = mgr.draft_begin(&admin).await.unwrap();
    mgr.draft_write_definition(
        &d.draft_id,
        "engineering",
        "crash_metric",
        &constant_provider_luau("engineering.crash_metric"),
        Some(&constant_runtime(42)),
        None,
    )
    .unwrap();
    mgr.draft_upsert_instance(
        &d.draft_id,
        &data_provider("crash_metric", "engineering.crash_metric"),
    )
    .unwrap();
    let mut doc = mgr.engine().graph.load().dashboards["engineering"].clone();
    doc.title = "Engineering after crash test".into();
    mgr.draft_update_dashboard(&d.draft_id, &doc).unwrap();

    mgr.set_crash_point_for_tests(Some(point));
    let err = mgr
        .draft_activate(&d.draft_id, &admin)
        .await
        .expect_err("crash point must interrupt activation")
        .to_string();
    assert!(
        err.contains(&format!("simulated crash at {point}")),
        "{err}"
    );
    // The process dies here: nothing else runs.
    drop(mgr);

    let mgr = open(ws.clone(), data.clone()).await;
    let store = mgr.store().clone();
    assert!(
        store
            .list_incomplete_activation_journals()
            .unwrap()
            .is_empty(),
        "{point}: journal left incomplete"
    );
    let head = git(&ws, &["rev-parse", "HEAD"]);
    let g = mgr.engine().graph.load_full();
    // Coherent: running Generation == HEAD == Workspace files.
    assert_eq!(g.revision_commit, head, "{point}");
    assert_eq!(mgr.active_generation_revision(), head, "{point}");
    assert_workspace_matches_head(&ws);
    assert!(
        g.generation_id > gen_before,
        "{point}: a fresh Generation was built at startup"
    );
    let landed = head != base;
    let status = mgr.draft_status(&d.draft_id).unwrap().status;
    if landed {
        assert_eq!(
            commit_count(&ws),
            base_count + 1,
            "{point}: exactly one commit"
        );
        assert_eq!(git(&ws, &["rev-parse", "HEAD^"]), base);
        assert!(g.instances.contains_key("crash_metric"), "{point}");
        assert_eq!(
            g.dashboards["engineering"].title,
            "Engineering after crash test"
        );
        assert!(ws
            .join("packages/engineering/crash_metric/runtime.luau")
            .exists());
        assert_eq!(status, "activated", "{point}");
        let v = mgr
            .engine()
            .queries
            .query("crash_metric", json!({}), &admin, false)
            .await
            .unwrap();
        assert_eq!(v.value.as_ref().unwrap()["value"], 42);
        // Never pushed before the crash: re-derived as unsynced, and retry works.
        let st = mgr.git_status().await.unwrap();
        assert_eq!(st.sync_status, "unsynced", "{point}");
        assert!(!st.head_diverged);
        assert_eq!(mgr.git_sync(&admin).await.unwrap().sync_status, "synced");
        assert_eq!(remote.main_commit().as_deref(), Some(head.as_str()));
    } else {
        assert_eq!(commit_count(&ws), base_count, "{point}");
        assert!(!g.instances.contains_key("crash_metric"), "{point}");
        assert!(!ws.join("packages/engineering/crash_metric").exists());
        assert_still_open(&status);
        assert_eq!(mgr.git_status().await.unwrap().sync_status, "synced");
        // The rolled-back draft is intact and can be activated normally.
        let act = mgr.draft_activate(&d.draft_id, &admin).await.unwrap();
        assert_eq!(act.sync_status, "synced");
        assert_eq!(
            commit_count(&ws),
            base_count + 1,
            "{point}: one commit after retry"
        );
        assert_eq!(git(&ws, &["rev-parse", "HEAD^"]), base);
        assert!(mgr
            .engine()
            .graph
            .load()
            .instances
            .contains_key("crash_metric"));
        assert_workspace_matches_head(&ws);
    }
    assert_eq!(landed, expect_landed, "{point}: landed={landed}");
    CrashOutcome { landed }
}

#[tokio::test]
async fn ac14_crash_before_commit_rolls_back() {
    assert!(
        !crash_and_recover("after_journal_started", false)
            .await
            .landed
    );
}

#[tokio::test]
async fn ac14_crash_after_commit_object_before_head_moved_rolls_back() {
    assert!(!crash_and_recover("after_commit_object", false).await.landed);
    assert!(
        !crash_and_recover("after_target_journalled", false)
            .await
            .landed
    );
}

#[tokio::test]
async fn ac14_crash_after_head_moved_before_worktree_switch_completes_activation() {
    assert!(crash_and_recover("after_head_moved", true).await.landed);
}

#[tokio::test]
async fn ac14_crash_after_commit_before_generation_swap_completes_activation() {
    assert!(crash_and_recover("after_committed", true).await.landed);
    assert!(
        crash_and_recover("after_generation_recorded", true)
            .await
            .landed
    );
}

#[tokio::test]
async fn ac14_crash_after_swap_before_push_restarts_unsynced_and_sync_retry_works() {
    assert!(crash_and_recover("after_swap", true).await.landed);
    assert!(crash_and_recover("before_push", true).await.landed);
}

#[test]
fn ac14_every_crash_point_is_covered() {
    let covered = [
        "after_journal_started",
        "after_commit_object",
        "after_target_journalled",
        "after_head_moved",
        "after_committed",
        "after_generation_recorded",
        "after_swap",
        "before_push",
    ];
    assert_eq!(dash_git::drafts::CRASH_POINTS, covered.as_slice());
}
