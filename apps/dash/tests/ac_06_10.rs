//! §17 acceptance criteria 6–10 against the reference application (§16), exercised end to end:
//! live connectors over the host HTTP broker against a local fake GitHub/Jira upstream (so every
//! upstream execution and side effect is counted outside the engine), real SQLite grants, Drafts,
//! restarts on the same data directory, and the services/HTTP entry points.
//!
//! - AC6: true concurrent singleflight, parameter independence, authorization partitions, and
//!   dynamic-dependency switches that stop propagation from the previous upstream.
//! - AC7: interval polling, on-demand refresh, simulated push, keyed delta reconciliation against
//!   the following snapshot, gap resync, and last good value retained after failure.
//! - AC8: durable replay across restarts without duplicate side effects, polling-detected merges
//!   emitting the same Event type as dashboard merges, history under its Service principal with
//!   retention, surviving restart.
//! - AC9: Instance/Dashboard State independence and persistence, stable widget_ids and layout
//!   overrides across refresh, reordering, new Generations and restarts.
//! - AC10: Dry run without side effects, persistent execution records, nested permission checks,
//!   invalidation, lost-response `unknown` reconciled by the host, audited manual resolution.
//! - Appendix A.3 / §10.3: stale-Generation and unbound-widget_id Requests are rejected; a
//!   connector without a Mock fails Preview with MISSING_MOCK.

use axum::extract::{Path as AxPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use dash_core::{
    apply_delta_envelope, CapabilityCeiling, EnvelopeKind, Principal, PrincipalKind, QueryEnvelope,
    QueryStatus,
};
use dash_git::{run_preview_and_scenarios, DraftManager};
use dash_luau::ResourceBudget;
use dash_server::AppServices;
use dash_store::SqliteStore;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;

// ---------------------------------------------------------------------------------------------
// Fake upstream (GitHub + Jira REST) with request counting, latency, failure and lost responses
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum MergeMode {
    /// Merge and answer 200.
    #[default]
    Ok,
    /// Merge, then answer 502: the response is lost after the side effect happened, and reads
    /// fail (500) until `fail_reads` is cleared.
    ApplyThenBadGateway,
    /// Answer 503 without merging: the outcome is unknown to the caller, nothing happened.
    Unavailable,
}

#[derive(Default)]
struct UpstreamState {
    prs: Vec<Value>,
    issues: Vec<Value>,
    /// Request counters, e.g. `list:acme/platform`, `get_pr:acme/platform#42`, `merge:...`.
    hits: BTreeMap<String, usize>,
    /// Requests to list PRs block until this is reopened (true concurrency for singleflight).
    hold_lists: bool,
    fail_reads: bool,
    merge_mode: MergeMode,
    merges: Vec<String>,
    transitions: Vec<(String, String)>,
}

#[derive(Clone)]
struct Upstream {
    addr: SocketAddr,
    state: Arc<Mutex<UpstreamState>>,
    released: Arc<tokio::sync::Notify>,
}

type Shared = (Arc<Mutex<UpstreamState>>, Arc<tokio::sync::Notify>);

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center")
}

fn fixture_json(rel: &str) -> Value {
    serde_json::from_slice(&std::fs::read(fixture_dir().join(rel)).unwrap()).unwrap()
}

impl Upstream {
    async fn start() -> Self {
        let state = Arc::new(Mutex::new(UpstreamState {
            prs: fixture_json("packages/github/mock/fixtures/prs.json")
                .as_array()
                .unwrap()
                .clone(),
            issues: fixture_json("packages/jira/mock/fixtures/issues.json")
                .as_array()
                .unwrap()
                .clone(),
            ..Default::default()
        }));
        let released = Arc::new(tokio::sync::Notify::new());
        let app = Router::new()
            .route("/repos/{owner}/{repo}/pulls", get(list_prs))
            .route("/repos/{owner}/{repo}/pulls/{num}", get(get_pr))
            .route("/repos/{owner}/{repo}/pulls/{num}/merge", put(merge_pr))
            .route("/rest/api/3/search", get(search_issues))
            .route("/rest/api/3/issue/{key}/transitions", post(transition))
            .with_state((state.clone(), released.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            addr,
            state,
            released,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn hits(&self, key: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .hits
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    fn with<R>(&self, f: impl FnOnce(&mut UpstreamState) -> R) -> R {
        f(&mut self.state.lock().unwrap())
    }

    fn hold_lists(&self) {
        self.with(|s| s.hold_lists = true);
    }

    fn release_lists(&self) {
        self.with(|s| s.hold_lists = false);
        self.released.notify_waiters();
    }

    /// A merge performed outside the dashboard (e.g. on github.com).
    fn merge_externally(&self, repo: &str, number: i64) {
        self.with(|s| {
            let pr = s
                .prs
                .iter_mut()
                .find(|p| p["repository"] == repo && p["number"] == number)
                .expect("PR in upstream fixture");
            pr["state"] = json!("merged");
            pr["merged"] = json!(true);
        });
    }

    fn pr_state(&self, repo: &str, number: i64) -> String {
        self.with(|s| {
            s.prs
                .iter()
                .find(|p| p["repository"] == repo && p["number"] == number)
                .unwrap()["state"]
                .as_str()
                .unwrap()
                .to_string()
        })
    }

    fn transitions(&self) -> Vec<(String, String)> {
        self.with(|s| s.transitions.clone())
    }

    fn merges(&self) -> Vec<String> {
        self.with(|s| s.merges.clone())
    }
}

fn bump(s: &mut UpstreamState, key: String) {
    *s.hits.entry(key).or_default() += 1;
}

async fn list_prs(
    State((state, released)): State<Shared>,
    AxPath((owner, repo)): AxPath<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let repo = format!("{owner}/{repo}");
    let wanted = q.get("state").cloned().unwrap_or_else(|| "open".into());
    state
        .lock()
        .unwrap()
        .hits
        .entry(format!("list:{repo}"))
        .and_modify(|n| *n += 1)
        .or_insert(1);
    loop {
        let notified = released.notified();
        if !state.lock().unwrap().hold_lists {
            break;
        }
        notified.await;
    }
    let s = state.lock().unwrap();
    if s.fail_reads {
        return (StatusCode::INTERNAL_SERVER_ERROR, "upstream down").into_response();
    }
    let rows: Vec<Value> = s
        .prs
        .iter()
        .filter(|p| {
            p["repository"] == repo.as_str() && (wanted == "all" || p["state"] == wanted.as_str())
        })
        .cloned()
        .collect();
    Json(rows).into_response()
}

async fn get_pr(
    State((state, _)): State<Shared>,
    AxPath((owner, repo, num)): AxPath<(String, String, i64)>,
) -> Response {
    let repo = format!("{owner}/{repo}");
    let mut s = state.lock().unwrap();
    bump(&mut s, format!("get_pr:{repo}#{num}"));
    if s.fail_reads {
        return (StatusCode::INTERNAL_SERVER_ERROR, "upstream down").into_response();
    }
    match s
        .prs
        .iter()
        .find(|p| p["repository"] == repo.as_str() && p["number"] == num)
    {
        Some(pr) => Json(pr.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, "no such PR").into_response(),
    }
}

async fn merge_pr(
    State((state, _)): State<Shared>,
    AxPath((owner, repo, num)): AxPath<(String, String, i64)>,
) -> Response {
    let repo = format!("{owner}/{repo}");
    let mut s = state.lock().unwrap();
    bump(&mut s, format!("merge:{repo}#{num}"));
    if s.merge_mode == MergeMode::Unavailable {
        return (StatusCode::SERVICE_UNAVAILABLE, "try later").into_response();
    }
    let Some(pr) = s
        .prs
        .iter_mut()
        .find(|p| p["repository"] == repo.as_str() && p["number"] == num)
    else {
        return (StatusCode::NOT_FOUND, "no such PR").into_response();
    };
    if pr["state"] != "open" {
        return (StatusCode::METHOD_NOT_ALLOWED, "not mergeable").into_response();
    }
    pr["state"] = json!("merged");
    pr["merged"] = json!(true);
    s.merges.push(format!("{repo}#{num}"));
    if s.merge_mode == MergeMode::ApplyThenBadGateway {
        // The gateway outage that lost the response also fails reads until it recovers.
        s.fail_reads = true;
        return (StatusCode::BAD_GATEWAY, "upstream timeout").into_response();
    }
    Json(json!({"merged": true, "sha": format!("sha{num}")})).into_response()
}

async fn search_issues(
    State((state, _)): State<Shared>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let jql = q.get("jql").cloned().unwrap_or_default();
    let keys: BTreeSet<String> = jql
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .map(|(inner, _)| inner.split(',').map(|k| k.trim().to_string()).collect())
        .unwrap_or_default();
    let mut s = state.lock().unwrap();
    bump(&mut s, "jira_search".into());
    let rows: Vec<Value> = s
        .issues
        .iter()
        .filter(|i| keys.contains(i["key"].as_str().unwrap_or_default()))
        .cloned()
        .collect();
    Json(rows).into_response()
}

async fn transition(
    State((state, _)): State<Shared>,
    AxPath(key): AxPath<String>,
    Json(body): Json<Value>,
) -> Response {
    let status = body["status"].as_str().unwrap_or("Done").to_string();
    let mut s = state.lock().unwrap();
    bump(&mut s, format!("transition:{key}"));
    s.transitions.push((key.clone(), status.clone()));
    if let Some(issue) = s.issues.iter_mut().find(|i| i["key"] == key.as_str()) {
        issue["status"] = json!(status.clone());
    }
    Json(json!({"key": key, "status": status, "transitioned": true})).into_response()
}

// ---------------------------------------------------------------------------------------------
// Reference app wired to the fake upstream (live Generation, SQLite authority)
// ---------------------------------------------------------------------------------------------

const PLATFORM: &str = "acme/platform";

fn open_params(repo: &str) -> Value {
    json!({"repository": repo, "state": "open"})
}

struct App {
    services: AppServices,
    ws: PathBuf,
    data: PathBuf,
}

/// Copy the reference app, point its connectors at `upstream`, and allow loopback egress.
fn prepare_workspace(ws: &Path, upstream: &Upstream) {
    dash_git::packages::copy_tree_excluding_git(&fixture_dir(), ws).unwrap();
    let conns = ws.join("instances/connectors.yaml");
    let text = std::fs::read_to_string(&conns)
        .unwrap()
        .replace("https://api.github.com", &upstream.base_url())
        .replace("https://jira.example.com", &upstream.base_url());
    std::fs::write(&conns, text).unwrap();
    let dash_toml = ws.join("dashboard.toml");
    let text = std::fs::read_to_string(&dash_toml).unwrap().replace(
        "allowed_hosts = [\"api.github.com\", \"jira.example.com\"]",
        "allowed_hosts = [\"127.0.0.1\"]\nallow_loopback_for_tests = true",
    );
    assert!(text.contains("allow_loopback_for_tests"));
    std::fs::write(&dash_toml, text).unwrap();
}

/// Administrator-side setup that lives in SQLite, outside the Workspace (§5.2, §11.3).
fn provision_store(store: &SqliteStore, grant_service_principals: bool) {
    store
        .set_secret(
            "github-work",
            "PAT",
            "ghp_t2_secret",
            &["github_reader".into(), "github_maintainer".into()],
        )
        .unwrap();
    store
        .set_secret(
            "jira-work",
            "PAT",
            "jira_t2_secret",
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
    if grant_service_principals {
        grant_work_completion(store);
        grant_history_sampler(store, &["open_prs"]);
    }
}

fn grant_work_completion(store: &SqliteStore) {
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
                allowed_secrets: BTreeSet::from(["jira-work".into()]),
                ..Default::default()
            })
            .unwrap(),
            "admin",
        )
        .unwrap();
}

fn grant_history_sampler(store: &SqliteStore, providers: &[&str]) {
    store
        .set_service_principal_grant(
            "pr_history_sampler",
            serde_json::to_value(CapabilityCeiling {
                allowed_providers: providers.iter().map(|s| s.to_string()).collect(),
                allowed_connectors: BTreeSet::from(["github_reader".into()]),
                allowed_capabilities: BTreeSet::from(["pull_requests.read".into()]),
                allowed_secrets: BTreeSet::from(["github-work".into()]),
                ..Default::default()
            })
            .unwrap(),
            "admin",
        )
        .unwrap();
}

impl App {
    async fn boot(tmp: &TempDir, upstream: &Upstream) -> Self {
        Self::boot_with(tmp, upstream, true, |_| {}).await
    }

    async fn boot_with(
        tmp: &TempDir,
        upstream: &Upstream,
        grant_service_principals: bool,
        customize: impl FnOnce(&Path),
    ) -> Self {
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        prepare_workspace(&ws, upstream);
        customize(&ws);
        let store = SqliteStore::open(&data).unwrap();
        provision_store(&store, grant_service_principals);
        Self::open(ws, data, store).await
    }

    async fn open(ws: PathBuf, data: PathBuf, store: SqliteStore) -> Self {
        let mgr =
            DraftManager::initialize(ws.clone(), data.clone(), store, ResourceBudget::default())
                .await
                .unwrap();
        let services = AppServices::new(mgr, vec![]);
        assert!(
            !services.graph().is_preview && !services.graph().substitute_mocks,
            "these tests run the live Generation"
        );
        Self { services, ws, data }
    }

    /// Process restart on the same Workspace and data directory (the old services are dropped).
    async fn restart(self) -> Self {
        let App { services, ws, data } = self;
        services
            .engine()
            .telemetry
            .flush_spans(Duration::from_secs(2));
        drop(services);
        let store = SqliteStore::open(&data).unwrap();
        Self::open(ws, data, store).await
    }

    fn s(&self) -> &AppServices {
        &self.services
    }
}

fn admin() -> Principal {
    Principal::administrator()
}

fn agent(id: &str, ceiling: CapabilityCeiling) -> Principal {
    Principal {
        id: id.into(),
        kind: PrincipalKind::Agent,
        ceiling,
    }
}

fn pr_reader_agent(id: &str) -> Principal {
    agent(
        id,
        CapabilityCeiling {
            allowed_providers: BTreeSet::from(["open_prs".into()]),
            allowed_connectors: BTreeSet::from(["github_reader".into()]),
            allowed_capabilities: BTreeSet::from(["pull_requests.read".into()]),
            allowed_secrets: BTreeSet::from(["github-work".into()]),
            ..Default::default()
        },
    )
}

fn rows(env: &QueryEnvelope) -> Vec<Value> {
    env.value
        .as_ref()
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
}

fn numbers(rows: &[Value]) -> Vec<i64> {
    let mut n: Vec<i64> = rows.iter().map(|r| r["number"].as_i64().unwrap()).collect();
    n.sort();
    n
}

/// Rows keyed by `id`, for order-insensitive comparison.
fn by_id(rows: &[Value]) -> BTreeMap<String, Value> {
    rows.iter()
        .map(|r| (r["id"].as_str().unwrap().to_string(), r.clone()))
        .collect()
}

async fn recv_within(
    rx: &mut tokio::sync::broadcast::Receiver<QueryEnvelope>,
    secs: u64,
) -> QueryEnvelope {
    tokio::time::timeout(Duration::from_secs(secs), rx.recv())
        .await
        .expect("timed out waiting for a query Signal")
        .expect("signal stream closed")
}

async fn wait_until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// =============================================================================================
// AC6 — reactive graph
// =============================================================================================

/// 100 truly concurrent identical queries (the upstream holds the first request open, so no
/// caller can be served from cache) share one provider execution and one upstream request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac06_concurrent_identical_queries_share_one_upstream_execution() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    up.hold_lists();

    let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..100 {
        let eng = app.s().engine().clone();
        let started = started.clone();
        tasks.push(tokio::spawn(async move {
            started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            eng.queries
                .query("open_prs", open_params(PLATFORM), &admin(), false)
                .await
        }));
    }
    // The leader's request reached the upstream and is held there; every other caller is
    // running concurrently against an empty cache.
    wait_until("leader request at the upstream", 10, || {
        up.hits("list:acme/platform") >= 1
            && started.load(std::sync::atomic::Ordering::SeqCst) == 100
    })
    .await;
    // Give non-deduplicated callers ample opportunity to reach the upstream themselves.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        up.hits("list:acme/platform"),
        1,
        "concurrent identical queries must join one in-flight execution"
    );
    up.release_lists();

    let mut keys = BTreeSet::new();
    let mut versions = BTreeSet::new();
    for t in tasks {
        let env = t.await.unwrap().unwrap();
        assert_eq!(env.status, QueryStatus::Fresh);
        assert_eq!(numbers(&rows(&env)), vec![42, 43, 44]);
        keys.insert(env.query_key.clone());
        versions.insert(env.version);
    }
    assert_eq!(keys.len(), 1, "one query key");
    assert_eq!(
        versions.len(),
        1,
        "every caller sees the same computed version"
    );
    assert_eq!(up.hits("list:acme/platform"), 1);
}

/// Differing params are independent computations; updating one never touches the other.
#[tokio::test]
async fn ac06_differing_params_are_independent() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let s = app.s();

    let platform = s
        .provider_query("open_prs", open_params(PLATFORM), &admin(), false)
        .await
        .unwrap();
    let payments = s
        .provider_query("open_prs", open_params("acme/payments"), &admin(), false)
        .await
        .unwrap();
    assert_ne!(platform.query_key, payments.query_key);
    assert_eq!(numbers(&rows(&platform)), vec![42, 43, 44]);
    assert_eq!(numbers(&rows(&payments)), vec![7]);
    assert_eq!(up.hits("list:acme/platform"), 1);
    assert_eq!(up.hits("list:acme/payments"), 1);

    let mut platform_rx = s.engine().signals.subscribe_stream(&platform.query_key);
    let mut payments_rx = s.engine().signals.subscribe_stream(&payments.query_key);
    up.merge_externally("acme/payments", 7);
    let refreshed = s
        .provider_query("open_prs", open_params("acme/payments"), &admin(), true)
        .await
        .unwrap();
    assert!(
        rows(&refreshed).is_empty(),
        "{refreshed:?} hits={:?}",
        up.with(|s| s.hits.clone())
    );
    let sig = recv_within(&mut payments_rx, 5).await;
    assert_eq!(sig.query_key, payments.query_key);

    // The other parameter set was neither re-executed nor signalled.
    assert_eq!(up.hits("list:acme/platform"), 1);
    assert!(matches!(
        platform_rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let cached = s
        .engine()
        .queries
        .cached_entry(&platform.query_key)
        .unwrap();
    assert_eq!(cached.version, platform.version);
    assert_eq!(numbers(cached.value.as_array().unwrap()), vec![42, 43, 44]);
}

/// Identical params under different authorization never share a computation, a cache entry or a
/// Signal (§6.1 authorization-partitioned query keys).
#[tokio::test]
async fn ac06_authorization_partitions_are_isolated() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let s = app.s();
    let reader = pr_reader_agent("agt_reader");

    let as_admin = s
        .provider_query("open_prs", open_params(PLATFORM), &admin(), false)
        .await
        .unwrap();
    assert_eq!(up.hits("list:acme/platform"), 1);
    let as_agent = s
        .provider_query("open_prs", open_params(PLATFORM), &reader, false)
        .await
        .unwrap();
    assert_ne!(as_admin.query_key, as_agent.query_key);
    assert_eq!(
        up.hits("list:acme/platform"),
        2,
        "a second Principal gets its own computation, not the Administrator's cached result"
    );
    assert_eq!(numbers(&rows(&as_agent)), vec![42, 43, 44]);
    let agent_entry = s
        .engine()
        .queries
        .cached_entry(&as_agent.query_key)
        .unwrap();
    assert_eq!(agent_entry.principal.id, "agt_reader");

    // A Principal without access is denied before anything runs.
    let outsider = agent("agt_outsider", CapabilityCeiling::default());
    assert!(s
        .provider_query("open_prs", open_params(PLATFORM), &outsider, false)
        .await
        .is_err());
    assert_eq!(up.hits("list:acme/platform"), 2);

    // An update in the Administrator's partition is invisible in the Agent's.
    let mut agent_rx = s.engine().signals.subscribe_stream(&as_agent.query_key);
    s.engine()
        .queries
        .push_query_value(
            "open_prs",
            &open_params(PLATFORM),
            &admin(),
            json!([{"id": "acme/platform#99", "number": 99, "title": "admin only",
                    "repository": PLATFORM, "state": "open"}]),
        )
        .unwrap();
    assert!(matches!(
        agent_rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let agent_entry = s
        .engine()
        .queries
        .cached_entry(&as_agent.query_key)
        .unwrap();
    assert_eq!(
        numbers(agent_entry.value.as_array().unwrap()),
        vec![42, 43, 44]
    );
    let again = s
        .provider_query("open_prs", open_params(PLATFORM), &reader, false)
        .await
        .unwrap();
    assert_eq!(numbers(&rows(&again)), vec![42, 43, 44]);
}

/// Add a local Package (`probe`) and Instances to a Workspace before its first commit.
fn add_package(ws: &Path, defs: &[(&str, &str, &str)]) {
    let pkg = ws.join("packages/probe");
    std::fs::create_dir_all(&pkg).unwrap();
    std::fs::write(
        pkg.join("package.toml"),
        "name = \"probe\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    for (name, decl, rt) in defs {
        std::fs::create_dir_all(pkg.join(name)).unwrap();
        std::fs::write(pkg.join(name).join("definition.luau"), decl).unwrap();
        std::fs::write(pkg.join(name).join("runtime.luau"), rt).unwrap();
    }
    let toml = ws.join("packages.toml");
    let text = std::fs::read_to_string(&toml)
        .unwrap()
        .replace("\"engineering\"]", "\"engineering\", \"probe\"]");
    assert!(text.contains("\"probe\""));
    std::fs::write(&toml, text).unwrap();
}

/// Append Instance entries (already indented under the category key) to `instances/<file>`.
fn append_instances(ws: &Path, file: &str, yaml: &str) {
    let p = ws.join("instances").join(file);
    let mut text = std::fs::read_to_string(&p).unwrap();
    if !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(yaml);
    std::fs::write(&p, text).unwrap();
}

const SELECTOR_DECL: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "probe.selector",
    runtime = "runtime.luau",
    handler = "query",
    output = dash.object { branch = dash.string() },
    updates = { on_demand = true, subscribe = true },
}"#;
const SELECTOR_RT: &str =
    "local R = {}\nfunction R.query(ctx) return { branch = \"a\" } end\nreturn R";
const SOURCE_DECL: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "probe.source",
    runtime = "runtime.luau",
    handler = "query",
    config = dash.object { label = dash.string() },
    output = dash.list(dash.object { id = dash.string(), v = dash.integer() }),
    updates = { on_demand = true, subscribe = true },
}"#;
const SOURCE_RT: &str =
    "local R = {}\nfunction R.query(ctx) return { { id = ctx.config.label, v = 0 } } end\nreturn R";
const ROUTER_DECL: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "probe.router",
    runtime = "runtime.luau",
    handler = "query",
    inputs = {
        sel = dash.data_ref("probe.selector"),
        a = dash.data_ref("probe.source"),
        b = dash.data_ref("probe.source"),
    },
    output = dash.list(dash.object { id = dash.string(), v = dash.integer() }),
    updates = { on_demand = true, subscribe = true },
}"#;
const ROUTER_RT: &str = r#"local R = {}
function R.query(ctx)
    local s = ctx:await(ctx.inputs.sel:query({}))
    if s.branch == "a" then
        return ctx:await(ctx.inputs.a:query({}))
    end
    return ctx:await(ctx.inputs.b:query({}))
end
return R"#;

fn add_router_graph(ws: &Path) {
    add_package(
        ws,
        &[
            ("selector", SELECTOR_DECL, SELECTOR_RT),
            ("source", SOURCE_DECL, SOURCE_RT),
            ("router", ROUTER_DECL, ROUTER_RT),
        ],
    );
    append_instances(
        ws,
        "data.yaml",
        "  sel:\n    definition: probe.selector\n\
         \x20 src_a:\n    definition: probe.source\n    config:\n      label: A\n\
         \x20 src_b:\n    definition: probe.source\n    config:\n      label: B\n\
         \x20 router:\n    definition: probe.router\n    inputs:\n      sel: sel\n      a: src_a\n      b: src_b\n",
    );
}

fn query_spans(app: &App, instance: &str, query_key: &str) -> usize {
    app.s()
        .engine()
        .telemetry
        .recent_spans(5000)
        .iter()
        .filter(|s| s.name == format!("query:{instance}") && s.attributes["query_key"] == query_key)
        .count()
}

/// A dynamic dependency switch moves the subscription: after the router stops reading `src_a`,
/// changes to `src_a` neither recompute nor signal the router, while `src_b` changes do.
#[tokio::test]
async fn ac06_dynamic_dependency_switch_releases_the_previous_upstream() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot_with(&tmp, &up, true, add_router_graph).await;
    let s = app.s();
    let q = &s.engine().queries;
    let key = |inst: &str| q.resolve_query_key(inst, &json!({}), &admin()).unwrap();
    let (router, sel, src_a, src_b) = (key("router"), key("sel"), key("src_a"), key("src_b"));

    // Subscribe first, as a live widget does, then compute.
    let mut rx = s.engine().signals.subscribe_stream(&router);
    let first = s
        .provider_query("router", json!({}), &admin(), false)
        .await
        .unwrap();
    assert_eq!(first.query_key, router);
    assert_eq!(rows(&first), vec![json!({"id": "A", "v": 0})]);
    let deps = q.recorded_dependencies(&router);
    assert!(deps.contains(&sel) && deps.contains(&src_a) && !deps.contains(&src_b));
    // The first computation's `loading` and snapshot Signals.
    let mut local = Vec::new();
    let mut ver = 0;
    drain_ready(&mut rx, &mut ver, &mut local);
    assert_eq!((ver, &local), (first.version, &rows(&first)));

    // Positive control: while routed to A, an A change recomputes and signals the router.
    q.push_query_value("src_a", &json!({}), &admin(), json!([{"id": "A", "v": 1}]))
        .unwrap();
    sync_until(&mut rx, &mut ver, &mut local, &[json!({"id": "A", "v": 1})]).await;

    // Switch the branch.
    q.push_query_value("sel", &json!({}), &admin(), json!({"branch": "b"}))
        .unwrap();
    sync_until(&mut rx, &mut ver, &mut local, &[json!({"id": "B", "v": 0})]).await;
    let deps = q.recorded_dependencies(&router);
    assert!(deps.contains(&src_b) && !deps.contains(&src_a), "{deps:?}");
    assert!(!q.reverse_subscribers(&src_a).contains(&router));
    assert!(q.reverse_subscribers(&src_b).contains(&router));

    // The old upstream no longer drives the router: no recompute, no Signal.
    tokio::time::sleep(Duration::from_millis(100)).await;
    drain_ready(&mut rx, &mut ver, &mut local);
    let runs_before = query_spans(&app, "router", &router);
    q.push_query_value("src_a", &json!({}), &admin(), json!([{"id": "A", "v": 2}]))
        .unwrap();
    // No positive signal exists for "nothing happened": allow far more than the recompute
    // debounce for a leaked edge to fire.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(query_spans(&app, "router", &router), runs_before);
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    // The new upstream does.
    q.push_query_value("src_b", &json!({}), &admin(), json!([{"id": "B", "v": 9}]))
        .unwrap();
    sync_until(&mut rx, &mut ver, &mut local, &[json!({"id": "B", "v": 9})]).await;
    assert_eq!(query_spans(&app, "router", &router), runs_before + 1);
}

/// Apply Signals until the replica equals `expected` (each must apply cleanly).
async fn sync_until(
    rx: &mut tokio::sync::broadcast::Receiver<QueryEnvelope>,
    ver: &mut u64,
    local: &mut Vec<Value>,
    expected: &[Value],
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while local.as_slice() != expected {
        let left = deadline.saturating_duration_since(Instant::now());
        let env = tokio::time::timeout(left, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("replica {local:?} never reached {expected:?}"))
            .expect("signal stream closed");
        apply_envelope(ver, local, &env);
    }
}

/// Apply everything already delivered on `rx` to the replica.
fn drain_ready(
    rx: &mut tokio::sync::broadcast::Receiver<QueryEnvelope>,
    ver: &mut u64,
    local: &mut Vec<Value>,
) {
    while let Ok(env) = rx.try_recv() {
        apply_envelope(ver, local, &env);
    }
}

/// Apply a snapshot or keyed delta to a local replica, as a client does. `loading` and
/// value-less `error` envelopes carry no data and leave the replica as it is.
fn apply_envelope(ver: &mut u64, local: &mut Vec<Value>, env: &QueryEnvelope) {
    if env.kind == EnvelopeKind::Snapshot && env.value.as_ref().is_none_or(|v| v.is_null()) {
        return;
    }
    match env.kind {
        EnvelopeKind::Snapshot => {
            *local = rows(env);
            *ver = env.version;
        }
        EnvelopeKind::Delta => {
            apply_delta_envelope(ver, local, env, env.row_key.as_deref().unwrap_or("id"))
                .expect("delta applies onto the replica's version");
        }
    }
}

/// §16 behaviour 5: the Table and Trend Adapters share one `open_prs(acme/platform, open)`
/// execution (the Table through `linked_prs`), and both render from it.
#[tokio::test]
async fn ac06_table_and_trend_adapters_share_one_open_prs_execution() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let out = app
        .s()
        .dashboard_get("engineering", &admin())
        .await
        .unwrap();
    assert_eq!(
        up.hits("list:acme/platform"),
        1,
        "one upstream request across both Adapters"
    );
    let shared = app
        .s()
        .engine()
        .queries
        .resolve_query_key("open_prs", &open_params(PLATFORM), &admin())
        .unwrap();
    assert_eq!(query_spans(&app, "open_prs", &shared), 1);
    assert_eq!(
        app.s().engine().queries.reverse_subscribers(&shared).len(),
        2,
        "linked_prs and the Trend Adapter render both depend on the one execution"
    );
    let rendered = serde_json::to_string(&out["rendered"]).unwrap();
    assert!(rendered.contains("Open PRs (acme/platform)"));
    let w = widgets(&out["rendered"]);
    assert_eq!(w["pr_trend_adapter_open_count"]["props"]["value"], 3);
    assert_eq!(
        w["pr_table_adapter_pr_table"]["props"]["rows"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
}

/// widget_id -> node, over every Adapter's rendered tree.
fn widgets(rendered: &Value) -> BTreeMap<String, Value> {
    fn walk(v: &Value, out: &mut BTreeMap<String, Value>) {
        match v {
            Value::Object(o) => {
                if let Some(id) = o.get("widget_id").and_then(|x| x.as_str()) {
                    out.insert(id.to_string(), v.clone());
                }
                for c in o.values() {
                    walk(c, out);
                }
            }
            Value::Array(a) => a.iter().for_each(|c| walk(c, out)),
            _ => {}
        }
    }
    let mut out = BTreeMap::new();
    walk(rendered, &mut out);
    out
}

// =============================================================================================
// AC7 — updates
// =============================================================================================

/// Subscribe to `open_prs(params)` under the Administrator before computing it, then sync a
/// local replica to the first snapshot. Returns (receiver, query_key, version, rows).
async fn subscribed_replica(
    app: &App,
    params: Value,
) -> (
    tokio::sync::broadcast::Receiver<QueryEnvelope>,
    String,
    u64,
    Vec<Value>,
) {
    let s = app.s();
    let key = s
        .engine()
        .queries
        .resolve_query_key("open_prs", &params, &admin())
        .unwrap();
    let mut rx = s.engine().signals.subscribe_stream(&key);
    let env = s
        .provider_query("open_prs", params, &admin(), false)
        .await
        .unwrap();
    let (mut ver, mut local) = (0, Vec::new());
    drain_ready(&mut rx, &mut ver, &mut local);
    assert_eq!((ver, &local), (env.version, &rows(&env)));
    (rx, key, ver, local)
}

/// Scheduled polling (no browser, no foreground query) detects upstream changes once per
/// `poll_interval_secs` and delivers them to the subscriber, whose replica reconciles to the
/// engine's value.
#[tokio::test]
async fn ac07_polling_on_interval_delivers_upstream_changes() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let s = app.s();
    let (mut rx, key, mut ver, mut local) = subscribed_replica(&app, open_params(PLATFORM)).await;
    assert_eq!(numbers(&local), vec![42, 43, 44]);
    let base_hits = up.hits("list:acme/platform");

    let t0 = chrono::Utc::now().timestamp();
    up.merge_externally(PLATFORM, 42);
    assert!(s.run_polls_at(t0).await >= 1);
    assert!(up.hits("list:acme/platform") > base_hits);
    sync_until_numbers(&mut rx, &mut ver, &mut local, &[43, 44]).await;

    // Within the interval nothing is fetched again, even though the upstream changed.
    up.merge_externally(PLATFORM, 43);
    let hits = up.hits("list:acme/platform");
    assert_eq!(s.run_polls_at(t0 + 30).await, 0);
    assert_eq!(up.hits("list:acme/platform"), hits);
    // After poll_interval_secs (60) the next poll delivers it.
    assert!(s.run_polls_at(t0 + 61).await >= 1);
    sync_until_numbers(&mut rx, &mut ver, &mut local, &[44]).await;

    let cached = s.engine().queries.cached_entry(&key).unwrap();
    assert_eq!(cached.version, ver, "replica is at the engine's version");
    assert_eq!(by_id(&local), by_id(cached.value.as_array().unwrap()));
}

async fn sync_until_numbers(
    rx: &mut tokio::sync::broadcast::Receiver<QueryEnvelope>,
    ver: &mut u64,
    local: &mut Vec<Value>,
    expected: &[i64],
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while numbers(local) != expected {
        let left = deadline.saturating_duration_since(Instant::now());
        let env = tokio::time::timeout(left, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("replica {:?} never reached {expected:?}", numbers(local)))
            .expect("signal stream closed");
        apply_envelope(ver, local, &env);
    }
}

/// On-demand refresh re-executes against the upstream and signals subscribers; a plain read is
/// served from cache.
#[tokio::test]
async fn ac07_on_demand_refresh() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let s = app.s();
    let (mut rx, _key, mut ver, mut local) = subscribed_replica(&app, open_params(PLATFORM)).await;
    up.merge_externally(PLATFORM, 44);

    let cached = s
        .provider_query("open_prs", open_params(PLATFORM), &admin(), false)
        .await
        .unwrap();
    assert_eq!(numbers(&rows(&cached)), vec![42, 43, 44], "cached read");
    assert_eq!(up.hits("list:acme/platform"), 1);

    let refreshed = s
        .provider_query("open_prs", open_params(PLATFORM), &admin(), true)
        .await
        .unwrap();
    assert_eq!(up.hits("list:acme/platform"), 2);
    assert_eq!(numbers(&rows(&refreshed)), vec![42, 43]);
    assert!(refreshed.version > cached.version);
    sync_until_numbers(&mut rx, &mut ver, &mut local, &[42, 43]).await;
    assert_eq!(ver, refreshed.version);
}

/// A simulated push produces a keyed delta whose application reproduces the snapshot that
/// follows it exactly; a dropped delta is detected as a gap and the replica resyncs from the
/// engine's latest snapshot, after which deltas apply again.
#[tokio::test]
async fn ac07_push_deltas_reconcile_with_snapshots_and_gaps_resync() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let s = app.s();
    let q = &s.engine().queries;
    let (mut rx, key, mut ver, mut local) = subscribed_replica(&app, open_params(PLATFORM)).await;

    // Update #43's title, drop #44, insert #45 (between rows, to check positions).
    let mut next = local.clone();
    next[1]["title"] = json!("DEV-124 Add delta resync on sequence gap (rebased)");
    next.remove(2);
    next.insert(
        1,
        json!({"id": "acme/platform#45", "number": 45, "title": "DEV-200 New",
               "repository": PLATFORM, "author": "erin", "state": "open"}),
    );
    q.push_query_value(
        "open_prs",
        &open_params(PLATFORM),
        &admin(),
        json!(next.clone()),
    )
    .unwrap();
    let delta = recv_within(&mut rx, 5).await;
    assert_eq!(delta.kind, EnvelopeKind::Delta);
    assert_eq!(delta.base_version, Some(ver));
    let ops = delta.operations.as_ref().unwrap();
    assert!(ops.len() >= 3, "keyed ops, not a full replacement: {ops:?}");
    apply_envelope(&mut ver, &mut local, &delta);
    let snap = recv_within(&mut rx, 5).await;
    assert_eq!(snap.kind, EnvelopeKind::Snapshot);
    assert_eq!(snap.version, ver);
    assert_eq!(
        local,
        rows(&snap),
        "delta-applied rows equal the following snapshot"
    );
    assert_eq!(local, next);

    // Two more pushes; the replica loses the first one's frames.
    let mut second = next.clone();
    second.remove(0);
    q.push_query_value(
        "open_prs",
        &open_params(PLATFORM),
        &admin(),
        json!(second.clone()),
    )
    .unwrap();
    let mut third = second.clone();
    third[0]["title"] = json!("DEV-200 New (edited)");
    q.push_query_value(
        "open_prs",
        &open_params(PLATFORM),
        &admin(),
        json!(third.clone()),
    )
    .unwrap();
    let lost_delta = recv_within(&mut rx, 5).await;
    let _lost_snapshot = recv_within(&mut rx, 5).await;
    assert_eq!(lost_delta.kind, EnvelopeKind::Delta);
    let gap_delta = recv_within(&mut rx, 5).await;
    assert_eq!(gap_delta.kind, EnvelopeKind::Delta);
    let before = (ver, local.clone());
    assert!(
        apply_delta_envelope(&mut ver, &mut local, &gap_delta, "id").is_err(),
        "a delta whose base_version is not the replica's version is a gap"
    );
    assert_eq!(
        (ver, local.clone()),
        before,
        "a rejected delta leaves the replica untouched"
    );

    // Resync from the engine's latest materialized snapshot.
    let resync = s
        .engine()
        .signals
        .latest_snapshot(&key)
        .expect("resync snapshot");
    assert_eq!(resync.version, gap_delta.version);
    apply_envelope(&mut ver, &mut local, &resync);
    assert_eq!(local, third);
    drain_ready(&mut rx, &mut ver, &mut local);

    // Subsequent deltas apply onto the resynced replica.
    let mut fourth = third.clone();
    fourth.pop();
    q.push_query_value(
        "open_prs",
        &open_params(PLATFORM),
        &admin(),
        json!(fourth.clone()),
    )
    .unwrap();
    let d = recv_within(&mut rx, 5).await;
    assert_eq!(d.kind, EnvelopeKind::Delta);
    apply_envelope(&mut ver, &mut local, &d);
    assert_eq!(local, fourth);
    let cached = q.cached_entry(&key).unwrap();
    assert_eq!((cached.version, cached.value), (ver, json!(fourth)));
}

/// When the upstream fails, the last good value stays visible as `stale` with its original
/// update time and retry metadata; the background retry restores `fresh` once it recovers.
#[tokio::test]
async fn ac07_upstream_failure_keeps_last_good_value_visible_as_stale() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let s = app.s();
    let (mut rx, key, _ver, good) = subscribed_replica(&app, open_params(PLATFORM)).await;
    let good_at = s
        .engine()
        .queries
        .cached_entry(&key)
        .unwrap()
        .source_updated_at;

    up.with(|u| u.fail_reads = true);
    let stale = s
        .provider_query("open_prs", open_params(PLATFORM), &admin(), true)
        .await
        .unwrap();
    assert_eq!(stale.status, QueryStatus::Stale);
    assert_eq!(rows(&stale), good, "last good rows remain visible");
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(&stale.updated_at).unwrap(),
        good_at,
        "updated_at is the last successful update"
    );
    assert!(
        stale.error.as_deref().unwrap_or("").contains("HTTP 500"),
        "{stale:?}"
    );
    assert!(stale.retry.as_ref().is_some_and(|r| r.attempt >= 1));
    // Subscribers see the same stale value.
    let sig = loop {
        let e = recv_within(&mut rx, 5).await;
        if e.status == QueryStatus::Stale {
            break e;
        }
    };
    assert_eq!(rows(&sig), good);

    // Recovery: the engine retries subscribed failing queries in the background.
    up.merge_externally(PLATFORM, 42);
    up.with(|u| u.fail_reads = false);
    let fresh = loop {
        let e = recv_within(&mut rx, 10).await;
        if e.status == QueryStatus::Fresh && e.kind == EnvelopeKind::Snapshot {
            break e;
        }
    };
    assert_eq!(numbers(&rows(&fresh)), vec![43, 44]);
}

// =============================================================================================
// AC8 — Events, automations and history
// =============================================================================================

fn merged_events(app: &App, source: &str) -> Vec<dash_core::EventEnvelope> {
    app.s()
        .engine()
        .events
        .replay_since(source, 0)
        .unwrap()
        .into_iter()
        .filter(|e| e.semantic_type == "github.pr_merged")
        .collect()
}

fn executions(app: &App, action: &str) -> Vec<dash_store::ActionExecutionRecord> {
    app.s()
        .store()
        .list_action_executions(1000)
        .unwrap()
        .into_iter()
        .filter(|r| r.action_instance == action)
        .collect()
}

/// A merge detected by background polling is a durable Event: delivered from the log after a
/// restart that happened before any dispatch, it transitions the Jira issue exactly once, and
/// later restarts and redeliveries never repeat the side effect.
#[tokio::test]
async fn ac08_durable_event_survives_restart_and_never_duplicates_side_effects() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let mut app = App::boot(&tmp, &up).await;

    // The keep-warm poll (history series params, under pr_history_sampler) records PR states.
    let t0 = chrono::Utc::now().timestamp();
    assert!(app.s().run_polls_at(t0).await >= 1);
    assert!(merged_events(&app, "open_prs").is_empty());
    up.merge_externally(PLATFORM, 42);
    assert!(app.s().run_polls_at(t0 + 61).await >= 1);
    let evs = merged_events(&app, "open_prs");
    assert_eq!(evs.len(), 1);
    assert!(evs[0].durable);
    assert_eq!(evs[0].payload["source"], "polling");
    assert_eq!(evs[0].payload["issue_key"], "DEV-123");
    assert!(
        up.transitions().is_empty(),
        "nothing dispatched before the crash"
    );

    // Crash before delivery.
    app = app.restart().await;
    assert_eq!(
        merged_events(&app, "open_prs").len(),
        1,
        "the Event is durable"
    );
    let report = app.s().drain_durable_events().await;
    assert!(report.succeeded >= 1, "{report:?}");
    assert_eq!(up.transitions(), vec![("DEV-123".into(), "Done".into())]);
    let runs = executions(&app, "transition_issue");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, "succeeded");
    assert_eq!(runs[0].principal_id, "work_completion");
    assert_eq!(runs[0].principal_kind, "service_principal");

    // Redelivery from the checkpoint, and again after another restart: no new side effect.
    app.s().drain_durable_events().await;
    app = app.restart().await;
    app.s().drain_durable_events().await;
    // The background consumer's startup catch-up is the same delivery path.
    let workers = app.s().spawn_background_workers();
    tokio::time::sleep(Duration::from_millis(300)).await;
    workers.abort();
    assert_eq!(up.transitions().len(), 1, "exactly one Jira transition");
    assert_eq!(executions(&app, "transition_issue").len(), 1);
    assert!(app
        .s()
        .automation_executions(Some("complete_work"), 10)
        .unwrap()
        .iter()
        .all(|r| r.status == "succeeded"));
}

/// A merge made from the dashboard (widget Request -> merge_pr) and a merge detected by polling
/// emit the same Event type and contract; each logical merge transitions its issue once.
#[tokio::test]
async fn ac08_dashboard_and_polling_merges_emit_the_same_event_type() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let s = app.s();
    let t0 = chrono::Utc::now().timestamp();
    s.run_polls_at(t0).await;

    // Dashboard merge of #42 through the Table widget's Request.
    s.dashboard_get("engineering", &admin()).await.unwrap();
    let merge = confirmed(
        s,
        widget_request(
            s,
            "pr_table_adapter",
            "pr_table_adapter_pr_table",
            "merge_requested",
            json!({"repository": PLATFORM, "number": 42, "jira_key": "DEV-123"}),
        ),
    )
    .await;
    let rec = s.dispatch_widget_request(merge, &admin()).await.unwrap();
    assert_eq!(rec.status, "succeeded", "{rec:?}");
    assert_eq!(up.merges(), vec!["acme/platform#42".to_string()]);

    // External merge of #43, detected by the next scheduled poll.
    up.merge_externally(PLATFORM, 43);
    s.run_polls_at(t0 + 61).await;

    let from_dashboard = merged_events(&app, "merge_pr");
    let from_polling = merged_events(&app, "open_prs");
    assert_eq!(from_dashboard.len(), 1);
    let polled_43 = from_polling
        .iter()
        .find(|e| e.payload["number"] == 43)
        .expect("polling detects the external merge");
    let d = &from_dashboard[0];
    assert_eq!(d.payload["source"], "action");
    assert_eq!(polled_43.payload["source"], "polling");
    assert_eq!(d.semantic_type, polled_43.semantic_type);
    assert_eq!(d.schema_version, polled_43.schema_version);
    assert_eq!(d.durable, polled_43.durable);
    let fields = |e: &dash_core::EventEnvelope| {
        e.payload
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(fields(d), fields(polled_43), "same payload contract");

    s.drain_durable_events().await;
    let mut transitioned = up.transitions();
    transitioned.sort();
    assert_eq!(
        transitioned,
        vec![
            ("DEV-123".to_string(), "Done".to_string()),
            ("DEV-124".to_string(), "Done".to_string())
        ],
        "one transition per logical merge, whichever path observed it"
    );
}

fn history_values(samples: &Value) -> Vec<f64> {
    samples
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["value_num"].as_f64().unwrap())
        .collect()
}

/// History is sampled by the background worker from the Workspace's configured series, under
/// its Service principal's Administrator grant (not the Administrator, not the YAML ceiling);
/// samples older than `retention_days` are compacted; history survives a restart.
#[tokio::test]
async fn ac08_history_sampled_under_service_principal_with_retention_and_restart() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let mut app = App::boot_with(&tmp, &up, false, |_| {}).await;
    let series = app
        .s()
        .graph()
        .workspace
        .history
        .iter()
        .find(|h| h.series_id == "open_prs_30d")
        .cloned()
        .expect("series configured in dashboard.toml");
    assert_eq!(series.service_principal, "pr_history_sampler");
    let history = app.s().engine().history.clone();

    // The YAML ceiling alone grants nothing.
    let err = history.sample_series(&series, None).await.unwrap_err();
    assert!(err.contains("no Administrator grant"), "{err}");
    // A grant that excludes the source provider is enforced.
    grant_history_sampler(app.s().store(), &[]);
    assert!(history.sample_series(&series, None).await.is_err());
    assert_eq!(
        up.hits("list:acme/platform"),
        0,
        "denied before any upstream call"
    );
    assert!(history
        .query_samples("open_prs_30d", None, 100)
        .unwrap()
        .is_empty());

    grant_history_sampler(app.s().store(), &["open_prs"]);
    let store = app.s().store().clone();
    let old = (chrono::Utc::now() - chrono::Duration::days(31)).to_rfc3339();
    let recent = (chrono::Utc::now() - chrono::Duration::days(29)).to_rfc3339();
    store
        .append_history_sample("open_prs_30d", &old, 99.0, None)
        .unwrap();
    store
        .append_history_sample("open_prs_30d", &recent, 5.0, None)
        .unwrap();

    // The background worker samples due series on its first tick.
    let workers = app.s().spawn_background_workers();
    wait_until("background history sample", 10, || {
        history
            .query_samples("open_prs_30d", None, 100)
            .unwrap()
            .iter()
            .any(|s| s.value_num == 3.0)
    })
    .await;
    workers.abort();
    let samples = history.query_samples("open_prs_30d", None, 100).unwrap();
    let values: Vec<f64> = samples.iter().map(|s| s.value_num).collect();
    assert_eq!(
        values,
        vec![5.0, 3.0],
        "31-day-old sample compacted, 29-day one kept"
    );
    // The sample's query ran in the Service principal's partition.
    let entries = app
        .s()
        .engine()
        .queries
        .cached_entries_for_instance("open_prs");
    assert!(entries
        .iter()
        .any(|(_, e)| e.principal.id == "pr_history_sampler"
            && e.principal.kind == PrincipalKind::ServicePrincipal
            && e.params == series.params));
    assert!(entries
        .iter()
        .all(|(_, e)| e.principal.kind != PrincipalKind::Administrator));

    app = app.restart().await;
    let hist = app
        .s()
        .provider_query(
            "pr_count_history",
            json!({"series_id": "open_prs_30d"}),
            &admin(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(history_values(hist.value.as_ref().unwrap()), vec![5.0, 3.0]);
    let out = app
        .s()
        .dashboard_get("engineering", &admin())
        .await
        .unwrap();
    let w = widgets(&out["rendered"]);
    assert_eq!(
        history_values(&w["pr_trend_adapter_trend_chart"]["props"]["series"]),
        vec![5.0, 3.0],
        "the trend chart renders the persisted history"
    );
}

fn widget_request(
    s: &AppServices,
    adapter: &str,
    widget_id: &str,
    request: &str,
    payload: Value,
) -> dash_core::WidgetRequestEnvelope {
    dash_core::WidgetRequestEnvelope {
        protocol: "dash.widget/1".into(),
        dashboard_id: "engineering".into(),
        adapter_instance: adapter.into(),
        widget_id: widget_id.into(),
        generation: s.graph().generation_id,
        sequence: 1,
        kind: "request".into(),
        request: request.into(),
        payload,
        dry_run_token: None,
    }
}

/// The UI flow: Dry run the Request, then dispatch it carrying the Dry run's token (R7:SEC-1).
async fn confirmed(
    s: &AppServices,
    mut req: dash_core::WidgetRequestEnvelope,
) -> dash_core::WidgetRequestEnvelope {
    req.dry_run_token = s
        .widget_request_dry_run(req.clone(), &admin())
        .await
        .unwrap()
        .dry_run_token;
    req
}

// =============================================================================================
// AC9 — State scopes, widget_ids and layout overrides
// =============================================================================================

fn instance_state(app: &App, owner: &str, key: &str) -> Option<dash_store::StateRecord> {
    app.s().store().get_state("instance", owner, key).unwrap()
}

fn dashboard_state(app: &App, dashboard: &str) -> BTreeMap<String, Value> {
    app.s()
        .engine()
        .state
        .list_dashboard_state(dashboard)
        .unwrap()
        .into_iter()
        .map(|r| (r.state_key, r.value))
        .collect()
}

fn set_repo(app: &App, dashboard: &str, repo: &str) -> Result<(), String> {
    let g = app.s().graph();
    app.s()
        .engine()
        .state
        .set_dashboard_state(
            &g,
            &admin(),
            dashboard,
            "selected_repository",
            json!(repo),
            None,
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Instance State (the provider's `seen_pr_states`) and Dashboard State (`selected_repository`)
/// are separate in storage, visibility and mutation, and both persist across a restart.
#[tokio::test]
async fn ac09_instance_and_dashboard_state_are_independent_and_persist() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let mut app = App::boot(&tmp, &up).await;

    let out = app
        .s()
        .dashboard_get("engineering", &admin())
        .await
        .unwrap();
    let seen = instance_state(&app, "open_prs", "seen_pr_states:acme/platform")
        .expect("the provider records Instance State while rendering");
    assert_eq!(seen.value["42"], "open");
    assert_eq!(
        dashboard_state(&app, "engineering"),
        BTreeMap::from([("selected_repository".to_string(), json!(PLATFORM))])
    );
    // Instance State is never part of a Dashboard read.
    assert!(!serde_json::to_string(&out)
        .unwrap()
        .contains("seen_pr_states"));
    let visible = app
        .s()
        .engine()
        .state
        .list_dashboard_state_for(&app.s().graph(), &admin(), "engineering")
        .unwrap();
    assert!(visible.iter().all(|r| r.scope == "dashboard"));
    // ...nor writable through the Dashboard State path.
    assert!(app
        .s()
        .engine()
        .state
        .set_dashboard_state(
            &app.s().graph(),
            &admin(),
            "engineering",
            "seen_pr_states:acme/platform",
            json!({}),
            None
        )
        .is_err());

    // Changing Dashboard State re-targets the render without touching Instance State.
    set_repo(&app, "engineering", "acme/payments").unwrap();
    let out = app
        .s()
        .dashboard_get("engineering", &admin())
        .await
        .unwrap();
    let w = widgets(&out["rendered"]);
    assert_eq!(
        w["pr_trend_adapter_open_count"]["props"]["label"],
        "Open PRs (acme/payments)"
    );
    assert_eq!(w["pr_table_adapter_pr_summary"]["props"]["open_count"], 1);
    let seen_after = instance_state(&app, "open_prs", "seen_pr_states:acme/platform").unwrap();
    assert_eq!(seen_after.version, seen.version);
    assert_eq!(seen_after.value, seen.value);
    assert!(instance_state(&app, "open_prs", "seen_pr_states:acme/payments").is_some());

    app = app.restart().await;
    assert_eq!(
        dashboard_state(&app, "engineering")["selected_repository"],
        json!("acme/payments"),
        "Dashboard State persists and is not re-seeded from initial_state"
    );
    let seen_restarted = instance_state(&app, "open_prs", "seen_pr_states:acme/platform").unwrap();
    assert_eq!(seen_restarted.value, seen.value);
    let out = app
        .s()
        .dashboard_get("engineering", &admin())
        .await
        .unwrap();
    assert_eq!(
        widgets(&out["rendered"])["pr_trend_adapter_open_count"]["props"]["label"],
        "Open PRs (acme/payments)"
    );
    // The persisted Instance State keeps driving the provider: a merge since the last poll is
    // detected against the pre-restart PR states.
    up.merge_externally(PLATFORM, 44);
    app.s()
        .provider_query("open_prs", open_params(PLATFORM), &admin(), true)
        .await
        .unwrap();
    let evs = merged_events(&app, "open_prs");
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].payload["number"], 44);
}

const CLIENT_PROBE_DECL: &str = r#"local dash = require("dash")
return dash.adapter {
    id = "probe.client_probe",
    runtime = "runtime.luau",
    handler = "render",
}"#;
const CLIENT_PROBE_RT: &str = r#"local R = {}
function R.render(ctx)
    local ok, v = pcall(function() return ctx.client_state end)
    local seen = ok and tostring(v) or ("error: " .. tostring(v))
    return { key = "probe", widget_type = "dash.text", props = { text = seen } }
end
return R"#;

/// Dashboard State is per Dashboard: two Dashboards placing the same Adapters render from their
/// own `selected_repository`, and writing one never changes the other. Client State is not
/// reachable from Adapter code at all (it lives only in the browser).
#[tokio::test]
async fn ac09_dashboard_state_is_per_dashboard_and_client_state_is_not_on_the_server() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot_with(&tmp, &up, true, |ws| {
        add_package(ws, &[("client_probe", CLIENT_PROBE_DECL, CLIENT_PROBE_RT)]);
        append_instances(
            ws,
            "adapters.yaml",
            "  client_probe:\n    definition: probe.client_probe\n",
        );
        std::fs::write(
            ws.join("dashboards/payments.yaml"),
            "id: payments\ntitle: Payments\nlayout: grid\ninitial_state:\n  selected_repository: acme/payments\n\
             adapters:\n  - adapter_instance: pr_trend_adapter\n    area: top\n    query_params: {}\n\
             \x20 - adapter_instance: client_probe\n    area: main\n    query_params: {}\n",
        )
        .unwrap();
    })
    .await;
    let s = app.s();
    let label = |out: &Value| {
        widgets(&out["rendered"])["pr_trend_adapter_open_count"]["props"]["label"].clone()
    };

    let eng = s.dashboard_get("engineering", &admin()).await.unwrap();
    let pay = s.dashboard_get("payments", &admin()).await.unwrap();
    assert_eq!(label(&eng), "Open PRs (acme/platform)");
    assert_eq!(label(&pay), "Open PRs (acme/payments)");

    set_repo(&app, "engineering", "acme/payments").unwrap();
    set_repo(&app, "payments", PLATFORM).unwrap();
    assert_eq!(
        dashboard_state(&app, "engineering")["selected_repository"],
        "acme/payments"
    );
    assert_eq!(
        dashboard_state(&app, "payments")["selected_repository"],
        PLATFORM
    );
    let eng = s.dashboard_get("engineering", &admin()).await.unwrap();
    let pay = s.dashboard_get("payments", &admin()).await.unwrap();
    assert_eq!(label(&eng), "Open PRs (acme/payments)");
    assert_eq!(label(&pay), "Open PRs (acme/platform)");

    let probe = &widgets(&pay["rendered"])["client_probe_probe"]["props"]["text"];
    let probe = probe.as_str().unwrap();
    assert!(probe == "nil" || probe.starts_with("error"), "{probe}");
}

fn widget_types(rendered: &Value) -> BTreeMap<String, Value> {
    widgets(rendered)
        .into_iter()
        .map(|(id, node)| (id, node["widget_type"].clone()))
        .collect()
}

/// widget_ids are derived from Adapter + node key, so they are identical across refreshes, an
/// Adapter reorder in a new Generation and a restart; layout overrides saved through a Draft
/// apply to those ids and survive refresh, the new Generation and a restart.
#[tokio::test]
async fn ac09_widget_ids_and_layout_overrides_survive_refresh_reorder_and_restart() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let mut app = App::boot(&tmp, &up).await;
    let r1 = app
        .s()
        .dashboard_get("engineering", &admin())
        .await
        .unwrap();
    let ids = widget_types(&r1["rendered"]);
    assert_eq!(
        ids.keys().cloned().collect::<Vec<_>>(),
        vec![
            "pr_table_adapter_pr_summary",
            "pr_table_adapter_pr_table",
            "pr_table_adapter_root",
            "pr_trend_adapter_open_count",
            "pr_trend_adapter_trend_chart",
            "pr_trend_adapter_trend_root",
        ]
    );
    let r2 = app
        .s()
        .dashboard_get("engineering", &admin())
        .await
        .unwrap();
    assert_eq!(
        widget_types(&r2["rendered"]),
        ids,
        "refresh keeps widget_ids"
    );
    assert_eq!(
        widgets(&r2["rendered"])["pr_table_adapter_pr_table"]["layout_override"]["w"],
        12
    );

    // Save a reorder and new overrides through the Draft flow (as the designer does).
    let gen_before = app.s().graph().generation_id;
    let draft = app.s().draft_begin(&admin()).await.unwrap();
    let mut doc = app.s().graph().dashboards["engineering"].clone();
    doc.adapters.reverse();
    doc.overrides.insert(
        "pr_table_adapter_pr_table".into(),
        serde_json::from_value(json!({"x": 0, "y": 2, "w": 6, "h": 3, "visible": true})).unwrap(),
    );
    doc.overrides.insert(
        "pr_table_adapter_pr_summary".into(),
        serde_json::from_value(json!({"visible": false})).unwrap(),
    );
    app.s()
        .dashboard_update_in_draft(&draft.draft_id, &doc, &admin())
        .unwrap();
    app.s()
        .draft_activate(&draft.draft_id, &admin())
        .await
        .unwrap();
    assert!(app.s().graph().generation_id > gen_before);

    let check = |out: &Value| {
        assert_eq!(
            out["rendered"]["adapters"][0]["adapter_instance"],
            "pr_table_adapter"
        );
        assert_eq!(
            widget_types(&out["rendered"]),
            ids,
            "reordering keeps widget_ids"
        );
        let w = widgets(&out["rendered"]);
        let o = &w["pr_table_adapter_pr_table"]["layout_override"];
        assert_eq!((o["x"].clone(), o["y"].clone()), (json!(0), json!(2)));
        assert_eq!((o["w"].clone(), o["h"].clone()), (json!(6), json!(3)));
        assert_eq!(
            w["pr_table_adapter_pr_summary"]["layout_override"]["visible"],
            false
        );
        assert_eq!(w["pr_trend_adapter_trend_chart"]["layout_override"]["w"], 8);
    };
    check(
        &app.s()
            .dashboard_get("engineering", &admin())
            .await
            .unwrap(),
    );
    check(
        &app.s()
            .dashboard_get("engineering", &admin())
            .await
            .unwrap(),
    );
    let saved = std::fs::read_to_string(app.ws.join("dashboards/engineering.yaml")).unwrap();
    assert!(
        saved.contains("pr_table_adapter_pr_summary"),
        "committed to the Workspace"
    );

    app = app.restart().await;
    check(
        &app.s()
            .dashboard_get("engineering", &admin())
            .await
            .unwrap(),
    );
}

// =============================================================================================
// AC10 — actions
// =============================================================================================

fn merge_args(number: i64) -> Value {
    json!({"repository": PLATFORM, "number": number, "issue_key": "DEV-123"})
}

/// A Dry run reads (get_pr) but never writes, emits, records an execution or invalidates.
#[tokio::test]
async fn ac10_dry_run_has_no_side_effects() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let s = app.s();
    let before = s
        .provider_query("open_prs", open_params(PLATFORM), &admin(), false)
        .await
        .unwrap();

    let report = s
        .action_dry_run("merge_pr", merge_args(42), &admin())
        .await
        .unwrap();
    assert!(
        report.supported && report.feasible && report.advisory,
        "{report:?}"
    );
    assert_eq!(report.preconditions[0]["check"], "pr_state");
    assert_eq!(report.preconditions[0]["actual"], "open");
    assert_eq!(report.effects["action"], "squash_and_merge");
    assert_eq!(
        up.hits("get_pr:acme/platform#42"),
        1,
        "the Dry run read the upstream"
    );
    assert_eq!(
        up.hits("merge:acme/platform#42"),
        0,
        "...and never wrote to it"
    );
    assert_eq!(up.pr_state(PLATFORM, 42), "open");
    assert!(merged_events(&app, "merge_pr").is_empty());
    assert!(executions(&app, "merge_pr").is_empty());
    let after = s.engine().queries.cached_entry(&before.query_key).unwrap();
    assert_eq!(after.version, before.version, "no invalidation");
    assert!(!after.stale);

    // The report reflects upstream state: an already-merged PR is not feasible.
    up.merge_externally(PLATFORM, 43);
    let report = s
        .action_dry_run("merge_pr", merge_args(43), &admin())
        .await
        .unwrap();
    assert!(!report.feasible);
    assert!(report.warnings[0].contains("merged"), "{report:?}");
}

/// Execution records are durable (inspectable after restart), idempotency holds under
/// concurrent retries (one upstream write), and a successful merge invalidates the affected
/// queries so subscribers receive the new value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac10_persistent_records_idempotency_and_invalidation() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let mut app = App::boot(&tmp, &up).await;
    let (mut rx, _key, mut ver, mut local) = subscribed_replica(&app, open_params(PLATFORM)).await;
    let linked_key = app
        .s()
        .engine()
        .queries
        .resolve_query_key("linked_prs", &open_params(PLATFORM), &admin())
        .unwrap();
    let mut linked_rx = app.s().engine().signals.subscribe_stream(&linked_key);
    app.s()
        .provider_query("linked_prs", open_params(PLATFORM), &admin(), false)
        .await
        .unwrap();
    let (mut lver, mut linked) = (0, Vec::new());
    drain_ready(&mut linked_rx, &mut lver, &mut linked);
    assert_eq!(numbers(&linked), vec![42, 43, 44]);

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let s = app.s().clone();
        tasks.push(tokio::spawn(async move {
            s.action_invoke(
                "merge_pr",
                merge_args(42),
                Some("merge-42-once".into()),
                &admin(),
            )
            .await
        }));
    }
    let mut ids = BTreeSet::new();
    for t in tasks {
        let rec = t.await.unwrap().unwrap();
        ids.insert(rec.execution_id.clone());
    }
    assert_eq!(ids.len(), 1, "one execution for one idempotency key");
    assert_eq!(up.hits("merge:acme/platform#42"), 1, "one upstream write");
    let id = ids.into_iter().next().unwrap();
    wait_until("execution finished", 10, || {
        executions(&app, "merge_pr")
            .iter()
            .any(|r| r.execution_id == id && r.status == "succeeded")
    })
    .await;

    // Invalidation: subscribers of the merged PR's queries receive the new value.
    sync_until_numbers(&mut rx, &mut ver, &mut local, &[43, 44]).await;
    sync_until_numbers(&mut linked_rx, &mut lver, &mut linked, &[43, 44]).await;

    app = app.restart().await;
    let rec = app.s().execution_inspect(Some(&id)).unwrap();
    assert_eq!(rec["status"], "succeeded");
    assert_eq!(rec["result"], json!({"merged": true, "sha": "sha42"}));
    assert_eq!(rec["principal_id"], "admin");
    // The idempotency key still names the persisted execution after the restart.
    let again = app
        .s()
        .action_invoke(
            "merge_pr",
            merge_args(42),
            Some("merge-42-once".into()),
            &admin(),
        )
        .await
        .unwrap();
    assert_eq!(again.execution_id, id);
    assert_eq!(up.hits("merge:acme/platform#42"), 1);
}

const SHIP_DECL: &str = r#"local dash = require("dash")
return dash.action_provider {
    id = "probe.ship",
    runtime = "runtime.luau",
    handler = "execute",
    inputs = {
        merge = dash.action_ref("github.merge_pr"),
        transition = dash.action_ref("jira.transition_issue"),
    },
    args = dash.object { number = dash.integer() },
}"#;
const SHIP_RT: &str = r#"local R = {}
local function try(f)
    local ok, res = pcall(f)
    if ok then
        return { ok = true, status = res.status, execution_id = res.execution_id }
    end
    return { ok = false, error = tostring(res) }
end
function R.execute(ctx)
    local merge = try(function()
        return ctx:await(ctx.inputs.merge:invoke({
            repository = "acme/platform", number = ctx.args.number,
        }))
    end)
    local transition = try(function()
        return ctx:await(ctx.inputs.transition:invoke({ key = "DEV-123", status = "Done" }))
    end)
    return { merge = merge, transition = transition }
end
return R"#;

fn add_ship_action(ws: &Path) {
    add_package(ws, &[("ship", SHIP_DECL, SHIP_RT)]);
    append_instances(
        ws,
        "actions.yaml",
        "  ship:\n    definition: probe.ship\n    inputs:\n      merge: merge_pr\n      transition: transition_issue\n",
    );
}

/// An action that invokes other actions: each nested invocation is authorized on its own
/// against the caller's ceiling (being allowed to run the parent grants nothing), and each
/// allowed child gets its own linked execution record.
#[tokio::test]
async fn ac10_nested_actions_are_authorized_independently() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot_with(&tmp, &up, true, add_ship_action).await;
    let s = app.s();
    let shipper = agent(
        "agt_shipper",
        CapabilityCeiling {
            allowed_actions: BTreeSet::from(["ship".into(), "merge_pr".into()]),
            allowed_connectors: BTreeSet::from(["github_maintainer".into()]),
            allowed_capabilities: BTreeSet::from([
                "pull_requests.read".into(),
                "pull_requests.merge".into(),
            ]),
            allowed_secrets: BTreeSet::from(["github-work".into()]),
            ..Default::default()
        },
    );

    let parent = s
        .action_invoke("ship", json!({"number": 42}), None, &shipper)
        .await
        .unwrap();
    let result = parent.result.clone().unwrap_or_default();
    assert_eq!(result["merge"]["ok"], true, "{parent:?}");
    assert_eq!(result["merge"]["status"], "succeeded");
    assert_eq!(result["transition"]["ok"], false, "{parent:?}");
    assert!(
        result["transition"]["error"]
            .as_str()
            .unwrap()
            .contains("transition_issue"),
        "{result}"
    );
    assert_eq!(up.merges(), vec!["acme/platform#42".to_string()]);
    assert!(up.transitions().is_empty(), "the denied child never ran");

    // The allowed child is its own record, linked to the parent and its trace.
    let merge_rec = executions(&app, "merge_pr");
    assert_eq!(merge_rec.len(), 1);
    assert_eq!(
        merge_rec[0].parent_execution_id.as_deref(),
        Some(parent.execution_id.as_str())
    );
    assert_eq!(merge_rec[0].trace_id, parent.trace_id);
    assert_eq!(merge_rec[0].principal_id, "agt_shipper");
    let steps = s
        .store()
        .list_action_step_children(&parent.execution_id)
        .unwrap();
    assert_eq!(steps.len(), 1, "{steps:?}");
    assert!(executions(&app, "transition_issue").is_empty());
    // The nested denial is audited against the child action, with the caller.
    assert!(s.store().list_audits(200).unwrap().iter().any(|a| {
        a.outcome == "denied"
            && a.principal_id == "agt_shipper"
            && a.target.contains("transition_issue")
    }));

    // Holding the nested actions is not enough without the parent.
    let merger = agent(
        "agt_merger",
        CapabilityCeiling {
            allowed_actions: BTreeSet::from(["merge_pr".into(), "transition_issue".into()]),
            ..shipper.ceiling.clone()
        },
    );
    assert!(s
        .action_invoke("ship", json!({"number": 43}), None, &merger)
        .await
        .is_err());
    assert_eq!(up.merges().len(), 1);

    // Positive control: a Principal holding every action runs both children.
    let both = s
        .action_invoke("ship", json!({"number": 43}), None, &admin())
        .await
        .unwrap();
    let result = both.result.unwrap();
    assert_eq!(result["merge"]["ok"], true, "{result}");
    assert_eq!(result["transition"]["ok"], true, "{result}");
    assert_eq!(up.transitions(), vec![("DEV-123".into(), "Done".into())]);
}

/// The upstream performs the merge but the response is lost (502): the outcome is `unknown`
/// (never `failed`), a reconcile is scheduled, and the host's backoff worker reconciles it to
/// `succeeded` by reading the upstream — without merging again.
#[tokio::test]
async fn ac10_lost_response_is_unknown_and_reconciled_by_the_host() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let s = app.s();
    up.with(|u| u.merge_mode = MergeMode::ApplyThenBadGateway);

    let rec = s
        .action_invoke("merge_pr", merge_args(42), None, &admin())
        .await
        .unwrap();
    assert_eq!(rec.status, "unknown", "{rec:?}");
    assert_eq!(
        up.merges(),
        vec!["acme/platform#42".to_string()],
        "the merge did happen"
    );
    let id = rec.execution_id.clone();
    let current = || s.store().get_action_execution(&id).unwrap().unwrap();
    // The inline reconcile attempt could not reach the upstream: still unknown, with backoff.
    assert_eq!(current().status, "unknown");
    let schedule = s.store().get_action_reconcile_schedule(&id).unwrap();
    assert!(schedule.is_some(), "a reconcile is scheduled");
    // Not due yet: the worker leaves it alone.
    s.engine().actions.run_due_reconciles(16).await;
    assert_eq!(current().status, "unknown");

    // The upstream recovers; when the backoff elapses the host's worker reconciles it.
    up.with(|u| u.fail_reads = false);
    s.store()
        .schedule_action_reconcile(&id, &chrono::Utc::now().to_rfc3339())
        .unwrap();
    s.engine().actions.run_due_reconciles(16).await;
    let done = current();
    assert_eq!(done.status, "succeeded", "{done:?}");
    assert!(done.reconcile_attempts >= 1);
    assert_eq!(
        up.hits("merge:acme/platform#42"),
        1,
        "reconcile never re-merges"
    );
    assert!(s
        .store()
        .get_action_reconcile_schedule(&id)
        .unwrap()
        .is_none());
    assert!(s
        .store()
        .list_audits(200)
        .unwrap()
        .iter()
        .any(|a| a.operation == "action.reconcile" && a.target == id));
}

/// The upstream is unavailable (503) and nothing was merged: reconcile cannot prove either
/// outcome, so the record stays `unknown` until an authorized Principal resolves it manually,
/// with a reason, audited; unauthorized resolution attempts are denied and audited; the
/// resolution is final and survives a restart.
#[tokio::test]
async fn ac10_unresolvable_unknown_requires_authorized_audited_manual_resolution() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let mut app = App::boot(&tmp, &up).await;
    up.with(|u| u.merge_mode = MergeMode::Unavailable);

    let rec = app
        .s()
        .action_invoke("merge_pr", merge_args(42), None, &admin())
        .await
        .unwrap();
    assert_eq!(rec.status, "unknown", "{rec:?}");
    assert!(up.merges().is_empty());
    let id = rec.execution_id.clone();
    // A reconcile attempt sees the PR still open: still unknown.
    app.s()
        .store()
        .schedule_action_reconcile(&id, &chrono::Utc::now().to_rfc3339())
        .unwrap();
    app.s().engine().actions.run_due_reconciles(16).await;
    let r = app.s().store().get_action_execution(&id).unwrap().unwrap();
    assert_eq!(r.status, "unknown");

    // Agents without both the resolve permission and the action are denied.
    let no_action = agent(
        "agt_resolver",
        CapabilityCeiling {
            can_resolve_executions: true,
            ..Default::default()
        },
    );
    let no_resolve = agent(
        "agt_merger",
        CapabilityCeiling {
            allowed_actions: BTreeSet::from(["merge_pr".into()]),
            ..Default::default()
        },
    );
    for p in [&no_action, &no_resolve] {
        assert!(app
            .s()
            .execution_resolve(&id, "failed", "looked at GitHub", p)
            .await
            .is_err());
    }
    assert!(
        app.s()
            .execution_resolve(&id, "failed", "  ", &admin())
            .await
            .is_err(),
        "a reason is required"
    );
    let audits = app.s().store().list_audits(500).unwrap();
    for who in ["agt_resolver", "agt_merger"] {
        assert!(
            audits.iter().any(|a| a.operation == "action.resolve"
                && a.principal_id == who
                && a.outcome == "denied"),
            "denial of {who} is audited"
        );
    }
    assert_eq!(
        app.s()
            .store()
            .get_action_execution(&id)
            .unwrap()
            .unwrap()
            .status,
        "unknown"
    );

    let resolver = agent(
        "agt_oncall",
        CapabilityCeiling {
            can_resolve_executions: true,
            allowed_actions: BTreeSet::from(["merge_pr".into()]),
            allowed_connectors: BTreeSet::from(["github_maintainer".into()]),
            allowed_capabilities: BTreeSet::from([
                "pull_requests.read".into(),
                "pull_requests.merge".into(),
            ]),
            ..Default::default()
        },
    );
    let resolved = app
        .s()
        .execution_resolve(
            &id,
            "failed",
            "PR still open on GitHub; merge never landed",
            &resolver,
        )
        .await
        .unwrap();
    assert_eq!(resolved.status, "failed");
    let audit = app
        .s()
        .store()
        .list_audits(500)
        .unwrap()
        .into_iter()
        .find(|a| a.operation == "action.resolve" && a.target == id && a.outcome != "denied")
        .expect("resolution audited");
    assert_eq!(audit.principal_id, "agt_oncall");
    assert_eq!(audit.detail["previous_status"], "unknown");

    app = app.restart().await;
    let r = app.s().store().get_action_execution(&id).unwrap().unwrap();
    assert_eq!(r.status, "failed");
    assert!(
        app.s()
            .execution_resolve(&id, "succeeded", "second opinion", &admin())
            .await
            .is_err(),
        "a resolution is final"
    );
    // Nothing on the restarted host reconciles it back.
    app.s().engine().actions.run_due_reconciles(16).await;
    assert_eq!(
        app.s()
            .store()
            .get_action_execution(&id)
            .unwrap()
            .unwrap()
            .status,
        "failed"
    );
    assert!(up.merges().is_empty());
}

// =============================================================================================
// Appendix A.3 / §10.3 — widget Request validation and MISSING_MOCK
// =============================================================================================

/// Requests carrying an old Generation, a widget_id not bound to the Adapter's current render,
/// or a Request name the Adapter does not accept are rejected before any Adapter or action
/// code runs.
#[tokio::test]
async fn ac10_widget_requests_with_stale_generation_or_unbound_widget_are_rejected() {
    let tmp = TempDir::new().unwrap();
    let up = Upstream::start().await;
    let app = App::boot(&tmp, &up).await;
    let s = app.s();
    s.dashboard_get("engineering", &admin()).await.unwrap();
    let old_gen = s.graph().generation_id;

    // A new Generation (title change) makes the page's Generation stale.
    let draft = s.draft_begin(&admin()).await.unwrap();
    let mut doc = s.graph().dashboards["engineering"].clone();
    doc.title = "Engineering (renamed)".into();
    s.dashboard_update_in_draft(&draft.draft_id, &doc, &admin())
        .unwrap();
    s.draft_activate(&draft.draft_id, &admin()).await.unwrap();
    assert!(s.graph().generation_id > old_gen);
    s.dashboard_get("engineering", &admin()).await.unwrap();

    let payload = json!({"repository": PLATFORM, "number": 42, "jira_key": "DEV-123"});
    let mut stale = widget_request(
        s,
        "pr_table_adapter",
        "pr_table_adapter_pr_table",
        "merge_requested",
        payload.clone(),
    );
    stale.generation = old_gen;
    let err = s
        .dispatch_widget_request(stale, &admin())
        .await
        .unwrap_err();
    assert!(err.contains("stale"), "{err}");

    // A widget rendered by another Adapter, and an id no Adapter rendered.
    for widget_id in [
        "pr_trend_adapter_trend_chart",
        "pr_table_adapter_not_rendered",
    ] {
        let req = widget_request(
            s,
            "pr_table_adapter",
            widget_id,
            "merge_requested",
            payload.clone(),
        );
        let err = s.dispatch_widget_request(req, &admin()).await.unwrap_err();
        assert!(!err.is_empty(), "{widget_id}");
    }
    let undeclared = widget_request(
        s,
        "pr_table_adapter",
        "pr_table_adapter_pr_table",
        "delete_everything",
        json!({}),
    );
    assert!(s
        .dispatch_widget_request(undeclared, &admin())
        .await
        .is_err());
    let wrong_protocol = dash_core::WidgetRequestEnvelope {
        protocol: "dash.widget/0".into(),
        ..widget_request(
            s,
            "pr_table_adapter",
            "pr_table_adapter_pr_table",
            "merge_requested",
            payload.clone(),
        )
    };
    assert!(s
        .dispatch_widget_request(wrong_protocol, &admin())
        .await
        .is_err());

    assert_eq!(
        up.hits("merge:acme/platform#42"),
        0,
        "nothing reached the action"
    );
    assert!(executions(&app, "merge_pr").is_empty());

    // Positive control: the same Request on the current Generation and bound widget succeeds.
    let merge = confirmed(
        s,
        widget_request(
            s,
            "pr_table_adapter",
            "pr_table_adapter_pr_table",
            "merge_requested",
            payload,
        ),
    )
    .await;
    let ok = s.dispatch_widget_request(merge, &admin()).await.unwrap();
    assert_eq!(ok.status, "succeeded");
    assert_eq!(up.merges(), vec!["acme/platform#42".to_string()]);
}

/// §10.3: a connector Definition without a Mock fails Preview with MISSING_MOCK.
#[tokio::test]
async fn ac10_preview_fails_with_missing_mock() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    dash_git::packages::copy_tree_excluding_git(&fixture_dir(), &ws).unwrap();
    let full = run_preview_and_scenarios(&ws, &data, None).await.unwrap();
    assert!(full.all_passed, "control: the reference app passes Preview");

    std::fs::remove_dir_all(ws.join("packages/github/mock")).unwrap();
    let rep = run_preview_and_scenarios(&ws, &tmp.path().join("data2"), None)
        .await
        .unwrap();
    assert!(!rep.all_passed);
    let missing: Vec<_> = rep
        .validation
        .issues
        .iter()
        .filter(|i| i.code == "MISSING_MOCK")
        .collect();
    assert!(!missing.is_empty(), "{:?}", rep.validation.issues);
    assert!(missing.iter().all(|i| i.severity == "error"));
    assert!(
        missing.iter().any(|i| i.target.contains("github")),
        "{missing:?}"
    );
    assert!(
        !rep.validation
            .issues
            .iter()
            .any(|i| i.code == "MISSING_MOCK" && i.target.contains("jira")),
        "only the connector without a Mock is reported"
    );
}
