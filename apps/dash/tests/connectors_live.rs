//! The service-specific and generic REST/JSON connectors, end to end: the real engine, host HTTP
//! broker, network policy, vault credential schemes and Definitions, against a loopback upstream
//! that answers with the real APIs' payloads (pagination headers, JSON nulls, error bodies,
//! rate-limit headers, Atlassian Document Format, 204s).
//!
//! - GitHub: Link pagination, normalization (merged = closed + merged_at, nulls), CI status from
//!   statuses + check runs, workflow runs, search, issue writes, merge guarded by the head SHA,
//!   405/409 definitive and 5xx unknown outcomes, rate limits.
//! - Jira Cloud (v3, Basic email:token) and Server/DC (v2, Bearer): JQL search pagination
//!   (nextPageToken / startAt), unknown keys skipped, workflow transitions by status with an ADF
//!   comment, comment and issue creation bodies.
//! - dash.http: header-scheme credential, cursor pagination with field mapping through
//!   dash.http_query, dash.http_action writes with idempotency keys and Dry runs, path safety,
//!   and dash.http_mock in Preview.

mod common_t4;

use axum::extract::{OriginalUri, Path as AxPath, Query, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::Engine as _;
use common_t4::*;
use dash_core::Principal;
use dash_luau::ResourceBudget;
use dash_store::SqliteStore;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------------------------
// Loopback upstream
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Seen {
    method: String,
    uri: String,
    headers: BTreeMap<String, String>,
    body: Value,
}

#[derive(Clone, Default)]
struct Up {
    seen: Arc<Mutex<Vec<Seen>>>,
    jira_status: Arc<Mutex<BTreeMap<String, String>>>,
}

impl Up {
    fn record(&self, method: &Method, uri: &str, headers: &HeaderMap, body: Value) {
        let headers = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        self.seen.lock().unwrap().push(Seen {
            method: method.to_string(),
            uri: uri.to_string(),
            headers,
            body,
        });
    }

    fn find(&self, method: &str, uri_prefix: &str) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.method == method && s.uri.starts_with(uri_prefix))
            .cloned()
            .collect()
    }
}

/// Record every request (method, URI, headers, JSON body) before routing it.
async fn recorder(
    State(up): State<Up>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let (parts, body) = req.into_parts();
    let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    up.record(&parts.method, &parts.uri.to_string(), &parts.headers, json);
    next.run(axum::extract::Request::from_parts(
        parts,
        axum::body::Body::from(bytes),
    ))
    .await
}

fn gh_pr(number: i64, state: &str, merged: bool) -> Value {
    json!({
        "id": 5000 + number, "node_id": "x", "number": number, "state": state,
        "title": format!("OPS-{number} change {number}"),
        "user": { "login": "alice", "id": 1 }, "body": null, "draft": number == 3,
        "html_url": format!("https://github.com/acme/api/pull/{number}"),
        "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-02T00:00:00Z",
        "closed_at": if state == "closed" { json!("2026-01-03T00:00:00Z") } else { Value::Null },
        "merged_at": if merged { json!("2026-01-03T00:00:00Z") } else { Value::Null },
        "assignee": null, "assignees": [], "requested_reviewers": [{ "login": "bob" }],
        "labels": if number == 1 { json!([{ "id": 1, "name": "bug", "color": "f00" }]) } else { json!([]) },
        "milestone": null,
        "head": { "ref": format!("feat/{number}"), "sha": format!("head{number}"), "repo": { "full_name": "acme/api" } },
        "base": { "ref": "main", "sha": "base", "repo": { "full_name": "acme/api" } },
    })
}

fn github(base: String) -> Router<Up> {
    Router::new()
        .route(
            "/repos/acme/api/pulls",
            get(move |Query(q): Query<BTreeMap<String, String>>| {
                let base = base.clone();
                async move {
                    let state = q.get("state").cloned().unwrap_or_default();
                    if state == "closed" {
                        return Json(json!([gh_pr(8, "closed", true), gh_pr(9, "closed", false)]))
                            .into_response();
                    }
                    if q.get("page").map(String::as_str) == Some("2") {
                        return Json(json!([gh_pr(3, "open", false)])).into_response();
                    }
                    (
                        [(
                            "link",
                            format!(
                                "<{base}/gh/repos/acme/api/pulls?page=2&per_page=100&state=open>; rel=\"next\", <{base}/gh/repos/acme/api/pulls?page=2&per_page=100&state=open>; rel=\"last\""
                            ),
                        )],
                        Json(json!([gh_pr(1, "open", false), gh_pr(2, "open", false)])),
                    )
                        .into_response()
                }
            }),
        )
        .route(
            "/repos/acme/api/pulls/{n}",
            get(|AxPath(n): AxPath<i64>| async move {
                let mut pr = gh_pr(n, "open", false);
                pr["merged"] = json!(false);
                pr["mergeable"] = json!(n != 2);
                pr["mergeable_state"] = json!(if n == 2 { "dirty" } else { "clean" });
                pr["merged_by"] = Value::Null;
                Json(pr)
            }),
        )
        .route(
            "/repos/acme/api/pulls/{n}/merge",
            put(|AxPath(n): AxPath<i64>, Json(body): Json<Value>| async move {
                match n {
                    _ if body["sha"] != format!("head{n}").as_str() => (
                        StatusCode::CONFLICT,
                        Json(json!({ "message": "Head branch was modified. Review and try the merge again." })),
                    )
                        .into_response(),
                    1 => Json(json!({ "sha": "merge1", "merged": true, "message": "Pull Request successfully merged" })).into_response(),
                    2 => (StatusCode::METHOD_NOT_ALLOWED, Json(json!({ "message": "Pull Request is not mergeable" }))).into_response(),
                    _ => (StatusCode::BAD_GATEWAY, "upstream timeout").into_response(),
                }
            }),
        )
        .route(
            "/repos/acme/api/issues",
            get(|| async {
                Json(json!([
                    { "id": 1, "number": 10, "title": "Crash on start", "state": "open", "state_reason": null,
                      "user": { "login": "erin" }, "labels": [{ "name": "bug" }], "assignees": [], "comments": 4,
                      "html_url": "https://github.com/acme/api/issues/10", "created_at": "c", "updated_at": "u",
                      "closed_at": null, "body": null, "repository_url": "http://x/repos/acme/api" },
                    { "id": 2, "number": 11, "title": "A PR", "state": "open", "user": { "login": "a" },
                      "labels": [], "assignees": [], "pull_request": { "url": "x", "merged_at": null } }
                ]))
            })
            .post(|Json(body): Json<Value>| async move {
                (
                    StatusCode::CREATED,
                    Json(json!({ "id": 3, "number": 12, "title": body["title"], "state": "open",
                                 "user": { "login": "dash-bot" }, "labels": body["labels"], "assignees": [],
                                 "html_url": "https://github.com/acme/api/issues/12", "body": body["body"] })),
                )
            }),
        )
        .route(
            "/repos/acme/api/issues/{n}",
            get(|AxPath(n): AxPath<i64>| async move {
                Json(json!({ "id": 1, "number": n, "title": "Crash on start", "state": "open",
                             "user": { "login": "erin" }, "labels": [], "assignees": [] }))
            })
            .patch(|AxPath(n): AxPath<i64>, Json(body): Json<Value>| async move {
                Json(json!({ "id": 1, "number": n, "title": "Crash on start",
                             "state": body["state"].as_str().unwrap_or("open"),
                             "state_reason": body["state_reason"], "user": { "login": "erin" },
                             "labels": body.get("labels").cloned().unwrap_or(json!([])), "assignees": [] }))
            }),
        )
        .route(
            "/repos/acme/api/issues/{n}/comments",
            post(|| async {
                (StatusCode::CREATED, Json(json!({ "id": 9001, "html_url": "https://github.com/c/9001", "created_at": "t" })))
            }),
        )
        .route(
            "/search/issues",
            get(|| async {
                Json(json!({ "total_count": 1, "incomplete_results": false, "items": [
                    { "id": 7, "number": 77, "title": "Review me", "state": "open", "user": { "login": "a" },
                      "labels": [], "assignees": [], "repository_url": "https://api.github.com/repos/acme/web",
                      "pull_request": { "merged_at": null } }
                ] }))
            }),
        )
        .route(
            "/repos/acme/api/commits/{reference}/status",
            get(|AxPath(r): AxPath<String>| async move {
                assert_eq!(r, "release/1.0", "the ref arrives percent-decoded as one segment");
                Json(json!({ "state": "success", "statuses": [
                    { "context": "coverage", "state": "success", "description": "91%", "target_url": "u" }
                ] }))
            }),
        )
        .route(
            "/repos/acme/api/commits/{reference}/check-runs",
            get(|| async {
                Json(json!({ "total_count": 2, "check_runs": [
                    { "name": "build", "status": "completed", "conclusion": "success", "html_url": "b",
                      "started_at": "s", "completed_at": "c" },
                    { "name": "e2e", "status": "completed", "conclusion": "timed_out", "html_url": "e",
                      "started_at": "s", "completed_at": null }
                ] }))
            }),
        )
        .route(
            "/repos/acme/api/actions/runs",
            get(|| async {
                Json(json!({ "total_count": 1, "workflow_runs": [
                    { "id": 30_000_000_001_i64, "name": "CI", "display_title": "OPS-1 change 1", "workflow_id": 4,
                      "run_number": 12, "run_attempt": 1, "status": "completed", "conclusion": null,
                      "head_branch": "main", "head_sha": "abc", "event": "push", "actor": { "login": "carol" },
                      "html_url": "u", "created_at": "c", "updated_at": "u", "run_started_at": "s" }
                ] }))
            }),
        )
        .route(
            "/repos/acme/limited/pulls",
            get(|| async {
                (
                    StatusCode::FORBIDDEN,
                    [("x-ratelimit-remaining", "0"), ("x-ratelimit-reset", "1767225600")],
                    Json(json!({ "message": "API rate limit exceeded for user ID 1." })),
                )
            }),
        )
}

fn jira_issue(key: &str, status: &str) -> Value {
    let cat = match status {
        "To Do" => "new",
        "Done" => "done",
        _ => "indeterminate",
    };
    json!({ "id": format!("1{}", key.len()), "key": key, "self": "x", "fields": {
        "summary": format!("Summary {key}"),
        "status": { "name": status, "statusCategory": { "key": cat } },
        "priority": null, "assignee": null, "reporter": { "displayName": "Bob" },
        "issuetype": { "name": "Bug" }, "project": { "key": key.split('-').next().unwrap() },
        "labels": [], "created": "c", "updated": "u", "resolution": null, "duedate": null,
        "customfield_10016": 3
    } })
}

fn jira_cloud() -> Router<Up> {
    Router::new()
        .route(
            "/rest/api/3/search/jql",
            get(|State(up): State<Up>, Query(q): Query<BTreeMap<String, String>>| async move {
                let jql = q.get("jql").cloned().unwrap_or_default();
                if jql.contains("OPS-404") {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "errorMessages": ["An issue with key 'OPS-404' does not exist for field 'key'."], "errors": {} })),
                    )
                        .into_response();
                }
                if jql.starts_with("key in") {
                    let st = up.jira_status.lock().unwrap().get("OPS-1").cloned().unwrap_or("In Review".into());
                    return Json(json!({ "issues": [jira_issue("OPS-1", &st)], "isLast": true })).into_response();
                }
                match q.get("nextPageToken").map(String::as_str) {
                    None => Json(json!({ "issues": [jira_issue("OPS-1", "To Do"), jira_issue("OPS-2", "To Do")], "nextPageToken": "p2", "isLast": false })).into_response(),
                    Some("p2") => Json(json!({ "issues": [jira_issue("OPS-3", "To Do")], "isLast": true })).into_response(),
                    Some(_) => StatusCode::BAD_REQUEST.into_response(),
                }
            }),
        )
        .route(
            "/rest/api/3/issue/{key}",
            get(|State(up): State<Up>, AxPath(key): AxPath<String>| async move {
                let st = up.jira_status.lock().unwrap().get(&key).cloned().unwrap_or("In Review".into());
                Json(jira_issue(&key, &st))
            }),
        )
        .route(
            "/rest/api/3/issue/{key}/transitions",
            get(|| async {
                Json(json!({ "transitions": [
                    { "id": "51", "name": "Back to Progress", "to": { "name": "In Progress", "statusCategory": { "key": "indeterminate" } } },
                    { "id": "31", "name": "Close Issue", "to": { "name": "Done", "statusCategory": { "key": "done" } } }
                ] }))
            })
            .post(|State(up): State<Up>, AxPath(key): AxPath<String>, Json(body): Json<Value>| async move {
                if body["transition"]["id"] == "31" {
                    up.jira_status.lock().unwrap().insert(key, "Done".into());
                    StatusCode::NO_CONTENT.into_response()
                } else {
                    StatusCode::BAD_REQUEST.into_response()
                }
            }),
        )
        .route(
            "/rest/api/3/issue/{key}/comment",
            post(|| async { (StatusCode::CREATED, Json(json!({ "id": "10500", "created": "t" }))) }),
        )
        .route(
            "/rest/api/3/issue",
            post(|| async { (StatusCode::CREATED, Json(json!({ "id": "10600", "key": "OPS-600", "self": "x" }))) }),
        )
}

fn jira_dc() -> Router<Up> {
    Router::new()
        .route(
            "/rest/api/2/search",
            get(|Query(q): Query<BTreeMap<String, String>>| async move {
                assert_eq!(q.get("validateQuery").map(String::as_str), Some("warn"));
                match q.get("startAt").map(String::as_str) {
                    Some("0") => Json(json!({ "startAt": 0, "maxResults": 2, "total": 3, "issues": [jira_issue("DC-1", "Open"), jira_issue("DC-2", "Open")] })),
                    _ => Json(json!({ "startAt": 2, "maxResults": 2, "total": 3, "issues": [jira_issue("DC-3", "Open")] })),
                }
            }),
        )
        .route(
            "/rest/api/2/issue/{key}/comment",
            post(|| async { (StatusCode::CREATED, Json(json!({ "id": 501, "created": "t" }))) }),
        )
}

fn rest_api() -> Router<Up> {
    Router::new()
        .route(
            "/v1/teams/{team}/services",
            get(|headers: HeaderMap, AxPath(team): AxPath<String>, Query(q): Query<BTreeMap<String, String>>| async move {
                if headers.get("x-api-key").map(|v| v.to_str().unwrap()) != Some("svc_secret_e2e") {
                    return (StatusCode::UNAUTHORIZED, Json(json!({ "error": { "message": "bad api key" } }))).into_response();
                }
                if team == "empty" {
                    return Json(json!({ "data": [], "meta": { "next": null } })).into_response();
                }
                let svc = |id: &str, healthy: bool| json!({ "id": id, "type": "service", "attributes": { "name": format!("{team}-{id}"), "healthy": healthy, "owner": null } });
                match q.get("cursor").map(String::as_str) {
                    None => Json(json!({ "data": [svc("api", true), svc("web", false)], "meta": { "next": "c2" } })).into_response(),
                    Some("c2") => Json(json!({ "data": [svc("worker", true)], "meta": { "next": null } })).into_response(),
                    Some(_) => StatusCode::BAD_REQUEST.into_response(),
                }
            }),
        )
        .route(
            "/v1/deploys",
            post(|Json(body): Json<Value>| async move {
                (StatusCode::CREATED, [("location", "/v1/deploys/d-1")], Json(json!({ "id": "d-1", "env": body["env"], "ref": body["ref"] })))
            }),
        )
        .route("/v1/flaky", post(|| async { (StatusCode::SERVICE_UNAVAILABLE, "maintenance") }))
        // Same origin, outside the connector's base path: the broker must not follow it.
        .route(
            "/v1/moved",
            get(|| async { (StatusCode::FOUND, [("location", "/jira/rest/api/3/myself")]) }),
        )
        .route(
            "/v1/anything/{*rest}",
            get(|OriginalUri(uri): OriginalUri| async move { Json(json!({ "uri": uri.to_string() })) }),
        )
}

fn fal_api() -> Router<Up> {
    Router::new()
        .route("/fal-ai/flux/schnell", post(|| async {
            Json(json!({ "request_id": "r1", "queue_position": 2 }))
        }))
        .route("/fal-ai/flux/requests/r1/status", get(|| async {
            Json(json!({ "request_id": "r1", "status": "COMPLETED", "logs": [{ "message": "done" }] }))
        }))
        .route("/fal-ai/flux/requests/r1", get(|| async {
            Json(json!({ "images": [{ "url": "https://example.test/image" }] }))
        }))
        .route("/fal-ai/flux/requests/r1/cancel", put(|| async {
            (StatusCode::ACCEPTED, Json(json!({ "status": "CANCELLATION_REQUESTED" })))
        }))
        .route("/workflows/team/demo/requests/r2/status", get(|| async {
            Json(json!({ "request_id": "r2", "status": "IN_PROGRESS" }))
        }))
        .route("/team/busy", post(|| async { StatusCode::BAD_GATEWAY }))
}

fn hn_api() -> Router<Up> {
    Router::new()
        .route("/search", get(|| async {
            Json(json!({ "hits": [], "page": 0, "nbHits": 0, "nbPages": 0, "hitsPerPage": 20 }))
        }))
        .route("/search_by_date", get(|| async {
            Json(json!({ "hits": [{
                "objectID": "42", "_tags": ["comment", "story_9"], "title": null,
                "story_id": 9, "story_title": "A story", "comment_text": "A reply", "author": "reader"
            }], "page": 1, "nbHits": 12, "nbPages": 6, "hitsPerPage": 2 }))
        }))
        .route("/items/42", get(|| async {
            Json(json!({ "id": 42, "type": "comment", "children": [{ "id": 43, "text": "reply" }] }))
        }))
}

async fn start_upstream() -> (Up, String) {
    let up = Up::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .nest("/gh", github(base.clone()))
        .nest("/jira", jira_cloud())
        .nest("/jdc", jira_dc())
        .nest("/api", rest_api())
        .nest("/met", weather_api())
        .nest("/fx", currency_api())
        .nest("/fal", fal_api())
        .nest("/hn/api/v1", hn_api())
        .layer(axum::middleware::from_fn_with_state(up.clone(), recorder))
        .with_state(up.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (up, base)
}

// ---------------------------------------------------------------------------------------------
// Workspace wiring
// ---------------------------------------------------------------------------------------------

fn workspace(base: &str) -> Workspace {
    let w = Workspace::ecc();
    w.append_instances(
        "connectors.yaml",
        &format!(
            r#"  gh:
    definition: github.connector
    secrets:
      auth: secret://gh-token
    capabilities: [pull_requests.read, pull_requests.merge, issues.read, issues.write, checks.read, actions.read]
    config:
      base_url: {base}/gh
  jira_cloud:
    definition: jira.connector
    secrets:
      auth: secret://jira-cloud
    capabilities: [issues.read, issues.transition, issues.write]
    config:
      base_url: {base}/jira
      page_size: 2
  jira_dc:
    definition: jira.connector
    secrets:
      auth: secret://jira-dc
    capabilities: [issues.read, issues.write]
    config:
      base_url: {base}/jdc
      api_version: "2"
      page_size: 2
  fal:
    definition: dash.fal
    secrets: {{ auth: secret://fal-token }}
    capabilities: [queue.submit, queue.read, queue.cancel]
    config:
      base_url: {base}/fal
      mock:
        "POST /fal-ai/flux/schnell": {{ request_id: preview1, status: IN_QUEUE }}
  hn:
    definition: dash.hacker_news
    capabilities: [items.read]
    config:
      base_url: {base}/hn/api/v1
      mock:
        "GET /search?tags=front_page": {{ hits: [], page: 0, nbHits: 0, nbPages: 0, hitsPerPage: 20 }}
        "GET /items/12": {{ status: 429, headers: {{ retry-after: "10" }}, body: {{ message: slow down }} }}
  svc_api:
    definition: dash.http
    secrets:
      auth: secret://svc-key
    capabilities: [http.read, http.write]
    config:
      base_url: {base}/api/v1
      headers:
        X-Team: core
      mock:
        "GET /teams/core/services":
          data:
            - {{ id: mocked, attributes: {{ name: core-mocked, healthy: true }} }}
        "GET /teams/gone/services":
          status: 404
          body: {{ error: {{ message: team deleted }} }}
"#
        ),
    );
    w.append_instances(
        "data.yaml",
        r#"  team_services:
    definition: dash.http_query
    inputs:
      api: svc_api
    config:
      path: /teams/{team}/services
      paginate:
        mode: cursor
        items_path: data
        cursor_path: meta.next
        cursor_param: cursor
        size_param: limit
        page_size: 2
      fields:
        id: id
        name: attributes.name
        healthy: attributes.healthy
  gh_ci:
    definition: github.ci_status
    inputs:
      github: gh
  jira_open:
    definition: jira.search
    inputs:
      jira: jira_cloud
"#,
    );
    w.append_instances(
        "actions.yaml",
        r#"  deploy:
    definition: dash.http_action
    inputs:
      api: svc_api
    config:
      method: POST
      path: /deploys
      body:
        env: prod
      idempotency_header: Idempotency-Key
  flaky_write:
    definition: dash.http_action
    inputs:
      api: svc_api
    config:
      path: /flaky
  gh_merge:
    definition: github.merge_pr
    inputs:
      github: gh
  gh_close:
    definition: github.set_issue_state
    inputs:
      github: gh
  gh_issue:
    definition: github.create_issue
    inputs:
      github: gh
  jira_done:
    definition: jira.transition_issue
    inputs:
      jira: jira_cloud
"#,
    );
    let toml_path = w.ws.join("dashboard.toml");
    let text = std::fs::read_to_string(&toml_path).unwrap().replace(
        "allowed_hosts = [\"api.github.com\", \"jira.example.com\"]",
        "allowed_hosts = [\"127.0.0.1\"]\nallow_loopback_for_tests = true",
    );
    assert!(text.contains("allow_loopback_for_tests"));
    std::fs::write(&toml_path, text).unwrap();

    let store = SqliteStore::open(&w.data).unwrap();
    let put = |name: &str, plaintext: &str, binding: &str, scheme: Option<&str>| {
        store
            .set_secret_with_policy(name, name, plaintext, &[binding.to_string()], None, scheme)
            .unwrap();
    };
    put("fal-token", "fal_e2e_key", "fal", Some("key"));
    put("gh-token", "ghp_e2e_token_123", "gh", None);
    put(
        "jira-cloud",
        "ops@acme.test:atl_e2e_token",
        "jira_cloud",
        Some("basic"),
    );
    put("jira-dc", "dc_pat_e2e", "jira_dc", None);
    put(
        "svc-key",
        "svc_secret_e2e",
        "svc_api",
        Some("header:X-Api-Key"),
    );
    store
        .set_network_policy_ceiling(
            json!({
                "allowed_hosts": ["127.0.0.1"],
                "allowed_methods": ["GET", "POST", "PUT", "PATCH"],
                "allow_loopback_for_tests": true
            }),
            "admin",
        )
        .unwrap();
    w
}

fn admin() -> Principal {
    Principal::administrator()
}

async fn connector(b: &Booted, id: &str, op: &str, args: Value) -> Result<Value, String> {
    b.services
        .engine()
        .queries
        .invoke_connector(id, op, args, &admin(), "trace-e2e")
        .await
}

async fn query(b: &Booted, id: &str, params: Value) -> Result<Value, String> {
    b.services
        .provider_query(id, params, &admin(), true)
        .await
        .map(|env| env.value.unwrap_or(Value::Null))
}

// ---------------------------------------------------------------------------------------------
// GitHub
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn github_reads_normalize_real_payloads_across_pages() {
    let (up, base) = start_upstream().await;
    let w = workspace(&base);
    let b = boot(&w, false, ResourceBudget::default()).await;

    let prs = connector(&b, "gh", "list_prs", json!({ "repository": "acme/api" }))
        .await
        .unwrap();
    let prs = prs.as_array().unwrap();
    assert_eq!(prs.len(), 3, "both Link pages: {prs:?}");
    let first = &prs[0];
    assert_eq!(first["id"], "acme/api#1");
    assert_eq!(first["repository"], "acme/api");
    assert_eq!(first["state"], "open");
    assert_eq!(first["author"], "alice");
    assert_eq!(first["labels"], json!(["bug"]));
    assert_eq!(first["requested_reviewers"], json!(["bob"]));
    assert_eq!(first["head_sha"], "head1");
    assert_eq!(prs[1]["labels"], json!([]), "empty lists stay lists");
    // JSON nulls (merged_at, closed_at, body) never leak through as values.
    for k in ["merged_at", "closed_at", "body"] {
        assert!(first.get(k).is_none_or(Value::is_null), "{k}: {first}");
    }
    assert_eq!(prs[2]["draft"], true);

    let pages = up.find("GET", "/gh/repos/acme/api/pulls");
    assert_eq!(pages.len(), 2);
    assert_eq!(
        pages[0].uri,
        "/gh/repos/acme/api/pulls?per_page=100&state=open"
    );
    let h = &pages[0].headers;
    assert_eq!(h["authorization"], "Bearer ghp_e2e_token_123");
    assert_eq!(h["accept"], "application/vnd.github+json");
    assert_eq!(h["x-github-api-version"], "2022-11-28");

    let merged = connector(
        &b,
        "gh",
        "list_prs",
        json!({ "repository": "acme/api", "state": "merged" }),
    )
    .await
    .unwrap();
    assert_eq!(merged.as_array().unwrap().len(), 1);
    assert_eq!(merged[0]["state"], "merged");
    assert_eq!(merged[0]["merged"], true);

    let issues = connector(
        &b,
        "gh",
        "list_issues",
        json!({ "repository": "acme/api", "labels": ["bug"] }),
    )
    .await
    .unwrap();
    assert_eq!(
        issues.as_array().unwrap().len(),
        1,
        "pull requests dropped: {issues}"
    );
    assert_eq!(issues[0]["comments"], 4);
    assert!(
        up.find("GET", "/gh/repos/acme/api/issues?labels=bug&")
            .len()
            == 1
    );

    let found = connector(
        &b,
        "gh",
        "search_issues",
        json!({ "query": "is:pr review-requested:@me" }),
    )
    .await
    .unwrap();
    assert_eq!(found[0]["repository"], "acme/web");
    assert_eq!(found[0]["is_pull_request"], true);
    assert_eq!(
        up.find("GET", "/gh/search/issues")[0].uri,
        "/gh/search/issues?per_page=100&q=is%3Apr%20review-requested%3A%40me"
    );

    let ci = query(
        &b,
        "gh_ci",
        json!({ "repository": "acme/api", "ref": "release/1.0" }),
    )
    .await
    .unwrap();
    assert_eq!(
        ci["state"], "failure",
        "a timed-out check run fails the ref: {ci}"
    );
    assert_eq!(ci["total"], 3);
    assert_eq!(ci["success"], 2);
    assert!(
        up.find("GET", "/gh/repos/acme/api/commits/release%2F1.0/status")
            .len()
            == 1
    );

    let runs = connector(
        &b,
        "gh",
        "list_workflow_runs",
        json!({ "repository": "acme/api" }),
    )
    .await
    .unwrap();
    assert_eq!(runs[0]["id"], 30_000_000_001_i64);
    assert_eq!(runs[0]["actor"], "carol");
    assert!(runs[0].get("conclusion").is_none_or(Value::is_null));

    let limited = connector(
        &b,
        "gh",
        "list_prs",
        json!({ "repository": "acme/limited" }),
    )
    .await
    .unwrap_err();
    assert!(limited.contains("RATE_LIMITED"), "{limited}");
    assert!(limited.contains("resets at epoch 1767225600"), "{limited}");
    assert!(!limited.contains("ghp_e2e_token_123"));

    let bad = connector(&b, "gh", "list_prs", json!({ "repository": "../../admin" }))
        .await
        .unwrap_err();
    assert!(bad.contains("INVALID_ARGS"), "{bad}");
}

#[tokio::test]
async fn github_writes_merge_safely_and_classify_outcomes() {
    let (up, base) = start_upstream().await;
    let w = workspace(&base);
    let b = boot(&w, false, ResourceBudget::default()).await;
    let s = &b.services;

    // Dry run reads mergeability; a conflicting PR is infeasible.
    let dirty = s
        .action_dry_run(
            "gh_merge",
            json!({ "repository": "acme/api", "number": 2 }),
            &admin(),
        )
        .await
        .unwrap();
    assert!(!dirty.feasible, "{dirty:?}");
    assert!(
        dirty.warnings.iter().any(|w| w.contains("merge conflicts")),
        "{dirty:?}"
    );
    let draft = s
        .action_dry_run(
            "gh_merge",
            json!({ "repository": "acme/api", "number": 3 }),
            &admin(),
        )
        .await
        .unwrap();
    assert!(
        !draft.feasible && draft.warnings.iter().any(|w| w.contains("draft")),
        "{draft:?}"
    );
    assert!(up.find("PUT", "/gh/").is_empty(), "Dry runs never write");

    // The merge carries the head SHA it just read.
    let ok = s
        .action_invoke(
            "gh_merge",
            json!({ "repository": "acme/api", "number": 1 }),
            None,
            &admin(),
        )
        .await
        .unwrap();
    assert_eq!(ok.status, "succeeded", "{ok:?}");
    assert_eq!(ok.result.as_ref().unwrap()["sha"], "merge1");
    let put = up.find("PUT", "/gh/repos/acme/api/pulls/1/merge");
    assert_eq!(
        put[0].body,
        json!({ "merge_method": "squash", "sha": "head1" })
    );

    // A head that moved since review is refused before any write.
    let stale = s
        .action_invoke(
            "gh_merge",
            json!({ "repository": "acme/api", "number": 1, "expected_head_sha": "reviewed-sha" }),
            None,
            &admin(),
        )
        .await
        .unwrap();
    assert_eq!(stale.status, "failed", "{stale:?}");
    assert!(
        stale.reason.as_deref().unwrap_or("").contains("moved"),
        "{stale:?}"
    );
    assert_eq!(
        up.find("PUT", "/gh/repos/acme/api/pulls/1/merge").len(),
        1,
        "no second merge attempt"
    );

    // 405 (not mergeable) is a definitive failure; a 502 leaves the outcome unknown.
    let refused = s
        .action_invoke(
            "gh_merge",
            json!({ "repository": "acme/api", "number": 2 }),
            None,
            &admin(),
        )
        .await
        .unwrap();
    assert_eq!(refused.status, "failed", "{refused:?}");
    assert!(
        refused
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("NOT_MERGEABLE"),
        "{refused:?}"
    );
    let lost = s
        .action_invoke(
            "gh_merge",
            json!({ "repository": "acme/api", "number": 4 }),
            None,
            &admin(),
        )
        .await
        .unwrap();
    assert_eq!(lost.status, "unknown", "{lost:?}");

    // Issue writes send exactly the given fields; an empty list stays a JSON list.
    let created = s
        .action_invoke(
            "gh_issue",
            json!({ "repository": "acme/api", "title": "Pager fired", "labels": ["incident"] }),
            None,
            &admin(),
        )
        .await
        .unwrap();
    assert_eq!(created.status, "succeeded", "{created:?}");
    assert_eq!(created.result.as_ref().unwrap()["number"], 12);
    assert_eq!(
        up.find("POST", "/gh/repos/acme/api/issues")[0].body,
        json!({ "title": "Pager fired", "labels": ["incident"] })
    );
    connector(
        &b,
        "gh",
        "update_issue",
        json!({ "repository": "acme/api", "number": 10, "labels": [] }),
    )
    .await
    .unwrap();
    assert_eq!(
        up.find("PATCH", "/gh/repos/acme/api/issues/10")[0].body,
        json!({ "labels": [] })
    );

    let closed = s
        .action_invoke(
            "gh_close",
            json!({ "repository": "acme/api", "number": 10, "state": "closed" }),
            None,
            &admin(),
        )
        .await
        .unwrap();
    assert_eq!(closed.status, "succeeded", "{closed:?}");
    assert_eq!(closed.result.as_ref().unwrap()["changed"], true);
    let patches = up.find("PATCH", "/gh/repos/acme/api/issues/10");
    assert_eq!(
        patches.last().unwrap().body,
        json!({ "state": "closed", "state_reason": "completed" })
    );

    let comment = connector(
        &b,
        "gh",
        "add_comment",
        json!({ "repository": "acme/api", "number": 10, "body": "ack" }),
    )
    .await
    .unwrap();
    assert_eq!(comment["id"], 9001);
}

// ---------------------------------------------------------------------------------------------
// Jira
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn jira_cloud_and_server_paths_work_end_to_end() {
    let (up, base) = start_upstream().await;
    let w = workspace(&base);
    let b = boot(&w, false, ResourceBudget::default()).await;
    let s = &b.services;

    // Cloud: Basic email:token, /search/jql with nextPageToken.
    let open = query(
        &b,
        "jira_open",
        json!({ "jql": "project = OPS AND statusCategory != Done" }),
    )
    .await
    .unwrap();
    let open = open.as_array().unwrap();
    assert_eq!(open.len(), 3, "{open:?}");
    assert_eq!(open[0]["key"], "OPS-1");
    assert_eq!(open[0]["status_category"], "todo");
    assert_eq!(open[0]["url"], format!("{base}/jira/browse/OPS-1"));
    assert!(
        open[0].get("assignee").is_none_or(Value::is_null),
        "null assignee: {}",
        open[0]
    );
    let searches = up.find("GET", "/jira/rest/api/3/search/jql");
    let expected = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("ops@acme.test:atl_e2e_token")
    );
    assert_eq!(searches[0].headers["authorization"], expected);
    assert!(
        searches[1].uri.contains("nextPageToken=p2"),
        "{}",
        searches[1].uri
    );

    // Unknown keys are dropped and the query asked again.
    let issues = connector(
        &b,
        "jira_cloud",
        "get_issues",
        json!({ "keys": ["OPS-1", "OPS-404"] }),
    )
    .await
    .unwrap();
    assert_eq!(issues.as_array().unwrap().len(), 1);
    assert_eq!(issues[0]["status"], "In Review");

    // Transition by target status through the workflow, with an ADF comment.
    let dry = s
        .action_dry_run(
            "jira_done",
            json!({ "key": "OPS-1", "comment": "Shipped" }),
            &admin(),
        )
        .await
        .unwrap();
    assert!(dry.feasible, "{dry:?}");
    assert_eq!(dry.effects["transition"], "Close Issue");
    let done = s
        .action_invoke(
            "jira_done",
            json!({ "key": "OPS-1", "comment": "Shipped", "resolution": "Fixed" }),
            None,
            &admin(),
        )
        .await
        .unwrap();
    assert_eq!(done.status, "succeeded", "{done:?}");
    assert_eq!(done.result.as_ref().unwrap()["status"], "Done");
    let posted = &up.find("POST", "/jira/rest/api/3/issue/OPS-1/transitions")[0].body;
    assert_eq!(posted["transition"], json!({ "id": "31" }));
    assert_eq!(posted["fields"]["resolution"]["name"], "Fixed");
    assert_eq!(posted["update"]["comment"][0]["add"]["body"]["type"], "doc");
    // Repeating is a no-op: no second POST.
    let again = s
        .action_invoke("jira_done", json!({ "key": "OPS-1" }), None, &admin())
        .await
        .unwrap();
    assert_eq!(again.result.as_ref().unwrap()["transitioned"], false);
    assert_eq!(
        up.find("POST", "/jira/rest/api/3/issue/OPS-1/transitions")
            .len(),
        1
    );

    let created = connector(&b, "jira_cloud", "create_issue", json!({
        "project": "OPS", "issue_type": "Bug", "summary": "Pager fired", "description": "Line 1\nLine 2", "labels": []
    })).await.unwrap();
    assert_eq!(created["key"], "OPS-600");
    let body = &up
        .find("POST", "/jira/rest/api/3/issue")
        .into_iter()
        .find(|s| s.uri == "/jira/rest/api/3/issue")
        .unwrap()
        .body;
    assert_eq!(body["fields"]["project"], json!({ "key": "OPS" }));
    assert_eq!(body["fields"]["labels"], json!([]));
    assert_eq!(
        body["fields"]["description"]["content"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    // Server/Data Center: Bearer PAT, v2 /search with startAt, plain-text comments.
    let dc = connector(
        &b,
        "jira_dc",
        "search",
        json!({ "jql": "project = DC", "fields": ["customfield_10016"] }),
    )
    .await
    .unwrap();
    assert_eq!(dc.as_array().unwrap().len(), 3);
    assert_eq!(dc[0]["fields"]["customfield_10016"], 3);
    let dc_search = up.find("GET", "/jdc/rest/api/2/search");
    assert_eq!(dc_search[0].headers["authorization"], "Bearer dc_pat_e2e");
    assert!(dc_search[1].uri.contains("startAt=2"));
    connector(
        &b,
        "jira_dc",
        "add_comment",
        json!({ "key": "DC-1", "body": "hello" }),
    )
    .await
    .unwrap();
    assert_eq!(
        up.find("POST", "/jdc/rest/api/2/issue/DC-1/comment")[0].body,
        json!({ "body": "hello" })
    );
}

// ---------------------------------------------------------------------------------------------
// Generic REST/JSON
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn generic_rest_connector_reads_writes_and_stays_on_its_base_url() {
    let (up, base) = start_upstream().await;
    let w = workspace(&base);
    let b = boot(&w, false, ResourceBudget::default()).await;
    let s = &b.services;

    let services = query(&b, "team_services", json!({ "team": "core" }))
        .await
        .unwrap();
    assert_eq!(
        services,
        json!([
            { "id": "api", "name": "core-api", "healthy": true },
            { "id": "web", "name": "core-web", "healthy": false },
            { "id": "worker", "name": "core-worker", "healthy": true }
        ])
    );
    let reads = up.find("GET", "/api/v1/teams/core/services");
    assert_eq!(reads.len(), 2);
    assert_eq!(reads[0].uri, "/api/v1/teams/core/services?limit=2");
    assert_eq!(
        reads[1].uri,
        "/api/v1/teams/core/services?cursor=c2&limit=2"
    );
    assert_eq!(reads[0].headers["x-api-key"], "svc_secret_e2e");
    assert_eq!(reads[0].headers["x-team"], "core");
    assert!(!reads[0].headers.contains_key("authorization"));

    // An empty paginated result is a JSON list, like a non-empty one.
    assert_eq!(
        query(&b, "team_services", json!({ "team": "empty" }))
            .await
            .unwrap(),
        json!([])
    );

    // A same-origin redirect outside base_url is refused before the credential goes anywhere.
    let err = connector(&b, "svc_api", "get", json!({ "path": "/moved" }))
        .await
        .unwrap_err();
    assert!(err.contains("leaves the connector's base URL"), "{err}");
    assert!(up.find("GET", "/jira/rest/api/3/myself").is_empty());

    // Template values cannot climb out of their path segment.
    let err = query(&b, "team_services", json!({ "team": "../admin" }))
        .await
        .unwrap_err();
    assert!(err.contains("INVALID_ARGS"), "{err}");
    let err = connector(&b, "svc_api", "get", json!({ "path": "//evil.test/x" }))
        .await
        .unwrap_err();
    assert!(err.contains("INVALID_ARGS"), "{err}");
    assert!(up.find("GET", "/api/v1/teams/..").is_empty());

    // Writes: Dry run shows the request; execution sends JSON with the idempotency key.
    let dry = s
        .action_dry_run("deploy", json!({ "ref": "v1.2.3" }), &admin())
        .await
        .unwrap();
    assert_eq!(
        dry.effects["body"],
        json!({ "env": "prod", "ref": "v1.2.3" })
    );
    assert!(up.find("POST", "/api/v1/deploys").is_empty());
    let rec = s
        .action_invoke(
            "deploy",
            json!({ "ref": "v1.2.3" }),
            Some("deploy-1".into()),
            &admin(),
        )
        .await
        .unwrap();
    assert_eq!(rec.status, "succeeded", "{rec:?}");
    let result = rec.result.unwrap();
    assert_eq!(result["status"], 201);
    assert_eq!(result["location"], "/v1/deploys/d-1");
    assert_eq!(result["body"]["ref"], "v1.2.3");
    let sent = &up.find("POST", "/api/v1/deploys")[0];
    assert_eq!(sent.body, json!({ "env": "prod", "ref": "v1.2.3" }));
    assert!(
        sent.headers.contains_key("idempotency-key"),
        "{:?}",
        sent.headers
    );

    // A 5xx answer to a write is an unknown outcome, never a plain failure.
    let flaky = s
        .action_invoke("flaky_write", json!({}), None, &admin())
        .await
        .unwrap();
    assert_eq!(flaky.status, "unknown", "{flaky:?}");
}

#[tokio::test]
async fn generic_rest_connector_serves_configured_mock_responses_in_preview() {
    let (up, base) = start_upstream().await;
    let w = workspace(&base);
    let b = boot(&w, true, ResourceBudget::default()).await;
    let services = query(&b, "team_services", json!({ "team": "core" }))
        .await
        .unwrap();
    assert_eq!(
        services,
        json!([{ "id": "mocked", "name": "core-mocked", "healthy": true }])
    );
    let err = query(&b, "team_services", json!({ "team": "other" }))
        .await
        .unwrap_err();
    assert!(err.contains("MOCK_NOT_CONFIGURED"), "{err}");
    let err = query(&b, "team_services", json!({ "team": "gone" }))
        .await
        .unwrap_err();
    assert!(
        err.contains("NOT_FOUND") && err.contains("team deleted"),
        "mock envelopes map like live errors: {err}"
    );
    assert!(
        up.seen.lock().unwrap().is_empty(),
        "Preview never reaches the upstream"
    );
}

#[tokio::test]
async fn fal_queue_uses_key_auth_and_model_scoped_request_paths() {
    let (up, base) = start_upstream().await;
    let w = workspace(&base);
    let b = boot(&w, false, ResourceBudget::default()).await;
    let endpoint = "fal-ai/flux/schnell";
    let input = json!({ "prompt": "a landscape", "seed": null });
    let queued = connector(
        &b,
        "fal",
        "submit",
        json!({ "endpoint_id": endpoint, "input": input }),
    )
    .await
    .unwrap();
    assert_eq!(queued["queue_position"], 2);
    let sent = &up.find("POST", "/fal/fal-ai/flux/schnell")[0];
    assert_eq!(sent.body, input);
    assert_eq!(sent.headers["authorization"], "Key fal_e2e_key");

    let job = json!({ "endpoint_id": endpoint, "request_id": queued["request_id"] });
    let mut with_logs = job.clone();
    with_logs["logs"] = json!(true);
    let status = connector(&b, "fal", "status", with_logs).await.unwrap();
    assert_eq!(status["logs"][0]["message"], "done");
    assert_eq!(
        up.find("GET", "/fal/fal-ai/flux/requests/r1/status?logs=1")
            .len(),
        1
    );
    let result = connector(&b, "fal", "result", job.clone()).await.unwrap();
    assert_eq!(result["images"][0]["url"], "https://example.test/image");
    let cancel = connector(&b, "fal", "cancel", job).await.unwrap();
    assert_eq!(cancel["status"], "CANCELLATION_REQUESTED");

    let status = connector(
        &b,
        "fal",
        "status",
        json!({
            "endpoint_id": "workflows/team/demo", "request_id": "r2"
        }),
    )
    .await
    .unwrap();
    assert_eq!(status["status"], "IN_PROGRESS");
    let err = connector(
        &b,
        "fal",
        "submit",
        json!({ "endpoint_id": "team/busy", "input": {} }),
    )
    .await
    .unwrap_err();
    assert!(err.contains("UNKNOWN_OUTCOME"), "{err}");
    let before = up.seen.lock().unwrap().len();
    for (endpoint, request_id) in [
        ("owner/../alias", "r1"),
        ("owner/app", "r1/status"),
        ("owner/app%2fextra", "r1"),
    ] {
        let err = connector(
            &b,
            "fal",
            "result",
            json!({ "endpoint_id": endpoint, "request_id": request_id }),
        )
        .await
        .unwrap_err();
        assert!(err.contains("INVALID_ARGS"), "{err}");
    }
    assert_eq!(up.seen.lock().unwrap().len(), before);
}

#[tokio::test]
async fn hacker_news_search_and_item_lookup_are_anonymous() {
    let (up, base) = start_upstream().await;
    let w = workspace(&base);
    let b = boot(&w, false, ResourceBudget::default()).await;
    let search = connector(
        &b,
        "hn",
        "search",
        json!({
            "query": "C++ & Rust", "sort": "date", "tags": "comment,story_9",
            "numeric_filters": "created_at_i>100", "page": 1, "hits_per_page": 2
        }),
    )
    .await
    .unwrap();
    assert_eq!(search["total_pages"], 6);
    assert_eq!(search["total_hits"], 12);
    assert_eq!(search["hits"][0]["text"], "A reply");
    assert_eq!(
        search["hits"][0]["discussion_url"],
        "https://news.ycombinator.com/item?id=42"
    );
    let sent = &up.find("GET", "/hn/api/v1/search_by_date")[0];
    assert_eq!(sent.uri, "/hn/api/v1/search_by_date?hitsPerPage=2&numericFilters=created_at_i%3E100&page=1&query=C%2B%2B%20%26%20Rust&tags=comment%2Cstory_9");
    assert!(!sent.headers.contains_key("authorization"));
    let empty = connector(&b, "hn", "search", json!({ "sort": null }))
        .await
        .unwrap();
    assert_eq!(empty["hits"], json!([]));

    let item = connector(
        &b,
        "hn",
        "get_item",
        json!({ "id": search["hits"][0]["id"] }),
    )
    .await
    .unwrap();
    assert_eq!(item["children"][0]["id"], 43);
    let missing = connector(&b, "hn", "get_item", json!({ "id": 123 }))
        .await
        .unwrap_err();
    assert!(missing.contains("NOT_FOUND"), "{missing}");
}

#[tokio::test]
async fn fal_and_hacker_news_use_http_fixtures_in_preview() {
    let (up, base) = start_upstream().await;
    let w = workspace(&base);
    let b = boot(&w, true, ResourceBudget::default()).await;
    let job = connector(
        &b,
        "fal",
        "submit",
        json!({ "endpoint_id": "fal-ai/flux/schnell", "input": {} }),
    )
    .await
    .unwrap();
    assert_eq!(job["request_id"], "preview1");
    let search = connector(&b, "hn", "search", json!({ "tags": "front_page" }))
        .await
        .unwrap();
    assert_eq!(search["hits"], json!([]));
    let err = connector(&b, "hn", "search", json!({})).await.unwrap_err();
    assert!(err.contains("MOCK_NOT_CONFIGURED"), "{err}");
    let err = connector(&b, "hn", "get_item", json!({ "id": 12 }))
        .await
        .unwrap_err();
    assert!(
        err.contains("RATE_LIMITED") && err.contains("retry after 10s"),
        "{err}"
    );
    assert!(up.seen.lock().unwrap().is_empty());
}

fn met_forecast() -> Value {
    let period = |time: &str, temperature: i32| {
        json!({
            "time": time,
            "data": {
                "instant": { "details": { "air_temperature": temperature } },
                "next_6_hours": { "summary": { "symbol_code": "cloudy" }, "details": { "precipitation_amount": 1.2 } }
            }
        })
    };
    json!({
        "type": "Feature", "geometry": { "type": "Point", "coordinates": [-13.4051, 52.52, 30] },
        "properties": {
            "meta": { "updated_at": "2026-11-02T03:45:00Z",
                      "units": { "air_temperature": "celsius", "precipitation_amount": "mm" } },
            "timeseries": [
                period("2026-11-02T04:00:00Z", 8), period("2026-11-02T05:00:00Z", 10),
                period("2026-11-02T06:00:00Z", 11)
            ]
        }
    })
}

fn weather_api() -> Router<Up> {
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;
    Router::new().route(
        "/compact",
        get(|| async {
            let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
            gzip.write_all(&serde_json::to_vec(&met_forecast()).unwrap())
                .unwrap();
            (
                [
                    ("content-encoding", "gzip"),
                    ("content-type", "application/json"),
                ],
                gzip.finish().unwrap(),
            )
        }),
    )
}

fn fx_rate() -> Value {
    json!({ "date": "2026-10-30", "base": "EUR", "quote": "USD", "rate": 1.125 })
}

fn currency_api() -> Router<Up> {
    Router::new()
        .route("/rate/eur/usd", get(|| async { Json(fx_rate()) }))
        .route(
            "/providers/ecb/rate/eur/usd",
            get(|| async { Json(fx_rate()) }),
        )
        .route(
            "/providers/ecb/rates",
            get(|| async { Json(json!([fx_rate()])) }),
        )
        .route("/rates", get(|| async { Json(json!([fx_rate()])) }))
        .route(
            "/currencies",
            get(|| async {
                Json(json!([
                    { "iso_code": "EUR", "name": "Euro", "type": "currency" }
                ]))
            }),
        )
}

fn weather_currency_workspace(base: &str) -> Workspace {
    let w = workspace(base);
    let sample = met_forecast();
    let rate = fx_rate();
    w.append_instances(
        "connectors.yaml",
        &format!(
            r#"  weather:
    definition: dash.weather
    capabilities: [forecast.read]
    config:
      base_url: {base}/met
      user_agent: WeatherTest/1 https://example.com/contact
      mock_now: "2026-11-02T05:30:00Z"
      mock:
        "GET /compact?lat=52.52&lon=-13.4051": {sample}
  fx:
    definition: dash.currency
    capabilities: [rates.read]
    config:
      base_url: {base}/fx
      mock:
        "GET /providers/ecb/rate/eur/usd?date=2026-11-01": {rate}
        "GET /rates": []
        "GET /currencies": {{ status: 429, headers: {{ retry-after: "15" }} }}
"#
        ),
    );
    w
}

#[tokio::test]
async fn weather_normalizes_coordinates_decompresses_and_caches_by_location() {
    let (up, base) = start_upstream().await;
    let w = weather_currency_workspace(&base);
    let b = boot(&w, false, ResourceBudget::default()).await;
    let args = json!({ "latitude": 52.52009, "longitude": -13.40519 });
    let forecast = connector(&b, "weather", "forecast", args).await.unwrap();
    assert_eq!(
        forecast["location"],
        json!({"latitude":52.52, "longitude":-13.4051, "altitude":30})
    );
    assert_eq!(
        forecast["timeseries"][0]["next_6_hours"]["summary"]["symbol_code"],
        "cloudy"
    );
    assert_eq!(forecast["units"]["air_temperature"], "celsius");

    connector(
        &b,
        "weather",
        "forecast",
        json!({ "latitude": 52.52, "longitude": -13.4051 }),
    )
    .await
    .unwrap();
    let sent = up.find("GET", "/met/compact");
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].uri, "/met/compact?lat=52.52&lon=-13.4051");
    assert_eq!(
        sent[0].headers["user-agent"],
        "WeatherTest/1 https://example.com/contact"
    );
    assert!(sent[0].headers["accept-encoding"].contains("gzip"));
    assert!(!sent[0].headers.contains_key("authorization"));

    connector(
        &b,
        "weather",
        "forecast",
        json!({ "latitude": 52.52, "longitude": -13.4051, "altitude": 120 }),
    )
    .await
    .unwrap();
    assert_eq!(up.find("GET", "/met/compact?altitude=120").len(), 1);
    let err = connector(
        &b,
        "weather",
        "forecast",
        json!({ "latitude": -91, "longitude": 12 }),
    )
    .await
    .unwrap_err();
    assert!(err.contains("INVALID_ARGS"), "{err}");
    assert_eq!(up.find("GET", "/met/compact").len(), 2);
}

#[tokio::test]
async fn frankfurter_reads_converts_and_preserves_observation_dates() {
    let (up, base) = start_upstream().await;
    let w = weather_currency_workspace(&base);
    let b = boot(&w, false, ResourceBudget::default()).await;
    let pair = json!({ "base": "eur", "quote": "Usd" });
    assert_eq!(connector(&b, "fx", "rate", pair).await.unwrap(), fx_rate());
    let converted = connector(
        &b,
        "fx",
        "convert",
        json!({
            "base": "EUR", "quote": "USD", "date": "2026-11-01", "provider": "ECB", "amount": -10
        }),
    )
    .await
    .unwrap();
    assert_eq!(converted["date"], "2026-10-30");
    assert_eq!(converted["converted_amount"], -11.25);
    assert_eq!(
        up.find("GET", "/fx/providers/ecb/rate/eur/usd?date=2026-11-01")
            .len(),
        1
    );

    let rates = connector(
        &b,
        "fx",
        "rates",
        json!({ "base": "eur", "quotes": ["usd", "gbp"] }),
    )
    .await
    .unwrap();
    assert_eq!(rates, json!([fx_rate()]));
    assert_eq!(
        up.find("GET", "/fx/rates?base=EUR&quotes=USD%2CGBP").len(),
        1
    );
    let series = connector(&b, "fx", "time_series", json!({
        "base": "EUR", "quotes": ["USD"], "from": "2026-10-29", "to": "2026-11-01", "provider": "ecb"
    })).await.unwrap();
    assert_eq!(series[0]["date"], "2026-10-30");
    assert_eq!(
        up.find(
            "GET",
            "/fx/providers/ecb/rates?base=EUR&from=2026-10-29&quotes=USD&to=2026-11-01"
        )
        .len(),
        1
    );
    let currencies = connector(&b, "fx", "currencies", json!({})).await.unwrap();
    assert_eq!(currencies[0]["iso_code"], "EUR");
    let before = up.find("GET", "/fx");
    assert!(before
        .iter()
        .all(|r| !r.headers.contains_key("authorization")));
    for args in [
        json!({ "base": "EUR", "quote": "USD", "date": "2026-02-30" }),
        json!({ "base": "EUR", "quote": "../USD" }),
        json!({ "base": "EUR", "quote": "USD", "provider": "ecb/other" }),
    ] {
        let err = connector(&b, "fx", "rate", args).await.unwrap_err();
        assert!(err.contains("INVALID_ARGS"), "{err}");
    }
    assert_eq!(up.find("GET", "/fx").len(), before.len());
}

#[tokio::test]
async fn weather_and_currency_preview_use_offline_fixtures() {
    let (up, base) = start_upstream().await;
    let w = weather_currency_workspace(&base);
    let b = boot(&w, true, ResourceBudget::default()).await;
    let current = connector(
        &b,
        "weather",
        "current",
        json!({ "latitude":52.52, "longitude":-13.4051 }),
    )
    .await
    .unwrap();
    assert_eq!(current["period"]["details"]["air_temperature"], 10);
    assert_eq!(current["attribution"], "Based on data from MET Norway");
    let missing = connector(
        &b,
        "weather",
        "forecast",
        json!({ "latitude":0, "longitude":0 }),
    )
    .await
    .unwrap_err();
    assert!(missing.contains("MOCK_NOT_CONFIGURED"), "{missing}");
    let converted = connector(
        &b,
        "fx",
        "convert",
        json!({
            "base": "EUR", "quote": "USD", "amount": 4, "date": "2026-11-01", "provider": "ecb"
        }),
    )
    .await
    .unwrap();
    assert_eq!(converted["converted_amount"], 4.5);
    assert_eq!(
        connector(&b, "fx", "rates", json!({ "base": "EUR" }))
            .await
            .unwrap(),
        json!([])
    );
    let limited = connector(&b, "fx", "currencies", json!({}))
        .await
        .unwrap_err();
    assert!(
        limited.contains("RATE_LIMITED") && limited.contains("retry after 15s"),
        "{limited}"
    );
    assert!(up.seen.lock().unwrap().is_empty());
}
