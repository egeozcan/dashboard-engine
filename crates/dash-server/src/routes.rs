//! Axum HTTP router, REST API, auth endpoints, custom widget asset sandbox serving,
//! embedded React UI assets (`rust-embed`), and CSP headers (§10.1, §14).

use crate::auth::{
    authenticate_request, build_session_cookie, create_browser_session, extract_cookie,
    is_request_https, verify_password, LoginRateLimiter,
};
use crate::mcp::{mcp_delete_handler, mcp_get_handler, mcp_post_handler};
use crate::services::AppServices;
use crate::ws::ws_handler;
use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use dash_core::{CapabilityCeiling, DashboardDoc, InstanceConfig, WidgetRequestEnvelope};
use dash_git::PackageDependencySpec;
use rust_embed::RustEmbed;
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;

#[derive(RustEmbed)]
#[folder = "../../web/dist"]
struct WebAssets;

#[derive(Clone)]
struct ServerState {
    services: AppServices,
    rate_limiter: LoginRateLimiter,
}

pub fn build_router(services: AppServices) -> Router {
    let state = ServerState {
        services: services.clone(),
        rate_limiter: LoginRateLimiter::default(),
    };

    let api_routes = Router::new()
        .route("/api/auth/login", post(login_handler))
        .route("/api/auth/logout", post(logout_handler))
        .route("/api/auth/me", get(me_handler))
        .route("/api/catalog", get(catalog_handler))
        .route("/api/contracts/{id}", get(contract_handler))
        .route("/api/graph", get(graph_handler))
        .route("/api/dashboards", get(dashboards_list_handler))
        .route("/api/dashboards/{id}", get(dashboard_get_handler))
        .route("/api/dashboards/{id}/state", post(dashboard_state_handler))
        .route("/api/widget-request", post(widget_request_handler))
        .route("/api/queries/{instance_id}", post(query_handler))
        .route("/api/actions/{instance_id}/dry-run", post(action_dry_run_handler))
        .route("/api/actions/{instance_id}/invoke", post(action_invoke_handler))
        .route("/api/executions", get(executions_list_handler))
        .route("/api/executions/{id}/resolve", post(execution_resolve_handler))
        .route("/api/drafts", get(drafts_list_handler).post(draft_begin_handler))
        .route("/api/drafts/{id}", get(draft_status_handler).delete(draft_abort_handler))
        .route("/api/drafts/{id}/validate", post(draft_validate_handler))
        .route("/api/drafts/{id}/preview", post(draft_preview_handler))
        .route("/api/drafts/{id}/rebase", post(draft_rebase_handler))
        .route("/api/drafts/{id}/activate", post(draft_activate_handler))
        .route("/api/drafts/{id}/definitions", post(draft_definition_handler))
        .route("/api/drafts/{id}/instances", post(draft_instance_upsert_handler))
        .route(
            "/api/drafts/{id}/instances/{instance_id}",
            axum::routing::delete(draft_instance_delete_handler),
        )
        .route("/api/drafts/{id}/dashboards", post(draft_dashboard_handler))
        .route("/api/drafts/{id}/dependencies", post(draft_dependency_handler))
        .route("/api/definitions/{id}/test", post(definition_test_handler))
        .route("/api/diagnostics/logs", get(logs_handler))
        .route("/api/diagnostics/traces", get(traces_handler))
        .route("/api/diagnostics/metrics", get(metrics_handler))
        .route("/api/diagnostics/audits", get(audits_handler))
        .route("/api/git/status", get(git_status_handler))
        .route("/api/git/history", get(git_history_handler))
        .route("/api/git/sync", post(git_sync_handler))
        .route("/api/secrets", get(secrets_list_handler).post(secrets_set_handler))
        .route("/api/drafts/{id}/secrets/bind", post(secret_bind_handler))
        .route("/api/agents", get(agents_list_handler).post(agents_create_handler))
        .route("/api/agents/{id}/revoke", post(agents_revoke_handler))
        .route("/api/service-principals", get(sp_list_handler))
        .route("/api/service-principals/{id}/grant", post(sp_grant_handler))
        .with_state(state.clone());

    Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/ws", get(ws_handler).with_state(services.clone()))
        .route(
            "/mcp",
            post(mcp_post_handler)
                .get(mcp_get_handler)
                .delete(mcp_delete_handler)
                .with_state(services.clone()),
        )
        .route(
            "/widgets/{def_id}/{*asset_path}",
            get(widget_asset_handler).with_state(services.clone()),
        )
        .merge(api_routes)
        .fallback(get(embedded_ui_handler))
}

async fn healthz_handler() -> Json<Value> {
    Json(json!({ "status": "ok", "engine": "dash", "version": "0.2.0" }))
}

#[derive(Deserialize)]
struct LoginReq {
    username: String,
    password: String,
}

async fn login_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<LoginReq>,
) -> Response {
    if !st.rate_limiter.check_and_record(peer.ip()) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": "rate limit exceeded" })),
        )
            .into_response();
    }
    let creds = match st.services.store().get_admin_credentials() {
        Ok(Some(c)) => c,
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "admin credentials not initialized" })),
            )
                .into_response()
        }
    };
    if req.username != creds.0 || !verify_password(&req.password, &creds.1) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "invalid credentials" })),
        )
            .into_response();
    }
    st.rate_limiter.clear(peer.ip());
    let (tok, csrf) = match create_browser_session(&st.services, &creds.0) {
        Ok(v) => v,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e }))).into_response(),
    };
    let secure = is_request_https(&headers, peer.ip(), &st.services.trusted_proxies);
    let cookie = build_session_cookie(&tok, secure);
    let mut resp = Json(json!({
        "username": creds.0,
        "principal_kind": "administrator",
        "csrf_token": csrf,
    }))
    .into_response();
    resp.headers_mut()
        .insert(header::SET_COOKIE, HeaderValue::from_str(&cookie).unwrap());
    resp
}

async fn logout_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err((status, msg)) = authenticate_request(&st.services, &headers, &Method::POST, peer) {
        return (status, Json(json!({ "error": msg }))).into_response();
    }
    if let Some(tok) = extract_cookie(&headers, "dash_session") {
        let _ = st.services.store().delete_session(tok);
    }
    let mut resp = Json(json!({ "ok": true })).into_response();
    resp.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static("dash_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"),
    );
    resp
}

async fn me_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    match authenticate_request(&st.services, &headers, &Method::GET, peer) {
        Ok(crate::auth::AuthContext::BrowserSession {
            principal,
            csrf_token,
        }) => Json(json!({
            "authenticated": true,
            "principal_id": principal.id,
            "principal_kind": principal.kind,
            "csrf_token": csrf_token,
        }))
        .into_response(),
        Ok(crate::auth::AuthContext::BearerToken { principal }) => Json(json!({
            "authenticated": true,
            "principal_id": principal.id,
            "principal_kind": principal.kind,
        }))
        .into_response(),
        Err((status, msg)) => (status, Json(json!({ "error": msg }))).into_response(),
    }
}

fn auth_or_err(
    st: &ServerState,
    headers: &HeaderMap,
    method: &Method,
    peer: SocketAddr,
) -> Result<dash_core::Principal, Response> {
    authenticate_request(&st.services, headers, method, peer)
        .map(|a| a.principal().clone())
        .map_err(|(s, m)| (s, Json(json!({ "error": m }))).into_response())
}

#[derive(Deserialize)]
struct SearchParams {
    q: Option<String>,
    limit: Option<usize>,
}

async fn catalog_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(p): Query<SearchParams>,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    Json(st.services.catalog_search(p.q.as_deref())).into_response()
}

async fn contract_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    match st.services.contract_get(&id) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(json!({ "error": e }))).into_response(),
    }
}

async fn graph_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    Json(st.services.graph_inspect()).into_response()
}

async fn dashboards_list_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    let g = st.services.graph();
    let list: Vec<&DashboardDoc> = g.dashboards.values().collect();
    Json(list).into_response()
}

async fn dashboard_get_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::GET, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.dashboard_get(&id, &p).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize)]
struct SetDashStateReq {
    key: String,
    value: Value,
    expected_version: Option<i64>,
}

async fn dashboard_state_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<SetDashStateReq>,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::POST, peer) {
        return r;
    }
    let gen_id = st.services.graph().generation_id;
    match st.services.engine().state.set(
        "dashboard",
        &id,
        &req.key,
        1,
        gen_id,
        req.value,
        req.expected_version,
    ) {
        Ok(rec) => Json(rec).into_response(),
        Err(e) => (StatusCode::CONFLICT, Json(json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn widget_request_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<WidgetRequestEnvelope>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.dispatch_widget_request(req, &p).await {
        Ok(rec) => Json(rec).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize)]
struct QueryReq {
    #[serde(default)]
    params: Value,
    #[serde(default)]
    force_refresh: bool,
}

async fn query_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(instance_id): Path<String>,
    Json(req): Json<QueryReq>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st
        .services
        .provider_query(&instance_id, req.params, &p, req.force_refresh)
        .await
    {
        Ok(env) => Json(env).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize)]
struct ActionReq {
    #[serde(default)]
    args: Value,
    #[serde(default)]
    idempotency_key: Option<String>,
}

async fn action_dry_run_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(instance_id): Path<String>,
    Json(req): Json<ActionReq>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.action_dry_run(&instance_id, req.args, &p).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn action_invoke_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(instance_id): Path<String>,
    Json(req): Json<ActionReq>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st
        .services
        .action_invoke(&instance_id, req.args, req.idempotency_key, &p)
        .await
    {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response(),
    }
}

async fn executions_list_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    match st.services.execution_inspect(None) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize)]
struct ResolveReq {
    status: String,
    #[serde(default)]
    reason: String,
}

async fn execution_resolve_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<ResolveReq>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st
        .services
        .execution_resolve(&id, &req.status, &req.reason, &p)
        .await
    {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn drafts_list_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::GET, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.draft_list_for(&p) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize, Default)]
struct DraftBeginParams {
    #[serde(default)]
    base_revision: Option<String>,
}

async fn draft_begin_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<DraftBeginParams>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.draft_begin_at(&p, q.base_revision.as_deref()).await {
        Ok(d) => Json(d).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn draft_status_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::GET, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.draft_status_as(&id, &p) {
        Ok(d) => Json(d).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(json!({ "error": e }))).into_response(),
    }
}

async fn draft_abort_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::DELETE, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.draft_abort_as(&id, &p).await {
        Ok(()) => Json(json!({ "aborted": true })).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn draft_validate_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.draft_validate_as(&id, &p).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn draft_preview_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.draft_preview_as(&id, &p).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn draft_rebase_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.draft_rebase_as(&id, &p).await {
        Ok(d) => Json(d).into_response(),
        Err(e) => (StatusCode::CONFLICT, Json(json!({ "error": e }))).into_response(),
    }
}

async fn draft_activate_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.draft_activate(&id, &p).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize)]
struct DraftDefReq {
    package: String,
    name: String,
    definition_luau: String,
    runtime_luau: Option<String>,
    tests_luau: Option<String>,
}

async fn draft_definition_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<DraftDefReq>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.definition_upsert_in_draft(
        &id,
        &req.package,
        &req.name,
        &req.definition_luau,
        req.runtime_luau.as_deref(),
        req.tests_luau.as_deref(),
        &p,
    ) {
        Ok(d) => Json(d).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn draft_instance_upsert_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(inst): Json<InstanceConfig>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.instance_upsert_in_draft(&id, &inst, &p) {
        Ok(d) => Json(d).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn draft_instance_delete_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path((id, instance_id)): Path<(String, String)>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::DELETE, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.instance_delete_in_draft(&id, &instance_id, &p) {
        Ok(d) => Json(d).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn draft_dashboard_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(doc): Json<DashboardDoc>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.dashboard_update_in_draft(&id, &doc, &p) {
        Ok(d) => Json(d).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize)]
struct DraftDepReq {
    package: String,
    git: String,
    #[serde(rename = "ref", default = "default_main")]
    git_ref: String,
    subdir: Option<String>,
}

fn default_main() -> String {
    "main".into()
}

async fn draft_dependency_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<DraftDepReq>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st
        .services
        .dependency_update(
            &id,
            &req.package,
            PackageDependencySpec {
                git: req.git,
                git_ref: req.git_ref,
                subdir: req.subdir,
            },
            &p,
        )
        .await
    {
        Ok((draft, lock)) => Json(json!({ "draft": draft, "lock": lock })).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize)]
struct DefTestReq {
    draft_id: Option<String>,
}

async fn definition_test_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<DefTestReq>,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::POST, peer) {
        return r;
    }
    match st
        .services
        .definition_test(&id, req.draft_id.as_deref())
        .await
    {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn logs_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(p): Query<SearchParams>,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    Json(st.services.logs_query(p.limit.unwrap_or(100))).into_response()
}

async fn traces_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(p): Query<SearchParams>,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    Json(st.services.graph_trace(p.limit.unwrap_or(100))).into_response()
}

async fn metrics_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    Json(st.services.metrics_query()).into_response()
}

async fn audits_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(p): Query<SearchParams>,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    match st.services.audit_query(p.limit.unwrap_or(100)) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e }))).into_response(),
    }
}

async fn git_status_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    match st.services.git_status().await {
        Ok(s) => Json(s).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e }))).into_response(),
    }
}

async fn git_history_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(p): Query<SearchParams>,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    match st.services.git_history(p.limit.unwrap_or(20)).await {
        Ok(h) => Json(h).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e }))).into_response(),
    }
}

async fn git_sync_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.git_sync(&p).await {
        Ok(s) => Json(s).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    }
}

async fn secrets_list_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::GET, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.secret_list(&p) {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize)]
struct SetSecretReq {
    name: String,
    #[serde(default)]
    description: String,
    plaintext: String,
    #[serde(default)]
    allowed_bindings: Vec<String>,
}

async fn secrets_set_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<SetSecretReq>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.secret_set(
        &req.name,
        &req.description,
        &req.plaintext,
        &req.allowed_bindings,
        &p,
    ) {
        Ok(m) => Json(m).into_response(),
        Err(e) => (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize)]
struct BindSecretReq {
    instance_id: String,
    #[serde(default = "default_auth_slot")]
    slot: String,
    secret_name: String,
}

fn default_auth_slot() -> String {
    "auth".into()
}

async fn secret_bind_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<BindSecretReq>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st
        .services
        .secret_bind(&id, &req.instance_id, &req.slot, &req.secret_name, &p)
    {
        Ok(d) => Json(d).into_response(),
        Err(e) => (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response(),
    }
}

async fn agents_list_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    match st.services.agent_list() {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e }))).into_response(),
    }
}

#[derive(Deserialize)]
struct CreateAgentReq {
    name: String,
    #[serde(default)]
    ceiling: CapabilityCeiling,
}

async fn agents_create_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<CreateAgentReq>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.agent_create(&req.name, req.ceiling, None, &p) {
        Ok((rec, token)) => Json(json!({ "agent": rec, "token": token })).into_response(),
        Err(e) => (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response(),
    }
}

async fn agents_revoke_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.agent_revoke(&id, &p) {
        Ok(revoked) => Json(json!({ "revoked": revoked })).into_response(),
        Err(e) => (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response(),
    }
}

async fn sp_list_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = auth_or_err(&st, &headers, &Method::GET, peer) {
        return r;
    }
    Json(st.services.service_principal_list()).into_response()
}

async fn sp_grant_handler(
    State(st): State<ServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(ceiling): Json<CapabilityCeiling>,
) -> Response {
    let p = match auth_or_err(&st, &headers, &Method::POST, peer) {
        Ok(p) => p,
        Err(r) => return r,
    };
    match st.services.service_principal_grant(&id, ceiling, &p) {
        Ok(()) => Json(json!({ "granted": true, "principal_id": id })).into_response(),
        Err(e) => (StatusCode::FORBIDDEN, Json(json!({ "error": e }))).into_response(),
    }
}

/// Serve custom widget provider static assets with strict isolated CSP (§10.1, §14.3).
async fn widget_asset_handler(
    State(services): State<AppServices>,
    Path((def_id, asset_path)): Path<(String, String)>,
) -> Response {
    let g = services.graph();
    let clean = asset_path.trim_start_matches('/');
    let Some(def) = g.definitions.get(&def_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(bytes) = def.widget_assets.get(clean) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mime = mime_guess::from_path(clean)
        .first_or_octet_stream()
        .to_string();
    let mut resp = (StatusCode::OK, bytes.clone()).into_response();
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_str(&mime).unwrap());
    resp.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src data:; connect-src 'none'",
        ),
    );
    resp
}

async fn embedded_ui_handler(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let (file, chosen_path) = if !path.is_empty() {
        if let Some(f) = WebAssets::get(path) {
            (f, path)
        } else {
            let Some(idx) = WebAssets::get("index.html") else {
                return StatusCode::NOT_FOUND.into_response();
            };
            (idx, "index.html")
        }
    } else {
        let Some(idx) = WebAssets::get("index.html") else {
            return StatusCode::NOT_FOUND.into_response();
        };
        (idx, "index.html")
    };

    let mime = mime_guess::from_path(chosen_path)
        .first_or_octet_stream()
        .to_string();
    let mut resp = (StatusCode::OK, file.data.into_owned()).into_response();
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_str(&mime).unwrap());
    resp.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self' ws: wss:; frame-src 'self'",
        ),
    );
    resp
}
