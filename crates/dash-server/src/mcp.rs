//! MCP Streamable HTTP endpoint (`/mcp`) with protocol version negotiation, Origin validation,
//! Agent Bearer auth, all 35 tools (§11.2) with real input schemas, and MCP resources (§11.1, §11.3).
//!
//! Protocol behaviour (Streamable HTTP):
//! - JSON-RPC notifications and client responses are answered `202 Accepted` with no body.
//! - JSON-RPC batches (arrays) are accepted (required by the 2025-03-26 revision we advertise).
//! - Malformed bodies get a JSON-RPC `-32700` parse error.
//! - `initialize` creates an `Mcp-Session-Id` bound to the Principal; requests carrying an unknown,
//!   expired or foreign session id get `404` (client must re-initialize); `DELETE /mcp` terminates.
//!   Requests without a session id are still served (stateless clients).
//! - `MCP-Protocol-Version` in responses echoes the negotiated version.
//! - The server sends no notifications (GET returns 405), so `listChanged` is advertised `false`.
//! - Requests carrying `X-Dash-Transport: stdio` (sent by `dash mcp-stdio`) must authenticate as an
//!   Agent; the daemon refuses Administrator credentials on that transport (§11.1, §17.4).

use crate::auth::{authenticate_request, validate_origin};
use crate::services::AppServices;
use axum::{
    body::Bytes,
    extract::{ConnectInfo, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use dash_core::{DashboardDoc, InstanceConfig, Principal, PrincipalKind};
use dash_git::PackageDependencySpec;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;

const SUPPORTED_MCP_VERSIONS: &[&str] = &[
    "2026-07-28",
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
];

/// Header the stdio proxy sets on every forwarded request.
pub const STDIO_TRANSPORT_HEADER: &str = "x-dash-transport";

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
}

fn rpc_error_body(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message.into() }
    })
}

fn rpc_error_response(status: StatusCode, code: i64, message: impl Into<String>) -> Response {
    (status, Json(rpc_error_body(Value::Null, code, message))).into_response()
}

/// Wrap a tool result so `structuredContent` is always a JSON object (MCP 2025-06-18+).
/// Arrays become `{"items": [...]}`; other non-object values become `{"value": ...}`.
pub fn structured_content(val: &Value) -> Value {
    match val {
        Value::Object(_) => val.clone(),
        Value::Array(_) => json!({ "items": val }),
        other => json!({ "value": other }),
    }
}

/// Outcome of handling one JSON-RPC message.
struct Handled {
    response: Option<Value>,
    negotiated_version: Option<String>,
}

pub async fn mcp_post_handler(
    State(services): State<AppServices>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err((status, msg)) = validate_origin(&headers, peer.ip(), &services.trusted_proxies) {
        return rpc_error_response(status, -32001, msg);
    }

    let auth = match authenticate_request(&services, &headers, &Method::POST, peer) {
        Ok(a) => a,
        Err((status, msg)) => return rpc_error_response(status, -32001, msg),
    };
    let principal = auth.principal().clone();

    // §11.1: stdio maps to an explicitly configured Agent token, never the Administrator.
    let via_stdio = headers
        .get(STDIO_TRANSPORT_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("stdio"));
    if via_stdio && principal.kind != PrincipalKind::Agent {
        let _ = services.store().append_audit(
            &principal.id,
            principal.kind.as_str(),
            "mcp.stdio",
            "/mcp",
            "denied",
            json!({ "reason": "stdio MCP requires an Agent principal" }),
        );
        return rpc_error_response(
            StatusCode::FORBIDDEN,
            -32001,
            "stdio MCP requires an Agent token; never runs as Administrator (§11.1)",
        );
    }

    // Protocol version requested by the client for this request (if any).
    let header_version = headers
        .get("mcp-protocol-version")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    if let Some(ref proto) = header_version {
        if !SUPPORTED_MCP_VERSIONS.contains(&proto.as_str()) {
            return rpc_error_response(
                StatusCode::BAD_REQUEST,
                -32600,
                format!("unsupported MCP-Protocol-Version '{proto}'"),
            );
        }
    }

    // Session validation: a supplied session must exist and belong to this Principal.
    let session_header = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let mut session_version = None;
    if let Some(ref sid) = session_header {
        match services.server.mcp_session_touch(sid, &principal.id) {
            Some(v) => session_version = Some(v),
            None => {
                return rpc_error_response(
                    StatusCode::NOT_FOUND,
                    -32001,
                    "unknown or terminated MCP session; re-initialize",
                )
            }
        }
    }

    let parsed: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return rpc_error_response(StatusCode::BAD_REQUEST, -32700, format!("Parse error: {e}"))
        }
    };

    let mut negotiated: Option<String> = None;
    let (status, out_body) = match parsed {
        Value::Array(items) => {
            if items.is_empty() {
                (
                    StatusCode::BAD_REQUEST,
                    Some(rpc_error_body(Value::Null, -32600, "Invalid Request: empty batch")),
                )
            } else {
                let mut responses = Vec::new();
                for item in items {
                    let h = handle_message(&services, &principal, item).await;
                    if h.negotiated_version.is_some() {
                        negotiated = h.negotiated_version;
                    }
                    if let Some(r) = h.response {
                        responses.push(r);
                    }
                }
                if responses.is_empty() {
                    (StatusCode::ACCEPTED, None)
                } else {
                    (StatusCode::OK, Some(Value::Array(responses)))
                }
            }
        }
        single => {
            let h = handle_message(&services, &principal, single).await;
            negotiated = h.negotiated_version;
            match h.response {
                Some(r) => {
                    let bad = r.get("error").and_then(|e| e.get("code")).and_then(|c| c.as_i64())
                        == Some(-32600);
                    (if bad { StatusCode::BAD_REQUEST } else { StatusCode::OK }, Some(r))
                }
                None => (StatusCode::ACCEPTED, None),
            }
        }
    };

    let mut resp = match out_body {
        Some(b) => (status, Json(b)).into_response(),
        None => status.into_response(),
    };
    if let Some(ref ver) = negotiated {
        // Successful initialize: open a Principal-bound session.
        let sid = services.server.mcp_session_create(&principal.id, ver);
        if let Ok(hdr) = HeaderValue::from_str(&sid) {
            resp.headers_mut().insert("mcp-session-id", hdr);
        }
    } else if let Some(ref sid) = session_header {
        if let Ok(hdr) = HeaderValue::from_str(sid) {
            resp.headers_mut().insert("mcp-session-id", hdr);
        }
    }
    if let Some(ver) = negotiated.or(session_version).or(header_version) {
        if let Ok(hdr) = HeaderValue::from_str(&ver) {
            resp.headers_mut().insert("mcp-protocol-version", hdr);
        }
    }
    resp
}

async fn handle_message(services: &AppServices, principal: &Principal, msg: Value) -> Handled {
    let none = Handled {
        response: None,
        negotiated_version: None,
    };
    let Some(obj) = msg.as_object() else {
        return Handled {
            response: Some(rpc_error_body(Value::Null, -32600, "Invalid Request: expected object")),
            negotiated_version: None,
        };
    };
    // Client -> server JSON-RPC responses (e.g. to elicitation) need no reply.
    if !obj.contains_key("method") && (obj.contains_key("result") || obj.contains_key("error")) {
        return none;
    }
    let id_opt = obj.get("id").cloned();
    let req: JsonRpcRequest = match serde_json::from_value(msg.clone()) {
        Ok(r) => r,
        Err(e) => {
            return Handled {
                response: id_opt
                    .map(|id| rpc_error_body(id, -32600, format!("Invalid Request: {e}"))),
                negotiated_version: None,
            }
        }
    };
    if req.jsonrpc != "2.0" {
        return Handled {
            response: Some(rpc_error_body(
                req.id.unwrap_or(Value::Null),
                -32600,
                "Invalid Request: expected jsonrpc 2.0",
            )),
            negotiated_version: None,
        };
    }
    // Notifications (no id): handled for side effects only, never answered.
    let Some(id) = req.id.clone() else {
        // notifications/initialized, notifications/cancelled, ... carry no server-side effect here.
        return none;
    };

    let mut negotiated_version = None;
    let result: Result<Value, (i64, String)> = match req.method.as_str() {
        "initialize" => {
            let client_ver = req
                .params
                .get("protocolVersion")
                .and_then(|v| v.as_str())
                .unwrap_or(SUPPORTED_MCP_VERSIONS[0]);
            let chosen = if SUPPORTED_MCP_VERSIONS.contains(&client_ver) {
                client_ver
            } else {
                SUPPORTED_MCP_VERSIONS[0]
            };
            negotiated_version = Some(chosen.to_string());
            Ok(json!({
                "protocolVersion": chosen,
                "capabilities": {
                    "tools": { "listChanged": false },
                    "resources": { "subscribe": false, "listChanged": false }
                },
                "serverInfo": {
                    "name": "dash-mcp",
                    "version": "0.2.0"
                },
                "instructions": "Configuration writes happen inside Drafts (draft_begin ... draft_activate); action_invoke is a live operation checked against your Agent ceiling. Read dash://docs/mcp-tools for argument details."
            }))
        }
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": mcp_tool_definitions() })),
        "tools/call" => {
            let name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let args = req
                .params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let span = dash_core::telemetry::current_request_trace().map(|ctx| {
                services.engine().telemetry.start_span(
                    &ctx.trace_id,
                    Some(&ctx.root_span_id),
                    &format!("mcp.tool:{name}"),
                    &principal.id,
                )
            });
            let outcome = call_mcp_tool(services, principal, name, args).await;
            if let Some(span) = span {
                span.finish(
                    if outcome.is_ok() { "ok" } else { "error" },
                    json!({ "tool": name, "principal_kind": principal.kind.as_str() }),
                );
            }
            match outcome {
                Ok(val) => Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": serde_json::to_string_pretty(&val).unwrap_or_default()
                    }],
                    "structuredContent": structured_content(&val),
                    "isError": false
                })),
                Err(err) => Ok(json!({
                    "content": [{ "type": "text", "text": err }],
                    "isError": true
                })),
            }
        }
        "resources/list" => Ok(json!({ "resources": mcp_resources_list(services, principal) })),
        "resources/read" => {
            let uri = req
                .params
                .get("uri")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match read_mcp_resource(services, principal, uri) {
                Ok(McpResourceContent::Json(val)) => Ok(json!({
                    "contents": [{
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": serde_json::to_string_pretty(&val).unwrap_or_default()
                    }]
                })),
                Ok(McpResourceContent::Markdown(text)) => Ok(json!({
                    "contents": [{
                        "uri": uri,
                        "mimeType": "text/markdown",
                        "text": text
                    }]
                })),
                Err(msg) => Err((-32602, msg)),
            }
        }
        other => Err((-32601, format!("method not found: {other}"))),
    };

    let response = match result {
        Ok(res) => serde_json::to_value(JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id,
            result: Some(res),
            error: None,
        })
        .unwrap_or(Value::Null),
        Err((code, message)) => serde_json::to_value(JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(JsonRpcError { code, message }),
        })
        .unwrap_or(Value::Null),
    };
    Handled {
        response: Some(response),
        negotiated_version,
    }
}

pub async fn mcp_get_handler(
    State(services): State<AppServices>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_origin(&headers, peer.ip(), &services.trusted_proxies) {
        return (status, msg).into_response();
    }
    if let Err((status, msg)) = authenticate_request(&services, &headers, &Method::GET, peer) {
        return (status, msg).into_response();
    }
    (
        StatusCode::METHOD_NOT_ALLOWED,
        "Streamable HTTP MCP server accepts JSON-RPC POST on /mcp (no server-initiated stream)",
    )
        .into_response()
}

/// `DELETE /mcp` terminates the caller's session (§11.1 Streamable HTTP session lifecycle).
pub async fn mcp_delete_handler(
    State(services): State<AppServices>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_origin(&headers, peer.ip(), &services.trusted_proxies) {
        return (status, msg).into_response();
    }
    let auth = match authenticate_request(&services, &headers, &Method::DELETE, peer) {
        Ok(a) => a,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let Some(sid) = headers.get("mcp-session-id").and_then(|v| v.to_str().ok()) else {
        return (StatusCode::BAD_REQUEST, "missing Mcp-Session-Id").into_response();
    };
    if services
        .server
        .mcp_session_terminate(sid, &auth.principal().id)
    {
        StatusCode::NO_CONTENT.into_response()
    } else {
        (StatusCode::NOT_FOUND, "unknown MCP session").into_response()
    }
}

fn req_str<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("missing required string argument '{key}'"))
}

pub async fn call_mcp_tool(
    services: &AppServices,
    principal: &Principal,
    name: &str,
    args: Value,
) -> Result<Value, String> {
    // Every Draft-scoped tool requires draft authoring rights and ownership of the Draft (§11.3).
    if let Some(draft_id) = args.get("draft_id").and_then(|v| v.as_str()) {
        services.authorize_draft(draft_id, principal)?;
    }
    match name {
        // Discover
        "catalog_search" => {
            let q = args.get("query").and_then(|v| v.as_str());
            Ok(json!(services.catalog_search(q)))
        }
        "contract_get" => services.contract_get(req_str(&args, "definition_id")?),
        "graph_inspect" => Ok(services.graph_inspect_for(principal)),
        "capability_explain" => Ok(services.capability_explain(req_str(&args, "target")?, principal)),

        // Drafts
        "draft_begin" => {
            let base = args.get("base_revision").and_then(|v| v.as_str());
            Ok(json!(services.draft_begin_at(principal, base).await?))
        }
        "draft_status" => Ok(json!(services.draft_status_as(req_str(&args, "draft_id")?, principal)?)),
        "draft_validate" => Ok(json!(services.draft_validate_as(req_str(&args, "draft_id")?, principal).await?)),
        "draft_preview" => Ok(json!(services.draft_preview_as(req_str(&args, "draft_id")?, principal).await?)),
        "draft_test" => Ok(json!(services.draft_test_as(req_str(&args, "draft_id")?, principal).await?)),
        "draft_rebase" => Ok(json!(services.draft_rebase_as(req_str(&args, "draft_id")?, principal).await?)),
        "draft_activate" => Ok(json!(services.draft_activate_as(req_str(&args, "draft_id")?, principal).await?)),
        "draft_abort" => {
            services.draft_abort_as(req_str(&args, "draft_id")?, principal).await?;
            Ok(json!({ "aborted": true }))
        }

        // Definitions (configuration mutations strictly inside a Draft!)
        "definition_create" | "definition_edit" => {
            let draft_id = req_str(&args, "draft_id")?;
            let package = req_str(&args, "package")?;
            let def_name = req_str(&args, "name")?;
            let def_luau = req_str(&args, "definition_luau")?;
            let rt_luau = args.get("runtime_luau").and_then(|v| v.as_str());
            let tests_luau = args.get("tests_luau").and_then(|v| v.as_str());
            let rec = if name == "definition_create" {
                services.definition_create_in_draft(
                    draft_id, package, def_name, def_luau, rt_luau, tests_luau, principal,
                )?
            } else {
                services.definition_edit_in_draft(
                    draft_id, package, def_name, def_luau, rt_luau, tests_luau, principal,
                )?
            };
            Ok(json!(rec))
        }
        "definition_test" => {
            let def_id = req_str(&args, "definition_id")?;
            let draft_id = args.get("draft_id").and_then(|v| v.as_str());
            Ok(json!(services.definition_test(def_id, draft_id).await?))
        }

        // Instances
        "instance_create" | "instance_configure" => {
            let draft_id = req_str(&args, "draft_id")?;
            let inst: InstanceConfig = serde_json::from_value(
                args.get("instance")
                    .cloned()
                    .ok_or_else(|| "missing 'instance' object".to_string())?,
            )
            .map_err(|e| format!("invalid instance payload: {e}"))?;
            let rec = if name == "instance_create" {
                services.instance_create_in_draft(draft_id, &inst, principal)?
            } else {
                services.instance_configure_in_draft(draft_id, &inst, principal)?
            };
            Ok(json!(rec))
        }
        "instance_delete" => {
            let draft_id = req_str(&args, "draft_id")?;
            let inst_id = req_str(&args, "instance_id")?;
            Ok(json!(services.instance_delete_in_draft(
                draft_id,
                inst_id,
                principal
            )?))
        }

        // Dashboards
        "dashboard_get" => {
            services
                .dashboard_get(req_str(&args, "dashboard_id")?, principal)
                .await
        }
        "dashboard_update" => {
            let draft_id = req_str(&args, "draft_id")?;
            let doc: DashboardDoc = serde_json::from_value(
                args.get("dashboard")
                    .cloned()
                    .ok_or_else(|| "missing 'dashboard' object".to_string())?,
            )
            .map_err(|e| format!("invalid dashboard document: {e}"))?;
            Ok(json!(services.dashboard_update_in_draft(
                draft_id, &doc, principal
            )?))
        }
        "dashboard_preview" => {
            services
                .dashboard_preview_as(
                    req_str(&args, "draft_id")?,
                    req_str(&args, "dashboard_id")?,
                    principal,
                )
                .await
        }

        // Runtime (§11.3: action_invoke is live, outside drafts!)
        "provider_query" => {
            let inst_id = req_str(&args, "instance_id")?;
            let params = args.get("params").cloned().unwrap_or_else(|| json!({}));
            let force = args
                .get("force_refresh")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            Ok(json!(services
                .provider_query(inst_id, params, principal, force)
                .await?))
        }
        "action_dry_run" => {
            let act_id = req_str(&args, "action_instance")?;
            let act_args = args.get("args").cloned().unwrap_or_else(|| json!({}));
            Ok(json!(services.action_dry_run(act_id, act_args, principal).await?))
        }
        "action_invoke" => {
            let act_id = req_str(&args, "action_instance")?;
            let act_args = args.get("args").cloned().unwrap_or_else(|| json!({}));
            let ikey = args
                .get("idempotency_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            Ok(json!(services
                .action_invoke(act_id, act_args, ikey, principal)
                .await?))
        }
        "execution_inspect" => {
            let exec_id = args.get("execution_id").and_then(|v| v.as_str());
            services.execution_inspect_for(exec_id, principal)
        }
        "execution_resolve" => {
            let exec_id = req_str(&args, "execution_id")?;
            let status = req_str(&args, "status")?;
            let reason = args
                .get("reason")
                .and_then(|v| v.as_str())
                .unwrap_or("resolved via MCP");
            Ok(json!(services
                .execution_resolve(exec_id, status, reason, principal)
                .await?))
        }

        "execution_cancel" => {
            let exec_id = req_str(&args, "execution_id")?;
            let reason = args
                .get("reason")
                .and_then(|v| v.as_str())
                .unwrap_or("cancelled via MCP");
            Ok(json!(services
                .execution_cancel(exec_id, reason, principal)
                .await?))
        }

        // Diagnostics
        "logs_query" => {
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
            Ok(json!(services.logs_query(limit)))
        }
        "graph_trace" => {
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
            Ok(services.graph_trace(limit))
        }
        "metrics_query" => Ok(json!(services.metrics_query())),

        // Git
        "git_status" => Ok(json!(services.git_status().await?)),
        "git_history" => {
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as usize;
            Ok(json!(services.git_history(limit).await?))
        }
        "git_sync" => Ok(json!(services.git_sync(principal).await?)),
        "dependency_update" => {
            let draft_id = req_str(&args, "draft_id")?;
            let pkg = req_str(&args, "package")?;
            let git = req_str(&args, "git")?.to_string();
            let git_ref = args
                .get("ref")
                .and_then(|v| v.as_str())
                .unwrap_or("main")
                .to_string();
            let subdir = args
                .get("subdir")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let (draft, lock) = services
                .dependency_update(
                    draft_id,
                    pkg,
                    PackageDependencySpec {
                        git,
                        git_ref,
                        subdir,
                    },
                    principal,
                )
                .await?;
            Ok(json!({ "draft": draft, "lock": lock }))
        }

        // Secrets (never plaintext!)
        "secret_list" => Ok(json!(services.secret_list(principal)?)),
        "secret_bind" => {
            let draft_id = req_str(&args, "draft_id")?;
            let inst_id = req_str(&args, "instance_id")?;
            let slot = args
                .get("slot")
                .and_then(|v| v.as_str())
                .unwrap_or("auth");
            let secret_name = req_str(&args, "secret_name")?;
            Ok(json!(services.secret_bind(
                draft_id,
                inst_id,
                slot,
                secret_name,
                principal
            )?))
        }

        other => Err(format!("unknown MCP tool '{other}'")),
    }
}

// ---- Resources ----

/// Embedded documentation exposed as `dash://docs/{topic}` (§11.2 "documentation").
const DOC_TOPICS: &[(&str, &str, &str)] = &[
    ("glossary", "Domain glossary (authoritative vocabulary)", include_str!("../../../GLOSSARY.md")),
    (
        "adr-0001-adapters-evaluated-per-dashboard",
        "ADR 0001: Adapters evaluated per Dashboard",
        include_str!("../../../docs/adr/0001-adapters-evaluated-per-dashboard.md"),
    ),
    (
        "adr-0002-service-principals-for-background-work",
        "ADR 0002: Service principals for background work",
        include_str!("../../../docs/adr/0002-service-principals-for-background-work.md"),
    ),
    (
        "adr-0003-luau-vm-scheduling-and-quotas",
        "ADR 0003: Luau VM scheduling and quotas",
        include_str!("../../../docs/adr/0003-luau-vm-scheduling-and-quotas.md"),
    ),
    (
        "adr-0004-secret-vault-and-git-activation-journal",
        "ADR 0004: Secret vault and Git activation journal",
        include_str!("../../../docs/adr/0004-secret-vault-and-git-activation-journal.md"),
    ),
];

pub enum McpResourceContent {
    Json(Value),
    Markdown(String),
}

fn mcp_resources_list(services: &AppServices, principal: &Principal) -> Vec<Value> {
    let mut resources = vec![
        json!({ "uri": "dash://manifests", "name": "Generated Definition Manifests", "mimeType": "application/json" }),
        json!({ "uri": "dash://graph", "name": "Configured Graph & Validation (Compatibility) Report", "mimeType": "application/json" }),
        json!({ "uri": "dash://graph/dependencies", "name": "Dependency Graph (Instance bindings and Dashboard placements)", "mimeType": "application/json" }),
        json!({ "uri": "dash://traces", "name": "Sanitized Runtime Execution Graph & Traces", "mimeType": "application/json" }),
        json!({ "uri": "dash://logs", "name": "Sanitized Runtime Logs", "mimeType": "application/json" }),
        json!({ "uri": "dash://metrics", "name": "Runtime Metrics", "mimeType": "application/json" }),
        json!({ "uri": "dash://docs/mcp-tools", "name": "MCP tool reference (arguments and semantics)", "mimeType": "text/markdown" }),
    ];
    for (topic, title, _) in DOC_TOPICS {
        resources.push(json!({
            "uri": format!("dash://docs/{topic}"),
            "name": title,
            "mimeType": "text/markdown"
        }));
    }
    for id in services.graph().definitions.keys() {
        resources.push(json!({
            "uri": format!("dash://definitions/{id}"),
            "name": format!("Contract, JSON Schemas & Luau Types: {id}"),
            "mimeType": "application/json"
        }));
    }
    if let Ok(drafts) = services.draft_list_for(principal) {
        for d in drafts.iter().filter(|d| d.status == "open") {
            resources.push(json!({
                "uri": format!("dash://drafts/{}/validation", d.draft_id),
                "name": format!("Draft {} validation / compatibility report", d.draft_id),
                "mimeType": "application/json"
            }));
            resources.push(json!({
                "uri": format!("dash://drafts/{}/preview-report", d.draft_id),
                "name": format!("Draft {} Preview & test report", d.draft_id),
                "mimeType": "application/json"
            }));
        }
    }
    resources
}

/// Explicit dependency edges of the active graph: Instance input bindings and Dashboard placements.
pub fn dependency_graph(services: &AppServices) -> Value {
    let g = services.graph();
    let mut edges = Vec::new();
    for (id, inst) in &g.instances {
        for (slot, target) in &inst.inputs {
            edges.push(json!({
                "from": id,
                "to": target,
                "kind": "input",
                "slot": slot,
            }));
        }
        if let Some(ref sp) = inst.run_as {
            edges.push(json!({ "from": id, "to": sp, "kind": "run_as" }));
        }
    }
    for (dash_id, doc) in &g.dashboards {
        for placement in &doc.adapters {
            edges.push(json!({
                "from": format!("dashboard:{dash_id}"),
                "to": placement.adapter_instance,
                "kind": "placement",
                "area": placement.area,
            }));
        }
    }
    let nodes: Vec<Value> = g
        .instances
        .values()
        .map(|i| json!({ "id": i.id, "category": i.category.as_str(), "definition": i.definition }))
        .chain(g.dashboards.keys().map(|d| json!({ "id": format!("dashboard:{d}"), "category": "dashboard" })))
        .collect();
    json!({
        "generation_id": g.generation_id,
        "revision_commit": g.revision_commit,
        "nodes": nodes,
        "edges": edges,
    })
}

fn tool_reference_markdown() -> String {
    let mut out = String::from("# dash MCP tools\n\nEvery tool is backed by the same application services as the UI and CLI. Configuration writes happen inside a Draft; `action_invoke` is live and checked against the Agent's ceiling.\n\n");
    for t in mcp_tool_definitions() {
        out.push_str(&format!(
            "## `{}`\n\n{}\n\n```json\n{}\n```\n\n",
            t["name"].as_str().unwrap_or(""),
            t["description"].as_str().unwrap_or(""),
            serde_json::to_string_pretty(&t["inputSchema"]).unwrap_or_default()
        ));
    }
    out
}

pub fn read_mcp_resource(
    services: &AppServices,
    principal: &Principal,
    uri: &str,
) -> Result<McpResourceContent, String> {
    use McpResourceContent::{Json as J, Markdown as M};
    match uri {
        "dash://manifests" => {
            let g = services.graph();
            let map: std::collections::BTreeMap<String, Value> = g
                .definitions
                .iter()
                .map(|(id, d)| (id.clone(), serde_json::to_value(&d.manifest).unwrap()))
                .collect();
            Ok(J(json!(map)))
        }
        "dash://graph" => Ok(J(services.graph_inspect_for(principal))),
        "dash://graph/dependencies" => Ok(J(dependency_graph(services))),
        "dash://traces" => Ok(J(services.graph_trace(100))),
        "dash://logs" => Ok(J(json!(services.logs_query(100)))),
        "dash://metrics" => Ok(J(json!(services.metrics_query()))),
        "dash://docs/mcp-tools" => Ok(M(tool_reference_markdown())),
        other if other.starts_with("dash://docs/") => {
            let topic = other.trim_start_matches("dash://docs/");
            DOC_TOPICS
                .iter()
                .find(|(t, _, _)| *t == topic)
                .map(|(_, _, body)| M((*body).to_string()))
                .ok_or_else(|| format!("unknown documentation topic '{topic}'"))
        }
        other if other.starts_with("dash://definitions/") => {
            let def_id = other.strip_prefix("dash://definitions/").unwrap();
            services.contract_get(def_id).map(J)
        }
        other if other.starts_with("dash://drafts/") => {
            let rest = other.trim_start_matches("dash://drafts/");
            let (draft_id, kind) = rest
                .split_once('/')
                .ok_or_else(|| format!("unknown MCP resource '{other}'"))?;
            let draft = services.authorize_draft(draft_id, principal)?;
            let report = match kind {
                "validation" => draft.validation_report,
                "preview-report" => draft.test_report,
                _ => return Err(format!("unknown MCP resource '{other}'")),
            };
            Ok(J(json!({
                "draft_id": draft.draft_id,
                "status": draft.status,
                "base_revision": draft.base_revision,
                "report": report,
                "report_available": report.is_some(),
            })))
        }
        other => Err(format!("unknown MCP resource '{other}'")),
    }
}

// ---- Tool input schemas ----

fn s(desc: &str) -> Value {
    json!({ "type": "string", "description": desc })
}

fn obj(props: Vec<(&str, Value)>, required: &[&str]) -> Value {
    let mut map = serde_json::Map::new();
    for (k, v) in props {
        map.insert(k.to_string(), v);
    }
    json!({
        "type": "object",
        "properties": map,
        "required": required,
        "additionalProperties": false
    })
}

fn draft_only() -> Value {
    obj(vec![("draft_id", s("Draft id returned by draft_begin"))], &["draft_id"])
}

fn instance_schema() -> Value {
    json!({
        "type": "object",
        "description": "Instance configuration (InstanceConfig)",
        "properties": {
            "id": s("Stable Instance id"),
            "category": {
                "type": "string",
                "enum": ["connector", "mock", "data_provider", "action_provider", "adapter", "widget_provider", "library", "automation"]
            },
            "definition": s("Definition id, e.g. 'github.pull_requests'"),
            "inputs": { "type": "object", "additionalProperties": { "type": "string" }, "description": "slot -> bound Instance id" },
            "config": { "type": "object" },
            "secrets": { "type": "object", "additionalProperties": { "type": "string" }, "description": "slot -> 'secret://<name>'" },
            "capabilities": { "type": "array", "items": { "type": "string" } },
            "run_as": s("Service principal id for background work"),
            "enabled": { "type": "boolean" }
        },
        "required": ["id", "category", "definition"]
    })
}

fn dashboard_schema() -> Value {
    json!({
        "type": "object",
        "description": "Dashboard document (DashboardDoc)",
        "properties": {
            "id": s("Dashboard id"),
            "title": s("Title"),
            "layout": s("Layout kind (default 'grid')"),
            "adapters": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "adapter_instance": s("Adapter Instance id"),
                        "area": s("Layout area"),
                        "query_params": { "type": "object" }
                    },
                    "required": ["adapter_instance"]
                }
            },
            "initial_state": { "type": "object" },
            "overrides": { "type": "object", "description": "widget_id -> layout override" }
        },
        "required": ["id", "title"]
    })
}

fn limit_schema(default: u64) -> Value {
    obj(
        vec![(
            "limit",
            json!({ "type": "integer", "minimum": 1, "default": default }),
        )],
        &[],
    )
}

/// JSON Schema of one tool's arguments (§11.2, §17.12 discovery).
pub fn tool_input_schema(name: &str) -> Value {
    let def_props = || {
        vec![
            ("draft_id", s("Draft id")),
            ("package", s("Local Package name, e.g. 'engineering'")),
            ("name", s("Definition directory name inside the Package")),
            ("definition_luau", s("definition.luau source (declaration)")),
            ("runtime_luau", s("runtime.luau source (handlers)")),
            ("tests_luau", s("tests.luau source (Definition tests)")),
        ]
    };
    match name {
        "catalog_search" => obj(vec![("query", s("Substring matched against id, description and category"))], &[]),
        "contract_get" => obj(vec![("definition_id", s("Definition id"))], &["definition_id"]),
        "graph_inspect" | "metrics_query" | "git_status" | "git_sync" | "secret_list" => obj(vec![], &[]),
        "draft_begin" => obj(
            vec![("base_revision", s("Optional base Revision; defaults to the active Revision"))],
            &[],
        ),
        "capability_explain" => obj(vec![("target", s("Instance id to explain authority for"))], &["target"]),
        "draft_status" | "draft_validate" | "draft_preview" | "draft_test" | "draft_rebase"
        | "draft_activate" | "draft_abort" => draft_only(),
        "definition_create" | "definition_edit" => {
            obj(def_props(), &["draft_id", "package", "name", "definition_luau"])
        }
        "definition_test" => obj(
            vec![
                ("definition_id", s("Definition id")),
                ("draft_id", s("Optional Draft whose worktree is tested")),
            ],
            &["definition_id"],
        ),
        "instance_create" | "instance_configure" => obj(
            vec![("draft_id", s("Draft id")), ("instance", instance_schema())],
            &["draft_id", "instance"],
        ),
        "instance_delete" => obj(
            vec![("draft_id", s("Draft id")), ("instance_id", s("Instance id"))],
            &["draft_id", "instance_id"],
        ),
        "dashboard_get" => obj(vec![("dashboard_id", s("Dashboard id"))], &["dashboard_id"]),
        "dashboard_update" => obj(
            vec![("draft_id", s("Draft id")), ("dashboard", dashboard_schema())],
            &["draft_id", "dashboard"],
        ),
        "dashboard_preview" => obj(
            vec![("draft_id", s("Draft id")), ("dashboard_id", s("Dashboard id"))],
            &["draft_id", "dashboard_id"],
        ),
        "provider_query" => obj(
            vec![
                ("instance_id", s("Data provider Instance id")),
                ("params", json!({ "type": "object", "description": "Query parameters" })),
                ("force_refresh", json!({ "type": "boolean", "default": false })),
            ],
            &["instance_id"],
        ),
        "action_dry_run" => obj(
            vec![
                ("action_instance", s("Action provider Instance id")),
                ("args", json!({ "type": "object", "description": "Action arguments" })),
            ],
            &["action_instance"],
        ),
        "action_invoke" => obj(
            vec![
                ("action_instance", s("Action provider Instance id")),
                ("args", json!({ "type": "object", "description": "Action arguments" })),
                ("idempotency_key", s("Optional key; repeats return the original execution")),
            ],
            &["action_instance"],
        ),
        "execution_inspect" => obj(vec![("execution_id", s("Execution id; omit to list recent executions"))], &[]),
        "execution_resolve" => obj(
            vec![
                ("execution_id", s("Execution id in status 'unknown'")),
                ("status", json!({ "type": "string", "enum": ["succeeded", "failed", "cancelled"] })),
                ("reason", s("Audit reason")),
            ],
            &["execution_id", "status"],
        ),
        "execution_cancel" => obj(
            vec![
                ("execution_id", s("Execution id in status 'queued' or 'running'")),
                ("reason", s("Optional audit reason")),
            ],
            &["execution_id"],
        ),
        "logs_query" | "graph_trace" => limit_schema(100),
        "git_history" => limit_schema(20),
        "dependency_update" => obj(
            vec![
                ("draft_id", s("Draft id")),
                ("package", s("Package name")),
                ("git", s("Git URL of the Package repository")),
                ("ref", json!({ "type": "string", "default": "main" })),
                ("subdir", s("Optional subdirectory inside the repository")),
            ],
            &["draft_id", "package", "git"],
        ),
        "secret_bind" => obj(
            vec![
                ("draft_id", s("Draft id")),
                ("instance_id", s("Connector Instance id")),
                ("slot", json!({ "type": "string", "default": "auth" })),
                ("secret_name", s("Existing secret name")),
            ],
            &["draft_id", "instance_id", "secret_name"],
        ),
        _ => obj(vec![], &[]),
    }
}

pub fn mcp_tool_definitions() -> Vec<Value> {
    let specs: &[(&str, &str)] = &[
        ("catalog_search", "Search installed Definitions across all categories"),
        ("contract_get", "Get generated manifest, JSON Schema, and Luau types for a Definition"),
        ("graph_inspect", "Inspect Configured Graph, bindings, Service principals, and validation"),
        ("capability_explain", "Explain effective authority for the current Principal on an Instance"),
        ("draft_begin", "Start an isolated Draft transaction from `base_revision` (optional; defaults to the active Revision)"),
        ("draft_status", "Get status, changes, validation, and test reports for a Draft"),
        ("draft_validate", "Run static contract, compatibility, capability, and cycle validation on a Draft"),
        ("draft_preview", "Instantiate isolated Preview with Mocks and run Scenario + Definition tests (includes Preview renders)"),
        ("draft_test", "Run Definition tests and all Scenario tests in isolated Preview (test report only)"),
        ("draft_rebase", "3-way rebase Draft onto current active Revision (fails explicitly on conflicts)"),
        ("draft_activate", "Commit, run Generation carry-over, atomically swap active Generation, and push"),
        ("draft_abort", "Abort and discard an unactivated Draft"),
        ("definition_create", "Create a new Luau Definition inside a Draft (fails if it already exists)"),
        ("definition_edit", "Edit an existing Luau Definition inside a Draft (fails if it does not exist)"),
        ("definition_test", "Run Luau Definition tests for one Definition against Mocks"),
        ("instance_create", "Create a configured Instance inside a Draft (fails if the id already exists)"),
        ("instance_configure", "Replace bindings, capabilities, or settings of an existing Instance inside a Draft"),
        ("instance_delete", "Delete an Instance inside a Draft"),
        ("dashboard_get", "Render and inspect a Dashboard document and its widget tree"),
        ("dashboard_update", "Update Dashboard layout, adapters, or overrides inside a Draft"),
        ("dashboard_preview", "Render a Dashboard in a Draft's isolated Mock Preview"),
        ("provider_query", "Execute or read a cached parameterized data-provider query"),
        ("action_dry_run", "Run an action's advisory Dry run without side effects"),
        ("action_invoke", "Invoke an authorized live action outside Drafts"),
        ("execution_inspect", "Inspect action execution records and uncertain outcomes visible to you"),
        ("execution_resolve", "Manually resolve an unknown action execution with audit trail"),
        ("execution_cancel", "Cancel a queued or running action execution (audited)"),
        ("logs_query", "Query sanitized runtime logs"),
        ("graph_trace", "Inspect read-only runtime Execution Graph and trace spans"),
        ("metrics_query", "Inspect runtime metrics (VMs, cache, subscribers, event lag, refresh latency, SQLite size)"),
        ("git_status", "Inspect Workspace Git status, branch, and remote sync state"),
        ("git_history", "Inspect Workspace Git commit history"),
        ("git_sync", "Retry pushing committed Workspace Revisions to remote"),
        ("dependency_update", "Add or update an external Git Package dependency in a Draft and regenerate packages.lock"),
        ("secret_list", "List metadata for accessible secrets (never returns plaintext)"),
        ("secret_bind", "Bind an existing secret to a connector Instance inside a Draft"),
    ];
    specs
        .iter()
        .map(|(name, desc)| {
            json!({
                "name": name,
                "description": desc,
                "inputSchema": tool_input_schema(name),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_tool_declares_its_required_arguments() {
        let tools = mcp_tool_definitions();
        assert_eq!(tools.len(), 36);
        for t in &tools {
            let schema = &t["inputSchema"];
            assert_eq!(schema["type"], "object");
            assert!(schema["properties"].is_object(), "{} lacks properties", t["name"]);
            for req in schema["required"].as_array().unwrap() {
                assert!(
                    schema["properties"].get(req.as_str().unwrap()).is_some(),
                    "{}: required '{}' not declared",
                    t["name"],
                    req
                );
            }
        }
        let find = |n: &str| tools.iter().find(|t| t["name"] == n).unwrap()["inputSchema"].clone();
        assert_eq!(find("action_invoke")["required"], json!(["action_instance"]));
        assert_eq!(
            find("definition_create")["required"],
            json!(["draft_id", "package", "name", "definition_luau"])
        );
        assert_eq!(find("secret_bind")["required"], json!(["draft_id", "instance_id", "secret_name"]));
        assert!(find("instance_create")["properties"]["instance"]["properties"]["category"]["enum"].is_array());
    }

    #[test]
    fn structured_content_is_always_an_object() {
        assert!(structured_content(&json!([1, 2])).is_object());
        assert_eq!(structured_content(&json!([1]))["items"], json!([1]));
        assert!(structured_content(&json!(null)).is_object());
        assert_eq!(structured_content(&json!({"a": 1})), json!({"a": 1}));
    }

    #[test]
    fn docs_are_embedded() {
        assert!(DOC_TOPICS.iter().any(|(t, _, body)| *t == "glossary" && body.contains("Generation")));
        let md = tool_reference_markdown();
        assert!(md.contains("## `action_invoke`"));
    }
}
