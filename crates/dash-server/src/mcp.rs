//! MCP Streamable HTTP endpoint (`/mcp`) with protocol version negotiation,
//! Origin validation, Agent Bearer auth, all 35 tools (§11.2), and MCP resources (§11.1, §11.3).

use crate::auth::{authenticate_request, validate_origin};
use crate::services::AppServices;
use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::IntoResponse,
    Json,
};
use dash_core::{DashboardDoc, InstanceConfig, Principal};
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

pub async fn mcp_post_handler(
    State(services): State<AppServices>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    if let Err((status, msg)) = validate_origin(&headers, peer.ip(), &services.trusted_proxies) {
        return (status, Json(json!({ "error": msg }))).into_response();
    }

    let auth = match authenticate_request(&services, &headers, &Method::POST, peer) {
        Ok(a) => a,
        Err((status, msg)) => return (status, Json(json!({ "error": msg }))).into_response(),
    };
    let principal = auth.principal().clone();

    // Negotiate MCP protocol version header if provided
    if let Some(proto) = headers
        .get("mcp-protocol-version")
        .and_then(|v| v.to_str().ok())
    {
        if !SUPPORTED_MCP_VERSIONS.contains(&proto) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("unsupported MCP-Protocol-Version '{proto}'") })),
            )
                .into_response();
        }
    }

    let id = req.id.clone().unwrap_or(Value::Null);
    if req.jsonrpc != "2.0" {
        return (
            StatusCode::BAD_REQUEST,
            Json(JsonRpcResponse {
                jsonrpc: "2.0".into(),
                id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32600,
                    message: "Invalid Request: expected jsonrpc 2.0".into(),
                }),
            }),
        )
            .into_response();
    }

    let session_id = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("mcp_{}", uuid::Uuid::now_v7().simple()));

    let result = match req.method.as_str() {
        "initialize" => {
            let client_ver = req
                .params
                .get("protocolVersion")
                .and_then(|v| v.as_str())
                .unwrap_or("2026-07-28");
            let chosen = if SUPPORTED_MCP_VERSIONS.contains(&client_ver) {
                client_ver
            } else {
                SUPPORTED_MCP_VERSIONS[0]
            };
            Ok(json!({
                "protocolVersion": chosen,
                "capabilities": {
                    "tools": { "listChanged": true },
                    "resources": { "subscribe": false, "listChanged": true }
                },
                "serverInfo": {
                    "name": "dash-mcp",
                    "version": "0.2.0"
                }
            }))
        }
        "notifications/initialized" | "ping" => Ok(json!({})),
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
            match call_mcp_tool(&services, &principal, name, args).await {
                Ok(val) => Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": serde_json::to_string_pretty(&val).unwrap_or_default()
                    }],
                    "structuredContent": val,
                    "isError": false
                })),
                Err(err) => Ok(json!({
                    "content": [{ "type": "text", "text": err }],
                    "isError": true
                })),
            }
        }
        "resources/list" => Ok(json!({ "resources": mcp_resources_list(&services) })),
        "resources/read" => {
            let uri = req
                .params
                .get("uri")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match read_mcp_resource(&services, &principal, uri) {
                Ok(val) => Ok(json!({
                    "contents": [{
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": serde_json::to_string_pretty(&val).unwrap_or_default()
                    }]
                })),
                Err(msg) => Err((-32602, msg)),
            }
        }
        other => Err((-32601, format!("method not found: {other}"))),
    };

    let mut resp = match result {
        Ok(res) => Json(JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id,
            result: Some(res),
            error: None,
        })
        .into_response(),
        Err((code, message)) => Json(JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(JsonRpcError { code, message }),
        })
        .into_response(),
    };
    if let Ok(hdr) = HeaderValue::from_str(&session_id) {
        resp.headers_mut().insert("mcp-session-id", hdr);
    }
    resp.headers_mut().insert(
        "mcp-protocol-version",
        HeaderValue::from_static("2026-07-28"),
    );
    resp
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
        "Streamable HTTP MCP server accepts JSON-RPC POST on /mcp",
    )
        .into_response()
}

pub async fn mcp_delete_handler(
    State(services): State<AppServices>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err((status, msg)) = authenticate_request(&services, &headers, &Method::DELETE, peer) {
        return (status, msg).into_response();
    }
    StatusCode::OK.into_response()
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
    match name {
        // Discover
        "catalog_search" => {
            let q = args.get("query").and_then(|v| v.as_str());
            Ok(json!(services.catalog_search(q)))
        }
        "contract_get" => services.contract_get(req_str(&args, "definition_id")?),
        "graph_inspect" => Ok(services.graph_inspect()),
        "capability_explain" => Ok(services.capability_explain(req_str(&args, "target")?, principal)),

        // Drafts
        "draft_begin" => Ok(json!(services.draft_begin(principal).await?)),
        "draft_status" => Ok(json!(services.draft_status(req_str(&args, "draft_id")?)?)),
        "draft_validate" => Ok(json!(services.draft_validate(req_str(&args, "draft_id")?).await?)),
        "draft_preview" => Ok(json!(services.draft_preview(req_str(&args, "draft_id")?).await?)),
        "draft_test" => Ok(json!(services.draft_test(req_str(&args, "draft_id")?).await?)),
        "draft_rebase" => Ok(json!(services.draft_rebase(req_str(&args, "draft_id")?).await?)),
        "draft_activate" => Ok(json!(services.draft_activate(req_str(&args, "draft_id")?, principal).await?)),
        "draft_abort" => {
            services.draft_abort(req_str(&args, "draft_id")?).await?;
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
            Ok(json!(services.definition_upsert_in_draft(
                draft_id,
                package,
                def_name,
                def_luau,
                rt_luau,
                tests_luau,
                principal
            )?))
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
            Ok(json!(services.instance_upsert_in_draft(
                draft_id,
                &inst,
                principal
            )?))
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
                .dashboard_preview(
                    req_str(&args, "draft_id")?,
                    req_str(&args, "dashboard_id")?,
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
            services.execution_inspect(exec_id)
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

fn mcp_resources_list(services: &AppServices) -> Vec<Value> {
    let mut resources = vec![
        json!({ "uri": "dash://manifests", "name": "Generated Definition Manifests", "mimeType": "application/json" }),
        json!({ "uri": "dash://graph", "name": "Configured Graph & Validation Report", "mimeType": "application/json" }),
        json!({ "uri": "dash://traces", "name": "Sanitized Runtime Execution Graph & Traces", "mimeType": "application/json" }),
        json!({ "uri": "dash://logs", "name": "Sanitized Runtime Logs", "mimeType": "application/json" }),
        json!({ "uri": "dash://metrics", "name": "Runtime Metrics", "mimeType": "application/json" }),
    ];
    for id in services.graph().definitions.keys() {
        resources.push(json!({
            "uri": format!("dash://definitions/{id}"),
            "name": format!("Contract & Luau Types: {id}"),
            "mimeType": "application/json"
        }));
    }
    resources
}

fn read_mcp_resource(
    services: &AppServices,
    _principal: &Principal,
    uri: &str,
) -> Result<Value, String> {
    match uri {
        "dash://manifests" => {
            let g = services.graph();
            let map: std::collections::BTreeMap<String, Value> = g
                .definitions
                .iter()
                .map(|(id, d)| (id.clone(), serde_json::to_value(&d.manifest).unwrap()))
                .collect();
            Ok(json!(map))
        }
        "dash://graph" => Ok(services.graph_inspect()),
        "dash://traces" => Ok(services.graph_trace(100)),
        "dash://logs" => Ok(json!(services.logs_query(100))),
        "dash://metrics" => Ok(json!(services.metrics_query())),
        other if other.starts_with("dash://definitions/") => {
            let def_id = other.strip_prefix("dash://definitions/").unwrap();
            services.contract_get(def_id)
        }
        other => Err(format!("unknown MCP resource '{other}'")),
    }
}

pub fn mcp_tool_definitions() -> Vec<Value> {
    let specs: &[(&str, &str)] = &[
        ("catalog_search", "Search installed Definitions across all categories"),
        ("contract_get", "Get generated manifest, JSON Schema, and Luau types for a Definition"),
        ("graph_inspect", "Inspect Configured Graph, bindings, Service principals, and validation"),
        ("capability_explain", "Explain effective authority for the current Principal on an Instance"),
        ("draft_begin", "Start an isolated Draft transaction against the active Revision"),
        ("draft_status", "Get status, changes, validation, and test reports for a Draft"),
        ("draft_validate", "Run static contract, compatibility, capability, and cycle validation on a Draft"),
        ("draft_preview", "Instantiate isolated Preview with Mocks and run Scenario + Definition tests"),
        ("draft_test", "Run Definition tests and all Scenario tests in isolated Preview"),
        ("draft_rebase", "3-way rebase Draft onto current active Revision (fails explicitly on conflicts)"),
        ("draft_activate", "Commit, run Generation carry-over, atomically swap active Generation, and push"),
        ("draft_abort", "Abort and discard an unactivated Draft"),
        ("definition_create", "Create a new Luau Definition inside a Draft"),
        ("definition_edit", "Edit an existing Luau Definition inside a Draft"),
        ("definition_test", "Run Luau Definition tests for one Definition against Mocks"),
        ("instance_create", "Create a configured Instance inside a Draft"),
        ("instance_configure", "Configure bindings, capabilities, or settings for an Instance inside a Draft"),
        ("instance_delete", "Delete an Instance inside a Draft"),
        ("dashboard_get", "Render and inspect a Dashboard document and its widget tree"),
        ("dashboard_update", "Update Dashboard layout, adapters, or overrides inside a Draft"),
        ("dashboard_preview", "Render a Dashboard in a Draft's isolated Mock Preview"),
        ("provider_query", "Execute or read a cached parameterized data-provider query"),
        ("action_dry_run", "Run an action's advisory Dry run without side effects"),
        ("action_invoke", "Invoke an authorized live action outside Drafts"),
        ("execution_inspect", "Inspect action execution records and uncertain outcomes"),
        ("execution_resolve", "Manually resolve an unknown action execution with audit trail"),
        ("execution_cancel", "Cancel a queued or running action execution (audited)"),
        ("logs_query", "Query sanitized runtime logs"),
        ("graph_trace", "Inspect read-only runtime Execution Graph and trace spans"),
        ("metrics_query", "Inspect runtime metrics (VMs, cache, subscribers, SQLite size)"),
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
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": true
                }
            })
        })
        .collect()
}
