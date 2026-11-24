//! Argon2id browser login, rate limiting, HttpOnly/SameSite cookies, CSRF, Origin checks,
//! trusted-proxy handling, and scoped Agent Bearer authentication (§14.2, §14.3).

use crate::context::{split_host_port, SecureCookieMode};
use crate::services::AppServices;
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::{Duration, Utc};
use dash_core::telemetry::{note_request_principal, REQUEST_TRACE};
use dash_core::{CapabilityCeiling, Principal, PrincipalKind, RequestTraceContext, Telemetry};
use dashmap::DashMap;
use rand::rngs::OsRng;
use rand::RngCore;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

/// Login attempts allowed per client address per window.
pub const LOGIN_ATTEMPTS_PER_WINDOW: usize = 15;
const LOGIN_WINDOW: std::time::Duration = std::time::Duration::from_secs(60);
/// Sweep stale limiter entries once the map holds this many client addresses (§20 bounded maps).
const LIMITER_SWEEP_THRESHOLD: usize = 1024;

#[derive(Clone, Default)]
pub struct LoginRateLimiter {
    attempts: Arc<DashMap<IpAddr, Vec<Instant>>>,
}

impl LoginRateLimiter {
    /// Record one attempt for the *client* address (see [`client_ip`]); false when over the limit.
    pub fn check_and_record(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        if self.attempts.len() >= LIMITER_SWEEP_THRESHOLD {
            self.attempts.retain(|_, v| {
                v.retain(|t| now.duration_since(*t) < LOGIN_WINDOW);
                !v.is_empty()
            });
        }
        let mut entry = self.attempts.entry(ip).or_default();
        entry.retain(|t| now.duration_since(*t) < LOGIN_WINDOW);
        if entry.len() >= LOGIN_ATTEMPTS_PER_WINDOW {
            return false;
        }
        entry.push(now);
        true
    }

    pub fn clear(&self, ip: IpAddr) {
        self.attempts.remove(&ip);
    }

    pub fn tracked_clients(&self) -> usize {
        self.attempts.len()
    }
}

/// Resolve the originating client address. When the TCP peer is an explicitly trusted proxy
/// (e.g. Tailscale Serve on loopback), walk `X-Forwarded-For` right-to-left and return the first
/// hop that is not itself a trusted proxy; otherwise the TCP peer is the client (§14.2).
pub fn client_ip(headers: &HeaderMap, peer_ip: IpAddr, trusted_proxies: &[IpAddr]) -> IpAddr {
    if !trusted_proxies.contains(&peer_ip) {
        return peer_ip;
    }
    let mut hops: Vec<IpAddr> = Vec::new();
    for v in headers.get_all("x-forwarded-for") {
        if let Ok(s) = v.to_str() {
            for part in s.split(',') {
                if let Ok(ip) = part.trim().parse::<IpAddr>() {
                    hops.push(ip);
                }
            }
        }
    }
    hops.into_iter()
        .rev()
        .find(|ip| !trusted_proxies.contains(ip))
        .unwrap_or(peer_ip)
}

/// True when the request carries any proxy-forwarding header.
fn has_forwarding_headers(headers: &HeaderMap) -> bool {
    ["x-forwarded-for", "forwarded", "x-forwarded-host", "x-real-ip"]
        .iter()
        .any(|h| headers.contains_key(*h))
}

/// Decide whether the session cookie gets the `Secure` attribute (§14.3).
pub fn session_cookie_secure(services: &AppServices, headers: &HeaderMap, peer_ip: IpAddr) -> bool {
    match services.server.secure_cookies {
        SecureCookieMode::Always => true,
        SecureCookieMode::Never => false,
        SecureCookieMode::Auto => {
            if is_request_https(headers, peer_ip, &services.trusted_proxies) {
                return true;
            }
            // Deliberate non-loopback bind reached under a non-loopback name: the cookie would
            // otherwise cross a network in cleartext. Loopback names (e.g. Docker's
            // `-p 127.0.0.1:8080:8080`) stay usable over plain HTTP.
            services.server.bind_is_public
                && !headers
                    .get(axum::http::header::HOST)
                    .and_then(|v| v.to_str().ok())
                    .map(|h| {
                        let (host, _) = split_host_port(h);
                        host == "localhost" || host == "::1" || host.starts_with("127.")
                    })
                    .unwrap_or(false)
        }
    }
}

pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| e.to_string())
}

pub fn verify_password(password: &str, hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

pub fn generate_random_token(prefix: &str) -> String {
    let mut bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("{prefix}_{}", hex::encode(bytes))
}

/// Extract cookie by name from Cookie header.
pub fn extract_cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let raw = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    for part in raw.split(';') {
        let trimmed = part.trim();
        if let Some((k, v)) = trimmed.split_once('=') {
            if k.trim() == name {
                return Some(v.trim());
            }
        }
    }
    None
}

/// Resolve whether the connection came over HTTPS (directly or via a trusted proxy `X-Forwarded-Proto`).
pub fn is_request_https(
    headers: &HeaderMap,
    peer_ip: IpAddr,
    trusted_proxies: &[IpAddr],
) -> bool {
    if trusted_proxies.contains(&peer_ip) {
        if let Some(proto) = headers
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
        {
            return proto.eq_ignore_ascii_case("https");
        }
    }
    false
}

/// Validate the `Origin` header on API, MCP and WebSocket entry points (§11.1, §14.3).
///
/// A browser Origin must match the request's own origin exactly: same host and same port
/// (default ports normalized), and a compatible scheme. The expected authority is the `Host`
/// header, or `X-Forwarded-Host` only when the TCP peer is a trusted proxy. Host names
/// themselves are checked against the allowlist by [`host_guard`] (DNS-rebinding defense).
/// Requests without an `Origin` header (CLI, curl, stdio proxy) are not browser cross-origin
/// requests and must still authenticate.
pub fn validate_origin(
    headers: &HeaderMap,
    peer_ip: IpAddr,
    trusted_proxies: &[IpAddr],
) -> Result<(), (StatusCode, &'static str)> {
    let Some(origin_raw) = headers.get(axum::http::header::ORIGIN).and_then(|v| v.to_str().ok()) else {
        return Ok(());
    };
    let Ok(origin_url) = url::Url::parse(origin_raw) else {
        return Err((StatusCode::FORBIDDEN, "invalid Origin header"));
    };
    let scheme = origin_url.scheme();
    if scheme != "http" && scheme != "https" {
        return Err((StatusCode::FORBIDDEN, "cross-origin request rejected"));
    }
    let Some(origin_host) = origin_url.host_str() else {
        return Err((StatusCode::FORBIDDEN, "invalid Origin header"));
    };
    let origin_host = origin_host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    let origin_port = origin_url.port();

    let trusted_peer = trusted_proxies.contains(&peer_ip);
    let expected = if trusted_peer {
        headers
            .get("x-forwarded-host")
            .and_then(|v| v.to_str().ok())
            .or_else(|| headers.get(axum::http::header::HOST).and_then(|v| v.to_str().ok()))
    } else {
        headers.get(axum::http::header::HOST).and_then(|v| v.to_str().ok())
    };
    let Some(expected) = expected else {
        return Err((StatusCode::FORBIDDEN, "cross-origin request rejected"));
    };
    let (exp_host, mut exp_port) = split_host_port(expected);
    let default_port = if scheme == "https" { 443 } else { 80 };
    if exp_port == Some(default_port) {
        exp_port = None;
    }
    if origin_host != exp_host || origin_port != exp_port {
        return Err((StatusCode::FORBIDDEN, "cross-origin request rejected"));
    }
    // Scheme: an https page behind a TLS-terminating trusted proxy talks to us over http;
    // otherwise the scheme must match what this hop observed.
    let request_https = is_request_https(headers, peer_ip, trusted_proxies);
    let scheme_ok = if request_https {
        scheme == "https"
    } else {
        scheme == "http" || trusted_peer
    };
    if !scheme_ok {
        return Err((StatusCode::FORBIDDEN, "cross-origin request rejected"));
    }
    Ok(())
}

/// Router-wide guard (§11.1 DNS-rebinding defense, §15.2 drain):
/// - rejects requests whose `Host` (and trusted `X-Forwarded-Host`) is not on the allowlist,
/// - refuses new state-changing requests once graceful shutdown has begun,
/// - opens a request-scoped root trace span for API/MCP/WebSocket requests (§15.1).
pub async fn host_guard(
    State(services): State<AppServices>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let peer_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip());
    let headers = req.headers();
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| req.uri().authority().map(|a| a.as_str().to_string()));
    if let Some(ref h) = host {
        if !services.server.host_allowed(h) {
            return (
                StatusCode::MISDIRECTED_REQUEST,
                axum::Json(serde_json::json!({ "error": "Host not allowed (configure --allowed-host)" })),
            )
                .into_response();
        }
    }
    let trusted_peer = peer_ip.is_some_and(|ip| services.trusted_proxies.contains(&ip));
    if trusted_peer {
        if let Some(fh) = headers.get("x-forwarded-host").and_then(|v| v.to_str().ok()) {
            if !services.server.host_allowed(fh) {
                return (
                    StatusCode::MISDIRECTED_REQUEST,
                    axum::Json(serde_json::json!({ "error": "forwarded Host not allowed" })),
                )
                    .into_response();
            }
        }
    }
    let client = peer_ip.map(|ip| client_ip(headers, ip, &services.trusted_proxies));
    let method = req.method().clone();
    if services.server.is_shutting_down()
        && !matches!(method, Method::GET | Method::HEAD | Method::OPTIONS)
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({ "error": "daemon is shutting down" })),
        )
            .into_response();
    }

    let path = req.uri().path().to_string();
    // Trace API/MCP/WebSocket entry points, but not the UI's own diagnostics polling (which would
    // otherwise flood the bounded trace views it is displaying).
    let traced = (path.starts_with("/api/") || path == "/mcp" || path == "/ws")
        && !(method == Method::GET
            && (path.starts_with("/api/diagnostics/") || path == "/api/auth/me"));
    if traced {
        let telemetry = services.engine().telemetry.clone();
        let trace_id = Telemetry::new_trace_id();
        let root = telemetry.start_span(&trace_id, None, &format!("http:{method} {path}"), "anonymous");
        let ctx = RequestTraceContext::new(trace_id, root.span_id.clone());
        let resp = REQUEST_TRACE.scope(ctx.clone(), next.run(req)).await;
        let (pid, pkind) = ctx
            .principal()
            .unwrap_or_else(|| ("anonymous".into(), "anonymous".into()));
        let status = resp.status();
        let mut attrs = serde_json::json!({
            "method": method.as_str(),
            "path": path,
            "http_status": status.as_u16(),
            "principal_id": pid,
            "principal_kind": pkind,
        });
        if let Some(ip) = client {
            attrs["client_ip"] = serde_json::json!(ip.to_string());
        }
        root.finish(if status.is_success() || status.is_informational() { "ok" } else { "error" }, attrs);
        resp
    } else {
        next.run(req).await
    }
}

pub enum AuthContext {
    BrowserSession {
        principal: Principal,
        csrf_token: String,
    },
    BearerToken {
        principal: Principal,
    },
}

impl AuthContext {
    pub fn principal(&self) -> &Principal {
        match self {
            Self::BrowserSession { principal, .. } => principal,
            Self::BearerToken { principal } => principal,
        }
    }
}

/// Authenticate either via `Authorization: Bearer <token>` (Agent MCP token or local admin CLI token)
/// OR `dash_session` cookie (Browser Administrator session).
/// NEVER trusts `Tailscale-User-*` / `X-Forwarded-User` identity headers (§14.2)!
pub fn authenticate_request(
    services: &AppServices,
    headers: &HeaderMap,
    method: &Method,
    peer_addr: SocketAddr,
) -> Result<AuthContext, (StatusCode, String)> {
    validate_origin(headers, peer_addr.ip(), &services.trusted_proxies)
        .map_err(|(s, m)| (s, m.to_string()))?;

    // 1. Bearer token (Agent MCP token or local Admin CLI token)
    if let Some(auth_hdr) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(token) = auth_hdr.strip_prefix("Bearer ").map(str::trim) {
            if let Ok(Some(agent)) = services.store().authenticate_agent_token(token) {
                let ceiling: CapabilityCeiling =
                    serde_json::from_value(agent.ceiling).unwrap_or_default();
                note_request_principal(&agent.agent_id, PrincipalKind::Agent.as_str());
                return Ok(AuthContext::BearerToken {
                    principal: Principal {
                        id: agent.agent_id,
                        kind: PrincipalKind::Agent,
                        ceiling,
                    },
                });
            }
            // Local admin CLI token (`<data_dir>/admin.token`, 0600): used only by `dash` CLI
            // subcommands talking to the running daemon. Accepted solely from a loopback peer
            // with no proxy-forwarding headers, so it can never be used through Tailscale Serve
            // or any other proxy hop (§14.2, §14.3).
            if admin_cli_token_matches(services, token, headers, peer_addr.ip()) {
                note_request_principal("admin", PrincipalKind::Administrator.as_str());
                return Ok(AuthContext::BearerToken {
                    principal: Principal::administrator(),
                });
            }
            return Err((StatusCode::UNAUTHORIZED, "invalid or revoked bearer token".into()));
        }
    }

    // 2. Browser session cookie
    if let Some(cookie_tok) = extract_cookie(headers, "dash_session") {
        if let Ok(Some((_sid, csrf_token, pid))) = services.store().validate_session(cookie_tok) {
            // Enforce CSRF token header on state-changing browser mutations (§14.3)
            let is_mutation = matches!(
                *method,
                Method::POST | Method::PUT | Method::PATCH | Method::DELETE
            );
            if is_mutation {
                let supplied_csrf = headers
                    .get("x-csrf-token")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                if !constant_time_eq(supplied_csrf.as_bytes(), csrf_token.as_bytes()) {
                    return Err((
                        StatusCode::FORBIDDEN,
                        "missing or invalid X-CSRF-Token".into(),
                    ));
                }
            }
            let mut admin = Principal::administrator();
            admin.id = pid;
            note_request_principal(&admin.id, PrincipalKind::Administrator.as_str());
            return Ok(AuthContext::BrowserSession {
                principal: admin,
                csrf_token,
            });
        }
    }

    Err((StatusCode::UNAUTHORIZED, "authentication required".into()))
}

/// Whether `token` is the local admin CLI token and the request is eligible to use it.
pub fn admin_cli_token_matches(
    services: &AppServices,
    token: &str,
    headers: &HeaderMap,
    peer_ip: IpAddr,
) -> bool {
    if !peer_ip.is_loopback() || has_forwarding_headers(headers) {
        return false;
    }
    let Ok(admin_tok) = std::fs::read_to_string(&services.admin_cli_token_path) else {
        return false;
    };
    let expected = admin_tok.trim();
    if expected.is_empty() {
        return false;
    }
    // Compare SHA-256 digests in constant time (fixed length, no early exit on prefix).
    constant_time_eq(
        dash_store::hash_token(token).as_bytes(),
        dash_store::hash_token(expected).as_bytes(),
    )
}

pub fn create_browser_session(
    services: &AppServices,
    username: &str,
) -> Result<(String, String), String> {
    let token = generate_random_token("ses");
    let csrf = generate_random_token("csrf");
    let expires = Utc::now() + Duration::hours(24);
    services
        .store()
        .create_session(&token, &csrf, username, expires)
        .map_err(|e| e.to_string())?;
    Ok((token, csrf))
}

pub fn build_session_cookie(token: &str, secure: bool) -> String {
    if secure {
        format!(
            "dash_session={token}; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=86400"
        )
    } else {
        format!("dash_session={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400")
    }
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
