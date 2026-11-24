//! Argon2id browser login, rate limiting, HttpOnly/SameSite cookies, CSRF, Origin checks,
//! trusted-proxy handling, and scoped Agent Bearer authentication (§14.2, §14.3).

use crate::services::AppServices;
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::http::{HeaderMap, Method, StatusCode};
use chrono::{Duration, Utc};
use dash_core::{CapabilityCeiling, Principal, PrincipalKind};
use dashmap::DashMap;
use rand::rngs::OsRng;
use rand::RngCore;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Default)]
pub struct LoginRateLimiter {
    attempts: Arc<DashMap<IpAddr, Vec<Instant>>>,
}

impl LoginRateLimiter {
    pub fn check_and_record(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let window = std::time::Duration::from_secs(60);
        let mut entry = self.attempts.entry(ip).or_default();
        entry.retain(|t| now.duration_since(*t) < window);
        if entry.len() >= 15 {
            return false;
        }
        entry.push(now);
        true
    }

    pub fn clear(&self, ip: IpAddr) {
        self.attempts.remove(&ip);
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

/// Validate `Origin` header on state-changing requests, WebSocket handshakes, and MCP endpoints (§11.1, §14.3).
/// Rejects cross-origin browser requests while respecting trusted proxy `X-Forwarded-Host`.
pub fn validate_origin(
    headers: &HeaderMap,
    peer_ip: IpAddr,
    trusted_proxies: &[IpAddr],
) -> Result<(), (StatusCode, &'static str)> {
    let Some(origin_raw) = headers.get(axum::http::header::ORIGIN).and_then(|v| v.to_str().ok()) else {
        // Non-browser CLI / curl / stdio-proxy clients without Origin header are allowed when authenticated
        return Ok(());
    };
    let Ok(origin_url) = url::Url::parse(origin_raw) else {
        return Err((StatusCode::FORBIDDEN, "invalid Origin header"));
    };
    let origin_host_port = match origin_url.port() {
        Some(p) => format!("{}:{p}", origin_url.host_str().unwrap_or("")),
        None => origin_url.host_str().unwrap_or("").to_string(),
    };
    let origin_host = origin_url.host_str().unwrap_or("");

    let expected_host = if trusted_proxies.contains(&peer_ip) {
        headers
            .get("x-forwarded-host")
            .and_then(|v| v.to_str().ok())
            .or_else(|| headers.get(axum::http::header::HOST).and_then(|v| v.to_str().ok()))
    } else {
        headers.get(axum::http::header::HOST).and_then(|v| v.to_str().ok())
    };

    if let Some(host_hdr) = expected_host {
        let host_only = host_hdr.split(':').next().unwrap_or(host_hdr);
        if origin_host_port.eq_ignore_ascii_case(host_hdr)
            || origin_host.eq_ignore_ascii_case(host_only)
        {
            return Ok(());
        }
    }

    Err((StatusCode::FORBIDDEN, "cross-origin request rejected"))
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
                return Ok(AuthContext::BearerToken {
                    principal: Principal {
                        id: agent.agent_id,
                        kind: PrincipalKind::Agent,
                        ceiling,
                    },
                });
            }
            // Check local admin CLI token file
            if let Ok(admin_tok) = std::fs::read_to_string(&services.admin_cli_token_path) {
                if !admin_tok.trim().is_empty() && constant_time_eq(token.as_bytes(), admin_tok.trim().as_bytes()) {
                    return Ok(AuthContext::BearerToken {
                        principal: Principal::administrator(),
                    });
                }
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
            return Ok(AuthContext::BrowserSession {
                principal: admin,
                csrf_token,
            });
        }
    }

    Err((StatusCode::UNAUTHORIZED, "authentication required".into()))
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
