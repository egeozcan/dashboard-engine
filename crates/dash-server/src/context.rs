//! Server-wide configuration and runtime state shared by the HTTP, WebSocket and MCP entry points:
//! the Host allowlist (DNS-rebinding defense), cookie policy, shutdown/drain flag, MCP sessions and
//! the storage retention policy (§11.1, §13, §14.2, §14.3, §15.2).

use dash_store::RetentionPolicy;
use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// When to mark the browser session cookie `Secure`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecureCookieMode {
    /// `Secure` when the request arrived over HTTPS (directly or via a trusted proxy's
    /// `X-Forwarded-Proto`) or the daemon is bound to a non-loopback address.
    #[default]
    Auto,
    /// Always `Secure` (use behind an HTTPS proxy that does not send `X-Forwarded-Proto`).
    Always,
    /// Never `Secure` (plain-HTTP loopback development only).
    Never,
}

impl std::str::FromStr for SecureCookieMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "always" => Ok(Self::Always),
            "never" => Ok(Self::Never),
            other => Err(format!("invalid secure-cookies mode '{other}' (auto|always|never)")),
        }
    }
}

/// One accepted `Host` pattern:
/// - `name` accepts that host on any port,
/// - `name:port` accepts exactly that authority,
/// - `.suffix` accepts any subdomain of `suffix` (e.g. `.ts.net` for Tailscale Serve names),
/// - `*` disables the Host check (explicit opt-out; not recommended).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPattern(String);

impl HostPattern {
    pub fn new(raw: &str) -> Self {
        Self(raw.trim().to_ascii_lowercase())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn matches(&self, host: &str, port: Option<u16>) -> bool {
        let p = self.0.as_str();
        if p == "*" {
            return true;
        }
        if let Some(suffix) = p.strip_prefix('.') {
            return host == suffix || host.ends_with(&format!(".{suffix}"));
        }
        let (ph, pp) = split_host_port(p);
        if ph != host {
            return false;
        }
        match pp {
            Some(want) => port == Some(want),
            None => true,
        }
    }
}

/// Split `host[:port]` / `[v6]:port` into a lowercase host (without brackets) and port.
pub fn split_host_port(authority: &str) -> (String, Option<u16>) {
    let a = authority.trim().to_ascii_lowercase();
    if let Some(rest) = a.strip_prefix('[') {
        if let Some((h, tail)) = rest.split_once(']') {
            let port = tail.strip_prefix(':').and_then(|p| p.parse().ok());
            return (h.to_string(), port);
        }
    }
    // Bare IPv6 without brackets has several colons: treat it all as the host.
    if a.matches(':').count() > 1 {
        return (a, None);
    }
    match a.rsplit_once(':') {
        Some((h, p)) => match p.parse::<u16>() {
            Ok(port) => (h.to_string(), Some(port)),
            Err(_) => (a.clone(), None),
        },
        None => (a, None),
    }
}

/// Live MCP Streamable HTTP session, bound to the Principal that initialized it.
#[derive(Debug, Clone)]
pub struct McpSession {
    pub principal_id: String,
    pub protocol_version: String,
    pub last_seen: Instant,
}

/// Upper bound on concurrently tracked MCP sessions (§20: no unbounded caches).
pub const MAX_MCP_SESSIONS: usize = 4096;
/// Idle MCP sessions expire after this long.
pub const MCP_SESSION_IDLE: Duration = Duration::from_secs(24 * 3600);

#[derive(Clone)]
pub struct ServerContext {
    pub allowed_hosts: Arc<Vec<HostPattern>>,
    pub secure_cookies: SecureCookieMode,
    /// True when the daemon listens on a non-loopback address (deliberate exposure).
    pub bind_is_public: bool,
    pub retention: RetentionPolicy,
    shutting_down: Arc<AtomicBool>,
    pub mcp_sessions: Arc<DashMap<String, McpSession>>,
}

impl Default for ServerContext {
    fn default() -> Self {
        Self {
            allowed_hosts: Arc::new(Self::loopback_hosts()),
            secure_cookies: SecureCookieMode::Auto,
            bind_is_public: false,
            retention: RetentionPolicy::default(),
            shutting_down: Arc::new(AtomicBool::new(false)),
            mcp_sessions: Arc::new(DashMap::new()),
        }
    }
}

impl ServerContext {
    /// Loopback names are always accepted (any port); a DNS-rebinding page never has them as Host.
    pub fn loopback_hosts() -> Vec<HostPattern> {
        ["localhost", "127.0.0.1", "::1"]
            .iter()
            .map(|h| HostPattern::new(h))
            .collect()
    }

    /// Build the allowlist from loopback names, the bind address (when specific) and extra
    /// operator-configured hosts (e.g. the Tailscale Serve MagicDNS name).
    pub fn with_allowed_hosts(mut self, bind_ip: Option<IpAddr>, extra: &[String]) -> Self {
        let mut hosts = Self::loopback_hosts();
        if let Some(ip) = bind_ip {
            if !ip.is_unspecified() && !ip.is_loopback() {
                hosts.push(HostPattern::new(&ip.to_string()));
            }
            self.bind_is_public = !ip.is_loopback();
        }
        for h in extra {
            for part in h.split(',') {
                if !part.trim().is_empty() {
                    hosts.push(HostPattern::new(part));
                }
            }
        }
        self.allowed_hosts = Arc::new(hosts);
        self
    }

    pub fn with_secure_cookies(mut self, mode: SecureCookieMode) -> Self {
        self.secure_cookies = mode;
        self
    }

    pub fn with_retention(mut self, policy: RetentionPolicy) -> Self {
        self.retention = policy;
        self
    }

    /// Whether an authority (`Host` / `X-Forwarded-Host` value) is on the allowlist.
    pub fn host_allowed(&self, authority: &str) -> bool {
        let (host, port) = split_host_port(authority);
        if host.is_empty() {
            return false;
        }
        self.allowed_hosts.iter().any(|p| p.matches(&host, port))
    }

    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    /// Create an MCP session for `principal_id`, evicting idle/oldest sessions beyond the cap.
    pub fn mcp_session_create(&self, principal_id: &str, protocol_version: &str) -> String {
        let now = Instant::now();
        self.mcp_sessions
            .retain(|_, s| now.duration_since(s.last_seen) < MCP_SESSION_IDLE);
        while self.mcp_sessions.len() >= MAX_MCP_SESSIONS {
            let oldest = self
                .mcp_sessions
                .iter()
                .min_by_key(|e| e.value().last_seen)
                .map(|e| e.key().clone());
            match oldest {
                Some(k) => {
                    self.mcp_sessions.remove(&k);
                }
                None => break,
            }
        }
        let id = format!("mcp_{}", uuid::Uuid::new_v4().simple());
        self.mcp_sessions.insert(
            id.clone(),
            McpSession {
                principal_id: principal_id.to_string(),
                protocol_version: protocol_version.to_string(),
                last_seen: now,
            },
        );
        id
    }

    /// Validate a client-supplied session id for `principal_id`; refreshes its idle timer.
    /// Returns the negotiated protocol version on success.
    pub fn mcp_session_touch(&self, session_id: &str, principal_id: &str) -> Option<String> {
        let mut entry = self.mcp_sessions.get_mut(session_id)?;
        if entry.principal_id != principal_id
            || entry.last_seen.elapsed() >= MCP_SESSION_IDLE
        {
            return None;
        }
        entry.last_seen = Instant::now();
        Some(entry.protocol_version.clone())
    }

    /// Terminate a session (MCP `DELETE /mcp`). Only its owning Principal may terminate it.
    pub fn mcp_session_terminate(&self, session_id: &str, principal_id: &str) -> bool {
        self.mcp_sessions
            .remove_if(session_id, |_, s| s.principal_id == principal_id)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_allowlist_rejects_rebinding_names() {
        let ctx = ServerContext::default();
        assert!(ctx.host_allowed("127.0.0.1:8080"));
        assert!(ctx.host_allowed("localhost:5173"));
        assert!(ctx.host_allowed("[::1]:8080"));
        assert!(!ctx.host_allowed("evil.example:8080"));
        assert!(!ctx.host_allowed("localhost.evil.example"));
        assert!(!ctx.host_allowed(""));

        let ts = ServerContext::default().with_allowed_hosts(
            Some("127.0.0.1".parse().unwrap()),
            &["dash-box.tailnet-abc.ts.net".into(), "other.lan:9000".into()],
        );
        assert!(ts.host_allowed("dash-box.tailnet-abc.ts.net"));
        assert!(!ts.host_allowed("evil-box.tailnet-abc.ts.net"));
        assert!(ts.host_allowed("other.lan:9000"));
        assert!(!ts.host_allowed("other.lan:9001"));
        assert!(!ts.bind_is_public);

        let suffix = ServerContext::default().with_allowed_hosts(None, &[".ts.net".into()]);
        assert!(suffix.host_allowed("box.tailnet.ts.net"));
        assert!(!suffix.host_allowed("ts.net.evil.example"));
    }

    #[test]
    fn mcp_sessions_are_principal_bound_and_bounded() {
        let ctx = ServerContext::default();
        let sid = ctx.mcp_session_create("agt_a", "2025-06-18");
        assert_eq!(ctx.mcp_session_touch(&sid, "agt_a").as_deref(), Some("2025-06-18"));
        assert!(ctx.mcp_session_touch(&sid, "agt_b").is_none());
        assert!(!ctx.mcp_session_terminate(&sid, "agt_b"));
        assert!(ctx.mcp_session_terminate(&sid, "agt_a"));
        assert!(ctx.mcp_session_touch(&sid, "agt_a").is_none());
        for _ in 0..(MAX_MCP_SESSIONS + 5) {
            ctx.mcp_session_create("agt_a", "2026-07-28");
        }
        assert!(ctx.mcp_sessions.len() <= MAX_MCP_SESSIONS);
    }
}
