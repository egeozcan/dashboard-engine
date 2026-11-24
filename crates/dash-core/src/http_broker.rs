//! Host-mediated outbound HTTP broker with allowlists, SSRF protection, redirect validation, and credential injection.

use crate::authority::{check_connector_operation_authority, AuthError, Principal};
use crate::model::{ConfiguredGraph, InstanceConfig, NetworkPolicy};
use dash_luau::{HttpHostRequest, HttpHostResponse};
use dash_store::SqliteStore;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::Duration;
use thiserror::Error;
use url::Url;

const MAX_REDIRECTS: usize = 5;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum HttpBrokerError {
    #[error("authority denied: {0}")]
    Auth(#[from] AuthError),
    #[error("invalid url '{0}': {1}")]
    InvalidUrl(String, String),
    #[error("network policy denied destination '{0}': {1}")]
    PolicyDenied(String, String),
    #[error("SSRF protection blocked private/loopback address '{0}'")]
    SsrfBlocked(String),
    #[error("credential error for '{0}': {1}")]
    CredentialError(String, String),
    #[error("http transport failed: {0}")]
    Transport(String),
    /// The request may already have reached the upstream (failure after the connection was established,
    /// a timeout, or a lost response body). Carries the stable `UNKNOWN_OUTCOME` code so action
    /// executions record `unknown`, never `failed` (§9.1, Appendix A.4).
    #[error("UNKNOWN_OUTCOME: http request may have reached the upstream but no complete response was received: {0}")]
    OutcomeUnknown(String),
    #[error("too many redirects")]
    TooManyRedirects,
    #[error("response exceeded maximum size of {MAX_RESPONSE_BYTES} bytes")]
    ResponseTooLarge,
}

#[derive(Clone)]
pub struct HttpBroker {
    client: reqwest::Client,
    store: SqliteStore,
}

impl HttpBroker {
    pub fn new(store: SqliteStore) -> Result<Self, HttpBrokerError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("dash-engine/0.2")
            .build()
            .map_err(|e| HttpBrokerError::Transport(e.to_string()))?;
        Ok(Self { client, store })
    }

    pub async fn execute(
        &self,
        principal: &Principal,
        connector_inst: &InstanceConfig,
        operation: Option<&str>,
        graph: &ConfiguredGraph,
        req: HttpHostRequest,
    ) -> Result<HttpHostResponse, HttpBrokerError> {
        if let Some(op) = operation {
            check_connector_operation_authority(principal, connector_inst, op, graph)?;
        }

        let policy = &graph.workspace.network;
        let method_upper = req.method.to_ascii_uppercase();
        if !policy.allowed_methods.is_empty()
            && !policy
                .allowed_methods
                .iter()
                .any(|m| m.eq_ignore_ascii_case(&method_upper))
        {
            return Err(HttpBrokerError::PolicyDenied(
                req.url,
                format!("HTTP method '{method_upper}' not allowed"),
            ));
        }

        // Resolve credential handle via Vault (never exposed to Luau!)
        let mut auth_header_val: Option<String> = None;
        if let Some(ref cred_slot) = req.credential {
            let secret_name = connector_inst
                .resolved_secret_name(cred_slot)
                .ok_or_else(|| {
                    HttpBrokerError::CredentialError(
                        cred_slot.clone(),
                        "connector instance has no secret binding for this slot".into(),
                    )
                })?;
            if !principal.ceiling.allows_secret(&secret_name) {
                return Err(HttpBrokerError::CredentialError(
                    secret_name,
                    "principal ceiling does not permit binding this secret".into(),
                ));
            }
            let (meta, plaintext) = self
                .store
                .resolve_secret_plaintext(&secret_name)
                .map_err(|e| HttpBrokerError::CredentialError(secret_name.clone(), e.to_string()))?;
            if !meta.allowed_bindings.is_empty()
                && !meta.allowed_bindings.iter().any(|b| b == &connector_inst.id || b == "*")
            {
                return Err(HttpBrokerError::CredentialError(
                    secret_name,
                    format!(
                        "secret access policy denies binding to connector '{}'",
                        connector_inst.id
                    ),
                ));
            }
            self.store.redactor().register_secret(&plaintext);
            auth_header_val = Some(format!("Bearer {plaintext}"));
        }

        let mut current_url = Url::parse(&req.url)
            .map_err(|e| HttpBrokerError::InvalidUrl(req.url.clone(), e.to_string()))?;
        let reqwest_method = reqwest::Method::from_bytes(method_upper.as_bytes())
            .map_err(|e| HttpBrokerError::InvalidUrl(req.url.clone(), e.to_string()))?;

        for _redirect_hop in 0..=MAX_REDIRECTS {
            validate_destination(&current_url, policy).await?;

            let mut builder = self.client.request(reqwest_method.clone(), current_url.clone());
            for (k, v) in &req.headers {
                // Do not allow Luau scripts to override Host or inject raw Authorization when using credential handles
                if k.eq_ignore_ascii_case("host") {
                    continue;
                }
                if auth_header_val.is_some() && k.eq_ignore_ascii_case("authorization") {
                    continue;
                }
                builder = builder.header(k.as_str(), v.as_str());
            }
            if let Some(ref auth) = auth_header_val {
                builder = builder.header("authorization", auth.as_str());
            }
            if let Some(ref body) = req.body {
                if !body.is_null() {
                    builder = builder.json(body);
                }
            }

            let resp = builder.send().await.map_err(|e| {
                let redacted = self.store.redactor().redact_str(&error_chain(&e));
                if request_provably_not_sent(&e) || is_safe_method(&method_upper) {
                    HttpBrokerError::Transport(redacted)
                } else {
                    HttpBrokerError::OutcomeUnknown(redacted)
                }
            })?;

            if resp.status().is_redirection() {
                if let Some(loc) = resp.headers().get("location").and_then(|v| v.to_str().ok()) {
                    current_url = current_url
                        .join(loc)
                        .map_err(|e| HttpBrokerError::InvalidUrl(loc.to_string(), e.to_string()))?;
                    continue;
                }
            }

            let status = resp.status().as_u16();
            let mut resp_headers = BTreeMap::new();
            for (k, v) in resp.headers() {
                if let Ok(vs) = v.to_str() {
                    let safe_v = self.store.redactor().redact_str(vs);
                    resp_headers.insert(k.to_string(), safe_v);
                }
            }

            // The upstream already answered (status line received): a body failure loses the outcome.
            let bytes = resp.bytes().await.map_err(|e| {
                let redacted = self.store.redactor().redact_str(&error_chain(&e));
                if is_safe_method(&method_upper) {
                    HttpBrokerError::Transport(redacted)
                } else {
                    HttpBrokerError::OutcomeUnknown(redacted)
                }
            })?;
            if bytes.len() > MAX_RESPONSE_BYTES {
                return Err(HttpBrokerError::ResponseTooLarge);
            }

            let parsed_body = match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(v) => self.store.redactor().redact_json(&v),
                Err(_) => {
                    let s = String::from_utf8_lossy(&bytes);
                    serde_json::Value::String(self.store.redactor().redact_str(&s))
                }
            };

            return Ok(HttpHostResponse {
                status,
                headers: resp_headers,
                body: parsed_body,
            });
        }

        Err(HttpBrokerError::TooManyRedirects)
    }
}

/// A request that failed while building it or while establishing the connection never reached the
/// upstream. Anything else (send/body/timeout after connect) may have been processed.
fn request_provably_not_sent(e: &reqwest::Error) -> bool {
    e.is_builder() || e.is_connect()
}

/// Safe methods have no upstream side effect, so a lost response never makes their outcome uncertain.
fn is_safe_method(method_upper: &str) -> bool {
    matches!(method_upper, "GET" | "HEAD" | "OPTIONS")
}

/// `Display` of a reqwest error omits its source chain; include it for diagnostics.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(src) = cur {
        out.push_str(": ");
        out.push_str(&src.to_string());
        cur = src.source();
    }
    out
}

async fn validate_destination(url: &Url, policy: &NetworkPolicy) -> Result<(), HttpBrokerError> {
    let scheme = url.scheme();
    if scheme != "https" && scheme != "http" {
        return Err(HttpBrokerError::PolicyDenied(
            url.to_string(),
            format!("unsupported scheme '{scheme}'"),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| HttpBrokerError::InvalidUrl(url.to_string(), "missing host".into()))?;

    if !policy.allowed_hosts.is_empty() {
        let host_ok = policy
            .allowed_hosts
            .iter()
            .any(|allowed| allowed == "*" || allowed.eq_ignore_ascii_case(host));
        if !host_ok {
            return Err(HttpBrokerError::PolicyDenied(
                url.to_string(),
                format!("host '{host}' is not in network allowlist"),
            ));
        }
    }

    let port = url.port_or_known_default().unwrap_or(443);
    // Direct IP check first
    if let Ok(ip) = host.parse::<IpAddr>() {
        check_ip_allowed(ip, policy)?;
        return Ok(());
    }

    let addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| HttpBrokerError::Transport(format!("DNS lookup failed for {host}: {e}")))?;
    let mut resolved_any = false;
    for addr in addrs {
        resolved_any = true;
        check_ip_allowed(addr.ip(), policy)?;
    }
    if !resolved_any {
        return Err(HttpBrokerError::Transport(format!(
            "DNS lookup returned no addresses for {host}"
        )));
    }
    Ok(())
}

fn check_ip_allowed(ip: IpAddr, policy: &NetworkPolicy) -> Result<(), HttpBrokerError> {
    if policy.allow_loopback_for_tests && ip.is_loopback() {
        return Ok(());
    }
    let blocked = match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_multicast()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0b1100_0000) == 64) // CGNAT 100.64.0.0/10
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || ((v6.segments()[0] & 0xfe00) == 0xfc00) // ULA fc00::/7
                || ((v6.segments()[0] & 0xffc0) == 0xfe80) // link-local fe80::/10
        }
    };
    if blocked {
        return Err(HttpBrokerError::SsrfBlocked(ip.to_string()));
    }
    Ok(())
}
