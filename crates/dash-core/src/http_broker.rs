//! Host-mediated outbound HTTP broker with allowlists, SSRF protection, redirect validation, and credential injection.
//!
//! Every request is checked against the *effective* network policy: the Workspace's requested
//! `[network]` section intersected with the Administrator network ceiling stored in SQLite
//! (outside the agent-editable repository, §11.3). Without an Administrator ceiling, and for empty
//! allowlists, every destination is denied (deny by default, §20).
//!
//! SSRF protections (§5.2): destinations are resolved once, every resolved address is vetted
//! (private, loopback, link-local, CGNAT, reserved, documentation, multicast, IPv4-mapped /
//! compatible / NAT64 / 6to4 / Teredo IPv6 forms), and the connection is pinned to exactly those
//! vetted addresses, so a DNS answer cannot change between check and connect (DNS rebinding).
//! Redirects are re-validated per hop; credentials are never forwarded to another origin and
//! `https` → `http` downgrades are refused. Responses are size-limited while streaming. Every
//! outbound request (and every denial) is audited as `http.request`; a request whose caller is
//! dropped mid-flight (VM deadline, cancellation, bounded wait) is still audited, as `aborted`.
//! Each hop is recorded as a `connector_http:<connector>` trace span (§15.1) with destination,
//! method, status, outcome and retry classification.
//!
//! Transport errors never carry the request URL (reqwest prints it, query credentials included):
//! messages name only the credential-free destination of the hop.

use crate::authority::{check_connector_operation_authority, AuthError, Principal};
use crate::model::{ConfiguredGraph, InstanceConfig, NetworkPolicy};
use crate::telemetry::Telemetry;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use dash_luau::{HttpHostRequest, HttpHostResponse};
use dash_store::SqliteStore;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;
use url::{Host, Url};

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

impl HttpBrokerError {
    /// Whether this error is an authority/policy denial (audited as `denied`).
    pub fn is_denial(&self) -> bool {
        matches!(
            self,
            Self::Auth(_)
                | Self::PolicyDenied(..)
                | Self::SsrfBlocked(_)
                | Self::CredentialError(..)
        )
    }
}

/// Load the Administrator network ceiling from SQLite (`None` = not granted yet).
pub fn load_network_ceiling(store: &SqliteStore) -> Option<NetworkPolicy> {
    store
        .get_network_policy_ceiling()
        .ok()
        .flatten()
        .and_then(|(v, _, _)| serde_json::from_value::<NetworkPolicy>(v).ok())
}

/// Requested Workspace policy ∩ Administrator ceiling.
#[derive(Debug, Clone)]
pub struct EffectiveNetworkPolicy {
    pub requested: NetworkPolicy,
    pub ceiling: Option<NetworkPolicy>,
}

impl EffectiveNetworkPolicy {
    pub fn new(requested: &NetworkPolicy, ceiling: Option<NetworkPolicy>) -> Self {
        Self {
            requested: requested.clone(),
            ceiling,
        }
    }

    pub fn for_graph(graph: &ConfiguredGraph, store: &SqliteStore) -> Self {
        Self::new(&graph.workspace.network, load_network_ceiling(store))
    }

    fn host_denial(&self, host: &str) -> Option<String> {
        let Some(ceiling) = self.ceiling.as_ref() else {
            return Some(
                "no Administrator network ceiling has been granted (pending grant; `dash network grant`)"
                    .into(),
            );
        };
        if !self.requested.allows_host(host) {
            return Some(format!(
                "host '{host}' is not in the Workspace network allowlist"
            ));
        }
        if !ceiling.allows_host(host) {
            return Some(format!(
                "host '{host}' exceeds the Administrator network ceiling (pending grant)"
            ));
        }
        None
    }

    pub fn allows_host(&self, host: &str) -> bool {
        self.host_denial(host).is_none()
    }

    fn method_denial(&self, method: &str) -> Option<String> {
        let Some(ceiling) = self.ceiling.as_ref() else {
            return Some(
                "no Administrator network ceiling has been granted (pending grant; `dash network grant`)"
                    .into(),
            );
        };
        if !self.requested.allows_method(method) {
            return Some(format!(
                "HTTP method '{method}' is not in the Workspace network policy"
            ));
        }
        if !ceiling.allows_method(method) {
            return Some(format!(
                "HTTP method '{method}' exceeds the Administrator network ceiling (pending grant)"
            ));
        }
        None
    }

    pub fn allows_method(&self, method: &str) -> bool {
        self.method_denial(method).is_none()
    }

    pub fn allow_loopback(&self) -> bool {
        self.requested.allow_loopback_for_tests
            && self
                .ceiling
                .as_ref()
                .is_some_and(|c| c.allow_loopback_for_tests)
    }
}

/// A resolved credential handle. The plaintext never leaves Rust.
struct Credential {
    secret_name: String,
    scheme: String,
    plaintext: String,
    allowed_hosts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Origin {
    scheme: String,
    host: String,
    port: u16,
}

fn origin_of(url: &Url) -> Origin {
    Origin {
        scheme: url.scheme().to_string(),
        host: url.host_str().unwrap_or("").to_ascii_lowercase(),
        port: url.port_or_known_default().unwrap_or(0),
    }
}

/// `scheme://host:port/path` — never the query string, which may carry credentials.
fn audit_destination(url: &Url) -> String {
    format!(
        "{}://{}:{}{}",
        url.scheme(),
        url.host_str().unwrap_or(""),
        url.port_or_known_default().unwrap_or(0),
        url.path()
    )
}

/// Trace context of one brokered request, shared by its hops.
struct HopTrace<'a> {
    trace_id: &'a str,
    connector_id: &'a str,
    principal_id: &'a str,
    /// Set right before a hop is handed to the transport.
    sent: &'a AtomicBool,
}

#[derive(Clone)]
pub struct HttpBroker {
    store: SqliteStore,
    /// Where per-hop `connector_http:*` spans go (§15.1); `None` records no spans.
    telemetry: Option<Telemetry>,
}

/// Writes the `http.request` audit (and trace span) of a request whose future is dropped before it
/// completed (VM deadline, cancellation, bounded wait): exactly the requests whose upstream outcome
/// is uncertain must not vanish from the audit log.
struct AbortedRequestAudit<'a> {
    broker: &'a HttpBroker,
    principal: &'a Principal,
    connector_id: &'a str,
    trace_id: &'a str,
    detail: serde_json::Value,
    sent: &'a AtomicBool,
    started: Instant,
    completed: bool,
}

impl Drop for AbortedRequestAudit<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let reached_send = self.sent.load(Ordering::SeqCst);
        let mut detail = self.detail.clone();
        if let Some(obj) = detail.as_object_mut() {
            obj.insert("reached_send".into(), serde_json::Value::Bool(reached_send));
            obj.insert(
                "error".into(),
                serde_json::Value::String(
                    "request dropped before completion (deadline, cancellation or caller gone); \
                     the upstream may have processed it"
                        .into(),
                ),
            );
        }
        let _ = self.broker.store.append_audit(
            &self.principal.id,
            self.principal.kind.as_str(),
            "http.request",
            self.connector_id,
            "aborted",
            detail.clone(),
        );
        if let Some(t) = &self.broker.telemetry {
            t.record_span(
                self.trace_id,
                None,
                &format!("connector_http:{}", self.connector_id),
                &self.principal.id,
                detail,
                "aborted",
                self.started.elapsed().as_millis() as i64,
            );
        }
    }
}

impl HttpBroker {
    pub fn new(store: SqliteStore) -> Result<Self, HttpBrokerError> {
        // Fail fast if the TLS/HTTP stack cannot be initialized.
        build_client(None)?;
        Ok(Self {
            store,
            telemetry: None,
        })
    }

    /// Record per-hop `connector_http:<connector>` trace spans into `telemetry` (§15.1).
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    pub async fn execute(
        &self,
        principal: &Principal,
        connector_inst: &InstanceConfig,
        operation: Option<&str>,
        graph: &ConfiguredGraph,
        req: HttpHostRequest,
    ) -> Result<HttpHostResponse, HttpBrokerError> {
        self.execute_traced(principal, connector_inst, operation, graph, req, None)
            .await
    }

    /// [`HttpBroker::execute`] inside trace `trace_id` (the calling query/action's trace), so the
    /// connector's outbound hops show up in that trace (§15.1). Without a trace id a fresh one is
    /// used; inside a browser/agent request scope the spans still link under the request root.
    pub async fn execute_traced(
        &self,
        principal: &Principal,
        connector_inst: &InstanceConfig,
        operation: Option<&str>,
        graph: &ConfiguredGraph,
        req: HttpHostRequest,
        trace_id: Option<&str>,
    ) -> Result<HttpHostResponse, HttpBrokerError> {
        let trace_id = trace_id
            .map(str::to_string)
            .unwrap_or_else(Telemetry::new_trace_id);
        let method = req.method.to_ascii_uppercase();
        let destination = Url::parse(&req.url)
            .map(|u| audit_destination(&u))
            .unwrap_or_else(|_| "<invalid url>".into());
        let credential_slot = req.credential.clone();
        let sent = AtomicBool::new(false);
        let mut abort_audit = AbortedRequestAudit {
            broker: self,
            principal,
            connector_id: &connector_inst.id,
            trace_id: &trace_id,
            detail: serde_json::json!({
                "operation": operation,
                "method": method,
                "destination": destination,
                "credential_slot": credential_slot,
            }),
            sent: &sent,
            started: Instant::now(),
            completed: false,
        };
        let hop_ctx = HopTrace {
            trace_id: &trace_id,
            connector_id: &connector_inst.id,
            principal_id: &principal.id,
            sent: &sent,
        };
        let result = self
            .execute_inner(principal, connector_inst, operation, graph, req, &hop_ctx)
            .await;
        abort_audit.completed = true;
        let (outcome, detail) = match &result {
            Ok((resp, final_dest, cred_attached)) => (
                "ok",
                serde_json::json!({
                    "operation": operation,
                    "method": method,
                    "destination": destination,
                    "final_destination": final_dest,
                    "status": resp.status,
                    "credential_slot": credential_slot,
                    "credential_attached": cred_attached,
                }),
            ),
            Err(e) => (
                if e.is_denial() { "denied" } else { "error" },
                serde_json::json!({
                    "operation": operation,
                    "method": method,
                    "destination": destination,
                    "credential_slot": credential_slot,
                    "error": self.store.redactor().redact_str(&e.to_string()),
                }),
            ),
        };
        let _ = self.store.append_audit(
            &principal.id,
            principal.kind.as_str(),
            "http.request",
            &connector_inst.id,
            outcome,
            detail,
        );
        result.map(|(resp, _, _)| resp)
    }

    async fn execute_inner(
        &self,
        principal: &Principal,
        connector_inst: &InstanceConfig,
        operation: Option<&str>,
        graph: &ConfiguredGraph,
        req: HttpHostRequest,
        hop_ctx: &HopTrace<'_>,
    ) -> Result<(HttpHostResponse, String, bool), HttpBrokerError> {
        if let Some(op) = operation {
            check_connector_operation_authority(principal, connector_inst, op, graph)?;
        }

        let policy = EffectiveNetworkPolicy::for_graph(graph, &self.store);
        let method_upper = req.method.to_ascii_uppercase();
        if let Some(reason) = policy.method_denial(&method_upper) {
            let dest = Url::parse(&req.url)
                .map(|u| audit_destination(&u))
                .unwrap_or_else(|_| "<invalid url>".into());
            return Err(HttpBrokerError::PolicyDenied(dest, reason));
        }

        // Resolve credential handle via Vault (never exposed to Luau!)
        let credential = match req.credential.as_deref() {
            Some(slot) => Some(self.resolve_credential(principal, connector_inst, slot)?),
            None => None,
        };

        let mut current_url = Url::parse(&req.url)
            .map_err(|e| HttpBrokerError::InvalidUrl(req.url.clone(), e.to_string()))?;
        let mut method = reqwest::Method::from_bytes(method_upper.as_bytes())
            .map_err(|e| HttpBrokerError::InvalidUrl(req.url.clone(), e.to_string()))?;
        let mut body = req.body.clone().filter(|b| !b.is_null());
        let first_origin = origin_of(&current_url);
        let mut credential_active = credential.is_some();

        for redirect_hop in 0..=MAX_REDIRECTS {
            let pinned = vet_destination(&current_url, &policy).await?;
            let host = host_for_policy(&current_url)?;

            let mut request_url = current_url.clone();
            let mut cred_header: Option<(String, String)> = None;
            if let (Some(cred), true) = (credential.as_ref(), credential_active) {
                if !cred.allowed_hosts.is_empty()
                    && !NetworkPolicy::pattern_list_matches(&cred.allowed_hosts, &host)
                {
                    return Err(HttpBrokerError::CredentialError(
                        cred.secret_name.clone(),
                        format!(
                            "secret access policy does not allow attaching it to host '{host}'"
                        ),
                    ));
                }
                cred_header = apply_credential(cred, &mut request_url);
            }

            let client = match &pinned {
                Pinned::Domain(domain, addrs) => build_client(Some((domain.as_str(), addrs)))?,
                Pinned::IpLiteral => build_client(None)?,
            };
            // Kept only to scrub it (query credentials included) out of transport error text.
            let request_url_for_errors = request_url.to_string();
            let mut builder = client.request(method.clone(), request_url);
            for (k, v) in &req.headers {
                if is_reserved_request_header(
                    k,
                    cred_header.as_ref(),
                    credential.is_some(),
                    body.is_some(),
                ) {
                    continue;
                }
                builder = builder.header(k.as_str(), v.as_str());
            }
            if let Some((name, value)) = cred_header.as_ref() {
                builder = builder.header(name.as_str(), value.as_str());
            }
            if let Some(ref b) = body {
                builder = builder.json(b);
            }

            // After a 303 redirect the current method may differ from the original one.
            hop_ctx.sent.store(true, Ordering::SeqCst);
            let hop_started = Instant::now();
            let hop_dest = audit_destination(&current_url);
            let mut resp = match builder.send().await {
                Ok(r) => r,
                Err(e) => {
                    let not_sent = request_provably_not_sent(&e);
                    let msg = self.transport_error_message(e, &request_url_for_errors, &hop_dest);
                    let err = if not_sent || is_safe_method(method.as_str()) {
                        HttpBrokerError::Transport(msg)
                    } else {
                        HttpBrokerError::OutcomeUnknown(msg)
                    };
                    self.record_hop(
                        hop_ctx,
                        redirect_hop,
                        method.as_str(),
                        &hop_dest,
                        Err(&err),
                        hop_started,
                    );
                    return Err(err);
                }
            };
            self.record_hop(
                hop_ctx,
                redirect_hop,
                method.as_str(),
                &hop_dest,
                Ok(resp.status().as_u16()),
                hop_started,
            );

            if resp.status().is_redirection() {
                if let Some(loc) = resp.headers().get("location").and_then(|v| v.to_str().ok()) {
                    let next = current_url
                        .join(loc)
                        .map_err(|e| HttpBrokerError::InvalidUrl(loc.to_string(), e.to_string()))?;
                    if current_url.scheme() == "https" && next.scheme() != "https" {
                        return Err(HttpBrokerError::PolicyDenied(
                            audit_destination(&next),
                            "redirect downgrades https to an insecure scheme".into(),
                        ));
                    }
                    // Credentials (and Luau-supplied credential headers) never follow a redirect
                    // to another origin (§5.2 "narrowly authorized outbound requests").
                    if origin_of(&next) != first_origin {
                        credential_active = false;
                    }
                    let status = resp.status().as_u16();
                    let not_get_or_head =
                        method != reqwest::Method::GET && method != reqwest::Method::HEAD;
                    if (status == 303 && method != reqwest::Method::HEAD)
                        || ((status == 301 || status == 302) && not_get_or_head)
                    {
                        method = reqwest::Method::GET;
                        body = None;
                    }
                    current_url = next;
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

            if resp
                .content_length()
                .is_some_and(|len| len > MAX_RESPONSE_BYTES as u64)
            {
                return Err(HttpBrokerError::ResponseTooLarge);
            }
            // The upstream already answered (status line received): a body failure loses the
            // outcome of a non-safe request.
            let mut bytes: Vec<u8> = Vec::new();
            while let Some(chunk) = resp.chunk().await.map_err(|e| {
                let msg = self.transport_error_message(e, &request_url_for_errors, &hop_dest);
                if is_safe_method(method.as_str()) {
                    HttpBrokerError::Transport(msg)
                } else {
                    HttpBrokerError::OutcomeUnknown(msg)
                }
            })? {
                if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                    return Err(HttpBrokerError::ResponseTooLarge);
                }
                bytes.extend_from_slice(&chunk);
            }

            let parsed_body = match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(v) => self.store.redactor().redact_json(&v),
                Err(_) => {
                    let s = String::from_utf8_lossy(&bytes);
                    serde_json::Value::String(self.store.redactor().redact_str(&s))
                }
            };

            return Ok((
                HttpHostResponse {
                    status,
                    headers: resp_headers,
                    body: parsed_body,
                },
                audit_destination(&current_url),
                credential_active && credential.is_some(),
            ));
        }

        Err(HttpBrokerError::TooManyRedirects)
    }

    /// Error text of a failed hop: reqwest's message *without* the request URL (whose query may
    /// carry a `query:` credential in an encoded form that plaintext redaction could miss),
    /// labelled with the credential-free [`audit_destination`], then redacted.
    fn transport_error_message(
        &self,
        e: reqwest::Error,
        request_url: &str,
        destination: &str,
    ) -> String {
        let e = e.without_url();
        let mut msg = format!("{} [{destination}]", error_chain(&e));
        // Defence in depth: a source error that still embeds the URL or its query loses it here.
        if !request_url.is_empty() && msg.contains(request_url) {
            msg = msg.replace(request_url, destination);
        }
        if let Some((_, query)) = request_url.split_once('?') {
            if !query.is_empty() && msg.contains(query) {
                msg = msg.replace(query, "[REDACTED_QUERY]");
            }
        }
        self.store.redactor().redact_str(&msg)
    }

    /// Record one hop as a `connector_http:<connector>` span: the status line received
    /// (`Ok(status)`) or the transport failure, with a retry classification for diagnostics.
    fn record_hop(
        &self,
        hop_ctx: &HopTrace<'_>,
        hop: usize,
        method: &str,
        destination: &str,
        result: Result<u16, &HttpBrokerError>,
        started: Instant,
    ) {
        let Some(t) = &self.telemetry else {
            return;
        };
        let safe = is_safe_method(method);
        let (status, outcome, retry_class, error) = match result {
            Ok(status) => {
                let (outcome, retry_class) = match status {
                    300..=399 => ("redirect", "none"),
                    429 => ("rate_limited", "retry_after_backoff"),
                    500..=599 if safe => ("server_error", "retryable"),
                    500..=599 => ("server_error", "do_not_retry"),
                    400..=499 => ("client_error", "do_not_retry"),
                    _ => ("ok", "none"),
                };
                (Some(status), outcome, retry_class, None)
            }
            Err(e @ HttpBrokerError::OutcomeUnknown(_)) => {
                (None, "unknown_outcome", "do_not_retry", Some(e.to_string()))
            }
            Err(e) => (None, "transport_error", "retryable", Some(e.to_string())),
        };
        let ok = status.is_some_and(|s| s < 400);
        t.record_span(
            hop_ctx.trace_id,
            None,
            &format!("connector_http:{}", hop_ctx.connector_id),
            hop_ctx.principal_id,
            serde_json::json!({
                "hop": hop,
                "method": method,
                "destination": destination,
                "status": status,
                "outcome": outcome,
                "retry_class": retry_class,
                "error": error,
            }),
            if ok { "ok" } else { "error" },
            started.elapsed().as_millis() as i64,
        );
    }

    fn resolve_credential(
        &self,
        principal: &Principal,
        connector_inst: &InstanceConfig,
        cred_slot: &str,
    ) -> Result<Credential, HttpBrokerError> {
        let secret_name = connector_inst
            .resolved_secret_name(cred_slot)
            .ok_or_else(|| {
                HttpBrokerError::CredentialError(
                    cred_slot.to_string(),
                    "connector instance has no secret binding for this slot".into(),
                )
            })?;
        // Runtime intersection with the invoking Principal (§5.1.4): Service principals and Agents
        // need the secret in their ceiling; binding-time checks happen in drafts/activation.
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
            && !meta
                .allowed_bindings
                .iter()
                .any(|b| b == &connector_inst.id || b == "*")
        {
            return Err(HttpBrokerError::CredentialError(
                secret_name,
                format!(
                    "secret access policy denies binding to connector '{}'",
                    connector_inst.id
                ),
            ));
        }
        let redactor = self.store.redactor();
        redactor.register_secret(&plaintext);
        if meta.credential_scheme == "basic" {
            redactor.register_secret(&B64.encode(plaintext.as_bytes()));
        }
        if meta.credential_scheme.starts_with("query:") {
            // `apply_credential` writes the value form-urlencoded; that wire form shows up in
            // transport errors (reqwest prints the URL) and in upstream echoes of the request
            // target, so it must be redacted like the plaintext.
            let encoded: String =
                url::form_urlencoded::byte_serialize(plaintext.as_bytes()).collect();
            if encoded != plaintext {
                redactor.register_secret(&encoded);
                // Upstreams echoing the request target may re-encode a space as `%20`.
                if encoded.contains('+') && plaintext.contains(' ') {
                    redactor.register_secret(&encoded.replace('+', "%20"));
                }
            }
        }
        Ok(Credential {
            secret_name,
            scheme: meta.credential_scheme,
            plaintext,
            allowed_hosts: meta.allowed_hosts,
        })
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

/// Attach a credential per its Administrator-defined scheme. Returns the header to set (if any);
/// query credentials are written into `url` (never into `current_url`, so they are not audited).
fn apply_credential(cred: &Credential, url: &mut Url) -> Option<(String, String)> {
    let scheme = cred.scheme.as_str();
    if scheme == "basic" {
        return Some((
            "authorization".into(),
            format!("Basic {}", B64.encode(cred.plaintext.as_bytes())),
        ));
    }
    if let Some(name) = scheme.strip_prefix("header:") {
        return Some((name.to_ascii_lowercase(), cred.plaintext.clone()));
    }
    if let Some(param) = scheme.strip_prefix("query:") {
        let kept: Vec<(String, String)> = url
            .query_pairs()
            .filter(|(k, _)| k != param)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        {
            let mut q = url.query_pairs_mut();
            q.clear();
            for (k, v) in &kept {
                q.append_pair(k, v);
            }
            q.append_pair(param, &cred.plaintext);
        }
        return None;
    }
    Some(("authorization".into(), format!("Bearer {}", cred.plaintext)))
}

/// Luau-supplied headers the broker never forwards: `Host`, credential headers when a credential
/// handle is in use (attached or dropped on a cross-origin redirect), and body headers once a
/// redirect turned the request into a body-less GET.
fn is_reserved_request_header(
    name: &str,
    cred_header: Option<&(String, String)>,
    credential_requested: bool,
    has_body: bool,
) -> bool {
    let lower = name.to_ascii_lowercase();
    if lower == "host" || lower == "content-length" {
        return true;
    }
    if credential_requested
        && (lower == "authorization" || lower == "cookie" || lower == "proxy-authorization")
    {
        return true;
    }
    if cred_header.is_some_and(|(n, _)| n.eq_ignore_ascii_case(&lower)) {
        return true;
    }
    !has_body && lower == "content-type"
}

fn build_client(pin: Option<(&str, &[SocketAddr])>) -> Result<reqwest::Client, HttpBrokerError> {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent("dash-engine/0.2")
        // A proxy would resolve the destination itself and defeat address pinning.
        .no_proxy();
    if let Some((domain, addrs)) = pin {
        builder = builder.resolve_to_addrs(domain, addrs);
    }
    builder
        .build()
        .map_err(|e| HttpBrokerError::Transport(e.to_string()))
}

enum Pinned {
    /// Domain name pinned to its vetted addresses.
    Domain(String, Vec<SocketAddr>),
    IpLiteral,
}

fn host_for_policy(url: &Url) -> Result<String, HttpBrokerError> {
    match url.host() {
        Some(Host::Domain(d)) => Ok(d.to_ascii_lowercase()),
        Some(Host::Ipv4(ip)) => Ok(ip.to_string()),
        Some(Host::Ipv6(ip)) => Ok(ip.to_string()),
        None => Err(HttpBrokerError::InvalidUrl(
            url.to_string(),
            "missing host".into(),
        )),
    }
}

/// Validate scheme, host allowlist and every resolved address; return what to pin the connection to.
async fn vet_destination(
    url: &Url,
    policy: &EffectiveNetworkPolicy,
) -> Result<Pinned, HttpBrokerError> {
    let scheme = url.scheme();
    if scheme != "https" && scheme != "http" {
        return Err(HttpBrokerError::PolicyDenied(
            audit_destination(url),
            format!("unsupported scheme '{scheme}'"),
        ));
    }
    let host = host_for_policy(url)?;
    if let Some(reason) = policy.host_denial(&host) {
        return Err(HttpBrokerError::PolicyDenied(
            audit_destination(url),
            reason,
        ));
    }
    let allow_loopback = policy.allow_loopback();
    let port = url.port_or_known_default().unwrap_or(443);

    match url.host() {
        Some(Host::Ipv4(ip)) => {
            check_ip_allowed(IpAddr::V4(ip), allow_loopback)?;
            Ok(Pinned::IpLiteral)
        }
        Some(Host::Ipv6(ip)) => {
            check_ip_allowed(IpAddr::V6(ip), allow_loopback)?;
            Ok(Pinned::IpLiteral)
        }
        _ => {
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
                .await
                .map_err(|e| {
                    HttpBrokerError::Transport(format!("DNS lookup failed for {host}: {e}"))
                })?
                .collect();
            if addrs.is_empty() {
                return Err(HttpBrokerError::Transport(format!(
                    "DNS lookup returned no addresses for {host}"
                )));
            }
            for addr in &addrs {
                check_ip_allowed(addr.ip(), allow_loopback)?;
            }
            Ok(Pinned::Domain(host, addrs))
        }
    }
}

fn ipv4_blocked(v4: Ipv4Addr) -> bool {
    let o = v4.octets();
    v4.is_loopback()                                   // 127.0.0.0/8
        || v4.is_private()                             // 10/8, 172.16/12, 192.168/16
        || v4.is_link_local()                          // 169.254/16 (cloud metadata)
        || v4.is_multicast()                           // 224/4
        || v4.is_broadcast()
        || o[0] == 0                                   // 0.0.0.0/8 "this network"
        || (o[0] == 100 && (o[1] & 0b1100_0000) == 64) // 100.64/10 CGNAT
        || (o[0] == 192 && o[1] == 0 && o[2] == 0)     // 192.0.0.0/24 IETF protocol assignments
        || (o[0] == 192 && o[1] == 0 && o[2] == 2)     // 192.0.2.0/24 TEST-NET-1
        || (o[0] == 198 && (o[1] & 0xfe) == 18)        // 198.18/15 benchmarking
        || (o[0] == 198 && o[1] == 51 && o[2] == 100)  // 198.51.100.0/24 TEST-NET-2
        || (o[0] == 203 && o[1] == 0 && o[2] == 113)   // 203.0.113.0/24 TEST-NET-3
        || o[0] >= 240 // 240/4 reserved (+ broadcast)
}

/// IPv4 address embedded in an IPv6 form that the OS or a translator would route to IPv4.
fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    let s = v6.segments();
    if let Some(v4) = v6.to_ipv4_mapped() {
        return Some(v4); // ::ffff:a.b.c.d
    }
    if s[0..6] == [0, 0, 0, 0, 0, 0] && !(s[6] == 0 && s[7] <= 1) {
        // ::a.b.c.d (deprecated IPv4-compatible); excludes :: and ::1
        return Some(Ipv4Addr::new(
            (s[6] >> 8) as u8,
            s[6] as u8,
            (s[7] >> 8) as u8,
            s[7] as u8,
        ));
    }
    if s[0] == 0x2002 {
        // 6to4 2002:a.b.c.d::/48
        return Some(Ipv4Addr::new(
            (s[1] >> 8) as u8,
            s[1] as u8,
            (s[2] >> 8) as u8,
            s[2] as u8,
        ));
    }
    None
}

fn ipv6_blocked(v6: Ipv6Addr) -> bool {
    let s = v6.segments();
    v6.is_loopback()                                    // ::1
        || v6.is_unspecified()                          // ::
        || v6.is_multicast()                            // ff00::/8
        || (s[0] & 0xfe00) == 0xfc00                    // fc00::/7 ULA
        || (s[0] & 0xffc0) == 0xfe80                    // fe80::/10 link-local
        || (s[0] & 0xffc0) == 0xfec0                    // fec0::/10 site-local (deprecated)
        || (s[0] == 0x64 && s[1] == 0xff9b)             // 64:ff9b::/96 and 64:ff9b:1::/48 NAT64
        || (s[0] == 0x100 && s[1] == 0 && s[2] == 0 && s[3] == 0) // 100::/64 discard
        || (s[0] == 0x2001 && s[1] == 0)                // 2001::/32 Teredo (embeds obfuscated IPv4)
        || (s[0] == 0x2001 && s[1] == 0xdb8) // 2001:db8::/32 documentation
}

fn check_ip_allowed(ip: IpAddr, allow_loopback: bool) -> Result<(), HttpBrokerError> {
    // Normalize IPv6 forms that reach IPv4 destinations before classifying.
    let effective = match ip {
        IpAddr::V6(v6) => embedded_ipv4(v6).map(IpAddr::V4).unwrap_or(ip),
        v4 => v4,
    };
    if allow_loopback && effective.is_loopback() {
        return Ok(());
    }
    let blocked = match effective {
        IpAddr::V4(v4) => ipv4_blocked(v4),
        IpAddr::V6(v6) => ipv6_blocked(v6),
    };
    if blocked {
        return Err(HttpBrokerError::SsrfBlocked(ip.to_string()));
    }
    Ok(())
}

/// Vet a non-HTTP outbound destination steered by a non-Administrator (for example the Git remote
/// of a `dependency_update`, §14.2) with the broker's rules: the host must be allowed by the
/// Administrator network `ceiling` (deny without one), and every address it resolves to must pass
/// the SSRF IP-class checks (loopback only when the ceiling opts in for tests). Returns the
/// vetted addresses.
pub async fn vet_outbound_host(
    host: &str,
    port: u16,
    ceiling: Option<&NetworkPolicy>,
) -> Result<Vec<SocketAddr>, HttpBrokerError> {
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    let dest = format!("{host}:{port}");
    let Some(ceiling) = ceiling else {
        return Err(HttpBrokerError::PolicyDenied(
            dest,
            "no Administrator network ceiling has been granted (pending grant; `dash network grant`)"
                .into(),
        ));
    };
    if host.is_empty() {
        return Err(HttpBrokerError::InvalidUrl(dest, "missing host".into()));
    }
    if !ceiling.allows_host(&host) {
        return Err(HttpBrokerError::PolicyDenied(
            dest,
            format!("host '{host}' exceeds the Administrator network ceiling (pending grant)"),
        ));
    }
    let allow_loopback = ceiling.allow_loopback_for_tests;
    if let Ok(ip) = host.parse::<IpAddr>() {
        check_ip_allowed(ip, allow_loopback)?;
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|e| HttpBrokerError::Transport(format!("DNS lookup failed for {host}: {e}")))?
        .collect();
    if addrs.is_empty() {
        return Err(HttpBrokerError::Transport(format!(
            "DNS lookup returned no addresses for {host}"
        )));
    }
    for addr in &addrs {
        check_ip_allowed(addr.ip(), allow_loopback)?;
    }
    Ok(addrs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::PrincipalKind;
    use crate::model::CapabilityCeiling;
    use axum::{
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::{get, post},
        Json, Router,
    };
    use dash_luau::DefinitionCategory;
    use serde_json::json;

    fn blocked(s: &str) -> bool {
        check_ip_allowed(s.parse().unwrap(), false).is_err()
    }

    #[test]
    fn ssrf_blocks_every_private_and_reserved_class() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "0.0.0.0",
            "0.1.2.3",
            "100.64.0.1",
            "192.0.0.8",
            "192.0.2.1",
            "198.18.0.1",
            "198.19.255.1",
            "198.51.100.7",
            "203.0.113.9",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fc00::1",
            "fd12::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            // IPv4-mapped / compatible / NAT64 / 6to4 / Teredo forms of internal targets
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            "::ffff:10.0.0.1",
            "::127.0.0.1",
            "64:ff9b::a9fe:a9fe",
            "64:ff9b:1::1",
            "2002:7f00:1::1",
            "2002:0a00:0001::1",
            "2001:0:4136:e378::1",
            "2001:db8::1",
            "100::1",
        ] {
            assert!(blocked(ip), "{ip} must be blocked");
        }
        for ip in [
            "8.8.8.8",
            "140.82.112.3",
            "2606:4700::1111",
            "::ffff:8.8.8.8",
            "2002:0808:0808::1",
        ] {
            assert!(!blocked(ip), "{ip} must be allowed");
        }
        // Loopback opt-in also covers the mapped form, nothing else.
        assert!(check_ip_allowed("::ffff:127.0.0.1".parse().unwrap(), true).is_ok());
        assert!(check_ip_allowed("10.0.0.1".parse().unwrap(), true).is_err());
    }

    #[test]
    fn effective_policy_is_deny_by_default_and_intersects_admin_ceiling() {
        let requested = NetworkPolicy {
            allowed_hosts: vec!["api.github.com".into(), "evil.example.net".into()],
            allowed_methods: vec!["GET".into(), "POST".into()],
            allow_loopback_for_tests: true,
        };
        // No Administrator ceiling: everything denied.
        let none = EffectiveNetworkPolicy::new(&requested, None);
        assert!(!none.allows_host("api.github.com"));
        assert!(!none.allows_method("GET"));
        assert!(!none.allow_loopback());

        let ceiling = NetworkPolicy {
            allowed_hosts: vec!["*.github.com".into()],
            allowed_methods: vec!["GET".into()],
            allow_loopback_for_tests: false,
        };
        let eff = EffectiveNetworkPolicy::new(&requested, Some(ceiling));
        assert!(eff.allows_host("api.github.com"));
        assert!(
            !eff.allows_host("evil.example.net"),
            "Workspace cannot exceed the ceiling"
        );
        assert!(
            !eff.allows_host("github.com"),
            "*.suffix matches subdomains only"
        );
        assert!(eff.allows_method("get"));
        assert!(!eff.allows_method("POST"));
        assert!(
            !eff.allow_loopback(),
            "loopback needs both the request and the ceiling"
        );

        // Empty requested lists deny everything even under a wildcard ceiling.
        let wide = NetworkPolicy {
            allowed_hosts: vec!["*".into()],
            allowed_methods: vec!["*".into()],
            allow_loopback_for_tests: true,
        };
        let empty = EffectiveNetworkPolicy::new(&NetworkPolicy::default(), Some(wide));
        assert!(!empty.allows_host("api.github.com"));
        assert!(!empty.allows_method("GET"));
        assert!(!NetworkPolicy::pattern_list_matches(
            &["*.github.com".into()],
            "evilgithub.com"
        ));
    }

    fn conn_inst(secret: Option<&str>) -> InstanceConfig {
        InstanceConfig {
            id: "gh_conn".into(),
            category: DefinitionCategory::Connector,
            definition: "github.connector".into(),
            inputs: BTreeMap::new(),
            config: json!({}),
            secrets: secret
                .map(|s| BTreeMap::from([("auth".to_string(), format!("secret://{s}"))]))
                .unwrap_or_default(),
            capabilities: vec![],
            run_as: None,
            enabled: None,
        }
    }

    fn loopback_graph() -> ConfiguredGraph {
        let mut g = ConfiguredGraph::default();
        g.workspace.network = NetworkPolicy {
            allowed_hosts: vec!["127.0.0.1".into(), "localhost".into()],
            allowed_methods: vec!["GET".into(), "POST".into()],
            allow_loopback_for_tests: true,
        };
        g
    }

    fn grant_loopback_ceiling(store: &SqliteStore) {
        store
            .set_network_policy_ceiling(
                json!({
                    "allowed_hosts": ["127.0.0.1", "localhost"],
                    "allowed_methods": ["*"],
                    "allow_loopback_for_tests": true
                }),
                "admin",
            )
            .unwrap();
    }

    fn get_req(url: String, credential: Option<&str>) -> HttpHostRequest {
        HttpHostRequest {
            method: "GET".into(),
            url,
            headers: BTreeMap::new(),
            body: None,
            credential: credential.map(str::to_string),
        }
    }

    async fn serve(app: Router) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    fn echo_auth(headers: &HeaderMap, name: &str) -> String {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    }

    #[tokio::test]
    async fn requests_are_denied_without_admin_ceiling_and_audited() {
        let addr = serve(Router::new().route("/x", get(|| async { "ok" }))).await;
        let store = SqliteStore::open_in_memory().unwrap();
        let broker = HttpBroker::new(store.clone()).unwrap();
        let admin = Principal::administrator();
        let err = broker
            .execute(
                &admin,
                &conn_inst(None),
                None,
                &loopback_graph(),
                get_req(format!("http://{addr}/x"), None),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, HttpBrokerError::PolicyDenied(..)), "{err}");
        assert!(err.to_string().contains("Administrator network ceiling"));

        grant_loopback_ceiling(&store);
        let ok = broker
            .execute(
                &admin,
                &conn_inst(None),
                None,
                &loopback_graph(),
                get_req(format!("http://{addr}/x?q=1"), None),
            )
            .await
            .unwrap();
        assert_eq!(ok.status, 200);
        let audits = store.list_audits(10).unwrap();
        let ops: Vec<_> = audits
            .iter()
            .filter(|a| a.operation == "http.request")
            .map(|a| a.outcome.as_str())
            .collect();
        assert_eq!(ops.len(), 2);
        assert!(ops.contains(&"denied") && ops.contains(&"ok"));
        let ok_rec = audits.iter().find(|a| a.outcome == "ok").unwrap();
        assert_eq!(ok_rec.target, "gh_conn");
        assert!(!ok_rec.detail["destination"]
            .as_str()
            .unwrap()
            .contains("q=1"));
    }

    #[tokio::test]
    async fn hostnames_are_resolved_once_vetted_and_pinned() {
        let requested = loopback_graph().workspace.network;
        let wide = NetworkPolicy {
            allowed_hosts: vec!["*".into()],
            allowed_methods: vec!["*".into()],
            allow_loopback_for_tests: true,
        };
        let url = Url::parse("http://localhost:9/x").unwrap();
        let pol = EffectiveNetworkPolicy::new(&requested, Some(wide.clone()));
        match vet_destination(&url, &pol).await.unwrap() {
            Pinned::Domain(host, addrs) => {
                assert_eq!(host, "localhost");
                assert!(!addrs.is_empty());
                assert!(addrs.iter().all(|a| a.ip().is_loopback()));
            }
            Pinned::IpLiteral => panic!("hostname must be pinned"),
        }
        // Without the loopback opt-in, a name resolving to loopback is blocked.
        let mut strict = wide;
        strict.allow_loopback_for_tests = false;
        let pol = EffectiveNetworkPolicy::new(&requested, Some(strict));
        assert!(matches!(
            vet_destination(&url, &pol).await,
            Err(HttpBrokerError::SsrfBlocked(_))
        ));
        let mapped = Url::parse("http://[::ffff:169.254.169.254]/latest/meta-data").unwrap();
        let mut req2 = requested.clone();
        req2.allowed_hosts = vec!["*".into()];
        let pol = EffectiveNetworkPolicy::new(
            &req2,
            Some(NetworkPolicy {
                allowed_hosts: vec!["*".into()],
                allowed_methods: vec!["*".into()],
                allow_loopback_for_tests: true,
            }),
        );
        assert!(matches!(
            vet_destination(&mapped, &pol).await,
            Err(HttpBrokerError::SsrfBlocked(_))
        ));
        assert!(matches!(
            vet_destination(&Url::parse("file:///etc/passwd").unwrap(), &pol).await,
            Err(HttpBrokerError::PolicyDenied(..)) | Err(HttpBrokerError::InvalidUrl(..))
        ));
    }

    #[tokio::test]
    async fn redirect_logic_on_single_host() {
        // Deterministic variant on one loopback origin pair (127.0.0.1 only).
        let target = serve(
            Router::new().route(
                "/land",
                get(|headers: HeaderMap| async move {
                    Json(json!({ "auth": echo_auth(&headers, "authorization"), "method": "GET" }))
                })
                .post(|| async { Json(json!({ "method": "POST" })) }),
            ),
        )
        .await;
        let t1 = format!("http://127.0.0.1:{}/land", target.port());
        let t2 = t1.clone();
        let origin = serve(
            Router::new()
                .route(
                    "/go",
                    get(move || {
                        let t = t1.clone();
                        async move { (StatusCode::FOUND, [("location", t)]).into_response() }
                    }),
                )
                .route(
                    "/submit",
                    post(move || {
                        let t = t2.clone();
                        async move { (StatusCode::SEE_OTHER, [("location", t)]).into_response() }
                    }),
                )
                .route(
                    "/same",
                    get(|| async {
                        (StatusCode::TEMPORARY_REDIRECT, [("location", "/local")]).into_response()
                    }),
                )
                .route(
                    "/local",
                    get(|headers: HeaderMap| async move {
                        Json(json!({ "auth": echo_auth(&headers, "authorization") }))
                    }),
                ),
        )
        .await;
        let store = SqliteStore::open_in_memory().unwrap();
        grant_loopback_ceiling(&store);
        store
            .set_secret("gh", "", "ghp_redirect_secret_2", &[])
            .unwrap();
        let broker = HttpBroker::new(store.clone()).unwrap();
        let admin = Principal::administrator();
        let g = loopback_graph();

        // Different port = different origin.
        let mut req = get_req(
            format!("http://127.0.0.1:{}/go", origin.port()),
            Some("auth"),
        );
        req.headers
            .insert("Authorization".into(), "Bearer luau-supplied".into());
        let resp = broker
            .execute(&admin, &conn_inst(Some("gh")), None, &g, req)
            .await
            .unwrap();
        assert_eq!(
            resp.body["auth"], "",
            "credential must not follow a cross-origin redirect"
        );
        let audits = store.list_audits(5).unwrap();
        let rec = audits
            .iter()
            .find(|a| a.operation == "http.request")
            .unwrap();
        assert_eq!(rec.detail["credential_attached"], false);

        // Same-origin redirect keeps the credential (echo is redacted before reaching Luau).
        let resp = broker
            .execute(
                &admin,
                &conn_inst(Some("gh")),
                None,
                &g,
                get_req(
                    format!("http://127.0.0.1:{}/same", origin.port()),
                    Some("auth"),
                ),
            )
            .await
            .unwrap();
        assert_eq!(resp.body["auth"], "Bearer [REDACTED]");

        let post_req = HttpHostRequest {
            method: "POST".into(),
            url: format!("http://127.0.0.1:{}/submit", origin.port()),
            headers: BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: Some(json!({ "merge": true })),
            credential: None,
        };
        let resp = broker
            .execute(&admin, &conn_inst(None), None, &g, post_req)
            .await
            .unwrap();
        assert_eq!(
            resp.body["method"], "GET",
            "303 must switch to GET and drop the body"
        );
    }

    #[tokio::test]
    async fn credential_schemes_host_policy_and_principal_ceiling() {
        let addr = serve(Router::new().route(
            "/echo",
            get(|headers: HeaderMap, uri: axum::http::Uri| async move {
                Json(json!({
                    "auth_seen": echo_auth(&headers, "authorization"),
                    "x_api_key": echo_auth(&headers, "x-api-key"),
                    "query": uri.query().unwrap_or(""),
                }))
            }),
        ))
        .await;
        let store = SqliteStore::open_in_memory().unwrap();
        grant_loopback_ceiling(&store);
        let broker = HttpBroker::new(store.clone()).unwrap();
        let admin = Principal::administrator();
        let g = loopback_graph();
        let url = format!("http://127.0.0.1:{}/echo", addr.port());

        store
            .set_secret_with_policy(
                "basic",
                "",
                "me@example.com:tok_basic_1",
                &[],
                None,
                Some("basic"),
            )
            .unwrap();
        let r = broker
            .execute(
                &admin,
                &conn_inst(Some("basic")),
                None,
                &g,
                get_req(url.clone(), Some("auth")),
            )
            .await
            .unwrap();
        assert_eq!(r.body["auth_seen"], "Basic [REDACTED]");

        store
            .set_secret_with_policy(
                "hdr",
                "",
                "key_header_123",
                &[],
                None,
                Some("header:X-API-Key"),
            )
            .unwrap();
        let r = broker
            .execute(
                &admin,
                &conn_inst(Some("hdr")),
                None,
                &g,
                get_req(url.clone(), Some("auth")),
            )
            .await
            .unwrap();
        assert_eq!(r.body["x_api_key"], "[REDACTED]");
        assert_eq!(r.body["auth_seen"], "");

        store
            .set_secret_with_policy("qry", "", "key_query_456", &[], None, Some("query:api_key"))
            .unwrap();
        let r = broker
            .execute(
                &admin,
                &conn_inst(Some("qry")),
                None,
                &g,
                get_req(format!("{url}?a=1"), Some("auth")),
            )
            .await
            .unwrap();
        assert_eq!(r.body["query"], "a=1&api_key=[REDACTED]");
        let audits = store.list_audits(1).unwrap();
        assert!(!serde_json::to_string(&audits)
            .unwrap()
            .contains("key_query_456"));

        // Secret destination policy: credential may only be attached to listed hosts.
        store
            .set_secret_with_policy(
                "pinned",
                "",
                "ghp_pinned_789",
                &[],
                Some(&["api.github.com".to_string()]),
                None,
            )
            .unwrap();
        let err = broker
            .execute(
                &admin,
                &conn_inst(Some("pinned")),
                None,
                &g,
                get_req(url.clone(), Some("auth")),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, HttpBrokerError::CredentialError(..)), "{err}");

        // A Service principal without the secret in its ceiling is denied at runtime.
        let sp = Principal {
            id: "sp".into(),
            kind: PrincipalKind::ServicePrincipal,
            ceiling: CapabilityCeiling::default(),
        };
        let err = broker
            .execute(
                &sp,
                &conn_inst(Some("hdr")),
                None,
                &g,
                get_req(url, Some("auth")),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, HttpBrokerError::CredentialError(..)));
    }

    #[tokio::test]
    async fn oversized_responses_are_rejected_while_streaming() {
        let big = || async { "x".repeat(MAX_RESPONSE_BYTES + 10) };
        let streamed = || async {
            let chunks = (0..20)
                .map(|_| Ok::<_, std::io::Error>(axum::body::Bytes::from(vec![b'y'; 512 * 1024])));
            axum::body::Body::from_stream(futures_util::stream::iter(chunks))
        };
        let addr = serve(
            Router::new()
                .route("/big", get(big))
                .route("/stream", get(streamed)),
        )
        .await;
        let store = SqliteStore::open_in_memory().unwrap();
        grant_loopback_ceiling(&store);
        let broker = HttpBroker::new(store).unwrap();
        let admin = Principal::administrator();
        for path in ["big", "stream"] {
            let err = broker
                .execute(
                    &admin,
                    &conn_inst(None),
                    None,
                    &loopback_graph(),
                    get_req(format!("http://127.0.0.1:{}/{path}", addr.port()), None),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, HttpBrokerError::ResponseTooLarge),
                "{path}: {err}"
            );
        }
    }

    #[test]
    fn https_downgrade_and_reserved_headers() {
        assert!(is_reserved_request_header("Host", None, false, true));
        assert!(is_reserved_request_header(
            "Authorization",
            None,
            true,
            true
        ));
        assert!(is_reserved_request_header("Cookie", None, true, true));
        assert!(!is_reserved_request_header(
            "Authorization",
            None,
            false,
            true
        ));
        let h = ("x-api-key".to_string(), "v".to_string());
        assert!(is_reserved_request_header(
            "X-API-Key",
            Some(&h),
            true,
            true
        ));
        assert!(is_reserved_request_header(
            "content-type",
            None,
            false,
            false
        ));
        assert!(!is_reserved_request_header("accept", None, true, false));
    }

    // R7:SEC-3: a `query:` credential whose wire form differs from its plaintext never reaches
    // transport error text (reqwest prints the full URL; the broker strips it).
    #[tokio::test]
    async fn query_credentials_never_appear_in_transport_errors() {
        // A port with nothing listening: the connect error carries the URL in reqwest's Display.
        let closed = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let store = SqliteStore::open_in_memory().unwrap();
        grant_loopback_ceiling(&store);
        let secret = "Zk/9+q==~:x y";
        store
            .set_secret_with_policy("wx", "", secret, &[], None, Some("query:appid"))
            .unwrap();
        let broker = HttpBroker::new(store.clone()).unwrap();
        let err = broker
            .execute(
                &Principal::administrator(),
                &conn_inst(Some("wx")),
                None,
                &loopback_graph(),
                get_req(format!("http://127.0.0.1:{closed}/v1?city=x"), Some("auth")),
            )
            .await
            .unwrap_err();
        let msg = err.to_string();
        let form: String = url::form_urlencoded::byte_serialize(secret.as_bytes()).collect();
        for leaked in [secret, form.as_str(), "Zk%2F9", "appid=", "city=x"] {
            assert!(!msg.contains(leaked), "'{leaked}' leaked into: {msg}");
        }
        assert!(
            msg.contains(&format!("http://127.0.0.1:{closed}/v1")),
            "{msg}"
        );
        let audits = serde_json::to_string(&store.list_audits(5).unwrap()).unwrap();
        assert!(
            !audits.contains("appid=") && !audits.contains("Zk%2F9"),
            "{audits}"
        );
    }

    // R2:HTTP-AUDIT-1: a request dropped mid-flight (VM deadline, cancel, bounded wait) is still
    // audited as `http.request` with outcome `aborted`.
    #[tokio::test]
    async fn requests_dropped_mid_flight_are_audited_as_aborted() {
        let addr = serve(Router::new().route(
            "/merge",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                "late"
            }),
        ))
        .await;
        let store = SqliteStore::open_in_memory().unwrap();
        grant_loopback_ceiling(&store);
        let broker = HttpBroker::new(store.clone()).unwrap();
        let req = HttpHostRequest {
            method: "POST".into(),
            url: format!("http://127.0.0.1:{}/merge?sha=abc", addr.port()),
            headers: BTreeMap::new(),
            body: Some(json!({ "merge": true })),
            credential: None,
        };
        let admin = Principal::administrator();
        let inst = conn_inst(None);
        let g = loopback_graph();
        let timed_out = tokio::time::timeout(
            Duration::from_millis(400),
            broker.execute(&admin, &inst, None, &g, req),
        )
        .await;
        assert!(timed_out.is_err(), "the upstream never answers");
        let rec = store
            .list_audits(10)
            .unwrap()
            .into_iter()
            .find(|a| a.operation == "http.request")
            .expect("dropped request must be audited");
        assert_eq!(rec.outcome, "aborted");
        assert_eq!(rec.target, "gh_conn");
        assert_eq!(rec.detail["method"], "POST");
        assert_eq!(rec.detail["reached_send"], true);
        assert_eq!(
            rec.detail["destination"],
            format!("http://127.0.0.1:{}/merge", addr.port())
        );
    }

    // R8:SC-11: every outbound hop is a `connector_http:<connector>` span in the caller's trace.
    #[tokio::test]
    async fn outbound_hops_are_recorded_as_trace_spans() {
        let origin = serve(
            Router::new()
                .route(
                    "/start",
                    get(|| async {
                        (StatusCode::TEMPORARY_REDIRECT, [("location", "/end")]).into_response()
                    }),
                )
                .route(
                    "/end",
                    get(|| async { (StatusCode::TOO_MANY_REQUESTS, "slow down") }),
                ),
        )
        .await;
        let store = SqliteStore::open_in_memory().unwrap();
        grant_loopback_ceiling(&store);
        let telemetry = Telemetry::new(store.clone());
        let broker = HttpBroker::new(store.clone())
            .unwrap()
            .with_telemetry(telemetry.clone());
        let resp = broker
            .execute_traced(
                &Principal::administrator(),
                &conn_inst(None),
                None,
                &loopback_graph(),
                get_req(
                    format!("http://127.0.0.1:{}/start?q=1", origin.port()),
                    None,
                ),
                Some("tr_hops"),
            )
            .await
            .unwrap();
        assert_eq!(resp.status, 429);
        let mut hops: Vec<_> = telemetry
            .recent_spans(50)
            .into_iter()
            .filter(|s| s.name == "connector_http:gh_conn")
            .collect();
        hops.sort_by_key(|s| s.attributes["hop"].as_u64());
        assert_eq!(hops.len(), 2, "{hops:?}");
        assert!(hops.iter().all(|s| s.trace_id == "tr_hops"));
        assert_eq!(hops[0].attributes["status"], 307);
        assert_eq!(hops[0].attributes["outcome"], "redirect");
        assert_eq!(hops[1].attributes["status"], 429);
        assert_eq!(hops[1].attributes["retry_class"], "retry_after_backoff");
        assert_eq!(hops[1].status, "error");
        assert!(!hops[0].attributes["destination"]
            .as_str()
            .unwrap()
            .contains("q=1"));
    }

    // R7:SEC-5 support: non-HTTP outbound destinations get the same ceiling + SSRF vetting.
    #[tokio::test]
    async fn outbound_hosts_are_vetted_against_ceiling_and_ip_classes() {
        let ceiling = NetworkPolicy {
            allowed_hosts: vec!["*".into()],
            allowed_methods: vec!["*".into()],
            allow_loopback_for_tests: false,
        };
        assert!(matches!(
            vet_outbound_host("example.com", 443, None).await,
            Err(HttpBrokerError::PolicyDenied(..))
        ));
        for internal in [
            "10.0.0.12",
            "127.0.0.1",
            "[::1]",
            "169.254.169.254",
            "localhost",
        ] {
            assert!(
                matches!(
                    vet_outbound_host(internal, 22, Some(&ceiling)).await,
                    Err(HttpBrokerError::SsrfBlocked(_))
                ),
                "{internal}"
            );
        }
        let narrow = NetworkPolicy {
            allowed_hosts: vec!["packages.example.com".into()],
            ..ceiling.clone()
        };
        assert!(matches!(
            vet_outbound_host("internal-scm", 22, Some(&narrow)).await,
            Err(HttpBrokerError::PolicyDenied(..))
        ));
        assert!(vet_outbound_host("8.8.8.8", 443, Some(&ceiling))
            .await
            .is_ok());
        let loopback = NetworkPolicy {
            allow_loopback_for_tests: true,
            ..ceiling
        };
        assert!(vet_outbound_host("127.0.0.1", 22, Some(&loopback))
            .await
            .is_ok());
    }
}
