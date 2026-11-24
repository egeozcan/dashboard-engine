//! WebSocket endpoint (`/ws`) delivering coalesced Signal streams and validating Widget Requests (`dash.widget/1`).
//!
//! Protocol (client -> server, `type` tagged):
//! - `subscribe_query {instance, params}` / `unsubscribe_query {query_key}` /
//!   `resync_query {query_key}` (request a fresh snapshot)
//! - `subscribe_dashboard {dashboard_id}` / `unsubscribe_dashboard {dashboard_id}`: a live
//!   subscription; `dashboard_render` (`protocol: "dash.render/1"`) is pushed initially and on
//!   every Dashboard State, upstream Query Signal or Generation change
//! - `set_dashboard_state {dashboard_id, key, value, expected_version?}`
//! - `widget_request {...}`
//!
//! Delivery guarantees (§6.2, §7.1):
//! - A query subscription is registered *before* the initial computation, so no update is lost
//!   between the first snapshot and the stream, and it stays registered when that computation
//!   fails (later successes are delivered). Subscriptions are deduplicated per socket by
//!   `query_key` and capped. When a new Generation changes a subscription's `query_key` (e.g.
//!   its Definition changed), the server re-binds it and sends
//!   `{type: "subscribed", query_key, replaces}` followed by a resync snapshot.
//! - A keyed delta is not followed by the full snapshot of the same version, and envelopes
//!   buffered before the initial snapshot (`loading`, older versions) are not replayed.
//! - Deltas are only forwarded when their `base_version` matches the last version this socket
//!   delivered. Lagging behind the bounded stream, or a base mismatch, makes the socket skip to
//!   the latest snapshot (`resync: true`) instead of silently stopping or sending broken deltas.
//! - State Signals are filtered per socket: only Dashboard State of dashboards this socket
//!   subscribed to (after an authority check, repeated whenever the Generation changes) is
//!   forwarded. Instance State never leaves the
//!   host and Client State is browser-only.
//! - The Principal is re-validated periodically; revoked tokens or expired sessions close the
//!   socket.

use crate::auth::authenticate_request;
use crate::services::AppServices;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    http::{HeaderMap, Method},
    response::IntoResponse,
};
use dash_core::signal::QueryStatus;
use dash_core::{EnvelopeKind, Principal, QueryEnvelope, WidgetRequestEnvelope};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, watch, Notify};

/// Per-socket outbound queue (bounded; forwarders wait for capacity, then skip to latest).
const OUTBOUND_QUEUE_CAPACITY: usize = 64;
/// Maximum distinct query subscriptions per socket.
pub const MAX_QUERY_SUBSCRIPTIONS_PER_SOCKET: usize = 64;
/// Maximum Dashboard State subscriptions per socket.
pub const MAX_DASHBOARD_SUBSCRIPTIONS_PER_SOCKET: usize = 16;
/// How often an open socket re-checks its Principal (revocation, logout, expiry).
const PRINCIPAL_REVALIDATE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsClientMsg {
    SubscribeQuery {
        instance: String,
        #[serde(default)]
        params: Value,
    },
    UnsubscribeQuery {
        query_key: String,
    },
    ResyncQuery {
        query_key: String,
    },
    SubscribeDashboard {
        dashboard_id: String,
    },
    UnsubscribeDashboard {
        dashboard_id: String,
    },
    SetDashboardState {
        dashboard_id: String,
        key: String,
        value: Value,
        /// CAS: the record version the write is based on (`0` = only if absent).
        #[serde(default)]
        expected_version: Option<i64>,
    },
    WidgetRequest {
        #[serde(flatten)]
        envelope: WidgetRequestEnvelope,
    },
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(services): State<AppServices>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate_request(&services, &headers, &Method::GET, peer) {
        Ok(a) => a,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let principal = auth.principal().clone();
    ws.on_upgrade(move |socket| handle_ws_socket(socket, services, principal, headers, peer))
        .into_response()
}

struct QuerySubscription {
    handle: tokio::task::JoinHandle<()>,
    resync: Arc<Notify>,
    /// The query_key this subscription currently streams. The forwarder re-binds it when a new
    /// Generation changes the key (e.g. a Definition update changes its `identity_version`).
    key: Arc<RwLock<String>>,
}

impl QuerySubscription {
    fn current_key(&self) -> Option<String> {
        self.key.read().ok().map(|k| k.clone())
    }
}

/// Re-key the socket's query subscriptions after their forwarders re-bound to a new query_key,
/// and drop subscriptions whose forwarder ended (e.g. the Instance was removed or became
/// inaccessible in a new Generation). Two subscriptions converging on one key keep one forwarder.
fn rekey_query_subscriptions(subs: &mut HashMap<String, QuerySubscription>) {
    let current = |k: &String, s: &QuerySubscription| {
        !s.handle.is_finished() && s.current_key().as_ref() == Some(k)
    };
    if subs.iter().all(|(k, s)| current(k, s)) {
        return;
    }
    for (_, s) in std::mem::take(subs) {
        let Some(key) = s.current_key().filter(|_| !s.handle.is_finished()) else {
            s.handle.abort();
            continue;
        };
        match subs.entry(key) {
            std::collections::hash_map::Entry::Occupied(_) => s.handle.abort(),
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(s);
            }
        }
    }
}

/// Dashboards this socket subscribed to, each with the Generation in which the Principal's read
/// authority over it was last verified. Read authority depends on the Generation (the Adapters
/// placed on the Dashboard), so it is re-verified whenever the active Generation changes.
type DashboardSet = Arc<RwLock<HashMap<String, i64>>>;

async fn handle_ws_socket(
    socket: WebSocket,
    services: AppServices,
    principal: Principal,
    headers: HeaderMap,
    peer: SocketAddr,
) {
    let (mut sender, mut receiver) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<Value>(OUTBOUND_QUEUE_CAPACITY);

    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let txt = serde_json::to_string(&msg).unwrap_or_default();
            if sender.send(Message::Text(txt.into())).await.is_err() {
                break;
            }
        }
        let _ = sender.close().await;
    });

    let dashboards: DashboardSet = Arc::new(RwLock::new(HashMap::new()));
    let state_task = tokio::spawn(forward_state_signals(
        services.clone(),
        principal.clone(),
        services.engine().signals.subscribe_state(),
        Arc::clone(&dashboards),
        out_tx.clone(),
    ));

    let mut subs: HashMap<String, QuerySubscription> = HashMap::new();
    // Live Dashboard render subscriptions (one task per subscribed Dashboard).
    let mut dash_subs: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut revalidate = tokio::time::interval(PRINCIPAL_REVALIDATE_INTERVAL);
    revalidate.tick().await; // first tick fires immediately

    loop {
        let frame = tokio::select! {
            frame = receiver.next() => frame,
            _ = revalidate.tick() => {
                if !principal_still_valid(&services, &headers, peer, &principal) {
                    let _ = out_tx
                        .send(json!({
                            "type": "error",
                            "code": "unauthenticated",
                            "message": "session or token is no longer valid"
                        }))
                        .await;
                    break;
                }
                continue;
            }
        };
        let Some(Ok(frame)) = frame else { break };
        let txt = match frame {
            Message::Text(t) => t.to_string(),
            Message::Close(_) => break,
            _ => continue,
        };
        let Ok(parsed) = serde_json::from_str::<WsClientMsg>(&txt) else {
            let _ = out_tx
                .send(json!({ "type": "error", "code": "invalid", "message": "invalid ws message payload" }))
                .await;
            continue;
        };
        rekey_query_subscriptions(&mut subs);
        match parsed {
            WsClientMsg::SubscribeQuery { instance, params } => {
                subscribe_query(&services, &principal, &out_tx, &mut subs, instance, params).await;
            }
            WsClientMsg::UnsubscribeQuery { query_key } => {
                let existed = subs.remove(&query_key).map(|s| s.handle.abort()).is_some();
                let _ = out_tx
                    .send(json!({ "type": "unsubscribed", "query_key": query_key, "existed": existed }))
                    .await;
            }
            WsClientMsg::ResyncQuery { query_key } => match subs.get(&query_key) {
                Some(s) => s.resync.notify_one(),
                None => {
                    let _ = out_tx
                        .send(json!({ "type": "error", "code": "not_subscribed", "query_key": query_key, "message": "not subscribed to this query" }))
                        .await;
                }
            },
            WsClientMsg::SubscribeDashboard { dashboard_id } => {
                if let Some(prev) = dash_subs.remove(&dashboard_id) {
                    prev.abort();
                }
                // Re-subscribing re-checks authority: a refused re-subscribe must also stop
                // State Signal delivery for this Dashboard.
                if let Ok(mut d) = dashboards.write() {
                    d.remove(&dashboard_id);
                }
                if let Some(handle) = subscribe_dashboard(
                    &services,
                    &principal,
                    &out_tx,
                    &dashboards,
                    dashboard_id.clone(),
                )
                .await
                {
                    dash_subs.insert(dashboard_id, handle);
                }
            }
            WsClientMsg::UnsubscribeDashboard { dashboard_id } => {
                if let Some(prev) = dash_subs.remove(&dashboard_id) {
                    prev.abort();
                }
                let existed = dashboards
                    .write()
                    .map(|mut d| d.remove(&dashboard_id).is_some())
                    .unwrap_or(false);
                let _ = out_tx
                    .send(json!({ "type": "unsubscribed", "dashboard_id": dashboard_id, "existed": existed }))
                    .await;
            }
            WsClientMsg::SetDashboardState {
                dashboard_id,
                key,
                value,
                expected_version,
            } => {
                let graph = services.graph();
                match services.engine().state.set_dashboard_state(
                    &graph,
                    &principal,
                    &dashboard_id,
                    &key,
                    value,
                    expected_version,
                ) {
                    Ok(rec) => {
                        let _ = out_tx
                            .send(json!({ "type": "state_updated", "record": rec }))
                            .await;
                    }
                    Err(err) => {
                        let _ = out_tx
                            .send(json!({
                                "type": "error",
                                "code": err.code(),
                                "dashboard_id": dashboard_id,
                                "key": key,
                                "message": err.to_string()
                            }))
                            .await;
                    }
                }
            }
            WsClientMsg::WidgetRequest { envelope } => {
                match services
                    .dispatch_widget_request_outcome(envelope, &principal)
                    .await
                {
                    Ok(exec) => {
                        let _ = out_tx
                            .send(json!({ "type": "request_result", "execution": exec }))
                            .await;
                    }
                    Err(err) => {
                        let _ = out_tx
                            .send(json!({ "type": "request_error", "message": err }))
                            .await;
                    }
                }
            }
        }
    }

    for (_, s) in subs {
        s.handle.abort();
    }
    for (_, h) in dash_subs {
        h.abort();
    }
    state_task.abort();
    drop(out_tx);
    // Let the writer flush queued frames (e.g. the final error) and close, but never hang.
    let _ = tokio::time::timeout(Duration::from_secs(2), writer_task).await;
}

fn principal_still_valid(
    services: &AppServices,
    headers: &HeaderMap,
    peer: SocketAddr,
    principal: &Principal,
) -> bool {
    match authenticate_request(services, headers, &Method::GET, peer) {
        Ok(auth) => {
            let current = auth.principal();
            current.id == principal.id
                && current.authorization_fingerprint() == principal.authorization_fingerprint()
        }
        Err(_) => false,
    }
}

async fn subscribe_query(
    services: &AppServices,
    principal: &Principal,
    out: &mpsc::Sender<Value>,
    subs: &mut HashMap<String, QuerySubscription>,
    instance: String,
    params: Value,
) {
    // Read before resolving: a Generation swap in between only causes a no-op re-resolution.
    let generation = services.graph().generation_id;
    let qk = match services
        .engine()
        .queries
        .resolve_query_key(&instance, &params, principal)
    {
        Ok(qk) => qk,
        Err(err) => {
            let _ = out
                .send(json!({ "type": "error", "code": "query_denied", "instance": instance, "message": err }))
                .await;
            return;
        }
    };
    if subs.contains_key(&qk) {
        // Deduplicated: one forwarder per query per socket. Send a fresh snapshot instead.
        if let Some(s) = subs.get(&qk) {
            s.resync.notify_one();
        }
        let _ = out
            .send(json!({ "type": "subscribed", "query_key": qk, "instance": instance, "duplicate": true }))
            .await;
        return;
    }
    if subs.len() >= MAX_QUERY_SUBSCRIPTIONS_PER_SOCKET {
        let _ = out
            .send(json!({
                "type": "error",
                "code": "subscription_limit",
                "instance": instance,
                "message": format!("at most {MAX_QUERY_SUBSCRIPTIONS_PER_SOCKET} query subscriptions per connection")
            }))
            .await;
        return;
    }

    // Subscribe BEFORE the initial computation so no published update is missed.
    let rx = services.engine().signals.subscribe_stream(&qk);
    let _ = out
        .send(json!({ "type": "subscribed", "query_key": qk, "instance": instance }))
        .await;
    let mut cursor = DeliveryCursor::default();
    match services
        .provider_query(&instance, params.clone(), principal, false)
        .await
    {
        Ok(env) => {
            if out.send(cursor.snapshot_msg(&env, false)).await.is_err() {
                return;
            }
        }
        Err(err) => {
            // Stay subscribed: an `error` Signal was published and later successes will follow.
            let _ = out
                .send(json!({ "type": "error", "code": "query_failed", "query_key": qk, "instance": instance, "message": err }))
                .await;
        }
    }

    let resync = Arc::new(Notify::new());
    let key = Arc::new(RwLock::new(qk.clone()));
    let forwarder = QueryForwarder {
        services: services.clone(),
        principal: principal.clone(),
        instance,
        params,
        key: Arc::clone(&key),
        out: out.clone(),
        resync: Arc::clone(&resync),
    };
    let handle = tokio::spawn(forwarder.run(qk.clone(), rx, cursor, generation));
    subs.insert(
        qk,
        QuerySubscription {
            handle,
            resync,
            key,
        },
    );
}

/// What this socket last delivered for one query.
#[derive(Default)]
struct DeliveryCursor {
    /// Last version delivered (`None` until a valued snapshot was sent).
    version: Option<u64>,
    /// (version, status) of the state the client holds after the last delivered snapshot or
    /// delta. A snapshot of exactly that state (e.g. the snapshot published right after the
    /// keyed delta of the same version) is a duplicate.
    last_snapshot: Option<(u64, QueryStatus)>,
}

/// How a forwarder handles one envelope from the query's stream.
#[derive(Debug)]
enum Delivery {
    /// Forward this message.
    Send(Value),
    /// Already reflected by what this socket delivered.
    Skip,
    /// Cannot be applied on top of what was delivered: skip to the latest snapshot.
    Resync,
}

impl DeliveryCursor {
    fn snapshot_msg(&mut self, env: &QueryEnvelope, resync: bool) -> Value {
        if env.version > 0 {
            self.version = Some(env.version);
        }
        self.last_snapshot = Some((env.version, env.status));
        if resync {
            json!({ "type": "query_signal", "envelope": env, "resync": true })
        } else {
            json!({ "type": "query_signal", "envelope": env })
        }
    }

    fn is_duplicate_snapshot(&self, env: &QueryEnvelope) -> bool {
        self.last_snapshot == Some((env.version, env.status))
    }

    /// A snapshot older than the value this socket delivered, or a `loading` status envelope
    /// once a value was delivered (e.g. buffered from before the initial snapshot): forwarding
    /// it would make the client's status or version go backwards.
    fn is_superseded_snapshot(&self, env: &QueryEnvelope) -> bool {
        match self.version {
            Some(have) => {
                (env.version > 0 && env.version < have)
                    || (env.version == 0 && env.status == QueryStatus::Loading)
            }
            None => false,
        }
    }

    fn deliver(&mut self, env: &QueryEnvelope) -> Delivery {
        match env.kind {
            EnvelopeKind::Snapshot => {
                if self.is_duplicate_snapshot(env) || self.is_superseded_snapshot(env) {
                    Delivery::Skip
                } else {
                    Delivery::Send(self.snapshot_msg(env, false))
                }
            }
            EnvelopeKind::Delta => match (self.version, env.base_version) {
                (Some(have), Some(base)) if base == have => {
                    self.version = Some(env.version);
                    self.last_snapshot = Some((env.version, env.status));
                    Delivery::Send(json!({ "type": "query_signal", "envelope": env }))
                }
                // Already covered by a newer snapshot this socket delivered.
                (Some(have), Some(base)) if base < have => Delivery::Skip,
                // Gap, or no base to apply onto: skip to the latest snapshot.
                _ => Delivery::Resync,
            },
        }
    }
}

/// Streams one query subscription's Signals to the socket.
struct QueryForwarder {
    services: AppServices,
    principal: Principal,
    instance: String,
    params: Value,
    /// Current query_key, shared with the socket loop (re-bound on a Generation change).
    key: Arc<RwLock<String>>,
    out: mpsc::Sender<Value>,
    resync: Arc<Notify>,
}

impl QueryForwarder {
    /// `qk`/`rx`/`cursor` are the subscription's current binding and what was delivered on it;
    /// `generation` is the Generation `qk` was resolved in.
    ///
    /// The query_key hashes the Definition's `identity_version`, so a new Generation may compute
    /// (and publish) the same query under a new key. On every Generation change the key is
    /// re-resolved; when it moved, the forwarder subscribes to the new key, announces
    /// `{type: "subscribed", query_key, replaces}` and sends a resync snapshot. When the query
    /// can no longer be resolved (Instance removed, access denied) the subscription ends with an
    /// error.
    async fn run(
        self,
        mut qk: String,
        mut rx: broadcast::Receiver<QueryEnvelope>,
        mut cursor: DeliveryCursor,
        mut generation: i64,
    ) {
        let mut gen_tick = tokio::time::interval(GENERATION_CHECK_INTERVAL);
        gen_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let need_resync = tokio::select! {
                r = rx.recv() => match r {
                    Ok(env) => match cursor.deliver(&env) {
                        Delivery::Send(msg) => {
                            if self.out.send(msg).await.is_err() {
                                return;
                            }
                            false
                        }
                        Delivery::Skip => false,
                        Delivery::Resync => true,
                    },
                    // Fell behind the bounded stream: intermediate values are skipped (§6.2).
                    Err(RecvError::Lagged(_)) => true,
                    Err(RecvError::Closed) => return,
                },
                _ = self.resync.notified() => true,
                _ = gen_tick.tick() => {
                    let gid = self.services.graph().generation_id;
                    if gid == generation {
                        continue;
                    }
                    generation = gid;
                    match self.services.engine().queries.resolve_query_key(
                        &self.instance,
                        &self.params,
                        &self.principal,
                    ) {
                        Ok(new_qk) if new_qk == qk => continue,
                        Ok(new_qk) => {
                            // Subscribe before computing so no update on the new key is lost.
                            rx = self.services.engine().signals.subscribe_stream(&new_qk);
                            if let Ok(mut k) = self.key.write() {
                                *k = new_qk.clone();
                            }
                            let msg = json!({
                                "type": "subscribed",
                                "query_key": new_qk,
                                "instance": self.instance,
                                "replaces": qk,
                            });
                            if self.out.send(msg).await.is_err() {
                                return;
                            }
                            qk = new_qk;
                            cursor = DeliveryCursor::default();
                            true
                        }
                        Err(err) => {
                            let _ = self
                                .out
                                .send(json!({
                                    "type": "error",
                                    "code": "query_denied",
                                    "query_key": qk,
                                    "instance": self.instance,
                                    "message": err,
                                }))
                                .await;
                            return;
                        }
                    }
                }
            };
            if need_resync {
                // Drop everything buffered; only values published from now on are streamed.
                rx = rx.resubscribe();
                let latest = match self.services.engine().signals.latest_snapshot(&qk) {
                    Some(env) => Ok(env),
                    None => {
                        self.services
                            .provider_query(
                                &self.instance,
                                self.params.clone(),
                                &self.principal,
                                false,
                            )
                            .await
                    }
                };
                let msg = match latest {
                    Ok(env) => cursor.snapshot_msg(&env, true),
                    Err(err) => {
                        json!({ "type": "error", "code": "query_failed", "query_key": qk, "message": err })
                    }
                };
                if self.out.send(msg).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Authorize and register a Dashboard subscription (State Signal filtering), then start its
/// live render subscription. Returns the render task, or `None` when refused.
async fn subscribe_dashboard(
    services: &AppServices,
    principal: &Principal,
    out: &mpsc::Sender<Value>,
    dashboards: &DashboardSet,
    dashboard_id: String,
) -> Option<tokio::task::JoinHandle<()>> {
    let graph = services.graph();
    if let Err(err) = dash_core::state::authorize_dashboard_read(principal, &graph, &dashboard_id) {
        let _ = out
            .send(json!({ "type": "error", "code": err.code(), "dashboard_id": dashboard_id, "message": err.to_string() }))
            .await;
        return None;
    }
    let admitted = match dashboards.write() {
        Ok(mut d) => {
            if d.contains_key(&dashboard_id) || d.len() < MAX_DASHBOARD_SUBSCRIPTIONS_PER_SOCKET {
                d.insert(dashboard_id.clone(), graph.generation_id);
                true
            } else {
                false
            }
        }
        Err(_) => false,
    };
    if !admitted {
        let _ = out
            .send(json!({
                "type": "error",
                "code": "subscription_limit",
                "dashboard_id": dashboard_id,
                "message": format!("at most {MAX_DASHBOARD_SUBSCRIPTIONS_PER_SOCKET} dashboard subscriptions per connection")
            }))
            .await;
        return None;
    }
    // Initial render now, then re-render pushes on State / upstream Query / Generation changes.
    Some(tokio::spawn(run_dashboard_subscription(
        services.clone(),
        principal.clone(),
        dashboard_id,
        out.clone(),
        Arc::clone(dashboards),
    )))
}

/// Whether this socket may still read `dashboard_id`: it subscribed to it, and its read
/// authority holds in `generation`. Authority verified in an older Generation is re-checked
/// with `authorize` (the Adapters placed on a Dashboard may have changed); a Dashboard that is
/// no longer readable is removed from the set.
fn dashboard_readable(
    dashboards: &DashboardSet,
    dashboard_id: &str,
    generation: i64,
    authorize: impl FnOnce(&str) -> bool,
) -> bool {
    let verified = match dashboards.read() {
        Ok(d) => d.get(dashboard_id).copied(),
        Err(_) => return false,
    };
    match verified {
        None => false,
        Some(g) if g == generation => true,
        Some(_) => {
            let allowed = authorize(dashboard_id);
            if let Ok(mut d) = dashboards.write() {
                if allowed {
                    if let Some(g) = d.get_mut(dashboard_id) {
                        *g = generation;
                    }
                } else {
                    d.remove(dashboard_id);
                }
            }
            allowed
        }
    }
}

/// Whether a State Signal may be delivered to a socket subscribed to `dashboards` (see
/// [`dashboard_readable`]).
fn state_signal_visible(
    sig: &dash_core::StateSignalPayload,
    dashboards: &DashboardSet,
    generation: i64,
    authorize: impl FnOnce(&str) -> bool,
) -> bool {
    sig.scope == "dashboard" && dashboard_readable(dashboards, &sig.owner, generation, authorize)
}

/// [`dashboard_readable`] against the active Generation.
fn dashboard_readable_now(
    services: &AppServices,
    principal: &Principal,
    dashboards: &DashboardSet,
    dashboard_id: &str,
) -> bool {
    let graph = services.graph();
    dashboard_readable(dashboards, dashboard_id, graph.generation_id, |id| {
        dash_core::state::authorize_dashboard_read(principal, &graph, id).is_ok()
    })
}

async fn forward_state_signals(
    services: AppServices,
    principal: Principal,
    mut rx: broadcast::Receiver<dash_core::StateSignalPayload>,
    dashboards: DashboardSet,
    out: mpsc::Sender<Value>,
) {
    loop {
        match rx.recv().await {
            Ok(sig) => {
                let graph = services.graph();
                let visible = state_signal_visible(&sig, &dashboards, graph.generation_id, |id| {
                    dash_core::state::authorize_dashboard_read(&principal, &graph, id).is_ok()
                });
                if !visible {
                    continue;
                }
                if out
                    .send(json!({ "type": "state_signal", "payload": sig }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Err(RecvError::Lagged(_)) => {
                // Skip intermediate values and resend the current State of subscribed dashboards.
                rx = rx.resubscribe();
                let ids: Vec<String> = dashboards
                    .read()
                    .map(|d| d.keys().cloned().collect())
                    .unwrap_or_default();
                for id in ids {
                    if !dashboard_readable_now(&services, &principal, &dashboards, &id) {
                        continue;
                    }
                    let records = services
                        .engine()
                        .state
                        .list_dashboard_state(&id)
                        .unwrap_or_default();
                    if out
                        .send(json!({ "type": "state_resync", "dashboard_id": id, "records": records }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            Err(RecvError::Closed) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(scope: &str, owner: &str) -> dash_core::StateSignalPayload {
        dash_core::StateSignalPayload {
            scope: scope.into(),
            owner: owner.into(),
            state_key: "k".into(),
            version: 1,
            value: json!(1),
            updated_at: String::new(),
        }
    }

    #[test]
    fn state_signals_are_filtered_by_scope_and_subscription() {
        let set: DashboardSet = Arc::new(RwLock::new(HashMap::from([("eng".to_string(), 1)])));
        let allow = |_: &str| true;
        assert!(state_signal_visible(
            &sig("dashboard", "eng"),
            &set,
            1,
            allow
        ));
        assert!(!state_signal_visible(
            &sig("dashboard", "ops"),
            &set,
            1,
            allow
        ));
        assert!(!state_signal_visible(
            &sig("instance", "eng"),
            &set,
            1,
            allow
        ));
        assert!(!state_signal_visible(&sig("client", "eng"), &set, 1, allow));
    }

    #[test]
    fn state_signal_authority_is_reverified_when_the_generation_changes() {
        let set: DashboardSet = Arc::new(RwLock::new(HashMap::from([("eng".to_string(), 1)])));
        // Same Generation: no re-check needed.
        assert!(state_signal_visible(
            &sig("dashboard", "eng"),
            &set,
            1,
            |_| { panic!("authority verified in this Generation must not be re-checked") }
        ));
        // New Generation, still readable: re-checked once and recorded.
        assert!(state_signal_visible(
            &sig("dashboard", "eng"),
            &set,
            2,
            |id| id == "eng"
        ));
        assert_eq!(set.read().unwrap().get("eng"), Some(&2));
        // A later Generation places an Adapter the Principal may not read: denied and dropped.
        assert!(!state_signal_visible(
            &sig("dashboard", "eng"),
            &set,
            3,
            |_| false
        ));
        assert!(set.read().unwrap().is_empty());
        assert!(!state_signal_visible(
            &sig("dashboard", "eng"),
            &set,
            3,
            |_| true
        ));
    }

    #[test]
    fn delivery_cursor_dedups_identical_snapshots_only() {
        let mut c = DeliveryCursor::default();
        let env =
            QueryEnvelope::snapshot("i", "q", 1, 3, QueryStatus::Fresh, None, json!([]), None);
        let _ = c.snapshot_msg(&env, false);
        assert!(c.is_duplicate_snapshot(&env));
        let stale =
            QueryEnvelope::snapshot("i", "q", 1, 3, QueryStatus::Stale, None, json!([]), None);
        assert!(!c.is_duplicate_snapshot(&stale));
        assert_eq!(c.version, Some(3));
    }

    fn snapshot(version: u64, status: QueryStatus) -> QueryEnvelope {
        QueryEnvelope::snapshot("i", "q", 1, version, status, None, json!([]), None)
    }

    #[test]
    fn delivered_delta_suppresses_the_same_version_snapshot() {
        let mut c = DeliveryCursor::default();
        let _ = c.snapshot_msg(&snapshot(1, QueryStatus::Fresh), false);
        let mut delta = QueryEnvelope::delta(
            "i",
            "q",
            1,
            1,
            2,
            None,
            vec![dash_core::DeltaOp::Delete { key: "a".into() }],
        );
        delta.status = QueryStatus::Fresh;
        assert!(matches!(c.deliver(&delta), Delivery::Send(_)));
        // publish_commit streams the full snapshot of the same version right after the delta.
        assert!(matches!(
            c.deliver(&snapshot(2, QueryStatus::Fresh)),
            Delivery::Skip
        ));
        // A status change at that version is still delivered.
        assert!(matches!(
            c.deliver(&snapshot(2, QueryStatus::Stale)),
            Delivery::Send(_)
        ));
        assert_eq!(c.version, Some(2));
    }

    #[test]
    fn buffered_pre_snapshot_envelopes_are_not_replayed() {
        // The forwarder starts after the initial snapshot (v12) was sent; the stream still holds
        // the leader's `loading` envelope and the very same snapshot.
        let mut c = DeliveryCursor::default();
        let _ = c.snapshot_msg(&snapshot(12, QueryStatus::Fresh), false);
        let loading = QueryEnvelope::loading("i", "q", 1, None);
        assert!(matches!(c.deliver(&loading), Delivery::Skip));
        assert!(matches!(
            c.deliver(&snapshot(12, QueryStatus::Fresh)),
            Delivery::Skip
        ));
        // Older snapshots never make the version go backwards.
        assert!(matches!(
            c.deliver(&snapshot(7, QueryStatus::Fresh)),
            Delivery::Skip
        ));
        // Errors and newer values still flow.
        let err = QueryEnvelope::error("i", "q", 1, None, "boom".into());
        assert!(matches!(c.deliver(&err), Delivery::Send(_)));
        assert!(matches!(
            c.deliver(&snapshot(13, QueryStatus::Fresh)),
            Delivery::Send(_)
        ));
        // Before any value was delivered, `loading` is forwarded.
        let mut fresh = DeliveryCursor::default();
        assert!(matches!(fresh.deliver(&loading), Delivery::Send(_)));
    }

    #[test]
    fn rekeying_follows_rebound_subscriptions_and_drops_ended_ones() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let sub = |key: &str, run: bool| {
                let handle = if run {
                    tokio::spawn(std::future::pending::<()>())
                } else {
                    tokio::spawn(async {})
                };
                QuerySubscription {
                    handle,
                    resync: Arc::new(Notify::new()),
                    key: Arc::new(RwLock::new(key.to_string())),
                }
            };
            let mut subs = HashMap::new();
            subs.insert("k1".to_string(), sub("k1", true));
            subs.insert("k2".to_string(), sub("k2", false));
            tokio::task::yield_now().await;
            *subs["k1"].key.write().unwrap() = "k1b".into();
            rekey_query_subscriptions(&mut subs);
            assert_eq!(subs.keys().cloned().collect::<Vec<_>>(), vec!["k1b"]);
            for (_, s) in subs {
                s.handle.abort();
            }
        });
    }
}

/// Protocol identifier carried by every pushed `dashboard_render` message.
pub const DASHBOARD_RENDER_PROTOCOL: &str = "dash.render/1";
/// Lower bound between two renders of one Dashboard subscription (coalesces bursts).
const MIN_DASHBOARD_RENDER_INTERVAL: Duration = Duration::from_millis(250);
/// How often a subscription checks whether a new Generation was activated.
const GENERATION_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// One live Dashboard subscription on a socket (§6.2, §7.1, §8, §10.1).
///
/// Sends an initial `dashboard_render`, then re-renders under the subscribing Principal and
/// pushes a new `dashboard_render` whenever
/// - a Dashboard State Signal for this Dashboard arrives (`reason: "state"`),
/// - a Query Signal of a query this Dashboard's renders read carries a new version or status
///   (`reason: "query"`),
/// - a new Generation is activated (`reason: "generation"`), or
/// - a Signal stream lagged and intermediate values were skipped (`reason: "resync"`).
///
/// The subscription is a Signal consumer of every query its renders read (transitively,
/// through the dependencies recorded for the Adapter renders under this Principal): it holds a
/// watch subscription on each, so polling, background retries and dependent recomputation
/// treat them as active (§6.1, §6.2, §15.2). Only Signals of exactly those queries (this
/// Principal's authorization partition and these parameters) count; other partitions of the
/// same Instances are ignored.
///
/// Intermediate values coalesce: renders are rate limited and identical renders are not
/// re-sent. Query values from the Signal fan-in are never forwarded; only their freshness
/// metadata (`status`, `updated_at`, `version`) is reported per upstream Instance in `signals`.
///
/// Read authority is re-verified on every Generation change; when it no longer holds, the
/// Dashboard is removed from `dashboards` (stopping its State Signals), an error is sent and the
/// subscription ends.
pub(crate) async fn run_dashboard_subscription(
    services: AppServices,
    principal: dash_core::Principal,
    dashboard_id: String,
    out_tx: mpsc::Sender<Value>,
    dashboards: DashboardSet,
) {
    let mut state_rx = services.engine().signals.subscribe_state();
    let mut activity_rx = services.engine().signals.subscribe_query_activity();
    let mut generation = services.graph().generation_id;
    // Watch subscriptions on every query this Dashboard's renders read (see above).
    let mut observed: HashMap<String, watch::Receiver<Option<QueryEnvelope>>> = HashMap::new();
    let mut seen: HashMap<String, (u64, QueryStatus)> = HashMap::new();
    let mut freshness: BTreeMap<String, Value> = BTreeMap::new();
    let mut last_sent: Option<(String, String)> = None;
    let mut last_error: Option<String> = None;
    let mut pending: Option<&'static str> = Some("subscribe");
    let mut last_render: Option<Instant> = None;
    let mut gen_tick = tokio::time::interval(GENERATION_CHECK_INTERVAL);
    gen_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if let Some(reason) = pending.take() {
            if let Some(t) = last_render {
                let since = t.elapsed();
                if since < MIN_DASHBOARD_RENDER_INTERVAL {
                    tokio::time::sleep(MIN_DASHBOARD_RENDER_INTERVAL - since).await;
                }
            }
            last_render = Some(Instant::now());
            let rendered = services
                .dashboard_get_for_ui(&dashboard_id, &principal)
                .await;
            // Register as a consumer of what this render read (also after a failed render: the
            // dependencies recorded up to the failure are retried in the background).
            let keys = dashboard_observed_query_keys(&services, &dashboard_id, &principal);
            let added = sync_observed_queries(&services, &mut observed, keys);
            match rendered {
                Ok(data) => {
                    last_error = None;
                    let data_txt = data.to_string();
                    let fresh_txt = serde_json::to_string(&freshness).unwrap_or_default();
                    let changed = last_sent
                        .as_ref()
                        .map(|(d, f)| d != &data_txt || f != &fresh_txt)
                        .unwrap_or(true);
                    if changed {
                        let rendered_generation = data
                            .pointer("/rendered/generation")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(generation);
                        let msg = json!({
                            "type": "dashboard_render",
                            "protocol": DASHBOARD_RENDER_PROTOCOL,
                            "dashboard_id": dashboard_id,
                            "generation": rendered_generation,
                            "reason": reason,
                            "signals": freshness,
                            "data": data,
                        });
                        if out_tx.send(msg).await.is_err() {
                            break;
                        }
                        last_sent = Some((data_txt, fresh_txt));
                    }
                }
                Err(err) => {
                    if last_error.as_deref() != Some(err.as_str()) {
                        let msg = json!({
                            "type": "error",
                            "code": "render_failed",
                            "dashboard_id": dashboard_id,
                            "message": err,
                        });
                        if out_tx.send(msg).await.is_err() {
                            break;
                        }
                        last_error = Some(err);
                    }
                    last_sent = None;
                    if added {
                        // The failing queries ran before this subscription consumed them, so no
                        // background retry was scheduled for them: render once more as a consumer.
                        pending = Some("query");
                        continue;
                    }
                }
            }
        }

        tokio::select! {
            r = state_rx.recv() => match r {
                Ok(sig) => {
                    if sig.scope == "dashboard" && sig.owner == dashboard_id {
                        pending = Some("state");
                    }
                }
                Err(RecvError::Lagged(_)) => pending = Some("resync"),
                Err(RecvError::Closed) => break,
            },
            r = activity_rx.recv() => match r {
                Ok(env) => {
                    if observed.contains_key(&env.query_key) {
                        let marker = (env.version, env.status);
                        if seen.get(&env.query_key) != Some(&marker) {
                            seen.insert(env.query_key.clone(), marker);
                            freshness.insert(
                                env.instance.clone(),
                                json!({
                                    "status": env.status,
                                    "updated_at": env.updated_at,
                                    "version": env.version,
                                    "generation": env.generation,
                                }),
                            );
                            pending = Some("query");
                        }
                    }
                }
                Err(RecvError::Lagged(_)) => pending = Some("resync"),
                Err(RecvError::Closed) => break,
            },
            _ = gen_tick.tick() => {
                let graph = services.graph();
                let gid = graph.generation_id;
                if gid != generation {
                    generation = gid;
                    // Read authority depends on the Generation's Adapter placements (§7.1).
                    if let Err(err) =
                        dash_core::state::authorize_dashboard_read(&principal, &graph, &dashboard_id)
                    {
                        if let Ok(mut d) = dashboards.write() {
                            d.remove(&dashboard_id);
                        }
                        let _ = out_tx
                            .send(json!({
                                "type": "error",
                                "code": err.code(),
                                "dashboard_id": dashboard_id,
                                "message": err.to_string(),
                            }))
                            .await;
                        break;
                    }
                    if let Ok(mut d) = dashboards.write() {
                        if let Some(g) = d.get_mut(&dashboard_id) {
                            *g = gid;
                        }
                    }
                    let upstream = services.dashboard_upstream_instances(&dashboard_id);
                    seen.clear();
                    freshness.retain(|k, _| upstream.contains(k));
                    pending = Some("generation");
                }
            },
            _ = out_tx.closed() => break,
        }
    }
}

/// Query keys `dashboard_id`'s Adapter renders for `principal` read in the active Generation:
/// the dependencies recorded for its render keys, transitively.
fn dashboard_observed_query_keys(
    services: &AppServices,
    dashboard_id: &str,
    principal: &Principal,
) -> BTreeSet<String> {
    let queries = &services.engine().queries;
    let mut stack: Vec<String> = queries
        .dashboard_render_keys(dashboard_id, principal)
        .iter()
        .flat_map(|rk| queries.recorded_dependencies(rk))
        .collect();
    let mut keys = BTreeSet::new();
    while let Some(k) = stack.pop() {
        if keys.insert(k.clone()) {
            stack.extend(queries.recorded_dependencies(&k));
        }
    }
    keys
}

/// Hold a watch subscription on exactly `keys` (dropping the others). Returns whether a key was
/// added.
fn sync_observed_queries(
    services: &AppServices,
    observed: &mut HashMap<String, watch::Receiver<Option<QueryEnvelope>>>,
    keys: BTreeSet<String>,
) -> bool {
    observed.retain(|k, _| keys.contains(k));
    let mut added = false;
    for k in keys {
        if let std::collections::hash_map::Entry::Vacant(v) = observed.entry(k) {
            let rx = services.engine().signals.subscribe_watch(v.key());
            v.insert(rx);
            added = true;
        }
    }
    added
}

#[cfg(test)]
mod dashboard_subscription_tests {
    use super::*;
    use dash_core::Principal;
    use dash_git::DraftManager;
    use dash_luau::ResourceBudget;
    use dash_store::SqliteStore;
    use std::path::Path;
    use tempfile::TempDir;

    async fn boot_services() -> (TempDir, AppServices) {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
        dash_git::packages::copy_tree_excluding_git(&fixture, &ws).unwrap();
        let store = SqliteStore::open(&data).unwrap();
        let mgr =
            DraftManager::initialize_with_mocks(ws, data, store, ResourceBudget::default(), true)
                .await
                .unwrap();
        (tmp, AppServices::new(mgr, vec![]))
    }

    fn dashboard_set(id: &str, services: &AppServices) -> DashboardSet {
        Arc::new(RwLock::new(HashMap::from([(
            id.to_string(),
            services.graph().generation_id,
        )])))
    }

    fn spawn_admin_subscription(
        services: &AppServices,
    ) -> (tokio::task::JoinHandle<()>, mpsc::Receiver<Value>) {
        let (tx, rx) = mpsc::channel::<Value>(64);
        let task = tokio::spawn(run_dashboard_subscription(
            services.clone(),
            Principal::administrator(),
            "engineering".into(),
            tx,
            dashboard_set("engineering", services),
        ));
        (task, rx)
    }

    /// Cached `open_prs` entries of the Administrator's authorization partition.
    fn admin_open_prs_entries(services: &AppServices) -> Vec<(String, u64)> {
        let admin = Principal::administrator();
        services
            .engine()
            .queries
            .cached_entries_for_instance("open_prs")
            .into_iter()
            .filter(|(_, e)| e.auth_fingerprint == admin.authorization_fingerprint())
            .map(|(k, e)| (k, e.version))
            .collect()
    }

    /// Everything received within `wait`.
    async fn drain(rx: &mut mpsc::Receiver<Value>, wait: Duration) -> Vec<Value> {
        let mut out = Vec::new();
        while let Ok(Some(m)) = tokio::time::timeout(wait, rx.recv()).await {
            out.push(m);
        }
        out
    }

    #[tokio::test]
    async fn dashboard_subscription_is_a_signal_consumer_so_polls_refresh_what_it_shows() {
        let (_tmp, services) = boot_services().await;
        let (task, mut rx) = spawn_admin_subscription(&services);
        next_render(&mut rx).await;
        let _ = drain(&mut rx, Duration::from_millis(600)).await;

        let before = admin_open_prs_entries(&services);
        assert!(!before.is_empty(), "the render computed open_prs");
        for (qk, _) in &before {
            assert!(
                services.engine().signals.subscriber_count(qk) > 0,
                "the dashboard subscription must consume {qk}"
            );
        }

        // A scheduled poll refreshes the Administrator's partition (not only keep-warm ones)...
        services.run_polls_at(chrono::Utc::now().timestamp()).await;
        let after = admin_open_prs_entries(&services);
        for (qk, v0) in &before {
            let v1 = after
                .iter()
                .find(|(k, _)| k == qk)
                .map(|(_, v)| *v)
                .unwrap();
            assert!(
                v1 > *v0,
                "poll skipped the dashboard's open_prs partition {qk}"
            );
        }
        // ...and the refreshed value is pushed as a query-driven render.
        let target = after.iter().map(|(_, v)| *v).max().unwrap();
        let mut pushed = false;
        for _ in 0..8 {
            let msg = next_render(&mut rx).await;
            if msg["reason"] == "query"
                && msg["signals"]["open_prs"]["version"].as_u64() >= Some(target)
            {
                pushed = true;
                break;
            }
        }
        assert!(pushed, "polled value was not pushed to the dashboard");

        // Ending the subscription releases its consumers.
        task.abort();
        let _ = task.await;
        for (qk, _) in &before {
            assert_eq!(services.engine().signals.subscriber_count(qk), 0);
        }
    }

    #[tokio::test]
    async fn dashboard_freshness_ignores_other_partitions_of_upstream_instances() {
        let (_tmp, services) = boot_services().await;
        let (task, mut rx) = spawn_admin_subscription(&services);
        next_render(&mut rx).await;
        let _ = drain(&mut rx, Duration::from_millis(600)).await;

        // Another Principal's / parameter set's partition of open_prs fails.
        let generation = services.graph().generation_id;
        services
            .engine()
            .signals
            .publish_query(QueryEnvelope::error(
                "open_prs",
                "someone-elses-partition",
                generation,
                None,
                "provider down".into(),
            ));
        let msgs = drain(&mut rx, Duration::from_millis(800)).await;
        assert!(
            msgs.iter().all(|m| m["type"] != "dashboard_render"),
            "foreign partition triggered a re-render / freshness change: {msgs:?}"
        );

        // The dashboard's own partition still drives freshness.
        let (qk, _) = admin_open_prs_entries(&services).remove(0);
        services
            .engine()
            .signals
            .publish_query(QueryEnvelope::error(
                "open_prs",
                &qk,
                generation,
                None,
                "provider down".into(),
            ));
        let mut saw = false;
        for _ in 0..4 {
            let msg = next_render(&mut rx).await;
            if msg["signals"]["open_prs"]["status"] == "error" {
                saw = true;
                break;
            }
        }
        assert!(saw, "own partition's Signal was not reported");
        task.abort();
    }

    #[tokio::test]
    async fn dashboard_subscription_ends_when_a_generation_revokes_read_authority() {
        let (_tmp, services) = boot_services().await;
        let adapters: std::collections::BTreeSet<String> = services.graph().dashboards
            ["engineering"]
            .adapters
            .iter()
            .map(|p| p.adapter_instance.clone())
            .collect();
        let agent = Principal {
            id: "agent-ws".into(),
            kind: dash_core::PrincipalKind::Agent,
            ceiling: dash_core::CapabilityCeiling {
                allowed_providers: adapters,
                ..Default::default()
            },
        };
        let set = dashboard_set("engineering", &services);
        let (tx, mut rx) = mpsc::channel::<Value>(64);
        let task = tokio::spawn(run_dashboard_subscription(
            services.clone(),
            agent,
            "engineering".into(),
            tx,
            Arc::clone(&set),
        ));
        let _ = drain(&mut rx, Duration::from_millis(600)).await;

        // A new Generation places an Adapter the Agent may not read.
        let mut g = (*services.graph()).clone();
        let mut extra = g.dashboards["engineering"].adapters[0].clone();
        extra.adapter_instance = "open_prs".into();
        g.dashboards
            .get_mut("engineering")
            .unwrap()
            .adapters
            .push(extra);
        g.generation_id += 1;
        services.engine().graph.store(Arc::new(g));

        let msgs = drain(&mut rx, Duration::from_millis(1500)).await;
        assert!(
            msgs.iter()
                .any(|m| m["type"] == "error" && m["code"] == "forbidden"),
            "{msgs:?}"
        );
        assert!(set.read().unwrap().is_empty(), "dashboard still forwarded");
        assert!(task.is_finished(), "subscription kept running");
    }

    async fn next_render(rx: &mut mpsc::Receiver<Value>) -> Value {
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(20), rx.recv())
                .await
                .expect("timed out waiting for dashboard_render")
                .expect("subscription channel closed");
            if msg["type"] == "dashboard_render" {
                return msg;
            }
            assert_ne!(msg["type"], "error", "subscription error: {msg}");
        }
    }

    #[tokio::test]
    async fn dashboard_subscription_pushes_rerender_on_dashboard_state_and_query_signals() {
        let (_tmp, services) = boot_services().await;
        let (tx, mut rx) = mpsc::channel::<Value>(64);
        let task = tokio::spawn(run_dashboard_subscription(
            services.clone(),
            Principal::administrator(),
            "engineering".into(),
            tx,
            dashboard_set("engineering", &services),
        ));

        let first = next_render(&mut rx).await;
        assert_eq!(first["protocol"], DASHBOARD_RENDER_PROTOCOL);
        assert_eq!(first["reason"], "subscribe");
        assert_eq!(first["dashboard_id"], "engineering");
        assert_eq!(
            first["generation"],
            json!(services.graph().generation_id),
            "render carries the active Generation"
        );

        // Another Client changes Dashboard State -> a new render is pushed without any request
        // from this socket.
        let gen_id = services.graph().generation_id;
        services
            .engine()
            .state
            .set(
                "dashboard",
                "engineering",
                "selected_repository",
                1,
                gen_id,
                json!("acme/payments"),
                None,
            )
            .unwrap();
        let mut saw_state = false;
        for _ in 0..6 {
            let msg = next_render(&mut rx).await;
            if msg["data"]["rendered"]["dashboard_state"]["selected_repository"] == "acme/payments"
            {
                saw_state = true;
                break;
            }
        }
        assert!(saw_state, "Dashboard State change was not pushed");

        // A Query Signal of an upstream Instance (e.g. a poll/invalidation refresh) re-renders and
        // reports freshness metadata for that Instance.
        services
            .engine()
            .queries
            .invalidate_instances(&["linked_prs".to_string()])
            .await;
        let mut saw_query = false;
        for _ in 0..6 {
            let msg = next_render(&mut rx).await;
            if msg["signals"]["linked_prs"]["status"].is_string() {
                saw_query = true;
                break;
            }
        }
        assert!(
            saw_query,
            "upstream Query Signal did not trigger a pushed render"
        );
        task.abort();
    }

    #[tokio::test]
    async fn dashboard_upstream_instances_follow_adapter_bindings_transitively() {
        let (_tmp, services) = boot_services().await;
        let up = services.dashboard_upstream_instances("engineering");
        for id in [
            "pr_table_adapter",
            "pr_trend_adapter",
            "linked_prs",
            "open_prs",
        ] {
            assert!(up.contains(id), "missing upstream instance {id}: {up:?}");
        }
        assert!(services.dashboard_upstream_instances("nope").is_empty());
    }
}
