//! WebSocket endpoint (`/ws`) delivering coalesced Signal streams and validating Widget Requests (`dash.widget/1`).
//!
//! Protocol (client -> server, `type` tagged):
//! - `subscribe_query {instance, params}` / `unsubscribe_query {query_key}` /
//!   `resync_query {query_key}` (request a fresh snapshot)
//! - `subscribe_dashboard {dashboard_id}` / `unsubscribe_dashboard {dashboard_id}`
//! - `set_dashboard_state {dashboard_id, key, value, expected_version?}`
//! - `widget_request {...}`
//!
//! Delivery guarantees (§6.2, §7.1):
//! - A query subscription is registered *before* the initial computation, so no update is lost
//!   between the first snapshot and the stream, and it stays registered when that computation
//!   fails (later successes are delivered). Subscriptions are deduplicated per socket by
//!   `query_key` and capped.
//! - Deltas are only forwarded when their `base_version` matches the last version this socket
//!   delivered. Lagging behind the bounded stream, or a base mismatch, makes the socket skip to
//!   the latest snapshot (`resync: true`) instead of silently stopping or sending broken deltas.
//! - State Signals are filtered per socket: only Dashboard State of dashboards this socket
//!   subscribed to (after an authority check) is forwarded. Instance State never leaves the
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
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, Notify};

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
}

type DashboardSet = Arc<RwLock<HashSet<String>>>;

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

    let dashboards: DashboardSet = Arc::new(RwLock::new(HashSet::new()));
    let state_task = tokio::spawn(forward_state_signals(
        services.clone(),
        services.engine().signals.subscribe_state(),
        Arc::clone(&dashboards),
        out_tx.clone(),
    ));

    let mut subs: HashMap<String, QuerySubscription> = HashMap::new();
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
                subscribe_dashboard(&services, &principal, &out_tx, &dashboards, dashboard_id).await;
            }
            WsClientMsg::UnsubscribeDashboard { dashboard_id } => {
                let existed = dashboards
                    .write()
                    .map(|mut d| d.remove(&dashboard_id))
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
                match services.dispatch_widget_request(envelope, &principal).await {
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
    let handle = tokio::spawn(forward_query_signals(
        services.clone(),
        principal.clone(),
        instance,
        params,
        qk.clone(),
        rx,
        out.clone(),
        cursor,
        Arc::clone(&resync),
    ));
    subs.insert(qk, QuerySubscription { handle, resync });
}

/// What this socket last delivered for one query.
#[derive(Default)]
struct DeliveryCursor {
    /// Last version delivered (`None` until a valued snapshot was sent).
    version: Option<u64>,
    /// (version, status) of the last delivered snapshot, unless a delta was delivered since.
    last_snapshot: Option<(u64, QueryStatus)>,
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
}

#[allow(clippy::too_many_arguments)]
async fn forward_query_signals(
    services: AppServices,
    principal: Principal,
    instance: String,
    params: Value,
    qk: String,
    mut rx: broadcast::Receiver<QueryEnvelope>,
    out: mpsc::Sender<Value>,
    mut cursor: DeliveryCursor,
    resync: Arc<Notify>,
) {
    loop {
        let need_resync = tokio::select! {
            r = rx.recv() => match r {
                Ok(env) => match env.kind {
                    EnvelopeKind::Snapshot => {
                        if cursor.is_duplicate_snapshot(&env) {
                            continue;
                        }
                        let msg = cursor.snapshot_msg(&env, false);
                        if out.send(msg).await.is_err() {
                            return;
                        }
                        false
                    }
                    EnvelopeKind::Delta => match (cursor.version, env.base_version) {
                        (Some(have), Some(base)) if base == have => {
                            cursor.version = Some(env.version);
                            cursor.last_snapshot = None;
                            if out
                                .send(json!({ "type": "query_signal", "envelope": env }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            false
                        }
                        // Already covered by a newer snapshot this socket delivered.
                        (Some(have), Some(base)) if base < have => false,
                        // Gap, or no base to apply onto: skip to the latest snapshot.
                        _ => true,
                    },
                },
                // Fell behind the bounded stream: intermediate values are skipped (§6.2).
                Err(RecvError::Lagged(_)) => true,
                Err(RecvError::Closed) => return,
            },
            _ = resync.notified() => true,
        };
        if need_resync {
            // Drop everything buffered; only values published from now on are streamed.
            rx = rx.resubscribe();
            let latest = match services.engine().signals.latest_snapshot(&qk) {
                Some(env) => Ok(env),
                None => {
                    services
                        .provider_query(&instance, params.clone(), &principal, false)
                        .await
                }
            };
            let msg = match latest {
                Ok(env) => cursor.snapshot_msg(&env, true),
                Err(err) => json!({ "type": "error", "code": "query_failed", "query_key": qk, "message": err }),
            };
            if out.send(msg).await.is_err() {
                return;
            }
        }
    }
}

async fn subscribe_dashboard(
    services: &AppServices,
    principal: &Principal,
    out: &mpsc::Sender<Value>,
    dashboards: &DashboardSet,
    dashboard_id: String,
) {
    let graph = services.graph();
    if let Err(err) =
        dash_core::state::authorize_dashboard_read(principal, &graph, &dashboard_id)
    {
        let _ = out
            .send(json!({ "type": "error", "code": err.code(), "dashboard_id": dashboard_id, "message": err.to_string() }))
            .await;
        return;
    }
    let admitted = match dashboards.write() {
        Ok(mut d) => {
            if d.contains(&dashboard_id) || d.len() < MAX_DASHBOARD_SUBSCRIPTIONS_PER_SOCKET {
                d.insert(dashboard_id.clone());
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
        return;
    }
    match services.dashboard_get(&dashboard_id, principal).await {
        Ok(data) => {
            let _ = out
                .send(json!({ "type": "dashboard_render", "dashboard_id": dashboard_id, "data": data }))
                .await;
        }
        Err(err) => {
            let _ = out
                .send(json!({ "type": "error", "code": "render_failed", "dashboard_id": dashboard_id, "message": err }))
                .await;
        }
    }
}

/// Whether a State Signal may be delivered to a socket subscribed to `dashboards`.
fn state_signal_visible(sig: &dash_core::StateSignalPayload, dashboards: &DashboardSet) -> bool {
    sig.scope == "dashboard"
        && dashboards
            .read()
            .map(|d| d.contains(&sig.owner))
            .unwrap_or(false)
}

async fn forward_state_signals(
    services: AppServices,
    mut rx: broadcast::Receiver<dash_core::StateSignalPayload>,
    dashboards: DashboardSet,
    out: mpsc::Sender<Value>,
) {
    loop {
        match rx.recv().await {
            Ok(sig) => {
                if !state_signal_visible(&sig, &dashboards) {
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
                    .map(|d| d.iter().cloned().collect())
                    .unwrap_or_default();
                for id in ids {
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
        let set: DashboardSet = Arc::new(RwLock::new(HashSet::from(["eng".to_string()])));
        assert!(state_signal_visible(&sig("dashboard", "eng"), &set));
        assert!(!state_signal_visible(&sig("dashboard", "ops"), &set));
        assert!(!state_signal_visible(&sig("instance", "eng"), &set));
        assert!(!state_signal_visible(&sig("client", "eng"), &set));
    }

    #[test]
    fn delivery_cursor_dedups_identical_snapshots_only() {
        let mut c = DeliveryCursor::default();
        let env = QueryEnvelope::snapshot("i", "q", 1, 3, QueryStatus::Fresh, None, json!([]), None);
        let _ = c.snapshot_msg(&env, false);
        assert!(c.is_duplicate_snapshot(&env));
        let stale = QueryEnvelope::snapshot("i", "q", 1, 3, QueryStatus::Stale, None, json!([]), None);
        assert!(!c.is_duplicate_snapshot(&stale));
        assert_eq!(c.version, Some(3));
    }
}
