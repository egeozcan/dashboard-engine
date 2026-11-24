//! WebSocket endpoint (`/ws`) delivering coalesced Signal streams and validating Widget Requests (`dash.widget/1`).

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
use dash_core::WidgetRequestEnvelope;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;
use tokio::sync::mpsc;
use dash_core::QueryStatus;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsClientMsg {
    SubscribeQuery {
        instance: String,
        #[serde(default)]
        params: Value,
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
    ws.on_upgrade(move |socket| handle_ws_socket(socket, services, principal))
        .into_response()
}

async fn handle_ws_socket(
    socket: WebSocket,
    services: AppServices,
    principal: dash_core::Principal,
) {
    let (mut sender, mut receiver) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<Value>(64);

    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let txt = serde_json::to_string(&msg).unwrap_or_default();
            if sender.send(Message::Text(txt.into())).await.is_err() {
                break;
            }
        }
    });

    let mut state_rx = services.engine().signals.subscribe_state();
    let out_state = out_tx.clone();
    let state_task = tokio::spawn(async move {
        while let Ok(sig) = state_rx.recv().await {
            let _ = out_state
                .try_send(json!({ "type": "state_signal", "payload": sig }));
        }
    });

    let mut sub_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut dash_subs: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

    while let Some(Ok(frame)) = receiver.next().await {
        let txt = match frame {
            Message::Text(t) => t.to_string(),
            Message::Close(_) => break,
            _ => continue,
        };
        let Ok(parsed) = serde_json::from_str::<WsClientMsg>(&txt) else {
            let _ = out_tx
                .try_send(json!({ "type": "error", "message": "invalid ws message payload" }));
            continue;
        };
        match parsed {
            WsClientMsg::SubscribeQuery { instance, params } => {
                match services
                    .provider_query(&instance, params, &principal, false)
                    .await
                {
                    Ok(env) => {
                        let qk = env.query_key.clone();
                        let _ = out_tx.send(json!({ "type": "query_signal", "envelope": env })).await;
                        let mut rx = services.engine().signals.subscribe_stream(&qk);
                        let tx_clone = out_tx.clone();
                        sub_handles.push(tokio::spawn(async move {
                            while let Ok(next_env) = rx.recv().await {
                                let _ = tx_clone.try_send(
                                    json!({ "type": "query_signal", "envelope": next_env }),
                                );
                            }
                        }));
                    }
                    Err(err) => {
                        let _ = out_tx.try_send(json!({ "type": "error", "message": err }));
                    }
                }
            }
            WsClientMsg::SubscribeDashboard { dashboard_id } => {
                // Live subscription: initial render now, then re-render pushes whenever the
                // Dashboard's State, an upstream Query Signal or the active Generation changes.
                if let Some(prev) = dash_subs.remove(&dashboard_id) {
                    prev.abort();
                }
                let handle = tokio::spawn(run_dashboard_subscription(
                    services.clone(),
                    principal.clone(),
                    dashboard_id.clone(),
                    out_tx.clone(),
                ));
                dash_subs.insert(dashboard_id, handle);
            }
            WsClientMsg::UnsubscribeDashboard { dashboard_id } => {
                if let Some(prev) = dash_subs.remove(&dashboard_id) {
                    prev.abort();
                }
            }
            WsClientMsg::SetDashboardState {
                dashboard_id,
                key,
                value,
            } => {
                let gen_id = services.graph().generation_id;
                match services.engine().state.set(
                    "dashboard",
                    &dashboard_id,
                    &key,
                    1,
                    gen_id,
                    value,
                    None,
                ) {
                    Ok(rec) => {
                        let _ = out_tx.try_send(json!({ "type": "state_updated", "record": rec }));
                    }
                    Err(err) => {
                        let _ = out_tx
                            .try_send(json!({ "type": "error", "message": err.to_string() }));
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

    for h in sub_handles {
        h.abort();
    }
    for (_, h) in dash_subs {
        h.abort();
    }
    state_task.abort();
    writer_task.abort();
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
/// - a Query Signal of an Instance upstream of the Dashboard's Adapters carries a new
///   version or status (`reason: "query"`),
/// - a new Generation is activated (`reason: "generation"`), or
/// - a Signal stream lagged and intermediate values were skipped (`reason: "resync"`).
///
/// Intermediate values coalesce: renders are rate limited and identical renders are not
/// re-sent. Query values from the Signal fan-in are never forwarded; only their freshness
/// metadata (`status`, `updated_at`, `version`) is reported per upstream Instance in `signals`.
pub(crate) async fn run_dashboard_subscription(
    services: AppServices,
    principal: dash_core::Principal,
    dashboard_id: String,
    out_tx: mpsc::Sender<Value>,
) {
    let mut state_rx = services.engine().signals.subscribe_state();
    let mut activity_rx = services.engine().signals.subscribe_query_activity();
    let mut generation = services.graph().generation_id;
    let mut upstream = services.dashboard_upstream_instances(&dashboard_id);
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
            match services.dashboard_get_for_ui(&dashboard_id, &principal).await {
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
                            "dashboard_id": dashboard_id,
                            "message": err,
                        });
                        if out_tx.send(msg).await.is_err() {
                            break;
                        }
                        last_error = Some(err);
                    }
                    last_sent = None;
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
                    if upstream.contains(&env.instance) {
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
                let gid = services.graph().generation_id;
                if gid != generation {
                    generation = gid;
                    upstream = services.dashboard_upstream_instances(&dashboard_id);
                    seen.clear();
                    freshness.retain(|k, _| upstream.contains(k));
                    pending = Some("generation");
                }
            },
            _ = out_tx.closed() => break,
        }
    }
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
            if msg["data"]["rendered"]["dashboard_state"]["selected_repository"] == "acme/payments" {
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
        assert!(saw_query, "upstream Query Signal did not trigger a pushed render");
        task.abort();
    }

    #[tokio::test]
    async fn dashboard_upstream_instances_follow_adapter_bindings_transitively() {
        let (_tmp, services) = boot_services().await;
        let up = services.dashboard_upstream_instances("engineering");
        for id in ["pr_table_adapter", "pr_trend_adapter", "linked_prs", "open_prs"] {
            assert!(up.contains(id), "missing upstream instance {id}: {up:?}");
        }
        assert!(services.dashboard_upstream_instances("nope").is_empty());
    }
}
