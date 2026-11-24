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
                match services.dashboard_get(&dashboard_id, &principal).await {
                    Ok(data) => {
                        let _ = out_tx
                            .send(json!({ "type": "dashboard_render", "dashboard_id": dashboard_id, "data": data }))
                            .await;
                    }
                    Err(err) => {
                        let _ = out_tx.try_send(json!({ "type": "error", "message": err }));
                    }
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
    state_task.abort();
    writer_task.abort();
}
