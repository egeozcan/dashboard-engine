//! WebSocket Signal delivery and mediated Dashboard State access (§6.2, §7.1, §8).

use dash_core::{CapabilityCeiling, Principal, QueryEnvelope, QueryStatus};
use dash_git::DraftManager;
use dash_luau::ResourceBudget;
use dash_server::{build_router, hash_password, AppServices};
use dash_store::SqliteStore;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const ADMIN_TOKEN: &str = "adm_cli_ws_test_token";

async fn boot() -> (
    TempDir,
    AppServices,
    SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&fixture, &ws).unwrap();
    let store = SqliteStore::open(&data).unwrap();
    store
        .set_admin_credentials("admin", &hash_password("pass1234").unwrap())
        .unwrap();
    let mgr = DraftManager::initialize(ws, data, store, ResourceBudget::default())
        .await
        .unwrap();
    let services = AppServices::new(mgr, vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
    std::fs::write(&services.admin_cli_token_path, ADMIN_TOKEN).unwrap();
    // Use Preview mocks so no real network is touched.
    let mut g = (*services.graph()).clone();
    g.is_preview = true;
    services.engine().graph.store(std::sync::Arc::new(g));

    let app = build_router(services.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (tmp, services, addr, h)
}

async fn connect(addr: SocketAddr, token: &str) -> Ws {
    let mut req = format!("ws://{addr}/ws").into_client_request().unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string().into())).await.unwrap();
}

async fn next_json(ws: &mut Ws, wait: Duration) -> Option<Value> {
    loop {
        let msg = tokio::time::timeout(wait, ws.next()).await.ok()??.ok()?;
        if let Message::Text(t) = msg {
            return Some(serde_json::from_str(t.as_str()).unwrap());
        }
    }
}

/// Read until a message of `ty` arrives; returns it and everything skipped before it.
async fn expect_type(ws: &mut Ws, ty: &str) -> (Value, Vec<Value>) {
    let mut skipped = Vec::new();
    loop {
        let v = next_json(ws, Duration::from_secs(10))
            .await
            .unwrap_or_else(|| panic!("timed out waiting for '{ty}'; saw {skipped:?}"));
        if v["type"] == ty {
            return (v, skipped);
        }
        skipped.push(v);
    }
}

/// Everything received within `wait`.
async fn drain(ws: &mut Ws, wait: Duration) -> Vec<Value> {
    let mut out = Vec::new();
    while let Some(v) = next_json(ws, wait).await {
        out.push(v);
    }
    out
}

fn agent(services: &AppServices, providers: &[&str]) -> String {
    let (_rec, token) = services
        .agent_create(
            "ws-agent",
            CapabilityCeiling {
                allowed_providers: providers
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<BTreeSet<_>>(),
                ..Default::default()
            },
            None,
            &Principal::administrator(),
        )
        .unwrap();
    token
}

#[tokio::test]
async fn state_signals_are_filtered_per_principal_and_subscription() {
    let (_tmp, services, addr, h) = boot().await;
    let state = services.engine().state.clone();

    // An Agent with an empty ceiling sees no State at all and cannot subscribe to the dashboard.
    let blind_token = agent(&services, &[]);
    let mut blind = connect(addr, &blind_token).await;
    send(
        &mut blind,
        json!({"type": "subscribe_dashboard", "dashboard_id": "engineering"}),
    )
    .await;
    let (err, _) = expect_type(&mut blind, "error").await;
    assert_eq!(err["code"], "forbidden");

    // The Administrator subscribes to `engineering` only.
    let mut admin = connect(addr, ADMIN_TOKEN).await;
    send(
        &mut admin,
        json!({"type": "subscribe_dashboard", "dashboard_id": "engineering"}),
    )
    .await;
    expect_type(&mut admin, "dashboard_render").await;

    state
        .set(
            "instance",
            "open_prs",
            "seen_pr_states:acme/platform",
            1,
            1,
            json!({"42": "open"}),
            None,
        )
        .unwrap();
    state
        .set(
            "dashboard",
            "other_dashboard",
            "selected_repository",
            1,
            1,
            json!("x"),
            None,
        )
        .unwrap();
    state
        .set("client", "client-123", "sort", 1, 1, json!("asc"), None)
        .unwrap();
    state
        .set(
            "dashboard",
            "engineering",
            "selected_repository",
            1,
            1,
            json!("acme/payments"),
            None,
        )
        .unwrap();

    let (sig, skipped) = expect_type(&mut admin, "state_signal").await;
    assert!(skipped.iter().all(|m| m["type"] != "state_signal"));
    assert_eq!(sig["payload"]["owner"], "engineering");
    assert_eq!(sig["payload"]["value"], "acme/payments");
    let rest = drain(&mut admin, Duration::from_millis(300)).await;
    assert!(rest.iter().all(|m| m["type"] != "state_signal"), "{rest:?}");

    let leaked = drain(&mut blind, Duration::from_millis(300)).await;
    assert!(
        leaked.iter().all(|m| m["type"] != "state_signal"),
        "{leaked:?}"
    );

    // After unsubscribing, the dashboard's State no longer arrives.
    send(
        &mut admin,
        json!({"type": "unsubscribe_dashboard", "dashboard_id": "engineering"}),
    )
    .await;
    expect_type(&mut admin, "unsubscribed").await;
    state
        .set(
            "dashboard",
            "engineering",
            "selected_repository",
            1,
            1,
            json!("acme/platform"),
            None,
        )
        .unwrap();
    let rest = drain(&mut admin, Duration::from_millis(300)).await;
    assert!(rest.iter().all(|m| m["type"] != "state_signal"), "{rest:?}");
    h.abort();
}

#[tokio::test]
async fn ws_dashboard_state_writes_are_authorized_and_cas_checked() {
    let (_tmp, services, addr, h) = boot().await;
    let mut admin = connect(addr, ADMIN_TOKEN).await;
    // Rendering seeds `selected_repository` at version 1.
    send(
        &mut admin,
        json!({"type": "subscribe_dashboard", "dashboard_id": "engineering"}),
    )
    .await;
    let (render, _) = expect_type(&mut admin, "dashboard_render").await;
    assert_eq!(
        render["data"]["rendered"]["dashboard_state_versions"]["selected_repository"],
        1
    );

    send(
        &mut admin,
        json!({"type": "set_dashboard_state", "dashboard_id": "engineering",
        "key": "selected_repository", "value": "acme/payments", "expected_version": 1}),
    )
    .await;
    let (ok, _) = expect_type(&mut admin, "state_updated").await;
    assert_eq!(ok["record"]["version"], 2);

    // Stale CAS version is a conflict, not a silent overwrite.
    send(
        &mut admin,
        json!({"type": "set_dashboard_state", "dashboard_id": "engineering",
        "key": "selected_repository", "value": "acme/lost", "expected_version": 1}),
    )
    .await;
    let (err, _) = expect_type(&mut admin, "error").await;
    assert_eq!(err["code"], "conflict");

    for (dashboard, key, code) in [
        ("nope", "selected_repository", "not_found"),
        ("engineering", "undeclared_key", "invalid"),
    ] {
        send(
            &mut admin,
            json!({"type": "set_dashboard_state", "dashboard_id": dashboard,
            "key": key, "value": 1}),
        )
        .await;
        let (err, _) = expect_type(&mut admin, "error").await;
        assert_eq!(err["code"], code);
    }

    // A read-only Agent may not steer shared Dashboard State.
    let reader = agent(&services, &["*"]);
    let mut agent_ws = connect(addr, &reader).await;
    send(
        &mut agent_ws,
        json!({"type": "set_dashboard_state", "dashboard_id": "engineering",
        "key": "selected_repository", "value": "acme/evil"}),
    )
    .await;
    let (err, _) = expect_type(&mut agent_ws, "error").await;
    assert_eq!(err["code"], "forbidden");
    let rec = services
        .engine()
        .state
        .get("dashboard", "engineering", "selected_repository")
        .unwrap()
        .unwrap();
    assert_eq!(rec.value, json!("acme/payments"));
    h.abort();
}

#[tokio::test]
async fn http_dashboard_state_route_is_mediated() {
    let (_tmp, services, addr, h) = boot().await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let admin_auth = format!("Bearer {ADMIN_TOKEN}");
    services
        .dashboard_get("engineering", &Principal::administrator())
        .await
        .unwrap();

    let get: Value = client
        .get(format!("{base}/api/dashboards/engineering/state"))
        .header("authorization", &admin_auth)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let version = get["records"][0]["version"].as_i64().unwrap();

    let post = |id: &str, body: Value, auth: String| {
        client
            .post(format!("{base}/api/dashboards/{id}/state"))
            .header("authorization", auth)
            .json(&body)
            .send()
    };
    let ok = post("engineering", json!({"key": "selected_repository", "value": "acme/payments", "expected_version": version}), admin_auth.clone()).await.unwrap();
    assert_eq!(ok.status(), 200);
    let stale = post(
        "engineering",
        json!({"key": "selected_repository", "value": "acme/x", "expected_version": version}),
        admin_auth.clone(),
    )
    .await
    .unwrap();
    assert_eq!(stale.status(), 409);
    let missing = post(
        "does_not_exist",
        json!({"key": "k", "value": 1}),
        admin_auth.clone(),
    )
    .await
    .unwrap();
    assert_eq!(missing.status(), 404);
    let undeclared = post(
        "engineering",
        json!({"key": "arbitrary", "value": 1}),
        admin_auth.clone(),
    )
    .await
    .unwrap();
    assert_eq!(undeclared.status(), 422);
    let reader = agent(&services, &["*"]);
    let denied = post(
        "engineering",
        json!({"key": "selected_repository", "value": "acme/evil"}),
        format!("Bearer {reader}"),
    )
    .await
    .unwrap();
    assert_eq!(denied.status(), 403);
    let blind = agent(&services, &[]);
    let hidden = client
        .get(format!("{base}/api/dashboards/engineering/state"))
        .header("authorization", format!("Bearer {blind}"))
        .send()
        .await
        .unwrap();
    assert_eq!(hidden.status(), 403);
    h.abort();
}

const PARAMS: &str = r#"{"repository": "acme/platform", "state": "open"}"#;

#[tokio::test]
async fn query_subscriptions_dedup_unsubscribe_and_deliver_deltas() {
    let (_tmp, services, addr, h) = boot().await;
    let params: Value = serde_json::from_str(PARAMS).unwrap();
    let mut ws = connect(addr, ADMIN_TOKEN).await;
    send(
        &mut ws,
        json!({"type": "subscribe_query", "instance": "open_prs", "params": params}),
    )
    .await;
    let (sub, _) = expect_type(&mut ws, "subscribed").await;
    let qk = sub["query_key"].as_str().unwrap().to_string();
    let (first, _) = expect_type(&mut ws, "query_signal").await;
    assert_eq!(first["envelope"]["kind"], "snapshot");
    assert_eq!(first["envelope"]["query_key"], qk.as_str());
    let v0 = first["envelope"]["version"].as_u64().unwrap();

    // Same query again: deduplicated (one forwarder, so updates are not doubled).
    send(
        &mut ws,
        json!({"type": "subscribe_query", "instance": "open_prs", "params": params}),
    )
    .await;
    let (dup, _) = expect_type(&mut ws, "subscribed").await;
    assert_eq!(dup["duplicate"], true);
    let _ = drain(&mut ws, Duration::from_millis(300)).await;
    assert_eq!(services.engine().signals.subscriber_count(&qk), 1);

    let admin = Principal::administrator();
    services
        .engine()
        .queries
        .push_query_value(
            "open_prs",
            &params,
            &admin,
            json!([{"id": "acme/platform#42", "number": 42, "state": "merged"}]),
        )
        .unwrap();
    let msgs = drain(&mut ws, Duration::from_millis(500)).await;
    let signals: Vec<&Value> = msgs
        .iter()
        .filter(|m| m["type"] == "query_signal")
        .collect();
    assert_eq!(signals.len(), 2, "{msgs:?}");
    assert_eq!(signals[0]["envelope"]["kind"], "delta");
    assert_eq!(signals[0]["envelope"]["base_version"].as_u64().unwrap(), v0);
    assert_eq!(signals[1]["envelope"]["kind"], "snapshot");

    send(
        &mut ws,
        json!({"type": "unsubscribe_query", "query_key": qk}),
    )
    .await;
    expect_type(&mut ws, "unsubscribed").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(services.engine().signals.subscriber_count(&qk), 0);
    services
        .engine()
        .queries
        .push_query_value("open_prs", &params, &admin, json!([]))
        .unwrap();
    let msgs = drain(&mut ws, Duration::from_millis(300)).await;
    assert!(msgs.iter().all(|m| m["type"] != "query_signal"), "{msgs:?}");
    h.abort();
}

#[tokio::test]
async fn failed_initial_query_stays_subscribed() {
    let (_tmp, services, addr, h) = boot().await;
    let params: Value = serde_json::from_str(PARAMS).unwrap();
    services
        .engine()
        .telemetry
        .inject_failure(dash_core::FailureInjectionRule {
            instance_id: "open_prs".into(),
            mode: "error".into(),
            message: "upstream down".into(),
            remaining: Some(1),
        });
    let mut ws = connect(addr, ADMIN_TOKEN).await;
    send(
        &mut ws,
        json!({"type": "subscribe_query", "instance": "open_prs", "params": params}),
    )
    .await;
    let (sub, _) = expect_type(&mut ws, "subscribed").await;
    let qk = sub["query_key"].as_str().unwrap().to_string();
    let (err, _) = expect_type(&mut ws, "error").await;
    assert_eq!(err["code"], "query_failed");

    // The provider recovers: the subscription delivers the new value.
    services
        .provider_query(
            "open_prs",
            params.clone(),
            &Principal::administrator(),
            true,
        )
        .await
        .unwrap();
    let mut statuses = Vec::new();
    loop {
        let (sig, _) = expect_type(&mut ws, "query_signal").await;
        assert_eq!(sig["envelope"]["query_key"], qk.as_str());
        let status = sig["envelope"]["status"].as_str().unwrap().to_string();
        statuses.push(status.clone());
        if status == "fresh" {
            break;
        }
    }
    // Subscribers saw the `loading` and `error` Signals before the recovered value.
    assert!(statuses.contains(&"loading".to_string()), "{statuses:?}");
    assert!(statuses.contains(&"error".to_string()), "{statuses:?}");
    h.abort();
}

#[tokio::test]
async fn lagging_subscriber_resyncs_to_latest_snapshot_and_keeps_receiving() {
    let (_tmp, services, addr, h) = boot().await;
    let params: Value = serde_json::from_str(PARAMS).unwrap();
    let mut ws = connect(addr, ADMIN_TOKEN).await;
    send(
        &mut ws,
        json!({"type": "subscribe_query", "instance": "open_prs", "params": params}),
    )
    .await;
    let (sub, _) = expect_type(&mut ws, "subscribed").await;
    let qk = sub["query_key"].as_str().unwrap().to_string();
    expect_type(&mut ws, "query_signal").await;

    // Flood far beyond the bounded stream capacity without yielding (current-thread runtime):
    // the forwarder must lag, then skip to the latest value instead of stopping.
    let hub = services.engine().signals.clone();
    let total = (hub.channel_capacity() as u64) * 3;
    for v in 1000..1000 + total {
        hub.publish_query(QueryEnvelope::snapshot(
            "open_prs",
            &qk,
            1,
            v,
            QueryStatus::Fresh,
            None,
            json!([{"id": "r", "v": v}]),
            None,
        ));
    }
    let last = 1000 + total - 1;
    let mut saw_resync = false;
    loop {
        let (m, _) = expect_type(&mut ws, "query_signal").await;
        if m["resync"] == true {
            saw_resync = true;
        }
        if m["envelope"]["version"].as_u64() == Some(last) {
            break;
        }
    }
    assert!(
        saw_resync,
        "lagged subscriber should receive a resync snapshot"
    );

    // Still subscribed afterwards.
    hub.publish_query(QueryEnvelope::snapshot(
        "open_prs",
        &qk,
        1,
        last + 1,
        QueryStatus::Fresh,
        None,
        json!([]),
        None,
    ));
    let (m, _) = expect_type(&mut ws, "query_signal").await;
    assert_eq!(m["envelope"]["version"].as_u64(), Some(last + 1));

    // Explicit client resync request returns the latest snapshot.
    send(&mut ws, json!({"type": "resync_query", "query_key": qk})).await;
    let (m, _) = expect_type(&mut ws, "query_signal").await;
    assert_eq!(m["resync"], true);
    assert_eq!(m["envelope"]["version"].as_u64(), Some(last + 1));
    h.abort();
}
