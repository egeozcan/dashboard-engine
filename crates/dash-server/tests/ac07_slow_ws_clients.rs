//! §17 AC7 / §16 behaviour 8 / §6.2: slow WebSocket clients of the reference application.
//!
//! A burst of real engine updates (keyed deltas + snapshots from `push_query_value` on the
//! reference app's `open_prs` query) overruns the bounded Signal stream of every socket. A client
//! that applies what it receives strictly (snapshots replace, deltas only onto their
//! `base_version`) must converge to the engine's final value by skipping intermediate values and
//! resyncing from a snapshot; a client that is slow to read never holds back another client.

use dash_core::{apply_delta_envelope, Principal, QueryEnvelope};
use dash_git::DraftManager;
use dash_luau::ResourceBudget;
use dash_server::{build_router, AppServices};
use dash_store::SqliteStore;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const TOKEN: &str = "adm_cli_ac07_slow_clients";

async fn boot() -> (TempDir, AppServices, SocketAddr) {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&fixture, &ws).unwrap();
    let store = SqliteStore::open(&data).unwrap();
    // `--use-mocks`: fixture Mocks, live authority.
    let mgr = DraftManager::initialize_with_mocks(ws, data, store, ResourceBudget::default(), true)
        .await
        .unwrap();
    let services = AppServices::new(mgr, vec![]);
    std::fs::write(&services.admin_cli_token_path, TOKEN).unwrap();
    let app = build_router(services.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (tmp, services, addr)
}

async fn connect(addr: SocketAddr) -> Ws {
    let mut req = format!("ws://{addr}/ws").into_client_request().unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

async fn next_json(ws: &mut Ws, deadline: Instant) -> Value {
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let msg = tokio::time::timeout(left, ws.next())
            .await
            .expect("timed out waiting for a WebSocket message")
            .expect("socket closed")
            .unwrap();
        if let Message::Text(t) = msg {
            return serde_json::from_str(t.as_str()).unwrap();
        }
    }
}

/// A strict client-side replica of one query.
#[derive(Default)]
struct Replica {
    version: u64,
    rows: Vec<Value>,
    frames: usize,
    resyncs: usize,
}

impl Replica {
    /// Apply one `query_signal`. Returns false when a delta did not chain onto the replica.
    fn apply(&mut self, msg: &Value) -> bool {
        let env: QueryEnvelope = serde_json::from_value(msg["envelope"].clone()).unwrap();
        self.frames += 1;
        if msg["resync"] == true {
            self.resyncs += 1;
        }
        match env.kind {
            dash_core::EnvelopeKind::Snapshot => {
                if let Some(Value::Array(rows)) = env.value {
                    self.rows = rows;
                    self.version = env.version;
                }
                true
            }
            dash_core::EnvelopeKind::Delta => {
                let key = env.row_key.clone().unwrap_or_else(|| "id".into());
                apply_delta_envelope(&mut self.version, &mut self.rows, &env, &key).is_ok()
            }
        }
    }

    /// Read until the replica is at `version`; a non-chaining delta triggers `resync_query`.
    async fn sync_to(&mut self, ws: &mut Ws, qk: &str, version: u64) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.version != version {
            let msg = next_json(ws, deadline).await;
            if msg["type"] != "query_signal" {
                continue;
            }
            if !self.apply(&msg) {
                ws.send(Message::Text(
                    json!({"type": "resync_query", "query_key": qk})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            }
        }
    }
}

async fn subscribe(ws: &mut Ws) -> (String, Replica) {
    ws.send(Message::Text(
        json!({"type": "subscribe_query", "instance": "open_prs",
               "params": {"repository": "acme/platform", "state": "open"}})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let qk = loop {
        let m = next_json(ws, deadline).await;
        if m["type"] == "subscribed" {
            break m["query_key"].as_str().unwrap().to_string();
        }
    };
    let mut replica = Replica::default();
    while replica.rows.is_empty() {
        let m = next_json(ws, deadline).await;
        if m["type"] == "query_signal" {
            assert!(replica.apply(&m));
        }
    }
    (qk, replica)
}

fn pr(number: i64, rev: u64) -> Value {
    json!({"id": format!("acme/platform#{number}"), "number": number,
           "title": format!("DEV-{number} rev {rev}"), "repository": "acme/platform",
           "author": "t2", "state": "open"})
}

#[tokio::test]
async fn slow_clients_skip_to_the_latest_value_and_never_block_fast_clients() {
    let (_tmp, services, addr) = boot().await;
    let mut fast = connect(addr).await;
    let mut slow = connect(addr).await;
    let (qk, mut fast_r) = subscribe(&mut fast).await;
    let (qk2, mut slow_r) = subscribe(&mut slow).await;
    assert_eq!(qk, qk2);
    assert_eq!(fast_r.rows.len(), 3);

    // A burst far beyond the bounded stream capacity, without yielding to the forwarders
    // (current-thread runtime): every update is a real engine commit with a keyed delta.
    let admin = Principal::administrator();
    let params = json!({"repository": "acme/platform", "state": "open"});
    let updates = services.engine().signals.channel_capacity() as u64 * 3;
    let mut last = None;
    for rev in 1..=updates {
        let rows = vec![pr(42, rev), pr(43, rev / 2), pr(44 + (rev % 3) as i64, 0)];
        let env = services
            .engine()
            .queries
            .push_query_value("open_prs", &params, &admin, json!(rows))
            .unwrap();
        last = Some(env);
    }
    let last = last.unwrap();
    let final_rows = last.value.clone().unwrap();

    // The fast client converges first while the slow one has not read anything.
    fast_r.sync_to(&mut fast, &qk, last.version).await;
    assert_eq!(json!(fast_r.rows), final_rows);
    assert!(
        fast_r.frames < 2 * updates as usize,
        "intermediate values were skipped ({} frames for {updates} updates)",
        fast_r.frames
    );

    // The slow client catches up by skipping to the latest value.
    slow_r.sync_to(&mut slow, &qk, last.version).await;
    assert_eq!(json!(slow_r.rows), final_rows);
    assert!(
        slow_r.resyncs >= 1,
        "a lagging socket is resynced from a snapshot"
    );
    assert!(slow_r.frames < 2 * updates as usize);
    let cached = services.engine().queries.cached_entry(&qk).unwrap();
    assert_eq!((cached.version, cached.value), (last.version, final_rows));

    // Both stay subscribed: the next update arrives as a delta that chains onto the replica.
    let next = services
        .engine()
        .queries
        .push_query_value(
            "open_prs",
            &params,
            &admin,
            json!([pr(42, 9999), pr(43, 0)]),
        )
        .unwrap();
    for (ws, r) in [(&mut fast, &mut fast_r), (&mut slow, &mut slow_r)] {
        let resyncs = r.resyncs;
        r.sync_to(ws, &qk, next.version).await;
        assert_eq!(json!(r.rows), next.value.clone().unwrap());
        assert_eq!(r.resyncs, resyncs, "no resync needed once caught up");
    }
}
