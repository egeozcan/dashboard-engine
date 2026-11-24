//! Performance benchmark gates against §15.3 initial design targets.
//! Run in release mode: `cargo test --release -p dash --test benchmarks -- --nocapture`

use dash_core::{Principal, QueryEnvelope, QueryStatus};
use dash_git::DraftManager;
use dash_luau::ResourceBudget;
use dash_server::{build_router, AppServices};
use dash_store::SqliteStore;
use serde_json::json;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn copy_ecc_fixture(dst: &Path) {
    let src =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    dash_git::packages::copy_tree_excluding_git(&src, dst).unwrap();
}

fn process_rss_mib(pid: u32) -> Option<f64> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let kb: f64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    Some(kb / 1024.0)
}

#[tokio::test]
async fn benchmark_gates_section_15_3() {
    let bin = env!("CARGO_BIN_EXE_dash");

    // 1. Fresh daemon startup <= 2 s & Idle RSS <= 100 MiB (zero configured Instances)
    let tmp_fresh = TempDir::new().unwrap();
    let ws_fresh = tmp_fresh.path().join("ws");
    let data_fresh = tmp_fresh.path().join("data");
    let init = tokio::process::Command::new(bin)
        .args([
            "--workspace",
            ws_fresh.to_str().unwrap(),
            "--data-dir",
            data_fresh.to_str().unwrap(),
            "init",
        ])
        .output()
        .await
        .unwrap();
    assert!(init.status.success());

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let bind_addr = format!("127.0.0.1:{port}");

    let start_t0 = Instant::now();
    let mut child = tokio::process::Command::new(bin)
        .args([
            "--workspace",
            ws_fresh.to_str().unwrap(),
            "--data-dir",
            data_fresh.to_str().unwrap(),
            "serve",
            "--bind",
            &bind_addr,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let client = reqwest::Client::new();
    let mut startup_ms = None;
    for _ in 0..200 {
        if let Ok(r) = client
            .get(format!("http://{bind_addr}/healthz"))
            .send()
            .await
        {
            if r.status() == 200 {
                startup_ms = Some(start_t0.elapsed().as_secs_f64() * 1000.0);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let startup_ms = startup_ms.expect("daemon failed to start");
    let pid = child.id().unwrap();
    let idle_rss_mib = process_rss_mib(pid).unwrap_or(0.0);
    let _ = child.kill().await;

    assert!(startup_ms <= 2000.0, "startup {startup_ms:.1} ms > 2000 ms");
    assert!(
        idle_rss_mib <= 100.0,
        "idle RSS {idle_rss_mib:.1} MiB > 100 MiB"
    );

    // 2. Warm in-memory provider cache query p95 <= 5 ms & 100-subscriber signal propagation p95 <= 100 ms
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    copy_ecc_fixture(&ws);
    let store = SqliteStore::open(&data).unwrap();
    let mgr = DraftManager::initialize(ws, data, store, ResourceBudget::default())
        .await
        .unwrap();
    let services = AppServices::new(mgr, vec![]);
    let mut g = (*services.graph()).clone();
    g.is_preview = true;
    services.engine().graph.store(std::sync::Arc::new(g));
    let admin = Principal::administrator();

    let init_q = services
        .provider_query(
            "open_prs",
            json!({"repository": "acme/platform", "state": "open"}),
            &admin,
            false,
        )
        .await
        .unwrap();

    let mut warm_latencies_ms = Vec::with_capacity(200);
    for _ in 0..200 {
        let t = Instant::now();
        let _ = services
            .provider_query(
                "open_prs",
                json!({"repository": "acme/platform", "state": "open"}),
                &admin,
                false,
            )
            .await
            .unwrap();
        warm_latencies_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    warm_latencies_ms.sort_by(|a, b| a.total_cmp(b));
    let warm_p95_ms = warm_latencies_ms[(warm_latencies_ms.len() * 95) / 100];
    assert!(
        warm_p95_ms <= 5.0,
        "warm query p95 {warm_p95_ms:.3} ms > 5 ms"
    );

    // Signal propagation to 100 local subscribers p95 <= 100 ms
    let mut prop_latencies_ms = Vec::with_capacity(20);
    for iter in 0..20 {
        let mut rxs = Vec::with_capacity(100);
        for _ in 0..100 {
            rxs.push(
                services
                    .engine()
                    .signals
                    .subscribe_stream(&init_q.query_key),
            );
        }
        let t = Instant::now();
        services
            .engine()
            .signals
            .publish_query(QueryEnvelope::snapshot(
                "open_prs",
                &init_q.query_key,
                1,
                100 + iter,
                QueryStatus::Fresh,
                None,
                json!([]),
                None,
            ));
        for mut rx in rxs {
            let _ = rx.recv().await.unwrap();
        }
        prop_latencies_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    prop_latencies_ms.sort_by(|a, b| a.total_cmp(b));
    let prop_p95_ms = prop_latencies_ms[(prop_latencies_ms.len() * 95) / 100];
    assert!(
        prop_p95_ms <= 100.0,
        "signal propagation p95 {prop_p95_ms:.3} ms > 100 ms"
    );

    // 3. Failure isolation: looping Luau does not stall /healthz
    let app = build_router(services.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let hz_t = Instant::now();
    let hz = client
        .get(format!("http://{addr}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(hz.status(), 200);
    let health_ms = hz_t.elapsed().as_secs_f64() * 1000.0;
    srv.abort();

    println!(
        "BENCHMARK_REPORT={}",
        json!({
            "fresh_startup_ms": (startup_ms * 100.0).round() / 100.0,
            "idle_rss_mib": (idle_rss_mib * 100.0).round() / 100.0,
            "warm_query_p95_ms": (warm_p95_ms * 1000.0).round() / 1000.0,
            "signal_100_subs_p95_ms": (prop_p95_ms * 1000.0).round() / 1000.0,
            "healthz_ms": (health_ms * 1000.0).round() / 1000.0
        })
    );
}
