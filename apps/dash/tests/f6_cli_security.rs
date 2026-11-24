//! CLI security regressions: routing to a daemon bound to one specific non-loopback interface,
//! session revocation on credential reset, and policy-preserving secret rotation
//! (§5.2, §14.1, §14.3).

use serde_json::{json, Value};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_dash");

async fn dash(ws: &Path, data: &Path, args: &[&str]) -> std::process::Output {
    let mut all = vec![
        "--workspace".to_string(),
        ws.to_string_lossy().to_string(),
        "--data-dir".to_string(),
        data.to_string_lossy().to_string(),
    ];
    all.extend(args.iter().map(|s| s.to_string()));
    tokio::process::Command::new(BIN)
        .args(&all)
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap()
}

fn ok(out: &std::process::Output) -> Value {
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or(Value::Null)
}

fn free_addr_on(ip: IpAddr) -> SocketAddr {
    let probe = std::net::TcpListener::bind(SocketAddr::new(ip, 0)).unwrap();
    probe.local_addr().unwrap()
}

async fn wait_ready(addr: &SocketAddr) {
    let client = reqwest::Client::new();
    for _ in 0..200 {
        if let Ok(r) = client.get(format!("http://{addr}/healthz")).send().await {
            if r.status() == 200 {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("daemon never became ready on {addr}");
}

async fn init(ws: &Path, data: &Path, password: &str) {
    ok(&dash(ws, data, &["init", "--admin-password", password]).await);
}

fn spawn_serve(ws: &Path, data: &Path, bind: &SocketAddr) -> tokio::process::Child {
    tokio::process::Command::new(BIN)
        .args([
            "--workspace",
            ws.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
            "serve",
            "--bind",
            &bind.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

async fn stop(mut child: tokio::process::Child, data: &Path) {
    let pid = child.id().unwrap().to_string();
    let _ = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status();
    let _ = tokio::time::timeout(Duration::from_secs(30), child.wait()).await;
    assert!(!data.join("daemon.json").exists());
}

/// A local, non-loopback interface address of this host (the source address the kernel would
/// use for an off-host route). `None` when the host has no such interface.
fn local_interface_ip() -> Option<IpAddr> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    // connect() on UDP only selects a route and source address; nothing is sent.
    sock.connect("192.0.2.1:9").ok()?;
    let ip = sock.local_addr().ok()?.ip();
    if ip.is_loopback() || ip.is_unspecified() {
        return None;
    }
    // Must be bindable here.
    std::net::TcpListener::bind(SocketAddr::new(ip, 0)).ok()?;
    Some(ip)
}

// ---- R5:SRV-4 + R5:SRV-6 (through the daemon) ----------------------------------------------

#[tokio::test]
async fn cli_routes_to_a_daemon_bound_to_a_specific_interface_and_rotation_keeps_bindings() {
    let Some(ip) = local_interface_ip() else {
        eprintln!("skipping: this host has no non-loopback interface address");
        return;
    };
    let tmp = TempDir::new().unwrap();
    let (ws, data) = (tmp.path().join("ws"), tmp.path().join("data"));
    init(&ws, &data, "secret123").await;
    let bind = free_addr_on(ip);
    let child = spawn_serve(&ws, &data, &bind);
    wait_ready(&bind).await;

    // daemon.json points the CLI at a loopback admin channel, never at the interface address.
    let info: Value =
        serde_json::from_str(&std::fs::read_to_string(data.join("daemon.json")).unwrap()).unwrap();
    let url = info["url"].as_str().unwrap();
    assert!(
        url.starts_with("http://127.0.0.1:"),
        "daemon.json must name a loopback URL: {url}"
    );

    // Routed admin commands authenticate (previously: 401 invalid or revoked bearer token).
    ok(&dash(
        &ws,
        &data,
        &[
            "secret",
            "set",
            "github-work",
            "--value",
            "ghp_v1",
            "--description",
            "Work PAT",
            "--allowed-binding",
            "github_reader",
        ],
    )
    .await);
    // Plain value rotation keeps the binding restriction and description.
    let rotated = ok(&dash(
        &ws,
        &data,
        &["secret", "set", "github-work", "--value", "ghp_v2"],
    )
    .await);
    assert_eq!(rotated["key_version"], 2);
    assert_eq!(rotated["allowed_bindings"], json!(["github_reader"]));
    assert_eq!(rotated["description"], "Work PAT");
    let listed = ok(&dash(&ws, &data, &["secret", "list"]).await);
    assert_eq!(listed[0]["allowed_bindings"], json!(["github_reader"]));
    // Widening is an explicit flag.
    let widened = ok(&dash(
        &ws,
        &data,
        &[
            "secret",
            "set",
            "github-work",
            "--value",
            "ghp_v3",
            "--clear-allowed-bindings",
        ],
    )
    .await);
    assert_eq!(widened["allowed_bindings"], json!([]));
    assert_eq!(widened["description"], "Work PAT");
    ok(&dash(&ws, &data, &["git", "status"]).await);

    // The interface address keeps serving browsers/agents; the admin token is not accepted there.
    let r = reqwest::Client::new()
        .get(format!("http://{bind}/api/secrets"))
        .bearer_auth(
            std::fs::read_to_string(data.join("admin.token"))
                .unwrap()
                .trim(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    stop(child, &data).await;
}

// ---- R5:SRV-6 (in-process, no daemon) -------------------------------------------------------

#[tokio::test]
async fn in_process_secret_rotation_keeps_binding_policy() {
    let tmp = TempDir::new().unwrap();
    let (ws, data) = (tmp.path().join("ws"), tmp.path().join("data"));
    init(&ws, &data, "secret123").await;
    ok(&dash(
        &ws,
        &data,
        &[
            "secret",
            "set",
            "gh",
            "--value",
            "v1",
            "--allowed-binding",
            "github_reader",
            "--allowed-host",
            "api.github.com",
        ],
    )
    .await);
    let rotated = ok(&dash(&ws, &data, &["secret", "set", "gh", "--value", "v2"]).await);
    assert_eq!(rotated["allowed_bindings"], json!(["github_reader"]));
    assert_eq!(rotated["allowed_hosts"], json!(["api.github.com"]));
    // The two binding flags are mutually exclusive.
    let both = dash(
        &ws,
        &data,
        &[
            "secret",
            "set",
            "gh",
            "--value",
            "v3",
            "--allowed-binding",
            "x",
            "--clear-allowed-bindings",
        ],
    )
    .await;
    assert!(!both.status.success());
}

// ---- R5:SRV-5 -------------------------------------------------------------------------------

async fn login_cookie(addr: &SocketAddr, password: &str) -> String {
    let r = reqwest::Client::new()
        .post(format!("http://{addr}/api/auth/login"))
        .json(&json!({ "username": "admin", "password": password }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    r.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

async fn cookie_status(addr: &SocketAddr, cookie: &str) -> u16 {
    reqwest::Client::new()
        .get(format!("http://{addr}/api/graph"))
        .header("cookie", cookie)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

#[tokio::test]
async fn reset_admin_password_and_token_rotation_revoke_browser_sessions() {
    let tmp = TempDir::new().unwrap();
    let (ws, data) = (tmp.path().join("ws"), tmp.path().join("data"));
    init(&ws, &data, "first-password").await;
    let addr = free_addr_on("127.0.0.1".parse().unwrap());

    let child = spawn_serve(&ws, &data, &addr);
    wait_ready(&addr).await;
    let stolen = login_cookie(&addr, "first-password").await;
    assert_eq!(cookie_status(&addr, &stolen).await, 200);
    stop(child, &data).await;

    ok(&dash(
        &ws,
        &data,
        &[
            "init",
            "--reset-admin-password",
            "--admin-password",
            "second-password",
        ],
    )
    .await);
    let child = spawn_serve(&ws, &data, &addr);
    wait_ready(&addr).await;
    assert_eq!(
        cookie_status(&addr, &stolen).await,
        401,
        "a session from before the password reset must be revoked"
    );
    let fresh = login_cookie(&addr, "second-password").await;
    assert_eq!(cookie_status(&addr, &fresh).await, 200);
    stop(child, &data).await;

    ok(&dash(&ws, &data, &["init", "--rotate-admin-token"]).await);
    let child = spawn_serve(&ws, &data, &addr);
    wait_ready(&addr).await;
    assert_eq!(cookie_status(&addr, &fresh).await, 401);
    stop(child, &data).await;
}
