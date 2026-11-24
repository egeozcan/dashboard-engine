//! CLI <-> daemon integration: first-run bootstrap without default credentials, Installation lock,
//! CLI subcommands routed to the running daemon, stdio MCP proxy, and SIGTERM graceful shutdown
//! (§11.1, §14.1, §14.3, §15.2).

use serde_json::{json, Value};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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

fn free_addr() -> String {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    format!("127.0.0.1:{port}")
}

async fn wait_ready(addr: &str) {
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

#[tokio::test]
async fn init_generates_password_and_never_silently_overwrites_credentials() {
    let tmp = TempDir::new().unwrap();
    let (ws, data) = (tmp.path().join("ws"), tmp.path().join("data"));
    let first = dash(&ws, &data, &["init"]).await;
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let out = String::from_utf8_lossy(&first.stdout).to_string();
    let line = out
        .lines()
        .find(|l| l.contains("Generated Administrator password"))
        .expect("generated password must be printed once");
    let password = line.rsplit(": ").next().unwrap().trim().to_string();
    assert!(password.len() >= 32);
    assert_ne!(password, "admin");

    // Re-running init keeps the existing credentials and prints no new password.
    let second = dash(&ws, &data, &["init"]).await;
    assert!(second.status.success());
    assert!(!String::from_utf8_lossy(&second.stdout).contains("Generated Administrator password"));

    // Too-short explicit passwords are refused.
    let short = dash(
        &ws,
        &data,
        &["init", "--reset-admin-password", "--admin-password", "abc"],
    )
    .await;
    assert!(!short.status.success());

    // The generated password works for browser login.
    let addr = free_addr();
    let mut child = tokio::process::Command::new(BIN)
        .args([
            "--workspace",
            ws.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
            "serve",
            "--bind",
            &addr,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_ready(&addr).await;
    let client = reqwest::Client::new();
    let ok = client
        .post(format!("http://{addr}/api/auth/login"))
        .json(&json!({ "username": "admin", "password": password }))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
    let default_pw = client
        .post(format!("http://{addr}/api/auth/login"))
        .json(&json!({ "username": "admin", "password": "admin" }))
        .send()
        .await
        .unwrap();
    assert_eq!(default_pw.status(), 401);
    let _ = child.kill().await;
}

#[tokio::test]
async fn cli_commands_route_to_running_daemon_and_stdio_mcp_proxies_cleanly() {
    let tmp = TempDir::new().unwrap();
    let (ws, data) = (tmp.path().join("ws"), tmp.path().join("data"));
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
    let init = dash(
        &ws,
        &data,
        &[
            "init",
            "--admin-password",
            "secret123",
            "--from-fixture",
            fixture.to_str().unwrap(),
        ],
    )
    .await;
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );

    let addr = free_addr();
    let mut child = tokio::process::Command::new(BIN)
        .args([
            "--workspace",
            ws.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
            "serve",
            "--bind",
            &addr,
            "--use-mocks",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_ready(&addr).await;
    assert!(data.join("daemon.json").exists());

    // A second daemon against the same Installation is refused.
    let second = dash(&ws, &data, &["serve", "--bind", &free_addr()]).await;
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("Installation lock"));

    // Token creation goes through the daemon (it shows up in the daemon's audit trail).
    let tok = dash(
        &ws,
        &data,
        &[
            "token",
            "create",
            "stdio-bot",
            "--ceiling",
            r#"{"allowed_providers":["*"],"can_author_drafts":true}"#,
            "--expires-in",
            "1h",
        ],
    )
    .await;
    assert!(
        tok.status.success(),
        "{}",
        String::from_utf8_lossy(&tok.stderr)
    );
    let tok_json: Value = serde_json::from_slice(&tok.stdout).unwrap();
    let agent_token = tok_json["token"].as_str().unwrap().to_string();
    assert!(tok_json["agent"]["expires_at"].is_string());

    let doctor = dash(&ws, &data, &["doctor"]).await;
    let doc: Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert!(doc["checks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["name"] == "sqlite_quick_check" && c["ok"] == true));

    let reload = dash(&ws, &data, &["reload"]).await;
    assert!(
        reload.status.success(),
        "{}",
        String::from_utf8_lossy(&reload.stderr)
    );

    // Secret plaintext from stdin (never argv), stored by the daemon.
    let mut set = tokio::process::Command::new(BIN)
        .args([
            "--workspace",
            ws.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
            "secret",
            "set",
            "cli-secret",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    set.stdin
        .take()
        .unwrap()
        .write_all(b"s3cr3t-from-stdin\n")
        .await
        .unwrap();
    let set_out = set.wait_with_output().await.unwrap();
    assert!(
        set_out.status.success(),
        "{}",
        String::from_utf8_lossy(&set_out.stderr)
    );
    assert!(!String::from_utf8_lossy(&set_out.stdout).contains("s3cr3t-from-stdin"));
    let listed = dash(&ws, &data, &["secret", "list"]).await;
    assert!(String::from_utf8_lossy(&listed.stdout).contains("cli-secret"));

    let git = dash(&ws, &data, &["git", "status"]).await;
    assert!(
        git.status.success(),
        "{}",
        String::from_utf8_lossy(&git.stderr)
    );

    let manifest = dash(&ws, &data, &["manifest", "engineering.linked_prs"]).await;
    assert!(
        manifest.status.success(),
        "{}",
        String::from_utf8_lossy(&manifest.stderr)
    );
    let m: Value = serde_json::from_slice(&manifest.stdout).unwrap();
    assert_eq!(m["id"], "engineering.linked_prs");

    // stdio MCP: one JSON line per request, nothing for notifications, errors stay JSON-RPC.
    let mut proxy = tokio::process::Command::new(BIN)
        .args([
            "mcp-stdio",
            "--url",
            &format!("http://{addr}/mcp"),
            "--token",
            &agent_token,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = proxy.stdin.take().unwrap();
    let mut out = BufReader::new(proxy.stdout.take().unwrap()).lines();
    for msg in [
        json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": { "protocolVersion": "2025-06-18" } }),
        json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
        json!({ "jsonrpc": "2.0", "id": 3, "method": "no/such/method" }),
    ] {
        stdin
            .write_all(format!("{msg}\n").as_bytes())
            .await
            .unwrap();
    }
    stdin.write_all(b"not json\n").await.unwrap();
    drop(stdin);
    let mut lines = Vec::new();
    while let Ok(Some(l)) = tokio::time::timeout(Duration::from_secs(20), out.next_line())
        .await
        .unwrap()
    {
        lines.push(serde_json::from_str::<Value>(&l).expect("every stdout line is JSON"));
    }
    assert_eq!(lines.len(), 4, "{lines:?}");
    let by_id = |id: Value| lines.iter().find(|l| l["id"] == id).cloned().unwrap();
    assert_eq!(by_id(json!(1))["result"]["protocolVersion"], "2025-06-18");
    assert!(by_id(json!(2))["result"]["tools"].as_array().unwrap().len() >= 35);
    assert_eq!(by_id(json!(3))["error"]["code"], -32601);
    assert_eq!(by_id(Value::Null)["error"]["code"], -32700);
    let _ = proxy.wait().await;

    // SIGTERM triggers the graceful drain path: the process exits cleanly and unpublishes itself.
    let pid = child.id().unwrap().to_string();
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap();
    assert!(killed.success());
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .expect("daemon must exit after SIGTERM")
        .unwrap();
    assert!(
        status.success(),
        "graceful shutdown must exit 0, got {status:?}"
    );
    assert!(!data.join("daemon.json").exists());

    // With no daemon, CLI commands run in-process under the Installation lock.
    let offline = dash(&ws, &data, &["secret", "list"]).await;
    assert!(
        offline.status.success(),
        "{}",
        String::from_utf8_lossy(&offline.stderr)
    );
}
