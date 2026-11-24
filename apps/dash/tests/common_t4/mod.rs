//! Shared helpers for the criterion 15 (sandbox) and criterion 17 (benchmark gate) suites.
//!
//! Each test copies the Engineering Command Center fixture into a temp Workspace, adds the probe
//! Definitions/Instances it needs *before* the daemon services start (so they are part of the
//! initial Generation exactly like committed Workspace content), and boots the real router.
#![allow(dead_code)]

use dash_git::DraftManager;
use dash_luau::ResourceBudget;
use dash_server::{build_router, hash_password, AppServices};
use dash_store::SqliteStore;
use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Bearer token the booted services accept as the Administrator CLI token.
pub const ADMIN_TOKEN: &str = "adm_t4_local";

pub struct Workspace {
    pub tmp: TempDir,
    pub ws: PathBuf,
    pub data: PathBuf,
}

impl Workspace {
    /// A fresh copy of the reference fixture.
    pub fn ecc() -> Self {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        let src =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/engineering-command-center");
        dash_git::packages::copy_tree_excluding_git(&src, &ws).unwrap();
        Self { tmp, ws, data }
    }

    /// Add a Definition to the fixture's local `engineering` package.
    pub fn add_definition(&self, name: &str, definition: &str, runtime: &str) {
        let dir = self.ws.join("packages/engineering").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("definition.luau"), definition).unwrap();
        std::fs::write(dir.join("runtime.luau"), runtime).unwrap();
    }

    /// Append raw YAML entries (already indented under the top-level key) to `instances/<file>`.
    pub fn append_instances(&self, file: &str, yaml_entries: &str) {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(self.ws.join("instances").join(file))
            .unwrap();
        writeln!(f).unwrap();
        f.write_all(yaml_entries.as_bytes()).unwrap();
    }

    /// Append TOML to `dashboard.toml` (e.g. `[resources]` budgets).
    pub fn append_dashboard_toml(&self, toml: &str) {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(self.ws.join("dashboard.toml"))
            .unwrap();
        writeln!(f).unwrap();
        f.write_all(toml.as_bytes()).unwrap();
    }
}

pub struct Booted {
    pub services: AppServices,
    pub addr: SocketAddr,
    pub server: tokio::task::JoinHandle<()>,
}

impl Booted {
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }
}

impl Drop for Booted {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Start the real services and HTTP router for `w`. `use_mocks` is the `dash serve --use-mocks`
/// path: the live Generation substitutes the fixture Mocks for GitHub/Jira connectors.
pub async fn boot(w: &Workspace, use_mocks: bool, budget: ResourceBudget) -> Booted {
    let store = SqliteStore::open(&w.data).unwrap();
    store
        .set_admin_credentials("admin", &hash_password("adminpass").unwrap())
        .unwrap();
    let mgr =
        DraftManager::initialize_with_mocks(w.ws.clone(), w.data.clone(), store, budget, use_mocks)
            .await
            .unwrap();
    let services = AppServices::new(mgr, vec![]);
    std::fs::write(&services.admin_cli_token_path, ADMIN_TOKEN).unwrap();
    let app = build_router(services.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Booted {
        services,
        addr,
        server,
    }
}

/// Poll `check` every 5 ms until it returns `Some`, or panic after `timeout`.
pub async fn wait_for<T, F, Fut>(what: &str, timeout: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let t0 = Instant::now();
    loop {
        if let Some(v) = check().await {
            return v;
        }
        assert!(
            t0.elapsed() < timeout,
            "timed out after {timeout:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Instance State value written by a probe's `ctx.state:set` (a deterministic "the Luau handler is
/// now executing" signal).
pub fn instance_state(
    services: &AppServices,
    instance: &str,
    key: &str,
) -> Option<serde_json::Value> {
    services
        .store()
        .get_state("instance", instance, key)
        .unwrap()
        .map(|r| r.value)
}

/// Nearest-rank percentile (`q` in 0..=100) of `samples` in milliseconds.
pub fn percentile(samples: &[f64], q: f64) -> f64 {
    assert!(!samples.is_empty(), "no samples");
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.total_cmp(b));
    let rank = ((q / 100.0) * s.len() as f64).ceil() as usize;
    s[rank.clamp(1, s.len()) - 1]
}

pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Timing gates are defined for optimized builds (§15.3 "publish benchmark hardware ... and
/// methods"). Unoptimized debug builds run the same scenarios with this slack factor on latency
/// bounds only; every functional gate (dedup count, interruption, reclamation, coherence) is
/// identical in both profiles.
pub fn debug_slack() -> f64 {
    if cfg!(debug_assertions) {
        10.0
    } else {
        1.0
    }
}

/// Resident set size of `pid` in MiB, via `ps` (macOS and Linux).
pub fn process_rss_mib(pid: u32) -> Result<f64, String> {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .map_err(|e| format!("ps failed: {e}"))?;
    let text = String::from_utf8_lossy(&out.stdout);
    let kib: f64 = text
        .trim()
        .parse()
        .map_err(|e| format!("cannot parse ps rss output {text:?}: {e}"))?;
    if kib <= 0.0 {
        return Err(format!("implausible RSS {kib} KiB for pid {pid}"));
    }
    Ok(kib / 1024.0)
}

/// Spawn `dash serve` on a fresh, initialized Workspace with zero configured Instances and return
/// the child, its address, and the time from spawn until `/healthz` answered 200.
pub async fn spawn_fresh_daemon(tmp: &TempDir) -> (tokio::process::Child, String, Duration) {
    let bin = env!("CARGO_BIN_EXE_dash");
    let ws = tmp.path().join("ws");
    let data = tmp.path().join("data");
    let init = tokio::process::Command::new(bin)
        .args([
            "--workspace",
            ws.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
            "init",
            "--admin-password",
            "secret123",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        init.status.success(),
        "dash init failed: {}",
        String::from_utf8_lossy(&init.stderr)
    );
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = probe.local_addr().unwrap().to_string();
    drop(probe);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let t0 = Instant::now();
    let child = tokio::process::Command::new(bin)
        .args([
            "--workspace",
            ws.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
            "serve",
            "--bind",
            &bind,
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    loop {
        if let Ok(r) = client.get(format!("http://{bind}/healthz")).send().await {
            if r.status() == 200 {
                break;
            }
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "daemon never served /healthz"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    (child, bind, t0.elapsed())
}

pub const LOOP_DEF: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.runaway_loop",
    description = "Signals entry through Instance State, then spins forever",
    runtime = "runtime.luau",
    handler = "query",
}
"#;
pub const LOOP_RT: &str = r#"local R = {}
function R.query(ctx)
    ctx:await(ctx.state:set("entered", ctx.query))
    while true do end
end
return R
"#;
pub const BOMB_DEF: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.runaway_memory",
    description = "Allocates without bound",
    runtime = "runtime.luau",
    handler = "query",
}
"#;
pub const BOMB_RT: &str = r#"local R = {}
function R.query(ctx)
    local hoard = {}
    local i = 0
    while true do
        i += 1
        hoard[i] = string.rep("x", 4096) .. tostring(i)
    end
end
return R
"#;

pub const BOMB_MEMORY_MIB: u64 = 8;

/// The reference fixture plus a looping and a memory-exhausting data provider Instance
/// (`runaway_loop`, `runaway_memory`) with per-Instance budgets.
pub fn runaway_workspace(loop_timeout_ms: u64) -> Workspace {
    let w = Workspace::ecc();
    w.add_definition("runaway_loop", LOOP_DEF, LOOP_RT);
    w.add_definition("runaway_memory", BOMB_DEF, BOMB_RT);
    w.append_instances(
        "data.yaml",
        "  runaway_loop:\n    definition: engineering.runaway_loop\n  runaway_memory:\n    definition: engineering.runaway_memory\n",
    );
    // The loop is stopped by its wall-clock deadline (interrupt budget raised to the daemon
    // ceiling so time, not instruction count, is what interrupts it); the bomb by its memory
    // limit (its deadline is far away).
    w.append_dashboard_toml(&format!(
        "[resources.instances.runaway_loop]\ntimeout_ms = {loop_timeout_ms}\nmax_interrupts = 50000000\n\n\
         [resources.instances.runaway_memory]\nmemory_limit_mib = {BOMB_MEMORY_MIB}\ntimeout_ms = 30000\n"
    ));
    w
}
