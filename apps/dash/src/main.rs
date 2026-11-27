//! Single-executable `dash` CLI and daemon (§14.1).
//!
//! `dash serve` owns the Installation: it holds an exclusive OS lock on `<data_dir>/daemon.lock`
//! and publishes its loopback URL in `<data_dir>/daemon.json`. While a daemon runs, the other
//! subcommands (`reload`, `git status|sync`, `doctor`, `secret`, `token`, `grant`) talk to it over
//! its HTTP API with the local admin CLI token instead of opening a competing engine against the
//! same SQLite files and activation journal (§11.1, §12.2, §14.1). Without a daemon they run
//! in-process while holding the same lock.

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use dash_core::{CapabilityCeiling, NetworkPolicy, Principal};
use dash_git::{run_preview_and_scenarios, DraftManager};
use dash_luau::ResourceBudget;
use dash_server::routes::resolve_agent_token_expiry;
use dash_server::{
    build_router, generate_random_token, hash_password, AppServices, SecureCookieMode,
    ServerContext,
};
use dash_store::SqliteStore;
use serde_json::{json, Value};
use std::fs::File;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

/// Minimum accepted Administrator password length.
const MIN_ADMIN_PASSWORD_LEN: usize = 8;
/// Latest MCP protocol revision the stdio proxy announces before negotiation.
const DEFAULT_MCP_PROTOCOL: &str = "2026-07-28";

#[derive(Parser)]
#[command(name = "dash", version = "0.2.0", about = "Universal Dashboard Engine")]
struct Cli {
    #[arg(long, env = "DASH_WORKSPACE", default_value = "./workspace")]
    workspace: PathBuf,
    #[arg(long, env = "DASH_DATA_DIR", default_value = "./.dash-data")]
    data_dir: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new Workspace and Installation data directory
    Init {
        #[arg(long, default_value = "admin")]
        username: String,
        /// Administrator password. When omitted on first init, a random password is generated
        /// and printed once. There is no default password.
        #[arg(long, env = "DASH_ADMIN_PASSWORD")]
        admin_password: Option<String>,
        /// Replace existing Administrator credentials (otherwise `init` never overwrites them).
        #[arg(long, default_value_t = false)]
        reset_admin_password: bool,
        /// Generate a new local admin CLI token, invalidating the previous one.
        #[arg(long, default_value_t = false)]
        rotate_admin_token: bool,
        #[arg(long)]
        from_fixture: Option<PathBuf>,
    },
    /// Start HTTP, WebSocket, MCP, and background scheduler daemon
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: SocketAddr,
        #[arg(long = "trusted-proxy", default_value = "127.0.0.1")]
        trusted_proxies: Vec<IpAddr>,
        /// Additional accepted Host names (DNS-rebinding allowlist), e.g. the Tailscale Serve
        /// MagicDNS name `box.tailnet.ts.net`; `.suffix` accepts subdomains. Loopback names are
        /// always accepted.
        #[arg(
            long = "allowed-host",
            env = "DASH_ALLOWED_HOSTS",
            value_delimiter = ','
        )]
        allowed_hosts: Vec<String>,
        /// When session cookies get `Secure`: auto (HTTPS via trusted proxy or non-loopback bind), always, never.
        #[arg(long = "secure-cookies", default_value = "auto")]
        secure_cookies: SecureCookieMode,
        /// Graceful shutdown drain limit for in-flight requests and admitted actions (§15.2).
        #[arg(long = "shutdown-drain-secs", default_value_t = 30)]
        shutdown_drain_secs: u64,
        /// Substitute Mock connector definitions for local demos without external credentials
        #[arg(long, default_value_t = false)]
        use_mocks: bool,
    },
    /// Run all Luau Definition tests and YAML Scenario tests in isolated Preview
    Test,
    /// Validate and atomically hot-reload Workspace changes via Draft protocol
    Reload,
    /// Git Workspace operations
    Git {
        #[command(subcommand)]
        action: GitAction,
    },
    /// Inspect SQLite, vault, activation journal, Git sync, budgets, manifests and grants
    Doctor,
    /// Print generated Definition manifests and JSON Schemas (§4.3); all when no id is given
    Manifest { definition_id: Option<String> },
    /// Proxy stdio MCP JSON-RPC to a running `dash serve` daemon using an explicit Agent token (NEVER Administrator)
    McpStdio {
        #[arg(long, default_value = "http://127.0.0.1:8080/mcp")]
        url: String,
        #[arg(long, env = "DASH_AGENT_TOKEN")]
        token: String,
    },
    /// Manage encrypted central secrets
    Secret {
        #[command(subcommand)]
        action: SecretAction,
    },
    /// Manage scoped Agent MCP tokens
    Token {
        #[command(subcommand)]
        action: TokenAction,
    },
    /// Grant an Administrator capability ceiling to a Service principal
    Grant {
        principal_id: String,
        #[arg(long)]
        ceiling: String,
    },
    /// Show or grant the Administrator network ceiling (outbound HTTP is denied until granted)
    Network {
        #[command(subcommand)]
        action: NetworkAction,
    },
}

#[derive(Subcommand)]
enum NetworkAction {
    /// Show the requested Workspace `[network]` policy and the Administrator ceiling
    Show,
    /// Set the Administrator network ceiling (stored in SQLite, outside the Git Workspace)
    Grant {
        /// Ceiling as JSON, e.g. '{"allowed_hosts":["api.github.com"],"allowed_methods":["GET"]}'
        #[arg(long, conflicts_with = "from_workspace")]
        policy: Option<String>,
        /// Grant exactly the policy currently requested by the Workspace `dashboard.toml`
        #[arg(long, default_value_t = false)]
        from_workspace: bool,
    },
}

#[derive(Subcommand)]
enum GitAction {
    Status,
    Sync,
}

#[derive(Subcommand)]
enum SecretAction {
    Set {
        name: String,
        /// Secret plaintext on the command line (visible in the process table and shell
        /// history; discouraged). Without it, the value is read from stdin.
        #[arg(long)]
        value: Option<String>,
        /// Description (omitted on a rotation = keep the current one)
        #[arg(long)]
        description: Option<String>,
        /// Instance ids allowed to bind the secret (omitted on a rotation = keep the current
        /// binding policy)
        #[arg(long = "allowed-binding", conflicts_with = "clear_allowed_bindings")]
        allowed_bindings: Vec<String>,
        /// Deliberately remove the binding restriction so any Instance may bind the secret
        #[arg(long = "clear-allowed-bindings", default_value_t = false)]
        clear_allowed_bindings: bool,
        /// Destination hosts the credential may be attached to (default: any allowed host)
        #[arg(long = "allowed-host")]
        allowed_hosts: Vec<String>,
        /// How the broker presents the credential: bearer | basic | key | header:<Name> | query:<param>
        #[arg(long = "credential-scheme")]
        credential_scheme: Option<String>,
    },
    List,
}

#[derive(Subcommand)]
enum TokenAction {
    Create {
        name: String,
        #[arg(long, default_value = "{}")]
        ceiling: String,
        /// Token lifetime, e.g. `3600`, `12h`, `30d` (default 90d, max 365d).
        #[arg(long = "expires-in")]
        expires_in: Option<String>,
    },
    Revoke {
        agent_id: String,
    },
}

/// Worker-thread stack size for the daemon runtime. The default 2 MiB leaves too little headroom
/// for deeply nested Adapter → action → connector call chains (notably in debug builds), so the
/// runtime is built explicitly with a larger, bounded stack.
const WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;

fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(WORKER_STACK_BYTES)
        .build()?
        .block_on(run())
}

async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "dash=info,dash_server=info,dash_core=info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Init {
            ref username,
            ref admin_password,
            reset_admin_password,
            rotate_admin_token,
            ref from_fixture,
        } => {
            std::fs::create_dir_all(&cli.workspace)?;
            std::fs::create_dir_all(&cli.data_dir)?;
            let _lock = match InstallationLock::try_acquire(&cli.data_dir)? {
                Some(l) => l,
                None => bail!(
                    "a running `dash serve` holds this Installation; stop it before `dash init`"
                ),
            };
            if let Some(src) = from_fixture {
                dash_git::packages::copy_tree_excluding_git(src, &cli.workspace)
                    .map_err(|e| anyhow::anyhow!(e))?;
            } else if !cli.workspace.join("dashboard.toml").exists() {
                std::fs::write(
                    cli.workspace.join("dashboard.toml"),
                    "name = \"my-dash-workspace\"\ngit_branch = \"main\"\n",
                )?;
                std::fs::create_dir_all(cli.workspace.join("instances"))?;
                std::fs::create_dir_all(cli.workspace.join("dashboards"))?;
                std::fs::create_dir_all(cli.workspace.join("packages"))?;
                std::fs::create_dir_all(cli.workspace.join("scenarios"))?;
            }

            let store = SqliteStore::open(&cli.data_dir)?;
            let existing = store.get_admin_credentials()?;
            if existing.is_some() && !reset_admin_password {
                eprintln!(
                    "Administrator credentials already set; keeping them (use --reset-admin-password to replace)."
                );
            } else {
                let (password, generated) = match admin_password {
                    Some(p) => (p.clone(), false),
                    None => (generate_admin_password(), true),
                };
                if password.chars().count() < MIN_ADMIN_PASSWORD_LEN {
                    bail!("Administrator password must be at least {MIN_ADMIN_PASSWORD_LEN} characters");
                }
                let pw_hash = hash_password(&password).map_err(|e| anyhow::anyhow!(e))?;
                store.set_admin_credentials(username, &pw_hash)?;
                if generated {
                    println!(
                        "Generated Administrator password for '{username}' (shown once, store it now): {password}"
                    );
                }
            }

            let admin_tok_path = cli.data_dir.join("admin.token");
            if !admin_tok_path.exists() || rotate_admin_token {
                if rotate_admin_token {
                    // Rotating credentials after a suspected compromise also logs out every
                    // browser session (resetting the password already does, in the store).
                    let revoked = store.revoke_all_sessions()?;
                    if revoked > 0 {
                        eprintln!("Revoked {revoked} browser session(s).");
                    }
                }
                let tok = generate_random_token("adm");
                std::fs::write(&admin_tok_path, &tok)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(
                        &admin_tok_path,
                        std::fs::Permissions::from_mode(0o600),
                    )?;
                }
            }

            let mgr = DraftManager::initialize(
                cli.workspace.clone(),
                cli.data_dir.clone(),
                store,
                ResourceBudget::default(),
            )
            .await?;
            println!(
                "Initialized workspace {} at revision {}",
                cli.workspace.display(),
                mgr.active_revision().await?
            );
        }

        Commands::Serve {
            bind,
            ref trusted_proxies,
            ref allowed_hosts,
            secure_cookies,
            shutdown_drain_secs,
            use_mocks,
        } => {
            std::fs::create_dir_all(&cli.data_dir)?;
            let _lock = match InstallationLock::try_acquire(&cli.data_dir)? {
                Some(l) => l,
                None => bail!(
                    "another dash process holds the Installation lock at {}; only one daemon may own it",
                    cli.data_dir.join("daemon.lock").display()
                ),
            };
            let ctx = ServerContext::default()
                .with_allowed_hosts(Some(bind.ip()), allowed_hosts)
                .with_secure_cookies(secure_cookies);
            let services = open_services(
                &cli.workspace,
                &cli.data_dir,
                trusted_proxies.clone(),
                use_mocks,
            )
            .await?
            .with_server_context(ctx);

            // Crash recovery before admitting work (§15.2): we hold the exclusive Installation
            // lock, so executions left running by a previous process (crash, SIGKILL, OOM) have an
            // unprovable outcome (`unknown`, reconcile scheduled) and queued ones never started.
            let recovered = services.engine().actions.recover_after_restart();
            if !recovered.marked_unknown.is_empty() || !recovered.marked_not_started.is_empty() {
                tracing::warn!(
                    "recovered {} orphaned running action(s) as unknown and {} never-started queued action(s) as failed",
                    recovered.marked_unknown.len(),
                    recovered.marked_not_started.len()
                );
            }

            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let worker_handle = services.spawn_background_workers_until(shutdown_rx.clone());
            let app = build_router(services.clone());

            let listener = tokio::net::TcpListener::bind(bind)
                .await
                .with_context(|| format!("failed to bind {bind}"))?;
            let local = listener.local_addr()?;
            // The admin CLI token is accepted only from loopback peers, so a deliberate bind to
            // one specific non-loopback interface (LAN, tailnet) also serves the same router on
            // loopback, and `daemon.json` points the CLI there (§14.1, §14.3).
            let admin_listener = bind_loopback_admin_channel(local).await?;
            let cli_addr = match admin_listener {
                Some(ref l) => l.local_addr()?,
                None => local,
            };
            let daemon_file = DaemonFile::write(&cli.data_dir, cli_addr)?;
            tracing::info!("dash serving on http://{local}");
            if admin_listener.is_some() {
                tracing::info!("local CLI admin channel on http://{cli_addr}");
            }

            let drain = Duration::from_secs(shutdown_drain_secs);
            let signal_services = services.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                tracing::info!(
                    "shutdown requested; draining for up to {}s",
                    drain.as_secs()
                );
                signal_services.server.begin_shutdown();
                let _ = shutdown_tx.send(true);
            });

            let mut graceful_rx = shutdown_rx.clone();
            let mut admin_graceful_rx = shutdown_rx.clone();
            let admin_app = app.clone();
            let admin_server = async move {
                match admin_listener {
                    Some(l) => {
                        axum::serve(
                            l,
                            admin_app.into_make_service_with_connect_info::<SocketAddr>(),
                        )
                        .with_graceful_shutdown(async move {
                            let _ = admin_graceful_rx.wait_for(|v| *v).await;
                        })
                        .await
                    }
                    None => Ok(()),
                }
            };
            let main_server = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = graceful_rx.wait_for(|v| *v).await;
            });
            let server = async move {
                tokio::try_join!(
                    std::future::IntoFuture::into_future(main_server),
                    admin_server
                )
                .map(|_| ())
            };
            let mut deadline_rx = shutdown_rx.clone();
            let drain_deadline = async move {
                let _ = deadline_rx.wait_for(|v| *v).await;
                tokio::time::sleep(drain).await;
            };
            let started_drain = Instant::now();
            tokio::select! {
                res = server => { res?; }
                _ = drain_deadline => {
                    tracing::warn!("drain limit reached; closing remaining connections (e.g. WebSockets)");
                }
            }

            // Readers are gone: stop read computations first so they free VM capacity for the
            // admitted actions still draining (§15.2).
            services.cancel_inflight_reads_for_shutdown();

            // Stop the background worker: it finishes its current iteration (e.g. an
            // automation-initiated action) within the remaining drain budget, then is aborted.
            let remaining = drain.saturating_sub(started_drain.elapsed());
            let abort = worker_handle.abort_handle();
            if tokio::time::timeout(remaining, worker_handle)
                .await
                .is_err()
            {
                abort.abort();
            }
            // Wait for admitted actions still running in detached tasks.
            while services
                .store()
                .count_inflight_action_executions()
                .unwrap_or(0)
                > 0
                && started_drain.elapsed() < drain
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let unknown_marked = services
                .engine()
                .actions
                .mark_inflight_unknown_on_shutdown();
            if unknown_marked > 0 {
                tracing::warn!(
                    "marked {unknown_marked} incomplete in-flight action(s) as unknown during shutdown"
                );
            }
            services
                .engine()
                .telemetry
                .flush_spans(Duration::from_secs(2));
            let _ = services.store().checkpoint_wal_truncate();
            drop(daemon_file);
        }

        Commands::Test => {
            std::fs::create_dir_all(&cli.data_dir)?;
            let report = run_preview_and_scenarios(&cli.workspace, &cli.data_dir, None).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.all_passed {
                bail!("workspace validation or tests failed");
            }
        }

        Commands::Manifest { ref definition_id } => {
            let graph =
                dash_git::load_workspace_graph(&cli.workspace, &cli.data_dir, "cli", 0, false)
                    .map_err(|e| anyhow::anyhow!(e))?;
            let out = match definition_id {
                Some(id) => {
                    let def = graph
                        .definitions
                        .get(id)
                        .ok_or_else(|| anyhow::anyhow!("definition '{id}' not found"))?;
                    dash_server::services::contract_json(def).map_err(|e| anyhow::anyhow!(e))?
                }
                None => {
                    let mut all = serde_json::Map::new();
                    for (id, def) in &graph.definitions {
                        all.insert(
                            id.clone(),
                            dash_server::services::contract_json(def)
                                .map_err(|e| anyhow::anyhow!(e))?,
                        );
                    }
                    Value::Object(all)
                }
            };
            println!("{}", serde_json::to_string_pretty(&out)?);
        }

        Commands::McpStdio { ref url, ref token } => {
            run_mcp_stdio_proxy(url, token).await?;
        }

        _ => run_admin_command(&cli).await?,
    }
    Ok(())
}

/// Commands that operate on the Installation: routed to the running daemon when there is one.
async fn run_admin_command(cli: &Cli) -> Result<()> {
    std::fs::create_dir_all(&cli.data_dir)?;
    // Held for the whole command so a daemon cannot start against the same files meanwhile.
    let mut _lock: Option<InstallationLock> = None;
    let target = match InstallationLock::try_acquire(&cli.data_dir)? {
        Some(lock) => {
            _lock = Some(lock);
            let services = open_services(&cli.workspace, &cli.data_dir, vec![], false).await?;
            Target::Local(Box::new(services))
        }
        None => Target::Daemon(DaemonClient::discover(&cli.data_dir)?),
    };
    let admin = Principal::administrator();
    let out: Value = match (&cli.command, &target) {
        (Commands::Reload, Target::Local(s)) => {
            json!(s
                .workspace_reload(&admin)
                .await
                .map_err(|e| anyhow::anyhow!(e))?)
        }
        (Commands::Reload, Target::Daemon(d)) => d.post("/api/workspace/reload", json!({})).await?,

        (
            Commands::Git {
                action: GitAction::Status,
            },
            Target::Local(s),
        ) => {
            json!(s.git_status().await.map_err(|e| anyhow::anyhow!(e))?)
        }
        (
            Commands::Git {
                action: GitAction::Status,
            },
            Target::Daemon(d),
        ) => d.get("/api/git/status").await?,
        (
            Commands::Git {
                action: GitAction::Sync,
            },
            Target::Local(s),
        ) => {
            json!(s.git_sync(&admin).await.map_err(|e| anyhow::anyhow!(e))?)
        }
        (
            Commands::Git {
                action: GitAction::Sync,
            },
            Target::Daemon(d),
        ) => d.post("/api/git/sync", json!({})).await?,

        (Commands::Doctor, t) => {
            let doc = match t {
                Target::Local(s) => s.workspace_doctor()?,
                Target::Daemon(d) => d.get("/api/doctor").await?,
            };
            println!("{}", serde_json::to_string_pretty(&doc)?);
            if doc.get("ok") == Some(&Value::Bool(false)) {
                bail!("doctor found problems (see failing checks above)");
            }
            return Ok(());
        }

        (Commands::Secret { action }, t) => match action {
            SecretAction::Set {
                name,
                value,
                description,
                allowed_bindings,
                clear_allowed_bindings,
                allowed_hosts,
                credential_scheme,
            } => {
                let plaintext = match value {
                    Some(v) => {
                        eprintln!(
                            "warning: --value exposes the secret in the process table and shell history; prefer piping it on stdin"
                        );
                        v.clone()
                    }
                    None => read_secret_from_stdin().await?,
                };
                // Every policy field is optional: what is not given keeps the secret's current
                // value, so rotating a value never widens its binding or destination policy.
                let hosts = (!allowed_hosts.is_empty()).then_some(allowed_hosts.as_slice());
                let bindings: Option<&[String]> = if *clear_allowed_bindings {
                    Some(&[])
                } else {
                    (!allowed_bindings.is_empty()).then_some(allowed_bindings.as_slice())
                };
                match t {
                    Target::Local(s) => json!(s
                        .secret_update(
                            name,
                            &plaintext,
                            dash_store::SecretPolicyUpdate {
                                description: description.as_deref(),
                                allowed_bindings: bindings,
                                allowed_hosts: hosts,
                                credential_scheme: credential_scheme.as_deref(),
                            },
                            &admin,
                        )
                        .map_err(|e| anyhow::anyhow!(e))?),
                    Target::Daemon(d) => {
                        let mut body = json!({
                            "name": name,
                            "plaintext": plaintext,
                        });
                        if let Some(desc) = description {
                            body["description"] = json!(desc);
                        }
                        if let Some(b) = bindings {
                            body["allowed_bindings"] = json!(b);
                        }
                        if let Some(h) = hosts {
                            body["allowed_hosts"] = json!(h);
                        }
                        if let Some(c) = credential_scheme {
                            body["credential_scheme"] = json!(c);
                        }
                        d.post("/api/secrets", body).await?
                    }
                }
            }
            SecretAction::List => match t {
                Target::Local(s) => json!(s.secret_list(&admin).map_err(|e| anyhow::anyhow!(e))?),
                Target::Daemon(d) => d.get("/api/secrets").await?,
            },
        },

        (Commands::Token { action }, t) => match action {
            TokenAction::Create {
                name,
                ceiling,
                expires_in,
            } => {
                let parsed: CapabilityCeiling = serde_json::from_str(ceiling)?;
                let secs = expires_in.as_deref().map(parse_duration_secs).transpose()?;
                match t {
                    Target::Local(s) => {
                        let exp = resolve_agent_token_expiry(secs, None)
                            .map_err(|e| anyhow::anyhow!(e))?;
                        let (rec, plaintext) = s
                            .agent_create(name, parsed, Some(exp), &admin)
                            .map_err(|e| anyhow::anyhow!(e))?;
                        json!({ "agent": rec, "token": plaintext })
                    }
                    Target::Daemon(d) => {
                        d.post(
                            "/api/agents",
                            json!({ "name": name, "ceiling": parsed, "expires_in_secs": secs }),
                        )
                        .await?
                    }
                }
            }
            TokenAction::Revoke { agent_id } => match t {
                Target::Local(s) => {
                    json!({ "revoked": s.agent_revoke(agent_id, &admin).map_err(|e| anyhow::anyhow!(e))? })
                }
                Target::Daemon(d) => {
                    d.post(&format!("/api/agents/{agent_id}/revoke"), json!({}))
                        .await?
                }
            },
        },

        (
            Commands::Grant {
                principal_id,
                ceiling,
            },
            t,
        ) => {
            let parsed: CapabilityCeiling = serde_json::from_str(ceiling)?;
            match t {
                Target::Local(s) => {
                    s.service_principal_grant(principal_id, parsed, &admin)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    json!({ "granted": true, "principal_id": principal_id })
                }
                Target::Daemon(d) => {
                    d.post(
                        &format!("/api/service-principals/{principal_id}/grant"),
                        json!(parsed),
                    )
                    .await?
                }
            }
        }
        (Commands::Network { action }, t) => match action {
            NetworkAction::Show => match t {
                Target::Local(s) => s.network_policy_status(),
                Target::Daemon(d) => d.get("/api/network-policy").await?,
            },
            NetworkAction::Grant {
                policy,
                from_workspace,
            } => {
                let parsed: Option<NetworkPolicy> = match (policy, from_workspace) {
                    (Some(raw), false) => Some(serde_json::from_str(raw)?),
                    (None, true) => None,
                    _ => bail!("pass either --policy '<json>' or --from-workspace"),
                };
                match t {
                    Target::Local(s) => {
                        let policy = parsed.unwrap_or_else(|| s.graph().workspace.network.clone());
                        s.network_policy_grant(policy, &admin)
                            .map_err(|e| anyhow::anyhow!(e))?;
                        s.network_policy_status()
                    }
                    Target::Daemon(d) => {
                        d.post(
                            "/api/network-policy/grant",
                            json!({ "policy": parsed, "from_workspace": *from_workspace }),
                        )
                        .await?
                    }
                }
            }
        },
        _ => bail!("unsupported command"),
    };
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

enum Target {
    Local(Box<AppServices>),
    Daemon(DaemonClient),
}

/// Exclusive OS file lock on `<data_dir>/daemon.lock` (released automatically on exit/crash).
struct InstallationLock {
    _file: File,
}

impl InstallationLock {
    fn try_acquire(data_dir: &Path) -> Result<Option<Self>> {
        let path = data_dir.join("daemon.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("cannot open {}", path.display()))?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self { _file: file })),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(e)) => {
                Err(anyhow::anyhow!(e).context(format!("cannot lock {}", path.display())))
            }
        }
    }
}

/// When `serve` binds one specific non-loopback address, bind a loopback listener for the local
/// CLI admin channel: the same port when free, otherwise an ephemeral one. Wildcard and loopback
/// binds already accept loopback connections and need none.
async fn bind_loopback_admin_channel(local: SocketAddr) -> Result<Option<tokio::net::TcpListener>> {
    let ip = local.ip();
    if ip.is_loopback() || ip.is_unspecified() {
        return Ok(None);
    }
    let lo = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    let listener = match tokio::net::TcpListener::bind(SocketAddr::new(lo, local.port())).await {
        Ok(l) => l,
        Err(_) => tokio::net::TcpListener::bind(SocketAddr::new(lo, 0))
            .await
            .context("failed to bind the loopback CLI admin channel")?,
    };
    Ok(Some(listener))
}

/// `<data_dir>/daemon.json`: where the running daemon can be reached over loopback.
struct DaemonFile {
    path: PathBuf,
}

impl DaemonFile {
    fn write(data_dir: &Path, local: SocketAddr) -> Result<Self> {
        let ip = match local.ip() {
            IpAddr::V4(v4) if v4.is_unspecified() => IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            IpAddr::V6(v6) if v6.is_unspecified() => IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            other => other,
        };
        let url = format!("http://{}", SocketAddr::new(ip, local.port()));
        let path = data_dir.join("daemon.json");
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&json!({ "url": url, "pid": std::process::id() }))?,
        )?;
        Ok(Self { path })
    }
}

impl Drop for DaemonFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct DaemonClient {
    base: String,
    token: String,
    http: reqwest::Client,
}

impl DaemonClient {
    fn discover(data_dir: &Path) -> Result<Self> {
        let info: Value = serde_json::from_str(
            &std::fs::read_to_string(data_dir.join("daemon.json"))
                .context("a daemon holds the Installation lock but daemon.json is missing")?,
        )?;
        let base = info["url"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("daemon.json has no url"))?
            .trim_end_matches('/')
            .to_string();
        let token = std::fs::read_to_string(data_dir.join("admin.token"))
            .context("cannot read the local admin CLI token (run `dash init`)")?
            .trim()
            .to_string();
        Ok(Self {
            base,
            token,
            http: reqwest::Client::new(),
        })
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<Value> {
        let resp = req
            .bearer_auth(&self.token)
            .send()
            .await
            .with_context(|| format!("cannot reach the running daemon at {}", self.base))?;
        let status = resp.status();
        let text = resp.text().await?;
        let body: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));
        if !status.is_success() {
            let msg = body
                .get("error")
                .and_then(|e| e.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| body.to_string());
            bail!("daemon returned {status}: {msg}");
        }
        Ok(body)
    }

    async fn get(&self, path: &str) -> Result<Value> {
        self.send(self.http.get(format!("{}{path}", self.base)))
            .await
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value> {
        self.send(self.http.post(format!("{}{path}", self.base)).json(&body))
            .await
    }
}

fn generate_admin_password() -> String {
    // 24 random bytes, hex encoded (48 chars): never a well-known default (§14.3).
    generate_random_token("pw")
        .trim_start_matches("pw_")
        .to_string()
}

/// Parse `3600`, `90s`, `30m`, `12h`, `30d` into seconds.
fn parse_duration_secs(raw: &str) -> Result<i64> {
    let t = raw.trim();
    let (num, mult) = match t.chars().last() {
        Some('s') => (&t[..t.len() - 1], 1),
        Some('m') => (&t[..t.len() - 1], 60),
        Some('h') => (&t[..t.len() - 1], 3600),
        Some('d') => (&t[..t.len() - 1], 86400),
        _ => (t, 1),
    };
    let n: i64 = num
        .trim()
        .parse()
        .with_context(|| format!("invalid duration '{raw}' (e.g. 3600, 12h, 30d)"))?;
    if n <= 0 {
        bail!("duration must be positive");
    }
    Ok(n * mult)
}

async fn read_secret_from_stdin() -> Result<String> {
    let mut buf = String::new();
    tokio::io::stdin().read_to_string(&mut buf).await?;
    let v = buf.trim_end_matches(['\n', '\r']).to_string();
    if v.is_empty() {
        bail!("no secret value on stdin (pipe it in, e.g. `printf %s \"$TOKEN\" | dash secret set NAME`)");
    }
    Ok(v)
}

async fn open_services(
    workspace: &Path,
    data_dir: &Path,
    trusted_proxies: Vec<IpAddr>,
    use_mocks: bool,
) -> Result<AppServices> {
    let store = SqliteStore::open(data_dir)?;
    let mgr = DraftManager::initialize_with_mocks(
        workspace.to_path_buf(),
        data_dir.to_path_buf(),
        store,
        ResourceBudget::default(),
        use_mocks,
    )
    .await?;
    Ok(AppServices::new(mgr, trusted_proxies))
}

// ---- stdio MCP proxy (§11.1) ----

#[derive(Default)]
struct ProxySession {
    session_id: Option<String>,
    protocol_version: Option<String>,
}

/// JSON-RPC ids of the requests in a message (single or batch); notifications have none.
fn request_ids(msg: &Value) -> Vec<Value> {
    let one = |m: &Value| -> Option<Value> {
        if m.get("method").is_some() {
            m.get("id").cloned().filter(|id| !id.is_null())
        } else {
            None
        }
    };
    match msg {
        Value::Array(items) => items.iter().filter_map(one).collect(),
        other => one(other).into_iter().collect(),
    }
}

fn rpc_error_line(id: &Value, code: i64, message: &str) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }).to_string()
}

/// Forward one stdin message to the daemon and return the stdout lines to emit.
async fn forward_mcp_message(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    session: &tokio::sync::Mutex<ProxySession>,
    msg: Value,
) -> Vec<String> {
    let ids = request_ids(&msg);
    let is_initialize = msg.get("method").and_then(|m| m.as_str()) == Some("initialize");
    let (sid, ver) = {
        let s = session.lock().await;
        (s.session_id.clone(), s.protocol_version.clone())
    };
    let mut req = client
        .post(url)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        // Server-side marker: the daemon refuses non-Agent principals on this transport.
        .header("x-dash-transport", "stdio");
    if !is_initialize {
        req = req.header(
            "mcp-protocol-version",
            ver.as_deref().unwrap_or(DEFAULT_MCP_PROTOCOL),
        );
        if let Some(ref sid) = sid {
            req = req.header("mcp-session-id", sid);
        }
    }
    let resp = match req.body(msg.to_string()).send().await {
        Ok(r) => r,
        Err(e) => {
            let m = format!("dash daemon unreachable at {url}: {e}");
            return ids
                .iter()
                .map(|id| rpc_error_line(id, -32000, &m))
                .collect();
        }
    };
    let status = resp.status();
    let new_sid = resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let body = resp.text().await.unwrap_or_default();
    if status == reqwest::StatusCode::ACCEPTED || (body.trim().is_empty() && ids.is_empty()) {
        return vec![];
    }
    if status == reqwest::StatusCode::NOT_FOUND && sid.is_some() {
        // Session expired/terminated (e.g. daemon restart): the client must re-initialize.
        let mut s = session.lock().await;
        s.session_id = None;
    }
    let parsed: Option<Value> = serde_json::from_str(&body).ok();
    if status.is_success() {
        if let Some(v) = parsed {
            if is_initialize {
                let mut s = session.lock().await;
                s.session_id = new_sid.or(s.session_id.take());
                s.protocol_version = v
                    .pointer("/result/protocolVersion")
                    .and_then(|p| p.as_str())
                    .map(|p| p.to_string());
            }
            // Re-serialize compactly: exactly one JSON value per stdout line.
            return vec![v.to_string()];
        }
    }
    // Non-2xx or non-JSON body: synthesize a JSON-RPC error for each request id.
    let detail = parsed
        .as_ref()
        .and_then(|v| v.pointer("/error/message").or_else(|| v.get("error")))
        .and_then(|e| e.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| body.chars().take(500).collect());
    let code = parsed
        .as_ref()
        .and_then(|v| v.pointer("/error/code"))
        .and_then(|c| c.as_i64())
        .unwrap_or(-32000);
    let m = format!("HTTP {status}: {detail}");
    ids.iter().map(|id| rpc_error_line(id, code, &m)).collect()
}

/// Proxy newline-delimited JSON-RPC over stdin/stdout to a running daemon's `/mcp` endpoint.
/// Enforces §11.1: stdio MCP maps strictly to an explicitly configured Agent token, never
/// Administrator. The daemon enforces the same rule via the `X-Dash-Transport: stdio` marker.
pub async fn run_mcp_stdio_proxy(url: &str, token: &str) -> Result<()> {
    let trimmed = token.trim().to_string();
    if trimmed.is_empty() || !trimmed.starts_with("dsh_agt_") {
        bail!(
            "stdio MCP requires an explicit Agent MCP token ('dsh_agt_...'); never runs as Administrator (§11.1)"
        );
    }

    let client = reqwest::Client::new();
    let session = Arc::new(tokio::sync::Mutex::new(ProxySession::default()));
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(256);
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(line) = out_rx.recv().await {
            if stdout.write_all(line.as_bytes()).await.is_err()
                || stdout.write_all(b"\n").await.is_err()
                || stdout.flush().await.is_err()
            {
                break;
            }
        }
    });
    // Bounded concurrency so a long draft_preview does not block pings or cancellations.
    let permits = Arc::new(tokio::sync::Semaphore::new(16));
    let mut tasks = tokio::task::JoinSet::new();
    let url = Arc::new(url.to_string());
    let token = Arc::new(trimmed);

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    while let Some(line) = lines.next_line().await? {
        let trimmed_line = line.trim();
        if trimmed_line.is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(trimmed_line) {
            Ok(v) => v,
            Err(e) => {
                let _ = out_tx
                    .send(rpc_error_line(
                        &Value::Null,
                        -32700,
                        &format!("Parse error: {e}"),
                    ))
                    .await;
                continue;
            }
        };
        let is_initialize = msg.get("method").and_then(|m| m.as_str()) == Some("initialize");
        if is_initialize {
            // Serialize initialize so the negotiated session/version apply to what follows.
            for l in forward_mcp_message(&client, &url, &token, &session, msg).await {
                let _ = out_tx.send(l).await;
            }
            continue;
        }
        let permit = permits.clone().acquire_owned().await?;
        let (client, url, token, session, out_tx) = (
            client.clone(),
            url.clone(),
            token.clone(),
            session.clone(),
            out_tx.clone(),
        );
        tasks.spawn(async move {
            let _permit = permit;
            for l in forward_mcp_message(&client, &url, &token, &session, msg).await {
                let _ = out_tx.send(l).await;
            }
        });
        while tasks.try_join_next().is_some() {}
    }
    while tasks.join_next().await.is_some() {}
    drop(out_tx);
    let _ = writer.await;
    Ok(())
}

/// Resolve on SIGINT (Ctrl-C) or SIGTERM (`docker stop`, systemd).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration_secs("3600").unwrap(), 3600);
        assert_eq!(parse_duration_secs("12h").unwrap(), 43200);
        assert_eq!(parse_duration_secs("30d").unwrap(), 30 * 86400);
        assert!(parse_duration_secs("0").is_err());
        assert!(parse_duration_secs("abc").is_err());
    }

    #[test]
    fn request_ids_skip_notifications_and_responses() {
        let batch = json!([
            { "jsonrpc": "2.0", "id": 1, "method": "ping" },
            { "jsonrpc": "2.0", "method": "notifications/initialized" },
            { "jsonrpc": "2.0", "id": 5, "result": {} }
        ]);
        assert_eq!(request_ids(&batch), vec![json!(1)]);
        assert!(
            request_ids(&json!({ "jsonrpc": "2.0", "method": "notifications/cancelled" }))
                .is_empty()
        );
    }

    #[test]
    fn installation_lock_is_exclusive() {
        let dir = tempfile::TempDir::new().unwrap();
        let first = InstallationLock::try_acquire(dir.path()).unwrap();
        assert!(first.is_some());
        assert!(InstallationLock::try_acquire(dir.path()).unwrap().is_none());
        drop(first);
        assert!(InstallationLock::try_acquire(dir.path()).unwrap().is_some());
    }

    #[test]
    fn generated_password_is_long_and_random() {
        let a = generate_admin_password();
        let b = generate_admin_password();
        assert!(a.len() >= 32);
        assert_ne!(a, b);
        assert_ne!(a, "admin");
    }
}
