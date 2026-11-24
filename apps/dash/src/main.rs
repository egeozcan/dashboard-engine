//! Single-executable `dash` CLI and daemon (§14.1).

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use dash_core::{CapabilityCeiling, Principal};
use dash_git::{run_preview_and_scenarios, DraftManager};
use dash_luau::ResourceBudget;
use dash_server::{build_router, generate_random_token, hash_password, AppServices};
use dash_store::SqliteStore;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
        #[arg(long, env = "DASH_ADMIN_PASSWORD", default_value = "admin")]
        admin_password: String,
        #[arg(long)]
        from_fixture: Option<PathBuf>,
    },
    /// Start HTTP, WebSocket, MCP, and background scheduler daemon
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: SocketAddr,
        #[arg(long = "trusted-proxy", default_value = "127.0.0.1")]
        trusted_proxies: Vec<IpAddr>,
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
    /// Inspect SQLite, Git, manifests, capabilities, and Service principal grants
    Doctor,
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
        #[arg(long)]
        value: String,
        #[arg(long, default_value = "")]
        description: String,
        #[arg(long = "allowed-binding")]
        allowed_bindings: Vec<String>,
    },
    List,
}

#[derive(Subcommand)]
enum TokenAction {
    Create {
        name: String,
        #[arg(long, default_value = "{}")]
        ceiling: String,
    },
    Revoke {
        agent_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "dash=info,dash_server=info,dash_core=info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Init {
            username,
            admin_password,
            from_fixture,
        } => {
            std::fs::create_dir_all(&cli.workspace)?;
            std::fs::create_dir_all(&cli.data_dir)?;
            if let Some(ref src) = from_fixture {
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
            let pw_hash = hash_password(&admin_password).map_err(|e| anyhow::anyhow!(e))?;
            store.set_admin_credentials(&username, &pw_hash)?;

            let admin_tok_path = cli.data_dir.join("admin.token");
            if !admin_tok_path.exists() {
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
            trusted_proxies,
        } => {
            let services = open_services(&cli.workspace, &cli.data_dir, trusted_proxies).await?;
            let worker_handle = services.spawn_background_workers();
            let app = build_router(services.clone());

            let listener = tokio::net::TcpListener::bind(bind)
                .await
                .with_context(|| format!("failed to bind {bind}"))?;
            tracing::info!("dash serving on http://{bind}");

            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown_signal())
            .await?;

            worker_handle.abort();
            let unknown_marked = services
                .engine()
                .actions
                .mark_inflight_unknown_on_shutdown();
            if unknown_marked > 0 {
                tracing::warn!(
                    "marked {unknown_marked} incomplete in-flight action(s) as unknown during shutdown"
                );
            }
        }

        Commands::Test => {
            std::fs::create_dir_all(&cli.data_dir)?;
            let report = run_preview_and_scenarios(&cli.workspace, &cli.data_dir, None).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if !report.all_passed {
                bail!("workspace validation or tests failed");
            }
        }

        Commands::Reload => {
            let services = open_services(&cli.workspace, &cli.data_dir, vec![]).await?;
            let act = services
                .drafts
                .reload_workspace(&Principal::administrator())
                .await?;
            println!("{}", serde_json::to_string_pretty(&act)?);
        }

        Commands::Git { action } => {
            let services = open_services(&cli.workspace, &cli.data_dir, vec![]).await?;
            match action {
                GitAction::Status => {
                    let s = services.git_status().await.map_err(|e| anyhow::anyhow!(e))?;
                    println!("{}", serde_json::to_string_pretty(&s)?);
                }
                GitAction::Sync => {
                    let s = services
                        .git_sync(&Principal::administrator())
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?;
                    println!("{}", serde_json::to_string_pretty(&s)?);
                }
            }
        }

        Commands::Doctor => {
            let services = open_services(&cli.workspace, &cli.data_dir, vec![]).await?;
            let doc = services.workspace_doctor()?;
            println!("{}", serde_json::to_string_pretty(&doc)?);
        }

        Commands::McpStdio { url, token } => {
            run_mcp_stdio_proxy(&url, &token).await?;
        }

        Commands::Secret { action } => {
            let services = open_services(&cli.workspace, &cli.data_dir, vec![]).await?;
            let admin = Principal::administrator();
            match action {
                SecretAction::Set {
                    name,
                    value,
                    description,
                    allowed_bindings,
                } => {
                    let meta = services
                        .secret_set(&name, &description, &value, &allowed_bindings, &admin)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    println!("{}", serde_json::to_string_pretty(&meta)?);
                }
                SecretAction::List => {
                    let list = services.secret_list(&admin).map_err(|e| anyhow::anyhow!(e))?;
                    println!("{}", serde_json::to_string_pretty(&list)?);
                }
            }
        }

        Commands::Token { action } => {
            let services = open_services(&cli.workspace, &cli.data_dir, vec![]).await?;
            let admin = Principal::administrator();
            match action {
                TokenAction::Create { name, ceiling } => {
                    let parsed: CapabilityCeiling = serde_json::from_str(&ceiling)?;
                    let (rec, plaintext) = services
                        .agent_create(&name, parsed, None, &admin)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "agent": rec,
                            "token": plaintext,
                        }))?
                    );
                }
                TokenAction::Revoke { agent_id } => {
                    let ok = services
                        .agent_revoke(&agent_id, &admin)
                        .map_err(|e| anyhow::anyhow!(e))?;
                    println!("revoked: {ok}");
                }
            }
        }

        Commands::Grant {
            principal_id,
            ceiling,
        } => {
            let services = open_services(&cli.workspace, &cli.data_dir, vec![]).await?;
            let parsed: CapabilityCeiling = serde_json::from_str(&ceiling)?;
            services
                .service_principal_grant(&principal_id, parsed, &Principal::administrator())
                .map_err(|e| anyhow::anyhow!(e))?;
            println!("Granted ceiling to service principal '{principal_id}'");
        }
    }
    Ok(())
}

async fn open_services(
    workspace: &PathBuf,
    data_dir: &PathBuf,
    trusted_proxies: Vec<IpAddr>,
) -> Result<AppServices> {
    let store = SqliteStore::open(data_dir)?;
    let mgr = DraftManager::initialize(
        workspace.clone(),
        data_dir.clone(),
        store,
        ResourceBudget::default(),
    )
    .await?;
    Ok(AppServices::new(mgr, trusted_proxies))
}

/// Proxy newline-delimited JSON-RPC over stdin/stdout to a running daemon's `/mcp` endpoint.
/// Enforces §11.1: stdio MCP maps strictly to an explicitly configured Agent token, never Administrator!
pub async fn run_mcp_stdio_proxy(url: &str, token: &str) -> Result<()> {
    let trimmed = token.trim();
    if trimmed.is_empty() || !trimmed.starts_with("dsh_agt_") {
        bail!(
            "stdio MCP requires an explicit Agent MCP token ('dsh_agt_...'); never runs as Administrator (§11.1)"
        );
    }

    let client = reqwest::Client::new();
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = tokio::io::stdout();
    let mut session_id: Option<String> = None;

    while let Some(line) = lines.next_line().await? {
        let trimmed_line = line.trim();
        if trimmed_line.is_empty() {
            continue;
        }
        let mut req = client
            .post(url)
            .header("authorization", format!("Bearer {trimmed}"))
            .header("content-type", "application/json")
            .header("mcp-protocol-version", "2026-07-28");
        if let Some(ref sid) = session_id {
            req = req.header("mcp-session-id", sid);
        }
        let resp = req.body(trimmed_line.to_string()).send().await?;
        if let Some(sid) = resp
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
        {
            session_id = Some(sid.to_string());
        }
        let body = resp.text().await?;
        if !body.trim().is_empty() {
            stdout.write_all(body.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
