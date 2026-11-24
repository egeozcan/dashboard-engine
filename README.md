# `dash` — Universal Dashboard Engine

A single-executable, self-hosted, reactive dashboard engine written in Rust with sandboxed Luau extensibility, Git-backed transactional configuration, encrypted central secrets, an embedded React/TypeScript visual studio, and an authenticated Streamable HTTP + stdio MCP interface.

- Specification: [`specs/dashboard-engine-spec-v0.2.md`](./specs/dashboard-engine-spec-v0.2.md)
- Canonical vocabulary: [`GLOSSARY.md`](./GLOSSARY.md)
- Architectural Decision Records: [`docs/adr/`](./docs/adr/)

## Architecture

| Crate / App | Responsibility |
| --- | --- |
| `crates/dash-store` | SQLite WAL storage engine, transactional migrations, AES-256-GCM secret vault (`DASH_MASTER_KEY` / `<data_dir>/vault.key`), redaction |
| `crates/dash-luau` | Sandboxed `mlua` Luau runtime, restricted deterministic declaration evaluator, `.d.luau` typegen, async coroutine bridge (`ctx:await`), quotas & VM pool |
| `crates/dash-core` | Contracts, Principal capability broker, host-mediated SSRF-checked HTTP broker, singleflight query engine, Signals (`dash.query/1`), Events, History, Actions (`DryRun`, `reconcile`, `resolve`), Automations |
| `crates/dash-git` | Canonical Workspace serializer, transitive Git Package lockfile (`packages.lock`), offline package cache, isolated Mock Preview & YAML Scenario runner, concurrent Draft worktrees, 3-way rebase, Generation carry-over, activation journal |
| `crates/dash-server` | Shared `AppServices`, Axum REST API, Argon2id session + CSRF + Origin + trusted-proxy auth, WebSocket Signals & `dash.widget/1` Requests, MCP Streamable HTTP (`/mcp`, 35 tools + resources), isolated custom widget CSP serving, `rust-embed` UI assets |
| `apps/dash` | Single executable CLI & daemon (`dash`, 20 MiB release binary) |
| `web` | React + TypeScript + Vite visual development UI (Dashboards & Designer, Dependency Graph Editor, Package Studio, Execution Graph & Security Admin) |
| `fixtures/engineering-command-center` | Full §16 Engineering Command Center Workspace (GitHub + Jira + Mocks + custom sandboxed widget + YAML Scenarios) |

## Quickstart

```bash
# 1. Build web assets and release binary
pnpm --dir web install
pnpm --dir web build
cargo build --release --bin dash

# 2. Initialize a Workspace from the Engineering Command Center fixture
./target/release/dash \
  --workspace ./my-workspace \
  --data-dir ./my-data \
  init --from-fixture ./fixtures/engineering-command-center --admin-password "change-me"

# 3. Validate and run all Definition & Scenario tests against Mocks
./target/release/dash --workspace ./my-workspace --data-dir ./my-data test

# 4. Run the daemon on loopback (default 127.0.0.1:8080)
./target/release/dash --workspace ./my-workspace --data-dir ./my-data serve --bind 127.0.0.1:8080
```

### Tailscale Serve Exposure (§14.2)

Expose the local daemon privately to your tailnet without public ingress or Tailscale Funnel:

```bash
dash serve --bind 127.0.0.1:8080 --trusted-proxy 127.0.0.1
tailscale serve --bg 8080
```

Built-in Argon2id login, `HttpOnly`/`SameSite=Lax` sessions, CSRF validation, and scoped Agent MCP tokens remain enforced regardless of tailnet membership; spoofed identity headers (`Tailscale-User-*`, `X-Forwarded-User`) are never trusted.

### MCP Integration

- **Streamable HTTP:** `POST /mcp` with `Authorization: Bearer <agent_token>` (`dsh_agt_...`).
- **Stdio proxy:**
  ```bash
  dash mcp-stdio --url http://127.0.0.1:8080/mcp --token dsh_agt_...
  ```
  `mcp-stdio` strictly requires an explicit Agent token and refuses Administrator tokens.

### Docker

```bash
docker build -t dash:0.2.0 .
docker run --rm -p 127.0.0.1:8080:8080 \
  -v "$(pwd)/my-workspace:/workspace" \
  -v "$(pwd)/my-data:/data" \
  dash:0.2.0
```

## Measured Benchmark Gates (§15.3)

Measured in `--release` mode via `cargo test --release -p dash --test benchmarks -- --nocapture` on **Apple M4 Pro (`Darwin 24.6.0 arm64`, 48 GiB RAM)** against [`apps/dash/tests/benchmarks.rs`](./apps/dash/tests/benchmarks.rs):

| Metric | §15.3 Target | Measured (`--release`) |
| --- | --- | --- |
| Fresh daemon startup to `/healthz` (0 Instances) | `<= 2,000 ms` | **32.03 ms** |
| Baseline idle RSS (0 active subscriptions) | `<= 100 MiB` | **13.97 MiB** |
| Warm in-memory provider cache query (p95, 200 samples) | `<= 5 ms` | **0.005 ms** |
| Signal propagation to 100 local subscribers (p95) | `<= 100 ms` | **0.022 ms** |
| `/healthz` latency under load | fast isolation | **0.401 ms** |
