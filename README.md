# `dash` — Universal Dashboard Engine

A single-executable, self-hosted, reactive dashboard engine written in Rust with sandboxed Luau extensibility, Git-backed transactional configuration, encrypted central secrets, an embedded React/TypeScript visual studio, and an authenticated Streamable HTTP + stdio MCP interface.

- Specification: [`specs/dashboard-engine-spec-v0.2.md`](./specs/dashboard-engine-spec-v0.2.md)
- Canonical vocabulary: [`GLOSSARY.md`](./GLOSSARY.md)
- Architectural Decision Records: [`docs/adr/`](./docs/adr/)

## Architecture

| Crate / App | Responsibility |
| --- | --- |
| `crates/dash-store` | SQLite WAL storage engine, transactional migrations, AES-256-GCM secret vault (`DASH_MASTER_KEY`, `DASH_MASTER_KEY_FILE` or passphrase-wrapped `<data_dir>/vault.key`), redaction |
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

# 2. Initialize a Workspace from the Engineering Command Center fixture.
#    There is no default password: pass one (>= 8 chars) or let `init` generate and print one once.
#    Re-running `init` never overwrites existing credentials unless --reset-admin-password is given.
./target/release/dash \
  --workspace ./my-workspace \
  --data-dir ./my-data \
  init --from-fixture ./fixtures/engineering-command-center --admin-password "change-me-please"

# 3. Validate and run all Definition & Scenario tests against Mocks
./target/release/dash --workspace ./my-workspace --data-dir ./my-data test

# 4. Run the daemon on loopback (default 127.0.0.1:8080)
./target/release/dash --workspace ./my-workspace --data-dir ./my-data serve --bind 127.0.0.1:8080
```

### CLI and the running daemon (§14.1)

`dash serve` takes an exclusive lock on `<data_dir>/daemon.lock` and publishes its loopback URL in
`<data_dir>/daemon.json`. While it runs, `dash reload`, `dash git status|sync`, `dash doctor`,
`dash secret`, `dash token` and `dash grant` call the daemon's HTTP API using the local admin CLI
token (`<data_dir>/admin.token`, accepted only from direct loopback connections without proxy
headers; rotate with `dash init --rotate-admin-token`). Without a daemon they run in-process under
the same lock. A second daemon against the same data directory is refused.

```bash
dash doctor                                   # integrity, journal mode, vault, journal, Git sync, budgets; non-zero exit on failure
dash manifest engineering.linked_prs          # generated manifest + JSON Schemas (all Definitions when no id)
printf %s "$GITHUB_TOKEN" | dash secret set github-work --allowed-binding github_reader   # value from stdin, not argv
dash token create ci-bot --ceiling '{"allowed_providers":["*"],"can_author_drafts":true}' --expires-in 30d
```

Agent tokens always expire (default 90 days, maximum 365 days; `expires_in_secs` on `POST /api/agents`).

`dash serve` drains on SIGINT/SIGTERM: it stops admitting new state-changing requests and actions,
waits up to `--shutdown-drain-secs` (default 30) for in-flight requests, automations and actions,
then marks anything unproven as `unknown`. On startup, executions left `queued`/`running` by a
crashed process are marked `unknown` (`crash_recovery`).

Storage is bounded: trace spans (7 d), audit records (365 d), consumed Events (30 d, newest per
source kept), terminal action executions (90 d), resolved dead letters, expired sessions and sampled
runtime metrics are pruned every 5 minutes, the WAL is truncated past 64 MiB, and a 1 GiB total
SQLite budget (db + WAL) trims diagnostics first and is reported by `dash doctor`.

### Tailscale Serve Exposure (§14.2)

Expose the local daemon privately to your tailnet without public ingress or Tailscale Funnel:

```bash
dash serve --bind 127.0.0.1:8080 --trusted-proxy 127.0.0.1 --allowed-host my-box.my-tailnet.ts.net
tailscale serve --bg 8080
```

Every request's `Host` must be on the allowlist (loopback names are always allowed; add your
MagicDNS name with `--allowed-host`, or `.my-tailnet.ts.net` for any subdomain). This is the
DNS-rebinding defense for the API, `/ws`, `/mcp` and login. Browser `Origin` must match the
request origin exactly (scheme, host and port). Login rate limiting keys on the forwarded client
address from trusted proxies, so one tailnet device cannot lock out another.

Built-in Argon2id login, `HttpOnly`/`SameSite=Lax` sessions, CSRF validation, and scoped Agent MCP tokens remain enforced regardless of tailnet membership; spoofed identity headers (`Tailscale-User-*`, `X-Forwarded-User`) are never trusted. Session cookies are `Secure` when the request arrived over HTTPS (trusted proxy `X-Forwarded-Proto`); use `--secure-cookies always` if your HTTPS proxy does not send that header.

### MCP Integration

- **Streamable HTTP:** `POST /mcp` with `Authorization: Bearer <agent_token>` (`dsh_agt_...`).
  Notifications get `202 Accepted`; batches are accepted; `initialize` returns a Principal-bound
  `Mcp-Session-Id` (terminate with `DELETE /mcp`); every tool publishes a JSON Schema for its
  arguments; `structuredContent` is always an object (arrays are wrapped as `{"items": [...]}`).
- **Resources:** `dash://manifests`, `dash://definitions/{id}`, `dash://graph`,
  `dash://graph/dependencies`, `dash://drafts/{id}/validation`, `dash://drafts/{id}/preview-report`,
  `dash://traces`, `dash://logs`, `dash://metrics`, `dash://docs/{glossary,mcp-tools,adr-*}`.
- **Stdio proxy:**
  ```bash
  dash mcp-stdio --url http://127.0.0.1:8080/mcp --token dsh_agt_...
  ```
  `mcp-stdio` strictly requires an explicit Agent token; it marks its requests with
  `X-Dash-Transport: stdio` and the daemon refuses any non-Agent principal on that transport.
  Every stdout line is a JSON-RPC message (HTTP or transport failures become JSON-RPC errors).

### Docker

```bash
docker build -t dash:0.2.0 .
docker run --rm -p 127.0.0.1:8080:8080 \
  --user "$(id -u):$(id -g)" \
  -e DASH_ADMIN_PASSWORD="change-me-please" \
  -e DASH_VAULT_PASSPHRASE="a long vault passphrase" \
  -v "$(pwd)/my-workspace:/workspace" \
  -v "$(pwd)/my-data:/data" \
  dash:0.2.0
```

Inside the container the daemon binds `0.0.0.0:8080` (required for `-p` publishing); keep the host
side `127.0.0.1:` so exposure stays deliberate. On first start, the entrypoint runs `dash init`
(without `DASH_ADMIN_PASSWORD` a random password is printed once to the container log; set
`DASH_INIT_FROM_FIXTURE=/opt/dash/fixtures/engineering-command-center` to seed the Workspace).
The vault refuses to keep its master key in plaintext next to the encrypted secrets in `/data`
(§5.2): pass `DASH_VAULT_PASSPHRASE` (the key file is Argon2id-wrapped), `DASH_MASTER_KEY`, or
`DASH_MASTER_KEY_FILE` on a separately mounted secret volume. `DASH_ALLOW_COLOCATED_VAULT_KEY=1`
accepts the co-located plaintext key explicitly (`dash doctor` keeps warning about it).
The image runs as an unprivileged user under `tini`, so `docker stop` triggers the graceful drain.
If a reverse proxy on the Docker host terminates TLS, pass its bridge address, e.g.
`serve --bind 0.0.0.0:8080 --trusted-proxy 172.17.0.1 --allowed-host dash.example.internal`.

## Measured Benchmark Gates (§15.3)

Measured with `cargo test --release -p dash --test benchmarks -- --nocapture` (prints one
`BENCHMARK_REPORT=` JSON line) against [`apps/dash/tests/benchmarks.rs`](./apps/dash/tests/benchmarks.rs).
Hardware: **Apple M4 Pro, 14 cores, 48 GiB RAM, macOS 15.6.1 (Darwin 24.6.0, arm64)**, rustc 1.97.1.
This is not the §15.3 reference Linux x86-64 machine, so the RSS figure is indicative only.
Numbers are from one run; the range over three consecutive runs is in brackets.

| Metric (§15.3) | Target | Method | Measured (`--release`) |
| --- | --- | --- | --- |
| Fresh daemon startup | `<= 2 s` to `/healthz` | spawned `dash serve` on a fresh `dash init` Workspace (0 Instances); spawn until the first `200`; max of 3 runs | **37.95 ms** [37.9 – 43.7] |
| Baseline idle RSS | `<= 100 MiB` | `ps -o rss` of that daemon 1.5 s after it is ready; max of 3 runs; fails if RSS cannot be read | **19.53 MiB** [19.5 – 19.9] |
| Warm in-memory provider cache query | p95 `<= 5 ms` | 1,000 in-process `provider_query` cache hits on `open_prs` (asserted: no Luau execution) | **0.0042 ms** [0.0042 – 0.0043] |
| Signal propagation | p95 `<= 100 ms` | already-computed snapshot published to 100 in-process `SignalHub` subscribers, until all 100 received it; 50 rounds (no WebSocket serialization) | **0.0152 ms** [0.0151 – 0.0156] |
| Shared query deduplication | 1 computation | 100 identical concurrent `force_refresh` queries, all joined before the provider is released by a gate; counted by the provider in Instance State and by the VM pool | **1 computation** (0.90 ms for all 100) |
| Failure isolation | no stall (gated here at p95 `<= 50 ms` for `/healthz`, `<= 100 ms` for the unrelated provider; §15.3 sets no number) | while an actual `while true do end` Instance (80 queued queries, more than the 64 pool permits) and an actual Instance holding 5 MiB of a 16 MiB budget run: 100 × `GET /healthz` and 100 × `POST /api/queries/open_prs` with `force_refresh` (Luau plus Mock connector each time) over HTTP. Then the hog exceeds its limit (`memory budget exceeded`) and the loop is interrupted (`timed out` after 1.17 s, when it reached the 50 M interrupt budget before its 3 s deadline) | `/healthz` p95 **0.124 ms** (idle baseline 0.118 ms); unrelated provider p95 **0.787 ms** (idle baseline 0.400 ms) |
| Idle VM reclamation | back to baseline after the TTL | `idle_ttl_secs = 1`; VMs used last are kept by an immediate `reclaim_idle`; the pool then returns to 0 VMs | **8 VMs reclaimed, 1,006 ms** after last use; cold re-boot query 0.41 ms |
| Live graph replacement | no partial graph; old graph kept on failure | 4 readers query a two-Instance pair (`b` reads `a`) during a draft activation that changes both; then a draft whose runtime fails to initialise is activated | **0 mixed results** in 31,261 reader queries (p95 0.091 ms); activation 218 ms; failed activation kept the Generation |

`cargo test` (debug) runs the same scenarios. Functional gates (one computation, interruption,
reclamation, no mixed Generation) are identical in both profiles. Latency bounds are multiplied by
10 in unoptimized builds (`debug_slack()` in `apps/dash/tests/common_t4/mod.rs`), so only the
`--release` run enforces the §15.3 numbers. The debug criterion 17 sanity check
(`ac17_fresh_daemon_and_warm_query_sanity` in `apps/dash/tests/ac_15_17.rs`) times the same
zero-Instance spawned daemon.
