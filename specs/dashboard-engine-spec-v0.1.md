# Universal Dashboard Engine

**Technical Product Specification**  
**Version:** 0.1.0 (implementation baseline)  
**Date:** 2026-11-23  
**Status:** Architecture agreed; interfaces proposed; not yet implemented  
**Working name:** `dash` (provisional)

> This specification records the product decisions made to date, proposes concrete implementation contracts, and defines a testable MVP. Sections marked **Proposed** are implementation choices rather than decisions already validated through a prototype. All code using `dash` is proposed API syntax.

## 1. Product definition

A universal, high-performance, self-hosted dashboard engine implemented primarily in Rust. Its extensibility model uses **Luau plugins** to provide connectors, data providers, action providers, adapters, widget providers, reusable libraries, and automations. A reactive dependency graph shares computation and propagates updates. A visual editor and an MCP server allow human users and autonomous agents to create, inspect, test, version, and operate dashboards.

One Rust executable serves the web application, HTTP API, WebSocket updates, MCP endpoints, runtime, and local storage. The expected installation lives inside a **Tailscale tailnet**, accessed with Tailscale Serve, with no public ingress required.

### 1.1 Goals

1. **Universal composition:** Connect arbitrary data sources, reusable domain transformations, UI widgets, and privileged actions.
2. **Performance by design:** Small idle footprint; lazy execution; shared queries; bounded resources; incremental updates and efficient paths for large datasets.
3. **Developer-friendly plugin model:** Luau scripts with generated, discoverable contracts, gradual typing, reusable libraries, tests, and hot reload.
4. **Explicit security:** Capability-based authorization, centrally managed secrets, sandboxed script and widget execution, scoped agent permissions, audit trails.
5. **Agent-native operations:** Full autonomous management through MCP within an administrator-defined capability ceiling.
6. **Reproducible configuration:** Git-backed canonical configuration and plugin source, exact external dependency pins, transactional changes and automatic commits/pushes.
7. **Usable out of the box:** Built-in table, chart, map, counter, layout, and form primitives; visual editing and diagnostics.
8. **One-person deployment:** Single-user, self-hosted, single executable, embedded frontend, SQLite by default, optional Docker.

### 1.2 Non-goals for MVP

- Multi-user or multi-tenant account and organization administration.
- A public plugin marketplace, hosted control plane, or distributed workers.
- Supporting multiple plugin scripting languages (only Luau initially).
- Guaranteeing exactly-once side effects against external APIs.
- Supporting arbitrary npm modules in custom widgets or unrestricted native modules in Luau.
- Automatically exposing cloud webhooks to the public internet.
- A complete general-purpose IDE or container orchestration system.

## 2. Fundamental concepts

| Concept | Meaning |
| --- | --- |
| **Package** | Git-versioned distribution unit; contains one or many plugin definitions, code, declarations, tests, UI assets and libraries. |
| **Definition** | A named exported connector, data provider, action provider, adapter, widget provider, automation, or library. |
| **Instance** | A configured use of a definition, with stable ID, input bindings, configuration, secret references, scopes and granted capabilities. |
| **Connector** | Luau implementation of integration operations. Declares capability vocabulary and operations; requests host-mediated I/O. |
| **Data provider** | Typed, parameterized, reactive query endpoint; may depend on connectors and other data providers. Owns reusable domain logic. |
| **Action provider** | Typed side-effecting operation, optionally previewable; may compose other action providers. |
| **Adapter** | Lightweight presentation transformation, event binding, multi-widget composition and interaction state. May generate nested widget trees. |
| **Widget provider** | Renderer definition for built-in UI primitives or sandboxed custom HTML/CSS/JavaScript. |
| **Library** | Reusable Luau functions/types; inherits the invoking instance's authority, never independent privileges. |
| **Automation** | Rust-scheduled trigger/rule plus optional Luau workflow, authorized to invoke actions. |
| **Signal** | Latest-value reactive state; snapshot and optional keyed deltas; may coalesce intermediate updates. |
| **Event** | Discrete occurrence with source identity, ordering scope and optional durable delivery. |
| **Contract** | Generated static description of interfaces, data types, query parameters, capabilities, events and state. |
| **Configured graph** | Git-backed instance bindings, dashboards, state channels, action bindings and permission configuration. |
| **Execution graph** | Runtime graph of *actual* queries, subscriptions, dependencies, cached results, and action executions. |
| **Draft transaction** | Isolated set of source/config edits to validate and preview before one atomic activation and one Git commit. |

### 2.1 Boundary rules

- **Packages are distribution units, not security boundaries.** Capabilities and resource budgets attach to instances and invocations.
- **Connectors expose capabilities; instances grant subsets of them.** Each operation checks both connector-instance grants and the caller's effective authority.
- **Data providers own domain calculations**, reusable joins, filtering and enrichment. Adapters perform only lightweight presentation transformations, widget layout and interaction binding.
- **Widgets never invoke unrestricted Luau or fetch host secrets.** They communicate through typed, mediated host APIs.
- **Actions are separate from reads**, even if a provider can invoke either through authorized dependencies.
- **All persistent state has explicit ownership, scope, schema version and migration policy.**

## 3. Logical architecture

```text
 Tailnet browsers / CLI / MCP agents
                   |
           Tailscale Serve
          private HTTPS/TLS
                   |
       127.0.0.1:8080 (default)
                   |
    +-----------------------------+
    | Single Rust executable      |
    |                             |
    | Axum HTTP + WebSocket + MCP |
    | Auth + API + UI assets      |
    |                             |
    | Configured Graph / Registry |
    | Reactive Query Engine       |
    | Event Log / Scheduler       |
    | Action Execution Manager    |
    | Luau Instance Scheduler     |
    | Capability & Secret Broker  |
    | State / History / Telemetry |
    | Git Transaction Manager     |
    +-----------------------------+
       |             |           |
     SQLite      Luau VMs     Git worktree
     + vault     on demand    + remote refs
```

### 3.1 Suggested workspace

```text
dash/
  Cargo.toml
  crates/
    dash-core/       # contracts, registry, graph, actions, events, scheduling
    dash-luau/       # Luau VMs, sandbox, host API, lifecycle
    dash-store/      # SQLite adapter, vault, history, state, migrations
    dash-server/     # HTTP, WebSocket, auth, MCP, embedded assets
    dash-git/        # packages, lock resolution, drafts, synchronization
  apps/dash/         # single binary, CLI and daemon
  web/               # TypeScript / React UI
  builtins/          # bundled Luau definitions, tests, widget assets
  fixtures/          # reference application, mocks, benchmark datasets
```

The crates above are suggested module boundaries; they need not imply independently versioned public APIs. Suggested Rust building blocks: Tokio, Axum, Serde, SQLite, `mlua` with Luau, and embedded static assets. Specific crate versions must be pinned and tested before implementation.

### 3.2 Runtime isolation

- Separate sandboxed Luau environment per *active instance*, created lazily; idle VMs may be reclaimed after serializing persistent state through Rust.
- Rust controls wall-clock deadlines, instruction/interrupt budgets, memory usage, I/O quotas, cancellation, and concurrency.
- Blocking work is never performed on the Tokio event-loop threads; Luau execution is scheduled appropriately.
- Long-running work uses coroutines (`ctx:await(...)`) and host-provided asynchronous operations.
- Untrusted native loading, direct OS/network/file APIs, raw sockets, FFI, and unrestricted global module loading are unavailable.
- In-process Luau sandboxing limits exposure but is **not an OS-level security boundary**. A later self-worker process mode is reserved for hostile plugin threat models.

## 4. Plugin system and contracts

### 4.1 Definition categories

| Type | Declares | Executes |
| --- | --- | --- |
| Connector | Operations; operation schemas; declared capability vocabulary; credential requirements | Host-mediated calls to APIs/services |
| Data provider | Input bindings; query schema; output semantic/structural type; refresh modes | Snapshot or keyed delta calculation; dependency queries |
| Action provider | Input schema; result schema; required capabilities; optional preview | Authorized side effects and/or nested action calls |
| Adapter | Accepted data/actions; local state; widget events; render output | UI binding and widget-tree composition |
| Widget provider | Properties; accepted data; events; renderer asset identity | Built-in renderer or sandboxed frontend assets |
| Library | Reusable module API and optional types | Pure code or caller-authorized helper logic |
| Automation | Trigger spec; optional rule/workflow; action dependencies | Scheduled/event/signal-triggered logic |

### 4.2 Type system

- A **structural contract** uses JSON Schema (or a constrained, versioned subset) for plain interoperability.
- A **semantic contract** is a named, versioned type, e.g. `dev.pull_request@1`, paired with a structural schema.
- Generic widgets can accept structural types; domain-specific adapters/providers can constrain semantic types.
- Compatibility is checked during wiring and activation, including version constraints. Explicit conversion providers resolve incompatible types.
- Contracts use stable identifiers and document breaking schema evolution. Existing bindings are revalidated when definitions update.
- Validate untrusted inputs at boundaries: API/MCP, connector results, plugin output, action arguments, state migrations and widget messages. Avoid repeated validation of already-checked internal buffers.
- Large datasets may use Rust-owned, immutable, typed columnar/record batches and opaque handles to avoid repeated JSON copies; serialization remains available at interoperable boundaries. Handles are scoped, revocable and non-serializable.

### 4.3 Declaration and generated manifest

The canonical authoring language is Luau. A **restricted declaration phase** evaluates only deterministic declaration APIs and generates a canonical manifest and schema artifact. Operational handlers are loaded only in runtime VMs. The declaration phase cannot inspect secrets, perform I/O, consult time, or execute operational handlers. Non-deterministic declarations are rejected.

**Proposed contract shape:**

```luau
-- plugins/engineering/linked_prs/definition.luau
local dash = require("dash")

return dash.data_provider {
    id = "engineering.linked_prs",
    runtime = "runtime.luau",
    handler = "query",
    inputs = {
        prs = dash.data_ref("github.pull_requests"),
        issues = dash.data_ref("jira.issues_by_keys"),
    },
    query = dash.object {
        repository = dash.string(),
        state = dash.enum { "open", "closed", "all" },
    },
    output = dash.list("engineering.linked_pr@1"),
    updates = {
        on_demand = true,
        subscribe = true,
    },
}
```

```luau
-- plugins/engineering/linked_prs/runtime.luau
--!strict
local Runtime = {}

function Runtime.query(ctx)
    local prs = ctx:await(ctx.inputs.prs:query({
        repository = ctx.query.repository,
        state = ctx.query.state,
    }))

    local keys = {}
    local seen = {}
    for _, pr in ipairs(prs) do
        local key = pr.title:match("([A-Z][A-Z0-9]+%-%d+)")
        if key ~= nil and not seen[key] then
            seen[key] = true
            table.insert(keys, key)
        end
    end

    local issues = {}
    if #keys > 0 then
        issues = ctx:await(ctx.inputs.issues:query({ keys = keys }))
    end
    local byKey = {}
    for _, issue in ipairs(issues) do
        byKey[issue.key] = issue
    end

    local result = {}
    for _, pr in ipairs(prs) do
        local key = pr.title:match("([A-Z][A-Z0-9]+%-%d+)")
        table.insert(result, { pr = pr, issue = key and byKey[key] or nil })
    end
    return result
end

return Runtime
```

The above is an interface design example, not executable against an existing SDK. The generated manifest must be deterministic, diffable, validated, and available through CLI, UI, API, and MCP.

### 4.4 Package distribution

- A package can export multiple definitions of any category and include UI assets and reusable libraries.
- Packages can be authored directly in the bound **instance repository**, or referenced from external Git repositories.
- External dependencies are declared in `plugins.toml` and resolved into a committed `plugins.lock` with exact commit hashes and content identifiers.
- Git dependency resolution is transitive with deterministic conflict handling; cycles and incompatible requirements fail validation.
- Library import is restricted to resolved packages. Libraries inherit the invoking instance's authority.
- Runtime does not silently update external dependencies. An authorized agent may update pins, test, activate, commit and push.
- Plugins never receive arbitrary Git credentials. Git I/O is performed by the host.

**Proposed instance repository:**

```text
engineering-dash/
  dashboard.toml
  plugins.toml
  plugins.lock
  instances/
    connectors.yaml
    data.yaml
    actions.yaml
    adapters.yaml
  dashboards/
    engineering.yaml
  plugins/
    engineering/
      package.toml
      linked_prs/
        definition.luau
        runtime.luau
        tests.luau
  tests/
    engineering-flow.yaml
```

**Proposed dependency declarations (example, not a final parser schema):**

```toml
[dependencies.github]
git = "https://github.com/example/dashboard-github.git"
ref = "v1.2.0"

[dependencies.jira]
git = "https://github.com/example/dashboard-jira.git"
ref = "main"
```

The lockfile pins Git SHA-1/SHA-256 commit identity as supplied by Git, tracks resolved subdependencies, and records the exact fetched tree digest. `ref = "main"` is a request for *resolution*, never permission to execute a moving version without a lockfile update.

## 5. Instances and capabilities

### 5.1 Authority model

1. A connector definition declares operations and capability names (e.g. `pull_requests.read`, `pull_requests.merge`).
2. A configured connector instance grants only an explicit subset of those capabilities.
3. Providers declare minimum required capabilities. A provider may reject insufficient bindings or adapt to a subset through explicit optional capabilities.
4. Every connector/action invocation checks **effective authority**: intersection of administrator ceiling, current initiating principal or token, relevant plugin-instance grants, requested operation, and host I/O policies. A powerful connector binding does not automatically empower every caller.
5. Nested actions are checked independently; authority cannot increase through delegation. A call chain preserves a traceable initiating principal and call context.
6. A library cannot grant new authority to its importer.
7. Drafts and previews have separate capability contexts and do not inherit live side-effect privileges by default.

**Proposed instance configuration:**

```yaml
connectors:
  github_reader:
    plugin: github.connector
    secrets:
      auth: secret://github-work
    capabilities:
      - pull_requests.read

  github_maintainer:
    plugin: github.connector
    secrets:
      auth: secret://github-work
    capabilities:
      - pull_requests.read
      - pull_requests.merge

  jira_reader:
    plugin: jira.connector
    secrets:
      auth: secret://jira-work
    capabilities:
      - issues.read

data:
  jira_issues_by_keys:
    plugin: jira.issues_by_keys
    inputs:
      jira: jira_reader

  open_prs:
    plugin: github.pull_requests
    inputs:
      github: github_reader

  linked_prs:
    plugin: engineering.linked_prs
    inputs:
      prs: open_prs
      issues: jira_issues_by_keys

actions:
  merge_pr:
    plugin: github.merge_pr
    inputs:
      github: github_maintainer
```

This binding example is complete for the connector and data-provider instances it references. It still requires the corresponding installed package definitions and centrally stored secret entries.

### 5.2 Central secrets

- Actual secret values **never appear in Git**, generated manifests, API responses, logs, event payloads or MCP resources.
- SQLite stores encrypted secret material (or references to an optional external secret store) with keys outside the versioned repository. **Proposed:** OS keychain where available, otherwise administrator-provided master key. Never store ciphertext and its effective decryption key together without an additional protection boundary.
- Connector code ordinarily receives **credential handles**, not plaintext. The Rust broker attaches headers, query credentials or supported signatures to narrowly authorized outbound requests.
- Where an integration requires specialized signing, provide audited host credential operations rather than unrestricted `secret.read`.
- Outbound HTTP has destination allowlists, protocol/method restrictions, redirect checks, SSRF protections (including DNS resolution and IP class changes), limits and audit logging.
- An agent may bind an existing secret only when its token and capability ceiling permit the specific binding. Secret names and existence may themselves be access-controlled.
- Git remote credentials are separately managed host secrets.

## 6. Reactive query graph

### 6.1 Parameterized queries

- A data-provider instance exposes a typed `query(parameters)` endpoint, optional `subscribe(parameters)`, and optional push/poll hooks.
- Cache identity includes instance ID, stable definition version, canonicalized query parameters, authorization-context fingerprint, and relevant cache policy. No cross-authorization cache leakage.
- Identical subscribed queries share one computation and underlying subscriptions; consumers are reference-counted.
- Different parameter sets have separately cached results and dependencies.
- Dependencies can be dynamic: an upstream result may determine which other provider queries a run performs. Dependency edges are recorded from each execution.
- The scheduler batches compatible dependency queries and offers provider-declared filtering, pagination, sorting and projections to push work toward the source.
- Stale data is retained with `fresh | stale | loading | error` metadata, retry/backoff and the last successful update timestamp.
- Unused queries can be cancelled after a grace period; idle cached entries expire through bounded LRU/TTL policies.
- Any dependency cycle detected in a configured or dynamic execution path fails the affected query with a diagnostic trace, not the entire server.

### 6.2 Snapshots, deltas and subscriptions

- Every query produces an initial typed snapshot.
- Providers may subsequently emit keyed `insert`, `update`, and `delete` deltas with stable row IDs and monotonic source-local sequence numbers.
- Engine verifies delta bases. Sequence gaps trigger resynchronization from a snapshot.
- Bounded subscriber queues implement backpressure. Slow widget consumers may skip intermediate signal states, but durable event consumers follow separate delivery semantics.
- No automatic external side effects during read/query recomputation.
- Push, polling and on-demand updates share the same subscription interface. Scheduled refresh is deduplicated per active query.

### 6.3 Query API (proposed)

```luau
local prs = ctx:await(ctx.inputs.prs:query({
    repository = "acme/platform",
    state = "open",
}))
```

`ctx:await` cooperates with the Rust scheduler. Context carries cancellation, deadlines, trace IDs and mediated bindings. It never exposes raw Rust internals, arbitrary services, or credentials.

## 7. Signals, events, scheduling and history

### 7.1 Signals

- Latest-state reactive channels, typed and explicitly scoped. Support snapshots, keyed deltas, and optional historical recording.
- Subscriptions can be dashboard/client/instance scoped, controlled by declared access policies.
- Delivery to widgets can coalesce intermediate states under backpressure.

### 7.2 Events

- Events contain stable event ID, source instance, source-local sequence, timestamp, semantic type/schema version, trace ID and payload.
- Ephemeral delivery by default; provider or consumer can request durable events with retention and acknowledgements.
- Durable streams offer **at-least-once** delivery, consumer checkpoints, replay and bounded retry/dead-letter handling.
- Ordering is guaranteed per source instance (under documented producer semantics), not globally across unrelated sources.
- Replaying events does **not** automatically repeat completed action side effects. Re-execution requires explicit authorized intent and idempotency safeguards.
- Use SQLite as the first durable event-log backend; storage contract remains replaceable.

### 7.3 Automations

- Rust handles cron/time triggers, events, signal predicates, concurrency, retries and execution records.
- Luau can implement complex conditions and workflows. Automations can call authorized action providers, but have no implicit additional permissions.
- Automations run even if no browser dashboard is open.
- Triggers and executions are traceable; missed schedules/restarts have explicit catch-up policies (proposed default: skip unless configured otherwise).

### 7.4 Historical data

- Opt-in per data-provider instance and **selected parameterized query**, with configurable retention, cadence/sampling and aggregation.
- Providers emit current snapshots/signals; Rust records selected values and exposes historical series through a normal data-provider contract.
- For a PR trend chart, store compact count samples, not repeated full PR arrays.
- Storage adapters must define retention/compaction and query budgets. Default SQLite; pluggable backend contract is required, non-SQLite backends deferred.
- History is independent of durable event delivery.

## 8. State model

| Scope | Ownership and lifetime | Examples |
| --- | --- | --- |
| Ephemeral | Widget/adapter execution lifetime; not persisted | Hover, open menu |
| Client | Stable client ID; persisted across browser reloads | Local filters, selected tab, layout tweaks |
| Dashboard | One dashboard ID, shared between its clients | Selected repository, shared context |
| Instance | Plugin instance ID, persisted independently of browsers | Cursor, checkpoint, synchronized mapping |

- State access is typed and mediated by Rust. Cross-widget communication uses explicit typed state channels or configured graph edges, not an unrestricted global namespace.
- Adapters can generate nested widget trees: containers, tabs, grids, and any supported widget node.
- Generated widget IDs are stable so component state survives reordering and refreshing.
- User overrides (size, position, visibility and selected configurable props) are separate from adapter-generated hierarchy, keyed by stable widget IDs.
- Every persisted state record has owner, scope, schema version, size limit and migration handler. Migrations run transactionally; failed migrations prevent activation and preserve existing live state.
- Concurrency uses versioned compare-and-swap or transaction semantics to prevent lost updates; client/dashboard state conflicts have documented merge rules.

## 9. Actions

### 9.1 Action lifecycle

1. Validate typed arguments, initiating principal, target bindings and required capabilities.
2. Optionally invoke `preview`, returning estimated effects, feasibility, warnings and preconditions. Preview is advisory, not a guarantee.
3. Create a durable execution record in `queued` state before external side effects.
4. Run action under deadline, quotas, cancellation and trace context.
5. Store status, result (redacted if necessary), audit metadata and execution duration.
6. Emit events and invalidate declared affected queries; let providers refresh authoritative data.

Execution statuses: `queued`, `running`, `succeeded`, `failed`, `unknown`, `cancelled`. `unknown` means the external operation may have succeeded despite a missing response; never automatically retry it without an idempotency/verification strategy.

### 9.2 Composition

- An action may invoke other actions. Each nested action has its own execution record and independent permission checks.
- Composition can report partial success, compensation attempts and unknown outcomes. External multi-system operations are **not** claimed to be atomic.
- Actions are callable from widgets (via adapters), automations, other actions and authorized MCP clients using the same service and policy enforcement.
- Optimistic widget updates remain temporary UI state, reconciled with the next authoritative query result.
- Idempotency keys are supported where the downstream integration can honor or reconcile them.

## 10. Widget rendering and visual development

### 10.1 Rendering model

- Built-in widget providers include: table, chart, map, counter/status, text, form, button/action control, container, grid, tabs and basic filters.
- Custom widgets ship HTML/CSS/JavaScript assets and run in **sandboxed iframes**, not the application shell's shared DOM.
- Recommended iframe policy: `sandbox="allow-scripts"` without `allow-same-origin` for untrusted content, isolated CSP, no ambient network permissions, and minimal `postMessage` bridge.
- Host/widget messages are typed, length-limited, versioned, tied to widget instance IDs and validated for source, origin/channel and authority. Widget events are requests, not privileged commands.
- Custom widget assets are bundled through packages and subjected to size/hash/integrity checks. No secrets or raw capability handles go to browsers.
- Built-in widgets share optimized host rendering and receive Rust-managed data over a versioned browser protocol.

### 10.2 Visual development environment

Three interconnected views:

1. **Dashboard Designer:** drag-and-drop layouts, nested widgets, bindings, property editors, screen-size previews and generated-widget user overrides.
2. **Dependency Graph Editor:** drag-and-drop configured connector/provider/action/adapter/widget nodes and connections; compatibility errors and required capabilities shown inline.
3. **Plugin Studio:** built-in Luau editor with generated type declarations, autocomplete, diagnostic output, tests, diff view and Git status. External IDE workflow remains equally supported.

A separate **read-only execution graph** reveals dynamic query nodes, real subscriptions, cache hits/misses, scheduling, action calls and source-level traces. Never confuse runtime-derived edges with the editable configured graph.

### 10.3 Isolated preview

- Entire draft graph can be instantiated in an isolated preview namespace, using mock connectors and simulated events by default.
- Preview supports time manipulation, mock snapshots, keyed deltas, error injection and interaction tests.
- Integration tests against real connectors are explicitly authorized through the agent/admin capability ceiling; mutating real operations are never silently enabled in test mode.
- Preview state, history and events do not affect production. No preview may share a mutable live instance or external side-effect authority merely because bindings match.

## 11. MCP and agent operations

### 11.1 Transport and authentication

- Serve MCP over the existing Axum application, using a supported, version-negotiated **Streamable HTTP** protocol on `/mcp`, plus optional local **stdio** mode. The stdio mode should proxy to the already running daemon rather than opening a competing independent engine against the same SQLite files.
- Follow the pinned MCP protocol version rather than hard-coding outdated GET/session requirements; protocol behavior changed across revisions. Validate `Origin`, enforce authorization on every invocation, and apply CSRF/DNS-rebinding defenses.
- MCP tokens are independently scoped and revocable. Stdio access inherits an explicit local identity and must not bypass the authorization service.
- The application uses local login sessions for the browser. Tokens, cookies and audit logs are separated.

### 11.2 MCP tools (initial names)

| Domain | Tools |
| --- | --- |
| Discover | `catalog_search`, `contract_get`, `graph_inspect`, `capability_explain` |
| Drafts | `draft_begin`, `draft_status`, `draft_validate`, `draft_preview`, `draft_test`, `draft_activate`, `draft_abort` |
| Plugins | `plugin_create`, `plugin_edit`, `plugin_test` |
| Instances | `instance_create`, `instance_configure`, `instance_delete` |
| Dashboards | `dashboard_get`, `dashboard_update`, `dashboard_preview` |
| Runtime | `provider_query`, `action_preview`, `action_invoke`, `execution_inspect` |
| Diagnostics | `logs_query`, `graph_trace`, `metrics_query` |
| Git | `git_status`, `git_history`, `git_sync`, `dependency_update` |
| Secrets | `secret_list`, `secret_bind` (never secret plaintext retrieval) |

MCP resources expose generated manifests, JSON Schemas, compatibility reports, dependency graphs, documentation, sanitized logs, traces and preview reports. Tools are backed by exactly the same application services used by UI and CLI.

### 11.3 Autonomy policy

- Agents may author code, modify definitions/instances/layouts, run tests, reload graph, invoke authorized actions, manage automations, and commit/push without per-edit confirmation.
- Agents **cannot expand their own capability ceiling** or read ungranted secrets, even by editing a connector or a policy file.
- A committed policy file can describe requested grants, but the effective administrator ceiling is outside the agent-editable repository. Exceeding it is a validation error or pending grant, not an automatic authorization.
- All agent writes occur within a draft transaction, and all side effects retain attribution to the initiating MCP principal.

## 12. Git-backed transactional editing

### 12.1 Source of truth

- The bound instance Git repository is the canonical source for declarative dashboard and plugin configuration.
- Visual editor, CLI and MCP all edit the same structured model, generating deterministic file changes.
- Secrets, secret keys, live state, caches, action logs and history remain outside Git.
- External plugin Git dependencies are pinned in `plugins.lock`; subdependencies are resolved and archived reproducibly.

### 12.2 Transaction protocol

1. Start a draft from a specified active revision. Materialize isolated files/worktree and draft storage namespace.
2. Apply any number of agent or human edits, retaining stable IDs and a change log.
3. Check format, schema, generated manifest, semantic type compatibility, dependency cycles, granted capabilities, migrations and Lua test results.
4. Start isolated preview VMs; run mocked integration/interaction tests and authorized real integration tests.
5. Prepare replacement live instances, check resource limits and migration viability.
6. Record the validated proposed revision and create **one local Git commit per successful transaction** (with source/agent attribution). If committing fails, do not activate.
7. Atomically switch the active config/graph generation to prepared instances; apply local transactional state migrations; preserve old generation until in-flight reads/actions reach a documented safe point. If activation fails, restore the old generation and mark commit as not active.
8. Attempt push to configured Git remote; if it fails, show `active / committed / unsynced` and allow retry. Never falsely report remote success or discard activated local changes.

Git and runtime updates cannot form one physical transaction. Persist an **activation journal** so crashes between local commit, graph swap and remote push are reconciled deterministically on next startup. Externally visible actions are never rolled back by switching revisions.

### 12.3 Concurrent edits

- Each draft has a base revision and performs optimistic conflict detection before activation.
- Conflicting visual editor, external Git and agent modifications require an explicit merge or rebase; no silent last-writer-wins.
- File watchers can create draft proposals from external changes, but must not bypass validation/activation.
- Auto-commits group the whole successful draft, not one commit per file.

## 13. Storage and secrets

**Default:** SQLite in WAL mode with bounded write paths and migrations. Storage interface remains replaceable, but additional adapters are not MVP work.

Proposed logical tables (not final SQL schema):

- `graph_revisions`, `draft_transactions`, `activation_journal`
- `instances`, `contracts`, `dependency_locks`
- `state_values` (owner, scope, schema version, generation, encoded value)
- `query_cache_index`, `history_series`, `history_samples`
- `events`, `event_consumer_checkpoints`, `dead_letters`
- `action_executions`, `action_steps`, `action_idempotency`
- `audit_records`, `trace_spans`, `runtime_metrics`
- `secret_metadata`, `encrypted_secret_blobs`, `api_token_hashes`, `sessions`
- `git_sync_state`

Large query caches and history partitions may use separate files/blobs behind storage abstractions. Retention, compaction and size budgets are mandatory. State and secret migrations require backup/restore tests.

## 14. HTTP/WebSocket and deployment

### 14.1 Packaging

- One executable: `dash`, serving embedded frontend and all backend services.
- CLI subcommands share application services: `dash init`, `dash serve`, `dash plugin test`, `dash plugin reload`, `dash git status`, `dash git sync`, `dash doctor`.
- No mandatory Postgres, Redis, reverse proxy or cloud control plane.
- Docker image is optional; a local data directory is mounted for SQLite, encrypted secret material and Git state.

### 14.2 Network

- Default listen address: **`127.0.0.1:8080`**. Binding to non-loopback addresses must be deliberate.
- Recommended tailnet exposure: Tailscale Serve proxied to local loopback; Tailscale handles private HTTPS, identity of tailnet devices and tailnet access controls. **Tailscale Funnel is not part of the default deployment.**
- Keep built-in local login, secure sessions and scoped API/MCP tokens; tailnet membership alone does not authorize actions.
- Honor forwarded headers only from explicitly trusted proxy hop(s); do not trust user-supplied identity headers or assume tailnet identity is equivalent to application identity.
- GitHub/Jira SaaS webhooks are not inbound-reachable on a private tailnet endpoint. Use polling initially; optional verified webhook relay/public ingress can be configured later.
- External API access and Git pushes remain outbound connections subject to connector and network policy.

**Recommended deployment sketch:**

```bash
dash serve --bind 127.0.0.1:8080
tailscale serve --bg 8080
```

Check the installed Tailscale CLI syntax and tailnet HTTPS settings as part of installation documentation.

### 14.3 Authentication/security baseline

- Browser login with secure password hashing (e.g., Argon2id), rate limiting and secure, HttpOnly, SameSite cookies.
- CSRF protections for cookie-authenticated mutations; origin checking on API/MCP/WebSocket entry points.
- Token material shown only at creation, stored hashed where verification permits, limited by expiry, scope and revocation.
- Audit all capability denials, plugin installation/updates, secret bindings, actions, state migrations, Git activation and pushes.
- Redact tokens and sensitive headers in traces, errors, previews and logs. Content security policy for shell UI and separate widget assets.

## 15. Observability, resource control and recovery

### 15.1 Diagnostics

Provide end-to-end traces with stable span IDs for:

- Browser/agent request and authenticated principal.
- Provider query identity (redacted/safe parameters), cache hit/miss, dynamic dependency queries and delta propagation.
- Lua VM startup, CPU/memory allocation, interruption, queue time and execution time.
- Connector outbound request timing, retry classification and rate limits.
- Action preview, execution, nested steps, uncertain outcomes and invalidation.
- Git draft validation, migrations, activation and remote sync.

UI and MCP can inspect the same sanitized trace data. Metrics include active VMs, cache memory, subscriber counts, queue depth, refresh latency, event lag, and SQLite space.

### 15.2 Failure handling

- A failed provider retains the last good value as **stale** and uses exponential retry/backoff with jitter.
- A Lua panic, timeout, memory violation or invalid result is contained to its affected instance/query; repeated failures trip a circuit breaker.
- A failed draft never replaces the live graph. Activation journal repairs interrupted lifecycle transitions.
- Push failure is an explicit unsynced state; it does not undo successfully activated local functionality.
- Graceful shutdown drains admitted actions within configured limits and persists consumer checkpoints; incomplete external actions become `unknown` if outcome cannot be proven.

### 15.3 Performance targets (proposed, benchmark gates)

These are **design targets, not measured claims**. Publish benchmark hardware, fixtures and methods alongside results.

| Metric | Initial target |
| --- | --- |
| Fresh daemon startup | <= 2 s to serve local health endpoint, with zero configured plugin instances |
| Baseline idle RSS | <= 100 MiB on a reference Linux x86-64 machine, no active provider subscriptions |
| Warm in-memory provider cache query | p95 <= 5 ms server processing time, excluding network/rendering |
| Signal propagation | p95 <= 100 ms to deliver an already-computed update to 100 local subscribers |
| Shared query deduplication | 100 identical concurrent authorized queries produce one provider computation |
| Idle VM reclamation | Inactive instances return to bounded baseline after configured TTL |
| Failure isolation | A looping/memory-exhausting instance cannot stall HTTP health checks or unrelated providers |
| Live graph replacement | No partial graph visible during activation; old graph remains usable on validation failure |

Revisit these figures after a prototype. Benchmark separately for connector network latency, serialization, large datasets, history writes, UI frame rate and load-test contention. Use resource ceilings and bounded queues regardless of the numeric results.

## 16. Reference application: Engineering Command Center

**Integrations:** GitHub and Jira, later optionally Jenkins.

**Configured graph:**

```text
GitHub connector [reader] ----> GitHub pull_requests [data] ----+
                                                                |
Jira connector [reader] ------> Jira issues_by_keys [data] -----+--> Linked PRs [data]
                                                                      |         |
                                                               Table adapter   Trend adapter
                                                                      |         |
                                                                 Table widget   Chart widget
                                                                      |
                                                               Merge event [UI]
                                                                      |
                                                               Merge PR [action]
                                                                      |
GitHub connector [maintainer] ------------------------------------------+
                                                                      |
                                                               Query invalidation

Durable PR merged event --> Complete Work [automation] --> Jira transition [action]
```

**Reference behavior:**

1. Create GitHub reader and maintainer instances, with a shared secret reference but distinct granted capability subsets. Create Jira reader and action instances.
2. Render a table of PRs joined to Jira issues through a reusable composed data provider.
3. Offer dashboard-scoped repository selection and client-scoped sorting/filter state.
4. Display open PR count plus a 30-day history of sampled count aggregates; don't store full PR records for trend history.
5. Multiple widgets subscribe to the same parameterized PR query and share its execution.
6. A user or MCP agent can preview and merge a permitted PR. The action execution is audited and affected queries refresh.
7. A durable merge event optionally starts an automation to transition a linked Jira issue, respecting separate action permissions and idempotency.
8. Simulate GitHub failures, slow clients, missing Jira issue keys, stale data, action unknown outcome, and Git push failures.
9. Use an agent to add a widget through MCP in a draft, run mock and interaction tests, activate without downtime, commit and push.

Mock GitHub and Jira backends are mandatory test fixtures so CI can verify this scenario without external tokens or side effects.

## 17. MVP acceptance criteria

All criteria must have automated coverage unless explicitly marked for manual security review.

1. **Single executable:** Clean install initializes its data directory and serves UI/API/MCP on loopback with SQLite; optional Docker image runs equivalently.
2. **Declarative Luau:** A local package exports connector/data/action/adapter/widget/library definitions and generates a deterministic manifest; bad contracts are rejected.
3. **Git packages:** The engine resolves a remote package, pins it in a lockfile and reopens the exact version after restart without network dependency updates.
4. **Capabilities:** Reader instance cannot perform merge; unauthorized provider/agent binding cannot bypass that denial; policy file edits cannot raise agent ceiling.
5. **Secrets:** No plaintext secret appears in Git, APIs, MCP, widget messages, logs or traces; host-mediated request can authenticate successfully.
6. **Reactive graph:** Identical authorized queries share execution; differing repository parameters remain independent; dynamic dependency changes do not leak subscriptions.
7. **Updates:** Polling, on-demand and simulated push updates work; snapshots and keyed deltas reconcile consistently; stale data remains visible after failure.
8. **Events/history:** Durable event can be replayed without duplicate side effects; selected PR count history obeys retention, remains available after restart.
9. **State:** Client, dashboard and instance state have independent persistence/visibility; generated widgets keep stable identity and layout overrides survive refresh.
10. **Actions:** Merge action supports optional preview, persistent record, nested permission checks, invalidation and `unknown` handling.
11. **Visual UI:** Drag-to-connect configured nodes, edit dashboard layout and code, observe full runtime traces and inspect compatibility/capability errors.
12. **MCP:** Agent can discover resources, create draft, edit Luau and graph, test, preview, activate, invoke authorized actions and inspect status.
13. **Transactional hot reload:** Invalid update leaves old graph functioning; valid update activates without visible partial state.
14. **Git transaction:** One local commit per successful draft; push failure leaves `active / committed / unsynced`; sync retry works; crash recovery restores coherent state.
15. **Sandbox:** Deny filesystem/native/network access beyond host APIs; memory/time runaway is interrupted and unrelated requests continue.
16. **Tailnet:** Service works behind Tailscale Serve with built-in auth; no public ingress required for polling-based reference application.
17. **Benchmark gates:** Profile and report startup, idle memory, query dedup, propagation latency and sandbox isolation against the published reference setup.

## 18. Implementation roadmap

| Phase | Deliverable | Exit condition |
| --- | --- | --- |
| **0. Spike** | Luau sandbox + `mlua`, async coroutine bridge, quotas, JSON/typed handles, SQLite, static manifest generator | Verified lifecycle and security assumptions; baseline benchmark |
| **1. Kernel** | Definitions/instances, type registry, capability broker, host-mediated HTTP, query scheduler, caching, state | GitHub reader provider + reusable library run headlessly |
| **2. Reactivity** | Subscriptions, polling/push, dynamic dependencies, deltas, history, stale data, events | Shared PR/Jira composed query behaves correctly under failure |
| **3. Actions** | Execution ledger, preview, nested calls, permission checks, invalidation, automation | Mock merge + Jira transition with unknown/replay tests |
| **4. Interface** | Embedded UI, standard widgets, sandbox bridge, dashboard and graph editors, traces | Reference dashboard can be configured and used visually |
| **5. Agent/Git** | MCP tools/resources, isolated draft, tests/preview, lockfile, commit, activation journal, push | Autonomous agent performs complete safe edit cycle |
| **6. Hardening** | Authentication, tailnet deployment, resource budgets, migration/restart safety, benchmarks | All MVP acceptance criteria pass |

Implementation order should prioritize **a vertical slice** (one connector -> one data query -> one widget -> one action -> one MCP draft) before the full visual editor or broad plugin catalog.

## 19. Explicitly open details

The following are not yet product decisions and must be resolved by design spikes or ADRs. They should not silently become requirements:

1. **Luau VM strategy:** VM threading, inter-instance fairness, coroutine scheduling, memory reclamation and whether any hard isolation requires self-worker subprocesses at MVP.
2. **Large data representation:** Rust record batch format, zero-copy semantics, opaque handle lifetime and delta/JSON conversion rules.
3. **Contract evolution:** Semantic type compatibility algorithm, JSON Schema subset, adapter version negotiation, generated Luau type definitions.
4. **Client state identity:** Stable client IDs, multi-tab coordination and dashboard-scoped conflict semantics.
5. **Secret encryption bootstrap:** OS keychain vs passphrase/master-key operational experience, backup, rotation and unattended restart.
6. **Auth/session details:** First-run bootstrap, recovery path, token audience and role/capability representation.
7. **Webhook ingress:** Whether optional relay or Tailscale-compatible ingress is offered after MVP; all public webhook authenticity must be verified.
8. **History sampling:** Per-query identity lifetime, rollups, retention budgets, restart behavior and history-provider API.
9. **Preview fidelity:** Mock definition/fixture format, time simulation, event injection and browser interaction-testing harness.
10. **Commit/activation crash semantics:** Precise fsync/journal and worktree switching protocol, reverting an unactivated local commit, reconciliation of external Git divergence.
11. **License/distribution and plugin supply chain:** Signature verification, hash policy, dependency licensing, unsafe asset scanning.
12. **Product name and naming conventions:** `dash` is a working identifier only.

## 20. Normative implementation principles

- **Deny by default**, with explicit minimal authority and no privilege escalation through nested providers or agents.
- **No secrets in Git or client code.**
- **No read side effects**, no silent repeat of uncertain external actions.
- **One domain truth:** reusable data providers, not presentation adapters, own business logic.
- **No unbounded queues or caches.**
- **Every mutable operation has stable identity, attribution, outcome and traceability.**
- **Never sacrifice a working revision for a broken draft.**
- **Canonical declarative configuration must round-trip through CLI, UI, MCP and Git without drift.**
- **Optimize with measurements; keep the initial architecture simple enough for a single-host installation.**

## 21. Technical references checked during drafting

- `mlua` official crate documentation, Luau and async features: https://docs.rs/crate/mlua/latest
- MCP Streamable HTTP protocol, including 2026-07-28 revision and compatibility: https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/basic/transports/streamable-http.mdx
- Tailscale Serve documentation: https://tailscale.com/docs/features/tailscale-serve

These references inform implementation feasibility; the concrete `dash` API described here is newly proposed and not an existing third-party API.

## Appendix A. Protocol examples (proposed; not yet implemented)

These examples fix important boundary semantics for an implementation spike. They are normative about **security properties and message meaning**, but their concrete field names may change during the SDK prototype.

### A.1 Data query response envelope

```json
{
  "protocol": "dash.query/1",
  "instance": "open_prs",
  "query_key": "5f9f7f0c",
  "revision": 16,
  "status": "fresh",
  "updated_at": "2026-11-23T10:00:00Z",
  "schema": "dev.pull_request@1",
  "kind": "snapshot",
  "value": [
    {
      "number": 42,
      "title": "DEV-123 Improve caching",
      "repository": "acme/platform",
      "state": "open"
    }
  ]
}
```

The actual `query_key` must be derived from a canonical, collision-resistant identity and include the authorization partition. The short illustrative key above is **not** a recommended hashing algorithm or key length. Consumers should not rely on comparing raw query keys across authorization contexts.

### A.2 Keyed delta envelope

```json
{
  "protocol": "dash.query/1",
  "instance": "open_prs",
  "revision": 17,
  "base_revision": 16,
  "kind": "delta",
  "operations": [
    {
      "op": "update",
      "key": "acme/platform#42",
      "value": {
        "number": 42,
        "title": "DEV-123 Improve caching",
        "repository": "acme/platform",
        "state": "merged"
      }
    }
  ]
}
```

A subscriber missing `base_revision` must request a new snapshot; it must not apply an out-of-order update. Snapshot revisions are monotonically increasing within a concrete query identity and graph generation.

### A.3 Browser widget event

```json
{
  "protocol": "dash.widget/1",
  "widget_instance": "engineering_pr_table",
  "graph_generation": 42,
  "sequence": 7,
  "kind": "event",
  "event": "merge_requested",
  "payload": {
    "repository": "acme/platform",
    "number": 42
  }
}
```

The event is **not** authorization to merge. The host resolves the authenticated browser session, adapter binding and action instance; validates the event schema and capability intersection; then creates a separate action execution. Widget messages must be rejected when their graph generation is stale or their instance ID is not currently bound.

### A.4 Action execution result

```json
{
  "execution_id": "act_01JBZQABCD1234",
  "action_instance": "merge_pr",
  "status": "unknown",
  "idempotency_key": "merge-acme-platform-42-1",
  "started_at": "2026-11-23T10:00:00Z",
  "reason": "upstream_connection_lost_after_request",
  "retry_safe": false
}
```

`unknown` is a first-class state. A recovery workflow may check the PR's current state through a read-only query, but may not automatically issue a second merge request unless the action's idempotency or reconciliation contract explicitly permits it.

## Appendix B. Design verification checklist

Before coding a new subsystem, its design document should answer:

1. Which entity owns its identity, configuration, state, authority and lifetime?
2. Which data is durable, cached, encrypted or versioned in Git?
3. Can an authorized agent invoke it without gaining more authority than its ceiling?
4. What is the cancellation, timeout, backpressure and memory-budget behavior?
5. How does a failed update or process crash leave a coherent active revision?
6. How are its boundaries type-checked, runtime-validated and traced?
7. What must be mocked to test it without real external credentials or side effects?
8. Can a single user deploy, inspect, back up and restore it without external infrastructure?
