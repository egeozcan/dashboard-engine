//! §17 criterion 15 (Sandbox) against the running services and HTTP router:
//!
//! - filesystem/native/OS/debug/dynamic-code/module escapes are absent or denied inside a real
//!   Instance VM (probe Definition reporting what it can reach);
//! - network is reachable only through the connector-only `ctx.http` host API, and only to
//!   destinations the effective network policy allows (loopback listeners prove no bytes leave
//!   for denied destinations);
//! - a looping Instance (with a deep queue of pending queries) and a memory-exhausting Instance
//!   are interrupted, while `/healthz` and an unrelated provider keep answering *during* the
//!   runaway (overlap proven through an Instance State "entered" signal, not a sleep).
//!
//! Criterion 17 timing gates live in `benchmarks.rs`; `ac17_*` below is the functional sanity
//! check that runs in the normal debug suite.

mod common_t4;

use common_t4::*;
use dash_core::{NetworkPolicy, Principal};
use dash_luau::ResourceBudget;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROBE_DEF: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.sandbox_probe",
    description = "Reports which escape hatches are reachable from an Instance VM",
    runtime = "runtime.luau",
    handler = "query",
}
"#;

const PROBE_RT: &str = r#"local R = {}

local function attempt(f)
    local ok, err = pcall(f)
    if ok then
        return "allowed"
    end
    return "denied: " .. tostring(err)
end

function R.query(ctx)
    local names = { "io", "os", "debug", "package", "load", "dofile", "loadfile", "module",
        "ffi", "jit", "socket", "string_dump" }
    local globals = {}
    for _, n in ipairs(names) do
        globals[n] = type(_G[n])
    end
    globals.string_dump = type(string.dump)
    return {
        globals = globals,
        require_fs = attempt(function() return require("fs") end),
        require_io = attempt(function() return require("io") end),
        require_os = attempt(function() return require("os") end),
        require_socket = attempt(function() return require("socket") end),
        require_undeclared_library = attempt(function() return require("engineering.pr_utils") end),
        loadstring = attempt(function() return loadstring("return 1")() end),
        http = attempt(function() return ctx.http end),
        global_write = attempt(function() string.rep = nil end),
        env_global_write = attempt(function() getfenv(0).string.rep = nil end),
        string_rep_intact = type(string.rep),
    }
end

return R
"#;

#[tokio::test]
async fn ac15_instance_vm_denies_filesystem_native_os_debug_and_dynamic_code() {
    let w = Workspace::ecc();
    w.add_definition("sandbox_probe", PROBE_DEF, PROBE_RT);
    w.append_instances(
        "data.yaml",
        "  sandbox_probe:\n    definition: engineering.sandbox_probe\n",
    );
    let b = boot(&w, true, ResourceBudget::default()).await;
    let client = reqwest::Client::new();
    // The real REST query path, authenticated as the Administrator (the broadest Principal).
    let resp = client
        .post(b.url("/api/queries/sandbox_probe"))
        .bearer_auth(ADMIN_TOKEN)
        .json(&json!({ "params": {}, "force_refresh": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let env: Value = resp.json().await.unwrap();
    let r = &env["value"];

    for lib in [
        "io",
        "os",
        "debug",
        "package",
        "load",
        "dofile",
        "loadfile",
        "module",
        "ffi",
        "jit",
        "socket",
        "string_dump",
    ] {
        assert_eq!(
            r["globals"][lib], "nil",
            "'{lib}' must not exist in an Instance VM: {r}"
        );
    }
    for m in ["require_fs", "require_io", "require_os", "require_socket"] {
        let v = r[m].as_str().unwrap();
        assert!(
            v.starts_with("denied:") && v.contains("SANDBOX_DENIED"),
            "{m}: {v}"
        );
    }
    let lib = r["require_undeclared_library"].as_str().unwrap();
    assert!(
        lib.contains("SANDBOX_DENIED") && lib.contains("not declared"),
        "a resolved but undeclared library must not load: {lib}"
    );
    let ls = r["loadstring"].as_str().unwrap();
    assert!(
        ls.contains("SANDBOX_DENIED") && ls.contains("loadstring"),
        "dynamic code loading must be denied: {ls}"
    );
    let http = r["http"].as_str().unwrap();
    assert!(
        http.contains("does not have access to ctx.http"),
        "only connectors reach the network host API: {http}"
    );
    for m in ["global_write", "env_global_write"] {
        let v = r[m].as_str().unwrap();
        assert!(
            v.contains("readonly"),
            "{m} must hit a read-only table: {v}"
        );
    }
    assert_eq!(r["string_rep_intact"], "function");
}

const NET_CONN_DEF: &str = r#"local dash = require("dash")
return dash.connector {
    id = "engineering.net_probe_connector",
    description = "Fetches an arbitrary URL through the host-mediated HTTP API",
    runtime = "runtime.luau",
    handler = "call",
    capabilities = { "net.fetch" },
    operations = {
        fetch = {
            description = "GET a URL",
            required_capabilities = { "net.fetch" },
            mode = "read",
            args = dash.object { url = dash.string() },
            result = dash.any(),
        },
    },
}
"#;
const NET_CONN_RT: &str = r#"local R = {}
function R.call(ctx)
    local resp = ctx:await(ctx.http:request({ method = "GET", url = ctx.args.url }))
    return { status = resp.status, body = resp.body }
end
return R
"#;
const NET_DP_DEF: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.net_probe",
    description = "Asks its connector to fetch query.url",
    runtime = "runtime.luau",
    handler = "query",
    inputs = {
        net = dash.connector_ref("engineering.net_probe_connector", {
            required_capabilities = { "net.fetch" },
        }),
    },
    query = dash.object { url = dash.string() },
}
"#;
const NET_DP_RT: &str = r#"local R = {}
function R.query(ctx)
    return ctx:await(ctx.inputs.net:call("fetch", { url = ctx.query.url }))
end
return R
"#;

/// A loopback TCP listener answering HTTP 200 and counting accepted connections.
async fn counting_listener() -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let h2 = Arc::clone(&hits);
    let app = axum::Router::new().fallback(|| async { "reached" });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let counted = app.layer(axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let h = Arc::clone(&h2);
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                next.run(req).await
            }
        },
    ));
    tokio::spawn(async move {
        axum::serve(listener, counted).await.unwrap();
    });
    (addr, hits)
}

#[tokio::test]
async fn ac15_network_only_through_connector_http_and_effective_policy() {
    let w = Workspace::ecc();
    w.add_definition("net_probe_connector", NET_CONN_DEF, NET_CONN_RT);
    w.add_definition("net_probe", NET_DP_DEF, NET_DP_RT);
    w.append_instances(
        "connectors.yaml",
        "  net_probe_conn:\n    definition: engineering.net_probe_connector\n    capabilities:\n      - net.fetch\n",
    );
    w.append_instances(
        "data.yaml",
        "  net_probe:\n    definition: engineering.net_probe\n    inputs:\n      net: net_probe_conn\n",
    );
    // Requested policy: only the IP literal 127.0.0.1 (loopback opt-in for this local test).
    let toml_path = w.ws.join("dashboard.toml");
    let toml = std::fs::read_to_string(&toml_path).unwrap().replace(
        "allowed_hosts = [\"api.github.com\", \"jira.example.com\"]",
        "allowed_hosts = [\"127.0.0.1\"]\nallow_loopback_for_tests = true",
    );
    assert!(toml.contains("allow_loopback_for_tests = true"));
    std::fs::write(&toml_path, toml).unwrap();

    // Live mode (no Mocks): the connector really talks to the network through the broker.
    let b = boot(&w, false, ResourceBudget::default()).await;
    let admin = Principal::administrator();
    let (allowed, allowed_hits) = counting_listener().await;
    let (denied, denied_hits) = counting_listener().await;
    let fetch = |url: String| {
        let s = b.services.clone();
        async move {
            s.provider_query(
                "net_probe",
                json!({ "url": url }),
                &Principal::administrator(),
                true,
            )
            .await
        }
    };

    // Without the Administrator network ceiling nothing leaves the process.
    let err = fetch(format!("http://{allowed}/before-grant"))
        .await
        .unwrap_err();
    assert!(err.contains("network policy denied"), "{err}");
    assert_eq!(allowed_hits.load(Ordering::SeqCst), 0);

    b.services
        .network_policy_grant(
            NetworkPolicy {
                allowed_hosts: vec!["127.0.0.1".into()],
                allowed_methods: vec!["GET".into()],
                allow_loopback_for_tests: true,
            },
            &admin,
        )
        .unwrap();

    // Positive control: the policy-allowed destination is reached through ctx.http.
    let ok = fetch(format!("http://{allowed}/granted")).await.unwrap();
    let v = ok.value.unwrap();
    assert_eq!(v["status"], 200, "{v}");
    assert_eq!(v["body"], "reached");
    assert_eq!(allowed_hits.load(Ordering::SeqCst), 1);

    // Destinations outside the effective policy never get a connection.
    let port = denied.port();
    for url in [
        format!("http://localhost:{port}/name-not-allowlisted"),
        format!("http://[::1]:{port}/v6-loopback"),
        format!("http://127.0.0.2:{port}/other-loopback"),
        "file:///etc/passwd".to_string(),
        format!("gopher://127.0.0.1:{port}/scheme"),
    ] {
        let err = fetch(url.clone()).await.unwrap_err();
        assert!(
            err.contains("denied") || err.contains("invalid url") || err.contains("blocked"),
            "{url}: {err}"
        );
    }
    assert_eq!(
        denied_hits.load(Ordering::SeqCst),
        0,
        "a denied destination received a connection"
    );
    // Every denial is audited against the connector Instance (§14.3).
    let denials = b
        .services
        .store()
        .list_audits(200)
        .unwrap()
        .into_iter()
        .filter(|a| a.operation == "http.request" && a.outcome == "denied")
        .filter(|a| a.target == "net_probe_conn")
        .count();
    assert!(denials >= 6, "expected audited denials, got {denials}");
}

const LOOP_TIMEOUT_MS: u64 = 4_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac15_runaway_loop_and_memory_bomb_are_interrupted_while_unrelated_requests_continue() {
    let w = runaway_workspace(LOOP_TIMEOUT_MS);
    let b = boot(&w, true, ResourceBudget::default()).await;
    let pool = b.services.engine().pool.clone();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // 80 distinct runaway queries: one spins in the VM, the rest queue behind it. That is more
    // than the pool-wide concurrency limit (64), so this also proves the queue holds no permits.
    let mut loops = Vec::new();
    for n in 0..80 {
        let s = b.services.clone();
        loops.push(tokio::spawn(async move {
            let t = Instant::now();
            let r = s
                .provider_query(
                    "runaway_loop",
                    json!({ "n": n }),
                    &Principal::administrator(),
                    false,
                )
                .await;
            (t.elapsed(), r)
        }));
    }
    // Deterministic overlap signal: the handler wrote "entered" right before `while true do end`.
    let entered = wait_for(
        "runaway loop to enter its handler",
        Duration::from_secs(10),
        || async { instance_state(&b.services, "runaway_loop", "entered") },
    )
    .await;
    let spinning = entered["n"].as_u64().unwrap() as usize;
    let loop_started = Instant::now();

    // Memory bomb fired repeatedly during the whole measurement window.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let bomb = {
        let (s, stop) = (b.services.clone(), Arc::clone(&stop));
        tokio::spawn(async move {
            let mut errors = Vec::new();
            let mut n = 0;
            while !stop.load(Ordering::SeqCst) {
                n += 1;
                let r = s
                    .provider_query(
                        "runaway_memory",
                        json!({ "n": n }),
                        &Principal::administrator(),
                        true,
                    )
                    .await;
                errors.push(r.expect_err("a memory bomb cannot succeed"));
                tokio::task::yield_now().await;
            }
            errors
        })
    };
    wait_for(
        "a memory-exceeded interruption",
        Duration::from_secs(10),
        || async { (pool.metrics().total_memory_exceeded >= 1).then_some(()) },
    )
    .await;

    // Unrelated work while both runaways are active.
    let mut healthz_ms = Vec::new();
    let mut unrelated_ms = Vec::new();
    for _ in 0..30 {
        let t = Instant::now();
        let hz = client.get(b.url("/healthz")).send().await.unwrap();
        assert_eq!(hz.status(), 200);
        healthz_ms.push(ms(t.elapsed()));

        let t = Instant::now();
        let resp = client
            .post(b.url("/api/queries/open_prs"))
            .bearer_auth(ADMIN_TOKEN)
            .json(&json!({
                "params": { "repository": "acme/platform", "state": "open" },
                "force_refresh": true
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "unrelated provider failed during runaway"
        );
        let env: Value = resp.json().await.unwrap();
        assert_eq!(env["value"].as_array().unwrap().len(), 3);
        unrelated_ms.push(ms(t.elapsed()));
    }
    // Overlap proof: the loop is still spinning and was never released early.
    assert!(
        !loops[spinning].is_finished(),
        "measurements must overlap the running loop"
    );
    assert!(loop_started.elapsed() < Duration::from_millis(LOOP_TIMEOUT_MS));
    stop.store(true, Ordering::SeqCst);
    let bomb_errors = bomb.await.unwrap();

    let bound = 100.0 * debug_slack();
    let hz95 = percentile(&healthz_ms, 95.0);
    let un95 = percentile(&unrelated_ms, 95.0);
    assert!(
        hz95 <= bound,
        "/healthz p95 {hz95:.2} ms during runaway (bound {bound} ms)"
    );
    assert!(
        un95 <= bound,
        "unrelated provider p95 {un95:.2} ms during runaway (bound {bound} ms)"
    );

    // The memory bomb is interrupted by its memory limit (later attempts may be refused by the
    // circuit breaker, which also never runs the script).
    let limit = BOMB_MEMORY_MIB * 1024 * 1024;
    assert!(
        bomb_errors
            .iter()
            .any(|e| e.contains(&format!("memory budget ({limit} bytes) exceeded"))),
        "{bomb_errors:?}"
    );
    assert!(bomb_errors
        .iter()
        .all(|e| e.contains("memory budget") || e.contains("circuit breaker open")));

    // The loop is interrupted by the host (wall-clock deadline, or the interrupt budget, which
    // optimized builds reach first) and not by anything the test did, within a bounded time.
    let (elapsed, res) = loops.remove(spinning).await.unwrap();
    let err = res.unwrap_err();
    assert!(err.contains("timed out"), "{err}");
    assert!(
        elapsed <= Duration::from_millis(LOOP_TIMEOUT_MS + 2_000),
        "runaway not interrupted in time: {elapsed:?}"
    );
    assert!(pool.metrics().total_timeouts >= 1);

    // Stop the queued runaways (daemon shutdown path) and check the engine is healthy after.
    b.services.engine().queries.cancel_all_in_flight();
    for h in loops {
        h.abort();
    }
    let after = b
        .services
        .provider_query(
            "linked_prs",
            json!({ "repository": "acme/platform", "state": "open" }),
            &Principal::administrator(),
            true,
        )
        .await
        .unwrap();
    assert_eq!(after.value.unwrap().as_array().unwrap().len(), 3);
}

const UP_DEF: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.gen_up",
    description = "Upstream that reports its revision",
    runtime = "runtime.luau",
    handler = "query",
}
"#;
const DOWN_DEF: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.gen_down",
    description = "Waits on a gate, then reads its upstream",
    runtime = "runtime.luau",
    handler = "query",
    inputs = { up = dash.data_ref("engineering.gen_up") },
}
"#;
const DOWN_RT: &str = r#"local R = {}
function R.query(ctx)
    ctx:await(ctx.state:set("entered", true))
    while ctx:await(ctx.state:get("gate")) ~= true do end
    local up = ctx:await(ctx.inputs.up:query({}))
    return { up = up.rev, down = "v1" }
end
return R
"#;

fn up_rt(rev: &str) -> String {
    format!("local R = {{}}\nfunction R.query(ctx) return {{ rev = \"{rev}\" }} end\nreturn R\n")
}

/// §15.3 "No partial graph visible during activation": a computation that started on Generation
/// N and reads its dependency after Generation N+1 (with a new upstream) was activated must still
/// see Generation N's upstream, never a mix of both. The downstream waits on a gate so the
/// activation deterministically lands between its start and its dependency read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac17_activation_mid_computation_never_mixes_generations() {
    let w = Workspace::ecc();
    w.add_definition("gen_up", UP_DEF, &up_rt("v1"));
    w.add_definition("gen_down", DOWN_DEF, DOWN_RT);
    w.append_instances(
        "data.yaml",
        "  gen_up:\n    definition: engineering.gen_up\n  gen_down:\n    definition: engineering.gen_down\n    inputs:\n      up: gen_up\n",
    );
    let b = boot(&w, true, ResourceBudget::default()).await;
    let s = b.services.clone();
    let admin = Principal::administrator();
    let gen_n = s.graph().generation_id;

    let down = {
        let s = s.clone();
        tokio::spawn(async move {
            s.provider_query("gen_down", json!({}), &Principal::administrator(), true)
                .await
        })
    };
    wait_for(
        "downstream to start on Generation N",
        Duration::from_secs(10),
        || async { instance_state(&s, "gen_down", "entered") },
    )
    .await;

    let draft = s.draft_begin(&admin).await.unwrap();
    s.definition_upsert_in_draft(
        &draft.draft_id,
        "engineering",
        "gen_up",
        UP_DEF,
        Some(&up_rt("v2")),
        None,
        &admin,
    )
    .unwrap();
    s.draft_activate(&draft.draft_id, &admin).await.unwrap();
    assert!(
        s.graph().generation_id > gen_n,
        "activation did not switch Generations"
    );
    assert!(!down.is_finished());

    s.engine()
        .state
        .set("instance", "gen_down", "gate", 1, gen_n, json!(true), None)
        .unwrap();
    let env = down.await.unwrap().unwrap();
    assert_eq!(env.generation, gen_n);
    assert_eq!(
        env.value,
        Some(json!({ "up": "v1", "down": "v1" })),
        "a Generation-N computation must read Generation-N dependencies"
    );

    // The live Generation serves the new upstream.
    let up = s
        .provider_query("gen_up", json!({}), &admin, true)
        .await
        .unwrap();
    assert_eq!(up.value, Some(json!({ "rev": "v2" })));
}

/// Criterion 17 functional sanity in the parallel debug suite: a fresh daemon with zero
/// configured Instances serves `/healthz`, and a warm cached query is served from memory. The
/// timing gates (2 s startup, 5 ms p95) are enforced in `--release` by `benchmarks.rs`; here the
/// bounds only catch gross regressions, scaled for unoptimized builds running in parallel.
#[tokio::test]
async fn ac17_fresh_daemon_and_warm_query_sanity() {
    let tmp = tempfile::TempDir::new().unwrap();
    let (mut child, bind, startup) = spawn_fresh_daemon(&tmp).await;
    let rss = process_rss_mib(child.id().unwrap()).expect("RSS must be measurable");
    let graph: Value = reqwest::Client::new()
        .get(format!("http://{bind}/healthz"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(graph["status"], "ok");
    let _ = child.kill().await;
    let startup_bound = Duration::from_secs_f64(2.0 * debug_slack());
    assert!(
        startup <= startup_bound,
        "startup {startup:?} > {startup_bound:?}"
    );
    assert!(
        rss > 1.0 && rss <= 100.0 * debug_slack().min(3.0),
        "idle RSS {rss:.1} MiB"
    );

    let w = Workspace::ecc();
    let b = boot(&w, true, ResourceBudget::default()).await;
    let admin = Principal::administrator();
    let params = json!({ "repository": "acme/platform", "state": "open" });
    let first = b
        .services
        .provider_query("open_prs", params.clone(), &admin, false)
        .await
        .unwrap();
    let executions = b.services.engine().pool.metrics().total_executions;
    let mut samples = Vec::new();
    for _ in 0..50 {
        let t = Instant::now();
        let warm = b
            .services
            .provider_query("open_prs", params.clone(), &admin, false)
            .await
            .unwrap();
        samples.push(ms(t.elapsed()));
        assert_eq!(warm.query_key, first.query_key);
        assert_eq!(
            warm.version, first.version,
            "warm query must be a cache hit"
        );
    }
    assert_eq!(
        b.services.engine().pool.metrics().total_executions,
        executions,
        "warm queries must not execute Luau"
    );
    let p95 = percentile(&samples, 95.0);
    assert!(p95 <= 5.0 * debug_slack(), "warm p95 {p95:.3} ms");
}
