//! §15.3 benchmark gates / §17 criterion 17 ("profile and report startup, idle memory, query
//! dedup, propagation latency and sandbox isolation against the published reference setup").
//!
//! Publish numbers only from an optimized build:
//! `cargo test --release -p dash --test benchmarks -- --nocapture` (prints `BENCHMARK_REPORT=`).
//!
//! Functional gates (one computation for 100 identical queries, runaway interruption, VM
//! reclamation after the TTL, no partial graph during activation, old graph kept on a failed
//! activation) are identical in every profile. Latency/time bounds are the §15.3 targets in
//! `--release` and are multiplied by `debug_slack()` in unoptimized builds, so the normal debug
//! `cargo test` run exercises every scenario without asserting release-only timings.

mod common_t4;

use common_t4::*;
use dash_core::{Principal, QueryEnvelope, QueryStatus};
use dash_luau::ResourceBudget;
use dash_server::AppServices;
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const COUNTER_DEF: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.bench_counter",
    description = "Counts its own computations in Instance State; waits for a gate first",
    runtime = "runtime.luau",
    handler = "query",
    query = dash.object { repository = dash.string() },
}
"#;
const COUNTER_RT: &str = r#"local R = {}
function R.query(ctx)
    while ctx:await(ctx.state:get("gate")) ~= true do end
    local n = (ctx:await(ctx.state:get("computations")) or 0) + 1
    ctx:await(ctx.state:set("computations", n))
    return { computation = n, repository = ctx.query.repository }
end
return R
"#;

fn pair_a(rev: &str) -> (String, String) {
    (
        r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.bench_pair_a",
    description = "Upstream half of a coherence pair",
    runtime = "runtime.luau",
    handler = "query",
}
"#
        .to_string(),
        format!(
            "local R = {{}}\nfunction R.query(ctx) return {{ rev = \"{rev}\" }} end\nreturn R\n"
        ),
    )
}

fn pair_b(rev: &str) -> (String, String) {
    (
        r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.bench_pair_b",
    description = "Downstream half of a coherence pair: reports both revisions",
    runtime = "runtime.luau",
    handler = "query",
    inputs = { a = dash.data_ref("engineering.bench_pair_a") },
}
"#
        .to_string(),
        format!(
            "local R = {{}}\nfunction R.query(ctx)\n    local a = ctx:await(ctx.inputs.a:query({{}}))\n    return {{ a = a.rev, b = \"{rev}\" }}\nend\nreturn R\n"
        ),
    )
}

const MEM_HOLD_DEF: &str = r#"local dash = require("dash")
return dash.data_provider {
    id = "engineering.memory_hold",
    description = "Fills most of its memory budget, holds it until released, then exceeds it",
    runtime = "runtime.luau",
    handler = "query",
}
"#;
const MEM_HOLD_RT: &str = r#"local R = {}
function R.query(ctx)
    local hoard = {}
    local i = 0
    while i < 1280 do
        i += 1
        hoard[i] = string.rep("x", 4096) .. tostring(i)
    end
    ctx:await(ctx.state:set("holding", i))
    while ctx:await(ctx.state:get("release")) ~= true do end
    while true do
        i += 1
        hoard[i] = string.rep("x", 4096) .. tostring(i)
    end
end
return R
"#;
/// The hog keeps ~5 MiB live (1,280 x 4 KiB strings): well under half of its budget, so the
/// garbage its hold loop creates is collected before the limit and only the final unbounded
/// allocation trips it.
const HOLD_MEMORY_MIB: u64 = 16;

const LOOP_TIMEOUT_MS: u64 = 3_000;
const IDLE_TTL_SECS: u64 = 1;

fn bench_workspace() -> Workspace {
    let w = runaway_workspace(LOOP_TIMEOUT_MS);
    w.add_definition("bench_counter", COUNTER_DEF, COUNTER_RT);
    w.add_definition("memory_hold", MEM_HOLD_DEF, MEM_HOLD_RT);
    w.append_dashboard_toml(&format!(
        "[resources.instances.memory_hold]\nmemory_limit_mib = {HOLD_MEMORY_MIB}\ntimeout_ms = 60000\nmax_interrupts = 50000000\n"
    ));
    let (d, r) = pair_a("v1");
    w.add_definition("bench_pair_a", &d, &r);
    let (d, r) = pair_b("v1");
    w.add_definition("bench_pair_b", &d, &r);
    w.append_instances(
        "data.yaml",
        "  memory_hold:\n    definition: engineering.memory_hold\n  bench_counter:\n    definition: engineering.bench_counter\n  bench_pair_a:\n    definition: engineering.bench_pair_a\n  bench_pair_b:\n    definition: engineering.bench_pair_b\n    inputs:\n      a: bench_pair_a\n",
    );
    // Workspace-wide idle TTL for the reclamation gate (resources are TOML: the table header must
    // precede the per-Instance tables appended by `runaway_workspace`, so set it via `defaults`).
    let toml_path = w.ws.join("dashboard.toml");
    let toml = std::fs::read_to_string(&toml_path).unwrap().replace(
        "[resources.instances.runaway_loop]",
        &format!(
            "[resources]\nidle_ttl_secs = {IDLE_TTL_SECS}\n\n[resources.instances.runaway_loop]"
        ),
    );
    std::fs::write(&toml_path, toml).unwrap();
    w
}

fn round(v: f64, digits: i32) -> f64 {
    let f = 10f64.powi(digits);
    (v * f).round() / f
}

/// 1. Fresh daemon startup and idle RSS (spawned `dash serve`, zero configured Instances).
async fn startup_and_idle_rss(report: &mut serde_json::Map<String, Value>) {
    let mut startups = Vec::new();
    let mut rss = Vec::new();
    for _ in 0..3 {
        let tmp = TempDir::new().unwrap();
        let (mut child, _bind, startup) = spawn_fresh_daemon(&tmp).await;
        startups.push(ms(startup));
        // Idle: let the first maintenance tick (retention, metrics sampling) run, then sample.
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let pid = child.id().expect("daemon exited");
        rss.push(process_rss_mib(pid).expect("idle RSS must be measurable"));
        let _ = child.kill().await;
    }
    let startup_max = startups.iter().cloned().fold(0.0, f64::max);
    let rss_max = rss.iter().cloned().fold(0.0, f64::max);
    let startup_bound = 2_000.0 * debug_slack();
    assert!(
        startup_max <= startup_bound,
        "fresh startup {startup_max:.1} ms > {startup_bound} ms ({startups:?})"
    );
    // RSS is a property of the binary, not of latency: debug builds get a smaller allowance.
    let rss_bound = 100.0 * debug_slack().min(2.0);
    assert!(
        rss_max <= rss_bound,
        "idle RSS {rss_max:.1} MiB > {rss_bound} MiB ({rss:?})"
    );
    report.insert(
        "fresh_startup_ms_p50".into(),
        json!(round(percentile(&startups, 50.0), 2)),
    );
    report.insert("fresh_startup_ms_max".into(), json!(round(startup_max, 2)));
    report.insert("idle_rss_mib_max".into(), json!(round(rss_max, 2)));
}

/// 2. Warm in-memory provider cache query (server processing time, in-process).
async fn warm_query(s: &AppServices, report: &mut serde_json::Map<String, Value>) {
    let admin = Principal::administrator();
    let params = json!({ "repository": "acme/platform", "state": "open" });
    let first = s
        .provider_query("open_prs", params.clone(), &admin, false)
        .await
        .unwrap();
    let executions = s.engine().pool.metrics().total_executions;
    let mut samples = Vec::with_capacity(1_000);
    for _ in 0..1_000 {
        let t = Instant::now();
        let env = s
            .provider_query("open_prs", params.clone(), &admin, false)
            .await
            .unwrap();
        samples.push(ms(t.elapsed()));
        assert_eq!(env.version, first.version);
    }
    assert_eq!(
        s.engine().pool.metrics().total_executions,
        executions,
        "warm queries must be cache hits"
    );
    let p95 = percentile(&samples, 95.0);
    let bound = 5.0 * debug_slack();
    assert!(p95 <= bound, "warm query p95 {p95:.4} ms > {bound} ms");
    report.insert("warm_query_p95_ms".into(), json!(round(p95, 4)));
}

/// 3. Delivery of an already-computed update to 100 local subscribers.
async fn propagation(s: &AppServices, report: &mut serde_json::Map<String, Value>) {
    let key = s
        .provider_query(
            "open_prs",
            json!({ "repository": "acme/platform", "state": "open" }),
            &Principal::administrator(),
            false,
        )
        .await
        .unwrap()
        .query_key;
    let mut samples = Vec::new();
    for iter in 0..50u64 {
        let mut rxs: Vec<_> = (0..100)
            .map(|_| s.engine().signals.subscribe_stream(&key))
            .collect();
        let version = 1_000_000 + iter;
        let env = QueryEnvelope::snapshot(
            "open_prs",
            &key,
            1,
            version,
            QueryStatus::Fresh,
            None,
            json!([{ "number": iter }]),
            None,
        );
        let t = Instant::now();
        s.engine().signals.publish_query(env);
        for rx in rxs.iter_mut() {
            let got = rx.recv().await.unwrap();
            assert_eq!(got.version, version);
        }
        samples.push(ms(t.elapsed()));
    }
    let p95 = percentile(&samples, 95.0);
    let bound = 100.0 * debug_slack();
    assert!(p95 <= bound, "propagation p95 {p95:.3} ms > {bound} ms");
    report.insert("signal_100_subs_p95_ms".into(), json!(round(p95, 4)));
}

/// 4. 100 identical concurrent authorized queries -> one provider computation. `force_refresh`
/// bypasses the cache, so only the shared in-flight computation can deduplicate. The provider
/// waits on a gate that opens only after all 100 callers have joined (each future is polled to
/// `Pending` first), so the result is deterministic rather than timing-dependent.
async fn dedup(s: &AppServices, report: &mut serde_json::Map<String, Value>) {
    let gen = s.graph().generation_id;
    let state = &s.engine().state;
    state
        .set(
            "instance",
            "bench_counter",
            "gate",
            1,
            gen,
            json!(false),
            None,
        )
        .unwrap();
    let executions_before = s.engine().pool.metrics().total_executions;
    let mut futs: Vec<Pin<Box<dyn Future<Output = Result<QueryEnvelope, String>> + Send>>> =
        Vec::new();
    for _ in 0..100 {
        let s = s.clone();
        futs.push(Box::pin(async move {
            s.provider_query(
                "bench_counter",
                json!({ "repository": "acme/platform" }),
                &Principal::administrator(),
                true,
            )
            .await
        }));
    }
    let t = Instant::now();
    std::future::poll_fn(|cx| {
        for f in futs.iter_mut() {
            assert!(f.as_mut().poll(cx).is_pending(), "gate is closed");
        }
        Poll::Ready(())
    })
    .await;
    state
        .set(
            "instance",
            "bench_counter",
            "gate",
            1,
            gen,
            json!(true),
            None,
        )
        .unwrap();
    let results = futures_util::future::join_all(futs).await;
    let elapsed = ms(t.elapsed());
    let envs: Vec<QueryEnvelope> = results.into_iter().map(|r| r.unwrap()).collect();
    let computations = instance_state(s, "bench_counter", "computations").unwrap();
    assert_eq!(
        computations,
        json!(1),
        "100 identical queries must compute once"
    );
    assert_eq!(
        s.engine().pool.metrics().total_executions - executions_before,
        1,
        "exactly one Luau execution"
    );
    for e in &envs {
        assert_eq!(e.version, envs[0].version);
        assert_eq!(
            e.value,
            Some(json!({ "computation": 1, "repository": "acme/platform" }))
        );
    }
    report.insert("dedup_100_identical_computations".into(), json!(1));
    report.insert(
        "dedup_100_identical_total_ms".into(),
        json!(round(elapsed, 3)),
    );
}

async fn unrelated_round(
    client: &reqwest::Client,
    b: &Booted,
    healthz: &mut Vec<f64>,
    unrelated: &mut Vec<f64>,
) {
    let t = Instant::now();
    let hz = client.get(b.url("/healthz")).send().await.unwrap();
    assert_eq!(hz.status(), 200);
    healthz.push(ms(t.elapsed()));
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
    assert_eq!(resp.status(), 200, "unrelated provider query failed");
    let env: Value = resp.json().await.unwrap();
    assert_eq!(env["value"].as_array().unwrap().len(), 3);
    unrelated.push(ms(t.elapsed()));
}

/// 5. Failure isolation: /healthz and an unrelated provider (recomputed through Luau and its
/// Mock connector on every request) while an actual looping Instance with 80 queued queries and
/// an actual memory-exhausting Instance are running.
async fn failure_isolation(b: &Booted, report: &mut serde_json::Map<String, Value>) {
    let s = &b.services;
    let pool = s.engine().pool.clone();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let (mut base_hz, mut base_un) = (Vec::new(), Vec::new());
    for _ in 0..100 {
        unrelated_round(&client, b, &mut base_hz, &mut base_un).await;
    }

    let mut loops = Vec::new();
    for n in 0..80 {
        let s = s.clone();
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
    let entered = wait_for("runaway loop entry", Duration::from_secs(10), || async {
        instance_state(s, "runaway_loop", "entered")
    })
    .await;
    let spinning = entered["n"].as_u64().unwrap() as usize;
    let loop_entered = Instant::now();

    // The memory-exhausting Instance fills most of its budget, then holds it (awaiting a host
    // read) for the whole measurement window, and finally allocates past the limit.
    let bomb = {
        let s = s.clone();
        tokio::spawn(async move {
            s.provider_query("memory_hold", json!({}), &Principal::administrator(), true)
                .await
        })
    };
    let held_chunks = wait_for(
        "memory hog to reach its plateau",
        Duration::from_secs(20),
        || async { instance_state(s, "memory_hold", "holding") },
    )
    .await;

    let (mut hz, mut un) = (Vec::new(), Vec::new());
    for _ in 0..100 {
        unrelated_round(&client, b, &mut hz, &mut un).await;
    }
    if bomb.is_finished() {
        panic!(
            "measurements must overlap the memory hog; it ended early: {:?}",
            bomb.await.unwrap()
        );
    }
    assert!(
        !loops[spinning].is_finished(),
        "measurements must overlap the running loop (raise LOOP_TIMEOUT_MS)"
    );
    assert!(loop_entered.elapsed() < Duration::from_millis(LOOP_TIMEOUT_MS));
    let oom_before = pool.metrics().total_memory_exceeded;
    let gen = s.graph().generation_id;
    s.engine()
        .state
        .set(
            "instance",
            "memory_hold",
            "release",
            1,
            gen,
            json!(true),
            None,
        )
        .unwrap();
    let oom = bomb.await.unwrap().unwrap_err();
    assert!(
        oom.contains(&format!(
            "memory budget ({} bytes) exceeded",
            HOLD_MEMORY_MIB * 1024 * 1024
        )),
        "{oom}"
    );
    assert_eq!(pool.metrics().total_memory_exceeded - oom_before, 1);

    let (elapsed, res) = loops.remove(spinning).await.unwrap();
    let err = res.unwrap_err();
    assert!(err.contains("timed out"), "{err}");
    s.engine().queries.cancel_all_in_flight();
    for h in loops {
        h.abort();
    }

    let hz95 = percentile(&hz, 95.0);
    let un95 = percentile(&un, 95.0);
    let (hz_bound, un_bound) = (50.0 * debug_slack(), 100.0 * debug_slack());
    assert!(
        hz95 <= hz_bound,
        "/healthz p95 under runaway {hz95:.3} ms > {hz_bound} ms"
    );
    assert!(
        un95 <= un_bound,
        "unrelated p95 under runaway {un95:.3} ms > {un_bound} ms"
    );
    report.insert(
        "isolation_baseline_healthz_p95_ms".into(),
        json!(round(percentile(&base_hz, 95.0), 3)),
    );
    report.insert(
        "isolation_baseline_unrelated_query_p95_ms".into(),
        json!(round(percentile(&base_un, 95.0), 3)),
    );
    report.insert("isolation_healthz_p95_ms".into(), json!(round(hz95, 3)));
    report.insert(
        "isolation_unrelated_query_p95_ms".into(),
        json!(round(un95, 3)),
    );
    report.insert(
        "isolation_memory_hog_limit_mib".into(),
        json!(HOLD_MEMORY_MIB),
    );
    report.insert(
        "isolation_memory_hog_held_mib".into(),
        json!(round(held_chunks.as_f64().unwrap() * 4.0 / 1024.0, 1)),
    );
    report.insert(
        "isolation_loop_interrupted_after_ms".into(),
        json!(round(ms(elapsed), 1)),
    );
}

/// 6. Idle VM reclamation: VMs idle past the configured TTL are dropped (and nothing earlier).
async fn idle_reclamation(s: &AppServices, report: &mut serde_json::Map<String, Value>) {
    let pool = s.engine().pool.clone();
    let admin = Principal::administrator();
    for inst in ["bench_pair_a", "bench_pair_b"] {
        s.provider_query(inst, json!({}), &admin, true)
            .await
            .unwrap();
    }
    s.provider_query(
        "linked_prs",
        json!({ "repository": "acme/payments", "state": "open" }),
        &admin,
        true,
    )
    .await
    .unwrap();
    let last_use = Instant::now();
    let active = pool.active_vm_count();
    assert!(
        active >= 5,
        "expected live VMs after activity, got {active}"
    );
    let fresh = ["bench_pair_a", "bench_pair_b", "linked_prs"];
    let uids: Vec<_> = fresh
        .iter()
        .map(|i| pool.vm_uid(i).expect("live VM"))
        .collect();
    let reclaimed_before = pool.total_reclaimed();
    // VMs left idle by earlier phases may go now; the ones just used must survive.
    pool.reclaim_idle(None);
    for (i, uid) in fresh.iter().zip(&uids) {
        assert_eq!(
            pool.vm_uid(i).as_ref(),
            Some(uid),
            "'{i}' reclaimed before its TTL"
        );
    }
    let ttl = Duration::from_secs(IDLE_TTL_SECS);
    let after = wait_for(
        "idle VMs to be reclaimed",
        ttl + Duration::from_secs(5),
        || {
            let pool = pool.clone();
            async move {
                pool.reclaim_idle(None);
                (pool.active_vm_count() == 0).then(|| last_use.elapsed())
            }
        },
    )
    .await;
    assert!(after >= ttl, "reclaimed before the TTL: {after:?}");
    assert!(pool.total_reclaimed() - reclaimed_before >= active as u64);
    // A reclaimed Instance boots lazily again on its next query.
    let t = Instant::now();
    let env = s
        .provider_query("bench_pair_a", json!({}), &admin, true)
        .await
        .unwrap();
    assert_eq!(env.value, Some(json!({ "rev": "v1" })));
    report.insert("reclaim_ttl_secs".into(), json!(IDLE_TTL_SECS));
    report.insert("reclaim_vms_reclaimed".into(), json!(active));
    report.insert(
        "reclaim_after_last_use_ms".into(),
        json!(round(ms(after), 1)),
    );
    report.insert(
        "reclaim_cold_restart_query_ms".into(),
        json!(round(ms(t.elapsed()), 3)),
    );
}

/// 7. Live graph replacement: readers running throughout a valid activation never observe a
/// partially replaced graph, and a failed activation leaves the old graph serving.
async fn live_graph_replacement(s: &AppServices, report: &mut serde_json::Map<String, Value>) {
    let admin = Principal::administrator();
    let stop = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    for _ in 0..4 {
        let (s, stop) = (s.clone(), Arc::clone(&stop));
        readers.push(tokio::spawn(async move {
            let (mut seen, mut lat) = (std::collections::BTreeSet::new(), Vec::new());
            while !stop.load(Ordering::SeqCst) {
                // Graph snapshot coherence: both halves always come from the same Revision.
                let g = s.graph();
                let ra = g.definitions["engineering.bench_pair_a"]
                    .runtime_source
                    .contains("v2");
                let rb = g.definitions["engineering.bench_pair_b"]
                    .runtime_source
                    .contains("v2");
                assert_eq!(
                    ra, rb,
                    "partial graph visible in generation {}",
                    g.generation_id
                );
                // Query coherence through the engine (downstream recomputed with its upstream).
                let t = Instant::now();
                let env = s
                    .provider_query("bench_pair_b", json!({}), &Principal::administrator(), true)
                    .await
                    .expect("reader query failed during activation");
                lat.push(ms(t.elapsed()));
                let v = env.value.unwrap();
                assert_eq!(v["a"], v["b"], "partial graph visible to a query: {v}");
                seen.insert(v["b"].as_str().unwrap().to_string());
                tokio::task::yield_now().await;
            }
            (seen, lat)
        }));
    }
    let gen_before = s.graph().generation_id;
    let draft = s.draft_begin(&admin).await.unwrap();
    let (d, r) = pair_a("v2");
    s.definition_upsert_in_draft(
        &draft.draft_id,
        "engineering",
        "bench_pair_a",
        &d,
        Some(&r),
        None,
        &admin,
    )
    .unwrap();
    let (d, r) = pair_b("v2");
    s.definition_upsert_in_draft(
        &draft.draft_id,
        "engineering",
        "bench_pair_b",
        &d,
        Some(&r),
        None,
        &admin,
    )
    .unwrap();
    let t = Instant::now();
    s.draft_activate(&draft.draft_id, &admin).await.unwrap();
    let activation_ms = ms(t.elapsed());
    let gen_after = s.graph().generation_id;
    assert!(gen_after > gen_before);
    // Keep reading on the new graph for a moment, then try an invalid replacement.
    wait_for("readers to observe v2", Duration::from_secs(10), || async {
        let env = s
            .provider_query("bench_pair_b", json!({}), &admin, false)
            .await
            .ok()?;
        (env.value? == json!({ "a": "v2", "b": "v2" })).then_some(())
    })
    .await;

    let bad = s.draft_begin(&admin).await.unwrap();
    let (d, _) = pair_a("v3");
    s.definition_upsert_in_draft(
        &bad.draft_id,
        "engineering",
        "bench_pair_a",
        &d,
        Some("local R = {}\nfunction R.query(ctx) return { rev = \"v3\" } end\nerror(\"v3 module fails to initialize\")\nreturn R\n"),
        None,
        &admin,
    )
    .unwrap();
    assert!(
        s.draft_activate(&bad.draft_id, &admin).await.is_err(),
        "a draft whose runtime module fails to initialize must not activate"
    );
    assert_eq!(
        s.graph().generation_id,
        gen_after,
        "failed activation replaced the graph"
    );
    stop.store(true, Ordering::SeqCst);
    let mut seen_all = std::collections::BTreeSet::new();
    let mut lat_all = Vec::new();
    for r in readers {
        let (seen, lat) = r.await.unwrap();
        seen_all.extend(seen);
        lat_all.extend(lat);
    }
    assert!(
        seen_all.contains("v1") && seen_all.contains("v2"),
        "{seen_all:?}"
    );
    assert!(!seen_all.contains("v3"));
    let env = s
        .provider_query("bench_pair_b", json!({}), &admin, true)
        .await
        .unwrap();
    assert_eq!(env.value, Some(json!({ "a": "v2", "b": "v2" })));
    report.insert(
        "graph_swap_activation_ms".into(),
        json!(round(activation_ms, 1)),
    );
    report.insert("graph_swap_reader_queries".into(), json!(lat_all.len()));
    report.insert(
        "graph_swap_reader_p95_ms".into(),
        json!(round(percentile(&lat_all, 95.0), 3)),
    );
    report.insert("graph_swap_partial_states_observed".into(), json!(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn benchmark_gates_section_15_3() {
    let mut report = serde_json::Map::new();
    report.insert(
        "profile".into(),
        json!(if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }),
    );
    report.insert(
        "host".into(),
        json!(format!(
            "{} {}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )),
    );

    startup_and_idle_rss(&mut report).await;

    let w = bench_workspace();
    let b = boot(&w, true, ResourceBudget::default()).await;
    warm_query(&b.services, &mut report).await;
    propagation(&b.services, &mut report).await;
    dedup(&b.services, &mut report).await;
    failure_isolation(&b, &mut report).await;
    idle_reclamation(&b.services, &mut report).await;
    live_graph_replacement(&b.services, &mut report).await;

    println!("BENCHMARK_REPORT={}", Value::Object(report));
}
