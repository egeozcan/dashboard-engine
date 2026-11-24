//! Query Signal semantics: status metadata, last-success timestamps, provider deltas with
//! source-local sequences, subscription-first keys, and idle pruning (§6.1, §6.2).

use dash_core::signal::{EnvelopeKind, ProviderDeltaOutcome, QueryStatus};
use dash_core::{
    apply_delta_envelope, ConfiguredGraph, DeltaOp, FailureInjectionRule, InstanceConfig,
    LoadedDefinition, Principal, RuntimeEngine,
};
use dash_luau::{evaluate_declaration, DefinitionCategory, ResourceBudget};
use dash_store::SqliteStore;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

fn load_def(decl: &str, runtime: &str) -> LoadedDefinition {
    let mut sources = BTreeMap::new();
    sources.insert("definition.luau".into(), decl.as_bytes().to_vec());
    sources.insert("runtime.luau".into(), runtime.as_bytes().to_vec());
    let manifest = evaluate_declaration(decl, &sources).unwrap();
    LoadedDefinition {
        package_name: "test".into(),
        manifest,
        definition_source: decl.into(),
        runtime_source: runtime.into(),
        tests_source: None,
        fixtures: BTreeMap::new(),
        widget_assets: BTreeMap::new(),
    }
}

fn engine() -> RuntimeEngine {
    let mut graph = ConfiguredGraph {
        generation_id: 1,
        ..Default::default()
    };
    let def = load_def(
        r#"local dash = require("dash")
return dash.data_provider { id = "test.list", updates = { on_demand = true, push = true } }"#,
        r#"local R = {}
function R.query(ctx) return { { id = "1", state = "open" }, { id = "2", state = "open" } } end
return R"#,
    );
    graph.definitions.insert("test.list".into(), def);
    graph.instances.insert(
        "inst_list".into(),
        InstanceConfig {
            id: "inst_list".into(),
            category: DefinitionCategory::DataProvider,
            definition: "test.list".into(),
            inputs: BTreeMap::new(),
            config: json!({}),
            secrets: BTreeMap::new(),
            capabilities: vec![],
            run_as: None,
            enabled: None,
        },
    );
    let store = SqliteStore::open_in_memory().unwrap();
    RuntimeEngine::new(graph, store, ResourceBudget::default()).unwrap()
}

#[tokio::test]
async fn resolved_query_key_matches_query_and_allows_subscribe_first() {
    let engine = engine();
    let admin = Principal::administrator();
    let qk = engine
        .queries
        .resolve_query_key("inst_list", &json!({}), &admin)
        .unwrap();
    let mut rx = engine.signals.subscribe_stream(&qk);
    let env = engine
        .queries
        .query("inst_list", json!({}), &admin, false)
        .await
        .unwrap();
    assert_eq!(env.query_key, qk);
    // The subscriber saw the whole lifecycle: loading, then the fresh snapshot.
    let first = rx.recv().await.unwrap();
    assert_eq!(first.status, QueryStatus::Loading);
    let second = rx.recv().await.unwrap();
    assert_eq!((second.status, second.version), (QueryStatus::Fresh, env.version));

    assert!(engine
        .queries
        .resolve_query_key("missing", &json!({}), &admin)
        .is_err());
}

#[tokio::test]
async fn cache_hits_and_stale_envelopes_report_last_successful_update() {
    let engine = engine();
    let admin = Principal::administrator();
    let fresh = engine
        .queries
        .query("inst_list", json!({}), &admin, false)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let hit = engine
        .queries
        .query("inst_list", json!({}), &admin, false)
        .await
        .unwrap();
    assert_eq!(hit.updated_at, fresh.updated_at);

    engine.telemetry.inject_failure(FailureInjectionRule {
        instance_id: "inst_list".into(),
        mode: "error".into(),
        message: "down".into(),
        remaining: Some(1),
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let stale = engine
        .queries
        .query("inst_list", json!({}), &admin, true)
        .await
        .unwrap();
    assert_eq!(stale.status, QueryStatus::Stale);
    assert_eq!(stale.updated_at, fresh.updated_at);
}

#[tokio::test]
async fn failure_without_cached_value_publishes_error_signal() {
    let engine = engine();
    let admin = Principal::administrator();
    let qk = engine
        .queries
        .resolve_query_key("inst_list", &json!({}), &admin)
        .unwrap();
    let mut rx = engine.signals.subscribe_stream(&qk);
    engine.telemetry.inject_failure(FailureInjectionRule {
        instance_id: "inst_list".into(),
        mode: "error".into(),
        message: "down".into(),
        remaining: Some(1),
    });
    assert!(engine
        .queries
        .query("inst_list", json!({}), &admin, false)
        .await
        .is_err());
    assert_eq!(rx.recv().await.unwrap().status, QueryStatus::Loading);
    let err = rx.recv().await.unwrap();
    assert_eq!(err.status, QueryStatus::Error);
    assert!(err.error.unwrap().contains("down"));
}

#[tokio::test]
async fn provider_deltas_are_sequence_checked_and_gaps_resync() {
    let engine = engine();
    let admin = Principal::administrator();
    let init = engine
        .queries
        .query("inst_list", json!({}), &admin, false)
        .await
        .unwrap();
    let mut rx = engine.signals.subscribe_stream(&init.query_key);
    let mut local_ver = init.version;
    let mut rows = init.value.clone().unwrap().as_array().unwrap().clone();

    let update = |state: &str| {
        vec![DeltaOp::Update {
            key: "1".into(),
            value: json!({"id": "1", "state": state}),
        }]
    };
    // Baseline + in-order deltas are applied and published with verified bases.
    for (seq, state) in [(10, "merged"), (11, "closed")] {
        match engine
            .queries
            .apply_provider_delta("inst_list", &json!({}), &admin, seq, update(state))
            .await
            .unwrap()
        {
            ProviderDeltaOutcome::Applied(env) => assert_eq!(env.source_sequence, Some(seq)),
            other => panic!("expected Applied, got {other:?}"),
        }
        let env = rx.recv().await.unwrap();
        assert_eq!(env.kind, EnvelopeKind::Delta);
        apply_delta_envelope(&mut local_ver, &mut rows, &env, "id").unwrap();
    }
    assert_eq!(rows[0]["state"], "closed");
    assert_eq!(
        engine.signals.latest_snapshot(&init.query_key).unwrap().version,
        local_ver
    );

    // Replays are ignored.
    assert!(matches!(
        engine
            .queries
            .apply_provider_delta("inst_list", &json!({}), &admin, 11, update("x"))
            .await
            .unwrap(),
        ProviderDeltaOutcome::Duplicate { last_sequence: 11 }
    ));

    // A gap triggers resynchronization from a fresh snapshot.
    match engine
        .queries
        .apply_provider_delta("inst_list", &json!({}), &admin, 15, update("x"))
        .await
        .unwrap()
    {
        ProviderDeltaOutcome::Resynced { reason, snapshot } => {
            assert!(reason.contains("gap"));
            assert_eq!(snapshot.value.unwrap()[0]["state"], "open");
        }
        other => panic!("expected Resynced, got {other:?}"),
    }
    let mut resynced = false;
    while let Ok(env) = rx.try_recv() {
        if env.kind == EnvelopeKind::Snapshot && env.status == QueryStatus::Fresh {
            apply_delta_envelope(&mut local_ver, &mut rows, &env, "id").unwrap();
            resynced = true;
        }
    }
    assert!(resynced);
    assert_eq!(rows[0]["state"], "open");
}

#[tokio::test]
async fn provider_deltas_require_declared_push_updates() {
    let engine = engine();
    let mut g = (*engine.graph.load_full()).clone();
    g.definitions.get_mut("test.list").unwrap().manifest.updates.push = false;
    engine.graph.store(Arc::new(g));
    let err = engine
        .queries
        .apply_provider_delta("inst_list", &json!({}), &Principal::administrator(), 1, vec![])
        .await
        .unwrap_err();
    assert!(err.contains("push"));
}

#[tokio::test]
async fn prune_idle_releases_unsubscribed_channels() {
    let engine = engine();
    let admin = Principal::administrator();
    let qk = engine
        .queries
        .resolve_query_key("inst_list", &json!({}), &admin)
        .unwrap();
    {
        let _rx = engine.signals.subscribe_stream(&qk);
        engine.queries.prune_idle();
        assert_eq!(engine.signals.query_channel_count(), 1);
    }
    // Unsubscribed channels survive one grace period (default 30s), so they are still retained
    // right after the last receiver drops.
    engine.queries.prune_idle();
    assert_eq!(engine.signals.subscriber_count(&qk), 0);
    // Fresh cached entries are not expired.
    engine
        .queries
        .query("inst_list", json!({}), &admin, false)
        .await
        .unwrap();
    assert_eq!(engine.queries.prune_idle(), 0);
    assert_eq!(engine.queries.cache_len(), 1);
}
