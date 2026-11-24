//! R3:luau-pool-nested-permit-starvation: a dependency query issued from inside a running
//! provider runs in a detached singleflight leader, yet it must still be admitted as a nested
//! execution (riding on its caller's pool permit and inheriting the caller's deadline).

use dash_core::{ConfiguredGraph, InstanceConfig, LoadedDefinition, Principal, RuntimeEngine};
use dash_luau::{evaluate_declaration, DefinitionCategory, ResourceBudget};
use dash_store::SqliteStore;
use serde_json::json;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

fn load_def(decl: &str, runtime: &str) -> LoadedDefinition {
    let mut sources = BTreeMap::new();
    sources.insert("definition.luau".into(), decl.as_bytes().to_vec());
    sources.insert("runtime.luau".into(), runtime.as_bytes().to_vec());
    LoadedDefinition {
        package_name: "t".into(),
        manifest: evaluate_declaration(decl, &sources).unwrap(),
        definition_source: decl.into(),
        runtime_source: runtime.into(),
        tests_source: None,
        fixtures: BTreeMap::new(),
        widget_assets: BTreeMap::new(),
    }
}

fn provider(id: &str, inputs: &[(&str, &str)]) -> InstanceConfig {
    InstanceConfig {
        id: id.into(),
        category: DefinitionCategory::DataProvider,
        definition: format!("t.{id}"),
        inputs: inputs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        config: json!({}),
        secrets: BTreeMap::new(),
        capabilities: Default::default(),
        run_as: None,
        enabled: None,
    }
}

/// grandparent -> parent -> child, every hop a provider-to-provider dependency query.
fn chain_graph() -> ConfiguredGraph {
    let mut g = ConfiguredGraph {
        generation_id: 1,
        ..Default::default()
    };
    g.definitions.insert(
        "t.child".into(),
        load_def(
            "local dash = require(\"dash\")\nreturn dash.data_provider { id = \"t.child\" }",
            "local R = {}\nfunction R.query(ctx) return 41 end\nreturn R",
        ),
    );
    for (id, up) in [("parent", "t.child"), ("grandparent", "t.parent")] {
        g.definitions.insert(
            format!("t.{id}"),
            load_def(
                &format!(
                    "local dash = require(\"dash\")\nreturn dash.data_provider {{ id = \"t.{id}\", inputs = {{ up = dash.data_ref(\"{up}\") }} }}"
                ),
                "local R = {}\nfunction R.query(ctx) return ctx:await(ctx.inputs.up:query({})) + 1 end\nreturn R",
            ),
        );
    }
    g.instances.insert("child".into(), provider("child", &[]));
    g.instances
        .insert("parent".into(), provider("parent", &[("up", "child")]));
    g.instances.insert(
        "grandparent".into(),
        provider("grandparent", &[("up", "parent")]),
    );
    g
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dependency_queries_ride_on_the_callers_permit() {
    // One pool-wide permit: the top-level grandparent holds it for the whole chain. Before the
    // fix each detached dependency flight waited for that same permit until the caller's
    // deadline expired.
    let engine = RuntimeEngine::new(
        chain_graph(),
        SqliteStore::open_in_memory().unwrap(),
        ResourceBudget {
            max_concurrent_executions: 1,
            wall_timeout: Duration::from_millis(1_500),
            queue_timeout: Duration::from_secs(10),
            ..ResourceBudget::default()
        },
    )
    .unwrap();
    let admin = Principal::administrator();
    let t0 = Instant::now();
    let env = engine
        .queries
        .query("grandparent", json!({}), &admin, true)
        .await
        .expect("nested dependency queries must not starve on the pool permit");
    assert_eq!(env.value, Some(json!(43)), "{env:?}");
    assert!(
        t0.elapsed() < Duration::from_millis(1_000),
        "dependency chain waited for a permit: {:?}",
        t0.elapsed()
    );

    // Several concurrent top-level chains with a single permit queue instead of timing out.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let (e, a) = (engine.clone(), admin.clone());
        handles.push(tokio::spawn(async move {
            e.queries.query("grandparent", json!({}), &a, true).await
        }));
    }
    for h in handles {
        let env = h.await.unwrap().expect("queued chain completes");
        assert_eq!(env.value, Some(json!(43)), "{env:?}");
    }
}
