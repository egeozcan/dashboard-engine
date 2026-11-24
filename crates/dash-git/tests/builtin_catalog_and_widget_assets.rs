//! `builtins/catalog.json` is the single source of built-in Definitions, and custom widget
//! assets are published only from `assets/` of widget provider Definitions (§4.4, §10.1).

use dash_git::load_workspace_graph;
use dash_luau::DefinitionCategory;
use std::path::Path;
use tempfile::TempDir;

fn catalog() -> serde_json::Value {
    let raw = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../builtins/catalog.json"),
    )
    .unwrap();
    serde_json::from_str(&raw).unwrap()
}

fn write(path: &Path, body: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

#[test]
fn builtin_definitions_are_loaded_from_catalog_json() {
    let tmp = TempDir::new().unwrap();
    let g = load_workspace_graph(tmp.path(), &tmp.path().join("data"), "test", 0, false).unwrap();
    let cat = catalog();
    let widgets = cat["widget_providers"].as_array().unwrap();
    assert!(!widgets.is_empty());
    for w in widgets {
        let id = w["id"].as_str().unwrap();
        let def = g
            .definitions
            .get(id)
            .unwrap_or_else(|| panic!("built-in {id} missing"));
        assert_eq!(def.manifest.category, DefinitionCategory::WidgetProvider);
        assert_eq!(def.manifest.description, w["description"].as_str().unwrap());
    }
    for d in cat["data_providers"].as_array().unwrap() {
        let id = d["id"].as_str().unwrap();
        assert_eq!(
            g.definitions[id].manifest.description,
            d["description"].as_str().unwrap()
        );
    }
    // Glossary: a Dry run is never called a "preview".
    let button = &g.definitions["dash.button"].manifest.description;
    assert!(button.contains("Dry run"), "{button}");
    assert!(!button.to_ascii_lowercase().contains("preview"), "{button}");
}

#[test]
fn widget_assets_come_only_from_assets_dir_of_widget_providers() {
    let tmp = TempDir::new().unwrap();
    let ws = tmp.path().join("ws");
    write(
        &ws.join("packages/demo/package.toml"),
        "name = \"demo\"\nversion = \"0.1.0\"\n",
    );
    write(
        &ws.join("packages/demo/badge/definition.luau"),
        "local dash = require(\"dash\")\nreturn dash.widget_provider { id = \"demo.badge\", asset = \"assets/app.html\", props = dash.object { label = dash.string() } }\n",
    );
    write(&ws.join("packages/demo/badge/assets/app.html"), "<p>hi</p>");
    write(&ws.join("packages/demo/badge/notes.json"), "{}");
    write(
        &ws.join("packages/demo/metric/definition.luau"),
        "local dash = require(\"dash\")\nreturn dash.data_provider { id = \"demo.metric\", handler = \"query\" }\n",
    );
    write(
        &ws.join("packages/demo/metric/runtime.luau"),
        "local R = {}\nfunction R.query(ctx) return {} end\nreturn R\n",
    );
    write(
        &ws.join("packages/demo/metric/assets/leak.html"),
        "<p>no</p>",
    );

    let g = load_workspace_graph(&ws, &tmp.path().join("data"), "test", 0, false).unwrap();
    let badge = &g.definitions["demo.badge"];
    assert!(badge.widget_assets.contains_key("assets/app.html"));
    assert!(!badge.widget_assets.contains_key("notes.json"));
    assert!(
        g.definitions["demo.metric"].widget_assets.is_empty(),
        "non-widget Definitions must not publish assets"
    );

    // A widget provider whose declared asset lies outside assets/ is rejected.
    write(
        &ws.join("packages/demo/badge/definition.luau"),
        "local dash = require(\"dash\")\nreturn dash.widget_provider { id = \"demo.badge\", asset = \"../metric/runtime.luau\", props = dash.object { label = dash.string() } }\n",
    );
    let err = load_workspace_graph(&ws, &tmp.path().join("data"), "test", 0, false)
        .unwrap_err()
        .to_string();
    // Escaping the Definition directory is already rejected by the declaration path check.
    assert!(
        err.contains("stay inside the Definition directory"),
        "{err}"
    );

    // Inside the Definition directory but outside assets/: rejected by the asset rule.
    write(
        &ws.join("packages/demo/badge/definition.luau"),
        "local dash = require(\"dash\")\nreturn dash.widget_provider { id = \"demo.badge\", asset = \"notes.json\", props = dash.object { label = dash.string() } }\n",
    );
    let err = load_workspace_graph(&ws, &tmp.path().join("data"), "test", 0, false)
        .unwrap_err()
        .to_string();
    assert!(err.contains("under assets/"), "{err}");
}
