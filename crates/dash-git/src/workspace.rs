//! Canonical Workspace loader & serializer ensuring Git/UI/CLI/MCP round-trip without drift (§20).

use crate::git_cli::GitError;
use crate::packages::{
    cached_package_roots, is_valid_package_name, read_packages_lock, read_packages_toml,
    write_packages_toml, PackageManifestToml, RESERVED_PACKAGE_NAME,
};
use dash_core::{
    ConfiguredGraph, DashboardDoc, InstanceConfig, LoadedDefinition, ServicePrincipalRequested,
};
use dash_luau::{evaluate_declaration, DefinitionCategory};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Per-file size limit for custom widget assets (§10.1 size checks).
pub const MAX_WIDGET_ASSET_BYTES: usize = 5 * 1024 * 1024;
/// Total widget asset bytes allowed per Definition.
pub const MAX_WIDGET_ASSETS_TOTAL_BYTES: usize = 20 * 1024 * 1024;
/// File extensions a custom widget may ship as static assets.
pub const ALLOWED_WIDGET_ASSET_EXTENSIONS: &[&str] = &[
    "html", "htm", "css", "js", "mjs", "json", "svg", "png", "jpg", "jpeg", "gif", "webp", "ico",
    "woff", "woff2", "ttf", "otf", "txt", "md", "map",
];

/// Validate a Package or Definition name used to build Workspace paths:
/// `^[a-z][a-z0-9_]{0,63}$` (no separators, no `..`, never absolute).
pub fn validate_identifier(kind: &str, value: &str) -> Result<(), GitError> {
    if dash_luau::declaration::is_valid_identifier(value) {
        Ok(())
    } else {
        Err(GitError::Workspace(format!(
            "invalid {kind} '{value}': expected [a-z][a-z0-9_]* (max 64 chars)"
        )))
    }
}

/// Validate a Dashboard ID used as a file name: `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`.
pub fn validate_dashboard_id(value: &str) -> Result<(), GitError> {
    let mut chars = value.chars();
    let ok = matches!(chars.next(), Some(c) if c.is_ascii_alphanumeric())
        && value.len() <= 128
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if ok {
        Ok(())
    } else {
        Err(GitError::Workspace(format!(
            "invalid dashboard id '{value}': expected [A-Za-z0-9][A-Za-z0-9_-]* (max 128 chars)"
        )))
    }
}

/// Async wrapper that runs [`load_workspace_graph`] (file reads + Luau declaration evaluation)
/// on the blocking thread pool so it never occupies a Tokio event-loop thread (§3.2).
pub async fn load_workspace_graph_async(
    workspace_dir: &Path,
    data_dir: &Path,
    revision_commit: &str,
    generation_id: i64,
    is_preview: bool,
) -> Result<ConfiguredGraph, GitError> {
    let ws = workspace_dir.to_path_buf();
    let data = data_dir.to_path_buf();
    let rev = revision_commit.to_string();
    tokio::task::spawn_blocking(move || {
        load_workspace_graph(&ws, &data, &rev, generation_id, is_preview)
    })
    .await
    .map_err(|e| GitError::Workspace(format!("workspace loader task failed: {e}")))?
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct ConnectorsFile {
    #[serde(default)]
    connectors: BTreeMap<String, InstanceEntryYaml>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct DataFile {
    #[serde(default)]
    data: BTreeMap<String, InstanceEntryYaml>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct ActionsFile {
    #[serde(default)]
    actions: BTreeMap<String, InstanceEntryYaml>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct AdaptersFile {
    #[serde(default)]
    adapters: BTreeMap<String, InstanceEntryYaml>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct AutomationsFile {
    #[serde(default)]
    automations: BTreeMap<String, InstanceEntryYaml>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct ServicePrincipalsFile {
    #[serde(default)]
    service_principals: BTreeMap<String, ServicePrincipalEntryYaml>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServicePrincipalEntryYaml {
    #[serde(default)]
    description: String,
    #[serde(default)]
    ceiling: dash_core::CapabilityCeiling,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InstanceEntryYaml {
    definition: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    inputs: BTreeMap<String, String>,
    #[serde(default = "default_null", skip_serializing_if = "serde_json::Value::is_null")]
    config: serde_json::Value,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    secrets: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    run_as: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
}

fn default_null() -> serde_json::Value {
    serde_json::Value::Null
}

impl From<&InstanceConfig> for InstanceEntryYaml {
    fn from(inst: &InstanceConfig) -> Self {
        Self {
            definition: inst.definition.clone(),
            inputs: inst.inputs.clone(),
            config: inst.config.clone(),
            secrets: inst.secrets.clone(),
            capabilities: inst.capabilities.clone(),
            run_as: inst.run_as.clone(),
            enabled: inst.enabled,
        }
    }
}

/// Load a complete `ConfiguredGraph` from a Workspace directory + cached locked Packages.
pub fn load_workspace_graph(
    workspace_dir: &Path,
    data_dir: &Path,
    revision_commit: &str,
    generation_id: i64,
    is_preview: bool,
) -> Result<ConfiguredGraph, GitError> {
    let mut graph = ConfiguredGraph {
        revision_commit: revision_commit.to_string(),
        generation_id,
        is_preview,
        ..Default::default()
    };

    register_builtin_definitions(&mut graph.definitions);

    // 1. dashboard.toml
    let dash_toml_path = workspace_dir.join("dashboard.toml");
    if dash_toml_path.exists() {
        let raw = std::fs::read_to_string(&dash_toml_path)?;
        graph.workspace = toml::from_str(&raw)
            .map_err(|e| GitError::Workspace(format!("invalid dashboard.toml: {e}")))?;
    }

    // 2. Load locked external packages from cache + local workspace packages
    let lock = read_packages_lock(workspace_dir)?;
    let cached_external = cached_package_roots(data_dir, &lock)?;
    let local_pkgs = discover_local_packages(workspace_dir)?;
    for (local_name, _) in &local_pkgs {
        if cached_external.iter().any(|(n, _)| n == local_name) {
            return Err(GitError::Workspace(format!(
                "package name '{local_name}' is both a Workspace-local package and a locked external dependency"
            )));
        }
    }
    for (pkg_name, root) in cached_external {
        if !is_valid_package_name(&pkg_name) || pkg_name == RESERVED_PACKAGE_NAME {
            return Err(GitError::Workspace(format!(
                "invalid or reserved external package name '{pkg_name}'"
            )));
        }
        load_package_dir(&pkg_name, &root, &mut graph.definitions)?;
    }
    let local_names: BTreeSet<String> = local_pkgs.iter().map(|(n, _)| n.clone()).collect();
    for (pkg_name, pkg_dir) in local_pkgs {
        load_package_dir(&pkg_name, &pkg_dir, &mut graph.definitions)?;
    }
    finalize_definitions(&mut graph.definitions, &local_names)?;

    // 3. Load instances/*.yaml
    let inst_dir = workspace_dir.join("instances");
    if inst_dir.exists() {
        let conn_path = inst_dir.join("connectors.yaml");
        if conn_path.exists() {
            let f: ConnectorsFile = serde_yaml_ng::from_str(&std::fs::read_to_string(&conn_path)?)
                .map_err(|e| GitError::Workspace(format!("connectors.yaml: {e}")))?;
            insert_instances(&mut graph.instances, f.connectors, DefinitionCategory::Connector);
        }
        let data_path = inst_dir.join("data.yaml");
        if data_path.exists() {
            let f: DataFile = serde_yaml_ng::from_str(&std::fs::read_to_string(&data_path)?)
                .map_err(|e| GitError::Workspace(format!("data.yaml: {e}")))?;
            insert_instances(&mut graph.instances, f.data, DefinitionCategory::DataProvider);
        }
        let act_path = inst_dir.join("actions.yaml");
        if act_path.exists() {
            let f: ActionsFile = serde_yaml_ng::from_str(&std::fs::read_to_string(&act_path)?)
                .map_err(|e| GitError::Workspace(format!("actions.yaml: {e}")))?;
            insert_instances(&mut graph.instances, f.actions, DefinitionCategory::ActionProvider);
        }
        let adp_path = inst_dir.join("adapters.yaml");
        if adp_path.exists() {
            let f: AdaptersFile = serde_yaml_ng::from_str(&std::fs::read_to_string(&adp_path)?)
                .map_err(|e| GitError::Workspace(format!("adapters.yaml: {e}")))?;
            insert_instances(&mut graph.instances, f.adapters, DefinitionCategory::Adapter);
        }
        let auto_path = inst_dir.join("automations.yaml");
        if auto_path.exists() {
            let f: AutomationsFile = serde_yaml_ng::from_str(&std::fs::read_to_string(&auto_path)?)
                .map_err(|e| GitError::Workspace(format!("automations.yaml: {e}")))?;
            insert_instances(&mut graph.instances, f.automations, DefinitionCategory::Automation);
        }
        let sp_path = inst_dir.join("service_principals.yaml");
        if sp_path.exists() {
            let f: ServicePrincipalsFile =
                serde_yaml_ng::from_str(&std::fs::read_to_string(&sp_path)?)
                    .map_err(|e| GitError::Workspace(format!("service_principals.yaml: {e}")))?;
            for (id, entry) in f.service_principals {
                graph.service_principals.insert(
                    id.clone(),
                    ServicePrincipalRequested {
                        id,
                        description: entry.description,
                        ceiling: entry.ceiling,
                    },
                );
            }
        }
    }

    // 4. Load dashboards/*.yaml
    let dash_dir = workspace_dir.join("dashboards");
    if dash_dir.exists() {
        let mut files: Vec<PathBuf> = std::fs::read_dir(&dash_dir)?
            .filter_map(|e| e.ok().map(|x| x.path()))
            .filter(|p| {
                p.extension()
                    .and_then(|s| s.to_str())
                    .map(|e| e == "yaml" || e == "yml")
                    .unwrap_or(false)
            })
            .collect();
        files.sort();
        for p in files {
            let doc: DashboardDoc = serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
                .map_err(|e| GitError::Workspace(format!("{}: {e}", p.display())))?;
            graph.dashboards.insert(doc.id.clone(), doc);
        }
    }

    Ok(graph)
}

fn register_builtin_definitions(out: &mut BTreeMap<String, LoadedDefinition>) {
    use dash_luau::{DefinitionManifest, SchemaSpec, UpdateModes};
    let mut hist_manifest = DefinitionManifest {
        id: "dash.history_series".into(),
        category: DefinitionCategory::DataProvider,
        identity_version: "builtin:dash.history_series@1".into(),
        description: "Built-in sampled time-series history data provider".into(),
        runtime: String::new(),
        handler: "query".into(),
        dry_run_handler: None,
        reconcile_handler: None,
        request_handler: None,
        target_connector: None,
        fixtures: BTreeMap::new(),
        capabilities: vec![],
        required_capabilities: vec![],
        optional_capabilities: vec![],
        credentials: vec![],
        operations: BTreeMap::new(),
        inputs: BTreeMap::new(),
        config: Some(SchemaSpec::Any),
        query: Some(SchemaSpec::Any),
        args: None,
        output: Some(SchemaSpec::Any),
        result: None,
        emits: vec![],
        invalidates: vec![],
        dashboard_state_reads: vec![],
        accepts_requests: vec![],
        request_schemas: BTreeMap::new(),
        widget_props: None,
        widget_asset: None,
        updates: UpdateModes::default(),
        triggers: vec![],
        libraries: vec![],
        generated_luau_types: String::new(),
    };
    hist_manifest.generate_luau_types();
    out.insert(
        "dash.history_series".into(),
        LoadedDefinition {
            package_name: "dash".into(),
            manifest: hist_manifest,
            definition_source: "-- built-in dash.history_series".into(),
            runtime_source: String::new(),
            tests_source: None,
            fixtures: BTreeMap::new(),
            widget_assets: BTreeMap::new(),
        },
    );

    for (wid, desc) in [
        ("dash.table", "Interactive data table widget"),
        ("dash.chart", "Time-series SVG chart widget"),
        ("dash.map", "Geographic marker map widget"),
        ("dash.counter", "KPI counter and status badge widget"),
        ("dash.text", "Formatted text block widget"),
        ("dash.form", "Typed action/query parameter form widget"),
        ("dash.button", "Action request trigger button"),
        ("dash.container", "Flex layout container"),
        ("dash.grid", "Responsive grid layout container"),
        ("dash.tabs", "Tabbed container widget"),
        ("dash.filter", "Filter control widget"),
    ] {
        let mut m = DefinitionManifest {
            id: wid.into(),
            category: DefinitionCategory::WidgetProvider,
            identity_version: format!("builtin:{wid}@1"),
            description: desc.into(),
            runtime: String::new(),
            handler: String::new(),
            dry_run_handler: None,
            reconcile_handler: None,
            request_handler: None,
            target_connector: None,
            fixtures: BTreeMap::new(),
            capabilities: vec![],
            required_capabilities: vec![],
            optional_capabilities: vec![],
            credentials: vec![],
            operations: BTreeMap::new(),
            inputs: BTreeMap::new(),
            config: None,
            query: None,
            args: None,
            output: None,
            result: None,
            emits: vec![],
            invalidates: vec![],
            dashboard_state_reads: vec![],
            accepts_requests: vec![],
        request_schemas: BTreeMap::new(),
            widget_props: Some(SchemaSpec::Any),
            widget_asset: Some("builtin".into()),
            updates: UpdateModes::default(),
            triggers: vec![],
            libraries: vec![],
            generated_luau_types: String::new(),
        };
        m.generate_luau_types();
        out.insert(
            wid.into(),
            LoadedDefinition {
                package_name: "dash".into(),
                manifest: m,
                definition_source: format!("-- built-in {wid}"),
                runtime_source: String::new(),
                tests_source: None,
                fixtures: BTreeMap::new(),
                widget_assets: BTreeMap::new(),
            },
        );
    }
}

fn insert_instances(
    dest: &mut BTreeMap<String, InstanceConfig>,
    entries: BTreeMap<String, InstanceEntryYaml>,
    category: DefinitionCategory,
) {
    for (id, e) in entries {
        dest.insert(
            id.clone(),
            InstanceConfig {
                id,
                category,
                definition: e.definition,
                inputs: e.inputs,
                config: e.config,
                secrets: e.secrets,
                capabilities: e.capabilities,
                run_as: e.run_as,
                enabled: e.enabled,
            },
        );
    }
}

/// Enumerate Workspace-local Packages under `packages/` as `(package name, directory)`.
///
/// When `packages.toml` lists `[local] packages = [...]`, exactly those directories are loaded
/// (a listed but missing package is an error, an unlisted directory is ignored). Otherwise every
/// non-hidden directory is a Package. A Package's `package.toml` name must equal its directory
/// name, so two directories can never claim the same Package.
fn discover_local_packages(workspace_dir: &Path) -> Result<Vec<(String, PathBuf)>, GitError> {
    let local_pkgs_dir = workspace_dir.join("packages");
    let listed = read_packages_toml(workspace_dir)
        .map_err(|e| GitError::Workspace(e.to_string()))?
        .local
        .packages;

    let mut dirs: Vec<(String, PathBuf)> = Vec::new();
    if local_pkgs_dir.exists() {
        reject_symlink(&local_pkgs_dir)?;
        for entry in std::fs::read_dir(&local_pkgs_dir)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') {
                continue;
            }
            if ft.is_symlink() {
                return Err(GitError::Workspace(format!(
                    "symbolic links are not allowed in packages/: {}",
                    entry.path().display()
                )));
            }
            if ft.is_dir() {
                dirs.push((name, entry.path()));
            }
        }
    }
    dirs.sort();

    let selected: Vec<(String, PathBuf)> = if listed.is_empty() {
        dirs
    } else {
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for name in &listed {
            if !seen.insert(name.clone()) {
                return Err(GitError::Workspace(format!(
                    "packages.toml [local] lists package '{name}' more than once"
                )));
            }
            match dirs.iter().find(|(n, _)| n == name) {
                Some(found) => out.push(found.clone()),
                None => {
                    return Err(GitError::Workspace(format!(
                        "packages.toml [local] lists package '{name}' but packages/{name}/ does not exist"
                    )))
                }
            }
        }
        out.sort();
        out
    };

    for (name, dir) in &selected {
        validate_identifier("package name", name)?;
        if name == RESERVED_PACKAGE_NAME {
            return Err(GitError::Workspace(format!(
                "package name '{RESERVED_PACKAGE_NAME}' is reserved for built-in Definitions"
            )));
        }
        let pkg_toml_path = dir.join("package.toml");
        if pkg_toml_path.exists() {
            reject_symlink(&pkg_toml_path)?;
            let raw = std::fs::read_to_string(&pkg_toml_path)?;
            let parsed: PackageManifestToml = toml::from_str(&raw).map_err(|e| {
                GitError::Workspace(format!("invalid {}: {e}", pkg_toml_path.display()))
            })?;
            if &parsed.name != name {
                return Err(GitError::Workspace(format!(
                    "{} declares name '{}' but lives in packages/{name}/",
                    pkg_toml_path.display(),
                    parsed.name
                )));
            }
        }
    }
    Ok(selected)
}

fn reject_symlink(path: &Path) -> Result<(), GitError> {
    match std::fs::symlink_metadata(path) {
        Ok(md) if md.file_type().is_symlink() => Err(GitError::Workspace(format!(
            "symbolic links are not allowed in the Workspace: {}",
            path.display()
        ))),
        _ => Ok(()),
    }
}

fn load_package_dir(
    pkg_name: &str,
    pkg_dir: &Path,
    out: &mut BTreeMap<String, LoadedDefinition>,
) -> Result<(), GitError> {
    let mut def_dirs = Vec::new();
    find_definition_dirs(pkg_dir, &mut def_dirs)?;
    def_dirs.sort();

    for def_dir in def_dirs {
        let decl_path = def_dir.join("definition.luau");
        let mut resolved_sources = BTreeMap::new();
        collect_definition_sources(&def_dir, &def_dir, &mut resolved_sources)?;
        let decl_src = source_text(&resolved_sources, "definition.luau", &decl_path)?
            .ok_or_else(|| GitError::Workspace(format!("missing {}", decl_path.display())))?;

        let manifest = evaluate_declaration(&decl_src, &resolved_sources).map_err(|e| {
            GitError::Workspace(format!("declaration '{}': {e}", decl_path.display()))
        })?;
        let ctx = |msg: String| GitError::Workspace(format!("declaration '{}': {msg}", decl_path.display()));

        // A Definition's ID is `package.definition` and must belong to its containing Package.
        let def_id = manifest.id.clone();
        if !def_id.starts_with(&format!("{pkg_name}.")) {
            return Err(ctx(format!(
                "definition id '{def_id}' must start with its package name '{pkg_name}.'"
            )));
        }
        if let Some(existing) = out.get(&def_id) {
            return Err(ctx(format!(
                "duplicate definition id '{def_id}' (already provided by package '{}')",
                existing.package_name
            )));
        }

        // The runtime module is read from the Definition's own resolved sources, so it is always
        // confined to the Definition directory and covered by its identity digest.
        let runtime_rel = if manifest.runtime.is_empty() {
            "runtime.luau".to_string()
        } else {
            manifest.runtime.clone()
        };
        let runtime_path = def_dir.join(&runtime_rel);
        let runtime_source = match source_text(&resolved_sources, &runtime_rel, &runtime_path)? {
            Some(src) => {
                dash_luau::declaration::check_runtime_source(&manifest, &src)
                    .map_err(|e| ctx(e.to_string()))?;
                src
            }
            None if manifest.category == DefinitionCategory::WidgetProvider => "return {}".into(),
            None => {
                return Err(ctx(format!(
                    "{} '{def_id}' requires runtime module '{runtime_rel}', which does not exist",
                    manifest.category.as_str()
                )))
            }
        };

        let tests_source = source_text(&resolved_sources, "tests.luau", &def_dir.join("tests.luau"))?;

        // Load JSON fixtures (under fixtures/ or declared paths). Declared fixtures must exist and
        // parse; other JSON files are offered as fixtures when they parse.
        let mut fixtures = BTreeMap::new();
        for (fixture_name, rel) in &manifest.fixtures {
            let Some(bytes) = resolved_sources.get(rel) else {
                return Err(ctx(format!(
                    "fixture '{fixture_name}' path '{rel}' does not exist in the Definition directory"
                )));
            };
            let val = serde_json::from_slice::<serde_json::Value>(bytes)
                .map_err(|e| ctx(format!("fixture '{fixture_name}' ({rel}) is not valid JSON: {e}")))?;
            fixtures.insert(rel.clone(), val);
        }
        for (rel, bytes) in &resolved_sources {
            if rel.ends_with(".json") && !fixtures.contains_key(rel) {
                if let Ok(val) = serde_json::from_slice::<serde_json::Value>(bytes) {
                    fixtures.insert(rel.clone(), val);
                }
            }
        }

        // Load custom widget assets (under assets/ or declared asset path) with size and type
        // checks (§10.1).
        if manifest.category == DefinitionCategory::WidgetProvider {
            let asset = manifest.widget_asset.as_deref().unwrap_or("");
            if !resolved_sources.contains_key(asset) {
                return Err(ctx(format!(
                    "widget_provider '{def_id}' asset '{asset}' does not exist in the Definition directory"
                )));
            }
        }
        let mut widget_assets = BTreeMap::new();
        let mut assets_total = 0usize;
        for (rel, bytes) in &resolved_sources {
            if rel.starts_with("assets/") || manifest.widget_asset.as_deref() == Some(rel.as_str()) {
                let ext = Path::new(rel)
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e.to_ascii_lowercase())
                    .unwrap_or_default();
                if !ALLOWED_WIDGET_ASSET_EXTENSIONS.contains(&ext.as_str()) {
                    return Err(ctx(format!(
                        "widget asset '{rel}' has a disallowed file type (allowed: {})",
                        ALLOWED_WIDGET_ASSET_EXTENSIONS.join(", ")
                    )));
                }
                if bytes.len() > MAX_WIDGET_ASSET_BYTES {
                    return Err(ctx(format!(
                        "widget asset '{rel}' is {} bytes, above the {MAX_WIDGET_ASSET_BYTES}-byte limit",
                        bytes.len()
                    )));
                }
                assets_total += bytes.len();
                if assets_total > MAX_WIDGET_ASSETS_TOTAL_BYTES {
                    return Err(ctx(format!(
                        "widget assets exceed the {MAX_WIDGET_ASSETS_TOTAL_BYTES}-byte per-Definition limit"
                    )));
                }
                widget_assets.insert(rel.clone(), bytes.clone());
            }
        }

        out.insert(
            def_id,
            LoadedDefinition {
                package_name: pkg_name.to_string(),
                manifest,
                definition_source: decl_src,
                runtime_source,
                tests_source,
                fixtures,
                widget_assets,
            },
        );
    }
    Ok(())
}

fn source_text(
    sources: &BTreeMap<String, Vec<u8>>,
    rel: &str,
    path_for_errors: &Path,
) -> Result<Option<String>, GitError> {
    match sources.get(rel) {
        None => Ok(None),
        Some(bytes) => String::from_utf8(bytes.clone()).map(Some).map_err(|_| {
            GitError::Workspace(format!("{} is not valid UTF-8", path_for_errors.display()))
        }),
    }
}

/// Cross-Definition checks and derived data, run once every Package is loaded:
/// 1. Mocks are bound to an existing Connector of the same Package (or a Workspace-local
///    Package), at most one per Connector, with the same operations and capabilities (§4.1,
///    §10.3). A Mock that declares none inherits them from its target.
/// 2. The semantic type registry pairs each semantic type with exactly one structural schema,
///    and shapeless references are resolved against it (§4.2).
/// 3. Each Definition's identity version is folded with the identity of every Library it uses
///    (transitively), the semantic shapes it resolved, and (for Mocks) its target's contract, so
///    any change to code or contracts a Definition depends on changes its identity (§4.1, §12.2
///    carry-over).
fn finalize_definitions(
    defs: &mut BTreeMap<String, LoadedDefinition>,
    local_packages: &BTreeSet<String>,
) -> Result<(), GitError> {
    // 1. Mocks
    let mut mock_targets: BTreeMap<String, String> = BTreeMap::new();
    let mock_ids: Vec<String> = defs
        .iter()
        .filter(|(_, d)| d.manifest.category == DefinitionCategory::Mock)
        .map(|(id, _)| id.clone())
        .collect();
    for mock_id in &mock_ids {
        let mock = &defs[mock_id];
        let target_id = mock.manifest.target_connector.clone().unwrap_or_default();
        let Some(target) = defs.get(&target_id) else {
            return Err(GitError::Workspace(format!(
                "mock '{mock_id}' targets '{target_id}', which is not a loaded Definition"
            )));
        };
        if target.manifest.category != DefinitionCategory::Connector {
            return Err(GitError::Workspace(format!(
                "mock '{mock_id}' targets '{target_id}', which is a {} rather than a connector",
                target.manifest.category.as_str()
            )));
        }
        if mock.package_name != target.package_name && !local_packages.contains(&mock.package_name) {
            return Err(GitError::Workspace(format!(
                "mock '{mock_id}' must be declared in the connector's package '{}' or in a Workspace-local package",
                target.package_name
            )));
        }
        if let Some(other) = mock_targets.insert(target_id.clone(), mock_id.clone()) {
            return Err(GitError::Workspace(format!(
                "connector '{target_id}' has more than one Mock ('{other}' and '{mock_id}')"
            )));
        }
        let target_ops = target.manifest.operations.clone();
        let target_caps = target.manifest.capabilities.clone();
        let mock = defs.get_mut(mock_id).unwrap();
        if mock.manifest.operations.is_empty() && mock.manifest.capabilities.is_empty() {
            mock.manifest.operations = target_ops;
            mock.manifest.capabilities = target_caps;
        } else {
            let mock_ops: BTreeSet<&String> = mock.manifest.operations.keys().collect();
            let tgt_ops: BTreeSet<&String> = target_ops.keys().collect();
            let mock_caps: BTreeSet<&String> = mock.manifest.capabilities.iter().collect();
            let tgt_caps: BTreeSet<&String> = target_caps.iter().collect();
            if mock_ops != tgt_ops || mock_caps != tgt_caps {
                return Err(GitError::Workspace(format!(
                    "mock '{mock_id}' must declare the same operations and capabilities as '{target_id}' (or declare none to inherit them)"
                )));
            }
        }
    }

    // 2. Semantic type registry
    let registry = dash_luau::manifest::build_semantic_registry(
        defs.iter().map(|(id, d)| (id.as_str(), &d.manifest)),
    )
    .map_err(GitError::Workspace)?;
    let mut used_semantics: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (id, def) in defs.iter_mut() {
        let mut used = BTreeSet::new();
        for schema in def.manifest.schemas_mut() {
            schema.resolve_semantic_shapes(&registry, &mut used);
        }
        if !used.is_empty() {
            def.manifest.generate_luau_types();
            used_semantics.insert(id.clone(), used);
        }
    }

    // 3. Library dependency closure and identity folding
    let libraries: BTreeSet<String> = defs
        .iter()
        .filter(|(_, d)| d.manifest.category == DefinitionCategory::Library)
        .map(|(id, _)| id.clone())
        .collect();
    let mut direct_libs: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (id, def) in defs.iter() {
        let mut deps = BTreeSet::new();
        for lib in &def.manifest.libraries {
            if !libraries.contains(lib) {
                return Err(GitError::Workspace(format!(
                    "definition '{id}' declares library '{lib}', which is not a Library Definition in this Workspace"
                )));
            }
            deps.insert(lib.clone());
        }
        let mut srcs = vec![def.runtime_source.as_str()];
        if let Some(t) = &def.tests_source {
            srcs.push(t.as_str());
        }
        for src in srcs {
            for module in dash_luau::declaration::scan_required_modules(src) {
                if libraries.contains(&module) {
                    deps.insert(module);
                }
            }
        }
        deps.remove(id);
        direct_libs.insert(id.clone(), deps);
    }
    let own_identity: BTreeMap<String, String> = defs
        .iter()
        .map(|(id, d)| (id.clone(), d.manifest.identity_version.clone()))
        .collect();
    for (id, def) in defs.iter_mut() {
        let mut closure = BTreeSet::new();
        let mut frontier: Vec<String> = direct_libs[id].iter().cloned().collect();
        while let Some(lib) = frontier.pop() {
            if lib != *id && closure.insert(lib.clone()) {
                frontier.extend(direct_libs[&lib].iter().cloned());
            }
        }
        let used = used_semantics.get(id);
        let mock_target = if def.manifest.category == DefinitionCategory::Mock {
            def.manifest.target_connector.clone()
        } else {
            None
        };
        if closure.is_empty() && used.is_none() && mock_target.is_none() {
            continue;
        }
        let mut h = Sha256::new();
        h.update(b"dash-identity-v1\0");
        h.update(own_identity[id].as_bytes());
        for lib in &closure {
            h.update(b"\0lib\0");
            h.update(lib.as_bytes());
            h.update(b"\0");
            h.update(own_identity[lib].as_bytes());
        }
        for sem in used.into_iter().flatten() {
            h.update(b"\0sem\0");
            h.update(sem.as_bytes());
            h.update(b"\0");
            h.update(serde_json::to_string(&registry[sem]).unwrap_or_default().as_bytes());
        }
        if let Some(t) = &mock_target {
            h.update(b"\0mock_target\0");
            h.update(t.as_bytes());
            h.update(b"\0");
            h.update(own_identity.get(t).map(String::as_str).unwrap_or("").as_bytes());
        }
        def.manifest.identity_version = format!("sha256:{}", hex::encode(h.finalize()));
    }
    Ok(())
}

fn find_definition_dirs(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), GitError> {
    if dir.join("definition.luau").exists() {
        out.push(dir.to_path_buf());
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        // `file_type()` does not follow symbolic links: a linked directory is never traversed.
        let ft = entry.file_type()?;
        if ft.is_symlink() {
            return Err(GitError::Workspace(format!(
                "symbolic links are not allowed in packages: {}",
                p.display()
            )));
        }
        if ft.is_dir() && entry.file_name() != ".git" {
            find_definition_dirs(&p, out)?;
        }
    }
    Ok(())
}

fn collect_definition_sources(
    base: &Path,
    cur: &Path,
    out: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), GitError> {
    for entry in std::fs::read_dir(cur)? {
        let entry = entry?;
        let p = entry.path();
        if entry.file_name() == ".git" {
            continue;
        }
        let ft = entry.file_type()?;
        if ft.is_symlink() {
            return Err(GitError::Workspace(format!(
                "symbolic links are not allowed in packages: {}",
                p.display()
            )));
        }
        if ft.is_dir() {
            collect_definition_sources(base, &p, out)?;
        } else if ft.is_file() {
            let rel = p
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            out.insert(rel, std::fs::read(&p)?);
        }
    }
    Ok(())
}

// ---- Canonical writers for Draft mutations ----

pub fn upsert_instance_in_workspace(
    workspace_dir: &Path,
    inst: &InstanceConfig,
) -> Result<(), GitError> {
    let inst_dir = workspace_dir.join("instances");
    std::fs::create_dir_all(&inst_dir)?;
    let entry = InstanceEntryYaml::from(inst);
    match inst.category {
        DefinitionCategory::Connector => {
            let p = inst_dir.join("connectors.yaml");
            let mut f: ConnectorsFile = if p.exists() {
                serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
                    .map_err(|e| GitError::Workspace(e.to_string()))?
            } else {
                Default::default()
            };
            f.connectors.insert(inst.id.clone(), entry);
            std::fs::write(p, serde_yaml_ng::to_string(&f).unwrap())?;
        }
        DefinitionCategory::DataProvider => {
            let p = inst_dir.join("data.yaml");
            let mut f: DataFile = if p.exists() {
                serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
                    .map_err(|e| GitError::Workspace(e.to_string()))?
            } else {
                Default::default()
            };
            f.data.insert(inst.id.clone(), entry);
            std::fs::write(p, serde_yaml_ng::to_string(&f).unwrap())?;
        }
        DefinitionCategory::ActionProvider => {
            let p = inst_dir.join("actions.yaml");
            let mut f: ActionsFile = if p.exists() {
                serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
                    .map_err(|e| GitError::Workspace(e.to_string()))?
            } else {
                Default::default()
            };
            f.actions.insert(inst.id.clone(), entry);
            std::fs::write(p, serde_yaml_ng::to_string(&f).unwrap())?;
        }
        DefinitionCategory::Adapter => {
            let p = inst_dir.join("adapters.yaml");
            let mut f: AdaptersFile = if p.exists() {
                serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
                    .map_err(|e| GitError::Workspace(e.to_string()))?
            } else {
                Default::default()
            };
            f.adapters.insert(inst.id.clone(), entry);
            std::fs::write(p, serde_yaml_ng::to_string(&f).unwrap())?;
        }
        DefinitionCategory::Automation => {
            let p = inst_dir.join("automations.yaml");
            let mut f: AutomationsFile = if p.exists() {
                serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
                    .map_err(|e| GitError::Workspace(e.to_string()))?
            } else {
                Default::default()
            };
            f.automations.insert(inst.id.clone(), entry);
            std::fs::write(p, serde_yaml_ng::to_string(&f).unwrap())?;
        }
        other => {
            return Err(GitError::Workspace(format!(
                "category '{}' cannot be instantiated as an Instance",
                other.as_str()
            )))
        }
    }
    Ok(())
}

pub fn delete_instance_in_workspace(workspace_dir: &Path, instance_id: &str) -> Result<bool, GitError> {
    let inst_dir = workspace_dir.join("instances");
    let mut removed = false;
    let p = inst_dir.join("connectors.yaml");
    if p.exists() {
        let mut f: ConnectorsFile = serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        if f.connectors.remove(instance_id).is_some() {
            std::fs::write(&p, serde_yaml_ng::to_string(&f).unwrap())?;
            removed = true;
        }
    }
    let p = inst_dir.join("data.yaml");
    if p.exists() {
        let mut f: DataFile = serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        if f.data.remove(instance_id).is_some() {
            std::fs::write(&p, serde_yaml_ng::to_string(&f).unwrap())?;
            removed = true;
        }
    }
    let p = inst_dir.join("actions.yaml");
    if p.exists() {
        let mut f: ActionsFile = serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        if f.actions.remove(instance_id).is_some() {
            std::fs::write(&p, serde_yaml_ng::to_string(&f).unwrap())?;
            removed = true;
        }
    }
    let p = inst_dir.join("adapters.yaml");
    if p.exists() {
        let mut f: AdaptersFile = serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        if f.adapters.remove(instance_id).is_some() {
            std::fs::write(&p, serde_yaml_ng::to_string(&f).unwrap())?;
            removed = true;
        }
    }
    let p = inst_dir.join("automations.yaml");
    if p.exists() {
        let mut f: AutomationsFile = serde_yaml_ng::from_str(&std::fs::read_to_string(&p)?)
            .map_err(|e| GitError::Workspace(e.to_string()))?;
        if f.automations.remove(instance_id).is_some() {
            std::fs::write(&p, serde_yaml_ng::to_string(&f).unwrap())?;
            removed = true;
        }
    }
    Ok(removed)
}

/// Create `dir` (and parents) inside `root`, refusing symbolic links on the way and verifying
/// that the canonical result stays inside the canonical `root` (defence in depth on top of
/// identifier validation).
fn create_dir_within(root: &Path, rel_components: &[&str]) -> Result<PathBuf, GitError> {
    let mut cur = root.to_path_buf();
    for comp in rel_components {
        cur = cur.join(comp);
        reject_symlink(&cur)?;
    }
    std::fs::create_dir_all(&cur)?;
    let canon_root = std::fs::canonicalize(root)?;
    let canon_dir = std::fs::canonicalize(&cur)?;
    if !canon_dir.starts_with(&canon_root) {
        return Err(GitError::Workspace(format!(
            "refusing to write outside the workspace: {}",
            cur.display()
        )));
    }
    Ok(cur)
}

/// Write a file, refusing to follow a pre-existing symbolic link at the target path.
fn write_file_no_follow(path: &Path, contents: &[u8]) -> Result<(), GitError> {
    reject_symlink(path)?;
    std::fs::write(path, contents)?;
    Ok(())
}

pub fn write_dashboard_in_workspace(
    workspace_dir: &Path,
    doc: &DashboardDoc,
) -> Result<(), GitError> {
    validate_dashboard_id(&doc.id)?;
    let dir = create_dir_within(workspace_dir, &["dashboards"])?;
    let path = dir.join(format!("{}.yaml", doc.id));
    let yaml = serde_yaml_ng::to_string(doc).map_err(|e| GitError::Workspace(e.to_string()))?;
    write_file_no_follow(&path, yaml.as_bytes())?;
    Ok(())
}

#[derive(Serialize)]
struct NewPackageToml<'a> {
    name: &'a str,
    version: &'a str,
}

/// Write a Definition's sources to `packages/<package>/<def_name>/` inside a Workspace (normally
/// a Draft worktree). `package` and `def_name` must be identifiers, and when the declaration
/// evaluates, its `id` must be exactly `<package>.<def_name>`; nothing is written otherwise.
pub fn write_definition_in_workspace(
    workspace_dir: &Path,
    package: &str,
    def_name: &str,
    definition_luau: &str,
    runtime_luau: Option<&str>,
    tests_luau: Option<&str>,
) -> Result<(), GitError> {
    validate_identifier("package name", package)?;
    validate_identifier("definition name", def_name)?;
    if package == RESERVED_PACKAGE_NAME {
        return Err(GitError::Workspace(format!(
            "package name '{RESERVED_PACKAGE_NAME}' is reserved for built-in Definitions"
        )));
    }
    let expected_id = format!("{package}.{def_name}");
    // A declaration that does not evaluate yet is still written (Drafts may hold work in
    // progress; validation reports it), but a declared id must match its location.
    if let Ok(m) = evaluate_declaration(definition_luau, &BTreeMap::new()) {
        if m.id != expected_id {
            return Err(GitError::Workspace(format!(
                "definition id '{}' does not match its location; expected '{expected_id}'",
                m.id
            )));
        }
    }

    let pkg_dir = create_dir_within(workspace_dir, &["packages", package])?;
    let pkg_toml = pkg_dir.join("package.toml");
    if !pkg_toml.exists() {
        reject_symlink(&pkg_toml)?;
        let raw = toml::to_string(&NewPackageToml {
            name: package,
            version: "0.1.0",
        })
        .map_err(|e| GitError::Workspace(format!("serialize package.toml: {e}")))?;
        write_file_no_follow(&pkg_toml, raw.as_bytes())?;
        // Keep an explicit `[local] packages` list in packages.toml in sync with new Packages.
        let mut cfg = read_packages_toml(workspace_dir)?;
        if !cfg.local.packages.is_empty() && !cfg.local.packages.iter().any(|p| p == package) {
            cfg.local.packages.push(package.to_string());
            write_packages_toml(workspace_dir, &cfg)?;
        }
    }
    let def_dir = create_dir_within(workspace_dir, &["packages", package, def_name])?;
    write_file_no_follow(&def_dir.join("definition.luau"), definition_luau.as_bytes())?;
    if let Some(rt) = runtime_luau {
        write_file_no_follow(&def_dir.join("runtime.luau"), rt.as_bytes())?;
    }
    if let Some(ts) = tests_luau {
        write_file_no_follow(&def_dir.join("tests.luau"), ts.as_bytes())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fixture_ws(tmp: &TempDir) -> (PathBuf, PathBuf) {
        let src = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/engineering-command-center");
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        crate::packages::copy_tree_excluding_git(&src, &ws).unwrap();
        (ws, data)
    }

    fn load(ws: &Path, data: &Path) -> Result<ConfiguredGraph, GitError> {
        load_workspace_graph(ws, data, "test", 1, true)
    }

    fn write_def(ws: &Path, pkg: &str, name: &str, decl: &str, runtime: Option<&str>) {
        let dir = ws.join("packages").join(pkg).join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("definition.luau"), decl).unwrap();
        if let Some(rt) = runtime {
            std::fs::write(dir.join("runtime.luau"), rt).unwrap();
        }
    }

    const QUERY_RT: &str = "local R = {}; function R.query(ctx) return {} end; return R";

    #[test]
    fn library_edits_change_dependent_identity_only() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        let g1 = load(&ws, &data).unwrap();
        let lib_rt = ws.join("packages/engineering/pr_utils/runtime.luau");
        let mut src = std::fs::read_to_string(&lib_rt).unwrap();
        src.push_str("\n-- library edit\n");
        std::fs::write(&lib_rt, src).unwrap();
        let g2 = load(&ws, &data).unwrap();

        let id = |g: &ConfiguredGraph, d: &str| g.definitions[d].manifest.identity_version.clone();
        assert_ne!(id(&g1, "engineering.pr_utils"), id(&g2, "engineering.pr_utils"));
        assert_ne!(
            id(&g1, "engineering.linked_prs"),
            id(&g2, "engineering.linked_prs"),
            "a dependent's identity must change when a Library it requires changes"
        );
        assert_eq!(id(&g1, "github.pull_requests"), id(&g2, "github.pull_requests"));
        assert_ne!(g1.graph_digest(), g2.graph_digest());
    }

    #[test]
    fn require_scan_folds_undeclared_library_use() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        write_def(
            &ws,
            "engineering",
            "uses_lib",
            r#"local dash = require("dash"); return dash.data_provider { id = "engineering.uses_lib" }"#,
            Some(r#"local u = require("engineering.pr_utils"); local R = {}; function R.query(ctx) return {} end; return R"#),
        );
        let g1 = load(&ws, &data).unwrap();
        let lib_rt = ws.join("packages/engineering/pr_utils/runtime.luau");
        std::fs::write(&lib_rt, "local M = {}; return M").unwrap();
        let g2 = load(&ws, &data).unwrap();
        assert_ne!(
            g1.definitions["engineering.uses_lib"].manifest.identity_version,
            g2.definitions["engineering.uses_lib"].manifest.identity_version
        );
    }

    #[test]
    fn write_definition_rejects_path_traversal_and_mismatched_ids() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path().join("wt");
        std::fs::create_dir_all(&ws).unwrap();
        let decl = r#"local dash = require("dash"); return dash.data_provider { id = "eng.metric" }"#;
        for (pkg, name) in [
            ("../escape", "metric"),
            ("/abs", "metric"),
            ("eng", "../../x"),
            ("eng", "a/b"),
            ("eng", ".."),
            ("Eng", "metric"),
            ("", "metric"),
            ("dash", "metric"),
        ] {
            assert!(
                write_definition_in_workspace(&ws, pkg, name, decl, Some(QUERY_RT), None).is_err(),
                "({pkg}, {name}) must be rejected"
            );
        }
        assert!(!tmp.path().join("escape").exists());
        assert!(!Path::new("/abs/metric").exists());

        // Declared id must equal package.def_name; nothing is written otherwise.
        let err = write_definition_in_workspace(&ws, "eng", "other", decl, Some(QUERY_RT), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not match"), "{err}");
        assert!(!ws.join("packages/eng/other").exists());

        write_definition_in_workspace(&ws, "eng", "metric", decl, Some(QUERY_RT), None).unwrap();
        let toml_raw = std::fs::read_to_string(ws.join("packages/eng/package.toml")).unwrap();
        let parsed: PackageManifestToml = toml::from_str(&toml_raw).unwrap();
        assert_eq!(parsed.name, "eng");
    }

    #[cfg(unix)]
    #[test]
    fn write_definition_refuses_symlinked_package_dir() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path().join("wt");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(ws.join("packages")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, ws.join("packages/eng")).unwrap();
        let decl = r#"local dash = require("dash"); return dash.data_provider { id = "eng.metric" }"#;
        assert!(write_definition_in_workspace(&ws, "eng", "metric", decl, Some(QUERY_RT), None).is_err());
        assert!(std::fs::read_dir(&outside).unwrap().next().is_none());
    }

    #[test]
    fn write_dashboard_rejects_path_traversal() {
        let tmp = TempDir::new().unwrap();
        let ws = tmp.path().join("wt");
        std::fs::create_dir_all(&ws).unwrap();
        let mut doc = DashboardDoc {
            id: "../../evil".into(),
            title: "x".into(),
            layout: "grid".into(),
            adapters: vec![],
            initial_state: BTreeMap::new(),
            overrides: BTreeMap::new(),
            state_schema: BTreeMap::new(),
        };
        assert!(write_dashboard_in_workspace(&ws, &doc).is_err());
        doc.id = "/tmp/evil".into();
        assert!(write_dashboard_in_workspace(&ws, &doc).is_err());
        doc.id = "ops-2".into();
        write_dashboard_in_workspace(&ws, &doc).unwrap();
        assert!(ws.join("dashboards/ops-2.yaml").exists());
    }

    #[test]
    fn new_package_is_added_to_explicit_local_list() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        write_definition_in_workspace(
            &ws,
            "metrics",
            "count",
            r#"local dash = require("dash"); return dash.data_provider { id = "metrics.count" }"#,
            Some(QUERY_RT),
            None,
        )
        .unwrap();
        let cfg = read_packages_toml(&ws).unwrap();
        assert!(cfg.local.packages.contains(&"metrics".to_string()));
        let g = load(&ws, &data).unwrap();
        assert!(g.definitions.contains_key("metrics.count"));
    }

    #[test]
    fn local_package_list_is_honoured() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        // An unlisted directory is not loaded.
        write_def(
            &ws,
            "scratch",
            "thing",
            r#"local dash = require("dash"); return dash.data_provider { id = "scratch.thing" }"#,
            Some(QUERY_RT),
        );
        let g = load(&ws, &data).unwrap();
        assert!(!g.definitions.contains_key("scratch.thing"));
        // A listed but missing package is an error.
        std::fs::write(
            ws.join("packages.toml"),
            "[local]\npackages = [\"github\", \"jira\", \"engineering\", \"missing\"]\n",
        )
        .unwrap();
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("missing"), "{err}");
    }

    #[test]
    fn definition_ids_are_bound_to_their_package_and_unique() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        write_def(
            &ws,
            "engineering",
            "hijack",
            r#"local dash = require("dash"); return dash.data_provider { id = "github.pull_requests" }"#,
            Some(QUERY_RT),
        );
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("must start with its package name"), "{err}");

        std::fs::remove_dir_all(ws.join("packages/engineering/hijack")).unwrap();
        write_def(
            &ws,
            "engineering",
            "dup",
            r#"local dash = require("dash"); return dash.data_provider { id = "engineering.linked_prs" }"#,
            Some(QUERY_RT),
        );
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("duplicate definition id"), "{err}");

        // The `dash` package name is reserved for built-ins.
        std::fs::remove_dir_all(ws.join("packages/engineering/dup")).unwrap();
        std::fs::write(ws.join("packages.toml"), "").unwrap();
        write_def(
            &ws,
            "dash",
            "table",
            r#"local dash = require("dash"); return dash.data_provider { id = "dash.table" }"#,
            Some(QUERY_RT),
        );
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("reserved"), "{err}");
    }

    #[test]
    fn package_toml_name_must_match_directory() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        std::fs::write(
            ws.join("packages/jira/package.toml"),
            "name = \"github\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("declares name 'github'"), "{err}");
    }

    #[test]
    fn runtime_must_exist_be_confined_and_define_handlers() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        write_def(
            &ws,
            "engineering",
            "no_rt",
            r#"local dash = require("dash"); return dash.data_provider { id = "engineering.no_rt" }"#,
            None,
        );
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("requires runtime module"), "{err}");

        std::fs::write(
            ws.join("packages/engineering/no_rt/runtime.luau"),
            "local R = {}; function R.other(ctx) return {} end; return R",
        )
        .unwrap();
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("not defined in its runtime module"), "{err}");

        std::fs::write(
            ws.join("packages/engineering/no_rt/definition.luau"),
            r#"local dash = require("dash"); return dash.data_provider { id = "engineering.no_rt", runtime = "/etc/passwd" }"#,
        )
        .unwrap();
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("runtime path"), "{err}");
    }

    #[test]
    fn mocks_inherit_contract_and_are_unique_per_connector() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        let g = load(&ws, &data).unwrap();
        let mock = &g.definitions["github.mock"].manifest;
        let conn = &g.definitions["github.connector"].manifest;
        assert_eq!(
            mock.operations.keys().collect::<Vec<_>>(),
            conn.operations.keys().collect::<Vec<_>>()
        );
        assert_eq!(mock.capabilities, conn.capabilities);

        write_def(
            &ws,
            "github",
            "mock2",
            r#"local dash = require("dash"); return dash.mock { id = "github.mock2", ["for"] = "github.connector" }"#,
            Some("local R = {}; function R.call(ctx) return {} end; return R"),
        );
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("more than one Mock"), "{err}");

        std::fs::write(
            ws.join("packages/github/mock2/definition.luau"),
            r#"local dash = require("dash"); return dash.mock { id = "github.mock2", ["for"] = "github.pull_requests" }"#,
        )
        .unwrap();
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("rather than a connector"), "{err}");

        std::fs::write(
            ws.join("packages/github/mock2/definition.luau"),
            r#"local dash = require("dash"); return dash.mock { id = "github.mock2", ["for"] = "jira.connector", capabilities = { "issues.read" } }"#,
        )
        .unwrap();
        std::fs::remove_dir_all(ws.join("packages/jira/mock")).unwrap();
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("same operations and capabilities"), "{err}");
    }

    #[test]
    fn semantic_references_resolve_to_one_structural_schema() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        let g = load(&ws, &data).unwrap();
        // github.connector's `get_pr` result is declared shapeless; the registry pairs it with the
        // shape declared by github.pull_requests.
        match &g.definitions["github.connector"].manifest.operations["get_pr"].result {
            dash_luau::SchemaSpec::Semantic { semantic_type, shape } => {
                assert_eq!(semantic_type, "dev.pull_request@1");
                assert!(shape.is_some());
            }
            other => panic!("unexpected schema {other:?}"),
        }

        // A conflicting shape for the same semantic type fails loading.
        write_def(
            &ws,
            "engineering",
            "conflict",
            r#"local dash = require("dash"); return dash.data_provider {
                id = "engineering.conflict",
                output = dash.semantic("dev.pull_request@1", dash.object { id = dash.integer() }),
            }"#,
            Some(QUERY_RT),
        );
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("conflicting structural schemas"), "{err}");

        // A semantic type that is never given a shape fails loading.
        std::fs::write(
            ws.join("packages/engineering/conflict/definition.luau"),
            r#"local dash = require("dash"); return dash.data_provider { id = "engineering.conflict", output = dash.list("dev.unknown@1") }"#,
        )
        .unwrap();
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("has no structural schema"), "{err}");
    }

    #[test]
    fn widget_assets_are_type_and_size_checked() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        let g = load(&ws, &data).unwrap();
        let widget = &g.definitions["engineering.pr_badge_widget"];
        assert!(widget
            .widget_asset_sha256("assets/index.html")
            .unwrap()
            .starts_with("sha256:"));

        let assets = ws.join("packages/engineering/pr_badge_widget/assets");
        std::fs::write(assets.join("payload.exe"), b"MZ").unwrap();
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("disallowed file type"), "{err}");
        std::fs::remove_file(assets.join("payload.exe")).unwrap();

        std::fs::write(assets.join("big.js"), vec![b'a'; MAX_WIDGET_ASSET_BYTES + 1]).unwrap();
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("limit"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_in_packages_are_rejected() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        let secret = tmp.path().join("vault.key");
        std::fs::write(&secret, b"top-secret").unwrap();
        std::os::unix::fs::symlink(
            &secret,
            ws.join("packages/engineering/pr_badge_widget/assets/k.txt"),
        )
        .unwrap();
        let err = load(&ws, &data).unwrap_err().to_string();
        assert!(err.contains("symbolic links"), "{err}");
    }

    #[test]
    fn wiring_flags_unbound_and_undeclared_inputs() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        let mut g = load(&ws, &data).unwrap();
        let store = dash_store::SqliteStore::open_in_memory().unwrap();
        let linked = g.instances.get_mut("linked_prs").unwrap();
        linked.inputs.remove("issues");
        linked.inputs.insert("bogus".into(), "open_prs".into());
        let report = dash_core::validate_configured_graph(&g, &store, true);
        assert!(report.issues.iter().any(|i| i.code == "UNBOUND_INPUT"), "{:?}", report.issues);
        assert!(report.issues.iter().any(|i| i.code == "UNDECLARED_INPUT"), "{:?}", report.issues);
    }

    #[tokio::test]
    async fn async_loader_matches_sync_loader() {
        let tmp = TempDir::new().unwrap();
        let (ws, data) = fixture_ws(&tmp);
        let a = load(&ws, &data).unwrap();
        let b = load_workspace_graph_async(&ws, &data, "test", 1, true).await.unwrap();
        assert_eq!(a.graph_digest(), b.graph_digest());
    }
}

#[cfg(test)]
mod library_activation_tests {
    use tempfile::TempDir;

    #[tokio::test]
    async fn library_only_draft_rebuilds_dependent_instances() {
        let tmp = TempDir::new().unwrap();
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/engineering-command-center");
        let ws = tmp.path().join("ws");
        let data = tmp.path().join("data");
        crate::packages::copy_tree_excluding_git(&src, &ws).unwrap();
        let store = dash_store::SqliteStore::open(&data).unwrap();
        let mgr = crate::DraftManager::initialize_with_mocks(
            ws.clone(),
            data,
            store,
            dash_luau::ResourceBudget::default(),
            true,
        )
        .await
        .unwrap();
        let admin = dash_core::Principal::administrator();
        let draft = mgr.draft_begin(&admin).await.unwrap();
        let lib_dir = ws.join("packages/engineering/pr_utils");
        let decl = std::fs::read_to_string(lib_dir.join("definition.luau")).unwrap();
        let mut rt = std::fs::read_to_string(lib_dir.join("runtime.luau")).unwrap();
        rt.push_str("\n-- library-only change\n");
        mgr.draft_write_definition(&draft.draft_id, "engineering", "pr_utils", &decl, Some(&rt), None)
            .unwrap();
        let act = mgr.draft_activate(&draft.draft_id, &admin).await.unwrap();
        assert!(
            act.rebuilt_instances.contains(&"linked_prs".to_string()),
            "rebuilt: {:?}, carried: {:?}",
            act.rebuilt_instances,
            act.carried_over_instances
        );
        assert!(!act.carried_over_instances.contains(&"linked_prs".to_string()));
        assert!(act.carried_over_instances.contains(&"open_prs".to_string()));
    }
}
