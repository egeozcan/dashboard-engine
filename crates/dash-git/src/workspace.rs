//! Canonical Workspace loader & serializer ensuring Git/UI/CLI/MCP round-trip without drift (§20).

use crate::git_cli::GitError;
use crate::packages::{cached_package_roots, read_packages_lock, PackageManifestToml};
use dash_core::{
    ConfiguredGraph, DashboardDoc, InstanceConfig, LoadedDefinition, ServicePrincipalRequested,
};
use dash_luau::{evaluate_declaration, DefinitionCategory};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
    for (pkg_name, root) in cached_external {
        load_package_dir(&pkg_name, &root, &mut graph.definitions)?;
    }

    let local_pkgs_dir = workspace_dir.join("packages");
    if local_pkgs_dir.exists() {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(&local_pkgs_dir)?
            .filter_map(|e| e.ok().map(|x| x.path()))
            .filter(|p| p.is_dir())
            .collect();
        entries.sort();
        for pkg_dir in entries {
            let pkg_toml_path = pkg_dir.join("package.toml");
            let pkg_name = if pkg_toml_path.exists() {
                let raw = std::fs::read_to_string(&pkg_toml_path)?;
                let parsed: PackageManifestToml = toml::from_str(&raw).map_err(|e| {
                    GitError::Workspace(format!("invalid {}: {e}", pkg_toml_path.display()))
                })?;
                parsed.name
            } else {
                pkg_dir
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("local")
                    .to_string()
            };
            load_package_dir(&pkg_name, &pkg_dir, &mut graph.definitions)?;
        }
    }

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
        let decl_src = std::fs::read_to_string(&decl_path)?;
        let mut resolved_sources = BTreeMap::new();
        collect_definition_sources(&def_dir, &def_dir, &mut resolved_sources)?;

        let manifest = evaluate_declaration(&decl_src, &resolved_sources).map_err(|e| {
            GitError::Workspace(format!("declaration '{}': {e}", decl_path.display()))
        })?;

        let runtime_path = if manifest.runtime.is_empty() {
            def_dir.join("runtime.luau")
        } else {
            def_dir.join(&manifest.runtime)
        };
        let runtime_source = if runtime_path.exists() {
            std::fs::read_to_string(&runtime_path)?
        } else {
            "return {}".into()
        };

        let tests_path = def_dir.join("tests.luau");
        let tests_source = if tests_path.exists() {
            Some(std::fs::read_to_string(&tests_path)?)
        } else {
            None
        };

        // Load JSON fixtures (under fixtures/ or declared paths)
        let mut fixtures = BTreeMap::new();
        for (rel, bytes) in &resolved_sources {
            if rel.ends_with(".json") {
                if let Ok(val) = serde_json::from_slice::<serde_json::Value>(bytes) {
                    fixtures.insert(rel.clone(), val);
                }
            }
        }

        // Load custom widget assets (under assets/ or declared asset path)
        let mut widget_assets = BTreeMap::new();
        for (rel, bytes) in &resolved_sources {
            if rel.starts_with("assets/") || manifest.widget_asset.as_deref() == Some(rel.as_str()) {
                widget_assets.insert(rel.clone(), bytes.clone());
            }
        }

        let def_id = manifest.id.clone();
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

fn find_definition_dirs(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), GitError> {
    if dir.join("definition.luau").exists() {
        out.push(dir.to_path_buf());
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.is_dir() && entry.file_name() != ".git" {
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
        if p.is_dir() {
            collect_definition_sources(base, &p, out)?;
        } else if p.is_file() {
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

pub fn write_dashboard_in_workspace(
    workspace_dir: &Path,
    doc: &DashboardDoc,
) -> Result<(), GitError> {
    let dir = workspace_dir.join("dashboards");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{}.yaml", doc.id));
    let yaml = serde_yaml_ng::to_string(doc).map_err(|e| GitError::Workspace(e.to_string()))?;
    std::fs::write(path, yaml)?;
    Ok(())
}

pub fn write_definition_in_workspace(
    workspace_dir: &Path,
    package: &str,
    def_name: &str,
    definition_luau: &str,
    runtime_luau: Option<&str>,
    tests_luau: Option<&str>,
) -> Result<(), GitError> {
    let pkg_dir = workspace_dir.join("packages").join(package);
    std::fs::create_dir_all(&pkg_dir)?;
    let pkg_toml = pkg_dir.join("package.toml");
    if !pkg_toml.exists() {
        std::fs::write(
            &pkg_toml,
            format!("name = \"{package}\"\nversion = \"0.1.0\"\n"),
        )?;
    }
    let def_dir = pkg_dir.join(def_name);
    std::fs::create_dir_all(&def_dir)?;
    std::fs::write(def_dir.join("definition.luau"), definition_luau)?;
    if let Some(rt) = runtime_luau {
        std::fs::write(def_dir.join("runtime.luau"), rt)?;
    }
    if let Some(ts) = tests_luau {
        std::fs::write(def_dir.join("tests.luau"), ts)?;
    }
    Ok(())
}
