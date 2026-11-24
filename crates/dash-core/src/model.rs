//! Core domain models adhering to `GLOSSARY.md`.

use dash_luau::{DefinitionCategory, DefinitionManifest};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkspaceConfig {
    #[serde(default = "default_ws_name")]
    pub name: String,
    #[serde(default)]
    pub git_remote: Option<String>,
    #[serde(default = "default_branch")]
    pub git_branch: String,
    #[serde(default)]
    pub network: NetworkPolicy,
    #[serde(default)]
    pub history: Vec<HistorySeriesConfig>,
}

fn default_ws_name() -> String {
    "dash-workspace".into()
}

fn default_branch() -> String {
    "main".into()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NetworkPolicy {
    /// Host allowlist for host-mediated HTTP requests (empty = deny all external unless matched).
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    #[serde(default)]
    pub allowed_methods: Vec<String>,
    /// Explicit opt-in ONLY for local CI/test mock HTTP servers on loopback.
    #[serde(default)]
    pub allow_loopback_for_tests: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistorySeriesConfig {
    pub series_id: String,
    pub instance_id: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default = "default_agg")]
    pub aggregator: String, // count | sum | avg | min | max | latest
    #[serde(default)]
    pub value_path: Option<String>,
    #[serde(default = "default_cadence")]
    pub cadence_secs: i64,
    #[serde(default = "default_retention")]
    pub retention_days: i64,
    pub service_principal: String,
}

fn default_agg() -> String {
    "count".into()
}
fn default_cadence() -> i64 {
    3600
}
fn default_retention() -> i64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstanceConfig {
    pub id: String,
    pub category: DefinitionCategory,
    pub definition: String,
    #[serde(default)]
    pub inputs: BTreeMap<String, String>,
    #[serde(default)]
    pub config: serde_json::Value,
    /// Mapping of credential slot -> secret name (or `secret://<name>`).
    #[serde(default)]
    pub secrets: BTreeMap<String, String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub run_as: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

impl InstanceConfig {
    pub fn config_digest(&self) -> String {
        let canonical = serde_json::to_string(self).unwrap_or_default();
        let mut h = Sha256::new();
        h.update(canonical.as_bytes());
        format!("cfg:{}", hex::encode(h.finalize()))
    }

    pub fn resolved_secret_name(&self, slot: &str) -> Option<String> {
        self.secrets
            .get(slot)
            .map(|s| s.strip_prefix("secret://").unwrap_or(s).to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CapabilityCeiling {
    #[serde(default)]
    pub unrestricted: bool,
    #[serde(default)]
    pub allowed_connectors: BTreeSet<String>,
    #[serde(default)]
    pub allowed_capabilities: BTreeSet<String>,
    #[serde(default)]
    pub allowed_actions: BTreeSet<String>,
    #[serde(default)]
    pub allowed_providers: BTreeSet<String>,
    #[serde(default)]
    pub allowed_secrets: BTreeSet<String>,
    #[serde(default)]
    pub can_author_drafts: bool,
    #[serde(default)]
    pub can_activate_drafts: bool,
    #[serde(default)]
    pub can_resolve_executions: bool,
    #[serde(default)]
    pub can_git_sync: bool,
}

impl CapabilityCeiling {
    pub fn administrator() -> Self {
        Self {
            unrestricted: true,
            allowed_connectors: BTreeSet::from(["*".into()]),
            allowed_capabilities: BTreeSet::from(["*".into()]),
            allowed_actions: BTreeSet::from(["*".into()]),
            allowed_providers: BTreeSet::from(["*".into()]),
            allowed_secrets: BTreeSet::from(["*".into()]),
            can_author_drafts: true,
            can_activate_drafts: true,
            can_resolve_executions: true,
            can_git_sync: true,
        }
    }

    pub fn read_only_agent() -> Self {
        Self {
            unrestricted: false,
            allowed_connectors: BTreeSet::new(),
            allowed_capabilities: BTreeSet::new(),
            allowed_actions: BTreeSet::new(),
            allowed_providers: BTreeSet::from(["*".into()]),
            allowed_secrets: BTreeSet::new(),
            can_author_drafts: false,
            can_activate_drafts: false,
            can_resolve_executions: false,
            can_git_sync: false,
        }
    }

    pub fn allows_action(&self, action_id: &str) -> bool {
        self.unrestricted
            || self.allowed_actions.contains("*")
            || self.allowed_actions.contains(action_id)
    }

    pub fn allows_provider(&self, provider_id: &str) -> bool {
        self.unrestricted
            || self.allowed_providers.contains("*")
            || self.allowed_providers.contains(provider_id)
    }

    pub fn allows_connector(&self, connector_id: &str) -> bool {
        self.unrestricted
            || self.allowed_connectors.contains("*")
            || self.allowed_connectors.contains(connector_id)
    }

    pub fn allows_capability(&self, cap: &str) -> bool {
        self.unrestricted
            || self.allowed_capabilities.contains("*")
            || self.allowed_capabilities.contains(cap)
    }

    pub fn allows_secret(&self, secret_name: &str) -> bool {
        self.unrestricted
            || self.allowed_secrets.contains("*")
            || self.allowed_secrets.contains(secret_name)
    }

    pub fn fingerprint(&self) -> String {
        let raw = serde_json::to_string(self).unwrap_or_default();
        let mut h = Sha256::new();
        h.update(raw.as_bytes());
        hex::encode(&h.finalize()[..16])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicePrincipalRequested {
    pub id: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub ceiling: CapabilityCeiling,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardAdapterPlacement {
    pub adapter_instance: String,
    #[serde(default)]
    pub area: String,
    #[serde(default)]
    pub query_params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayoutOverride {
    #[serde(default)]
    pub x: Option<i32>,
    #[serde(default)]
    pub y: Option<i32>,
    #[serde(default)]
    pub w: Option<i32>,
    #[serde(default)]
    pub h: Option<i32>,
    #[serde(default)]
    pub visible: Option<bool>,
    #[serde(default)]
    pub props: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardDoc {
    pub id: String,
    pub title: String,
    #[serde(default = "default_layout")]
    pub layout: String,
    #[serde(default)]
    pub adapters: Vec<DashboardAdapterPlacement>,
    #[serde(default)]
    pub initial_state: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub overrides: BTreeMap<String, LayoutOverride>,
}

fn default_layout() -> String {
    "grid".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedDefinition {
    pub package_name: String,
    pub manifest: DefinitionManifest,
    pub definition_source: String,
    pub runtime_source: String,
    pub tests_source: Option<String>,
    /// Fixture files inside the definition directory (relative path -> JSON value).
    pub fixtures: BTreeMap<String, serde_json::Value>,
    /// Custom widget static assets (relative path -> bytes).
    #[serde(skip)]
    pub widget_assets: BTreeMap<String, Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
pub struct ConfiguredGraph {
    pub revision_commit: String,
    pub generation_id: i64,
    pub workspace: WorkspaceConfig,
    pub definitions: BTreeMap<String, LoadedDefinition>,
    pub instances: BTreeMap<String, InstanceConfig>,
    pub dashboards: BTreeMap<String, DashboardDoc>,
    pub service_principals: BTreeMap<String, ServicePrincipalRequested>,
    pub is_preview: bool,
}

impl ConfiguredGraph {
    pub fn graph_digest(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.revision_commit.as_bytes());
        for (id, def) in &self.definitions {
            h.update(id.as_bytes());
            h.update(def.manifest.identity_version.as_bytes());
        }
        for (id, inst) in &self.instances {
            h.update(id.as_bytes());
            h.update(inst.config_digest().as_bytes());
        }
        for (id, dash) in &self.dashboards {
            h.update(id.as_bytes());
            h.update(serde_json::to_string(dash).unwrap_or_default().as_bytes());
        }
        format!("graph:{}", hex::encode(h.finalize()))
    }

    /// Find the Mock definition targeting `connector_def_id`, if any.
    pub fn find_mock_for(&self, connector_def_id: &str) -> Option<&LoadedDefinition> {
        self.definitions.values().find(|d| {
            d.manifest.category == DefinitionCategory::Mock
                && d.manifest.target_connector.as_deref() == Some(connector_def_id)
        })
    }

    /// Resolve all library sources available to a definition within the workspace.
    pub fn resolved_libraries(&self) -> BTreeMap<String, String> {
        self.definitions
            .values()
            .filter(|d| d.manifest.category == DefinitionCategory::Library)
            .map(|d| (d.manifest.id.clone(), d.runtime_source.clone()))
            .collect()
    }

    /// Compute the downstream dependents (reverse edges) of an instance.
    pub fn downstream_dependents(&self, changed: &BTreeSet<String>) -> BTreeSet<String> {
        let mut result = changed.clone();
        let mut frontier: Vec<String> = changed.iter().cloned().collect();
        while let Some(cur) = frontier.pop() {
            for (inst_id, inst) in &self.instances {
                if inst.inputs.values().any(|target| target == &cur) && result.insert(inst_id.clone())
                {
                    frontier.push(inst_id.clone());
                }
            }
        }
        result
    }
}
