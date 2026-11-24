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
    /// Luau resource budget overrides (`[resources]` in dashboard.toml), clamped by the daemon ceiling.
    #[serde(default)]
    pub resources: WorkspaceResources,
}

/// `[resources]` table: Workspace-wide defaults plus `[resources.instances.<id>]` per-Instance
/// overrides (§2.1 resource budgets attach to Instances).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct WorkspaceResources {
    #[serde(flatten)]
    pub defaults: dash_luau::ResourceOverrides,
    #[serde(default)]
    pub instances: BTreeMap<String, dash_luau::ResourceOverrides>,
}

impl WorkspaceResources {
    /// Effective overrides for a pooled VM key (Preview Mock VMs use `preview_mock:<instance>`).
    pub fn overrides_for(&self, vm_instance_id: &str) -> Option<dash_luau::ResourceOverrides> {
        let inst = vm_instance_id
            .strip_prefix("preview_mock:")
            .unwrap_or(vm_instance_id);
        let merged = match self.instances.get(inst) {
            Some(o) => o.merged_over(&self.defaults),
            None => self.defaults.clone(),
        };
        (merged != dash_luau::ResourceOverrides::default()).then_some(merged)
    }
}

fn default_ws_name() -> String {
    "dash-workspace".into()
}

fn default_branch() -> String {
    "main".into()
}

/// Host I/O policy for host-mediated HTTP (§5.2).
///
/// The `[network]` section of the Git-versioned `dashboard.toml` is only a *requested* policy.
/// The effective policy is its intersection with the Administrator network ceiling stored in
/// SQLite (outside the agent-editable Workspace, §11.3). Without an Administrator ceiling every
/// outbound request is denied (pending grant).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct NetworkPolicy {
    /// Host allowlist: exact host names/IP literals, `*.example.com` (subdomains only) or `*`.
    /// Empty = deny every destination (deny by default, §20).
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    /// HTTP method allowlist (case-insensitive, `*` = any). Empty = deny every method.
    #[serde(default)]
    pub allowed_methods: Vec<String>,
    /// Explicit opt-in ONLY for local CI/test mock HTTP servers on loopback. Effective only when
    /// both the requested policy and the Administrator ceiling enable it.
    #[serde(default)]
    pub allow_loopback_for_tests: bool,
}

impl NetworkPolicy {
    /// Whether `host` matches one of `patterns` (`*`, `*.suffix`, or exact, case-insensitive).
    pub fn pattern_list_matches(patterns: &[String], host: &str) -> bool {
        let host = host.trim_end_matches('.');
        patterns.iter().any(|p| {
            let p = p.trim();
            if p == "*" {
                return true;
            }
            if let Some(suffix) = p.strip_prefix("*.") {
                let lower = host.to_ascii_lowercase();
                let suffix = suffix.to_ascii_lowercase();
                return lower.len() > suffix.len() + 1
                    && lower.ends_with(&suffix)
                    && lower.as_bytes()[lower.len() - suffix.len() - 1] == b'.';
            }
            p.eq_ignore_ascii_case(host)
        })
    }

    pub fn allows_host(&self, host: &str) -> bool {
        Self::pattern_list_matches(&self.allowed_hosts, host)
    }

    pub fn allows_method(&self, method: &str) -> bool {
        self.allowed_methods
            .iter()
            .any(|m| m.trim() == "*" || m.trim().eq_ignore_ascii_case(method))
    }
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

    /// Whether every authority this ceiling confers is also conferred by `other` (used to stop
    /// Agents from borrowing a Service principal whose ceiling exceeds their own, §5.1.5).
    /// Draft/Git flags are not compared: they never apply to delegated background work.
    pub fn is_subset_of(&self, other: &CapabilityCeiling) -> bool {
        if other.unrestricted {
            return true;
        }
        if self.unrestricted {
            return false;
        }
        fn covered(mine: &BTreeSet<String>, theirs: &BTreeSet<String>) -> bool {
            if theirs.contains("*") {
                return true;
            }
            mine.iter().all(|m| theirs.contains(m))
        }
        covered(&self.allowed_connectors, &other.allowed_connectors)
            && covered(&self.allowed_capabilities, &other.allowed_capabilities)
            && covered(&self.allowed_actions, &other.allowed_actions)
            && covered(&self.allowed_providers, &other.allowed_providers)
            && covered(&self.allowed_secrets, &other.allowed_secrets)
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
    /// Declared Dashboard State keys: schema version, value type, size limit and migration
    /// policy (§8). Keys listed only in `initial_state` are implicitly declared (schema v1).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub state_schema: BTreeMap<String, crate::state::StateKeyDeclaration>,
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

impl LoadedDefinition {
    /// `sha256:<hex>` digest of a bundled widget asset, for integrity headers and audits.
    pub fn widget_asset_sha256(&self, rel_path: &str) -> Option<String> {
        self.widget_assets.get(rel_path).map(|bytes| {
            let mut h = Sha256::new();
            h.update(bytes);
            format!("sha256:{}", hex::encode(h.finalize()))
        })
    }
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
    /// True only for an isolated Preview runtime (Preview capability context, §5.1.7, §10.3).
    pub is_preview: bool,
    /// Live Generation that routes connector calls to their Mocks (`dash serve --use-mocks`).
    /// Unlike `is_preview`, authority still comes from Administrator grants in SQLite.
    pub substitute_mocks: bool,
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

    /// Whether connector invocations are routed to Mock Definitions (Preview or `--use-mocks`).
    pub fn routes_to_mocks(&self) -> bool {
        self.is_preview || self.substitute_mocks
    }

    /// Content digest of a Definition including every Library it can `require` at runtime
    /// (all Workspace Libraries are resolvable), so library edits count as code changes.
    pub fn effective_code_digest(&self, definition_id: &str) -> Option<String> {
        let def = self.definitions.get(definition_id)?;
        let mut h = Sha256::new();
        h.update(def.manifest.identity_version.as_bytes());
        for (id, d) in &self.definitions {
            if d.manifest.category == DefinitionCategory::Library {
                h.update(b"|lib:");
                h.update(id.as_bytes());
                h.update(b"@");
                h.update(d.manifest.identity_version.as_bytes());
            }
        }
        Some(format!("code:{}", hex::encode(h.finalize())))
    }

    /// Find the Mock definition targeting `connector_def_id`, if any.
    ///
    /// The Workspace loader admits at most one Mock per connector; should a hand-built graph hold
    /// several, the Mock from the connector's own Package wins, then the lowest Definition ID.
    pub fn find_mock_for(&self, connector_def_id: &str) -> Option<&LoadedDefinition> {
        let target_pkg = self
            .definitions
            .get(connector_def_id)
            .map(|d| d.package_name.as_str());
        let mut candidates = self.definitions.values().filter(|d| {
            d.manifest.category == DefinitionCategory::Mock
                && d.manifest.target_connector.as_deref() == Some(connector_def_id)
        });
        let first = candidates.next()?;
        if Some(first.package_name.as_str()) == target_pkg {
            return Some(first);
        }
        Some(
            candidates
                .find(|d| Some(d.package_name.as_str()) == target_pkg)
                .unwrap_or(first),
        )
    }

    /// Validate a generated Widget node against the Generation: `widget_type` must name a
    /// widget provider Definition, and `props` must satisfy its declared `props` schema (§4.2
    /// widget messages, §10.1).
    pub fn check_widget_node(
        &self,
        widget_type: &str,
        props: &serde_json::Value,
    ) -> Result<(), String> {
        let def = self
            .definitions
            .get(widget_type)
            .ok_or_else(|| format!("unknown widget type '{widget_type}'"))?;
        if def.manifest.category != DefinitionCategory::WidgetProvider {
            return Err(format!(
                "'{widget_type}' is a {} Definition, not a widget provider",
                def.manifest.category.as_str()
            ));
        }
        if let Some(schema) = &def.manifest.widget_props {
            schema
                .validate_value(props)
                .map_err(|e| format!("widget '{widget_type}' props: {e}"))?;
        }
        Ok(())
    }

    /// Resolve all library sources available to a definition within the workspace.
    pub fn resolved_libraries(&self) -> BTreeMap<String, String> {
        self.definitions
            .values()
            .filter(|d| d.manifest.category == DefinitionCategory::Library)
            .map(|d| (d.manifest.id.clone(), d.runtime_source.clone()))
            .collect()
    }

    /// Declared `libraries` entries that do not resolve to a Library Definition in a loaded
    /// package, as `(definition_id, library_id, reason)`. At runtime `require` only serves
    /// declared, resolved libraries, so each entry here would fail when loaded.
    pub fn unresolved_library_declarations(&self) -> Vec<(String, String, String)> {
        let mut out = Vec::new();
        for (def_id, def) in &self.definitions {
            for lib in &def.manifest.libraries {
                match self.definitions.get(lib) {
                    None => out.push((
                        def_id.clone(),
                        lib.clone(),
                        format!("declared library '{lib}' is not provided by any resolved package"),
                    )),
                    Some(d) if d.manifest.category != DefinitionCategory::Library => out.push((
                        def_id.clone(),
                        lib.clone(),
                        format!(
                            "declared library '{lib}' is a {} Definition, not a library",
                            d.manifest.category.as_str()
                        ),
                    )),
                    Some(_) => {}
                }
            }
        }
        out
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

#[cfg(test)]
mod tests {
    use super::*;
    use dash_luau::evaluate_declaration;

    fn def(pkg: &str, decl: &str) -> LoadedDefinition {
        let manifest = evaluate_declaration(decl, &BTreeMap::new()).unwrap();
        LoadedDefinition {
            package_name: pkg.into(),
            manifest,
            definition_source: decl.into(),
            runtime_source: String::new(),
            tests_source: None,
            fixtures: BTreeMap::new(),
            widget_assets: BTreeMap::new(),
        }
    }

    #[test]
    fn find_mock_prefers_connector_package() {
        let mut g = ConfiguredGraph::default();
        g.definitions.insert(
            "gh.connector".into(),
            def("gh", r#"local dash = require("dash"); return dash.connector { id = "gh.connector" }"#),
        );
        // `aaa.mock` sorts first but lives in another package.
        g.definitions.insert(
            "aaa.mock".into(),
            def("aaa", r#"local dash = require("dash"); return dash.mock { id = "aaa.mock", ["for"] = "gh.connector" }"#),
        );
        g.definitions.insert(
            "gh.mock".into(),
            def("gh", r#"local dash = require("dash"); return dash.mock { id = "gh.mock", ["for"] = "gh.connector" }"#),
        );
        assert_eq!(g.find_mock_for("gh.connector").unwrap().manifest.id, "gh.mock");
        assert!(g.find_mock_for("other.connector").is_none());
    }

    #[test]
    fn widget_nodes_are_checked_against_provider_contracts() {
        let mut g = ConfiguredGraph::default();
        let mut w = def(
            "eng",
            r#"local dash = require("dash"); return dash.widget_provider { id = "eng.badge", asset = "assets/index.html", props = dash.object { count = dash.integer() } }"#,
        );
        w.widget_assets.insert("assets/index.html".into(), b"<p>hi</p>".to_vec());
        g.definitions.insert("eng.badge".into(), w);
        g.definitions.insert(
            "eng.data".into(),
            def("eng", r#"local dash = require("dash"); return dash.data_provider { id = "eng.data" }"#),
        );
        assert!(g.check_widget_node("eng.badge", &serde_json::json!({ "count": 3 })).is_ok());
        assert!(g.check_widget_node("eng.badge", &serde_json::json!({ "count": "x" })).is_err());
        assert!(g.check_widget_node("eng.data", &serde_json::json!({})).is_err());
        assert!(g.check_widget_node("nope.widget", &serde_json::json!({})).is_err());
        let digest = g.definitions["eng.badge"].widget_asset_sha256("assets/index.html").unwrap();
        assert!(digest.starts_with("sha256:") && digest.len() == 71);
    }
}
