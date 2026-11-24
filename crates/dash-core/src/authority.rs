//! Principals, capability broker, static graph & binding validation, and host-mediated HTTP.

use crate::model::{CapabilityCeiling, ConfiguredGraph, InstanceConfig};
use dash_luau::{DefinitionCategory, InputRefSpec, SchemaSpec};
use dash_store::SqliteStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Administrator,
    Agent,
    ServicePrincipal,
}

impl PrincipalKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Administrator => "administrator",
            Self::Agent => "agent",
            Self::ServicePrincipal => "service_principal",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Principal {
    pub id: String,
    pub kind: PrincipalKind,
    pub ceiling: CapabilityCeiling,
}

impl Principal {
    pub fn administrator() -> Self {
        Self {
            id: "admin".into(),
            kind: PrincipalKind::Administrator,
            ceiling: CapabilityCeiling::administrator(),
        }
    }

    pub fn authorization_fingerprint(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.kind.as_str().as_bytes());
        h.update(b":");
        h.update(self.id.as_bytes());
        h.update(b":");
        h.update(self.ceiling.fingerprint().as_bytes());
        hex::encode(&h.finalize()[..16])
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("permission denied for {principal_id} ({principal_kind}) on '{target}': {reason}")]
    Denied {
        principal_id: String,
        principal_kind: String,
        target: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub severity: String, // error | warning
    pub code: String,
    pub target: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    pub valid: bool,
    pub issues: Vec<ValidationIssue>,
}

/// Check effective authority intersection across Principal ceiling, Instance grants, and required capabilities.
pub fn check_provider_access(
    principal: &Principal,
    instance: &InstanceConfig,
) -> Result<(), AuthError> {
    if !principal.ceiling.allows_provider(&instance.id) {
        return Err(AuthError::Denied {
            principal_id: principal.id.clone(),
            principal_kind: principal.kind.as_str().into(),
            target: instance.id.clone(),
            reason: "principal ceiling does not permit provider".into(),
        });
    }
    Ok(())
}

pub fn check_action_authority(
    principal: &Principal,
    action_inst: &InstanceConfig,
    graph: &ConfiguredGraph,
    store: &SqliteStore,
) -> Result<(), AuthError> {
    // 1. Principal ceiling must allow this action instance
    if !principal.ceiling.allows_action(&action_inst.id) {
        return Err(AuthError::Denied {
            principal_id: principal.id.clone(),
            principal_kind: principal.kind.as_str().into(),
            target: action_inst.id.clone(),
            reason: "principal ceiling does not grant action invocation".into(),
        });
    }

    // 2. If ServicePrincipal, also verify Administrator SQLite grant ceiling (never trust Workspace YAML alone!)
    if principal.kind == PrincipalKind::ServicePrincipal {
        let granted = store
            .get_service_principal_grant(&principal.id)
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value::<CapabilityCeiling>(v).ok());
        match granted {
            None => {
                return Err(AuthError::Denied {
                    principal_id: principal.id.clone(),
                    principal_kind: "service_principal".into(),
                    target: action_inst.id.clone(),
                    reason: "service principal has no Administrator grant".into(),
                });
            }
            Some(ceil) if !ceil.allows_action(&action_inst.id) => {
                return Err(AuthError::Denied {
                    principal_id: principal.id.clone(),
                    principal_kind: "service_principal".into(),
                    target: action_inst.id.clone(),
                    reason: "administrator grant for service principal excludes this action".into(),
                });
            }
            Some(_) => {}
        }
    }

    // 3. Verify action definition's required capabilities against bound connector instances & principal ceiling
    if let Some(def) = graph.definitions.get(&action_inst.definition) {
        for (input_slot, input_spec) in &def.manifest.inputs {
            if let InputRefSpec::Connector {
                required_capabilities,
                ..
            } = input_spec
            {
                let bound_id = action_inst.inputs.get(input_slot).ok_or_else(|| AuthError::Denied {
                    principal_id: principal.id.clone(),
                    principal_kind: principal.kind.as_str().into(),
                    target: action_inst.id.clone(),
                    reason: format!("missing connector input binding '{input_slot}'"),
                })?;
                let conn_inst = graph.instances.get(bound_id).ok_or_else(|| AuthError::Denied {
                    principal_id: principal.id.clone(),
                    principal_kind: principal.kind.as_str().into(),
                    target: bound_id.clone(),
                    reason: "bound connector instance does not exist".into(),
                })?;
                if !principal.ceiling.allows_connector(&conn_inst.id) {
                    return Err(AuthError::Denied {
                        principal_id: principal.id.clone(),
                        principal_kind: principal.kind.as_str().into(),
                        target: conn_inst.id.clone(),
                        reason: "principal ceiling excludes bound connector".into(),
                    });
                }
                for cap in required_capabilities {
                    if !conn_inst.capabilities.iter().any(|c| c == cap) {
                        return Err(AuthError::Denied {
                            principal_id: principal.id.clone(),
                            principal_kind: principal.kind.as_str().into(),
                            target: conn_inst.id.clone(),
                            reason: format!(
                                "connector instance '{}' lacks required capability '{cap}'",
                                conn_inst.id
                            ),
                        });
                    }
                    if !principal.ceiling.allows_capability(cap) {
                        return Err(AuthError::Denied {
                            principal_id: principal.id.clone(),
                            principal_kind: principal.kind.as_str().into(),
                            target: action_inst.id.clone(),
                            reason: format!("principal ceiling lacks required capability '{cap}'"),
                        });
                    }
                }
            }
        }
        for cap in &def.manifest.required_capabilities {
            if !principal.ceiling.allows_capability(cap) {
                return Err(AuthError::Denied {
                    principal_id: principal.id.clone(),
                    principal_kind: principal.kind.as_str().into(),
                    target: action_inst.id.clone(),
                    reason: format!("principal ceiling lacks capability '{cap}'"),
                });
            }
        }
    }

    Ok(())
}

pub fn check_connector_operation_authority(
    principal: &Principal,
    conn_inst: &InstanceConfig,
    operation: &str,
    graph: &ConfiguredGraph,
) -> Result<(), AuthError> {
    if !principal.ceiling.allows_connector(&conn_inst.id) {
        return Err(AuthError::Denied {
            principal_id: principal.id.clone(),
            principal_kind: principal.kind.as_str().into(),
            target: conn_inst.id.clone(),
            reason: "principal ceiling excludes connector".into(),
        });
    }
    let def = graph.definitions.get(&conn_inst.definition);
    if let Some(d) = def {
        if let Some(op_spec) = d.manifest.operations.get(operation) {
            for cap in &op_spec.required_capabilities {
                if !conn_inst.capabilities.iter().any(|c| c == cap) {
                    return Err(AuthError::Denied {
                        principal_id: principal.id.clone(),
                        principal_kind: principal.kind.as_str().into(),
                        target: conn_inst.id.clone(),
                        reason: format!(
                            "connector instance '{}' does not grant capability '{cap}' required by operation '{operation}'",
                            conn_inst.id
                        ),
                    });
                }
                if !principal.ceiling.allows_capability(cap) {
                    return Err(AuthError::Denied {
                        principal_id: principal.id.clone(),
                        principal_kind: principal.kind.as_str().into(),
                        target: conn_inst.id.clone(),
                        reason: format!("principal lacks capability '{cap}' for operation '{operation}'"),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Validate configured graph contracts, bindings, capabilities, cycles, and Preview mock coverage.
pub fn validate_configured_graph(
    graph: &ConfiguredGraph,
    store: &SqliteStore,
    require_mocks_for_preview: bool,
) -> ValidationReport {
    let mut issues = Vec::new();

    for (inst_id, inst) in &graph.instances {
        let Some(def) = graph.definitions.get(&inst.definition) else {
            issues.push(ValidationIssue {
                severity: "error".into(),
                code: "MISSING_DEFINITION".into(),
                target: inst_id.clone(),
                message: format!("references unknown definition '{}'", inst.definition),
            });
            continue;
        };

        if def.manifest.category != inst.category {
            issues.push(ValidationIssue {
                severity: "error".into(),
                code: "CATEGORY_MISMATCH".into(),
                target: inst_id.clone(),
                message: format!(
                    "instance category '{}' does not match definition '{}' ({})",
                    inst.category.as_str(),
                    def.manifest.id,
                    def.manifest.category.as_str()
                ),
            });
        }

        // Connector capability subset check
        if inst.category == DefinitionCategory::Connector {
            for cap in &inst.capabilities {
                if !def.manifest.capabilities.is_empty()
                    && !def.manifest.capabilities.iter().any(|c| c == cap)
                {
                    issues.push(ValidationIssue {
                        severity: "error".into(),
                        code: "UNDECLARED_CAPABILITY".into(),
                        target: inst_id.clone(),
                        message: format!(
                            "grants capability '{cap}' not declared by connector '{}'",
                            def.manifest.id
                        ),
                    });
                }
            }
            if require_mocks_for_preview && graph.find_mock_for(&def.manifest.id).is_none() {
                issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "MISSING_MOCK".into(),
                    target: inst_id.clone(),
                    message: format!(
                        "preview requires a Mock definition for connector '{}'",
                        def.manifest.id
                    ),
                });
            }
        }

        // Binding & forbidden-edge rules (§2.1)
        for (slot, bound_id) in &inst.inputs {
            let Some(target) = graph.instances.get(bound_id) else {
                issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "MISSING_INPUT_INSTANCE".into(),
                    target: inst_id.clone(),
                    message: format!("input '{slot}' references missing instance '{bound_id}'"),
                });
                continue;
            };

            if inst.category == DefinitionCategory::DataProvider
                && target.category == DefinitionCategory::ActionProvider
            {
                issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "FORBIDDEN_BINDING".into(),
                    target: inst_id.clone(),
                    message: format!(
                        "forbidden binding (§2.1): data provider '{inst_id}' cannot bind action '{bound_id}'"
                    ),
                });
            }
            if inst.category == DefinitionCategory::Adapter
                && target.category == DefinitionCategory::Connector
            {
                issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "FORBIDDEN_BINDING".into(),
                    target: inst_id.clone(),
                    message: format!(
                        "forbidden binding (§2.1): adapter '{inst_id}' cannot bind connector '{bound_id}'"
                    ),
                });
            }

            if let Some(input_spec) = def.manifest.inputs.get(slot) {
                match input_spec {
                    InputRefSpec::Connector {
                        definition,
                        required_capabilities,
                    } => {
                        if target.category != DefinitionCategory::Connector {
                            issues.push(ValidationIssue {
                                severity: "error".into(),
                                code: "INPUT_CATEGORY_MISMATCH".into(),
                                target: inst_id.clone(),
                                message: format!("input '{slot}' expects connector, got '{}'", target.category.as_str()),
                            });
                        }
                        if let Some(expected_def) = definition {
                            if &target.definition != expected_def {
                                issues.push(ValidationIssue {
                                    severity: "error".into(),
                                    code: "DEFINITION_MISMATCH".into(),
                                    target: inst_id.clone(),
                                    message: format!("input '{slot}' expects definition '{expected_def}', got '{}'", target.definition),
                                });
                            }
                        }
                        for req_cap in required_capabilities {
                            if !target.capabilities.iter().any(|c| c == req_cap) {
                                issues.push(ValidationIssue {
                                    severity: "error".into(),
                                    code: "INSUFFICIENT_CAPABILITY".into(),
                                    target: inst_id.clone(),
                                    message: format!(
                                        "input '{slot}' requires capability '{req_cap}' which connector '{bound_id}' does not grant"
                                    ),
                                });
                            }
                        }
                    }
                    InputRefSpec::Data {
                        definition,
                        expected_output,
                    } => {
                        if target.category != DefinitionCategory::DataProvider {
                            issues.push(ValidationIssue {
                                severity: "error".into(),
                                code: "INPUT_CATEGORY_MISMATCH".into(),
                                target: inst_id.clone(),
                                message: format!("input '{slot}' expects data_provider, got '{}'", target.category.as_str()),
                            });
                        }
                        if let Some(expected_def) = definition {
                            if &target.definition != expected_def {
                                issues.push(ValidationIssue {
                                    severity: "error".into(),
                                    code: "DEFINITION_MISMATCH".into(),
                                    target: inst_id.clone(),
                                    message: format!("input '{slot}' expects definition '{expected_def}', got '{}'", target.definition),
                                });
                            }
                        }
                        if let (Some(exp_schema), Some(target_def)) =
                            (expected_output, graph.definitions.get(&target.definition))
                        {
                            if let Some(prod_schema) = &target_def.manifest.output {
                                if let Err(msg) = SchemaSpec::is_compatible(prod_schema, exp_schema) {
                                    issues.push(ValidationIssue {
                                        severity: "error".into(),
                                        code: "SCHEMA_INCOMPATIBLE".into(),
                                        target: inst_id.clone(),
                                        message: format!("input '{slot}': {msg}"),
                                    });
                                }
                            }
                        }
                    }
                    InputRefSpec::Action { definition } => {
                        if target.category != DefinitionCategory::ActionProvider {
                            issues.push(ValidationIssue {
                                severity: "error".into(),
                                code: "INPUT_CATEGORY_MISMATCH".into(),
                                target: inst_id.clone(),
                                message: format!("input '{slot}' expects action_provider, got '{}'", target.category.as_str()),
                            });
                        }
                        if let Some(expected_def) = definition {
                            if &target.definition != expected_def {
                                issues.push(ValidationIssue {
                                    severity: "error".into(),
                                    code: "DEFINITION_MISMATCH".into(),
                                    target: inst_id.clone(),
                                    message: format!("input '{slot}' expects definition '{expected_def}', got '{}'", target.definition),
                                });
                            }
                        }
                    }
                }
            }
        }

        // Service principal validation for automations
        if inst.category == DefinitionCategory::Automation {
            match inst.run_as.as_deref() {
                None | Some("") => {
                    issues.push(ValidationIssue {
                        severity: "error".into(),
                        code: "MISSING_SERVICE_PRINCIPAL".into(),
                        target: inst_id.clone(),
                        message: "automation must declare 'run_as' Service principal (§7.3)".into(),
                    });
                }
                Some(sp_id) => {
                    if !graph.service_principals.contains_key(sp_id) {
                        issues.push(ValidationIssue {
                            severity: "error".into(),
                            code: "UNKNOWN_SERVICE_PRINCIPAL".into(),
                            target: inst_id.clone(),
                            message: format!("references undeclared service principal '{sp_id}'"),
                        });
                    } else if !graph.is_preview
                        && store
                            .get_service_principal_grant(sp_id)
                            .ok()
                            .flatten()
                            .is_none()
                    {
                        issues.push(ValidationIssue {
                            severity: "warning".into(),
                            code: "PENDING_SERVICE_PRINCIPAL_GRANT".into(),
                            target: sp_id.to_string(),
                            message: format!(
                                "service principal '{sp_id}' is requested in Workspace but awaiting Administrator grant"
                            ),
                        });
                    }
                }
            }
        }
    }

    // Cycle check in configured dependency graph
    if let Some(cycle) = detect_graph_cycle(graph) {
        issues.push(ValidationIssue {
            severity: "error".into(),
            code: "DEPENDENCY_CYCLE".into(),
            target: cycle.join(" -> "),
            message: format!("dependency cycle detected: {}", cycle.join(" -> ")),
        });
    }

    // Validate dashboard adapter placements
    for (dash_id, dash) in &graph.dashboards {
        for placement in &dash.adapters {
            match graph.instances.get(&placement.adapter_instance) {
                None => issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "MISSING_ADAPTER".into(),
                    target: dash_id.clone(),
                    message: format!("dashboard references missing adapter '{}'", placement.adapter_instance),
                }),
                Some(inst) if inst.category != DefinitionCategory::Adapter => issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "INVALID_DASHBOARD_INSTANCE".into(),
                    target: dash_id.clone(),
                    message: format!("dashboard placement '{}' must be an adapter", placement.adapter_instance),
                }),
                _ => {}
            }
        }
    }

    let valid = !issues.iter().any(|i| i.severity == "error");
    ValidationReport { valid, issues }
}

fn detect_graph_cycle(graph: &ConfiguredGraph) -> Option<Vec<String>> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Mark {
        Visiting,
        Visited,
    }
    fn dfs(
        node: &str,
        graph: &ConfiguredGraph,
        marks: &mut BTreeMap<String, Mark>,
        stack: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        marks.insert(node.to_string(), Mark::Visiting);
        stack.push(node.to_string());
        if let Some(inst) = graph.instances.get(node) {
            for dep in inst.inputs.values() {
                match marks.get(dep) {
                    Some(Mark::Visiting) => {
                        let pos = stack.iter().position(|n| n == dep).unwrap_or(0);
                        let mut cycle = stack[pos..].to_vec();
                        cycle.push(dep.clone());
                        return Some(cycle);
                    }
                    None => {
                        if let Some(c) = dfs(dep, graph, marks, stack) {
                            return Some(c);
                        }
                    }
                    Some(Mark::Visited) => {}
                }
            }
        }
        stack.pop();
        marks.insert(node.to_string(), Mark::Visited);
        None
    }

    let mut marks = BTreeMap::new();
    let mut stack = Vec::new();
    for id in graph.instances.keys() {
        if !marks.contains_key(id) {
            if let Some(cycle) = dfs(id, graph, &mut marks, &mut stack) {
                return Some(cycle);
            }
        }
    }
    None
}
