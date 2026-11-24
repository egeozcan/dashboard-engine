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

    // 2. If ServicePrincipal in live mode, also verify Administrator SQLite grant ceiling (never trust Workspace YAML alone!)
    if principal.kind == PrincipalKind::ServicePrincipal && !graph.is_preview {
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
                let bound_id =
                    action_inst
                        .inputs
                        .get(input_slot)
                        .ok_or_else(|| AuthError::Denied {
                            principal_id: principal.id.clone(),
                            principal_kind: principal.kind.as_str().into(),
                            target: action_inst.id.clone(),
                            reason: format!("missing connector input binding '{input_slot}'"),
                        })?;
                let conn_inst = graph
                    .instances
                    .get(bound_id)
                    .ok_or_else(|| AuthError::Denied {
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
    // Deny by default (§20): the operation must be declared by the connector Definition, which
    // must exist. Mocks are checked against their target connector's operation table, because
    // the Instance always names the real connector Definition.
    let Some(d) = graph.definitions.get(&conn_inst.definition) else {
        return Err(AuthError::Denied {
            principal_id: principal.id.clone(),
            principal_kind: principal.kind.as_str().into(),
            target: conn_inst.id.clone(),
            reason: format!(
                "connector definition '{}' is missing; operation '{operation}' denied",
                conn_inst.definition
            ),
        });
    };
    let Some(op_spec) = d.manifest.operations.get(operation) else {
        return Err(AuthError::Denied {
            principal_id: principal.id.clone(),
            principal_kind: principal.kind.as_str().into(),
            target: conn_inst.id.clone(),
            reason: format!(
                "undeclared operation '{operation}' on connector '{}'",
                d.manifest.id
            ),
        });
    };
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
    Ok(())
}

/// Reject connector operations with side effects from read contexts (data providers, Dry runs,
/// reconcile handlers) — §20 "No read side effects", §9 Dry run. Undeclared operations and
/// missing Definitions are denied as well.
pub fn check_connector_operation_read_only(
    principal: &Principal,
    conn_inst: &InstanceConfig,
    operation: &str,
    graph: &ConfiguredGraph,
) -> Result<(), AuthError> {
    let spec = graph
        .definitions
        .get(&conn_inst.definition)
        .and_then(|d| d.manifest.operations.get(operation));
    match spec {
        Some(op) if !op.has_side_effects() => Ok(()),
        Some(_) => Err(AuthError::Denied {
            principal_id: principal.id.clone(),
            principal_kind: principal.kind.as_str().into(),
            target: conn_inst.id.clone(),
            reason: format!(
                "operation '{operation}' has side effects and is forbidden in a read-only context"
            ),
        }),
        None => Err(AuthError::Denied {
            principal_id: principal.id.clone(),
            principal_kind: principal.kind.as_str().into(),
            target: conn_inst.id.clone(),
            reason: format!("undeclared operation '{operation}'"),
        }),
    }
}

/// Append a `denied` audit record for a capability denial (§14.3 "Audit all capability denials").
pub fn audit_denial(
    store: &SqliteStore,
    principal: &Principal,
    operation: &str,
    target: &str,
    reason: &str,
) {
    let _ = store.append_audit(
        &principal.id,
        principal.kind.as_str(),
        operation,
        target,
        "denied",
        serde_json::json!({ "reason": reason }),
    );
}

/// Audit an [`AuthError`] (if any) and pass the result through unchanged.
pub fn audited<T>(
    store: &SqliteStore,
    principal: &Principal,
    operation: &str,
    result: Result<T, AuthError>,
) -> Result<T, AuthError> {
    if let Err(AuthError::Denied { target, reason, .. }) = &result {
        audit_denial(store, principal, operation, target, reason);
    }
    result
}

/// Audit operations that attribute a Principal as an editor of a Draft.
pub const DRAFT_EDITOR_AUDIT_OPS: &[&str] = &["draft.begin", "draft.edit"];

/// Resolve a Principal recorded by id (and kind, when known) to its *current* authority.
/// Returns `None` for the Administrator (unrestricted) and for an id of unknown kind that
/// matches no Agent or Service principal (direct host/Administrator callers). Revoked or
/// expired Agents resolve to an empty ceiling, so their edits can no longer be approved.
pub fn resolve_recorded_principal(
    store: &SqliteStore,
    principal_id: &str,
    principal_kind: Option<&str>,
) -> Option<Principal> {
    let empty = |kind: PrincipalKind| Principal {
        id: principal_id.to_string(),
        kind,
        ceiling: CapabilityCeiling::default(),
    };
    if principal_kind == Some(PrincipalKind::Administrator.as_str()) {
        return None;
    }
    let agent = store
        .list_agent_principals()
        .ok()
        .and_then(|list| list.into_iter().find(|a| a.agent_id == principal_id));
    if let Some(a) = agent {
        let expired = a
            .expires_at
            .as_deref()
            .and_then(|e| chrono::DateTime::parse_from_rfc3339(e).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc) <= chrono::Utc::now())
            .unwrap_or(false);
        if a.revoked_at.is_some() || expired {
            return Some(empty(PrincipalKind::Agent));
        }
        let mut ceiling: CapabilityCeiling = serde_json::from_value(a.ceiling).unwrap_or_default();
        ceiling.unrestricted = false;
        return Some(Principal {
            id: principal_id.to_string(),
            kind: PrincipalKind::Agent,
            ceiling,
        });
    }
    match principal_kind {
        // Unknown kind (a Draft's `created_by`) that is not an Agent: a trusted host or
        // Administrator caller. Never matched against Service principal names.
        None => None,
        Some(k) if k == PrincipalKind::ServicePrincipal.as_str() => {
            let ceiling = store
                .get_service_principal_grant(principal_id)
                .ok()
                .flatten()
                .and_then(|v| serde_json::from_value::<CapabilityCeiling>(v).ok())
                .unwrap_or_default();
            Some(Principal {
                id: principal_id.to_string(),
                kind: PrincipalKind::ServicePrincipal,
                ceiling,
            })
        }
        // An Agent (or other recorded kind) without a live record has no authority.
        Some(_) => Some(empty(PrincipalKind::Agent)),
    }
}

/// Every non-Administrator Principal that edited (or began) a Draft, resolved to its current
/// ceiling. Attribution comes from `draft.begin`/`draft.edit` audit records and `created_by`.
pub fn draft_editor_principals(
    store: &SqliteStore,
    draft_id: &str,
    created_by: &str,
) -> Vec<Principal> {
    let mut out: Vec<Principal> = Vec::new();
    let recorded = store
        .list_audit_principals_for_target(draft_id, DRAFT_EDITOR_AUDIT_OPS)
        .unwrap_or_default();
    for (id, kind) in &recorded {
        if let Some(p) = resolve_recorded_principal(store, id, Some(kind)) {
            if !out.iter().any(|o| o.id == p.id) {
                out.push(p);
            }
        }
    }
    if !recorded.iter().any(|(id, _)| id == created_by) {
        if let Some(p) = resolve_recorded_principal(store, created_by, None) {
            if !out.iter().any(|o| o.id == p.id) {
                out.push(p);
            }
        }
    }
    out
}

/// Whether Instance `id` is new or changed (configuration, bindings, secrets, capabilities,
/// `run_as`, Definition identity or Definition/Library code) between two Generations.
fn instance_changed(active: &ConfiguredGraph, candidate: &ConfiguredGraph, id: &str) -> bool {
    let Some(new) = candidate.instances.get(id) else {
        return false;
    };
    let Some(old) = active.instances.get(id) else {
        return true;
    };
    old.config_digest() != new.config_digest()
        || active.effective_code_digest(&old.definition)
            != candidate.effective_code_digest(&new.definition)
}

fn input_closure(graph: &ConfiguredGraph, root: &str) -> std::collections::BTreeSet<String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut stack = vec![root.to_string()];
    while let Some(cur) = stack.pop() {
        if !seen.insert(cur.clone()) {
            continue;
        }
        if let Some(inst) = graph.instances.get(&cur) {
            stack.extend(inst.inputs.values().cloned());
        }
    }
    seen
}

/// Activation-time authority check for a Draft (§5.2, §11.3, §5.1.5, ADR-0002).
///
/// Compares the candidate graph against the active Generation and, for every non-Administrator
/// Principal in `principals` (the Draft's editors plus the activating Principal):
///
/// * **Secret bindings** — a connector Instance that holds secret bindings and is new or changed
///   (secrets, configuration such as `base_url`, capabilities, Definition or code) may only be
///   activated when each Principal's ceiling allows every bound secret. Otherwise an Agent could
///   redirect an Administrator-bound credential by editing the connector (`PENDING_SECRET_GRANT`).
/// * **Service principal consumers** — an automation or data provider with `run_as`, or a history
///   series, whose binding or anything in its input closure is new or changed, may only be
///   activated when the Service principal's ceiling (Administrator grant, or the requested ceiling
///   while pending) is a subset of each Principal's ceiling (`PENDING_SERVICE_PRINCIPAL_BINDING`).
///
/// Violations are validation errors: the Administrator must author such changes or extend the
/// Agent's ceiling. Nothing here can be satisfied by editing Workspace files.
pub fn check_draft_activation_authority(
    store: &SqliteStore,
    active: &ConfiguredGraph,
    candidate: &ConfiguredGraph,
    principals: &[Principal],
) -> Vec<ValidationIssue> {
    let restricted: Vec<&Principal> = principals
        .iter()
        .filter(|p| !p.ceiling.unrestricted)
        .collect();
    let mut issues = Vec::new();
    if restricted.is_empty() {
        return issues;
    }

    // 1. Secret-holding connector Instances.
    for (id, inst) in &candidate.instances {
        if inst.category != DefinitionCategory::Connector || inst.secrets.is_empty() {
            continue;
        }
        if !instance_changed(active, candidate, id) {
            continue;
        }
        for p in &restricted {
            for slot in inst.secrets.keys() {
                let Some(secret) = inst.resolved_secret_name(slot) else {
                    continue;
                };
                if !p.ceiling.allows_secret(&secret) {
                    issues.push(ValidationIssue {
                        severity: "error".into(),
                        code: "PENDING_SECRET_GRANT".into(),
                        target: id.clone(),
                        message: format!(
                            "{} '{}' changed connector '{id}', which holds secret '{secret}' outside its ceiling; \
                             an Administrator must make this change or grant the secret (§5.2, §11.3)",
                            p.kind.as_str(),
                            p.id
                        ),
                    });
                }
            }
        }
    }

    // 2. Consumers of Service principals.
    let mut consumers: Vec<(String, String, bool)> = Vec::new(); // (sp_id, root, binding_changed)
    for (id, inst) in &candidate.instances {
        if let Some(sp) = inst.run_as.as_deref().filter(|s| !s.is_empty()) {
            consumers.push((
                sp.to_string(),
                id.clone(),
                instance_changed(active, candidate, id),
            ));
        }
    }
    for series in &candidate.workspace.history {
        let canon = serde_json::to_string(series).unwrap_or_default();
        let existed = active
            .workspace
            .history
            .iter()
            .any(|s| serde_json::to_string(s).unwrap_or_default() == canon);
        consumers.push((
            series.service_principal.clone(),
            series.instance_id.clone(),
            !existed,
        ));
    }
    for (sp_id, root, binding_changed) in consumers {
        // Emitters feeding the consumer's unscoped Event triggers drive it like inputs (§7.3).
        let mut closure = input_closure(candidate, &root);
        for src in crate::automations::unscoped_event_sources(candidate, &root) {
            closure.extend(input_closure(candidate, &src));
        }
        let changed = binding_changed
            || closure
                .iter()
                .any(|i| instance_changed(active, candidate, i));
        if !changed {
            continue;
        }
        let sp_ceiling = store
            .get_service_principal_grant(&sp_id)
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value::<CapabilityCeiling>(v).ok())
            .or_else(|| {
                candidate
                    .service_principals
                    .get(&sp_id)
                    .map(|s| s.ceiling.clone())
            })
            .unwrap_or_default();
        for p in &restricted {
            if !sp_ceiling.is_subset_of(&p.ceiling) {
                issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "PENDING_SERVICE_PRINCIPAL_BINDING".into(),
                    target: root.clone(),
                    message: format!(
                        "{} '{}' changed '{root}' (or its inputs), which runs as Service principal '{sp_id}' \
                         whose ceiling exceeds its own; an Administrator must approve this binding (§5.1.5, ADR-0002)",
                        p.kind.as_str(),
                        p.id
                    ),
                });
            }
        }
    }
    issues
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
                                message: format!(
                                    "input '{slot}' expects connector, got '{}'",
                                    target.category.as_str()
                                ),
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
                                message: format!(
                                    "input '{slot}' expects data_provider, got '{}'",
                                    target.category.as_str()
                                ),
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
                                if let Err(msg) = SchemaSpec::is_compatible(prod_schema, exp_schema)
                                {
                                    issues.push(ValidationIssue {
                                        severity: "error".into(),
                                        code: "SCHEMA_INCOMPATIBLE".into(),
                                        target: inst_id.clone(),
                                        message: format!("input '{slot}': {msg}"),
                                    });
                                }
                            } else if !matches!(exp_schema, SchemaSpec::Any) {
                                issues.push(ValidationIssue {
                                    severity: "error".into(),
                                    code: "SCHEMA_INCOMPATIBLE".into(),
                                    target: inst_id.clone(),
                                    message: format!(
                                        "input '{slot}' expects a typed output but '{}' declares no output schema",
                                        target.definition
                                    ),
                                });
                            }
                        }
                    }
                    InputRefSpec::Action { definition } => {
                        if target.category != DefinitionCategory::ActionProvider {
                            issues.push(ValidationIssue {
                                severity: "error".into(),
                                code: "INPUT_CATEGORY_MISMATCH".into(),
                                target: inst_id.clone(),
                                message: format!(
                                    "input '{slot}' expects action_provider, got '{}'",
                                    target.category.as_str()
                                ),
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
            } else {
                issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "UNDECLARED_INPUT".into(),
                    target: inst_id.clone(),
                    message: format!(
                        "input '{slot}' is not declared by definition '{}'",
                        def.manifest.id
                    ),
                });
            }
        }
        // Every declared input must be bound (§4.2: compatibility is checked during wiring).
        for slot in def.manifest.inputs.keys() {
            if !inst.inputs.contains_key(slot) {
                issues.push(ValidationIssue {
                    severity: "error".into(),
                    code: "UNBOUND_INPUT".into(),
                    target: inst_id.clone(),
                    message: format!(
                        "declared input '{slot}' of definition '{}' is not bound",
                        def.manifest.id
                    ),
                });
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

    if !graph.routes_to_mocks() {
        // Service principals reaching credentialed connectors must also be allowed their secrets,
        // otherwise background work fails at the broker (§5.2, §7.4).
        for (sp_id, sp) in &graph.service_principals {
            let granted = store
                .get_service_principal_grant(sp_id)
                .ok()
                .flatten()
                .and_then(|v| serde_json::from_value::<CapabilityCeiling>(v).ok());
            let ceiling = granted.as_ref().unwrap_or(&sp.ceiling);
            for (conn_id, conn) in &graph.instances {
                if conn.category != DefinitionCategory::Connector
                    || ceiling.allowed_connectors.is_empty()
                    || !ceiling.allows_connector(conn_id)
                {
                    continue;
                }
                for slot in conn.secrets.keys() {
                    if let Some(secret) = conn.resolved_secret_name(slot) {
                        if !ceiling.allows_secret(&secret) {
                            issues.push(ValidationIssue {
                                severity: "warning".into(),
                                code: "SERVICE_PRINCIPAL_SECRET_NOT_GRANTED".into(),
                                target: sp_id.clone(),
                                message: format!(
                                    "service principal '{sp_id}' may use connector '{conn_id}' but its {} ceiling does not allow secret '{secret}'",
                                    if granted.is_some() { "granted" } else { "requested" }
                                ),
                            });
                        }
                    }
                }
            }
        }

        // The Workspace [network] section is only a request; flag hosts beyond the Administrator ceiling.
        let requested = &graph.workspace.network;
        let ceiling = crate::http_broker::load_network_ceiling(store);
        let uncovered: Vec<&String> = requested
            .allowed_hosts
            .iter()
            .filter(|h| ceiling.as_ref().map(|c| !c.allows_host(h)).unwrap_or(true))
            .collect();
        if !uncovered.is_empty() {
            issues.push(ValidationIssue {
                severity: "warning".into(),
                code: "PENDING_NETWORK_GRANT".into(),
                target: "workspace.network".into(),
                message: format!(
                    "requested hosts {:?} are outside the Administrator network ceiling and stay denied until granted (`dash network grant`)",
                    uncovered
                ),
            });
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
                    message: format!(
                        "dashboard references missing adapter '{}'",
                        placement.adapter_instance
                    ),
                }),
                Some(inst) if inst.category != DefinitionCategory::Adapter => {
                    issues.push(ValidationIssue {
                        severity: "error".into(),
                        code: "INVALID_DASHBOARD_INSTANCE".into(),
                        target: dash_id.clone(),
                        message: format!(
                            "dashboard placement '{}' must be an adapter",
                            placement.adapter_instance
                        ),
                    })
                }
                _ => {}
            }
        }
    }

    // §4.4: Definitions may only require declared libraries from resolved packages.
    for (def_id, _lib, message) in graph.unresolved_library_declarations() {
        issues.push(ValidationIssue {
            severity: "error".into(),
            code: "UNRESOLVED_LIBRARY".into(),
            target: def_id,
            message,
        });
    }

    // Action `invalidates` entries that match nothing silently invalidate nothing (§9.1 step 6).
    for (action_id, entry) in crate::actions::unresolved_invalidations(graph) {
        issues.push(ValidationIssue {
            severity: "warning".into(),
            code: "UNRESOLVED_INVALIDATION".into(),
            target: action_id,
            message: format!(
                "invalidates entry '{entry}' matches no Instance, Definition or input slot; nothing will be invalidated"
            ),
        });
    }
    // Background work: automation triggers, history series and keep-warm polling (§7.3, §7.4)
    issues.extend(crate::automations::validate_background_work(graph, store));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{HistorySeriesConfig, LoadedDefinition, ServicePrincipalRequested};
    use dash_luau::evaluate_declaration;
    use serde_json::json;
    use std::collections::BTreeSet;

    fn def(decl: &str, runtime: &str) -> LoadedDefinition {
        let mut sources = BTreeMap::new();
        sources.insert("definition.luau".to_string(), decl.as_bytes().to_vec());
        sources.insert("runtime.luau".to_string(), runtime.as_bytes().to_vec());
        LoadedDefinition {
            package_name: "test".into(),
            manifest: evaluate_declaration(decl, &sources).unwrap(),
            definition_source: decl.into(),
            runtime_source: runtime.into(),
            tests_source: None,
            fixtures: BTreeMap::new(),
            widget_assets: BTreeMap::new(),
        }
    }

    const CONNECTOR: &str = r#"
local dash = require("dash")
return dash.connector {
    id = "gh.connector",
    capabilities = { "pr.read", "pr.merge" },
    credentials = { "auth" },
    operations = {
        list = { required_capabilities = { "pr.read" } },
        close = { required_capabilities = { "pr.read" }, mode = "write" },
        merge = { required_capabilities = { "pr.merge" } },
    },
}
"#;

    fn inst(id: &str, category: DefinitionCategory, definition: &str) -> InstanceConfig {
        InstanceConfig {
            id: id.into(),
            category,
            definition: definition.into(),
            inputs: BTreeMap::new(),
            config: json!({}),
            secrets: BTreeMap::new(),
            capabilities: vec![],
            run_as: None,
            enabled: None,
        }
    }

    fn base_graph() -> ConfiguredGraph {
        let mut g = ConfiguredGraph::default();
        g.definitions
            .insert("gh.connector".into(), def(CONNECTOR, "return {}"));
        g.definitions.insert(
            "gh.prs".into(),
            def(
                r#"local dash = require("dash")
return dash.data_provider { id = "gh.prs", handler = "query",
  inputs = { gh = dash.connector_ref("gh.connector", { required_capabilities = { "pr.read" } }) } }"#,
                "local R = {}; function R.query(ctx) return {} end; return R",
            ),
        );
        g.definitions.insert(
            "gh.utils".into(),
            def(
                r#"local dash = require("dash"); return dash.library { id = "gh.utils" }"#,
                "return {}",
            ),
        );
        let mut reader = inst("gh_reader", DefinitionCategory::Connector, "gh.connector");
        reader
            .secrets
            .insert("auth".into(), "secret://gh-work".into());
        reader.capabilities = vec!["pr.read".into()];
        reader.config = json!({ "base_url": "https://api.github.com" });
        g.instances.insert(reader.id.clone(), reader);
        let mut prs = inst("open_prs", DefinitionCategory::DataProvider, "gh.prs");
        prs.inputs.insert("gh".into(), "gh_reader".into());
        g.instances.insert(prs.id.clone(), prs);
        g.service_principals.insert(
            "sampler".into(),
            ServicePrincipalRequested {
                id: "sampler".into(),
                description: String::new(),
                ceiling: CapabilityCeiling {
                    allowed_providers: BTreeSet::from(["open_prs".into()]),
                    allowed_connectors: BTreeSet::from(["gh_reader".into()]),
                    allowed_capabilities: BTreeSet::from(["pr.read".into()]),
                    allowed_secrets: BTreeSet::from(["gh-work".into()]),
                    ..Default::default()
                },
            },
        );
        g
    }

    fn agent(ceiling: CapabilityCeiling) -> Principal {
        Principal {
            id: "agt_1".into(),
            kind: PrincipalKind::Agent,
            ceiling,
        }
    }

    fn drafting_agent() -> Principal {
        agent(CapabilityCeiling {
            allowed_providers: BTreeSet::from(["*".into()]),
            can_author_drafts: true,
            can_activate_drafts: true,
            ..Default::default()
        })
    }

    #[test]
    fn undeclared_operations_and_missing_definitions_are_denied() {
        let g = base_graph();
        let admin = Principal::administrator();
        let reader = &g.instances["gh_reader"];
        assert!(check_connector_operation_authority(&admin, reader, "list", &g).is_ok());
        let err =
            check_connector_operation_authority(&admin, reader, "simulate_external_merge", &g)
                .unwrap_err();
        assert!(err.to_string().contains("undeclared operation"));
        // Capability still enforced for declared ops.
        assert!(check_connector_operation_authority(&admin, reader, "merge", &g).is_err());
        let mut orphan = reader.clone();
        orphan.definition = "gone.connector".into();
        assert!(check_connector_operation_authority(&admin, &orphan, "list", &g).is_err());
    }

    #[test]
    fn read_contexts_reject_operations_with_side_effects() {
        let g = base_graph();
        let admin = Principal::administrator();
        let reader = &g.instances["gh_reader"];
        assert!(check_connector_operation_read_only(&admin, reader, "list", &g).is_ok());
        // Explicit `mode = "write"` wins over read-only capabilities (no name heuristic).
        assert!(check_connector_operation_read_only(&admin, reader, "close", &g).is_err());
        assert!(check_connector_operation_read_only(&admin, reader, "merge", &g).is_err());
        assert!(check_connector_operation_read_only(&admin, reader, "nope", &g).is_err());
    }

    #[test]
    fn denials_are_audited() {
        let store = SqliteStore::open_in_memory().unwrap();
        let p = agent(CapabilityCeiling::default());
        let g = base_graph();
        let r = audited(
            &store,
            &p,
            "provider.query",
            check_provider_access(&p, &g.instances["open_prs"]),
        );
        assert!(r.is_err());
        let a = store.list_audits(5).unwrap();
        assert_eq!(a[0].operation, "provider.query");
        assert_eq!(a[0].outcome, "denied");
        assert_eq!(a[0].target, "open_prs");
    }

    #[test]
    fn agent_cannot_redirect_a_bound_secret_by_editing_connector_config_or_code() {
        let store = SqliteStore::open_in_memory().unwrap();
        let active = base_graph();
        let unchanged = base_graph();
        let ag = drafting_agent();
        assert!(check_draft_activation_authority(
            &store,
            &active,
            &unchanged,
            std::slice::from_ref(&ag)
        )
        .is_empty());

        // 1. Point the secret-holding connector at an attacker host.
        let mut cand = base_graph();
        cand.instances.get_mut("gh_reader").unwrap().config =
            json!({ "base_url": "https://evil.example" });
        let issues =
            check_draft_activation_authority(&store, &active, &cand, std::slice::from_ref(&ag));
        assert!(issues
            .iter()
            .any(|i| i.code == "PENDING_SECRET_GRANT" && i.target == "gh_reader"));
        // The Administrator may make the same change; so may an Agent granted the secret.
        assert!(check_draft_activation_authority(
            &store,
            &active,
            &cand,
            &[Principal::administrator()]
        )
        .is_empty());
        let mut granted = drafting_agent();
        granted.ceiling.allowed_secrets.insert("gh-work".into());
        assert!(check_draft_activation_authority(&store, &active, &cand, &[granted]).is_empty());

        // 2. Edit the connector's runtime code.
        let mut cand = base_graph();
        cand.definitions.insert(
            "gh.connector".into(),
            def(CONNECTOR, "return { exfil = true }"),
        );
        assert!(check_draft_activation_authority(
            &store,
            &active,
            &cand,
            std::slice::from_ref(&ag)
        )
        .iter()
        .any(|i| i.code == "PENDING_SECRET_GRANT"));

        // 3. Edit a Library any connector can `require`.
        let mut cand = base_graph();
        cand.definitions.insert(
            "gh.utils".into(),
            def(
                r#"local dash = require("dash"); return dash.library { id = "gh.utils" }"#,
                "return { x = 1 }",
            ),
        );
        assert!(!check_draft_activation_authority(
            &store,
            &active,
            &cand,
            std::slice::from_ref(&ag)
        )
        .is_empty());

        // 4. Rebind a different secret onto a new connector Instance.
        let mut cand = base_graph();
        let mut evil = cand.instances["gh_reader"].clone();
        evil.id = "evil_conn".into();
        evil.secrets
            .insert("auth".into(), "secret://admin-root".into());
        cand.instances.insert("evil_conn".into(), evil);
        let issues = check_draft_activation_authority(&store, &active, &cand, &[ag]);
        assert!(issues.iter().any(|i| i.target == "evil_conn"));
    }

    #[test]
    fn agent_cannot_borrow_a_service_principal_ceiling_exceeding_its_own() {
        let store = SqliteStore::open_in_memory().unwrap();
        let active = base_graph();
        let ag = drafting_agent();

        // New polled provider running as `sampler`.
        let mut cand = base_graph();
        let mut polled = inst("borrowed", DefinitionCategory::DataProvider, "gh.prs");
        polled.inputs.insert("gh".into(), "gh_reader".into());
        polled.run_as = Some("sampler".into());
        cand.instances.insert("borrowed".into(), polled);
        let issues =
            check_draft_activation_authority(&store, &active, &cand, std::slice::from_ref(&ag));
        assert!(issues
            .iter()
            .any(|i| i.code == "PENDING_SERVICE_PRINCIPAL_BINDING" && i.target == "borrowed"));

        // New history series under the Service principal.
        let mut cand2 = base_graph();
        cand2.workspace.history.push(HistorySeriesConfig {
            series_id: "s".into(),
            instance_id: "open_prs".into(),
            params: json!({}),
            aggregator: "count".into(),
            value_path: None,
            cadence_secs: 60,
            retention_days: 1,
            service_principal: "sampler".into(),
        });
        assert!(check_draft_activation_authority(
            &store,
            &active,
            &cand2,
            std::slice::from_ref(&ag)
        )
        .iter()
        .any(|i| i.code == "PENDING_SERVICE_PRINCIPAL_BINDING"));

        // The Administrator grant (not the YAML request) is the ceiling compared once it exists.
        store
            .set_service_principal_grant(
                "sampler",
                serde_json::to_value(CapabilityCeiling {
                    allowed_providers: BTreeSet::from(["open_prs".into(), "borrowed".into()]),
                    ..Default::default()
                })
                .unwrap(),
                "admin",
            )
            .unwrap();
        // drafting_agent has allowed_providers = * so the granted ceiling is now a subset.
        assert!(check_draft_activation_authority(&store, &active, &cand, &[ag]).is_empty());
        let narrow = agent(CapabilityCeiling {
            allowed_providers: BTreeSet::from(["open_prs".into()]),
            ..Default::default()
        });
        assert!(!check_draft_activation_authority(&store, &active, &cand, &[narrow]).is_empty());
    }

    #[test]
    fn recorded_principals_resolve_to_current_authority() {
        let store = SqliteStore::open_in_memory().unwrap();
        let rec = store
            .create_agent_principal(
                "bot",
                "dsh_agt_token_for_test_000000000000",
                serde_json::to_value(CapabilityCeiling {
                    allowed_secrets: BTreeSet::from(["gh-work".into()]),
                    unrestricted: true,
                    ..Default::default()
                })
                .unwrap(),
                None,
            )
            .unwrap();
        let p = resolve_recorded_principal(&store, &rec.agent_id, Some("agent")).unwrap();
        assert!(
            !p.ceiling.unrestricted,
            "an Agent record can never resolve as unrestricted"
        );
        assert!(p.ceiling.allows_secret("gh-work"));
        store.revoke_agent_principal(&rec.agent_id).unwrap();
        let p = resolve_recorded_principal(&store, &rec.agent_id, Some("agent")).unwrap();
        assert!(
            !p.ceiling.allows_secret("gh-work"),
            "revoked Agents lose their authority"
        );
        assert!(resolve_recorded_principal(&store, "admin", Some("administrator")).is_none());
        assert!(resolve_recorded_principal(&store, "admin", None).is_none());
        assert!(
            resolve_recorded_principal(&store, "agt_ghost", Some("agent"))
                .is_some_and(|p| p.ceiling == CapabilityCeiling::default())
        );

        store
            .append_audit(
                &rec.agent_id,
                "agent",
                "draft.edit",
                "dft_x",
                "ok",
                json!({}),
            )
            .unwrap();
        let editors = draft_editor_principals(&store, "dft_x", "admin");
        assert_eq!(editors.len(), 1);
        assert_eq!(editors[0].id, rec.agent_id);
    }

    #[test]
    fn ceiling_subset_semantics() {
        let small = CapabilityCeiling {
            allowed_actions: BTreeSet::from(["a".into()]),
            ..Default::default()
        };
        let star = CapabilityCeiling {
            allowed_actions: BTreeSet::from(["*".into()]),
            ..Default::default()
        };
        assert!(small.is_subset_of(&star));
        assert!(!star.is_subset_of(&small));
        assert!(CapabilityCeiling::default().is_subset_of(&small));
        assert!(star.is_subset_of(&CapabilityCeiling::administrator()));
        assert!(!CapabilityCeiling::administrator().is_subset_of(&star));
    }
}
