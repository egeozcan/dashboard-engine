//! Scoped State manager (Dashboard, Instance, Client) with CAS concurrency and Signal delivery (§8).
//!
//! Every persisted record carries owner, scope and schema version. Size limits apply to every
//! write ([`dash_store::DEFAULT_STATE_MAX_BYTES`] unless a declaration says otherwise).
//!
//! **Dashboard State declarations** live on the Dashboard document (`state_schema`): each key has
//! a schema version, an optional value type, an optional size limit and a migration policy. Keys
//! that appear only in `initial_state` are implicitly declared at schema version 1 with any type.
//! When a Generation is activated, records whose stored schema version differs from the declared
//! one are migrated according to the policy, in one SQLite transaction; any failure prevents
//! activation and leaves live State untouched.
//!
//! **Access** (deny by default): reading or subscribing to a Dashboard's State requires the
//! Principal to be allowed to read every Adapter placed on it (the same check used to render it).
//! Writing Dashboard State through the HTTP/WebSocket API additionally requires the
//! Administrator (or an unrestricted ceiling); Agents change shared configuration through drafts.
//!
//! **Concurrency / merge rule** (§19.4 is open; this is the documented default): writes may carry
//! `expected_version` (CAS; `0` = only if absent). A mismatching version is rejected with a
//! conflict and nothing is merged; the writer must re-read and decide. Writes without
//! `expected_version` are last-writer-wins.

use crate::authority::{check_provider_access, Principal, PrincipalKind};
use crate::model::{ConfiguredGraph, DashboardDoc};
use crate::signal::{SignalHub, StateSignalPayload};
use dash_store::{SqliteStore, StateMigrationStep, StateRecord, StoreError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// JSON value type accepted by a declared State key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StateValueType {
    #[default]
    Any,
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
}

impl StateValueType {
    pub fn matches(&self, v: &serde_json::Value) -> bool {
        match self {
            Self::Any => true,
            Self::String => v.is_string(),
            Self::Number => v.is_number(),
            Self::Integer => v.is_i64() || v.is_u64(),
            Self::Boolean => v.is_boolean(),
            Self::Object => v.is_object(),
            Self::Array => v.is_array(),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::String => "string",
            Self::Number => "number",
            Self::Integer => "integer",
            Self::Boolean => "boolean",
            Self::Object => "object",
            Self::Array => "array",
        }
    }
}

/// How records stored at a different schema version are migrated on activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StateMigrationPolicy {
    /// Keep the stored value if it validates against the new declaration; fail otherwise.
    #[default]
    Keep,
    /// Replace the stored value with the Dashboard's `initial_state` default (or remove it).
    Reset,
    /// Refuse activation while any record is stored at another schema version.
    Reject,
}

impl StateMigrationPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Keep => "keep",
            Self::Reset => "reset",
            Self::Reject => "reject",
        }
    }
}

fn default_schema_version() -> i64 {
    1
}

/// Declaration of one State key (§8: schema version, size limit, migration handler).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateKeyDeclaration {
    #[serde(default = "default_schema_version")]
    pub schema_version: i64,
    #[serde(default, rename = "type")]
    pub value_type: StateValueType,
    /// Whether `null` is accepted in addition to `type`.
    #[serde(default)]
    pub nullable: bool,
    /// Encoded-JSON size limit in bytes (default [`dash_store::DEFAULT_STATE_MAX_BYTES`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
    #[serde(default)]
    pub migration: StateMigrationPolicy,
}

impl Default for StateKeyDeclaration {
    fn default() -> Self {
        Self {
            schema_version: 1,
            value_type: StateValueType::Any,
            nullable: false,
            max_bytes: None,
            migration: StateMigrationPolicy::Keep,
        }
    }
}

impl StateKeyDeclaration {
    pub fn effective_max_bytes(&self) -> usize {
        self.max_bytes
            .unwrap_or(dash_store::DEFAULT_STATE_MAX_BYTES)
            .min(dash_store::HARD_STATE_MAX_BYTES)
    }

    /// Validate a value against this declaration (type and size).
    pub fn validate(&self, key: &str, value: &serde_json::Value) -> Result<(), StateError> {
        let type_ok = if value.is_null() {
            self.nullable || self.value_type == StateValueType::Any
        } else {
            self.value_type.matches(value)
        };
        if !type_ok {
            return Err(StateError::Invalid(format!(
                "state key '{key}' expects type '{}'{}",
                self.value_type.as_str(),
                if self.nullable { " or null" } else { "" }
            )));
        }
        let size = serde_json::to_string(value)
            .map(|s| s.len())
            .unwrap_or(usize::MAX);
        let limit = self.effective_max_bytes();
        if size > limit {
            return Err(StateError::TooLarge {
                key: key.to_string(),
                size,
                limit,
            });
        }
        Ok(())
    }
}

impl StateKeyDeclaration {
    /// Declaration of an Instance State key from a Definition manifest (`state = { ... }`).
    pub fn from_spec(spec: &dash_luau::manifest::StateKeySpec) -> Self {
        let value_type = match spec.value_type.as_str() {
            "string" => StateValueType::String,
            "number" => StateValueType::Number,
            "integer" => StateValueType::Integer,
            "boolean" => StateValueType::Boolean,
            "object" => StateValueType::Object,
            "array" => StateValueType::Array,
            _ => StateValueType::Any,
        };
        let migration = match spec.migration.as_str() {
            "reset" => StateMigrationPolicy::Reset,
            "reject" => StateMigrationPolicy::Reject,
            _ => StateMigrationPolicy::Keep,
        };
        Self {
            schema_version: spec.schema_version,
            value_type,
            nullable: spec.nullable,
            max_bytes: spec.max_bytes,
            migration,
        }
    }
}

/// Declaration of an Instance State key of `instance_id` in `graph` (§8).
///
/// A Definition that declares Instance State (`state = { ... }`) is held to it: undeclared keys
/// are rejected. A Definition that declares none keeps implicit keys (schema v1, any type,
/// default size limit), returned as `Ok(None)`.
pub fn instance_state_declaration(
    graph: &ConfiguredGraph,
    instance_id: &str,
    key: &str,
) -> Result<Option<StateKeyDeclaration>, StateError> {
    let Some(def) = graph
        .instances
        .get(instance_id)
        .and_then(|inst| graph.definitions.get(&inst.definition))
    else {
        return Ok(None);
    };
    let declared = &def.manifest.instance_state;
    if declared.is_empty() {
        return Ok(None);
    }
    declared
        .get(key)
        .map(|spec| Some(StateKeyDeclaration::from_spec(spec)))
        .ok_or_else(|| {
            StateError::Invalid(format!(
                "Instance State key '{key}' is not declared by definition '{}'",
                def.manifest.id
            ))
        })
}

/// Declaration of a Dashboard State key: explicit `state_schema` entry, or implicit (schema v1,
/// any type) for keys that only have an `initial_state` default. `None` = undeclared.
pub fn dashboard_state_declaration(doc: &DashboardDoc, key: &str) -> Option<StateKeyDeclaration> {
    if let Some(d) = doc.state_schema.get(key) {
        return Some(d.clone());
    }
    if doc.initial_state.contains_key(key) {
        return Some(StateKeyDeclaration::default());
    }
    None
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("dashboard '{0}' not found")]
    DashboardNotFound(String),
    #[error("state access denied: {0}")]
    Forbidden(String),
    #[error("invalid state write: {0}")]
    Invalid(String),
    #[error("state value for '{key}' is {size} bytes; limit is {limit} bytes")]
    TooLarge {
        key: String,
        size: usize,
        limit: usize,
    },
    #[error("state migration failed: {0}")]
    Migration(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl StateError {
    /// Stable machine-readable code for API/WS error payloads.
    pub fn code(&self) -> &'static str {
        match self {
            Self::DashboardNotFound(_) => "not_found",
            Self::Forbidden(_) => "forbidden",
            Self::Invalid(_) => "invalid",
            Self::TooLarge { .. } | Self::Store(StoreError::StateTooLarge { .. }) => "too_large",
            Self::Store(StoreError::CasConflict { .. }) => "conflict",
            Self::Migration(_) => "migration_failed",
            Self::Store(_) => "store_error",
        }
    }

    /// HTTP status code that corresponds to this error.
    pub fn http_status(&self) -> u16 {
        match self.code() {
            "not_found" => 404,
            "forbidden" => 403,
            "invalid" => 422,
            "too_large" => 413,
            "conflict" => 409,
            _ => 500,
        }
    }

    pub fn is_conflict(&self) -> bool {
        self.code() == "conflict"
    }
}

/// One planned migration of a persisted State record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlannedStateMigration {
    pub scope: String,
    pub owner: String,
    pub state_key: String,
    pub from_schema_version: i64,
    pub to_schema_version: i64,
    /// `keep` | `reset` | `remove`
    pub action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct StateMigrationReport {
    pub migrations: Vec<PlannedStateMigration>,
    /// Number of records written/removed (0 for a check-only plan).
    pub applied: usize,
    /// Number of `initial_state` defaults inserted.
    pub seeded: usize,
}

/// Check that `principal` may read `dashboard_id`'s State (it may read every placed Adapter).
pub fn authorize_dashboard_read<'g>(
    principal: &Principal,
    graph: &'g ConfiguredGraph,
    dashboard_id: &str,
) -> Result<&'g DashboardDoc, StateError> {
    let doc = graph
        .dashboards
        .get(dashboard_id)
        .ok_or_else(|| StateError::DashboardNotFound(dashboard_id.to_string()))?;
    for placement in &doc.adapters {
        let inst = graph
            .instances
            .get(&placement.adapter_instance)
            .ok_or_else(|| {
                StateError::Forbidden(format!(
                    "dashboard '{dashboard_id}' places unknown adapter instance '{}'",
                    placement.adapter_instance
                ))
            })?;
        check_provider_access(principal, inst).map_err(|e| StateError::Forbidden(e.to_string()))?;
    }
    Ok(doc)
}

/// Check that `principal` may write `dashboard_id`'s State through the API.
pub fn authorize_dashboard_state_write<'g>(
    principal: &Principal,
    graph: &'g ConfiguredGraph,
    dashboard_id: &str,
) -> Result<&'g DashboardDoc, StateError> {
    let doc = authorize_dashboard_read(principal, graph, dashboard_id)?;
    if principal.kind != PrincipalKind::Administrator && !principal.ceiling.unrestricted {
        return Err(StateError::Forbidden(format!(
            "{} '{}' may not write Dashboard State",
            principal.kind.as_str(),
            principal.id
        )));
    }
    Ok(doc)
}

#[derive(Clone)]
pub struct StateManager {
    store: SqliteStore,
    signals: SignalHub,
}

impl StateManager {
    pub fn new(store: SqliteStore, signals: SignalHub) -> Self {
        Self { store, signals }
    }

    pub fn get(
        &self,
        scope: &str,
        owner: &str,
        key: &str,
    ) -> Result<Option<StateRecord>, StoreError> {
        self.store.get_state(scope, owner, key)
    }

    pub fn list_dashboard_state(&self, dashboard_id: &str) -> Result<Vec<StateRecord>, StoreError> {
        self.store.list_state_for_owner("dashboard", dashboard_id)
    }

    fn publish(&self, rec: &StateRecord) {
        self.signals.publish_state(StateSignalPayload {
            scope: rec.scope.clone(),
            owner: rec.owner.clone(),
            state_key: rec.state_key.clone(),
            version: rec.version,
            value: rec.value.clone(),
            updated_at: rec.updated_at.clone(),
        });
    }

    /// Update state with optional optimistic CAS version check and publish a State Signal.
    /// Values above [`dash_store::DEFAULT_STATE_MAX_BYTES`] are rejected.
    #[allow(clippy::too_many_arguments)]
    pub fn set(
        &self,
        scope: &str,
        owner: &str,
        key: &str,
        schema_version: i64,
        generation_id: i64,
        value: serde_json::Value,
        expected_version: Option<i64>,
    ) -> Result<StateRecord, StoreError> {
        let rec = self.store.set_state(
            scope,
            owner,
            key,
            schema_version,
            generation_id,
            value,
            expected_version,
        )?;
        self.publish(&rec);
        Ok(rec)
    }

    /// Write a value under a declaration: validates type and size and stamps the declared
    /// schema version.
    #[allow(clippy::too_many_arguments)]
    pub fn set_declared(
        &self,
        scope: &str,
        owner: &str,
        key: &str,
        decl: &StateKeyDeclaration,
        generation_id: i64,
        value: serde_json::Value,
        expected_version: Option<i64>,
    ) -> Result<StateRecord, StateError> {
        decl.validate(key, &value)?;
        let rec = self.store.set_state_with_limit(
            scope,
            owner,
            key,
            decl.schema_version,
            generation_id,
            value,
            expected_version,
            decl.effective_max_bytes(),
        )?;
        self.publish(&rec);
        Ok(rec)
    }

    /// Mediated Instance State write (§8): stamps the declared schema version and enforces the
    /// declared type and size limit of the owning Instance's Definition; undeclared keys of a
    /// Definition that declares Instance State are rejected.
    pub fn set_instance_state(
        &self,
        graph: &ConfiguredGraph,
        instance_id: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<StateRecord, StateError> {
        let decl = instance_state_declaration(graph, instance_id, key)?.unwrap_or_default();
        self.set_declared(
            "instance",
            instance_id,
            key,
            &decl,
            graph.generation_id,
            value,
            None,
        )
    }

    /// Plan the Instance State migrations a Generation requires (§8, §12.2): for every stored
    /// Instance State record whose Instance's Definition declares the key at another schema
    /// version, apply the declared policy (`keep` validates, `reset` writes the declared default
    /// or removes the record, `reject` refuses activation). Declared defaults are validated too.
    /// Records of removed Instances or undeclared keys are left untouched.
    pub fn plan_instance_state_migrations(
        &self,
        graph: &ConfiguredGraph,
        generation_id: i64,
    ) -> Result<(Vec<StateMigrationStep>, Vec<PlannedStateMigration>), StateError> {
        let mut errors = Vec::new();
        for def in graph.definitions.values() {
            for (k, spec) in &def.manifest.instance_state {
                if let Some(d) = &spec.default {
                    if let Err(e) = StateKeyDeclaration::from_spec(spec).validate(k, d) {
                        errors.push(format!(
                            "definition '{}' Instance State default: {e}",
                            def.manifest.id
                        ));
                    }
                }
            }
        }
        let mut steps = Vec::new();
        let mut planned = Vec::new();
        for rec in self.store.list_state_for_scope("instance")? {
            let Some(spec) = graph
                .instances
                .get(&rec.owner)
                .and_then(|inst| graph.definitions.get(&inst.definition))
                .and_then(|def| def.manifest.instance_state.get(&rec.state_key))
            else {
                continue;
            };
            let decl = StateKeyDeclaration::from_spec(spec);
            if rec.schema_version == decl.schema_version {
                continue;
            }
            let new_value = match decl.migration {
                StateMigrationPolicy::Keep => match decl.validate(&rec.state_key, &rec.value) {
                    Ok(()) => Some(rec.value.clone()),
                    Err(e) => {
                        errors.push(format!(
                            "instance '{}' key '{}' (schema v{} -> v{}, policy keep): {e}",
                            rec.owner, rec.state_key, rec.schema_version, decl.schema_version
                        ));
                        continue;
                    }
                },
                StateMigrationPolicy::Reset => spec.default.clone(),
                StateMigrationPolicy::Reject => {
                    errors.push(format!(
                        "instance '{}' key '{}' is stored at schema v{} but declared v{} with policy 'reject'",
                        rec.owner, rec.state_key, rec.schema_version, decl.schema_version
                    ));
                    continue;
                }
            };
            let action = match (&decl.migration, &new_value) {
                (StateMigrationPolicy::Keep, _) => "keep",
                (_, Some(_)) => "reset",
                (_, None) => "remove",
            };
            planned.push(PlannedStateMigration {
                scope: rec.scope.clone(),
                owner: rec.owner.clone(),
                state_key: rec.state_key.clone(),
                from_schema_version: rec.schema_version,
                to_schema_version: decl.schema_version,
                action: action.into(),
            });
            steps.push(StateMigrationStep {
                scope: rec.scope,
                owner: rec.owner,
                state_key: rec.state_key,
                expected_version: rec.version,
                new_schema_version: decl.schema_version,
                generation_id,
                new_value,
            });
        }
        if !errors.is_empty() {
            return Err(StateError::Migration(errors.join("; ")));
        }
        Ok((steps, planned))
    }

    /// Check-only plan of every State migration (Dashboard and Instance State) a candidate
    /// Generation requires; no writes. Run before committing a draft (§8, §12.2).
    pub fn check_graph_state(
        &self,
        graph: &ConfiguredGraph,
    ) -> Result<StateMigrationReport, StateError> {
        let (_, mut migrations) = self.plan_dashboard_state_migrations(&graph.dashboards, 0)?;
        let (_, inst) = self.plan_instance_state_migrations(graph, 0)?;
        migrations.extend(inst);
        Ok(StateMigrationReport {
            migrations,
            applied: 0,
            seeded: 0,
        })
    }

    /// Migrate Dashboard and Instance State for a new Generation in one transaction
    /// (all-or-nothing), then seed missing Dashboard `initial_state` defaults (§8, §12.2).
    pub fn apply_graph_state(
        &self,
        graph: &ConfiguredGraph,
        generation_id: i64,
        principal: &Principal,
    ) -> Result<StateMigrationReport, StateError> {
        self.apply_state_for(&graph.dashboards, Some(graph), generation_id, principal)
    }

    /// Mediated Dashboard State write used by the HTTP and WebSocket APIs: checks that the
    /// dashboard exists, the Principal's authority, that the key is declared and the value
    /// valid, then performs a (optionally CAS) write. Denials are audited.
    #[allow(clippy::too_many_arguments)]
    pub fn set_dashboard_state(
        &self,
        graph: &ConfiguredGraph,
        principal: &Principal,
        dashboard_id: &str,
        key: &str,
        value: serde_json::Value,
        expected_version: Option<i64>,
    ) -> Result<StateRecord, StateError> {
        let doc = match authorize_dashboard_state_write(principal, graph, dashboard_id) {
            Ok(doc) => doc,
            Err(e) => {
                if matches!(e, StateError::Forbidden(_)) {
                    let _ = self.store.append_audit(
                        &principal.id,
                        principal.kind.as_str(),
                        "dashboard_state.set",
                        &format!("{dashboard_id}/{key}"),
                        "denied",
                        serde_json::json!({ "reason": e.to_string() }),
                    );
                }
                return Err(e);
            }
        };
        let decl = dashboard_state_declaration(doc, key).ok_or_else(|| {
            StateError::Invalid(format!(
                "state key '{key}' is not declared by dashboard '{dashboard_id}'"
            ))
        })?;
        self.set_declared(
            "dashboard",
            dashboard_id,
            key,
            &decl,
            graph.generation_id,
            value,
            expected_version,
        )
    }

    /// Dashboard State records (with versions, for CAS) visible to `principal`.
    pub fn list_dashboard_state_for(
        &self,
        graph: &ConfiguredGraph,
        principal: &Principal,
        dashboard_id: &str,
    ) -> Result<Vec<StateRecord>, StateError> {
        authorize_dashboard_read(principal, graph, dashboard_id)?;
        Ok(self.list_dashboard_state(dashboard_id)?)
    }

    /// Seed initial Dashboard state keys from Workspace DashboardDoc if absent.
    ///
    /// Uses an atomic insert-if-absent, so a concurrent Client write is never overwritten, and
    /// only publishes a State Signal for records it actually inserted.
    pub fn seed_dashboard_defaults(
        &self,
        dashboard: &DashboardDoc,
        generation_id: i64,
    ) -> Result<(), StoreError> {
        self.seed_dashboard_defaults_counted(dashboard, generation_id)
            .map(|_| ())
    }

    fn seed_dashboard_defaults_counted(
        &self,
        dashboard: &DashboardDoc,
        generation_id: i64,
    ) -> Result<usize, StoreError> {
        let mut seeded = 0;
        for (k, v) in &dashboard.initial_state {
            let schema_version = dashboard_state_declaration(dashboard, k)
                .map(|d| d.schema_version)
                .unwrap_or(1);
            if let Some(rec) = self.store.insert_state_if_absent(
                "dashboard",
                &dashboard.id,
                k,
                schema_version,
                generation_id,
                v.clone(),
            )? {
                self.publish(&rec);
                seeded += 1;
            }
        }
        Ok(seeded)
    }

    /// Plan the Dashboard State migrations a Generation with `dashboards` requires.
    ///
    /// Also validates every declared `initial_state` default against its declaration. Records of
    /// dashboards or keys that are no longer declared are left untouched.
    pub fn plan_dashboard_state_migrations(
        &self,
        dashboards: &BTreeMap<String, DashboardDoc>,
        generation_id: i64,
    ) -> Result<(Vec<StateMigrationStep>, Vec<PlannedStateMigration>), StateError> {
        let mut errors = Vec::new();
        for doc in dashboards.values() {
            for (k, v) in &doc.initial_state {
                if let Some(decl) = dashboard_state_declaration(doc, k) {
                    if let Err(e) = decl.validate(k, v) {
                        errors.push(format!("dashboard '{}' initial_state: {e}", doc.id));
                    }
                }
            }
        }

        let mut steps = Vec::new();
        let mut planned = Vec::new();
        for rec in self.store.list_state_for_scope("dashboard")? {
            let Some(doc) = dashboards.get(&rec.owner) else {
                continue;
            };
            let Some(decl) = dashboard_state_declaration(doc, &rec.state_key) else {
                continue;
            };
            if rec.schema_version == decl.schema_version {
                continue;
            }
            let new_value = match decl.migration {
                StateMigrationPolicy::Keep => match decl.validate(&rec.state_key, &rec.value) {
                    Ok(()) => Some(rec.value.clone()),
                    Err(e) => {
                        errors.push(format!(
                            "dashboard '{}' key '{}' (schema v{} -> v{}, policy keep): {e}",
                            rec.owner, rec.state_key, rec.schema_version, decl.schema_version
                        ));
                        continue;
                    }
                },
                StateMigrationPolicy::Reset => doc.initial_state.get(&rec.state_key).cloned(),
                StateMigrationPolicy::Reject => {
                    errors.push(format!(
                        "dashboard '{}' key '{}' is stored at schema v{} but declared v{} with policy 'reject'",
                        rec.owner, rec.state_key, rec.schema_version, decl.schema_version
                    ));
                    continue;
                }
            };
            let action = match (&decl.migration, &new_value) {
                (StateMigrationPolicy::Keep, _) => "keep",
                (_, Some(_)) => "reset",
                (_, None) => "remove",
            };
            planned.push(PlannedStateMigration {
                scope: rec.scope.clone(),
                owner: rec.owner.clone(),
                state_key: rec.state_key.clone(),
                from_schema_version: rec.schema_version,
                to_schema_version: decl.schema_version,
                action: action.into(),
            });
            steps.push(StateMigrationStep {
                scope: rec.scope,
                owner: rec.owner,
                state_key: rec.state_key,
                expected_version: rec.version,
                new_schema_version: decl.schema_version,
                generation_id,
                new_value,
            });
        }
        if !errors.is_empty() {
            return Err(StateError::Migration(errors.join("; ")));
        }
        Ok((steps, planned))
    }

    /// Check-only plan (no writes): run before committing a draft so an impossible migration
    /// refuses activation with live State untouched.
    pub fn check_generation_state(
        &self,
        dashboards: &BTreeMap<String, DashboardDoc>,
    ) -> Result<StateMigrationReport, StateError> {
        let (_, migrations) = self.plan_dashboard_state_migrations(dashboards, 0)?;
        Ok(StateMigrationReport {
            migrations,
            applied: 0,
            seeded: 0,
        })
    }

    /// Migrate Dashboard State for a new Generation (one transaction, all-or-nothing), then seed
    /// missing `initial_state` defaults. Migrations and failures are audited.
    pub fn apply_generation_state(
        &self,
        dashboards: &BTreeMap<String, DashboardDoc>,
        generation_id: i64,
        principal: &Principal,
    ) -> Result<StateMigrationReport, StateError> {
        self.apply_state_for(dashboards, None, generation_id, principal)
    }

    fn apply_state_for(
        &self,
        dashboards: &BTreeMap<String, DashboardDoc>,
        graph: Option<&ConfiguredGraph>,
        generation_id: i64,
        principal: &Principal,
    ) -> Result<StateMigrationReport, StateError> {
        let audit = |outcome: &str, detail: serde_json::Value| {
            let _ = self.store.append_audit(
                &principal.id,
                principal.kind.as_str(),
                "state.migrate",
                &format!("generation:{generation_id}"),
                outcome,
                detail,
            );
        };
        let planned = self
            .plan_dashboard_state_migrations(dashboards, generation_id)
            .and_then(|(mut steps, mut migrations)| {
                if let Some(g) = graph {
                    let (s, m) = self.plan_instance_state_migrations(g, generation_id)?;
                    steps.extend(s);
                    migrations.extend(m);
                }
                Ok((steps, migrations))
            });
        let (steps, migrations) = match planned {
            Ok(p) => p,
            Err(e) => {
                audit("failed", serde_json::json!({ "error": e.to_string() }));
                return Err(e);
            }
        };
        let mut applied = 0;
        if !steps.is_empty() {
            let results = match self.store.apply_state_migrations(&steps) {
                Ok(r) => r,
                Err(e) => {
                    let err = StateError::Migration(e.to_string());
                    audit("failed", serde_json::json!({ "error": err.to_string() }));
                    return Err(err);
                }
            };
            applied = results.len();
            for (step, rec) in steps.iter().zip(results) {
                match rec {
                    Some(rec) => self.publish(&rec),
                    None => self.signals.publish_state(StateSignalPayload {
                        scope: step.scope.clone(),
                        owner: step.owner.clone(),
                        state_key: step.state_key.clone(),
                        version: 0,
                        value: serde_json::Value::Null,
                        updated_at: chrono::Utc::now().to_rfc3339(),
                    }),
                }
            }
            audit(
                "succeeded",
                serde_json::to_value(&migrations).unwrap_or(serde_json::Value::Null),
            );
        }
        let mut seeded = 0;
        for doc in dashboards.values() {
            seeded += self.seed_dashboard_defaults_counted(doc, generation_id)?;
        }
        Ok(StateMigrationReport {
            migrations,
            applied,
            seeded,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CapabilityCeiling, DashboardAdapterPlacement, InstanceConfig};
    use dash_luau::DefinitionCategory;
    use serde_json::json;

    fn manager() -> (StateManager, SignalHub) {
        let store = SqliteStore::open_in_memory().unwrap();
        let hub = SignalHub::new(64);
        (StateManager::new(store, hub.clone()), hub)
    }

    fn dashboard(id: &str) -> DashboardDoc {
        DashboardDoc {
            id: id.into(),
            title: id.into(),
            layout: "grid".into(),
            adapters: vec![DashboardAdapterPlacement {
                adapter_instance: "table_adapter".into(),
                area: "main".into(),
                query_params: json!({}),
            }],
            initial_state: BTreeMap::from([("repo".to_string(), json!("acme/a"))]),
            overrides: BTreeMap::new(),
            state_schema: BTreeMap::new(),
        }
    }

    fn graph_with(doc: DashboardDoc) -> ConfiguredGraph {
        let mut g = ConfiguredGraph {
            generation_id: 3,
            ..Default::default()
        };
        let inst: InstanceConfig = serde_json::from_value(json!({
            "id": "table_adapter",
            "category": DefinitionCategory::Adapter,
            "definition": "x.table",
        }))
        .unwrap();
        g.instances.insert(inst.id.clone(), inst);
        g.dashboards.insert(doc.id.clone(), doc);
        g
    }

    fn agent(providers: &[&str]) -> Principal {
        Principal {
            id: "agt".into(),
            kind: PrincipalKind::Agent,
            ceiling: CapabilityCeiling {
                allowed_providers: providers.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            },
        }
    }

    #[test]
    fn dashboard_state_writes_are_mediated() {
        let (sm, _) = manager();
        let mut doc = dashboard("eng");
        doc.state_schema.insert(
            "repo".into(),
            StateKeyDeclaration {
                schema_version: 2,
                value_type: StateValueType::String,
                max_bytes: Some(32),
                ..Default::default()
            },
        );
        let g = graph_with(doc);
        let admin = Principal::administrator();

        let rec = sm
            .set_dashboard_state(&g, &admin, "eng", "repo", json!("acme/b"), Some(0))
            .unwrap();
        assert_eq!(
            (rec.version, rec.schema_version, rec.generation_id),
            (1, 2, 3)
        );

        let e = sm
            .set_dashboard_state(&g, &admin, "missing", "repo", json!("x"), None)
            .unwrap_err();
        assert_eq!(e.http_status(), 404);
        let e = sm
            .set_dashboard_state(&g, &admin, "eng", "undeclared", json!("x"), None)
            .unwrap_err();
        assert_eq!(e.http_status(), 422);
        let e = sm
            .set_dashboard_state(&g, &admin, "eng", "repo", json!(5), None)
            .unwrap_err();
        assert_eq!(e.http_status(), 422);
        let e = sm
            .set_dashboard_state(&g, &admin, "eng", "repo", json!("x".repeat(64)), None)
            .unwrap_err();
        assert_eq!(e.http_status(), 413);
        // CAS: stale expected version is a conflict.
        let e = sm
            .set_dashboard_state(&g, &admin, "eng", "repo", json!("acme/c"), Some(0))
            .unwrap_err();
        assert!(e.is_conflict());
        assert_eq!(e.http_status(), 409);

        // Agents may read (with provider access) but never write Dashboard State via the API.
        let reader = agent(&["*"]);
        assert!(sm.list_dashboard_state_for(&g, &reader, "eng").is_ok());
        let e = sm
            .set_dashboard_state(&g, &reader, "eng", "repo", json!("acme/z"), None)
            .unwrap_err();
        assert_eq!(e.http_status(), 403);
        let blind = agent(&[]);
        assert_eq!(
            sm.list_dashboard_state_for(&g, &blind, "eng")
                .unwrap_err()
                .http_status(),
            403
        );
        assert_eq!(
            sm.get("dashboard", "eng", "repo").unwrap().unwrap().value,
            json!("acme/b")
        );
    }

    #[test]
    fn seeding_never_overwrites_and_only_signals_inserts() {
        let (sm, hub) = manager();
        let doc = dashboard("eng");
        let mut rx = hub.subscribe_state();
        sm.seed_dashboard_defaults(&doc, 1).unwrap();
        assert_eq!(rx.try_recv().unwrap().value, json!("acme/a"));
        sm.set(
            "dashboard",
            "eng",
            "repo",
            1,
            1,
            json!("acme/user"),
            Some(1),
        )
        .unwrap();
        let _ = rx.try_recv();
        sm.seed_dashboard_defaults(&doc, 1).unwrap();
        assert!(rx.try_recv().is_err());
        assert_eq!(
            sm.get("dashboard", "eng", "repo").unwrap().unwrap().value,
            json!("acme/user")
        );
    }

    #[test]
    fn generation_migrations_keep_reset_and_reject() {
        let (sm, _) = manager();
        let admin = Principal::administrator();
        let mut doc = dashboard("eng");
        doc.initial_state
            .insert("filters".into(), json!({"open": true}));
        sm.seed_dashboard_defaults(&doc, 1).unwrap();
        sm.set(
            "dashboard",
            "eng",
            "filters",
            1,
            1,
            json!({"open": false}),
            None,
        )
        .unwrap();

        // v2: `repo` keeps its value (still a string); `filters` resets to the default.
        let mut v2 = doc.clone();
        v2.state_schema.insert(
            "repo".into(),
            StateKeyDeclaration {
                schema_version: 2,
                value_type: StateValueType::String,
                ..Default::default()
            },
        );
        v2.state_schema.insert(
            "filters".into(),
            StateKeyDeclaration {
                schema_version: 2,
                migration: StateMigrationPolicy::Reset,
                ..Default::default()
            },
        );
        let dashboards = BTreeMap::from([("eng".to_string(), v2.clone())]);
        let check = sm.check_generation_state(&dashboards).unwrap();
        assert_eq!(check.migrations.len(), 2);
        assert_eq!(check.applied, 0);
        assert_eq!(
            sm.get("dashboard", "eng", "filters")
                .unwrap()
                .unwrap()
                .schema_version,
            1
        );

        let report = sm.apply_generation_state(&dashboards, 2, &admin).unwrap();
        assert_eq!(report.applied, 2);
        let filters = sm.get("dashboard", "eng", "filters").unwrap().unwrap();
        assert_eq!(
            (filters.value, filters.schema_version),
            (json!({"open": true}), 2)
        );
        let repo = sm.get("dashboard", "eng", "repo").unwrap().unwrap();
        assert_eq!((repo.value, repo.schema_version), (json!("acme/a"), 2));
        // Re-applying the same Generation is a no-op.
        assert_eq!(
            sm.apply_generation_state(&dashboards, 2, &admin)
                .unwrap()
                .applied,
            0
        );

        // v3: `repo` becomes an integer with policy keep -> migration fails; `filters` rejects.
        // Nothing is written (live State preserved).
        let mut v3 = v2.clone();
        v3.initial_state.insert("repo".into(), json!(1));
        v3.state_schema.get_mut("repo").unwrap().schema_version = 3;
        v3.state_schema.get_mut("repo").unwrap().value_type = StateValueType::Integer;
        let bad = BTreeMap::from([("eng".to_string(), v3)]);
        let err = sm.apply_generation_state(&bad, 3, &admin).unwrap_err();
        assert!(matches!(err, StateError::Migration(_)));
        assert!(err.to_string().contains("repo"));
        let repo = sm.get("dashboard", "eng", "repo").unwrap().unwrap();
        assert_eq!((repo.value, repo.schema_version), (json!("acme/a"), 2));

        let mut v4 = v2;
        v4.state_schema.get_mut("filters").unwrap().schema_version = 4;
        v4.state_schema.get_mut("filters").unwrap().migration = StateMigrationPolicy::Reject;
        let rejected = BTreeMap::from([("eng".to_string(), v4)]);
        assert!(sm.check_generation_state(&rejected).is_err());
    }

    #[test]
    fn declaration_round_trips_through_yaml() {
        let doc: DashboardDoc = serde_yaml_ng::from_str(
            "id: d\ntitle: D\nstate_schema:\n  repo:\n    schema_version: 2\n    type: string\n    max_bytes: 128\n    migration: reset\n",
        )
        .unwrap();
        let decl = &doc.state_schema["repo"];
        assert_eq!(decl.value_type, StateValueType::String);
        assert_eq!(decl.migration, StateMigrationPolicy::Reset);
        assert_eq!(decl.effective_max_bytes(), 128);
        // Dashboards without declarations serialize without the field (no config drift).
        let plain: DashboardDoc = serde_yaml_ng::from_str("id: d\ntitle: D\n").unwrap();
        assert!(!serde_json::to_string(&plain)
            .unwrap()
            .contains("state_schema"));
    }

    fn instance_graph(state_decl: &str) -> ConfiguredGraph {
        let decl = format!(
            "local dash = require(\"dash\")\nreturn dash.data_provider {{ id = \"gh.prs\", state = {{ {state_decl} }} }}"
        );
        let mut sources = BTreeMap::new();
        sources.insert("definition.luau".to_string(), decl.as_bytes().to_vec());
        let manifest = dash_luau::evaluate_declaration(&decl, &sources).unwrap();
        let mut g = ConfiguredGraph::default();
        g.definitions.insert(
            "gh.prs".into(),
            crate::model::LoadedDefinition {
                package_name: "gh".into(),
                manifest,
                definition_source: decl.clone(),
                runtime_source: String::new(),
                tests_source: None,
                fixtures: BTreeMap::new(),
                widget_assets: BTreeMap::new(),
            },
        );
        let inst: InstanceConfig = serde_json::from_value(json!({
            "id": "prs",
            "category": DefinitionCategory::DataProvider,
            "definition": "gh.prs",
        }))
        .unwrap();
        g.instances.insert(inst.id.clone(), inst);
        g
    }

    // SC-7: Instance State declarations drive migrations on activation, like Dashboard State.
    #[test]
    fn instance_state_migrations_reset_keep_and_reject() {
        let (sm, _) = manager();
        let admin = Principal::administrator();
        // Written by an older Definition that declared nothing (implicit schema v1).
        sm.set(
            "instance",
            "prs",
            "cursor",
            1,
            1,
            json!("2026-11-01T00:00Z"),
            None,
        )
        .unwrap();
        sm.set("instance", "prs", "seen", 1, 1, json!(["a"]), None)
            .unwrap();

        // v2 changes the cursor shape (reset to the declared default) and keeps `seen`.
        let v2 = instance_graph(
            r#"cursor = { schema_version = 2, type = "object", migration = "reset", default = { page = 1 } },
               seen = { schema_version = 2, type = "array" }"#,
        );
        let check = sm.check_graph_state(&v2).unwrap();
        assert_eq!(check.migrations.len(), 2);
        assert_eq!(
            sm.get("instance", "prs", "cursor").unwrap().unwrap().value,
            json!("2026-11-01T00:00Z"),
            "check-only plan writes nothing"
        );
        let report = sm.apply_graph_state(&v2, 2, &admin).unwrap();
        assert_eq!(report.applied, 2);
        let cursor = sm.get("instance", "prs", "cursor").unwrap().unwrap();
        assert_eq!(
            (cursor.value, cursor.schema_version),
            (json!({"page": 1}), 2)
        );
        let seen = sm.get("instance", "prs", "seen").unwrap().unwrap();
        assert_eq!((seen.value, seen.schema_version), (json!(["a"]), 2));

        // Writes now follow the declaration: undeclared keys and wrong types are rejected.
        assert!(sm
            .set_instance_state(&v2, "prs", "other", json!(1))
            .is_err());
        assert!(sm
            .set_instance_state(&v2, "prs", "cursor", json!("x"))
            .is_err());
        assert_eq!(
            sm.set_instance_state(&v2, "prs", "cursor", json!({"page": 2}))
                .unwrap()
                .schema_version,
            2
        );

        // v3: `keep` of a value that no longer validates, and `reject`, both refuse activation
        // and leave live State untouched.
        let keep_bad = instance_graph(
            r#"cursor = { schema_version = 3, type = "string" }, seen = { schema_version = 2, type = "array" }"#,
        );
        assert!(matches!(
            sm.apply_graph_state(&keep_bad, 3, &admin),
            Err(StateError::Migration(_))
        ));
        let reject = instance_graph(
            r#"cursor = { schema_version = 3, migration = "reject" }, seen = { schema_version = 3, migration = "reset" }"#,
        );
        let err = sm.check_graph_state(&reject).unwrap_err();
        assert!(err.to_string().contains("reject"), "{err}");
        assert!(sm.apply_graph_state(&reject, 3, &admin).is_err());
        let cursor = sm.get("instance", "prs", "cursor").unwrap().unwrap();
        assert_eq!(
            (cursor.value, cursor.schema_version),
            (json!({"page": 2}), 2)
        );
        assert!(sm.get("instance", "prs", "seen").unwrap().is_some());

        // An invalid declared default is itself an activation error.
        let bad_default = instance_graph(
            r#"cursor = { schema_version = 4, type = "object", migration = "reset", default = "nope" }"#,
        );
        assert!(sm.check_graph_state(&bad_default).is_err());
    }
}
