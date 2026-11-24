//! Opt-in parameterized query history sampling, compaction, and retention (§7.4).
//!
//! Samples are written through the [`HistoryBackend`] storage contract (SQLite by default),
//! which owns retention/compaction and enforces a per-read query budget.

use crate::authority::{Principal, PrincipalKind, ValidationIssue};
use crate::model::{CapabilityCeiling, ConfiguredGraph, HistorySeriesConfig};
use crate::query_engine::QueryEngine;
use crate::signal::QueryStatus;
use chrono::Utc;
use dash_luau::DefinitionCategory;
use dash_store::{HistorySampleRecord, HistorySeriesSpec, SqliteStore, HISTORY_QUERY_BUDGET};
use std::sync::Arc;

/// Pluggable history storage contract (§7.4): series metadata, sample append, retention
/// compaction and budgeted range reads.
pub trait HistoryBackend: Send + Sync {
    fn upsert_series(&self, spec: &HistorySeriesSpec) -> Result<(), String>;
    fn append_sample(
        &self,
        series_id: &str,
        sampled_at: &str,
        value: f64,
        metadata: Option<serde_json::Value>,
    ) -> Result<(), String>;
    /// Drop samples older than `retention_days`; returns how many were removed.
    fn compact(&self, series_id: &str, retention_days: i64) -> Result<usize, String>;
    /// Samples at or after `since` (oldest first), or the most recent `limit` samples when no
    /// `since` is given. `limit` is clamped to [`HistoryBackend::query_budget`].
    fn query(
        &self,
        series_id: &str,
        since: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HistorySampleRecord>, String>;
    /// Maximum samples a single read may return.
    fn query_budget(&self) -> usize;
}

impl HistoryBackend for SqliteStore {
    fn upsert_series(&self, spec: &HistorySeriesSpec) -> Result<(), String> {
        self.upsert_history_series(spec).map_err(|e| e.to_string())
    }
    fn append_sample(
        &self,
        series_id: &str,
        sampled_at: &str,
        value: f64,
        metadata: Option<serde_json::Value>,
    ) -> Result<(), String> {
        self.append_history_sample(series_id, sampled_at, value, metadata)
            .map_err(|e| e.to_string())
    }
    fn compact(&self, series_id: &str, retention_days: i64) -> Result<usize, String> {
        self.compact_history_series(series_id, retention_days)
            .map_err(|e| e.to_string())
    }
    fn query(
        &self,
        series_id: &str,
        since: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HistorySampleRecord>, String> {
        self.query_history_samples(series_id, since, limit.min(HISTORY_QUERY_BUDGET))
            .map_err(|e| e.to_string())
    }
    fn query_budget(&self) -> usize {
        HISTORY_QUERY_BUDGET
    }
}

#[derive(Clone)]
pub struct HistoryManager {
    store: SqliteStore,
    backend: Arc<dyn HistoryBackend>,
    queries: QueryEngine,
}

impl HistoryManager {
    pub fn new(store: SqliteStore, queries: QueryEngine) -> Self {
        let backend: Arc<dyn HistoryBackend> = Arc::new(store.clone());
        Self::with_backend(store, backend, queries)
    }

    /// Use a different history backend; `store` is still used for Service principal grants.
    pub fn with_backend(
        store: SqliteStore,
        backend: Arc<dyn HistoryBackend>,
        queries: QueryEngine,
    ) -> Self {
        Self {
            store,
            backend,
            queries,
        }
    }

    pub fn backend(&self) -> &Arc<dyn HistoryBackend> {
        &self.backend
    }

    /// Ensure configured history series metadata exists in SQLite.
    pub fn sync_series_configs(&self, configs: &[HistorySeriesConfig]) -> Result<(), String> {
        let now = Utc::now().to_rfc3339();
        for cfg in configs {
            self.backend.upsert_series(&HistorySeriesSpec {
                series_id: cfg.series_id.clone(),
                instance_id: cfg.instance_id.clone(),
                params: cfg.params.clone(),
                aggregator: cfg.aggregator.clone(),
                cadence_secs: cfg.cadence_secs,
                retention_days: cfg.retention_days,
                service_principal: cfg.service_principal.clone(),
                created_at: now.clone(),
            })?;
        }
        Ok(())
    }

    /// Sample one history series under its named Service principal's granted authority.
    ///
    /// Only a `fresh` query result is recorded. When the provider fails and the engine falls back
    /// to the last good (stale) value, no sample is written: the series shows a gap instead of a
    /// repeated old value stamped with the current time (§15.2).
    pub async fn sample_series(
        &self,
        cfg: &HistorySeriesConfig,
        sampled_at_override: Option<&str>,
    ) -> Result<f64, String> {
        let grant_val = self
            .store
            .get_service_principal_grant(&cfg.service_principal)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| {
                format!(
                    "history sampling denied: service principal '{}' has no Administrator grant",
                    cfg.service_principal
                )
            })?;
        let ceiling: CapabilityCeiling =
            serde_json::from_value(grant_val).map_err(|e| e.to_string())?;
        let principal = Principal {
            id: cfg.service_principal.clone(),
            kind: PrincipalKind::ServicePrincipal,
            ceiling,
        };

        let env = self
            .queries
            .query(&cfg.instance_id, cfg.params.clone(), &principal, true)
            .await?;
        if env.status != QueryStatus::Fresh {
            return Err(format!(
                "history sample for '{}' skipped: query is {:?} ({})",
                cfg.series_id,
                env.status,
                env.error.as_deref().unwrap_or("no fresh value")
            ));
        }
        let val = env.value.unwrap_or(serde_json::Value::Null);
        let aggregated = aggregate_value(&val, &cfg.aggregator, cfg.value_path.as_deref())?;

        let ts = sampled_at_override
            .map(|s| s.to_string())
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        self.backend.append_sample(&cfg.series_id, &ts, aggregated, None)?;
        self.backend.compact(&cfg.series_id, cfg.retention_days)?;
        Ok(aggregated)
    }

    pub fn query_samples(
        &self,
        series_id: &str,
        since: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HistorySampleRecord>, String> {
        self.backend
            .query(series_id, since, limit.min(self.backend.query_budget()))
    }
}

/// Graph validation for configured history series (§7.4, ADR-0002): the series must name a
/// declared Service principal and a data-provider Instance; a missing grant or a granted ceiling
/// that excludes the Instance is reported as a warning (sampling would be denied).
pub fn validate_history_series(graph: &ConfiguredGraph, store: &SqliteStore) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let mut push = |severity: &str, code: &str, target: &str, message: String| {
        issues.push(ValidationIssue {
            severity: severity.into(),
            code: code.into(),
            target: target.into(),
            message,
        })
    };
    for series in &graph.workspace.history {
        let target = format!("history:{}", series.series_id);
        match graph.instances.get(&series.instance_id) {
            None => push(
                "error",
                "HISTORY_UNKNOWN_INSTANCE",
                &target,
                format!("history series references unknown instance '{}'", series.instance_id),
            ),
            Some(inst) if inst.category != DefinitionCategory::DataProvider => push(
                "error",
                "HISTORY_NOT_DATA_PROVIDER",
                &target,
                format!(
                    "history series instance '{}' is '{}', not a data_provider",
                    series.instance_id,
                    inst.category.as_str()
                ),
            ),
            _ => {}
        }
        if !matches!(
            series.aggregator.as_str(),
            "count" | "sum" | "avg" | "min" | "max" | "latest"
        ) {
            push(
                "error",
                "HISTORY_UNKNOWN_AGGREGATOR",
                &target,
                format!("unknown history aggregator '{}'", series.aggregator),
            );
        }
        let sp = series.service_principal.as_str();
        if sp.trim().is_empty() || !graph.service_principals.contains_key(sp) {
            push(
                "error",
                "HISTORY_UNKNOWN_SERVICE_PRINCIPAL",
                &target,
                format!("history series names undeclared service principal '{sp}'"),
            );
            continue;
        }
        if graph.is_preview {
            continue;
        }
        match store.get_service_principal_grant(sp).ok().flatten() {
            None => push(
                "warning",
                "PENDING_SERVICE_PRINCIPAL_GRANT",
                sp,
                format!(
                    "history series '{}' samples under '{sp}', which awaits an Administrator grant",
                    series.series_id
                ),
            ),
            Some(val) => {
                let allowed = serde_json::from_value::<CapabilityCeiling>(val)
                    .map(|c| c.allows_provider(&series.instance_id))
                    .unwrap_or(false);
                if !allowed {
                    push(
                        "warning",
                        "HISTORY_CEILING_EXCLUDES_INSTANCE",
                        &target,
                        format!(
                            "granted ceiling of '{sp}' does not allow provider '{}'; sampling will be denied",
                            series.instance_id
                        ),
                    );
                }
            }
        }
    }
    issues
}

fn aggregate_value(
    val: &serde_json::Value,
    aggregator: &str,
    value_path: Option<&str>,
) -> Result<f64, String> {
    match aggregator {
        "count" => match val {
            serde_json::Value::Array(arr) => Ok(arr.len() as f64),
            serde_json::Value::Object(obj) => {
                if let Some(p) = value_path {
                    if let Some(arr) = obj.get(p).and_then(|v| v.as_array()) {
                        return Ok(arr.len() as f64);
                    }
                }
                Ok(obj.len() as f64)
            }
            serde_json::Value::Number(n) => Ok(n.as_f64().unwrap_or(0.0)),
            serde_json::Value::Null => {
                Err("count aggregator received no value (null query result)".to_string())
            }
            _ => Ok(1.0),
        },
        "sum" | "avg" | "min" | "max" => {
            let arr = val
                .as_array()
                .ok_or_else(|| "numeric aggregation requires array result".to_string())?;
            if arr.is_empty() {
                return Ok(0.0);
            }
            let nums: Vec<f64> = arr
                .iter()
                .filter_map(|item| {
                    let target = match value_path {
                        Some(p) => item.get(p),
                        None => Some(item),
                    };
                    target.and_then(|v| v.as_f64())
                })
                .collect();
            if nums.is_empty() {
                return Ok(0.0);
            }
            match aggregator {
                "sum" => Ok(nums.iter().sum()),
                "avg" => Ok(nums.iter().sum::<f64>() / nums.len() as f64),
                "min" => Ok(nums.iter().copied().fold(f64::INFINITY, f64::min)),
                "max" => Ok(nums.iter().copied().fold(f64::NEG_INFINITY, f64::max)),
                _ => unreachable!(),
            }
        }
        "latest" => {
            let target = match value_path {
                Some(p) => val.get(p).unwrap_or(val),
                None => val,
            };
            target
                .as_f64()
                .ok_or_else(|| "latest aggregator expects numeric value".to_string())
        }
        other => Err(format!("unknown history aggregator '{other}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn count_of_null_is_an_error_not_zero() {
        assert!(aggregate_value(&serde_json::Value::Null, "count", None).is_err());
        assert_eq!(aggregate_value(&json!([1, 2, 3]), "count", None).unwrap(), 3.0);
    }

    #[test]
    fn sqlite_backend_enforces_query_budget() {
        let store = SqliteStore::open_in_memory().unwrap();
        let backend: &dyn HistoryBackend = &store;
        assert_eq!(backend.query_budget(), HISTORY_QUERY_BUDGET);
        backend
            .upsert_series(&HistorySeriesSpec {
                series_id: "s".into(),
                instance_id: "i".into(),
                params: json!({}),
                aggregator: "count".into(),
                cadence_secs: 60,
                retention_days: 1,
                service_principal: "sp".into(),
                created_at: Utc::now().to_rfc3339(),
            })
            .unwrap();
        let old = (Utc::now() - chrono::Duration::days(3)).to_rfc3339();
        backend.append_sample("s", &old, 1.0, None).unwrap();
        backend.append_sample("s", &Utc::now().to_rfc3339(), 2.0, None).unwrap();
        assert_eq!(backend.compact("s", 1).unwrap(), 1);
        let rows = backend.query("s", None, usize::MAX).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value_num, 2.0);
    }
}
