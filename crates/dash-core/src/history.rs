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
use dash_store::{
    HistoryAggregate, HistorySampleRecord, HistorySeriesSpec, SqliteStore, HISTORY_QUERY_BUDGET,
};
use std::sync::Arc;

/// Query parameters of the built-in `dash.history_series` data provider.
pub const HISTORY_QUERY_PARAMS: &[&str] = &[
    "series_id",
    "params",
    "since",
    "until",
    "window",
    "limit",
    "bucket_secs",
    "aggregate",
];

/// Smallest accepted `bucket_secs` of a downsampled history read.
pub const HISTORY_MIN_BUCKET_SECS: i64 = 1;
/// Largest accepted `bucket_secs` (one year).
pub const HISTORY_MAX_BUCKET_SECS: i64 = 366 * 86_400;

/// Read window, size and downsampling of one `dash.history_series` query (everything but the
/// series selection, which depends on the Instance's configured series).
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryReadOptions {
    /// Inclusive lower bound, normalized to RFC 3339 UTC.
    pub since: Option<String>,
    /// Exclusive upper bound, normalized to RFC 3339 UTC.
    pub until: Option<String>,
    /// Requested sample/bucket count (`None`: the provider default).
    pub limit: Option<usize>,
    /// Downsampling into `bucket_secs`-wide buckets with an aggregate.
    pub bucket: Option<(i64, HistoryAggregate)>,
    /// When more than `limit` samples/buckets fall in the range, keep the newest ones (a
    /// sliding `window`, or no lower bound) instead of the oldest (an explicit `since`).
    pub newest: bool,
}

fn as_count(v: &serde_json::Value) -> Option<u64> {
    v.as_u64().or_else(|| {
        v.as_f64()
            .filter(|f| *f >= 0.0 && f.fract() == 0.0 && *f <= u64::MAX as f64)
            .map(|f| f as u64)
    })
}

/// Seconds of a window/duration: a non-negative number of seconds, or `<n><unit>` with unit
/// `s`, `m`, `h`, `d` or `w` (`"15m"`, `"24h"`, `"30d"`).
pub fn parse_history_duration(v: &serde_json::Value) -> Option<i64> {
    if let Some(n) = as_count(v) {
        return i64::try_from(n).ok();
    }
    let s = v.as_str()?.trim();
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit())?);
    let n: i64 = num.parse().ok()?;
    let mult = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        "w" => 604_800,
        _ => return None,
    };
    n.checked_mul(mult)
}

fn parse_rfc3339(key: &str, v: &serde_json::Value) -> Result<Option<String>, String> {
    match v {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) => chrono::DateTime::parse_from_rfc3339(s)
            .map(|t| Some(t.with_timezone(&Utc).to_rfc3339()))
            .map_err(|e| format!("history_series '{key}' must be an RFC 3339 timestamp: {e}")),
        _ => Err(format!("history_series '{key}' must be an RFC 3339 string")),
    }
}

/// Validate a `dash.history_series` query's parameters (unknown keys rejected) and resolve its
/// read options; `window` is relative to `now`.
pub fn parse_history_read_options(
    params: &serde_json::Map<String, serde_json::Value>,
    now: chrono::DateTime<Utc>,
) -> Result<HistoryReadOptions, String> {
    if let Some(unknown) = params
        .keys()
        .find(|k| !HISTORY_QUERY_PARAMS.contains(&k.as_str()))
    {
        return Err(format!(
            "history_series query does not accept parameter '{unknown}' (allowed: {})",
            HISTORY_QUERY_PARAMS.join(", ")
        ));
    }
    let explicit_since = params.get("since").is_some_and(|v| !v.is_null());
    let mut since = match params.get("since") {
        Some(v) => parse_rfc3339("since", v)?,
        None => None,
    };
    let until = match params.get("until") {
        Some(v) => parse_rfc3339("until", v)?,
        None => None,
    };
    if let Some(w) = params.get("window").filter(|v| !v.is_null()) {
        if since.is_some() {
            return Err("history_series takes 'since' or 'window', not both".into());
        }
        let secs = parse_history_duration(w).filter(|s| *s > 0).ok_or_else(|| {
            format!("history_series 'window' must be a positive duration such as \"24h\" or seconds, got {w}")
        })?;
        let start = chrono::Duration::try_seconds(secs)
            .and_then(|d| now.checked_sub_signed(d))
            .ok_or_else(|| "history_series 'window' is too large".to_string())?;
        since = Some(start.to_rfc3339());
    }
    if let (Some(a), Some(b)) = (&since, &until) {
        if a >= b {
            return Err(format!(
                "history_series window is empty: since {a} is not before until {b}"
            ));
        }
    }
    let limit =
        match params.get("limit") {
            None | Some(serde_json::Value::Null) => None,
            Some(v) => Some(as_count(v).ok_or_else(|| {
                "history_series 'limit' must be a non-negative integer".to_string()
            })? as usize),
        };
    let aggregate = match params.get("aggregate") {
        None | Some(serde_json::Value::Null) => None,
        Some(v) => Some(
            v.as_str()
                .and_then(HistoryAggregate::parse)
                .ok_or_else(|| {
                    format!(
                        "history_series 'aggregate' must be one of {}, got {v}",
                        HistoryAggregate::NAMES.join(", ")
                    )
                })?,
        ),
    };
    let bucket = match params.get("bucket_secs").filter(|v| !v.is_null()) {
        None => {
            if aggregate.is_some() {
                return Err("history_series 'aggregate' requires 'bucket_secs'".into());
            }
            None
        }
        Some(v) => {
            let secs = parse_history_duration(v)
                .filter(|s| (HISTORY_MIN_BUCKET_SECS..=HISTORY_MAX_BUCKET_SECS).contains(s))
                .ok_or_else(|| {
                    format!(
                        "history_series 'bucket_secs' must be a duration between {HISTORY_MIN_BUCKET_SECS}s and {HISTORY_MAX_BUCKET_SECS}s, got {v}"
                    )
                })?;
            Some((secs, aggregate.unwrap_or(HistoryAggregate::Avg)))
        }
    };
    Ok(HistoryReadOptions {
        since,
        until,
        limit,
        bucket,
        newest: !explicit_since,
    })
}

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
        self.backend
            .append_sample(&cfg.series_id, &ts, aggregated, None)?;
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
pub fn validate_history_series(
    graph: &ConfiguredGraph,
    store: &SqliteStore,
) -> Vec<ValidationIssue> {
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
                format!(
                    "history series references unknown instance '{}'",
                    series.instance_id
                ),
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
        assert_eq!(
            aggregate_value(&json!([1, 2, 3]), "count", None).unwrap(),
            3.0
        );
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
        backend
            .append_sample("s", &Utc::now().to_rfc3339(), 2.0, None)
            .unwrap();
        assert_eq!(backend.compact("s", 1).unwrap(), 1);
        let rows = backend.query("s", None, usize::MAX).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value_num, 2.0);
    }

    #[test]
    fn history_read_options_parse_windows_and_buckets() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-02T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let parse = |v: serde_json::Value| parse_history_read_options(v.as_object().unwrap(), now);
        let o = parse(
            json!({ "window": "24h", "bucket_secs": "1h", "aggregate": "max", "limit": 48.0 }),
        )
        .unwrap();
        assert_eq!(o.since.as_deref(), Some("2026-01-01T00:00:00+00:00"));
        assert_eq!(o.bucket, Some((3600, HistoryAggregate::Max)));
        assert_eq!(o.limit, Some(48));
        assert!(o.newest, "a sliding window keeps the newest samples");
        let o = parse(json!({ "since": "2026-01-01T01:00:00+01:00", "until": "2026-01-01T12:00:00Z", "bucket_secs": 300 }))
            .unwrap();
        assert_eq!(o.since.as_deref(), Some("2026-01-01T00:00:00+00:00"));
        assert_eq!(o.until.as_deref(), Some("2026-01-01T12:00:00+00:00"));
        assert_eq!(o.bucket, Some((300, HistoryAggregate::Avg)));
        assert!(
            !o.newest,
            "an explicit since keeps the oldest samples after it"
        );
        assert_eq!(parse(json!({})).unwrap().bucket, None);

        for bad in [
            json!({ "since": "yesterday" }),
            json!({ "since": "2026-01-01T00:00:00Z", "window": "1h" }),
            json!({ "since": "2026-01-02T00:00:00Z", "until": "2026-01-01T00:00:00Z" }),
            json!({ "window": "-5m" }),
            json!({ "window": "5 parsecs" }),
            json!({ "bucket_secs": 0 }),
            json!({ "bucket_secs": "1y" }),
            json!({ "aggregate": "median", "bucket_secs": 60 }),
            json!({ "aggregate": "avg" }),
            json!({ "limit": -1 }),
            json!({ "step": 60 }),
            // Out of chrono's range: refused, never a panic.
            json!({ "window": 1e18 }),
            json!({ "window": "9223372036854775807s" }),
        ] {
            assert!(parse(bad.clone()).is_err(), "{bad} should be rejected");
        }
    }
}
