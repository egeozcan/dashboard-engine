//! Opt-in parameterized query history sampling, compaction, and retention (§7.4).

use crate::authority::{Principal, PrincipalKind};
use crate::model::{CapabilityCeiling, HistorySeriesConfig};
use crate::query_engine::QueryEngine;
use chrono::Utc;
use dash_store::{HistorySampleRecord, HistorySeriesSpec, SqliteStore};

#[derive(Clone)]
pub struct HistoryManager {
    store: SqliteStore,
    queries: QueryEngine,
}

impl HistoryManager {
    pub fn new(store: SqliteStore, queries: QueryEngine) -> Self {
        Self { store, queries }
    }

    /// Ensure configured history series metadata exists in SQLite.
    pub fn sync_series_configs(&self, configs: &[HistorySeriesConfig]) -> Result<(), String> {
        let now = Utc::now().to_rfc3339();
        for cfg in configs {
            self.store
                .upsert_history_series(&HistorySeriesSpec {
                    series_id: cfg.series_id.clone(),
                    instance_id: cfg.instance_id.clone(),
                    params: cfg.params.clone(),
                    aggregator: cfg.aggregator.clone(),
                    cadence_secs: cfg.cadence_secs,
                    retention_days: cfg.retention_days,
                    service_principal: cfg.service_principal.clone(),
                    created_at: now.clone(),
                })
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Sample one history series under its named Service principal's granted authority.
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
        let val = env.value.unwrap_or(serde_json::Value::Null);
        let aggregated = aggregate_value(&val, &cfg.aggregator, cfg.value_path.as_deref())?;

        let ts = sampled_at_override
            .map(|s| s.to_string())
            .unwrap_or_else(|| Utc::now().to_rfc3339());
        self.store
            .append_history_sample(&cfg.series_id, &ts, aggregated, None)
            .map_err(|e| e.to_string())?;
        let _ = self
            .store
            .compact_history_series(&cfg.series_id, cfg.retention_days);
        Ok(aggregated)
    }

    pub fn query_samples(
        &self,
        series_id: &str,
        since: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HistorySampleRecord>, String> {
        self.store
            .query_history_samples(series_id, since, limit)
            .map_err(|e| e.to_string())
    }
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
            serde_json::Value::Null => Ok(0.0),
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
