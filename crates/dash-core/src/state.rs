//! Scoped State manager (Dashboard, Instance, Client) with CAS concurrency and Signal delivery (§8).

use crate::model::DashboardDoc;
use crate::signal::{SignalHub, StateSignalPayload};
use dash_store::{SqliteStore, StateRecord, StoreError};

#[derive(Clone)]
pub struct StateManager {
    store: SqliteStore,
    signals: SignalHub,
}

impl StateManager {
    pub fn new(store: SqliteStore, signals: SignalHub) -> Self {
        Self { store, signals }
    }

    pub fn get(&self, scope: &str, owner: &str, key: &str) -> Result<Option<StateRecord>, StoreError> {
        self.store.get_state(scope, owner, key)
    }

    pub fn list_dashboard_state(&self, dashboard_id: &str) -> Result<Vec<StateRecord>, StoreError> {
        self.store.list_state_for_owner("dashboard", dashboard_id)
    }

    /// Update state with optional optimistic CAS version check and publish a State Signal.
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
        self.signals.publish_state(StateSignalPayload {
            scope: rec.scope.clone(),
            owner: rec.owner.clone(),
            state_key: rec.state_key.clone(),
            version: rec.version,
            value: rec.value.clone(),
            updated_at: rec.updated_at.clone(),
        });
        Ok(rec)
    }

    /// Seed initial Dashboard state keys from Workspace DashboardDoc if absent.
    pub fn seed_dashboard_defaults(
        &self,
        dashboard: &DashboardDoc,
        generation_id: i64,
    ) -> Result<(), StoreError> {
        for (k, v) in &dashboard.initial_state {
            if self.store.get_state("dashboard", &dashboard.id, k)?.is_none() {
                self.set(
                    "dashboard",
                    &dashboard.id,
                    k,
                    1,
                    generation_id,
                    v.clone(),
                    None,
                )?;
            }
        }
        Ok(())
    }
}
