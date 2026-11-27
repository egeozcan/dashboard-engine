//! Scoped host API bridge exposed to Luau runtime VMs.
//!
//! Enforces category boundaries (§2.1) and provides coroutine-friendly `FutureHandle` + `ctx:await(...)`.

use crate::manifest::{DefinitionCategory, EventDeclSpec};
use async_trait::async_trait;
use mlua::{Lua, LuaSerdeExt, Table, UserData, UserDataMethods, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpHostRequest {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: Option<serde_json::Value>,
    /// Secret binding name (credential handle, never plaintext).
    #[serde(default)]
    pub credential: Option<String>,
    /// URL prefix (`scheme://host[:port]/path`) the request and every redirect hop must stay
    /// under; a hop outside it is refused before anything (or any credential) is sent.
    #[serde(default)]
    pub confine_to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpHostResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: serde_json::Value,
}

#[async_trait]
pub trait HostBridge: Send + Sync {
    async fn query_input(
        &self,
        input_name: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String>;

    async fn call_input(
        &self,
        input_name: &str,
        operation: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String>;

    async fn invoke_action(
        &self,
        input_name: &str,
        args: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<serde_json::Value, String>;

    async fn http_request(&self, req: HttpHostRequest) -> Result<HttpHostResponse, String>;

    async fn emit_event(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        durable: bool,
    ) -> Result<(), String>;

    async fn state_get(&self, scope: &str, key: &str) -> Result<serde_json::Value, String>;

    async fn state_set(
        &self,
        scope: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), String>;

    async fn load_fixture(&self, fixture_path: &str) -> Result<serde_json::Value, String>;

    /// Emit a contract-declared Event with its declared schema version. `ctx:emit` calls this
    /// after checking the Event type and payload against the Definition contract. The default
    /// delegates to [`HostBridge::emit_event`] for bridges that do not record versions.
    async fn emit_event_versioned(
        &self,
        event_type: &str,
        _schema_version: &str,
        payload: serde_json::Value,
        durable: bool,
    ) -> Result<(), String> {
        self.emit_event(event_type, payload, durable).await
    }

    /// Called by the Definition test runner before each test case so bridges holding
    /// per-test state (Mock state, captured Events, Instance State) can reset it.
    async fn begin_test_case(&self, _test_name: &str) {}

    async fn mock_state_get(&self, _key: &str) -> Result<serde_json::Value, String> {
        Ok(serde_json::Value::Null)
    }

    async fn mock_state_set(&self, _key: &str, _value: serde_json::Value) -> Result<(), String> {
        Ok(())
    }

    /// Provider push (§6.1 push hook): replace the cached result of the calling data provider's
    /// own query `params` with a new snapshot. Returns `{ outcome, version }`. Only data
    /// providers that declare `updates.push` may push; the default bridge has no query graph.
    async fn push_snapshot(
        &self,
        _params: serde_json::Value,
        _value: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        Err("provider push is not available in this context".into())
    }

    /// Provider keyed delta (§6.2): `ops` is a list of `{ op = "insert" | "update" | "delete",
    /// key, value? }` applied to the calling data provider's own query `params`, ordered by the
    /// provider's monotonic source-local `source_sequence`. Returns `{ outcome, ... }` where
    /// outcome is `applied`, `duplicate` or `resync_scheduled`.
    async fn push_delta(
        &self,
        _params: serde_json::Value,
        _source_sequence: u64,
        _ops: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        Err("provider push is not available in this context".into())
    }
}

/// Default no-op host bridge for pure unit execution.
pub struct NullHostBridge;

#[async_trait]
impl HostBridge for NullHostBridge {
    async fn query_input(
        &self,
        input_name: &str,
        _params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        Err(format!("input '{input_name}' not bound"))
    }
    async fn call_input(
        &self,
        input_name: &str,
        operation: &str,
        _args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        Err(format!(
            "input '{input_name}' operation '{operation}' not bound"
        ))
    }
    async fn invoke_action(
        &self,
        input_name: &str,
        _args: serde_json::Value,
        _idempotency_key: Option<String>,
    ) -> Result<serde_json::Value, String> {
        Err(format!("action input '{input_name}' not bound"))
    }
    async fn http_request(&self, req: HttpHostRequest) -> Result<HttpHostResponse, String> {
        Err(format!(
            "http host bridge unavailable for {} {}",
            req.method, req.url
        ))
    }
    async fn emit_event(
        &self,
        _event_type: &str,
        _payload: serde_json::Value,
        _durable: bool,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn state_get(&self, _scope: &str, _key: &str) -> Result<serde_json::Value, String> {
        Ok(serde_json::Value::Null)
    }
    async fn state_set(
        &self,
        _scope: &str,
        _key: &str,
        _value: serde_json::Value,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn load_fixture(&self, fixture_path: &str) -> Result<serde_json::Value, String> {
        Err(format!("fixture '{fixture_path}' unavailable"))
    }
}

/// Record of external calls an execution issued, used to decide whether a failed action may
/// already have caused an external side effect (§9.1 `unknown`, §15.2).
#[derive(Debug, Default)]
pub struct CallTracking {
    mutating_issued: std::sync::atomic::AtomicUsize,
    mutating_succeeded: std::sync::atomic::AtomicUsize,
    mutating_in_flight: std::sync::atomic::AtomicUsize,
}

impl CallTracking {
    /// True when a mutating call succeeded, or was still in flight when the execution ended
    /// (abandoned by timeout, cancellation, memory exhaustion or a crash).
    pub fn side_effect_possible(&self) -> bool {
        use std::sync::atomic::Ordering::Acquire;
        self.mutating_succeeded.load(Acquire) > 0 || self.mutating_in_flight.load(Acquire) > 0
    }

    pub fn mutating_calls_issued(&self) -> usize {
        self.mutating_issued
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Classifies `(input_name, operation)` connector calls as mutating (`true`) or read-only.
pub type MutationClassifier = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

/// HostBridge decorator recording mutating connector calls and nested action invocations.
/// Nested actions always count as mutating; connector calls are classified by `is_mutating`.
pub struct CallTrackingBridge {
    inner: Arc<dyn HostBridge>,
    tracking: Arc<CallTracking>,
    is_mutating: MutationClassifier,
}

impl CallTrackingBridge {
    pub fn new(
        inner: Arc<dyn HostBridge>,
        tracking: Arc<CallTracking>,
        is_mutating: MutationClassifier,
    ) -> Self {
        Self {
            inner,
            tracking,
            is_mutating,
        }
    }

    async fn tracked<F>(&self, mutating: bool, fut: F) -> Result<serde_json::Value, String>
    where
        F: Future<Output = Result<serde_json::Value, String>> + Send,
    {
        use std::sync::atomic::Ordering::AcqRel;
        if !mutating {
            return fut.await;
        }
        self.tracking.mutating_issued.fetch_add(1, AcqRel);
        // Deliberately not a drop guard: if this future is dropped mid-call, the call stays
        // "in flight" and the outcome is treated as unprovable.
        self.tracking.mutating_in_flight.fetch_add(1, AcqRel);
        let res = fut.await;
        if res.is_ok() {
            self.tracking.mutating_succeeded.fetch_add(1, AcqRel);
        }
        self.tracking.mutating_in_flight.fetch_sub(1, AcqRel);
        res
    }
}

#[async_trait]
impl HostBridge for CallTrackingBridge {
    async fn query_input(
        &self,
        input_name: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.inner.query_input(input_name, params).await
    }
    async fn call_input(
        &self,
        input_name: &str,
        operation: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let mutating = (self.is_mutating)(input_name, operation);
        self.tracked(mutating, self.inner.call_input(input_name, operation, args))
            .await
    }
    async fn invoke_action(
        &self,
        input_name: &str,
        args: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<serde_json::Value, String> {
        self.tracked(
            true,
            self.inner.invoke_action(input_name, args, idempotency_key),
        )
        .await
    }
    async fn http_request(&self, req: HttpHostRequest) -> Result<HttpHostResponse, String> {
        let mutating =
            !req.method.eq_ignore_ascii_case("GET") && !req.method.eq_ignore_ascii_case("HEAD");
        if !mutating {
            return self.inner.http_request(req).await;
        }
        let inner = Arc::clone(&self.inner);
        let res = self
            .tracked(true, async move {
                inner
                    .http_request(req)
                    .await
                    .and_then(|r| serde_json::to_value(r).map_err(|e| e.to_string()))
            })
            .await?;
        serde_json::from_value(res).map_err(|e| e.to_string())
    }
    async fn emit_event(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        durable: bool,
    ) -> Result<(), String> {
        self.inner.emit_event(event_type, payload, durable).await
    }
    async fn emit_event_versioned(
        &self,
        event_type: &str,
        schema_version: &str,
        payload: serde_json::Value,
        durable: bool,
    ) -> Result<(), String> {
        self.inner
            .emit_event_versioned(event_type, schema_version, payload, durable)
            .await
    }
    async fn begin_test_case(&self, test_name: &str) {
        self.inner.begin_test_case(test_name).await
    }
    async fn state_get(&self, scope: &str, key: &str) -> Result<serde_json::Value, String> {
        self.inner.state_get(scope, key).await
    }
    async fn state_set(
        &self,
        scope: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), String> {
        self.inner.state_set(scope, key, value).await
    }
    async fn load_fixture(&self, fixture_path: &str) -> Result<serde_json::Value, String> {
        self.inner.load_fixture(fixture_path).await
    }
    async fn mock_state_get(&self, key: &str) -> Result<serde_json::Value, String> {
        self.inner.mock_state_get(key).await
    }
    async fn mock_state_set(&self, key: &str, value: serde_json::Value) -> Result<(), String> {
        self.inner.mock_state_set(key, value).await
    }
    async fn push_snapshot(
        &self,
        params: serde_json::Value,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.inner.push_snapshot(params, value).await
    }
    async fn push_delta(
        &self,
        params: serde_json::Value,
        source_sequence: u64,
        ops: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.inner.push_delta(params, source_sequence, ops).await
    }
}

type BoxedResultFuture =
    Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'static>>;

/// Opaque async handle returned by `ctx.inputs.*:query(...)`, `ctx.http:request(...)`, etc.
/// Resolved via `ctx:await(handle)` inside Luau handlers.
#[derive(Clone)]
pub struct FutureHandle {
    inner: Arc<Mutex<Option<BoxedResultFuture>>>,
}

impl FutureHandle {
    pub fn new<F>(fut: F) -> Self
    where
        F: Future<Output = Result<serde_json::Value, String>> + Send + 'static,
    {
        Self {
            inner: Arc::new(Mutex::new(Some(Box::pin(fut)))),
        }
    }

    pub async fn resolve(&self) -> Result<serde_json::Value, String> {
        let fut = {
            let mut guard = self.inner.lock().await;
            guard.take()
        };
        match fut {
            Some(f) => f.await,
            None => Err("FutureHandle already awaited".into()),
        }
    }
}

impl UserData for FutureHandle {}

#[derive(Clone)]
pub struct ExecutionContextConfig {
    pub category: DefinitionCategory,
    pub instance_id: String,
    pub input_names: Vec<String>,
    pub config: serde_json::Value,
    pub payload: serde_json::Value, // query / args / request / event
    pub operation: Option<String>,
    pub idempotency_key: Option<String>,
    /// Optional caller-side narrowing of the Event types `ctx:emit` may use. An Event type must
    /// always be declared in the executing Definition's contract (`emits`); when this list is
    /// non-empty the type must also appear here. An empty list adds no further narrowing; it never
    /// widens what the contract allows.
    pub allowed_emits: Vec<String>,
    pub now_iso: String,
    pub trace_id: String,
    pub bridge: Arc<dyn HostBridge>,
}

#[derive(Clone)]
pub struct LuaContext {
    pub cfg: ExecutionContextConfig,
    /// Event declarations from the executing Definition's contract. `ctx:emit` is fail-closed:
    /// an Event type not declared here is rejected, including when this list is empty.
    pub emit_declarations: Vec<EventDeclSpec>,
}

impl LuaContext {
    /// Context whose Definition contract declares no Events (every `ctx:emit` is rejected).
    pub fn new(cfg: ExecutionContextConfig) -> Self {
        Self {
            cfg,
            emit_declarations: Vec::new(),
        }
    }

    pub fn with_emit_declarations(
        cfg: ExecutionContextConfig,
        emit_declarations: Vec<EventDeclSpec>,
    ) -> Self {
        Self {
            cfg,
            emit_declarations,
        }
    }

    /// Validate an explicit `ctx:emit` against category rules (§7.2) and the contract, returning
    /// the matching declaration.
    pub fn check_emit(
        &self,
        event_type: &str,
        payload: &serde_json::Value,
    ) -> Result<&EventDeclSpec, String> {
        match self.cfg.category {
            // §7.2: data providers and connectors (and Mocks standing in for connectors) emit via
            // ctx:emit; action providers may emit the Event types their contract declares as part
            // of their completion. No other category emits explicitly.
            DefinitionCategory::DataProvider
            | DefinitionCategory::Connector
            | DefinitionCategory::Mock
            | DefinitionCategory::ActionProvider => {}
            DefinitionCategory::Adapter | DefinitionCategory::WidgetProvider => {
                return Err(
                    "adapters and widgets never emit Events; send a Request instead (§7.2)".into(),
                );
            }
            other => {
                return Err(format!(
                    "category '{}' cannot emit Events via ctx:emit (§7.2)",
                    other.as_str()
                ));
            }
        }
        let Some(decl) = self
            .emit_declarations
            .iter()
            .find(|d| d.semantic_type == event_type)
        else {
            return Err(format!(
                "event type '{event_type}' is not declared in definition contract"
            ));
        };
        if !self.cfg.allowed_emits.is_empty()
            && !self.cfg.allowed_emits.iter().any(|e| e == event_type)
        {
            return Err(format!(
                "event type '{event_type}' is not permitted for this execution"
            ));
        }
        if let Some(ref schema) = decl.payload {
            schema.validate_value(payload).map_err(|e| {
                format!("event '{event_type}' payload violates its declared schema: {e}")
            })?;
        }
        Ok(decl)
    }
}

impl UserData for LuaContext {
    fn add_fields<F: mlua::UserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("instance_id", |_, this| Ok(this.cfg.instance_id.clone()));
        fields.add_field_method_get("trace_id", |_, this| Ok(this.cfg.trace_id.clone()));
        fields.add_field_method_get("now", |_, this| Ok(this.cfg.now_iso.clone()));
        fields.add_field_method_get("operation", |_, this| Ok(this.cfg.operation.clone()));
        fields.add_field_method_get("idempotency_key", |_, this| {
            Ok(this.cfg.idempotency_key.clone())
        });
        fields.add_field_method_get("config", |lua, this| lua.to_value(&this.cfg.config));
        fields.add_field_method_get("query", |lua, this| match this.cfg.category {
            DefinitionCategory::DataProvider | DefinitionCategory::Adapter => {
                lua.to_value(&this.cfg.payload)
            }
            _ => Ok(Value::Nil),
        });
        fields.add_field_method_get("args", |lua, this| match this.cfg.category {
            DefinitionCategory::ActionProvider
            | DefinitionCategory::Connector
            | DefinitionCategory::Mock => lua.to_value(&this.cfg.payload),
            _ => Ok(Value::Nil),
        });
        fields.add_field_method_get("request", |lua, this| match this.cfg.category {
            DefinitionCategory::Adapter => lua.to_value(&this.cfg.payload),
            _ => Ok(Value::Nil),
        });
        fields.add_field_method_get("event", |lua, this| match this.cfg.category {
            DefinitionCategory::Automation => lua.to_value(&this.cfg.payload),
            _ => Ok(Value::Nil),
        });
        fields.add_field_method_get("inputs", |lua, this| build_inputs_table(lua, &this.cfg));
        fields.add_field_method_get("http", |lua, this| {
            if this.cfg.category == DefinitionCategory::Connector {
                Ok(Value::UserData(lua.create_userdata(HttpFacade {
                    bridge: Arc::clone(&this.cfg.bridge),
                })?))
            } else {
                Err(mlua::Error::runtime(format!(
                    "category '{}' does not have access to ctx.http",
                    this.cfg.category.as_str()
                )))
            }
        });
        // §8: Instance State belongs to connectors, data providers and automations only.
        fields.add_field_method_get("state", |lua, this| match this.cfg.category {
            DefinitionCategory::Connector
            | DefinitionCategory::DataProvider
            | DefinitionCategory::Automation => {
                Ok(Value::UserData(lua.create_userdata(StateFacade {
                    scope: "instance".into(),
                    read_only: false,
                    bridge: Arc::clone(&this.cfg.bridge),
                })?))
            }
            DefinitionCategory::Adapter => Err(mlua::Error::runtime(
                "adapters hold no state of their own (ADR-0001); use ctx.dashboard_state",
            )),
            DefinitionCategory::Mock => Err(mlua::Error::runtime(
                "Mocks keep fixture state via ctx:mock_get/ctx:mock_set, not ctx.state",
            )),
            other => Err(mlua::Error::runtime(format!(
                "category '{}' has no Instance State (§8: connectors, data providers, automations only)",
                other.as_str()
            ))),
        });
        fields.add_field_method_get("dashboard_state", |lua, this| match this.cfg.category {
            DefinitionCategory::Adapter | DefinitionCategory::Automation => {
                Ok(Value::UserData(lua.create_userdata(StateFacade {
                    scope: "dashboard".into(),
                    read_only: this.cfg.category == DefinitionCategory::Adapter,
                    bridge: Arc::clone(&this.cfg.bridge),
                })?))
            }
            _ => Err(mlua::Error::runtime(format!(
                "category '{}' cannot read dashboard_state directly",
                this.cfg.category.as_str()
            ))),
        });
    }

    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("await", |lua, _this, handle_val: Value| async move {
            match handle_val {
                Value::UserData(ud) => {
                    let handle = ud.borrow::<FutureHandle>()?.clone();
                    let json_val = handle.resolve().await.map_err(mlua::Error::runtime)?;
                    if json_val.is_null() {
                        Ok(Value::Nil)
                    } else {
                        lua.to_value(&json_val)
                    }
                }
                other => Ok(other),
            }
        });

        methods.add_async_method("await_all", |lua, _this, handles_tbl: Table| async move {
            let mut futs: Vec<BoxedResultFuture> = Vec::new();
            for pair in handles_tbl.sequence_values::<Value>() {
                let v = pair?;
                match v {
                    Value::UserData(ud) => {
                        let h = ud.borrow::<FutureHandle>()?.clone();
                        futs.push(Box::pin(async move { h.resolve().await }));
                    }
                    other => {
                        let j: serde_json::Value = lua.from_value(other)?;
                        futs.push(Box::pin(async move { Ok(j) }));
                    }
                }
            }
            let results = futures_util::future::join_all(futs).await;
            let tbl = lua.create_table()?;
            for (idx, res) in results.into_iter().enumerate() {
                let v = res.map_err(mlua::Error::runtime)?;
                tbl.set(idx + 1, lua.to_value(&v)?)?;
            }
            Ok(tbl)
        });

        methods.add_async_method(
            "emit",
            |_lua, this, (event_type, payload, durable): (String, Value, Option<bool>)| async move {
                let payload_json: serde_json::Value = serde_json::to_value(&payload)
                    .map_err(|e| mlua::Error::runtime(e.to_string()))?;
                let decl = this
                    .check_emit(&event_type, &payload_json)
                    .map_err(mlua::Error::runtime)?;
                // A contract that declares the Event durable keeps it durable: the script may
                // only upgrade an Event to durable, never downgrade it (§7.2, as `emit_declared`).
                let durable = decl.durable || durable.unwrap_or(false);
                let version = decl.schema_version.clone();
                this.cfg
                    .bridge
                    .emit_event_versioned(&event_type, &version, payload_json, durable)
                    .await
                    .map_err(mlua::Error::runtime)?;
                Ok(())
            },
        );

        // §6.1/§6.2 push hooks: a data provider declaring `updates.push` pushes snapshots or
        // keyed deltas for its own query (`params` defaults to the current `ctx.query`).
        methods.add_async_method("push_snapshot", |lua, this, spec: Table| async move {
            if this.cfg.category != DefinitionCategory::DataProvider {
                return Err(mlua::Error::runtime(
                    "ctx:push_snapshot is only available to data providers (§6.1)",
                ));
            }
            let value: serde_json::Value = match spec.get::<Value>("value")? {
                Value::Nil => serde_json::Value::Null,
                v => lua.from_value(v)?,
            };
            let params = push_params(&lua, &spec, &this.cfg.payload)?;
            let out = this
                .cfg
                .bridge
                .push_snapshot(params, value)
                .await
                .map_err(mlua::Error::runtime)?;
            lua.to_value(&out)
        });

        methods.add_async_method("push_delta", |lua, this, spec: Table| async move {
            if this.cfg.category != DefinitionCategory::DataProvider {
                return Err(mlua::Error::runtime(
                    "ctx:push_delta is only available to data providers (§6.2)",
                ));
            }
            let sequence = match spec.get::<Value>("sequence")? {
                Value::Integer(n) if n >= 0 => n as u64,
                Value::Number(n) if n >= 0.0 && n.fract() == 0.0 => n as u64,
                other => {
                    return Err(mlua::Error::runtime(format!(
                        "ctx:push_delta requires a non-negative integer 'sequence', got {}",
                        other.type_name()
                    )))
                }
            };
            let ops: serde_json::Value = match spec.get::<Value>("ops")? {
                Value::Table(t) if t.raw_len() == 0 => serde_json::Value::Array(Vec::new()),
                Value::Table(t) => lua.from_value(Value::Table(t))?,
                _ => {
                    return Err(mlua::Error::runtime(
                        "ctx:push_delta requires an 'ops' list",
                    ))
                }
            };
            let params = push_params(&lua, &spec, &this.cfg.payload)?;
            let out = this
                .cfg
                .bridge
                .push_delta(params, sequence, ops)
                .await
                .map_err(mlua::Error::runtime)?;
            lua.to_value(&out)
        });

        methods.add_async_method("fixture", |lua, this, path: String| async move {
            if this.cfg.category != DefinitionCategory::Mock {
                return Err(mlua::Error::runtime(
                    "ctx:fixture(...) is only available inside Mock definitions",
                ));
            }
            let val = this
                .cfg
                .bridge
                .load_fixture(&path)
                .await
                .map_err(mlua::Error::runtime)?;
            lua.to_value(&val)
        });

        methods.add_async_method("mock_get", |lua, this, key: String| async move {
            if this.cfg.category != DefinitionCategory::Mock {
                return Err(mlua::Error::runtime(
                    "ctx:mock_get(...) is only available inside Mock definitions",
                ));
            }
            let val = this
                .cfg
                .bridge
                .mock_state_get(&key)
                .await
                .map_err(mlua::Error::runtime)?;
            if val.is_null() {
                Ok(Value::Nil)
            } else {
                lua.to_value(&val)
            }
        });

        methods.add_async_method(
            "mock_set",
            |lua, this, (key, val): (String, Value)| async move {
                if this.cfg.category != DefinitionCategory::Mock {
                    return Err(mlua::Error::runtime(
                        "ctx:mock_set(...) is only available inside Mock definitions",
                    ));
                }
                let j: serde_json::Value = lua.from_value(val)?;
                this.cfg
                    .bridge
                    .mock_state_set(&key, j)
                    .await
                    .map_err(mlua::Error::runtime)?;
                Ok(())
            },
        );
    }
}

/// `params` of a push call: explicit `spec.params`, else the executing query's parameters.
fn push_params(
    lua: &Lua,
    spec: &Table,
    current: &serde_json::Value,
) -> mlua::Result<serde_json::Value> {
    match spec.get::<Value>("params")? {
        Value::Nil => Ok(current.clone()),
        Value::Table(t) if t.clone().pairs::<Value, Value>().next().is_none() => {
            Ok(serde_json::json!({}))
        }
        v => lua.from_value(v),
    }
}

fn build_inputs_table(lua: &Lua, cfg: &ExecutionContextConfig) -> mlua::Result<Table> {
    let tbl = lua.create_table()?;
    for name in &cfg.input_names {
        let handle = InputEndpoint {
            input_name: name.clone(),
            caller_category: cfg.category,
            bridge: Arc::clone(&cfg.bridge),
        };
        tbl.set(name.as_str(), handle)?;
    }
    Ok(tbl)
}

#[derive(Clone)]
struct InputEndpoint {
    input_name: String,
    caller_category: DefinitionCategory,
    bridge: Arc<dyn HostBridge>,
}

impl UserData for InputEndpoint {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("query", |lua, this, params: Option<Value>| {
            let p_json = match params {
                Some(v) => lua.from_value::<serde_json::Value>(v)?,
                None => serde_json::json!({}),
            };
            let name = this.input_name.clone();
            let bridge = Arc::clone(&this.bridge);
            Ok(FutureHandle::new(async move {
                bridge.query_input(&name, p_json).await
            }))
        });

        methods.add_method(
            "call",
            |lua, this, (operation, args): (String, Option<Value>)| {
                if this.caller_category == DefinitionCategory::Adapter {
                    return Err(mlua::Error::runtime(
                        "forbidden binding (§2.1): adapters cannot call connectors directly",
                    ));
                }
                let a_json = match args {
                    Some(v) => lua.from_value::<serde_json::Value>(v)?,
                    None => serde_json::json!({}),
                };
                let name = this.input_name.clone();
                let bridge = Arc::clone(&this.bridge);
                Ok(FutureHandle::new(async move {
                    bridge.call_input(&name, &operation, a_json).await
                }))
            },
        );

        methods.add_method(
            "invoke",
            |lua, this, (args, idempotency_key): (Option<Value>, Option<String>)| {
                match this.caller_category {
                    DefinitionCategory::DataProvider
                    | DefinitionCategory::Library
                    | DefinitionCategory::Connector
                    | DefinitionCategory::Mock => {
                        return Err(mlua::Error::runtime(
                            "forbidden action call (§2.1): data providers cannot invoke actions",
                        ));
                    }
                    _ => {}
                }
                let a_json = match args {
                    Some(v) => lua.from_value::<serde_json::Value>(v)?,
                    None => serde_json::json!({}),
                };
                let name = this.input_name.clone();
                let bridge = Arc::clone(&this.bridge);
                Ok(FutureHandle::new(async move {
                    bridge.invoke_action(&name, a_json, idempotency_key).await
                }))
            },
        );
    }
}

#[derive(Clone)]
struct HttpFacade {
    bridge: Arc<dyn HostBridge>,
}

impl UserData for HttpFacade {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("request", |lua, this, req_val: Value| {
            let req: HttpHostRequest = lua.from_value(req_val)?;
            let bridge = Arc::clone(&this.bridge);
            Ok(FutureHandle::new(async move {
                let resp = bridge.http_request(req).await?;
                serde_json::to_value(resp).map_err(|e| e.to_string())
            }))
        });
    }
}

#[derive(Clone)]
struct StateFacade {
    scope: String,
    read_only: bool,
    bridge: Arc<dyn HostBridge>,
}

impl UserData for StateFacade {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("get", |lua, this, key: String| async move {
            let val = this
                .bridge
                .state_get(&this.scope, &key)
                .await
                .map_err(mlua::Error::runtime)?;
            if val.is_null() {
                Ok(Value::Nil)
            } else {
                lua.to_value(&val)
            }
        });

        methods.add_async_method("set", |lua, this, (key, val): (String, Value)| async move {
            if this.read_only {
                return Err(mlua::Error::runtime(
                    "dashboard_state is read-only inside Adapters",
                ));
            }
            let j: serde_json::Value = lua.from_value(val)?;
            this.bridge
                .state_set(&this.scope, &key, j)
                .await
                .map_err(mlua::Error::runtime)?;
            Ok(())
        });
    }
}

/// Build the `dash` runtime helper table exposed via `require("dash")` inside runtime scripts.
pub fn build_dash_runtime_sdk(lua: &Lua) -> mlua::Result<Table> {
    let dash = lua.create_table()?;
    let json = lua.create_table()?;
    json.set(
        "encode",
        lua.create_function(|lua, v: Value| {
            let j: serde_json::Value = lua.from_value(v)?;
            serde_json::to_string(&j).map_err(|e| mlua::Error::runtime(e.to_string()))
        })?,
    )?;
    json.set(
        "decode",
        lua.create_function(|lua, s: String| {
            let j: serde_json::Value =
                serde_json::from_str(&s).map_err(|e| mlua::Error::runtime(e.to_string()))?;
            lua.to_value(&j)
        })?,
    )?;
    dash.set("json", json)?;
    dash.set("url", build_url_helpers(lua)?)?;
    // Both ctx.now and upstream HTTP cache dates can be compared on the same UTC clock.
    dash.set(
        "parse_timestamp",
        lua.create_function(|_, text: String| {
            Ok(chrono::DateTime::parse_from_rfc3339(&text)
                .or_else(|_| chrono::DateTime::parse_from_rfc2822(&text))
                .ok()
                .map(|t| t.timestamp()))
        })?,
    )?;
    // An empty table that serializes as a JSON array (`[]`) rather than an object (`{}`), for
    // list results built up in Luau that may stay empty.
    dash.set(
        "array",
        lua.create_function(|lua, ()| {
            let t = lua.create_table()?;
            t.set_metatable(Some(lua.array_metatable()))?;
            Ok(t)
        })?,
    )?;

    dash.set(
        "uuid",
        lua.create_function(|_, ()| Ok(uuid::Uuid::new_v4().to_string()))?,
    )?;
    // Structured `unknown` outcome for action handlers (§9.1): `return dash.unknown_outcome("reason")`
    // tells the host the external operation may have happened although no response proves it.
    dash.set(
        "unknown_outcome",
        lua.create_function(|lua, reason: Option<String>| {
            let t = lua.create_table()?;
            t.set("__unknown", true)?;
            t.set(
                "reason",
                reason.unwrap_or_else(|| "upstream_connection_lost_after_request".into()),
            )?;
            Ok(t)
        })?,
    )?;
    Ok(dash)
}

/// Percent-encode `s` as one URL component: every byte except RFC 3986 unreserved characters
/// (`A-Z a-z 0-9 - _ . ~`) becomes `%XX`.
pub fn percent_encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn query_scalar(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(match n.as_f64() {
            Some(f) if f.fract() == 0.0 && f.abs() < 9.0e15 => format!("{}", f as i64),
            _ => n.to_string(),
        }),
        _ => None,
    }
}

/// `k=v&...` query string of a JSON object: keys sorted, `null`/empty values skipped, lists
/// repeated per element (`k=a&k=b`), everything percent-encoded.
pub fn build_query_string(params: &serde_json::Value) -> Result<String, String> {
    let Some(obj) = params.as_object() else {
        return if params.is_null() {
            Ok(String::new())
        } else {
            Err("dash.url.query expects a table of parameters".into())
        };
    };
    let mut keys: Vec<&String> = obj.keys().collect();
    keys.sort();
    let mut parts = Vec::new();
    for k in keys {
        let v = &obj[k];
        let values: Vec<String> = match v {
            serde_json::Value::Null => Vec::new(),
            serde_json::Value::Array(items) => {
                let mut out = Vec::new();
                for item in items {
                    out.push(query_scalar(item).ok_or_else(|| {
                        format!("dash.url.query: list parameter '{k}' must hold scalars")
                    })?);
                }
                out
            }
            // An empty Luau table arrives as an empty object: no values.
            serde_json::Value::Object(m) if m.is_empty() => Vec::new(),
            serde_json::Value::Object(_) => {
                return Err(format!(
                    "dash.url.query: parameter '{k}' must be a scalar or a list of scalars"
                ))
            }
            other => vec![query_scalar(other).unwrap_or_default()],
        };
        for val in values {
            parts.push(format!(
                "{}={}",
                percent_encode_component(k),
                percent_encode_component(&val)
            ));
        }
    }
    Ok(parts.join("&"))
}

/// Parse an RFC 8288 `Link` header (`<url>; rel="next", <url>; rel="last"`) into `rel -> url`.
pub fn parse_link_header(header: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut rest = header;
    // Tokenize by `<...>` first: a target URL may itself contain `,` or `;`.
    while let Some(open) = rest.find('<') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('>') else {
            break;
        };
        let url = &after[..close];
        let tail = &after[close + 1..];
        // The parameters run up to the next link-value: a `,` followed by `<`.
        let params_end = {
            let mut end = tail.len();
            let mut in_quotes = false;
            for (i, c) in tail.char_indices() {
                match c {
                    '"' => in_quotes = !in_quotes,
                    ',' if !in_quotes && tail[i + 1..].trim_start().starts_with('<') => {
                        end = i;
                        break;
                    }
                    _ => {}
                }
            }
            end
        };
        for param in tail[..params_end].split(';') {
            let Some((name, value)) = param.split_once('=') else {
                continue;
            };
            if name.trim().eq_ignore_ascii_case("rel") {
                for rel in value.trim().trim_matches('"').split_whitespace() {
                    out.entry(rel.to_ascii_lowercase())
                        .or_insert_with(|| url.to_string());
                }
            }
        }
        rest = &tail[params_end..];
    }
    out
}

/// `dash.url`: URL helpers for connector runtimes (`encode`, `query`, `parse_links`).
fn build_url_helpers(lua: &Lua) -> mlua::Result<Table> {
    let url = lua.create_table()?;
    url.set(
        "encode",
        lua.create_function(|_, v: Value| {
            let s = match v {
                Value::String(s) => s.to_str()?.to_string(),
                Value::Integer(i) => i.to_string(),
                Value::Number(n) if n.fract() == 0.0 && n.abs() < 9.0e15 => (n as i64).to_string(),
                Value::Number(n) => n.to_string(),
                Value::Boolean(b) => b.to_string(),
                other => {
                    return Err(mlua::Error::runtime(format!(
                        "dash.url.encode expects a string or number, got {}",
                        other.type_name()
                    )))
                }
            };
            Ok(percent_encode_component(&s))
        })?,
    )?;
    url.set(
        "query",
        lua.create_function(|lua, v: Value| {
            let j: serde_json::Value = if v.is_nil() {
                serde_json::Value::Null
            } else {
                lua.from_value(v)?
            };
            build_query_string(&j).map_err(mlua::Error::runtime)
        })?,
    )?;
    url.set(
        "parse_links",
        lua.create_function(|lua, v: Option<String>| {
            let t = lua.create_table()?;
            if let Some(h) = v {
                for (rel, target) in parse_link_header(&h) {
                    t.set(rel, target)?;
                }
            }
            Ok(t)
        })?,
    )?;
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::tests::{bundle, cfg, ProbeBridge};
    use crate::pool::{ResourceBudget, VmPool};
    use serde_json::json;

    #[test]
    fn url_helpers_encode_build_queries_and_parse_link_headers() {
        assert_eq!(
            percent_encode_component("a b/c?d=é~"),
            "a%20b%2Fc%3Fd%3D%C3%A9~"
        );
        assert_eq!(
            build_query_string(&json!({
                "state": "open", "per_page": 100, "labels": ["bug", "p 1"],
                "skip": null, "empty": {}, "draft": false
            }))
            .unwrap(),
            "draft=false&labels=bug&labels=p%201&per_page=100&state=open"
        );
        assert_eq!(build_query_string(&json!(null)).unwrap(), "");
        assert!(build_query_string(&json!({ "nested": { "a": 1 } })).is_err());
        let links = parse_link_header(
            r#"<https://api.github.com/repos/a/b/pulls?page=2>; rel="next", <https://api.github.com/repos/a/b/pulls?page=5>; rel="last""#,
        );
        assert_eq!(
            links["next"],
            "https://api.github.com/repos/a/b/pulls?page=2"
        );
        assert_eq!(
            links["last"],
            "https://api.github.com/repos/a/b/pulls?page=5"
        );
        assert!(parse_link_header("garbage").is_empty());
        let links = parse_link_header(
            r#"</items?fields=a,b;c&page=2>; rel="next"; title="x, y", </items?page=9>; rel=last"#,
        );
        assert_eq!(links["next"], "/items?fields=a,b;c&page=2");
        assert_eq!(links["last"], "/items?page=9");

        let lua = Lua::new();
        lua.globals()
            .set("dash", build_dash_runtime_sdk(&lua).unwrap())
            .unwrap();
        let out: String = lua
            .load(
                r#"
                local links = dash.url.parse_links('<https://x/y?p=2>; rel="next"')
                return dash.url.encode("o/r") .. "|" .. dash.url.query({ q = "is:pr", n = 2 }) .. "|" .. links.next
            "#,
            )
            .eval()
            .unwrap();
        assert_eq!(out, "o%2Fr|n=2&q=is%3Apr|https://x/y?p=2");

        let arr: Value = lua
            .load("local empty = dash.array(); local full = dash.array(); table.insert(full, 1); return { empty = empty, full = full }")
            .eval()
            .unwrap();
        let j: serde_json::Value = lua.from_value(arr).unwrap();
        assert_eq!(j, json!({ "empty": [], "full": [1] }));
    }

    const EMIT_RT: &str = r#"
local R = {}
function R.query(ctx)
    ctx:await(ctx:emit(ctx.config.event_type or "dev.pr_merged", { number = 42 }))
    return true
end
R.call = R.query
R.execute = R.query
R.run = R.query
return R
"#;

    async fn run_emit(
        decl: &str,
        category: DefinitionCategory,
        allowed: Vec<String>,
        event_type: &str,
    ) -> (
        Result<serde_json::Value, crate::pool::RuntimeError>,
        Arc<ProbeBridge>,
    ) {
        let pool = VmPool::new(ResourceBudget::default());
        let bridge = Arc::new(ProbeBridge::default());
        let b = bundle("emitter", decl, EMIT_RT);
        let mut c = cfg(category, "emitter", &[], bridge.clone());
        c.allowed_emits = allowed;
        c.config = json!({ "event_type": event_type });
        let handler = match category {
            DefinitionCategory::Connector | DefinitionCategory::Mock => "call",
            DefinitionCategory::ActionProvider => "execute",
            DefinitionCategory::Automation => "run",
            _ => "query",
        };
        (pool.execute_handler(&b, handler, c, None).await, bridge)
    }

    #[tokio::test]
    async fn emit_is_fail_closed_without_declared_events() {
        let decl =
            r#"local dash = require("dash"); return dash.data_provider { id = "t.noemits" }"#;
        let (res, bridge) = run_emit(
            decl,
            DefinitionCategory::DataProvider,
            vec![],
            "dev.pr_merged",
        )
        .await;
        let err = res.unwrap_err().to_string();
        assert!(err.contains("not declared in definition contract"), "{err}");
        assert!(bridge.emitted.lock().unwrap().is_empty());

        // A connector that declares no emits cannot spoof Events either.
        let decl = r#"local dash = require("dash"); return dash.connector { id = "t.conn" }"#;
        let (res, _) = run_emit(
            decl,
            DefinitionCategory::Connector,
            vec![],
            "github.pr_merged",
        )
        .await;
        assert!(res.unwrap_err().to_string().contains("not declared"));
    }

    #[tokio::test]
    async fn emit_uses_declared_version_durability_and_payload_schema() {
        let decl = r#"
local dash = require("dash")
return dash.data_provider {
    id = "t.emits",
    emits = {
        { semantic_type = "dev.pr_merged", schema_version = "3", durable = true,
          payload = dash.object { number = dash.integer() } },
        { semantic_type = "dev.strict", schema_version = "1", durable = false,
          payload = dash.object { title = dash.string() } },
    },
}
"#;
        let (res, bridge) = run_emit(
            decl,
            DefinitionCategory::DataProvider,
            vec![],
            "dev.pr_merged",
        )
        .await;
        assert!(res.is_ok(), "{res:?}");
        let emitted = bridge.emitted.lock().unwrap().clone();
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].0, "dev.pr_merged");
        assert_eq!(
            emitted[0].1, "3",
            "declared schema version is passed through"
        );
        assert!(
            emitted[0].2,
            "declared durability applies when no flag is given"
        );

        // Payload { number = 42 } violates dev.strict's declared payload schema.
        let (res, bridge) =
            run_emit(decl, DefinitionCategory::DataProvider, vec![], "dev.strict").await;
        let err = res.unwrap_err().to_string();
        assert!(err.contains("violates its declared schema"), "{err}");
        assert!(bridge.emitted.lock().unwrap().is_empty());

        // Undeclared type is rejected even though other types are declared.
        let (res, _) = run_emit(decl, DefinitionCategory::DataProvider, vec![], "dev.other").await;
        assert!(res.unwrap_err().to_string().contains("not declared"));

        // A non-empty caller allow-list narrows further.
        let (res, _) = run_emit(
            decl,
            DefinitionCategory::DataProvider,
            vec!["dev.strict".into()],
            "dev.pr_merged",
        )
        .await;
        assert!(res.unwrap_err().to_string().contains("not permitted"));
    }

    #[tokio::test]
    async fn emit_cannot_downgrade_a_contract_declared_durable_event() {
        let decl = r#"
local dash = require("dash")
return dash.data_provider {
    id = "t.durable",
    emits = {
        { semantic_type = "dev.durable", schema_version = "1", durable = true },
        { semantic_type = "dev.live", schema_version = "1", durable = false },
    },
}
"#;
        let rt = r#"
local R = {}
function R.query(ctx)
    ctx:await(ctx:emit(ctx.config.event_type, { number = 1 }, ctx.config.durable))
    return true
end
return R
"#;
        let pool = VmPool::new(ResourceBudget::default());
        for (event_type, flag, expect) in [
            ("dev.durable", json!(false), true),
            ("dev.durable", json!(null), true),
            ("dev.live", json!(true), true),
            ("dev.live", json!(false), false),
            ("dev.live", json!(null), false),
        ] {
            let bridge = Arc::new(ProbeBridge::default());
            let b = bundle("emitter", decl, rt);
            let mut c = cfg(
                DefinitionCategory::DataProvider,
                "emitter",
                &[],
                bridge.clone(),
            );
            // A missing key is `nil` in Luau (no explicit flag).
            c.config = if flag.is_null() {
                json!({ "event_type": event_type })
            } else {
                json!({ "event_type": event_type, "durable": flag })
            };
            pool.execute_handler(&b, "query", c, None).await.unwrap();
            let emitted = bridge.emitted.lock().unwrap().clone();
            assert_eq!(emitted.len(), 1);
            assert_eq!(
                emitted[0].2, expect,
                "{event_type} with flag {flag}: durable must be {expect}"
            );
        }
    }

    #[tokio::test]
    async fn only_providers_connectors_mocks_and_actions_may_emit() {
        let decl = r#"
local dash = require("dash")
return dash.automation { id = "t.auto", emits = { "dev.pr_merged" } }
"#;
        let (res, _) = run_emit(
            decl,
            DefinitionCategory::Automation,
            vec![],
            "dev.pr_merged",
        )
        .await;
        let err = res.unwrap_err().to_string();
        assert!(err.contains("cannot emit Events"), "{err}");

        let decl = r#"
local dash = require("dash")
return dash.action_provider { id = "t.act", emits = { "dev.pr_merged" } }
"#;
        let (res, bridge) = run_emit(
            decl,
            DefinitionCategory::ActionProvider,
            vec![],
            "dev.pr_merged",
        )
        .await;
        assert!(res.is_ok(), "{res:?}");
        assert_eq!(bridge.emitted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn instance_state_is_limited_to_connectors_providers_and_automations() {
        let rt = r#"
local R = {}
function R.go(ctx)
    ctx:await(ctx.state:set("k", 1))
    return ctx:await(ctx.state:get("k"))
end
return R
"#;
        let pool = VmPool::new(ResourceBudget::default());
        for (category, decl, allowed) in [
            (
                DefinitionCategory::DataProvider,
                r#"local dash = require("dash"); return dash.data_provider { id = "t.s1" }"#,
                true,
            ),
            (
                DefinitionCategory::Connector,
                r#"local dash = require("dash"); return dash.connector { id = "t.s2" }"#,
                true,
            ),
            (
                DefinitionCategory::Automation,
                r#"local dash = require("dash"); return dash.automation { id = "t.s3" }"#,
                true,
            ),
            (
                DefinitionCategory::ActionProvider,
                r#"local dash = require("dash"); return dash.action_provider { id = "t.s4" }"#,
                false,
            ),
            (
                DefinitionCategory::Adapter,
                r#"local dash = require("dash"); return dash.adapter { id = "t.s5" }"#,
                false,
            ),
        ] {
            let id = format!("state_{}", category.as_str());
            let b = bundle(&id, decl, rt);
            let res = pool
                .execute_handler(
                    &b,
                    "go",
                    cfg(category, &id, &[], Arc::new(ProbeBridge::default())),
                    None,
                )
                .await;
            if allowed {
                assert_eq!(res.unwrap(), json!(1), "{category:?}");
            } else {
                let err = res.unwrap_err().to_string();
                assert!(
                    err.contains("State") || err.contains("state"),
                    "{category:?}: {err}"
                );
            }
        }
    }

    #[tokio::test]
    async fn call_tracking_marks_side_effect_possible_only_after_mutating_success() {
        let tracking = Arc::new(CallTracking::default());
        let inner: Arc<dyn HostBridge> = Arc::new(ProbeBridge::default());
        let bridge = CallTrackingBridge::new(
            inner,
            Arc::clone(&tracking),
            Arc::new(|_input: &str, op: &str| op != "get_pr"),
        );
        bridge.call_input("gh", "get_pr", json!({})).await.unwrap();
        assert!(
            !tracking.side_effect_possible(),
            "read-only call is not a side effect"
        );
        bridge
            .call_input("gh", "merge_pr", json!({}))
            .await
            .unwrap();
        assert!(tracking.side_effect_possible());
        assert_eq!(tracking.mutating_calls_issued(), 1);

        // A mutating call abandoned mid-flight (future dropped) is also unprovable.
        let tracking = Arc::new(CallTracking::default());
        let slow: Arc<dyn HostBridge> = Arc::new(ProbeBridge {
            sleep: std::time::Duration::from_secs(5),
            ..Default::default()
        });
        let bridge = CallTrackingBridge::new(
            slow,
            Arc::clone(&tracking),
            Arc::new(|_: &str, _: &str| true),
        );
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            bridge.call_input("gh", "merge_pr", json!({})),
        )
        .await;
        assert!(tracking.side_effect_possible());
    }
}
