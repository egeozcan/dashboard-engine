//! Scoped host API bridge exposed to Luau runtime VMs.
//!
//! Enforces category boundaries (§2.1) and provides coroutine-friendly `FutureHandle` + `ctx:await(...)`.

use crate::manifest::DefinitionCategory;
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
    pub allowed_emits: Vec<String>,
    pub now_iso: String,
    pub trace_id: String,
    pub bridge: Arc<dyn HostBridge>,
}

#[derive(Clone)]
pub struct LuaContext {
    pub cfg: ExecutionContextConfig,
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
        fields.add_field_method_get("state", |lua, this| match this.cfg.category {
            DefinitionCategory::Adapter => Err(mlua::Error::runtime(
                "adapters hold no state of their own (ADR-0001); use ctx.dashboard_state",
            )),
            _ => Ok(Value::UserData(lua.create_userdata(StateFacade {
                scope: "instance".into(),
                read_only: false,
                bridge: Arc::clone(&this.cfg.bridge),
            })?)),
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
                match this.cfg.category {
                    DefinitionCategory::Adapter | DefinitionCategory::WidgetProvider => {
                        return Err(mlua::Error::runtime(
                            "adapters and widgets never emit Events; send a Request instead (§7.2)",
                        ));
                    }
                    _ => {}
                }
                if !this.cfg.allowed_emits.is_empty()
                    && !this.cfg.allowed_emits.iter().any(|e| e == &event_type)
                {
                    return Err(mlua::Error::runtime(format!(
                        "event type '{event_type}' is not declared in definition contract"
                    )));
                }
                let payload_json: serde_json::Value = serde_json::to_value(&payload)
                    .map_err(|e| mlua::Error::runtime(e.to_string()))?;
                this.cfg
                    .bridge
                    .emit_event(&event_type, payload_json, durable.unwrap_or(false))
                    .await
                    .map_err(mlua::Error::runtime)?;
                Ok(())
            },
        );

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

        methods.add_async_method(
            "set",
            |lua, this, (key, val): (String, Value)| async move {
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
            },
        );
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

    dash.set(
        "uuid",
        lua.create_function(|_, ()| Ok(uuid::Uuid::new_v4().to_string()))?,
    )?;
    Ok(dash)
}
