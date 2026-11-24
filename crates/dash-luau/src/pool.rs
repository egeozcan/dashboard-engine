//! Sandboxed Luau VM pool, quota enforcement, controlled library loading, and Generation carry-over.

use crate::host_api::{build_dash_runtime_sdk, ExecutionContextConfig, LuaContext};
use crate::manifest::DefinitionManifest;
use dashmap::DashMap;
use mlua::{Function, Lua, LuaOptions, LuaSerdeExt, StdLib, Value, VmState};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("luau runtime error: {0}")]
    Luau(#[from] mlua::Error),
    #[error("execution timed out after {0:?}")]
    Timeout(Duration),
    #[error("execution cancelled")]
    Cancelled,
    #[error("memory budget ({0} bytes) exceeded")]
    MemoryExceeded(usize),
    #[error("definition '{0}' handler '{1}' not found")]
    HandlerNotFound(String, String),
    #[error("blocking execution join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone)]
pub struct ResourceBudget {
    pub memory_limit_bytes: usize,
    pub wall_timeout: Duration,
    pub max_interrupts: u64,
    pub idle_ttl: Duration,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            memory_limit_bytes: 32 * 1024 * 1024, // 32 MiB per active Instance VM
            wall_timeout: Duration::from_secs(5),
            max_interrupts: 2_000_000,
            idle_ttl: Duration::from_secs(300),
        }
    }
}

/// Source bundle needed to boot an Instance's Luau VM.
#[derive(Debug, Clone)]
pub struct CompiledInstanceBundle {
    pub instance_id: String,
    pub manifest: DefinitionManifest,
    pub runtime_source: String,
    pub config_digest: String,
    /// Resolved package libraries allowed in `require(lib_name)`.
    pub resolved_libraries: BTreeMap<String, String>,
}

struct PooledVm {
    vm_uid: String,
    definition_digest: String,
    config_digest: String,
    last_used: Instant,
    /// Protected behind an async mutex so only one coroutine executes in a given VM at a time,
    /// while multiple Instances execute fully concurrently across blocking pool threads.
    lua: Arc<tokio::sync::Mutex<Lua>>,
}

#[derive(Clone)]
pub struct VmPool {
    vms: Arc<DashMap<String, PooledVm>>,
    budget: ResourceBudget,
    total_reclaimed: Arc<AtomicU64>,
}

impl VmPool {
    pub fn new(budget: ResourceBudget) -> Self {
        Self {
            vms: Arc::new(DashMap::new()),
            budget,
            total_reclaimed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn active_vm_count(&self) -> usize {
        self.vms.len()
    }

    pub fn total_reclaimed(&self) -> u64 {
        self.total_reclaimed.load(Ordering::Relaxed)
    }

    /// Returns the live VM UID for an instance (used in tests to prove Generation carry-over!).
    pub fn vm_uid(&self, instance_id: &str) -> Option<String> {
        self.vms.get(instance_id).map(|v| v.vm_uid.clone())
    }

    /// Synchronize pool on Generation activation (§12.2 Generation carry-over).
    /// Unchanged instances (same `definition_digest` + `config_digest`) keep their live VM!
    pub fn sync_generation(&self, desired: &[CompiledInstanceBundle]) -> Vec<String> {
        let mut desired_map: HashMap<&str, &CompiledInstanceBundle> = HashMap::new();
        for b in desired {
            desired_map.insert(b.instance_id.as_str(), b);
        }

        let mut dropped = Vec::new();
        let existing_keys: Vec<String> = self.vms.iter().map(|e| e.key().clone()).collect();
        for inst_id in existing_keys {
            match desired_map.get(inst_id.as_str()) {
                None => {
                    self.vms.remove(&inst_id);
                    dropped.push(inst_id);
                }
                Some(bundle) => {
                    let unchanged = self
                        .vms
                        .get(&inst_id)
                        .map(|v| {
                            v.definition_digest == bundle.manifest.identity_version
                                && v.config_digest == bundle.config_digest
                        })
                        .unwrap_or(false);
                    if !unchanged {
                        self.vms.remove(&inst_id);
                        dropped.push(inst_id);
                    }
                }
            }
        }
        dropped
    }

    /// Reclaim VMs idle longer than `idle_ttl` (or custom override).
    pub fn reclaim_idle(&self, ttl_override: Option<Duration>) -> usize {
        let ttl = ttl_override.unwrap_or(self.budget.idle_ttl);
        let now = Instant::now();
        let to_remove: Vec<String> = self
            .vms
            .iter()
            .filter(|e| now.duration_since(e.value().last_used) >= ttl)
            .map(|e| e.key().clone())
            .collect();
        let count = to_remove.len();
        for k in to_remove {
            if self.vms.remove(&k).is_some() {
                self.total_reclaimed.fetch_add(1, Ordering::Relaxed);
            }
        }
        count
    }

    /// Evict a single instance VM (e.g., on unrecoverable runtime fault or circuit open).
    pub fn evict(&self, instance_id: &str) {
        self.vms.remove(instance_id);
    }

    fn get_or_spawn_vm(&self, bundle: &CompiledInstanceBundle) -> Result<Arc<tokio::sync::Mutex<Lua>>, RuntimeError> {
        if let Some(mut entry) = self.vms.get_mut(&bundle.instance_id) {
            if entry.definition_digest == bundle.manifest.identity_version
                && entry.config_digest == bundle.config_digest
            {
                entry.last_used = Instant::now();
                return Ok(Arc::clone(&entry.lua));
            }
        }

        let lua = build_sandboxed_vm(bundle, &self.budget)?;
        let vm_arc = Arc::new(tokio::sync::Mutex::new(lua));
        self.vms.insert(
            bundle.instance_id.clone(),
            PooledVm {
                vm_uid: format!("vm_{}", Uuid::now_v7().simple()),
                definition_digest: bundle.manifest.identity_version.clone(),
                config_digest: bundle.config_digest.clone(),
                last_used: Instant::now(),
                lua: Arc::clone(&vm_arc),
            },
        );
        Ok(vm_arc)
    }

    /// Execute a handler function (`module[handler_name](ctx)`) inside the Instance's sandboxed VM
    /// on a Tokio blocking pool thread so CPU-bound/looping Luau NEVER blocks the async event loop.
    pub async fn execute_handler(
        &self,
        bundle: &CompiledInstanceBundle,
        handler_name: &str,
        ctx_cfg: ExecutionContextConfig,
        cancel_flag: Option<Arc<AtomicBool>>,
    ) -> Result<serde_json::Value, RuntimeError> {
        let vm_lock = self.get_or_spawn_vm(bundle)?;
        let budget = self.budget.clone();
        let handler = handler_name.to_string();
        let def_id = bundle.manifest.id.clone();
        let runtime_source = bundle.runtime_source.clone();
        let handle = tokio::runtime::Handle::current();

        let res = tokio::task::spawn_blocking(move || {
            handle.block_on(async move {
                let lua = vm_lock.lock().await;
                let started = Instant::now();
                let ticks = Arc::new(AtomicU64::new(0));
                let ticks_ref = Arc::clone(&ticks);
                let timed_out = Arc::new(AtomicBool::new(false));
                let timed_out_ref = Arc::clone(&timed_out);
                let cancelled = cancel_flag.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
                let cancelled_ref = Arc::clone(&cancelled);
                let wall_timeout = budget.wall_timeout;
                let max_interrupts = budget.max_interrupts;

                lua.set_interrupt(move |_| {
                    if cancelled_ref.load(Ordering::Relaxed) {
                        return Err(mlua::Error::runtime("DASH_CANCELLED"));
                    }
                    let count = ticks_ref.fetch_add(1, Ordering::Relaxed);
                    if started.elapsed() > wall_timeout || count > max_interrupts {
                        timed_out_ref.store(true, Ordering::Relaxed);
                        return Err(mlua::Error::runtime("DASH_TIMEOUT"));
                    }
                    Ok(VmState::Continue)
                });

                // Load or retrieve cached module table
                let mod_val: Value = match lua.globals().get::<Value>("__dash_module")? {
                    Value::Table(t) => Value::Table(t),
                    _ => {
                        let evaluated: Value = lua
                            .load(&runtime_source)
                            .set_name(format!("{def_id}/runtime.luau"))
                            .eval()?;
                        lua.globals().set("__dash_module", evaluated.clone())?;
                        evaluated
                    }
                };

                let mod_tbl = match mod_val {
                    Value::Table(t) => t,
                    _ => {
                        return Err(RuntimeError::Luau(mlua::Error::runtime(
                            "runtime.luau must return a module table",
                        )))
                    }
                };

                let func: Function = mod_tbl.get(handler.as_str()).map_err(|_| {
                    RuntimeError::HandlerNotFound(def_id.clone(), handler.clone())
                })?;

                let ctx_ud = lua.create_userdata(LuaContext { cfg: ctx_cfg })?;
                let exec_res = func.call_async::<Value>(ctx_ud).await;

                // Clear interrupt after execution
                lua.set_interrupt(|_| Ok(VmState::Continue));

                match exec_res {
                    Ok(val) => {
                        let json_val: serde_json::Value = lua.from_value(val)?;
                        Ok(json_val)
                    }
                    Err(e) => {
                        if timed_out.load(Ordering::Relaxed) || e.to_string().contains("DASH_TIMEOUT") {
                            Err(RuntimeError::Timeout(wall_timeout))
                        } else if e.to_string().contains("DASH_CANCELLED") {
                            Err(RuntimeError::Cancelled)
                        } else if matches!(e, mlua::Error::MemoryError(_)) {
                            Err(RuntimeError::MemoryExceeded(budget.memory_limit_bytes))
                        } else {
                            Err(RuntimeError::Luau(e))
                        }
                    }
                }
            })
        })
        .await?;

        // Evict VM on timeout/memory exhaustion so clean state is guaranteed next time
        if matches!(
            res,
            Err(RuntimeError::Timeout(_)) | Err(RuntimeError::MemoryExceeded(_))
        ) {
            self.evict(&bundle.instance_id);
        } else if let Some(mut entry) = self.vms.get_mut(&bundle.instance_id) {
            entry.last_used = Instant::now();
        }

        res
    }
}

fn build_sandboxed_vm(
    bundle: &CompiledInstanceBundle,
    budget: &ResourceBudget,
) -> Result<Lua, RuntimeError> {
    // Exclude OS, IO, DEBUG, PACKAGE, JIT, FFI; expose only safe deterministic + coroutine stdlibs
    let libs = StdLib::COROUTINE
        | StdLib::TABLE
        | StdLib::STRING
        | StdLib::UTF8
        | StdLib::BIT
        | StdLib::MATH
        | StdLib::BUFFER
        | StdLib::VECTOR;
    let lua = Lua::new_with(libs, LuaOptions::default())?;
    lua.set_memory_limit(budget.memory_limit_bytes)?;

    let dash_sdk = build_dash_runtime_sdk(&lua)?;
    let libs_map = bundle.resolved_libraries.clone();
    let loaded_libs: Arc<std::sync::Mutex<HashMap<String, mlua::RegistryKey>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));

    let require_fn = lua.create_function(move |lua, name: String| -> mlua::Result<Value> {
        if name == "dash" {
            return Ok(Value::Table(dash_sdk.clone()));
        }
        if let Some(key) = loaded_libs.lock().unwrap().get(&name) {
            return lua.registry_value(key);
        }
        let Some(src) = libs_map.get(&name) else {
            return Err(mlua::Error::runtime(format!(
                "SANDBOX_DENIED: module '{name}' is not a resolved package library"
            )));
        };
        let evaluated: Value = lua.load(src).set_name(format!("lib/{name}.luau")).eval()?;
        let reg_key = lua.create_registry_value(evaluated.clone())?;
        loaded_libs.lock().unwrap().insert(name, reg_key);
        Ok(evaluated)
    })?;
    lua.globals().set("require", require_fn)?;
    lua.sandbox(true)?;

    Ok(lua)
}
