# Luau VM Scheduling, Coroutine Bridge, and Resource Quotas

## Decision

Each active Luau Instance runs inside an isolated `mlua` Luau state (`StdLib::COROUTINE | TABLE | STRING | UTF8 | BIT | MATH | BUFFER | VECTOR`) with `lua.sandbox(true)` and per-VM memory limits (`set_memory_limit`) plus periodic interrupt checks (`set_interrupt`) enforcing wall-clock deadlines, instruction counts, and cancellation flags.

All Luau handler invocations execute on Tokio's blocking thread pool (`tokio::task::spawn_blocking`) driving `Handle::current().block_on(func.call_async(...))`, while asynchronous host operations (`ctx:await(handle)`, `ctx:await_all(list)`) suspend the Luau coroutine and await Tokio futures without occupying async event-loop worker threads.

## Consequences

- Runaway infinite loops or memory-exhausting scripts cannot stall `/healthz`, WebSocket heartbeats, or unrelated providers.
- On timeout or memory violation, the affected Instance VM is evicted cleanly from `VmPool` and repeated failures open a per-Instance circuit breaker.
- Idle VMs past `idle_ttl` are reclaimed periodically by background workers, while unchanged `(definition_digest, config_digest)` Instances carry over their live VMs across Generation swaps.
- An external subprocess worker model remains reserved for future hostile-package isolation beyond MVP (§3.2).
