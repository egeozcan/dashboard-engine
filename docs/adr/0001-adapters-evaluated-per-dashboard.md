# Adapters are evaluated per dashboard, not per client

An Adapter Instance is evaluated once per dashboard and its output is shared, cached and deduplicated like a Query result. Client-specific transforms (sorting, local filters, selection) happen in built-in widgets in the browser, fed by Client and Ephemeral State. Adapters hold no state of their own; they read Dashboard State and Query results only.

## Considered Options

- **Per (adapter, client) evaluation in Luau** — rejected: VM count and cache entries scale with open browser tabs, and client state would fragment shared computation.
- **Browser-side JS adapters** — rejected: splits domain-adjacent logic across two languages and moves composition out of the sandboxed, traced Luau runtime.

## Consequences

Anything that must differ per client cannot be expressed in an Adapter; it must be a widget prop driven by Client State.
