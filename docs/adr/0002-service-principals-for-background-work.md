# Background work runs under explicit Service principals

Automations, history sampling, keep-warm polling and durable event consumers have no initiating human or agent request, so each names an administrator-granted Service principal in configuration and runs under that principal's capability ceiling. There is no implicit "system" identity.

## Considered Options

- **Inherit the Principal that activated the draft** — rejected: revoking an Agent token would silently break automations, and authority would be frozen to whoever happened to activate.
- **Run as the Administrator** — rejected: violates deny-by-default and makes any agent-authored automation an escalation path.

## Consequences

Agents may request a Service principal grant in a draft, but only the Administrator can approve it (consistent with the agent ceiling rule). Background queries carry the Service principal's authorization fingerprint, so their cache entries are not shared with interactive queries of a different Principal.
