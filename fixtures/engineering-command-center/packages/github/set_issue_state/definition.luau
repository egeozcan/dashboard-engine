local dash = require("dash")

return dash.action_provider {
    id = "github.set_issue_state",
    description = "Closes or reopens a GitHub issue (idempotent), with Dry run and reconcile",
    runtime = "runtime.luau",
    handler = "execute",
    dry_run = "dry_run",
    reconcile = "reconcile",
    inputs = {
        github = dash.connector_ref("github.connector", { required_capabilities = { "issues.read", "issues.write" } }),
    },
    args = dash.object {
        repository = dash.string(),
        number = dash.integer(),
        state = dash.enum { "open", "closed" },
        reason = dash.optional(dash.enum { "completed", "not_planned", "reopened" }),
    },
    result = dash.object {
        number = dash.integer(),
        state = dash.string(),
        changed = dash.boolean(),
    },
    invalidates = { "github.issues", "github.search" },
}
