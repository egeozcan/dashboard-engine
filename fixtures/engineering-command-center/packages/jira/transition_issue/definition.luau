local dash = require("dash")

return dash.action_provider {
    id = "jira.transition_issue",
    description = "Transitions a Jira issue to a target status or status category through its workflow (idempotent), with Dry run and reconcile",
    runtime = "runtime.luau",
    handler = "execute",
    dry_run = "dry_run",
    reconcile = "reconcile",
    libraries = { "jira.model" },
    inputs = {
        jira = dash.connector_ref("jira.connector", {
            required_capabilities = { "issues.read", "issues.transition" },
        }),
    },
    args = dash.object {
        key = dash.string(),
        -- A status name, or a status category (todo | in_progress | done); with neither, the
        -- issue moves to a `done` status, whatever the workflow calls it.
        status = dash.optional(dash.string()),
        status_category = dash.optional(dash.enum { "todo", "in_progress", "done" }),
        comment = dash.optional(dash.string()),
        resolution = dash.optional(dash.string()),
    },
    result = dash.object {
        key = dash.string(),
        status = dash.string(),
        transitioned = dash.boolean(),
    },
    invalidates = { "jira.issues_by_keys", "jira.search" },
}
