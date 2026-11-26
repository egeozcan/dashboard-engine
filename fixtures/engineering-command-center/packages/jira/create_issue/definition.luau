local dash = require("dash")

return dash.action_provider {
    id = "jira.create_issue",
    description = "Creates a Jira issue; the Dry run validates the request and shows the issue to be created",
    runtime = "runtime.luau",
    handler = "execute",
    dry_run = "dry_run",
    inputs = {
        jira = dash.connector_ref("jira.connector", { required_capabilities = { "issues.write" } }),
    },
    args = dash.object {
        project = dash.string(),
        issue_type = dash.optional(dash.string()),
        summary = dash.string(),
        description = dash.optional(dash.string()),
        labels = dash.optional(dash.list(dash.string())),
        priority = dash.optional(dash.string()),
        assignee_id = dash.optional(dash.string()),
    },
    result = dash.object {
        key = dash.string(),
        url = dash.optional(dash.string()),
    },
    invalidates = { "jira.search" },
}
