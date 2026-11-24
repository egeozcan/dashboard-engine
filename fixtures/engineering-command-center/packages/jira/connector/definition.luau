local dash = require("dash")

return dash.connector {
    id = "jira.connector",
    description = "Jira REST API connector",
    runtime = "runtime.luau",
    handler = "call",
    credentials = { "auth" },
    capabilities = { "issues.read", "issues.transition" },
    operations = {
        get_issues = {
            description = "Fetch issues by keys",
            required_capabilities = { "issues.read" },
            args = dash.object {
                keys = dash.list(dash.string()),
            },
            result = dash.list("pm.issue@1"),
        },
        transition_issue = {
            description = "Transition a Jira issue to a target status",
            required_capabilities = { "issues.transition" },
            args = dash.object {
                key = dash.string(),
                status = dash.optional(dash.string()),
            },
            result = dash.object {
                key = dash.string(),
                status = dash.string(),
                transitioned = dash.boolean(),
            },
        },
    },
}
