local dash = require("dash")

return dash.action_provider {
    id = "jira.add_comment",
    description = "Adds a comment to a Jira issue; the Dry run checks that the issue exists",
    runtime = "runtime.luau",
    handler = "execute",
    dry_run = "dry_run",
    inputs = {
        jira = dash.connector_ref("jira.connector", { required_capabilities = { "issues.read", "issues.write" } }),
    },
    args = dash.object {
        key = dash.string(),
        body = dash.string(),
    },
    result = dash.object {
        id = dash.string(),
        key = dash.string(),
    },
}
