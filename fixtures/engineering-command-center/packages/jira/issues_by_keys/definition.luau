local dash = require("dash")

return dash.data_provider {
    id = "jira.issues_by_keys",
    description = "Fetches Jira issues by issue key list (unknown keys are skipped)",
    runtime = "runtime.luau",
    libraries = { "jira.model" },
    handler = "query",
    inputs = {
        jira = dash.connector_ref("jira.connector", {
            required_capabilities = { "issues.read" },
        }),
    },
    query = dash.object {
        keys = dash.list(dash.string()),
    },
    output = dash.list("pm.issue@1"),
    updates = {
        on_demand = true,
        subscribe = true,
    },
}
