local dash = require("dash")

return dash.data_provider {
    id = "jira.issues_by_keys",
    description = "Fetches Jira issues by issue key list",
    runtime = "runtime.luau",
    handler = "query",
    inputs = {
        jira = dash.connector_ref("jira.connector", {
            required_capabilities = { "issues.read" },
        }),
    },
    query = dash.object {
        keys = dash.list(dash.string()),
    },
    output = dash.list(dash.semantic("pm.issue@1", dash.object {
        key = dash.string(),
        summary = dash.string(),
        status = dash.string(),
    })),
    updates = {
        on_demand = true,
        subscribe = true,
    },
}
