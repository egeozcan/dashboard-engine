local dash = require("dash")

return dash.data_provider {
    id = "jira.search",
    description = "Jira issues matching a JQL query (e.g. 'project = DEV AND statusCategory != Done ORDER BY priority DESC')",
    runtime = "runtime.luau",
    libraries = { "jira.model" },
    handler = "query",
    inputs = {
        jira = dash.connector_ref("jira.connector", { required_capabilities = { "issues.read" } }),
    },
    query = dash.object {
        jql = dash.string(),
        fields = dash.optional(dash.list(dash.string())),
        max_results = dash.optional(dash.integer()),
    },
    output = dash.list("pm.issue@1"),
    updates = { on_demand = true, subscribe = true, poll_interval_secs = 120 },
}
