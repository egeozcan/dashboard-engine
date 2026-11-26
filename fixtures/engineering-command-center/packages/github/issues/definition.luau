local dash = require("dash")

return dash.data_provider {
    id = "github.issues",
    description = "Lists issues of a repository (pull requests excluded), filtered by state, labels, assignee or creator",
    runtime = "runtime.luau",
    handler = "query",
    inputs = {
        github = dash.connector_ref("github.connector", { required_capabilities = { "issues.read" } }),
    },
    query = dash.object {
        repository = dash.string(),
        state = dash.optional(dash.enum { "open", "closed", "all" }),
        labels = dash.optional(dash.list(dash.string())),
        assignee = dash.optional(dash.string()),
        creator = dash.optional(dash.string()),
    },
    output = dash.list("dev.issue@1"),
    updates = { on_demand = true, subscribe = true, poll_interval_secs = 120 },
}
