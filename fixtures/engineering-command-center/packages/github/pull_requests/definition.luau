local dash = require("dash")

return dash.data_provider {
    id = "github.pull_requests",
    description = "Fetches pull requests and emits github.pr_merged on external merges detected during polling",
    runtime = "runtime.luau",
    handler = "query",
    libraries = { "github.model" },
    inputs = {
        github = dash.connector_ref("github.connector", {
            required_capabilities = { "pull_requests.read" },
        }),
    },
    query = dash.object {
        repository = dash.string(),
        state = dash.optional(dash.enum { "open", "closed", "merged", "all" }),
        base = dash.optional(dash.string()),
    },
    output = dash.list("dev.pull_request@1"),
    emits = {
        { semantic_type = "github.pr_merged", schema_version = "1", durable = true },
    },
    updates = {
        on_demand = true,
        subscribe = true,
        poll_interval_secs = 60,
    },
}
