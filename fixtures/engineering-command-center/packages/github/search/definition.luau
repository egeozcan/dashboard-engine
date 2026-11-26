local dash = require("dash")

return dash.data_provider {
    id = "github.search",
    description = "Issues and pull requests matching a GitHub search query (e.g. 'org:acme is:pr is:open review-requested:octocat')",
    runtime = "runtime.luau",
    libraries = { "github.model" },
    handler = "query",
    inputs = {
        github = dash.connector_ref("github.connector", { required_capabilities = { "issues.read" } }),
    },
    query = dash.object {
        query = dash.string(),
        sort = dash.optional(dash.enum { "created", "updated", "comments", "reactions", "interactions" }),
        order = dash.optional(dash.enum { "asc", "desc" }),
        max_pages = dash.optional(dash.integer()),
    },
    output = dash.list("dev.issue@1"),
    -- The search API allows 30 requests per minute.
    updates = { on_demand = true, subscribe = true, poll_interval_secs = 300 },
}
