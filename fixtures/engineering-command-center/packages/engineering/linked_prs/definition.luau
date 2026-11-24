local dash = require("dash")

return dash.data_provider {
    id = "engineering.linked_prs",
    description = "Joins GitHub pull requests with linked Jira issues",
    runtime = "runtime.luau",
    handler = "query",
    libraries = { "engineering.pr_utils" },
    inputs = {
        prs = dash.data_ref("github.pull_requests"),
        issues = dash.data_ref("jira.issues_by_keys"),
    },
    query = dash.object {
        repository = dash.string(),
        state = dash.optional(dash.enum { "open", "closed", "merged", "all" }),
    },
    output = dash.list(dash.semantic("engineering.linked_pr@1", dash.object {
        id = dash.string(),
        number = dash.integer(),
        title = dash.string(),
        repository = dash.string(),
        state = dash.string(),
    })),
    updates = {
        on_demand = true,
        subscribe = true,
    },
}
