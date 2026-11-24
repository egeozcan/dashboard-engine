local dash = require("dash")

return dash.action_provider {
    id = "github.merge_pr",
    description = "Merges a pull request with Dry run, reconcile, durable Event emission, and query invalidation",
    runtime = "runtime.luau",
    handler = "execute",
    dry_run = "dry_run",
    reconcile = "reconcile",
    inputs = {
        github = dash.connector_ref("github.connector", {
            required_capabilities = { "pull_requests.read", "pull_requests.merge" },
        }),
    },
    args = dash.object {
        repository = dash.string(),
        number = dash.integer(),
        issue_key = dash.optional(dash.string()),
    },
    result = dash.object {
        merged = dash.boolean(),
        sha = dash.string(),
    },
    emits = {
        { semantic_type = "github.pr_merged", schema_version = "1", durable = true },
    },
    invalidates = { "open_prs", "linked_prs" },
}
