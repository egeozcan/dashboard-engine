local dash = require("dash")

return dash.action_provider {
    id = "github.merge_pr",
    description = "Merges a pull request with Dry run, reconcile, durable Event emission, and query invalidation",
    runtime = "runtime.luau",
    handler = "execute",
    dry_run = "dry_run",
    reconcile = "reconcile",
    libraries = { "github.model" },
    inputs = {
        github = dash.connector_ref("github.connector", {
            required_capabilities = { "pull_requests.read", "pull_requests.merge" },
        }),
    },
    args = dash.object {
        repository = dash.string(),
        number = dash.integer(),
        issue_key = dash.optional(dash.string()),
        merge_method = dash.optional(dash.enum { "merge", "squash", "rebase" }),
        -- Merge only this head commit (e.g. the head_sha the Dry run showed); default: the head
        -- read right before merging.
        expected_head_sha = dash.optional(dash.string()),
    },
    result = dash.object {
        merged = dash.boolean(),
        sha = dash.string(),
    },
    emits = {
        { semantic_type = "github.pr_merged", schema_version = "1", durable = true },
    },
    -- Declares the affected data-provider Definitions (not Workspace Instance names); every
    -- Instance of them, and everything downstream of those (e.g. linked PR joins), is invalidated.
    invalidates = { "github.pull_requests", "github.search" },
}
