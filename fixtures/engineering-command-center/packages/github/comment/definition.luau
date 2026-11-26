local dash = require("dash")

return dash.action_provider {
    id = "github.comment",
    description = "Comments on a GitHub issue or pull request; the Dry run checks that the target exists",
    runtime = "runtime.luau",
    handler = "execute",
    dry_run = "dry_run",
    inputs = {
        github = dash.connector_ref("github.connector", { required_capabilities = { "issues.read", "issues.write" } }),
    },
    args = dash.object {
        repository = dash.string(),
        number = dash.integer(),
        body = dash.string(),
    },
    result = dash.object {
        id = dash.integer(),
        url = dash.optional(dash.string()),
    },
    invalidates = { "github.issues" },
}
