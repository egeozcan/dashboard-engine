local dash = require("dash")

return dash.action_provider {
    id = "github.create_issue",
    description = "Creates a GitHub issue; the Dry run validates the repository and shows the issue to be created",
    runtime = "runtime.luau",
    handler = "execute",
    dry_run = "dry_run",
    libraries = { "github.model" },
    inputs = {
        github = dash.connector_ref("github.connector", { required_capabilities = { "issues.write" } }),
    },
    args = dash.object {
        repository = dash.string(),
        title = dash.string(),
        body = dash.optional(dash.string()),
        labels = dash.optional(dash.list(dash.string())),
        assignees = dash.optional(dash.list(dash.string())),
    },
    result = dash.object {
        number = dash.integer(),
        url = dash.optional(dash.string()),
        id = dash.string(),
    },
    invalidates = { "github.issues", "github.search" },
}
