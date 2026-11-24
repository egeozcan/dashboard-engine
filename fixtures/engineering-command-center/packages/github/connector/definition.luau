local dash = require("dash")

return dash.connector {
    id = "github.connector",
    description = "GitHub REST API connector",
    runtime = "runtime.luau",
    handler = "call",
    credentials = { "auth" },
    capabilities = { "pull_requests.read", "pull_requests.merge" },
    operations = {
        list_prs = {
            description = "List pull requests for a repository",
            required_capabilities = { "pull_requests.read" },
            args = dash.object {
                repository = dash.string(),
                state = dash.optional(dash.string()),
            },
            result = dash.list("dev.pull_request@1"),
        },
        get_pr = {
            description = "Get single pull request state",
            required_capabilities = { "pull_requests.read" },
            args = dash.object {
                repository = dash.string(),
                number = dash.integer(),
            },
            result = dash.semantic("dev.pull_request@1"),
        },
        merge_pr = {
            description = "Merge a pull request",
            required_capabilities = { "pull_requests.merge" },
            args = dash.object {
                repository = dash.string(),
                number = dash.integer(),
            },
            result = dash.object {
                merged = dash.boolean(),
                sha = dash.string(),
            },
        },
    },
}
