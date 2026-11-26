local dash = require("dash")

return dash.data_provider {
    id = "github.workflow_runs",
    description = "Recent GitHub Actions workflow runs of a repository, optionally for one workflow, branch, status or event",
    runtime = "runtime.luau",
    libraries = { "github.model" },
    handler = "query",
    inputs = {
        github = dash.connector_ref("github.connector", { required_capabilities = { "actions.read" } }),
    },
    query = dash.object {
        repository = dash.string(),
        workflow = dash.optional(dash.string()),
        branch = dash.optional(dash.string()),
        status = dash.optional(dash.string()),
        event = dash.optional(dash.string()),
    },
    output = dash.list("dev.workflow_run@1"),
    updates = { on_demand = true, subscribe = true, poll_interval_secs = 60 },
}
