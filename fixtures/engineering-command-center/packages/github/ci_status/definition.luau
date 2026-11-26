local dash = require("dash")

return dash.data_provider {
    id = "github.ci_status",
    description = "Combined CI state (success, failure, pending, neutral or unknown) of a branch, tag or commit from commit statuses and check runs",
    runtime = "runtime.luau",
    libraries = { "github.model" },
    handler = "query",
    inputs = {
        github = dash.connector_ref("github.connector", { required_capabilities = { "checks.read" } }),
    },
    query = dash.object {
        repository = dash.string(),
        ref = dash.string(),
    },
    output = "dev.ci_status@1",
    updates = { on_demand = true, subscribe = true, poll_interval_secs = 60 },
}
