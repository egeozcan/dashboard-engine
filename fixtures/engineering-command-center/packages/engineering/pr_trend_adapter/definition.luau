local dash = require("dash")

return dash.adapter {
    id = "engineering.pr_trend_adapter",
    description = "Renders open PR count KPI and 30-day trend chart",
    runtime = "runtime.luau",
    handler = "render",
    inputs = {
        prs = dash.data_ref("github.pull_requests"),
        history = dash.data_ref("dash.history_series"),
    },
    dashboard_state_reads = { "selected_repository" },
}
