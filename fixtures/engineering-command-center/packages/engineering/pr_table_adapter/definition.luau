local dash = require("dash")

return dash.adapter {
    id = "engineering.pr_table_adapter",
    description = "Renders linked PR table and handles merge_requested widget Requests",
    runtime = "runtime.luau",
    handler = "render",
    on_request = "on_request",
    inputs = {
        linked = dash.data_ref("engineering.linked_prs"),
        merge = dash.action_ref("github.merge_pr"),
    },
    dashboard_state_reads = { "selected_repository" },
    accepts_requests = { "merge_requested", "refresh_requested" },
    requests = {
        merge_requested = dash.object {
            number = dash.integer(),
            repository = dash.optional(dash.string()),
            issue_key = dash.optional(dash.string()),
        },
        refresh_requested = dash.any(),
    },
}
