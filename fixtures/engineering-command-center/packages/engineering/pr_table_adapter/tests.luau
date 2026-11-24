local dash = require("dash")
local Runtime = require("runtime")

dash.test("render builds the PR table from Mock-backed linked PRs", function(make_ctx)
    local tree = Runtime.render(make_ctx({}))
    dash.assert_eq(tree.widget_type, "dash.container", "root container")
    local summary = tree.children[1]
    local tbl = tree.children[2]
    dash.assert_eq(summary.props.open_count, 3, "open PR count for the default repository")
    dash.assert_eq(#tbl.props.rows, 3, "table rows")
    dash.assert_eq(tbl.props.row_action.request, "merge_requested", "row action sends a Request")
end)

dash.test("on_request translates merge_requested to the merge action", function(make_ctx)
    local ctx = make_ctx({
        request = {
            widget_id = "pr_table_adapter_pr_table",
            request = "merge_requested",
            payload = { repository = "acme/platform", number = 42, issue_key = "DEV-123" },
        },
    })
    local out = Runtime.on_request(ctx)
    dash.assert_eq(out.action, "merge", "bound action slot")
    dash.assert_eq(out.args.number, 42, "PR number")
    dash.assert_eq(out.args.issue_key, "DEV-123", "issue key")
end)
