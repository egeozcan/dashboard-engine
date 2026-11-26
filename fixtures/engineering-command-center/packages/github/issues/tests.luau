local dash = require("dash")
local Runtime = require("runtime")

dash.test("lists open issues of a repository from the github Mock", function(make_ctx)
    local rows = Runtime.query(make_ctx({ query = { repository = "acme/platform" } }))
    dash.assert_eq(#rows, 2, "two open acme/platform issues")
    for _, r in ipairs(rows) do
        dash.assert(r.is_pull_request == false, "pull requests excluded")
        dash.assert_eq(r.state, "open", "state filter")
    end
end)

dash.test("filters by label and state", function(make_ctx)
    local rows = Runtime.query(make_ctx({ query = { repository = "acme/platform", state = "all", labels = { "bug" } } }))
    dash.assert_eq(#rows, 1, "one bug")
    dash.assert_eq(rows[1].number, 101, "issue 101")
    dash.assert_eq(rows[1].id, "acme/platform#101", "stable id")
end)
