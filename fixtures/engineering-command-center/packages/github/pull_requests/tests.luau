local dash = require("dash")
local Runtime = require("runtime")

dash.test("filters open PRs by repository via the github Mock", function(make_ctx)
    local ctx = make_ctx({
        query = { repository = "acme/platform", state = "open" },
    })
    local prs = Runtime.query(ctx)
    dash.assert_eq(#prs, 3, "open acme/platform PRs served by the Mock fixture")
    for _, pr in ipairs(prs) do
        dash.assert_eq(pr.repository, "acme/platform", "repository filter")
        dash.assert_eq(pr.state, "open", "state filter")
    end
end)

dash.test("other repositories are filtered out", function(make_ctx)
    local ctx = make_ctx({ query = { repository = "acme/payments" } })
    local prs = Runtime.query(ctx)
    dash.assert_eq(#prs, 1, "one open acme/payments PR")
    dash.assert_eq(prs[1].number, 7, "payments PR number")
end)
