local dash = require("dash")
local Runtime = require("runtime")

dash.test("search mixes issues and pull requests and honours qualifiers", function(make_ctx)
    local prs = Runtime.query(make_ctx({ query = { query = "repo:acme/platform is:pr is:open" } }))
    dash.assert_eq(#prs, 3, "three open platform PRs")
    for _, r in ipairs(prs) do
        dash.assert(r.is_pull_request == true, "pull requests only")
    end
    local bugs = Runtime.query(make_ctx({ query = { query = "label:bug is:issue is:open" } }))
    dash.assert_eq(#bugs, 2, "open bug issues across repositories")
    local text = Runtime.query(make_ctx({ query = { query = "refund" } }))
    dash.assert_eq(#text, 1, "free text matches titles")
    dash.assert_eq(text[1].repository, "acme/payments", "repository derived")
end)
