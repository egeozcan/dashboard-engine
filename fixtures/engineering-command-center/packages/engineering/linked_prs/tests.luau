local dash = require("dash")
local Runtime = require("runtime")
local pr_utils = require("engineering.pr_utils")

dash.test("extracts Jira keys and handles missing Jira key gracefully", function(make_ctx)
    local _ = make_ctx({ query = { repository = "acme/platform" } })
    dash.assert_eq(pr_utils.extract_jira_key("DEV-123 Improve caching"), "DEV-123", "extract key")
    dash.assert(pr_utils.extract_jira_key("Refactor README") == nil, "missing key should be nil")
    dash.assert(type(Runtime.query) == "function", "query handler exists")
end)

dash.test("joins Mock-backed PRs with Mock-backed Jira issues", function(make_ctx)
    local ctx = make_ctx({ query = { repository = "acme/platform", state = "open" } })
    local rows = Runtime.query(ctx)
    dash.assert_eq(#rows, 3, "three open acme/platform PRs")
    local by_number = {}
    for _, row in ipairs(rows) do
        by_number[row.number] = row
    end
    dash.assert_eq(by_number[42].jira_key, "DEV-123", "PR 42 links DEV-123")
    dash.assert_eq(by_number[42].jira_status, "In Review", "DEV-123 status from Jira Mock")
    dash.assert_eq(by_number[44].jira_status, "Unlinked", "PR 44 has no Jira key")
end)
