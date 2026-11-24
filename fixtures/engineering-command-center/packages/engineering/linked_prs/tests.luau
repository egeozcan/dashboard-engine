local dash = require("dash")
local Runtime = require("runtime")
local pr_utils = require("engineering.pr_utils")

dash.test("pr_utils extracts Jira keys and tolerates titles without one", function(_make_ctx)
    dash.assert_eq(pr_utils.extract_jira_key("DEV-123 Improve caching"), "DEV-123", "extract key")
    dash.assert_eq(pr_utils.extract_jira_key("Fix PAY-9 idempotency"), "PAY-9", "key anywhere in the title")
    dash.assert(pr_utils.extract_jira_key("Refactor README") == nil, "missing key should be nil")
    dash.assert(pr_utils.extract_jira_key(nil) == nil, "non-string title")
end)

dash.test("repository parameter selects the joined rows", function(make_ctx)
    local rows = Runtime.query(make_ctx({ query = { repository = "acme/payments" } }))
    dash.assert_eq(#rows, 1, "one open acme/payments PR")
    dash.assert_eq(rows[1].number, 7, "payments PR")
    dash.assert_eq(rows[1].jira_key, "PAY-9", "payments Jira key")
    dash.assert_eq(rows[1].jira_status, "In Review", "PAY-9 status from the Jira Mock")
    dash.assert_eq(rows[1].issue.priority, "Critical", "joined issue record")
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
