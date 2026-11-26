local dash = require("dash")
local Runtime = require("runtime")

dash.test("workflow runs of a repository, filtered by status", function(make_ctx)
    local all = Runtime.query(make_ctx({ query = { repository = "acme/platform" } }))
    dash.assert_eq(#all, 3, "three platform runs")
    local failed = Runtime.query(make_ctx({ query = { repository = "acme/platform", status = "failure" } }))
    dash.assert_eq(#failed, 1, "one failed run")
    dash.assert_eq(failed[1].branch, "feat/delta-resync", "failing branch")
    dash.assert_eq(failed[1].actor, "bob", "actor")
end)

dash.test("the workflow filter takes a file name or id, like the API", function(make_ctx)
    local by_file = Runtime.query(make_ctx({ query = { repository = "acme/platform", workflow = "ci.yml" } }))
    dash.assert(#by_file >= 1, "runs of ci.yml")
    for _, r in ipairs(by_file) do
        dash.assert_eq(r.name, "CI", "only CI runs")
    end
    local by_name = Runtime.query(make_ctx({ query = { repository = "acme/platform", workflow = "CI" } }))
    dash.assert_eq(#by_name, 0, "a display name is not a workflow id")
    local nulls = Runtime.query(make_ctx({ query = dash.json.decode('{"repository":"acme/platform","branch":null}') }))
    dash.assert(#nulls >= 2, "a null branch does not filter")
end)
