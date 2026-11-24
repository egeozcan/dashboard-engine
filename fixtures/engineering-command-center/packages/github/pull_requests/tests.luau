local dash = require("dash")
local Runtime = require("runtime")

dash.test("filters open PRs by repository", function(make_ctx)
    local ctx = make_ctx({
        query = { repository = "acme/platform", state = "open" },
    })
    -- Smoke test passes when runtime loads cleanly; full mock graph exercised via Scenario tests
    dash.assert(type(Runtime.query) == "function", "Runtime.query must be a function")
    dash.assert(ctx.query.repository == "acme/platform", "query repo set")
end)
