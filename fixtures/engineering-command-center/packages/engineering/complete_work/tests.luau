local dash = require("dash")
local Runtime = require("runtime")

dash.test("skips when no linked jira issue key exists", function(make_ctx)
    local ctx = make_ctx({
        event = {
            event_id = "evt_1",
            payload = { repository = "acme/platform", number = 44 },
        },
    })
    local res = Runtime.run(ctx)
    dash.assert(res.skipped == true, "expected skip when issue_key missing")
end)
