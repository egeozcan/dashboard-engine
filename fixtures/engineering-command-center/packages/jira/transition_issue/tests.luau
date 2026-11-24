local dash = require("dash")
local Runtime = require("runtime")

dash.test("dry_run returns target status Done by default", function(make_ctx)
    local ctx = make_ctx({ args = { key = "DEV-123" } })
    local res = Runtime.dry_run(ctx)
    dash.assert(res.feasible == true, "expected feasible")
    dash.assert_eq(res.effects.target_status, "Done", "default status Done")
end)
