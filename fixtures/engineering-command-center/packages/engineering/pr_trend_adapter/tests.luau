local dash = require("dash")
local Runtime = require("runtime")

dash.test("pr_trend_adapter exports render", function(make_ctx)
    local _ = make_ctx({})
    dash.assert(type(Runtime.render) == "function", "render missing")
end)
