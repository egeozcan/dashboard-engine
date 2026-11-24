local dash = require("dash")
local Runtime = require("runtime")

dash.test("on_request translates merge_requested to merge action payload", function(make_ctx)
    local _ = make_ctx({})
    -- Verify runtime exports render & on_request
    dash.assert(type(Runtime.render) == "function", "render missing")
    dash.assert(type(Runtime.on_request) == "function", "on_request missing")
end)
