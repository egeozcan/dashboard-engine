local dash = require("dash")
local Runtime = require("runtime")

dash.test("empty key list returns empty array without connector call", function(make_ctx)
    local ctx = make_ctx({ query = { keys = {} } })
    local res = Runtime.query(ctx)
    dash.assert_eq(#res, 0, "expected empty array")
end)
