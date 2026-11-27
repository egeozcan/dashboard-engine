local dash = require("dash")
local Runtime = require("runtime")

dash.test("Expires and the global refresh interval both constrain revalidation", function(_)
    local old = "Mon, 02 Nov 2026 04:00:00 GMT"
    local expires = "Mon, 02 Nov 2026 10:00:00 GMT"
    local stored = {}
    local sent = {}
    local body = {
        geometry = { coordinates = { 10, 60 } },
        properties = {
            meta = { updated_at = "2026-11-02T03:00:00Z", units = { air_temperature = "celsius" } },
            timeseries = { { time = "2026-11-02T04:00:00Z", data = { instant = { details = { air_temperature = 7 } } } } },
        },
    }
    local ctx = {
        args = { longitude = 10, latitude = 60 }, operation = "forecast",
        config = { user_agent = "WeatherTest/1 https://example.com/contact" },
        now = "2026-11-02T04:00:00Z",
        await = function(_, value) return value end,
        state = {
            get = function(_, key) return stored[key] end,
            set = function(_, key, value) stored[key] = value end,
        },
        http = { request = function(_, req)
            table.insert(sent, req)
            if #sent == 1 then
                return { status = 200, headers = { ["last-modified"] = old, expires = expires }, body = body }
            end
            return { status = 304, headers = {} }
        end },
    }
    local forecast = Runtime.call(ctx)
    dash.assert_eq(forecast.timeseries[1].details.air_temperature, 7, "parsed forecast")
    ctx.now = "2026-11-02T09:59:59Z"
    Runtime.call(ctx)
    dash.assert_eq(#sent, 1, "Expires extends the minimum interval")
    ctx.now = "2026-11-02T10:00:00Z"
    dash.assert_eq(Runtime.call(ctx), forecast, "304 reuses the previous payload")
    dash.assert_eq(sent[2].headers["If-Modified-Since"], old, "revalidate with the original validator")
    ctx.now = "2026-11-02T12:59:59Z"
    Runtime.call(ctx)
    dash.assert_eq(#sent, 2, "minimum interval applies again after a 304")
    ctx.now = "2026-11-02T13:00:00Z"
    Runtime.call(ctx)
    dash.assert_eq(sent[3].headers["If-Modified-Since"], old, "304 can omit Last-Modified")
end)

dash.test("a malformed upstream period cannot poison the persistent cache", function(_)
    local ctx = {
        operation = "forecast",
        now = "2026-11-02T05:00:00Z",
        args = { latitude = 60, longitude = 10 },
        config = { user_agent = "WeatherTest/1 https://example.com/contact" },
        await = function(_, value) return value end,
        state = {
            get = function() return nil end,
            set = function() error("unexpected cache write") end,
        },
        http = { request = function()
            return { status = 200, body = {
                geometry = { coordinates = { 10, 60 } },
                properties = {
                    meta = { updated_at = "2026-11-02T04:00:00Z", units = {} },
                    timeseries = { { time = "2026-11-02T05:00:00Z", data = { instant = { details = { air_temperature = "cold" } } } } },
                },
            } }
        end },
    }
    local ok, err = pcall(Runtime.call, ctx)
    dash.assert(not ok and string.find(tostring(err), "INVALID_RESPONSE", 1, true) ~= nil, tostring(err))
end)
