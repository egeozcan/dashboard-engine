local dash = require("dash")

local location = dash.object {
    latitude = dash.number(),
    longitude = dash.number(),
    altitude = dash.optional(dash.integer()),
}

local interval = dash.optional(dash.object {
    summary = dash.optional(dash.object { symbol_code = dash.string() }),
    details = dash.optional(dash.map(dash.number())),
})

local period = dash.object {
    time = dash.string(),
    details = dash.map(dash.number()),
    next_1_hours = interval,
    next_6_hours = interval,
    next_12_hours = interval,
}

local metadata = {
    updated_at = dash.string(),
    location = dash.object {
        latitude = dash.number(), longitude = dash.number(), altitude = dash.optional(dash.number()),
    },
    units = dash.map(dash.string()),
    attribution = dash.string(),
    license_url = dash.string(),
}
local forecast = table.clone(metadata)
forecast.timeseries = dash.list(period)
local current = table.clone(metadata)
current.period = period

return dash.connector {
    id = "dash.weather",
    description = "Worldwide point forecasts from MET Norway Locationforecast 2.0",
    runtime = "runtime.luau",
    handler = "call",
    libraries = { "dash.http_util" },
    capabilities = { "forecast.read" },
    config = dash.object {
        user_agent = dash.optional(dash.string()),
        base_url = dash.optional(dash.string()),
        mock_now = dash.optional(dash.string()),
        mock = dash.optional(dash.map(dash.any())),
    },
    operations = {
        forecast = {
            description = "Read the compact forecast time series for a latitude, longitude and optional altitude in metres",
            mode = "read", required_capabilities = { "forecast.read" },
            args = location, result = dash.object(forecast),
        },
        current = {
            description = "Read the forecast period for the current time at a location",
            mode = "read", required_capabilities = { "forecast.read" },
            args = location, result = dash.object(current),
        },
    },
}
