local dash = require("dash")

return dash.mock {
    id = "dash.weather_mock",
    ["for"] = "dash.weather",
    description = "Serve configured HTTP fixtures for dash.weather in Preview and tests",
    runtime = "runtime.luau",
    handler = "mock_call",
    libraries = { "dash.http_util" },
}
