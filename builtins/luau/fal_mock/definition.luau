local dash = require("dash")

return dash.mock {
    id = "dash.fal_mock",
    ["for"] = "dash.fal",
    description = "Serve configured HTTP fixtures for dash.fal in Preview and tests",
    runtime = "runtime.luau",
    handler = "mock_call",
    libraries = { "dash.http_util" },
}
