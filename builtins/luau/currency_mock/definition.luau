local dash = require("dash")

return dash.mock {
    id = "dash.currency_mock",
    ["for"] = "dash.currency",
    description = "Serve configured HTTP fixtures for dash.currency in Preview and tests",
    runtime = "runtime.luau",
    handler = "mock_call",
    libraries = { "dash.http_util" },
}
