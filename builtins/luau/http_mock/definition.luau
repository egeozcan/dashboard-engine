local dash = require("dash")

return dash.mock {
    id = "dash.http_mock",
    ["for"] = "dash.http",
    description = "Mock for dash.http: serves the connector Instance's config.mock responses (\"METHOD /path\" keys) in Preview and tests",
    runtime = "runtime.luau",
    handler = "call",
    libraries = { "dash.http_util" },
}
