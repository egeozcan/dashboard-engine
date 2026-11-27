local dash = require("dash")

return dash.mock {
    id = "dash.hacker_news_mock",
    ["for"] = "dash.hacker_news",
    description = "Serve configured HTTP fixtures for dash.hacker_news in Preview and tests",
    runtime = "runtime.luau",
    handler = "mock_call",
    libraries = { "dash.http_util" },
}
