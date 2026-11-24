local dash = require("dash")

return dash.mock {
    id = "github.mock",
    ["for"] = "github.connector",
    description = "Fixture-backed Mock for github.connector during Preview and tests",
    runtime = "runtime.luau",
    handler = "call",
    fixtures = {
        prs = "fixtures/prs.json",
    },
}
