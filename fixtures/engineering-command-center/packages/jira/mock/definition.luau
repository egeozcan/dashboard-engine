local dash = require("dash")

return dash.mock {
    id = "jira.mock",
    ["for"] = "jira.connector",
    description = "Fixture-backed Mock for jira.connector",
    runtime = "runtime.luau",
    handler = "call",
    fixtures = {
        issues = "fixtures/issues.json",
    },
}
