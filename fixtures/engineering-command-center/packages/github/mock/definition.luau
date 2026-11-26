local dash = require("dash")

return dash.mock {
    id = "github.mock",
    ["for"] = "github.connector",
    description = "Fixture-backed Mock for github.connector during Preview and tests: every operation, same normalized records, mutable fixture state",
    runtime = "runtime.luau",
    handler = "call",
    libraries = { "github.model" },
    fixtures = {
        prs = "fixtures/prs.json",
        issues = "fixtures/issues.json",
        workflow_runs = "fixtures/workflow_runs.json",
        checks = "fixtures/checks.json",
    },
}
