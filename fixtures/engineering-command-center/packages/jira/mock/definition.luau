local dash = require("dash")

return dash.mock {
    id = "jira.mock",
    ["for"] = "jira.connector",
    description = "Fixture-backed Mock for jira.connector: issues, a simple workflow (transitions), JQL subset, comments and issue creation",
    runtime = "runtime.luau",
    handler = "call",
    libraries = { "jira.model" },
    fixtures = {
        issues = "fixtures/issues.json",
        workflow = "fixtures/workflow.json",
    },
}
