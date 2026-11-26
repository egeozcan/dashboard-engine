local dash = require("dash")

return dash.library {
    id = "jira.model",
    description = "Normalizes Jira REST payloads (issues, transitions, Atlassian Document Format) into the package's stable records; shared by the connector and its Mock",
    runtime = "runtime.luau",
}
