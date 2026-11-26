local dash = require("dash")

return dash.library {
    id = "github.model",
    description = "Normalizes GitHub REST payloads (pull requests, issues, workflow runs, statuses and check runs) into the package's stable records; shared by the connector and its Mock",
    runtime = "runtime.luau",
}
