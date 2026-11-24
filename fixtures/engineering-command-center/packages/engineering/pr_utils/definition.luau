local dash = require("dash")

return dash.library {
    id = "engineering.pr_utils",
    description = "Shared Jira key extraction and PR enrichment helpers",
    runtime = "runtime.luau",
}
