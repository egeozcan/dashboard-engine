local dash = require("dash")

return dash.library {
    id = "dash.http_util",
    description = "Shared helpers of the dash.http family: safe relative paths, URL building, JSON path extraction, path templates and HTTP error mapping",
    runtime = "runtime.luau",
}
