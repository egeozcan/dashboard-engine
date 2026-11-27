local dash = require("dash")

return dash.library {
    id = "dash.http_util",
    description = "HTTP helpers for connectors and Preview mocks, including URL confinement, JSON path extraction, fixtures and error mapping",
    runtime = "runtime.luau",
}
