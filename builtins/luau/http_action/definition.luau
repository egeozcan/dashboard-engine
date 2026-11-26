local dash = require("dash")

return dash.action_provider {
    id = "dash.http_action",
    description = "Sends one JSON write (POST, PUT, PATCH or DELETE) through a dash.http connector; the Dry run shows the exact request without sending it",
    runtime = "runtime.luau",
    handler = "execute",
    dry_run = "dry_run",
    libraries = { "dash.http_util" },
    inputs = {
        api = dash.connector_ref("dash.http", { required_capabilities = { "http.write" } }),
    },
    config = dash.object {
        method = dash.optional(dash.enum { "POST", "PUT", "PATCH", "DELETE" }),
        path = dash.string(),
        query = dash.optional(dash.map(dash.any())),
        headers = dash.optional(dash.map(dash.string())),
        body = dash.optional(dash.map(dash.any())),
        result_path = dash.optional(dash.string()),
        idempotency_header = dash.optional(dash.string()),
    },
    args = dash.optional(dash.map(dash.any())),
    result = dash.object {
        status = dash.integer(),
        body = dash.optional(dash.any()),
        location = dash.optional(dash.string()),
    },
    -- Every dash.http_query Instance re-reads after a write (the engine cannot know which
    -- endpoints a configured write touches).
    invalidates = { "dash.http_query" },
}
