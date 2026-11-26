local dash = require("dash")

local paginate = dash.optional(dash.object {
    mode = dash.optional(dash.enum { "none", "link", "cursor", "page", "offset" }),
    items_path = dash.optional(dash.string()),
    cursor_path = dash.optional(dash.string()),
    cursor_param = dash.optional(dash.string()),
    page_param = dash.optional(dash.string()),
    size_param = dash.optional(dash.string()),
    page_size = dash.optional(dash.integer()),
    start = dash.optional(dash.integer()),
})

return dash.connector {
    id = "dash.http",
    description = "Generic REST/JSON connector: authenticated GETs with pagination and result extraction, and JSON writes, confined to one configured base URL",
    runtime = "runtime.luau",
    handler = "call",
    libraries = { "dash.http_util" },
    credentials = { "auth" },
    capabilities = { "http.read", "http.write" },
    config = dash.object {
        base_url = dash.string(),
        -- "secret" (default): attach the `auth` secret; "none": anonymous requests.
        auth = dash.optional(dash.enum { "secret", "none" }),
        headers = dash.optional(dash.map(dash.string())),
        query = dash.optional(dash.map(dash.any())),
        max_pages = dash.optional(dash.integer()),
        -- Responses served by dash.http_mock in Preview and tests, keyed "METHOD /path".
        mock = dash.optional(dash.map(dash.any())),
    },
    operations = {
        get = {
            description = "GET a path under base_url; optionally paginate and extract result_path",
            required_capabilities = { "http.read" },
            mode = "read",
            args = dash.object {
                path = dash.string(),
                query = dash.optional(dash.map(dash.any())),
                headers = dash.optional(dash.map(dash.string())),
                result_path = dash.optional(dash.string()),
                paginate = paginate,
                max_pages = dash.optional(dash.integer()),
                allow_not_found = dash.optional(dash.boolean()),
            },
            result = dash.any(),
        },
        request = {
            description = "Send a JSON write (POST, PUT, PATCH or DELETE) to a path under base_url",
            required_capabilities = { "http.write" },
            mode = "write",
            args = dash.object {
                method = dash.enum { "POST", "PUT", "PATCH", "DELETE" },
                path = dash.string(),
                query = dash.optional(dash.map(dash.any())),
                headers = dash.optional(dash.map(dash.string())),
                body = dash.optional(dash.any()),
                result_path = dash.optional(dash.string()),
                idempotency_key = dash.optional(dash.string()),
                idempotency_header = dash.optional(dash.string()),
            },
            result = dash.object {
                status = dash.integer(),
                body = dash.optional(dash.any()),
                location = dash.optional(dash.string()),
            },
        },
    },
}
