local dash = require("dash")

return dash.data_provider {
    id = "dash.http_query",
    description = "Reads a REST/JSON endpoint through a dash.http connector: a path template filled from query parameters, pagination, result extraction and field mapping, all configured on the Instance",
    runtime = "runtime.luau",
    handler = "query",
    libraries = { "dash.http_util" },
    inputs = {
        api = dash.connector_ref("dash.http", { required_capabilities = { "http.read" } }),
    },
    config = dash.object {
        path = dash.string(),
        query = dash.optional(dash.map(dash.any())),
        headers = dash.optional(dash.map(dash.string())),
        result_path = dash.optional(dash.string()),
        paginate = dash.optional(dash.object {
            mode = dash.optional(dash.enum { "none", "link", "cursor", "page", "offset" }),
            items_path = dash.optional(dash.string()),
            cursor_path = dash.optional(dash.string()),
            cursor_param = dash.optional(dash.string()),
            page_param = dash.optional(dash.string()),
            size_param = dash.optional(dash.string()),
            page_size = dash.optional(dash.integer()),
            start = dash.optional(dash.integer()),
        }),
        max_pages = dash.optional(dash.integer()),
        fields = dash.optional(dash.map(dash.string())),
        forward_params = dash.optional(dash.boolean()),
        allow_not_found = dash.optional(dash.boolean()),
        keep_warm = dash.optional(dash.list(dash.any())),
    },
    query = dash.optional(dash.map(dash.any())),
    output = dash.any(),
    updates = {
        on_demand = true,
        subscribe = true,
        poll_interval_secs = 60,
    },
}
