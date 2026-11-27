local dash = require("dash")

local hit = dash.object {
    id = dash.integer(),
    discussion_url = dash.string(),
    title = dash.optional(dash.string()),
    author = dash.optional(dash.string()),
    url = dash.optional(dash.string()),
    text = dash.optional(dash.string()),
    created_at = dash.optional(dash.string()),
    points = dash.optional(dash.integer()),
    num_comments = dash.optional(dash.integer()),
    story_id = dash.optional(dash.integer()),
    story_title = dash.optional(dash.string()),
    story_url = dash.optional(dash.string()),
    parent_id = dash.optional(dash.integer()),
    tags = dash.list(dash.string()),
}

return dash.connector {
    id = "dash.hacker_news",
    description = "Search Hacker News through Algolia HN Search and retrieve items with their comment trees",
    runtime = "runtime.luau",
    handler = "call",
    libraries = { "dash.http_util" },
    capabilities = { "items.read" },
    config = dash.object {
        base_url = dash.optional(dash.string()),
        mock = dash.optional(dash.map(dash.any())),
    },
    operations = {
        search = {
            description = "Read one zero-based page of hits by relevance or date, filtered by Algolia tags and numeric filters",
            required_capabilities = { "items.read" },
            mode = "read",
            args = dash.object {
                query = dash.optional(dash.string()),
                sort = dash.optional(dash.enum { "relevance", "date" }),
                tags = dash.optional(dash.string()),
                numeric_filters = dash.optional(dash.string()),
                page = dash.optional(dash.integer()),
                hits_per_page = dash.optional(dash.integer()),
            },
            result = dash.object {
                hits = dash.list(hit),
                page = dash.integer(),
                total_pages = dash.integer(),
                total_hits = dash.integer(),
                hits_per_page = dash.integer(),
            },
        },
        get_item = {
            description = "Retrieve a Hacker News item by numeric ID, including its nested comments",
            required_capabilities = { "items.read" },
            mode = "read",
            args = dash.object { id = dash.integer() },
            result = dash.object {
                id = dash.integer(),
                type = dash.string(),
                children = dash.list(dash.any()),
                discussion_url = dash.string(),
            },
        },
    },
}
