--!strict
local util = require("dash.http_util")

local Runtime = {}
local BASE_URL = "https://hn.algolia.com/api/v1"

local function discussion_url(id: number): string
    return "https://news.ycombinator.com/item?id=" .. tostring(id)
end

local function search(ctx: any, args: any, mock: boolean): any
    if args.page ~= nil and args.page < 0 then
        error("INVALID_ARGS: page must be zero or greater")
    end
    if args.hits_per_page ~= nil and (args.hits_per_page < 1 or args.hits_per_page > 1000) then
        error("INVALID_ARGS: hits_per_page must be between 1 and 1000")
    end
    local body = util.request_json(ctx, BASE_URL, {
        method = "GET",
        path = if args.sort == "date" then "/search_by_date" else "/search",
        query = {
            query = args.query,
            tags = args.tags,
            numericFilters = args.numeric_filters,
            page = args.page,
            hitsPerPage = args.hits_per_page,
        },
    }, mock)
    if type(body) ~= "table" or type(body.hits) ~= "table" or (next(body.hits) ~= nil and body.hits[1] == nil) then
        error("INVALID_RESPONSE: expected an Algolia search page with a hits list")
    end

    local hits = util.array()
    for _, hit in ipairs(body.hits) do
        local id = if type(hit) == "table" then tonumber(hit.objectID) else nil
        if id == nil then
            error("INVALID_RESPONSE: search hit has no numeric objectID")
        end
        table.insert(hits, {
            id = id,
            discussion_url = discussion_url(id),
            title = hit.title,
            author = hit.author,
            url = hit.url,
            text = util.v(hit.comment_text) or hit.story_text,
            created_at = hit.created_at,
            points = hit.points,
            num_comments = hit.num_comments,
            story_id = hit.story_id,
            story_title = hit.story_title,
            story_url = hit.story_url,
            parent_id = hit.parent_id,
            tags = hit._tags,
        })
    end
    return {
        hits = hits,
        page = body.page,
        total_pages = body.nbPages,
        total_hits = body.nbHits,
        hits_per_page = body.hitsPerPage,
    }
end

local function run(ctx: any, mock: boolean): any
    local args = util.clean(ctx.args)
    if ctx.operation == "search" then
        return search(ctx, args, mock)
    elseif ctx.operation == "get_item" then
        if args.id < 1 then
            error("INVALID_ARGS: id must be positive")
        end
        local item = util.request_json(ctx, BASE_URL, { method = "GET", path = "/items/" .. tostring(args.id) }, mock)
        if type(item) ~= "table" or item.id ~= args.id then
            error("INVALID_RESPONSE: expected the requested Hacker News item")
        end
        item.discussion_url = discussion_url(args.id)
        return item
    end
    error("unknown dash.hacker_news operation: " .. tostring(ctx.operation))
end

function Runtime.call(ctx: any): any
    return run(ctx, false)
end

function Runtime.mock_call(ctx: any): any
    return run(ctx, true)
end

return Runtime
