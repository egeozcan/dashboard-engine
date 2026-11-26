--!strict
local dash = require("dash")
local util = require("dash.http_util")

local Runtime = {}

local DEFAULT_MAX_PAGES = 5

local function headers_for(cfg: any, extra: any): { [string]: string }
    local h = util.merge({ Accept = "application/json" }, cfg.headers, extra)
    return h
end

local function send(ctx: any, cfg: any, method: string, url: string, headers: any, body: any): any
    -- The broker refuses the request, and any redirect hop, outside base_url.
    local req: { [string]: any } = {
        method = method,
        url = url,
        headers = headers,
        confine_to = util.base_url(cfg),
    }
    if body ~= nil then
        req.body = body
    end
    if cfg.auth ~= "none" then
        req.credential = "auth"
    end
    return ctx:await(ctx.http:request(req))
end

local function items_of(body: any, items_path: string?): { any }
    local items = util.get_path(body, items_path)
    if items == nil then
        return {}
    end
    -- A JSON object (non-empty table without element 1) is not a list: refuse it rather than
    -- silently returning no rows.
    if type(items) ~= "table" or (next(items) ~= nil and items[1] == nil) then
        error("INVALID_RESPONSE: '" .. tostring(items_path or "body") .. "' is not a list")
    end
    return items
end

local function links_of(resp: any): any
    return dash.url.parse_links(util.header(resp, "link"))
end

-- GET with optional pagination. Returns the extracted result (concatenated items when paging).
local function get(ctx: any, cfg: any, args: any): any
    local base = util.base_url(cfg)
    local path = util.check_path(args.path)
    local headers = headers_for(cfg, args.headers)
    local query = util.merge(cfg.query, args.query)
    local pg = args.paginate or {}
    local mode = pg.mode or "none"
    local what = "GET " .. path

    if mode == "none" then
        local resp = send(ctx, cfg, "GET", util.url(base, path, query), headers, nil)
        if resp.status == 404 and args.allow_not_found then
            return nil
        end
        if not util.is_success(resp) then
            util.fail("HTTP", what, resp, false)
        end
        return util.get_path(resp.body, args.result_path)
    end

    local max_pages = util.max_pages(args.max_pages, cfg.max_pages, DEFAULT_MAX_PAGES)
    local items_path = pg.items_path or args.result_path
    local out = util.array()
    local page_size = tonumber(pg.page_size) or 100
    if page_size < 1 then
        page_size = 100
    end
    local page_no = tonumber(pg.start) or (if mode == "offset" then 0 else 1)
    local next_url: string? = nil
    local next_cursor: string? = nil

    for page = 1, max_pages do
        local q = util.merge(query)
        local url
        if mode == "link" then
            if page == 1 then
                if pg.size_param then
                    q[pg.size_param] = page_size
                end
                url = util.url(base, path, q)
            else
                url = next_url
            end
        elseif mode == "cursor" then
            if page > 1 then
                q[pg.cursor_param or "cursor"] = next_cursor
            end
            if pg.size_param then
                q[pg.size_param] = page_size
            end
            url = util.url(base, path, q)
        else -- page / offset
            q[pg.page_param or (if mode == "offset" then "offset" else "page")] = page_no
            q[pg.size_param or (if mode == "offset" then "limit" else "per_page")] = page_size
            url = util.url(base, path, q)
        end

        local resp = send(ctx, cfg, "GET", url, headers, nil)
        if resp.status == 404 and args.allow_not_found and page == 1 then
            return nil
        end
        if not util.is_success(resp) then
            util.fail("HTTP", what .. " (page " .. page .. ")", resp, false)
        end
        local items = items_of(resp.body, items_path)
        for _, item in ipairs(items) do
            table.insert(out, item)
        end

        if mode == "link" then
            local link = links_of(resp).next
            if link == nil then
                break
            end
            -- Relative links resolve against this page's URL; the result must stay under
            -- base_url (the credential is bound to it).
            next_url = util.resolve_link(link, url, base)
        elseif mode == "cursor" then
            local cursor = util.get_path(resp.body, pg.cursor_path or "next_cursor")
            if cursor == nil or cursor == "" or cursor == false or type(cursor) == "table" then
                break
            end
            next_cursor = tostring(cursor)
        elseif mode == "page" then
            if #items < page_size or #items == 0 then
                break
            end
            page_no += 1
        else -- offset
            if #items < page_size or #items == 0 then
                break
            end
            page_no += #items
        end
    end
    return out
end

local function request(ctx: any, cfg: any, args: any): any
    local base = util.base_url(cfg)
    local path = util.check_path(args.path)
    local headers = headers_for(cfg, args.headers)
    if args.idempotency_key ~= nil and args.idempotency_key ~= "" then
        headers[args.idempotency_header or "Idempotency-Key"] = args.idempotency_key
    end
    local query = util.merge(cfg.query, args.query)
    local method = args.method
    local resp = send(ctx, cfg, method, util.url(base, path, query), headers, args.body)
    if not util.is_success(resp) then
        util.fail("HTTP", method .. " " .. path, resp, true)
    end
    local body = util.v(resp.body)
    if body == "" then
        body = nil
    end
    return {
        status = resp.status,
        body = util.get_path(body, args.result_path),
        location = util.header(resp, "location"),
    }
end

function Runtime.call(ctx: any): any
    -- Optional values may arrive as JSON null: drop them so they read as absent.
    local cfg = util.clean(ctx.config)
    local args = util.clean(ctx.args)
    if type(args.paginate) ~= "table" then
        args.paginate = nil
    end
    if ctx.operation == "get" then
        return get(ctx, cfg, args)
    elseif ctx.operation == "request" then
        return request(ctx, cfg, args)
    end
    error("unknown dash.http operation: " .. tostring(ctx.operation))
end

-- Exposed for tests.
Runtime._get = get
Runtime._request = request

return Runtime
