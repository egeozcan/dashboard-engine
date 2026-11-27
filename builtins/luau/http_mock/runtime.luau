--!strict
-- Mock for dash.http. Responses come from the connector Instance's `config.mock`, keyed
-- "METHOD /path" (query ignored) or "METHOD /path?query" (exact query string, which wins):
--   mock:
--     "GET /v1/services": [ { id: "api" } ]                           # a response body
--     "GET /v1/missing": { status: 404, body: { error: "gone" } }      # a response envelope
--     "POST /v1/deploys": { status: 201, headers: { location: "/v1/deploys/d1" }, body: { id: "d1" } }
-- A table with a numeric `status` and a `body` (and optional `headers`) is a response envelope
-- and goes through the same success/error handling as live responses. An unconfigured GET
-- fails with MOCK_NOT_CONFIGURED; an unconfigured write succeeds with status 200 and echoes the
-- request body. Writes are recorded (ctx:mock_get("requests")).
local dash = require("dash")
local util = require("dash.http_util")

local Runtime = {}

local function lookup(cfg: any, method: string, path: string, query: any): (boolean, any)
    local mock = cfg.mock or {}
    local only_path = string.match(path, "^([^?]*)") or path
    local qs = dash.url.query(util.merge(query, cfg.query))
    local candidates = {}
    if qs ~= "" then
        table.insert(candidates, method .. " " .. only_path .. "?" .. qs)
    end
    table.insert(candidates, method .. " " .. path)
    table.insert(candidates, method .. " " .. only_path)
    for _, key in ipairs(candidates) do
        local v = mock[key]
        if v ~= nil then
            return true, v
        end
    end
    return false, nil
end

-- A configured value as a response: envelopes as given, anything else a 200 with that body.
local function response(v: any): any
    if type(v) == "table" and type(v.status) == "number" then
        local envelope = true
        for k in pairs(v) do
            if k ~= "status" and k ~= "headers" and k ~= "body" then
                envelope = false
            end
        end
        if envelope then
            return { status = v.status, headers = v.headers or {}, body = util.v(v.body) }
        end
    end
    return { status = 200, headers = {}, body = v }
end

function Runtime.call(ctx: any): any
    local cfg = util.clean(ctx.config, { mock = true })
    local args = util.clean(ctx.args, { body = true })
    local path = util.check_path(args.path)
    if ctx.operation == "get" then
        local found, v = lookup(cfg, "GET", path, args.query)
        if not found then
            if args.allow_not_found then
                return nil
            end
            error("MOCK_NOT_CONFIGURED: no mock response for GET " .. path .. " (set config.mock[\"GET " .. path .. "\"])")
        end
        local resp = response(v)
        if resp.status == 404 and args.allow_not_found then
            return nil
        end
        if not util.is_success(resp) then
            util.fail("HTTP", "GET " .. path, resp, false)
        end
        local pg = type(args.paginate) == "table" and args.paginate or {}
        if pg.mode ~= nil and pg.mode ~= "none" then
            local items = util.get_path(resp.body, pg.items_path or args.result_path)
            if items == nil or (type(items) == "table" and next(items) == nil) then
                return util.array()
            end
            return items
        end
        return util.get_path(resp.body, args.result_path)
    elseif ctx.operation == "request" then
        local found, v = lookup(cfg, args.method, path, args.query)
        local log = ctx:await(ctx:mock_get("requests")) or {}
        table.insert(log, { method = args.method, path = path, query = args.query, body = args.body })
        ctx:await(ctx:mock_set("requests", log))
        local resp = if found then response(v) else { status = 200, headers = {}, body = args.body }
        if not util.is_success(resp) then
            util.fail("HTTP", args.method .. " " .. path, resp, true)
        end
        return {
            status = resp.status,
            body = util.get_path(resp.body, args.result_path),
            location = util.header(resp, "location"),
        }
    end
    error("unknown dash.http operation: " .. tostring(ctx.operation))
end

return Runtime
