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
local util = require("dash.http_util")

local Runtime = {}

function Runtime.call(ctx: any): any
    local cfg = util.clean(ctx.config, { mock = true })
    local args = util.clean(ctx.args, { body = true })
    local path = util.check_path(args.path)
    local query = util.merge(args.query, cfg.query)
    if ctx.operation == "get" then
        local resp = util.mock_response(cfg.mock, "GET", path, query)
        if resp == nil then
            if args.allow_not_found then
                return nil
            end
            error("MOCK_NOT_CONFIGURED: no mock response for GET " .. path .. " (set config.mock[\"GET " .. path .. "\"])")
        end
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
        local resp = util.mock_response(cfg.mock, args.method, path, query)
            or { status = 200, headers = {}, body = args.body }
        local log = ctx:await(ctx:mock_get("requests")) or {}
        table.insert(log, { method = args.method, path = path, query = args.query, body = args.body })
        ctx:await(ctx:mock_set("requests", log))
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
