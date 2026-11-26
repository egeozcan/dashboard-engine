--!strict
local util = require("dash.http_util")

local Runtime = {}

-- The request an invocation sends: `{name}` placeholders of config.path take arguments; the
-- `body` argument (if given) is the body, otherwise the remaining arguments merged over
-- config.body. DELETE sends no body unless one is given explicitly.
function Runtime.build(cfg: any, args: any): any
    if type(cfg.path) ~= "string" then
        error("CONFIG: dash.http_action needs config.path")
    end
    local a = args or {}
    local method = cfg.method or "POST"
    local path, used = util.fill_template(cfg.path, a)
    util.check_path(path)
    local body = nil
    if a.body ~= nil then
        body = a.body
    else
        local rest = {}
        local any = false
        for k, v in pairs(a) do
            if not used[k] then
                rest[k] = v
                any = true
            end
        end
        if method ~= "DELETE" or any then
            body = util.merge(cfg.body, rest)
        end
    end
    return {
        method = method,
        path = path,
        query = cfg.query,
        headers = cfg.headers,
        body = body,
        result_path = cfg.result_path,
    }
end

function Runtime.dry_run(ctx: any): any
    local req = Runtime.build(util.clean(ctx.config), util.clean(ctx.args))
    return {
        feasible = true,
        warnings = {},
        effects = {
            method = req.method,
            path = req.path,
            query = req.query,
            -- Configured (non-secret) headers; the vault credential is never shown.
            headers = req.headers,
            body = req.body,
        },
    }
end

function Runtime.execute(ctx: any): any
    local cfg = util.clean(ctx.config)
    local req = Runtime.build(cfg, util.clean(ctx.args))
    if cfg.idempotency_header ~= nil and ctx.idempotency_key ~= nil then
        req.idempotency_key = ctx.idempotency_key
        req.idempotency_header = cfg.idempotency_header
    end
    return ctx:await(ctx.inputs.api:call("request", req))
end

return Runtime
