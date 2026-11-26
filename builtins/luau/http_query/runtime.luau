--!strict
local util = require("dash.http_util")

local Runtime = {}

local function project(row: any, fields: { [string]: string }): any
    if type(row) ~= "table" then
        return row
    end
    local out = {}
    for name, path in pairs(fields) do
        out[name] = util.get_path(row, path)
    end
    return out
end

-- Map every row of a list (or a single record) to the configured `fields`.
function Runtime.map_fields(data: any, fields: any): any
    if type(fields) ~= "table" or next(fields) == nil or type(data) ~= "table" then
        return data
    end
    if data[1] ~= nil or next(data) == nil then
        local out = util.array()
        for i, row in ipairs(data) do
            out[i] = project(row, fields)
        end
        return out
    end
    return project(data, fields)
end

-- Path and query string of one query: `{name}` placeholders take query parameters; with
-- `forward_params` (default true) the remaining parameters are added to the query string.
function Runtime.request_args(cfg: any, params: any): any
    if type(cfg.path) ~= "string" then
        error("CONFIG: dash.http_query needs config.path")
    end
    local p = params or {}
    local path, used = util.fill_template(cfg.path, p)
    local query = util.merge(cfg.query)
    if cfg.forward_params ~= false then
        for k, v in pairs(p) do
            -- A forwarded parameter never replaces a configured one.
            if not used[k] and query[k] == nil then
                query[k] = v
            end
        end
    end
    return {
        path = path,
        query = query,
        headers = cfg.headers,
        result_path = cfg.result_path,
        paginate = cfg.paginate,
        max_pages = cfg.max_pages,
        allow_not_found = cfg.allow_not_found,
    }
end

function Runtime.query(ctx: any): any
    local cfg = util.clean(ctx.config)
    local data = ctx:await(ctx.inputs.api:call("get", Runtime.request_args(cfg, util.clean(ctx.query))))
    return Runtime.map_fields(data, cfg.fields)
end

return Runtime
