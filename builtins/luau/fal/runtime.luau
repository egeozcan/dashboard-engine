--!strict
local util = require("dash.http_util")

local Runtime = {}
local BASE_URL = "https://queue.fal.run"

local function check_segment(segment: string, name: string)
    if not string.match(segment, "^[%w._-]+$") or segment == "." or segment == ".." then
        error("INVALID_ARGS: invalid path segment in " .. name)
    end
end

local function run(ctx: any, mock: boolean): any
    local args = util.clean(ctx.args, { input = true })
    local parts = string.split(args.endpoint_id, "/")
    -- fal's queue routes use [namespace/]owner/app. Only submission includes the model subpath.
    local namespaced = parts[1] == "workflows" or parts[1] == "comfy"
    local app_parts = if namespaced then 3 else 2
    if #parts < app_parts then
        error("INVALID_ARGS: endpoint_id must include an owner and app")
    end
    for _, part in ipairs(parts) do
        check_segment(part, "endpoint_id")
    end

    local req: any = { credential = "auth" }
    if ctx.operation == "submit" then
        req.method = "POST"
        req.path = "/" .. args.endpoint_id
        req.body = args.input
    else
        check_segment(args.request_id, "request_id")
        req.path = "/" .. table.concat(parts, "/", 1, app_parts) .. "/requests/" .. args.request_id
        req.method = "GET"
        if ctx.operation == "status" then
            req.path ..= "/status"
            if args.logs then
                req.query = { logs = "1" }
            end
        elseif ctx.operation == "cancel" then
            req.method = "PUT"
            req.path ..= "/cancel"
        elseif ctx.operation ~= "result" then
            error("unknown dash.fal operation: " .. tostring(ctx.operation))
        end
    end
    return util.request_json(ctx, BASE_URL, req, mock)
end

function Runtime.call(ctx: any): any
    return run(ctx, false)
end

function Runtime.mock_call(ctx: any): any
    return run(ctx, true)
end

return Runtime
