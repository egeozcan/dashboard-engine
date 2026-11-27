--!strict
local dash = require("dash")
local util = require("dash.http_util")

local Runtime = {}
local BASE_URL = "https://api.frankfurter.dev/v2"

local function currency(code: string): string
    if not string.match(code, "^[A-Za-z0-9]+$") then
        error("INVALID_ARGS: currency codes must be alphanumeric")
    end
    return string.upper(code)
end

local function date(value: string): string
    if not string.match(value, "^%d%d%d%d%-%d%d%-%d%d$") or not dash.parse_timestamp(value .. "T00:00:00Z") then
        error("INVALID_ARGS: expected a calendar date in YYYY-MM-DD format")
    end
    return value
end

local function run(ctx: any, mock: boolean): any
    local op = ctx.operation
    local args = util.clean(ctx.args)
    if op == "currencies" then
        return util.request_json(ctx, BASE_URL, { method = "GET", path = "/currencies" }, mock)
    end

    local prefix = ""
    if args.provider then
        if not string.match(args.provider, "^[A-Za-z0-9][A-Za-z0-9._-]*$") then
            error("INVALID_ARGS: expected a Frankfurter provider key such as ecb")
        end
        prefix = "/providers/" .. string.lower(args.provider)
    end
    local base = currency(args.base)
    local query: any = {}
    local path
    if op == "rate" or op == "convert" then
        path = prefix .. "/rate/" .. string.lower(base) .. "/" .. string.lower(currency(args.quote))
    else
        path = prefix .. "/rates"
        query.base = base
        if args.quotes then
            if #args.quotes == 0 then
                error("INVALID_ARGS: quotes must contain at least one currency when supplied")
            end
            local codes = {}
            for _, code in ipairs(args.quotes) do
                table.insert(codes, currency(code))
            end
            query.quotes = table.concat(codes, ",")
        end
    end
    if op == "time_series" then
        query.from = date(args.from)
        query.to = date(args.to)
        if query.from > query.to then
            error("INVALID_ARGS: from must be on or before to")
        end
    elseif args.date then
        query.date = date(args.date)
    end

    local value = util.request_json(ctx, BASE_URL, { method = "GET", path = path, query = query }, mock)
    if op == "convert" then
        if type(value) ~= "table" or type(value.rate) ~= "number" then
            error("INVALID_RESPONSE: expected a Frankfurter rate")
        end
        local result = table.clone(value)
        result.amount = args.amount
        result.converted_amount = args.amount * value.rate
        if math.abs(result.converted_amount) == math.huge then
            error("INVALID_ARGS: converted amount exceeds the supported numeric range")
        end
        return result
    end
    return value
end

function Runtime.call(ctx: any): any
    return run(ctx, false)
end

function Runtime.mock_call(ctx: any): any
    return run(ctx, true)
end

return Runtime
