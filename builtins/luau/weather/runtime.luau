--!strict
local dash = require("dash")
local util = require("dash.http_util")

local Runtime = {}
local BASE_URL = "https://api.met.no/weatherapi/locationforecast/2.0"
-- A worldwide floor also covers locations outside MET's more frequently updated Nordic model.
local MIN_REFRESH_SECONDS = 3 * 60 * 60

local function coordinate(value: number, limit: number, name: string): number
    if value < -limit or value > limit then
        error("INVALID_ARGS: " .. name .. " must be between " .. -limit .. " and " .. limit)
    end
    -- Compare in degrees so a value already at four decimals survives binary rounding.
    local scaled = math.round(value * 10000)
    if math.abs(scaled / 10000) > math.abs(value) then
        scaled -= math.sign(value)
    end
    return scaled / 10000
end

-- Validate the fields we persist before a response can replace the previous forecast.
local function checked_map(value: any, value_type: string, field: string): any
    if type(value) ~= "table" then
        error("INVALID_RESPONSE: expected a map for " .. field)
    end
    for key, item in pairs(value) do
        if type(key) ~= "string" or type(item) ~= value_type then
            error("INVALID_RESPONSE: invalid entry in " .. field)
        end
    end
    return value
end

local function interval(value: any): any
    value = util.v(value)
    if value == nil then
        return nil
    end
    if type(value) ~= "table" then
        error("INVALID_RESPONSE: expected a forecast interval")
    end
    local summary = util.v(value.summary)
    if summary ~= nil and (type(summary) ~= "table" or type(summary.symbol_code) ~= "string") then
        error("INVALID_RESPONSE: invalid forecast summary")
    end
    if util.v(value.details) ~= nil then
        checked_map(value.details, "number", "interval details")
    end
    return value
end

local function normalize(body: any): any
    local coords = util.get_path(body, "geometry.coordinates")
    local meta = util.get_path(body, "properties.meta")
    local source = util.get_path(body, "properties.timeseries")
    if type(coords) ~= "table" or type(coords[1]) ~= "number" or type(coords[2]) ~= "number"
        or (coords[3] ~= nil and type(coords[3]) ~= "number")
        or type(meta) ~= "table" or type(meta.updated_at) ~= "string" or not dash.parse_timestamp(meta.updated_at)
        or type(source) ~= "table" or source[1] == nil then
        error("INVALID_RESPONSE: expected a MET Norway point forecast with metadata and periods")
    end

    local units = checked_map(meta.units, "string", "units")
    local periods = util.array()
    local previous_time = -math.huge
    for _, step in ipairs(source) do
        local instant = util.get_path(step, "data.instant.details")
        local timestamp = if type(step) == "table" and type(step.time) == "string" then dash.parse_timestamp(step.time) else nil
        if timestamp == nil or timestamp <= previous_time then
            error("INVALID_RESPONSE: expected forecast periods in chronological order")
        end
        previous_time = timestamp
        table.insert(periods, {
            time = step.time,
            details = checked_map(instant, "number", "instant details"),
            next_1_hours = interval(step.data.next_1_hours),
            next_6_hours = interval(step.data.next_6_hours),
            next_12_hours = interval(step.data.next_12_hours),
        })
    end
    return {
        location = { longitude = coords[1], latitude = coords[2], altitude = coords[3] },
        updated_at = meta.updated_at,
        units = units,
        timeseries = periods,
        attribution = "Based on data from MET Norway",
        license_url = "https://creativecommons.org/licenses/by/4.0/",
    }
end

local function fetch(ctx: any, args: any, mock: boolean, now: number): any
    local query = {
        lat = coordinate(args.latitude, 90, "latitude"),
        lon = coordinate(args.longitude, 180, "longitude"),
        altitude = args.altitude,
    }
    local req: any = { method = "GET", path = "/compact", query = query }
    if mock then
        return normalize(util.request_json(ctx, BASE_URL, req, true))
    end

    local cfg = util.v(ctx.config) or {}
    local agent = util.v(cfg.user_agent)
    if type(agent) ~= "string" or not string.match(agent, "%S") or string.find(agent, "%c") then
        error("CONFIG: set user_agent to the deployment name and contact information for MET Norway")
    end
    local base = util.base_url({ base_url = util.v(cfg.base_url) or BASE_URL })
    local key = "met_forecast_v1:" .. dash.url.query(query)
    local cached = util.v(ctx:await(ctx.state:get(key)))
    if cached and cached.base_url ~= base then
        cached = nil
    end
    if cached and now < cached.refresh_at then
        return cached.forecast
    end

    req.headers = { ["User-Agent"] = agent, ["If-Modified-Since"] = if cached then cached.last_modified else nil }
    local resp = util.http_response(ctx, BASE_URL, req, false)
    local forecast
    local last_modified = util.header(resp, "last-modified")
    if resp.status == 304 and cached and cached.last_modified then
        forecast = cached.forecast
        last_modified = last_modified or cached.last_modified
    elseif util.is_success(resp) then
        forecast = normalize(resp.body)
    else
        util.fail("MET Norway", "GET /compact", resp, false)
    end
    local expires = util.header(resp, "expires")
    local refresh_at = math.max(now + MIN_REFRESH_SECONDS, if expires then dash.parse_timestamp(expires) or 0 else 0)
    ctx:await(ctx.state:set(key, {
        base_url = base, refresh_at = refresh_at, last_modified = last_modified, forecast = forecast,
    }))
    return forecast
end

local function run(ctx: any, mock: boolean): any
    local cfg = util.v(ctx.config) or {}
    local timestamp = if mock then util.v(cfg.mock_now) or ctx.now else ctx.now
    local now = dash.parse_timestamp(timestamp)
    if now == nil then
        error("CONFIG: expected an ISO timestamp for mock_now")
    end
    local forecast = fetch(ctx, util.clean(ctx.args), mock, now)
    if ctx.operation == "forecast" then
        return forecast
    end
    -- MET's first step can precede the current hour, especially when serving a cached forecast.
    local periods = forecast.timeseries
    if now < dash.parse_timestamp(periods[1].time) or now > dash.parse_timestamp(periods[#periods].time) then
        error("NO_FORECAST: current time is outside the forecast time series")
    end
    local index = #periods
    while dash.parse_timestamp(periods[index].time) > now do
        index -= 1
    end
    local result = table.clone(forecast)
    result.timeseries = nil
    result.period = periods[index]
    return result
end

function Runtime.call(ctx: any): any
    return run(ctx, false)
end

function Runtime.mock_call(ctx: any): any
    return run(ctx, true)
end

return Runtime
