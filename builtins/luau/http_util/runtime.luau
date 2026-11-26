--!strict
-- Shared helpers of the built-in REST/JSON connector family (dash.http, dash.http_mock,
-- dash.http_query, dash.http_action). Also usable from Workspace Packages:
-- `libraries = { "dash.http_util" }` and `require("dash.http_util")`.
local dash = require("dash")

local Util = {}

local MAX_PAGES_CAP = 50

-- JSON null arrives in Luau as a userdata sentinel, not nil: normalize it to nil.
function Util.v(x: any): any
    if type(x) == "userdata" then
        return nil
    end
    return x
end

-- Remove JSON nulls (userdata sentinels) from a table, recursively and in place (so JSON
-- arrays keep their array marker). nil or null becomes an empty table.
function Util.clean(x: any): any
    if x == nil or type(x) == "userdata" then
        return {}
    end
    if type(x) ~= "table" then
        return x
    end
    local function walk(t: any)
        for k, val in pairs(t) do
            if type(val) == "userdata" then
                t[k] = nil
            elseif type(val) == "table" then
                walk(val)
            end
        end
    end
    walk(x)
    return x
end

-- An empty list that serializes as `[]` even when nothing is added.
function Util.array(): { any }
    return dash.array()
end

-- Lower-cased path with the encoded separators and dots that servers commonly decode
-- (`%2f`, `%5c`, `%2e`, `\`) in literal form.
local function decoded(path: string): string
    local p = string.lower(path)
    p = string.gsub(p, "%%2f", "/")
    p = string.gsub(p, "%%5c", "/")
    p = string.gsub(p, "\\", "/")
    p = string.gsub(p, "%%2e", ".")
    return p
end

local function has_dot_segment(path: string): boolean
    for segment in string.gmatch(decoded(path), "[^/]+") do
        if segment == "." or segment == ".." then
            return true
        end
    end
    return false
end

-- A path relative to the configured base URL. Absolute URLs, `//`, backslashes, control
-- characters, whitespace, fragments and dot segments (also behind encoded separators such as
-- `..%2F`) are refused, so a request can never leave the base URL (and its credential stays
-- there). Encoded slashes inside a segment (`group%2Fproject`) remain allowed.
function Util.check_path(path: any): string
    if type(path) ~= "string" or string.sub(path, 1, 1) ~= "/" then
        error("INVALID_ARGS: path must be a string starting with '/', got " .. tostring(path))
    end
    local only_path = string.match(path, "^([^?]*)") or path
    if string.find(path, "//", 1, true) or string.find(path, "\\", 1, true) then
        error("INVALID_ARGS: path must not contain '//' or '\\': " .. path)
    end
    if string.find(path, "[%c%s#]") then
        error("INVALID_ARGS: path must not contain whitespace, control characters or '#'")
    end
    if has_dot_segment(only_path) then
        error("INVALID_ARGS: path must not contain '.' or '..' segments: " .. path)
    end
    return path
end

-- `scheme://host[:port]` of a URL (lowercased, default ports dropped), or nil.
function Util.origin(url: string): string?
    local origin = string.match(url, "^(https?://[^/?#]+)")
    if origin == nil then
        return nil
    end
    origin = string.lower(origin)
    origin = string.gsub(origin, "^(https://[^/]+):443$", "%1")
    origin = string.gsub(origin, "^(http://[^/]+):80$", "%1")
    return origin
end

-- Base URL without trailing slashes; must be an http(s) URL without credentials, query string
-- or fragment.
function Util.base_url(cfg: any): string
    local base = cfg and Util.v(cfg.base_url)
    if type(base) ~= "string" or Util.origin(base) == nil then
        error("CONFIG: base_url must be an http(s) URL, got " .. tostring(base))
    end
    local authority = string.match(base, "^https?://([^/]*)") or ""
    if string.find(authority, "@", 1, true) or string.find(base, "[?#%s]") then
        error("CONFIG: base_url must not contain credentials, a query string, a fragment or whitespace: " .. base)
    end
    return (string.gsub(base, "/+$", ""))
end

-- Whether an absolute URL stays under `base` (as returned by base_url): same origin, a path
-- equal to or below the base path, and no dot segments even behind encoded separators.
function Util.within(url: string, base: string): boolean
    local origin = Util.origin(url)
    if origin == nil or origin ~= Util.origin(base) then
        return false
    end
    local rest = string.match(url, "^https?://[^/?#]+(.*)$") or ""
    local path = string.match(rest, "^([^?#]*)") or ""
    if has_dot_segment(path) then
        return false
    end
    local base_path = decoded(string.match(base, "^https?://[^/?#]+(.*)$") or "")
    base_path = string.gsub(base_path, "/+$", "")
    local p = decoded(path)
    return base_path == "" or p == base_path or string.sub(p, 1, #base_path + 1) == base_path .. "/"
end

-- Resolve a pagination link (absolute, `/absolute-path`, `?query` or a relative path, per
-- RFC 3986) against the URL of the response that carried it, and require it to stay under
-- `base`. Raises INVALID_RESPONSE otherwise.
function Util.resolve_link(link: string, current: string, base: string): string
    local target
    if string.match(link, "^https?://") then
        target = link
    elseif string.sub(link, 1, 2) == "//" then
        target = nil
    elseif string.sub(link, 1, 1) == "/" then
        target = (string.match(current, "^(https?://[^/?#]+)") or "") .. link
    elseif string.sub(link, 1, 1) == "?" then
        target = (string.match(current, "^([^?#]*)") or current) .. link
    else
        local dir = string.match(current, "^(https?://[^?#]*/)") or ((string.match(current, "^(https?://[^/?#]+)") or "") .. "/")
        target = dir .. link
    end
    if target == nil or string.find(target, "[%c%s]") or not Util.within(target, base) then
        error("INVALID_RESPONSE: pagination link leaves " .. base .. ": " .. tostring(link))
    end
    return target
end

-- Shallow merge: later tables win; nil tables are skipped.
function Util.merge(...: any): { [string]: any }
    local out = {}
    for i = 1, select("#", ...) do
        local t = select(i, ...)
        if type(t) == "table" then
            for k, v in pairs(t) do
                out[k] = v
            end
        end
    end
    return out
end

-- Full URL of `path` (already checked) under `base` with `query` appended.
function Util.url(base: string, path: string, query: any): string
    local qs = dash.url.query(query or {})
    if qs == "" then
        return base .. path
    end
    local sep = if string.find(path, "?", 1, true) then "&" else "?"
    return base .. path .. sep .. qs
end

-- Value at a dotted path (`data.items`, `items.0.id`); numeric segments index lists 0-based.
-- An empty or nil path returns the value itself.
function Util.get_path(value: any, path: string?): any
    if path == nil or path == "" then
        return Util.v(value)
    end
    local cur = Util.v(value)
    for segment in string.gmatch(path :: string, "[^%.]+") do
        if type(cur) ~= "table" then
            return nil
        end
        local idx = tonumber(segment)
        if idx ~= nil and cur[segment] == nil then
            cur = Util.v(cur[idx + 1])
        else
            cur = Util.v(cur[segment])
        end
    end
    return cur
end

-- A value placed into one path segment must not contain `.`/`..` pieces: an encoded slash
-- (`..%2Fadmin`) is decoded back into a separator by some servers and proxies.
function Util.check_segment_value(value: any, name: string)
    local s = tostring(value)
    for piece in string.gmatch(s, "[^/\\]+") do
        if piece == "." or piece == ".." then
            error("INVALID_ARGS: path parameter '" .. name .. "' must not contain '.' or '..' segments")
        end
    end
    if string.find(s, "[%c]") then
        error("INVALID_ARGS: path parameter '" .. name .. "' must not contain control characters")
    end
end

-- Fill `{name}` placeholders of a path template from `params` (URL-encoded). Returns the path
-- and the set of parameter names used. A missing or empty parameter is an error.
function Util.fill_template(template: string, params: any): (string, { [string]: boolean })
    local used = {}
    local p = params or {}
    local out = string.gsub(template, "{([%w_]+)}", function(name)
        local v = p[name]
        if v == nil or v == "" or type(v) == "table" then
            error("INVALID_ARGS: path parameter '" .. name .. "' is required (template " .. template .. ")")
        end
        Util.check_segment_value(v, name)
        used[name] = true
        return dash.url.encode(v)
    end)
    return out, used
end

-- Effective page cap: explicit, else configured, else `default`; never above 50.
function Util.max_pages(explicit: any, configured: any, default: number): number
    local n = tonumber(explicit) or tonumber(configured) or default
    n = math.floor(n)
    if n < 1 then
        n = 1
    end
    return math.min(n, MAX_PAGES_CAP)
end

local function header(resp: any, name: string): string?
    local h = resp and Util.v(resp.headers)
    if type(h) ~= "table" then
        return nil
    end
    return Util.v(h[name]) or Util.v(h[string.lower(name)])
end
Util.header = header

-- Best human-readable error text of an error response body.
function Util.error_message(body: any): string?
    body = Util.v(body)
    if type(body) == "string" then
        local s = string.gsub(body, "%s+", " ")
        if s == "" then
            return nil
        end
        return if #s > 300 then string.sub(s, 1, 300) .. "..." else s
    end
    if type(body) ~= "table" then
        return nil
    end
    local parts = {}
    local function add(v)
        v = Util.v(v)
        if type(v) == "string" and v ~= "" then
            table.insert(parts, v)
        end
    end
    add(body.message)
    add(body.detail)
    add(body.title)
    local err = Util.v(body.error)
    if type(err) == "string" then
        add(err)
    elseif type(err) == "table" then
        add(err.message)
    end
    local messages = Util.v(body.errorMessages)
    if type(messages) == "table" then
        for _, m in ipairs(messages) do
            add(m)
        end
    end
    local errors = Util.v(body.errors)
    if type(errors) == "table" then
        for k, e in pairs(errors) do
            if type(e) == "string" then
                add(if type(k) == "string" then k .. ": " .. e else e)
            elseif type(e) == "table" then
                add(e.message)
            end
        end
    end
    if #parts == 0 then
        return nil
    end
    local s = table.concat(parts, "; ")
    return if #s > 300 then string.sub(s, 1, 300) .. "..." else s
end

-- Stable error code of an HTTP failure status.
function Util.status_code(status: number, resp: any): string
    if status == 401 then
        return "UNAUTHORIZED"
    elseif status == 403 then
        local remaining = header(resp, "x-ratelimit-remaining")
        local msg = string.lower(Util.error_message(resp and resp.body) or "")
        if remaining == "0" or header(resp, "retry-after") ~= nil or string.find(msg, "rate limit", 1, true) then
            return "RATE_LIMITED"
        end
        return "FORBIDDEN"
    elseif status == 404 then
        return "NOT_FOUND"
    elseif status == 409 then
        return "CONFLICT"
    elseif status == 429 then
        return "RATE_LIMITED"
    elseif status == 400 or status == 422 then
        return "INVALID_REQUEST"
    elseif status >= 500 then
        return "UPSTREAM_ERROR"
    end
    return "HTTP_ERROR"
end

-- Raise the error of a failed response. `write = true` marks a side-effecting request: a 5xx
-- answer (other than 501 Not Implemented) does not prove the write did not happen, so it is
-- reported as UNKNOWN_OUTCOME (§9.1) and never retried blindly.
function Util.fail(service: string, what: string, resp: any, write: boolean?)
    local status = tonumber(resp and resp.status) or 0
    local code = Util.status_code(status, resp)
    local msg = Util.error_message(resp and resp.body)
    local text = string.format("%s %s returned HTTP %d", service, what, status)
    if msg ~= nil then
        text = text .. ": " .. msg
    end
    if code == "RATE_LIMITED" then
        local retry = header(resp, "retry-after")
        local reset = header(resp, "x-ratelimit-reset")
        if retry ~= nil then
            text = text .. " (retry after " .. retry .. "s)"
        elseif reset ~= nil then
            text = text .. " (rate limit resets at epoch " .. reset .. ")"
        end
    end
    if write and status >= 500 and status ~= 501 then
        error("UNKNOWN_OUTCOME: " .. text)
    end
    error(code .. ": " .. text)
end

function Util.is_success(resp: any): boolean
    local status = tonumber(resp and resp.status) or 0
    return status >= 200 and status < 300
end

return Util
