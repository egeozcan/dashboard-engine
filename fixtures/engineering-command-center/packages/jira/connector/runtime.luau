--!strict
-- Jira REST connector (Cloud v3 / Server-Data Center v2). Every operation returns normalized
-- records (jira.model) that satisfy the declared contracts and match the Mock.
local dash = require("dash")
local model = require("jira.model")
local util = require("dash.http_util")

local Runtime = {}

local DEFAULT_MAX_PAGES = 5
local KEY_CHUNK = 50

local function cfg_of(ctx: any): any
    -- Optional values may arrive as JSON null: drop them so they read as absent.
    return util.clean(ctx.config)
end

local function version(ctx: any): string
    return cfg_of(ctx).api_version or "3"
end

local function base(ctx: any): string
    return util.base_url(cfg_of(ctx))
end

local function api(ctx: any, path: string): string
    return "/rest/api/" .. version(ctx) .. path
end

local function request(ctx: any, method: string, path: string, query: any, body: any): any
    local req: { [string]: any } = {
        method = method,
        url = util.url(base(ctx), path, query),
        headers = { Accept = "application/json" },
        -- The broker refuses the request, and any redirect hop, outside base_url.
        confine_to = base(ctx),
    }
    if body ~= nil then
        req.body = body
    end
    if cfg_of(ctx).auth ~= "none" then
        req.credential = "auth"
    end
    return ctx:await(ctx.http:request(req))
end

local function call(ctx: any, method: string, path: string, query: any, body: any, what: string): any
    local resp = request(ctx, method, path, query, body)
    if not util.is_success(resp) then
        util.fail("Jira", what, resp, method ~= "GET")
    end
    return resp
end

local function fields_param(extra: any, with_description: boolean?): string
    local fields = table.clone(model.DEFAULT_FIELDS)
    if with_description then
        table.insert(fields, "description")
    end
    if type(extra) == "table" then
        for _, f in ipairs(extra) do
            if type(f) == "string" and string.match(f, "^[%w_%.%-]+$") then
                table.insert(fields, f)
            end
        end
    end
    return table.concat(fields, ",")
end

-- Raw issues of a JQL query: Cloud v3 pages with nextPageToken (/search/jql), Server/DC v2
-- with startAt (/search, validateQuery=warn); stops after max_results issues or max_pages.
-- With `tolerate_400`, a 400 answer to the first page is returned as (nil, response) instead
-- of raising, so the caller can inspect Jira's error messages.
local function search_raw(ctx: any, jql: string, fields: string, max_results: number?, tolerate_400: boolean?): ({ any }?, any)
    local cfg = cfg_of(ctx)
    local page_size = math.min(tonumber(cfg.page_size) or 100, 100)
    if page_size < 1 then
        page_size = 100
    end
    local pages = util.max_pages(nil, cfg.max_pages, DEFAULT_MAX_PAGES)
    local limit = max_results or (page_size * pages)
    -- Enough pages to deliver max_results, still bounded by the hard page cap.
    pages = math.max(pages, math.min(math.ceil(limit / page_size), 50))
    local out = {}
    local token: string? = nil
    local start_at = 0
    for page = 1, pages do
        local want = math.min(page_size, limit - #out)
        if want <= 0 then
            break
        end
        local resp
        if version(ctx) == "2" then
            resp = request(ctx, "GET", api(ctx, "/search"), {
                jql = jql, fields = fields, startAt = start_at, maxResults = want, validateQuery = "warn",
            }, nil)
        else
            resp = request(ctx, "GET", api(ctx, "/search/jql"), {
                jql = jql, fields = fields, maxResults = want, nextPageToken = token,
            }, nil)
        end
        if not util.is_success(resp) then
            if tolerate_400 and page == 1 and resp.status == 400 then
                return nil, resp
            end
            util.fail("Jira", "search", resp, false)
        end
        local body = model.v(resp.body) or {}
        local issues = model.v(body.issues) or {}
        for _, i in ipairs(issues) do
            table.insert(out, i)
        end
        if #issues == 0 then
            break
        end
        if version(ctx) == "2" then
            start_at += #issues
            local total = tonumber(model.v(body.total))
            if total ~= nil and start_at >= total then
                break
            end
        else
            token = model.v(body.nextPageToken)
            if token == nil or model.v(body.isLast) == true then
                break
            end
        end
    end
    return out, nil
end

-- Keys a Jira 400 response body names as nonexistent or invisible ("An issue with key 'X-1'
-- does not exist for field 'key'."), read from every error message.
local function missing_keys(body: any): { [string]: boolean }
    local found = {}
    body = model.v(body)
    local messages = {}
    if type(body) == "table" then
        for _, m in ipairs(model.v(body.errorMessages) or {}) do
            table.insert(messages, tostring(m))
        end
        local errors = model.v(body.errors)
        if type(errors) == "table" then
            for _, m in pairs(errors) do
                table.insert(messages, tostring(m))
            end
        end
    elseif type(body) == "string" then
        table.insert(messages, body)
    end
    for _, m in ipairs(messages) do
        for key in string.gmatch(m, "'([A-Za-z][A-Za-z0-9_]*%-%d+)'") do
            found[string.upper(key)] = true
        end
    end
    return found
end

local ops = {}

-- Issues by key. Keys that are not well-formed are skipped (they never reach JQL); keys Jira
-- reports as nonexistent or invisible are dropped and the query repeated until it succeeds;
-- an issue moved to another key is returned under its new key after the requested ones.
function ops.get_issues(ctx: any, a: any): any
    local keys, seen = {}, {}
    for _, k in ipairs(a.keys or {}) do
        local ok, key = pcall(model.check_key, k)
        if ok and not seen[key] then
            seen[key] = true
            table.insert(keys, key)
        end
    end
    local fields = fields_param(a.fields)
    local by_key, extra, extra_seen = {}, {}, {}
    for i = 1, #keys, KEY_CHUNK do
        local chunk = {}
        for j = i, math.min(i + KEY_CHUNK - 1, #keys) do
            table.insert(chunk, keys[j])
        end
        for _attempt = 1, #chunk + 1 do
            if #chunk == 0 then
                break
            end
            local issues, bad = search_raw(ctx, "key in (" .. table.concat(chunk, ",") .. ")", fields, #chunk, true)
            if issues ~= nil then
                for _, raw in ipairs(issues) do
                    local issue = model.issue(raw, base(ctx), a.fields)
                    if seen[issue.key] then
                        by_key[issue.key] = issue
                    elseif not extra_seen[issue.key] then
                        extra_seen[issue.key] = true
                        table.insert(extra, issue)
                    end
                end
                break
            end
            local gone = missing_keys(bad.body)
            local kept = {}
            for _, k in ipairs(chunk) do
                if not gone[k] then
                    table.insert(kept, k)
                end
            end
            if #kept == #chunk then
                -- A 400 that names none of the keys: a real error.
                util.fail("Jira", "search", bad, false)
            end
            chunk = kept
        end
    end
    local out = {}
    for _, k in ipairs(keys) do
        if by_key[k] ~= nil then
            table.insert(out, by_key[k])
        end
    end
    for _, issue in ipairs(extra) do
        table.insert(out, issue)
    end
    return out
end

function ops.get_issue(ctx: any, a: any): any
    local key = model.check_key(a.key)
    local resp = call(ctx, "GET", api(ctx, "/issue/" .. key), { fields = fields_param(a.fields, true) }, nil, "get_issue " .. key)
    return model.issue(resp.body, base(ctx), a.fields)
end

function ops.search(ctx: any, a: any): any
    if type(a.jql) ~= "string" or a.jql == "" then
        error("INVALID_ARGS: search requires jql")
    end
    local max_results = tonumber(a.max_results)
    local raw = search_raw(ctx, a.jql, fields_param(a.fields), max_results) or {}
    local out = {}
    for _, r in ipairs(raw) do
        table.insert(out, model.issue(r, base(ctx), a.fields))
    end
    return out
end

local function transitions_of(ctx: any, key: string): { any }
    local resp = call(ctx, "GET", api(ctx, "/issue/" .. key .. "/transitions"), nil, nil, "get_transitions " .. key)
    local out = {}
    for _, t in ipairs(model.v((model.v(resp.body) or {}).transitions) or {}) do
        table.insert(out, model.transition(t))
    end
    return out
end

function ops.get_transitions(ctx: any, a: any): any
    return transitions_of(ctx, model.check_key(a.key))
end

function ops.transition_issue(ctx: any, a: any): any
    local key = model.check_key(a.key)
    local target, category = model.target_of(a)
    local current = call(ctx, "GET", api(ctx, "/issue/" .. key), { fields = "status" }, nil, "get_issue " .. key)
    local issue = model.issue(current.body, nil, nil)
    local from = issue.status
    if a.transition_id == nil and model.at_target(issue, target, category) then
        return { key = key, status = from, from_status = from, transitioned = false }
    end
    local available = transitions_of(ctx, key)
    local chosen, why = model.pick_transition(available, target, a.transition_id, category)
    if chosen == nil then
        if why ~= nil then
            error(string.format("NO_TRANSITION: Jira issue %s: %s", key, why))
        end
        local wanted = a.transition_id or target or ("a status of category '" .. tostring(category) .. "'")
        error(string.format("NO_TRANSITION: Jira issue %s cannot move from '%s' to %s (available: %s)",
            key, from, tostring(wanted), model.transition_names(available)))
    end
    local body: { [string]: any } = { transition = { id = chosen.id } }
    if a.resolution ~= nil and a.resolution ~= "" then
        body.fields = { resolution = { name = a.resolution } }
    end
    if a.comment ~= nil and string.match(a.comment, "%S") then
        body.update = { comment = { { add = { body = model.body(a.comment, version(ctx)) } } } }
    end
    call(ctx, "POST", api(ctx, "/issue/" .. key .. "/transitions"), nil, body, "transition_issue " .. key)
    return {
        key = key,
        status = chosen.to_status,
        from_status = from,
        transitioned = true,
        transition_id = chosen.id,
    }
end

function ops.add_comment(ctx: any, a: any): any
    local key = model.check_key(a.key)
    if type(a.body) ~= "string" or not string.match(a.body, "%S") then
        error("INVALID_ARGS: add_comment requires a non-blank body")
    end
    local resp = call(ctx, "POST", api(ctx, "/issue/" .. key .. "/comment"), nil, { body = model.body(a.body, version(ctx)) }, "add_comment " .. key)
    local c = model.v(resp.body) or {}
    return { id = tostring(model.v(c.id) or ""), key = key, created = model.v(c.created) }
end

local function assignee_field(ctx: any, id: string): any
    if version(ctx) == "2" then
        return { name = id }
    end
    return { accountId = id }
end

function ops.create_issue(ctx: any, a: any): any
    if type(a.project) ~= "string" or not string.match(a.project, "^[%w_]+$") then
        error("INVALID_ARGS: project must be a project key, got " .. tostring(a.project))
    end
    if type(a.summary) ~= "string" or a.summary == "" then
        error("INVALID_ARGS: create_issue requires a summary")
    end
    local fields: { [string]: any } = {
        project = { key = a.project },
        issuetype = { name = a.issue_type },
        summary = a.summary,
    }
    if a.description ~= nil and string.match(a.description, "%S") then
        fields.description = model.body(a.description, version(ctx))
    end
    if a.labels ~= nil then
        fields.labels = a.labels
    end
    if a.priority ~= nil then
        fields.priority = { name = a.priority }
    end
    if a.assignee_id ~= nil then
        fields.assignee = assignee_field(ctx, a.assignee_id)
    end
    if a.parent ~= nil then
        fields.parent = { key = model.check_key(a.parent) }
    end
    local resp = call(ctx, "POST", api(ctx, "/issue"), nil, { fields = fields }, "create_issue in " .. a.project)
    local created = model.v(resp.body) or {}
    local key = tostring(model.v(created.key) or "")
    return { key = key, id = tostring(model.v(created.id) or ""), url = if key ~= "" then base(ctx) .. "/browse/" .. key else nil }
end

function ops.update_issue(ctx: any, a: any): any
    local key = model.check_key(a.key)
    local fields: { [string]: any } = {}
    if a.summary ~= nil then
        fields.summary = a.summary
    end
    if a.description ~= nil then
        if a.description == "" and version(ctx) ~= "2" then
            error("INVALID_ARGS: clearing a description is not supported on Jira Cloud; pass a non-empty description")
        end
        fields.description = model.body(a.description, version(ctx))
    end
    if a.labels ~= nil then
        fields.labels = a.labels
    end
    if a.priority ~= nil then
        fields.priority = { name = a.priority }
    end
    if a.assignee_id ~= nil then
        fields.assignee = assignee_field(ctx, a.assignee_id)
    end
    if next(fields) == nil then
        error("INVALID_ARGS: update_issue needs at least one field to change")
    end
    call(ctx, "PUT", api(ctx, "/issue/" .. key), nil, { fields = fields }, "update_issue " .. key)
    return { key = key, updated = true }
end

function Runtime.call(ctx: any): any
    local op = ops[ctx.operation]
    if op == nil then
        error("unknown Jira connector operation: " .. tostring(ctx.operation))
    end
    return op(ctx, util.clean(ctx.args))
end

return Runtime
