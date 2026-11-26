--!strict
-- Fixture-backed Mock for jira.connector. Records go through jira.model like the connector's;
-- transitions follow fixtures/workflow.json; writes mutate the Mock's fixture state.
local model = require("jira.model")

local Runtime = {}

local MOCK_BASE = "https://jira.example.com"

local function load(ctx: any, name: string): any
    local cached = ctx:await(ctx:mock_get(name))
    if cached ~= nil then
        return cached
    end
    local init = ctx:await(ctx:fixture("fixtures/" .. name .. ".json"))
    ctx:await(ctx:mock_set(name, init))
    return init
end

local function save(ctx: any, name: string, value: any)
    ctx:await(ctx:mock_set(name, value))
end

local function base_of(ctx: any): string
    local b = (ctx.config or {}).base_url
    if type(b) == "string" and b ~= "" then
        return (string.gsub(b, "/+$", ""))
    end
    return MOCK_BASE
end

local function find(issues: any, key: string): any
    for _, i in ipairs(issues) do
        if string.upper(i.key) == key then
            return i
        end
    end
    return nil
end

local function require_issue(ctx: any, key: string, what: string): any
    local raw = find(load(ctx, "issues"), key)
    if raw == nil then
        error("NOT_FOUND: Jira " .. what .. " returned HTTP 404: Issue does not exist or you do not have permission to see it. (mock)")
    end
    return raw
end

local function transitions_for(ctx: any, status: string): { any }
    local wf = load(ctx, "workflow")
    local out = {}
    for _, t in ipairs(wf.transitions[status] or {}) do
        table.insert(out, model.transition({
            id = t.id, name = t.name, to_status = t.to, to_category = wf.statuses[t.to],
        }))
    end
    return out
end

-- Minimal JQL: clauses joined by AND; `key in (A-1, B-2)`, `project = X`, `status = "Y"`,
-- `status in (...)`, `assignee = name`, `labels = x`, `statusCategory = Done`, `text ~ "x"`;
-- a trailing ORDER BY is ignored.
local function strip(s: string): string
    s = string.gsub(s, "^%s+", "")
    s = string.gsub(s, "%s+$", "")
    s = string.gsub(s, "^\"(.*)\"$", "%1")
    s = string.gsub(s, "^'(.*)'$", "%1")
    return s
end

local function list_of(s: string): { string }
    local out = {}
    local inner = string.match(s, "^%((.*)%)$") or s
    for item in string.gmatch(inner, "[^,]+") do
        table.insert(out, string.lower(strip(item)))
    end
    return out
end

local function clause_matches(issue: any, clause: string): boolean
    local field, op, value = string.match(clause, "^%s*([%w_]+)%s*([!~=]+)%s*(.-)%s*$")
    local in_field, in_values = string.match(clause, "^%s*([%w_]+)%s+[iI][nN]%s+(%b())%s*$")
    local function get(f: string): any
        f = string.lower(f)
        if f == "key" or f == "issuekey" then
            return issue.key
        elseif f == "statuscategory" then
            return ({ todo = "To Do", in_progress = "In Progress", done = "Done" })[issue.status_category or ""] or issue.status_category
        elseif f == "labels" then
            return issue.labels
        elseif f == "issuetype" or f == "type" then
            return issue.issue_type
        elseif f == "text" or f == "summary" then
            return issue.summary
        end
        return issue[f]
    end
    local function eq(actual: any, want: string): boolean
        if type(actual) == "table" then
            for _, x in ipairs(actual) do
                if string.lower(tostring(x)) == want then
                    return true
                end
            end
            return false
        end
        return actual ~= nil and string.lower(tostring(actual)) == want
    end
    if in_field ~= nil then
        local actual = get(in_field)
        for _, want in ipairs(list_of(in_values)) do
            if eq(actual, want) then
                return true
            end
        end
        return false
    end
    if field == nil then
        error("INVALID_REQUEST: Jira search returned HTTP 400: the Mock cannot parse JQL clause '" .. clause .. "'")
    end
    local want = string.lower(strip(value))
    local actual = get(field)
    if op == "=" then
        return eq(actual, want)
    elseif op == "!=" then
        return not eq(actual, want)
    elseif op == "~" then
        return actual ~= nil and string.find(string.lower(tostring(actual)), want, 1, true) ~= nil
    end
    error("INVALID_REQUEST: Jira search returned HTTP 400: unsupported JQL operator '" .. op .. "' (mock)")
end

local function jql_matches(issue: any, jql: string): boolean
    local where = string.gsub(jql, "%s+[oO][rR][dD][eE][rR]%s+[bB][yY].*$", "")
    if string.match(where, "^%s*$") then
        return true
    end
    local rest = where
    while true do
        local s, e = string.find(rest, "%s+[aA][nN][dD]%s+")
        local clause = if s then string.sub(rest, 1, s - 1) else rest
        if not clause_matches(issue, clause) then
            return false
        end
        if s == nil then
            return true
        end
        rest = string.sub(rest, e + 1)
    end
end

local ops = {}

function ops.get_issues(ctx: any, a: any): any
    local issues = load(ctx, "issues")
    local out, seen = {}, {}
    for _, k in ipairs(a.keys or {}) do
        local ok, key = pcall(model.check_key, k)
        local raw = if ok then find(issues, key) else nil
        if raw ~= nil and not seen[key] then
            seen[key] = true
            table.insert(out, model.issue(raw, base_of(ctx), nil))
        end
    end
    return out
end

function ops.get_issue(ctx: any, a: any): any
    local key = model.check_key(a.key)
    return model.issue(require_issue(ctx, key, "get_issue " .. key), base_of(ctx), nil)
end

function ops.search(ctx: any, a: any): any
    if type(a.jql) ~= "string" or a.jql == "" then
        error("INVALID_ARGS: search requires jql")
    end
    local out = {}
    local limit = tonumber(a.max_results) or 500
    for _, raw in ipairs(load(ctx, "issues")) do
        local issue = model.issue(raw, base_of(ctx), nil)
        if #out < limit and jql_matches(issue, a.jql) then
            table.insert(out, issue)
        end
    end
    return out
end

function ops.get_transitions(ctx: any, a: any): any
    local key = model.check_key(a.key)
    local raw = require_issue(ctx, key, "get_transitions " .. key)
    return transitions_for(ctx, raw.status)
end

function ops.transition_issue(ctx: any, a: any): any
    local key = model.check_key(a.key)
    local issues = load(ctx, "issues")
    local raw = find(issues, key)
    if raw == nil then
        error("NOT_FOUND: Jira get_issue " .. key .. " returned HTTP 404: Issue does not exist or you do not have permission to see it. (mock)")
    end
    local target, category = model.target_of(a)
    local issue = model.issue(raw, nil, nil)
    local from = raw.status
    if a.transition_id == nil and model.at_target(issue, target, category) then
        return { key = key, status = from, from_status = from, transitioned = false }
    end
    local available = transitions_for(ctx, from)
    local chosen = model.pick_transition(available, target, a.transition_id, category)
    if chosen == nil then
        local wanted = a.transition_id or target or ("a status of category '" .. tostring(category) .. "'")
        error(string.format("NO_TRANSITION: Jira issue %s cannot move from '%s' to %s (available: %s)",
            key, from, tostring(wanted), model.transition_names(available)))
    end
    raw.status = chosen.to_status
    raw.status_category = chosen.to_category
    if a.resolution ~= nil then
        raw.resolution = a.resolution
    end
    save(ctx, "issues", issues)
    if a.comment ~= nil and string.match(a.comment, "%S") then
        local comments = ctx:await(ctx:mock_get("comments")) or {}
        table.insert(comments, { key = key, body = a.comment })
        save(ctx, "comments", comments)
    end
    return { key = key, status = chosen.to_status, from_status = from, transitioned = true, transition_id = chosen.id }
end

function ops.add_comment(ctx: any, a: any): any
    local key = model.check_key(a.key)
    if type(a.body) ~= "string" or not string.match(a.body, "%S") then
        error("INVALID_ARGS: add_comment requires a non-blank body")
    end
    require_issue(ctx, key, "add_comment " .. key)
    local comments = ctx:await(ctx:mock_get("comments")) or {}
    table.insert(comments, { key = key, body = a.body })
    save(ctx, "comments", comments)
    return { id = tostring(30000 + #comments), key = key, created = "2026-11-23T12:00:00.000+0000" }
end

function ops.create_issue(ctx: any, a: any): any
    if type(a.project) ~= "string" or not string.match(a.project, "^[%w_]+$") then
        error("INVALID_ARGS: project must be a project key, got " .. tostring(a.project))
    end
    if type(a.summary) ~= "string" or a.summary == "" then
        error("INVALID_ARGS: create_issue requires a summary")
    end
    local issues = load(ctx, "issues")
    local project = string.upper(a.project)
    local n = 0
    for _, i in ipairs(issues) do
        local p, num = string.match(i.key, "^([^-]+)%-(%d+)$")
        if p == project and tonumber(num) > n then
            n = tonumber(num) :: number
        end
    end
    local key = project .. "-" .. tostring(n + 1)
    local id = tostring(90000 + #issues)
    table.insert(issues, {
        key = key, id = id, summary = a.summary, status = "To Do", status_category = "todo",
        priority = a.priority, assignee = a.assignee_id, issue_type = a.issue_type, project = project,
        labels = a.labels or {}, description = a.description, parent = a.parent,
        created = "2026-11-23T12:00:00.000+0000", updated = "2026-11-23T12:00:00.000+0000",
    })
    save(ctx, "issues", issues)
    return { key = key, id = id, url = base_of(ctx) .. "/browse/" .. key }
end

function ops.update_issue(ctx: any, a: any): any
    local key = model.check_key(a.key)
    local issues = load(ctx, "issues")
    local raw = find(issues, key)
    if raw == nil then
        error("NOT_FOUND: Jira update_issue " .. key .. " returned HTTP 404: Issue does not exist (mock)")
    end
    local changed = false
    for _, f in ipairs({ "summary", "description", "labels", "priority" }) do
        if a[f] ~= nil then
            raw[f] = a[f]
            changed = true
        end
    end
    if a.assignee_id ~= nil then
        raw.assignee = a.assignee_id
        changed = true
    end
    if not changed then
        error("INVALID_ARGS: update_issue needs at least one field to change")
    end
    save(ctx, "issues", issues)
    return { key = key, updated = true }
end

function Runtime.call(ctx: any): any
    local op = ops[ctx.operation]
    if op == nil then
        error("unknown jira mock operation: " .. tostring(ctx.operation))
    end
    return op(ctx, ctx.args or {})
end

return Runtime
