--!strict
-- Fixture-backed Mock for github.connector. Records go through the same github.model
-- normalizers as the real connector; writes mutate the Mock's fixture state.
local model = require("github.model")

local Runtime = {}

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

local function not_found(what: string)
    error("NOT_FOUND: GitHub " .. what .. " returned HTTP 404: resource not found (mock)")
end

local function has(list: any, value: any): boolean
    for _, x in ipairs(list or {}) do
        if x == value then
            return true
        end
    end
    return false
end

local function find(list: any, repo: string, number: number): (any, number?)
    for i, item in ipairs(list) do
        if item.repository == repo and item.number == number then
            return item, i
        end
    end
    return nil, nil
end

-- Minimal GitHub search: repo:, is:pr|issue|open|closed|merged, label:, author:, assignee:,
-- plus free text matched against titles.
local function matches(item: any, query: string): boolean
    for token in string.gmatch(query, "%S+") do
        local key, value = string.match(token, "^([%w%-]+):(.+)$")
        if key == "repo" then
            if item.repository ~= value then
                return false
            end
        elseif key == "is" then
            if value == "pr" and not item.is_pull_request then
                return false
            elseif value == "issue" and item.is_pull_request then
                return false
            elseif (value == "open" or value == "closed" or value == "merged") then
                local st = item.state
                if value == "closed" and st == "merged" then
                    st = "closed"
                end
                if st ~= value then
                    return false
                end
            end
        elseif key == "label" then
            if not has(item.labels, value) then
                return false
            end
        elseif key == "author" then
            if item.author ~= value then
                return false
            end
        elseif key == "assignee" then
            if not has(item.assignees, value) then
                return false
            end
        elseif key == nil then
            if not string.find(string.lower(item.title or ""), string.lower(token), 1, true) then
                return false
            end
        end
    end
    return true
end

local ops = {}

function ops.list_prs(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local state = a.state or "open"
    local out = {}
    for _, raw in ipairs(load(ctx, "prs")) do
        local pr = model.pull_request(raw, repo)
        local closed_match = state == "closed" and (pr.state == "closed" or pr.state == "merged")
        if pr.repository == repo and (state == "all" or pr.state == state or closed_match)
            and (a.base == nil or pr.base == a.base) then
            table.insert(out, pr)
        end
    end
    return out
end

function ops.get_pr(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local n = model.check_number(a.number)
    local raw = find(load(ctx, "prs"), repo, n)
    if raw == nil then
        not_found("get_pr " .. repo .. "#" .. n)
    end
    return model.pull_request(raw, repo)
end

function ops.merge_pr(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local n = model.check_number(a.number)
    local prs = load(ctx, "prs")
    local raw = find(prs, repo, n)
    if raw == nil then
        not_found("merge_pr " .. repo .. "#" .. n)
    end
    if raw.state ~= "open" or raw.draft == true then
        error("NOT_MERGEABLE: GitHub merge_pr " .. repo .. "#" .. n .. ": Pull Request is not mergeable (mock)")
    end
    if a.sha ~= nil and raw.head_sha ~= nil and a.sha ~= raw.head_sha then
        error("CONFLICT: GitHub merge_pr " .. repo .. "#" .. n .. ": Head branch was modified. Review and try the merge again. (mock)")
    end
    raw.state = "merged"
    raw.merged = true
    raw.merged_at = "2026-11-23T12:00:00Z"
    save(ctx, "prs", prs)
    return { merged = true, sha = "mocksha_" .. tostring(n), message = "Pull Request successfully merged" }
end

local function all_issues(ctx: any, include_prs: boolean): { any }
    local out = {}
    for _, raw in ipairs(load(ctx, "issues")) do
        table.insert(out, model.issue(raw, nil))
    end
    if include_prs then
        for _, raw in ipairs(load(ctx, "prs")) do
            local pr = model.pull_request(raw, nil)
            local item = model.issue({
                number = pr.number, title = pr.title, repository = pr.repository, state = pr.state,
                author = pr.author, labels = pr.labels, assignees = pr.assignees, url = pr.url,
                created_at = pr.created_at, updated_at = pr.updated_at, is_pull_request = true,
            }, nil)
            table.insert(out, item)
        end
    end
    return out
end

function ops.list_issues(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local state = a.state or "open"
    local out = {}
    for _, issue in ipairs(all_issues(ctx, false)) do
        local ok = issue.repository == repo and (state == "all" or issue.state == state)
        if ok and a.assignee ~= nil then
            ok = has(issue.assignees, a.assignee)
        end
        if ok and a.creator ~= nil then
            ok = issue.author == a.creator
        end
        if ok and type(a.labels) == "table" then
            for _, l in ipairs(a.labels) do
                if not has(issue.labels, l) then
                    ok = false
                end
            end
        end
        if ok then
            table.insert(out, issue)
        end
    end
    return out
end

function ops.get_issue(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local n = model.check_number(a.number)
    for _, issue in ipairs(all_issues(ctx, true)) do
        if issue.repository == repo and issue.number == n then
            return issue
        end
    end
    not_found("get_issue " .. repo .. "#" .. n)
    return nil
end

function ops.create_issue(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    if type(a.title) ~= "string" or a.title == "" then
        error("INVALID_ARGS: create_issue requires a non-empty title")
    end
    local issues = load(ctx, "issues")
    local next_number = 1000
    for _, i in ipairs(issues) do
        if i.number >= next_number then
            next_number = i.number + 1
        end
    end
    local raw = {
        number = next_number, title = a.title, body = a.body or "", repository = repo, state = "open",
        author = "dash-mock", labels = a.labels or {}, assignees = a.assignees or {}, comments = 0,
        created_at = "2026-11-23T12:00:00Z", updated_at = "2026-11-23T12:00:00Z",
        url = "https://github.com/" .. repo .. "/issues/" .. next_number,
    }
    table.insert(issues, raw)
    save(ctx, "issues", issues)
    return model.issue(raw, repo)
end

function ops.update_issue(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local n = model.check_number(a.number)
    local issues = load(ctx, "issues")
    local raw = find(issues, repo, n)
    if raw == nil then
        not_found("update_issue " .. repo .. "#" .. n)
    end
    local changed = false
    for _, k in ipairs({ "title", "body", "state", "state_reason", "labels", "assignees" }) do
        if a[k] ~= nil then
            raw[k] = a[k]
            changed = true
        end
    end
    if not changed then
        error("INVALID_ARGS: update_issue needs at least one field to change")
    end
    if a.state == "closed" then
        raw.closed_at = "2026-11-23T12:00:00Z"
    elseif a.state == "open" then
        raw.closed_at = nil
    end
    save(ctx, "issues", issues)
    return model.issue(raw, repo)
end

function ops.add_comment(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local n = model.check_number(a.number)
    if type(a.body) ~= "string" or a.body == "" then
        error("INVALID_ARGS: add_comment requires a non-empty body")
    end
    ops.get_issue(ctx, a)
    local comments = ctx:await(ctx:mock_get("comments")) or {}
    table.insert(comments, { repository = repo, number = n, body = a.body })
    save(ctx, "comments", comments)
    local issues = load(ctx, "issues")
    local raw = find(issues, repo, n)
    if raw ~= nil then
        raw.comments = (raw.comments or 0) + 1
        save(ctx, "issues", issues)
    end
    return {
        id = 500000 + #comments,
        url = "https://github.com/" .. repo .. "/issues/" .. n .. "#issuecomment-" .. (500000 + #comments),
        created_at = "2026-11-23T12:00:00Z",
    }
end

function ops.search_issues(ctx: any, a: any): any
    if type(a.query) ~= "string" or a.query == "" then
        error("INVALID_ARGS: search_issues requires a query")
    end
    local out = {}
    for _, item in ipairs(all_issues(ctx, true)) do
        if matches(item, a.query) then
            table.insert(out, item)
        end
    end
    return out
end

function ops.get_ci_status(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    if type(a.ref) ~= "string" or a.ref == "" then
        error("INVALID_ARGS: get_ci_status requires a ref (branch, tag or commit SHA)")
    end
    for _, entry in ipairs(load(ctx, "checks")) do
        if entry.repository == repo and has(entry.refs, a.ref) then
            local checks = {}
            for _, s in ipairs(entry.statuses or {}) do
                table.insert(checks, model.status_check(s))
            end
            for _, r in ipairs(entry.check_runs or {}) do
                table.insert(checks, model.check_run(r))
            end
            return model.ci_summary(repo, a.ref, checks)
        end
    end
    not_found("get_ci_status " .. repo .. "@" .. a.ref)
    return nil
end

-- Like the API's `/actions/workflows/{workflow_id}/runs`: a workflow id or its file name.
local function workflow_matches(raw: any, run: any, workflow: any): boolean
    local w = tostring(workflow)
    if tostring(run.workflow_id) == w then
        return true
    end
    local path = model.v(raw.path)
    return type(path) == "string" and string.match(path, "([^/]+)$") == w
end

function ops.list_workflow_runs(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local out = {}
    for _, raw in ipairs(load(ctx, "workflow_runs")) do
        local run = model.workflow_run(raw, repo)
        local ok = run.repository == repo
            and (a.branch == nil or run.branch == a.branch)
            and (a.status == nil or run.status == a.status or run.conclusion == a.status)
            and (a.event == nil or run.event == a.event)
            and (a.actor == nil or run.actor == a.actor)
            and (a.workflow == nil or workflow_matches(raw, run, a.workflow))
        if ok then
            table.insert(out, run)
        end
    end
    return out
end

function Runtime.call(ctx: any): any
    local op = ops[ctx.operation]
    if op == nil then
        error("unknown github mock operation: " .. tostring(ctx.operation))
    end
    return op(ctx, model.clean(ctx.args))
end

return Runtime
