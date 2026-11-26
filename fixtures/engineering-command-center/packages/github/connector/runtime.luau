--!strict
-- GitHub REST connector. Every operation returns the package's normalized records
-- (github.model), never raw API payloads, so results satisfy the declared contracts and match
-- the Mock exactly.
local dash = require("dash")
local model = require("github.model")
local util = require("dash.http_util")

local Runtime = {}

local DEFAULT_BASE = "https://api.github.com"
local DEFAULT_MAX_PAGES = 5

local function base_of(cfg: any): string
    return util.base_url({ base_url = cfg.base_url or DEFAULT_BASE })
end

local function request(ctx: any, method: string, url: string, body: any): any
    local cfg = util.clean(ctx.config)
    local req: { [string]: any } = {
        method = method,
        url = url,
        -- The broker refuses the request, and any redirect hop, outside base_url.
        confine_to = base_of(cfg),
        headers = {
            Accept = "application/vnd.github+json",
            ["X-GitHub-Api-Version"] = cfg.api_version or "2022-11-28",
        },
    }
    if body ~= nil then
        req.body = body
    end
    if cfg.auth ~= "none" then
        req.credential = "auth"
    end
    return ctx:await(ctx.http:request(req))
end

-- One request; raises a coded error (UNKNOWN_OUTCOME for 5xx answers to writes).
local function call(ctx: any, method: string, path: string, query: any, body: any, what: string): any
    local resp = request(ctx, method, util.url(base_of(util.clean(ctx.config)), path, query), body)
    if not util.is_success(resp) then
        util.fail("GitHub", what, resp, method ~= "GET")
    end
    return resp
end

-- GET every page of a list endpoint (Link rel="next", same origin only), up to max_pages.
-- `items_key` selects the list inside an object body (search, workflow runs).
local function get_all(ctx: any, path: string, query: any, items_key: string?, max_pages: any, what: string): { any }
    local cfg = util.clean(ctx.config)
    local base = base_of(cfg)
    local pages = util.max_pages(max_pages, cfg.max_pages, DEFAULT_MAX_PAGES)
    local q = util.merge({ per_page = tonumber(cfg.per_page) or 100 }, query)
    local url: string? = util.url(base, path, q)
    local out = {}
    for page = 1, pages do
        local resp = request(ctx, "GET", url :: string, nil)
        if not util.is_success(resp) then
            util.fail("GitHub", what .. (if page > 1 then " (page " .. page .. ")" else ""), resp, false)
        end
        local body = model.v(resp.body)
        local items = if items_key then model.v(type(body) == "table" and body[items_key] or nil) else body
        if type(items) ~= "table" then
            error("INVALID_RESPONSE: GitHub " .. what .. " did not return a list")
        end
        for _, item in ipairs(items) do
            table.insert(out, item)
        end
        local link = dash.url.parse_links(util.header(resp, "link")).next
        if link == nil then
            break
        end
        -- Relative links resolve against this page; the result must stay under base_url.
        url = util.resolve_link(link, url :: string, base)
    end
    return out
end

local function repo_path(repo: any): string
    local r = model.check_repository(repo)
    local owner, name = string.match(r, "^([^/]+)/(.+)$")
    return "/repos/" .. dash.url.encode(owner) .. "/" .. dash.url.encode(name)
end

local function csv(list: any): string?
    if type(list) ~= "table" or #list == 0 then
        return nil
    end
    return table.concat(list, ",")
end

local ops = {}

function ops.list_prs(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local state = a.state or "open"
    -- The API has no `merged` state: list closed PRs and keep the merged ones.
    local api_state = if state == "merged" then "closed" else state
    -- Closed/merged lists are long: newest activity first, so recent merges are within the
    -- page cap (GitHub's default order is by creation date).
    local sort, dir = a.sort, a.direction
    if sort == nil and api_state ~= "open" then
        sort, dir = "updated", dir or "desc"
    end
    local raw = get_all(ctx, repo_path(repo) .. "/pulls", {
        state = api_state,
        base = a.base,
        head = a.head,
        sort = sort,
        direction = dir,
    }, nil, a.max_pages, "list_prs " .. repo)
    local out = {}
    for _, item in ipairs(raw) do
        local pr = model.pull_request(item, repo)
        if state ~= "merged" or pr.state == "merged" then
            table.insert(out, pr)
        end
    end
    return out
end

function ops.get_pr(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local n = model.check_number(a.number)
    local resp = call(ctx, "GET", repo_path(repo) .. "/pulls/" .. n, nil, nil, "get_pr " .. repo .. "#" .. n)
    return model.pull_request(resp.body, repo)
end

function ops.merge_pr(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local n = model.check_number(a.number)
    local what = "merge_pr " .. repo .. "#" .. n
    local body: { [string]: any } = { merge_method = a.merge_method or "squash" }
    if a.sha then
        body.sha = a.sha
    end
    if a.commit_title then
        body.commit_title = a.commit_title
    end
    if a.commit_message then
        body.commit_message = a.commit_message
    end
    local resp = request(ctx, "PUT", util.url(base_of(util.clean(ctx.config)), repo_path(repo) .. "/pulls/" .. n .. "/merge", nil), body)
    if resp.status == 405 then
        error("NOT_MERGEABLE: GitHub " .. what .. ": " .. (util.error_message(resp.body) or "pull request is not mergeable"))
    elseif resp.status == 409 then
        error("CONFLICT: GitHub " .. what .. ": " .. (util.error_message(resp.body) or "head branch was modified"))
    elseif not util.is_success(resp) then
        util.fail("GitHub", what, resp, true)
    end
    local res = model.v(resp.body) or {}
    return {
        merged = model.v(res.merged) ~= false,
        sha = tostring(model.v(res.sha) or ""),
        message = model.v(res.message),
    }
end

function ops.list_issues(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local raw = get_all(ctx, repo_path(repo) .. "/issues", {
        state = a.state or "open",
        labels = csv(a.labels),
        assignee = a.assignee,
        creator = a.creator,
        since = a.since,
        sort = a.sort,
        direction = a.direction,
    }, nil, a.max_pages, "list_issues " .. repo)
    local out = {}
    for _, item in ipairs(raw) do
        -- The issues endpoint also returns pull requests.
        if model.v(item.pull_request) == nil then
            table.insert(out, model.issue(item, repo))
        end
    end
    return out
end

function ops.get_issue(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local n = model.check_number(a.number)
    local resp = call(ctx, "GET", repo_path(repo) .. "/issues/" .. n, nil, nil, "get_issue " .. repo .. "#" .. n)
    return model.issue(resp.body, repo)
end

function ops.create_issue(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    if type(a.title) ~= "string" or a.title == "" then
        error("INVALID_ARGS: create_issue requires a non-empty title")
    end
    local resp = call(ctx, "POST", repo_path(repo) .. "/issues", nil, {
        title = a.title,
        body = a.body,
        labels = a.labels,
        assignees = a.assignees,
        milestone = a.milestone,
    }, "create_issue " .. repo)
    return model.issue(resp.body, repo)
end

function ops.update_issue(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local n = model.check_number(a.number)
    local body = {}
    for _, k in ipairs({ "title", "body", "state", "state_reason", "labels", "assignees" }) do
        if a[k] ~= nil then
            body[k] = a[k]
        end
    end
    if next(body) == nil then
        error("INVALID_ARGS: update_issue needs at least one field to change")
    end
    local resp = call(ctx, "PATCH", repo_path(repo) .. "/issues/" .. n, nil, body, "update_issue " .. repo .. "#" .. n)
    return model.issue(resp.body, repo)
end

function ops.add_comment(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local n = model.check_number(a.number)
    if type(a.body) ~= "string" or a.body == "" then
        error("INVALID_ARGS: add_comment requires a non-empty body")
    end
    local resp = call(ctx, "POST", repo_path(repo) .. "/issues/" .. n .. "/comments", nil, { body = a.body }, "add_comment " .. repo .. "#" .. n)
    local c = model.v(resp.body) or {}
    return {
        id = tonumber(model.v(c.id)) or 0,
        url = model.v(c.html_url),
        created_at = model.v(c.created_at),
    }
end

function ops.search_issues(ctx: any, a: any): any
    if type(a.query) ~= "string" or a.query == "" then
        error("INVALID_ARGS: search_issues requires a query")
    end
    -- The search API serves at most 1000 results (10 pages of 100).
    local cfg_pages = util.clean(ctx.config).max_pages
    local pages = math.min(util.max_pages(a.max_pages, cfg_pages, 3), 10)
    local raw = get_all(ctx, "/search/issues", { q = a.query, sort = a.sort, order = a.order }, "items", pages, "search_issues")
    local out = {}
    for _, item in ipairs(raw) do
        table.insert(out, model.issue(item, nil))
    end
    return out
end

function ops.get_ci_status(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    if type(a.ref) ~= "string" or a.ref == "" then
        error("INVALID_ARGS: get_ci_status requires a ref (branch, tag or commit SHA)")
    end
    util.check_segment_value(a.ref, "ref")
    local commit = repo_path(repo) .. "/commits/" .. dash.url.encode(a.ref)
    local what = "get_ci_status " .. repo .. "@" .. a.ref
    local checks = {}
    local combined = call(ctx, "GET", commit .. "/status", { per_page = 100 }, nil, what)
    local statuses = model.v((model.v(combined.body) or {}).statuses)
    if type(statuses) == "table" then
        for _, s in ipairs(statuses) do
            table.insert(checks, model.status_check(s))
        end
    end
    local runs = get_all(ctx, commit .. "/check-runs", nil, "check_runs", nil, what)
    for _, r in ipairs(runs) do
        table.insert(checks, model.check_run(r))
    end
    return model.ci_summary(repo, a.ref, checks)
end

function ops.list_workflow_runs(ctx: any, a: any): any
    local repo = model.check_repository(a.repository)
    local path = repo_path(repo) .. "/actions/runs"
    if a.workflow ~= nil and a.workflow ~= "" then
        if not string.match(a.workflow, "^[%w%-_%.]+$") or a.workflow == "." or a.workflow == ".." then
            error("INVALID_ARGS: workflow must be a workflow id or file name such as ci.yml")
        end
        path = repo_path(repo) .. "/actions/workflows/" .. dash.url.encode(a.workflow) .. "/runs"
    end
    local raw = get_all(ctx, path, {
        branch = a.branch,
        status = a.status,
        event = a.event,
        actor = a.actor,
    }, "workflow_runs", a.max_pages or util.clean(ctx.config).max_pages or 1, "list_workflow_runs " .. repo)
    local out = {}
    for _, r in ipairs(raw) do
        table.insert(out, model.workflow_run(r, repo))
    end
    return out
end

function Runtime.call(ctx: any): any
    local op = ops[ctx.operation]
    if op == nil then
        error("unknown GitHub connector operation: " .. tostring(ctx.operation))
    end
    -- Optional values may arrive as JSON null: drop them so they read as absent.
    return op(ctx, util.clean(ctx.args))
end

return Runtime
