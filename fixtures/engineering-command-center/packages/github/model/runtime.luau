--!strict
-- Stable records of the GitHub package. Every normalizer accepts either a raw GitHub REST
-- payload or an already-normalized record (the Mock's fixtures), so the connector and the Mock
-- return exactly the same shapes.
local M = {}

-- JSON null arrives as a userdata sentinel: treat it like nil.
local function v(x: any): any
    if type(x) == "userdata" then
        return nil
    end
    return x
end
M.v = v

-- A copy of an argument/query table without its JSON nulls (userdata sentinels), so an
-- optional value sent as null reads as absent. Shallow: nested values are kept as they are.
function M.clean(t: any): any
    local out = {}
    if type(t) == "table" then
        for k, x in pairs(t) do
            if type(x) ~= "userdata" then
                out[k] = x
            end
        end
    end
    return out
end

local function str(x: any): string?
    x = v(x)
    if x == nil then
        return nil
    end
    return tostring(x)
end

local function login(u: any): string?
    u = v(u)
    if type(u) == "table" then
        return str(u.login)
    end
    return str(u)
end

local function names(list: any, field: string): { string }
    local out = {}
    list = v(list)
    if type(list) ~= "table" then
        return out
    end
    for _, item in ipairs(list) do
        item = v(item)
        if type(item) == "table" then
            local n = str(item[field])
            if n ~= nil then
                table.insert(out, n)
            end
        elseif item ~= nil then
            table.insert(out, tostring(item))
        end
    end
    return out
end

-- `owner/name`, both parts limited to GitHub's characters (no traversal, no URL syntax).
function M.check_repository(repo: any): string
    if type(repo) ~= "string" then
        error("INVALID_ARGS: repository must be 'owner/name', got " .. tostring(repo))
    end
    local owner, name = string.match(repo, "^([%w%-_%.]+)/([%w%-_%.]+)$")
    if owner == nil or owner == "." or owner == ".." or name == "." or name == ".." then
        error("INVALID_ARGS: repository must be 'owner/name', got '" .. repo .. "'")
    end
    return repo
end

function M.check_number(n: any, what: string?): number
    local num = tonumber(n)
    if num == nil or num < 1 or num ~= math.floor(num) then
        error("INVALID_ARGS: " .. (what or "number") .. " must be a positive integer, got " .. tostring(n))
    end
    return num
end

local function repo_of(raw: any, hint: string?): string
    local base = v(raw.base)
    if type(base) == "table" then
        local brepo = v(base.repo)
        if type(brepo) == "table" and v(brepo.full_name) ~= nil then
            return tostring(brepo.full_name)
        end
    end
    local r = v(raw.repository)
    if type(r) == "table" and v(r.full_name) ~= nil then
        return tostring(r.full_name)
    elseif type(r) == "string" and r ~= "" then
        return r
    end
    local ru = v(raw.repository_url)
    if type(ru) == "string" then
        local full = string.match(ru, "/repos/([^/]+/[^/]+)$")
        if full ~= nil then
            return full
        end
    end
    return hint or ""
end

local function ref_name(x: any): string?
    x = v(x)
    if type(x) == "table" then
        return str(x.ref)
    end
    return str(x)
end

-- dev.pull_request@1. State is `open`, `closed` (without merging) or `merged`.
function M.pull_request(raw: any, repo_hint: string?): any
    local repository = repo_of(raw, repo_hint)
    local number = tonumber(v(raw.number)) or 0
    local merged = v(raw.merged) == true or v(raw.merged_at) ~= nil or v(raw.state) == "merged"
    local state = if merged then "merged" else (str(raw.state) or "open")
    local head = v(raw.head)
    local pr: { [string]: any } = {
        id = repository .. "#" .. tostring(number),
        number = number,
        title = str(raw.title) or "",
        repository = repository,
        state = state,
        merged = merged,
        draft = v(raw.draft) == true,
        author = login(raw.user) or str(raw.author),
        url = str(raw.html_url) or str(raw.url),
        base = ref_name(raw.base),
        head = ref_name(raw.head),
        head_sha = (type(head) == "table" and str(head.sha)) or str(raw.head_sha),
        labels = names(raw.labels, "name"),
        assignees = names(raw.assignees, "login"),
        requested_reviewers = names(raw.requested_reviewers, "login"),
        created_at = str(raw.created_at),
        updated_at = str(raw.updated_at),
        closed_at = str(raw.closed_at),
        merged_at = str(raw.merged_at),
    }
    if type(v(raw.id)) == "number" then
        pr.github_id = raw.id
    end
    -- Single-PR detail (GET /pulls/{n}); absent from list payloads.
    for _, k in ipairs({ "mergeable_state", "additions", "deletions", "changed_files", "comments", "review_comments", "commits", "body" }) do
        if v(raw[k]) ~= nil then
            pr[k] = raw[k]
        end
    end
    if type(v(raw.mergeable)) == "boolean" then
        pr.mergeable = raw.mergeable
    end
    if v(raw.merged_by) ~= nil then
        pr.merged_by = login(raw.merged_by)
    end
    return pr
end

-- dev.issue@1. Pull requests surfaced by the issues/search APIs carry is_pull_request = true.
function M.issue(raw: any, repo_hint: string?): any
    local repository = repo_of(raw, repo_hint)
    local number = tonumber(v(raw.number)) or 0
    local is_pr = v(raw.pull_request) ~= nil or v(raw.is_pull_request) == true
    local state = str(raw.state) or "open"
    local pr_info = v(raw.pull_request)
    if is_pr and type(pr_info) == "table" and v(pr_info.merged_at) ~= nil then
        state = "merged"
    end
    local issue: { [string]: any } = {
        id = repository .. "#" .. tostring(number),
        number = number,
        title = str(raw.title) or "",
        repository = repository,
        state = state,
        state_reason = str(raw.state_reason),
        is_pull_request = is_pr,
        author = login(raw.user) or str(raw.author),
        labels = names(raw.labels, "name"),
        assignees = names(raw.assignees, "login"),
        comments = tonumber(v(raw.comments)),
        url = str(raw.html_url) or str(raw.url),
        created_at = str(raw.created_at),
        updated_at = str(raw.updated_at),
        closed_at = str(raw.closed_at),
    }
    if v(raw.body) ~= nil then
        issue.body = raw.body
    end
    if type(v(raw.id)) == "number" then
        issue.github_id = raw.id
    end
    return issue
end

-- dev.workflow_run@1.
function M.workflow_run(raw: any, repo_hint: string?): any
    local repository = repo_of(raw, repo_hint)
    return {
        id = tonumber(v(raw.id)) or 0,
        name = str(raw.name) or "",
        title = str(raw.display_title) or str(raw.title),
        workflow_id = tonumber(v(raw.workflow_id)),
        run_number = tonumber(v(raw.run_number)),
        run_attempt = tonumber(v(raw.run_attempt)),
        repository = repository,
        status = str(raw.status) or "unknown",
        conclusion = str(raw.conclusion),
        branch = str(raw.head_branch) or str(raw.branch),
        head_sha = str(raw.head_sha),
        event = str(raw.event),
        actor = login(raw.actor),
        url = str(raw.html_url) or str(raw.url),
        created_at = str(raw.created_at),
        updated_at = str(raw.updated_at),
        started_at = str(raw.run_started_at) or str(raw.started_at),
    }
end

local CHECK_FAILURE = {
    failure = true,
    timed_out = true,
    cancelled = true,
    action_required = true,
    startup_failure = true,
}

-- One CI check: a commit status or a check run, as { name, kind, state, ... } where state is
-- success | failure | pending | neutral.
function M.status_check(raw: any): any
    local state = str(raw.state) or "pending"
    if state == "error" then
        state = "failure"
    end
    return {
        name = str(raw.context) or str(raw.name) or "status",
        kind = "status",
        state = state,
        description = str(raw.description),
        url = str(raw.target_url) or str(raw.url),
        updated_at = str(raw.updated_at),
    }
end

function M.check_run(raw: any): any
    local status = str(raw.status) or "queued"
    local conclusion = str(raw.conclusion)
    local state
    if status ~= "completed" then
        state = "pending"
    elseif conclusion == "success" then
        state = "success"
    elseif conclusion ~= nil and CHECK_FAILURE[conclusion] then
        state = "failure"
    else
        state = "neutral"
    end
    return {
        name = str(raw.name) or "check",
        kind = "check",
        state = state,
        status = status,
        conclusion = conclusion,
        url = str(raw.html_url) or str(raw.details_url) or str(raw.url),
        started_at = str(raw.started_at),
        completed_at = str(raw.completed_at),
    }
end

-- Overall CI state of a ref from its checks: failure > pending > success > neutral; no checks
-- at all is `unknown`.
function M.ci_summary(repository: string, ref: string, checks: { any }): any
    local counts = { success = 0, failure = 0, pending = 0, neutral = 0 }
    for _, c in ipairs(checks) do
        counts[c.state] = (counts[c.state] or 0) + 1
    end
    local state = "unknown"
    if counts.failure > 0 then
        state = "failure"
    elseif counts.pending > 0 then
        state = "pending"
    elseif counts.success > 0 then
        state = "success"
    elseif counts.neutral > 0 then
        state = "neutral"
    end
    return {
        repository = repository,
        ref = ref,
        state = state,
        total = #checks,
        success = counts.success,
        failure = counts.failure,
        pending = counts.pending,
        neutral = counts.neutral,
        checks = checks,
    }
end

-- First Jira-style issue key (`ABC-123`) in a text.
function M.issue_key(text: any): string?
    if type(text) ~= "string" then
        return nil
    end
    return string.match(text, "([A-Z][A-Z0-9]+%-%d+)")
end

return M
