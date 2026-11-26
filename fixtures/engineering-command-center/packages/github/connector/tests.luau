local dash = require("dash")
local Runtime = require("runtime")

-- Fake ctx answering real GitHub REST payloads per "METHOD url" and recording requests.
local function fake(routes, operation, args, cfg)
    local ctx = { config = cfg or {}, operation = operation, args = args, sent = {} }
    ctx.http = {
        request = function(_self, req)
            table.insert(ctx.sent, req)
            local resp = routes[req.method .. " " .. req.url]
            if resp == nil then
                error("unexpected request " .. req.method .. " " .. req.url)
            end
            resp.headers = resp.headers or {}
            return resp
        end,
    }
    function ctx:await(v)
        return v
    end
    return ctx
end

local API = "https://api.github.com"

local function gh_pr(number, extra)
    local pr = {
        id = 1000 + number, number = number, state = "open", title = "DEV-1 change " .. number,
        user = { login = "alice" }, draft = false, html_url = "https://github.com/acme/api/pull/" .. number,
        base = { ref = "main", repo = { full_name = "acme/api" } },
        head = { ref = "feat/" .. number, sha = "sha" .. number },
        labels = { { name = "bug" } }, assignees = { { login = "bob" } }, requested_reviewers = {},
        created_at = "2026-01-01T00:00:00Z", updated_at = "2026-01-02T00:00:00Z",
    }
    for k, v in pairs(extra or {}) do
        pr[k] = v
    end
    return pr
end

dash.test("list_prs normalizes REST payloads, follows Link pages and sends GitHub headers", function(_)
    local ctx = fake({
        ["GET " .. API .. "/repos/acme/api/pulls?per_page=100&state=open"] = {
            status = 200, body = { gh_pr(1) },
            headers = { link = '<' .. API .. '/repositories/9/pulls?page=2&per_page=100&state=open>; rel="next"' },
        },
        ["GET " .. API .. "/repositories/9/pulls?page=2&per_page=100&state=open"] = { status = 200, body = { gh_pr(2) } },
    }, "list_prs", { repository = "acme/api" })
    local prs = Runtime.call(ctx)
    dash.assert_eq(#prs, 2, "both pages")
    local p = prs[1]
    dash.assert_eq(p.id, "acme/api#1", "stable id")
    dash.assert_eq(p.repository, "acme/api", "repository from base.repo")
    dash.assert_eq(p.author, "alice", "author from user.login")
    dash.assert_eq(p.labels, { "bug" }, "label names")
    dash.assert_eq(p.assignees, { "bob" }, "assignee logins")
    dash.assert_eq(p.head_sha, "sha1", "head sha")
    dash.assert_eq(p.github_id, 1001, "numeric GitHub id kept separately")
    dash.assert_eq(ctx.sent[1].headers.Accept, "application/vnd.github+json", "Accept")
    dash.assert_eq(ctx.sent[1].headers["X-GitHub-Api-Version"], "2022-11-28", "API version")
    dash.assert_eq(ctx.sent[1].credential, "auth", "credential handle")
end)

dash.test("state merged lists closed PRs and keeps merged ones", function(_)
    local prs = Runtime.call(fake({
        ["GET " .. API .. "/repos/acme/api/pulls?direction=desc&per_page=100&sort=updated&state=closed"] = { status = 200, body = {
            gh_pr(1, { state = "closed", merged_at = "2026-01-03T00:00:00Z" }),
            gh_pr(2, { state = "closed" }),
        } },
    }, "list_prs", { repository = "acme/api", state = "merged" }))
    dash.assert_eq(#prs, 1, "only merged")
    dash.assert_eq(prs[1].state, "merged", "merged state")
    dash.assert(prs[1].merged == true, "merged flag")
end)

dash.test("pagination never leaves the API origin and respects max_pages", function(_)
    local evil = fake({
        ["GET " .. API .. "/repos/acme/api/pulls?per_page=100&state=open"] = {
            status = 200, body = { gh_pr(1) }, headers = { link = '<https://evil.test/x>; rel="next"' },
        },
    }, "list_prs", { repository = "acme/api" })
    local ok, err = pcall(Runtime.call, evil)
    dash.assert(not ok and string.find(tostring(err), "leaves", 1, true) ~= nil, tostring(err))

    local capped = fake({
        ["GET " .. API .. "/repos/acme/api/pulls?per_page=100&state=open"] = {
            status = 200, body = { gh_pr(1) }, headers = { link = '<' .. API .. '/p2>; rel="next"' },
        },
    }, "list_prs", { repository = "acme/api", max_pages = 1 })
    dash.assert_eq(#Runtime.call(capped), 1, "stops after max_pages")
end)

dash.test("GitHub Enterprise base_url, per_page and anonymous access", function(_)
    local ctx = fake({
        ["GET https://ghe.acme.test/api/v3/repos/acme/api/pulls?direction=desc&per_page=20&sort=updated&state=all"] = { status = 200, body = {} },
    }, "list_prs", { repository = "acme/api", state = "all" }, { base_url = "https://ghe.acme.test/api/v3/", per_page = 20, auth = "none" })
    dash.assert_eq(Runtime.call(ctx), {}, "empty list")
    dash.assert(ctx.sent[1].credential == nil, "anonymous")
end)

dash.test("invalid repositories are refused before any request", function(_)
    for _, bad in ipairs({ "acme", "acme/api/x", "../etc", "acme/..", "acme/a b", "acme/api?x=1" }) do
        local ctx = fake({}, "list_prs", { repository = bad })
        local ok, err = pcall(Runtime.call, ctx)
        dash.assert(not ok and string.find(tostring(err), "INVALID_ARGS", 1, true) ~= nil, bad .. ": " .. tostring(err))
        dash.assert_eq(#ctx.sent, 0, "nothing sent for " .. bad)
    end
end)

dash.test("get_pr includes mergeability detail", function(_)
    local pr = Runtime.call(fake({
        ["GET " .. API .. "/repos/acme/api/pulls/7"] = { status = 200, body = gh_pr(7, {
            mergeable = false, mergeable_state = "dirty", additions = 10, deletions = 2, merged_by = { login = "carol" },
        }) },
    }, "get_pr", { repository = "acme/api", number = 7 }))
    dash.assert(pr.mergeable == false, "mergeable")
    dash.assert_eq(pr.mergeable_state, "dirty", "mergeable_state")
    dash.assert_eq(pr.additions, 10, "additions")
    dash.assert_eq(pr.merged_by, "carol", "merged_by login")
end)

dash.test("merge_pr sends method and sha; 405/409 are definitive, 5xx unknown", function(_)
    local url = "PUT " .. API .. "/repos/acme/api/pulls/7/merge"
    local ctx = fake({ [url] = { status = 200, body = { merged = true, sha = "m1", message = "Pull Request successfully merged" } } },
        "merge_pr", { repository = "acme/api", number = 7, sha = "sha7" })
    local res = Runtime.call(ctx)
    dash.assert_eq(res, { merged = true, sha = "m1", message = "Pull Request successfully merged" }, "result")
    dash.assert_eq(ctx.sent[1].body, { merge_method = "squash", sha = "sha7" }, "request body")
    local cases = {
        { status = 405, code = "NOT_MERGEABLE" },
        { status = 409, code = "CONFLICT" },
        { status = 502, code = "UNKNOWN_OUTCOME" },
        { status = 403, code = "FORBIDDEN" },
    }
    for _, c in ipairs(cases) do
        local ok, err = pcall(Runtime.call, fake({ [url] = { status = c.status, body = { message = "m" } } },
            "merge_pr", { repository = "acme/api", number = 7 }))
        dash.assert(not ok and string.find(tostring(err), c.code, 1, true) ~= nil, c.status .. ": " .. tostring(err))
    end
end)

dash.test("rate limits surface as RATE_LIMITED with the reset time", function(_)
    local ok, err = pcall(Runtime.call, fake({
        ["GET " .. API .. "/repos/acme/api/pulls/1"] = {
            status = 403, body = { message = "API rate limit exceeded" },
            headers = { ["x-ratelimit-remaining"] = "0", ["x-ratelimit-reset"] = "1767225600" },
        },
    }, "get_pr", { repository = "acme/api", number = 1 }))
    dash.assert(not ok, "fails")
    dash.assert(string.find(tostring(err), "RATE_LIMITED: GitHub get_pr acme/api#1 returned HTTP 403: API rate limit exceeded (rate limit resets at epoch 1767225600)", 1, true) ~= nil, tostring(err))
end)

dash.test("list_issues drops pull requests and encodes label filters", function(_)
    local issues = Runtime.call(fake({
        ["GET " .. API .. "/repos/acme/api/issues?labels=bug%2Cp1&per_page=100&state=open"] = { status = 200, body = {
            { id = 1, number = 3, title = "Bug", state = "open", user = { login = "a" }, labels = { { name = "bug" } }, assignees = {}, comments = 2, repository_url = API .. "/repos/acme/api" },
            { id = 2, number = 4, title = "A PR", state = "open", user = { login = "b" }, labels = {}, assignees = {}, pull_request = { url = "x" } },
        } },
    }, "list_issues", { repository = "acme/api", labels = { "bug", "p1" } }))
    dash.assert_eq(#issues, 1, "pull requests dropped")
    dash.assert_eq(issues[1].id, "acme/api#3", "id")
    dash.assert_eq(issues[1].comments, 2, "comments")
    dash.assert(issues[1].is_pull_request == false, "flag")
end)

dash.test("issue writes send only the given fields", function(_)
    local created = fake({
        ["POST " .. API .. "/repos/acme/api/issues"] = { status = 201, body = { id = 9, number = 50, title = "T", state = "open", user = { login = "me" }, labels = {}, assignees = {}, html_url = "https://github.com/acme/api/issues/50" } },
    }, "create_issue", { repository = "acme/api", title = "T", labels = { "bug" } })
    local issue = Runtime.call(created)
    dash.assert_eq(issue.number, 50, "number")
    dash.assert_eq(created.sent[1].body, { title = "T", labels = { "bug" } }, "body")

    local closed = fake({
        ["PATCH " .. API .. "/repos/acme/api/issues/50"] = { status = 200, body = { id = 9, number = 50, title = "T", state = "closed", state_reason = "completed", labels = {}, assignees = {} } },
    }, "update_issue", { repository = "acme/api", number = 50, state = "closed", state_reason = "completed" })
    dash.assert_eq(Runtime.call(closed).state, "closed", "closed")
    dash.assert_eq(closed.sent[1].body, { state = "closed", state_reason = "completed" }, "patch body")

    local ok, err = pcall(Runtime.call, fake({}, "update_issue", { repository = "acme/api", number = 50 }))
    dash.assert(not ok and string.find(tostring(err), "at least one field", 1, true) ~= nil, tostring(err))

    local c = fake({
        ["POST " .. API .. "/repos/acme/api/issues/50/comments"] = { status = 201, body = { id = 77, html_url = "u", created_at = "t" } },
    }, "add_comment", { repository = "acme/api", number = 50, body = "hi" })
    dash.assert_eq(Runtime.call(c), { id = 77, url = "u", created_at = "t" }, "comment")
    local ok2, err2 = pcall(Runtime.call, fake({
        ["POST " .. API .. "/repos/acme/api/issues/50/comments"] = { status = 503, body = "down" },
    }, "add_comment", { repository = "acme/api", number = 50, body = "hi" }))
    dash.assert(not ok2 and string.find(tostring(err2), "UNKNOWN_OUTCOME", 1, true) ~= nil, tostring(err2))
end)

dash.test("search_issues reads items, derives repositories and marks merged PRs", function(_)
    local res = Runtime.call(fake({
        ["GET " .. API .. "/search/issues?per_page=100&q=is%3Apr%20org%3Aacme"] = { status = 200, body = {
            total_count = 2, incomplete_results = false, items = {
                { id = 1, number = 5, title = "PR", state = "closed", repository_url = API .. "/repos/acme/web", pull_request = { merged_at = "2026-01-01T00:00:00Z" }, labels = {}, assignees = {} },
                { id = 2, number = 6, title = "Issue", state = "open", repository_url = API .. "/repos/acme/api", labels = {}, assignees = {} },
            },
        } },
    }, "search_issues", { query = "is:pr org:acme" }))
    dash.assert_eq(#res, 2, "items")
    dash.assert_eq(res[1].repository, "acme/web", "repository from repository_url")
    dash.assert_eq(res[1].state, "merged", "merged PR")
    dash.assert(res[1].is_pull_request == true and res[2].is_pull_request == false, "kinds")
end)

dash.test("get_ci_status combines commit statuses and check runs; refs are encoded", function(_)
    local commit = API .. "/repos/acme/api/commits/feature%2Fx"
    local ci = Runtime.call(fake({
        ["GET " .. commit .. "/status?per_page=100"] = { status = 200, body = { state = "success", statuses = {
            { context = "ci/legacy", state = "success", description = "ok", target_url = "u" },
        } } },
        ["GET " .. commit .. "/check-runs?per_page=100"] = { status = 200, body = { total_count = 2, check_runs = {
            { name = "build", status = "completed", conclusion = "success", html_url = "b" },
            { name = "e2e", status = "in_progress" },
        } } },
    }, "get_ci_status", { repository = "acme/api", ref = "feature/x" }))
    dash.assert_eq(ci.state, "pending", "in-progress check keeps the ref pending")
    dash.assert_eq(ci.total, 3, "all checks")
    dash.assert_eq(ci.checks[1].kind, "status", "status first")
    dash.assert_eq(ci.checks[3].state, "pending", "check run state")
end)

dash.test("list_workflow_runs targets one workflow and normalizes runs", function(_)
    local runs = Runtime.call(fake({
        ["GET " .. API .. "/repos/acme/api/actions/workflows/ci.yml/runs?branch=main&per_page=100"] = { status = 200, body = {
            total_count = 1, workflow_runs = {
                { id = 123456789012, name = "CI", display_title = "Fix", workflow_id = 5, run_number = 9, run_attempt = 1, status = "completed", conclusion = "failure", head_branch = "main", head_sha = "abc", event = "push", actor = { login = "bob" }, html_url = "u", created_at = "c", updated_at = "u2", run_started_at = "s" },
            },
        } },
    }, "list_workflow_runs", { repository = "acme/api", workflow = "ci.yml", branch = "main" }))
    dash.assert_eq(#runs, 1, "one run")
    dash.assert_eq(runs[1].id, 123456789012, "large run id")
    dash.assert_eq(runs[1].conclusion, "failure", "conclusion")
    dash.assert_eq(runs[1].branch, "main", "branch")
    dash.assert_eq(runs[1].actor, "bob", "actor")
end)
