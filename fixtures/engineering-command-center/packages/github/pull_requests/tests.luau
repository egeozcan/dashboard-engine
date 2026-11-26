local dash = require("dash")
local Runtime = require("runtime")

dash.test("filters open PRs by repository via the github Mock", function(make_ctx)
    local ctx = make_ctx({
        query = { repository = "acme/platform", state = "open" },
    })
    local prs = Runtime.query(ctx)
    dash.assert_eq(#prs, 3, "open acme/platform PRs served by the Mock fixture")
    for _, pr in ipairs(prs) do
        dash.assert_eq(pr.repository, "acme/platform", "repository filter")
        dash.assert_eq(pr.state, "open", "state filter")
    end
end)

dash.test("other repositories are filtered out", function(make_ctx)
    local ctx = make_ctx({ query = { repository = "acme/payments" } })
    local prs = Runtime.query(ctx)
    dash.assert_eq(#prs, 1, "one open acme/payments PR")
    dash.assert_eq(prs[1].number, 7, "payments PR number")
end)

dash.test("the reader input cannot merge through the connector", function(make_ctx)
    local ctx = make_ctx({ query = { repository = "acme/platform" } })
    local ok, err = pcall(function()
        return ctx:await(ctx.inputs.github:call("merge_pr", { repository = "acme/platform", number = 42 }))
    end)
    dash.assert(not ok, "merge_pr requires pull_requests.merge, which this input does not declare")
    dash.assert(string.find(tostring(err), "pull_requests.merge", 1, true) ~= nil, tostring(err))
    local prs = Runtime.query(ctx)
    dash.assert_eq(#prs, 3, "PR 42 is still open")
end)

-- Stand-in ctx with scripted open-PR lists, a get_pr lookup, in-memory Instance State and
-- recorded Events, to check the polling merge detection (§16 behaviour 7) deterministically.
local function polling_ctx(snapshots, details)
    local state = {}
    local ctx = {
        query = { repository = "acme/platform", state = "open" },
        emitted = {},
        calls = 0,
        lookups = {},
        inputs = { github = {} },
        state = {
            get = function(_self, key)
                return state[key]
            end,
            set = function(_self, key, value)
                state[key] = value
                return nil
            end,
        },
    }
    ctx.inputs.github.call = function(_self, op, args)
        if op == "list_prs" then
            dash.assert_eq(args, { repository = "acme/platform", state = "open" }, "lists only open PRs")
            ctx.calls += 1
            return snapshots[ctx.calls]
        elseif op == "get_pr" then
            table.insert(ctx.lookups, args.number)
            local d = details[args.number]
            if type(d) == "string" then
                error(d)
            end
            return d
        end
        error("unexpected operation " .. tostring(op))
    end
    function ctx:await(v)
        return v
    end
    function ctx:emit(event_type, payload, durable)
        table.insert(self.emitted, { event_type = event_type, payload = payload, durable = durable })
        return nil
    end
    return ctx
end

local function pr(number, title, state)
    return { id = "acme/platform#" .. number, number = number, title = title, repository = "acme/platform", state = state }
end

dash.test("polling emits one durable github.pr_merged when an open PR turns merged", function(_make_ctx)
    local open42 = pr(42, "DEV-123 Improve caching", "open")
    local open44 = pr(44, "Refactor README", "open")
    local ctx = polling_ctx({ { open42, open44 }, { open44 }, { open44 } }, {
        [42] = pr(42, "DEV-123 Improve caching", "merged"),
    })

    local first = Runtime.query(ctx)
    dash.assert_eq(#first, 2, "both PRs open")
    dash.assert_eq(#ctx.emitted, 0, "nothing merged yet")

    local second = Runtime.query(ctx)
    dash.assert_eq(#second, 1, "merged PR leaves the open list")
    dash.assert_eq(ctx.lookups, { 42 }, "the vanished PR is confirmed with get_pr")
    dash.assert_eq(#ctx.emitted, 1, "one merge detected")
    local ev = ctx.emitted[1]
    dash.assert_eq(ev.event_type, "github.pr_merged", "event type matches the merge action's")
    dash.assert(ev.durable == true, "durable Event")
    dash.assert_eq(ev.payload.number, 42, "merged PR number")
    dash.assert_eq(ev.payload.issue_key, "DEV-123", "Jira key from the title")
    dash.assert_eq(ev.payload.source, "polling", "source")

    Runtime.query(ctx)
    dash.assert_eq(#ctx.emitted, 1, "an already-merged PR is not reported again")
    dash.assert_eq(ctx.lookups, { 42 }, "and not looked up again")
end)

dash.test("closed-unmerged PRs are dropped silently; unconfirmed ones are retried", function(_make_ctx)
    local open1 = pr(1, "A", "open")
    local open2 = pr(2, "B", "open")
    local open3 = pr(3, "C", "open")
    local ctx = polling_ctx({ { open1, open2, open3 }, {}, {} }, {
        [1] = pr(1, "A", "closed"),
        [2] = "RATE_LIMITED: GitHub get_pr returned HTTP 429",
        [3] = "NOT_FOUND: GitHub get_pr returned HTTP 404",
    })
    Runtime.query(ctx)
    Runtime.query(ctx)
    dash.assert_eq(#ctx.emitted, 0, "no merges")
    table.sort(ctx.lookups)
    dash.assert_eq(ctx.lookups, { 1, 2, 3 }, "each vanished PR looked up once")
    ctx.lookups = {}
    Runtime.query(ctx)
    dash.assert_eq(ctx.lookups, { 2 }, "only the rate-limited one is retried")
end)

dash.test("a base filter applies to the rows, not to merge detection", function(_make_ctx)
    local function on(number, base, state)
        local p = pr(number, "T" .. number, state)
        p.base = base
        return p
    end
    local ctx = polling_ctx({ { on(1, "main", "open"), on(2, "release", "open") }, { on(2, "release", "open") } }, {
        [1] = on(1, "main", "merged"),
    })
    ctx.query.base = "main"
    local first = Runtime.query(ctx)
    dash.assert_eq(#first, 1, "only PRs into main are returned")
    dash.assert_eq(first[1].number, 1, "PR 1")
    local second = Runtime.query(ctx)
    dash.assert_eq(#second, 0, "PR 1 merged")
    dash.assert_eq(#ctx.emitted, 1, "the merge into main is detected")
end)

dash.test("confirmations per poll are bounded", function(_make_ctx)
    local opens, details = {}, {}
    for i = 1, 15 do
        table.insert(opens, pr(i, "T" .. i, "open"))
        details[i] = pr(i, "T" .. i, "merged")
    end
    local ctx = polling_ctx({ opens, {}, {}, {}, {} }, details)
    Runtime.query(ctx)
    Runtime.query(ctx)
    dash.assert_eq(#ctx.lookups, Runtime.MAX_CONFIRMATIONS, "bounded lookups")
    dash.assert_eq(#ctx.emitted, Runtime.MAX_CONFIRMATIONS, "merges confirmed so far")
    Runtime.query(ctx)
    Runtime.query(ctx)
    dash.assert_eq(#ctx.emitted, 15, "the rest on later polls")
    Runtime.query(ctx)
    dash.assert_eq(#ctx.lookups, 15, "nothing is looked up twice")
end)
