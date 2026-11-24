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

-- Stand-in ctx with a scripted list_prs sequence, in-memory Instance State and recorded Events,
-- to check the polling merge detection (§16 behaviour 7) deterministically.
local function polling_ctx(snapshots)
    local state = {}
    local ctx = {
        query = { repository = "acme/platform", state = "open" },
        emitted = {},
        calls = 0,
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
        dash.assert_eq(op, "list_prs", "polling only reads")
        dash.assert_eq(args, { repository = "acme/platform", state = "all" }, "list args")
        ctx.calls += 1
        return snapshots[ctx.calls]
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
    local merged42 = pr(42, "DEV-123 Improve caching", "merged")
    local open44 = pr(44, "Refactor README", "open")
    local ctx = polling_ctx({ { open42, open44 }, { merged42, open44 }, { merged42, open44 } })

    local first = Runtime.query(ctx)
    dash.assert_eq(#first, 2, "both PRs open")
    dash.assert_eq(#ctx.emitted, 0, "nothing merged yet")

    local second = Runtime.query(ctx)
    dash.assert_eq(#second, 1, "merged PR leaves the open list")
    dash.assert_eq(#ctx.emitted, 1, "one merge detected")
    local ev = ctx.emitted[1]
    dash.assert_eq(ev.event_type, "github.pr_merged", "event type matches the merge action's")
    dash.assert(ev.durable == true, "durable Event")
    dash.assert_eq(ev.payload.number, 42, "merged PR number")
    dash.assert_eq(ev.payload.issue_key, "DEV-123", "Jira key from the title")
    dash.assert_eq(ev.payload.source, "polling", "source")

    Runtime.query(ctx)
    dash.assert_eq(#ctx.emitted, 1, "an already-merged PR is not reported again")
end)
