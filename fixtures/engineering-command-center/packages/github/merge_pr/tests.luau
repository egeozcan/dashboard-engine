local dash = require("dash")
local Runtime = require("runtime")

-- Minimal stand-in for the host ctx: records connector operations so tests can prove that
-- dry_run and reconcile only issue read operations.
local function fake_ctx(pr, args)
    local calls = {}
    local ctx = {
        args = args or { repository = "acme/platform", number = 42 },
        emitted = {},
        inputs = {
            github = {
                call = function(_self, op, _a)
                    table.insert(calls, op)
                    if op == "get_pr" then
                        return pr
                    elseif op == "merge_pr" then
                        return { merged = true, sha = "abc123" }
                    end
                    error("unexpected connector operation " .. tostring(op))
                end,
            },
        },
    }
    function ctx:await(v)
        return v
    end
    function ctx:emit(event_type, payload, _durable)
        table.insert(self.emitted, { event_type = event_type, payload = payload })
        return nil
    end
    return ctx, calls
end

local function only_reads(calls)
    for _, op in ipairs(calls) do
        if op ~= "get_pr" then
            return false
        end
    end
    return true
end

dash.test("merge_pr exposes execute, dry_run, and reconcile handlers", function(make_ctx)
    local _ = make_ctx({ args = { repository = "acme/platform", number = 42 } })
    dash.assert(type(Runtime.execute) == "function", "execute missing")
    dash.assert(type(Runtime.dry_run) == "function", "dry_run missing")
    dash.assert(type(Runtime.reconcile) == "function", "reconcile missing")
end)

dash.test("reconcile reports succeeded once the PR is merged", function(_make_ctx)
    local ctx, calls = fake_ctx({ number = 42, state = "merged", merged = true })
    local res = Runtime.reconcile(ctx)
    dash.assert_eq(res.status, "succeeded", "merged PR reconciles to succeeded")
    dash.assert(only_reads(calls), "reconcile must only read")
end)

dash.test("reconcile reports failed when the PR was closed without merging", function(_make_ctx)
    local ctx, calls = fake_ctx({ number = 42, state = "closed", merged = false })
    local res = Runtime.reconcile(ctx)
    dash.assert_eq(res.status, "failed", "closed unmerged PR reconciles to failed")
    dash.assert(only_reads(calls), "reconcile must only read")
end)

dash.test("reconcile stays unknown while the PR is still open", function(_make_ctx)
    local ctx, calls = fake_ctx({ number = 42, state = "open", merged = false })
    local res = Runtime.reconcile(ctx)
    dash.assert_eq(res.status, "unknown", "open PR stays unknown")
    dash.assert(only_reads(calls), "reconcile must only read")
end)

dash.test("dry_run reports feasibility and preconditions without merging", function(_make_ctx)
    local ctx, calls = fake_ctx({ number = 42, state = "open", title = "DEV-123 Improve caching" })
    local res = Runtime.dry_run(ctx)
    dash.assert(res.feasible == true, "open PR is feasible")
    dash.assert(#res.preconditions >= 1, "dry_run reports preconditions")
    dash.assert(only_reads(calls), "dry_run must only read")

    local ctx2 = fake_ctx({ number = 42, state = "merged", title = "DEV-123 Improve caching" })
    local res2 = Runtime.dry_run(ctx2)
    dash.assert(res2.feasible == false, "already merged PR is not feasible")
    dash.assert(#res2.warnings == 1, "warning explains infeasibility")
end)

dash.test("execute merges, emits github.pr_merged and returns the typed result", function(_make_ctx)
    local ctx, calls = fake_ctx({ number = 42, state = "open", title = "DEV-123 Improve caching" })
    local res = Runtime.execute(ctx)
    dash.assert_eq(res, { merged = true, sha = "abc123" }, "typed result")
    dash.assert_eq(calls, { "get_pr", "merge_pr" }, "reads then merges once")
    dash.assert_eq(ctx.emitted[1].event_type, "github.pr_merged", "emits merge event")
    dash.assert_eq(ctx.emitted[1].payload.issue_key, "DEV-123", "derives Jira key from title")
end)
