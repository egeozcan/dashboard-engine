local dash = require("dash")
local Runtime = require("runtime")

-- Handlers exercised against the github Mock (§4.5).
dash.test("dry_run reports an open PR as feasible", function(make_ctx)
    local ctx = make_ctx({ args = { repository = "acme/platform", number = 42 } })
    local report = Runtime.dry_run(ctx)
    dash.assert(report.feasible == true, "open PR should be feasible")
    dash.assert_eq(report.effects.title, "DEV-123 Improve query cache deduplication", "Dry run effect title")
end)

dash.test("execute merges through the github Mock and reconcile observes the merge", function(make_ctx)
    local ctx = make_ctx({ args = { repository = "acme/platform", number = 42 } })
    local res = Runtime.execute(ctx)
    dash.assert(res.merged == true, "Mock merge result")
    dash.assert_eq(res.sha, "mocksha_42", "Mock merge sha")
    local rec = Runtime.reconcile(ctx)
    dash.assert_eq(rec.status, "succeeded", "reconcile sees merged PR")
end)

dash.test("Mock state is reset between tests", function(make_ctx)
    local ctx = make_ctx({ args = { repository = "acme/platform", number = 42 } })
    local rec = Runtime.reconcile(ctx)
    dash.assert_eq(rec.status, "unknown", "PR 42 is open again in a fresh test")
end)

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

dash.test("after a Mock merge, dry_run reports the PR infeasible with an explanation", function(make_ctx)
    local ctx = make_ctx({ args = { repository = "acme/platform", number = 43 } })
    dash.assert(Runtime.dry_run(ctx).feasible == true, "PR 43 starts open")
    local res = Runtime.execute(ctx)
    dash.assert_eq(res, { merged = true, sha = "mocksha_43" }, "typed merge result from the Mock")
    local report = Runtime.dry_run(ctx)
    dash.assert(report.feasible == false, "a merged PR cannot be merged again")
    dash.assert_eq(report.warnings, { "PR is currently in state 'merged'" }, "infeasibility warning")
    dash.assert_eq(report.preconditions[1].actual, "merged", "observed precondition")
    -- Other PRs are untouched by the merge.
    local other = make_ctx({ args = { repository = "acme/platform", number = 42 } })
    dash.assert_eq(Runtime.reconcile(other).status, "unknown", "PR 42 still open")
end)

dash.test("dry_run of a PR the Mock does not know fails instead of reporting feasible", function(make_ctx)
    local ctx = make_ctx({ args = { repository = "acme/platform", number = 999 } })
    local ok, err = pcall(Runtime.dry_run, ctx)
    dash.assert(not ok, "unknown PR must not be feasible")
    dash.assert(string.find(tostring(err), "not found", 1, true) ~= nil, tostring(err))
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
