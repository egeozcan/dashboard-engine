--!strict
local model = require("github.model")

local Runtime = {}

-- The issue with a merged pull request's state reported as `closed` (it is closed).
local function current(ctx: any): any
    local args = model.clean(ctx.args)
    local issue = ctx:await(ctx.inputs.github:call("get_issue", {
        repository = args.repository,
        number = args.number,
    }))
    if issue.state == "merged" then
        issue.state = "closed"
    end
    return issue
end

function Runtime.dry_run(ctx: any): any
    local args = model.clean(ctx.args)
    local issue = current(ctx)
    local warnings = {}
    if issue.is_pull_request then
        table.insert(warnings, "target is a pull request; closing it does not merge it")
    end
    if issue.state == args.state then
        table.insert(warnings, "already " .. args.state .. "; nothing to change")
    end
    return {
        feasible = true,
        warnings = warnings,
        preconditions = { { check = "state", expected = args.state, actual = issue.state } },
        effects = {
            repository = args.repository,
            number = args.number,
            title = issue.title,
            from = issue.state,
            to = args.state,
        },
    }
end

function Runtime.execute(ctx: any): any
    local args = model.clean(ctx.args)
    local issue = current(ctx)
    if issue.state == args.state then
        return { number = issue.number, state = issue.state, changed = false }
    end
    local reason = args.reason
    if reason == nil then
        reason = if args.state == "closed" then "completed" else "reopened"
    end
    local updated = ctx:await(ctx.inputs.github:call("update_issue", {
        repository = args.repository,
        number = args.number,
        state = args.state,
        state_reason = reason,
    }))
    return { number = updated.number, state = if updated.state == "merged" then "closed" else updated.state, changed = true }
end

function Runtime.reconcile(ctx: any): any
    local args = model.clean(ctx.args)
    local issue = current(ctx)
    if issue.state == args.state then
        return { status = "succeeded", number = issue.number, state = issue.state, changed = true }
    end
    return { status = "failed", number = issue.number, state = issue.state }
end

return Runtime
