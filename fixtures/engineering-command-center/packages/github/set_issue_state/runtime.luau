--!strict
local Runtime = {}

-- The issue with a merged pull request's state reported as `closed` (it is closed).
local function current(ctx: any): any
    local issue = ctx:await(ctx.inputs.github:call("get_issue", {
        repository = ctx.args.repository,
        number = ctx.args.number,
    }))
    if issue.state == "merged" then
        issue.state = "closed"
    end
    return issue
end

function Runtime.dry_run(ctx: any): any
    local issue = current(ctx)
    local warnings = {}
    if issue.is_pull_request then
        table.insert(warnings, "target is a pull request; closing it does not merge it")
    end
    if issue.state == ctx.args.state then
        table.insert(warnings, "already " .. ctx.args.state .. "; nothing to change")
    end
    return {
        feasible = true,
        warnings = warnings,
        preconditions = { { check = "state", expected = ctx.args.state, actual = issue.state } },
        effects = {
            repository = ctx.args.repository,
            number = ctx.args.number,
            title = issue.title,
            from = issue.state,
            to = ctx.args.state,
        },
    }
end

function Runtime.execute(ctx: any): any
    local issue = current(ctx)
    if issue.state == ctx.args.state then
        return { number = issue.number, state = issue.state, changed = false }
    end
    local reason = ctx.args.reason
    if reason == nil then
        reason = if ctx.args.state == "closed" then "completed" else "reopened"
    end
    local updated = ctx:await(ctx.inputs.github:call("update_issue", {
        repository = ctx.args.repository,
        number = ctx.args.number,
        state = ctx.args.state,
        state_reason = reason,
    }))
    return { number = updated.number, state = if updated.state == "merged" then "closed" else updated.state, changed = true }
end

function Runtime.reconcile(ctx: any): any
    local issue = current(ctx)
    if issue.state == ctx.args.state then
        return { status = "succeeded", number = issue.number, state = issue.state, changed = true }
    end
    return { status = "failed", number = issue.number, state = issue.state }
end

return Runtime
