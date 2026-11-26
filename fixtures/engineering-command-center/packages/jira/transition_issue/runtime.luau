--!strict
local model = require("jira.model")

local Runtime = {}

local function describe(target: string?, category: string?): string
    if target ~= nil then
        return target
    end
    return "category " .. tostring(category)
end

function Runtime.dry_run(ctx: any): any
    local target, category = model.target_of(ctx.args)
    local issue = ctx:await(ctx.inputs.jira:call("get_issue", { key = ctx.args.key }))
    local warnings = {}
    local feasible = true
    local via, to_status = nil, nil
    if model.at_target(issue, target, category) then
        table.insert(warnings, "already in '" .. issue.status .. "'; nothing to change")
        to_status = issue.status
    else
        local transitions = ctx:await(ctx.inputs.jira:call("get_transitions", { key = ctx.args.key }))
        local chosen = model.pick_transition(transitions, target, nil, category)
        if chosen == nil then
            feasible = false
            table.insert(warnings, "no transition from '" .. issue.status .. "' to " .. describe(target, category)
                .. " (available: " .. model.transition_names(transitions) .. ")")
        else
            via, to_status = chosen.name, chosen.to_status
        end
    end
    return {
        feasible = feasible,
        warnings = warnings,
        preconditions = {
            { check = "status", expected = "not " .. describe(target, category), actual = issue.status },
        },
        effects = {
            issue_key = issue.key,
            summary = issue.summary,
            from_status = issue.status,
            target_status = to_status or target,
            target_category = category,
            transition = via,
        },
    }
end

function Runtime.execute(ctx: any): any
    local target, category = model.target_of(ctx.args)
    local res = ctx:await(ctx.inputs.jira:call("transition_issue", {
        key = ctx.args.key,
        status = target,
        status_category = category,
        comment = ctx.args.comment,
        resolution = ctx.args.resolution,
    }))
    return { key = res.key, status = res.status, transitioned = res.transitioned }
end

function Runtime.reconcile(ctx: any): any
    local target, category = model.target_of(ctx.args)
    local issue = ctx:await(ctx.inputs.jira:call("get_issue", { key = ctx.args.key }))
    if model.at_target(issue, target, category) then
        return { status = "succeeded", key = issue.key, transitioned = true }
    end
    -- Jira applies transitions synchronously: not at the target means it did not happen.
    return { status = "failed", key = issue.key, current_status = issue.status }
end

return Runtime
