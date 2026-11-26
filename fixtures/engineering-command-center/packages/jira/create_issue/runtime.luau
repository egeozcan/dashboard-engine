--!strict
local Runtime = {}

local function problems(args: any): { string }
    local out = {}
    if type(args.project) ~= "string" or not string.match(args.project, "^[%w_]+$") then
        table.insert(out, "project must be a project key")
    end
    if type(args.summary) ~= "string" or string.match(args.summary, "^%s*$") then
        table.insert(out, "summary must not be empty")
    end
    return out
end

function Runtime.dry_run(ctx: any): any
    local p = problems(ctx.args)
    return {
        feasible = #p == 0,
        warnings = p,
        effects = {
            project = ctx.args.project,
            issue_type = ctx.args.issue_type or "Task",
            summary = ctx.args.summary,
            labels = ctx.args.labels,
        },
    }
end

function Runtime.execute(ctx: any): any
    local p = problems(ctx.args)
    if #p > 0 then
        error("INVALID_ARGS: " .. table.concat(p, "; "))
    end
    local res = ctx:await(ctx.inputs.jira:call("create_issue", {
        project = ctx.args.project,
        issue_type = ctx.args.issue_type or "Task",
        summary = ctx.args.summary,
        description = ctx.args.description,
        labels = ctx.args.labels,
        priority = ctx.args.priority,
        assignee_id = ctx.args.assignee_id,
    }))
    return { key = res.key, url = res.url }
end

return Runtime
