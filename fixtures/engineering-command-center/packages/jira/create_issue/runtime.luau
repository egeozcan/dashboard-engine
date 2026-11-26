--!strict
local model = require("jira.model")

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
    local args = model.clean(ctx.args)
    local p = problems(args)
    return {
        feasible = #p == 0,
        warnings = p,
        effects = {
            project = args.project,
            issue_type = args.issue_type or "Task",
            summary = args.summary,
            labels = args.labels,
        },
    }
end

function Runtime.execute(ctx: any): any
    local args = model.clean(ctx.args)
    local p = problems(args)
    if #p > 0 then
        error("INVALID_ARGS: " .. table.concat(p, "; "))
    end
    local res = ctx:await(ctx.inputs.jira:call("create_issue", {
        project = args.project,
        issue_type = args.issue_type or "Task",
        summary = args.summary,
        description = args.description,
        labels = args.labels,
        priority = args.priority,
        assignee_id = args.assignee_id,
    }))
    return { key = res.key, url = res.url }
end

return Runtime
