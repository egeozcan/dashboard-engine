--!strict
local model = require("jira.model")

local Runtime = {}

function Runtime.dry_run(ctx: any): any
    local args = model.clean(ctx.args)
    local issue = ctx:await(ctx.inputs.jira:call("get_issue", { key = args.key }))
    local empty = type(args.body) ~= "string" or string.match(args.body, "^%s*$") ~= nil
    return {
        feasible = not empty,
        warnings = if empty then { "comment body must not be empty" } else {},
        effects = { issue_key = issue.key, summary = issue.summary, status = issue.status },
    }
end

function Runtime.execute(ctx: any): any
    local args = model.clean(ctx.args)
    local res = ctx:await(ctx.inputs.jira:call("add_comment", { key = args.key, body = args.body }))
    return { id = res.id, key = res.key }
end

return Runtime
