--!strict
local Runtime = {}

function Runtime.dry_run(ctx: any): any
    local issue = ctx:await(ctx.inputs.jira:call("get_issue", { key = ctx.args.key }))
    local empty = type(ctx.args.body) ~= "string" or string.match(ctx.args.body, "^%s*$") ~= nil
    return {
        feasible = not empty,
        warnings = if empty then { "comment body must not be empty" } else {},
        effects = { issue_key = issue.key, summary = issue.summary, status = issue.status },
    }
end

function Runtime.execute(ctx: any): any
    local res = ctx:await(ctx.inputs.jira:call("add_comment", { key = ctx.args.key, body = ctx.args.body }))
    return { id = res.id, key = res.key }
end

return Runtime
