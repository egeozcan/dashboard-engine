--!strict
local Runtime = {}

function Runtime.query(ctx: any): any
    local q = ctx.query or {}
    return ctx:await(ctx.inputs.jira:call("search", {
        jql = q.jql,
        fields = q.fields,
        max_results = q.max_results,
    }))
end

return Runtime
