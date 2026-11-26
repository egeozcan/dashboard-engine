--!strict
local model = require("jira.model")

local Runtime = {}

function Runtime.query(ctx: any): any
    local query = model.clean(ctx.query)
    local q = query
    return ctx:await(ctx.inputs.jira:call("search", {
        jql = q.jql,
        fields = q.fields,
        max_results = q.max_results,
    }))
end

return Runtime
