--!strict
local model = require("github.model")

local Runtime = {}

function Runtime.query(ctx: any): any
    local query = model.clean(ctx.query)
    local q = query
    return ctx:await(ctx.inputs.github:call("list_issues", {
        repository = q.repository,
        state = q.state or "open",
        labels = q.labels,
        assignee = q.assignee,
        creator = q.creator,
    }))
end

return Runtime
