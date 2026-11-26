--!strict
local model = require("github.model")

local Runtime = {}

function Runtime.query(ctx: any): any
    local query = model.clean(ctx.query)
    local q = query
    return ctx:await(ctx.inputs.github:call("list_workflow_runs", {
        repository = q.repository,
        workflow = q.workflow,
        branch = q.branch,
        status = q.status,
        event = q.event,
    }))
end

return Runtime
