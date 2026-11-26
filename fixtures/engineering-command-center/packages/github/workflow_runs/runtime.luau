--!strict
local Runtime = {}

function Runtime.query(ctx: any): any
    local q = ctx.query or {}
    return ctx:await(ctx.inputs.github:call("list_workflow_runs", {
        repository = q.repository,
        workflow = q.workflow,
        branch = q.branch,
        status = q.status,
        event = q.event,
    }))
end

return Runtime
