--!strict
local Runtime = {}

function Runtime.query(ctx: any): any
    local q = ctx.query or {}
    return ctx:await(ctx.inputs.github:call("list_issues", {
        repository = q.repository,
        state = q.state or "open",
        labels = q.labels,
        assignee = q.assignee,
        creator = q.creator,
    }))
end

return Runtime
