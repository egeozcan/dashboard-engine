--!strict
local Runtime = {}

function Runtime.query(ctx: any): any
    local q = ctx.query or {}
    return ctx:await(ctx.inputs.github:call("get_ci_status", { repository = q.repository, ref = q.ref }))
end

return Runtime
