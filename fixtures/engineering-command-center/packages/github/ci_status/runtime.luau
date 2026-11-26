--!strict
local model = require("github.model")

local Runtime = {}

function Runtime.query(ctx: any): any
    local query = model.clean(ctx.query)
    local q = query
    return ctx:await(ctx.inputs.github:call("get_ci_status", { repository = q.repository, ref = q.ref }))
end

return Runtime
