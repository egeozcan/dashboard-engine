--!strict
local model = require("jira.model")

local Runtime = {}

function Runtime.query(ctx)
    local query = model.clean(ctx.query)
    local keys = query.keys or {}
    if #keys == 0 then
        return {}
    end
    return ctx:await(ctx.inputs.jira:call("get_issues", { keys = keys }))
end

return Runtime
