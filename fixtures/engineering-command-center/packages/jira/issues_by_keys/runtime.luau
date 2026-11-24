--!strict
local Runtime = {}

function Runtime.query(ctx)
    local keys = ctx.query.keys or {}
    if #keys == 0 then
        return {}
    end
    return ctx:await(ctx.inputs.jira:call("get_issues", { keys = keys }))
end

return Runtime
