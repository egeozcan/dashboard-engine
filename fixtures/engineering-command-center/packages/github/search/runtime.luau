--!strict
local model = require("github.model")

local Runtime = {}

function Runtime.query(ctx: any): any
    local query = model.clean(ctx.query)
    local q = query
    return ctx:await(ctx.inputs.github:call("search_issues", {
        query = q.query,
        sort = q.sort,
        order = q.order,
        max_pages = q.max_pages,
    }))
end

return Runtime
