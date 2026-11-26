--!strict
local Runtime = {}

function Runtime.dry_run(ctx: any): any
    local target = ctx:await(ctx.inputs.github:call("get_issue", {
        repository = ctx.args.repository,
        number = ctx.args.number,
    }))
    local warnings = {}
    local feasible = true
    if type(ctx.args.body) ~= "string" or string.match(ctx.args.body, "^%s*$") then
        feasible = false
        table.insert(warnings, "comment body must not be empty")
    end
    return {
        feasible = feasible,
        warnings = warnings,
        preconditions = { { check = "target_exists", expected = true, actual = true } },
        effects = {
            repository = ctx.args.repository,
            number = ctx.args.number,
            target_title = target.title,
            target_kind = if target.is_pull_request then "pull_request" else "issue",
        },
    }
end

function Runtime.execute(ctx: any): any
    local res = ctx:await(ctx.inputs.github:call("add_comment", {
        repository = ctx.args.repository,
        number = ctx.args.number,
        body = ctx.args.body,
    }))
    return { id = res.id, url = res.url }
end

return Runtime
