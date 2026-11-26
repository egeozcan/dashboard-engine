--!strict
local model = require("github.model")

local Runtime = {}

local function validate(args: any): { string }
    local problems = {}
    local ok, err = pcall(model.check_repository, args.repository)
    if not ok then
        table.insert(problems, tostring(err))
    end
    if type(args.title) ~= "string" or string.match(args.title, "^%s*$") then
        table.insert(problems, "title must not be empty")
    end
    return problems
end

function Runtime.dry_run(ctx: any): any
    local args = model.clean(ctx.args)
    local problems = validate(args)
    return {
        feasible = #problems == 0,
        warnings = problems,
        effects = {
            repository = args.repository,
            title = args.title,
            labels = args.labels,
            assignees = args.assignees,
        },
    }
end

function Runtime.execute(ctx: any): any
    local args = model.clean(ctx.args)
    local problems = validate(args)
    if #problems > 0 then
        error("INVALID_ARGS: " .. table.concat(problems, "; "))
    end
    local issue = ctx:await(ctx.inputs.github:call("create_issue", {
        repository = args.repository,
        title = args.title,
        body = args.body,
        labels = args.labels,
        assignees = args.assignees,
    }))
    return { number = issue.number, url = issue.url, id = issue.id }
end

return Runtime
