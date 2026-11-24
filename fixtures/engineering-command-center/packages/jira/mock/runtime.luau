--!strict
local Runtime = {}

local function load_issues(ctx)
    local cached = ctx:await(ctx:mock_get("issues"))
    if cached ~= nil then
        return cached
    end
    local init = ctx:await(ctx:fixture("fixtures/issues.json"))
    ctx:await(ctx:mock_set("issues", init))
    return init
end

function Runtime.call(ctx)
    local issues = load_issues(ctx)
    if ctx.operation == "get_issues" then
        local wanted = {}
        for _, k in ipairs(ctx.args.keys or {}) do
            wanted[k] = true
        end
        local out = {}
        for _, issue in ipairs(issues) do
            if wanted[issue.key] then
                table.insert(out, issue)
            end
        end
        return out
    elseif ctx.operation == "transition_issue" then
        local key = ctx.args.key
        local target_status = ctx.args.status or "Done"
        for _, issue in ipairs(issues) do
            if issue.key == key then
                issue.status = target_status
                ctx:await(ctx:mock_set("issues", issues))
                return {
                    key = key,
                    status = target_status,
                    transitioned = true,
                }
            end
        end
        return {
            key = key,
            status = target_status,
            transitioned = false,
            reason = "issue_not_found",
        }
    end
    error("unknown jira mock operation: " .. tostring(ctx.operation))
end

return Runtime
