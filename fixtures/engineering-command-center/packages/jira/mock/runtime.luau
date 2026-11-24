--!strict
local Runtime = {}
local issues_cache = nil

local function load_issues(ctx)
    if issues_cache == nil then
        issues_cache = ctx:await(ctx:fixture("fixtures/issues.json"))
    end
    return issues_cache
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
