--!strict
local pr_utils = require("engineering.pr_utils")
local Runtime = {}

function Runtime.query(ctx)
    local prs = ctx:await(ctx.inputs.prs:query({
        repository = ctx.query.repository,
        state = ctx.query.state or "open",
    }))

    local keys = {}
    local seen = {}
    for _, pr in ipairs(prs) do
        local key = pr_utils.extract_jira_key(pr.title)
        if key ~= nil and not seen[key] then
            seen[key] = true
            table.insert(keys, key)
        end
    end

    local issues = {}
    if #keys > 0 then
        issues = ctx:await(ctx.inputs.issues:query({ keys = keys }))
    end

    local by_key = {}
    for _, issue in ipairs(issues) do
        by_key[issue.key] = issue
    end

    local result = {}
    for _, pr in ipairs(prs) do
        local key = pr_utils.extract_jira_key(pr.title)
        local issue = key and by_key[key] or nil
        table.insert(result, {
            id = pr.id or (pr.repository .. "#" .. tostring(pr.number)),
            number = pr.number,
            title = pr.title,
            repository = pr.repository,
            author = pr.author,
            state = pr.state,
            jira_key = key,
            jira_status = issue and issue.status or "Unlinked",
            pr = pr,
            issue = issue,
        })
    end
    return result
end

return Runtime
