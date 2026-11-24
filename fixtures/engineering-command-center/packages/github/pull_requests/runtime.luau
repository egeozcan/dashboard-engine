--!strict
local Runtime = {}

function Runtime.query(ctx)
    local repo = ctx.query.repository
    if repo == nil or repo == "" then
        error("github.pull_requests requires query.repository")
    end
    local req_state = ctx.query.state or "open"
    local all_prs = ctx:await(ctx.inputs.github:call("list_prs", {
        repository = repo,
        state = "all",
    }))

    -- Track seen states to emit github.pr_merged when an external merge is detected by polling
    local prev_states = ctx:await(ctx.state:get("seen_pr_states:" .. repo)) or {}
    local next_states = {}
    local filtered = {}

    local merged = {}

    for _, pr in ipairs(all_prs) do
        local key = tostring(pr.number)
        next_states[key] = pr.state
        if prev_states[key] == "open" and pr.state == "merged" then
            table.insert(merged, pr)
        end
        if req_state == "all" or pr.state == req_state then
            table.insert(filtered, pr)
        end
    end

    -- Record the new states before emitting, so a concurrent computation that reads after this
    -- write does not report the same merge again. Any remaining duplicate is harmless: consumers
    -- key side effects on (repository, number).
    ctx:await(ctx.state:set("seen_pr_states:" .. repo, next_states))
    for _, pr in ipairs(merged) do
        local jira_key = string.match(pr.title, "([A-Z][A-Z0-9]+%-%d+)")
        ctx:await(ctx:emit("github.pr_merged", {
            repository = pr.repository,
            number = pr.number,
            title = pr.title,
            issue_key = jira_key,
            source = "polling",
        }, true))
    end
    return filtered
end

return Runtime
