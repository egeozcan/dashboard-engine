--!strict
local model = require("github.model")

local Runtime = {}

-- Bound on PRs that left the open list and are confirmed with get_pr per poll; the rest are
-- confirmed on later polls.
Runtime.MAX_CONFIRMATIONS = 5

local function is_not_found(err: any): boolean
    return string.find(tostring(err), "NOT_FOUND", 1, true) ~= nil
end

function Runtime.query(ctx: any): any
    local repo = ctx.query.repository
    if repo == nil or repo == "" then
        error("github.pull_requests requires query.repository")
    end
    local req_state = ctx.query.state or "open"
    -- Always list without the `base` filter (applied to the returned rows below): merge
    -- detection then sees every open PR of the repository, whatever the query's base.
    local prs = ctx:await(ctx.inputs.github:call("list_prs", {
        repository = repo,
        state = req_state,
    }))

    -- Merge detection (§16 behaviour 7): remember which PRs were open; a remembered open PR that
    -- is now merged emits one durable github.pr_merged. Listing only open PRs keeps polling cheap;
    -- a PR that disappeared from the open list is confirmed with get_pr.
    local state_key = "seen_pr_states:" .. repo
    local prev = ctx:await(ctx.state:get(state_key)) or {}
    local next_states = {}
    local listed = {}
    local merged = {}
    for _, pr in ipairs(prs) do
        local k = tostring(pr.number)
        listed[k] = true
        if pr.state == "open" then
            next_states[k] = "open"
        elseif pr.state == "merged" and prev[k] == "open" then
            table.insert(merged, pr)
        end
    end
    local open_list_complete = req_state == "open" or req_state == "all"
    local confirmations = 0
    for k, st in pairs(prev) do
        if st == "open" and not listed[k] then
            if open_list_complete and confirmations < Runtime.MAX_CONFIRMATIONS then
                confirmations += 1
                local ok, pr = pcall(function()
                    return ctx:await(ctx.inputs.github:call("get_pr", { repository = repo, number = tonumber(k) }))
                end)
                if ok and pr ~= nil then
                    if pr.state == "merged" then
                        table.insert(merged, pr)
                    elseif pr.state == "open" then
                        next_states[k] = "open"
                    end
                elseif not is_not_found(pr) then
                    -- Could not confirm (rate limit, outage): try again on the next poll.
                    next_states[k] = "open"
                end
            else
                next_states[k] = "open"
            end
        end
    end

    -- Record the new states before emitting, so a concurrent computation that reads after this
    -- write does not report the same merge again. Any remaining duplicate is harmless: consumers
    -- key side effects on (repository, number).
    ctx:await(ctx.state:set(state_key, next_states))
    for _, pr in ipairs(merged) do
        ctx:await(ctx:emit("github.pr_merged", {
            repository = pr.repository,
            number = pr.number,
            title = pr.title,
            issue_key = model.issue_key(pr.title),
            source = "polling",
        }, true))
    end

    local base = ctx.query.base
    local filtered = {}
    for _, pr in ipairs(prs) do
        local state_ok = req_state == "all" or pr.state == req_state or (req_state == "closed" and pr.state == "merged")
        if state_ok and (base == nil or pr.base == base) then
            table.insert(filtered, pr)
        end
    end
    return filtered
end

return Runtime
