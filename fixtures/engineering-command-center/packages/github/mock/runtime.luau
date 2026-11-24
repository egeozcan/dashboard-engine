--!strict
local Runtime = {}
local prs_cache = nil

local function load_prs(ctx)
    if prs_cache == nil then
        prs_cache = ctx:await(ctx:fixture("fixtures/prs.json"))
    end
    return prs_cache
end

function Runtime.call(ctx)
    local prs = load_prs(ctx)
    if ctx.operation == "list_prs" then
        local repo = ctx.args.repository
        local state = ctx.args.state or "open"
        local out = {}
        for _, pr in ipairs(prs) do
            if (repo == nil or repo == "" or pr.repository == repo)
                and (state == "all" or pr.state == state) then
                table.insert(out, pr)
            end
        end
        return out
    elseif ctx.operation == "get_pr" then
        local repo = ctx.args.repository
        local num = ctx.args.number
        for _, pr in ipairs(prs) do
            if pr.repository == repo and pr.number == num then
                return pr
            end
        end
        error("PR not found in mock fixture: " .. tostring(repo) .. "#" .. tostring(num))
    elseif ctx.operation == "merge_pr" then
        local repo = ctx.args.repository
        local num = ctx.args.number
        for _, pr in ipairs(prs) do
            if pr.repository == repo and pr.number == num then
                pr.state = "merged"
                pr.merged = true
                return { merged = true, sha = "mocksha_" .. tostring(num), number = num, repository = repo }
            end
        end
        error("PR not found for merge: " .. tostring(repo) .. "#" .. tostring(num))
    elseif ctx.operation == "simulate_external_merge" then
        local repo = ctx.args.repository
        local num = ctx.args.number
        for _, pr in ipairs(prs) do
            if pr.repository == repo and pr.number == num then
                pr.state = "merged"
                pr.merged = true
                return pr
            end
        end
    end
    error("unknown mock operation: " .. tostring(ctx.operation))
end

return Runtime
