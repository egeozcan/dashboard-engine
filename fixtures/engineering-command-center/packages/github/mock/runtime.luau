--!strict
local Runtime = {}

local function load_prs(ctx)
    local cached = ctx:await(ctx:mock_get("prs"))
    if cached ~= nil then
        return cached
    end
    local init = ctx:await(ctx:fixture("fixtures/prs.json"))
    ctx:await(ctx:mock_set("prs", init))
    return init
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
    elseif ctx.operation == "merge_pr" or ctx.operation == "simulate_external_merge" then
        local repo = ctx.args.repository
        local num = ctx.args.number
        for _, pr in ipairs(prs) do
            if pr.repository == repo and pr.number == num then
                pr.state = "merged"
                pr.merged = true
                ctx:await(ctx:mock_set("prs", prs))
                return { merged = true, sha = "mocksha_" .. tostring(num), number = num, repository = repo, pr = pr }
            end
        end
        error("PR not found for merge: " .. tostring(repo) .. "#" .. tostring(num))
    end
    error("unknown mock operation: " .. tostring(ctx.operation))
end

return Runtime
