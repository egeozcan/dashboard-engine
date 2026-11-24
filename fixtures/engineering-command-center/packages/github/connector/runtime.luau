--!strict
local Runtime = {}

function Runtime.call(ctx)
    local base = (ctx.config and ctx.config.base_url) or "https://api.github.com"
    if ctx.operation == "list_prs" then
        local repo = ctx.args.repository
        local st = ctx.args.state or "open"
        local resp = ctx:await(ctx.http:request({
            method = "GET",
            url = base .. "/repos/" .. repo .. "/pulls?state=" .. st,
            credential = "auth",
        }))
        if resp.status >= 400 then
            error("GitHub list_prs failed with HTTP " .. tostring(resp.status))
        end
        return resp.body
    elseif ctx.operation == "get_pr" then
        local repo = ctx.args.repository
        local num = tostring(ctx.args.number)
        local resp = ctx:await(ctx.http:request({
            method = "GET",
            url = base .. "/repos/" .. repo .. "/pulls/" .. num,
            credential = "auth",
        }))
        if resp.status >= 400 then
            error("GitHub get_pr failed with HTTP " .. tostring(resp.status))
        end
        return resp.body
    elseif ctx.operation == "merge_pr" then
        local repo = ctx.args.repository
        local num = tostring(ctx.args.number)
        local resp = ctx:await(ctx.http:request({
            method = "PUT",
            url = base .. "/repos/" .. repo .. "/pulls/" .. num .. "/merge",
            credential = "auth",
            body = { merge_method = "squash" },
        }))
        if resp.status >= 500 then
            -- A gateway/server error to a write does not prove the merge did not happen (§9.1):
            -- report an unknown outcome so the host reconciles instead of inviting a retry.
            error("UNKNOWN_OUTCOME: GitHub merge_pr returned HTTP " .. tostring(resp.status))
        end
        if resp.status >= 400 then
            error("GitHub merge_pr failed with HTTP " .. tostring(resp.status))
        end
        return resp.body
    end
    error("unknown GitHub connector operation: " .. tostring(ctx.operation))
end

return Runtime
