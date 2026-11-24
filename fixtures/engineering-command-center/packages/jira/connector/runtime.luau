--!strict
local Runtime = {}

function Runtime.call(ctx)
    local base = (ctx.config and ctx.config.base_url) or "https://jira.example.com"
    if ctx.operation == "get_issues" then
        local keys = table.concat(ctx.args.keys or {}, ",")
        local resp = ctx:await(ctx.http:request({
            method = "GET",
            url = base .. "/rest/api/3/search?jql=key+in+(" .. keys .. ")",
            credential = "auth",
        }))
        if resp.status >= 400 then
            error("Jira get_issues failed with HTTP " .. tostring(resp.status))
        end
        return resp.body
    elseif ctx.operation == "transition_issue" then
        local resp = ctx:await(ctx.http:request({
            method = "POST",
            url = base .. "/rest/api/3/issue/" .. ctx.args.key .. "/transitions",
            credential = "auth",
            body = { status = ctx.args.status or "Done" },
        }))
        if resp.status >= 400 then
            error("Jira transition_issue failed with HTTP " .. tostring(resp.status))
        end
        return resp.body
    end
    error("unknown Jira connector operation: " .. tostring(ctx.operation))
end

return Runtime
