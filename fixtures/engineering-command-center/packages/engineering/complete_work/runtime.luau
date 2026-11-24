--!strict
local Runtime = {}

function Runtime.run(ctx)
    local payload = ctx.event.payload or {}
    local issue_key = payload.issue_key
    if issue_key == nil or issue_key == "" then
        return { skipped = true, reason = "no_linked_jira_issue" }
    end
    local ikey = "complete-" .. tostring(ctx.event.event_id)
    return ctx:await(ctx.inputs.transition:invoke({
        key = issue_key,
        status = "Done",
    }, ikey))
end

return Runtime
