--!strict
local Runtime = {}

function Runtime.run(ctx)
    local payload = ctx.event.payload or {}
    local issue_key = payload.issue_key
    if issue_key == nil or issue_key == "" then
        return { skipped = true, reason = "no_linked_jira_issue" }
    end
    -- Key the side effect on the PR identity, not the Event: a dashboard merge (source = action)
    -- and the polling detection of the same merge (source = polling) are two Events for one
    -- logical merge and must transition the Jira issue only once.
    local ikey
    if payload.repository ~= nil and payload.number ~= nil then
        ikey = "complete-" .. tostring(payload.repository) .. "#" .. tostring(payload.number)
    else
        ikey = "complete-" .. tostring(ctx.event.event_id)
    end
    return ctx:await(ctx.inputs.transition:invoke({
        key = issue_key,
        status = "Done",
    }, ikey))
end

return Runtime
