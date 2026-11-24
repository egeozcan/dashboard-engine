--!strict
local Runtime = {}

function Runtime.render(ctx)
    local repo = ctx:await(ctx.dashboard_state:get("selected_repository")) or "acme/platform"
    local rows = ctx:await(ctx.inputs.linked:query({
        repository = repo,
        state = "open",
    }))

    return {
        key = "root",
        widget_type = "dash.container",
        props = { direction = "vertical", gap = 16 },
        children = {
            {
                key = "pr_summary",
                widget_type = "engineering.pr_badge_widget",
                props = {
                    repository = repo,
                    open_count = #rows,
                },
            },
            {
                key = "pr_table",
                widget_type = "dash.table",
                props = {
                    title = "Open Pull Requests — " .. repo,
                    columns = {
                        { key = "number", label = "#" },
                        { key = "title", label = "Title" },
                        { key = "author", label = "Author" },
                        { key = "jira_key", label = "Jira" },
                        { key = "jira_status", label = "Issue Status" },
                        { key = "state", label = "PR State" },
                    },
                    rows = rows,
                    client_sort_state_key = "pr_table_sort",
                    client_filter_state_key = "pr_table_filter",
                    row_action = {
                        label = "Merge PR",
                        request = "merge_requested",
                        action_instance = "merge_pr",
                    },
                },
            },
        },
    }
end

function Runtime.on_request(ctx)
    if ctx.request.request == "merge_requested" then
        local p = ctx.request.payload or {}
        -- Never guess the target: a Request without repository/number is rejected.
        if p.repository == nil or p.number == nil then
            error("merge_requested requires payload.repository and payload.number")
        end
        return {
            action = "merge",
            args = {
                repository = p.repository,
                number = p.number,
                -- Table rows carry the linked issue as `jira_key`.
                issue_key = p.issue_key or p.jira_key,
            },
        }
    elseif ctx.request.request == "refresh_requested" then
        return {
            refresh = true,
            invalidate = { "linked" },
        }
    end
    error("unsupported widget request: " .. tostring(ctx.request.request))
end

return Runtime
