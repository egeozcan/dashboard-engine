--!strict
local Runtime = {}

function Runtime.render(ctx)
    local repo = ctx:await(ctx.dashboard_state:get("selected_repository")) or "acme/platform"
    local selection = { repository = repo, state = "open" }
    local prs = ctx:await(ctx.inputs.prs:query(selection))
    -- The history Instance serves one sampled series per repository; select the one sampled
    -- with the same query parameters as the counter, so both follow the selected repository.
    -- An unsampled repository has no history (an empty series), never another repository's.
    local samples = ctx:await(ctx.inputs.history:query({ params = selection }))

    return {
        key = "trend_root",
        widget_type = "dash.grid",
        props = { columns = 2, gap = 16 },
        children = {
            {
                key = "open_count",
                widget_type = "dash.counter",
                props = {
                    label = "Open PRs (" .. repo .. ")",
                    value = #prs,
                    status = (#prs > 5) and "warning" or "healthy",
                },
            },
            {
                key = "trend_chart",
                widget_type = "dash.chart",
                props = {
                    title = "30-Day Open PR Count (" .. repo .. ")",
                    series = samples,
                    x_key = "sampled_at",
                    y_key = "value_num",
                },
            },
        },
    }
end

return Runtime
