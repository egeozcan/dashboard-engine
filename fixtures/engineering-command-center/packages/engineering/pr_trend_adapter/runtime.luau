--!strict
local Runtime = {}

function Runtime.render(ctx)
    local repo = ctx:await(ctx.dashboard_state:get("selected_repository")) or "acme/platform"
    local prs = ctx:await(ctx.inputs.prs:query({
        repository = repo,
        state = "open",
    }))
    local samples = ctx:await(ctx.inputs.history:query({
        series_id = "open_prs_30d",
    }))

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
                    title = "30-Day Open PR Count",
                    series = samples,
                    x_key = "sampled_at",
                    y_key = "value_num",
                },
            },
        },
    }
end

return Runtime
