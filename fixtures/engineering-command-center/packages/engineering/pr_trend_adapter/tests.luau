local dash = require("dash")
local Runtime = require("runtime")

dash.test("render counts Mock-backed open PRs and labels both widgets with the repository", function(make_ctx)
    -- No Dashboard State in a Definition test: the Adapter falls back to acme/platform. The
    -- built-in history provider has recorded no samples here, so the chart series is empty.
    local tree = Runtime.render(make_ctx({}))
    dash.assert_eq(tree.widget_type, "dash.grid", "root grid")
    local counter = tree.children[1]
    local chart = tree.children[2]
    dash.assert_eq(counter.widget_type, "dash.counter", "counter widget")
    dash.assert_eq(counter.props.value, 3, "three open acme/platform PRs in the github Mock")
    dash.assert_eq(counter.props.label, "Open PRs (acme/platform)", "counter label")
    dash.assert_eq(counter.props.status, "healthy", "3 open PRs is healthy")
    dash.assert_eq(chart.widget_type, "dash.chart", "chart widget")
    dash.assert_eq(chart.props.title, "30-Day Open PR Count (acme/platform)", "chart title names the repository")
    dash.assert_eq(#chart.props.series, 0, "no samples recorded in a Definition test")
end)

-- Stand-in ctx recording what the Adapter asks its inputs for (the Definition-test harness has
-- no way to seed Dashboard State or history samples).
local function fake_ctx(selected, prs_by_repo, samples_by_repo)
    local calls = { prs = {}, history = {} }
    local ctx = {
        dashboard_state = {
            get = function(_self, key)
                dash.assert_eq(key, "selected_repository", "only the declared state key is read")
                return selected
            end,
        },
        inputs = {
            prs = {
                query = function(_self, params)
                    table.insert(calls.prs, params)
                    return prs_by_repo[params.repository] or {}
                end,
            },
            history = {
                query = function(_self, params)
                    table.insert(calls.history, params)
                    local sel = params.params or {}
                    return samples_by_repo[sel.repository] or {}
                end,
            },
        },
    }
    function ctx:await(v)
        return v
    end
    return ctx, calls
end

local SAMPLES = {
    ["acme/platform"] = {
        { sampled_at = "2026-11-22T00:00:00Z", value_num = 3 },
        { sampled_at = "2026-11-23T00:00:00Z", value_num = 4 },
    },
    ["acme/payments"] = {
        { sampled_at = "2026-11-23T00:00:00Z", value_num = 1 },
    },
}
local PRS = {
    ["acme/platform"] = { { number = 42 }, { number = 43 }, { number = 44 } },
    ["acme/payments"] = { { number = 7 } },
}

dash.test("counter and trend chart both follow the selected repository", function(_make_ctx)
    local ctx, calls = fake_ctx("acme/payments", PRS, SAMPLES)
    local tree = Runtime.render(ctx)
    local counter = tree.children[1]
    local chart = tree.children[2]
    dash.assert_eq(calls.prs, { { repository = "acme/payments", state = "open" } }, "PR query params")
    dash.assert_eq(
        calls.history,
        { { params = { repository = "acme/payments", state = "open" } } },
        "history selected by the same parameters, no hardcoded series id"
    )
    dash.assert_eq(counter.props.value, 1, "payments open PR count")
    dash.assert_eq(counter.props.label, "Open PRs (acme/payments)", "counter label")
    dash.assert_eq(chart.props.series, SAMPLES["acme/payments"], "payments history, not platform's")
    dash.assert_eq(chart.props.title, "30-Day Open PR Count (acme/payments)", "chart title")
end)

dash.test("a repository without sampled history renders an empty trend, not another repository's", function(_make_ctx)
    local ctx = fake_ctx("acme/unsampled", PRS, SAMPLES)
    local tree = Runtime.render(ctx)
    dash.assert_eq(tree.children[1].props.value, 0, "no open PRs")
    dash.assert_eq(#tree.children[2].props.series, 0, "empty series")
end)

dash.test("more than five open PRs marks the counter as warning", function(_make_ctx)
    local many = {}
    for i = 1, 6 do
        table.insert(many, { number = i })
    end
    local ctx = fake_ctx("acme/platform", { ["acme/platform"] = many }, SAMPLES)
    local tree = Runtime.render(ctx)
    dash.assert_eq(tree.children[1].props.status, "warning", "warning above five open PRs")
    dash.assert_eq(#tree.children[2].props.series, 2, "platform samples")
end)
