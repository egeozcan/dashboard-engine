local dash = require("dash")

return dash.widget_provider {
    id = "engineering.pr_badge_widget",
    description = "Sandboxed custom iframe widget showing live repository PR summary",
    asset = "assets/index.html",
    props = dash.object {
        repository = dash.string(),
        open_count = dash.integer(),
    },
}
