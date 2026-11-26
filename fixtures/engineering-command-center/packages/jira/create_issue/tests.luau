local dash = require("dash")
local Runtime = require("runtime")

dash.test("creates the next issue of the project in the Mock", function(make_ctx)
    dash.assert(Runtime.dry_run(make_ctx({ args = { project = "DEV", summary = "" } })).feasible == false, "empty summary")
    local res = Runtime.execute(make_ctx({ args = { project = "DEV", summary = "Export dashboards as PDF", labels = { "frontend" } } }))
    dash.assert_eq(res.key, "DEV-131", "next DEV key")
    dash.assert_eq(res.url, "https://jira.example.com/browse/DEV-131", "browse URL")
end)
