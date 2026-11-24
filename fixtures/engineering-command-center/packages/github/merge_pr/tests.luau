local dash = require("dash")
local Runtime = require("runtime")

dash.test("merge_pr exposes execute, dry_run, and reconcile handlers", function(make_ctx)
    local _ = make_ctx({ args = { repository = "acme/platform", number = 42 } })
    dash.assert(type(Runtime.execute) == "function", "execute missing")
    dash.assert(type(Runtime.dry_run) == "function", "dry_run missing")
    dash.assert(type(Runtime.reconcile) == "function", "reconcile missing")
end)
