local dash = require("dash")

return dash.automation {
    id = "engineering.complete_work",
    description = "Transitions linked Jira issue to Done on github.pr_merged Events",
    runtime = "runtime.luau",
    handler = "run",
    inputs = {
        merge = dash.action_ref("github.merge_pr"),
        prs = dash.data_ref("github.pull_requests"),
        transition = dash.action_ref("jira.transition_issue"),
    },
    triggers = {
        { kind = "event", event_type = "github.pr_merged", source_input = "merge" },
        { kind = "event", event_type = "github.pr_merged", source_input = "prs" },
    },
}
