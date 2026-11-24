local dash = require("dash")

return dash.action_provider {
    id = "jira.transition_issue",
    description = "Transitions a Jira issue status",
    runtime = "runtime.luau",
    handler = "execute",
    dry_run = "dry_run",
    inputs = {
        jira = dash.connector_ref("jira.connector", {
            required_capabilities = { "issues.transition" },
        }),
    },
    args = dash.object {
        key = dash.string(),
        status = dash.optional(dash.string()),
    },
    result = dash.object {
        key = dash.string(),
        status = dash.string(),
        transitioned = dash.boolean(),
    },
    invalidates = { "jira_issues_by_keys", "linked_prs" },
}
