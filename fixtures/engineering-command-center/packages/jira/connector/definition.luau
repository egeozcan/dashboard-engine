local dash = require("dash")

local issue = dash.semantic("pm.issue@1", dash.object {
    key = dash.string(),
    summary = dash.string(),
    status = dash.string(),
    labels = dash.optional(dash.list(dash.string())),
})

local transition = dash.semantic("pm.transition@1", dash.object {
    id = dash.string(),
    name = dash.string(),
    to_status = dash.string(),
})

return dash.connector {
    id = "jira.connector",
    description = "Jira REST API connector for Jira Cloud (v3) and Server/Data Center (v2): issues by key, JQL search, transitions, comments and issue creation",
    runtime = "runtime.luau",
    handler = "call",
    libraries = { "jira.model", "dash.http_util" },
    credentials = { "auth" },
    capabilities = { "issues.read", "issues.transition", "issues.write" },
    config = dash.object {
        -- https://your-site.atlassian.net (Cloud) or https://jira.example.com (Server/DC)
        base_url = dash.string(),
        -- "3" (default, Jira Cloud) or "2" (Jira Server / Data Center)
        api_version = dash.optional(dash.enum { "2", "3" }),
        -- "secret" (default) attaches the `auth` secret (Cloud: scheme basic with
        -- "email:api_token"; Server/DC: bearer personal access token); "none" is anonymous.
        auth = dash.optional(dash.enum { "secret", "none" }),
        page_size = dash.optional(dash.integer()),
        max_pages = dash.optional(dash.integer()),
    },
    operations = {
        get_issues = {
            description = "Fetch issues by keys; malformed, nonexistent or invisible keys are skipped, moved issues come back under their new key",
            required_capabilities = { "issues.read" },
            mode = "read",
            args = dash.object {
                keys = dash.list(dash.string()),
                fields = dash.optional(dash.list(dash.string())),
            },
            result = dash.list(issue),
        },
        get_issue = {
            description = "Fetch one issue with its description",
            required_capabilities = { "issues.read" },
            mode = "read",
            args = dash.object {
                key = dash.string(),
                fields = dash.optional(dash.list(dash.string())),
            },
            result = "pm.issue@1",
        },
        search = {
            description = "Issues matching a JQL query, paginated up to max_results",
            required_capabilities = { "issues.read" },
            mode = "read",
            args = dash.object {
                jql = dash.string(),
                fields = dash.optional(dash.list(dash.string())),
                max_results = dash.optional(dash.integer()),
            },
            result = dash.list("pm.issue@1"),
        },
        get_transitions = {
            description = "Transitions available on an issue from its current status",
            required_capabilities = { "issues.read" },
            mode = "read",
            args = dash.object { key = dash.string() },
            result = dash.list(transition),
        },
        transition_issue = {
            description = "Move an issue to a target status name, a status category (todo | in_progress | done; the default is done) or by transition id; a no-op when already there",
            required_capabilities = { "issues.read", "issues.transition" },
            mode = "write",
            args = dash.object {
                key = dash.string(),
                status = dash.optional(dash.string()),
                status_category = dash.optional(dash.enum { "todo", "in_progress", "done" }),
                transition_id = dash.optional(dash.string()),
                comment = dash.optional(dash.string()),
                resolution = dash.optional(dash.string()),
            },
            result = dash.object {
                key = dash.string(),
                status = dash.string(),
                transitioned = dash.boolean(),
                from_status = dash.optional(dash.string()),
                transition_id = dash.optional(dash.string()),
            },
        },
        add_comment = {
            description = "Add a comment to an issue",
            required_capabilities = { "issues.write" },
            mode = "write",
            args = dash.object {
                key = dash.string(),
                body = dash.string(),
            },
            result = dash.object {
                id = dash.string(),
                key = dash.string(),
                created = dash.optional(dash.string()),
            },
        },
        create_issue = {
            description = "Create an issue",
            required_capabilities = { "issues.write" },
            mode = "write",
            args = dash.object {
                project = dash.string(),
                issue_type = dash.string(),
                summary = dash.string(),
                description = dash.optional(dash.string()),
                labels = dash.optional(dash.list(dash.string())),
                priority = dash.optional(dash.string()),
                assignee_id = dash.optional(dash.string()),
                parent = dash.optional(dash.string()),
            },
            result = dash.object {
                key = dash.string(),
                id = dash.string(),
                url = dash.optional(dash.string()),
            },
        },
        update_issue = {
            description = "Update summary, description, labels, priority or assignee of an issue",
            required_capabilities = { "issues.write" },
            mode = "write",
            args = dash.object {
                key = dash.string(),
                summary = dash.optional(dash.string()),
                description = dash.optional(dash.string()),
                labels = dash.optional(dash.list(dash.string())),
                priority = dash.optional(dash.string()),
                assignee_id = dash.optional(dash.string()),
            },
            result = dash.object {
                key = dash.string(),
                updated = dash.boolean(),
            },
        },
    },
}
