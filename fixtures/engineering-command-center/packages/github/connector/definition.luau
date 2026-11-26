local dash = require("dash")

local strings = dash.optional(dash.list(dash.string()))

local pull_request = dash.semantic("dev.pull_request@1", dash.object {
    id = dash.string(),
    number = dash.integer(),
    title = dash.string(),
    repository = dash.string(),
    state = dash.string(),
    labels = strings,
    assignees = strings,
    requested_reviewers = strings,
})

local issue = dash.semantic("dev.issue@1", dash.object {
    id = dash.string(),
    number = dash.integer(),
    title = dash.string(),
    repository = dash.string(),
    state = dash.string(),
    labels = strings,
    assignees = strings,
})

local workflow_run = dash.semantic("dev.workflow_run@1", dash.object {
    id = dash.integer(),
    name = dash.string(),
    repository = dash.string(),
    status = dash.string(),
})

local ci_status = dash.semantic("dev.ci_status@1", dash.object {
    repository = dash.string(),
    ref = dash.string(),
    state = dash.string(),
    total = dash.integer(),
    checks = dash.list(dash.object {
        name = dash.string(),
        kind = dash.string(),
        state = dash.string(),
    }),
})

local list_state = dash.optional(dash.enum { "open", "closed", "all" })
local direction = dash.optional(dash.enum { "asc", "desc" })

return dash.connector {
    id = "github.connector",
    description = "GitHub REST API connector (github.com or GitHub Enterprise Server): pull requests, issues, comments, search, CI status and Actions workflow runs",
    runtime = "runtime.luau",
    handler = "call",
    libraries = { "github.model", "dash.http_util" },
    credentials = { "auth" },
    capabilities = {
        "pull_requests.read",
        "pull_requests.merge",
        "issues.read",
        "issues.write",
        "checks.read",
        "actions.read",
    },
    config = dash.object {
        -- https://api.github.com (default) or https://<ghes-host>/api/v3
        base_url = dash.optional(dash.string()),
        -- "secret" (default) attaches the `auth` secret as a Bearer token; "none" is anonymous.
        auth = dash.optional(dash.enum { "secret", "none" }),
        api_version = dash.optional(dash.string()),
        per_page = dash.optional(dash.integer()),
        max_pages = dash.optional(dash.integer()),
    },
    operations = {
        list_prs = {
            description = "List pull requests of a repository (state merged = closed and merged)",
            required_capabilities = { "pull_requests.read" },
            mode = "read",
            args = dash.object {
                repository = dash.string(),
                state = dash.optional(dash.enum { "open", "closed", "merged", "all" }),
                base = dash.optional(dash.string()),
                head = dash.optional(dash.string()),
                sort = dash.optional(dash.enum { "created", "updated", "popularity", "long-running" }),
                direction = direction,
                max_pages = dash.optional(dash.integer()),
            },
            result = dash.list("dev.pull_request@1"),
        },
        get_pr = {
            description = "Get one pull request, including mergeability",
            required_capabilities = { "pull_requests.read" },
            mode = "read",
            args = dash.object {
                repository = dash.string(),
                number = dash.integer(),
            },
            result = pull_request,
        },
        merge_pr = {
            description = "Merge a pull request (optionally only if its head is still `sha`)",
            required_capabilities = { "pull_requests.merge" },
            mode = "write",
            args = dash.object {
                repository = dash.string(),
                number = dash.integer(),
                merge_method = dash.optional(dash.enum { "merge", "squash", "rebase" }),
                sha = dash.optional(dash.string()),
                commit_title = dash.optional(dash.string()),
                commit_message = dash.optional(dash.string()),
            },
            result = dash.object {
                merged = dash.boolean(),
                sha = dash.string(),
                message = dash.optional(dash.string()),
            },
        },
        list_issues = {
            description = "List issues of a repository (pull requests excluded)",
            required_capabilities = { "issues.read" },
            mode = "read",
            args = dash.object {
                repository = dash.string(),
                state = list_state,
                labels = strings,
                assignee = dash.optional(dash.string()),
                creator = dash.optional(dash.string()),
                since = dash.optional(dash.string()),
                sort = dash.optional(dash.enum { "created", "updated", "comments" }),
                direction = direction,
                max_pages = dash.optional(dash.integer()),
            },
            result = dash.list(issue),
        },
        get_issue = {
            description = "Get one issue (or pull request) by number",
            required_capabilities = { "issues.read" },
            mode = "read",
            args = dash.object {
                repository = dash.string(),
                number = dash.integer(),
            },
            result = "dev.issue@1",
        },
        create_issue = {
            description = "Create an issue",
            required_capabilities = { "issues.write" },
            mode = "write",
            args = dash.object {
                repository = dash.string(),
                title = dash.string(),
                body = dash.optional(dash.string()),
                labels = strings,
                assignees = strings,
                milestone = dash.optional(dash.integer()),
            },
            result = "dev.issue@1",
        },
        update_issue = {
            description = "Update an issue: title, body, state (close/reopen), labels, assignees",
            required_capabilities = { "issues.write" },
            mode = "write",
            args = dash.object {
                repository = dash.string(),
                number = dash.integer(),
                title = dash.optional(dash.string()),
                body = dash.optional(dash.string()),
                state = dash.optional(dash.enum { "open", "closed" }),
                state_reason = dash.optional(dash.enum { "completed", "not_planned", "reopened" }),
                labels = strings,
                assignees = strings,
            },
            result = "dev.issue@1",
        },
        add_comment = {
            description = "Comment on an issue or pull request",
            required_capabilities = { "issues.write" },
            mode = "write",
            args = dash.object {
                repository = dash.string(),
                number = dash.integer(),
                body = dash.string(),
            },
            result = dash.object {
                id = dash.integer(),
                url = dash.optional(dash.string()),
                created_at = dash.optional(dash.string()),
            },
        },
        search_issues = {
            description = "Search issues and pull requests with GitHub search syntax (e.g. 'repo:acme/api is:pr is:open review-requested:@me')",
            required_capabilities = { "issues.read" },
            mode = "read",
            args = dash.object {
                query = dash.string(),
                sort = dash.optional(dash.string()),
                order = direction,
                max_pages = dash.optional(dash.integer()),
            },
            result = dash.list("dev.issue@1"),
        },
        get_ci_status = {
            description = "Combined CI state of a ref from commit statuses and check runs",
            required_capabilities = { "checks.read" },
            mode = "read",
            args = dash.object {
                repository = dash.string(),
                ref = dash.string(),
            },
            result = ci_status,
        },
        list_workflow_runs = {
            description = "List GitHub Actions workflow runs of a repository or one workflow",
            required_capabilities = { "actions.read" },
            mode = "read",
            args = dash.object {
                repository = dash.string(),
                workflow = dash.optional(dash.string()),
                branch = dash.optional(dash.string()),
                status = dash.optional(dash.string()),
                event = dash.optional(dash.string()),
                actor = dash.optional(dash.string()),
                max_pages = dash.optional(dash.integer()),
            },
            result = dash.list(workflow_run),
        },
    },
}
