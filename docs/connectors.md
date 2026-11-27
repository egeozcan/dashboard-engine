# Connectors

The following connectors ship with dash.

| Connector | Where it lives | Talks to |
| --- | --- | --- |
| `github.connector` | `github` Package (in the reference Workspace) | github.com or GitHub Enterprise Server REST API |
| `jira.connector` | `jira` Package (in the reference Workspace) | Jira Cloud (REST v3) or Jira Server/Data Center (REST v2) |
| `dash.fal` | built in (every Workspace) | fal.ai asynchronous inference queue |
| `dash.hacker_news` | built in (every Workspace) | public Hacker News search and items via Algolia |
| `dash.http` | built in (every Workspace) | any REST/JSON API under one base URL |

The connectors share these runtime conventions.

- **The host makes every request.** Requests go through the host HTTP broker: the Workspace
  `[network]` policy intersected with the Administrator network ceiling, SSRF checks, credential
  injection, and redaction. Luau never sees a secret.
- **Result contracts apply in live runs and Preview.** Callers can inspect each operation’s manifest to see its return type.
  Some operations return provider-specific JSON, including fal model results.
- **Errors carry stable codes.** Each connector error message carries a code (after the Luau
  location prefix in action reasons and query errors): `UNAUTHORIZED`, `FORBIDDEN`,
  `NOT_FOUND`, `CONFLICT`, `INVALID_REQUEST`, `RATE_LIMITED` (with the `Retry-After` or reset
  time), `UPSTREAM_ERROR`, `INVALID_ARGS`, `INVALID_RESPONSE`, `CONFIG`, and connector-specific
  ones (`NOT_MERGEABLE`, `NO_TRANSITION`). The upstream's own error message is appended.
- **Lost writes are `unknown`, not `failed`.** A 5xx answer to a write (other than 501) means
  `UNKNOWN_OUTCOME`. So does a redirect answer to a write that dash will not follow: a 301 or
  302 (a browser would re-send it as a GET), a 3xx without a `Location`, or any redirect out of
  `base_url`. A 303 is followed
  with a GET (the write was applied; if that GET fails, the outcome is `unknown`), and a 307 or
  308 re-sends the same request. The action execution becomes `unknown` instead of `failed`, and it is never
  retried blindly. Actions with a `reconcile` handler resolve it.
- **Requests stay under `base_url`.** Every request, pagination link and redirect hop must stay
  under the connector's `base_url`: same origin and path prefix, and no `.`/`..` segments, even
  behind encoded separators such as `..%2F`. The host broker enforces this for redirects before
  the credential is attached, so a credential never reaches another path on the same host.
- **Paging is capped.** Operations that automatically paginate follow the API's own scheme (Link headers, which may be
  relative, plus cursors, tokens and offsets). It stops at `max_pages` (default 5, hard cap 50).
- **JSON nulls read as absent.** An optional argument or config value sent as `null` means the
  same as leaving it out. Inside a request body a `null` is data and is sent as is (PATCH APIs
  clear fields with it).
- **Mind the time budget.** A provider query, including its connector calls, runs within the
  Instance's Luau wall budget (5 s by default). Big lists need a larger budget for the data
  provider Instance and its connector Instance, e.g. `[resources.instances.<id>]
  timeout_ms = 20000` in `dashboard.toml`.

## Setup checklist

1. **Store the credential.** Use the vault with the right **credential scheme**:

   | Service | Secret value | Scheme |
   | --- | --- | --- |
   | GitHub (PAT, fine-grained token, GitHub App installation token) | the token | `bearer` (default) |
   | Jira Cloud | `email@company.com:API_TOKEN` | `basic` |
   | Jira Server/Data Center | personal access token | `bearer` (default) |
   | fal.ai | the raw fal API key | `key` (sends `Authorization: Key <value>`) |
   | Hacker News (Algolia) | anonymous | no secret binding |
   | REST API with an API-key header | the key | `header:X-Api-Key` (any header name) |
   | REST API with an API-key query parameter | the key | `query:api_key` |

   Restrict each secret to the connector Instances that may bind it and to the hosts it may be
   sent to:

   ```bash
   printf %s "$JIRA_EMAIL:$JIRA_API_TOKEN" | dash secret set jira-token \
     --allowed-binding jira_reader --allowed-binding jira_writer \
     --allowed-host your-site.atlassian.net --credential-scheme basic
   ```
2. **Allow the host.** Add the API host to `[network] allowed_hosts` in `dashboard.toml`, and to
   the Administrator network ceiling (`dash network grant`). Writes need their methods in
   `allowed_methods`: GitHub uses `PUT` (merge), `PATCH` (issue updates) and `POST` (issues and
   comments); Jira writes use `POST`/`PUT`; `dash.http` writes use whichever of `POST`, `PUT`,
   `PATCH` and `DELETE` your actions send. fal uses `POST` to submit and `PUT` to cancel.
3. **Declare the connector Instance.** Grant it only the capabilities it needs (least
   privilege: separate reader and writer Instances).
4. **Bind data providers and actions** to the Instance, in `instances/data.yaml` and
   `instances/actions.yaml`.

---

## GitHub (`github` Package)

```yaml
# instances/connectors.yaml
connectors:
  github_reader:
    definition: github.connector
    secrets: { auth: secret://github-token }
    capabilities: [pull_requests.read, issues.read, checks.read, actions.read]
    config:
      base_url: https://api.github.com      # GHES: https://ghe.example.com/api/v3
      # per_page: 100, max_pages: 5, api_version: "2022-11-28", auth: none (anonymous)
  github_writer:
    definition: github.connector
    secrets: { auth: secret://github-token }
    capabilities: [pull_requests.read, pull_requests.merge, issues.read, issues.write]
    config: { base_url: https://api.github.com }
```

Token permissions:

| Operations | Fine-grained token needs |
| --- | --- |
| Pull request reads | "Pull requests: read" |
| Merging | "Contents: write" |
| Issue reads and writes | "Issues: read/write" |
| `get_ci_status` | "Commit statuses: read" and "Checks: read" |
| `list_workflow_runs` | "Actions: read" |

### Operations

| Operation | Capability | Returns |
| --- | --- | --- |
| `list_prs {repository, state?, base?, head?, sort?, direction?, max_pages?}` | `pull_requests.read` | list of `dev.pull_request@1` |
| `get_pr {repository, number}` | `pull_requests.read` | `dev.pull_request@1` plus `mergeable`, `mergeable_state`, `additions`, ... |
| `merge_pr {repository, number, merge_method?, sha?, commit_title?, commit_message?}` | `pull_requests.merge` | `{merged, sha, message}` |
| `list_issues {repository, state?, labels?, assignee?, creator?, since?, sort?, direction?}` | `issues.read` | list of `dev.issue@1` (pull requests excluded) |
| `get_issue {repository, number}` | `issues.read` | `dev.issue@1` |
| `create_issue {repository, title, body?, labels?, assignees?, milestone?}` | `issues.write` | `dev.issue@1` |
| `update_issue {repository, number, title?, body?, state?, state_reason?, labels?, assignees?}` | `issues.write` | `dev.issue@1` |
| `add_comment {repository, number, body}` | `issues.write` | `{id, url, created_at}` |
| `search_issues {query, sort?, order?, max_pages?}` | `issues.read` | list of `dev.issue@1` (at most 1000, a GitHub limit) |
| `get_ci_status {repository, ref}` | `checks.read` | `dev.ci_status@1` |
| `list_workflow_runs {repository, workflow?, branch?, status?, event?, actor?, max_pages?}` | `actions.read` | list of `dev.workflow_run@1` |

Fields of each record:

- **`dev.pull_request@1`:**
  - Identity: `id` (`owner/repo#number`), `number`, `github_id`, `repository`, `url`.
  - Content: `title`, `author`, `labels`, `assignees`, `requested_reviewers`.
  - State: `state` (`open` | `closed` | `merged`), `merged`, `draft`.
  - Branches: `base`, `head`, `head_sha`.
  - Timestamps: `created_at`, `updated_at`, `closed_at`, `merged_at`.

  GitHub has no `merged` state of its own: dash reports a closed PR with a `merged_at` as
  `merged`, and `list_prs state=merged` lists closed PRs and keeps the merged ones.
- **`dev.issue@1`:** `id`, `number`, `title`, `repository`, `state` (`merged` for merged PRs in
  search results), `state_reason`, `is_pull_request`, `author`, `labels`, `assignees`,
  `comments`, `url`, timestamps.
- **`dev.ci_status@1`:** `state` is `failure` if any check failed, else `pending` if any is
  still running, else `success`, else `neutral`; `unknown` if there are no checks at all. Also
  `total`, the per-state counts, and `checks[]` (`{name, kind: status|check, state, url, ...}`).
  A timed-out, cancelled or action-required check run counts as a failure; skipped and neutral
  ones count as neutral.
- **`dev.workflow_run@1`:** `id`, `repository`, `name`, `title`, `workflow_id`, `run_number`, `run_attempt`,
  `status`, `conclusion`, `branch`, `head_sha`, `event`, `actor`, `url`, timestamps.

### Data providers and actions

| Definition | Kind | Notes |
| --- | --- | --- |
| `github.pull_requests` | data provider (polls every 60 s) | PRs of a repository, optionally into one `base` branch. While polling, it remembers every open PR (whatever `base` filter the query has). A PR that left the open list is confirmed with `get_pr` (at most 5 per poll; the rest on later polls), and each merge emits one durable `github.pr_merged`. `state=closed` includes merged PRs. Closed and merged lists come newest activity first. |
| `github.issues` | data provider (120 s) | Issues of a repository |
| `github.search` | data provider (300 s, search rate limit) | Any GitHub search query |
| `github.workflow_runs` | data provider (60 s) | Recent Actions runs |
| `github.ci_status` | data provider (60 s) | CI state of a branch, tag or SHA |
| `github.merge_pr` | action: Dry run, reconcile, emits `github.pr_merged` | The Dry run reports conflicts, branch protection and drafts as infeasible, and shows the `head_sha`. Pass that as `expected_head_sha` to merge only the reviewed commit: a moved head fails with `CONFLICT` before anything is written. Without it, the merge pins the head read right before merging. A refusal (405) fails definitively. |
| `github.create_issue` | action: Dry run | Validates the repository and title |
| `github.comment` | action: Dry run | The Dry run checks that the issue or PR exists |
| `github.set_issue_state` | action: Dry run, reconcile | Close or reopen; a no-op when already in that state |

---

## Jira (`jira` Package)

```yaml
connectors:
  jira_reader:
    definition: jira.connector
    secrets: { auth: secret://jira-token }   # Cloud: scheme basic, "email:api_token"
    capabilities: [issues.read]
    config:
      base_url: https://your-site.atlassian.net
      # api_version: "3" (Cloud, default) | "2" (Server/Data Center); page_size: 100; max_pages: 5
  jira_writer:
    definition: jira.connector
    secrets: { auth: secret://jira-token }
    capabilities: [issues.read, issues.transition, issues.write]
    config: { base_url: https://your-site.atlassian.net }
```

### Operations

| Operation | Capability | Returns |
| --- | --- | --- |
| `get_issues {keys, fields?}` | `issues.read` | list of `pm.issue@1` in key order. Malformed, nonexistent and invisible keys are skipped; an issue moved to another key comes back under its new key, after the requested ones. |
| `get_issue {key, fields?}` | `issues.read` | `pm.issue@1` plus `description` (plain text) |
| `search {jql, fields?, max_results?}` | `issues.read` | list of `pm.issue@1` |
| `get_transitions {key}` | `issues.read` | list of `pm.transition@1` (`{id, name, to_status, to_category}`) |
| `transition_issue {key, status?, status_category?, transition_id?, comment?, resolution?}` | `issues.read`, `issues.transition` | `{key, status, transitioned, from_status, transition_id}` |
| `add_comment {key, body}` | `issues.write` | `{id, key, created}` |
| `create_issue {project, issue_type, summary, description?, labels?, priority?, assignee_id?, parent?}` | `issues.write` | `{key, id, url}` |
| `update_issue {key, summary?, description?, labels?, priority?, assignee_id?}` | `issues.write` | `{key, updated}` |

How the API versions are handled:

- **Search.** Cloud uses `GET /rest/api/3/search/jql` with `nextPageToken` (the old `/search`
  endpoint is retired on Cloud). Server/DC uses `GET /rest/api/2/search` with `startAt` and
  `validateQuery=warn`.
- **Text fields.** Cloud bodies (comments, descriptions) are sent as Atlassian Document Format;
  descriptions read from Cloud come back as plain text. On v2, text goes as plain strings.
- **Assignees.** `assignee_id` is the account ID on Cloud and the user name on Server/DC.
- **Unknown issue keys.** Jira Cloud fails a whole `key in (...)` query when a key does not exist
  or is not visible. `get_issues` reads every key Jira names in the error, drops them all, and
  asks again; it repeats until the query succeeds.
- **Key validation.** Keys are validated (`PROJECT-123`) before they reach JQL or a URL, so they
  cannot inject JQL.

**`pm.issue@1`:**

- Identity and links: `key`, `id`, `url` (the `/browse/` link), `project`, `parent`.
- Content: `summary`, `labels`, `issue_type`, `priority`, `description` (from `get_issue` only).
- Status: `status`, `status_category` (`todo` | `in_progress` | `done`), `resolution`.
- People: `assignee`, `assignee_id`, `reporter`.
- Dates: `created`, `updated`, `due_date`.
- Extra fields: `fields{}` holds any custom fields you asked for in `fields`.

### Transitions

`transition_issue` moves an issue to a target through the issue's workflow. The target is one
of:

- `status`: a status name (case-insensitive).
- `status_category`: `todo`, `in_progress` or `done`. Use it when workflows call the final
  status Done, Closed, Resolved or a translation of these. When several statuses of the category
  are reachable (Done, Won't Do and Canceled are all `done`), the conventional one wins (`done`:
  Done, Resolved, Closed, Complete(d), Fixed; `in_progress`: In Progress; `todo`: To Do, Open,
  Backlog). Without one, the call fails with `NO_TRANSITION` and asks for `status`, and the Dry
  run reports it as infeasible: a category never picks Won't Do by workflow order. Statuses that
  mean the work was not done (a word starting with Won't, Cancel, Reject, Declin(ed),
  Duplicate, Invalid, Obsolete, Abandon(ed), or a phrase such as Not Doing, Will Not, Cannot
  Reproduce, Out of Scope, Not Planned, also written as one word like NotABug, in any apostrophe
  spelling and next to symbols such as ❌) are never picked by a category, even when
  they are the only ones reachable; name them with `status` to use them. This word list is
  English: with localized status names, two or more reachable statuses of the category are
  ambiguous (pass `status`), and status names match in any script.
- `transition_id`: an explicit transition.
- Neither: a status of category `done`, the same as `status_category: done` (what the
  reference `engineering.complete_work` automation sends).

The steps:

1. It reads the current status. If the issue is already at the target, it returns
   `transitioned = false` and sends no write.
2. It reads the available transitions and picks one whose resulting status matches the target.
   Transitions are never matched by their own name, so a target always names the resulting
   status, and repeating the call or reconciling it is reliable. When several transitions lead
   into that status, one named like "Won't Do" or "Cancel" (whose post-function usually sets a
   Won't Do resolution) is never picked unless the target status itself is such a status. A
   single remaining transition is used. Among several, only one named like the status or a plain
   completion (Done, Mark as Done, Close, Close Issue, Resolve, Resolve Issue, Complete, Finish,
   Fix) is picked, because transitions such as Not a Problem or Works for Me set their own
   resolution; otherwise the call fails with `NO_TRANSITION` as ambiguous. If only such
   transitions lead there, the call fails with `NO_TRANSITION`: move the issue in Jira, or call
   the connector operation `transition_issue` with that `transition_id`.
3. It posts `{transition: {id}}`, plus the optional resolution and comment (atomically, in the
   same request).

When no transition reaches the target, it fails with `NO_TRANSITION` and lists the available
transitions. Nothing is written in that case.

### Data providers and actions

| Definition | Kind | Notes |
| --- | --- | --- |
| `jira.issues_by_keys` | data provider | Issues for a list of keys |
| `jira.search` | data provider (polls every 120 s) | Any JQL query |
| `jira.transition_issue` | action: Dry run, reconcile | Target by `status` or `status_category` (default: a `done` status). The Dry run names the transition, or explains why the target is unreachable. Idempotent. Reconcile checks the status. |
| `jira.add_comment` | action: Dry run | The Dry run checks that the issue exists |
| `jira.create_issue` | action: Dry run | Validates the project and summary |

---

## fal.ai inference queue (`dash.fal`)

Allow `queue.fal.run` in the Workspace and Administrator network policies. Store the raw fal key
in the vault so the broker can attach it to requests.

```bash
printf %s "$FAL_KEY" | dash secret set fal-key --credential-scheme key \
  --allowed-binding fal_queue --allowed-host queue.fal.run
```

```yaml
# instances/connectors.yaml
connectors:
  fal_queue:
    definition: dash.fal
    secrets: { auth: secret://fal-key }
    capabilities: [queue.submit, queue.read, queue.cancel]
```

| Operation | Capability | Returns |
| --- | --- | --- |
| `submit {endpoint_id, input}` | `queue.submit` | Queue receipt with a `request_id`. The JSON input follows the model's schema. |
| `status {endpoint_id, request_id, logs?}` | `queue.read` | Current state (`IN_QUEUE`, `IN_PROGRESS`, `COMPLETED`), with queue position, logs, metrics or error details when available. |
| `result {endpoint_id, request_id}` | `queue.read` | The model's JSON output. Fetch after completion. |
| `cancel {endpoint_id, request_id}` | `queue.cancel` | The cancellation acknowledgement. `CANCELLATION_REQUESTED` means a running job can still finish. |

For example, bind an action input with
`dash.connector_ref("dash.fal", { required_capabilities = { "queue.submit" } })`. Its handler
can submit one job and return its receipt immediately.

```luau
return ctx:await(ctx.inputs.fal:call("submit", {
    endpoint_id = "fal-ai/flux/schnell",
    input = { prompt = ctx.args.prompt },
}))
```

Keep the endpoint ID and request ID together. Pass both to a later status, result or cancel
call, including the original model variant in `endpoint_id`. [fal queue API](https://fal.ai/docs/documentation/model-apis/inference/queue)

## Hacker News search by Algolia (`dash.hacker_news`)

Allow GET requests to `hn.algolia.com` in both network policies. The public Hacker News index
accepts anonymous requests. [Algolia HN Search API](https://hn.algolia.com/api)

```yaml
# instances/connectors.yaml
connectors:
  hn:
    definition: dash.hacker_news
    capabilities: [items.read]
```

Both operations require `items.read`.

| Operation | Returns |
| --- | --- |
| `search {query?, sort?, tags?, numeric_filters?, page?, hits_per_page?}` | One page with `hits`, `page`, `hits_per_page`, `total_hits` and `total_pages`. Each hit includes its numeric `id`, `discussion_url`, tags, and available story or comment fields. |
| `get_item {id}` | The Algolia item with its nested `children` and a `discussion_url` on the root item. |

Use `sort = "relevance"` (default) or `"date"` (newest first). Pages start at zero.
`tags` accepts Algolia expressions such as `front_page`, `show_hn` or `comment,story_42`.
Bind a data provider input with
`dash.connector_ref("dash.hacker_news", { required_capabilities = { "items.read" } })`.
For example, its handler can return the latest matching Show HN stories.

```luau
local page = ctx:await(ctx.inputs.hn:call("search", {
    query = "dashboard", tags = "show_hn", sort = "date", hits_per_page = 10,
}))
return page.hits
```

### Preview fixtures for fal and Hacker News

Set `config.mock` on the connector Instance to supply upstream response bodies. Keys use the
HTTP method and path relative to the API base. An exact query-string match takes precedence
over a path-only match. You can also supply a response envelope with a numeric HTTP `status`,
`body` and optional `headers`. A missing fixture raises `MOCK_NOT_CONFIGURED`.

```yaml
# In fal_queue.config.mock
"POST /fal-ai/flux/schnell": { request_id: demo, status: IN_QUEUE }
"GET /fal-ai/flux/requests/demo/status": { request_id: demo, status: COMPLETED }
"GET /fal-ai/flux/requests/demo": { images: [{ url: "https://example.com/demo.png" }] }

# In hn.config.mock
"GET /search_by_date": { hits: [], page: 0, hitsPerPage: 10, nbPages: 0, nbHits: 0 }
```

## Generic REST/JSON (`dash.http`, built in)

Use it for any JSON API that has no dedicated connector: status pages, internal services,
PagerDuty, Datadog, GitLab, and so on. Every request stays under the Instance's `base_url`:
paths must start with `/`, and `//`, backslashes, `.`/`..` segments (also percent-encoded) and
fragments are refused before anything is sent.

```yaml
connectors:
  status_api:
    definition: dash.http
    secrets: { auth: secret://status-key }        # vault scheme e.g. header:X-Api-Key
    capabilities: [http.read]                     # http.write for writes
    config:
      base_url: https://status.example.com/api/v1
      headers: { X-Team: core }                   # static, non-secret headers (per-call ones cannot replace them)
      query: { api-version: "2" }                 # static query parameters (per-call ones cannot replace them)
      # auth: none   (anonymous), max_pages: 5
      # mock:  responses for Preview and tests (see below)
```

### Operations

- **`get {path, query?, headers?, result_path?, paginate?, max_pages?, allow_not_found?}`**
  (`http.read`) returns the response body, or the value at `result_path` (dotted, with 0-based
  list indexes: `data.items`, `items.0.id`). With `allow_not_found`, a 404 returns `null`.
- **`request {method: POST|PUT|PATCH|DELETE, path, query?, headers?, body?, result_path?,
  idempotency_key?, idempotency_header?}`** (`http.write`) sends a JSON body and returns
  `{status, body, location}`.

`paginate.mode` sets how pages are fetched (the result is the concatenated items):

| Mode | How the next page is requested | Settings |
| --- | --- | --- |
| `link` | The `Link: rel="next"` URL (same origin only) | `size_param`, `page_size` |
| `cursor` | The cursor at `cursor_path` (default `next_cursor`), sent as `cursor_param` (default `cursor`) | `items_path`, `size_param`, `page_size` |
| `page` | `page_param` (default `page`) counted from `start` (default 1) | `size_param` (default `per_page`), `page_size` (default 100) |
| `offset` | `page_param` (default `offset`) advanced by the number of items received | `size_param` (default `limit`) |

Paging stops at an empty or short page, when there is no next cursor or link, or at
`max_pages`. In `link` mode later pages use the upstream's `next` URL as given (most APIs
repeat the full query string there).

### `dash.http_query`: a data provider configured in YAML

```yaml
data:
  team_services:
    definition: dash.http_query
    inputs: { api: status_api }
    config:
      path: /teams/{team}/services        # {name} comes from query parameters (URL-encoded)
      query: { include: health }
      paginate: { mode: cursor, items_path: data, cursor_path: meta.next, cursor_param: cursor, size_param: limit, page_size: 100 }
      fields:                              # optional projection: output field -> dotted path
        id: id
        name: attributes.name
        healthy: attributes.healthy
      # forward_params: true   (other query parameters become query-string parameters; they
      #                         never replace a configured `query` parameter)
      # result_path, headers, max_pages, allow_not_found, keep_warm
```

An Adapter queries it like any provider: `ctx.inputs.services:query({ team = "core" })`. It
polls every 60 s while a Dashboard subscribes to it.

### `dash.http_action`: an action configured in YAML

```yaml
actions:
  deploy:
    definition: dash.http_action
    inputs: { api: deploy_api }            # connector Instance with http.write
    config:
      method: POST                         # POST (default) | PUT | PATCH | DELETE
      path: /services/{service}/deploys    # {name} comes from the action arguments
      body: { env: prod }                  # static body; the remaining arguments merge over it
      idempotency_header: Idempotency-Key  # sends the execution's idempotency key
```

`args = { service = "api", ref = "v1.2.3" }` sends `POST /services/api/deploys` with body
`{"env": "prod", "ref": "v1.2.3"}`. An explicit `body` argument replaces the merged body. A `null`
argument is sent as JSON `null` in the body; a `null` path parameter counts as missing. The
Dry run shows the exact method, path and body without sending anything. A write invalidates
every `dash.http_query` Instance.

### Preview and tests: `dash.http_mock`

In Preview (`dash serve --use-mocks`, Drafts, scenarios), `dash.http` Instances answer from
their own `config.mock`. A value is either a response body, or a response envelope (a numeric
`status` and a `body`, optionally `headers`) that goes through the same success and error
handling as a live response:

```yaml
    config:
      base_url: https://status.example.com/api/v1
      mock:
        "GET /teams/core/services": { data: [ { id: api, attributes: { name: core-api, healthy: true } } ] }
        "GET /teams/core/services?include=health": { data: [] }   # exact query string wins
        "GET /teams/gone/services": { status: 404, body: { error: { message: team deleted } } }
        "POST /deploys": { status: 201, headers: { location: /deploys/d-1 }, body: { id: d-1 } }
```

An unconfigured GET fails with `MOCK_NOT_CONFIGURED`. An unconfigured write answers 200 and
echoes the request body. Writes are recorded in the Mock's state.

### Writing your own connector on top

A Workspace Package can reuse the shared helpers with `libraries = { "dash.http_util" }`:

- `check_path`, `url`, `get_path`, `fill_template`, `within` and `resolve_link` (the safe URL
  building above). Pass `confine_to = util.base_url(cfg)` in `ctx.http:request` so the broker
  holds redirects to the base URL too.
- `clean` (drops JSON nulls) and `array` (an empty list that serializes as `[]`).
- `fail` (the error codes above).
- `max_pages`.

The runtime SDK also offers `dash.url.encode`, `dash.url.query(table)` (sorted,
percent-encoded, lists repeated), `dash.url.parse_links(link_header)` and `dash.array()`.
