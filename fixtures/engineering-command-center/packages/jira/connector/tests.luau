local dash = require("dash")
local Runtime = require("runtime")

local function fake(routes, operation, args, cfg)
    local ctx = { config = cfg or { base_url = "https://acme.atlassian.net" }, operation = operation, args = args, sent = {} }
    ctx.http = {
        request = function(_self, req)
            table.insert(ctx.sent, req)
            local resp = routes[req.method .. " " .. req.url]
            if resp == nil then
                error("unexpected request " .. req.method .. " " .. req.url)
            end
            if type(resp) == "function" then
                resp = resp(req)
            end
            resp.headers = resp.headers or {}
            return resp
        end,
    }
    function ctx:await(v)
        return v
    end
    return ctx
end

local CLOUD = "https://acme.atlassian.net"
local FIELDS = "summary%2Cstatus%2Cpriority%2Cassignee%2Creporter%2Cissuetype%2Cproject%2Clabels%2Ccreated%2Cupdated%2Cresolution%2Cduedate%2Cparent"

local function jira_issue(key, status, cat)
    return {
        id = "100" .. string.match(key, "%d+$"), key = key, self = CLOUD .. "/rest/api/3/issue/" .. key,
        fields = {
            summary = "Summary of " .. key,
            status = { name = status, statusCategory = { key = cat or "indeterminate", name = "In Progress" } },
            priority = { name = "High" },
            assignee = { displayName = "Alice", accountId = "acc-1" },
            reporter = { displayName = "Bob" },
            issuetype = { name = "Story" },
            project = { key = string.match(key, "^(%u+)") },
            labels = { "backend" },
            created = "2026-01-01T00:00:00.000+0000",
            updated = "2026-01-02T00:00:00.000+0000",
            customfield_10016 = 5,
        },
    }
end

dash.test("get_issues uses Cloud /search/jql and normalizes fields in key order", function(_)
    local url = CLOUD .. "/rest/api/3/search/jql?fields=" .. FIELDS .. "&jql=key%20in%20%28DEV-1%2CPAY-2%29&maxResults=2"
    local ctx = fake({
        ["GET " .. url] = { status = 200, body = { issues = { jira_issue("PAY-2", "Done", "done"), jira_issue("DEV-1", "In Review") }, isLast = true } },
    }, "get_issues", { keys = { "dev-1", "PAY-2", "DEV-1" } })
    local res = Runtime.call(ctx)
    dash.assert_eq(#res, 2, "deduplicated")
    dash.assert_eq(res[1].key, "DEV-1", "requested order")
    local i = res[1]
    dash.assert_eq(i.summary, "Summary of DEV-1", "summary")
    dash.assert_eq(i.status, "In Review", "status name")
    dash.assert_eq(i.status_category, "in_progress", "category")
    dash.assert_eq(i.assignee, "Alice", "assignee")
    dash.assert_eq(i.assignee_id, "acc-1", "account id")
    dash.assert_eq(i.issue_type, "Story", "type")
    dash.assert_eq(i.project, "DEV", "project")
    dash.assert_eq(i.labels, { "backend" }, "labels")
    dash.assert_eq(i.url, CLOUD .. "/browse/DEV-1", "browse URL")
    dash.assert_eq(res[2].status_category, "done", "done category")
    dash.assert_eq(ctx.sent[1].credential, "auth", "credential handle")
end)

dash.test("unknown keys named by a 400 are dropped and the query retried once", function(_)
    local base = CLOUD .. "/rest/api/3/search/jql?fields=" .. FIELDS .. "&jql=key%20in%20%28"
    local ctx = fake({
        ["GET " .. base .. "DEV-1%2CDEV-999%29&maxResults=2"] = { status = 400, body = { errorMessages = { "An issue with key 'DEV-999' does not exist for field 'key'." } } },
        ["GET " .. base .. "DEV-1%29&maxResults=1"] = { status = 200, body = { issues = { jira_issue("DEV-1", "To Do", "new") } } },
    }, "get_issues", { keys = { "DEV-1", "DEV-999" } })
    local res = Runtime.call(ctx)
    dash.assert_eq(#res, 1, "known key kept")
    dash.assert_eq(#ctx.sent, 2, "one retry")
end)

dash.test("other 400s and auth failures are reported, not retried", function(_)
    local url = CLOUD .. "/rest/api/3/search/jql?fields=" .. FIELDS .. "&jql=key%20in%20%28DEV-1%29&maxResults=1"
    local ok, err = pcall(Runtime.call, fake({ ["GET " .. url] = { status = 401, body = { errorMessages = { "Client must be authenticated" } } } }, "get_issues", { keys = { "DEV-1" } }))
    dash.assert(not ok and string.find(tostring(err), "UNAUTHORIZED: Jira search returned HTTP 401: Client must be authenticated", 1, true) ~= nil, tostring(err))
end)

dash.test("malformed keys are skipped and never reach JQL", function(_)
    for _, bad in ipairs({ "DEV", "DEV-0", "DEV-01", "DEV-1) OR project = SECRET", "1-2", "DEV-1 " }) do
        local ctx = fake({}, "get_issues", { keys = { bad } })
        dash.assert_eq(Runtime.call(ctx), {}, "skipped: " .. bad)
        dash.assert_eq(#ctx.sent, 0, "nothing sent")
    end
    for _, bad in ipairs({ "DEV-1) OR project = SECRET", "x" }) do
        local ok, err = pcall(Runtime.call, fake({}, "get_issue", { key = bad }))
        dash.assert(not ok and string.find(tostring(err), "INVALID_ARGS", 1, true) ~= nil, tostring(err))
    end
end)

dash.test("many unknown keys in one batch are all dropped, however long Jira's error is", function(_)
    local good = { "OPS-1", "OPS-2" }
    local all, messages = { "OPS-1", "OPS-2" }, {}
    for i = 100, 111 do
        table.insert(all, "OPS-" .. i)
        table.insert(messages, "An issue with key 'OPS-" .. i .. "' does not exist for field 'key'.")
    end
    local base = CLOUD .. "/rest/api/3/search/jql?fields=" .. FIELDS .. "&jql=key%20in%20%28"
    local ctx = fake({
        ["GET " .. base .. table.concat(all, "%2C") .. "%29&maxResults=14"] = { status = 400, body = { errorMessages = messages, errors = {} } },
        ["GET " .. base .. table.concat(good, "%2C") .. "%29&maxResults=2"] = { status = 200, body = {
            -- OPS-2 was moved to NEW-9: Jira answers under the new key.
            issues = { jira_issue("OPS-1", "To Do", "new"), jira_issue("NEW-9", "To Do", "new") },
        } },
    }, "get_issues", { keys = all })
    local res = Runtime.call(ctx)
    dash.assert_eq(#ctx.sent, 2, "one retry drops all twelve unknown keys")
    dash.assert_eq(#res, 2, "requested issue plus the moved one")
    dash.assert_eq(res[1].key, "OPS-1", "requested first")
    dash.assert_eq(res[2].key, "NEW-9", "moved issue under its new key")
end)

dash.test("Cloud search pages with nextPageToken up to max_results", function(_)
    local p1 = CLOUD .. "/rest/api/3/search/jql?fields=" .. FIELDS .. "&jql=project%20%3D%20DEV&maxResults=2"
    local p2 = p1 .. "&nextPageToken=t2"
    local res = Runtime.call(fake({
        ["GET " .. p1] = { status = 200, body = { issues = { jira_issue("DEV-1", "To Do", "new"), jira_issue("DEV-2", "To Do", "new") }, nextPageToken = "t2", isLast = false } },
        ["GET " .. p2] = { status = 200, body = { issues = { jira_issue("DEV-3", "To Do", "new") }, isLast = true } },
    }, "search", { jql = "project = DEV" }, { base_url = CLOUD, page_size = 2 }))
    dash.assert_eq(#res, 3, "both pages")
end)

dash.test("Server/Data Center v2 search pages with startAt and keeps extra fields", function(_)
    local DC = "https://jira.acme.test"
    local fields = FIELDS .. "%2Ccustomfield_10016"
    local p = function(start)
        return "GET " .. DC .. "/rest/api/2/search?fields=" .. fields .. "&jql=project%20%3D%20DEV&maxResults=2&startAt=" .. start .. "&validateQuery=warn"
    end
    local res = Runtime.call(fake({
        [p(0)] = { status = 200, body = { startAt = 0, total = 3, issues = { jira_issue("DEV-1", "To Do", "new"), jira_issue("DEV-2", "To Do", "new") } } },
        [p(2)] = { status = 200, body = { startAt = 2, total = 3, issues = { jira_issue("DEV-3", "To Do", "new") } } },
    }, "search", { jql = "project = DEV", fields = { "customfield_10016" } }, { base_url = DC, api_version = "2", page_size = 2 }))
    dash.assert_eq(#res, 3, "all pages")
    dash.assert_eq(res[1].fields.customfield_10016, 5, "extra field")
end)

dash.test("get_issue returns the ADF description as text", function(_)
    local issue = jira_issue("DEV-1", "To Do", "new")
    issue.fields.description = { type = "doc", version = 1, content = {
        { type = "paragraph", content = { { type = "text", text = "Line one" } } },
        { type = "paragraph", content = { { type = "text", text = "Line " }, { type = "text", text = "two" } } },
    } }
    local res = Runtime.call(fake({
        ["GET " .. CLOUD .. "/rest/api/3/issue/DEV-1?fields=" .. FIELDS .. "%2Cdescription"] = { status = 200, body = issue },
    }, "get_issue", { key = "DEV-1" }))
    dash.assert_eq(res.description, "Line one\nLine two", "plain text")
end)

dash.test("transition_issue picks the workflow transition by target status and sends its id", function(_)
    local posted
    local ctx = fake({
        ["GET " .. CLOUD .. "/rest/api/3/issue/DEV-1?fields=status"] = { status = 200, body = jira_issue("DEV-1", "In Review") },
        ["GET " .. CLOUD .. "/rest/api/3/issue/DEV-1/transitions"] = { status = 200, body = { transitions = {
            { id = "51", name = "Back to Progress", to = { name = "In Progress", statusCategory = { key = "indeterminate" } } },
            { id = "31", name = "Close", to = { name = "Done", statusCategory = { key = "done" } } },
        } } },
        ["POST " .. CLOUD .. "/rest/api/3/issue/DEV-1/transitions"] = function(req)
            posted = req.body
            return { status = 204, body = "" }
        end,
    }, "transition_issue", { key = "DEV-1", status = "done", comment = "Merged", resolution = "Fixed" })
    local res = Runtime.call(ctx)
    dash.assert_eq(res, { key = "DEV-1", status = "Done", from_status = "In Review", transitioned = true, transition_id = "31" }, "result")
    dash.assert_eq(posted.transition, { id = "31" }, "transition id")
    dash.assert_eq(posted.fields, { resolution = { name = "Fixed" } }, "resolution")
    dash.assert_eq(posted.update.comment[1].add.body.type, "doc", "ADF comment on v3")
end)

dash.test("transition_issue is a no-op in the target status and fails without a path", function(_)
    local noop = fake({
        ["GET " .. CLOUD .. "/rest/api/3/issue/DEV-1?fields=status"] = { status = 200, body = jira_issue("DEV-1", "Done", "done") },
    }, "transition_issue", { key = "DEV-1" })
    local res = Runtime.call(noop)
    dash.assert(res.transitioned == false and res.status == "Done", "no-op")
    dash.assert_eq(#noop.sent, 1, "no write")

    local stuck = fake({
        ["GET " .. CLOUD .. "/rest/api/3/issue/DEV-1?fields=status"] = { status = 200, body = jira_issue("DEV-1", "To Do", "new") },
        ["GET " .. CLOUD .. "/rest/api/3/issue/DEV-1/transitions"] = { status = 200, body = { transitions = {
            { id = "11", name = "Start", to = { name = "In Progress" } },
        } } },
    }, "transition_issue", { key = "DEV-1", status = "Done" })
    local ok, err = pcall(Runtime.call, stuck)
    dash.assert(not ok and string.find(tostring(err), "NO_TRANSITION", 1, true) ~= nil and string.find(tostring(err), "Start -> In Progress", 1, true) ~= nil, tostring(err))

    local lost = fake({
        ["GET " .. CLOUD .. "/rest/api/3/issue/DEV-1?fields=status"] = { status = 200, body = jira_issue("DEV-1", "To Do", "new") },
        ["GET " .. CLOUD .. "/rest/api/3/issue/DEV-1/transitions"] = { status = 200, body = { transitions = {
            { id = "41", name = "Resolve", to = { name = "Resolved", statusCategory = { key = "done" } } },
        } } },
        ["POST " .. CLOUD .. "/rest/api/3/issue/DEV-1/transitions"] = { status = 504, body = "Gateway Timeout" },
    }, "transition_issue", { key = "DEV-1" })
    local ok2, err2 = pcall(Runtime.call, lost)
    dash.assert(not ok2 and string.find(tostring(err2), "UNKNOWN_OUTCOME", 1, true) ~= nil, tostring(err2))
    dash.assert_eq(lost.sent[3].body.transition, { id = "41" }, "the default target is any done-category status (here 'Resolved')")

    -- A transition is never chosen by its own name: "Close Issue" is not a status.
    local by_name = fake({
        ["GET " .. CLOUD .. "/rest/api/3/issue/DEV-1?fields=status"] = { status = 200, body = jira_issue("DEV-1", "To Do", "new") },
        ["GET " .. CLOUD .. "/rest/api/3/issue/DEV-1/transitions"] = { status = 200, body = { transitions = {
            { id = "31", name = "Close Issue", to = { name = "Done", statusCategory = { key = "done" } } },
        } } },
    }, "transition_issue", { key = "DEV-1", status = "Close Issue" })
    local ok3, err3 = pcall(Runtime.call, by_name)
    dash.assert(not ok3 and string.find(tostring(err3), "NO_TRANSITION", 1, true) ~= nil, tostring(err3))
end)

dash.test("comments and issue creation use ADF on v3 and plain text on v2", function(_)
    local c3 = fake({
        ["POST " .. CLOUD .. "/rest/api/3/issue/DEV-1/comment"] = { status = 201, body = { id = "10001", created = "t" } },
    }, "add_comment", { key = "DEV-1", body = "hello\nworld" })
    dash.assert_eq(Runtime.call(c3), { id = "10001", key = "DEV-1", created = "t" }, "comment result")
    dash.assert_eq(#c3.sent[1].body.body.content, 2, "two ADF paragraphs")

    local DC = "https://jira.acme.test"
    local c2 = fake({
        ["POST " .. DC .. "/rest/api/2/issue/DEV-1/comment"] = { status = 201, body = { id = 5 } },
    }, "add_comment", { key = "DEV-1", body = "hello" }, { base_url = DC, api_version = "2" })
    Runtime.call(c2)
    dash.assert_eq(c2.sent[1].body, { body = "hello" }, "plain body on v2")
    local blank = fake({}, "add_comment", { key = "DEV-1", body = " \n " })
    local okb, errb = pcall(Runtime.call, blank)
    dash.assert(not okb and string.find(tostring(errb), "non-blank", 1, true) ~= nil and #blank.sent == 0, tostring(errb))

    local created = fake({
        ["POST " .. CLOUD .. "/rest/api/3/issue"] = { status = 201, body = { id = "10200", key = "DEV-200", self = "x" } },
    }, "create_issue", { project = "DEV", issue_type = "Bug", summary = "Crash", description = "Steps", labels = { "p1" }, priority = "High", assignee_id = "acc-1" })
    dash.assert_eq(Runtime.call(created), { key = "DEV-200", id = "10200", url = CLOUD .. "/browse/DEV-200" }, "created")
    local f = created.sent[1].body.fields
    dash.assert_eq(f.project, { key = "DEV" }, "project")
    dash.assert_eq(f.issuetype, { name = "Bug" }, "issue type")
    dash.assert_eq(f.assignee, { accountId = "acc-1" }, "Cloud assignee by accountId")
    dash.assert_eq(f.description.type, "doc", "ADF description")

    local updated = fake({
        ["PUT " .. DC .. "/rest/api/2/issue/DEV-1"] = { status = 204, body = "" },
    }, "update_issue", { key = "DEV-1", assignee_id = "jdoe", labels = {} }, { base_url = DC, api_version = "2" })
    dash.assert_eq(Runtime.call(updated), { key = "DEV-1", updated = true }, "updated")
    dash.assert_eq(updated.sent[1].body.fields.assignee, { name = "jdoe" }, "Server assignee by name")
end)
