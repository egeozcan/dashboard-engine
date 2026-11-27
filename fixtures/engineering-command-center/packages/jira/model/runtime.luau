--!strict
-- Stable records of the Jira package. Normalizers accept raw Jira REST payloads (v2 Server/Data
-- Center or v3 Cloud) or already-normalized records (the Mock's fixtures).
local M = {}

-- JSON null arrives as a userdata sentinel: treat it like nil.
local function v(x: any): any
    if type(x) == "userdata" then
        return nil
    end
    return x
end
M.v = v

-- A copy of an argument/query table without its JSON nulls (userdata sentinels), so an
-- optional value sent as null reads as absent. Shallow: nested values are kept as they are.
function M.clean(t: any): any
    local out = {}
    if type(t) == "table" then
        for k, x in pairs(t) do
            if type(x) ~= "userdata" then
                out[k] = x
            end
        end
    end
    return out
end

local function str(x: any): string?
    x = v(x)
    if x == nil then
        return nil
    end
    return tostring(x)
end

local function named(x: any, field: string?): string?
    x = v(x)
    if type(x) == "table" then
        return str(x[field or "name"])
    end
    return str(x)
end

M.DEFAULT_FIELDS = {
    "summary", "status", "priority", "assignee", "reporter", "issuetype", "project",
    "labels", "created", "updated", "resolution", "duedate", "parent",
}

-- Issue keys are PROJECT-123. Returns the upper-cased key or raises INVALID_ARGS (keys are
-- interpolated into JQL and URLs, so nothing else may pass).
function M.check_key(key: any): string
    if type(key) ~= "string" or string.match(key, "^[A-Za-z][A-Za-z0-9_]*%-[1-9]%d*$") == nil then
        error("INVALID_ARGS: invalid Jira issue key " .. tostring(key))
    end
    return string.upper(key)
end

local CATEGORY = { new = "todo", indeterminate = "in_progress", done = "done" }

function M.status_category(status: any): string?
    status = v(status)
    if type(status) ~= "table" then
        return nil
    end
    local cat = v(status.statusCategory)
    if type(cat) ~= "table" then
        return nil
    end
    return CATEGORY[str(cat.key) or ""] or str(cat.key)
end

-- Plain text of an Atlassian Document Format node (v3) or a v2 string.
function M.adf_text(doc: any): string?
    doc = v(doc)
    if doc == nil then
        return nil
    end
    if type(doc) == "string" then
        return doc
    end
    if type(doc) ~= "table" then
        return tostring(doc)
    end
    local out = {}
    local function walk(node: any)
        node = v(node)
        if type(node) ~= "table" then
            return
        end
        if node.type == "text" and type(v(node.text)) == "string" then
            table.insert(out, node.text)
        elseif node.type == "hardBreak" then
            table.insert(out, "\n")
        end
        local content = v(node.content)
        if type(content) == "table" then
            for _, child in ipairs(content) do
                walk(child)
            end
        end
        local t = node.type
        if t == "paragraph" or t == "heading" or t == "listItem" or t == "codeBlock" or t == "blockquote" then
            table.insert(out, "\n")
        end
    end
    walk(doc)
    local text = table.concat(out)
    text = string.gsub(text, "\n+$", "")
    return text
end

-- Atlassian Document Format for plain text (Cloud v3 bodies): one paragraph per line.
function M.adf(text: string): any
    local content = {}
    for line in string.gmatch(text .. "\n", "([^\n]*)\n") do
        if string.match(line, "%S") then
            table.insert(content, { type = "paragraph", content = { { type = "text", text = line } } })
        end
    end
    if #content == 0 then
        error("INVALID_ARGS: text must not be blank")
    end
    return { type = "doc", version = 1, content = content }
end

-- Body text for the configured API version: ADF on v3, plain string on v2.
function M.body(text: string, api_version: string): any
    if api_version == "2" then
        return text
    end
    return M.adf(text)
end

-- pm.issue@1.
function M.issue(raw: any, base_url: string?, extra_fields: any): any
    local key = str(raw.key) or ""
    local f = v(raw.fields)
    local issue: { [string]: any }
    if type(f) == "table" then
        local assignee = v(f.assignee)
        local labels = {}
        for _, l in ipairs(v(f.labels) or {}) do
            table.insert(labels, tostring(l))
        end
        issue = {
            key = key,
            id = str(raw.id),
            summary = str(f.summary) or "",
            status = named(f.status) or "Unknown",
            status_category = M.status_category(f.status),
            priority = named(f.priority),
            assignee = named(assignee, "displayName"),
            assignee_id = if type(assignee) == "table" then (str(assignee.accountId) or str(assignee.name)) else nil,
            reporter = named(f.reporter, "displayName"),
            issue_type = named(f.issuetype),
            project = named(f.project, "key"),
            labels = labels,
            created = str(f.created),
            updated = str(f.updated),
            resolution = named(f.resolution),
            due_date = str(f.duedate),
            parent = named(f.parent, "key"),
        }
        if v(f.description) ~= nil then
            issue.description = M.adf_text(f.description)
        end
        if type(extra_fields) == "table" and #extra_fields > 0 then
            local extra = {}
            for _, name in ipairs(extra_fields) do
                extra[name] = v(f[name])
            end
            issue.fields = extra
        end
    else
        issue = {
            key = key,
            id = str(raw.id),
            summary = str(raw.summary) or "",
            status = str(raw.status) or "Unknown",
            status_category = str(raw.status_category),
            priority = str(raw.priority),
            assignee = str(raw.assignee),
            assignee_id = str(raw.assignee_id),
            reporter = str(raw.reporter),
            issue_type = str(raw.issue_type),
            project = str(raw.project) or string.match(key, "^([^-]+)%-"),
            labels = v(raw.labels) or {},
            created = str(raw.created),
            updated = str(raw.updated),
            resolution = str(raw.resolution),
            due_date = str(raw.due_date),
            parent = str(raw.parent),
            description = str(raw.description),
        }
    end
    if base_url ~= nil and key ~= "" then
        issue.url = base_url .. "/browse/" .. key
    else
        issue.url = str(raw.url)
    end
    return issue
end

-- pm.transition@1.
function M.transition(raw: any): any
    local to = v(raw.to)
    return {
        id = str(raw.id) or "",
        name = str(raw.name) or "",
        to_status = (type(to) == "table" and str(to.name)) or str(raw.to_status) or "",
        to_category = (type(to) == "table" and M.status_category(to)) or str(raw.to_category),
    }
end

-- Words that mark a status or transition as "closed without doing the work" (Won't Do, Won't
-- Fix, Canceled, Declined, Duplicate, Not Doing, ...). Matched as substrings of the name reduced
-- to ASCII letters and digits, so apostrophe variants (', ’, ‘, ´), spaces, punctuation and
-- suffixes ("Closed - Won't Fix", "Cancelled by customer") all match.
local NEGATIVE_WORDS = {
    "wont", "cancel", "reject", "declin", "duplicat", "invalid", "obsolete", "abandon",
    "notdoing", "notneeded", "notabug", "notfix", "cannotrepro", "notrepro", "incomplete",
    "outofscope", "nofix", "withdrawn", "discard",
}

local function squash(name: string?): string
    return (string.gsub(string.lower(name or ""), "[^a-z0-9]", ""))
end

-- Whether a status or transition name means the work was not done.
function M.is_negative(name: string?): boolean
    local s = squash(name)
    for _, w in ipairs(NEGATIVE_WORDS) do
        if string.find(s, w, 1, true) ~= nil then
            return true
        end
    end
    return false
end

-- Conventional status names of each category, in order of preference (squashed).
local PREFERRED_STATUS = {
    done = { "done", "resolved", "closed", "complete", "completed", "fixed" },
    in_progress = { "inprogress" },
    todo = { "todo", "open", "backlog" },
}

-- The transition to use (plus, when there is none, an optional reason). The target is one of:
-- - `transition_id`: exactly that transition.
-- - `target_category` (todo | in_progress | done): a status of that category. For `done`,
--   statuses that mean the work was not done (see `is_negative`) are never candidates. One
--   distinct status is used as is; with several, the conventional one of the category wins;
--   otherwise it is ambiguous.
-- - `target_status`: the status with that name (case-insensitive).
-- Transitions are matched by their resulting status, never by their own name, so a target
-- always identifies the resulting status (idempotency and reconcile depend on that). Among
-- several transitions into the chosen status, one with a negative name ("Won't Do" into Done,
-- whose post-function sets resolution Won't Do) is never picked unless the target status is
-- itself negative; a transition named like the status wins, else the first in workflow order.
function M.pick_transition(transitions: { any }, target_status: string?, transition_id: string?, target_category: string?): (any, string?)
    if transition_id ~= nil then
        for _, t in ipairs(transitions) do
            if t.id == transition_id then
                return t
            end
        end
        return nil
    end

    -- Candidate transitions grouped by resulting status, in workflow order.
    local groups: { [string]: { any } } = {}
    local order: { string } = {}
    local excluded: { string } = {}
    local want = squash(target_status)
    for _, t in ipairs(transitions) do
        local key = squash(t.to_status)
        local matches
        if target_category ~= nil then
            matches = t.to_category == target_category
            if matches and target_category == "done" and M.is_negative(t.to_status) then
                matches = false
                table.insert(excluded, t.name .. " -> " .. t.to_status)
            end
        else
            matches = key ~= "" and key == want
        end
        if matches and not M.is_negative(t.to_status) and M.is_negative(t.name) then
            matches = false
            table.insert(excluded, t.name .. " -> " .. t.to_status)
        end
        if matches then
            if groups[key] == nil then
                groups[key] = {}
                table.insert(order, key)
            end
            table.insert(groups[key], t)
        end
    end

    local chosen_key: string? = nil
    if #order == 1 then
        chosen_key = order[1]
    elseif #order > 1 then
        for _, name in ipairs(PREFERRED_STATUS[target_category or ""] or {}) do
            if groups[name] ~= nil then
                chosen_key = name
                break
            end
        end
        if chosen_key == nil then
            local names = {}
            for _, k in ipairs(order) do
                table.insert(names, groups[k][1].to_status)
            end
            return nil, "several statuses of category '" .. tostring(target_category) .. "' are reachable ("
                .. table.concat(names, ", ") .. "); pass `status` to choose one"
        end
    end
    if chosen_key == nil then
        if #excluded > 0 then
            return nil, "only " .. table.concat(excluded, ", ")
                .. " would get there, which means the work was not done and is never picked automatically;"
                .. " pass `transition_id` (or a `status` that names it) to use it"
        end
        return nil
    end
    local group = groups[chosen_key]
    for _, t in ipairs(group) do
        if squash(t.name) == chosen_key then
            return t
        end
    end
    return group[1]
end

-- Whether an issue already satisfies a transition target (status name or category).
function M.at_target(issue: any, target_status: string?, target_category: string?): boolean
    if target_category ~= nil then
        return issue.status_category == target_category
    end
    return target_status ~= nil and string.lower(issue.status or "") == string.lower(target_status)
end

-- Normalized transition target of transition args: an explicit status name, a status
-- category, or (neither) the `done` category.
function M.target_of(args: any): (string?, string?)
    local status = v(args.status)
    local category = v(args.status_category)
    if status ~= nil and status ~= "" then
        return status, nil
    end
    if category ~= nil and category ~= "" then
        return nil, category
    end
    return nil, "done"
end

function M.transition_names(transitions: { any }): string
    local names = {}
    for _, t in ipairs(transitions) do
        table.insert(names, t.name .. " -> " .. t.to_status)
    end
    return if #names == 0 then "none" else table.concat(names, ", ")
end

return M
