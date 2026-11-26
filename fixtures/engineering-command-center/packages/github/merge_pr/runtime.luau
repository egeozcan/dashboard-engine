--!strict
local model = require("github.model")

local Runtime = {}

-- mergeable_state values that make the merge API refuse (405).
local BLOCKING = {
    dirty = "the branch has merge conflicts",
    blocked = "branch protection blocks the merge (required reviews or checks)",
    draft = "the pull request is a draft",
}
-- mergeable_state values worth a warning but not blocking.
local WARN = {
    behind = "the head branch is behind its base",
    unstable = "some non-required checks are failing",
    unknown = "GitHub has not computed mergeability yet",
}

local function method_of(ctx: any): string
    local args = model.clean(ctx.args)
    return args.merge_method or "squash"
end

function Runtime.dry_run(ctx: any): any
    local args = model.clean(ctx.args)
    local pr = ctx:await(ctx.inputs.github:call("get_pr", {
        repository = args.repository,
        number = args.number,
    }))
    local warnings = {}
    local feasible = (pr.state == "open")
    if not feasible then
        table.insert(warnings, "PR is currently in state '" .. tostring(pr.state) .. "'")
    end
    local preconditions = {
        { check = "pr_state", expected = "open", actual = pr.state },
    }
    if pr.state == "open" and args.expected_head_sha ~= nil and pr.head_sha ~= nil
        and args.expected_head_sha ~= pr.head_sha then
        feasible = false
        table.insert(warnings, "the head moved to " .. pr.head_sha .. " since " .. args.expected_head_sha .. " was reviewed")
    end
    if pr.state == "open" then
        if pr.draft == true then
            feasible = false
            table.insert(warnings, "PR is a draft")
        end
        table.insert(preconditions, { check = "draft", expected = false, actual = pr.draft == true })
        local ms = pr.mergeable_state
        if ms ~= nil then
            table.insert(preconditions, { check = "mergeable_state", expected = "clean", actual = ms })
            if BLOCKING[ms] and not (ms == "draft" and pr.draft == true) then
                feasible = false
                table.insert(warnings, "Not mergeable: " .. BLOCKING[ms])
            elseif WARN[ms] then
                table.insert(warnings, "Warning: " .. WARN[ms])
            end
        end
        if pr.mergeable == false and (ms == nil or not BLOCKING[ms]) then
            feasible = false
            table.insert(warnings, "GitHub reports the PR as not mergeable")
        end
    end
    return {
        feasible = feasible,
        warnings = warnings,
        preconditions = preconditions,
        effects = {
            repository = args.repository,
            number = args.number,
            title = pr.title,
            head_sha = pr.head_sha,
            action = method_of(ctx) .. "_and_merge",
        },
    }
end

function Runtime.execute(ctx: any): any
    local args = model.clean(ctx.args)
    local pr = ctx:await(ctx.inputs.github:call("get_pr", {
        repository = args.repository,
        number = args.number,
    }))
    -- Merge exactly the reviewed head: `expected_head_sha` (what the Dry run showed) or the head
    -- just read. Commits pushed in between make GitHub refuse (409) instead of merging them.
    local expected = args.expected_head_sha
    if expected ~= nil and pr.head_sha ~= nil and expected ~= pr.head_sha then
        error("CONFLICT: the head of " .. args.repository .. "#" .. args.number
            .. " moved from " .. expected .. " to " .. pr.head_sha .. " since it was reviewed")
    end
    local res = ctx:await(ctx.inputs.github:call("merge_pr", {
        repository = args.repository,
        number = args.number,
        merge_method = method_of(ctx),
        sha = expected or pr.head_sha,
    }))
    local jira_key = args.issue_key or model.issue_key(pr.title)
    ctx:await(ctx:emit("github.pr_merged", {
        repository = args.repository,
        number = args.number,
        title = pr.title,
        issue_key = jira_key,
        source = "action",
    }, true))
    return {
        merged = res.merged == true,
        sha = tostring(res.sha or ""),
    }
end

function Runtime.reconcile(ctx: any): any
    local args = model.clean(ctx.args)
    local pr = ctx:await(ctx.inputs.github:call("get_pr", {
        repository = args.repository,
        number = args.number,
    }))
    if pr.state == "merged" or pr.merged == true then
        return { status = "succeeded", merged = true, number = pr.number }
    end
    if pr.state == "closed" then
        -- Closed without merging: the merge verifiably did not happen.
        return { status = "failed", merged = false, number = pr.number }
    end
    -- Still open: the merge may yet land (or never did); stay unknown and let the host retry.
    return { status = "unknown" }
end

return Runtime
