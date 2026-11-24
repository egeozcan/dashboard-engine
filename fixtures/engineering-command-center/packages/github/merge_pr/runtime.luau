--!strict
local Runtime = {}

function Runtime.dry_run(ctx)
    local pr = ctx:await(ctx.inputs.github:call("get_pr", {
        repository = ctx.args.repository,
        number = ctx.args.number,
    }))
    local feasible = (pr.state == "open")
    local warnings = {}
    if not feasible then
        table.insert(warnings, "PR is currently in state '" .. tostring(pr.state) .. "'")
    end
    return {
        feasible = feasible,
        warnings = warnings,
        preconditions = {
            { check = "pr_state", expected = "open", actual = pr.state },
        },
        effects = {
            repository = ctx.args.repository,
            number = ctx.args.number,
            title = pr.title,
            action = "squash_and_merge",
        },
    }
end

function Runtime.execute(ctx)
    local pr = ctx:await(ctx.inputs.github:call("get_pr", {
        repository = ctx.args.repository,
        number = ctx.args.number,
    }))
    local res = ctx:await(ctx.inputs.github:call("merge_pr", {
        repository = ctx.args.repository,
        number = ctx.args.number,
    }))
    local jira_key = ctx.args.issue_key or string.match(pr.title or "", "([A-Z][A-Z0-9]+%-%d+)")
    ctx:await(ctx:emit("github.pr_merged", {
        repository = ctx.args.repository,
        number = ctx.args.number,
        title = pr.title,
        issue_key = jira_key,
        source = "action",
    }, true))
    return {
        merged = res.merged == true,
        sha = tostring(res.sha or ""),
    }
end

function Runtime.reconcile(ctx)
    local pr = ctx:await(ctx.inputs.github:call("get_pr", {
        repository = ctx.args.repository,
        number = ctx.args.number,
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
