--!strict
local Runtime = {}

function Runtime.dry_run(ctx)
    return {
        feasible = true,
        warnings = {},
        effects = {
            issue_key = ctx.args.key,
            target_status = ctx.args.status or "Done",
        },
    }
end

function Runtime.execute(ctx)
    return ctx:await(ctx.inputs.jira:call("transition_issue", {
        key = ctx.args.key,
        status = ctx.args.status or "Done",
    }))
end

return Runtime
