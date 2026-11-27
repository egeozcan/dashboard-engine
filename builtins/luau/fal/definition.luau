local dash = require("dash")

local request_args = dash.object {
    endpoint_id = dash.string(),
    request_id = dash.string(),
}

local status = dash.enum { "IN_QUEUE", "IN_PROGRESS", "COMPLETED" }

local queue_state = dash.object {
    request_id = dash.string(),
    status = status,
    queue_position = dash.optional(dash.integer()),
    logs = dash.optional(dash.list(dash.any())),
    metrics = dash.optional(dash.map(dash.any())),
    error = dash.optional(dash.string()),
}

return dash.connector {
    id = "dash.fal",
    description = "Submit model inputs to the fal.ai inference queue, read status and results, and request cancellation",
    runtime = "runtime.luau",
    handler = "call",
    libraries = { "dash.http_util" },
    credentials = { "auth" },
    capabilities = { "queue.submit", "queue.read", "queue.cancel" },
    config = dash.object {
        base_url = dash.optional(dash.string()),
        mock = dash.optional(dash.map(dash.any())),
    },
    operations = {
        submit = {
            description = "Queue a model-specific JSON input at an endpoint such as fal-ai/flux/schnell",
            required_capabilities = { "queue.submit" },
            mode = "write",
            args = dash.object { endpoint_id = dash.string(), input = dash.map(dash.any()) },
            result = dash.object {
                request_id = dash.string(),
                status = dash.optional(status),
                queue_position = dash.optional(dash.integer()),
                status_url = dash.optional(dash.string()),
                response_url = dash.optional(dash.string()),
                cancel_url = dash.optional(dash.string()),
            },
        },
        status = {
            description = "Read the queue state, optionally including runner logs",
            required_capabilities = { "queue.read" },
            mode = "read",
            args = dash.object { endpoint_id = dash.string(), request_id = dash.string(), logs = dash.optional(dash.boolean()) },
            result = queue_state,
        },
        result = {
            description = "Read a completed request's model-specific JSON output",
            required_capabilities = { "queue.read" },
            mode = "read",
            args = request_args,
            result = dash.any(),
        },
        cancel = {
            description = "Request cancellation of a queued or running request",
            required_capabilities = { "queue.cancel" },
            mode = "write",
            args = request_args,
            result = dash.object { status = dash.string() },
        },
    },
}
