local dash = require("dash")
local Runtime = require("runtime")

dash.test("build fills the path and uses the remaining args as the body", function(_)
    local req = Runtime.build({ path = "/services/{id}/deploys", body = { env = "prod" } }, { id = "api", ref = "main" })
    dash.assert_eq(req.method, "POST", "default method")
    dash.assert_eq(req.path, "/services/api/deploys", "path")
    dash.assert_eq(req.body, { env = "prod", ref = "main" }, "body merges args over config.body")
    local explicit = Runtime.build({ method = "PUT", path = "/x" }, { body = { a = 1 }, ignored = true })
    dash.assert_eq(explicit.body, { a = 1 }, "explicit body wins")
    local del = Runtime.build({ method = "DELETE", path = "/items/{id}" }, { id = 5 })
    dash.assert(del.body == nil, "DELETE without extra args has no body")
end)

dash.test("dry_run reports the request without calling the connector", function(_)
    local ctx = { config = { method = "PATCH", path = "/flags/{name}" }, args = { name = "beta", enabled = true }, inputs = {} }
    local rep = Runtime.dry_run(ctx)
    dash.assert(rep.feasible == true, "feasible")
    dash.assert_eq(rep.effects, { method = "PATCH", path = "/flags/beta", body = { enabled = true } }, "effects")
end)

dash.test("execute forwards the idempotency key when configured", function(_)
    local sent
    local ctx = {
        config = { path = "/x", idempotency_header = "Idempotency-Key" },
        args = { a = 1 },
        idempotency_key = "exec-1",
        inputs = { api = { call = function(_self, op, req)
            sent = { op = op, req = req }
            return { status = 200, body = {} }
        end } },
    }
    function ctx:await(v)
        return v
    end
    Runtime.execute(ctx)
    dash.assert_eq(sent.op, "request", "write operation")
    dash.assert_eq(sent.req.idempotency_key, "exec-1", "key")
    dash.assert_eq(sent.req.idempotency_header, "Idempotency-Key", "header")
end)

dash.test("null arguments merged into the body are sent as JSON null", function(_)
    local sent
    local ctx = {
        config = { method = "PATCH", path = "/issues/{id}" },
        args = dash.json.decode('{"id":7,"milestone":null,"title":"x"}'),
        inputs = { api = { call = function(_self, _op, req)
            sent = req
            return { status = 200, body = {} }
        end } },
    }
    function ctx:await(v)
        return v
    end
    Runtime.execute(ctx)
    dash.assert_eq(sent.path, "/issues/7", "path")
    dash.assert_eq(dash.json.encode(sent.body), '{"milestone":null,"title":"x"}', "null kept")
    local ok, err = pcall(Runtime.build, { path = "/issues/{id}" }, dash.json.decode('{"id":null}'))
    dash.assert(not ok and string.find(tostring(err), "INVALID_ARGS", 1, true) ~= nil, "a null path parameter is missing: " .. tostring(err))
end)
