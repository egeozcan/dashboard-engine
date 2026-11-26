local dash = require("dash")
local Runtime = require("runtime")

dash.test("path placeholders take query parameters; the rest is forwarded", function(_)
    local args = Runtime.request_args(
        { path = "/repos/{owner}/{repo}/issues", query = { per_page = 50 }, result_path = "items" },
        { owner = "acme", repo = "a b", state = "open" }
    )
    dash.assert_eq(args.path, "/repos/acme/a%20b/issues", "encoded placeholders")
    dash.assert_eq(args.query, { per_page = 50, state = "open" }, "forwarded params")
    dash.assert_eq(args.result_path, "items", "result path")
    local strict = Runtime.request_args({ path = "/x", forward_params = false }, { a = 1 })
    dash.assert_eq(strict.query, {}, "forward_params = false")
    local ok, err = pcall(Runtime.request_args, { path = "/u/{id}" }, {})
    dash.assert(not ok and string.find(tostring(err), "'id' is required", 1, true) ~= nil, tostring(err))
    for _, bad in ipairs({ "..", ".", "../admin", "a/../b", "x\\..\\y" }) do
        local ok2, err2 = pcall(Runtime.request_args, { path = "/u/{id}" }, { id = bad })
        dash.assert(not ok2 and string.find(tostring(err2), "must not contain", 1, true) ~= nil, bad .. ": " .. tostring(err2))
    end
    dash.assert_eq(Runtime.request_args({ path = "/projects/{id}" }, { id = "group/project" }).path, "/projects/group%2Fproject", "encoded slash allowed")
end)

dash.test("fields map rows and single records", function(_)
    local fields = { id = "id", name = "attributes.name", first_tag = "tags.0" }
    dash.assert_eq(
        Runtime.map_fields({ { id = 1, attributes = { name = "a" }, tags = { "x", "y" } } }, fields),
        { { id = 1, name = "a", first_tag = "x" } },
        "list rows"
    )
    dash.assert_eq(Runtime.map_fields({ id = 2, attributes = { name = "b" } }, { id = "id", name = "attributes.name" }), { id = 2, name = "b" }, "single record")
    dash.assert_eq(Runtime.map_fields({}, fields), dash.array(), "empty list stays a JSON array")
end)

dash.test("query calls the connector's get operation", function(_)
    local seen
    local ctx = {
        config = { path = "/services/{team}", fields = { id = "id" } },
        query = { team = "core" },
        inputs = { api = { call = function(_self, op, args)
            seen = { op = op, args = args }
            return { { id = "api", extra = true } }
        end } },
    }
    function ctx:await(v)
        return v
    end
    dash.assert_eq(Runtime.query(ctx), { { id = "api" } }, "mapped rows")
    dash.assert_eq(seen.op, "get", "read operation")
    dash.assert_eq(seen.args.path, "/services/core", "filled path")
end)

dash.test("forwarded parameters never replace configured query parameters", function(_)
    local args = Runtime.request_args({ path = "/items", query = { project = "public" } }, { project = "internal", page = 2 })
    dash.assert_eq(args.query, { project = "public", page = 2 }, "configured project kept")
end)
