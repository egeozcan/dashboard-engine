local dash = require("dash")
local Runtime = require("runtime")

-- Fake ctx: answers requests from `routes` ("METHOD url" -> response) and records them.
local function fake(cfg, routes, operation, args)
    local ctx = { config = cfg, operation = operation, args = args, sent = {} }
    ctx.http = {
        request = function(_self, req)
            table.insert(ctx.sent, req)
            local resp = routes[req.method .. " " .. req.url]
            if resp == nil then
                error("unexpected request " .. req.method .. " " .. req.url)
            end
            return resp
        end,
    }
    function ctx:await(v)
        return v
    end
    return ctx
end

local CFG = { base_url = "https://api.example.com/v1/", headers = { ["X-Team"] = "core" }, query = { ["api-version"] = "2" } }

dash.test("get extracts result_path, sends static headers/query and the credential", function(_)
    local ctx = fake(CFG, {
        ["GET https://api.example.com/v1/services?api-version=2&team=core%20x"] = {
            status = 200, headers = {}, body = { data = { items = { { id = "a" }, { id = "b" } } } },
        },
    }, "get", { path = "/services", query = { team = "core x" }, result_path = "data.items" })
    local out = Runtime.call(ctx)
    dash.assert_eq(out, { { id = "a" }, { id = "b" } }, "extracted items")
    dash.assert_eq(ctx.sent[1].credential, "auth", "credential handle attached")
    dash.assert_eq(ctx.sent[1].headers["X-Team"], "core", "static header")
    dash.assert_eq(ctx.sent[1].headers.Accept, "application/json", "default Accept")
end)

dash.test("auth = none sends anonymous requests", function(_)
    local ctx = fake({ base_url = "https://x.test", auth = "none" }, {
        ["GET https://x.test/ping"] = { status = 200, headers = {}, body = { ok = true } },
    }, "get", { path = "/ping" })
    dash.assert_eq(Runtime.call(ctx), { ok = true }, "body")
    dash.assert(ctx.sent[1].credential == nil, "no credential")
end)

dash.test("paths that could leave base_url are refused before any request", function(_)
    for _, bad in ipairs({ "services", "//evil.test/x", "/a/../b", "/a/%2e%2E/b", "/a\\b", "/a b", "/x#frag", "https://evil.test" }) do
        local ctx = fake(CFG, {}, "get", { path = bad })
        local ok, err = pcall(Runtime.call, ctx)
        dash.assert(not ok, "refused: " .. bad)
        dash.assert(string.find(tostring(err), "INVALID_ARGS", 1, true) ~= nil, tostring(err))
        dash.assert_eq(#ctx.sent, 0, "nothing sent for " .. bad)
    end
end)

dash.test("link pagination follows rel=next on the same origin and stops at max_pages", function(_)
    local base = "https://api.example.com/v1"
    local routes = {
        ["GET " .. base .. "/items?api-version=2&per_page=2"] = {
            status = 200, headers = { link = '<' .. base .. '/items?page=2>; rel="next"' }, body = { 1, 2 },
        },
        ["GET " .. base .. "/items?page=2"] = {
            status = 200, headers = { link = '<' .. base .. '/items?page=3>; rel="next"' }, body = { 3, 4 },
        },
        ["GET " .. base .. "/items?page=3"] = { status = 200, headers = {}, body = { 5 } },
    }
    local all = Runtime.call(fake(CFG, routes, "get", { path = "/items", paginate = { mode = "link", size_param = "per_page", page_size = 2 } }))
    dash.assert_eq(all, { 1, 2, 3, 4, 5 }, "all pages")
    local capped = Runtime.call(fake(CFG, routes, "get", { path = "/items", max_pages = 2, paginate = { mode = "link", size_param = "per_page", page_size = 2 } }))
    dash.assert_eq(capped, { 1, 2, 3, 4 }, "max_pages caps the walk")

    local evil = {
        ["GET " .. base .. "/items?api-version=2"] = {
            status = 200, headers = { link = '<https://evil.test/steal>; rel="next"' }, body = { 1 },
        },
    }
    local ok, err = pcall(Runtime.call, fake(CFG, evil, "get", { path = "/items", paginate = { mode = "link" } }))
    dash.assert(not ok and string.find(tostring(err), "leaves", 1, true) ~= nil, tostring(err))
end)

dash.test("cursor, page and offset pagination", function(_)
    local b = "https://x.test"
    local cfg = { base_url = b }
    local cursor = Runtime.call(fake(cfg, {
        ["GET " .. b .. "/e?limit=2"] = { status = 200, headers = {}, body = { data = { 1, 2 }, meta = { next = "c2" } } },
        ["GET " .. b .. "/e?after=c2&limit=2"] = { status = 200, headers = {}, body = { data = { 3 }, meta = { next = nil } } },
    }, "get", { path = "/e", paginate = { mode = "cursor", items_path = "data", cursor_path = "meta.next", cursor_param = "after", size_param = "limit", page_size = 2 } }))
    dash.assert_eq(cursor, { 1, 2, 3 }, "cursor pages")

    local paged = Runtime.call(fake(cfg, {
        ["GET " .. b .. "/p?page=1&per_page=2"] = { status = 200, headers = {}, body = { 1, 2 } },
        ["GET " .. b .. "/p?page=2&per_page=2"] = { status = 200, headers = {}, body = { 3 } },
    }, "get", { path = "/p", paginate = { mode = "page", page_size = 2 } }))
    dash.assert_eq(paged, { 1, 2, 3 }, "page pages")

    local offset = Runtime.call(fake(cfg, {
        ["GET " .. b .. "/o?limit=2&offset=0"] = { status = 200, headers = {}, body = { rows = { 1, 2 } } },
        ["GET " .. b .. "/o?limit=2&offset=2"] = { status = 200, headers = {}, body = { rows = {} } },
    }, "get", { path = "/o", paginate = { mode = "offset", items_path = "rows", page_size = 2 } }))
    dash.assert_eq(offset, { 1, 2 }, "offset pages")
end)

dash.test("errors carry stable codes and upstream messages; 404 can be allowed", function(_)
    local b = "https://x.test"
    local cases = {
        { status = 401, body = { message = "Bad credentials" }, code = "UNAUTHORIZED" },
        { status = 403, body = { message = "nope" }, code = "FORBIDDEN" },
        { status = 403, body = {}, headers = { ["x-ratelimit-remaining"] = "0", ["x-ratelimit-reset"] = "1700000000" }, code = "RATE_LIMITED" },
        { status = 429, body = "slow down", headers = { ["retry-after"] = "30" }, code = "RATE_LIMITED" },
        { status = 404, body = { error = { message = "missing" } }, code = "NOT_FOUND" },
        { status = 422, body = { errors = { { message = "title is required" } } }, code = "INVALID_REQUEST" },
        { status = 503, body = "down", code = "UPSTREAM_ERROR" },
    }
    for _, c in ipairs(cases) do
        local ctx = fake({ base_url = b }, {
            ["GET " .. b .. "/r"] = { status = c.status, headers = c.headers or {}, body = c.body },
        }, "get", { path = "/r" })
        local ok, err = pcall(Runtime.call, ctx)
        dash.assert(not ok, "status " .. c.status .. " fails")
        dash.assert(string.find(tostring(err), c.code .. ":", 1, true) ~= nil, tostring(err))
    end
    local ok, err = pcall(Runtime.call, fake({ base_url = b }, {
        ["GET " .. b .. "/r"] = { status = 429, headers = { ["retry-after"] = "30" }, body = "slow down" },
    }, "get", { path = "/r" }))
    dash.assert(not ok and string.find(tostring(err), "retry after 30s", 1, true) ~= nil, tostring(err))
    local nf = Runtime.call(fake({ base_url = b }, {
        ["GET " .. b .. "/r"] = { status = 404, headers = {}, body = {} },
    }, "get", { path = "/r", allow_not_found = true }))
    dash.assert(nf == nil, "404 allowed")
end)

dash.test("writes send JSON with an idempotency key; 5xx is an unknown outcome, 4xx a failure", function(_)
    local b = "https://x.test"
    local ctx = fake({ base_url = b }, {
        ["POST " .. b .. "/deploys"] = { status = 201, headers = { location = b .. "/deploys/9" }, body = { id = 9 } },
    }, "request", { method = "POST", path = "/deploys", body = { ref = "main" }, idempotency_key = "k1" })
    local res = Runtime.call(ctx)
    dash.assert_eq(res, { status = 201, body = { id = 9 }, location = b .. "/deploys/9" }, "write result")
    dash.assert_eq(ctx.sent[1].body, { ref = "main" }, "JSON body")
    dash.assert_eq(ctx.sent[1].headers["Idempotency-Key"], "k1", "idempotency header")

    local ok, err = pcall(Runtime.call, fake({ base_url = b }, {
        ["PUT " .. b .. "/x"] = { status = 502, headers = {}, body = "bad gateway" },
    }, "request", { method = "PUT", path = "/x", body = {} }))
    dash.assert(not ok and string.find(tostring(err), "UNKNOWN_OUTCOME", 1, true) ~= nil, tostring(err))
    local ok2, err2 = pcall(Runtime.call, fake({ base_url = b }, {
        ["DELETE " .. b .. "/x"] = { status = 409, headers = {}, body = { message = "locked" } },
    }, "request", { method = "DELETE", path = "/x" }))
    dash.assert(not ok2 and string.find(tostring(err2), "CONFLICT: HTTP DELETE /x returned HTTP 409: locked", 1, true) ~= nil, tostring(err2))
end)

dash.test("relative Link targets resolve against the page; links leaving the base path are refused", function(_)
    local cfg = { base_url = "https://x.test/api/v1" }
    local routes = {
        ["GET https://x.test/api/v1/items"] = { status = 200, headers = { link = '</api/v1/items?page=2>; rel="next"' }, body = { 1 } },
        ["GET https://x.test/api/v1/items?page=2"] = { status = 200, headers = { link = '<?page=3>; rel="next"' }, body = { 2 } },
        ["GET https://x.test/api/v1/items?page=3"] = { status = 200, headers = { link = '<items?page=4>; rel="next"' }, body = { 3 } },
        ["GET https://x.test/api/v1/items?page=4"] = { status = 200, headers = {}, body = { 4 } },
    }
    local ctx = fake(cfg, routes, "get", { path = "/items", paginate = { mode = "link" } })
    dash.assert_eq(Runtime.call(ctx), { 1, 2, 3, 4 }, "absolute-path, query-only and relative links")
    dash.assert_eq(ctx.sent[1].confine_to, "https://x.test/api/v1", "the broker confines every hop to base_url")
    for _, bad in ipairs({ "</admin>", "<https://x.test/api/v10/x>", "<../x>", "<//evil.test/x>", "</api/v1/..%2Fadmin>" }) do
        local r = { ["GET https://x.test/api/v1/items"] = { status = 200, headers = { link = bad .. '; rel="next"' }, body = { 1 } } }
        local ok, err = pcall(Runtime.call, fake(cfg, r, "get", { path = "/items", paginate = { mode = "link" } }))
        dash.assert(not ok and string.find(tostring(err), "leaves", 1, true) ~= nil, bad .. ": " .. tostring(err))
    end
end)

dash.test("encoded traversal in paths and unusable base URLs are refused", function(_)
    for _, bad in ipairs({ "/..%2Fadmin", "/x/%2E%2E%5Cadmin", "/a/..%2f..%2fetc" }) do
        local ctx = fake(CFG, {}, "get", { path = bad })
        local ok, err = pcall(Runtime.call, ctx)
        dash.assert(not ok and string.find(tostring(err), "INVALID_ARGS", 1, true) ~= nil, bad .. ": " .. tostring(err))
    end
    local ok = Runtime.call(fake({ base_url = "https://x.test" }, {
        ["GET https://x.test/projects/group%2Fproject"] = { status = 200, headers = {}, body = { id = 1 } },
    }, "get", { path = "/projects/group%2Fproject" }))
    dash.assert_eq(ok, { id = 1 }, "an encoded slash inside a segment is allowed")
    for _, base in ipairs({ "https://x.test/api?tenant=1", "https://x.test/api#frag", "https://user:pw@x.test/api", "ftp://x.test" }) do
        local okb, err = pcall(Runtime.call, fake({ base_url = base }, {}, "get", { path = "/x" }))
        dash.assert(not okb and string.find(tostring(err), "CONFIG", 1, true) ~= nil, base .. ": " .. tostring(err))
    end
end)

dash.test("JSON nulls in optional args read as absent", function(_)
    local args = dash.json.decode('{"path":"/r","paginate":null,"allow_not_found":null,"result_path":null,"query":{"a":null,"b":1}}')
    local ctx = fake({ base_url = "https://x.test" }, {
        ["GET https://x.test/r?b=1"] = { status = 404, headers = {}, body = {} },
    }, "get", args)
    local ok, err = pcall(Runtime.call, ctx)
    dash.assert(not ok and string.find(tostring(err), "NOT_FOUND", 1, true) ~= nil, "a null allow_not_found does not allow 404s: " .. tostring(err))
    local w = fake({ base_url = "https://x.test" }, {
        ["POST https://x.test/w"] = { status = 200, headers = {}, body = {} },
    }, "request", dash.json.decode('{"method":"POST","path":"/w","idempotency_key":null,"body":{"x":1}}'))
    Runtime.call(w)
    dash.assert(w.sent[1].headers["Idempotency-Key"] == nil, "no idempotency header for a null key")
end)

dash.test("an object where a list of items is expected is an error, not zero rows", function(_)
    local ok, err = pcall(Runtime.call, fake({ base_url = "https://x.test" }, {
        ["GET https://x.test/e?limit=100"] = { status = 200, headers = {}, body = { data = { id = 1 } } },
    }, "get", { path = "/e", paginate = { mode = "cursor", items_path = "data", size_param = "limit" } }))
    dash.assert(not ok and string.find(tostring(err), "INVALID_RESPONSE", 1, true) ~= nil, tostring(err))
end)

dash.test("JSON nulls inside a request body are sent, not dropped", function(_)
    local w = fake({ base_url = "https://x.test" }, {
        ["PATCH https://x.test/issues/7"] = { status = 200, headers = {}, body = {} },
    }, "request", dash.json.decode('{"method":"PATCH","path":"/issues/7","body":{"milestone":null,"title":"x"}}'))
    Runtime.call(w)
    dash.assert_eq(dash.json.encode(w.sent[1].body), '{"milestone":null,"title":"x"}', "null kept in the body")
end)

dash.test("the Instance's static query and headers win over per-call ones", function(_)
    local ctx = fake({ base_url = "https://x.test", query = { project = "public" }, headers = { ["X-Tenant"] = "a" } }, {
        ["GET https://x.test/items?project=public"] = { status = 200, headers = {}, body = {} },
    }, "get", { path = "/items", query = { project = "internal" }, headers = { ["x-tenant"] = "b", accept = "text/csv" } })
    Runtime.call(ctx)
    local h = ctx.sent[1].headers
    dash.assert_eq(h["X-Tenant"], "a", "static header kept")
    dash.assert(h["x-tenant"] == nil, "per-call header with the same name dropped")
    dash.assert_eq(h.accept, "text/csv", "the default Accept can be replaced")
    dash.assert(h.Accept == nil, "no second Accept")
end)

dash.test("link pagination keeps going when the next URL contains commas", function(_)
    local base = "https://x.test"
    local ctx = fake({ base_url = base }, {
        ["GET " .. base .. "/items"] = { status = 200, headers = { link = '<' .. base .. '/items?fields=a,b&page=2>; rel="next"' }, body = { 1 } },
        ["GET " .. base .. "/items?fields=a,b&page=2"] = { status = 200, headers = {}, body = { 2 } },
    }, "get", { path = "/items", paginate = { mode = "link" } })
    dash.assert_eq(Runtime.call(ctx), { 1, 2 }, "both pages")
end)
